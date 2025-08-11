use crate::api::{ExecutablePipeline, Mapper, Reducer};
use crate::io::{ensure_dir, hash_to_partition, open_writer, read_bin_line,
    LocalFsSource, TextLineFormat, S3Source, Split, Source, Format};
use crate::sort::external_sort_by_key;
use crate::stats::StatsCollector;
use crate::slurm::SlurmEnv;
use anyhow::{Context, Result};
use rayon::prelude::*;
use std::fs;
use std::io::Write;
use memmap2::Mmap;
use std::path::Path;
use std::sync::{Arc, Mutex};
use tracing::{error, info, debug};
use crate::writer::{WriterPool, WriterJoiner};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

// moved to inner section below

pub struct RuntimePipeline {
    inputs: Vec<String>,
    output: Option<String>,
}

impl RuntimePipeline {
    pub fn new() -> Self { Self { inputs: vec![], output: None } }
}

impl Default for RuntimePipeline { fn default() -> Self { Self::new() } }

impl ExecutablePipeline for RuntimePipeline {
    fn add_input<T: Send + 'static>(&mut self, input_path: impl Into<String>) {
        let _ = std::marker::PhantomData::<T>;
        self.inputs.push(input_path.into());
    }

    fn add_output(&mut self, output_path: impl Into<String>) { self.output = Some(output_path.into()); }

    fn map_reduce<M, R>(&mut self, mapper: M, reducer: R) -> Result<()>
    where
        M: Mapper<Input = String> + Send + Sync + 'static,
        R: Reducer<Key = M::Key, ValueIn = M::Value> + Send + Sync + 'static,
    {
        // Detect environment and build execution plan
        let slurm = SlurmEnv::detect().unwrap_or({
            let pid = std::process::id();
            let ts = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_secs();
            let local_tasks = std::env::var("BJORN_LOCAL_TASKS").ok().and_then(|v| v.parse::<usize>().ok()).unwrap_or_else(|| num_cpus::get());
            SlurmEnv { job_id: format!("local-{}-{}", pid, ts), ntasks: local_tasks.max(1), node_id: 0, node_list: "localhost".into() }
        });
        let output_dir = self.output.clone().context("output not set")?;
        let keep_intermediates = std::env::var("BJORN_KEEP_INTERMEDIATES")
            .ok()
            .map(|v| {
                let v = v.to_ascii_lowercase();
                v == "1" || v == "true" || v == "yes"
            })
            .unwrap_or(false);

        // Configure Rayon threads based on environment (Slurm cpus-per-task or override)
        if std::env::var("RAYON_NUM_THREADS").is_err() {
            let n = std::env::var("BJORN_RAYON_THREADS").ok().and_then(|v| v.parse::<usize>().ok())
                .or_else(|| std::env::var("SLURM_CPUS_PER_TASK").ok().and_then(|v| v.parse::<usize>().ok()));
            if let Some(n) = n { if n > 0 { std::env::set_var("RAYON_NUM_THREADS", n.to_string()); } }
        }
        let launch_root = format!(".bjorn_runs/{}", slurm.job_id);
        ensure_dir(&launch_root)?;
        let map_out_dir = format!("{}/map_out", launch_root);
        let sort_out_dir = format!("{}/sort_out", launch_root);
        ensure_dir(&map_out_dir)?;
        ensure_dir(&sort_out_dir)?;

        // Prepare output directory (clean it before starting)
        if SlurmEnv::is_slurm() {
            if slurm.node_id == 0 {
                let _ = fs::remove_dir_all(&output_dir);
                ensure_dir(&output_dir)?;
                let barrier_path = format!("{}/barrier_output_cleared_0", launch_root);
                let _ = fs::write(&barrier_path, b"ok");
            } else {
                // wait for rank0 to clear output
                wait_for_barrier(&launch_root, "output_cleared", 1);
            }
        } else {
            let _ = fs::remove_dir_all(&output_dir);
            ensure_dir(&output_dir)?;
        }

        // inputs -> splits (local or s3)
        let mut all_splits: Vec<Split> = Vec::new();
        for inp in &self.inputs {
            if inp.starts_with("s3://") {
                let src = S3Source::from_uri(inp)?;
                let mut splits = src.list_splits(inp)?;
                all_splits.append(&mut splits);
            } else {
                let src = LocalFsSource;
                let mut splits = src.list_splits(inp)?;
                all_splits.append(&mut splits);
            }
        }

        // Determine task topology and plan what this process should do
        let is_slurm = SlurmEnv::is_slurm();
        let local_subtasks = if is_slurm {
            std::env::var("BJORN_LOCAL_SUBTASKS").ok().and_then(|v| v.parse::<usize>().ok())
                .or_else(|| std::env::var("SLURM_CPUS_PER_TASK").ok().and_then(|v| v.parse::<usize>().ok()))
                .unwrap_or_else(|| num_cpus::get())
                .max(1)
        } else { 1 };
        let global_ntasks_raw = if is_slurm { slurm.ntasks.max(1) * local_subtasks } else { slurm.ntasks.max(1) };
        let global_ntasks = global_ntasks_raw.min(all_splits.len().max(1));

        // partition files among logical tasks
        let chunks: Vec<Vec<Split>> = (0..global_ntasks)
            .map(|i| {
                all_splits
                    .iter()
                    .enumerate()
                    .filter(|(idx, _)| idx % global_ntasks == i)
                    .map(|(_, p)| p.clone())
                    .collect()
            })
            .collect();

        let mapper = Arc::new(mapper);
        let default_reducers = if is_slurm { slurm.ntasks.max(1) } else { global_ntasks };
        let num_reducers = std::env::var("BJORN_NUM_REDUCERS").ok().and_then(|v| v.parse::<usize>().ok()).unwrap_or(default_reducers).clamp(1, default_reducers);

        // Log environment and limits
        let fd_limit = get_fd_soft_limit();
        info!(
            slurm = %SlurmEnv::is_slurm(), job_id = %slurm.job_id, node_id = slurm.node_id, ntasks = global_ntasks, num_reducers = num_reducers,
            fd_soft_limit = fd_limit.unwrap_or(0),
            "Bjorn starting map phase"
        );

        // Writer pool shared within the process
        let flush_bytes = std::env::var("BJORN_FLUSH_BYTES").ok().and_then(|v| v.parse::<usize>().ok()).unwrap_or(16 * 1024 * 1024);
        let flush_interval_ms = std::env::var("BJORN_FLUSH_INTERVAL_MS").ok().and_then(|v| v.parse::<u64>().ok()).unwrap_or(200);
        let queue_cap = std::env::var("BJORN_WRITER_QUEUE_CAP").ok().and_then(|v| v.parse::<usize>().ok()).unwrap_or(1024);
        let (pool_inner, pool_joiner) = WriterPool::new(map_out_dir.clone(), slurm.node_id, num_reducers, flush_bytes, Duration::from_millis(flush_interval_ms), queue_cap)?;
        let writer_pool = Arc::new(pool_inner);
        let mut writer_joiner = pool_joiner;
        // ========== Map phase (inner, env-agnostic) ==========
        let mut stats = StatsCollector::new();
        let parallelize_splits = is_slurm; // hint: under Slurm, parallelize across files inside a task
        let map_phase_start = Instant::now();
        let my_task_ids: Vec<usize> = if is_slurm {
            (0..local_subtasks)
                .map(|k| slurm.node_id + k * slurm.ntasks)
                .filter(|&id| id < global_ntasks)
                .collect()
        } else { (0..global_ntasks).collect() };
        let map_stats_vec = run_map_phase(mapper.clone(), &my_task_ids, &chunks, num_reducers, writer_pool.clone(), &launch_root, parallelize_splits);

        // wait for all tasks to finish map when under slurm
        if is_slurm {
            let t0 = Instant::now();
            wait_for_barrier(&launch_root, "map_done", global_ntasks);
            info!(phase = "barrier_map_wait", wait_ms = t0.elapsed().as_millis() as u64, "Barrier wait for map phase completed");
        }

        // Ensure all writers are closed before proceeding
        writer_pool.close_all();
        writer_joiner.join_all();

        let map_phase_ms = map_phase_start.elapsed().as_millis() as u64;
        // aggregate via StatsCollector
        if !map_stats_vec.is_empty() {
            let per_task: Vec<(u64, u64, u64, u64)> = map_stats_vec
                .iter()
                .map(|s| (s.total_emits, s.total_bytes_out, s.total_flushes, s.wall_ms))
                .collect();
            stats.record_map(&per_task, map_phase_ms);
            // log
            if let Some(ms) = &stats.map {
                info!(phase = "map",
                      tasks = ms.tasks,
                      total_emits = ms.total_emits,
                      total_bytes_out = ms.total_bytes_out,
                      total_flushes = ms.total_flushes,
                      min_task_ms = ms.min_task_ms,
                      max_task_ms = ms.max_task_ms,
                      wall_ms = ms.wall_ms,
                      "Map phase complete");
            }
        }

        // ========== Sort/shuffle (inner, env-agnostic) ==========
        let sort_phase_start = Instant::now();
        let my_reducers: Vec<usize> = if is_slurm { vec![slurm.node_id] } else { (0..num_reducers).collect() };
        let sort_stats_vec = run_sort_phase(&my_reducers, &map_out_dir, &sort_out_dir, &launch_root);
        if is_slurm {
            // wait for all reducers' sort
            let t0 = Instant::now();
            wait_for_barrier(&launch_root, "sort_done", num_reducers);
            info!(phase = "barrier_sort_wait", wait_ms = t0.elapsed().as_millis() as u64, "Barrier wait for sort phase completed");
        }
        let sort_phase_ms = sort_phase_start.elapsed().as_millis() as u64;
        if !sort_stats_vec.is_empty() {
            let total_lines: u64 = sort_stats_vec.iter().map(|s| s.lines_in).sum();
            let total_bytes: u64 = sort_stats_vec.iter().map(|s| s.bytes_in).sum();
            let min_wall = sort_stats_vec.iter().map(|s| s.wall_ms).min().unwrap_or(0);
            let max_wall = sort_stats_vec.iter().map(|s| s.wall_ms).max().unwrap_or(0);
            let total_sort_ms: u64 = sort_stats_vec.iter().map(|s| s.sort_ms).sum();
            let total_mmap_ms: u64 = sort_stats_vec.iter().map(|s| s.io_read_ms).sum();
            let total_scan_ms: u64 = sort_stats_vec.iter().map(|s| s.scan_ms).sum();
            let total_io_write_ms: u64 = sort_stats_vec.iter().map(|s| s.io_write_ms).sum();
            stats.record_sort(sort_stats_vec.len(), total_lines, total_bytes, min_wall, max_wall, sort_phase_ms);
            if let Some(ss) = &stats.sort {
                info!(phase = "sort",
                      reducers = ss.reducers, total_lines = ss.total_lines, total_bytes = ss.total_bytes,
                      total_sort_ms = total_sort_ms, total_mmap_ms = total_mmap_ms, total_scan_ms = total_scan_ms, total_io_write_ms = total_io_write_ms,
                      min_reducer_ms = ss.min_reducer_ms, max_reducer_ms = ss.max_reducer_ms,
                      wall_ms = ss.wall_ms,
                      "Sort phase complete");
            }
            // Emit writer pool metrics snapshot for visibility
            let wp_metrics = writer_pool.metrics_snapshot();
            let hot = wp_metrics.iter().enumerate().max_by_key(|(_, m)| m.queue_len).map(|(i, m)| (i, m.queue_len, m.max_queue_len, m.avg_enqueue_wait_ms));
            if let Some((part, q, qmax, wait_ms)) = hot {
                info!(component = "writer_pool", hottest_partition = part, queue_len = q, max_queue_len = qmax, avg_enqueue_wait_ms = wait_ms, "Writer pool metrics snapshot");
            }
        }

        // ========== Reduce phase (inner, env-agnostic) ==========
        let reduce_phase_start = Instant::now();
        let my_reducers: Vec<usize> = if is_slurm { vec![slurm.node_id] } else { (0..num_reducers).collect() };
        let reduce_done_label = if is_slurm { Some(slurm.node_id) } else { None };
        let reduce_stats_vec = run_reduce_phase(Arc::new(reducer), &my_reducers, &sort_out_dir, &output_dir, &launch_root, reduce_done_label);
        if is_slurm {
            // only rank0 performs cleanup after all are done
            if slurm.node_id == 0 {
                let t0 = Instant::now();
                wait_for_barrier(&launch_root, "reduce_done", slurm.ntasks.max(1));
                info!(phase = "barrier_reduce_wait", wait_ms = t0.elapsed().as_millis() as u64, "Barrier wait for reduce phase completed");
                if !keep_intermediates { let _ = fs::remove_dir_all(&launch_root); }
            }
        } else {
            // local cleanup of intermediates
            if !keep_intermediates { let _ = fs::remove_dir_all(&launch_root); }
        }

        let reduce_phase_ms = reduce_phase_start.elapsed().as_millis() as u64;
        if !reduce_stats_vec.is_empty() {
            let total_lines: u64 = reduce_stats_vec.iter().map(|s| s.lines_in).sum();
            let total_groups: u64 = reduce_stats_vec.iter().map(|s| s.groups).sum();
            let min_wall = reduce_stats_vec.iter().map(|s| s.wall_ms).min().unwrap_or(0);
            let max_wall = reduce_stats_vec.iter().map(|s| s.wall_ms).max().unwrap_or(0);
            let total_parse_ms: u64 = reduce_stats_vec.iter().map(|s| s.parse_ms).sum();
            let total_reduce_ms: u64 = reduce_stats_vec.iter().map(|s| s.reduce_ms).sum();
            let total_write_ms: u64 = reduce_stats_vec.iter().map(|s| s.write_ms).sum();
            let total_io_read_ms: u64 = reduce_stats_vec.iter().map(|s| s.io_read_ms).sum();
            stats.record_reduce(reduce_stats_vec.len(), total_lines, total_groups, min_wall, max_wall, reduce_phase_ms);
            if let Some(rs) = &stats.reduce {
                info!(phase = "reduce",
                      reducers = rs.reducers, total_lines = rs.total_lines, total_groups = rs.total_groups,
                      total_parse_ms = total_parse_ms, total_reduce_ms = total_reduce_ms, total_write_ms = total_write_ms, total_io_read_ms = total_io_read_ms,
                      min_reducer_ms = rs.min_reducer_ms, max_reducer_ms = rs.max_reducer_ms,
                      wall_ms = rs.wall_ms,
                      "Reduce phase complete");
            }
        }

        Ok(())
    }
}

pub fn default_pipeline() -> RuntimePipeline { RuntimePipeline::new() }

fn wait_for_barrier(root: &str, phase: &str, expected: usize) {
    use std::{thread, time::Duration};
    let start = std::time::Instant::now();
    loop {
        let mut done = 0usize;
        for i in 0..expected {
            let p = format!("{}/barrier_{}_{}", root, phase, i);
            if Path::new(&p).exists() { done += 1; }
        }
        if done >= expected { break; }
        if start.elapsed().as_secs() > 24 * 60 * 60 { // 24h safety
            error!("barrier {} timeout after 24h: {}/{}", phase, done, expected);
            break;
        }
        thread::sleep(Duration::from_millis(500));
    }
}

#[cfg(target_os = "linux")]
fn get_fd_soft_limit() -> Option<u64> {
    use libc::{getrlimit, rlimit, RLIMIT_NOFILE};
    let mut lim = rlimit { rlim_cur: 0, rlim_max: 0 };
    let rc = unsafe { getrlimit(RLIMIT_NOFILE, &mut lim as *mut rlimit) };
    if rc == 0 { Some(lim.rlim_cur as u64) } else { None }
}

#[cfg(not(target_os = "linux"))]
fn get_fd_soft_limit() -> Option<u64> { None }

// WriterPool moved to writer.rs

// ============== Inner, env-agnostic phases ==============
#[derive(Clone, Debug)]
struct MapTaskStats {
    task_id: usize,
    num_files: u64,
    total_emits: u64,
    total_bytes_out: u64,
    total_flushes: u64,
    emit_time_ms: u64,
    wall_ms: u64,
}

#[derive(Clone, Debug)]
struct SortReducerWall {
    reducer: u64,
    lines_in: u64,
    bytes_in: u64,
    sort_ms: u64,
    io_read_ms: u64, // mmap time
    scan_ms: u64,
    io_write_ms: u64,
    wall_ms: u64,
}

#[derive(Clone, Debug)]
struct ReduceStats {
    reducer: u64,
    lines_in: u64,
    groups: u64,
    parse_ms: u64,
    reduce_ms: u64,
    write_ms: u64,
    io_read_ms: u64,
    wall_ms: u64,
}

fn run_map_phase<M: Mapper<Input = String> + Send + Sync + 'static>(
    mapper: Arc<M>,
    my_task_ids: &[usize],
    chunks: &Vec<Vec<Split>>,
    num_reducers: usize,
    writer_pool: Arc<WriterPool>,
    launch_root: &str,
    parallelize_splits: bool,
) -> Vec<MapTaskStats> {
    let map_stats: Arc<Mutex<Vec<MapTaskStats>>> = Arc::new(Mutex::new(Vec::new()));
    let run_map_for = |task_id: usize| {
        let task_start = Instant::now();
        let splits = &chunks[task_id];
        debug!(task_id, num_files = splits.len(), writers = num_reducers, "map task starting");
        // Stats
        let mut total_bytes_out: u64 = 0;
        let mut total_flushes: u64 = 0; // number of write_chunk sends
        let mut total_emits: u64 = 0;
        let mut total_emit_time = Duration::from_nanos(0);

        let text_format = TextLineFormat;
        // Re-introduce small thread-local aggregation to reduce per-emit channel sends
        let local_batch_bytes = std::env::var("BJORN_LOCAL_BATCH_BYTES").ok().and_then(|v| v.parse::<usize>().ok()).unwrap_or(256 * 1024);
        let mut tw = writer_pool.make_thread_writer(num_reducers, local_batch_bytes);
        let mut process_one_file = |split: &Split| {
            let reader: Box<dyn std::io::Read + Send> = if split.uri.starts_with("s3://") {
                match S3Source::from_uri(&split.uri).and_then(|s| s.open_split(split)) { Ok(r) => r, Err(e) => { error!("open_split {}: {}", split.uri, e); return; } }
            } else {
                match LocalFsSource.open_split(split) { Ok(r) => r, Err(e) => { error!("open_split {}: {}", split.uri, e); return; } }
            };
            let lines_iter = match text_format.decode(reader) { Ok(it) => it, Err(e) => { error!("decode {}: {}", split.uri, e); return; } };
            let mut emit = |k: M::Key, v: M::Value| {
                let emit_start = Instant::now();
                let part = hash_to_partition(&k, num_reducers);
                // build a binary record and buffer via ThreadWriter
                let key_bytes = match bincode::serialize(&k) { Ok(b) => b, Err(e) => { error!("bincode key: {}", e); return; } };
                let val_bytes = match bincode::serialize(&v) { Ok(b) => b, Err(e) => { error!("bincode val: {}", e); return; } };
                let klen = key_bytes.len() as u32; let vlen = val_bytes.len() as u32;
                let mut record = Vec::with_capacity(8 + key_bytes.len() + val_bytes.len());
                record.extend_from_slice(&klen.to_le_bytes());
                record.extend_from_slice(&vlen.to_le_bytes());
                record.extend_from_slice(&key_bytes);
                record.extend_from_slice(&val_bytes);
                total_bytes_out += record.len() as u64;
                tw.emit_record(part, &record);
                total_emits += 1;
                total_emit_time += emit_start.elapsed();
            };
            mapper.do_map(lines_iter, &mut emit);
            tw.flush_all();
        };

        if parallelize_splits {
            splits.par_iter().for_each(|split| {
                let reader: Box<dyn std::io::Read + Send> = if split.uri.starts_with("s3://") {
                    match S3Source::from_uri(&split.uri).and_then(|s| s.open_split(split)) { Ok(r) => r, Err(e) => { error!("open_split {}: {}", split.uri, e); return; } }
                } else {
                    match LocalFsSource.open_split(split) { Ok(r) => r, Err(e) => { error!("open_split {}: {}", split.uri, e); return; } }
                };
                let lines_iter = match text_format.decode(reader) { Ok(it) => it, Err(e) => { error!("decode {}: {}", split.uri, e); return; } };
                let local_batch_bytes = std::env::var("BJORN_LOCAL_BATCH_BYTES").ok().and_then(|v| v.parse::<usize>().ok()).unwrap_or(256 * 1024);
                let mut tw = writer_pool.make_thread_writer(num_reducers, local_batch_bytes);
                let mut emit = |k: M::Key, v: M::Value| {
                    let part = hash_to_partition(&k, num_reducers);
                    let key_bytes = match bincode::serialize(&k) { Ok(b) => b, Err(e) => { error!("bincode key: {}", e); return; } };
                    let val_bytes = match bincode::serialize(&v) { Ok(b) => b, Err(e) => { error!("bincode val: {}", e); return; } };
                    let klen = key_bytes.len() as u32; let vlen = val_bytes.len() as u32;
                    let mut record = Vec::with_capacity(8 + key_bytes.len() + val_bytes.len());
                    record.extend_from_slice(&klen.to_le_bytes());
                    record.extend_from_slice(&vlen.to_le_bytes());
                    record.extend_from_slice(&key_bytes);
                    record.extend_from_slice(&val_bytes);
                    tw.emit_record(part, &record);
                };
                mapper.do_map(lines_iter, &mut emit);
                tw.flush_all();
            });
        } else {
            for split in splits { process_one_file(split); }
        }

        // touch barrier file
        let barrier_path = format!("{}/barrier_map_done_{}", launch_root, task_id);
        if let Err(e) = fs::write(&barrier_path, b"ok") { error!("barrier write: {}", e); }

        // record stats
        let mut guard = map_stats.lock().unwrap();
        guard.push(MapTaskStats {
            task_id,
            num_files: splits.len() as u64,
            total_emits,
            total_bytes_out,
            total_flushes,
            emit_time_ms: total_emit_time.as_millis() as u64,
            wall_ms: task_start.elapsed().as_millis() as u64,
        });
    };

    my_task_ids.into_par_iter().for_each(|task_id| run_map_for(*task_id));
    let out = map_stats.lock().unwrap().clone();
    out
}

fn run_sort_phase(my_reducers: &[usize], map_out_dir: &str, sort_out_dir: &str, launch_root: &str) -> Vec<SortReducerWall> {
    let sort_stats: Arc<Mutex<Vec<SortReducerWall>>> = Arc::new(Mutex::new(Vec::new()));
    let run_sort_for = |r: usize| {
        let reducer_start = Instant::now();
        let pattern = format!("{}/task*_part{}.tsv", map_out_dir, r);
        let mut paths = match glob::glob(&pattern) { Ok(g) => g.flatten().collect::<Vec<_>>(), Err(e) => { error!("glob error: {}", e); Vec::new() } };
        paths.sort();
        let input_paths: Vec<String> = paths.iter().map(|p| p.to_string_lossy().to_string()).collect();
        let out_path = format!("{}/reduce_in_part{}.tsv", sort_out_dir, r);
        match external_sort_by_key(&input_paths, &out_path) {
            Ok(outcome) => {
                // barrier for sort
                let barrier_path = format!("{}/barrier_sort_done_{}", launch_root, r);
                let _ = fs::write(&barrier_path, b"ok");
                let mut guard = sort_stats.lock().unwrap();
                guard.push(SortReducerWall {
                    reducer: r as u64,
                    lines_in: outcome.lines_in,
                    bytes_in: outcome.bytes_in,
                    sort_ms: outcome.sort_only_ms,
                    io_read_ms: outcome.mmap_ms,
                    scan_ms: outcome.scan_ms,
                    io_write_ms: outcome.io_write_ms,
                    wall_ms: reducer_start.elapsed().as_millis() as u64,
                });
            }
            Err(e) => error!("external_sort_by_key failed for reducer {}: {}", r, e),
        }
    };
    my_reducers.into_par_iter().for_each(|r| run_sort_for(*r));
    let out = sort_stats.lock().unwrap().clone();
    out
}

fn run_reduce_phase<R: Reducer + Send + Sync + 'static>(
    reducer: Arc<R>,
    my_reducers: &[usize],
    sort_out_dir: &str,
    output_dir: &str,
    launch_root: &str,
    mark_done_index: Option<usize>,
) -> Vec<ReduceStats> {
    let reduce_stats: Arc<Mutex<Vec<ReduceStats>>> = Arc::new(Mutex::new(Vec::new()));
    let run_reduce_for = |r: usize| {
        let reducer_start = Instant::now();
        let in_path = format!("{}/reduce_in_part{}.tsv", sort_out_dir, r);
        let io_read_start = Instant::now();
        let file = match std::fs::File::open(&in_path) { Ok(f) => f, Err(e) => { error!("open {}: {}", in_path, e); return; } };
        let map = match unsafe { Mmap::map(&file) } { Ok(m) => m, Err(e) => { error!("mmap {}: {}", in_path, e); return; } };
        let bytes = &map[..];
        let io_read_ms = io_read_start.elapsed().as_millis() as u64;

        let mut current_key: Option<R::Key> = None;
        let mut buffer: Vec<R::ValueIn> = Vec::new();

        let mut out_writer = match open_writer(format!("{}/part-{:05}.tsv", output_dir, r)) { Ok(w) => w, Err(e) => { error!("open_writer output: {}", e); return; } };

        let mut parse_time = Duration::from_nanos(0);
        let mut reduce_time = Duration::from_nanos(0);
        let mut write_time = Duration::from_nanos(0);
        let mut groups: u64 = 0;

        let mut flush_group = |key: &R::Key, vals: &Vec<R::ValueIn>, reducer: &R, out_writer: &mut std::io::BufWriter<std::fs::File>| {
            let reduce_start = Instant::now();
            let mut emit = |line: String| {
                let w_start = Instant::now();
                let _ = writeln!(out_writer, "{}", line);
                write_time += w_start.elapsed();
            };
            reducer.do_reduce(key, vals.clone().into_iter(), &mut emit);
            reduce_time += reduce_start.elapsed();
            groups += 1;
        };

        let mut lines_in: u64 = 0;
        let mut off = 0usize;
        while let Some((k, v, next)) = read_bin_line(bytes, off) {
            let p_start = Instant::now();
            let k_typed: R::Key = match bincode::deserialize(k) { Ok(v) => v, Err(_e) => { break; } };
            let v_typed: R::ValueIn = match bincode::deserialize(v) { Ok(v) => v, Err(_e) => { break; } };
            parse_time += p_start.elapsed();
            lines_in += 1;
            match &current_key {
                None => { current_key = Some(k_typed); buffer.clear(); buffer.push(v_typed); }
                Some(cur) if cur == &k_typed => { buffer.push(v_typed); }
                Some(cur) => {
                    flush_group(cur, &buffer, &reducer, &mut out_writer);
                    current_key = Some(k_typed);
                    buffer.clear();
                    buffer.push(v_typed);
                }
            }
            off = next;
        }
        if let Some(cur) = current_key.take() { flush_group(&cur, &buffer, &reducer, &mut out_writer); }
        // record stats
        let mut guard = reduce_stats.lock().unwrap();
        guard.push(ReduceStats {
            reducer: r as u64,
            lines_in,
            groups,
            parse_ms: parse_time.as_millis() as u64,
            reduce_ms: reduce_time.as_millis() as u64,
            write_ms: write_time.as_millis() as u64,
            io_read_ms,
            wall_ms: reducer_start.elapsed().as_millis() as u64,
        });
    };

    my_reducers.into_par_iter().for_each(|r| run_reduce_for(*r));
    if let Some(idx) = mark_done_index {
        let barrier_path = format!("{}/barrier_reduce_done_{}", launch_root, idx);
        let _ = fs::write(&barrier_path, b"ok");
    }
    let out = reduce_stats.lock().unwrap().clone();
    out
}

// WriterPool API is used below
