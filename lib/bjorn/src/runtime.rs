use crate::api::{ExecutablePipeline, Mapper, Reducer};
use crate::io::{ensure_dir, hash_to_partition, list_files_recursive, open_reader, open_writer, read_lines, write_bin, read_bin_line, decode_bin, write_tsv};
use crate::slurm::SlurmEnv;
use anyhow::{Context, Result};
use rayon::prelude::*;
use std::fs;
use std::io::{BufRead, Write};
use memmap2::Mmap;
use memchr::memchr;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use tracing::{error, info, debug};
use crossbeam_channel as channel;
use serde::Serialize;
use std::thread;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

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
struct SortStats {
    reducer: u64,
    input_files: u64,
    lines_in: u64,
    bytes_in: u64,
    sort_ms: u64,
    io_read_ms: u64,
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
    wall_ms: u64,
}

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

        // inputs
        let mut all_files = Vec::new();
        for inp in &self.inputs {
            let mut files = list_files_recursive(inp)?;
            all_files.append(&mut files);
        }

        // Determine task topology
        let is_slurm = SlurmEnv::is_slurm();
        let local_subtasks = if is_slurm {
            std::env::var("BJORN_LOCAL_SUBTASKS").ok().and_then(|v| v.parse::<usize>().ok())
                .or_else(|| std::env::var("SLURM_CPUS_PER_TASK").ok().and_then(|v| v.parse::<usize>().ok()))
                .unwrap_or_else(|| num_cpus::get())
                .max(1)
        } else { 1 };
        let global_ntasks_raw = if is_slurm { slurm.ntasks.max(1) * local_subtasks } else { slurm.ntasks.max(1) };
        let global_ntasks = global_ntasks_raw.min(all_files.len().max(1));

        // partition files among logical tasks
        let chunks: Vec<Vec<PathBuf>> = (0..global_ntasks)
            .map(|i| all_files.iter().cloned().enumerate().filter(|(idx, _)| idx % global_ntasks == i).map(|(_, p)| p).collect())
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
        let (pool_inner, pool_joiner) = WriterPool::new(map_out_dir.clone(), slurm.node_id, num_reducers, flush_bytes, Duration::from_millis(flush_interval_ms))?;
        let writer_pool = Arc::new(pool_inner);
        let mut writer_joiner = pool_joiner;

        // Map phase
        let map_stats: Arc<Mutex<Vec<MapTaskStats>>> = Arc::new(Mutex::new(Vec::new()));
        let map_phase_start = Instant::now();
        let run_map_for = |task_id: usize| {
            let task_start = Instant::now();
            let files = &chunks[task_id];
            debug!(task_id, num_files = files.len(), writers = num_reducers, "map task starting");
            // thread-local batching buffers per partition
            let mut local_buffers: Vec<Vec<u8>> = (0..num_reducers).map(|_| Vec::with_capacity(256 * 1024)).collect();
            const LOCAL_BATCH_BYTES: usize = 256 * 1024; // 256KB per partition chunk
            let mut total_bytes_out: u64 = 0;
            let mut total_flushes: u64 = 0;
            let mut total_emits: u64 = 0;
            let mut total_emit_time = Duration::from_nanos(0);
            let mut flush_part = |part: usize, pool: &WriterPool, buffers: &mut Vec<Vec<u8>>| {
                if !buffers[part].is_empty() {
                    let chunk = std::mem::take(&mut buffers[part]);
                    total_bytes_out += chunk.len() as u64;
                    total_flushes += 1;
                    if let Err(e) = pool.send_raw(part, chunk) { error!("writer_pool send_raw failed: {}", e); }
                }
            };

            let mut process_one_file = |file: &PathBuf, local_buffers: &mut Vec<Vec<u8>>| {
                let lines = match read_lines(file) { Ok(it) => it, Err(e) => { error!("read_lines {}: {}", file.display(), e); return; } };
                let mut emit = |k: M::Key, v: M::Value| {
                    let emit_start = Instant::now();
                    let part = hash_to_partition(&k, num_reducers);
                    let buf = &mut local_buffers[part];
                    // append a binary record
                    let key_bytes = match bincode::serialize(&k) { Ok(b) => b, Err(e) => { error!("bincode key: {}", e); return; } };
                    let val_bytes = match bincode::serialize(&v) { Ok(b) => b, Err(e) => { error!("bincode val: {}", e); return; } };
                    let klen = key_bytes.len() as u32; let vlen = val_bytes.len() as u32;
                    buf.extend_from_slice(&klen.to_le_bytes());
                    buf.extend_from_slice(&vlen.to_le_bytes());
                    buf.extend_from_slice(&key_bytes);
                    buf.extend_from_slice(&val_bytes);
                    if buf.len() >= LOCAL_BATCH_BYTES { flush_part(part, &writer_pool, local_buffers); }
                    total_emits += 1;
                    total_emit_time += emit_start.elapsed();
                };
                let _do_map_start = Instant::now();
                mapper.do_map(lines.filter_map(|r| r.ok()), &mut emit);
            };

            if is_slurm {
                // Parallelize within this task across files when under Slurm (use available cpus-per-task)
                files.par_iter().for_each(|file| {
                    let mut thread_local_buffers: Vec<Vec<u8>> = (0..num_reducers).map(|_| Vec::with_capacity(256 * 1024)).collect();
                    // Process file with a local emit that uses local buffers
                    let lines = match read_lines(file) { Ok(it) => it, Err(e) => { error!("read_lines {}: {}", file.display(), e); return; } };
                    let mut emit = |k: M::Key, v: M::Value| {
                        let part = hash_to_partition(&k, num_reducers);
                        let buf = &mut thread_local_buffers[part];
                        let key_bytes = match bincode::serialize(&k) { Ok(b) => b, Err(e) => { error!("bincode key: {}", e); return; } };
                        let val_bytes = match bincode::serialize(&v) { Ok(b) => b, Err(e) => { error!("bincode val: {}", e); return; } };
                        let klen = key_bytes.len() as u32; let vlen = val_bytes.len() as u32;
                        buf.extend_from_slice(&klen.to_le_bytes());
                        buf.extend_from_slice(&vlen.to_le_bytes());
                        buf.extend_from_slice(&key_bytes);
                        buf.extend_from_slice(&val_bytes);
                        if buf.len() >= LOCAL_BATCH_BYTES {
                            let chunk = std::mem::take(buf);
                            if let Err(e) = writer_pool.send_raw(part, chunk) { error!("writer_pool send_raw failed: {}", e); }
                        }
                    };
                    mapper.do_map(lines.filter_map(|r| r.ok()), &mut emit);
                    // flush per-thread buffers
                    for part in 0..num_reducers {
                        if !thread_local_buffers[part].is_empty() {
                            let chunk = std::mem::take(&mut thread_local_buffers[part]);
                            if let Err(e) = writer_pool.send_raw(part, chunk) { error!("writer_pool send_raw failed: {}", e); }
                        }
                    }
                });
            } else {
                for file in files { process_one_file(file, &mut local_buffers); }
            }
            // flush remaining buffers
            for part in 0..num_reducers { flush_part(part, &writer_pool, &mut local_buffers); }
            // touch barrier file
            let barrier_path = format!("{}/barrier_map_done_{}", launch_root, task_id);
            if let Err(e) = fs::write(&barrier_path, b"ok") { error!("barrier write: {}", e); }

            // record stats
            let mut guard = map_stats.lock().unwrap();
            guard.push(MapTaskStats {
                task_id,
                num_files: files.len() as u64,
                total_emits,
                total_bytes_out,
                total_flushes,
                emit_time_ms: total_emit_time.as_millis() as u64,
                wall_ms: task_start.elapsed().as_millis() as u64,
            });
        };

        if is_slurm {
            // Run this process's logical subtasks: ids are node_id + k * slurm.ntasks
            let my_task_ids: Vec<usize> = (0..local_subtasks)
                .map(|k| slurm.node_id + k * slurm.ntasks)
                .filter(|&id| id < global_ntasks)
                .collect();
            my_task_ids.into_par_iter().for_each(|task_id| run_map_for(task_id));
        } else {
            // local: still run tasks in parallel
            (0..global_ntasks).into_par_iter().for_each(|task_id| run_map_for(task_id));
        }

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
        // emit per-task stats and aggregate
        let map_stats_vec = map_stats.lock().unwrap().clone();
        if !map_stats_vec.is_empty() {
            let total_emits: u64 = map_stats_vec.iter().map(|s| s.total_emits).sum();
            let total_bytes_out: u64 = map_stats_vec.iter().map(|s| s.total_bytes_out).sum();
            let total_flushes: u64 = map_stats_vec.iter().map(|s| s.total_flushes).sum();
            let min_wall = map_stats_vec.iter().map(|s| s.wall_ms).min().unwrap_or(0);
            let max_wall = map_stats_vec.iter().map(|s| s.wall_ms).max().unwrap_or(0);
            info!(phase = "map",
                  tasks = map_stats_vec.len(),
                  total_emits, total_bytes_out, total_flushes,
                  min_task_ms = min_wall, max_task_ms = max_wall,
                  wall_ms = map_phase_ms,
                  "Map phase complete");
        }

        // Sort/shuffle
        let sort_stats: Arc<Mutex<Vec<SortStats>>> = Arc::new(Mutex::new(Vec::new()));
        let sort_phase_start = Instant::now();
        let run_sort_for = |r: usize| {
            let reducer_start = Instant::now();
            let pattern = format!("{}/task*_part{}.tsv", map_out_dir, r);
            let mut paths = match glob::glob(&pattern) { Ok(g) => g.flatten().collect::<Vec<_>>(), Err(e) => { error!("glob error: {}", e); Vec::new() } };
            paths.sort();

            // Mmap each input file and build record index: (file_idx, start, key_end, end)
            let mut file_maps: Vec<Mmap> = Vec::new();
            let mut all_lines: Vec<(usize, usize, usize, usize)> = Vec::new();
            let mut bytes_in: u64 = 0;
            let mut lines_in: u64 = 0;
            let mut io_read = Duration::from_nanos(0);

            for p in &paths {
                match std::fs::File::open(&p) {
                    Ok(file) => {
                        let meta_len = file.metadata().ok().map(|m| m.len()).unwrap_or(0);
                        bytes_in += meta_len as u64;
                        let read_start = Instant::now();
                        match unsafe { Mmap::map(&file) } {
                            Ok(map) => {
                                let file_idx = file_maps.len();
                                let bytes = &map[..];
                                let mut off = 0usize;
                                while let Some((k, v, next)) = read_bin_line(bytes, off) {
                                    let key_start = off + 8; // after 2 u32 lengths
                                    let key_end = key_start + k.len();
                                    let end = next;
                                    all_lines.push((file_idx, key_start - 8, key_end, end));
                                    lines_in += 1;
                                    off = next;
                                }
                                file_maps.push(map);
                            }
                            Err(e) => error!("mmap {}: {}", p.display(), e),
                        }
                        io_read += read_start.elapsed();
                    }
                    Err(e) => { error!("open {}: {}", p.display(), e); }
                }
            }

            let sort_only_start = Instant::now();
            all_lines.par_sort_by(|a, b| {
                let (fia, sa, ka, _ea) = *a;
                let (fib, sb, kb, _eb) = *b;
                // Keys are at [sa+8..ka] because [sa..sa+8) holds the lengths
                file_maps[fia][(sa+8)..ka].cmp(&file_maps[fib][(sb+8)..kb])
            });
            let sort_only_ms = sort_only_start.elapsed().as_millis() as u64;

            let out_path = format!("{}/reduce_in_part{}.tsv", sort_out_dir, r);
            let io_write_start = Instant::now();
            match std::fs::File::create(&out_path) {
                Ok(file) => {
                    let mut w = std::io::BufWriter::with_capacity(32 * 1024 * 1024, file);
                    for &(fi, s, _k, e) in &all_lines {
                        if let Err(e2) = w.write_all(&file_maps[fi][s..e]) { error!("write reduce_in {}: {}", out_path, e2); break; }
                    }
                    if let Err(e) = w.flush() { error!("flush reduce_in {}: {}", out_path, e); }
                }
                Err(e) => error!("create reduce_in {}: {}", out_path, e),
            }
            let io_write_ms = io_write_start.elapsed().as_millis() as u64;
            // barrier for sort
            let barrier_path = format!("{}/barrier_sort_done_{}", launch_root, r);
            if let Err(e) = fs::write(&barrier_path, b"ok") { error!("barrier write: {}", e); }

            // record stats
            let mut guard = sort_stats.lock().unwrap();
            guard.push(SortStats {
                reducer: r as u64,
                input_files: paths.len() as u64,
                lines_in,
                bytes_in,
                sort_ms: sort_only_ms,
                io_read_ms: io_read.as_millis() as u64,
                io_write_ms,
                wall_ms: reducer_start.elapsed().as_millis() as u64,
            });
        };

        if is_slurm {
            run_sort_for(slurm.node_id);
            // wait for all reducers' sort
            let t0 = Instant::now();
            wait_for_barrier(&launch_root, "sort_done", num_reducers);
            info!(phase = "barrier_sort_wait", wait_ms = t0.elapsed().as_millis() as u64, "Barrier wait for sort phase completed");
        } else {
            (0..num_reducers).into_par_iter().for_each(|r| run_sort_for(r));
        }
        let sort_phase_ms = sort_phase_start.elapsed().as_millis() as u64;
        let sort_stats_vec = sort_stats.lock().unwrap().clone();
        if !sort_stats_vec.is_empty() {
            let total_lines: u64 = sort_stats_vec.iter().map(|s| s.lines_in).sum();
            let total_bytes: u64 = sort_stats_vec.iter().map(|s| s.bytes_in).sum();
            let min_wall = sort_stats_vec.iter().map(|s| s.wall_ms).min().unwrap_or(0);
            let max_wall = sort_stats_vec.iter().map(|s| s.wall_ms).max().unwrap_or(0);
            info!(phase = "sort",
                  reducers = sort_stats_vec.len(), total_lines, total_bytes,
                  min_reducer_ms = min_wall, max_reducer_ms = max_wall,
                  wall_ms = sort_phase_ms,
                  "Sort phase complete");
        }

        // Reduce phase
        let reducer = Arc::new(reducer);
        let reduce_stats: Arc<Mutex<Vec<ReduceStats>>> = Arc::new(Mutex::new(Vec::new()));
        let reduce_phase_start = Instant::now();
        let run_reduce_for = |r: usize| {
            let reducer_start = Instant::now();
            let in_path = format!("{}/reduce_in_part{}.tsv", sort_out_dir, r);
            let file = match std::fs::File::open(&in_path) { Ok(f) => f, Err(e) => { error!("open {}: {}", in_path, e); return; } };
            let map = match unsafe { Mmap::map(&file) } { Ok(m) => m, Err(e) => { error!("mmap {}: {}", in_path, e); return; } };
            let bytes = &map[..];

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
                    if let Err(e) = writeln!(out_writer, "{}", line) { error!("write reduce tsv line: {}", e); }
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
                let k_typed: R::Key = match bincode::deserialize(k) { Ok(v) => v, Err(e) => { error!("bad key bin: {}", e); break; } };
                let v_typed: R::ValueIn = match bincode::deserialize(v) { Ok(v) => v, Err(e) => { error!("bad val bin: {}", e); break; } };
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
                wall_ms: reducer_start.elapsed().as_millis() as u64,
            });
        };

        if is_slurm {
            run_reduce_for(slurm.node_id);
            // mark reduce done for this node
            let barrier_path = format!("{}/barrier_reduce_done_{}", launch_root, slurm.node_id);
            let _ = fs::write(&barrier_path, b"ok");
            // only rank0 performs cleanup after all are done
            if slurm.node_id == 0 {
                let t0 = Instant::now();
                wait_for_barrier(&launch_root, "reduce_done", slurm.ntasks.max(1));
                info!(phase = "barrier_reduce_wait", wait_ms = t0.elapsed().as_millis() as u64, "Barrier wait for reduce phase completed");
                if !keep_intermediates { let _ = fs::remove_dir_all(&launch_root); }
            }
        } else {
            (0..num_reducers).into_par_iter().for_each(|r| run_reduce_for(r));
            // local cleanup of intermediates
            if !keep_intermediates { let _ = fs::remove_dir_all(&launch_root); }
        }

        let reduce_phase_ms = reduce_phase_start.elapsed().as_millis() as u64;
        let reduce_stats_vec = reduce_stats.lock().unwrap().clone();
        if !reduce_stats_vec.is_empty() {
            let total_lines: u64 = reduce_stats_vec.iter().map(|s| s.lines_in).sum();
            let total_groups: u64 = reduce_stats_vec.iter().map(|s| s.groups).sum();
            let min_wall = reduce_stats_vec.iter().map(|s| s.wall_ms).min().unwrap_or(0);
            let max_wall = reduce_stats_vec.iter().map(|s| s.wall_ms).max().unwrap_or(0);
            info!(phase = "reduce",
                  reducers = reduce_stats_vec.len(), total_lines, total_groups,
                  min_reducer_ms = min_wall, max_reducer_ms = max_wall,
                  wall_ms = reduce_phase_ms,
                  "Reduce phase complete");
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

// ============== Writer Pool ==============
struct WriterPool {
    senders: Vec<channel::Sender<WriterMsg>>,
}

enum WriterMsg {
    Data(Vec<u8>),
    Flush,
    Close,
}

struct WriterJoiner {
    handles: Vec<thread::JoinHandle<()>>,
}

impl WriterJoiner { fn join_all(&mut self) { for h in self.handles.drain(..) { let _ = h.join(); } } }

impl WriterPool {
    fn new(base_dir: String, node_id: usize, num_partitions: usize, flush_bytes: usize, flush_interval: Duration) -> Result<(Self, WriterJoiner)> {
        ensure_dir(&base_dir)?;
        let mut senders = Vec::with_capacity(num_partitions);
        let mut handles = Vec::with_capacity(num_partitions);
        for part in 0..num_partitions {
            let (tx, rx) = channel::bounded::<WriterMsg>(1024);
            let path = format!("{}/task{}_part{}.tsv", base_dir, node_id, part);
            let handle = thread::spawn(move || {
                let mut writer = match open_writer(&path) { Ok(w) => w, Err(e) => { error!("open_writer {}: {}", path, e); return; } };
                let mut buf: Vec<u8> = Vec::with_capacity(flush_bytes);
                let mut last_flush = Instant::now();
                loop {
                    let timeout = flush_interval.saturating_sub(last_flush.elapsed());
                    let msg_opt = rx.recv_timeout(timeout).ok();
                    match msg_opt {
                        Some(WriterMsg::Data(mut bytes)) => {
                            buf.extend_from_slice(&bytes);
                        }
                        Some(WriterMsg::Flush) => {}
                        Some(WriterMsg::Close) => {
                            if !buf.is_empty() {
                                if let Err(e) = writer.write_all(&buf) { error!("writer write_all: {}", e); }
                                buf.clear();
                            }
                            if let Err(e) = writer.flush() { error!("writer flush: {}", e); }
                            break;
                        }
                        None => {}
                    }
                    if buf.len() >= flush_bytes || last_flush.elapsed() >= flush_interval {
                        if !buf.is_empty() {
                            if let Err(e) = writer.write_all(&buf) { error!("writer write_all: {}", e); }
                            buf.clear();
                        }
                        if let Err(e) = writer.flush() { error!("writer flush: {}", e); }
                        last_flush = Instant::now();
                    }
                }
            });
            senders.push(tx);
            handles.push(handle);
        }
        Ok((Self { senders }, WriterJoiner { handles }))
    }

    fn send<K: Serialize, V: Serialize>(&self, partition: usize, key: &K, value: &V) -> Result<()> {
        let key_str = serde_json::to_string(key)?;
        let value_str = serde_json::to_string(value)?;
        let mut line = Vec::with_capacity(key_str.len() + value_str.len() + 2);
        line.extend_from_slice(key_str.as_bytes());
        line.push(b'\t');
        line.extend_from_slice(value_str.as_bytes());
        line.push(b'\n');
        self.senders[partition].send(WriterMsg::Data(line)).map_err(|e| anyhow::anyhow!("send failed: {}", e))
    }

    fn send_raw(&self, partition: usize, bytes: Vec<u8>) -> Result<()> {
        self.senders[partition].send(WriterMsg::Data(bytes)).map_err(|e| anyhow::anyhow!("send failed: {}", e))
    }

    fn close_all(&self) {
        for tx in &self.senders { let _ = tx.send(WriterMsg::Close); }
        // Note: we cannot join here because we don't own the handles; provide separate join
    }
}
