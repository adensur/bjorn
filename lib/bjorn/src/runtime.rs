use crate::api::{ExecutablePipeline, Mapper, Reducer};
use crate::io::{ensure_dir, hash_to_partition, list_files_recursive, open_reader, open_writer, read_lines, write_tsv};
use crate::slurm::SlurmEnv;
use anyhow::{Context, Result};
use rayon::prelude::*;
use std::fs;
use std::io::{BufRead, Write};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use tracing::{error, info, debug};
use crossbeam_channel as channel;
use serde::Serialize;
use std::thread;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

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
        R: Reducer<Key = M::Key, ValueIn = M::Value, ValueOut = M::Value> + Send + Sync + 'static,
    {
        let slurm = SlurmEnv::detect().unwrap_or({
            let pid = std::process::id();
            let ts = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_secs();
            SlurmEnv { job_id: format!("local-{}-{}", pid, ts), ntasks: num_cpus::get(), node_id: 0, node_list: "localhost".into() }
        });
        let output_dir = self.output.clone().context("output not set")?;
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

        // partition files among tasks
        let ntasks = slurm.ntasks.max(1);
        let chunks: Vec<Vec<PathBuf>> = (0..ntasks)
            .map(|i| all_files.iter().cloned().enumerate().filter(|(idx, _)| idx % ntasks == i).map(|(_, p)| p).collect())
            .collect();

        let mapper = Arc::new(mapper);
        let num_reducers = ntasks; // simple heuristic: one reducer per task

        // Log environment and limits
        let fd_limit = get_fd_soft_limit();
        info!(
            slurm = %SlurmEnv::is_slurm(), job_id = %slurm.job_id, node_id = slurm.node_id, ntasks = ntasks, num_reducers = num_reducers,
            fd_soft_limit = fd_limit.unwrap_or(0),
            "Bjorn starting map phase"
        );

        // Writer pool shared within the process
        let (pool_inner, pool_joiner) = WriterPool::new(map_out_dir.clone(), slurm.node_id, num_reducers, 16 * 1024 * 1024, Duration::from_millis(200))?;
        let writer_pool = Arc::new(pool_inner);
        let mut writer_joiner = pool_joiner;

        // Map phase
        let run_map_for = |task_id: usize| {
            let files = &chunks[task_id];
            debug!(task_id, num_files = files.len(), writers = num_reducers, "map task starting");
            // thread-local batching buffers per partition
            let mut local_buffers: Vec<Vec<u8>> = (0..num_reducers).map(|_| Vec::with_capacity(256 * 1024)).collect();
            const LOCAL_BATCH_BYTES: usize = 256 * 1024; // 256KB per partition chunk
            let mut flush_part = |part: usize, pool: &WriterPool, buffers: &mut Vec<Vec<u8>>| {
                if !buffers[part].is_empty() {
                    let chunk = std::mem::take(&mut buffers[part]);
                    if let Err(e) = pool.send_raw(part, chunk) { error!("writer_pool send_raw failed: {}", e); }
                }
            };

            for file in files {
                let lines = match read_lines(file) { Ok(it) => it, Err(e) => { error!("read_lines {}: {}", file.display(), e); continue; } };
                let mut emit = |k: M::Key, v: M::Value| {
                    let part = hash_to_partition(&k, num_reducers);
                    let key_str = match serde_json::to_string(&k) { Ok(s) => s, Err(e) => { error!("serde key: {}", e); return; } };
                    let value_str = match serde_json::to_string(&v) { Ok(s) => s, Err(e) => { error!("serde val: {}", e); return; } };
                    let buf = &mut local_buffers[part];
                    buf.extend_from_slice(key_str.as_bytes());
                    buf.push(b'\t');
                    buf.extend_from_slice(value_str.as_bytes());
                    buf.push(b'\n');
                    if buf.len() >= LOCAL_BATCH_BYTES {
                        flush_part(part, &writer_pool, &mut local_buffers);
                    }
                };
                mapper.do_map(lines.filter_map(|r| r.ok()), &mut emit);
            }
            // flush remaining buffers
            for part in 0..num_reducers { flush_part(part, &writer_pool, &mut local_buffers); }
            // touch barrier file
            let barrier_path = format!("{}/barrier_map_done_{}", launch_root, task_id);
            if let Err(e) = fs::write(&barrier_path, b"ok") { error!("barrier write: {}", e); }
        };

        if SlurmEnv::is_slurm() {
            run_map_for(slurm.node_id);
        } else {
            // local: still run tasks in parallel
            (0..ntasks).into_par_iter().for_each(|task_id| run_map_for(task_id));
        }

        // wait for all tasks to finish map when under slurm
        if SlurmEnv::is_slurm() {
            wait_for_barrier(&launch_root, "map_done", ntasks);
        }

        // Ensure all writers are closed before proceeding
        writer_pool.close_all();
        writer_joiner.join_all();

        // Sort/shuffle
        let run_sort_for = |r: usize| {
            let pattern = format!("{}/task*_part{}.tsv", map_out_dir, r);
            let mut paths = match glob::glob(&pattern) { Ok(g) => g.flatten().collect::<Vec<_>>(), Err(e) => { error!("glob error: {}", e); Vec::new() } };
            paths.sort();
            let mut all_lines: Vec<String> = Vec::new();
            for p in paths {
                match fs::read_to_string(&p) { Ok(content) => {
                    // Avoid allocating per line: reuse split
                    for line in content.split('\n') { if !line.is_empty() { all_lines.push(line.to_string()); } }
                }, Err(e) => { error!("read_to_string {}: {}", p.display(), e); } }
            }
            all_lines.par_sort();
            let out_path = format!("{}/reduce_in_part{}.tsv", sort_out_dir, r);
            if let Err(e) = fs::write(&out_path, all_lines.join("\n") + "\n") { error!("write reduce_in {}: {}", out_path, e) }
            // barrier for sort
            let barrier_path = format!("{}/barrier_sort_done_{}", launch_root, r);
            if let Err(e) = fs::write(&barrier_path, b"ok") { error!("barrier write: {}", e); }
        };

        if SlurmEnv::is_slurm() {
            run_sort_for(slurm.node_id);
            // wait for all reducers' sort
            wait_for_barrier(&launch_root, "sort_done", num_reducers);
        } else {
            (0..num_reducers).into_par_iter().for_each(|r| run_sort_for(r));
        }

        // Reduce phase
        let reducer = Arc::new(reducer);
        let run_reduce_for = |r: usize| {
            let in_path = format!("{}/reduce_in_part{}.tsv", sort_out_dir, r);
            let reader = match open_reader(&in_path) { Ok(rdr) => rdr, Err(e) => { error!("open_reader {}: {}", in_path, e); return; } };
            let lines = reader.lines().filter_map(|l| l.ok());

            let mut current_key: Option<serde_json::Value> = None;
            let mut buffer: Vec<serde_json::Value> = Vec::new();

            let mut out_writer = match open_writer(format!("{}/part-{:05}.tsv", output_dir, r)) { Ok(w) => w, Err(e) => { error!("open_writer output: {}", e); return; } };

            let flush_group = |key: &serde_json::Value, vals: &Vec<serde_json::Value>, reducer: &R, out_writer: &mut std::io::BufWriter<std::fs::File>| {
                let key_typed: R::Key = serde_json::from_value(key.clone()).expect("key type");
                let vals_typed: Vec<R::ValueIn> = vals.iter().cloned().map(|v| serde_json::from_value(v).expect("val type")).collect();
                let mut emit = |k: R::Key, v: R::ValueOut| {
                    if let Err(e) = write_tsv(out_writer, &k, &v) { error!("write_tsv reduce: {}", e); }
                };
                reducer.do_reduce(&key_typed, vals_typed.into_iter(), &mut emit);
            };

            for line in lines {
                if let Some((k_str, v_str)) = line.split_once('\t') {
                    let k_json: serde_json::Value = match serde_json::from_str(k_str) { Ok(v) => v, Err(e) => { error!("bad key json: {}", e); continue; } };
                    let v_json: serde_json::Value = match serde_json::from_str(v_str) { Ok(v) => v, Err(e) => { error!("bad val json: {}", e); continue; } };
                    match &current_key {
                        None => { current_key = Some(k_json); buffer.clear(); buffer.push(v_json); }
                        Some(cur) if cur == &k_json => { buffer.push(v_json); }
                        Some(cur) => {
                            flush_group(cur, &buffer, &reducer, &mut out_writer);
                            current_key = Some(k_json);
                            buffer.clear();
                            buffer.push(v_json);
                        }
                    }
                }
            }
            if let Some(cur) = current_key.take() { flush_group(&cur, &buffer, &reducer, &mut out_writer); }
        };

        if SlurmEnv::is_slurm() {
            run_reduce_for(slurm.node_id);
            // mark reduce done for this node
            let barrier_path = format!("{}/barrier_reduce_done_{}", launch_root, slurm.node_id);
            let _ = fs::write(&barrier_path, b"ok");
            // only rank0 performs cleanup after all are done
            if slurm.node_id == 0 {
                wait_for_barrier(&launch_root, "reduce_done", ntasks);
                let _ = fs::remove_dir_all(&launch_root);
            }
        } else {
            (0..num_reducers).into_par_iter().for_each(|r| run_reduce_for(r));
            // local cleanup of intermediates
            let _ = fs::remove_dir_all(&launch_root);
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
