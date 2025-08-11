use anyhow::Result;
use crossbeam_channel as channel;
use std::io::Write;
use std::thread;
use std::time::{Duration, Instant};
use tracing::error;
use std::sync::{Arc, atomic::{AtomicU64, AtomicUsize, Ordering}};

// Public WriterPool API intended for mappers to call.
// Strategy: one dedicated IO thread per partition (file). Senders are bounded for backpressure.
pub struct WriterPool {
    senders: Vec<channel::Sender<WriterMsg>>,
    metrics: Arc<Vec<PartitionCounters>>,
}

enum WriterMsg { Data(Vec<u8>, Instant), Close }

pub struct WriterJoiner {
    handles: Vec<thread::JoinHandle<()>>,
}

impl WriterJoiner {
    pub fn join_all(&mut self) {
        for h in self.handles.drain(..) {
            let _ = h.join();
        }
    }
}

impl WriterPool {
    pub fn new(base_dir: String, node_id: usize, num_partitions: usize, flush_bytes: usize, flush_interval: Duration, queue_cap: usize) -> Result<(Self, WriterJoiner)> {
        crate::io::ensure_dir(&base_dir)?;
        let mut senders = Vec::with_capacity(num_partitions);
        let mut handles = Vec::with_capacity(num_partitions);
        let metrics: Arc<Vec<PartitionCounters>> = Arc::new((0..num_partitions).map(|_| PartitionCounters::new()).collect());
        for part in 0..num_partitions {
            let (tx, rx) = channel::bounded::<WriterMsg>(queue_cap);
            let path = format!("{}/task{}_part{}.tsv", base_dir, node_id, part);
            let counters = PartitionCountersRef(unsafe { &*(&metrics[part] as *const PartitionCounters) });
            let handle = thread::spawn(move || {
                let mut writer = match crate::io::open_writer(&path) { Ok(w) => w, Err(e) => { error!("open_writer {}: {}", path, e); return; } };
                let mut buf: Vec<u8> = Vec::with_capacity(flush_bytes);
                let mut last_flush = Instant::now();
                loop {
                    // We aim to batch writes up to flush_bytes or flush_interval.
                    let timeout = flush_interval.saturating_sub(last_flush.elapsed());
                    let msg_opt = rx.recv_timeout(timeout).ok();
                    match msg_opt {
                        Some(WriterMsg::Data(bytes, t_enq)) => {
                            let wait_ns = Instant::now().saturating_duration_since(t_enq).as_nanos() as u64;
                            counters.0.total_enqueue_wait_ns.fetch_add(wait_ns, Ordering::Relaxed);
                            counters.0.recv_count.fetch_add(1, Ordering::Relaxed);
                            counters.0.queue_len.fetch_sub(1, Ordering::Relaxed);
                            buf.extend_from_slice(&bytes);
                        }
                        Some(WriterMsg::Close) => {
                            if !buf.is_empty() {
                                let w_start = Instant::now();
                                if let Err(e) = writer.write_all(&buf) { error!("writer write_all: {}", e); }
                                let dur = w_start.elapsed();
                                counters.0.bytes_written.fetch_add(buf.len() as u64, Ordering::Relaxed);
                                counters.0.write_calls.fetch_add(1, Ordering::Relaxed);
                                counters.0.total_write_ns.fetch_add(dur.as_nanos() as u64, Ordering::Relaxed);
                                buf.clear();
                            }
                            if let Err(e) = writer.flush() { error!("writer flush: {}", e); }
                            break;
                        }
                        None => {}
                    }
                    let due_to_size = buf.len() >= flush_bytes;
                    let due_to_timer = !due_to_size && last_flush.elapsed() >= flush_interval;
                    if due_to_size || due_to_timer {
                        if !buf.is_empty() {
                            let w_start = Instant::now();
                            if let Err(e) = writer.write_all(&buf) { error!("writer write_all: {}", e); }
                            let dur = w_start.elapsed();
                            counters.0.bytes_written.fetch_add(buf.len() as u64, Ordering::Relaxed);
                            counters.0.write_calls.fetch_add(1, Ordering::Relaxed);
                            counters.0.total_write_ns.fetch_add(dur.as_nanos() as u64, Ordering::Relaxed);
                            buf.clear();
                        }
                        if due_to_size { counters.0.flush_by_size.fetch_add(1, Ordering::Relaxed); }
                        if due_to_timer { counters.0.flush_by_timer.fetch_add(1, Ordering::Relaxed); }
                        if let Err(e) = writer.flush() { error!("writer flush: {}", e); }
                        last_flush = Instant::now();
                    }
                }
            });
            senders.push(tx);
            handles.push(handle);
        }
        Ok((Self { senders, metrics }, WriterJoiner { handles }))
    }

    // Simple API: callers pass a Vec<u8> chunk to write to partition.
    // The pool takes ownership, enqueues, and IO threads handle batching and flush policy.
    pub fn write_chunk(&self, partition: usize, bytes: Vec<u8>) -> Result<()> {
        let counters = &self.metrics[partition];
        counters.enq_count.fetch_add(1, Ordering::Relaxed);
        counters.enq_bytes.fetch_add(bytes.len() as u64, Ordering::Relaxed);
        let q = counters.queue_len.fetch_add(1, Ordering::Relaxed) + 1;
        // track max queue length
        loop {
            let prev = counters.max_queue_len.load(Ordering::Relaxed);
            if q > prev {
                if counters.max_queue_len.compare_exchange(prev, q, Ordering::Relaxed, Ordering::Relaxed).is_ok() { break; }
            } else { break; }
        }
        self.senders[partition]
            .send(WriterMsg::Data(bytes, Instant::now()))
            .map_err(|e| anyhow::anyhow!("send failed: {}", e))
    }

    pub fn close_all(&self) {
        for tx in &self.senders {
            let _ = tx.send(WriterMsg::Close);
        }
    }

    pub fn make_thread_writer(&self, num_partitions: usize, local_batch_bytes: usize) -> ThreadWriter {
        ThreadWriter::new(self, num_partitions, local_batch_bytes)
    }
}

pub struct ThreadWriter<'a> {
    pool: &'a WriterPool,
    local_buffers: Vec<Vec<u8>>, // per-partition aggregation buffers
    batch_bytes: usize,
    flushes: u64,
    bytes_sent: u64,
}

impl<'a> ThreadWriter<'a> {
    fn new(pool: &'a WriterPool, num_partitions: usize, batch_bytes: usize) -> Self {
        let local_buffers = (0..num_partitions).map(|_| Vec::with_capacity(batch_bytes)).collect();
        Self { pool, local_buffers, batch_bytes, flushes: 0, bytes_sent: 0 }
    }

    pub fn emit_record(&mut self, partition: usize, record: &[u8]) {
        let buf = &mut self.local_buffers[partition];
        buf.extend_from_slice(record);
        if buf.len() >= self.batch_bytes {
            self.flush_partition(partition);
        }
    }

    pub fn flush_all(&mut self) {
        for p in 0..self.local_buffers.len() {
            if !self.local_buffers[p].is_empty() {
                self.flush_partition(p);
            }
        }
    }

    fn flush_partition(&mut self, partition: usize) {
        let chunk = std::mem::take(&mut self.local_buffers[partition]);
        self.bytes_sent += chunk.len() as u64;
        self.flushes += 1;
        if let Err(e) = self.pool.write_chunk(partition, chunk) {
            error!("writer_pool write_chunk failed: {}", e);
        }
    }

    pub fn stats(&self) -> (u64, u64) {
        (self.flushes, self.bytes_sent)
    }
}

struct PartitionCounters {
    queue_len: AtomicUsize,
    max_queue_len: AtomicUsize,
    enq_count: AtomicU64,
    enq_bytes: AtomicU64,
    recv_count: AtomicU64,
    total_enqueue_wait_ns: AtomicU64,
    flush_by_size: AtomicU64,
    flush_by_timer: AtomicU64,
    bytes_written: AtomicU64,
    write_calls: AtomicU64,
    total_write_ns: AtomicU64,
}

impl PartitionCounters {
    fn new() -> Self {
        Self {
            queue_len: AtomicUsize::new(0),
            max_queue_len: AtomicUsize::new(0),
            enq_count: AtomicU64::new(0),
            enq_bytes: AtomicU64::new(0),
            recv_count: AtomicU64::new(0),
            total_enqueue_wait_ns: AtomicU64::new(0),
            flush_by_size: AtomicU64::new(0),
            flush_by_timer: AtomicU64::new(0),
            bytes_written: AtomicU64::new(0),
            write_calls: AtomicU64::new(0),
            total_write_ns: AtomicU64::new(0),
        }
    }
}

#[derive(Clone, Copy)]
struct PartitionCountersRef<'a>(&'a PartitionCounters);

#[derive(Clone, Debug)]
pub struct WriterPartitionMetrics {
    pub queue_len: usize,
    pub max_queue_len: usize,
    pub enq_count: u64,
    pub enq_bytes: u64,
    pub recv_count: u64,
    pub avg_enqueue_wait_ms: f64,
    pub flush_by_size: u64,
    pub flush_by_timer: u64,
    pub bytes_written: u64,
    pub write_calls: u64,
    pub avg_write_call_ms: f64,
}

impl WriterPool {
    pub fn metrics_snapshot(&self) -> Vec<WriterPartitionMetrics> {
        self.metrics.iter().map(|c| {
            let recv = c.recv_count.load(Ordering::Relaxed);
            let total_wait_ns = c.total_enqueue_wait_ns.load(Ordering::Relaxed) as f64;
            let avg_wait_ms = if recv > 0 { (total_wait_ns / 1_000_000.0) / (recv as f64) } else { 0.0 };
            let writes = c.write_calls.load(Ordering::Relaxed);
            let total_write_ns = c.total_write_ns.load(Ordering::Relaxed) as f64;
            let avg_write_ms = if writes > 0 { (total_write_ns / 1_000_000.0) / (writes as f64) } else { 0.0 };
            WriterPartitionMetrics {
                queue_len: c.queue_len.load(Ordering::Relaxed),
                max_queue_len: c.max_queue_len.load(Ordering::Relaxed),
                enq_count: c.enq_count.load(Ordering::Relaxed),
                enq_bytes: c.enq_bytes.load(Ordering::Relaxed),
                recv_count: recv,
                avg_enqueue_wait_ms: avg_wait_ms,
                flush_by_size: c.flush_by_size.load(Ordering::Relaxed),
                flush_by_timer: c.flush_by_timer.load(Ordering::Relaxed),
                bytes_written: c.bytes_written.load(Ordering::Relaxed),
                write_calls: writes,
                avg_write_call_ms: avg_write_ms,
            }
        }).collect()
    }
}
