use anyhow::Result;
use crossbeam_channel as channel;
use std::io::Write;
use std::thread;
use std::time::{Duration, Instant};
use tracing::error;

// Public WriterPool API intended for mappers to call.
// Strategy: one dedicated IO thread per partition (file). Senders are bounded for backpressure.
pub struct WriterPool {
    senders: Vec<channel::Sender<WriterMsg>>,
}

enum WriterMsg {
    Data(Vec<u8>),
    Close,
}

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
    pub fn new(base_dir: String, node_id: usize, num_partitions: usize, flush_bytes: usize, flush_interval: Duration) -> Result<(Self, WriterJoiner)> {
        crate::io::ensure_dir(&base_dir)?;
        let mut senders = Vec::with_capacity(num_partitions);
        let mut handles = Vec::with_capacity(num_partitions);
        for part in 0..num_partitions {
            let (tx, rx) = channel::bounded::<WriterMsg>(1024);
            let path = format!("{}/task{}_part{}.tsv", base_dir, node_id, part);
            let handle = thread::spawn(move || {
                let mut writer = match crate::io::open_writer(&path) { Ok(w) => w, Err(e) => { error!("open_writer {}: {}", path, e); return; } };
                let mut buf: Vec<u8> = Vec::with_capacity(flush_bytes);
                let mut last_flush = Instant::now();
                loop {
                    // We aim to batch writes up to flush_bytes or flush_interval.
                    let timeout = flush_interval.saturating_sub(last_flush.elapsed());
                    let msg_opt = rx.recv_timeout(timeout).ok();
                    match msg_opt {
                        Some(WriterMsg::Data(bytes)) => {
                            buf.extend_from_slice(&bytes);
                        }
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

    // Simple API: callers pass a Vec<u8> chunk to write to partition.
    // The pool takes ownership, enqueues, and IO threads handle batching and flush policy.
    pub fn write_chunk(&self, partition: usize, bytes: Vec<u8>) -> Result<()> {
        self.senders[partition]
            .send(WriterMsg::Data(bytes))
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
