//! Centralized environment variable names and default values for Bjorn runtime tuning.

// Environment variable names
pub const ENV_KEEP_INTERMEDIATES: &str = "BJORN_KEEP_INTERMEDIATES";
pub const ENV_RAYON_THREADS: &str = "BJORN_RAYON_THREADS";
pub const ENV_NUM_REDUCERS: &str = "BJORN_NUM_REDUCERS";
pub const ENV_FLUSH_BYTES: &str = "BJORN_FLUSH_BYTES";
pub const ENV_FLUSH_INTERVAL_MS: &str = "BJORN_FLUSH_INTERVAL_MS";
pub const ENV_WRITER_QUEUE_CAP: &str = "BJORN_WRITER_QUEUE_CAP";
pub const ENV_LOCAL_BATCH_BYTES: &str = "BJORN_LOCAL_BATCH_BYTES";
pub const ENV_LOCAL_TASKS: &str = "BJORN_LOCAL_TASKS";
/// AWS region variable used by S3 source
pub const ENV_AWS_REGION: &str = "AWS_REGION";

// Defaults (picked to reduce wakeups/syscalls under heavy shuffle)
// Increase per-thread local batch to reduce channel sends
pub const DEFAULT_LOCAL_BATCH_BYTES: usize = 4 * 1024 * 1024; // 4 MiB (tunable 1–16 MiB)
// Increase writer queue capacity to reduce backpressure churn
pub const DEFAULT_WRITER_QUEUE_CAP: usize = 16_384; // 16K (8192+ recommended)
// Increase flush interval to amortize IO
pub const DEFAULT_FLUSH_INTERVAL_MS: u64 = 500; // 300–800 ms recommended
// Keep existing flush-bytes default (effective maximum buffered before forced flush)
pub const DEFAULT_FLUSH_BYTES: usize = 16 * 1024 * 1024; // 16 MiB

/// Default AWS region when `AWS_REGION` is not set
pub const DEFAULT_AWS_REGION: &str = "us-east-1";
