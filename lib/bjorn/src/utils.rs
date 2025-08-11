use crate::slurm::SlurmEnv;
use std::time::{SystemTime, UNIX_EPOCH};

pub fn detect_env_or_local() -> SlurmEnv {
    SlurmEnv::detect().unwrap_or_else(|_| {
        let pid = std::process::id();
        let ts = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs();
        let local_tasks = std::env::var("BJORN_LOCAL_TASKS")
            .ok()
            .and_then(|v| v.parse::<usize>().ok())
            .unwrap_or_else(|| num_cpus::get());
        SlurmEnv {
            job_id: format!("local-{}-{}", pid, ts),
            ntasks: local_tasks.max(1),
            node_id: 0,
            node_list: "localhost".into(),
        }
    })
}

pub fn env_var_truthy(name: &str) -> bool {
    match std::env::var(name) {
        Ok(v) => {
            let v = v.to_ascii_lowercase();
            v == "1" || v == "true" || v == "yes" || v == "on"
        }
        Err(_) => false,
    }
}
