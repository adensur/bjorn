use anyhow::{Context, Result};
use std::env;

#[derive(Debug, Clone)]
pub struct SlurmEnv {
    pub job_id: String,
    pub ntasks: usize,
    pub node_id: usize,
    pub node_list: String,
}

impl SlurmEnv {
    pub fn detect() -> Result<Self> {
        let job_id = env::var("SLURM_JOB_ID").context("SLURM_JOB_ID not set")?;
        let ntasks = env::var("SLURM_NTASKS")
            .ok()
            .and_then(|v| v.parse::<usize>().ok())
            .or_else(|| env::var("SLURM_TASKS_PER_NODE").ok().and_then(|s| s.split(',').next().and_then(|x| x.parse::<usize>().ok())))
            .unwrap_or(1);
        let node_id = env::var("SLURM_PROCID").ok().and_then(|v| v.parse().ok()).unwrap_or(0);
        let node_list = env::var("SLURM_NODELIST").unwrap_or_else(|_| "localhost".to_string());
        Ok(Self { job_id, ntasks, node_id, node_list })
    }

    pub fn is_slurm() -> bool { env::var("SLURM_JOB_ID").is_ok() }
}
