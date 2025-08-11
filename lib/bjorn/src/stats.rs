use serde::Serialize;

#[derive(Default, Clone, Debug, Serialize)]
pub struct MapStats {
    pub tasks: usize,
    pub total_emits: u64,
    pub total_bytes_out: u64,
    pub total_flushes: u64,
    pub min_task_ms: u64,
    pub max_task_ms: u64,
    pub wall_ms: u64,
}

#[derive(Default, Clone, Debug, Serialize)]
pub struct SortStatsAgg {
    pub reducers: usize,
    pub total_lines: u64,
    pub total_bytes: u64,
    pub min_reducer_ms: u64,
    pub max_reducer_ms: u64,
    pub wall_ms: u64,
}

#[derive(Default, Clone, Debug, Serialize)]
pub struct ReduceStatsAgg {
    pub reducers: usize,
    pub total_lines: u64,
    pub total_groups: u64,
    pub min_reducer_ms: u64,
    pub max_reducer_ms: u64,
    pub wall_ms: u64,
}

pub struct StatsCollector {
    pub map: Option<MapStats>,
    pub sort: Option<SortStatsAgg>,
    pub reduce: Option<ReduceStatsAgg>,
}

impl Default for StatsCollector {
    fn default() -> Self { Self { map: None, sort: None, reduce: None } }
}

impl StatsCollector {
    pub fn new() -> Self { Self::default() }

    pub fn record_map(&mut self, per_task: &Vec<(u64, u64, u64, u64)>, wall_ms: u64) {
        if per_task.is_empty() { return; }
        let tasks = per_task.len();
        let total_emits = per_task.iter().map(|t| t.0).sum();
        let total_bytes_out = per_task.iter().map(|t| t.1).sum();
        let total_flushes = per_task.iter().map(|t| t.2).sum();
        let min_task_ms = per_task.iter().map(|t| t.3).min().unwrap_or(0);
        let max_task_ms = per_task.iter().map(|t| t.3).max().unwrap_or(0);
        self.map = Some(MapStats { tasks, total_emits, total_bytes_out, total_flushes, min_task_ms, max_task_ms, wall_ms });
    }

    pub fn record_sort(&mut self, reducers: usize, lines: u64, bytes: u64, min_ms: u64, max_ms: u64, wall_ms: u64) {
        self.sort = Some(SortStatsAgg { reducers, total_lines: lines, total_bytes: bytes, min_reducer_ms: min_ms, max_reducer_ms: max_ms, wall_ms });
    }

    pub fn record_reduce(&mut self, reducers: usize, lines: u64, groups: u64, min_ms: u64, max_ms: u64, wall_ms: u64) {
        self.reduce = Some(ReduceStatsAgg { reducers, total_lines: lines, total_groups: groups, min_reducer_ms: min_ms, max_reducer_ms: max_ms, wall_ms });
    }
}
