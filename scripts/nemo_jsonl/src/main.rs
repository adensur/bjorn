use anyhow::{Context, Result};
use bjorn::{Mapper, Reducer, RuntimePipeline};
use bjorn::api::ExecutablePipeline;
use bjorn::io::{ParquetFormat, ParquetRow, ParquetValue, TextLineSink};
use clap::Parser;
use rand::{seq::SliceRandom, thread_rng};
use serde::Serialize;

#[derive(Parser, Debug)]
struct Args {
    /// Input parquet dir (s3:// or local path) with columns: qid, query, country, body_full, position
    #[arg(long)]
    input: String,
    /// Optional parquet dir with external docs (page_uuid -> body_full)
    #[arg(long)]
    doc_input: Option<String>,
    /// Output directory for JSONL
    #[arg(long)]
    output: String,
    /// Minimum position considered positive (inclusive lower bound of negatives).
    #[arg(long, default_value_t = 4)]
    min_positive_position: i32,
    /// Maximum position considered positive.
    #[arg(long, default_value_t = 3)]
    max_positive_position: i32,
    /// Number of negatives to sample per qid
    #[arg(long, default_value_t = 4)]
    num_negatives: usize,
}

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
struct DocVal { position: i32, body_full: String, query: String, country: String }

#[derive(Clone, Debug, Serialize)]
struct NemoOut {
    neg_doc: Vec<String>,
    pos_doc: String,
    query: String,
}

struct NemoMapper;
impl Mapper for NemoMapper {
    type Input = ParquetRow;
    type Key = String; // qid
    type Value = DocVal; // one value per document row (with meta)

    fn do_map<I, F>(&self, input: I, emit: &mut F)
    where
        I: IntoIterator<Item = Self::Input>,
        F: FnMut(Self::Key, Self::Value),
    {
        for row in input {
            let qid = pq_req_string(&row, "qid");
            let query = pq_req_string(&row, "q");
            let position = pq_req_int(&row, "position");
            // country may be missing; skip such rows
            let Some(country) = pq_get_string(&row, "country") else { continue; };
            // body_full may be missing; skip such rows
            let Some(body_full) = pq_get_string(&row, "body_full") else { continue; };
            let val = DocVal { position, body_full, query, country };
            emit(qid, val);
        }
    }
}

struct NemoReducer {
    min_pos: i32,
    max_pos: i32,
    num_negs: usize,
}

impl Reducer for NemoReducer {
    type Key = String;
    type ValueIn = DocVal;
    type Out = String; // JSONL

    fn do_reduce<I, F>(&self, _key: &Self::Key, values: I, emit: &mut F)
    where
        I: IntoIterator<Item = Self::ValueIn>,
        F: FnMut(Self::Out),
    {
        let mut any_query: Option<(String, String)> = None; // (query, country)
        let mut pos: Vec<String> = Vec::new();
        let mut neg: Vec<String> = Vec::new();

        for v in values {
            if any_query.is_none() { any_query = Some((v.query.clone(), v.country.clone())); }
            if v.position <= self.max_pos {
                pos.push(v.body_full);
            } else if v.position >= self.min_pos {
                neg.push(v.body_full);
            }
        }

        let Some((query_text, country)) = any_query else { return; };
        if pos.is_empty() { return; }
        let mut rng = thread_rng();
        let pos_doc = pos.choose(&mut rng).cloned().unwrap();
        neg.shuffle(&mut rng);
        let neg_doc: Vec<String> = neg.into_iter().take(self.num_negs).collect();
        let query = format!("{}\n{}", query_text, country);
        let out = NemoOut { neg_doc, pos_doc, query };
        if let Ok(s) = serde_json::to_string(&out) { emit(s); }
    }
}

fn pq_get_string(row: &ParquetRow, key: &str) -> Option<String> {
    match row.get(key) { Some(ParquetValue::String(s)) => Some(s.clone()), _ => None }
}
fn pq_get_int(row: &ParquetRow, key: &str) -> Option<i32> {
    match row.get(key) { Some(ParquetValue::Int64(v)) => Some(*v as i32), _ => None }
}
fn pq_req_string(row: &ParquetRow, key: &str) -> String {
    pq_get_string(row, key).unwrap_or_else(|| {
        let row_json = serde_json::to_string(row).unwrap_or_else(|_| format!("<debug {:?}>", row));
        tracing::error!(target = "nemo_jsonl", missing = %key, row = %row_json, "required string column missing");
        panic!("missing required string column: {}", key)
    })
}
fn pq_req_int(row: &ParquetRow, key: &str) -> i32 {
    pq_get_int(row, key).unwrap_or_else(|| {
        let row_json = serde_json::to_string(row).unwrap_or_else(|_| format!("<debug {:?}>", row));
        tracing::error!(target = "nemo_jsonl", missing = %key, row = %row_json, "required int column missing");
        panic!("missing required int column: {}", key)
    })
}

fn main() -> Result<()> {
    let args = Args::parse();
    // Init logging if not already initialized by environment
    {
        static ONCE: std::sync::Once = std::sync::Once::new();
        ONCE.call_once(|| {
            let _ = tracing_subscriber::fmt().with_env_filter(tracing_subscriber::EnvFilter::from_default_env()).try_init();
        });
    }

    if let Some(doc_uri) = &args.doc_input {
        // Two-stage: join body_full externally by page_uuid, then group by qid
        // Stage 1 types and pipeline
        #[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
        enum JoinIn { Main { page_uuid: String, qid: String, q: String, country: String, position: i32 }, Doc { page_uuid: String, body_full: String } }

        struct JoinMapper;
        impl Mapper for JoinMapper {
            type Input = JoinIn;
            type Key = String; // page_uuid
            type Value = JoinIn;
            fn do_map<I, F>(&self, input: I, emit: &mut F)
            where I: IntoIterator<Item = Self::Input>, F: FnMut(Self::Key, Self::Value) {
                for rec in input {
                    match &rec {
                        JoinIn::Main { page_uuid, .. } => emit(page_uuid.clone(), rec.clone()),
                        JoinIn::Doc { page_uuid, .. } => emit(page_uuid.clone(), rec.clone()),
                    }
                }
            }
        }

        #[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
        struct DocStage1 { qid: String, position: i32, body_full: String, query: String, country: String }

        struct JoinReducer;
        impl Reducer for JoinReducer {
            type Key = String; // page_uuid
            type ValueIn = JoinIn;
            type Out = String; // JSON lines of DocStage1
            fn do_reduce<I, F>(&self, _key: &Self::Key, values: I, emit: &mut F)
            where I: IntoIterator<Item = Self::ValueIn>, F: FnMut(Self::Out) {
                let mut body: Option<String> = None;
                let mut mains: Vec<(String, i32, String, String)> = Vec::new(); // (qid, position, qtext, country)
                for v in values {
                    match v {
                        JoinIn::Doc { page_uuid: _pu, body_full } => { if body.is_none() { body = Some(body_full); } },
                        JoinIn::Main { page_uuid: _pu, qid, q, country, position } => {
                            mains.push((qid, position, q, country));
                        }
                    }
                }
                if let Some(b) = body {
                    for (qid, position, q, country) in mains.into_iter() {
                        let out = DocStage1 { qid, position, body_full: b.clone(), query: q, country };
                        if let Ok(s) = serde_json::to_string(&out) { emit(s); }
                    }
                }
            }
        }

        // Stage 1 pipeline
        let stage1_out = format!("{}__stage1", args.output);
        let mut p1: RuntimePipeline<JoinIn> = RuntimePipeline::new();
        // Main input mapping
        p1.add_input::<ParquetRow, _>(&args.input, ParquetFormat, |row| {
            let page_uuid = pq_req_string(&row, "page_uuid");
            let qid = pq_req_string(&row, "qid");
            let q = pq_req_string(&row, "q");
            let position = pq_req_int(&row, "position");
            let Some(country) = pq_get_string(&row, "country") else { return JoinIn::Main { page_uuid, qid, q, country: String::new(), position }; };
            JoinIn::Main { page_uuid, qid, q, country, position }
        });
        // Doc input mapping
        p1.add_input::<ParquetRow, _>(doc_uri, ParquetFormat, |row| {
            let page_uuid = pq_req_string(&row, "page_uuid");
            let body_full = pq_req_string(&row, "body_full");
            JoinIn::Doc { page_uuid, body_full }
        });
        p1.add_output(&stage1_out);
        p1.map_reduce(JoinMapper, JoinReducer, TextLineSink { base: stage1_out.clone() })?;

        // Stage 2: map JSON lines -> (qid -> DocVal), then group & sample via NemoReducer
        #[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
        struct DocStage1In { qid: String, position: i32, body_full: String, query: String, country: String }
        struct Stage2Mapper;
        impl Mapper for Stage2Mapper {
            type Input = String;
            type Key = String; // qid
            type Value = DocVal;
            fn do_map<I, F>(&self, input: I, emit: &mut F)
            where I: IntoIterator<Item = Self::Input>, F: FnMut(Self::Key, Self::Value) {
                for line in input {
                    if let Ok(d) = serde_json::from_str::<DocStage1In>(&line) {
                        let val = DocVal { position: d.position, body_full: d.body_full, query: d.query, country: d.country };
                        emit(d.qid, val);
                    }
                }
            }
        }
        let mut p2: RuntimePipeline<String> = RuntimePipeline::new();
        p2.add_input_single::<String, _>(&stage1_out, bjorn::io::TextLineFormat);
        p2.add_output(&args.output);
        let reducer = NemoReducer { min_pos: args.min_positive_position, max_pos: args.max_positive_position, num_negs: args.num_negatives };
        p2.map_reduce(Stage2Mapper, reducer, TextLineSink { base: args.output.clone() })?;
        Ok(())
    } else {
        // Single-stage pipeline (body_full is in the input)
        let mut p: RuntimePipeline<ParquetRow> = RuntimePipeline::new();
        p.add_input_single::<ParquetRow, _>(&args.input, ParquetFormat);
        p.add_output(&args.output);
        let reducer = NemoReducer { min_pos: args.min_positive_position, max_pos: args.max_positive_position, num_negs: args.num_negatives };
        p.map_reduce(NemoMapper, reducer, TextLineSink { base: args.output.clone() })
            .with_context(|| "map_reduce failed")?;
        Ok(())
    }
}
