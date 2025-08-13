use anyhow::{Context, Result};
use bjorn::{Mapper, Reducer, RuntimePipeline};
use bjorn::api::ExecutablePipeline;
use bjorn::io::{ParquetRow, ParquetValue, TextLineSink, BinaryLineSink, BinaryLineFormat};
use std::io::Read;
use std::fs;
use clap::Parser;
use rand::{seq::SliceRandom, thread_rng};
use serde::Serialize;

#[derive(Parser, Debug)]
struct Args {
    /// Input parquet dir(s) (s3:// or local path). Accepts multiple or comma-separated.
    /// Columns: qid, q (query text), country, position, page_uuid, body_full
    #[arg(long, value_delimiter = ',', num_args = 1..)]
    input: Vec<String>,
    /// Optional parquet dir with external docs (page_uuid -> body column)
    #[arg(long)]
    doc_input: Option<String>,
    /// Output directory for JSONL
    #[arg(long)]
    output: String,
    /// Name of the document body column
    #[arg(long, default_value = "body_full")]
    body_colname: String,
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

struct NemoMapper { body_colname: String }
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
            let Some(body_full) = pq_get_string(&row, &self.body_colname) else { continue; };
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

// ===== Two-stage join types (when --doc-input is used) =====
#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
enum JoinIn {
    Main { page_uuid: String, qid: String, q: String, country: String, position: i32 },
    // body_full is optional; if missing, we skip emitting this doc at map time
    Doc { page_uuid: String, body_full: Option<String> },
}

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
                // Only emit docs that actually have a body_full
                JoinIn::Doc { page_uuid, body_full: Some(_) } => emit(page_uuid.clone(), rec.clone()),
                JoinIn::Doc { .. } => { /* skip */ }
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
    type Out = DocStage1; // binary inter-stage records
    fn do_reduce<I, F>(&self, _key: &Self::Key, values: I, emit: &mut F)
    where I: IntoIterator<Item = Self::ValueIn>, F: FnMut(Self::Out) {
        let mut body: Option<String> = None;
        let mut mains: Vec<(String, i32, String, String)> = Vec::new(); // (qid, position, qtext, country)
        for v in values {
            match v {
                JoinIn::Doc { page_uuid: _pu, body_full } => {
                    if body.is_none() {
                        if let Some(b) = body_full { if !b.is_empty() { body = Some(b); } }
                    }
                },
                JoinIn::Main { page_uuid: _pu, qid, q, country, position } => {
                    mains.push((qid, position, q, country));
                }
            }
        }
        if let Some(b) = body {
            for (qid, position, q, country) in mains.into_iter() {
                if country.is_empty() { continue; }
                emit(DocStage1 { qid, position, body_full: b.clone(), query: q, country });
            }
        }
    }
}

struct Stage2Mapper;
impl Mapper for Stage2Mapper {
    type Input = DocStage1;
    type Key = String; // qid
    type Value = DocVal;
    fn do_map<I, F>(&self, input: I, emit: &mut F)
    where I: IntoIterator<Item = Self::Input>, F: FnMut(Self::Key, Self::Value) {
        for d in input {
            let val = DocVal { position: d.position, body_full: d.body_full, query: d.query, country: d.country };
            emit(d.qid, val);
        }
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

    if let Some(doc_uri) = &args.doc_input {
        // Two-stage: join body_full externally by page_uuid, then group by qid

        // Stage 1 pipeline (project only required columns from main/doc inputs)
        let stage1_out = format!("{}__stage1", args.output);
        let mut p1: RuntimePipeline<JoinIn> = RuntimePipeline::new();
        // Main input mapping (can be multiple URIs)
        for in_uri in &args.input {
            let main_proj = bjorn::io::ParquetFormatProjected::new(["page_uuid","qid","q","country","position"]);
            p1.add_input::<ParquetRow, _>(in_uri, main_proj, |row| {
                let page_uuid = pq_req_string(&row, "page_uuid");
                let qid = pq_req_string(&row, "qid");
                let q = pq_req_string(&row, "q");
                let position = pq_req_int(&row, "position");
                let Some(country) = pq_get_string(&row, "country") else { return JoinIn::Main { page_uuid, qid, q, country: String::new(), position }; };
                JoinIn::Main { page_uuid, qid, q, country, position }
            });
        }
        // Doc input mapping: decode JoinIn directly with dynamic body column
        struct DocInputFormat { body_colname: String }
        impl bjorn::io::Format<JoinIn> for DocInputFormat {
            fn decode<'a>(&self, r: Box<dyn Read + Send + 'a>) -> Result<Box<dyn Iterator<Item = JoinIn> + Send + 'a>> {
                let proj = bjorn::io::ParquetFormatProjected::new(vec!["page_uuid".to_string(), self.body_colname.clone()]);
                let it = proj.decode(r)?;
                let body_key = self.body_colname.clone();
                let rows: Vec<JoinIn> = it
                    .map(move |row| {
                        let page_uuid = pq_req_string(&row, "page_uuid");
                        let body_full = pq_get_string(&row, &body_key);
                        JoinIn::Doc { page_uuid, body_full }
                    })
                    .collect();
                Ok(Box::new(rows.into_iter()))
            }
        }
        p1.add_input_single::<JoinIn, _>(doc_uri, DocInputFormat { body_colname: args.body_colname.clone() });
        p1.add_output(&stage1_out);
        let stage1_sink = BinaryLineSink::<DocStage1>::new(&stage1_out);
        p1.map_reduce(JoinMapper, JoinReducer, stage1_sink)?;

        // Stage 2: map stage1 binary records -> (qid -> DocVal), then group & sample via NemoReducer
        let mut p2: RuntimePipeline<DocStage1> = RuntimePipeline::new();
        p2.add_input_single::<DocStage1, _>(&stage1_out, BinaryLineFormat::<DocStage1>::default());
        p2.add_output(&args.output);
        let reducer = NemoReducer { min_pos: args.min_positive_position, max_pos: args.max_positive_position, num_negs: args.num_negatives };
        p2.map_reduce(Stage2Mapper, reducer, TextLineSink { base: args.output.clone() })?;
        // Cleanup stage1 intermediates (only once in Slurm: rank 0). In non-Slurm, always.
        let is_rank0 = std::env::var("SLURM_PROCID").ok().map_or(true, |v| v == "0");
        if is_rank0 {
            match fs::remove_dir_all(&stage1_out) {
                Ok(()) => tracing::info!(target = "nemo_jsonl", stage1 = %stage1_out, "removed stage1 intermediates"),
                Err(e) => tracing::warn!(target = "nemo_jsonl", stage1 = %stage1_out, error = %e, "failed to remove stage1 intermediates"),
            }
        }
        Ok(())
    } else {
        // Single-stage pipeline (body column is in the input) with projection
        let mut p: RuntimePipeline<ParquetRow> = RuntimePipeline::new();
        for in_uri in &args.input {
            let proj = bjorn::io::ParquetFormatProjected::new(vec![
                "qid".to_string(),
                "q".to_string(),
                "country".to_string(),
                "position".to_string(),
                "page_uuid".to_string(),
                args.body_colname.clone(),
            ]);
            p.add_input_single::<ParquetRow, _>(in_uri, proj);
        }
        p.add_output(&args.output);
        let reducer = NemoReducer { min_pos: args.min_positive_position, max_pos: args.max_positive_position, num_negs: args.num_negatives };
        let mapper = NemoMapper { body_colname: args.body_colname.clone() };
        p.map_reduce(mapper, reducer, TextLineSink { base: args.output.clone() })
            .with_context(|| "map_reduce failed")?;
        Ok(())
    }
}
