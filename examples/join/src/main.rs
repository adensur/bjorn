use anyhow::Result;
use bjorn::{Mapper, Reducer, RuntimePipeline};
use bjorn::api::ExecutablePipeline;
use bjorn::io::{TextLineFormat, TextLineSink};
use clap::Parser;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

#[derive(Parser, Debug)]
struct Args {
    /// Input1 directory (search entries)
    #[arg(long)]
    input1: String,
    /// Input2 directory (pages)
    #[arg(long)]
    input2: String,
    /// Output directory
    #[arg(long)]
    output: String,
}

#[derive(Serialize, Deserialize, Clone, Debug)]
struct SearchEntry { query: String, page_uuid: Uuid, position: u8 }

#[derive(Serialize, Deserialize, Clone, Debug)]
struct Page { page_uuid: Uuid, body: String }

#[derive(Serialize, Deserialize, Clone, Debug)]
enum JoinIn { Search(SearchEntry), Page(Page) }

impl From<String> for JoinIn {
    fn from(s: String) -> Self { JoinIn::Search(parse_search(&s)) }
}

fn parse_search(line: &str) -> SearchEntry {
    // Expect JSON object per line: {"query":..., "page_uuid":..., "position":...}
    serde_json::from_str::<SearchEntry>(line).unwrap()
}

fn parse_page(line: &str) -> Page {
    serde_json::from_str::<Page>(line).unwrap()
}

#[derive(Serialize, Deserialize, Clone, Debug)]
enum JoinVal {
    Search { query: String, position: u8 },
    PageBody(String),
}

#[derive(Serialize, Deserialize, Clone, Debug)]
struct Joined { query: String, page_uuid: Uuid, position: u8, body: String }

struct JoinMapper;

impl Mapper for JoinMapper {
    type Input = JoinIn;
    type Key = Uuid;
    type Value = JoinVal;

    fn do_map<I, F>(&self, input: I, emit: &mut F)
    where
        I: IntoIterator<Item = Self::Input>,
        F: FnMut(Self::Key, Self::Value),
    {
        for rec in input {
            match rec {
                JoinIn::Search(s) => emit(s.page_uuid, JoinVal::Search { query: s.query, position: s.position }),
                JoinIn::Page(p) => emit(p.page_uuid, JoinVal::PageBody(p.body)),
            }
        }
    }
}

struct JoinReducer;

impl Reducer for JoinReducer {
    type Key = Uuid;
    type ValueIn = JoinVal;
    type Out = String; // TSV JSON

    fn do_reduce<I, F>(&self, key: &Self::Key, values: I, emit: &mut F)
    where
        I: IntoIterator<Item = Self::ValueIn>,
        F: FnMut(Self::Out),
    {
        let mut body: Option<String> = None;
        let mut pending: Vec<(String, u8)> = Vec::new();
        for v in values {
            match v {
                JoinVal::PageBody(b) => {
                    if body.is_some() { /* duplicate bodies unexpected */ }
                    body = Some(b);
                }
                JoinVal::Search { query, position } => {
                    if let Some(b) = body.as_ref() {
                        let out = Joined { query, page_uuid: *key, position, body: b.clone() };
                        emit(serde_json::to_string(&out).unwrap());
                    } else {
                        pending.push((query, position));
                    }
                }
            }
        }
        if let Some(b) = body {
            for (query, position) in pending { emit(serde_json::to_string(&Joined { query, page_uuid: *key, position, body: b.clone() }).unwrap()); }
        }
    }
}

fn main() -> Result<()> {
    let args = Args::parse();
    let mut pipeline: RuntimePipeline<JoinIn> = RuntimePipeline::new();
    // input1: searches as JSONL
    pipeline.add_input::<String, _>(&args.input1, TextLineFormat, |line| JoinIn::Search(parse_search(&line)));
    // input2: pages as JSONL
    pipeline.add_input::<String, _>(&args.input2, TextLineFormat, |line| JoinIn::Page(parse_page(&line)));
    pipeline.add_output(&args.output);
    pipeline.map_reduce(JoinMapper, JoinReducer, TextLineSink { base: args.output.clone() })?
    ;
    Ok(())
}
