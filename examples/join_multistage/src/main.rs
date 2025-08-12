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
    /// Output directory for final grouped result
    #[arg(long)]
    output: String,
    /// Optional temp dir for stage1 output (defaults to sibling under output)
    #[arg(long)]
    tmp: Option<String>,
}

#[derive(Serialize, Deserialize, Clone, Debug)]
struct SearchEntry { query: String, page_uuid: Uuid, position: u8 }

#[derive(Serialize, Deserialize, Clone, Debug)]
struct Page { page_uuid: Uuid, body: String }

#[derive(Serialize, Deserialize, Clone, Debug)]
enum JoinIn { Search(SearchEntry), Page(Page) }
impl From<String> for JoinIn { fn from(s: String) -> Self { JoinIn::Search(parse_search(&s)) } }
fn parse_search(line: &str) -> SearchEntry { serde_json::from_str::<SearchEntry>(line).unwrap() }
fn parse_page(line: &str) -> Page { serde_json::from_str::<Page>(line).unwrap() }

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
    where I: IntoIterator<Item = Self::Input>, F: FnMut(Self::Key, Self::Value), {
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
    type Out = String; // JSONL per joined record
    fn do_reduce<I, F>(&self, key: &Self::Key, values: I, emit: &mut F)
    where I: IntoIterator<Item = Self::ValueIn>, F: FnMut(Self::Out), {
        let mut body: Option<String> = None;
        let mut pending: Vec<(String, u8)> = Vec::new();
        for v in values {
            match v {
                JoinVal::PageBody(b) => { if body.is_none() { body = Some(b); } }
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

// Stage 2 types
#[derive(Deserialize, Clone, Debug)]
struct JoinedIn { query: String, page_uuid: Uuid, position: u8, body: String }
#[derive(Serialize, Deserialize, Clone, Debug)]
struct Doc { page_uuid: Uuid, position: u8, body: String }
#[derive(Serialize, Clone, Debug)]
struct Grouped { query: String, docs: Vec<Doc> }

struct GroupByMapper;
impl Mapper for GroupByMapper {
    type Input = String;                  // lines from stage-1 output
    type Key = String;                    // query
    type Value = Doc;
    fn do_map<I, F>(&self, input: I, emit: &mut F)
    where I: IntoIterator<Item = Self::Input>, F: FnMut(Self::Key, Self::Value), {
        for line in input {
            if let Ok(j) = serde_json::from_str::<JoinedIn>(&line) {
                emit(j.query.clone(), Doc { page_uuid: j.page_uuid, position: j.position, body: j.body });
            }
        }
    }
}

struct GroupByReducer;
impl Reducer for GroupByReducer {
    type Key = String;
    type ValueIn = Doc;
    type Out = String; // JSONL grouped record
    fn do_reduce<I, F>(&self, key: &Self::Key, values: I, emit: &mut F)
    where I: IntoIterator<Item = Self::ValueIn>, F: FnMut(Self::Out), {
        let docs: Vec<Doc> = values.into_iter().collect();
        let out = Grouped { query: key.clone(), docs };
        emit(serde_json::to_string(&out).unwrap());
    }
}

fn main() -> Result<()> {
    let args = Args::parse();
    let stage1_out = args.tmp.clone().unwrap_or_else(|| format!("{}__stage1", args.output));

    // Stage 1: join to produce per-document JSON lines
    {
        let mut p1: RuntimePipeline<JoinIn> = RuntimePipeline::new();
        p1.add_input::<String, _>(&args.input1, TextLineFormat, |line| JoinIn::Search(parse_search(&line)));
        p1.add_input::<String, _>(&args.input2, TextLineFormat, |line| JoinIn::Page(parse_page(&line)));
        p1.add_output(&stage1_out);
        p1.map_reduce(JoinMapper, JoinReducer, TextLineSink { base: stage1_out.clone() })?;
    }

    // Stage 2: group by query -> list of docs
    {
        let mut p2: RuntimePipeline<String> = RuntimePipeline::new();
        p2.add_input_single::<String, _>(&stage1_out, TextLineFormat);
        p2.add_output(&args.output);
        p2.map_reduce(GroupByMapper, GroupByReducer, TextLineSink { base: args.output.clone() })?;
    }

    Ok(())
}
