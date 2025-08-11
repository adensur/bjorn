use anyhow::Result;
use bjorn::{Mapper, Reducer, RuntimePipeline};
use bjorn::api::ExecutablePipeline;
use clap::Parser;
use regex::Regex;
use std::iter;

#[derive(Parser, Debug)]
struct Args {
    /// Input directory
    #[arg(long)]
    input: String,
    /// Output directory
    #[arg(long)]
    output: String,
}

struct WordcountMapper;

impl Mapper for WordcountMapper {
    type Input = String;
    type Key = String;
    type Value = u64;

    fn do_map<I, F>(&self, input: I, emit: &mut F)
    where
        I: IntoIterator<Item = Self::Input>,
        F: FnMut(Self::Key, Self::Value),
    {
        let word_re = Regex::new(r"[A-Za-z0-9_']+").unwrap();
        for line in input {
            for cap in word_re.find_iter(&line) {
                let w = cap.as_str().to_lowercase();
                emit(w, 1);
            }
        }
    }
}

struct WordcountReducer;

impl Reducer for WordcountReducer {
    type Key = String;
    type ValueIn = u64;

    fn do_reduce<I, F>(&self, key: &Self::Key, values: I, emit: &mut F)
    where
        I: IntoIterator<Item = Self::ValueIn>,
        F: FnMut(String),
    {
        let sum: u64 = values.into_iter().sum();
        emit(format!("\"{}\"\t{}", key, sum));
    }
}

fn main() -> Result<()> {
    tracing_subscriber::fmt().with_env_filter("info").init();
    let args = Args::parse();
    let mut pipeline = RuntimePipeline::new();
    pipeline.add_input::<String>(&args.input);
    pipeline.add_output(&args.output);
    pipeline.map_reduce(WordcountMapper, WordcountReducer)?;
    Ok(())
}
