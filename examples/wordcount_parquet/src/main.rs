use anyhow::Result;
use bjorn::{Mapper, Reducer, RuntimePipeline};
use bjorn::api::ExecutablePipeline;
use bjorn::io::{ParquetFormat, ParquetRow, ParquetValue, ParquetRowSink, ParquetFieldType};
use clap::Parser;
use regex::Regex;

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
    type Input = ParquetRow;
    type Key = String;
    type Value = i64;

    fn do_map<I, F>(&self, input: I, emit: &mut F)
    where
        I: IntoIterator<Item = Self::Input>,
        F: FnMut(Self::Key, Self::Value),
    {
        let word_re = Regex::new(r"[A-Za-z0-9_']+").unwrap();
        for row in input {
            if let Some(ParquetValue::String(line)) = row.get("line") {
                for cap in word_re.find_iter(line) {
                    let w = cap.as_str().to_lowercase();
                    emit(w, 1);
                }
            }
        }
    }
}

struct WordcountReducer;

impl Reducer for WordcountReducer {
    type Key = String;
    type ValueIn = i64;
    type Out = ParquetRow;

    fn do_reduce<I, F>(&self, key: &Self::Key, values: I, emit: &mut F)
    where
        I: IntoIterator<Item = Self::ValueIn>,
        F: FnMut(Self::Out),
    {
        let sum: i64 = values.into_iter().sum();
        let mut row = ParquetRow::new();
        row.insert("word".to_string(), ParquetValue::String(key.clone()));
        row.insert("count".to_string(), ParquetValue::Int64(sum));
        emit(row);
    }
}

fn main() -> Result<()> {
    let args = Args::parse();
    let mut pipeline = RuntimePipeline::new();
    pipeline.add_input::<ParquetRow>(&args.input);
    pipeline.add_output(&args.output);
    let mut schema = std::collections::BTreeMap::new();
    schema.insert("word".to_string(), ParquetFieldType::String);
    schema.insert("count".to_string(), ParquetFieldType::Int64);
    let sink = ParquetRowSink { base: args.output.clone(), schema };
    pipeline.map_reduce(WordcountMapper, WordcountReducer, ParquetFormat, sink)?;
    Ok(())
}
