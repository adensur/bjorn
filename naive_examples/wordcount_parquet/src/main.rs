use anyhow::Result;
use clap::Parser;
use rayon::prelude::*;
use regex::Regex;
use std::collections::HashMap;
use std::fs;
use std::path::PathBuf;
use walkdir::WalkDir;

#[derive(Parser, Debug)]
struct Args {
    #[arg(long)]
    input: String,
    #[arg(long)]
    output: String,
}

fn list_files(input: &str) -> Vec<PathBuf> {
    WalkDir::new(input)
        .into_iter()
        .filter_map(|e| e.ok())
        .filter(|e| e.file_type().is_file())
        .map(|e| e.into_path())
        .collect()
}

fn read_parquet_lines(path: &PathBuf) -> Vec<String> {
    let file = std::fs::File::open(path).unwrap();
    let reader = parquet::file::serialized_reader::SerializedFileReader::new(file).unwrap();
    let iter = parquet::record::reader::RowIter::from_file(None, &reader).unwrap();
    let mut out = Vec::new();
    for row_res in iter {
        let row = row_res.expect("row read failed");
        if let Some((_, field)) = row.get_column_iter().find(|(n, _)| *n == "line") {
            if let parquet::record::Field::Str(s) = field { out.push(s.clone()); }
        }
    }
    out
}

fn write_parquet_pairs(path: &str, key_name: &str, value_name: &str, rows: &[(String, i64)]) -> Result<()> {
    use parquet::basic::Compression;
    use parquet::data_type::ByteArray;
    use parquet::file::properties::WriterProperties;
    use parquet::file::writer::SerializedFileWriter;
    use parquet::schema::parser::parse_message_type;

    let schema_str = format!(
        "message schema {{ OPTIONAL BINARY {} (UTF8); OPTIONAL INT64 {} ; }}",
        key_name, value_name
    );
    let schema = parse_message_type(&schema_str).map_err(|e| anyhow::anyhow!(e.to_string()))?;

    let props = WriterProperties::builder().set_compression(Compression::SNAPPY).build();
    let file = std::fs::File::create(path)?;
    let mut writer = SerializedFileWriter::new(file, std::sync::Arc::new(schema), std::sync::Arc::new(props))
        .map_err(|e| anyhow::anyhow!(e.to_string()))?;

    let mut row_group = writer.next_row_group().map_err(|e| anyhow::anyhow!(e.to_string()))?;
    if let Some(mut col_writer) = row_group.next_column().map_err(|e| anyhow::anyhow!(e.to_string()))? {
        let typed = col_writer.typed::<parquet::data_type::ByteArrayType>();
        let data: Vec<ByteArray> = rows.iter().map(|(k, _)| ByteArray::from(k.as_str())).collect();
        let def: Vec<i16> = std::iter::repeat(1).take(data.len()).collect();
        let _ = typed.write_batch(&data, Some(&def), None).map_err(|e| anyhow::anyhow!(e.to_string()))?;
        col_writer.close().map_err(|e| anyhow::anyhow!(e.to_string()))?;
    }
    if let Some(mut col_writer) = row_group.next_column().map_err(|e| anyhow::anyhow!(e.to_string()))? {
        let typed = col_writer.typed::<parquet::data_type::Int64Type>();
        let data: Vec<i64> = rows.iter().map(|(_, v)| *v).collect();
        let def: Vec<i16> = std::iter::repeat(1).take(data.len()).collect();
        let _ = typed.write_batch(&data, Some(&def), None).map_err(|e| anyhow::anyhow!(e.to_string()))?;
        col_writer.close().map_err(|e| anyhow::anyhow!(e.to_string()))?;
    }
    row_group.close().map_err(|e| anyhow::anyhow!(e.to_string()))?;
    writer.close().map_err(|e| anyhow::anyhow!(e.to_string()))?;
    Ok(())
}

fn main() -> Result<()> {
    tracing_subscriber::fmt().with_env_filter("info").init();
    let args = Args::parse();
    let files = list_files(&args.input);
    let word_re = Regex::new(r"[A-Za-z0-9_']+").unwrap();

    let map_results: Vec<HashMap<String, i64>> = files
        .par_iter()
        .filter(|p| p.extension().map(|e| e.to_string_lossy()) == Some("parquet".into()))
        .map(|path| {
            let mut local: HashMap<String, i64> = HashMap::new();
            for line in read_parquet_lines(path) {
                for cap in word_re.find_iter(&line) {
                    *local.entry(cap.as_str().to_lowercase()).or_insert(0) += 1;
                }
            }
            local
        })
        .collect();

    let mut global: HashMap<String, i64> = HashMap::new();
    for m in map_results {
        for (k, v) in m {
            *global.entry(k).or_insert(0) += v;
        }
    }

    // write parquet single part
    fs::create_dir_all(&args.output)?;
    let mut rows: Vec<(String, i64)> = global.into_iter().collect();
    rows.sort_by(|a, b| a.0.cmp(&b.0));
    write_parquet_pairs(&format!("{}/part-00000.parquet", args.output), "word", "count", &rows)?;

    Ok(())
}
