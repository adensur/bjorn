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

fn main() -> Result<()> {
    tracing_subscriber::fmt().with_env_filter("info").init();
    let args = Args::parse();
    let files = list_files(&args.input);
    let word_re = Regex::new(r"[A-Za-z0-9_']+").unwrap();

    let map_results: Vec<HashMap<String, u64>> = files
        .par_iter()
        .map(|path| {
            let content = fs::read_to_string(path).unwrap_or_default();
            let mut local: HashMap<String, u64> = HashMap::new();
            for line in content.lines() {
                for cap in word_re.find_iter(line) {
                    *local.entry(cap.as_str().to_lowercase()).or_insert(0) += 1;
                }
            }
            local
        })
        .collect();

    let mut global: HashMap<String, u64> = HashMap::new();
    for m in map_results {
        for (k, v) in m {
            *global.entry(k).or_insert(0) += v;
        }
    }

    // write TSV parts sequentially
    fs::create_dir_all(&args.output)?;
    let out_path = format!("{}/part-00000.tsv", args.output);
    let mut lines: Vec<String> = global.into_iter().map(|(k, v)| format!("\"{}\"\t{}", k, v)).collect();
    lines.sort();
    fs::write(out_path, lines.join("\n") + "\n")?;

    Ok(())
}
