use anyhow::Result;
use clap::Parser;
use rayon::prelude::*;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fs;
use std::path::PathBuf;
use uuid::Uuid;
use walkdir::WalkDir;

#[derive(Parser, Debug)]
struct Args {
    #[arg(long)]
    input1: String, // searches dir
    #[arg(long)]
    input2: String, // pages dir
    #[arg(long)]
    output: String,
}

#[derive(Serialize, Deserialize, Clone, Debug)]
struct SearchEntry { query: String, page_uuid: Uuid, position: u8 }

#[derive(Serialize, Deserialize, Clone, Debug)]
struct Page { page_uuid: Uuid, body: String }

#[derive(Serialize, Deserialize, Clone, Debug)]
struct Joined { query: String, page_uuid: Uuid, position: u8, body: String }

#[derive(Serialize, Deserialize, Clone, Debug)]
struct Doc { page_uuid: Uuid, position: u8, body: String }

#[derive(Serialize, Deserialize, Clone, Debug)]
struct Grouped { query: String, docs: Vec<Doc> }

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

    // Load pages into map
    let page_files = list_files(&args.input2);
    let pages: HashMap<Uuid, String> = page_files
        .par_iter()
        .map(|p| fs::read_to_string(p).unwrap_or_default())
        .flat_map(|contents| contents.lines().map(|l| serde_json::from_str::<Page>(l).unwrap()).collect::<Vec<_>>())
        .map(|p| (p.page_uuid, p.body))
        .collect();

    // Stream searches and join
    let search_files = list_files(&args.input1);
    let joined: Vec<Joined> = search_files
        .par_iter()
        .map(|p| fs::read_to_string(p).unwrap_or_default())
        .flat_map(|contents| {
            contents
                .lines()
                .map(|l| serde_json::from_str::<SearchEntry>(l).unwrap())
                .collect::<Vec<_>>()
        })
        .filter_map(|s| pages.get(&s.page_uuid).map(|body| Joined { query: s.query, page_uuid: s.page_uuid, position: s.position, body: body.clone() }))
        .collect();

    // Group by query
    let mut groups: HashMap<String, Vec<Doc>> = HashMap::new();
    for j in joined.into_iter() {
        groups.entry(j.query.clone()).or_default().push(Doc { page_uuid: j.page_uuid, position: j.position, body: j.body });
    }

    // write text lines sequentially
    fs::create_dir_all(&args.output)?;
    let out_path = format!("{}/part-00000.txt", args.output);
    let mut lines: Vec<String> = groups
        .into_iter()
        .map(|(query, docs)| serde_json::to_string(&Grouped { query, docs }).unwrap())
        .collect();
    lines.sort();
    fs::write(out_path, lines.join("\n") + "\n")?;

    Ok(())
}
