use anyhow::{Context, Result};
use serde::Serialize;
use std::fs::{self, File};
use std::hash::Hasher;
use std::io::{BufRead, BufReader, BufWriter, Write};
use std::path::{Path, PathBuf};
use std::collections::hash_map::DefaultHasher;

pub fn ensure_dir(path: impl AsRef<Path>) -> Result<()> {
    fs::create_dir_all(path.as_ref()).with_context(|| format!("create_dir_all {}", path.as_ref().display()))
}

pub fn list_files_recursive(path: impl AsRef<Path>) -> Result<Vec<PathBuf>> {
    let mut files = Vec::new();
    for entry in walkdir::WalkDir::new(path) {
        let entry = entry?;
        if entry.file_type().is_file() {
            files.push(entry.path().to_path_buf());
        }
    }
    Ok(files)
}

pub fn read_lines(path: impl AsRef<Path>) -> Result<impl Iterator<Item = Result<String>>> {
    let file = File::open(path.as_ref())?;
    let reader = BufReader::new(file);
    Ok(reader.lines().map(|l| l.map_err(anyhow::Error::from)))
}

pub fn hash_to_partition<K: Serialize>(key: &K, num_partitions: usize) -> usize {
    let mut hasher = DefaultHasher::new();
    let bytes = serde_json::to_vec(key).expect("serialize key");
    hasher.write(&bytes);
    (hasher.finish() as usize) % num_partitions
}

pub fn write_tsv<K, V>(writer: &mut BufWriter<File>, key: &K, value: &V) -> Result<()>
where
    K: Serialize,
    V: Serialize,
{
    let key_str = serde_json::to_string(key)?;
    let value_str = serde_json::to_string(value)?;
    writeln!(writer, "{}\t{}", key_str, value_str)?;
    Ok(())
}

pub fn open_writer(path: impl AsRef<Path>) -> Result<BufWriter<File>> {
    if let Some(parent) = path.as_ref().parent() {
        ensure_dir(parent)?;
    }
    let file = File::create(path)?;
    Ok(BufWriter::new(file))
}

pub fn open_reader(path: impl AsRef<Path>) -> Result<BufReader<File>> {
    let file = File::open(path)?;
    Ok(BufReader::new(file))
}
