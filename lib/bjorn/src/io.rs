use anyhow::{Context, Result};
use serde::{Serialize, de::DeserializeOwned};
use std::fs::{self, File};
use std::hash::{Hasher, Hash};
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

pub fn hash_to_partition<K: Hash>(key: &K, num_partitions: usize) -> usize {
    let mut hasher = DefaultHasher::new();
    key.hash(&mut hasher);
    (hasher.finish() as usize) % num_partitions
}

// Binary intermediate format: [u32 key_len][u32 val_len][key_bytes][val_bytes]
pub fn write_bin<K, V>(writer: &mut BufWriter<File>, key: &K, value: &V) -> Result<()>
where
    K: Serialize,
    V: Serialize,
{
    let key_bytes = bincode::serialize(key)?;
    let val_bytes = bincode::serialize(value)?;
    let klen = key_bytes.len() as u32;
    let vlen = val_bytes.len() as u32;
    writer.write_all(&klen.to_le_bytes())?;
    writer.write_all(&vlen.to_le_bytes())?;
    writer.write_all(&key_bytes)?;
    writer.write_all(&val_bytes)?;
    Ok(())
}

pub fn read_bin_line<'a>(bytes: &'a [u8], mut off: usize) -> Option<(&'a [u8], &'a [u8], usize)> {
    if off + 8 > bytes.len() { return None; }
    let klen = u32::from_le_bytes([bytes[off], bytes[off+1], bytes[off+2], bytes[off+3]]) as usize; off += 4;
    let vlen = u32::from_le_bytes([bytes[off], bytes[off+1], bytes[off+2], bytes[off+3]]) as usize; off += 4;
    if off + klen + vlen > bytes.len() { return None; }
    let k = &bytes[off..off+klen];
    let v = &bytes[off+klen..off+klen+vlen];
    Some((k, v, off + klen + vlen))
}

pub fn decode_bin<K: DeserializeOwned, V: DeserializeOwned>(k: &[u8], v: &[u8]) -> Result<(K, V)> {
    let key: K = bincode::deserialize(k)?;
    let val: V = bincode::deserialize(v)?;
    Ok((key, val))
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
    // Use a large buffer to reduce write syscalls under heavy shuffle
    Ok(BufWriter::with_capacity(32 * 1024 * 1024, file))
}

pub fn open_reader(path: impl AsRef<Path>) -> Result<BufReader<File>> {
    let file = File::open(path)?;
    Ok(BufReader::new(file))
}
