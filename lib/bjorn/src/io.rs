use anyhow::{Context, Result};
use serde::{Serialize, de::DeserializeOwned};
use std::fs::{self, File};
use std::hash::{Hasher, Hash};
use std::io::{BufRead, BufReader, BufWriter, Write};
use std::path::{Path, PathBuf};
use std::collections::hash_map::DefaultHasher;
use std::io::Read;

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

// ============== Pluggable IO: Sources & Formats & Sinks (minimal skeleton) ==============

#[derive(Clone)]
pub struct Split {
    pub uri: String,
    pub byte_range: Option<(u64, u64)>,
    pub meta: Option<serde_json::Value>,
}

pub trait Source: Send + Sync {
    fn list_splits(&self, input: &str) -> Result<Vec<Split>>;
    fn open_split(&self, split: &Split) -> Result<Box<dyn Read + Send>>;
}

pub trait Format<I>: Send + Sync + 'static {
    fn decode<'a>(&self, r: Box<dyn Read + Send + 'a>) -> Result<Box<dyn Iterator<Item = I> + Send + 'a>>;
}

pub trait Sink: Send + Sync {
    fn open_partition(&self, partition_index: usize) -> Result<Box<dyn std::io::Write + Send>>;
}

// Local FS implementations
pub struct LocalFsSource;
impl Source for LocalFsSource {
    fn list_splits(&self, input: &str) -> Result<Vec<Split>> {
        let mut paths = list_files_recursive(input)?;
        paths.sort();
        Ok(paths.into_iter().map(|p| Split { uri: p.to_string_lossy().to_string(), byte_range: None, meta: None }).collect())
    }
    fn open_split(&self, split: &Split) -> Result<Box<dyn Read + Send>> {
        let f = File::open(&split.uri)?;
        Ok(Box::new(f))
    }
}

pub struct TextLineFormat;
impl Format<String> for TextLineFormat {
    fn decode<'a>(&self, r: Box<dyn Read + Send + 'a>) -> Result<Box<dyn Iterator<Item = String> + Send + 'a>> {
        let reader = BufReader::new(r);
        Ok(Box::new(reader.lines().filter_map(|l| l.ok())))
    }
}

pub struct LocalFsSink { pub base: String }
impl Sink for LocalFsSink {
    fn open_partition(&self, partition_index: usize) -> Result<Box<dyn std::io::Write + Send>> {
        let path = format!("{}/part-{:05}.tsv", self.base, partition_index);
        let w = open_writer(path)?;
        Ok(Box::new(w))
    }
}

// S3 source (read-only): list objects and open via blocking client
#[derive(Clone)]
pub struct S3Source { pub bucket: String, pub prefix: String, pub region: String }

impl S3Source {
    pub fn from_uri(uri: &str) -> Result<Self> {
        // Expect s3://bucket/prefix...
        let rest = uri.strip_prefix("s3://").context("s3 uri must start with s3://")?;
        let (bucket, prefix) = rest.split_once('/').unwrap_or((rest, ""));
        Ok(Self { bucket: bucket.to_string(), prefix: prefix.to_string(), region: std::env::var("AWS_REGION").unwrap_or_else(|_| "us-east-1".into()) })
    }
}

impl Source for S3Source {
    fn list_splits(&self, input: &str) -> Result<Vec<Split>> {
        let me = if input.starts_with("s3://") { Self::from_uri(input)? } else { self.clone() };
        let rt = tokio::runtime::Runtime::new()?;
        let out: Vec<Split> = rt.block_on(async move {
            let conf = aws_config::load_defaults(aws_config::BehaviorVersion::latest()).await;
            let client = aws_sdk_s3::Client::new(&conf);
            let mut splits = Vec::new();
            let mut cont: Option<String> = None;
            loop {
                let mut req = client.list_objects_v2().bucket(&me.bucket).prefix(&me.prefix);
                if let Some(token) = cont.clone() { req = req.continuation_token(token); }
                let resp = req.send().await.map_err(|e| anyhow::anyhow!(e.to_string()))?;
                for obj in resp.contents() { if let Some(key) = obj.key() { splits.push(Split { uri: format!("s3://{}/{}", me.bucket, key), byte_range: None, meta: None }); } }
                cont = resp.next_continuation_token().map(|s| s.to_string());
                if cont.is_none() { break; }
            }
            Ok::<_, anyhow::Error>(splits)
        })?;
        Ok(out)
    }
    fn open_split(&self, split: &Split) -> Result<Box<dyn Read + Send>> {
        let key_full = split.uri.strip_prefix("s3://").unwrap_or(&split.uri);
        let (_bucket, key) = key_full.split_once('/').unwrap_or((&self.bucket, key_full));
        let rt = tokio::runtime::Runtime::new()?;
        let data = rt.block_on(async move {
            let conf = aws_config::load_defaults(aws_config::BehaviorVersion::latest()).await;
            let client = aws_sdk_s3::Client::new(&conf);
            let resp = client.get_object().bucket(&self.bucket).key(key).send().await.map_err(|e| anyhow::anyhow!(e.to_string()))?;
            let bytes = resp.body.collect().await.map_err(|e| anyhow::anyhow!(e.to_string()))?.into_bytes();
            Ok::<_, anyhow::Error>(bytes)
        })?;
        Ok(Box::new(std::io::Cursor::new(data)))
    }
}

