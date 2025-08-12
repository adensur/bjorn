use anyhow::{Context, Result};
use serde::{Serialize, de::DeserializeOwned};
use std::fs::{self, File};
use std::hash::{Hasher, Hash};
use std::io::{BufRead, BufReader, BufWriter, Write};
use std::path::{Path, PathBuf};
use std::collections::hash_map::DefaultHasher;
use std::io::Read;
use std::collections::BTreeMap;

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

// ============== Pluggable sinks (generic over output record type) ==============

pub trait PartitionWriter<Out>: Send {
    fn write(&mut self, record: &Out) -> Result<()>;
}

pub trait Sink<Out>: Send + Sync + 'static {
    type Writer: PartitionWriter<Out> + Send;
    fn open_partition(&self, partition_index: usize) -> Result<Self::Writer>;
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
        // Read entire reader and require valid UTF-8 to avoid tokenizing binary files
        let mut buf = Vec::new();
        let mut rd = BufReader::new(r);
        std::io::Read::read_to_end(&mut rd, &mut buf)?;
        match String::from_utf8(buf) {
            Ok(s) => {
                let lines: Vec<String> = s.lines().map(|l| l.to_owned()).collect();
                Ok(Box::new(lines.into_iter()))
            }
            Err(_) => {
                // Non-UTF8: treat as empty iterator
                Ok(Box::new(std::iter::empty::<String>()))
            }
        }
    }
}

// ============== Parquet dynamic row support ==============

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, PartialEq, PartialOrd)]
pub enum ParquetValue {
    Null,
    Bool(bool),
    Int64(i64),
    Double(f64),
    Bytes(Vec<u8>),
    String(String),
    TimestampMicros(i64),
    List(Vec<ParquetValue>),
    Map(BTreeMap<String, ParquetValue>),
}

pub type ParquetRow = BTreeMap<String, ParquetValue>;

pub struct ParquetFormat;

impl Format<ParquetRow> for ParquetFormat {
    fn decode<'a>(&self, mut r: Box<dyn Read + Send + 'a>) -> Result<Box<dyn Iterator<Item = ParquetRow> + Send + 'a>> {
        // Minimal implementation: read into memory and use Bytes for parquet reader
        let mut buf = Vec::new();
        std::io::Read::read_to_end(&mut *r, &mut buf)?;
        let bytes = bytes::Bytes::from(buf);
        let reader = parquet::file::serialized_reader::SerializedFileReader::new(bytes)
            .map_err(|e| anyhow::anyhow!(e.to_string()))?;
        let boxed: Box<dyn parquet::file::reader::FileReader> = Box::new(reader);
        let iter = parquet::record::reader::RowIter::from_file_into(boxed);
        let rows: Vec<ParquetRow> = iter.map(|row_res| row_to_pqrow(&row_res.unwrap())).collect();
        Ok(Box::new(rows.into_iter()))
    }
}

/// Trait implemented by user static types to convert into a dynamic ParquetRow.
pub trait ParquetRecord {
    fn to_row(&self) -> ParquetRow;
}

fn row_to_pqrow(row: &parquet::record::Row) -> ParquetRow {
    let mut out: ParquetRow = BTreeMap::new();
    for (name, field) in row.get_column_iter() {
        out.insert(name.to_string(), field_to_pqval(field));
    }
    out
}

fn field_to_pqval(field: &parquet::record::Field) -> ParquetValue {
    use parquet::record::Field as F;
    match field {
        F::Null => ParquetValue::Null,
        F::Bool(b) => ParquetValue::Bool(*b),
        F::Byte(b) => ParquetValue::Int64(*b as i64),
        F::Short(s) => ParquetValue::Int64(*s as i64),
        F::Int(i) => ParquetValue::Int64(*i as i64),
        F::Long(l) => ParquetValue::Int64(*l),
        F::UByte(b) => ParquetValue::Int64(*b as i64),
        F::UShort(s) => ParquetValue::Int64(*s as i64),
        F::UInt(i) => ParquetValue::Int64(*i as i64),
        F::ULong(l) => ParquetValue::Int64(*l as i64),
        F::Float(f) => ParquetValue::Double(*f as f64),
        F::Double(d) => ParquetValue::Double(*d),
        F::Bytes(b) => ParquetValue::Bytes(b.data().to_vec()),
        F::Str(s) => ParquetValue::String(s.clone()),
        F::TimestampMicros(v) => ParquetValue::TimestampMicros(*v),
        F::ListInternal(_list) => ParquetValue::List(Vec::new()),
        F::MapInternal(_map) => ParquetValue::Map(BTreeMap::new()),
        F::Group(g) => {
            let mut m = BTreeMap::new();
            for (name, field) in g.get_column_iter() { m.insert(name.to_string(), field_to_pqval(field)); }
            ParquetValue::Map(m)
        }
        _ => ParquetValue::Null,
    }
}

/// Text line sink: expects reducer to emit a `String` per record. Writes one line per record.
pub struct TextLineSink { pub base: String }

pub struct TextLineWriter { inner: BufWriter<File> }

impl PartitionWriter<String> for TextLineWriter {
    fn write(&mut self, record: &String) -> Result<()> {
        writeln!(self.inner, "{}", record).map_err(anyhow::Error::from)
    }
}

// (removed specialized (String, i64) Parquet sink in favor of generic ParquetRowSink)

// ============== Generic Parquet sink for ParquetRow (flat schema, explicit types) ==============
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ParquetFieldType { Bool, Int64, Double, String, Bytes, TimestampMicros }

pub struct ParquetRowSink { pub base: String, pub schema: BTreeMap<String, ParquetFieldType> }

pub struct ParquetRowWriter { path: String, schema: BTreeMap<String, ParquetFieldType>, rows: Vec<ParquetRow> }

impl PartitionWriter<ParquetRow> for ParquetRowWriter {
    fn write(&mut self, record: &ParquetRow) -> Result<()> {
        self.rows.push(record.clone());
        Ok(())
    }
}

impl Drop for ParquetRowWriter {
    fn drop(&mut self) {
        if let Err(e) = write_parquet_rows(&self.path, &self.schema, &self.rows) {
            eprintln!("ParquetRowWriter flush error: {}", e);
        }
    }
}

impl Sink<ParquetRow> for ParquetRowSink {
    type Writer = ParquetRowWriter;
    fn open_partition(&self, partition_index: usize) -> Result<Self::Writer> {
        ensure_dir(&self.base)?;
        let path = format!("{}/part-{:05}.parquet", self.base, partition_index);
        Ok(ParquetRowWriter { path, schema: self.schema.clone(), rows: Vec::new() })
    }
}

fn write_parquet_rows(path: &str, schema_map: &BTreeMap<String, ParquetFieldType>, rows: &[ParquetRow]) -> Result<()> {
    use parquet::basic::Compression;
    use parquet::data_type::ByteArray;
    use parquet::file::properties::WriterProperties;
    use parquet::file::writer::SerializedFileWriter;
    use parquet::schema::parser::parse_message_type;

    // Build schema message with OPTIONAL fields from provided schema
    let mut fields = String::new();
    for (name, ct) in schema_map.iter() {
        let (phys, annot): (&str, Option<&str>) = match ct {
            ParquetFieldType::Bool => ("BOOLEAN", None),
            ParquetFieldType::Int64 => ("INT64", None),
            ParquetFieldType::Double => ("DOUBLE", None),
            ParquetFieldType::String => ("BINARY", Some("(UTF8)")),
            ParquetFieldType::Bytes => ("BINARY", None),
            ParquetFieldType::TimestampMicros => ("INT64", None),
        };
        match annot {
            Some(a) => fields.push_str(&format!(" OPTIONAL {} {} {} ;", phys, name, a)),
            None => fields.push_str(&format!(" OPTIONAL {} {} ;", phys, name)),
        }
    }
    let schema_str = format!("message schema {{{}}}", fields);
    let schema = parse_message_type(&schema_str).map_err(|e| anyhow::anyhow!(e.to_string()))?;

    // Open writer
    let props = WriterProperties::builder().set_compression(Compression::SNAPPY).build();
    let file = std::fs::File::create(path)?;
    let mut writer = SerializedFileWriter::new(file, std::sync::Arc::new(schema), std::sync::Arc::new(props))?;

    let mut row_group = writer.next_row_group()?;
    // For each column, materialize values + def levels
    for (name, ct) in schema_map.iter() {
        if let Some(mut col_writer) = row_group.next_column()? {
            match ct {
                ParquetFieldType::Bool => {
                    let mut values: Vec<bool> = Vec::new();
                    let mut def: Vec<i16> = Vec::new();
                    for row in rows {
                        match row.get(name) {
                            Some(ParquetValue::Bool(b)) => { values.push(*b); def.push(1); }
                            Some(ParquetValue::Null) | None => { def.push(0); }
                            _ => return Err(anyhow::anyhow!("column {} type mismatch", name)),
                        }
                    }
                    let typed = col_writer.typed::<parquet::data_type::BoolType>();
                    typed.write_batch(&values, Some(&def), None)?;
                }
                ParquetFieldType::Int64 | ParquetFieldType::TimestampMicros => {
                    let mut values: Vec<i64> = Vec::new();
                    let mut def: Vec<i16> = Vec::new();
                    for row in rows {
                        match row.get(name) {
                            Some(ParquetValue::Int64(v)) => { values.push(*v); def.push(1); }
                            Some(ParquetValue::TimestampMicros(v)) => { values.push(*v); def.push(1); }
                            Some(ParquetValue::Null) | None => { def.push(0); }
                            _ => return Err(anyhow::anyhow!("column {} type mismatch", name)),
                        }
                    }
                    let typed = col_writer.typed::<parquet::data_type::Int64Type>();
                    typed.write_batch(&values, Some(&def), None)?;
                }
                ParquetFieldType::Double => {
                    let mut values: Vec<f64> = Vec::new();
                    let mut def: Vec<i16> = Vec::new();
                    for row in rows {
                        match row.get(name) {
                            Some(ParquetValue::Double(v)) => { values.push(*v); def.push(1); }
                            Some(ParquetValue::Int64(v)) => { values.push(*v as f64); def.push(1); }
                            Some(ParquetValue::Null) | None => { def.push(0); }
                            _ => return Err(anyhow::anyhow!("column {} type mismatch", name)),
                        }
                    }
                    let typed = col_writer.typed::<parquet::data_type::DoubleType>();
                    typed.write_batch(&values, Some(&def), None)?;
                }
                ParquetFieldType::String => {
                    let mut values: Vec<ByteArray> = Vec::new();
                    let mut def: Vec<i16> = Vec::new();
                    for row in rows {
                        match row.get(name) {
                            Some(ParquetValue::String(s)) => { values.push(ByteArray::from(s.as_str())); def.push(1); }
                            Some(ParquetValue::Null) | None => { def.push(0); }
                            _ => return Err(anyhow::anyhow!("column {} type mismatch", name)),
                        }
                    }
                    let typed = col_writer.typed::<parquet::data_type::ByteArrayType>();
                    typed.write_batch(&values, Some(&def), None)?;
                }
                ParquetFieldType::Bytes => {
                    let mut values: Vec<ByteArray> = Vec::new();
                    let mut def: Vec<i16> = Vec::new();
                    for row in rows {
                        match row.get(name) {
                            Some(ParquetValue::Bytes(b)) => { values.push(ByteArray::from(b.as_slice())); def.push(1); }
                            Some(ParquetValue::Null) | None => { def.push(0); }
                            _ => return Err(anyhow::anyhow!("column {} type mismatch", name)),
                        }
                    }
                    let typed = col_writer.typed::<parquet::data_type::ByteArrayType>();
                    typed.write_batch(&values, Some(&def), None)?;
                }
            }
            col_writer.close()?;
        }
    }
    row_group.close()?;
    writer.close()?;
    Ok(())
}

impl Sink<String> for TextLineSink {
    type Writer = TextLineWriter;
    fn open_partition(&self, partition_index: usize) -> Result<Self::Writer> {
        let path = format!("{}/part-{:05}.txt", self.base, partition_index);
        let w = open_writer(path)?;
        Ok(TextLineWriter { inner: w })
    }
}

/// List splits for a given input URI by delegating to the appropriate source implementation.
/// Supports local filesystem paths and `s3://` URIs.
pub fn list_splits_for_uri(input: &str) -> Result<Vec<Split>> {
    if input.starts_with("s3://") {
        let src = S3Source::from_uri(input)?;
        src.list_splits(input)
    } else {
        let src = LocalFsSource;
        src.list_splits(input)
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
        Ok(Self { bucket: bucket.to_string(), prefix: prefix.to_string(), region: std::env::var(crate::constants::ENV_AWS_REGION).unwrap_or_else(|_| crate::constants::DEFAULT_AWS_REGION.into()) })
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

/// Open a reader for a given `Split`'s URI by delegating to the appropriate source implementation.
pub fn open_reader_for_split(split: &Split) -> Result<Box<dyn Read + Send>> {
    if split.uri.starts_with("s3://") {
        let src = S3Source::from_uri(&split.uri)?;
        src.open_split(split)
    } else {
        LocalFsSource.open_split(split)
    }
}

// (duplicate removed)

