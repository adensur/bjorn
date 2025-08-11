use anyhow::{Context, Result};
use clap::Parser;
use memchr::memchr;
use memmap2::Mmap;
use std::fs::File;
use std::io::Write;
use std::time::Instant;

#[derive(Parser, Debug)]
struct Args {
    /// Input TSV file (key\tvalue per line)
    #[arg(long)]
    input: String,
    /// Output TSV file path
    #[arg(long)]
    output: String,
}

fn main() -> Result<()> {
    let args = Args::parse();

    let t0 = Instant::now();
    let f = File::open(&args.input).with_context(|| format!("open {}", &args.input))?;
    let mmap = unsafe { Mmap::map(&f) }.with_context(|| format!("mmap {}", &args.input))?;
    let bytes = &mmap[..];

    // Build line index as (start, end, key_end)
    let mut lines: Vec<(usize, usize, usize)> = Vec::new();
    let mut offset = 0usize;
    let mut lines_in: u64 = 0;
    let bytes_in: u64 = bytes.len() as u64;

    while offset < bytes.len() {
        let nl = memchr(b'\n', &bytes[offset..]).map(|i| offset + i + 1).unwrap_or(bytes.len());
        // Trim trailing newline and optional \r
        let mut end = nl;
        if end > offset && bytes[end - 1] == b'\n' { end -= 1; }
        if end > offset && bytes[end - 1] == b'\r' { end -= 1; }
        if end > offset {
            let key_end = memchr(b'\t', &bytes[offset..end]).map(|i| offset + i).unwrap_or(end);
            lines.push((offset, end, key_end));
            lines_in += 1;
        }
        offset = nl;
    }
    let t_read = t0.elapsed();

    // Sort by key bytes
    let t_sort0 = Instant::now();
    lines.sort_unstable_by(|a, b| bytes[a.0..a.2].cmp(&bytes[b.0..b.2]));
    let t_sort = t_sort0.elapsed();

    // Write out using large BufWriter (fastest in measurements)
    let t_write0 = Instant::now();
    let out = File::create(&args.output).with_context(|| format!("create {}", &args.output))?;
    let mut w = std::io::BufWriter::with_capacity(32 * 1024 * 1024, out);
    for &(s, e, _) in &lines {
        w.write_all(&bytes[s..e])?;
        w.write_all(b"\n")?;
    }
    w.flush()?;
    let t_write = t_write0.elapsed();

    eprintln!(
        "fs_sort: lines={} bytes={} read_ms={} sort_ms={} write_ms={} total_ms={}",
        lines_in,
        bytes_in,
        t_read.as_millis(),
        t_sort.as_millis(),
        t_write.as_millis(),
        t0.elapsed().as_millis()
    );

    Ok(())
}
