use crate::io::read_bin_line;
use anyhow::Result;
use memmap2::Mmap;
use rayon::prelude::*;
use std::io::Write;
use std::time::{Duration, Instant};

#[derive(Clone, Debug)]
pub struct SortOutcome {
    pub lines_in: u64,
    pub bytes_in: u64,
    pub sort_only_ms: u64,
    pub io_read_ms: u64,
    pub io_write_ms: u64,
}

// Given set of partition files, produce a single sorted-by-key file at out_path.
// Intermediate file format is binary records: [klen][vlen][k][v]
pub fn external_sort_by_key(input_paths: &[String], out_path: &str) -> Result<SortOutcome> {
    // Mmap each input file and build record index: (file_idx, start, key_end, end)
    let mut file_maps: Vec<Mmap> = Vec::new();
    let mut all_lines: Vec<(usize, usize, usize, usize)> = Vec::new();
    let mut bytes_in: u64 = 0;
    let mut lines_in: u64 = 0;
    let mut io_read = Duration::from_nanos(0);

    for p in input_paths {
        match std::fs::File::open(&p) {
            Ok(file) => {
                let meta_len = file.metadata().ok().map(|m| m.len()).unwrap_or(0);
                bytes_in += meta_len as u64;
                let read_start = Instant::now();
                match unsafe { Mmap::map(&file) } {
                    Ok(map) => {
                        let file_idx = file_maps.len();
                        let bytes = &map[..];
                        let mut off = 0usize;
                        while let Some((k, _v, next)) = read_bin_line(bytes, off) {
                            let key_start = off + 8; // after 2 u32 lengths
                            let key_end = key_start + k.len();
                            let end = next;
                            all_lines.push((file_idx, key_start - 8, key_end, end));
                            lines_in += 1;
                            off = next;
                        }
                        file_maps.push(map);
                    }
                    Err(_e) => {}
                }
                io_read += read_start.elapsed();
            }
            Err(_e) => {}
        }
    }

    let sort_only_start = Instant::now();
    all_lines.par_sort_by(|a, b| {
        let (fia, sa, ka, _ea) = *a;
        let (fib, sb, kb, _eb) = *b;
        // Keys are at [sa+8..ka] because [sa..sa+8) holds the lengths
        file_maps[fia][(sa+8)..ka].cmp(&file_maps[fib][(sb+8)..kb])
    });
    let sort_only_ms = sort_only_start.elapsed().as_millis() as u64;

    let io_write_start = Instant::now();
    match std::fs::File::create(&out_path) {
        Ok(file) => {
            let mut w = std::io::BufWriter::with_capacity(32 * 1024 * 1024, file);
            for &(fi, s, _k, e) in &all_lines {
                if let Err(_e2) = w.write_all(&file_maps[fi][s..e]) { break; }
            }
            let _ = w.flush();
        }
        Err(_e) => {}
    }
    let io_write_ms = io_write_start.elapsed().as_millis() as u64;

    Ok(SortOutcome { lines_in, bytes_in, sort_only_ms, io_read_ms: io_read.as_millis() as u64, io_write_ms })
}
