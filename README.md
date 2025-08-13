# Bjorn: A simple, fast MapReduce framework for Rust

Bjorn is a lightweight MapReduce-style framework with an on-disk shuffle that runs:
- Locally on a single machine (default), or across a SLURM cluster (auto-detected)
- With inputs/outputs on local filesystem or S3 (`s3://...`)
- Over common formats: text lines and Parquet rows

In slurm mode, framework assumes that filesystem is shared accross nodes.  

## Quick start
Build all examples and tools:
```bash
cargo build --release --workspace
```

Generate sample data and run benchmarks (local, then SLURM):
```bash
python3 benchmarks/wordcount/benchmark.py --generate --files 5 --lines 50000 --format text
```
This will:
- Generate input under `benchmarks/wordcount/data`
- Run a naive baseline, the Bjorn single-node run, and (optionally) a SLURM run
- Validate results and print timings

Set logging:
```bash
export RUST_LOG=info   # or debug for verbose split listings
```

## API walkthrough

### Minimal MapReduce over text (wordcount)
```rust
struct WordcountMapper;

impl Mapper for WordcountMapper {
    type Input = String;
    type Key = String;
    type Value = u64;

    fn do_map<I, F>(&self, input: I, emit: &mut F)
    where
        I: IntoIterator<Item = Self::Input>,
        F: FnMut(Self::Key, Self::Value),
    {
        let word_re = Regex::new(r"[A-Za-z0-9_']+").unwrap();
        for line in input {
            for cap in word_re.find_iter(&line) {
                let w = cap.as_str().to_lowercase();
                emit(w, 1);
            }
        }
    }
}
```
```rust
struct WordcountReducer;

impl Reducer for WordcountReducer {
    type Key = String;
    type ValueIn = u64;
    type Out = String;

    fn do_reduce<I, F>(&self, key: &Self::Key, values: I, emit: &mut F)
    where
        I: IntoIterator<Item = Self::ValueIn>,
        F: FnMut(Self::Out),
    {
        let sum: u64 = values.into_iter().sum();
        emit(format!("\"{}\"\t{}", key, sum));
    }
}
```
And a minimal end-to-end example:
```rust
fn main() -> Result<()> {
    let args = Args::parse();
    let mut pipeline: RuntimePipeline<String> = RuntimePipeline::new();
    pipeline.add_input_single::<String, _>(&args.input, TextLineFormat);
    pipeline.add_output(&args.output);
    pipeline.map_reduce(WordcountMapper, WordcountReducer, TextLineSink { base: args.output.clone() })?;
    Ok(())
}
```
- Mapper emits `(word, 1)` per token; reducer sums counts and writes TSV lines

Full code available at [examples/wordcount](examples/wordcount/src/main.rs)

### Other examples
- Reading and writing parquet: [link](examples/wordcount_parquet/src/main.rs)
- Pipeline with multiple inputs, join: [link](examples/join/src/main.rs)
- Multi stage pipeline - mapreduce followed by another mapreduce: [link](examples/join_multistage/src/main.rs)

## Running with S3 inputs/outputs
- Use `s3://bucket/prefix` paths for inputs (and outputs when your sink writes to S3)
- AWS credentials are picked up by the official AWS Rust SDK (env vars, instance profile, etc.)
- The runtime logs discovered split URIs at startup; enable `RUST_LOG=debug` for full lists
- S3 listing skips directory markers; reads include simple retries on transient streaming errors

Example (single-node):
```bash
cargo run -p bjorn-wordcount-text --release -- \
  --input s3://my-bucket/wordcount_text/ \
  --output out_wordcount
```

## Running on SLURM
- If launched under slurm, several tasks auto-discover environment (rank, world-size) and communicate.
- See benchmark submit scripts under `benchmarks/**/slurm_submit_*.sbatch` for patterns
- Key envs:
  - `RUST_LOG` to control logging
  - `BJORN_NUM_REDUCERS` to override reducers per process (otherwise derived from `SLURM_CPUS_PER_TASK`)

Example SBATCH (from the repo, wordcount):
```bash
sbatch benchmarks/wordcount/slurm_submit_wordcount.sbatch
```

## Benchmarks
- Wordcount benchmark harness: `benchmarks/wordcount/benchmark.py`
  - Generates data (text or parquet), runs naive vs Bjorn on single-node and SLURM, validates outputs
- Join and join-multistage: `benchmarks/join/benchmark.py`, `benchmarks/join_multistage/benchmark.py`

Run example:
```bash
python3 benchmarks/wordcount/benchmark.py --generate --files 10 --lines 100000 --format text
```

## Nemo JSONL script
A practical 2-stage pipeline used for ranking data generation.
- Source: `scripts/nemo_jsonl/src/main.rs`
- Local run example (multiple inputs supported):
```bash
cargo run -p nemo-jsonl --release -- \
  --input s3://bucket/main1 --input s3://bucket/main2 \
  --output out_nemo \
  --body-colname body_full \
  --doc-input s3://bucket/docs_prefix
```
- SLURM submit script: `scripts/nemo_jsonl/slurm_submit_nemo.sbatch`
```bash
sbatch scripts/nemo_jsonl/slurm_submit_nemo.sbatch \
  --input s3://bucket/main1 --input s3://bucket/main2 \
  --output s3://bucket/out_nemo \
  --doc-input s3://bucket/docs_prefix \
  --body-colname body_full \
  --min-positive-position 4 --max-positive-position 3 --num-negatives 4
```
Notes:
- Two-stage jobs auto-clean their stage1 intermediates (rank 0) after completion
- The doc-side body column is configurable via `--body-colname` (default `body_full`)

## License
MIT
