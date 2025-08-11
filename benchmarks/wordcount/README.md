### Wordcount benchmarking (Bjorn)

This folder contains scripts to generate data, run the naive baseline, run the Bjorn MapReduce example (single-node and Slurm multi-node), validate results, and report timings.

### Prerequisites
- Build release binaries first (recommended for benchmarking):
  ```bash
  cargo build --release --workspace
  ```
- Ensure Python 3 is available.
- For multi-node runs, ensure Slurm is installed and available (`sbatch`, `srun`).

### End-to-end (recommended)
Runs data generation (optional), naive baseline, single-node Bjorn, and Slurm multi-node Bjorn (by default), then validates outputs and prints timings.

```bash
# From repo root
python3 benchmarks/wordcount/benchmark.py \
  --generate \
  --files 20 \
  --lines 20000 \
  --words-per-line 50
```
Options:
- `--generate`: generate synthetic data under `benchmarks/wordcount/data`
- `--files`, `--lines`, `--words-per-line`: data parameters
- `--input <dir>`: use an existing input dir instead of generating
- `--skip-multi`: skip Slurm multi-node stage

Outputs:
- Naive: `benchmarks/wordcount/out_naive`
- Single-node: `benchmarks/wordcount/out_single`
- Multi-node: `benchmarks/wordcount/out_multi`

### Run stages separately

#### 1) Data generation
```bash
python3 benchmarks/wordcount/data_gen.py \
  --out benchmarks/wordcount/data \
  --files 20 \
  --lines 20000 \
  --words-per-line 50
```

#### 2) Naive baseline
```bash
./naive_examples/wordcount/target/release/naive-wordcount \
  --input benchmarks/wordcount/data \
  --output benchmarks/wordcount/out_naive
```

#### 3) Bjorn single-node (no Slurm)
```bash
./examples/wordcount/target/release/bjorn-wordcount \
  --input benchmarks/wordcount/data \
  --output benchmarks/wordcount/out_single
```

#### 4) Bjorn multi-node via Slurm
Set environment variables and submit the Slurm job:
```bash
export INPUT_DIR=$(pwd)/benchmarks/wordcount/data
export OUTPUT_DIR=$(pwd)/benchmarks/wordcount/out_multi
export BIN_WORDCOUNT=$(pwd)/examples/wordcount/target/release/bjorn-wordcount

sbatch --wait benchmarks/wordcount/slurm_submit_wordcount.sbatch
```
Notes:
- The submission file uses `#SBATCH --ntasks=4` by default. Adjust to your cluster/resources.
- Each Slurm task acts as a mapper and reducer id. Intermediate state lives under `.bjorn_runs/$SLURM_JOB_ID/`.

#### 5) Validate outputs
Compare two result folders (e.g., naive vs single-node, naive vs multi-node):
```bash
python3 benchmarks/wordcount/validate_outputs.py \
  benchmarks/wordcount/out_naive \
  benchmarks/wordcount/out_single

python3 benchmarks/wordcount/validate_outputs.py \
  benchmarks/wordcount/out_naive \
  benchmarks/wordcount/out_multi
```

### Notes
- Keys/values are JSON-encoded in TSV lines. Validation script aggregates partitions and checks equality.
- To start fresh, delete previous outputs and run again.
