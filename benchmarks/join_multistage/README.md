### Multistage Join benchmark (Bjorn)

Two-stage pipeline:
- Stage 1: Join searches to pages → per-document JSONL (query, page_uuid, position, body)
- Stage 2: Group by query → JSONL: { query, docs: [{page_uuid, position, body}, ...] }

Usage (recommended end-to-end):

```bash
# From repo root
python3 benchmarks/join_multistage/benchmark.py --generate --pages 10000 --searches 50000 --files 20
```

Validate outputs:
```bash
python3 benchmarks/join_multistage/validate_outputs.py benchmarks/join_multistage/out_naive benchmarks/join_multistage/out_single
```

Run Slurm multi-node:
```bash
export INPUT_DIR1=$(pwd)/benchmarks/join_multistage/data/input_search
export INPUT_DIR2=$(pwd)/benchmarks/join_multistage/data/input_pages
export OUTPUT_DIR=$(pwd)/benchmarks/join_multistage/out_multi
export BIN_MULTI=$(pwd)/target/release/bjorn-join-multistage-text
sbatch --wait benchmarks/join_multistage/slurm_submit_join_multistage.sbatch
```
