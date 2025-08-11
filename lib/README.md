Bjorn - a minimal MapReduce framework for Slurm-backed clusters.

Layout:
- `bjorn` library in `lib/bjorn` exposes `Mapper`, `Reducer`, and a `RuntimePipeline` with `add_input`, `add_output`, and `map_reduce`.
- Wordcount example at `examples/wordcount`.
- Naive baseline at `naive_examples/wordcount`.
- Benchmarks and scripts in `benchmarks/`.

Notes:
- Distributed execution uses Slurm env vars. Each `srun` task performs one of the logical task ids.
- Barriers are file-based in `.bjorn_runs/<SLURM_JOB_ID>/`.
- Output is written as TSV, JSON-encoded key and value per line.
