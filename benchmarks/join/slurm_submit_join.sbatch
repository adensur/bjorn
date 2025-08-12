#!/bin/bash
#SBATCH --job-name=bjorn_join
#SBATCH --ntasks=4
#SBATCH --cpus-per-task=4
#SBATCH --time=00:30:00
#SBATCH --output=slurm_join_%j.out
#SBATCH --error=slurm_join_%j.err

set -euo pipefail

BIN=${BIN_JOIN:-$(pwd)/examples/join/target/release/bjorn-join-text}
INPUT1=${INPUT_DIR1:?must set INPUT_DIR1}
INPUT2=${INPUT_DIR2:?must set INPUT_DIR2}
OUTPUT=${OUTPUT_DIR:?must set OUTPUT_DIR}

export RUST_LOG=${RUST_LOG:-info}

srun --ntasks=$SLURM_NTASKS --cpus-per-task=$SLURM_CPUS_PER_TASK \
  "$BIN" --input1 "$INPUT1" --input2 "$INPUT2" --output "$OUTPUT"
