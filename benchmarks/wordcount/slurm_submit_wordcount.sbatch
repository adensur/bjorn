#!/bin/bash
#SBATCH --job-name=bjorn_wcount
#SBATCH --output=bjorn_wcount_%j.out
#SBATCH --error=bjorn_wcount_%j.err
#SBATCH --ntasks=4
#SBATCH --cpus-per-task=96
#SBATCH --mem=0

set -euo pipefail

BIN_WORDCOUNT=${BIN_WORDCOUNT:-target/release/bjorn-wordcount}
INPUT_DIR=${INPUT_DIR:?set INPUT_DIR}
OUTPUT_DIR=${OUTPUT_DIR:?set OUTPUT_DIR}

srun -n ${SLURM_NTASKS} ${BIN_WORDCOUNT} --input "$INPUT_DIR" --output "$OUTPUT_DIR"
