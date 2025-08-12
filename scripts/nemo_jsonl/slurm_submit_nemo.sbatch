#!/bin/bash
#SBATCH --job-name=nemo_jsonl
#SBATCH --ntasks=4
#SBATCH --cpus-per-task=96
#SBATCH --time=01:00:00
#SBATCH --output=slurm_nemo_%j.out
#SBATCH --error=slurm_nemo_%j.err

set -euo pipefail

#
# Named CLI args parser
# Supported:
#   --input <uri>   (required)
#   --output <dir>  (required)
#   --doc-input <uri>              (optional)
#   --max-positive-position <int>  (default 3)
#   --min-positive-position <int>  (default 4)
#   --num-negatives <int>          (default 4)
#   --bin <path>                   (default "$PWD/target/release/nemo-jsonl")
#

BIN="$(pwd)/target/release/nemo-jsonl"
INPUT=""
OUTPUT=""
DOC_INPUT=""
MAX_POS=3
MIN_POS=4
NUM_NEGS=4

while [[ $# -gt 0 ]]; do
  case "$1" in
    --input)
      INPUT=${2:?}; shift 2;;
    --output)
      OUTPUT=${2:?}; shift 2;;
    --max-positive-position)
      MAX_POS=${2:?}; shift 2;;
    --min-positive-position)
      MIN_POS=${2:?}; shift 2;;
    --num-negatives)
      NUM_NEGS=${2:?}; shift 2;;
    --doc-input)
      DOC_INPUT=${2:?}; shift 2;;
    --bin)
      BIN=${2:?}; shift 2;;
    --)
      shift; break;;
    *)
      echo "Unknown argument: $1" >&2; exit 2;;
  esac
done

if [[ -z "$INPUT" || -z "$OUTPUT" ]]; then
  echo "Usage: sbatch [sbatch-opts] $0 --input <s3://...|dir> --output <dir> [--max-positive-position N] [--min-positive-position N] [--num-negatives N] [--bin PATH]" >&2
  exit 2
fi

export RUST_LOG=${RUST_LOG:-info}
export RUST_LOG_FORMAT=${RUST_LOG_FORMAT:-plain}

echo "Running with:"
echo "  BIN=$BIN"
echo "  INPUT=$INPUT"
echo "  OUTPUT=$OUTPUT"
if [[ -n "$DOC_INPUT" ]]; then echo "  DOC_INPUT=$DOC_INPUT"; fi
echo "  MAX_POS=$MAX_POS  MIN_POS=$MIN_POS  NUM_NEGS=$NUM_NEGS"
echo "  SLURM_NTASKS=$SLURM_NTASKS  SLURM_CPUS_PER_TASK=$SLURM_CPUS_PER_TASK"

SINFO=$(scontrol show job $SLURM_JOB_ID 2>/dev/null || true)
echo "$SINFO"

srun --ntasks=$SLURM_NTASKS --cpus-per-task=$SLURM_CPUS_PER_TASK --mpi=none \
  "$BIN" \
  --input "$INPUT" \
  --output "$OUTPUT" \
  ${DOC_INPUT:+--doc-input "$DOC_INPUT"} \
  --max-positive-position "$MAX_POS" \
  --min-positive-position "$MIN_POS" \
  --num-negatives "$NUM_NEGS"
