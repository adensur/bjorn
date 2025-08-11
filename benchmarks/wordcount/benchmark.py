#!/usr/bin/env python3
import argparse
import os
import shutil
import subprocess
import time
from pathlib import Path

REPO = Path(__file__).resolve().parents[2]
BENCH_ROOT = Path(__file__).resolve().parent


def run(cmd, env=None):
    print("$", " ".join(cmd))
    t0 = time.time()
    subprocess.check_call(cmd, env=env)
    return time.time() - t0


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--generate', action='store_true')
    ap.add_argument('--skip-multi', action='store_true')
    ap.add_argument('--files', type=int, default=20)
    ap.add_argument('--lines', type=int, default=20000)
    ap.add_argument('--words-per-line', type=int, default=50)
    ap.add_argument('--input', default=str(BENCH_ROOT / 'data'))
    args = ap.parse_args()

    data_dir = Path(args.input)
    out_naive = BENCH_ROOT / 'out_naive'
    out_single = BENCH_ROOT / 'out_single'
    out_multi = BENCH_ROOT / 'out_multi'

    for p in [out_naive, out_single, out_multi]:
        if p.exists():
            shutil.rmtree(p)
        p.mkdir(parents=True, exist_ok=True)

    if args.generate:
        gen_args = [
            'python3', str(BENCH_ROOT / 'data_gen.py'),
            '--out', str(data_dir),
            '--files', str(args.files),
            '--lines', str(args.lines),
            '--words-per-line', str(args.words_per_line),
        ]
        t = run(gen_args)
        print(f"Generated data in {t:.2f}s")

    # Assume binaries are pre-built
    # Binaries are built into the workspace root target directory
    naive_bin = REPO / 'target' / 'release' / 'naive-wordcount'
    bjorn_bin = REPO / 'target' / 'release' / 'bjorn-wordcount'

    # Naive
    t_naive = run([str(naive_bin), '--input', str(data_dir), '--output', str(out_naive)])

    # Single-node (no SLURM env)
    env_single = os.environ.copy()
    if 'SLURM_JOB_ID' in env_single:
        env_single.pop('SLURM_JOB_ID')
    t_single = run([str(bjorn_bin), '--input', str(data_dir), '--output', str(out_single)], env=env_single)

    # Multi-node via SLURM
    t_multi = None
    if not args.skip_multi:
        submit = BENCH_ROOT / 'slurm_submit_wordcount.sbatch'
        env = os.environ.copy()
        env['INPUT_DIR'] = str(data_dir)
        env['OUTPUT_DIR'] = str(out_multi)
        env['BIN_WORDCOUNT'] = str(bjorn_bin)
        t_multi = run(['sbatch', '--wait', str(submit)], env=env)

    # Validate
    val = ['python3', str(BENCH_ROOT / 'validate_outputs.py')]
    print("Validating naive vs single-node...")
    subprocess.check_call(val + [str(out_naive), str(out_single)])
    if not args.skip_multi:
        print("Validating naive vs multi-node...")
        subprocess.check_call(val + [str(out_naive), str(out_multi)])

    print('--- Timings (seconds) ---')
    print(f"naive={t_naive:.2f}")
    print(f"single-node={t_single:.2f}")
    if t_multi is not None:
        print(f"multi-node={t_multi:.2f}")


if __name__ == '__main__':
    main()
