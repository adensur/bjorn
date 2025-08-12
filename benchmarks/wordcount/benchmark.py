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


def is_s3_uri(s: str) -> bool:
    return isinstance(s, str) and s.startswith("s3://")


def _aws_cli_available() -> bool:
    return shutil.which("aws") is not None


def _parse_s3_uri(uri: str):
    rest = uri[len("s3://"):]
    if "/" in rest:
        bucket, prefix = rest.split("/", 1)
    else:
        bucket, prefix = rest, ""
    return bucket, prefix


def sync_dir_to_s3(local_dir: Path, s3_uri: str):
    local_dir = Path(local_dir)
    if _aws_cli_available():
        run(["aws", "s3", "sync", str(local_dir), s3_uri, "--only-show-errors"]) 
        return
    # boto3 fallback
    try:
        import boto3  # type: ignore
    except Exception as e:
        raise RuntimeError("S3 upload requires AWS CLI or boto3 to be installed") from e
    bucket, prefix = _parse_s3_uri(s3_uri)
    s3 = boto3.client("s3")
    for p in local_dir.rglob("*"):
        if p.is_file():
            key = "/".join(filter(None, [prefix, p.relative_to(local_dir).as_posix()]))
            s3.upload_file(str(p), bucket, key)


def sync_s3_to_dir(s3_uri: str, local_dir: Path):
    local_dir = Path(local_dir)
    local_dir.mkdir(parents=True, exist_ok=True)
    if _aws_cli_available():
        run(["aws", "s3", "sync", s3_uri, str(local_dir), "--only-show-errors"]) 
        return
    # boto3 fallback
    try:
        import boto3  # type: ignore
    except Exception as e:
        raise RuntimeError("S3 download requires AWS CLI or boto3 to be installed") from e
    bucket, prefix = _parse_s3_uri(s3_uri)
    s3 = boto3.client("s3")
    paginator = s3.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        for obj in page.get("Contents", []):
            key = obj["Key"]
            rel = key[len(prefix):].lstrip("/") if key.startswith(prefix) else key
            dest = local_dir / rel
            dest.parent.mkdir(parents=True, exist_ok=True)
            s3.download_file(bucket, key, str(dest))


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--generate', action='store_true')
    ap.add_argument('--skip-multi', action='store_true')
    ap.add_argument('--files', type=int, default=20)
    ap.add_argument('--lines', type=int, default=20000)
    ap.add_argument('--words-per-line', type=int, default=50)
    ap.add_argument('--input', default=str(BENCH_ROOT / 'data'))
    args = ap.parse_args()

    input_arg = args.input
    input_is_s3 = is_s3_uri(input_arg)

    # Output dirs (always local)
    out_naive = BENCH_ROOT / 'out_naive'
    out_single = BENCH_ROOT / 'out_single'
    out_multi = BENCH_ROOT / 'out_multi'

    for p in [out_naive, out_single, out_multi]:
        if p.exists():
            shutil.rmtree(p)
        p.mkdir(parents=True, exist_ok=True)

    # Determine input locations for each stage
    naive_input_dir: Path
    bjorn_input_for_runs: str

    if input_is_s3:
        s3_uri = input_arg
        # For S3, generate to a local temp dir, then sync to S3. Use local for naive, S3 for Bjorn.
        local_gen_dir = BENCH_ROOT / '_generated_local'
        if local_gen_dir.exists():
            shutil.rmtree(local_gen_dir)
        local_gen_dir.mkdir(parents=True, exist_ok=True)

        if args.generate:
            gen_args = [
                'python3', str(BENCH_ROOT / 'data_gen.py'),
                '--out', str(local_gen_dir),
                '--files', str(args.files),
                '--lines', str(args.lines),
                '--words-per-line', str(args.words_per_line),
            ]
            t = run(gen_args)
            print(f"Generated data locally in {t:.2f}s; syncing to S3 {s3_uri}...")
            sync_dir_to_s3(local_gen_dir, s3_uri)
        else:
            # If not generating, fetch S3 locally for the naive baseline
            print(f"Input is S3; downloading to local cache for naive baseline...")
            sync_s3_to_dir(s3_uri, local_gen_dir)

        naive_input_dir = local_gen_dir
        bjorn_input_for_runs = s3_uri
    else:
        data_dir = Path(input_arg)
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
        naive_input_dir = data_dir
        bjorn_input_for_runs = str(data_dir)

    # Assume binaries are pre-built
    # Binaries are built into the workspace root target directory
    naive_bin = REPO / 'target' / 'release' / 'naive-wordcount'
    bjorn_bin = REPO / 'target' / 'release' / 'bjorn-wordcount'

    # Naive
    t_naive = run([str(naive_bin), '--input', str(naive_input_dir), '--output', str(out_naive)])

    # Single-node (no SLURM env)
    env_single = os.environ.copy()
    if 'SLURM_JOB_ID' in env_single:
        env_single.pop('SLURM_JOB_ID')
    t_single = run([str(bjorn_bin), '--input', bjorn_input_for_runs, '--output', str(out_single)], env=env_single)

    # Multi-node via SLURM
    t_multi = None
    if not args.skip_multi:
        submit = BENCH_ROOT / 'slurm_submit_wordcount.sbatch'
        env = os.environ.copy()
        env['INPUT_DIR'] = bjorn_input_for_runs
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
