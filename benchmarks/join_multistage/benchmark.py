import argparse
import os
import shutil
import subprocess
import sys
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]


def run(cmd, cwd=None, env=None):
    print("$", " ".join(cmd))
    t0 = time.time()
    subprocess.check_call(cmd, cwd=cwd, env=env)
    return time.time() - t0


def main(args):
    base = Path(args.base).resolve()
    inp_search = base / "input_search"
    inp_pages = base / "input_pages"
    out_naive = base / "out_naive"
    out_single = base / "out_single"
    out_multi = base / "out_multi"
    for d in [out_naive, out_single, out_multi]:
        if d.exists():
            shutil.rmtree(d)
        d.mkdir(parents=True, exist_ok=True)
    # Clear previous intermediates
    bjorn_runs = ROOT / '.bjorn_runs'
    if bjorn_runs.exists():
        shutil.rmtree(bjorn_runs)

    if args.generate:
        t_gen = run([sys.executable, str(ROOT / "benchmarks/join/data_gen.py"), "--out", str(base), "--pages", str(args.pages), "--searches", str(args.searches), "--files", str(args.files)])
        print(f"Generated data in {t_gen:.2f}s")

    # Build
    run(["cargo", "build", "--release", "--workspace"], cwd=str(ROOT))

    # Naive
    t_naive = run([str(ROOT / "target/release/naive-join-multistage-text"), "--input1", str(inp_search), "--input2", str(inp_pages), "--output", str(out_naive)])

    # Single node (multistage MR in one binary)
    env_single = os.environ.copy()
    if 'SLURM_JOB_ID' in env_single:
        env_single.pop('SLURM_JOB_ID')
    t_single = run([str(ROOT / "target/release/bjorn-join-multistage-text"), "--input1", str(inp_search), "--input2", str(inp_pages), "--output", str(out_single)], env=env_single)

    # Multi-node via Slurm (optional)
    t_multi = None
    if not args.skip_multi:
        env = os.environ.copy()
        env["INPUT_DIR1"] = str(inp_search)
        env["INPUT_DIR2"] = str(inp_pages)
        env["OUTPUT_DIR"] = str(out_multi)
        env["BIN_MULTI"] = str(ROOT / "target/release/bjorn-join-multistage-text")
        t_multi = run(["sbatch", "--wait", str(Path(__file__).parent / "slurm_submit_join_multistage.sbatch")], env=env)

    # Validate
    run([sys.executable, str(Path(__file__).parent / "validate_outputs.py"), str(out_naive), str(out_single)])
    if not args.skip_multi:
        run([sys.executable, str(Path(__file__).parent / "validate_outputs.py"), str(out_naive), str(out_multi)])

    print('\n--- Timings (seconds) ---')
    print(f"naive={t_naive:.2f}")
    print(f"single-node={t_single:.2f}")
    if t_multi is not None:
        print(f"multi-node={t_multi:.2f}")


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--base", default=str(Path(__file__).parent / "data"))
    ap.add_argument("--generate", action="store_true")
    ap.add_argument("--pages", type=int, default=10000)
    ap.add_argument("--searches", type=int, default=50000)
    ap.add_argument("--files", type=int, default=20)
    ap.add_argument("--skip-multi", action="store_true")
    args = ap.parse_args()
    main(args)
