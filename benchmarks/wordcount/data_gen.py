#!/usr/bin/env python3
import argparse
import os
import random
from pathlib import Path
from typing import Optional

from tqdm import tqdm  # assume installed; no fallbacks

WORD_POOL = [
    "apple", "banana", "carrot", "delta", "echo", "foxtrot",
    "golf", "hotel", "india", "juliet", "kilo", "lima",
    "mango", "nectarine", "orange", "papaya", "quartz", "raspberry",
    "strawberry", "tomato", "ugli", "vanilla", "watermelon", "xigua",
    "yam", "zucchini"
]

def gen_line(rng: random.Random, num_words: int) -> str:
    return " ".join(rng.choice(WORD_POOL) for _ in range(num_words))


def write_one_file(idx: int, out_dir: str, num_lines: int, words_per_line: int, seed: Optional[int], buffer_lines: int, show_inner_progress: bool = False):
    # Each worker uses its own RNG to avoid contention and ensure determinism if seed is set
    rng = random.Random((seed + idx) if seed is not None else None)
    path = Path(out_dir) / f"data_{idx:05}.txt"
    # Write in chunks to reduce syscall overhead
    lines_written = 0
    bar = None
    if show_inner_progress:
        bar = tqdm(total=num_lines, desc=f"file {idx:05}", leave=False)
    try:
        with open(path, "w") as f:
            buf = []
            for _ in range(num_lines):
                buf.append(gen_line(rng, words_per_line))
                if len(buf) >= buffer_lines:
                    f.write("\n".join(buf) + "\n")
                    lines_written += len(buf)
                    if bar is not None:
                        bar.update(len(buf))
                    buf.clear()
            if buf:
                f.write("\n".join(buf) + "\n")
                lines_written += len(buf)
                if bar is not None:
                    bar.update(len(buf))
    finally:
        if bar is not None:
            bar.close()
    return lines_written


def _worker_star(params):
    return write_one_file(*params)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--out", required=True)
    ap.add_argument("--files", type=int, default=10)
    ap.add_argument("--lines", type=int, default=10000)
    ap.add_argument("--words-per-line", type=int, default=50)
    ap.add_argument("--jobs", type=int, default=32, help="Number of parallel worker processes")
    ap.add_argument("--format", choices=["text", "parquet"], default="text")
    ap.add_argument("--seed", type=int, default=None, help="Base RNG seed for reproducible data")
    ap.add_argument("--buffer-lines", type=int, default=10000, help="Flush to disk every N lines (per file)")
    ap.add_argument("--progress-per-file", action="store_true", help="Show per-file progress bars (when jobs=1)")
    args = ap.parse_args()

    out = Path(args.out)
    out.mkdir(parents=True, exist_ok=True)

    total_lines = args.files * args.lines

    if args.format == "text":
        # Single-process with optional per-file progress
        if args.jobs <= 1:
            outer = tqdm(total=total_lines, desc="generating", unit="lines")
            for i in range(args.files):
                written = write_one_file(
                    i, str(out), args.lines, args.words_per_line, args.seed, args.buffer_lines, args.progress_per_file
                )
                outer.update(written)
            outer.close()
            return

    # Multi-process: progress is tracked at the granularity of files completed
    from multiprocessing import Pool

    if args.format == "text":
        tasks = [
            (i, str(out), args.lines, args.words_per_line, args.seed, args.buffer_lines, False)
            for i in range(args.files)
        ]

        progress_bar = tqdm(total=total_lines, desc="generating", unit="lines")

        with Pool(processes=args.jobs) as pool:
            # Each completed file accounts for args.lines lines
            for _ in pool.imap_unordered(_worker_star, tasks, chunksize=1):
                progress_bar.update(args.lines)
        progress_bar.close()
    else:
        # parquet generation in Python requires pyarrow
        try:
            import pyarrow as pa  # type: ignore
            import pyarrow.parquet as pq  # type: ignore
        except Exception as e:
            raise RuntimeError("Parquet generation requires pyarrow to be installed") from e

        rng = random.Random(args.seed)
        for i in range(args.files):
            lines = [gen_line(rng, args.words_per_line) for _ in range(args.lines)]
            table = pa.table({"line": pa.array(lines, type=pa.string())})
            pq.write_table(table, out / f"data_{i:05}.parquet")


if __name__ == "__main__":
    main()
