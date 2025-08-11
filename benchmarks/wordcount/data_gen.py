#!/usr/bin/env python3
import argparse
import os
import random
import string
from pathlib import Path

WORD_POOL = [
    "apple", "banana", "carrot", "delta", "echo", "foxtrot",
    "golf", "hotel", "india", "juliet", "kilo", "lima",
    "mango", "nectarine", "orange", "papaya", "quartz", "raspberry",
    "strawberry", "tomato", "ugli", "vanilla", "watermelon", "xigua",
    "yam", "zucchini"
]


def gen_line(num_words: int) -> str:
    return " ".join(random.choice(WORD_POOL) for _ in range(num_words))


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--out", required=True)
    ap.add_argument("--files", type=int, default=10)
    ap.add_argument("--lines", type=int, default=10000)
    ap.add_argument("--words-per-line", type=int, default=50)
    args = ap.parse_args()

    out = Path(args.out)
    out.mkdir(parents=True, exist_ok=True)

    for i in range(args.files):
        with open(out / f"data_{i:05}.txt", "w") as f:
            for _ in range(args.lines):
                f.write(gen_line(args.words_per_line) + "\n")


if __name__ == "__main__":
    main()
