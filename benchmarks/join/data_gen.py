import argparse
import os
import random
import uuid
import json
from pathlib import Path

WORDS = [
    "lorem", "ipsum", "dolor", "sit", "amet", "consectetur", "adipiscing", "elit",
    "sed", "do", "eiusmod", "tempor", "incididunt", "ut", "labore", "et", "dolore", "magna", "aliqua"
]


def rand_words(n):
    return " ".join(random.choice(WORDS) for _ in range(n))


def gen(args):
    random.seed(42)
    base = Path(args.out)
    searches_dir = base / "input_search"
    pages_dir = base / "input_pages"
    searches_dir.mkdir(parents=True, exist_ok=True)
    pages_dir.mkdir(parents=True, exist_ok=True)

    num_pages = args.pages
    page_ids = [uuid.uuid4() for _ in range(num_pages)]

    # Pages
    page_files = max(1, args.pages // max(1, args.files))
    pf = open(pages_dir / f"pages_0000.jsonl", "w")
    for pid in page_ids:
        body = rand_words(random.randint(1, 64))
        rec = {"page_uuid": str(pid), "body": body}
        pf.write(json.dumps(rec) + "\n")
    pf.close()

    # Searches
    total_searches = args.searches
    per_file = max(1, total_searches // max(1, args.files))
    written = 0
    for i in range(args.files):
        with open(searches_dir / f"searches_{i:04}.jsonl", "w") as f:
            for _ in range(per_file):
                if written >= total_searches:
                    break
                q = rand_words(random.randint(1, 10))
                pid = random.choice(page_ids)
                pos = random.randint(1, 10)
                rec = {"query": q, "page_uuid": str(pid), "position": pos}
                f.write(json.dumps(rec) + "\n")
                written += 1
    print(f"Generated pages={num_pages} searches={written}")


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--out", required=True)
    ap.add_argument("--pages", type=int, default=10000)
    ap.add_argument("--searches", type=int, default=50000)
    ap.add_argument("--files", type=int, default=20)
    args = ap.parse_args()
    gen(args)
