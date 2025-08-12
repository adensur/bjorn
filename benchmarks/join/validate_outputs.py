import argparse
import json
import os
from pathlib import Path

def read_jsonl_dir(d):
    rows = []
    for p in sorted(Path(d).glob("*.txt")):
        with open(p) as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                rows.append(json.loads(line))
    return rows

if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("naive")
    ap.add_argument("candidate")
    args = ap.parse_args()

    a = read_jsonl_dir(args.naive)
    b = read_jsonl_dir(args.candidate)
    a.sort(key=lambda r: (r["page_uuid"], r["position"], r["query"]))
    b.sort(key=lambda r: (r["page_uuid"], r["position"], r["query"]))

    assert len(a) == len(b), f"count mismatch {len(a)} vs {len(b)}"
    for i, (ra, rb) in enumerate(zip(a, b)):
        assert ra == rb, f"row mismatch at {i}: {ra} vs {rb}"
    print(f"OK: {len(a)} rows match")
