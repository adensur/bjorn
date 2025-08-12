#!/usr/bin/env python3
import argparse
from pathlib import Path

def read_folder(folder: Path):
    results = {}
    for p in sorted(folder.glob('*.txt')):
        with open(p, 'r') as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                # Expect TSV with JSON-quoted key and value
                k, v = line.split('\t', 1)
                key = k.strip().strip('"') if k.startswith('"') else k
                val = int(v)
                results[key] = results.get(key, 0) + val
    return results


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('a')
    ap.add_argument('b')
    args = ap.parse_args()

    A = read_folder(Path(args.a))
    B = read_folder(Path(args.b))

    ok = A == B
    print(f"equal={ok}")
    if not ok:
        diffs = []
        keys = set(A.keys()).union(B.keys())
        for k in sorted(keys):
            if A.get(k, 0) != B.get(k, 0):
                diffs.append((k, A.get(k, 0), B.get(k, 0)))
            if len(diffs) >= 5:
                break
        for k, va, vb in diffs:
            print(f"DIFF\t{k}\t{va}\t{vb}")

if __name__ == '__main__':
    main()
