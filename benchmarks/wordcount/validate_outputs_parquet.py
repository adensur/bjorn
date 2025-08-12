#!/usr/bin/env python3
import argparse
from pathlib import Path

try:
    import pyarrow as pa  # type: ignore
    import pyarrow.parquet as pq  # type: ignore
except Exception as e:
    raise SystemExit("Parquet validation requires pyarrow to be installed")


def read_folder(folder: Path):
    results = {}
    for p in sorted(folder.glob('*.parquet')):
        table = pq.read_table(p)
        cols = set(table.column_names)
        if {'word', 'count'} <= cols:
            words = table['word'].to_pylist()
            counts = table['count'].to_pylist()
            for w, c in zip(words, counts):
                # normalize to str (pyarrow may yield bytes under BINARY(UTF8) depending on versions)
                ws = w.decode('utf-8') if isinstance(w, (bytes, bytearray)) else str(w)
                results[ws] = results.get(ws, 0) + int(c)
        else:
            # fallback try infer two columns
            if len(table.column_names) >= 2:
                words = table.column(0).to_pylist()
                counts = table.column(1).to_pylist()
                for w, c in zip(words, counts):
                    ws = w.decode('utf-8') if isinstance(w, (bytes, bytearray)) else str(w)
                    results[ws] = results.get(ws, 0) + int(c)
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
