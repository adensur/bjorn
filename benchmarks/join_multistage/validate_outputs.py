import argparse
import json
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

    # Normalize docs ordering inside each group (sort by (page_uuid, position))
    def normalize(groups):
        key_to_docs = {}
        for g in groups:
            docs = list(g.get("docs", []))
            for d in docs:
                # keep as-is
                pass
            docs.sort(key=lambda d: (d["page_uuid"], d["position"]))
            key_to_docs[g["query"]] = docs
        return key_to_docs

    na = normalize(a)
    nb = normalize(b)

    assert set(na.keys()) == set(nb.keys()), f"group keys mismatch: {len(na)} vs {len(nb)}"
    for k in sorted(na.keys()):
        assert na[k] == nb[k], f"group {k} mismatch"
    print(f"OK: {len(na)} groups match")
