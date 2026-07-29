#!/usr/bin/env bash
set -euo pipefail

repository_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
input="${1:-$repository_root/target/secondary-index-bench.csv}"

python3 - "$input" "$repository_root" <<'PY'
import csv
import json
import math
import pathlib
import subprocess
import sys

path = pathlib.Path(sys.argv[1])
root = pathlib.Path(sys.argv[2])
groups = {}
with path.open(newline="") as handle:
    for row in csv.DictReader(handle):
        if row["verified"] != "true":
            raise SystemExit(f"unverified benchmark row: {row}")
        groups.setdefault(row["operation"], []).append(float(row["total_ms"]))

def percentile(values, quantile):
    ordered = sorted(values)
    return ordered[max(0, math.ceil(len(ordered) * quantile) - 1)]

if not groups or min(map(len, groups.values())) < 5:
    raise SystemExit("benchmark classification requires at least five samples per operation")

result = {
    "schema": "prolly.secondary-index.benchmark/1",
    "revision": subprocess.check_output(
        ["git", "rev-parse", "HEAD"], cwd=root, text=True
    ).strip(),
    "rustc": subprocess.check_output(["rustc", "--version"], text=True).strip(),
    "operations": {
        name: {
            "samples": len(values),
            "p50_ms": percentile(values, 0.50),
            "p95_ms": percentile(values, 0.95),
            "p99_ms": percentile(values, 0.99),
        }
        for name, values in sorted(groups.items())
    },
}
summary = path.with_suffix(".summary.json")
summary.write_text(json.dumps(result, indent=2, sort_keys=True) + "\n")
print(summary)
PY
