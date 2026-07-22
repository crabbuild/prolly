#!/usr/bin/env python3
import argparse
import csv
import json
import math
import pathlib
import statistics
import sys
from collections import defaultdict


PAIR_FIELDS = (
    "kind",
    "records",
    "repetition",
    "operation",
    "pattern",
    "cache_state",
)
PARITY_FIELDS = (
    "contract_version",
    "kind",
    "records",
    "repetition",
    "operation",
    "pattern",
    "cache_state",
    "logical_operations",
    "observed_items",
    "result_entries",
    "expected_entries",
    "observed_entries",
    "validated",
)
IMPLEMENTATIONS = {"rust", "dolt-go"}


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", required=True, type=pathlib.Path)
    parser.add_argument("--manifest", required=True, type=pathlib.Path)
    parser.add_argument("--output-dir", required=True, type=pathlib.Path)
    parser.add_argument("--expected-runs", required=True, type=int)
    parser.add_argument("--expected-sizes", required=True)
    parser.add_argument("--expected-cells", type=int, default=None)
    return parser.parse_args()


def load_rows(path):
    rows = []
    for line_number, line in enumerate(path.read_text(encoding="utf-8").splitlines(), 1):
        if not line.strip():
            continue
        try:
            row = json.loads(line)
        except json.JSONDecodeError as error:
            raise ValueError(f"invalid JSON at line {line_number}: {error}") from error
        validate_row(row, line_number)
        rows.append(row)
    if not rows:
        raise ValueError("input has no result rows")
    return rows


def validate_row(row, line_number):
    required = set(PAIR_FIELDS) | set(PARITY_FIELDS) | {
        "implementation",
        "revision",
        "total_ns",
        "ns_per_operation",
        "operations_per_second",
        "db_bytes",
        "wal_bytes",
        "shm_bytes",
        "total_database_bytes",
        "query_strategy",
        "error",
    }
    missing = required - set(row)
    if missing:
        raise ValueError(f"row {line_number} missing fields: {sorted(missing)}")
    if row["implementation"] not in IMPLEMENTATIONS:
        raise ValueError(f"row {line_number} has unknown implementation")
    if row["contract_version"] != "sqlite-scale-v2":
        raise ValueError(f"row {line_number} has wrong contract version")
    if row["kind"] not in {"fixture", "cell"}:
        raise ValueError(f"row {line_number} has wrong kind")
    if row["validated"] is not True or row["error"]:
        raise ValueError(f"row {line_number} is not validated")
    for field in ("records", "repetition", "logical_operations", "observed_items", "expected_entries", "observed_entries", "total_ns"):
        if not isinstance(row[field], int) or row[field] <= 0:
            raise ValueError(f"row {line_number} has invalid {field}")
    for field in ("ns_per_operation", "operations_per_second"):
        if not isinstance(row[field], (int, float)) or not math.isfinite(row[field]) or row[field] < 0:
            raise ValueError(f"row {line_number} has invalid {field}")
    if row["operation"] == "query":
        expected = "native_get_many" if row["implementation"] == "rust" else "repeated_map_get"
        if row["query_strategy"] != expected:
            raise ValueError(f"row {line_number} has wrong query strategy")
    elif row["query_strategy"] is not None:
        raise ValueError(f"row {line_number} has query strategy on non-query row")


def load_manifest(path):
    with path.open(newline="", encoding="utf-8") as handle:
        rows = list(csv.DictReader(handle))
    metrics = {}
    for row in rows:
        if row["exit_status"] != "0" or row["validation"] != "ok":
            raise ValueError(f"failed process in manifest: {row}")
        key = (
            row["implementation"],
            row["kind"],
            int(row["records"]),
            int(row["repetition"]),
            row["operation"],
            row["pattern"],
            row["cache_state"],
        )
        if key in metrics:
            raise ValueError(f"duplicate manifest row: {key}")
        rss = int(row["peak_rss_bytes"])
        if rss <= 0:
            raise ValueError(f"invalid peak RSS for {key}")
        metrics[key] = rss
    return metrics


def pair_rows(rows, manifest, expected_runs, expected_sizes, expected_cells):
    pairs = defaultdict(dict)
    seen_manifest = set()
    for row in rows:
        key = tuple(row[field] for field in PAIR_FIELDS)
        impl = row["implementation"]
        if impl in pairs[key]:
            raise ValueError(f"duplicate result row: {key}/{impl}")
        manifest_key = (impl,) + key
        if manifest_key not in manifest:
            raise ValueError(f"result has no successful process manifest: {manifest_key}")
        row = dict(row)
        row["peak_rss_bytes"] = manifest[manifest_key]
        seen_manifest.add(manifest_key)
        pairs[key][impl] = row
    if seen_manifest != set(manifest):
        raise ValueError("manifest and result row sets differ")
    for key, pair in pairs.items():
        if set(pair) != IMPLEMENTATIONS:
            raise ValueError(f"incomplete language pair: {key}")
        for field in PARITY_FIELDS:
            if pair["rust"][field] != pair["dolt-go"][field]:
                raise ValueError(f"parity mismatch for {key}: {field}")
    observed_sizes = {key[1] for key in pairs}
    if observed_sizes != expected_sizes:
        raise ValueError(f"size matrix mismatch: {observed_sizes} != {expected_sizes}")
    for size in expected_sizes:
        repetitions = {key[2] for key in pairs if key[1] == size}
        if repetitions != set(range(1, expected_runs + 1)):
            raise ValueError(f"repetition matrix mismatch for {size}: {repetitions}")
        for repetition in repetitions:
            fixture_count = sum(1 for key in pairs if key[0] == "fixture" and key[1] == size and key[2] == repetition)
            cell_count = sum(1 for key in pairs if key[0] == "cell" and key[1] == size and key[2] == repetition)
            if fixture_count != 1:
                raise ValueError(f"expected one fixture pair for {size}/{repetition}, found {fixture_count}")
            if expected_cells is not None and cell_count != expected_cells:
                raise ValueError(f"expected {expected_cells} cell pairs for {size}/{repetition}, found {cell_count}")
    return pairs


def median(values):
    return statistics.median(values)


def coefficient_of_variation(values):
    if len(values) < 2 or statistics.mean(values) == 0:
        return 0.0
    return statistics.stdev(values) / statistics.mean(values) * 100


def summaries(pairs):
    groups = defaultdict(lambda: {"rust": [], "dolt-go": []})
    for key, pair in pairs.items():
        group = (key[0], key[1], key[3], key[4], key[5])
        for impl in IMPLEMENTATIONS:
            groups[group][impl].append(pair[impl])
    output = []
    for group, implementations in sorted(groups.items()):
        rust_times = [row["total_ns"] for row in implementations["rust"]]
        go_times = [row["total_ns"] for row in implementations["dolt-go"]]
        rust_median = median(rust_times)
        go_median = median(go_times)
        winner = "rust" if rust_median < go_median else "dolt-go" if go_median < rust_median else "tie"
        output.append({
            "kind": group[0], "records": group[1], "operation": group[2], "pattern": group[3], "cache_state": group[4],
            "runs": len(rust_times), "logical_operations": implementations["rust"][0]["logical_operations"],
            "rust_median_ns": rust_median, "dolt_go_median_ns": go_median,
            "go_over_rust_ratio": go_median / rust_median if rust_median else math.inf,
            "winner": winner, "rust_cv_percent": coefficient_of_variation(rust_times), "dolt_go_cv_percent": coefficient_of_variation(go_times),
            "rust_peak_rss_bytes": median([row["peak_rss_bytes"] for row in implementations["rust"]]),
            "dolt_go_peak_rss_bytes": median([row["peak_rss_bytes"] for row in implementations["dolt-go"]]),
            "rust_database_bytes": median([row["total_database_bytes"] for row in implementations["rust"]]),
            "dolt_go_database_bytes": median([row["total_database_bytes"] for row in implementations["dolt-go"]]),
        })
    return output


def write_results(output_dir, pairs, summary):
    output_dir.mkdir(parents=True, exist_ok=True)
    flat = []
    for pair in pairs.values():
        flat.extend(pair.values())
    flat.sort(key=lambda row: (row["records"], row["repetition"], row["kind"], row["operation"], row["pattern"], row["implementation"]))
    fields = list(flat[0])
    with (output_dir / "results.csv").open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields)
        writer.writeheader(); writer.writerows(flat)
    summary_fields = list(summary[0])
    with (output_dir / "summary.csv").open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=summary_fields)
        writer.writeheader(); writer.writerows(summary)
    lines = [
        "# Dolt Go vs Rust SQLite prolly comparison",
        "",
        f"Validated pairs: {len(pairs)}. All workload counts and logical cardinalities matched.",
        "",
        "| Records | Operation | Pattern | Rust median | Dolt Go median | Go/Rust | Winner |",
        "|---:|---|---|---:|---:|---:|---|",
    ]
    for row in summary:
        lines.append(f"| {row['records']} | {row['operation']} | {row['pattern']} | {row['rust_median_ns']:.0f} ns | {row['dolt_go_median_ns']:.0f} ns | {row['go_over_rust_ratio']:.2f}x | {row['winner']} |")
    lines.extend([
        "", "## Interpretation limits", "",
        "Each implementation uses its native tree encoding and chunking policy; only logical workloads and outcomes are paired.",
        "Rust query rows use native `get_many`; Dolt exposes no map-level multi-get, so its query rows use repeated `Map.Get` calls.",
        "SQLite WAL and `synchronous=NORMAL` match, but runtime caches, allocators, and persisted chunk layouts differ.",
    ])
    (output_dir / "report.md").write_text("\n".join(lines) + "\n", encoding="utf-8")


def main():
    args = parse_args()
    try:
        expected_sizes = {int(value) for value in args.expected_sizes.split(",") if value}
        rows = load_rows(args.input)
        manifest = load_manifest(args.manifest)
        pairs = pair_rows(rows, manifest, args.expected_runs, expected_sizes, args.expected_cells)
        summary = summaries(pairs)
        write_results(args.output_dir, pairs, summary)
    except (OSError, ValueError, KeyError, TypeError) as error:
        print(f"comparison summary failed: {error}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
