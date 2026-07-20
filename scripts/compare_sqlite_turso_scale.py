#!/usr/bin/env python3
"""Compare compatible SQLite and Turso scale benchmark baselines."""

import argparse
import csv
import math
import statistics
import sys
from collections import defaultdict
from pathlib import Path


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("--sqlite-dir", type=Path, required=True)
    parser.add_argument("--turso-dir", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    return parser.parse_args()


def read_csv(path):
    try:
        with path.open(newline="", encoding="utf-8") as source:
            return list(csv.DictReader(source))
    except OSError as error:
        raise ValueError(f"failed to read {path}: {error}") from error


def read_manifest(path):
    try:
        lines = path.read_text(encoding="utf-8").splitlines()
    except OSError as error:
        raise ValueError(f"failed to read {path}: {error}") from error
    return dict(line.split("=", 1) for line in lines if "=" in line)


def summary_key(row):
    return (
        int(row["records"]),
        row["operation"],
        row["pattern"],
        row["cache_state"],
    )


def load_summary(directory):
    rows = read_csv(directory / "summary.csv")
    indexed = {summary_key(row): row for row in rows}
    if len(indexed) != len(rows) or not rows:
        raise ValueError(f"summary must contain unique workload cells: {directory}")
    if any(int(row["repetitions"]) <= 0 for row in rows):
        raise ValueError(f"summary contains an invalid repetition count: {directory}")
    return indexed


def validated_fixture_medians(directory):
    rows = [row for row in read_csv(directory / "fixture-results.csv") if row["validated"] == "true"]
    if not rows:
        raise ValueError(f"fixture results contain no validated rows: {directory}")
    return (
        statistics.median(int(row["build_ns"]) for row in rows),
        statistics.median(int(row["database_bytes"]) for row in rows),
        len(rows),
    )


def percent_delta(sqlite_value, turso_value):
    return (turso_value / sqlite_value - 1.0) * 100.0


def geometric_mean(values):
    return math.exp(sum(math.log(value) for value in values) / len(values))


def format_delta(value):
    return f"{value:+.1f}%"


def format_cache_state(value):
    return "n/a" if value == "not-applicable" else value


def build_report(sqlite_dir, turso_dir):
    sqlite = load_summary(sqlite_dir)
    turso = load_summary(turso_dir)
    if sqlite.keys() != turso.keys():
        raise ValueError("SQLite and Turso workload cells differ")

    sqlite_manifest = read_manifest(sqlite_dir / "run-manifest.txt")
    turso_manifest = read_manifest(turso_dir / "run-manifest.txt")
    contract_keys = (
        "sizes",
        "runs",
        "operations",
        "patterns",
        "changes",
        "read_samples",
        "merge_changes_semantics",
        "random_merge_branch_distribution",
        "key_bytes",
        "value_bytes",
        "random_seed",
        "manager_cache",
        "os_cache",
    )
    mismatches = [
        key
        for key in contract_keys
        if sqlite_manifest.get(key) != turso_manifest.get(key)
    ]
    if mismatches:
        raise ValueError(f"workload manifests differ for: {', '.join(mismatches)}")

    cells = []
    by_operation = defaultdict(list)
    for key in sorted(sqlite):
        sqlite_ns = float(sqlite[key]["median_ns_per_operation"])
        turso_ns = float(turso[key]["median_ns_per_operation"])
        if sqlite_ns <= 0 or turso_ns <= 0:
            raise ValueError(f"non-positive median for workload cell: {key}")
        speedup = sqlite_ns / turso_ns
        cells.append((key, sqlite_ns, turso_ns, percent_delta(sqlite_ns, turso_ns), speedup))
        by_operation[key[1]].append(speedup)

    faster = sum(speedup > 1.0 for *_, speedup in cells)
    slower = sum(speedup < 1.0 for *_, speedup in cells)
    tied = len(cells) - faster - slower
    overall = geometric_mean([speedup for *_, speedup in cells])
    sqlite_build, sqlite_bytes, sqlite_fixtures = validated_fixture_medians(sqlite_dir)
    turso_build, turso_bytes, turso_fixtures = validated_fixture_medians(turso_dir)

    lines = [
        "# SQLite versus Turso prolly scale baseline",
        "",
        "Lower latency and higher SQLite/Turso speedup are better for Turso.",
        "",
        "## Outcome",
        "",
        f"Turso is faster in {faster} of {len(cells)} cells; SQLite is faster in {slower}; {tied} are tied. The equally weighted geometric-mean SQLite/Turso latency ratio is **{overall:.3f}x**.",
        "",
        "## Fixture build and storage",
        "",
        "| Metric | SQLite | Turso | Turso delta | SQLite/Turso ratio |",
        "|---|---:|---:|---:|---:|",
        f"| Median build | {sqlite_build / 1e6:.3f} ms | {turso_build / 1e6:.3f} ms | {format_delta(percent_delta(sqlite_build, turso_build))} | {sqlite_build / turso_build:.3f}x |",
        f"| Median database size | {sqlite_bytes / 2**20:.2f} MiB | {turso_bytes / 2**20:.2f} MiB | {format_delta(percent_delta(sqlite_bytes, turso_bytes))} | {sqlite_bytes / turso_bytes:.3f}x |",
        f"| Validated fixtures | {sqlite_fixtures} | {turso_fixtures} | — | — |",
        "",
        "## Geometric mean by operation",
        "",
        "| Operation | SQLite/Turso latency ratio | Winner |",
        "|---|---:|---|",
    ]
    for operation in sorted(by_operation):
        ratio = geometric_mean(by_operation[operation])
        winner = "Turso" if ratio > 1.0 else "SQLite" if ratio < 1.0 else "Tie"
        lines.append(f"| {operation} | {ratio:.3f}x | {winner} |")

    lines.extend(
        [
            "",
            "## Per-cell comparison",
            "",
            "`Turso delta` is `(Turso ns/op ÷ SQLite ns/op) − 1`; negative values favor Turso. `Speedup` is `SQLite ns/op ÷ Turso ns/op`; values above 1 favor Turso.",
            "",
            "| Records | Operation | Pattern | Cache | SQLite ns/op | Turso ns/op | Turso delta | Speedup |",
            "|---:|---|---|---|---:|---:|---:|---:|",
        ]
    )
    for (records, operation, pattern, cache), sqlite_ns, turso_ns, delta, speedup in cells:
        lines.append(
            f"| {records} | {operation} | {pattern} | {format_cache_state(cache)} | {sqlite_ns:.1f} | {turso_ns:.1f} | {format_delta(delta)} | {speedup:.3f}x |"
        )

    sqlite_revision = sqlite_manifest.get("revision", "unknown")
    turso_revision = turso_manifest.get("revision", "unknown")
    lines.extend(
        [
            "",
            "## Comparability and limits",
            "",
            f"- SQLite revision: `{sqlite_revision}` (`dirty={sqlite_manifest.get('dirty', 'unknown')}`); Turso revision: `{turso_revision}` (`dirty={turso_manifest.get('dirty', 'unknown')}`).",
            "- Both runs use the same 1M/30%/10K/three-repetition workload contract, deterministic data, manager-cache rules, M2 Max host, and Rust 1.97 toolchain.",
            "- This is not a strict causal A/B because the recorded revisions differ and both source trees were dirty. Treat it as an indicative backend baseline, not proof that the store adapter alone caused every delta.",
            "- SQLite is synchronous with WAL and `synchronous=NORMAL`; Turso uses local-only async execution with four Tokio workers and no cloud synchronization.",
            "- OS filesystem cache state is uncontrolled. Medians summarize three independent fixtures and do not provide confidence intervals.",
            "",
        ]
    )
    return "\n".join(lines)


def main():
    args = parse_args()
    try:
        report = build_report(args.sqlite_dir, args.turso_dir)
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(report, encoding="utf-8")
    except (KeyError, OSError, ValueError) as error:
        print(f"comparison failed: {error}", file=sys.stderr)
        return 2
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
