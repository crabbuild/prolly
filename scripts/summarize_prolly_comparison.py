#!/usr/bin/env python3
"""Validate and summarize native Dolt-Go versus Rust prolly benchmark rows."""

from __future__ import annotations

import csv
import statistics
import sys
from collections import defaultdict
from pathlib import Path


def fail(message: str) -> None:
    raise SystemExit(f"invalid benchmark results: {message}")


def main() -> None:
    if len(sys.argv) != 3:
        raise SystemExit("usage: summarize_prolly_comparison.py RESULTS.csv OUT_DIR")
    source = Path(sys.argv[1])
    out = Path(sys.argv[2])
    rows = list(csv.DictReader(source.open(newline="", encoding="utf-8")))
    if not rows:
        fail("no measurement rows")

    by_run: dict[tuple[str, ...], dict[str, dict[str, str]]] = defaultdict(dict)
    for row in rows:
        if row["validated"] != "true":
            fail(f"unvalidated row: {row}")
        key = (row["records"], row["phase"], row["workload"], row["operation"], row["run"])
        implementation = row["implementation"]
        if implementation in by_run[key]:
            fail(f"duplicate {implementation} row for {key}")
        by_run[key][implementation] = row

    for key, pair in by_run.items():
        if set(pair) != {"rust", "dolt-go"}:
            fail(f"missing implementation for {key}: {sorted(pair)}")
        rust, go = pair["rust"], pair["dolt-go"]
        for field in ("operations", "workload_digest", "result_count"):
            if rust[field] != go[field]:
                fail(f"{field} mismatch for {key}: rust={rust[field]} go={go[field]}")

    groups: dict[tuple[str, ...], dict[str, list[float]]] = defaultdict(lambda: defaultdict(list))
    for row in rows:
        key = (row["records"], row["phase"], row["workload"], row["operation"])
        groups[key][row["implementation"]].append(float(row["ns_per_op"]))

    summary_rows: list[dict[str, str]] = []
    for key in sorted(groups, key=lambda item: (int(item[0]), item[1], item[2], item[3])):
        values = groups[key]
        rust = statistics.median(values["rust"])
        go = statistics.median(values["dolt-go"])
        winner = "rust" if rust < go else "dolt-go" if go < rust else "tie"
        speedup = max(rust, go) / min(rust, go) if min(rust, go) else float("inf")
        summary_rows.append(
            {
                "records": key[0],
                "phase": key[1],
                "workload": key[2],
                "operation": key[3],
                "runs": str(len(values["rust"])),
                "rust_median_ns_per_op": f"{rust:.3f}",
                "dolt_go_median_ns_per_op": f"{go:.3f}",
                "winner": winner,
                "winner_speedup": f"{speedup:.3f}",
            }
        )

    fields = list(summary_rows[0])
    with (out / "summary.csv").open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields)
        writer.writeheader()
        writer.writerows(summary_rows)

    wins = defaultdict(int)
    operation_wins: dict[str, dict[str, int]] = defaultdict(lambda: defaultdict(int))
    for row in summary_rows:
        wins[row["winner"]] += 1
        operation_wins[row["operation"]][row["winner"]] += 1
    run_counts = [int(row["runs"]) for row in summary_rows]
    report = [
        "# Dolt Go vs Rust Prolly Performance",
        "",
        "All figures are medians from process-isolated, single-worker, in-memory runs. "
        "Lower nanoseconds per operation is better. Workload digests, operation counts, "
        "result cardinalities, point-read values, and scan ordering/cardinality were validated.",
        "",
        f"Rust wins: {wins['rust']}; Dolt Go wins: {wins['dolt-go']}; ties: {wins['tie']}.",
        "",
        "| Operation | Rust wins | Dolt Go wins | Ties |",
        "|---|---:|---:|---:|",
    ]
    for operation in ("write", "point_read", "range_scan"):
        counts = operation_wins[operation]
        report.append(
            f"| {operation} | {counts['rust']} | {counts['dolt-go']} | {counts['tie']} |"
        )
    report.extend([
        "",
        f"Measured repetitions range from {min(run_counts)} to {max(run_counts)} per scenario.",
    ])
    if min(run_counts) < 3:
        report.extend([
            "",
            "> Some scenarios have fewer than three repetitions. Treat their exact speedup "
            "as provisional; the winner is a complete measured pass, not a confidence interval.",
        ])
    report.extend([
        "",
        "| Records | Phase | Workload | Operation | Runs | Rust ns/op | Dolt Go ns/op | Winner | Speedup |",
        "|---:|---|---|---|---:|---:|---:|---|---:|",
    ])
    for row in summary_rows:
        report.append(
            "| {records} | {phase} | {workload} | {operation} | {runs} | "
            "{rust_median_ns_per_op} | {dolt_go_median_ns_per_op} | {winner} | "
            "{winner_speedup}x |".format(**row)
        )
    report.extend(
        [
            "",
            "## Workload contract",
            "",
            "- Dataset sizes: 10K, 50K, 1M, 5M, and 10M base records.",
            "- Keys: fixed-width, zero-padded UTF-8 strings; values: deterministic pseudo-random 1–100 byte payloads.",
            "- Fresh workloads: ascending append order, uniform deterministic permutation, and permuted 1,000-key clusters.",
            "- Mutation workloads: 30% of base size; random and clustered workloads use 50% inserts and 50% updates.",
            "- Each build or mutation workload is submitted as one bulk write; fixture and tuple generation is excluded from write timing.",
            "- Point reads use at most 100,000 validated existing keys after one untimed warm pass.",
            "- Range scans traverse and validate the complete resulting tree.",
            "- Both implementations use their product-default tree encoding and chunking policy.",
            "",
            "Raw process output is retained in `raw/`; `results.csv` contains normalized measurements.",
        ]
    )
    (out / "report.md").write_text("\n".join(report) + "\n", encoding="utf-8")


if __name__ == "__main__":
    main()
