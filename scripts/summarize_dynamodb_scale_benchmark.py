#!/usr/bin/env python3
"""Summarize the durable CSV emitted by the DynamoDB Local benchmark."""

from __future__ import annotations

import argparse
import csv
import statistics
from collections import defaultdict
from pathlib import Path


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", required=True, type=Path)
    parser.add_argument("--output-dir", required=True, type=Path)
    parser.add_argument("--baseline", type=Path)
    return parser.parse_args()


def load(path: Path) -> dict[str, list[dict[str, str]]]:
    source = path / "raw-results.csv" if path.is_dir() else path
    grouped: dict[str, list[dict[str, str]]] = defaultdict(list)
    with source.open(newline="", encoding="utf-8") as handle:
        for row in csv.DictReader(handle):
            if row["validated"] != "true" or row["error"]:
                raise ValueError(
                    f"invalid row: {row['operation']} repetition {row['repetition']}"
                )
            grouped[row["operation"]].append(row)
    if not grouped:
        raise ValueError(f"no benchmark rows in {source}")
    return dict(grouped)


def medians(
    grouped: dict[str, list[dict[str, str]]],
) -> dict[str, dict[str, float]]:
    return {
        operation: {
            "runs": float(len(rows)),
            "logical_operations": statistics.median(
                int(row["logical_operations"]) for row in rows
            ),
            "total_ms": statistics.median(
                int(row["total_ns"]) / 1_000_000 for row in rows
            ),
            "ns_per_op": statistics.median(float(row["ns_per_op"]) for row in rows),
            "ops_per_sec": statistics.median(
                float(row["ops_per_sec"]) for row in rows
            ),
        }
        for operation, rows in grouped.items()
    }


def main() -> None:
    args = parse_args()
    args.output_dir.mkdir(parents=True, exist_ok=True)
    current = medians(load(args.input))
    baseline = medians(load(args.baseline)) if args.baseline else {}
    summary_path = args.output_dir / "summary.csv"
    report_path = args.output_dir / "report.md"

    with summary_path.open("w", newline="", encoding="utf-8") as handle:
        fieldnames = [
            "operation",
            "runs",
            "logical_operations",
            "median_total_ms",
            "median_ns_per_op",
            "median_ops_per_sec",
            "baseline_ops_per_sec",
            "speedup",
        ]
        writer = csv.DictWriter(handle, fieldnames=fieldnames, lineterminator="\n")
        writer.writeheader()
        for operation in sorted(current):
            values = current[operation]
            old = baseline.get(operation)
            speedup = values["ops_per_sec"] / old["ops_per_sec"] if old else None
            writer.writerow(
                {
                    "operation": operation,
                    "runs": int(values["runs"]),
                    "logical_operations": int(values["logical_operations"]),
                    "median_total_ms": f"{values['total_ms']:.3f}",
                    "median_ns_per_op": f"{values['ns_per_op']:.3f}",
                    "median_ops_per_sec": f"{values['ops_per_sec']:.3f}",
                    "baseline_ops_per_sec": (
                        f"{old['ops_per_sec']:.3f}" if old else ""
                    ),
                    "speedup": f"{speedup:.3f}" if speedup is not None else "",
                }
            )

    lines = [
        "# DynamoDB Local performance report",
        "",
        "Medians are reported across completed repetitions. DynamoDB Local is a "
        "repeatability tool, not a predictor of AWS DynamoDB capacity or latency.",
        "",
        "| Operation | Median time | Operations/s | Speedup |",
        "|---|---:|---:|---:|",
    ]
    for operation in sorted(current):
        values = current[operation]
        old = baseline.get(operation)
        speedup = values["ops_per_sec"] / old["ops_per_sec"] if old else None
        lines.append(
            f"| {operation} | {values['total_ms']:.2f} ms | "
            f"{values['ops_per_sec']:,.1f} | "
            f"{speedup:.2f}x |" if speedup is not None else
            f"| {operation} | {values['total_ms']:.2f} ms | "
            f"{values['ops_per_sec']:,.1f} | — |"
        )
    report_path.write_text("\n".join(lines) + "\n", encoding="utf-8")
    print(f"DynamoDB summary: {summary_path}")
    print(f"DynamoDB report: {report_path}")


if __name__ == "__main__":
    main()
