#!/usr/bin/env python3
"""Validate raw client samples and emit deterministic percentile evidence."""

import argparse
import csv
import json
import math
from collections import defaultdict
from pathlib import Path

SCHEMA = "versioned-dynamodb-client-samples-v2"
FIELDS = (
    "operation",
    "cache_mode",
    "samples",
    "p50_latency_ns",
    "p95_latency_ns",
    "p99_latency_ns",
    "mean_sdk_executions",
    "mean_http_attempts",
    "mean_sdk_retries",
    "mean_request_bytes",
    "mean_response_bytes",
    "mean_transaction_actions",
)


def percentile(values, quantile):
    ordered = sorted(values)
    return ordered[max(0, math.ceil(len(ordered) * quantile) - 1)]


def mean(rows, field):
    return sum(int(row[field]) for row in rows) / len(rows)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", required=True, type=Path)
    parser.add_argument("--output-dir", required=True, type=Path)
    args = parser.parse_args()

    with args.input.open(newline="", encoding="utf-8") as source:
        rows = list(csv.DictReader(source))
    if not rows:
        raise SystemExit("raw sample file is empty")

    groups = defaultdict(list)
    for row in rows:
        if row["schema"] != SCHEMA or row["validated"] != "true":
            raise SystemExit("raw sample schema or validation marker is invalid")
        if int(row["latency_ns"]) <= 0:
            raise SystemExit("latency must be positive")
        executions = int(row["sdk_executions"])
        attempts = int(row["http_attempts"])
        if executions <= 0 or attempts < executions:
            raise SystemExit("physical execution/attempt counters are invalid")
        if int(row["sdk_retries"]) != attempts - executions:
            raise SystemExit("SDK retry derivation is inconsistent")
        if not json.loads(row["api_attempts_json"]):
            raise SystemExit("per-API attempt evidence is empty")
        if row["physical_response_bytes_complete"] != "true":
            raise SystemExit("response-byte evidence is incomplete")
        groups[(row["operation"], row["cache_mode"])].append(row)

    summaries = []
    for (operation, cache_mode), samples in sorted(groups.items()):
        latencies = [int(row["latency_ns"]) for row in samples]
        summaries.append(
            {
                "operation": operation,
                "cache_mode": cache_mode,
                "samples": len(samples),
                "p50_latency_ns": percentile(latencies, 0.50),
                "p95_latency_ns": percentile(latencies, 0.95),
                "p99_latency_ns": percentile(latencies, 0.99),
                "mean_sdk_executions": mean(samples, "sdk_executions"),
                "mean_http_attempts": mean(samples, "http_attempts"),
                "mean_sdk_retries": mean(samples, "sdk_retries"),
                "mean_request_bytes": mean(samples, "physical_request_bytes"),
                "mean_response_bytes": mean(samples, "physical_response_bytes"),
                "mean_transaction_actions": mean(samples, "transaction_actions"),
            }
        )

    args.output_dir.mkdir(parents=True, exist_ok=True)
    with (args.output_dir / "summary.csv").open("w", newline="", encoding="utf-8") as target:
        writer = csv.DictWriter(target, fieldnames=FIELDS)
        writer.writeheader()
        writer.writerows(summaries)

    report = [
        "# Versioned DynamoDB client benchmark",
        "",
        "> DynamoDB Local regression evidence only. These values are not AWS performance, capacity, or cost claims.",
        "",
        "| Operation | Cache | N | p50 ms | p95 ms | p99 ms | HTTP attempts/op | Request bytes/op | Response bytes/op | Tx actions/op |",
        "| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |",
    ]
    for row in summaries:
        report.append(
            f"| {row['operation']} | {row['cache_mode']} | {row['samples']} | "
            f"{row['p50_latency_ns'] / 1_000_000:.3f} | {row['p95_latency_ns'] / 1_000_000:.3f} | "
            f"{row['p99_latency_ns'] / 1_000_000:.3f} | {row['mean_http_attempts']:.2f} | "
            f"{row['mean_request_bytes']:.2f} | {row['mean_response_bytes']:.2f} | "
            f"{row['mean_transaction_actions']:.2f} |"
        )
    report.extend(
        [
            "",
            "This is the expanded client/history/index/blob/admin slice, not the complete release matrix.",
            "See `extensions/dynamodb/client/PERFORMANCE.md` for the open workloads and scale dimensions.",
            "",
        ]
    )
    (args.output_dir / "report.md").write_text("\n".join(report), encoding="utf-8")


if __name__ == "__main__":
    main()
