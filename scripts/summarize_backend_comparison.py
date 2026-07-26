#!/usr/bin/env python3
"""Compare equivalent PostgreSQL and DynamoDB Local Prolly workloads."""

from __future__ import annotations

import argparse
import csv
import statistics
from collections import defaultdict
from pathlib import Path


COMMON_OPERATIONS = (
    "build",
    "batch",
    "query",
    "concurrent_query",
    "diff",
    "merge",
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--postgres", required=True, type=Path)
    parser.add_argument("--dynamodb", required=True, type=Path)
    parser.add_argument("--output-dir", required=True, type=Path)
    parser.add_argument("--postgres-baseline", type=Path)
    parser.add_argument("--dynamodb-baseline", type=Path)
    return parser.parse_args()


def raw_path(path: Path) -> Path:
    return path / "raw-results.csv" if path.is_dir() else path


def load_postgres(path: Path) -> dict[str, list[dict[str, str]]]:
    grouped: dict[str, list[dict[str, str]]] = defaultdict(list)
    with raw_path(path).open(newline="", encoding="utf-8") as handle:
        for row in csv.DictReader(handle):
            if row["schema"] != "postgres-scale-v1":
                raise ValueError(f"unsupported PostgreSQL schema: {row['schema']}")
            if row["validated"].lower() != "true" or row["error"]:
                raise ValueError(f"invalid PostgreSQL row: {row['operation']}")
            operation = row["operation"]
            if operation not in COMMON_OPERATIONS:
                continue
            expected_pattern = "base" if operation == "build" else "random"
            if row["pattern"] == expected_pattern and row["cache_state"] == "cold-manager":
                grouped[operation].append(row)
    return dict(grouped)


def load_dynamodb(
    path: Path, allow_legacy_schema: bool = False
) -> dict[str, list[dict[str, str]]]:
    grouped: dict[str, list[dict[str, str]]] = defaultdict(list)
    with raw_path(path).open(newline="", encoding="utf-8") as handle:
        for row in csv.DictReader(handle):
            supported = {"dynamodb-local-scale-v5"}
            if allow_legacy_schema:
                supported.add("dynamodb-local-scale-v4")
            if row["schema"] not in supported:
                raise ValueError(f"unsupported DynamoDB schema: {row['schema']}")
            if row["validated"].lower() != "true" or row["error"]:
                raise ValueError(f"invalid DynamoDB row: {row['operation']}")
            if row["operation"] in COMMON_OPERATIONS:
                grouped[row["operation"]].append(row)
    return dict(grouped)


def manifest_value(path: Path, key: str) -> str:
    manifest = path / "run-manifest.txt" if path.is_dir() else path.parent / "run-manifest.txt"
    with manifest.open(encoding="utf-8") as handle:
        values = dict(
            line.rstrip("\n").split("=", 1)
            for line in handle
            if "=" in line
        )
    try:
        return values[key]
    except KeyError as error:
        raise ValueError(f"{manifest} does not declare {key}") from error


def summarize(
    rows: list[dict[str, str]], value_bytes_override: int | None = None
) -> dict[str, float | int]:
    records = {int(row["records"]) for row in rows}
    logical = {int(row["logical_operations"]) for row in rows}
    value_bytes = (
        {value_bytes_override}
        if value_bytes_override is not None
        else {int(row["value_bytes"]) for row in rows}
    )
    if len(records) != 1 or len(logical) != 1 or len(value_bytes) != 1:
        raise ValueError("comparison rows contain mixed workload dimensions")
    return {
        "records": records.pop(),
        "logical_operations": logical.pop(),
        "value_bytes": value_bytes.pop(),
        "runs": len(rows),
        "median_ms": statistics.median(int(row["total_ns"]) for row in rows)
        / 1_000_000,
        "median_ops_per_sec": statistics.median(
            float(row["ops_per_sec"]) for row in rows
        ),
    }


def main() -> None:
    args = parse_args()
    postgres = load_postgres(args.postgres)
    dynamodb = load_dynamodb(args.dynamodb)
    postgres_value_bytes = int(manifest_value(args.postgres, "value_bytes"))
    postgres_concurrency = int(manifest_value(args.postgres, "concurrency"))
    dynamodb_concurrency = {
        int(row["concurrency"])
        for rows in dynamodb.values()
        for row in rows
    }
    if dynamodb_concurrency != {postgres_concurrency}:
        raise ValueError(
            "concurrency differs: "
            f"PostgreSQL={postgres_concurrency}, DynamoDB={sorted(dynamodb_concurrency)}"
        )
    missing = [
        operation
        for operation in COMMON_OPERATIONS
        if operation not in postgres or operation not in dynamodb
    ]
    if missing:
        raise ValueError(f"missing comparable operations: {', '.join(missing)}")

    comparisons = []
    for operation in COMMON_OPERATIONS:
        pg = summarize(postgres[operation], postgres_value_bytes)
        dynamo = summarize(dynamodb[operation])
        for dimension in ("records", "logical_operations", "value_bytes"):
            if pg[dimension] != dynamo[dimension]:
                raise ValueError(
                    f"{operation} {dimension} differs: "
                    f"PostgreSQL={pg[dimension]}, DynamoDB={dynamo[dimension]}"
                )
        comparisons.append(
            {
                "operation": operation,
                "records": pg["records"],
                "value_bytes": pg["value_bytes"],
                "logical_operations": pg["logical_operations"],
                "concurrency": postgres_concurrency,
                "postgres_runs": pg["runs"],
                "dynamodb_runs": dynamo["runs"],
                "postgres_median_ms": pg["median_ms"],
                "dynamodb_median_ms": dynamo["median_ms"],
                "postgres_ops_per_sec": pg["median_ops_per_sec"],
                "dynamodb_ops_per_sec": dynamo["median_ops_per_sec"],
                "dynamodb_to_postgres_latency": dynamo["median_ms"]
                / pg["median_ms"],
            }
        )

    args.output_dir.mkdir(parents=True, exist_ok=True)
    csv_path = args.output_dir / "comparison.csv"
    fields = list(comparisons[0])
    with csv_path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields, lineterminator="\n")
        writer.writeheader()
        for comparison in comparisons:
            writer.writerow(comparison)

    records = comparisons[0]["records"]
    value_bytes = comparisons[0]["value_bytes"]
    lines = [
        "# PostgreSQL vs DynamoDB Local",
        "",
        f"Equivalent public Prolly API workloads over {records:,} records with "
        f"{value_bytes}-byte values and concurrency {postgres_concurrency}. "
        "Times are medians; lower is better.",
        "",
        "| Operation | Logical ops | PostgreSQL | DynamoDB Local | Lower latency |",
        "|---|---:|---:|---:|---:|",
    ]
    for comparison in comparisons:
        ratio = comparison["dynamodb_to_postgres_latency"]
        if 0.99 <= ratio <= 1.01:
            faster = "Within 1%"
        elif ratio >= 1:
            faster = f"PostgreSQL by {ratio:.2f}×"
        else:
            faster = f"DynamoDB Local by {1 / ratio:.2f}×"
        lines.append(
            f"| {comparison['operation']} | "
            f"{comparison['logical_operations']:,} | "
            f"{comparison['postgres_median_ms']:.2f} ms | "
            f"{comparison['dynamodb_median_ms']:.2f} ms | {faster} |"
        )
    lines.extend(
        [
            "",
            "## Interpretation",
            "",
            "- Both adapters use the same record count, value size, operation count, "
            "and cold Prolly manager state.",
            "- Build has one measured sample because constructing the shared fixture is "
            "the measurement; other operations use the configured repetition count.",
            "- DynamoDB Local is useful for repeatable adapter regression testing. It "
            "does not model AWS network latency, partitions, throttling, or capacity.",
            "- PostgreSQL runs in Docker with its own process and buffer cache. These "
            "numbers compare local implementations, not production infrastructure.",
            "",
            "Detailed backend reports are in `postgres/report.md` and "
            "`dynamodb/report.md`.",
        ]
    )
    if args.postgres_baseline or args.dynamodb_baseline:
        lines.extend(
            [
                "",
                "## Change from supplied baselines",
                "",
                "Positive throughput change is an improvement. Merge is omitted when "
                "a legacy DynamoDB baseline used the former double-sized merge workload.",
                "",
                "| Backend | Operation | Baseline | Current | Throughput change |",
                "|---|---|---:|---:|---:|",
            ]
        )
        baseline_sets = []
        if args.postgres_baseline:
            baseline_sets.append(
                (
                    "PostgreSQL",
                    load_postgres(args.postgres_baseline),
                    postgres,
                    postgres_value_bytes,
                )
            )
        if args.dynamodb_baseline:
            baseline_sets.append(
                (
                    "DynamoDB Local",
                    load_dynamodb(args.dynamodb_baseline, allow_legacy_schema=True),
                    dynamodb,
                    None,
                )
            )
        for backend, baseline, current, value_override in baseline_sets:
            for operation in COMMON_OPERATIONS:
                if operation not in baseline or operation not in current:
                    continue
                old = summarize(baseline[operation], value_override)
                new = summarize(current[operation], value_override)
                if (
                    old["records"] != new["records"]
                    or old["logical_operations"] != new["logical_operations"]
                ):
                    continue
                throughput_change = old["median_ms"] / new["median_ms"] - 1
                lines.append(
                    f"| {backend} | {operation} | {old['median_ms']:.2f} ms | "
                    f"{new['median_ms']:.2f} ms | {throughput_change:+.1%} |"
                )
    report_path = args.output_dir / "report.md"
    report_path.write_text("\n".join(lines) + "\n", encoding="utf-8")
    print(f"Backend comparison CSV: {csv_path}")
    print(f"Backend comparison report: {report_path}")


if __name__ == "__main__":
    main()
