#!/usr/bin/env python3
import argparse
import csv
import math
import pathlib
import statistics


OPERATIONS = ("batch_put", "batch_get", "concurrent_get", "contended_root_cas")
RESAMPLES = 10_000
SEED = 0x243F6A8885A308D3


def manifest(path: pathlib.Path) -> dict[str, str]:
    values: dict[str, str] = {}
    for line in path.read_text().splitlines():
        key, separator, value = line.partition("=")
        if not separator or key in values:
            raise ValueError(f"invalid manifest line: {line}")
        values[key] = value
    required = {
        "status": "complete",
        "resumed": "false",
        "dirty": "false",
        "backend_a": "postgres",
        "backend_b": "mysql",
    }
    for key, expected in required.items():
        if values.get(key) != expected:
            raise ValueError(f"manifest {key} must be {expected}")
    if int(values["repetitions"]) < 7:
        raise ValueError("service comparison requires at least seven repetitions")
    return values


def load(path: pathlib.Path, values: dict[str, str]) -> list[dict[str, str]]:
    with path.open(newline="") as handle:
        rows = list(csv.DictReader(handle))
    expected_count = int(values["repetitions"]) * len(OPERATIONS) * 2
    if len(rows) != expected_count:
        raise ValueError(f"service matrix has {len(rows)} rows, expected {expected_count}")
    indexed: dict[tuple[str, str, int], dict[str, str]] = {}
    for row in rows:
        if (
            row["schema"] != "sql-service-comparison-v1"
            or row["run_id"] != values["run_id"]
            or row["revision"] != values["revision"]
            or row["tree_hash"] != values["tree_hash"]
            or row["validated"] != "true"
            or row["error"]
        ):
            raise ValueError("service row provenance or validation differs")
        backend = row["backend"]
        if backend not in ("postgres", "mysql"):
            raise ValueError(f"unexpected service backend: {backend}")
        if row["binary_sha256"] != values[f"{backend}_binary_sha256"]:
            raise ValueError(f"{backend} service binary identity differs")
        key = (backend, row["operation"], int(row["repetition"]))
        if key in indexed:
            raise ValueError(f"duplicate service row: {key}")
        for field in ("total_ns", "ops_per_sec", "p50_ns", "p95_ns", "p99_ns", "p999_ns", "max_ns"):
            number = float(row[field])
            if not math.isfinite(number) or number <= 0:
                raise ValueError(f"invalid service measurement {field}")
        indexed[key] = row
    return rows


def splitmix64(value: int) -> int:
    value = (value + 0x9E3779B97F4A7C15) & ((1 << 64) - 1)
    mixed = value
    mixed = ((mixed ^ (mixed >> 30)) * 0xBF58476D1CE4E5B9) & ((1 << 64) - 1)
    mixed = ((mixed ^ (mixed >> 27)) * 0x94D049BB133111EB) & ((1 << 64) - 1)
    return mixed ^ (mixed >> 31)


def percentile(values: list[float], quantile: float) -> float:
    ordered = sorted(values)
    rank = max(1, math.ceil(quantile * len(ordered)))
    return ordered[min(rank - 1, len(ordered) - 1)]


def bootstrap_ratio(first: list[float], second: list[float], seed: int) -> tuple[float, float]:
    state = seed
    ratios = []
    for _ in range(RESAMPLES):
        a = []
        b = []
        for _ in first:
            state = splitmix64(state)
            index = state % len(first)
            a.append(first[index])
            b.append(second[index])
        ratios.append(statistics.median(b) / statistics.median(a))
    return percentile(ratios, 0.025), percentile(ratios, 0.975)


def summarize(rows: list[dict[str, str]], repetitions: int) -> list[dict[str, str]]:
    indexed = {
        (row["backend"], row["operation"], int(row["repetition"])): row for row in rows
    }
    summaries = []
    for operation_index, operation in enumerate(OPERATIONS):
        postgres = []
        mysql = []
        postgres_p99 = []
        mysql_p99 = []
        reference = None
        for repetition in range(1, repetitions + 1):
            pg = indexed[("postgres", operation, repetition)]
            my = indexed[("mysql", operation, repetition)]
            identity_fields = (
                "clients",
                "pool_size",
                "adapter_batch_items",
                "logical_operations",
                "applied",
                "conflicts",
            )
            if any(pg[field] != my[field] for field in identity_fields):
                raise ValueError(f"{operation} repetition {repetition} differs between backends")
            identity = tuple(pg[field] for field in identity_fields)
            if reference is not None and identity != reference:
                raise ValueError(f"{operation} configuration changed between repetitions")
            reference = identity
            postgres.append(float(pg["total_ns"]))
            mysql.append(float(my["total_ns"]))
            postgres_p99.append(float(pg["p99_ns"]))
            mysql_p99.append(float(my["p99_ns"]))
        pg_median = statistics.median(postgres)
        my_median = statistics.median(mysql)
        ratio = my_median / pg_median
        low, high = bootstrap_ratio(postgres, mysql, SEED ^ operation_index)
        if low > 1 and ratio > 1.05:
            winner = "postgres"
        elif high < 1 and ratio < 1 / 1.05:
            winner = "mysql"
        else:
            winner = "inconclusive"
        summaries.append(
            {
                "operation": operation,
                "clients": reference[0],
                "pool_size": reference[1],
                "adapter_batch_items": reference[2],
                "logical_operations": reference[3],
                "postgres_median_ms": f"{pg_median / 1_000_000:.6f}",
                "postgres_ops_per_sec": f"{float(reference[3]) * 1_000_000_000 / pg_median:.6f}",
                "postgres_p99_ms": f"{statistics.median(postgres_p99) / 1_000_000:.6f}",
                "mysql_median_ms": f"{my_median / 1_000_000:.6f}",
                "mysql_ops_per_sec": f"{float(reference[3]) * 1_000_000_000 / my_median:.6f}",
                "mysql_p99_ms": f"{statistics.median(mysql_p99) / 1_000_000:.6f}",
                "mysql_to_postgres_latency": f"{ratio:.9f}",
                "ratio_ci_low": f"{low:.9f}",
                "ratio_ci_high": f"{high:.9f}",
                "winner": winner,
            }
        )
    return summaries


def report(rows: list[dict[str, str]], values: dict[str, str]) -> str:
    lines = [
        "# PostgreSQL vs MySQL adapter service comparison",
        "",
        f"Environment class: `{values.get('environment_class', 'controlled_local')}`. "
        f"Results use {values['repetitions']} measured repetitions.",
        "",
        "| Operation | Logical ops | PostgreSQL median | PostgreSQL ops/s | "
        "PostgreSQL p99 | MySQL median | MySQL ops/s | MySQL p99 | "
        "MySQL/PG 95% CI | Result |",
        "|---|---:|---:|---:|---:|---:|---:|---:|---:|---|",
    ]
    for row in rows:
        lines.append(
            "| {operation} | {logical_operations} | {postgres_median_ms} ms | "
            "{postgres_ops_per_sec} | {postgres_p99_ms} ms | {mysql_median_ms} ms | "
            "{mysql_ops_per_sec} | {mysql_p99_ms} ms | {ratio_ci_low}–"
            "{ratio_ci_high} | {winner} |".format(**row)
        )
    lines.extend(
        [
            "",
            "Batch rows measure one bounded public batch. Concurrent-get and "
            "contended-root-CAS p99 values are request-level latency. Exactly one "
            "CAS contender must win each repetition.",
            "",
            "Winner claims require a paired bootstrap 95% interval excluding "
            "parity and a median latency effect above 5%.",
            "",
        ]
    )
    return "\n".join(lines)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", type=pathlib.Path, required=True)
    parser.add_argument("--manifest", type=pathlib.Path, required=True)
    parser.add_argument("--output-dir", type=pathlib.Path, required=True)
    args = parser.parse_args()
    values = manifest(args.manifest)
    rows = load(args.input, values)
    summaries = summarize(rows, int(values["repetitions"]))
    csv_path = args.output_dir / "service-comparison.csv"
    report_path = args.output_dir / "service-report.md"
    if csv_path.exists() or report_path.exists():
        raise ValueError("refusing to overwrite service comparison output")
    with csv_path.open("x", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=summaries[0].keys())
        writer.writeheader()
        writer.writerows(summaries)
    report_path.write_text(report(summaries, values))


if __name__ == "__main__":
    main()
