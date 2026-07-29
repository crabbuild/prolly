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
    }
    for key, expected in required.items():
        if values.get(key) != expected:
            raise ValueError(f"manifest {key} must be {expected}")
    if int(values["repetitions"]) < 7:
        raise ValueError("service comparison requires at least seven repetitions")
    if not values.get("backend_a") or not values.get("backend_b"):
        raise ValueError("service comparison requires two declared backends")
    if values["backend_a"] == values["backend_b"]:
        raise ValueError("service comparison backends must differ")
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
        if backend not in (values["backend_a"], values["backend_b"]):
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


def summarize(
    rows: list[dict[str, str]], repetitions: int, backend_a: str, backend_b: str
) -> list[dict[str, str]]:
    indexed = {
        (row["backend"], row["operation"], int(row["repetition"])): row for row in rows
    }
    summaries = []
    for operation_index, operation in enumerate(OPERATIONS):
        first_samples = []
        second_samples = []
        first_p99 = []
        second_p99 = []
        reference = None
        for repetition in range(1, repetitions + 1):
            first = indexed[(backend_a, operation, repetition)]
            second = indexed[(backend_b, operation, repetition)]
            identity_fields = (
                "clients",
                "pool_size",
                "adapter_batch_items",
                "logical_operations",
                "applied",
                "conflicts",
            )
            if any(first[field] != second[field] for field in identity_fields):
                raise ValueError(f"{operation} repetition {repetition} differs between backends")
            identity = tuple(first[field] for field in identity_fields)
            if reference is not None and identity != reference:
                raise ValueError(f"{operation} configuration changed between repetitions")
            reference = identity
            first_samples.append(float(first["total_ns"]))
            second_samples.append(float(second["total_ns"]))
            first_p99.append(float(first["p99_ns"]))
            second_p99.append(float(second["p99_ns"]))
        first_median = statistics.median(first_samples)
        second_median = statistics.median(second_samples)
        ratio = second_median / first_median
        low, high = bootstrap_ratio(first_samples, second_samples, SEED ^ operation_index)
        if low > 1 and ratio > 1.05:
            winner = backend_a
        elif high < 1 and ratio < 1 / 1.05:
            winner = backend_b
        else:
            winner = "inconclusive"
        summaries.append(
            {
                "operation": operation,
                "clients": reference[0],
                "pool_size": reference[1],
                "adapter_batch_items": reference[2],
                "logical_operations": reference[3],
                f"{backend_a}_median_ms": f"{first_median / 1_000_000:.6f}",
                f"{backend_a}_ops_per_sec": f"{float(reference[3]) * 1_000_000_000 / first_median:.6f}",
                f"{backend_a}_p99_ms": f"{statistics.median(first_p99) / 1_000_000:.6f}",
                f"{backend_b}_median_ms": f"{second_median / 1_000_000:.6f}",
                f"{backend_b}_ops_per_sec": f"{float(reference[3]) * 1_000_000_000 / second_median:.6f}",
                f"{backend_b}_p99_ms": f"{statistics.median(second_p99) / 1_000_000:.6f}",
                f"{backend_b}_to_{backend_a}_latency": f"{ratio:.9f}",
                "ratio_ci_low": f"{low:.9f}",
                "ratio_ci_high": f"{high:.9f}",
                "winner": winner,
            }
        )
    return summaries


def report(rows: list[dict[str, str]], values: dict[str, str]) -> str:
    backend_a = values["backend_a"]
    backend_b = values["backend_b"]
    labels = {
        "postgres": "PostgreSQL",
        "mysql": "MySQL",
        "spanner": "Spanner",
        "dynamodb_local": "DynamoDB Local",
    }
    first_label = labels.get(backend_a, backend_a)
    second_label = labels.get(backend_b, backend_b)
    lines = [
        f"# {first_label} vs {second_label} adapter service comparison",
        "",
        f"Environment class: `{values.get('environment_class', 'controlled_local')}`. "
        f"Results use {values['repetitions']} measured repetitions.",
        "",
        f"| Operation | Logical ops | {first_label} median | {first_label} ops/s | "
        f"{first_label} p99 | {second_label} median | {second_label} ops/s | "
        f"{second_label} p99 | {second_label}/{first_label} 95% CI | Result |",
        "|---|---:|---:|---:|---:|---:|---:|---:|---:|---|",
    ]
    for row in rows:
        lines.append(
            "| {operation} | {logical_operations} | {first_median_ms} ms | "
            "{first_ops_per_sec} | {first_p99_ms} ms | {second_median_ms} ms | "
            "{second_ops_per_sec} | {second_p99_ms} ms | {ratio_ci_low}–"
            "{ratio_ci_high} | {winner} |".format(
                first_median_ms=row[f"{backend_a}_median_ms"],
                first_ops_per_sec=row[f"{backend_a}_ops_per_sec"],
                first_p99_ms=row[f"{backend_a}_p99_ms"],
                second_median_ms=row[f"{backend_b}_median_ms"],
                second_ops_per_sec=row[f"{backend_b}_ops_per_sec"],
                second_p99_ms=row[f"{backend_b}_p99_ms"],
                **row,
            )
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
    summaries = summarize(
        rows,
        int(values["repetitions"]),
        values["backend_a"],
        values["backend_b"],
    )
    csv_path = args.output_dir / "service-comparison.csv"
    report_path = args.output_dir / "service-report.md"
    if csv_path.exists() or report_path.exists():
        raise ValueError("refusing to overwrite service comparison output")
    with csv_path.open("x", newline="") as handle:
        writer = csv.DictWriter(
            handle, fieldnames=summaries[0].keys(), lineterminator="\n"
        )
        writer.writeheader()
        writer.writerows(summaries)
    report_path.write_text(report(summaries, values))


if __name__ == "__main__":
    main()
