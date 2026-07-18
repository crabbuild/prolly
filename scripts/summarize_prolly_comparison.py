#!/usr/bin/env python3
"""Strictly validate and summarize native Dolt-Go versus Rust benchmarks."""

from __future__ import annotations

import argparse
import csv
import statistics
from collections import defaultdict
from dataclasses import dataclass
from itertools import product
from pathlib import Path
from typing import Sequence


IMPLEMENTATIONS = ("rust", "dolt-go")
PHASES = ("fresh", "mutation")
WORKLOADS = ("append", "random", "clustered")
OPERATIONS = ("write", "point_read", "range_scan")
CONTRACT_VERSION = "prolly-compare-v1"
DEFAULT_SIZES = (10_000, 50_000, 1_000_000, 5_000_000, 10_000_000)
GOLDEN_10K_DIGESTS = {
    ("fresh", "append"): "51f55fcd59187cbf",
    ("fresh", "random"): "004197dd790a1245",
    ("fresh", "clustered"): "86e38047f6ae04b3",
    ("mutation", "append"): "2ef1df79e1226620",
    ("mutation", "random"): "3bc7e45ef276a1c5",
    ("mutation", "clustered"): "5caed8dbd3056277",
}
REQUIRED_FIELDS = {
    "implementation",
    "revision",
    "contract_version",
    "records",
    "phase",
    "workload",
    "operation",
    "operations",
    "elapsed_ns",
    "ns_per_op",
    "ops_per_sec",
    "workload_digest",
    "result_count",
    "validated",
    "repetition",
    "peak_rss_bytes",
}


class BenchmarkValidationError(ValueError):
    """Raised when measured rows cannot support a valid comparison."""


@dataclass(frozen=True)
class MatrixStats:
    processes: int
    rows: int
    pairs: int


def load_rows(path: Path) -> list[dict[str, str]]:
    with path.open(newline="", encoding="utf-8") as handle:
        reader = csv.DictReader(handle)
        if reader.fieldnames is None:
            raise BenchmarkValidationError("input has no CSV header")
        missing = REQUIRED_FIELDS - set(reader.fieldnames)
        if missing:
            raise BenchmarkValidationError(f"input is missing fields: {sorted(missing)}")
        rows = list(reader)
    if not rows:
        raise BenchmarkValidationError("no measurement rows")
    return rows


def _positive_integer(row: dict[str, str], field: str) -> int:
    try:
        value = int(row[field])
    except (KeyError, ValueError) as error:
        raise BenchmarkValidationError(f"invalid {field}: {row.get(field)!r}") from error
    if value <= 0:
        raise BenchmarkValidationError(f"invalid {field}: {value}")
    return value


def _scenario_key(row: dict[str, str]) -> tuple[str, str, str, str]:
    return (row["records"], row["phase"], row["workload"], row["operation"])


def _pair_key(row: dict[str, str]) -> tuple[str, str, str, str, str]:
    return (*_scenario_key(row), row["repetition"])


def validate_matrix(
    rows: Sequence[dict[str, str]],
    expected_runs: int,
    expected_sizes: Sequence[int],
    allow_partial: bool = False,
) -> MatrixStats:
    if not rows:
        raise BenchmarkValidationError("no measurement rows")
    if expected_runs <= 0:
        raise BenchmarkValidationError("expected_runs must be positive")
    sizes = {str(value) for value in expected_sizes}
    if not sizes:
        raise BenchmarkValidationError("expected_sizes must not be empty")

    by_pair: dict[tuple[str, ...], dict[str, dict[str, str]]] = defaultdict(dict)
    repetitions: dict[tuple[str, ...], set[int]] = defaultdict(set)
    processes: set[tuple[str, ...]] = set()
    observed_scenarios: set[tuple[str, ...]] = set()

    for row in rows:
        missing = REQUIRED_FIELDS - set(row)
        if missing:
            raise BenchmarkValidationError(f"row is missing fields: {sorted(missing)}")
        implementation = row["implementation"]
        if implementation not in IMPLEMENTATIONS:
            raise BenchmarkValidationError(f"unknown implementation: {implementation!r}")
        if row["records"] not in sizes:
            raise BenchmarkValidationError(f"unexpected records value: {row['records']!r}")
        if row["phase"] not in PHASES:
            raise BenchmarkValidationError(f"unknown phase: {row['phase']!r}")
        if row["workload"] not in WORKLOADS:
            raise BenchmarkValidationError(f"unknown workload: {row['workload']!r}")
        if row["operation"] not in OPERATIONS:
            raise BenchmarkValidationError(f"unknown operation: {row['operation']!r}")
        if row["contract_version"] != CONTRACT_VERSION:
            raise BenchmarkValidationError(
                f"contract_version mismatch: {row['contract_version']!r}"
            )
        if row["validated"] != "true":
            raise BenchmarkValidationError(f"validated mismatch: {row['validated']!r}")
        _positive_integer(row, "operations")
        _positive_integer(row, "elapsed_ns")
        _positive_integer(row, "result_count")
        _positive_integer(row, "peak_rss_bytes")
        repetition = _positive_integer(row, "repetition")
        try:
            float(row["ns_per_op"])
            float(row["ops_per_sec"])
        except ValueError as error:
            raise BenchmarkValidationError(f"invalid rate fields in row: {row}") from error

        if row["records"] == "10000":
            expected_digest = GOLDEN_10K_DIGESTS[(row["phase"], row["workload"])]
            if row["workload_digest"] != expected_digest:
                raise BenchmarkValidationError(
                    "workload_digest mismatch for 10K "
                    f"{row['phase']}/{row['workload']}: {row['workload_digest']}"
                )

        pair_key = _pair_key(row)
        if implementation in by_pair[pair_key]:
            raise BenchmarkValidationError(f"duplicate {implementation} row for {pair_key}")
        by_pair[pair_key][implementation] = row
        scenario = _scenario_key(row)
        observed_scenarios.add(scenario)
        repetitions[scenario].add(repetition)
        processes.add(
            (
                implementation,
                row["records"],
                row["phase"],
                row["workload"],
                row["repetition"],
            )
        )

    expected_repetitions = set(range(1, expected_runs + 1))
    for scenario, actual in repetitions.items():
        if actual != expected_repetitions:
            raise BenchmarkValidationError(
                f"repetitions for {scenario} are {sorted(actual)}, "
                f"want {sorted(expected_repetitions)}"
            )

    for key, pair in by_pair.items():
        if set(pair) != set(IMPLEMENTATIONS):
            raise BenchmarkValidationError(
                f"missing implementation for {key}: {sorted(pair)}"
            )
        rust, go = pair["rust"], pair["dolt-go"]
        for field in (
            "contract_version",
            "operations",
            "workload_digest",
            "result_count",
            "validated",
        ):
            if rust[field] != go[field]:
                raise BenchmarkValidationError(
                    f"{field} mismatch for {key}: rust={rust[field]} go={go[field]}"
                )

    if not allow_partial:
        required = {
            (str(size), phase_name, workload_name, operation_name)
            for size, phase_name, workload_name, operation_name in product(
                expected_sizes, PHASES, WORKLOADS, OPERATIONS
            )
        }
        missing = required - observed_scenarios
        extra = observed_scenarios - required
        if missing:
            raise BenchmarkValidationError(
                f"missing scenarios: {sorted(missing, key=_sortable_scenario)[:5]}"
            )
        if extra:
            raise BenchmarkValidationError(
                f"unexpected scenarios: {sorted(extra, key=_sortable_scenario)[:5]}"
            )
        required_process_operations = {
            (
                implementation,
                str(size),
                phase_name,
                workload_name,
                str(repetition),
            )
            for implementation, size, phase_name, workload_name, repetition in product(
                IMPLEMENTATIONS,
                expected_sizes,
                PHASES,
                WORKLOADS,
                range(1, expected_runs + 1),
            )
        }
        if processes != required_process_operations:
            raise BenchmarkValidationError("process set does not match the required matrix")

    return MatrixStats(processes=len(processes), rows=len(rows), pairs=len(by_pair))


def _sortable_scenario(key: tuple[str, ...]) -> tuple[int, str, str, str]:
    return (int(key[0]), key[1], key[2], key[3])


def _coefficient_of_variation(values: Sequence[float]) -> float:
    mean = statistics.fmean(values)
    return statistics.pstdev(values) / mean if mean else 0.0


def summarize(rows: Sequence[dict[str, str]]) -> list[dict[str, str]]:
    groups: dict[tuple[str, ...], dict[str, list[dict[str, str]]]] = defaultdict(
        lambda: defaultdict(list)
    )
    for row in rows:
        groups[_scenario_key(row)][row["implementation"]].append(row)

    summary_rows: list[dict[str, str]] = []
    for key in sorted(groups, key=_sortable_scenario):
        implementations = groups[key]
        metrics: dict[str, dict[str, float]] = {}
        for implementation in IMPLEMENTATIONS:
            measured = implementations[implementation]
            ns_per_op = [
                int(row["elapsed_ns"]) / int(row["operations"]) for row in measured
            ]
            peak_rss = [int(row["peak_rss_bytes"]) for row in measured]
            metrics[implementation] = {
                "median": statistics.median(ns_per_op),
                "minimum": min(ns_per_op),
                "maximum": max(ns_per_op),
                "cv": _coefficient_of_variation(ns_per_op),
                "rss_median": statistics.median(peak_rss),
                "rss_maximum": max(peak_rss),
            }
        rust = metrics["rust"]["median"]
        go = metrics["dolt-go"]["median"]
        winner = "rust" if rust < go else "dolt-go" if go < rust else "tie"
        slower = max(rust, go)
        faster = min(rust, go)
        speedup = slower / faster if faster else float("inf")
        summary_rows.append(
            {
                "records": key[0],
                "phase": key[1],
                "workload": key[2],
                "operation": key[3],
                "contract_version": CONTRACT_VERSION,
                "runs": str(len(implementations["rust"])),
                "rust_median_ns_per_op": f"{rust:.3f}",
                "rust_min_ns_per_op": f"{metrics['rust']['minimum']:.3f}",
                "rust_max_ns_per_op": f"{metrics['rust']['maximum']:.3f}",
                "rust_cv": f"{metrics['rust']['cv']:.6f}",
                "rust_median_peak_rss_bytes": str(
                    int(metrics["rust"]["rss_median"])
                ),
                "rust_max_peak_rss_bytes": str(
                    int(metrics["rust"]["rss_maximum"])
                ),
                "dolt_go_median_ns_per_op": f"{go:.3f}",
                "dolt_go_min_ns_per_op": f"{metrics['dolt-go']['minimum']:.3f}",
                "dolt_go_max_ns_per_op": f"{metrics['dolt-go']['maximum']:.3f}",
                "dolt_go_cv": f"{metrics['dolt-go']['cv']:.6f}",
                "dolt_go_median_peak_rss_bytes": str(
                    int(metrics["dolt-go"]["rss_median"])
                ),
                "dolt_go_max_peak_rss_bytes": str(
                    int(metrics["dolt-go"]["rss_maximum"])
                ),
                "winner": winner,
                "winner_speedup": f"{speedup:.6f}",
            }
        )
    return summary_rows


def _validate_historical_contract(history_summary: Path) -> None:
    results_path = history_summary.with_name("results.csv")
    if not results_path.is_file():
        raise BenchmarkValidationError(
            f"historical workload contract cannot be verified without {results_path}"
        )
    with results_path.open(newline="", encoding="utf-8") as handle:
        rows = list(csv.DictReader(handle))
    observed: dict[tuple[str, str], set[str]] = defaultdict(set)
    for row in rows:
        if row.get("records") == "10000":
            key = (row.get("phase", ""), row.get("workload", ""))
            observed[key].add(row.get("workload_digest", ""))
    for key, digest in GOLDEN_10K_DIGESTS.items():
        if observed.get(key) != {digest}:
            raise BenchmarkValidationError(
                f"historical workload contract mismatch for {key}: "
                f"{sorted(observed.get(key, set()))}"
            )


def compare_history(
    current_summary: Sequence[dict[str, str]], history_summary: Path
) -> list[dict[str, str]]:
    _validate_historical_contract(history_summary)
    with history_summary.open(newline="", encoding="utf-8") as handle:
        historical = list(csv.DictReader(handle))
    historical_by_key = {
        (row["records"], row["phase"], row["workload"], row["operation"]): row
        for row in historical
    }
    deltas: list[dict[str, str]] = []
    for current in current_summary:
        if current["contract_version"] != CONTRACT_VERSION:
            continue
        key = (
            current["records"],
            current["phase"],
            current["workload"],
            current["operation"],
        )
        old = historical_by_key.get(key)
        if old is None:
            continue
        historical_ns = float(old["rust_median_ns_per_op"])
        current_ns = float(current["rust_median_ns_per_op"])
        improvement = (
            (historical_ns - current_ns) / historical_ns * 100
            if historical_ns
            else 0.0
        )
        deltas.append(
            {
                "records": current["records"],
                "phase": current["phase"],
                "workload": current["workload"],
                "operation": current["operation"],
                "contract_version": CONTRACT_VERSION,
                "historical_rust_median_ns_per_op": f"{historical_ns:.3f}",
                "current_rust_median_ns_per_op": f"{current_ns:.3f}",
                "improvement_percent": f"{improvement:.3f}",
            }
        )
    return deltas


def _write_csv(path: Path, rows: Sequence[dict[str, str]]) -> None:
    if not rows:
        raise BenchmarkValidationError(f"refusing to write empty CSV: {path.name}")
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(
            handle, fieldnames=list(rows[0]), lineterminator="\n"
        )
        writer.writeheader()
        writer.writerows(rows)


def _current_report(summary_rows: Sequence[dict[str, str]]) -> str:
    wins: dict[str, int] = defaultdict(int)
    operation_wins: dict[str, dict[str, int]] = defaultdict(lambda: defaultdict(int))
    for row in summary_rows:
        wins[row["winner"]] += 1
        operation_wins[row["operation"]][row["winner"]] += 1
    report = [
        "# Dolt Go vs Rust Prolly Performance",
        "",
        "All figures are medians from process-isolated, single-worker, in-memory runs. "
        "Lower nanoseconds per operation is better. Winner selection uses unrounded "
        "elapsed nanoseconds divided by logical operations.",
        "",
        f"Rust wins: {wins['rust']}; Dolt Go wins: {wins['dolt-go']}; ties: {wins['tie']}.",
        "",
        "| Operation | Rust wins | Dolt Go wins | Ties |",
        "|---|---:|---:|---:|",
    ]
    for operation in OPERATIONS:
        counts = operation_wins[operation]
        report.append(
            f"| {operation} | {counts['rust']} | {counts['dolt-go']} | {counts['tie']} |"
        )
    report.extend(
        [
            "",
            "| Records | Phase | Workload | Operation | Runs | Rust ns/op | Dolt Go ns/op | Rust CV | Dolt CV | Winner | Speedup |",
            "|---:|---|---|---|---:|---:|---:|---:|---:|---|---:|",
        ]
    )
    for row in summary_rows:
        report.append(
            "| {records} | {phase} | {workload} | {operation} | {runs} | "
            "{rust_median_ns_per_op} | {dolt_go_median_ns_per_op} | {rust_cv} | "
            "{dolt_go_cv} | {winner} | {winner_speedup}x |".format(**row)
        )
    report.extend(
        [
            "",
            "## Limitations",
            "",
            "This compares native product paths with each implementation's default "
            "encoding, chunking, allocator, runtime, and in-memory store. It does not "
            "isolate language runtime speed. Three observations expose gross variance "
            "but do not establish statistical significance; inspect min/max and CV before "
            "treating a narrow winner as robust.",
            "",
            "Peak RSS covers the entire scenario process, including untimed fixture and "
            "tuple preparation. Timing rows exclude that preparation.",
        ]
    )
    return "\n".join(report) + "\n"


def _historical_report(deltas: Sequence[dict[str, str]]) -> str:
    lines = [
        "# Rust Prolly Historical Performance Delta",
        "",
        "Positive improvement means the current Rust median is faster than the July 16 "
        "median; negative values are regressions. Only exact contract and scenario matches "
        "are included.",
        "",
        "| Records | Phase | Workload | Operation | Historical ns/op | Current ns/op | Improvement |",
        "|---:|---|---|---|---:|---:|---:|",
    ]
    for row in deltas:
        lines.append(
            "| {records} | {phase} | {workload} | {operation} | "
            "{historical_rust_median_ns_per_op} | {current_rust_median_ns_per_op} | "
            "{improvement_percent}% |".format(**row)
        )
    return "\n".join(lines) + "\n"


def write_outputs(
    rows: Sequence[dict[str, str]],
    summary_rows: Sequence[dict[str, str]],
    output_dir: Path,
    history_summary: Path | None = None,
) -> None:
    del rows  # Validation occurs before reporting; raw normalized rows stay in results.csv.
    output_dir.mkdir(parents=True, exist_ok=True)
    _write_csv(output_dir / "summary.csv", summary_rows)
    (output_dir / "report.md").write_text(
        _current_report(summary_rows), encoding="utf-8"
    )
    if history_summary is not None:
        deltas = compare_history(summary_rows, history_summary)
        _write_csv(output_dir / "historical-delta.csv", deltas)
        (output_dir / "historical-report.md").write_text(
            _historical_report(deltas), encoding="utf-8"
        )


def _parse_sizes(raw: str) -> list[int]:
    try:
        sizes = [int(value) for value in raw.split(",") if value]
    except ValueError as error:
        raise argparse.ArgumentTypeError("sizes must be comma-separated integers") from error
    if not sizes or any(value <= 0 for value in sizes):
        raise argparse.ArgumentTypeError("sizes must be positive")
    return sizes


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", required=True, type=Path)
    parser.add_argument("--output-dir", required=True, type=Path)
    parser.add_argument("--expected-runs", type=int, default=3)
    parser.add_argument(
        "--expected-sizes",
        type=_parse_sizes,
        default=list(DEFAULT_SIZES),
    )
    parser.add_argument("--history-summary", type=Path)
    parser.add_argument("--allow-partial", action="store_true")
    args = parser.parse_args()
    try:
        rows = load_rows(args.input)
        validate_matrix(
            rows,
            expected_runs=args.expected_runs,
            expected_sizes=args.expected_sizes,
            allow_partial=args.allow_partial,
        )
        summary_rows = summarize(rows)
        write_outputs(rows, summary_rows, args.output_dir, args.history_summary)
    except BenchmarkValidationError as error:
        raise SystemExit(f"invalid benchmark results: {error}") from error


if __name__ == "__main__":
    main()
