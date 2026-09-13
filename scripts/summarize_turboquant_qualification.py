#!/usr/bin/env python3
"""Validate all TurboQuant qualification shards and emit gate summaries."""

from __future__ import annotations

import argparse
import csv
import importlib.util
import io
import json
import math
from pathlib import Path
import sys
from typing import Sequence


RUNNER_PATH = Path(__file__).resolve().with_name("run_turboquant_qualification.py")
SPEC = importlib.util.spec_from_file_location("run_turboquant_qualification", RUNNER_PATH)
runner = importlib.util.module_from_spec(SPEC)
assert SPEC.loader is not None
sys.modules[SPEC.name] = runner
SPEC.loader.exec_module(runner)


class SummaryError(RuntimeError):
    """The retained matrix is incomplete, mixed, or invalid."""


SHARD_FIELDS = {
    "shard_index",
    "shard_cell_count",
    "shard_matrix_digest",
    "wasm_smoke",
}


def read_json(path: Path) -> dict[str, object]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as error:
        raise SummaryError(f"cannot read {path}: {error}") from error
    if not isinstance(value, dict):
        raise SummaryError(f"expected JSON object in {path}")
    return value


def common_contract(contract: dict[str, object]) -> dict[str, object]:
    return {key: value for key, value in contract.items() if key not in SHARD_FIELDS}


def validate_contract_set(contracts: Sequence[dict[str, object]]) -> list[dict[str, object]]:
    if not contracts:
        raise SummaryError("at least one shard is required")
    expected_common = common_contract(contracts[0])
    try:
        shard_count = int(contracts[0]["shard_count"])
    except (KeyError, TypeError, ValueError) as error:
        raise SummaryError("invalid shard_count in manifest") from error
    if shard_count <= 0:
        raise SummaryError("shard_count must be positive")
    by_index: dict[int, dict[str, object]] = {}
    for contract in contracts:
        if common_contract(contract) != expected_common:
            raise SummaryError("shards have mixed matrix contracts")
        try:
            index = int(contract["shard_index"])
        except (KeyError, TypeError, ValueError) as error:
            raise SummaryError("invalid shard_index in manifest") from error
        if index in by_index:
            raise SummaryError(f"duplicate shard index {index}")
        by_index[index] = contract
    expected_indices = set(range(shard_count))
    if set(by_index) != expected_indices:
        raise SummaryError(
            f"incomplete shard set: missing={sorted(expected_indices - set(by_index))} "
            f"unexpected={sorted(set(by_index) - expected_indices)}"
        )
    return [by_index[index] for index in range(shard_count)]


def operation_value(parsed, operation: str, column: int) -> float:
    rows = parsed.rows[operation]
    if len(rows) != 1:
        raise SummaryError(f"expected one {operation} row")
    return float(rows[0][column])


def worker_value(parsed, operation: str, worker: int, column: int) -> float:
    matches = [row for row in parsed.rows[operation] if int(row[2]) == worker]
    if len(matches) != 1:
        raise SummaryError(f"expected one {operation} row for worker {worker}")
    return float(matches[0][column])


SUMMARY_FIELDS = (
    "cell_id",
    "records",
    "dimensions",
    "metric",
    "requested_k",
    "effective_k",
    "eligibility_ppm",
    "bits",
    "rerank_kind",
    "rerank_multiplier",
    "environment",
    "source_bytes",
    "turboquant_manifest_bytes",
    "turboquant_code_tree_bytes",
    "turboquant_sidecar_bytes",
    "turboquant_encoded_bytes",
    "pq_manifest_bytes",
    "pq_code_tree_bytes",
    "pq_sidecar_bytes",
    "turboquant_build_micros",
    "pq_build_micros",
    "turboquant_search_median_micros",
    "turboquant_search_p95_micros",
    "turboquant_search_p99_micros",
    "pq_search_median_micros",
    "pq_search_p95_micros",
    "pq_search_p99_micros",
    "turboquant_recall",
    "pq_recall",
    "turboquant_quantized_evaluations",
    "turboquant_exact_evaluations",
    "turboquant_logical_bytes",
    "turboquant_physical_bytes",
    "turboquant_logical_nodes",
    "turboquant_physical_reads",
    "turboquant_candidate_peak",
    "turboquant_candidate_bytes_peak",
    "turboquant_frontier_peak",
    "turboquant_reranked_candidates",
    "completion",
)


def summarize_cell(cell, parsed, workers: Sequence[int]) -> dict[str, object]:
    async_mode = cell.environment.async_quantizers
    tq_search = "turboquant_search_async" if async_mode else "turboquant_search_auto"
    pq_search = "pq_search_async" if async_mode else "pq_search"
    tq_recall = "turboquant_recall_async" if async_mode else "turboquant_recall"
    pq_recall = "pq_recall_async" if async_mode else "pq_recall"
    tq_build = "turboquant_build_async" if async_mode else "turboquant_build"
    pq_build = "pq_build_async" if async_mode else "pq_build"
    worker = max(workers)
    return {
        "cell_id": cell.identifier,
        "records": cell.records,
        "dimensions": cell.dimensions,
        "metric": cell.metric,
        "requested_k": cell.k,
        "effective_k": int(parsed.preamble["effective_k"]),
        "eligibility_ppm": cell.eligibility_ppm,
        "bits": cell.bits,
        "rerank_kind": cell.rerank,
        "rerank_multiplier": cell.rerank_multiplier,
        "environment": cell.environment.name,
        "source_bytes": int(operation_value(parsed, "source_closure_bytes", 4)),
        "turboquant_manifest_bytes": int(
            operation_value(parsed, "turboquant_manifest_code_bytes", 4)
        ),
        "turboquant_code_tree_bytes": int(
            operation_value(parsed, "turboquant_manifest_code_bytes", 5)
        ),
        "turboquant_sidecar_bytes": int(
            operation_value(parsed, "turboquant_sidecar_bytes", 4)
        ),
        "turboquant_encoded_bytes": int(
            operation_value(parsed, "turboquant_sidecar_bytes", 5)
        ),
        "pq_manifest_bytes": int(operation_value(parsed, "pq_manifest_code_bytes", 4)),
        "pq_code_tree_bytes": int(operation_value(parsed, "pq_manifest_code_bytes", 5)),
        "pq_sidecar_bytes": int(operation_value(parsed, "pq_sidecar_bytes", 4)),
        "turboquant_build_micros": worker_value(parsed, tq_build, worker, 3),
        "pq_build_micros": worker_value(parsed, pq_build, worker, 3),
        "turboquant_search_median_micros": operation_value(parsed, tq_search, 3),
        "turboquant_search_p95_micros": operation_value(parsed, tq_search + "_p95", 3),
        "turboquant_search_p99_micros": operation_value(parsed, tq_search + "_p99", 3),
        "pq_search_median_micros": operation_value(parsed, pq_search, 3),
        "pq_search_p95_micros": operation_value(parsed, pq_search + "_p95", 3),
        "pq_search_p99_micros": operation_value(parsed, pq_search + "_p99", 3),
        "turboquant_recall": operation_value(parsed, tq_recall, 4),
        "pq_recall": operation_value(parsed, pq_recall, 4),
        "turboquant_quantized_evaluations": int(operation_value(parsed, tq_search, 4)),
        "turboquant_exact_evaluations": int(operation_value(parsed, tq_search, 5)),
        "turboquant_logical_bytes": int(operation_value(parsed, tq_search + "_p95", 4)),
        "turboquant_physical_bytes": int(operation_value(parsed, tq_search + "_p95", 5)),
        "turboquant_logical_nodes": int(operation_value(parsed, tq_search + "_io", 4)),
        "turboquant_physical_reads": int(operation_value(parsed, tq_search + "_io", 5)),
        "turboquant_candidate_peak": int(operation_value(parsed, tq_search + "_p99", 4)),
        "turboquant_candidate_bytes_peak": int(
            operation_value(parsed, tq_search + "_p99", 5)
        ),
        "turboquant_frontier_peak": int(operation_value(parsed, tq_search + "_work", 4)),
        "turboquant_reranked_candidates": int(
            operation_value(parsed, tq_search + "_rerank", 4)
        ),
        "completion": int(operation_value(parsed, tq_search + "_work", 5)),
    }


def evaluate_gates(
    rows: Sequence[dict[str, object]],
    profile: str,
    scalability_failures: Sequence[dict[str, object]] = (),
) -> dict[str, object]:
    if profile != "full":
        return {
            "profile": profile,
            "matrix_complete": True,
            "forced_matrix_qualified": False,
            "auto_qualified": False,
            "typed_scalability_failures": len(scalability_failures),
            "note": "smoke evidence does not evaluate production GA or Auto gates",
        }

    failures: dict[str, list[str]] = {
        "recall_floor": [],
        "recall_vs_pq": [],
        "candidate_bound": [],
        "transform_count": [],
        "code_value_size": [],
        "four_bit_ratio": [],
    }
    default_rows = [
        row
        for row in rows
        if row["bits"] == 4
        and row["rerank_kind"] == "fixed"
        and row["rerank_multiplier"] == 8
        and row["requested_k"] == 10
    ]
    for row in default_rows:
        if float(row["turboquant_recall"]) < 0.95:
            failures["recall_floor"].append(str(row["cell_id"]))
        if float(row["turboquant_recall"]) + 0.01 < float(row["pq_recall"]):
            failures["recall_vs_pq"].append(str(row["cell_id"]))
    for row in rows:
        eligible = max(
            1,
            math.ceil(int(row["records"]) * int(row["eligibility_ppm"]) / 1_000_000),
        )
        target = min(
            eligible,
            int(row["effective_k"]) * int(row["rerank_multiplier"]),
        )
        if int(row["turboquant_candidate_peak"]) > target:
            failures["candidate_bound"].append(str(row["cell_id"]))
        expected_transforms = int(row["records"]) * int(row["dimensions"])
        # Synthetic qualification vectors are all nonzero. The base build row's
        # first counter is transformed components, which must be one transform
        # per source vector.
        # This value is validated directly from raw output below when loading.
        if int(row["_transformed_components"]) != expected_transforms:
            failures["transform_count"].append(str(row["cell_id"]))
        expected_encoded = int(row["records"]) * (
            8 + math.ceil(int(row["dimensions"]) * int(row["bits"]) / 8)
        )
        if int(row["turboquant_encoded_bytes"]) != expected_encoded:
            failures["code_value_size"].append(str(row["cell_id"]))
        raw_bytes = int(row["records"]) * int(row["dimensions"]) * 4
        if (
            int(row["bits"]) == 4
            and int(row["dimensions"]) >= 128
            and int(row["turboquant_encoded_bytes"]) > 0.15 * raw_bytes
        ):
            failures["four_bit_ratio"].append(str(row["cell_id"]))

    auto_scope = [
        row
        for row in rows
        if row["records"] == 100_000
        and row["dimensions"] in (768, 1_536)
        and row["metric"] in runner.FULL_METRICS
        and row["requested_k"] == 10
        and row["eligibility_ppm"] == 1_000_000
        and row["bits"] == 4
        and row["rerank_kind"] == "fixed"
        and row["rerank_multiplier"] == 8
        and row["environment"] == "memory-warm-sync"
    ]
    auto_failures = {"build": [], "p95": [], "value": []}
    for row in auto_scope:
        if float(row["turboquant_build_micros"]) >= float(row["pq_build_micros"]):
            auto_failures["build"].append(str(row["cell_id"]))
        p95_ratio = float(row["turboquant_search_p95_micros"]) / max(
            float(row["pq_search_p95_micros"]), 1e-12
        )
        if float(row["turboquant_recall"]) < 0.95 or p95_ratio > 1.25:
            auto_failures["p95"].append(str(row["cell_id"]))
        faster = float(row["turboquant_search_p95_micros"]) < float(
            row["pq_search_p95_micros"]
        )
        smaller = int(row["turboquant_sidecar_bytes"]) <= 0.75 * int(
            row["pq_sidecar_bytes"]
        )
        recall_gain = float(row["turboquant_recall"]) >= float(row["pq_recall"]) + 0.02
        recall_gain = recall_gain and p95_ratio <= 1.25
        if not (faster or smaller or recall_gain):
            auto_failures["value"].append(str(row["cell_id"]))

    forced_pass = bool(default_rows) and not any(failures.values())
    auto_pass = forced_pass and len(auto_scope) == 6 and not any(auto_failures.values())
    return {
        "profile": profile,
        "matrix_complete": True,
        "default_recall_rows": len(default_rows),
        "forced_matrix_failures": failures,
        "forced_matrix_qualified": forced_pass,
        "typed_scalability_failures": len(scalability_failures),
        "typed_scalability_failure_cells": [
            str(failure["cell_id"]) for failure in scalability_failures
        ],
        "auto_scope_rows": len(auto_scope),
        "auto_failures": auto_failures,
        "auto_qualified": auto_pass,
        "release_gates_not_evaluated": [
            "legal/patent disposition",
            "repository-wide binding inventory",
            "final supported-host release commands",
        ],
    }


def load_matrix(
    inputs: Sequence[Path],
) -> tuple[dict[str, object], list[dict[str, object]], list[dict[str, object]]]:
    manifests = [read_json(path / "manifest.json") for path in inputs]
    contracts = []
    for path, manifest in zip(inputs, manifests, strict=True):
        contract = manifest.get("contract")
        if not isinstance(contract, dict):
            raise SummaryError(f"missing contract in {path / 'manifest.json'}")
        contracts.append(contract)
    ordered_contracts = validate_contract_set(contracts)
    path_by_index = {
        int(contract["shard_index"]): path for path, contract in zip(inputs, contracts, strict=True)
    }
    profile = str(ordered_contracts[0]["profile"])
    revision = str(ordered_contracts[0]["revision"])
    workers = tuple(int(value) for value in ordered_contracts[0]["workers"])
    repeats = int(ordered_contracts[0]["search_repeats"])
    raw_limit = ordered_contracts[0].get("max_cell_records")
    max_cell_records = None if raw_limit is None else int(raw_limit)
    all_cells = runner.enumerate_cells(profile)
    summaries = []
    scalability_failures = []
    for contract in ordered_contracts:
        index = int(contract["shard_index"])
        shard_count = int(contract["shard_count"])
        path = path_by_index[index]
        expected_cells = runner.shard_cells(all_cells, index, shard_count)
        expected_contract = runner.make_contract(
            profile,
            revision,
            workers,
            repeats,
            index,
            shard_count,
            all_cells,
            expected_cells,
            max_cell_records,
        )
        if contract != expected_contract:
            raise SummaryError(f"shard {index} manifest differs from the canonical contract")
        status = read_json(path / "status.json")
        if status.get("complete") is not True or status.get("completed_cells") != len(
            expected_cells
        ):
            raise SummaryError(f"shard {index} is not complete")
        expected_failure_count = sum(
            runner.expected_scalability_failure(cell, max_cell_records) is not None
            for cell in expected_cells
        )
        if status.get("typed_scalability_failures") != expected_failure_count:
            raise SummaryError(
                f"shard {index} typed scalability failure count mismatch"
            )
        if index == 0:
            try:
                runner.wasm_smoke_is_valid(path, revision)
            except runner.QualificationError as error:
                raise SummaryError(str(error)) from error
        benchmark_cells = [
            cell
            for cell in expected_cells
            if runner.expected_scalability_failure(cell, max_cell_records) is None
        ]
        expected_names = {f"{cell.identifier}.csv" for cell in benchmark_cells}
        actual_names = {entry.name for entry in (path / "raw").glob("*.csv")}
        if actual_names != expected_names:
            raise SummaryError(
                f"shard {index} raw cell set mismatch missing={sorted(expected_names - actual_names)} "
                f"unexpected={sorted(actual_names - expected_names)}"
            )
        expected_state_names = {f"{cell.identifier}.json" for cell in expected_cells}
        actual_state_names = {entry.name for entry in (path / "state").glob("*.json")}
        if actual_state_names != expected_state_names:
            raise SummaryError(
                f"shard {index} state set mismatch "
                f"missing={sorted(expected_state_names - actual_state_names)} "
                f"unexpected={sorted(actual_state_names - expected_state_names)}"
            )
        for cell in expected_cells:
            try:
                runner.completed_cell_is_valid(
                    path,
                    cell,
                    revision,
                    workers,
                    repeats,
                    max_cell_records,
                )
                failure = runner.expected_scalability_failure(cell, max_cell_records)
                if failure is not None:
                    scalability_failures.append(
                        {"cell_id": cell.identifier, **failure}
                    )
                    continue
                raw = (path / "raw" / f"{cell.identifier}.csv").read_text(encoding="utf-8")
                parsed = runner.validate_output(raw, cell, revision, workers, repeats)
            except (OSError, runner.QualificationError) as error:
                raise SummaryError(str(error)) from error
            summary = summarize_cell(cell, parsed, workers)
            base_build = "turboquant_build_async" if cell.environment.async_quantizers else "turboquant_build"
            summary["_transformed_components"] = int(
                worker_value(parsed, base_build, max(workers), 4)
            )
            summaries.append(summary)
    dispositions = len(summaries) + len(scalability_failures)
    if dispositions != len(all_cells):
        raise SummaryError(f"validated {dispositions} cells, expected {len(all_cells)}")
    return ordered_contracts[0], summaries, scalability_failures


def write_summary(
    output: Path,
    contract: dict[str, object],
    rows: list[dict[str, object]],
    scalability_failures: list[dict[str, object]],
) -> None:
    if output.exists() and any(output.iterdir()):
        raise SummaryError(f"refusing to overwrite non-empty summary output: {output}")
    output.mkdir(parents=True, exist_ok=True)
    buffer = io.StringIO()
    writer = csv.DictWriter(buffer, fieldnames=SUMMARY_FIELDS, extrasaction="ignore")
    writer.writeheader()
    writer.writerows(rows)
    runner.atomic_write(output / "summary.csv", buffer.getvalue())
    runner.write_json(output / "scalability-failures.json", scalability_failures)
    gates = evaluate_gates(rows, str(contract["profile"]), scalability_failures)
    runner.write_json(output / "gates.json", gates)
    failed_forced = sum(len(value) for value in gates.get("forced_matrix_failures", {}).values())
    failed_auto = sum(len(value) for value in gates.get("auto_failures", {}).values())
    report = f"""# TurboQuant qualification summary

- Revision: `{contract['revision']}`
- Profile: `{contract['profile']}`
- Validated matrix dispositions: {len(rows) + len(scalability_failures)}
- Completed benchmark cells: {len(rows)}
- Typed scalability failures: {len(scalability_failures)}
- Matrix contract: `{contract['full_matrix_digest']}`
- Forced-backend matrix gates: {'PASS' if gates['forced_matrix_qualified'] else 'NOT QUALIFIED'}
- Auto gates: {'PASS' if gates['auto_qualified'] else 'NOT QUALIFIED'}
- Forced matrix failures: {failed_forced}
- Auto failures: {failed_auto}

This report covers benchmark-matrix gates only. Legal/patent disposition,
repository-wide binding inventory, and final supported-host release commands
remain independent release gates and are never inferred from benchmark data.
"""
    runner.atomic_write(output / "report.md", report)


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--input", type=Path, nargs="+", required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args(argv)
    contract, rows, scalability_failures = load_matrix(
        [path.resolve() for path in args.input]
    )
    write_summary(args.output.resolve(), contract, rows, scalability_failures)
    print(
        f"validated {len(rows) + len(scalability_failures)} TurboQuant cells "
        f"({len(scalability_failures)} typed scalability failures) "
        f"into {args.output.resolve()}"
    )
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except SummaryError as error:
        print(f"error: {error}", file=sys.stderr)
        raise SystemExit(2) from error
