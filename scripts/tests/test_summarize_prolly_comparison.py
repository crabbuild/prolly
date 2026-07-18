import csv
import tempfile
import unittest
from copy import deepcopy
from pathlib import Path

from scripts.summarize_prolly_comparison import (
    BenchmarkValidationError,
    compare_history,
    summarize,
    validate_matrix,
    write_outputs,
)


SIZES = [10_000, 50_000, 1_000_000, 5_000_000, 10_000_000]
PHASES = ["fresh", "mutation"]
WORKLOADS = ["append", "random", "clustered"]
OPERATIONS = ["write", "point_read", "range_scan"]
GOLDEN = {
    ("fresh", "append"): "51f55fcd59187cbf",
    ("fresh", "random"): "004197dd790a1245",
    ("fresh", "clustered"): "86e38047f6ae04b3",
    ("mutation", "append"): "2ef1df79e1226620",
    ("mutation", "random"): "3bc7e45ef276a1c5",
    ("mutation", "clustered"): "5caed8dbd3056277",
}


def scenario_rows(
    records=10_000,
    phase="fresh",
    workload="append",
    operation="write",
    runs=3,
):
    rows = []
    for repetition in range(1, runs + 1):
        for implementation, ns_per_op, rss in (
            ("rust", repetition * 10, repetition * 1_000),
            ("dolt-go", repetition * 10 + 30, repetition * 1_000 + 3_000),
        ):
            operations = 100
            rows.append(
                {
                    "implementation": implementation,
                    "revision": f"{implementation}-revision",
                    "contract_version": "prolly-compare-v1",
                    "records": str(records),
                    "phase": phase,
                    "workload": workload,
                    "operation": operation,
                    "operations": str(operations),
                    "elapsed_ns": str(ns_per_op * operations),
                    "ns_per_op": f"{ns_per_op:.3f}",
                    "ops_per_sec": f"{1_000_000_000 / ns_per_op:.3f}",
                    "workload_digest": GOLDEN[(phase, workload)],
                    "result_count": str(records),
                    "validated": "true",
                    "repetition": str(repetition),
                    "peak_rss_bytes": str(rss),
                }
            )
    return rows


def complete_matrix_rows():
    rows = []
    for records in SIZES:
        for phase in PHASES:
            for workload in WORKLOADS:
                for operation in OPERATIONS:
                    rows.extend(scenario_rows(records, phase, workload, operation))
    return rows


def write_csv(path: Path, rows):
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(rows[0]))
        writer.writeheader()
        writer.writerows(rows)


class MatrixValidationTest(unittest.TestCase):
    def test_computes_exact_statistics_and_winner(self):
        rows = scenario_rows()
        validate_matrix(rows, expected_runs=3, expected_sizes=[10_000], allow_partial=True)
        [summary] = summarize(rows)
        self.assertEqual(summary["rust_median_ns_per_op"], "20.000")
        self.assertEqual(summary["rust_min_ns_per_op"], "10.000")
        self.assertEqual(summary["rust_max_ns_per_op"], "30.000")
        self.assertEqual(summary["rust_cv"], "0.408248")
        self.assertEqual(summary["rust_median_peak_rss_bytes"], "2000")
        self.assertEqual(summary["rust_max_peak_rss_bytes"], "3000")
        self.assertEqual(summary["dolt_go_median_ns_per_op"], "50.000")
        self.assertEqual(summary["winner"], "rust")
        self.assertEqual(summary["winner_speedup"], "2.500000")

    def test_rejects_pair_semantic_mismatches(self):
        fields = [
            "workload_digest",
            "operations",
            "result_count",
            "validated",
            "contract_version",
        ]
        for field in fields:
            with self.subTest(field=field):
                rows = scenario_rows()
                target = next(row for row in rows if row["implementation"] == "dolt-go")
                target[field] = "false" if field == "validated" else "different"
                with self.assertRaisesRegex(BenchmarkValidationError, field):
                    validate_matrix(rows, 3, [10_000], allow_partial=True)

    def test_rejects_duplicate_missing_and_wrong_repetition_counts(self):
        rows = scenario_rows()
        with self.assertRaisesRegex(BenchmarkValidationError, "duplicate"):
            validate_matrix(rows + [deepcopy(rows[0])], 3, [10_000], allow_partial=True)

        missing_implementation = [
            row
            for row in rows
            if not (row["implementation"] == "dolt-go" and row["repetition"] == "1")
        ]
        with self.assertRaisesRegex(BenchmarkValidationError, "missing implementation"):
            validate_matrix(missing_implementation, 3, [10_000], allow_partial=True)

        too_few = [row for row in rows if row["repetition"] != "3"]
        with self.assertRaisesRegex(BenchmarkValidationError, "repetitions"):
            validate_matrix(too_few, 3, [10_000], allow_partial=True)

        too_many = rows + scenario_rows(runs=4)[-2:]
        with self.assertRaisesRegex(BenchmarkValidationError, "repetitions"):
            validate_matrix(too_many, 3, [10_000], allow_partial=True)

    def test_complete_matrix_has_exact_acceptance_counts(self):
        stats = validate_matrix(complete_matrix_rows(), 3, SIZES, allow_partial=False)
        self.assertEqual(stats.processes, 180)
        self.assertEqual(stats.rows, 540)
        self.assertEqual(stats.pairs, 270)

    def test_rejects_missing_required_scenario(self):
        rows = complete_matrix_rows()
        missing = [
            row
            for row in rows
            if not (
                row["records"] == "10000"
                and row["phase"] == "fresh"
                and row["workload"] == "append"
                and row["operation"] == "write"
            )
        ]
        with self.assertRaisesRegex(BenchmarkValidationError, "missing scenarios"):
            validate_matrix(missing, 3, SIZES, allow_partial=False)


class HistoricalComparisonTest(unittest.TestCase):
    def historical_fixture(self, directory: Path):
        history_summary = directory / "summary.csv"
        history_rows = [
            {
                "records": "10000",
                "phase": "fresh",
                "workload": "append",
                "operation": "write",
                "runs": "3",
                "rust_median_ns_per_op": "25.000",
                "dolt_go_median_ns_per_op": "70.000",
                "winner": "rust",
                "winner_speedup": "2.800",
            }
        ]
        write_csv(history_summary, history_rows)

        contract_rows = []
        for (phase, workload), digest in GOLDEN.items():
            row = scenario_rows(phase=phase, workload=workload, runs=1)[0]
            row.pop("contract_version")
            row.pop("repetition")
            row.pop("peak_rss_bytes")
            row["run"] = "1"
            row["workload_digest"] = digest
            contract_rows.append(row)
        write_csv(directory / "results.csv", contract_rows)
        return history_summary

    def test_history_delta_requires_matching_contract_and_writes_outputs(self):
        rows = scenario_rows()
        summaries = summarize(rows)
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            history_dir = root / "history"
            history_dir.mkdir()
            history = self.historical_fixture(history_dir)

            delta = compare_history(summaries, history)
            self.assertEqual(len(delta), 1)
            self.assertEqual(delta[0]["historical_rust_median_ns_per_op"], "25.000")
            self.assertEqual(delta[0]["current_rust_median_ns_per_op"], "20.000")
            self.assertEqual(delta[0]["improvement_percent"], "20.000")

            out = root / "out"
            write_outputs(rows, summaries, out, history)
            for name in (
                "summary.csv",
                "report.md",
                "historical-delta.csv",
                "historical-report.md",
            ):
                self.assertTrue((out / name).is_file(), name)

    def test_history_rejects_unknown_contract(self):
        summaries = summarize(scenario_rows())
        with tempfile.TemporaryDirectory() as temp:
            history_dir = Path(temp)
            history = self.historical_fixture(history_dir)
            with (history_dir / "results.csv").open(newline="") as handle:
                results = list(csv.DictReader(handle))
            results[0]["workload_digest"] = "0000000000000000"
            write_csv(history_dir / "results.csv", results)
            with self.assertRaisesRegex(BenchmarkValidationError, "historical workload contract"):
                compare_history(summaries, history)


if __name__ == "__main__":
    unittest.main()
