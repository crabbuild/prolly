import csv
import pathlib
import subprocess
import sys
import tempfile
import unittest


SCRIPT = pathlib.Path(__file__).parents[1] / "summarize_node_publication_revision_gate.py"


class RevisionGateTest(unittest.TestCase):
    def run_gate(self, rows, limitations=None, minimum_pairs=5):
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        root = pathlib.Path(temporary.name)
        source = root / "raw.csv"
        with source.open("w", newline="", encoding="utf-8") as handle:
            writer = csv.DictWriter(handle, fieldnames=list(rows[0]))
            writer.writeheader()
            writer.writerows(rows)
        command = [
            sys.executable,
            str(SCRIPT),
            "--input",
            str(source),
            "--output-dir",
            str(root / "report"),
            "--minimum-pairs",
            str(minimum_pairs),
        ]
        if limitations:
            limitation_path = root / "limitations.csv"
            with limitation_path.open("w", newline="", encoding="utf-8") as handle:
                writer = csv.DictWriter(handle, fieldnames=["adapter", "reason"])
                writer.writeheader()
                writer.writerows(limitations)
            command.extend(["--environment-limitations", str(limitation_path)])
        result = subprocess.run(command, text=True, capture_output=True, check=False)
        return result, root

    def rows(
        self,
        *,
        candidate_latency=96_000,
        candidate_throughput=104,
        candidate_p95=105_000,
        pairs=5,
    ):
        rows = []
        for pair in range(1, pairs + 1):
            for role in ("baseline", "candidate"):
                candidate = role == "candidate"
                rows.append(
                    {
                        "suite": "local-adapters",
                        "revision_role": role,
                        "pair": pair,
                        "revision": role,
                        "adapter": "memory-sync",
                        "records": 10000,
                        "changes": 100,
                        "api": "put",
                        "pattern": "random",
                        "run": 1,
                        "total_ns": candidate_latency if candidate else 100_000,
                        "operations_per_sec": candidate_throughput if candidate else 100,
                        "p50_ns": candidate_latency if candidate else 100_000,
                        "p95_ns": candidate_p95 if candidate else 100_000,
                        "root": "canonical-root",
                        "node_count": 10,
                        "byte_count": 1000,
                        "value_valid": "true",
                        "count_valid": "true",
                        "root_valid": "true",
                        "reopen_valid": "true",
                    }
                )
        return rows

    def test_passes_within_directional_limits(self):
        result, _ = self.run_gate(self.rows())
        self.assertEqual(result.returncode, 0, result.stderr)

    def test_applies_directional_gates_at_explicit_three_pair_minimum(self):
        result, _ = self.run_gate(
            self.rows(candidate_latency=106_000, pairs=3), minimum_pairs=3
        )
        self.assertEqual(result.returncode, 2)
        self.assertIn("median_latency_regression", result.stderr)

    def test_uses_alternating_pair_changes_for_directional_gate(self):
        rows = self.rows()
        baseline_latencies = [100, 200, 300, 400, 500]
        candidate_latencies = [100, 200, 3000, 4000, 500]
        for row in rows:
            pair = row["pair"] - 1
            row["total_ns"] = (
                candidate_latencies[pair]
                if row["revision_role"] == "candidate"
                else baseline_latencies[pair]
            )
            row["p50_ns"] = 100
            row["p95_ns"] = 100
            row["operations_per_sec"] = 100
        result, root = self.run_gate(rows)
        self.assertEqual(result.returncode, 0, result.stderr)
        with (root / "report" / "summary.csv").open(newline="", encoding="utf-8") as handle:
            summary = next(csv.DictReader(handle))
        self.assertGreater(float(summary["median_latency_change_pct"]), 5.0)
        self.assertEqual(float(summary["paired_median_latency_change_pct"]), 0.0)

    def test_collapses_repeated_samples_inside_each_revision_pair(self):
        rows = []
        for row in self.rows(candidate_latency=100):
            for sample, latency in enumerate((100, 100, 10_000), start=1):
                repeated = row.copy()
                repeated["run"] = sample
                repeated["total_ns"] = latency
                repeated["p50_ns"] = latency
                repeated["p95_ns"] = latency
                repeated["operations_per_sec"] = 100
                if repeated["revision_role"] == "candidate":
                    repeated["total_ns"] = latency * 1.04
                    repeated["p50_ns"] = latency * 1.04
                    repeated["p95_ns"] = latency * 1.04
                rows.append(repeated)
        result, root = self.run_gate(rows)
        self.assertEqual(result.returncode, 0, result.stderr)
        with (root / "report" / "summary.csv").open(newline="", encoding="utf-8") as handle:
            summary = next(csv.DictReader(handle))
        self.assertEqual(summary["samples_per_revision_pair"], "3")
        self.assertAlmostEqual(
            float(summary["paired_median_latency_change_pct"]), 4.0
        )

    def test_rejects_mismatched_sample_counts(self):
        rows = self.rows()
        extra = rows[0].copy()
        extra["run"] = 2
        rows.append(extra)
        result, _ = self.run_gate(rows)
        self.assertEqual(result.returncode, 2)
        self.assertIn("sample_count_mismatch", result.stderr)

    def test_rejects_mismatched_sample_identifiers(self):
        rows = self.rows()
        candidate = next(
            row
            for row in rows
            if row["pair"] == 1 and row["revision_role"] == "candidate"
        )
        candidate["run"] = 2
        result, _ = self.run_gate(rows)
        self.assertEqual(result.returncode, 2)
        self.assertIn("sample_id_mismatch", result.stderr)

    def test_rejects_latency_regression(self):
        result, _ = self.run_gate(self.rows(candidate_latency=106_000))
        self.assertEqual(result.returncode, 2)
        self.assertIn("median_latency_regression", result.stderr)

    def test_rejects_throughput_regression(self):
        result, _ = self.run_gate(
            self.rows(candidate_latency=106_000, candidate_throughput=94)
        )
        self.assertEqual(result.returncode, 2)
        self.assertIn("median_throughput_regression", result.stderr)

    def test_rejects_p95_regression(self):
        result, _ = self.run_gate(self.rows(candidate_p95=111_000))
        self.assertEqual(result.returncode, 2)
        self.assertIn("p95_latency_regression", result.stderr)

    def test_rejects_missing_pair(self):
        rows = self.rows()
        rows.pop()
        result, _ = self.run_gate(rows)
        self.assertEqual(result.returncode, 2)
        self.assertIn("missing_pair", result.stderr)

    def test_rejects_validation_failure(self):
        rows = self.rows()
        rows[0]["root_valid"] = "false"
        result, _ = self.run_gate(rows)
        self.assertEqual(result.returncode, 2)
        self.assertIn("fixture_validation_failure", result.stderr)

    def test_records_environment_limitation(self):
        result, root = self.run_gate(
            self.rows(),
            [{"adapter": "pglite-sync", "reason": "Node package unavailable"}],
        )
        self.assertEqual(result.returncode, 0, result.stderr)
        report = (root / "report" / "report.md").read_text(encoding="utf-8")
        self.assertIn("pglite-sync", report)
        self.assertIn("Node package unavailable", report)

    def test_rejects_turso_point_target_miss(self):
        rows = self.rows(
            candidate_latency=70_000,
            candidate_throughput=140,
            candidate_p95=70_000,
        )
        for row in rows:
            row["suite"] = "sqlite-turso"
            row["adapter"] = "turso-async"
            row["validated"] = "true"
        result, _ = self.run_gate(rows)
        self.assertEqual(result.returncode, 2)
        self.assertIn("turso_point_target_miss", result.stderr)

    def test_rejects_turso_point_percentile_regression(self):
        rows = self.rows(
            candidate_latency=50_000,
            candidate_throughput=200,
            candidate_p95=101_000,
        )
        for row in rows:
            row["suite"] = "sqlite-turso"
            row["adapter"] = "turso-async"
            row["validated"] = "true"
            if row["revision_role"] == "candidate":
                row["p50_ns"] = 101_000
        result, _ = self.run_gate(rows)
        self.assertEqual(result.returncode, 2)
        self.assertIn("turso_point_p50_regression", result.stderr)
        self.assertIn("turso_point_p95_regression", result.stderr)

    def test_local_adapter_noise_floor_ignores_sub_microsecond_change(self):
        rows = self.rows(candidate_latency=100_500, candidate_throughput=94)
        result, root = self.run_gate(rows)
        self.assertEqual(result.returncode, 0, result.stderr)
        with (root / "report" / "gate.csv").open(newline="", encoding="utf-8") as handle:
            gate = next(csv.DictReader(handle))
        self.assertEqual(gate["status"], "pass")
        self.assertEqual(float(gate["paired_median_latency_change_ns"]), 500.0)


if __name__ == "__main__":
    unittest.main()
