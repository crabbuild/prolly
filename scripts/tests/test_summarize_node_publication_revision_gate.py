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
        candidate_latency=96,
        candidate_throughput=104,
        candidate_p95=105,
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
                        "total_ns": candidate_latency if candidate else 100,
                        "operations_per_sec": candidate_throughput if candidate else 100,
                        "p50_ns": candidate_latency if candidate else 100,
                        "p95_ns": candidate_p95 if candidate else 100,
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
            self.rows(candidate_latency=106, pairs=3), minimum_pairs=3
        )
        self.assertEqual(result.returncode, 2)
        self.assertIn("median_latency_regression", result.stderr)

    def test_rejects_latency_regression(self):
        result, _ = self.run_gate(self.rows(candidate_latency=106))
        self.assertEqual(result.returncode, 2)
        self.assertIn("median_latency_regression", result.stderr)

    def test_rejects_throughput_regression(self):
        result, _ = self.run_gate(self.rows(candidate_throughput=94))
        self.assertEqual(result.returncode, 2)
        self.assertIn("median_throughput_regression", result.stderr)

    def test_rejects_p95_regression(self):
        result, _ = self.run_gate(self.rows(candidate_p95=111))
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
        rows = self.rows(candidate_latency=70, candidate_throughput=140, candidate_p95=70)
        for row in rows:
            row["suite"] = "sqlite-turso"
            row["adapter"] = "turso-async"
            row["validated"] = "true"
        result, _ = self.run_gate(rows)
        self.assertEqual(result.returncode, 2)
        self.assertIn("turso_point_target_miss", result.stderr)


if __name__ == "__main__":
    unittest.main()
