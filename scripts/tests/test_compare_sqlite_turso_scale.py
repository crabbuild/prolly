import csv
import pathlib
import subprocess
import tempfile
import unittest


ROOT = pathlib.Path(__file__).resolve().parents[2]
SCRIPT = ROOT / "scripts" / "compare_sqlite_turso_scale.py"


def write_csv(path, fieldnames, rows):
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as output:
        writer = csv.DictWriter(output, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


class ComparisonTests(unittest.TestCase):
    def make_baseline(self, root, revision, timings):
        summary_fields = [
            "records",
            "operation",
            "pattern",
            "cache_state",
            "repetitions",
            "median_total_ns",
            "min_total_ns",
            "max_total_ns",
            "median_ns_per_operation",
            "median_operations_per_sec",
            "min_operations_per_sec",
            "max_operations_per_sec",
        ]
        rows = []
        for operation, pattern, ns_per_operation in timings:
            rows.append(
                {
                    "records": "1000000",
                    "operation": operation,
                    "pattern": pattern,
                    "cache_state": "not-applicable",
                    "repetitions": "3",
                    "median_total_ns": str(int(ns_per_operation * 10)),
                    "min_total_ns": "1",
                    "max_total_ns": "2",
                    "median_ns_per_operation": str(ns_per_operation),
                    "median_operations_per_sec": str(1e9 / ns_per_operation),
                    "min_operations_per_sec": "1",
                    "max_operations_per_sec": "2",
                }
            )
        write_csv(root / "summary.csv", summary_fields, rows)
        write_csv(
            root / "fixture-results.csv",
            ["build_ns", "database_bytes", "validated"],
            [
                {"build_ns": "900000000", "database_bytes": "104857600", "validated": "true"},
                {"build_ns": "1000000000", "database_bytes": "105906176", "validated": "true"},
                {"build_ns": "1100000000", "database_bytes": "106954752", "validated": "true"},
            ],
        )
        (root / "run-manifest.txt").write_text(
            f"revision={revision}\ndirty=true\nsizes=1000000\nruns=3\n",
            encoding="utf-8",
        )
        (root / "machine.txt").write_text("Apple M2 Max\nrustc 1.97.0\n", encoding="utf-8")

    def test_writes_ratio_table_and_revision_caveat(self):
        with tempfile.TemporaryDirectory() as directory:
            temp = pathlib.Path(directory)
            sqlite = temp / "sqlite"
            turso = temp / "turso"
            self.make_baseline(sqlite, "sqlite-rev", [("put", "append", 300.0), ("scan", "random", 100.0)])
            self.make_baseline(turso, "turso-rev", [("put", "append", 200.0), ("scan", "random", 200.0)])
            output = temp / "comparison.md"

            subprocess.run(
                ["python3", str(SCRIPT), "--sqlite-dir", str(sqlite), "--turso-dir", str(turso), "--output", str(output)],
                check=True,
            )

            report = output.read_text(encoding="utf-8")
            self.assertIn("Turso is faster in 1 of 2 cells", report)
            self.assertIn("| 1000000 | put | append | n/a | 300.0 | 200.0 | -33.3% | 1.500x |", report)
            self.assertIn("| 1000000 | scan | random | n/a | 100.0 | 200.0 | +100.0% | 0.500x |", report)
            self.assertIn("sqlite-rev", report)
            self.assertIn("turso-rev", report)
            self.assertIn("not a strict causal A/B", report)

    def test_rejects_mismatched_workload_cells(self):
        with tempfile.TemporaryDirectory() as directory:
            temp = pathlib.Path(directory)
            sqlite = temp / "sqlite"
            turso = temp / "turso"
            self.make_baseline(sqlite, "same", [("put", "append", 300.0)])
            self.make_baseline(turso, "same", [("scan", "append", 200.0)])

            result = subprocess.run(
                ["python3", str(SCRIPT), "--sqlite-dir", str(sqlite), "--turso-dir", str(turso), "--output", str(temp / "out.md")],
                capture_output=True,
                text=True,
            )

            self.assertEqual(result.returncode, 2)
            self.assertIn("workload cells differ", result.stderr)


if __name__ == "__main__":
    unittest.main()
