import csv
import importlib.util
import pathlib
import tempfile
import unittest


ROOT = pathlib.Path(__file__).resolve().parents[2]
SCRIPT = ROOT / "scripts" / "summarize_sqlite_turso_local_comparison.py"


def load_module():
    spec = importlib.util.spec_from_file_location("sqlite_turso_summary", SCRIPT)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


class SummarizerTests(unittest.TestCase):
    def make_output(self, missing=False):
        temp = tempfile.TemporaryDirectory()
        output = pathlib.Path(temp.name)
        (output / "run-manifest.txt").write_text(
            "schema=sqlite-turso-local-v1\n"
            "revision=abc\n"
            "dirty=false\n"
            "seed=1\n"
            "adapters=sqlite-sync,turso-async\n"
            "sizes=10000\n"
            "runs=2\n"
            "apis=batch\n"
            "patterns=random\n"
            "changes=automatic\n"
            "tokio_workers=2\n"
            "build_batch_size=50000\n",
            encoding="utf-8",
        )
        fields = [
            "schema", "revision", "dirty", "adapter", "records", "repetition",
            "api", "pattern", "configured_changes", "observed_changes", "total_ns",
            "operations_per_sec", "p50_ns", "p95_ns", "p99_ns", "max_ns",
            "db_bytes_before", "db_bytes_after", "expected_records", "observed_records",
            "validated", "error",
        ]
        rows = []
        for adapter, latency in (
            ("sqlite-sync", 1000),
            ("turso-async", 2000),
        ):
            for repetition in (1, 2):
                rows.append({
                    "schema": "sqlite-turso-local-v1", "revision": "abc", "dirty": "false",
                    "adapter": adapter, "records": 10000, "repetition": repetition,
                    "api": "batch", "pattern": "random", "configured_changes": 100,
                    "observed_changes": 100, "total_ns": latency * repetition,
                    "operations_per_sec": 100 * 1_000_000_000 / (latency * repetition), "p50_ns": "", "p95_ns": "",
                    "p99_ns": "", "max_ns": "", "db_bytes_before": 1,
                    "db_bytes_after": 2, "expected_records": 10000,
                    "observed_records": 10000, "validated": "true", "error": "",
                })
        if missing:
            rows.pop()
        with (output / "raw-results.csv").open("w", newline="", encoding="utf-8") as handle:
            writer = csv.DictWriter(handle, fieldnames=fields, lineterminator="\n")
            writer.writeheader()
            writer.writerows(rows)
        fixture_fields = [
            "schema", "revision", "dirty", "adapter", "records", "repetition",
            "build_ns", "records_per_sec", "database_bytes", "observed_records",
            "validated", "error",
        ]
        with (output / "fixture-results.csv").open("w", newline="", encoding="utf-8") as handle:
            writer = csv.DictWriter(handle, fieldnames=fixture_fields, lineterminator="\n")
            writer.writeheader()
            for adapter in ("sqlite-sync", "turso-async"):
                for repetition in (1, 2):
                    writer.writerow({
                        "schema": "sqlite-turso-local-v1", "revision": "abc", "dirty": "false",
                        "adapter": adapter, "records": 10000, "repetition": repetition,
                        "build_ns": 1000, "records_per_sec": 10_000_000_000.0,
                        "database_bytes": 4096, "observed_records": 10000,
                        "validated": "true", "error": "",
                    })
        return temp, output

    def test_aggregates_medians_and_ratio_direction(self):
        module = load_module()
        temp, output = self.make_output()
        self.addCleanup(temp.cleanup)
        rows = module.summarize(output)
        self.assertEqual(len(rows), 1)
        row = rows[0]
        self.assertEqual(row["sqlite_latency_median_ns"], 1500.0)
        self.assertEqual(row["turso_latency_median_ns"], 3000.0)
        self.assertEqual(row["turso_over_sqlite_latency_ratio"], 2.0)
        self.assertEqual(row["turso_over_sqlite_throughput_ratio"], 0.5)
        self.assertTrue((output / "summary.csv").is_file())
        self.assertTrue((output / "report.md").is_file())

    def test_rejects_an_incomplete_matrix(self):
        module = load_module()
        temp, output = self.make_output(missing=True)
        self.addCleanup(temp.cleanup)
        with self.assertRaisesRegex(ValueError, "incomplete benchmark matrix"):
            module.summarize(output)

    def test_rejects_corrupt_operations_and_incomplete_fixtures(self):
        module = load_module()
        temp, output = self.make_output()
        self.addCleanup(temp.cleanup)
        with (output / "raw-results.csv").open(encoding="utf-8") as handle:
            rows = list(csv.DictReader(handle))
        rows[0]["observed_changes"] = "99"
        with (output / "raw-results.csv").open("w", newline="", encoding="utf-8") as handle:
            writer = csv.DictWriter(handle, fieldnames=list(rows[0]), lineterminator="\n")
            writer.writeheader()
            writer.writerows(rows)
        with self.assertRaisesRegex(ValueError, "operation count"):
            module.summarize(output)

        temp2, output2 = self.make_output()
        self.addCleanup(temp2.cleanup)
        with (output2 / "fixture-results.csv").open(encoding="utf-8") as handle:
            fixture_rows = list(csv.DictReader(handle))
        with (output2 / "fixture-results.csv").open("w", newline="", encoding="utf-8") as handle:
            writer = csv.DictWriter(handle, fieldnames=list(fixture_rows[0]), lineterminator="\n")
            writer.writeheader()
            writer.writerows(fixture_rows[:-1])
        with self.assertRaisesRegex(ValueError, "incomplete fixture matrix"):
            module.summarize(output2)


if __name__ == "__main__":
    unittest.main()

