import csv
import importlib.util
import pathlib
import tempfile
import unittest


ROOT = pathlib.Path(__file__).resolve().parents[2]
SCRIPT = ROOT / "scripts" / "summarize_postgres_scale_benchmark.py"


def load_module():
    spec = importlib.util.spec_from_file_location("postgres_scale_summary", SCRIPT)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def row(repetition, total_ns, validated="true"):
    return {
        "schema": "postgres-scale-v1",
        "revision": "abc",
        "dirty": "true",
        "records": "1000",
        "repetition": str(repetition),
        "operation": "batch",
        "pattern": "random",
        "cache_state": "cold-manager",
        "sample_count": "1",
        "logical_operations": "100",
        "observed_items": "100",
        "total_ns": str(total_ns),
        "ns_per_op": str(total_ns / 100),
        "ops_per_sec": str(100 * 1_000_000_000 / total_ns),
        "validated": validated,
        "error": "" if validated == "true" else "failed",
    }


class SummarizerTests(unittest.TestCase):
    def test_aggregates_median_min_max_and_sample_count(self):
        module = load_module()
        rows = [row(1, 1000), row(2, 3000), row(3, 2000)]
        module.validate_rows(rows)
        summary = module.aggregate(rows)
        self.assertEqual(len(summary), 1)
        self.assertEqual(summary[0]["latency_median_ns"], 2000)
        self.assertEqual(summary[0]["latency_min_ns"], 1000)
        self.assertEqual(summary[0]["latency_max_ns"], 3000)
        self.assertEqual(summary[0]["repetitions"], 3)

    def test_rejects_failed_duplicate_and_inconsistent_rows(self):
        module = load_module()
        with self.assertRaisesRegex(ValueError, "failed cell"):
            module.validate_rows([row(1, 1000, validated="false")])
        with self.assertRaisesRegex(ValueError, "duplicate"):
            module.validate_rows([row(1, 1000), row(1, 1000)])
        broken = row(1, 1000)
        broken["ops_per_sec"] = "1"
        with self.assertRaisesRegex(ValueError, "throughput"):
            module.validate_rows([broken])

    def test_report_labels_single_sample_and_limitations(self):
        module = load_module()
        report = module.render_report(
            module.aggregate([row(1, 1000)]),
            [row(1, 1000)],
            {
                "changes": "300000",
                "read_samples": "10000",
                "merge_changes_semantics": "total_split_evenly",
                "random_merge_branch_distribution": "interleaved",
            },
        )
        self.assertIn("n=1", report)
        self.assertIn("Docker Desktop", report)
        self.assertIn("cold-manager", report)
        self.assertIn("300,000", report)
        self.assertIn("10,000", report)
        self.assertIn("150,000 changes per branch", report)
        self.assertIn("interleaved across both branches", report)

    def test_summary_csv_uses_repository_lf_line_endings(self):
        module = load_module()
        with tempfile.TemporaryDirectory() as directory:
            root = pathlib.Path(directory)
            input_path = root / "raw.csv"
            output_path = root / "output"
            sample = row(1, 1000)
            with input_path.open("w", newline="", encoding="utf-8") as handle:
                writer = csv.DictWriter(handle, fieldnames=list(sample))
                writer.writeheader()
                writer.writerow(sample)

            module.summarize(input_path, None, output_path, allow_partial=True)

            self.assertNotIn(b"\r\n", (output_path / "summary.csv").read_bytes())


if __name__ == "__main__":
    unittest.main()
