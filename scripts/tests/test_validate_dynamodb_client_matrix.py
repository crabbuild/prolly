import csv
import subprocess
import tempfile
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[2]
VALIDATOR = ROOT / "scripts" / "validate_dynamodb_client_matrix.py"


class AggregateMatrixValidatorTest(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.directory = Path(self.temp.name)

    def tearDown(self):
        self.temp.cleanup()

    def run_validator(self, kind, expected_cases, history_shape=None):
        command = [
                "python3", str(VALIDATOR), "--kind", kind,
                "--input", str(self.directory / "matrix.csv"),
                "--manifest", str(self.directory / "manifest.txt"),
                "--profile", "smoke", "--revision", "revision",
                "--samples", "1", "--node-cache-max-bytes", "67108864",
                "--expected-cases", expected_cases,
            ]
        if history_shape is not None:
            command.extend(["--expected-history-shape", history_shape])
        return subprocess.run(
            command,
            text=True,
            capture_output=True,
        )

    def write_history(self):
        fields = [
            "schema", "matrix_profile", "history_depth", "samples", "records",
            "value_bytes", "read_batch_items", "transaction_shapes",
            "concurrency_writers", "concurrency_operations_per_writer",
            "concurrency_retry_limit", "node_cache_max_bytes", "revision",
            "result_dir", "status",
        ]
        with (self.directory / "matrix.csv").open(
            "w", newline="", encoding="utf-8"
        ) as target:
            writer = csv.DictWriter(target, fieldnames=fields)
            writer.writeheader()
            writer.writerow({
                "schema": "dynamodb-client-history-matrix-v2",
                "matrix_profile": "smoke", "history_depth": 10, "samples": 1,
                "records": 100, "value_bytes": 1024, "read_batch_items": 100,
                "transaction_shapes": "1,10", "concurrency_writers": "1",
                "concurrency_operations_per_writer": 1,
                "concurrency_retry_limit": 7, "node_cache_max_bytes": 67108864,
                "revision": "revision", "result_dir": "history-10",
                "status": "validated",
            })
        (self.directory / "manifest.txt").write_text(
            "schema=dynamodb-client-history-matrix-v2\nprofile=smoke\n"
            "revision=revision\nhistory_depths=10\ncases=1\nsamples_per_case=1\n"
            "node_cache_max_bytes=67108864\ncompleted_utc=2026-01-01T00:00:00Z\n",
            encoding="utf-8",
        )

    def write_full(self, extra_field=False):
        fields = [
            "schema", "matrix_profile", "case", "records", "value_bytes",
            "read_batch_items", "history_depth", "transaction_shapes",
            "concurrency_writers", "concurrency_operations_per_writer",
            "concurrency_retry_limit", "node_cache_max_bytes", "samples",
            "revision", "result_dir", "status",
        ]
        if extra_field:
            fields.append("unexpected")
        with (self.directory / "matrix.csv").open(
            "w", newline="", encoding="utf-8"
        ) as target:
            writer = csv.DictWriter(target, fieldnames=fields)
            writer.writeheader()
            row = {
                "schema": "dynamodb-client-matrix-v2",
                "matrix_profile": "smoke", "case": "small", "records": 100,
                "value_bytes": 1024, "read_batch_items": 100,
                "history_depth": 10, "transaction_shapes": "1,10",
                "concurrency_writers": "1,4", "concurrency_operations_per_writer": 5,
                "concurrency_retry_limit": 7, "node_cache_max_bytes": 67108864,
                "samples": 1, "revision": "revision", "result_dir": "small",
                "status": "validated",
            }
            if extra_field:
                row["unexpected"] = ""
            writer.writerow(row)
        (self.directory / "manifest.txt").write_text(
            "schema=dynamodb-client-matrix-v2\nprofile=smoke\nrevision=revision\n"
            "case_names=small\ncases=1\nsamples_per_case=1\n"
            "node_cache_max_bytes=67108864\ncompleted_utc=2026-01-01T00:00:00Z\n",
            encoding="utf-8",
        )

    def test_accepts_exact_full_matrix(self):
        self.write_full()
        result = self.run_validator("full", "small:100:1024:100:1,10")
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertIn("rows=1", result.stdout)

    def test_rejects_extra_csv_column(self):
        self.write_full(extra_field=True)
        result = self.run_validator("full", "small:100:1024:100:1,10")
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("invalid CSV schema", result.stderr)

    def test_rejects_trailing_row_value_without_a_header(self):
        self.write_full()
        matrix = self.directory / "matrix.csv"
        lines = matrix.read_text(encoding="utf-8").splitlines()
        lines[1] += ",unexpected"
        matrix.write_text("\n".join(lines) + "\n", encoding="utf-8")
        result = self.run_validator("full", "small:100:1024:100:1,10")
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("invalid CSV schema", result.stderr)

    def test_rejects_manifest_cache_drift(self):
        self.write_full()
        manifest = self.directory / "manifest.txt"
        manifest.write_text(
            manifest.read_text(encoding="utf-8").replace(
                "node_cache_max_bytes=67108864", "node_cache_max_bytes=1"
            ),
            encoding="utf-8",
        )
        result = self.run_validator("full", "small:100:1024:100:1,10")
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("manifest mismatch", result.stderr)

    def test_accepts_exact_history_matrix_and_rejects_shape_drift(self):
        self.write_history()
        shape = "100:1024:100:1,10:1:1:7"
        accepted = self.run_validator("history", "10", shape)
        self.assertEqual(accepted.returncode, 0, accepted.stderr)

        matrix = self.directory / "matrix.csv"
        matrix.write_text(
            matrix.read_text(encoding="utf-8").replace(",100,1024,", ",101,1024,"),
            encoding="utf-8",
        )
        rejected = self.run_validator("history", "10", shape)
        self.assertNotEqual(rejected.returncode, 0)
        self.assertIn("common shape mismatch", rejected.stderr)


if __name__ == "__main__":
    unittest.main()
