import csv
import re
import subprocess
import tempfile
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[2]
VALIDATOR = ROOT / "scripts" / "validate_dynamodb_client_benchmark.py"
FIXED = [
    ("GetItem", "warm"),
    ("PutItemIndexed", "warm"),
    ("QueryGsi", "warm"),
    ("PutItemBlob128KiB", "warm"),
    ("GetItemBlob128KiB", "warm"),
    ("Restore", "warm"),
    ("IndexPlan", "warm"),
    ("IndexApply", "warm"),
    ("IndexReplacePlan", "warm"),
    ("IndexReplaceApply", "warm"),
    ("IndexRemovePlan", "warm"),
    ("IndexRemoveApply", "warm"),
    ("HistoryVersionsAll", "warm"),
    ("HistoryGetOldest", "warm"),
    ("HistoryDiffOldestHead", "warm"),
    ("RetentionPlan", "warm"),
    ("RetentionApply", "warm"),
    ("GcPlan", "warm"),
    ("GcApply", "warm"),
    ("GetItemAt", "warm"),
    ("Query", "warm"),
    ("Scan", "warm"),
    ("BatchGetItem10", "warm"),
    ("TransactGetItems10", "warm"),
    ("GetItem", "cold"),
    ("BatchWriteItem10", "warm"),
    ("BatchWriteItem25", "warm"),
    ("PutItem", "warm"),
    ("Diff", "warm"),
    ("UpdateItem", "warm"),
    ("DeleteItem", "warm"),
    ("Versions", "warm"),
]
VARIABLE = [
    ("BatchGetItem100", "warm"),
    ("TransactGetItems100", "warm"),
    ("TransactWriteItems1", "warm"),
    ("TransactWriteItems10", "warm"),
    ("TransactWriteItems100", "warm"),
    ("ConcurrentPutItemW1O5", "warm"),
    ("ConcurrentPutItemW4O5", "warm"),
    ("ConcurrentPutItemW8O5", "warm"),
]
HISTORY = [
    ("HistoryAppendAll", "warm"),
    ("HistoryVersionsAll", "warm"),
    ("HistoryGetOldest", "warm"),
    ("HistoryDiffOldestHead", "warm"),
    ("RetentionPlan", "warm"),
    ("RetentionApply", "warm"),
]


def rolling_versions_created(operation, history_depth):
    if operation == "HistoryAppendAll":
        return history_depth
    if operation.startswith("BatchWriteItem"):
        return int(operation.removeprefix("BatchWriteItem"))
    if operation.startswith("TransactWriteItems"):
        return 1
    concurrent = re.fullmatch(r"ConcurrentPutItemW([1-9][0-9]*)O([1-9][0-9]*)", operation)
    if concurrent:
        return int(concurrent.group(1)) * int(concurrent.group(2))
    if operation in {
        "PutItem",
        "UpdateItem",
        "DeleteItem",
        "PutItemIndexed",
        "PutItemBlob128KiB",
        "IndexApply",
        "IndexReplaceApply",
        "IndexRemoveApply",
    }:
        return 1
    return 0


class WorkloadValidatorTest(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.path = Path(self.temp.name) / "raw.csv"
        self.manifest = Path(self.temp.name) / "run-manifest.txt"
        self.history_depth = 100
        self.workload = "full"
        self.write_manifest()

    def write_manifest(self):
        self.manifest.write_text(
            "schema=versioned-dynamodb-client-samples-v2\n"
            "runner_version=15\n"
            "revision=test-revision\n"
            "samples=1\n"
            "records=100\n"
            "value_bytes=1024\n"
            "read_batch_items=100\n"
            f"history_depth={self.history_depth}\n"
            f"workload={self.workload}\n"
            "teardown=namespace\n"
            "transaction_shapes=1,10,100\n"
            "concurrency_writers=1,4,8\n"
            "concurrency_operations_per_writer=5\n"
            "concurrency_retry_limit=7\n"
            "node_cache_max_bytes=67108864\n"
            "ended_utc=2026-01-01T00:00:00Z\n",
            encoding="utf-8",
        )
        cache_fields = [
            "schema", "revision", "sample", "client_role", "configured_max_bytes",
            "entries", "serialized_bytes", "pinned_entries", "pinned_serialized_bytes",
        ]
        with (self.path.parent / "cache-usage.csv").open(
            "w", newline="", encoding="utf-8"
        ) as target:
            writer = csv.DictWriter(target, fieldnames=cache_fields)
            writer.writeheader()
            writer.writerow({
                "schema": "versioned-dynamodb-client-cache-usage-v1",
                "revision": "test-revision",
                "sample": 0,
                "client_role": "primary",
                "configured_max_bytes": 67108864,
                "entries": 100,
                "serialized_bytes": 1024,
                "pinned_entries": 0,
                "pinned_serialized_bytes": 0,
            })
        gc_fields = [
            "schema", "revision", "sample", "max_protected_trees",
            "retained_roots", "protected_trees",
            "live_nodes", "live_node_bytes", "scanned_blob_nodes", "scanned_values",
            "live_blobs", "live_blob_bytes", "examined_node_candidates",
            "examined_blob_candidates",
        ]
        with (self.path.parent / "gc-reachability.csv").open(
            "w", newline="", encoding="utf-8"
        ) as target:
            writer = csv.DictWriter(target, fieldnames=gc_fields)
            writer.writeheader()
            writer.writerow({
                "schema": "versioned-dynamodb-client-gc-reachability-v2",
                "revision": "test-revision",
                "sample": 0,
                "max_protected_trees": 10_400,
                "retained_roots": 10,
                "protected_trees": 12,
                "live_nodes": 11,
                "live_node_bytes": 1024,
                "scanned_blob_nodes": 3,
                "scanned_values": 20,
                "live_blobs": 1,
                "live_blob_bytes": 128,
                "examined_node_candidates": 2,
                "examined_blob_candidates": 1,
            })

    def tearDown(self):
        self.temp.cleanup()

    def write(self, pairs):
        with self.path.open("w", newline="", encoding="utf-8") as target:
            writer = csv.DictWriter(
                target,
                fieldnames=[
                    "schema",
                    "revision",
                    "records",
                    "configured_value_bytes",
                    "validated",
                    "sample",
                    "operation",
                    "cache_mode",
                    "logical_input_item_bytes",
                    "logical_output_item_bytes",
                    "logical_item_bytes_complete",
                    "observed_items",
                    "versions_created",
                    "sdk_executions",
                    "http_attempts",
                    "sdk_retries",
                    "physical_response_bytes_complete",
                ],
            )
            writer.writeheader()
            for operation, cache in pairs:
                logical_input = 0
                logical_output = 0
                logical_complete = "true"
                if operation == "PutItemBlob128KiB":
                    logical_input = 128 * 1024 + 1
                elif operation == "GetItemBlob128KiB":
                    logical_output = 128 * 1024 + 1
                elif operation == "Restore":
                    logical_input = 64
                elif operation == "HistoryAppendAll":
                    logical_input = self.history_depth * 128
                elif operation.startswith("ConcurrentPutItem"):
                    logical_input = 1024
                elif operation in {
                    "IndexPlan",
                    "IndexApply",
                    "IndexReplacePlan",
                    "IndexReplaceApply",
                    "IndexRemovePlan",
                    "IndexRemoveApply",
                    "RetentionPlan",
                    "RetentionApply",
                    "GcPlan",
                    "GcApply",
                }:
                    logical_complete = "false"
                observed_items = 1
                if operation.startswith("BatchGetItem"):
                    observed_items = int(operation.removeprefix("BatchGetItem"))
                elif operation.startswith("TransactGetItems"):
                    observed_items = int(operation.removeprefix("TransactGetItems"))
                elif operation.startswith("BatchWriteItem"):
                    observed_items = int(operation.removeprefix("BatchWriteItem"))
                elif operation.startswith("ConcurrentPutItem"):
                    concurrent = re.fullmatch(
                        r"ConcurrentPutItemW([1-9][0-9]*)O([1-9][0-9]*)",
                        operation,
                    )
                    assert concurrent is not None
                    observed_items = int(concurrent.group(1)) * int(concurrent.group(2))
                elif operation in {
                    "IndexPlan",
                    "IndexApply",
                    "IndexReplacePlan",
                    "IndexReplaceApply",
                    "IndexRemovePlan",
                    "IndexRemoveApply",
                }:
                    observed_items = 2
                elif operation == "RetentionPlan":
                    observed_items = self.history_depth + 1
                elif operation == "HistoryVersionsAll":
                    observed_items = self.history_depth + 1
                elif operation == "HistoryAppendAll":
                    observed_items = self.history_depth
                elif operation == "RetentionApply":
                    observed_items = min(self.history_depth, 80)
                writer.writerow(
                    {
                        "schema": "versioned-dynamodb-client-samples-v2",
                        "revision": "test-revision",
                        "records": 100,
                        "configured_value_bytes": 1024,
                        "validated": "true",
                        "sample": 0,
                        "operation": operation,
                        "cache_mode": cache,
                        "logical_input_item_bytes": logical_input,
                        "logical_output_item_bytes": logical_output,
                        "logical_item_bytes_complete": logical_complete,
                        "observed_items": observed_items,
                        "versions_created": rolling_versions_created(
                            operation, self.history_depth
                        ),
                        "sdk_executions": 1,
                        "http_attempts": 1,
                        "sdk_retries": 0,
                        "physical_response_bytes_complete": "true",
                    }
                )

    def run_validator(self):
        return subprocess.run(
            [
                "python3",
                str(VALIDATOR),
                "--input",
                str(self.path),
                "--manifest",
                str(self.manifest),
                "--samples",
                "1",
                "--records",
                "100",
                "--value-bytes",
                "1024",
                "--read-batch-items",
                "100",
                "--history-depth",
                str(self.history_depth),
                "--workload",
                self.workload,
                "--revision",
                "test-revision",
                "--transaction-shapes",
                "1,10,100",
                "--concurrency-writers",
                "1,4,8",
                "--concurrency-operations-per-writer",
                "5",
                "--concurrency-retry-limit",
                "7",
                "--node-cache-max-bytes",
                "67108864",
            ],
            text=True,
            capture_output=True,
        )

    def test_accepts_exact_manifest(self):
        self.write(FIXED + VARIABLE)
        result = self.run_validator()
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertIn("rows=40", result.stdout)

    def test_accepts_shallow_history_and_exact_retention_bound(self):
        self.history_depth = 10
        self.write_manifest()
        self.write(FIXED + VARIABLE)
        result = self.run_validator()
        self.assertEqual(result.returncode, 0, result.stderr)

    def test_accepts_exact_history_only_workload(self):
        self.history_depth = 10
        self.workload = "history"
        self.write_manifest()
        self.write(HISTORY)
        result = self.run_validator()
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertIn("rows=6", result.stdout)

    def test_rejects_partial_manifest(self):
        self.write(FIXED)
        result = self.run_validator()
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("expected 40 rows", result.stderr)

    def test_rejects_missing_gc_reachability_evidence(self):
        self.write(FIXED + VARIABLE)
        (self.path.parent / "gc-reachability.csv").unlink()
        result = self.run_validator()
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("GC reachability evidence is unavailable", result.stderr)

    def test_rejects_missing_cache_usage_evidence(self):
        self.write(FIXED + VARIABLE)
        (self.path.parent / "cache-usage.csv").unlink()
        result = self.run_validator()
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("cache usage evidence is unavailable", result.stderr)

    def test_rejects_cache_usage_above_unpinned_byte_bound(self):
        self.write(FIXED + VARIABLE)
        cache_path = self.path.parent / "cache-usage.csv"
        cache_path.write_text(
            cache_path.read_text(encoding="utf-8").replace(
                ",100,1024,0,0", ",100,67108865,0,0"
            ),
            encoding="utf-8",
        )
        result = self.run_validator()
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("exceeds its configured byte bound", result.stderr)

    def test_rejects_inconsistent_pinned_cache_occupancy(self):
        self.write(FIXED + VARIABLE)
        cache_path = self.path.parent / "cache-usage.csv"
        cache_path.write_text(
            cache_path.read_text(encoding="utf-8").replace(
                ",100,1024,0,0", ",100,1024,1,0"
            ),
            encoding="utf-8",
        )
        result = self.run_validator()
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("inconsistent pinned occupancy", result.stderr)

    def test_rejects_corrupt_gc_reachability_evidence(self):
        self.write(FIXED + VARIABLE)
        gc_path = self.path.parent / "gc-reachability.csv"
        gc_path.write_text(
            gc_path.read_text(encoding="utf-8").replace(",10,12,11,", ",10,0,11,"),
            encoding="utf-8",
        )
        result = self.run_validator()
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("empty protected graph", result.stderr)

    def test_rejects_gc_graph_above_declared_limit(self):
        self.write(FIXED + VARIABLE)
        gc_path = self.path.parent / "gc-reachability.csv"
        gc_path.write_text(
            gc_path.read_text(encoding="utf-8").replace(",10400,10,12,", ",10,10,12,"),
            encoding="utf-8",
        )
        result = self.run_validator()
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("exceeds its declared tree bound", result.stderr)

    def test_rejects_unknown_teardown_mode(self):
        self.write_manifest()
        manifest = self.manifest.read_text(encoding="utf-8")
        self.manifest.write_text(
            manifest.replace("teardown=namespace", "teardown=shared-table-delete"),
            encoding="utf-8",
        )
        self.write(FIXED + VARIABLE)
        result = self.run_validator()
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("invalid teardown mode", result.stderr)

    def test_rejects_duplicate_substituted_for_missing_operation(self):
        pairs = FIXED + VARIABLE[:-1] + [("ConcurrentPutItemW4O5", "warm")]
        self.write(pairs)
        result = self.run_validator()
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("workload manifest mismatch", result.stderr)

    def test_rejects_manifest_provenance_mismatch(self):
        self.write(
            FIXED + VARIABLE
        )
        self.manifest.write_text(
            self.manifest.read_text(encoding="utf-8").replace(
                "revision=test-revision", "revision=other"
            ),
            encoding="utf-8",
        )
        result = self.run_validator()
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("run manifest mismatch", result.stderr)

    def test_rejects_semantically_corrupt_named_row(self):
        pairs = FIXED + VARIABLE
        self.write(pairs)
        with self.path.open(newline="", encoding="utf-8") as source:
            rows = list(csv.DictReader(source))
        for row in rows:
            if row["operation"] == "GetItemBlob128KiB":
                row["logical_output_item_bytes"] = "1"
        with self.path.open("w", newline="", encoding="utf-8") as target:
            writer = csv.DictWriter(target, fieldnames=rows[0].keys())
            writer.writeheader()
            writer.writerows(rows)

        result = self.run_validator()
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("blob get logical-byte evidence is invalid", result.stderr)

    def test_rejects_incomplete_history_enumeration(self):
        self.write(FIXED + VARIABLE)
        with self.path.open(newline="", encoding="utf-8") as source:
            rows = list(csv.DictReader(source))
        for row in rows:
            if row["operation"] == "HistoryVersionsAll":
                row["observed_items"] = str(self.history_depth)
        with self.path.open("w", newline="", encoding="utf-8") as target:
            writer = csv.DictWriter(target, fieldnames=rows[0].keys())
            writer.writeheader()
            writer.writerows(rows)

        result = self.run_validator()
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("enumerated an invalid history depth", result.stderr)

    def test_rejects_concurrency_without_effective_local_admission(self):
        self.write(FIXED + VARIABLE)
        with self.path.open(newline="", encoding="utf-8") as source:
            rows = list(csv.DictReader(source))
        for row in rows:
            if row["operation"] == "ConcurrentPutItemW8O5":
                row["sdk_executions"] = "1000"
                row["http_attempts"] = "1000"
        with self.path.open("w", newline="", encoding="utf-8") as target:
            writer = csv.DictWriter(target, fieldnames=rows[0].keys())
            writer.writeheader()
            writer.writerows(rows)

        result = self.run_validator()
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("admission amplification", result.stderr)


if __name__ == "__main__":
    unittest.main()
