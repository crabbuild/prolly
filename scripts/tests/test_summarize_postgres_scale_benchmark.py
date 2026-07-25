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

def service_row(
    operation="point_read",
    tenant_class="independent",
    clients=8,
    pool_size=8,
    rate=1000.0,
    p99_ns=2_000_000,
    pg_calls=200,
):
    attempted = 1000
    completed = 1000
    duration_ns = 1_000_000_000
    return {
        "schema": "postgres-service-v1",
        "config_hash": "hash",
        "revision": "abc",
        "dirty": "false",
        "records": "1000",
        "value_bytes": "64",
        "clients": str(clients),
        "pool_size": str(pool_size),
        "operation": operation,
        "tenant_class": tenant_class,
        "duration_ns": str(duration_ns),
        "sample_count": str(attempted),
        "attempted": str(attempted),
        "completed": str(completed),
        "cell_attempted": str(attempted),
        "cell_completed": str(completed),
        "attempted_ops_per_sec": "1000.0",
        "successful_ops_per_sec": str(rate),
        "p50_ns": "1000000",
        "p95_ns": "1500000",
        "p99_ns": str(p99_ns),
        "p999_ns": str(max(p99_ns, 2500000)),
        "max_ns": str(max(p99_ns, 3000000)),
        "cas_attempts": "0",
        "conflicts": "0",
        "retries": "0",
        "exhausted_retries": "0",
        "semantic_conflicts": "0",
        "timeouts": "0",
        "sql_errors": "0",
        "validation_errors": "0",
        "worker_panics": "0",
        "prolly_node_cache_hits": "10",
        "prolly_node_cache_misses": "2",
        "prolly_node_cache_evictions": "0",
        "prolly_nodes_read": "2",
        "prolly_bytes_read": "200",
        "prolly_nodes_written": "0",
        "prolly_bytes_written": "0",
        "prolly_store_get_calls": "2",
        "prolly_store_batch_get_calls": "1",
        "prolly_store_batch_get_keys": "2",
        "prolly_store_put_calls": "0",
        "prolly_store_batch_put_calls": "0",
        "prolly_store_batch_put_nodes": "0",
        "pg_statement_calls": str(pg_calls),
        "pg_execution_ms": "1.0",
        "pg_shared_blks_hit": "1",
        "pg_shared_blks_read": "0",
        "pg_shared_blks_dirtied": "0",
        "pg_shared_blks_written": "0",
        "pg_temp_blks_read": "0",
        "pg_temp_blks_written": "0",
        "pg_wal_bytes": "0",
        "pg_commits": "1",
        "pg_rollbacks": "0",
        "database_bytes_before": "1",
        "database_bytes_after": "1",
        "prolly_table_bytes_before": "1",
        "prolly_table_bytes_after": "1",
        "prolly_index_bytes_before": "1",
        "prolly_index_bytes_after": "1",
        "validated": "true",
        "error": "",
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

    def test_service_summary_separates_tenant_classes(self):
        module = load_module()
        rows = [
            service_row(),
            service_row(tenant_class="hot"),
        ]
        summary = module.aggregate_service(rows)
        self.assertEqual(len(summary), 2)
        self.assertEqual(
            {item["tenant_class"] for item in summary},
            {"independent", "hot"},
        )
        self.assertEqual(summary[0]["prolly_nodes_read_per_operation"], 0.002)

    def test_service_gate_rejects_throughput_and_p99_regressions(self):
        module = load_module()
        baseline = [service_row(rate=1000.0, p99_ns=2_000_000)]
        current = [service_row(rate=800.0, p99_ns=3_000_000)]
        current[0]["completed"] = "800"
        current[0]["cell_completed"] = "800"
        environment = {"machine.txt": "same", "postgres.txt": "same"}
        with self.assertRaisesRegex(ValueError, "throughput|p99"):
            module.compare_service(
                current,
                baseline,
                {
                    "max_throughput_loss_percent": 10.0,
                    "max_p99_increase_percent": 20.0,
                    "minimum_percentile_samples": 1000,
                },
                environment,
                environment,
            )

    def test_service_gate_requires_same_configuration(self):
        module = load_module()
        baseline = [service_row()]
        current = [service_row()]
        current[0]["config_hash"] = "different"
        with self.assertRaisesRegex(ValueError, "configuration hash"):
            module.compare_service(current, baseline, {})

    def test_environment_ignores_capture_time_and_disk_availability(self):
        module = load_module()
        with tempfile.TemporaryDirectory() as directory:
            root = pathlib.Path(directory)
            (root / "machine.txt").write_text(
                "captured_utc=now\nhost\nrustc 1\n"
                "Filesystem Size Used Avail Capacity Mounted\n"
                "/dev/disk 1T 1G 999G 1% /tmp\n",
                encoding="utf-8",
            )
            (root / "postgres.txt").write_text("PostgreSQL 16\n", encoding="utf-8")
            self.assertEqual(
                module.read_environment(root),
                {
                    "machine.txt": "host\nrustc 1",
                    "postgres.txt": "PostgreSQL 16",
                },
            )


if __name__ == "__main__":
    unittest.main()
