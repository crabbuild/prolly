from __future__ import annotations

import json
import pathlib
import sys
import tempfile
import unittest


SCRIPTS = pathlib.Path(__file__).resolve().parents[1]
sys.path.insert(0, str(SCRIPTS))

import verify_production_limits as verifier  # noqa: E402


class ProductionLimitsVerifierTests(unittest.TestCase):
    def setUp(self) -> None:
        self.limits = {
            "schema": verifier.SCHEMA,
            "request_budgets": {
                "put_object": {
                    "max_sdk_calls": 3,
                    "max_request_cost_usd_per_1000_logical_operations": 0.02,
                }
            },
            "profiles": {
                "test": {
                    "cost": {
                        "max_wire_retries_per_operation": 0,
                        "require_request_prices": False,
                    },
                    "contention": {
                        "logical_retry_limit": 16,
                        "tiers": {
                            "1": {
                                "max_p95_ms": 100,
                                "max_calls_per_write": 4,
                                "max_wire_retry_rate": 0.0,
                            }
                        },
                    },
                    "load": {
                        "provider": "aws",
                        "operations": {
                            "put_object": {
                                "min_samples": 10,
                                "max_p95_ms": 200,
                                "max_error_rate": 0.0,
                                "max_throttle_rate": 0.01,
                            }
                        },
                    },
                    "scale": {
                        "provider": "aws",
                        "min_live_keys": 100,
                        "min_retained_versions": 1000,
                        "max_cold_read_p95_ms": 200,
                        "min_bulk_files_per_second": 10,
                        "max_writable_reopen_seconds": 30,
                        "max_memory_bytes": 1024,
                    },
                }
            },
        }
        self.temporary_paths: list[pathlib.Path] = []

    def tearDown(self) -> None:
        for path in self.temporary_paths:
            path.unlink(missing_ok=True)

    def evidence(self, text: str) -> pathlib.Path:
        handle = tempfile.NamedTemporaryFile("w", encoding="utf-8", delete=False)
        with handle:
            handle.write(text)
        path = pathlib.Path(handle.name)
        self.temporary_paths.append(path)
        return path

    def test_checked_in_limits_are_valid(self) -> None:
        limits_path = SCRIPTS.parent / "qualification" / "production-limits-v1.json"
        limits = verifier.load_limits(limits_path)
        self.assertEqual(limits["schema"], verifier.SCHEMA)
        self.assertIn("aws-release", limits["profiles"])
        self.assertEqual(len(limits["request_budgets"]), 45)

    def test_cost_budget_passes_and_rejects_call_regression(self) -> None:
        passing = self.evidence(
            "OPERATION_COST operation=put_object sdk_calls=3 wire_transmissions=3 "
            "wire_retries=0 get=1 head=0 put=2 list=0 list_versions=0 delete=0\n"
        )
        output = verifier.verify_cost(passing, self.limits, "test")
        self.assertIn("operations=1", output)

        failing = self.evidence(
            "OPERATION_COST operation=put_object sdk_calls=4 wire_transmissions=4 "
            "wire_retries=0 get=1 head=0 put=3 list=0 list_versions=0 delete=0\n"
        )
        with self.assertRaisesRegex(verifier.LimitsError, "sdk_calls=4 exceeds 3"):
            verifier.verify_cost(failing, self.limits, "test")

    def test_cost_budget_rejects_missing_operation(self) -> None:
        empty = self.evidence("test result: ok\n")
        with self.assertRaisesRegex(verifier.LimitsError, "cost operation mismatch"):
            verifier.verify_cost(empty, self.limits, "test")

    def test_request_price_model_is_region_input_and_budgeted(self) -> None:
        evidence = self.evidence(
            "OPERATION_COST operation=put_object sdk_calls=3 wire_transmissions=3 "
            "wire_retries=0 get=1 head=0 put=2 list=0 list_versions=0 delete=0\n"
        )
        prices = self.evidence(
            json.dumps(
                {
                    "get": 0.001,
                    "head": 0.001,
                    "put": 0.005,
                    "list": 0.005,
                    "list_versions": 0.005,
                    "delete": 0.0,
                }
            )
        )
        output = verifier.verify_cost(evidence, self.limits, "test", prices)
        self.assertIn("request_prices=provided", output)

        expensive = self.evidence(
            json.dumps(
                {
                    "get": 0.01,
                    "head": 0.01,
                    "put": 0.01,
                    "list": 0.01,
                    "list_versions": 0.01,
                    "delete": 0.0,
                }
            )
        )
        with self.assertRaisesRegex(verifier.LimitsError, "modeled request cost"):
            verifier.verify_cost(evidence, self.limits, "test", expensive)

    def test_contention_budget_checks_retry_contract_latency_and_calls(self) -> None:
        passing = self.evidence(
            "CONTENTION_PROBE writers=1 logical_retry_limit=16 p95_ms=99 "
            "calls_per_write=4 wire_transmissions=4 wire_retries=0\n"
        )
        output = verifier.verify_contention(passing, self.limits, "test", {"1"})
        self.assertIn("writers=1", output)

        wrong_retry_limit = self.evidence(
            "CONTENTION_PROBE writers=1 logical_retry_limit=255 p95_ms=99 "
            "calls_per_write=4 wire_transmissions=4 wire_retries=0\n"
        )
        with self.assertRaisesRegex(verifier.LimitsError, "logical_retry_limit=255"):
            verifier.verify_contention(wrong_retry_limit, self.limits, "test", {"1"})

    def test_aws_load_and_scale_are_fail_closed(self) -> None:
        load = self.evidence(
            "LOAD_QUALIFICATION provider=aws operation=put_object samples=10 p95_ms=200 "
            "error_rate=0 throttle_rate=0.01 request_cost_usd_per_1000=0.02\n"
        )
        self.assertIn("operations=1", verifier.verify_load(load, self.limits, "test"))

        scale = self.evidence(
            "SCALE_QUALIFICATION provider=aws live_keys=100 retained_versions=1000 "
            "cold_read_p95_ms=200 bulk_files_per_second=10 writable_reopen_seconds=30 "
            "memory_bytes=1024\n"
        )
        self.assertIn("live_keys=100", verifier.verify_scale(scale, self.limits, "test"))

        too_small = self.evidence(
            "SCALE_QUALIFICATION provider=aws live_keys=99 retained_versions=1000 "
            "cold_read_p95_ms=200 bulk_files_per_second=10 writable_reopen_seconds=30 "
            "memory_bytes=1024\n"
        )
        with self.assertRaisesRegex(verifier.LimitsError, "live_keys is below"):
            verifier.verify_scale(too_small, self.limits, "test")


if __name__ == "__main__":
    unittest.main()
