import importlib.util
from pathlib import Path
import sys
import unittest


SCRIPT = Path(__file__).resolve().parents[1] / "summarize_turboquant_qualification.py"
SPEC = importlib.util.spec_from_file_location("summarize_turboquant_qualification", SCRIPT)
summary = importlib.util.module_from_spec(SPEC)
assert SPEC.loader is not None
sys.modules[SPEC.name] = summary
SPEC.loader.exec_module(summary)


class TurboQuantSummaryTests(unittest.TestCase):
    def contracts(self, shard_count=3):
        cells = summary.runner.enumerate_cells("smoke")
        contracts = []
        for index in range(shard_count):
            selected = summary.runner.shard_cells(cells, index, shard_count)
            contracts.append(
                summary.runner.make_contract(
                    "smoke",
                    "a" * 40,
                    (1, 2, 4),
                    2,
                    index,
                    shard_count,
                    cells,
                    selected,
                )
            )
        return contracts

    def test_contract_set_requires_every_unique_matching_shard(self):
        contracts = self.contracts()
        self.assertEqual(
            [contract["shard_index"] for contract in summary.validate_contract_set(contracts)],
            [0, 1, 2],
        )
        with self.assertRaises(summary.SummaryError):
            summary.validate_contract_set(contracts[:2])
        with self.assertRaises(summary.SummaryError):
            summary.validate_contract_set([contracts[0], contracts[0], contracts[2]])
        changed = dict(contracts[1])
        changed["revision"] = "b" * 40
        with self.assertRaises(summary.SummaryError):
            summary.validate_contract_set([contracts[0], changed, contracts[2]])

    def auto_rows(self):
        rows = []
        for dimensions in (768, 1536):
            for metric in summary.runner.FULL_METRICS:
                records = 100_000
                rows.append(
                    {
                        "cell_id": f"{dimensions}-{metric}",
                        "records": records,
                        "dimensions": dimensions,
                        "metric": metric,
                        "requested_k": 10,
                        "effective_k": 10,
                        "eligibility_ppm": 1_000_000,
                        "bits": 4,
                        "rerank_kind": "fixed",
                        "rerank_multiplier": 8,
                        "environment": "memory-warm-sync",
                        "turboquant_recall": 0.98,
                        "pq_recall": 0.98,
                        "turboquant_candidate_peak": 80,
                        "_transformed_components": records * dimensions,
                        "turboquant_encoded_bytes": records
                        * (8 + (dimensions * 4 + 7) // 8),
                        "turboquant_build_micros": 90.0,
                        "pq_build_micros": 100.0,
                        "turboquant_search_p95_micros": 90.0,
                        "pq_search_p95_micros": 100.0,
                        "turboquant_sidecar_bytes": 70,
                        "pq_sidecar_bytes": 100,
                    }
                )
        return rows

    def test_gate_evaluation_separates_forced_and_auto_results(self):
        gates = summary.evaluate_gates(self.auto_rows(), "full")
        self.assertTrue(gates["forced_matrix_qualified"])
        self.assertTrue(gates["auto_qualified"])
        self.assertEqual(gates["auto_scope_rows"], 6)

        rows = self.auto_rows()
        rows[0]["turboquant_recall"] = 0.90
        failed = summary.evaluate_gates(rows, "full")
        self.assertFalse(failed["forced_matrix_qualified"])
        self.assertFalse(failed["auto_qualified"])
        self.assertEqual(failed["forced_matrix_failures"]["recall_floor"], ["768-l2"])

    def test_smoke_never_claims_ga_or_auto_qualification(self):
        gates = summary.evaluate_gates([], "smoke")
        self.assertTrue(gates["matrix_complete"])
        self.assertFalse(gates["forced_matrix_qualified"])
        self.assertFalse(gates["auto_qualified"])


if __name__ == "__main__":
    unittest.main()
