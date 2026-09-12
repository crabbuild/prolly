import importlib.util
import json
from pathlib import Path
import sys
import tempfile
import unittest


SCRIPT = Path(__file__).resolve().parents[1] / "run_turboquant_qualification.py"
SPEC = importlib.util.spec_from_file_location("run_turboquant_qualification", SCRIPT)
qualification = importlib.util.module_from_spec(SPEC)
assert SPEC.loader is not None
sys.modules[SPEC.name] = qualification
SPEC.loader.exec_module(qualification)


def valid_output(cell, revision="a" * 40, workers=(1, 2, 4), repeats=2):
    preamble = qualification._expected_preamble(cell, revision, repeats)
    preamble.update(
        {
            "compiler": "rustc test",
            "target_arch": "test",
            "target_os": "test",
            "machine": "test",
            "seed": "0",
        }
    )
    lines = ["prolly proximity benchmark"]
    lines.extend(f"{key}={value}" for key, value in preamble.items())
    lines.append("operation,dimensions,threads,micros,metric_a,metric_b")
    counts = qualification._required_operation_counts(
        cell.environment.async_quantizers, workers
    )
    worker_operations = {
        "turboquant_build",
        "turboquant_build_resources",
        "pq_build",
        "turboquant_build_async",
        "turboquant_build_async_resources",
        "turboquant_build_async_publication",
        "pq_build_async",
        "pq_build_async_publication",
    }
    for operation, count in counts.items():
        operation_workers = workers if operation in worker_operations else (0,) * count
        for worker in operation_workers:
            metric_a = "1.0" if "recall" in operation else "10"
            physical = "2" if cell.environment.cold else "0"
            metric_b = physical if operation.endswith(("_p95", "_io")) else "20"
            lines.append(
                f"{operation},{cell.dimensions},{worker},1.0,{metric_a},{metric_b}"
            )
    return "\n".join(lines) + "\n"


class TurboQuantQualificationTests(unittest.TestCase):
    def test_full_matrix_is_complete_and_deterministic(self):
        first = qualification.enumerate_cells("full")
        second = qualification.enumerate_cells("full")
        self.assertEqual(len(first), 45_360)
        self.assertEqual(first, second)
        self.assertEqual(len({cell.identifier for cell in first}), len(first))
        self.assertEqual(
            {cell.records for cell in first}, {1_000, 10_000, 100_000, 1_000_000}
        )
        self.assertTrue(any(cell.rerank == "exhaustive" for cell in first))
        self.assertFalse(
            any(cell.rerank == "exhaustive" and cell.records > 10_000 for cell in first)
        )

    def test_hash_shards_are_disjoint_and_cover_matrix(self):
        cells = qualification.enumerate_cells("smoke")
        shards = [qualification.shard_cells(cells, index, 3) for index in range(3)]
        identifiers = [{cell.identifier for cell in shard} for shard in shards]
        self.assertEqual(set.union(*identifiers), {cell.identifier for cell in cells})
        self.assertFalse(identifiers[0] & identifiers[1])
        self.assertFalse(identifiers[0] & identifiers[2])
        self.assertFalse(identifiers[1] & identifiers[2])

    def test_resume_requires_exact_contract(self):
        cells = qualification.enumerate_cells("smoke")
        contract = qualification.make_contract(
            "smoke", "a" * 40, (1, 2, 4), 2, 0, 1, cells, cells
        )
        with tempfile.TemporaryDirectory() as directory:
            output = Path(directory)
            qualification.prepare_output(output, contract, False)
            qualification.prepare_output(output, contract, True)
            changed = dict(contract)
            changed["revision"] = "b" * 40
            with self.assertRaises(qualification.QualificationError):
                qualification.prepare_output(output, changed, True)
            manifest = json.loads((output / "manifest.json").read_text())
            self.assertEqual(manifest["contract"], contract)

    def test_output_validator_accepts_complete_sync_and_async_cells(self):
        revision = "a" * 40
        for cell in qualification.enumerate_cells("smoke"):
            qualification.validate_output(valid_output(cell), cell, revision, (1, 2, 4), 2)

    def test_output_validator_rejects_missing_rows_and_wrong_provenance(self):
        cell = qualification.enumerate_cells("smoke")[0]
        output = valid_output(cell)
        with self.assertRaises(qualification.QualificationError):
            qualification.validate_output(
                output.replace("revision=" + "a" * 40, "revision=" + "b" * 40),
                cell,
                "a" * 40,
                (1, 2, 4),
                2,
            )
        with self.assertRaises(qualification.QualificationError):
            qualification.validate_output(
                "\n".join(
                    line
                    for line in output.splitlines()
                    if not line.startswith("pq_recall,")
                ),
                cell,
                "a" * 40,
                (1, 2, 4),
                2,
            )

    def test_resume_revalidates_digest_and_rows(self):
        cell = qualification.enumerate_cells("smoke")[0]
        revision = "a" * 40
        raw = valid_output(cell)
        with tempfile.TemporaryDirectory() as directory:
            output = Path(directory)
            qualification.atomic_write(output / "raw" / f"{cell.identifier}.csv", raw)
            qualification.write_json(
                output / "state" / f"{cell.identifier}.json",
                {
                    "cell": qualification.asdict(cell),
                    "sha256": qualification.hashlib.sha256(raw.encode()).hexdigest(),
                },
            )
            self.assertTrue(
                qualification.completed_cell_is_valid(
                    output, cell, revision, (1, 2, 4), 2
                )
            )
            qualification.atomic_write(
                output / "raw" / f"{cell.identifier}.csv", raw + "corruption\n"
            )
            with self.assertRaises(qualification.QualificationError):
                qualification.completed_cell_is_valid(
                    output, cell, revision, (1, 2, 4), 2
                )

    def test_wasm_resume_requires_revision_digest_and_unskipped_pass(self):
        revision = "a" * 40
        test_log = "# tests 2\n# pass 2\n# fail 0\n# skipped 0\n"
        with tempfile.TemporaryDirectory() as directory:
            output = Path(directory)
            qualification.atomic_write(output / "wasm" / "test.log", test_log)
            qualification.write_json(
                output / "wasm" / "status.json",
                {
                    "revision": revision,
                    "test_sha256": qualification.hashlib.sha256(
                        test_log.encode()
                    ).hexdigest(),
                },
            )
            self.assertTrue(qualification.wasm_smoke_is_valid(output, revision))
            with self.assertRaises(qualification.QualificationError):
                qualification.wasm_smoke_is_valid(output, "b" * 40)
            qualification.atomic_write(
                output / "wasm" / "test.log",
                "# tests 2\n# pass 1\n# fail 0\n# skipped 1\n",
            )
            with self.assertRaises(qualification.QualificationError):
                qualification.wasm_smoke_is_valid(output, revision)


if __name__ == "__main__":
    unittest.main()
