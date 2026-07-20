import csv
import pathlib
import subprocess
import sys
import tempfile
import unittest


SCRIPT = pathlib.Path(__file__).parents[1] / "compose_node_publication_confirmation_gate.py"
FIELDS = [
    "suite",
    "revision_role",
    "pair",
    "revision",
    "adapter",
    "records",
    "api",
    "pattern",
    "run",
]


def row(pattern, role, pair, run=1):
    return {
        "suite": "local-adapters",
        "revision_role": role,
        "pair": pair,
        "revision": role,
        "adapter": "memory-sync",
        "records": 100,
        "api": "diff",
        "pattern": pattern,
        "run": run,
    }


class ConfirmationComposerTest(unittest.TestCase):
    def write_csv(self, path, rows):
        with path.open("w", newline="", encoding="utf-8") as handle:
            writer = csv.DictWriter(handle, fieldnames=FIELDS)
            writer.writeheader()
            writer.writerows(rows)

    def invoke(self, confirmations):
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        root = pathlib.Path(temporary.name)
        screen = root / "screen.csv"
        screen_rows = [
            row(pattern, role, 1)
            for pattern in ("append", "random")
            for role in ("baseline", "candidate")
        ]
        self.write_csv(screen, screen_rows)
        paths = []
        for index, rows in enumerate(confirmations):
            path = root / f"confirmation-{index}.csv"
            self.write_csv(path, rows)
            paths.append(path)
        command = [
            sys.executable,
            str(SCRIPT),
            "--screen",
            str(screen),
            "--output",
            str(root / "composed.csv"),
            "--sources-output",
            str(root / "sources.csv"),
        ]
        for path in paths:
            command.extend(["--confirmation", str(path)])
        result = subprocess.run(command, text=True, capture_output=True, check=False)
        return result, root

    def test_replaces_complete_groups_and_records_source_shape(self):
        confirmation = [
            row("append", role, pair, run)
            for pair in (1, 2)
            for role in ("baseline", "candidate")
            for run in (1, 2, 3)
        ]
        result, root = self.invoke([confirmation])
        self.assertEqual(result.returncode, 0, result.stderr)
        with (root / "composed.csv").open(newline="", encoding="utf-8") as handle:
            rows = list(csv.DictReader(handle))
        self.assertEqual(len(rows), 14)
        with (root / "sources.csv").open(newline="", encoding="utf-8") as handle:
            source = next(csv.DictReader(handle))
        self.assertEqual(source["pairs"], "2")
        self.assertEqual(source["samples_per_revision_pair"], "3")

    def test_rejects_overlapping_confirmation_groups(self):
        confirmation = [row("append", role, 1) for role in ("baseline", "candidate")]
        result, _ = self.invoke([confirmation, confirmation])
        self.assertEqual(result.returncode, 2)
        self.assertIn("overlaps", result.stderr)

    def test_rejects_incomplete_confirmation_pair(self):
        result, _ = self.invoke([[row("append", "baseline", 1)]])
        self.assertEqual(result.returncode, 2)
        self.assertIn("incomplete confirmation pair", result.stderr)

    def test_rejects_mismatched_confirmation_sample_identifiers(self):
        confirmation = [
            row("append", "baseline", 1, 1),
            row("append", "candidate", 1, 2),
        ]
        result, _ = self.invoke([confirmation])
        self.assertEqual(result.returncode, 2)
        self.assertIn("sample identifiers differ", result.stderr)


if __name__ == "__main__":
    unittest.main()
