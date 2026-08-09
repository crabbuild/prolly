import os
import subprocess
import tempfile
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[2]
RUNNER = ROOT / "scripts" / "run_dynamodb_client_history_matrix.sh"


class HistoryMatrixRunnerTest(unittest.TestCase):
    def run_runner(self, *arguments, env=None):
        effective_env = os.environ.copy()
        effective_env.pop("BENCH_HISTORY_DEPTHS", None)
        effective_env.pop("BENCH_SAMPLES", None)
        if env:
            effective_env.update(env)
        return subprocess.run(
            [str(RUNNER), *arguments],
            cwd=ROOT,
            env=effective_env,
            text=True,
            capture_output=True,
        )

    def test_smoke_contract_is_exact_and_overrideable(self):
        result = self.run_runner("--profile", "smoke", "--print-config")
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertIn("history_depths=10,100,1000", result.stdout)
        self.assertIn("samples=1", result.stdout)
        self.assertIn("node_cache_max_bytes=67108864", result.stdout)

        overridden = self.run_runner(
            "--profile",
            "smoke",
            "--print-config",
            env={"BENCH_HISTORY_DEPTHS": "10,10000", "BENCH_SAMPLES": "3"},
        )
        self.assertEqual(overridden.returncode, 0, overridden.stderr)
        self.assertIn("history_depths=10,10000", overridden.stdout)
        self.assertIn("samples=3", overridden.stdout)

    def test_qualification_contract_cannot_be_narrowed(self):
        result = self.run_runner("--profile", "qualification", "--print-config")
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertIn("history_depths=10,100,1000,10000,100000", result.stdout)
        self.assertIn("samples=10", result.stdout)

        narrowed = self.run_runner(
            "--profile",
            "qualification",
            "--print-config",
            env={"BENCH_HISTORY_DEPTHS": "10,100"},
        )
        self.assertNotEqual(narrowed.returncode, 0)
        self.assertIn("cannot be overridden", narrowed.stderr)

    def test_rejects_noncanonical_depths_and_samples(self):
        for depths in ("9", "100,10", "10,10", "10,100001", "10,abc"):
            with self.subTest(depths=depths):
                result = self.run_runner(
                    "--profile",
                    "smoke",
                    "--print-config",
                    env={"BENCH_HISTORY_DEPTHS": depths},
                )
                self.assertNotEqual(result.returncode, 0)

        samples = self.run_runner(
            "--profile",
            "smoke",
            "--print-config",
            env={"BENCH_SAMPLES": "0"},
        )
        self.assertNotEqual(samples.returncode, 0)
        self.assertIn("BENCH_SAMPLES must be positive", samples.stderr)

    def test_rejects_tampered_completed_manifest_before_execution(self):
        revision = subprocess.run(
            ["git", "rev-parse", "HEAD"],
            cwd=ROOT,
            check=True,
            text=True,
            capture_output=True,
        ).stdout.strip()
        with tempfile.TemporaryDirectory() as directory:
            manifest = Path(directory) / "history-matrix-manifest.txt"
            manifest.write_text(
                "schema=dynamodb-client-history-matrix-v2\n"
                "profile=smoke\n"
                f"revision={revision}\n"
                "history_depths=10\n"
                "cases=99\n"
                "samples_per_case=1\n"
                "node_cache_max_bytes=67108864\n"
                "completed_utc=2026-01-01T00:00:00Z\n",
                encoding="utf-8",
            )
            result = self.run_runner(
                "--profile",
                "smoke",
                "--output",
                directory,
                env={"BENCH_HISTORY_DEPTHS": "10"},
            )
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("does not match this exact run", result.stderr)


if __name__ == "__main__":
    unittest.main()
