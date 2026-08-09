import os
import subprocess
import tempfile
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[2]
RUNNER = ROOT / "scripts" / "run_dynamodb_client_benchmark_matrix.sh"


class BenchmarkMatrixRunnerTest(unittest.TestCase):
    def run_runner(self, *arguments, env=None):
        effective_env = os.environ.copy()
        effective_env.pop("BENCH_SAMPLES", None)
        effective_env.pop("BENCH_NODE_CACHE_MAX_BYTES", None)
        if env:
            effective_env.update(env)
        return subprocess.run(
            [str(RUNNER), *arguments],
            cwd=ROOT,
            env=effective_env,
            text=True,
            capture_output=True,
        )

    def test_smoke_and_qualification_contracts_are_explicit(self):
        smoke = self.run_runner("--profile", "smoke", "--print-config")
        self.assertEqual(smoke.returncode, 0, smoke.stderr)
        self.assertIn("cases=smoke-1k,smoke-64k,smoke-near400k", smoke.stdout)
        self.assertIn("samples=1", smoke.stdout)
        self.assertIn("node_cache_max_bytes=67108864", smoke.stdout)

        qualification = self.run_runner(
            "--profile", "qualification", "--print-config"
        )
        self.assertEqual(qualification.returncode, 0, qualification.stderr)
        self.assertIn(
            "cases=10k-1k,10k-16k,10k-64k,10k-near400k,1m-1k",
            qualification.stdout,
        )
        self.assertIn("samples=100", qualification.stdout)

    def test_rejects_invalid_samples_and_cache_limit(self):
        zero_samples = self.run_runner(
            "--profile", "smoke", "--print-config", env={"BENCH_SAMPLES": "0"}
        )
        self.assertNotEqual(zero_samples.returncode, 0)

        invalid_cache = self.run_runner(
            "--profile",
            "smoke",
            "--print-config",
            env={"BENCH_NODE_CACHE_MAX_BYTES": "invalid"},
        )
        self.assertNotEqual(invalid_cache.returncode, 0)

    def test_rejects_tampered_completed_manifest_before_execution(self):
        revision = subprocess.run(
            ["git", "rev-parse", "HEAD"],
            cwd=ROOT,
            check=True,
            text=True,
            capture_output=True,
        ).stdout.strip()
        with tempfile.TemporaryDirectory() as directory:
            manifest = Path(directory) / "matrix-manifest.txt"
            manifest.write_text(
                "schema=dynamodb-client-matrix-v2\n"
                "profile=smoke\n"
                f"revision={revision}\n"
                "case_names=smoke-1k,smoke-64k,smoke-near400k\n"
                "cases=99\n"
                "samples_per_case=1\n"
                "node_cache_max_bytes=67108864\n"
                "completed_utc=2026-01-01T00:00:00Z\n",
                encoding="utf-8",
            )
            result = self.run_runner(
                "--profile", "smoke", "--output", directory
            )
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("does not match this exact run", result.stderr)


if __name__ == "__main__":
    unittest.main()
