import os
import pathlib
import subprocess
import tempfile
import unittest


ROOT = pathlib.Path(__file__).resolve().parents[2]
DRIVER = ROOT / "scripts" / "run_postgres_scale_benchmark.sh"


class DriverTests(unittest.TestCase):
    def test_smoke_driver_forwards_reproducible_contract(self):
        with tempfile.TemporaryDirectory() as directory:
            temp = pathlib.Path(directory)
            output = temp / "output"
            args_path = temp / "args.txt"
            python_args = temp / "python.txt"
            benchmark = temp / "benchmark"
            benchmark.write_text(
                "#!/bin/sh\nprintf '%s\\n' \"$@\" > \"$ARGS_PATH\"\n",
                encoding="utf-8",
            )
            benchmark.chmod(0o755)
            python = temp / "python"
            python.write_text(
                "#!/bin/sh\nprintf '%s\\n' \"$@\" > \"$PYTHON_ARGS\"\n",
                encoding="utf-8",
            )
            python.chmod(0o755)
            env = os.environ.copy()
            env.update(
                {
                    "BENCH_PROFILE": "smoke",
                    "BENCH_OUT": str(output),
                    "PROLLY_BENCH_SKIP_DOCKER": "1",
                    "PROLLY_BENCH_SKIP_BUILD": "1",
                    "PROLLY_BENCH_EXECUTABLE": str(benchmark),
                    "PYTHON_BIN": str(python),
                    "ARGS_PATH": str(args_path),
                    "PYTHON_ARGS": str(python_args),
                }
            )
            subprocess.run([str(DRIVER)], cwd=ROOT, env=env, check=True)
            arguments = args_path.read_text(encoding="utf-8").splitlines()
            for expected in ("smoke", str(output), "1000", "1"):
                self.assertIn(expected, arguments)
            self.assertTrue((output / "machine.txt").is_file())
            self.assertTrue((output / "run-manifest.txt").is_file())
            self.assertIn(str(output), python_args.read_text(encoding="utf-8"))

    def test_unified_driver_forwards_workload_and_baseline(self):
        with tempfile.TemporaryDirectory() as directory:
            temp = pathlib.Path(directory)
            output = temp / "output"
            args_path = temp / "args.txt"
            python_args = temp / "python.txt"
            benchmark = temp / "benchmark"
            benchmark.write_text(
                "#!/bin/sh\nprintf '%s\\n' \"$@\" > \"$ARGS_PATH\"\n"
                "touch \"$BENCH_OUT/service-raw.csv\"\n",
                encoding="utf-8",
            )
            benchmark.chmod(0o755)
            python = temp / "python"
            python.write_text(
                "#!/bin/sh\nprintf '%s\\n' \"$@\" > \"$PYTHON_ARGS\"\n",
                encoding="utf-8",
            )
            python.chmod(0o755)
            config = temp / "workload.toml"
            config.write_text("schema = 1\n", encoding="utf-8")
            baseline = temp / "baseline"
            env = os.environ.copy()
            env.update(
                {
                    "BENCH_OUT": str(output),
                    "PROLLY_BENCH_SKIP_DOCKER": "1",
                    "PROLLY_BENCH_SKIP_BUILD": "1",
                    "PROLLY_BENCH_EXECUTABLE": str(benchmark),
                    "PYTHON_BIN": str(python),
                    "ARGS_PATH": str(args_path),
                    "PYTHON_ARGS": str(python_args),
                }
            )
            subprocess.run(
                [
                    str(DRIVER),
                    "--config",
                    str(config),
                    "--suite",
                    "both",
                    "--baseline",
                    str(baseline),
                ],
                cwd=ROOT,
                env=env,
                check=True,
            )
            arguments = args_path.read_text(encoding="utf-8").splitlines()
            for expected in (
                "--config",
                str(config),
                "--suite",
                "both",
                "--baseline",
                str(baseline),
            ):
                self.assertIn(expected, arguments)
            summary_arguments = python_args.read_text(encoding="utf-8")
            self.assertIn("--service-input", summary_arguments)
            self.assertIn("--baseline-dir", summary_arguments)


if __name__ == "__main__":
    unittest.main()
