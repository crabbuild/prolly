import os
import pathlib
import subprocess
import tempfile
import unittest


ROOT = pathlib.Path(__file__).resolve().parents[2]
DRIVER = ROOT / "scripts" / "run_sqlite_turso_local_comparison.sh"


class DriverTests(unittest.TestCase):
    def test_smoke_driver_invokes_local_binary_and_summarizer(self):
        with tempfile.TemporaryDirectory() as directory:
            temp = pathlib.Path(directory)
            output = temp / "output"
            args_file = temp / "args.txt"
            python_args = temp / "python-args.txt"
            benchmark = temp / "benchmark"
            benchmark.write_text(
                "#!/bin/sh\nprintf '%s\\n' \"$@\" > \"$ARGS_FILE\"\n",
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
            env.update({
                "PROLLY_BENCH_EXECUTABLE": str(benchmark),
                "PROLLY_BENCH_SKIP_BUILD": "1",
                "PYTHON_BIN": str(python),
                "ARGS_FILE": str(args_file),
                "PYTHON_ARGS": str(python_args),
            })
            subprocess.run(
                [str(DRIVER), "--profile", "smoke", "--output", str(output)],
                cwd=ROOT,
                env=env,
                check=True,
            )
            arguments = args_file.read_text(encoding="utf-8").splitlines()
            self.assertIn("smoke", arguments)
            self.assertIn(str(output), arguments)
            self.assertNotIn("sync", arguments)
            self.assertTrue((output / "machine.txt").is_file())
            self.assertIn(str(output), python_args.read_text(encoding="utf-8").splitlines())

    def test_environment_interface_selects_smoke_dimensions(self):
        with tempfile.TemporaryDirectory() as directory:
            temp = pathlib.Path(directory)
            benchmark = temp / "benchmark"
            args_file = temp / "args.txt"
            benchmark.write_text("#!/bin/sh\nprintf '%s\\n' \"$@\" > \"$ARGS_FILE\"\n", encoding="utf-8")
            benchmark.chmod(0o755)
            python = temp / "python"
            python.write_text("#!/bin/sh\nexit 0\n", encoding="utf-8")
            python.chmod(0o755)
            env = os.environ.copy()
            env.update({
                "BENCH_PROFILE": "smoke", "BENCH_OUT": str(temp / "output"),
                "BENCH_SIZES": "100", "BENCH_RUNS": "1", "BENCH_APIS": "put",
                "BENCH_PATTERNS": "append", "BENCH_ADAPTERS": "sqlite-sync",
                "BENCH_TOKIO_WORKERS": "2", "PROLLY_BENCH_EXECUTABLE": str(benchmark),
                "PROLLY_BENCH_SKIP_BUILD": "1", "PYTHON_BIN": str(python),
                "ARGS_FILE": str(args_file),
            })
            subprocess.run([str(DRIVER)], cwd=ROOT, env=env, check=True)
            arguments = args_file.read_text(encoding="utf-8").splitlines()
            for value in ("smoke", "100", "1", "put", "append", "sqlite-sync", "2"):
                self.assertIn(value, arguments)


if __name__ == "__main__":
    unittest.main()

