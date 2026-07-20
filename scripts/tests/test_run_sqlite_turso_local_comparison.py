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
            self.assertNotIn("turso-cloud-sync", arguments)
            self.assertTrue((output / "machine.txt").is_file())
            self.assertIn(str(output), python_args.read_text(encoding="utf-8").splitlines())

    def test_driver_refuses_turso_cloud_sync_without_pipefail_false_negative(self):
        with tempfile.TemporaryDirectory() as directory:
            temp = pathlib.Path(directory)
            cargo = temp / "cargo"
            cargo.write_text(
                """#!/usr/bin/env python3
import sys

if sys.argv[1:] == ["--version"]:
    print("cargo 1.88.0 (fake)")
elif sys.argv[1:2] == ["tree"] and "-e" in sys.argv:
    print('prolly-store-turso feature "turso-cloud-sync"')
    for index in range(100_000):
        print(f"filler feature line {index}")
""",
                encoding="utf-8",
            )
            cargo.chmod(0o755)
            benchmark = temp / "benchmark"
            benchmark.write_text("#!/bin/sh\nexit 99\n", encoding="utf-8")
            benchmark.chmod(0o755)
            env = os.environ.copy()
            env.update({
                "PATH": f"{temp}{os.pathsep}{env['PATH']}",
                "PROLLY_BENCH_EXECUTABLE": str(benchmark),
            })

            result = subprocess.run(
                [str(DRIVER), "--profile", "smoke", "--output", str(temp / "output")],
                cwd=ROOT,
                env=env,
                capture_output=True,
                text=True,
            )

            self.assertEqual(result.returncode, 2, result.stderr)
            self.assertIn(
                "refusing to run: prolly-store-turso/turso-cloud-sync is enabled",
                result.stderr,
            )

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

    def test_cli_dimension_overrides_are_forwarded_to_summarizer(self):
        with tempfile.TemporaryDirectory() as directory:
            temp = pathlib.Path(directory)
            benchmark = temp / "benchmark"
            benchmark.write_text("#!/bin/sh\nexit 0\n", encoding="utf-8")
            benchmark.chmod(0o755)
            python_args = temp / "python-args.txt"
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
                "PYTHON_ARGS": str(python_args),
            })

            subprocess.run(
                [
                    str(DRIVER),
                    "--profile", "full",
                    "--output", str(temp / "output"),
                    "--sizes", "10000",
                    "--runs", "5",
                ],
                cwd=ROOT,
                env=env,
                check=True,
            )

            arguments = python_args.read_text(encoding="utf-8").splitlines()
            sizes_index = arguments.index("--sizes")
            runs_index = arguments.index("--runs")
            self.assertEqual(arguments[sizes_index + 1], "10000")
            self.assertEqual(arguments[runs_index + 1], "5")

    def test_driver_runs_binary_from_manifest_target_directory(self):
        with tempfile.TemporaryDirectory() as directory:
            temp = pathlib.Path(directory)
            fake_target = temp / "manifest-target"
            args_file = temp / "args.txt"
            cargo = temp / "cargo"
            cargo.write_text(
                """#!/usr/bin/env python3
import json
import os
import pathlib
import sys

args = sys.argv[1:]
target = pathlib.Path(os.environ["FAKE_TARGET"])
if args == ["--version"]:
    print("cargo 1.88.0 (fake)")
elif args[:1] == ["metadata"]:
    print(json.dumps({"target_directory": str(target)}))
elif args[:1] == ["build"]:
    binary = target / "release" / "prolly-sqlite-turso-local-bench"
    binary.parent.mkdir(parents=True, exist_ok=True)
    binary.write_text("#!/bin/sh\\nprintf '%s\\n' \\\"$@\\\" > \\\"$ARGS_FILE\\\"\\n", encoding="utf-8")
    binary.chmod(0o755)
elif args[:1] == ["tree"]:
    print("prolly-sqlite-turso-local-bench v0.0.0")
""",
                encoding="utf-8",
            )
            cargo.chmod(0o755)
            python = temp / "python"
            python.write_text("#!/bin/sh\nexit 0\n", encoding="utf-8")
            python.chmod(0o755)
            env = os.environ.copy()
            env.update({
                "PATH": f"{temp}{os.pathsep}{env['PATH']}",
                "FAKE_TARGET": str(fake_target),
                "ARGS_FILE": str(args_file),
                "PYTHON_BIN": str(python),
            })

            subprocess.run(
                [str(DRIVER), "--profile", "smoke", "--output", str(temp / "output")],
                cwd=ROOT,
                env=env,
                check=True,
            )

            self.assertTrue(args_file.is_file())
            self.assertIn("smoke", args_file.read_text(encoding="utf-8").splitlines())


if __name__ == "__main__":
    unittest.main()
