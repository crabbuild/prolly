import json
import os
import pathlib
import subprocess
import tempfile
import unittest


ROOT = pathlib.Path(__file__).resolve().parents[2]
DRIVER = ROOT / "scripts" / "run_turso_scale_benchmark.sh"


class DriverTests(unittest.TestCase):
    def test_smoke_driver_forwards_hardened_turso_dimensions(self):
        with tempfile.TemporaryDirectory() as directory:
            temp = pathlib.Path(directory)
            output = temp / "output"
            args_file = temp / "args.txt"
            benchmark = temp / "benchmark"
            benchmark.write_text(
                "#!/bin/sh\nprintf '%s\\n' \"$@\" > \"$ARGS_FILE\"\n",
                encoding="utf-8",
            )
            benchmark.chmod(0o755)
            env = os.environ.copy()
            env.update(
                {
                    "TURSO_BENCH_EXECUTABLE": str(benchmark),
                    "TURSO_BENCH_SKIP_BUILD": "1",
                    "TURSO_BENCH_TOKIO_WORKERS": "2",
                    "ARGS_FILE": str(args_file),
                }
            )
            subprocess.run(
                [str(DRIVER), "--profile", "smoke", "--output", str(output)],
                cwd=ROOT,
                env=env,
                check=True,
            )
            arguments = args_file.read_text(encoding="utf-8").splitlines()
            for value in (
                "smoke",
                str(output),
                "100",
                "1",
                "10",
                "put,batch,get_cold,get_warm,query,scan,full_scan,diff,merge",
                "append,random,clustered",
                "2",
            ):
                self.assertIn(value, arguments)
            for artifact in (
                "machine.txt",
                "source-status.txt",
                "source-diff.patch.gz",
                "harness-source.tar.gz",
                "harness-source.sha256",
                "dependencies.txt",
                "dependency-features.txt",
                "binary.sha256",
                "driver-provenance.txt",
                "run.log",
            ):
                self.assertTrue((output / artifact).is_file(), artifact)

    def test_driver_refuses_turso_cloud_sync(self):
        with tempfile.TemporaryDirectory() as directory:
            temp = pathlib.Path(directory)
            cargo = temp / "cargo"
            cargo.write_text(
                """#!/usr/bin/env python3
import sys
if sys.argv[1:] == ["--version"]:
    print("cargo 1.88.0 (fake)")
elif sys.argv[1:2] == ["tree"]:
    print('prolly-store-turso feature "turso-cloud-sync"')
""",
                encoding="utf-8",
            )
            cargo.chmod(0o755)
            benchmark = temp / "benchmark"
            benchmark.write_text("#!/bin/sh\nexit 99\n", encoding="utf-8")
            benchmark.chmod(0o755)
            env = os.environ.copy()
            env.update(
                {
                    "PATH": f"{temp}{os.pathsep}{env['PATH']}",
                    "TURSO_BENCH_EXECUTABLE": str(benchmark),
                    "TURSO_BENCH_SKIP_BUILD": "1",
                }
            )
            result = subprocess.run(
                [str(DRIVER), "--profile", "smoke", "--output", str(temp / "output")],
                cwd=ROOT,
                env=env,
                capture_output=True,
                text=True,
            )
            self.assertEqual(result.returncode, 2, result.stderr)
            self.assertIn("turso-cloud-sync is enabled", result.stderr)

    def test_driver_discovers_the_manifest_target_directory(self):
        with tempfile.TemporaryDirectory() as directory:
            temp = pathlib.Path(directory)
            target = temp / "target"
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
    binary = target / "release" / "prolly-turso-scale-bench"
    binary.parent.mkdir(parents=True, exist_ok=True)
    binary.write_text("#!/bin/sh\\nprintf '%s\\n' \\\"$@\\\" > \\\"$ARGS_FILE\\\"\\n", encoding="utf-8")
    binary.chmod(0o755)
elif args[:1] == ["tree"]:
    print("prolly-turso-scale-bench v0.0.0")
""",
                encoding="utf-8",
            )
            cargo.chmod(0o755)
            env = os.environ.copy()
            env.update(
                {
                    "PATH": f"{temp}{os.pathsep}{env['PATH']}",
                    "FAKE_TARGET": str(target),
                    "ARGS_FILE": str(args_file),
                }
            )
            subprocess.run(
                [str(DRIVER), "--profile", "smoke", "--output", str(temp / "output")],
                cwd=ROOT,
                env=env,
                check=True,
            )
            self.assertTrue(args_file.is_file())


if __name__ == "__main__":
    unittest.main()
