import csv
import os
import stat
import subprocess
import tempfile
import textwrap
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[2]
DRIVER = ROOT / "scripts" / "run_prolly_comparison.sh"


def executable(path: Path, content: str):
    path.write_text(textwrap.dedent(content).lstrip(), encoding="utf-8")
    path.chmod(path.stat().st_mode | stat.S_IXUSR)


class DriverBlackBoxTest(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.root = Path(self.temp.name)
        self.fake_bin = self.root / "bin"
        self.fake_bin.mkdir()
        self.dolt_origin = self.root / "dolt-origin"
        (self.dolt_origin / "go").mkdir(parents=True)
        (self.dolt_origin / "README.md").write_text("fixture\n", encoding="utf-8")
        subprocess.run(
            ["git", "init", "-b", "main"], cwd=self.dolt_origin, check=True, capture_output=True
        )
        subprocess.run(["git", "add", "."], cwd=self.dolt_origin, check=True)
        subprocess.run(
            [
                "git",
                "-c",
                "user.name=Benchmark Test",
                "-c",
                "user.email=benchmark@example.invalid",
                "commit",
                "-m",
                "fixture",
            ],
            cwd=self.dolt_origin,
            check=True,
            capture_output=True,
        )
        self.dolt_sha = subprocess.check_output(
            ["git", "rev-parse", "HEAD"], cwd=self.dolt_origin, text=True
        ).strip()
        self.fake_target = self.root / "target"
        self.fake_runner = self.root / "fake_runner.py"
        executable(
            self.fake_runner,
            r'''
            #!/usr/bin/env python3
            import os
            import sys

            implementation = sys.argv[1]
            args = dict(zip(sys.argv[2::2], sys.argv[3::2]))
            records = int(args["--records"])
            phase = args["--phase"]
            workload = args["--workload"]
            if os.environ.get("RAYON_NUM_THREADS") != "1" or os.environ.get("GOMAXPROCS") != "1":
                raise SystemExit("single-worker environment missing")
            if os.environ.get("FAKE_RUNNER_MODE") == "fail" and implementation == "dolt-go":
                raise SystemExit(17)
            if os.environ.get("FAKE_RUNNER_MODE") == "malformed" and implementation == "dolt-go":
                print("not,csv")
                raise SystemExit(0)
            digests = {
                ("fresh", "append"): "51f55fcd59187cbf",
                ("fresh", "random"): "004197dd790a1245",
                ("fresh", "clustered"): "86e38047f6ae04b3",
                ("mutation", "append"): "2ef1df79e1226620",
                ("mutation", "random"): "3bc7e45ef276a1c5",
                ("mutation", "clustered"): "5caed8dbd3056277",
            }
            writes = records if phase == "fresh" else records * 30 // 100
            if phase == "fresh":
                result_count = records
            elif workload == "append":
                result_count = records + writes
            else:
                result_count = records + writes // 2
            reads = min(100_000, records if phase == "fresh" else records + writes)
            revision = os.environ["BENCH_REVISION"]
            print("implementation,revision,contract_version,records,phase,workload,operation,operations,elapsed_ns,ns_per_op,ops_per_sec,workload_digest,result_count,validated")
            for operation, operations in (("write", writes), ("point_read", reads), ("range_scan", result_count)):
                ns = 10 if implementation == "rust" else 20
                elapsed = ns * operations
                print(f"{implementation},{revision},prolly-compare-v1,{records},{phase},{workload},{operation},{operations},{elapsed},{ns:.3f},{1_000_000_000/ns:.3f},{digests[(phase, workload)]},{result_count},true")
            ''',
        )
        executable(
            self.fake_bin / "cargo",
            r'''
            #!/bin/sh
            set -eu
            if [ "$1" = metadata ]; then
                printf '{"target_directory":"%s"}\n' "$FAKE_TARGET"
                exit 0
            fi
            if [ "$1" = build ]; then
                mkdir -p "$FAKE_TARGET/release"
                printf '#!/bin/sh\nexec python3 "%s" rust "$@"\n' "$FAKE_RUNNER" >"$FAKE_TARGET/release/prolly_compare"
                chmod +x "$FAKE_TARGET/release/prolly_compare"
                exit 0
            fi
            printf 'unexpected cargo invocation: %s\n' "$*" >&2
            exit 3
            ''',
        )
        executable(
            self.fake_bin / "go",
            r'''
            #!/bin/sh
            set -eu
            if [ "$1" = version ]; then
                printf 'go version go-test fixture\n'
                exit 0
            fi
            if [ "$1" = test ]; then
                test -f cmd/prolly-compare/main.go
                test -f cmd/prolly-compare/main_test.go
                grep -q 'contractVersion' cmd/prolly-compare/main.go
                exit 0
            fi
            if [ "$1" = build ]; then
                shift
                output=
                while [ "$#" -gt 0 ]; do
                    if [ "$1" = -o ]; then
                        output=$2
                        shift 2
                    else
                        shift
                    fi
                done
                test -n "$output"
                printf '#!/bin/sh\nexec python3 "%s" dolt-go "$@"\n' "$FAKE_RUNNER" >"$output"
                chmod +x "$output"
                exit 0
            fi
            printf 'unexpected go invocation: %s\n' "$*" >&2
            exit 4
            ''',
        )
        executable(
            self.fake_bin / "rustc",
            """
            #!/bin/sh
            printf 'rustc test fixture\\n'
            """,
        )
        executable(
            self.fake_bin / "time",
            r'''
            #!/bin/sh
            set +e
            mode=$1
            shift
            timing_output=
            if [ "$1" = -o ]; then
                timing_output=$2
                shift 2
            fi
            "$@"
            process_exit=$?
            if [ "$mode" = -l ]; then
                metric='123456 maximum resident set size'
            else
                metric='Maximum resident set size (kbytes): 121'
            fi
            if [ -n "$timing_output" ]; then
                printf '%s\n' "$metric" >"$timing_output"
            else
                printf '%s\n' "$metric" >&2
            fi
            exit "$process_exit"
            ''',
        )

    def tearDown(self):
        self.temp.cleanup()

    def run_driver(self, mode="ok"):
        output = self.root / f"output-{mode}"
        env = os.environ.copy()
        env.update(
            {
                "PATH": f"{self.fake_bin}:{env['PATH']}",
                "FAKE_TARGET": str(self.fake_target),
                "FAKE_RUNNER": str(self.fake_runner),
                "FAKE_RUNNER_MODE": mode,
                "DOLT_REPO_URL": self.dolt_origin.as_uri(),
                "DOLT_CACHE": str(self.root / f"dolt-cache-{mode}"),
                "BENCH_TIME_BIN": str(self.fake_bin / "time"),
                "BENCH_OUT": str(output),
                "BENCH_SIZES": "10000",
                "BENCH_RUNS": "1",
                "BENCH_LARGE_RUNS": "1",
                "BENCH_HISTORY_SUMMARY": "",
                "GIT_TRACE": str(self.root / f"git-{mode}.log"),
            }
        )
        completed = subprocess.run(
            [str(DRIVER)], cwd=ROOT, env=env, text=True, capture_output=True
        )
        return completed, output

    def test_current_main_build_smoke_and_matrix_are_reproducible(self):
        completed, output = self.run_driver()
        self.assertEqual(completed.returncode, 0, completed.stderr)

        with (output / "results.csv").open(newline="") as handle:
            rows = list(csv.DictReader(handle))
        self.assertEqual(len(rows), 36)
        self.assertTrue(all(row["validated"] == "true" for row in rows))
        self.assertTrue(all(row["peak_rss_bytes"] == "123456" for row in rows))
        self.assertEqual({row["repetition"] for row in rows}, {"1"})

        manifest = (output / "manifest.txt").read_text(encoding="utf-8")
        self.assertIn(f"dolt_commit={self.dolt_sha}", manifest)
        self.assertIn("rust_binary_sha256=", manifest)
        self.assertIn("dolt_binary_sha256=", manifest)
        self.assertIn("dolt_runner_sha256=", manifest)

        copied_runner = self.root / "dolt-cache-ok" / "go" / "cmd" / "prolly-compare" / "main.go"
        self.assertTrue(copied_runner.is_file())
        with (output / "manifest.csv").open(newline="") as handle:
            processes = list(csv.DictReader(handle))
        self.assertEqual(len(processes), 12)
        self.assertTrue(all(row["exit_status"] == "0" for row in processes))
        first_by_scenario = {}
        for row in processes:
            key = (row["records"], row["phase"], row["workload"], row["repetition"])
            first_by_scenario.setdefault(key, row["implementation"])
            self.assertTrue(Path(row["stdout"]).is_file())
            self.assertTrue(Path(row["stderr"]).is_file())
            self.assertTrue(Path(row["time"]).is_file())
        self.assertEqual(set(first_by_scenario.values()), {"rust", "dolt-go"})

        trace = (self.root / "git-ok.log").read_text(encoding="utf-8")
        self.assertEqual(trace.count("rev-parse origin/main"), 1)

    def test_nonzero_and_malformed_runner_output_fail_closed(self):
        failed, _ = self.run_driver("fail")
        self.assertNotEqual(failed.returncode, 0)
        self.assertIn("benchmark failed", failed.stderr)

        malformed, _ = self.run_driver("malformed")
        self.assertNotEqual(malformed.returncode, 0)
        self.assertIn("malformed runner CSV", malformed.stderr)


if __name__ == "__main__":
    unittest.main()
