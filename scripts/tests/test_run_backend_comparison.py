import os
import pathlib
import stat
import subprocess
import tempfile
import textwrap
import unittest


ROOT = pathlib.Path(__file__).parents[2]
DRIVER = ROOT / "scripts" / "run_backend_comparison.sh"


class BackendComparisonDriverTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.root = pathlib.Path(self.temp.name)
        self.bin = self.root / "bin"
        self.bin.mkdir()
        self.log = self.root / "commands.log"
        self._write_fakes()

    def tearDown(self):
        self.temp.cleanup()

    def run_driver(self, *, output=None, runs="7", dirty="", fail_repetition=""):
        output = output or self.root / "result"
        env = os.environ.copy()
        env.update(
            {
                "PATH": f"{self.bin}:{env['PATH']}",
                "BENCH_OUT": str(output),
                "BENCH_RECORDS": "100",
                "BENCH_CHANGES": "10",
                "BENCH_SAMPLES": "10",
                "BENCH_RUNS": runs,
                "BENCH_SKIP_BUILD": "1",
                "BENCH_POSTGRES_EXECUTABLE": str(self.bin / "postgres-runner"),
                "BENCH_DYNAMODB_EXECUTABLE": str(self.bin / "dynamodb-runner"),
                "BENCH_SUMMARIZER_EXECUTABLE": str(self.bin / "summarizer"),
                "BENCH_GIT_BIN": str(self.bin / "git"),
                "BENCH_DOCKER_BIN": str(self.bin / "docker"),
                "BENCH_CURL_BIN": str(self.bin / "curl"),
                "FAKE_LOG": str(self.log),
                "FAKE_GIT_STATUS": dirty,
                "FAKE_FAIL_REPETITION": fail_repetition,
            }
        )
        return subprocess.run(
            [str(DRIVER)],
            cwd=ROOT,
            env=env,
            text=True,
            capture_output=True,
            check=False,
        )

    def test_refuses_existing_output(self):
        output = self.root / "existing"
        output.mkdir()
        result = self.run_driver(output=output)
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("refusing to overwrite", result.stderr)

    def test_requires_seven_repetitions(self):
        result = self.run_driver(runs="6")
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("at least 7", result.stderr)

    def test_rejects_dirty_tracked_worktree(self):
        result = self.run_driver(dirty=" M tracked.rs")
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("tracked worktree", result.stderr)

    def test_alternates_backend_order_and_excludes_warmups(self):
        result = self.run_driver()
        self.assertEqual(result.returncode, 0, result.stderr)
        runner_lines = [
            line
            for line in self.log.read_text().splitlines()
            if line.startswith(
                ("prolly-backend-postgres ", "prolly-backend-dynamodb ")
            )
        ]
        self.assertEqual(len(runner_lines), 16)
        self.assertIn("--run-id", runner_lines[0])
        self.assertIn("warmup-postgres", runner_lines[0])
        self.assertIn("warmup-dynamodb", runner_lines[1])
        measured = runner_lines[2:]
        for repetition in range(1, 8):
            pair = measured[(repetition - 1) * 2 : repetition * 2]
            expected = (
                ["prolly-backend-postgres", "prolly-backend-dynamodb"]
                if repetition % 2
                else ["prolly-backend-dynamodb", "prolly-backend-postgres"]
            )
            self.assertEqual([line.split()[0] for line in pair], expected)
            self.assertTrue(all(f"--repetition {repetition}" in line for line in pair))
        manifest = (self.root / "result" / "manifest.txt").read_text()
        self.assertIn("status=complete\n", manifest)
        self.assertIn("resumed=false\n", manifest)
        self.assertTrue((self.root / "result" / "raw-results.csv").exists())
        self.assertTrue((self.root / "result" / "report.md").exists())

    def test_failed_runner_never_invokes_summarizer(self):
        result = self.run_driver(fail_repetition="3")
        self.assertNotEqual(result.returncode, 0)
        lines = self.log.read_text().splitlines()
        self.assertFalse(any(line.startswith("summarizer ") for line in lines))
        self.assertTrue((self.root / "result" / "failure.txt").exists())

    def _write_fakes(self):
        self._script(
            "git",
            """
            if [[ "$*" == *"status --porcelain --untracked-files=no"* ]]; then
              printf '%s\n' "${FAKE_GIT_STATUS:-}"
            elif [[ "$*" == *"rev-parse HEAD^{tree}"* ]]; then
              printf '%040d\n' 0 | tr 0 b
            elif [[ "$*" == *"rev-parse HEAD"* ]]; then
              printf '%040d\n' 0 | tr 0 a
            else
              exit 2
            fi
            """,
        )
        self._script(
            "docker",
            """
            printf 'docker %s\n' "$*" >> "$FAKE_LOG"
            if [[ "$*" == *"image inspect"* ]]; then
              printf 'sha256:%064d\n' 0
            elif [[ "$*" == *"inspect --format"* ]]; then
              printf 'healthy\n'
            elif [[ "$1" == "info" ]]; then
              printf 'docker_server=fake cpus=8 memory=16000000000\n'
            fi
            """,
        )
        self._script("curl", "exit 0")
        runner = """
            name="$(basename "$0")"
            printf '%s %s\n' "$name" "$*" >> "$FAKE_LOG"
            output=""
            repetition=""
            while (($#)); do
              case "$1" in
                --output) output="$2"; shift 2 ;;
                --repetition) repetition="$2"; shift 2 ;;
                *) shift ;;
              esac
            done
            if [[ -n "${FAKE_FAIL_REPETITION:-}" && "$repetition" == "$FAKE_FAIL_REPETITION" ]]; then
              exit 9
            fi
            mkdir -p "$(dirname "$output")"
            printf 'header\n%s\n' "$name-$repetition" > "$output"
        """
        self._script("postgres-runner", runner)
        self._script("dynamodb-runner", runner)
        self._script(
            "summarizer",
            """
            printf 'summarizer %s\n' "$*" >> "$FAKE_LOG"
            output=""
            while (($#)); do
              if [[ "$1" == "--output-dir" ]]; then output="$2"; shift 2; else shift; fi
            done
            printf 'summary\n' > "$output/comparison.csv"
            printf 'report\n' > "$output/report.md"
            """,
        )

    def _script(self, name, body):
        path = self.bin / name
        path.write_text(
            "#!/usr/bin/env bash\nset -euo pipefail\n"
            + textwrap.dedent(body).strip()
            + "\n"
        )
        path.chmod(path.stat().st_mode | stat.S_IXUSR)


if __name__ == "__main__":
    unittest.main()
