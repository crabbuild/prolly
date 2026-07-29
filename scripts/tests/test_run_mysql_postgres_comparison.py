import os
import pathlib
import stat
import subprocess
import tempfile
import textwrap
import unittest


ROOT = pathlib.Path(__file__).parents[2]
DRIVER = ROOT / "scripts" / "run_mysql_postgres_comparison.sh"


class SqlComparisonDriverTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.root = pathlib.Path(self.temp.name)
        self.bin = self.root / "bin"
        self.bin.mkdir()
        self.log = self.root / "commands.log"
        self._write_fakes()

    def tearDown(self):
        self.temp.cleanup()

    def run_driver(self, **overrides):
        env = os.environ.copy()
        env.update(
            {
                "PATH": f"{self.bin}:{env['PATH']}",
                "BENCH_OUT": str(self.root / "result"),
                "BENCH_RECORDS": "100",
                "BENCH_CHANGES": "10",
                "BENCH_SAMPLES": "10",
                "BENCH_RUNS": "7",
                "BENCH_SKIP_BUILD": "1",
                "BENCH_SKIP_IMAGE_PULL": "1",
                "BENCH_POSTGRES_EXECUTABLE": str(self.bin / "postgres-runner"),
                "BENCH_MYSQL_EXECUTABLE": str(self.bin / "mysql-runner"),
                "BENCH_SUMMARIZER_EXECUTABLE": str(self.bin / "summarizer"),
                "BENCH_SERVICE_SUMMARIZER": str(self.bin / "service-summarizer"),
                "BENCH_GIT_BIN": str(self.bin / "git"),
                "BENCH_DOCKER_BIN": str(self.bin / "docker"),
                "FAKE_LOG": str(self.log),
            }
        )
        env.update(overrides)
        return subprocess.run(
            [str(DRIVER)],
            cwd=ROOT,
            env=env,
            text=True,
            capture_output=True,
            check=False,
        )

    def test_local_run_alternates_backends_and_records_pair(self):
        result = self.run_driver()
        self.assertEqual(result.returncode, 0, result.stderr)
        runners = [
            line
            for line in self.log.read_text().splitlines()
            if line.startswith(("prolly-backend-postgres ", "prolly-backend-mysql "))
            and "--suite end-to-end" in line
        ]
        self.assertEqual(len(runners), 16)
        measured = runners[2:]
        for repetition in range(1, 8):
            expected = (
                ["prolly-backend-postgres", "prolly-backend-mysql"]
                if repetition % 2
                else ["prolly-backend-mysql", "prolly-backend-postgres"]
            )
            pair = measured[(repetition - 1) * 2 : repetition * 2]
            self.assertEqual([line.split()[0] for line in pair], expected)
        manifest = (self.root / "result" / "manifest.txt").read_text()
        self.assertIn("environment_class=controlled_local\n", manifest)
        self.assertIn("backend_a=postgres\n", manifest)
        self.assertIn("backend_b=mysql\n", manifest)

    def test_external_mode_requires_acknowledgement(self):
        result = self.run_driver(BENCH_MODE="external")
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("BENCH_EXTERNAL_RESET_ACK", result.stderr)

    def test_external_commands_redact_credentials(self):
        result = self.run_driver(
            BENCH_MODE="external",
            BENCH_EXTERNAL_RESET_ACK="I_UNDERSTAND_BENCHMARK_DATA_WILL_BE_DELETED",
            BENCH_EXTERNAL_POSTGRES_IDENTITY="postgres-16-managed",
            BENCH_EXTERNAL_MYSQL_IDENTITY="mysql-8-managed",
            PROLLY_BACKEND_POSTGRES_URL="postgres://admin:secret@db.example/prolly",
            PROLLY_BACKEND_MYSQL_URL="mysql://admin:secret@db.example/prolly",
        )
        self.assertEqual(result.returncode, 0, result.stderr)
        commands = (self.root / "result" / "measurement-commands.txt").read_text()
        self.assertNotIn("admin", commands)
        self.assertNotIn("secret", commands)
        self.assertIn("REDACTED", commands)

    def _write_fakes(self):
        self._script(
            "git",
            """
            if [[ "$*" == *"status --porcelain --untracked-files=no"* ]]; then
              exit 0
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
        runner = """
            name="$(basename "$0")"
            printf '%s %s\n' "$name" "$*" >> "$FAKE_LOG"
            output=""
            while (($#)); do
              case "$1" in
                --output) output="$2"; shift 2 ;;
                *) shift ;;
              esac
            done
            mkdir -p "$(dirname "$output")"
            printf 'header\nrow\n' > "$output"
        """
        self._script("postgres-runner", runner)
        self._script("mysql-runner", runner)
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
        self._script(
            "service-summarizer",
            """
            output=""
            while (($#)); do
              if [[ "$1" == "--output-dir" ]]; then output="$2"; shift 2; else shift; fi
            done
            printf 'service summary\n' > "$output/service-comparison.csv"
            printf 'service report\n' > "$output/service-report.md"
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
