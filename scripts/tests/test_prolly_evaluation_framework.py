import csv
import json
import os
import stat
import subprocess
import tempfile
import textwrap
import unittest
from pathlib import Path

from scripts.summarize_prolly_evaluation import quality_label, safe_ratio


ROOT = Path(__file__).resolve().parents[2]
DRIVER = ROOT / "scripts" / "run_prolly_evaluation.sh"


def executable(path: Path, content: str) -> None:
    path.write_text(textwrap.dedent(content).lstrip(), encoding="utf-8")
    path.chmod(path.stat().st_mode | stat.S_IXUSR)


class ProllyEvaluationFrameworkTest(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary = tempfile.TemporaryDirectory()
        self.root = Path(self.temporary.name)
        self.bin = self.root / "bin"
        self.bin.mkdir()
        self.log = self.root / "invocations.log"
        self.runner = self.root / "runner.py"
        executable(
            self.runner,
            r'''
            #!/usr/bin/env python3
            import os
            import sys

            domain, implementation = sys.argv[1:3]
            args = dict(zip(sys.argv[3::2], sys.argv[4::2]))
            profile = os.environ.get("PROLLY_BENCH_CACHE_PROFILE", "native")
            with open(os.environ["FAKE_INVOCATION_LOG"], "a", encoding="utf-8") as log:
                log.write(f"{domain},{implementation},{profile},{sys.argv[3:]}\n")
            revision = os.environ["BENCH_REVISION"]
            if domain == "tree":
                records = int(args["--records"])
                phase = args["--phase"]
                workload = args["--workload"]
                digests = {
                    ("fresh", "append"): "51f55fcd59187cbf",
                    ("fresh", "random"): "004197dd790a1245",
                    ("fresh", "clustered"): "86e38047f6ae04b3",
                    ("mutation", "append"): "2ef1df79e1226620",
                    ("mutation", "random"): "3bc7e45ef276a1c5",
                    ("mutation", "clustered"): "5caed8dbd3056277",
                }
                writes = records if phase == "fresh" else records * 30 // 100
                result_count = records if phase == "fresh" else records + writes
                ns = 20 if implementation == "dolt-go" else 15 if profile == "bounded" else 10
                print("implementation,revision,contract_version,records,phase,workload,operation,operations,elapsed_ns,ns_per_op,ops_per_sec,workload_digest,result_count,validated")
                for operation, operations in (("write", writes), ("point_read", records), ("range_scan", result_count)):
                    elapsed = ns * operations
                    print(f"{implementation},{revision},prolly-compare-v1,{records},{phase},{workload},{operation},{operations},{elapsed},{ns:.3f},{1_000_000_000/ns:.3f},{digests[(phase, workload)]},{result_count},true")
                raise SystemExit(0)

            records = int(args["--records"])
            density = int(args.get("--density", "0"))
            locality = args.get("--locality", "none")
            if domain == "lifecycle":
                scenario = args["--scenario"]
                operations = {
                    "publish": ("version_publish",),
                    "read": ("head_resolve", "snapshot_resolve", "historical_point_read", "historical_range_scan", "version_list"),
                    "rollback": ("rollback",),
                    "prune": ("retention_prune",),
                }[scenario]
                runner_implementation = "rust-lifecycle"
            else:
                scenario = "common"
                operations = (
                    "full_diff", "range_diff", "patch_generate", "patch_apply",
                    "merge_noop" if density == 0 else "merge_disjoint",
                ) if density == 0 else (
                    "full_diff", "range_diff", "patch_generate", "patch_apply",
                    "merge_disjoint", "merge_convergent", "merge_conflict",
                )
                runner_implementation = implementation
            ns = 20 if implementation == "dolt-go" else 15 if profile == "bounded" else 10
            relationship = {
                "full_diff": "compare", "range_diff": "compare",
                "patch_generate": "compare", "patch_apply": "compare",
                "merge_noop": "noop", "merge_disjoint": "disjoint",
                "merge_convergent": "convergent", "merge_conflict": "conflict",
                "version_publish": "publish", "head_resolve": "read",
                "snapshot_resolve": "read", "historical_point_read": "read",
                "historical_range_scan": "read", "version_list": "read",
                "rollback": "rollback", "retention_prune": "prune",
            }
            print("implementation,revision,contract_version,records,density,locality,operation,relationship,operations,elapsed_ns,ns_per_op,ops_per_sec,workload_digest,result_digest,result_count,base_count,target_count,conflict_count,validated")
            for operation in operations:
                units = max(1, records * max(1, density) // 100)
                elapsed = ns * units
                conflicts = units if operation == "merge_conflict" else 0
                print(f"{runner_implementation},{revision},prolly-version-compare-v3,{records},{density},{locality},{operation},{relationship[operation]},{units},{elapsed},{ns:.3f},{1_000_000_000/ns:.3f},1111111111111111,2222222222222222,{units},{records},{records},{conflicts},true")
            ''',
        )
        for name, domain, implementation in (
            ("rust-tree", "tree", "rust"),
            ("go-tree", "tree", "dolt-go"),
            ("rust-version", "version", "rust"),
            ("go-version", "version", "dolt-go"),
            ("rust-lifecycle", "lifecycle", "rust"),
        ):
            executable(
                self.bin / name,
                f'''#!/bin/sh
                exec python3 "{self.runner}" {domain} {implementation} "$@"
                ''',
            )
        self.time = self.bin / "time"
        executable(
            self.time,
            r'''
            #!/bin/sh
            set +e
            mode=$1
            shift
            output=
            if [ "$1" = -o ]; then
                output=$2
                shift 2
            fi
            "$@"
            status=$?
            if [ "$mode" = -l ]; then
                metric='1048576 maximum resident set size'
            else
                metric='Maximum resident set size (kbytes): 1024'
            fi
            if [ -n "$output" ]; then
                printf '%s\n' "$metric" >"$output"
            else
                printf '%s\n' "$metric" >&2
            fi
            exit "$status"
            ''',
        )

    def tearDown(self) -> None:
        self.temporary.cleanup()

    def environment(self, output: Path, resume: bool = False) -> dict[str, str]:
        env = os.environ.copy()
        env.update(
            {
                "BENCH_PROFILE": "smoke",
                "BENCH_OUT": str(output),
                "BENCH_SKIP_BUILD": "1",
                "BENCH_SKIP_SMOKE": "1",
                "BENCH_RESUME": "1" if resume else "0",
                "BENCH_LIFECYCLE": "1",
                "BENCH_TIME_BIN": str(self.time),
                "BENCH_RUST_CACHE_PROFILES": "bounded unbounded",
                "DOLT_REV": "0123456789abcdef0123456789abcdef01234567",
                "PROLLY_EVAL_RUST_TREE_BIN": str(self.bin / "rust-tree"),
                "PROLLY_EVAL_GO_TREE_BIN": str(self.bin / "go-tree"),
                "PROLLY_EVAL_RUST_VERSION_BIN": str(self.bin / "rust-version"),
                "PROLLY_EVAL_GO_VERSION_BIN": str(self.bin / "go-version"),
                "PROLLY_EVAL_RUST_LIFECYCLE_BIN": str(self.bin / "rust-lifecycle"),
                "FAKE_INVOCATION_LOG": str(self.log),
            }
        )
        return env

    def run_driver(self, output: Path, resume: bool = False) -> subprocess.CompletedProcess[str]:
        return subprocess.run(
            [str(DRIVER)],
            cwd=ROOT,
            env=self.environment(output, resume),
            text=True,
            capture_output=True,
        )

    def test_profiles_share_one_go_baseline_and_resume_without_rerunning(self) -> None:
        output = self.root / "results"
        first = self.run_driver(output)
        self.assertEqual(first.returncode, 0, first.stderr)

        with (output / "tree" / "results.csv").open(newline="") as handle:
            tree = list(csv.DictReader(handle))
        with (output / "version" / "results-common.csv").open(newline="") as handle:
            version = list(csv.DictReader(handle))
        with (output / "lifecycle" / "results.csv").open(newline="") as handle:
            lifecycle = list(csv.DictReader(handle))
        self.assertEqual({row["cache_profile"] for row in tree}, {"native", "bounded", "unbounded"})
        self.assertEqual({row["cache_profile"] for row in version}, {"native", "bounded", "unbounded"})
        self.assertEqual(len(tree), 54)
        self.assertEqual(len(version), 21)
        self.assertEqual(len(lifecycle), 16)
        self.assertEqual(
            {row["cache_profile"] for row in lifecycle}, {"bounded", "unbounded"}
        )
        self.assertTrue((output / "COMPLETE").is_file())
        self.assertTrue((output / "report.md").is_file())

        invocations = self.log.read_text(encoding="utf-8").splitlines()
        self.assertEqual(sum(",dolt-go,native," in line for line in invocations), 7)
        self.assertEqual(sum(",rust,bounded," in line for line in invocations), 11)
        self.assertEqual(sum(",rust,unbounded," in line for line in invocations), 11)

        for binary in (
            "rust-tree",
            "go-tree",
            "rust-version",
            "go-version",
            "rust-lifecycle",
        ):
            (self.bin / binary).unlink()
        resumed = self.run_driver(output, resume=True)
        self.assertEqual(resumed.returncode, 0, resumed.stderr)
        self.assertEqual(self.log.read_text(encoding="utf-8").splitlines(), invocations)
        self.assertIn("reusing", resumed.stderr)

        lock = output / ".lock"
        lock.mkdir()
        (lock / "pid").write_text(f"{os.getpid()}\n", encoding="utf-8")
        locked = self.run_driver(output, resume=True)
        self.assertNotEqual(locked.returncode, 0)
        self.assertIn("is locked", locked.stderr)
        (lock / "pid").unlink()
        lock.rmdir()

        lifecycle_path = output / "lifecycle" / "results.csv"
        with lifecycle_path.open(newline="") as handle:
            lifecycle_rows = list(csv.DictReader(handle))
            lifecycle_fields = list(lifecycle_rows[0])
        diverged_rows = [dict(row) for row in lifecycle_rows]
        next(
            row
            for row in diverged_rows
            if row["cache_profile"] == "unbounded"
            and row["operation"] == "head_resolve"
        )["result_digest"] = "3333333333333333"
        with lifecycle_path.open("w", newline="") as handle:
            writer = csv.DictWriter(
                handle, fieldnames=lifecycle_fields, lineterminator="\n"
            )
            writer.writeheader()
            writer.writerows(diverged_rows)
        warned = subprocess.run(
            [
                "python3",
                str(ROOT / "scripts" / "summarize_prolly_evaluation.py"),
                "--output",
                str(output),
            ],
            cwd=ROOT,
            text=True,
            capture_output=True,
        )
        self.assertEqual(warned.returncode, 0, warned.stderr)
        self.assertTrue((output / "WARNINGS").is_file())
        self.assertIn(
            "Lifecycle result-digest divergence groups: 1 (WARN)",
            (output / "report.md").read_text(encoding="utf-8"),
        )
        with lifecycle_path.open("w", newline="") as handle:
            writer = csv.DictWriter(
                handle, fieldnames=lifecycle_fields, lineterminator="\n"
            )
            writer.writeheader()
            writer.writerows(lifecycle_rows)
        clean = subprocess.run(
            [
                "python3",
                str(ROOT / "scripts" / "summarize_prolly_evaluation.py"),
                "--output",
                str(output),
            ],
            cwd=ROOT,
            text=True,
            capture_output=True,
        )
        self.assertEqual(clean.returncode, 0, clean.stderr)
        self.assertFalse((output / "WARNINGS").exists())

        tree_path = output / "tree" / "results.csv"
        with tree_path.open(newline="") as handle:
            tampered_rows = list(csv.DictReader(handle))
            fieldnames = list(tampered_rows[0])
        next(
            row
            for row in tampered_rows
            if row["implementation"] == "rust"
            and row["cache_profile"] == "bounded"
        )["workload_digest"] = "tampered"
        with tree_path.open("w", newline="") as handle:
            writer = csv.DictWriter(handle, fieldnames=fieldnames, lineterminator="\n")
            writer.writeheader()
            writer.writerows(tampered_rows)
        summary = subprocess.run(
            [
                "python3",
                str(ROOT / "scripts" / "summarize_prolly_evaluation.py"),
                "--output",
                str(output),
            ],
            cwd=ROOT,
            text=True,
            capture_output=True,
        )
        self.assertNotEqual(summary.returncode, 0)
        self.assertIn("identity mismatch", summary.stderr)

    def test_resume_fails_closed_when_sample_state_is_tampered(self) -> None:
        output = self.root / "tampered"
        first = self.run_driver(output)
        self.assertEqual(first.returncode, 0, first.stderr)
        state_path = next((output / "tree" / "state").glob("*.json"))
        state = json.loads(state_path.read_text(encoding="utf-8"))
        state["fingerprint"] = "tampered"
        state_path.write_text(json.dumps(state), encoding="utf-8")

        resumed = self.run_driver(output, resume=True)
        self.assertNotEqual(resumed.returncode, 0)
        self.assertIn("resume state mismatch", resumed.stderr)
        self.assertFalse((output / "COMPLETE").exists())

    def test_resume_fails_closed_when_raw_sample_is_tampered(self) -> None:
        output = self.root / "tampered-raw"
        first = self.run_driver(output)
        self.assertEqual(first.returncode, 0, first.stderr)
        raw_path = next((output / "tree" / "raw").glob("*.csv"))
        raw_path.write_bytes(raw_path.read_bytes() + b"\n")

        resumed = self.run_driver(output, resume=True)
        self.assertNotEqual(resumed.returncode, 0)
        self.assertIn("resume artifact mismatch", resumed.stderr)
        self.assertFalse((output / "COMPLETE").exists())

    def test_zero_duration_ratios_remain_reportable(self) -> None:
        self.assertEqual(safe_ratio(0, 0), 1.0)
        self.assertEqual(safe_ratio(10, 0), float("inf"))
        self.assertEqual(safe_ratio(10, 5), 2.0)

    def test_smoke_repetition_is_not_labeled_stable(self) -> None:
        self.assertEqual(
            quality_label(1, True, 2.0, 0.0), "insufficient_repetitions"
        )


if __name__ == "__main__":
    unittest.main()
