#!/usr/bin/env python3

from __future__ import annotations

import pathlib
import subprocess
import tempfile
import unittest


SCRIPT = pathlib.Path(__file__).parents[1] / "verify_soak_evidence.py"


def valid_log() -> str:
    return """\
SOAK_START schema=prolly-s3-soak/v2 run_id=fixture epoch=100 duration_seconds=10 iteration_interval_seconds=1 initial_data_kib=1000 initial_build_kib=2000 container=prolly-rustfs health=healthy image=rustfs/rustfs:test@sha256:abc mount=/Volumes/Workspace/prolly-data:/data restart_count=0 source_revision=abc source_state=dirty test_binary_sha256=aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa cargo_version=cargo_1.94.1 rustc_version=rustc_1.94.1 max_rustfs_memory_bytes=1000000 max_data_growth_kib_per_iteration=100 max_total_data_growth_kib=1000 max_repository_bytes_per_workflow=10000 max_build_growth_kib=20
SOAK_WORKFLOW run_id=fixture iteration=1 name=ref-contention physical_storage_bytes=7000 final_fsck=ok
SOAK_CLEANUP run_id=fixture iteration=1 name=ref-contention deleted_versions=100 remaining_versions=0
SOAK_TEST run_id=fixture iteration=1 name=ref-contention status=passed elapsed_millis=2000
SOAK_WORKFLOW run_id=fixture iteration=1 name=multipart-recovery physical_storage_bytes=5000 final_fsck=ok
SOAK_CLEANUP run_id=fixture iteration=1 name=multipart-recovery deleted_versions=50 remaining_versions=0
SOAK_TEST run_id=fixture iteration=1 name=multipart-recovery status=passed elapsed_millis=1000
SOAK_ITERATION run_id=fixture epoch=103 iteration=1 elapsed_millis=3000 rustfs_memory_bytes=500000 health=healthy restart_count=0
SOAK_WORKFLOW run_id=fixture iteration=2 name=ref-contention physical_storage_bytes=7100 final_fsck=ok
SOAK_CLEANUP run_id=fixture iteration=2 name=ref-contention deleted_versions=101 remaining_versions=0
SOAK_TEST run_id=fixture iteration=2 name=ref-contention status=passed elapsed_millis=3000
SOAK_WORKFLOW run_id=fixture iteration=2 name=multipart-recovery physical_storage_bytes=5100 final_fsck=ok
SOAK_CLEANUP run_id=fixture iteration=2 name=multipart-recovery deleted_versions=51 remaining_versions=0
SOAK_TEST run_id=fixture iteration=2 name=multipart-recovery status=passed elapsed_millis=2000
SOAK_ITERATION run_id=fixture epoch=108 iteration=2 elapsed_millis=5000 rustfs_memory_bytes=600000 health=healthy restart_count=0
SOAK_COMPLETE schema=prolly-s3-soak/v2 run_id=fixture epoch=110 elapsed_seconds=10 iterations=2 test_runs=4 final_data_kib=1090 data_growth_kib=90 final_build_kib=2005 build_growth_kib=5 max_rustfs_memory_bytes_observed=600000 max_iteration_millis=5000 health=healthy restart_count=0
"""


class VerifySoakEvidenceTests(unittest.TestCase):
    def run_verifier(self, content: str, minimum: int = 10) -> subprocess.CompletedProcess[str]:
        with tempfile.TemporaryDirectory() as temporary:
            path = pathlib.Path(temporary) / "soak.log"
            path.write_text(content, encoding="utf-8")
            return subprocess.run(
                [
                    "python3",
                    str(SCRIPT),
                    "--test-log",
                    str(path),
                    "--minimum-seconds",
                    str(minimum),
                ],
                text=True,
                capture_output=True,
                check=False,
            )

    def test_accepts_complete_bounded_evidence(self) -> None:
        result = self.run_verifier(valid_log())
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertIn("SOAK_EVIDENCE_VERIFIED", result.stdout)

    def test_accepts_provider_compaction(self) -> None:
        content = valid_log().replace(
            "final_data_kib=1090 data_growth_kib=90",
            "final_data_kib=990 data_growth_kib=-10",
        )
        result = self.run_verifier(content)
        self.assertEqual(result.returncode, 0, result.stderr)

    def test_rejects_missing_required_test(self) -> None:
        content = valid_log().replace(
            "SOAK_TEST run_id=fixture iteration=2 name=multipart-recovery status=passed elapsed_millis=2000\n",
            "",
        )
        result = self.run_verifier(content)
        self.assertNotEqual(result.returncode, 0)

    def test_rejects_missing_required_cleanup(self) -> None:
        content = valid_log().replace(
            "SOAK_CLEANUP run_id=fixture iteration=2 name=multipart-recovery deleted_versions=51 remaining_versions=0\n",
            "",
        )
        result = self.run_verifier(content)
        self.assertNotEqual(result.returncode, 0)

    def test_rejects_short_elapsed_interval(self) -> None:
        result = self.run_verifier(valid_log(), minimum=11)
        self.assertNotEqual(result.returncode, 0)

    def test_rejects_provider_restart(self) -> None:
        content = valid_log().replace(
            "health=healthy restart_count=0\nSOAK_COMPLETE",
            "health=healthy restart_count=1\nSOAK_COMPLETE",
            1,
        )
        result = self.run_verifier(content)
        self.assertNotEqual(result.returncode, 0)

    def test_rejects_resource_limit_violation(self) -> None:
        content = valid_log().replace(
            "rustfs_memory_bytes=600000", "rustfs_memory_bytes=1000001"
        ).replace(
            "max_rustfs_memory_bytes_observed=600000",
            "max_rustfs_memory_bytes_observed=1000001",
        )
        result = self.run_verifier(content)
        self.assertNotEqual(result.returncode, 0)

    def test_rejects_absolute_provider_growth_violation(self) -> None:
        content = valid_log().replace(
            "max_total_data_growth_kib=1000",
            "max_total_data_growth_kib=89",
        )
        result = self.run_verifier(content)
        self.assertNotEqual(result.returncode, 0)


if __name__ == "__main__":
    unittest.main()
