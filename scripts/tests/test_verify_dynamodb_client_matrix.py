import os
from pathlib import Path
import subprocess
import tempfile
import unittest


REPO_ROOT = Path(__file__).resolve().parents[2]
VERIFY_SCRIPT = REPO_ROOT / "scripts" / "verify_dynamodb_client_matrix.sh"


class VerifyDynamoDbClientMatrixTests(unittest.TestCase):
    def test_resolves_bare_cross_linker_name_from_path(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            temp = Path(temp_dir)
            fake_bin = temp / "bin"
            fake_bin.mkdir()
            target_libdir = temp / "target-libdir"
            target_libdir.mkdir()

            self._write_executable(fake_bin / "cargo", "#!/usr/bin/env bash\nexit 0\n")
            self._write_executable(
                fake_bin / "rustc",
                """#!/usr/bin/env bash
if [[ "${1:-}" == +* ]]; then
  shift
fi
if [[ "${1:-}" == "--print" && "${2:-}" == "target-libdir" ]]; then
  printf '%s\\n' "$FAKE_TARGET_LIBDIR"
fi
exit 0
""",
            )
            self._write_executable(
                fake_bin / "fake-aarch64-linker",
                "#!/usr/bin/env bash\nprintf '%s\\n' 'fake linker 1.0'\n",
            )

            env = os.environ.copy()
            env.update(
                {
                    "PATH": f"{fake_bin}{os.pathsep}{env['PATH']}",
                    "FAKE_TARGET_LIBDIR": str(target_libdir),
                    "CARGO_TARGET_AARCH64_UNKNOWN_LINUX_GNU_LINKER": "fake-aarch64-linker",
                    "TMPDIR": str(temp),
                }
            )
            result = subprocess.run(
                [str(VERIFY_SCRIPT), "--target", "aarch64-unknown-linux-gnu"],
                cwd=REPO_ROOT,
                env=env,
                check=False,
                capture_output=True,
                text=True,
            )

            self.assertEqual(result.returncode, 0, result.stderr)
            self.assertIn("matrix_ok toolchain=1.91.1 target=aarch64-unknown-linux-gnu", result.stdout)

    @staticmethod
    def _write_executable(path: Path, contents: str) -> None:
        path.write_text(contents, encoding="utf-8")
        path.chmod(0o755)


if __name__ == "__main__":
    unittest.main()
