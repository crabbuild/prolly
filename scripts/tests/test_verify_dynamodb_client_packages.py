import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[2]


class PackageVerificationContractTest(unittest.TestCase):
    def test_release_lockfiles_exist_and_are_not_ignored(self):
        lockfiles = [
            ROOT / "Cargo.lock",
            ROOT / "stores/prolly-store-dynamodb/Cargo.lock",
            ROOT / "extensions/dynamodb/core/Cargo.lock",
            ROOT / "extensions/dynamodb/client/Cargo.lock",
            ROOT / "extensions/dynamodb/admin/Cargo.lock",
            ROOT / "benchmarks/dynamodb-client/Cargo.lock",
        ]
        for lockfile in lockfiles:
            with self.subTest(lockfile=lockfile.relative_to(ROOT)):
                self.assertTrue(lockfile.is_file())
                self.assertGreater(lockfile.stat().st_size, 0)

        ignore = (ROOT / ".gitignore").read_text(encoding="utf-8")
        self.assertNotIn("\n/Cargo.lock\n", f"\n{ignore}\n")
        self.assertIn("!stores/prolly-store-dynamodb/Cargo.lock", ignore)

    def test_archive_verifier_is_locked_with_scoped_offline_substitution(self):
        script = (ROOT / "scripts/verify_dynamodb_client_packages.sh").read_text(
            encoding="utf-8"
        )
        self.assertIn('cargo package "$@" --locked', script)
        self.assertGreaterEqual(script.count("cargo test \\\n"), 2)
        self.assertGreaterEqual(script.count("  --locked \\\n"), 2)
        self.assertIn("CARGO_NET_OFFLINE=true cargo update", script)
        self.assertIn("--offline", script)
        self.assertIn(
            "registry+https://github.com/rust-lang/crates.io-index#prolly-map@0.7.0",
            script,
        )
        self.assertIn("CARGO_INCREMENTAL=0", script)
        self.assertIn("CARGO_PROFILE_TEST_DEBUG=0", script)


if __name__ == "__main__":
    unittest.main()
