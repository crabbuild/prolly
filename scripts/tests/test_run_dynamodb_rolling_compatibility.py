import importlib.util
import json
import os
from pathlib import Path
import tempfile
import unittest


SCRIPT = Path(__file__).resolve().parents[1] / "run_dynamodb_rolling_compatibility.py"
SPEC = importlib.util.spec_from_file_location("rolling_compatibility", SCRIPT)
assert SPEC is not None and SPEC.loader is not None
rolling = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(rolling)


def identity(**overrides):
    capabilities = {field: f"value-{field}" for field in rolling.DURABLE_CAPABILITY_FIELDS}
    capabilities.update(overrides)
    return {
        "protocol": rolling.PROTOCOL,
        "command": "identity",
        "package_version": "0.1.0",
        "capabilities": capabilities,
    }


class RollingCompatibilityTests(unittest.TestCase):
    def test_probe_output_is_one_exact_protocol_object(self):
        value = identity()
        parsed = rolling.parse_probe_output(json.dumps(value) + "\n", "identity")
        self.assertEqual(parsed, value)

        for invalid in (
            "",
            json.dumps(value) + "\nnoise\n",
            "[]\n",
            json.dumps({**value, "protocol": "other"}) + "\n",
            json.dumps({**value, "command": "write"}) + "\n",
            json.dumps({**value, "package_version": 1}) + "\n",
        ):
            with self.subTest(invalid=invalid):
                with self.assertRaises(rolling.QualificationError):
                    rolling.parse_probe_output(invalid, "identity")

    def test_identity_requires_every_equal_durable_capability(self):
        baseline = identity()
        rolling.validate_identity_pair(baseline, identity())

        for field in rolling.DURABLE_CAPABILITY_FIELDS:
            with self.subTest(field=field):
                with self.assertRaises(rolling.QualificationError):
                    rolling.validate_identity_pair(
                        baseline, identity(**{field: f"different-{field}"})
                    )

        missing = identity()
        del missing["capabilities"][rolling.DURABLE_CAPABILITY_FIELDS[0]]
        with self.assertRaises(rolling.QualificationError):
            rolling.validate_identity_pair(baseline, missing)

    def test_write_and_verify_manifests_are_exact(self):
        write = {
            "writer": "old",
            "start": 0,
            "count": 3,
            "first": {"id": "old-0000000000", "version": "a" * 64},
            "last": {"id": "old-0000000002", "version": "b" * 64},
        }
        rolling.validate_write(write, "old", 3)
        rolling.validate_verify(
            {"items": 6, "versions": 7, "commits": 7, "head": "c" * 64}, 6
        )

        for key, value in (
            ("count", 2),
            ("first", {"id": "wrong", "version": "a" * 64}),
            ("last", {"id": "old-0000000002", "version": "A" * 64}),
        ):
            invalid = dict(write)
            invalid[key] = value
            with self.subTest(key=key):
                with self.assertRaises(rolling.QualificationError):
                    rolling.validate_write(invalid, "old", 3)

        for key, value in (
            ("items", 5),
            ("versions", 6),
            ("commits", 6),
            ("head", "short"),
        ):
            invalid = {"items": 6, "versions": 7, "commits": 7, "head": "c" * 64}
            invalid[key] = value
            with self.subTest(key=key):
                with self.assertRaises(rolling.QualificationError):
                    rolling.validate_verify(invalid, 6)

    def test_sha256_file_is_streaming_and_exact(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "binary"
            path.write_bytes(b"rolling-probe")
            self.assertEqual(
                rolling.sha256_file(path),
                "ae4f82a3953fedd9a772e83041ba863a95cb8a36c4f42b39f57bf3f534310283",
            )

    def test_writer_failure_terminates_and_records_its_peer(self):
        with tempfile.TemporaryDirectory() as directory:
            directory_path = Path(directory)
            old = directory_path / "old"
            new = directory_path / "new"
            old.write_text("#!/usr/bin/env python3\nraise SystemExit(7)\n", encoding="utf-8")
            new.write_text(
                "#!/usr/bin/env python3\nimport time\ntime.sleep(30)\n", encoding="utf-8"
            )
            old.chmod(0o755)
            new.chmod(0o755)
            runner = rolling.Runner(old, new, os.environ.copy(), 2.0)

            with self.assertRaises(rolling.QualificationError):
                runner.run_writers(1)

            self.assertEqual(
                [record["label"] for record in runner.commands],
                ["old-write", "new-write"],
            )
            self.assertEqual(runner.commands[0]["returncode"], 7)
            self.assertTrue(runner.commands[1]["terminated_by_coordinator"])
            self.assertIsNotNone(runner.commands[1]["returncode"])


if __name__ == "__main__":
    unittest.main()
