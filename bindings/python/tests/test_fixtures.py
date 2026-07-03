from pathlib import Path
import json
import sys
import unittest

PORT_ROOT = Path(__file__).resolve().parents[1]
if str(PORT_ROOT) not in sys.path:
    sys.path.insert(0, str(PORT_ROOT))

from src import (
    Config,
    MemoryStore,
    Node,
    Prolly,
    Tree,
    ValueRef,
    VersionedValue,
    decode_segments,
    encode_segment,
    from_hex,
    i128_key,
    i64_key,
    is_boundary_config,
    key_proof_from_node_bytes,
    key_proof_path_node_bytes,
    prefix_end,
    prefix_range,
    to_hex,
    u128_key,
    u64_key,
    verify_key_proof,
)

FIXTURE_PATH = Path(__file__).resolve().parents[3] / "conformance/prolly-fixtures.v1.json"


def load_fixtures() -> dict:
    return json.loads(FIXTURE_PATH.read_text())


class FixtureTests(unittest.TestCase):
    def test_node_fixtures_decode_encode_and_hash(self) -> None:
        fixtures = load_fixtures()
        for fixture in fixtures["node_fixtures"]:
            node = Node.from_bytes(from_hex(fixture["bytes"]))
            self.assertEqual(to_hex(node.to_bytes()), fixture["bytes"])
            self.assertEqual(node.cid().hex(), fixture["cid"])

    def test_boundary_and_key_fixtures_match_rust(self) -> None:
        fixtures = load_fixtures()
        for fixture in fixtures["boundary_fixtures"]:
            config = Config.from_fixture(fixture["config"])
            self.assertEqual(
                is_boundary_config(
                    config,
                    fixture["count"],
                    from_hex(fixture["key"]),
                    from_hex(fixture["value"]),
                ),
                fixture["is_boundary"],
            )

        for fixture in fixtures["key_fixtures"]["prefix_end"]:
            prefix = from_hex(fixture["prefix"])
            actual = prefix_end(prefix)
            self.assertEqual(to_hex(actual) if actual is not None else None, fixture["end"])
            bounds = prefix_range(prefix)
            self.assertEqual(bounds.start, prefix)
            self.assertEqual(to_hex(bounds.end) if bounds.end is not None else None, fixture["end"])

        for fixture in fixtures["key_fixtures"]["numeric"]:
            value = int(fixture["value"])
            if fixture["kind"] == "u64":
                self.assertEqual(to_hex(u64_key(value)), fixture["encoded"])
            if fixture["kind"] == "u128":
                self.assertEqual(to_hex(u128_key(value)), fixture["encoded"])
            if fixture["kind"] == "i64":
                self.assertEqual(to_hex(i64_key(value)), fixture["encoded"])
            if fixture["kind"] == "i128":
                self.assertEqual(to_hex(i128_key(value)), fixture["encoded"])

        for fixture in fixtures["key_fixtures"]["segments"]:
            encoded = b"".join(encode_segment(from_hex(segment)) for segment in fixture["segments"])
            self.assertEqual(to_hex(encoded), fixture["encoded"])
            self.assertEqual(
                [to_hex(segment) for segment in decode_segments(from_hex(fixture["encoded"]))],
                fixture["decoded"],
            )

    def test_tree_fixture_get_range_and_diff(self) -> None:
        fixtures = load_fixtures()
        fixture = fixtures["tree_fixtures"][0]
        store = MemoryStore.from_fixture(fixture)
        tree = Tree.from_fixture(fixture)
        prolly = Prolly(store, tree.config)

        for lookup in fixture["lookups"]:
            actual = prolly.get(tree, from_hex(lookup["key"]))
            self.assertEqual(to_hex(actual) if actual is not None else None, lookup["value"])

        present_lookup = next(lookup for lookup in fixture["lookups"] if lookup["value"] is not None)
        proof = prolly.prove_key(tree, from_hex(present_lookup["key"]))
        verified = verify_key_proof(proof)
        self.assertTrue(verified.valid)
        self.assertTrue(verified.exists)
        self.assertFalse(verified.absence)
        self.assertEqual(to_hex(verified.value or b""), present_lookup["value"])

        decoded = key_proof_from_node_bytes(
            proof.root,
            proof.key,
            key_proof_path_node_bytes(proof),
        )
        self.assertEqual(verify_key_proof(decoded).value, verified.value)

        absent = verify_key_proof(prolly.prove_key(tree, b"definitely-missing"))
        self.assertTrue(absent.valid)
        self.assertFalse(absent.exists)
        self.assertTrue(absent.absence)

        assert proof.root is not None
        tampered = type(proof)(
            root=type(proof.root)(bytes([proof.root.bytes[0] ^ 0x01]) + proof.root.bytes[1:]),
            key=proof.key,
            path=proof.path,
        )
        self.assertFalse(verify_key_proof(tampered).valid)

        for range_fixture in fixture["ranges"]:
            actual = prolly.range(
                tree,
                from_hex(range_fixture["start"]),
                from_hex(range_fixture["end"]) if range_fixture["end"] is not None else None,
            )
            self.assertEqual(
                [{"key": to_hex(key), "value": to_hex(value)} for key, value in actual],
                range_fixture["entries"],
            )

        diff_fixture = fixtures["diff_fixtures"][0]
        diff_store = MemoryStore.from_fixture(diff_fixture)
        diff_prolly = Prolly(diff_store, Config.from_fixture(diff_fixture["config"]))
        base = Tree.from_fixture({"root": diff_fixture["base_root"], "config": diff_fixture["config"]})
        other = Tree.from_fixture({"root": diff_fixture["other_root"], "config": diff_fixture["config"]})
        self.assertEqual(
            [
                {key: (to_hex(value) if isinstance(value, bytes) else value) for key, value in entry.items()}
                for entry in diff_prolly.diff(base, other)
            ],
            diff_fixture["diffs"],
        )

    def test_value_and_blob_fixtures_decode(self) -> None:
        fixtures = load_fixtures()
        for fixture in fixtures["value_fixtures"]:
            value = VersionedValue.from_bytes(from_hex(fixture["bytes"]))
            self.assertEqual(value.schema, fixture["schema_name"])
            self.assertEqual(value.version, fixture["version"])
            self.assertEqual(value.encoding, fixture["encoding"]["kind"])
            self.assertEqual(to_hex(value.payload), fixture["payload"])
            self.assertEqual(to_hex(value.to_bytes()), fixture["bytes"])

        for fixture in fixtures["blob_fixtures"]:
            value_ref = ValueRef.from_bytes(from_hex(fixture["bytes"]))
            self.assertEqual(value_ref.kind, fixture["kind"])
            self.assertEqual(to_hex(value_ref.to_bytes()), fixture["bytes"])
            if fixture["kind"] == "inline":
                self.assertEqual(to_hex(value_ref.value or b""), fixture["value"])
            else:
                self.assertIsNotNone(value_ref.blob_ref)
                assert value_ref.blob_ref is not None
                self.assertEqual(value_ref.blob_ref.cid.hex(), fixture["cid"])
                self.assertEqual(value_ref.blob_ref.length, fixture["len"])


if __name__ == "__main__":
    unittest.main()
