from pathlib import Path
import json
import sys
import tempfile
import unittest

PACKAGE_ROOT = Path(__file__).resolve().parents[1]
if str(PACKAGE_ROOT) not in sys.path:
    sys.path.insert(0, str(PACKAGE_ROOT))

import prolly


@unittest.skipUnless(
    hasattr(prolly, "ProllyEngine"),
    "generated UniFFI module is not built; run `maturin develop` first",
)
class UniFfiBindingTests(unittest.TestCase):
    def test_memory_engine_round_trips_basic_operations(self) -> None:
        engine = prolly.ProllyEngine.memory(prolly.default_config())
        tree = engine.create()
        tree = engine.put(tree, b"a", b"1")
        tree = engine.put(tree, b"b", b"2")

        self.assertEqual(engine.get(tree, b"a"), b"1")
        self.assertEqual(
            engine.get_many(tree, [b"b", b"missing", b"a"]),
            [b"2", None, b"1"],
        )

        entries = engine.range(tree, b"", None)
        self.assertEqual(
            [(entry.key, entry.value) for entry in entries],
            [(b"a", b"1"), (b"b", b"2")],
        )

        deleted = engine.delete(tree, b"a")
        diffs = engine.diff(tree, deleted)
        self.assertEqual(len(diffs), 1)
        self.assertEqual(diffs[0].key, b"a")

    def test_key_helpers_build_composite_keys(self) -> None:
        segments = [b"tenant", b"\x00\x01\xff", b""]
        expected = b"tenant\x00\x00\x00\xff\x01\xff\x00\x00\x00\x00"
        self.assertEqual(prolly.key_from_segments(segments), expected)
        prefix = prolly.key_from_segments(segments[:1])
        self.assertEqual(prolly.key_from_prefixed_segments(prefix, segments[1:]), expected)
        self.assertEqual(prolly.decode_segments(expected), segments)

    def test_key_proofs_verify_without_store(self) -> None:
        engine = prolly.ProllyEngine.memory(prolly.default_config())
        tree = engine.create()
        for key, value in [(b"a", b"1"), (b"b", b"2"), (b"c", b"3")]:
            tree = engine.put(tree, key, value)

        proof = engine.prove_key(tree, b"b")
        verified = prolly.verify_key_proof(proof)
        self.assertTrue(verified.valid)
        self.assertTrue(verified.exists)
        self.assertFalse(verified.absence)
        self.assertEqual(verified.value, b"2")

        path_node_bytes = prolly.key_proof_path_node_bytes(proof)
        decoded = prolly.key_proof_from_node_bytes(proof.root, proof.key, path_node_bytes)
        self.assertEqual(prolly.verify_key_proof(decoded).value, b"2")
        key_bundle = prolly.key_proof_to_bytes(proof)
        key_summary = prolly.inspect_proof_bundle(key_bundle)
        self.assertEqual(key_summary.kind, "key")
        self.assertEqual(key_summary.root, tree.root)
        self.assertEqual(key_summary.key_count, 1)
        self.assertEqual(key_summary.path_node_count, len(proof.path))
        key_bundle_verified = prolly.verify_proof_bundle(key_bundle)
        self.assertTrue(key_bundle_verified.valid)
        self.assertEqual(key_bundle_verified.summary.kind, "key")
        self.assertEqual(key_bundle_verified.exists_count, 1)
        self.assertEqual(key_bundle_verified.absence_count, 0)
        decoded_from_bytes = prolly.key_proof_from_bytes(key_bundle)
        self.assertEqual(prolly.verify_key_proof(decoded_from_bytes).value, b"2")

        absent = prolly.verify_key_proof(engine.prove_key(tree, b"missing"))
        self.assertTrue(absent.valid)
        self.assertFalse(absent.exists)
        self.assertTrue(absent.absence)

        assert proof.root is not None
        tampered_root = bytes([proof.root[0] ^ 0x01]) + proof.root[1:]
        tampered = prolly.KeyProofRecord(
            root=tampered_root,
            key=proof.key,
            path=proof.path,
        )
        self.assertFalse(prolly.verify_proof_bundle(prolly.key_proof_to_bytes(tampered)).valid)
        self.assertFalse(prolly.verify_key_proof(tampered).valid)

        multi = engine.prove_keys(tree, [b"a", b"missing", b"b"])
        verified_multi = prolly.verify_multi_key_proof(multi)
        self.assertTrue(verified_multi.valid)
        self.assertEqual(len(verified_multi.results), 3)
        self.assertEqual(verified_multi.results[0].value, b"1")
        self.assertTrue(verified_multi.results[1].absence)
        self.assertEqual(verified_multi.results[2].value, b"2")
        decoded_multi = prolly.multi_key_proof_from_node_bytes(
            multi.root,
            multi.keys,
            prolly.multi_key_proof_path_node_bytes(multi),
        )
        self.assertEqual(prolly.verify_multi_key_proof(decoded_multi).results[2].value, b"2")
        decoded_multi_from_bytes = prolly.multi_key_proof_from_bytes(
            prolly.multi_key_proof_to_bytes(multi)
        )
        self.assertEqual(
            prolly.verify_multi_key_proof(decoded_multi_from_bytes).results[2].value,
            b"2",
        )

        range_proof = engine.prove_range(tree, b"a", b"c")
        verified_range = prolly.verify_range_proof(range_proof)
        self.assertTrue(verified_range.valid)
        self.assertEqual([(entry.key, entry.value) for entry in verified_range.entries], [(b"a", b"1"), (b"b", b"2")])
        decoded_range = prolly.range_proof_from_node_bytes(
            range_proof.root,
            range_proof.start,
            range_proof.end,
            prolly.range_proof_path_node_bytes(range_proof),
        )
        self.assertEqual(prolly.verify_range_proof(decoded_range).entries[1].value, b"2")
        decoded_range_from_bytes = prolly.range_proof_from_bytes(
            prolly.range_proof_to_bytes(range_proof)
        )
        self.assertEqual(prolly.verify_range_proof(decoded_range_from_bytes).entries[1].value, b"2")
        prefix_proof = engine.prove_prefix(tree, b"a")
        verified_prefix = prolly.verify_range_proof(prefix_proof)
        self.assertTrue(verified_prefix.valid)
        self.assertEqual([(entry.key, entry.value) for entry in verified_prefix.entries], [(b"a", b"1")])
        proved_page = engine.prove_range_page(
            tree,
            prolly.RangeCursorRecord(after_key=b"a"),
            None,
            1,
        )
        page_verified = prolly.verify_range_page_proof(proved_page.proof)
        self.assertTrue(page_verified.valid)
        self.assertEqual([(entry.key, entry.value) for entry in page_verified.entries], [(b"b", b"2")])
        decoded_page = prolly.range_page_proof_from_node_bytes(
            proved_page.proof.root,
            proved_page.proof.after,
            proved_page.proof.end,
            prolly.range_page_proof_path_node_bytes(proved_page.proof),
        )
        self.assertEqual(prolly.verify_range_page_proof(decoded_page).entries[0].key, b"b")
        decoded_page_from_bytes = prolly.range_page_proof_from_bytes(
            prolly.range_page_proof_to_bytes(proved_page.proof)
        )
        self.assertEqual(prolly.verify_range_page_proof(decoded_page_from_bytes).entries[0].key, b"b")

        other = engine.delete(tree, b"a")
        other = engine.put(other, b"b", b"22")
        other = engine.put(other, b"d", b"4")
        proved_diff_page = engine.prove_diff_page(tree, other, None, None, 1)
        self.assertEqual(len(proved_diff_page.page.diffs), 1)
        self.assertEqual(proved_diff_page.page.diffs[0].kind, prolly.DiffKind.REMOVED)
        self.assertEqual(proved_diff_page.page.diffs[0].key, b"a")
        self.assertEqual(proved_diff_page.proof.base.end, b"b")
        self.assertEqual(proved_diff_page.proof.lookahead_base.key, b"b")
        self.assertEqual(proved_diff_page.page.next_cursor.after_key, b"a")

        diff_page_verified = prolly.verify_diff_page_proof(proved_diff_page.proof)
        self.assertTrue(diff_page_verified.valid)
        self.assertTrue(diff_page_verified.lookahead_valid)
        self.assertEqual(diff_page_verified.diffs, proved_diff_page.page.diffs)
        self.assertEqual(diff_page_verified.next_cursor, proved_diff_page.page.next_cursor)

        diff_page_bundle = prolly.diff_page_proof_to_bytes(proved_diff_page.proof)
        self.assertEqual(
            diff_page_bundle,
            prolly.diff_page_proof_to_bytes(proved_diff_page.proof),
        )
        diff_page_summary = prolly.inspect_proof_bundle(diff_page_bundle)
        self.assertEqual(diff_page_summary.kind, "diff_page")
        self.assertEqual(diff_page_summary.root, tree.root)
        self.assertEqual(diff_page_summary.other_root, other.root)
        self.assertEqual(diff_page_summary.limit, 1)
        self.assertTrue(diff_page_summary.has_lookahead)
        diff_page_bundle_verified = prolly.verify_proof_bundle(diff_page_bundle)
        self.assertTrue(diff_page_bundle_verified.valid)
        self.assertEqual(diff_page_bundle_verified.summary.kind, "diff_page")
        self.assertEqual(diff_page_bundle_verified.diff_count, 1)
        self.assertEqual(diff_page_bundle_verified.next_cursor, proved_diff_page.page.next_cursor)
        decoded_diff_page = prolly.diff_page_proof_from_bytes(diff_page_bundle)
        self.assertEqual(
            prolly.verify_diff_page_proof(decoded_diff_page).diffs,
            proved_diff_page.page.diffs,
        )

        signed = prolly.sign_proof_bundle_hmac_sha256(
            prolly.key_proof_to_bytes(proof),
            b"python-key",
            b"shared secret",
            b"tenant=t1",
            1700000000000,
            1700000100000,
            b"nonce-1",
        )
        signed_bytes = prolly.authenticated_proof_envelope_to_bytes(signed)
        self.assertEqual(signed_bytes, prolly.authenticated_proof_envelope_to_bytes(signed))
        decoded_signed = prolly.authenticated_proof_envelope_from_bytes(signed_bytes)
        envelope_verified = prolly.verify_authenticated_proof_envelope(
            decoded_signed,
            b"shared secret",
            1700000050000,
        )
        self.assertTrue(envelope_verified.valid)
        self.assertTrue(envelope_verified.signature_valid)
        self.assertEqual(envelope_verified.key_id, b"python-key")
        self.assertEqual(envelope_verified.context, b"tenant=t1")
        decoded_signed_proof = prolly.key_proof_from_bytes(envelope_verified.proof_bundle)
        self.assertEqual(prolly.verify_key_proof(decoded_signed_proof).value, b"2")
        authenticated_bundle = prolly.verify_authenticated_proof_bundle(
            signed_bytes,
            b"shared secret",
            1700000050000,
        )
        self.assertTrue(authenticated_bundle.valid)
        self.assertTrue(authenticated_bundle.envelope.valid)
        self.assertIsNone(authenticated_bundle.proof_error)
        self.assertEqual(authenticated_bundle.proof.exists_count, 1)
        self.assertFalse(
            prolly.verify_authenticated_proof_envelope(
                decoded_signed,
                b"wrong secret",
                1700000050000,
            ).valid
        )
        wrong_authenticated_bundle = prolly.verify_authenticated_proof_bundle(
            signed_bytes,
            b"wrong secret",
            1700000050000,
        )
        self.assertFalse(wrong_authenticated_bundle.valid)
        self.assertFalse(wrong_authenticated_bundle.envelope.valid)
        self.assertIsNone(wrong_authenticated_bundle.proof)

    def test_batch_with_stats_reports_coalescing_and_append_path(self) -> None:
        engine = prolly.ProllyEngine.memory(prolly.default_config())
        empty = engine.create()
        built = engine.build_from_sorted_entries(
            [
                prolly.EntryRecord(key=b"a", value=b"1"),
                prolly.EntryRecord(key=b"b", value=b"2"),
                prolly.EntryRecord(key=b"c", value=b"3"),
            ],
        )
        self.assertEqual(prolly.upsert_mutation(b"probe", b"value").kind, prolly.MutationKind.UPSERT)
        self.assertEqual(prolly.delete_mutation(b"probe").kind, prolly.MutationKind.DELETE)

        batch = engine.batch_with_stats(
            empty,
            [
                prolly.upsert_mutation(b"b", b"2"),
                prolly.upsert_mutation(b"a", b"1"),
                prolly.upsert_mutation(b"a", b"11"),
            ],
        )
        self.assertEqual(engine.get(batch.tree, b"a"), b"11")
        self.assertEqual(batch.stats.input_mutations, 3)
        self.assertEqual(batch.stats.effective_mutations, 2)
        self.assertFalse(batch.stats.preprocess_input_sorted)

        appended = engine.append_batch_with_stats(
            built,
            [
                prolly.MutationRecord(kind=prolly.MutationKind.UPSERT, key=b"d", value=b"4"),
                prolly.MutationRecord(kind=prolly.MutationKind.UPSERT, key=b"e", value=b"5"),
                prolly.MutationRecord(kind=prolly.MutationKind.UPSERT, key=b"d", value=b"44"),
            ],
        )
        self.assertEqual(engine.get(appended.tree, b"d"), b"44")
        self.assertEqual(appended.stats.input_mutations, 3)
        self.assertEqual(appended.stats.effective_mutations, 2)
        self.assertFalse(appended.stats.preprocess_input_sorted)
        self.assertTrue(appended.stats.used_append_fast_path)
        self.assertGreater(appended.stats.written_nodes, 0)

    def test_custom_store_callbacks_drive_engine(self) -> None:
        class MemoryHostStore(prolly.HostStoreCallback):
            def __init__(self) -> None:
                self.nodes: dict[bytes, bytes] = {}
                self.hints: dict[tuple[bytes, bytes], bytes] = {}
                self.roots: dict[bytes, prolly.RootManifestRecord] = {}

            def get(self, key: bytes):
                return prolly.HostStoreBytesResultRecord(
                    value=self.nodes.get(key),
                    error=None,
                )

            def put(self, key: bytes, value: bytes):
                self.nodes[bytes(key)] = bytes(value)
                return prolly.HostStoreUnitResultRecord(error=None)

            def delete(self, key: bytes):
                self.nodes.pop(key, None)
                return prolly.HostStoreUnitResultRecord(error=None)

            def batch(self, ops):
                for op in ops:
                    if op.kind == prolly.MutationKind.UPSERT:
                        self.nodes[bytes(op.key)] = bytes(op.value)
                    else:
                        self.nodes.pop(op.key, None)
                return prolly.HostStoreUnitResultRecord(error=None)

            def batch_get_ordered(self, keys):
                return prolly.HostStoreBatchGetResultRecord(
                    values=[self.nodes.get(key) for key in keys],
                    error=None,
                )

            def prefers_batch_reads(self):
                return prolly.HostStoreBoolResultRecord(value=True, error=None)

            def supports_hints(self):
                return prolly.HostStoreBoolResultRecord(value=True, error=None)

            def get_hint(self, namespace: bytes, key: bytes):
                return prolly.HostStoreBytesResultRecord(
                    value=self.hints.get((namespace, key)),
                    error=None,
                )

            def put_hint(self, namespace: bytes, key: bytes, value: bytes):
                self.hints[(bytes(namespace), bytes(key))] = bytes(value)
                return prolly.HostStoreUnitResultRecord(error=None)

            def list_node_cids(self):
                return prolly.HostStoreListBytesResultRecord(
                    values=list(self.nodes.keys()),
                    error=None,
                )

            def get_root(self, name: bytes):
                return prolly.HostStoreRootResultRecord(
                    value=self.roots.get(name),
                    error=None,
                )

            def put_root(self, name: bytes, manifest):
                self.roots[bytes(name)] = manifest
                return prolly.HostStoreUnitResultRecord(error=None)

            def delete_root(self, name: bytes):
                self.roots.pop(name, None)
                return prolly.HostStoreUnitResultRecord(error=None)

            def compare_and_swap_root(self, name: bytes, expected, replacement):
                current = self.roots.get(name)
                if self._same_manifest(current, expected):
                    if replacement is None:
                        self.roots.pop(name, None)
                    else:
                        self.roots[bytes(name)] = replacement
                    return prolly.HostStoreRootCasResultRecord(
                        applied=True,
                        current=None,
                        error=None,
                    )
                return prolly.HostStoreRootCasResultRecord(
                    applied=False,
                    current=current,
                    error=None,
                )

            def list_roots(self):
                return prolly.HostStoreListRootsResultRecord(
                    values=[
                        prolly.HostStoreNamedRootManifestRecord(
                            name=name,
                            manifest=manifest,
                        )
                        for name, manifest in self.roots.items()
                    ],
                    error=None,
                )

            @staticmethod
            def _same_manifest(left, right) -> bool:
                if left is None or right is None:
                    return left is right
                return prolly.root_manifest_to_bytes(left) == prolly.root_manifest_to_bytes(right)

        source = prolly.ProllyEngine.custom_store(
            MemoryHostStore(),
            prolly.default_config(),
        )
        empty = source.create()
        tree = source.batch(
            empty,
            [
                prolly.MutationRecord(
                    kind=prolly.MutationKind.UPSERT,
                    key=b"a",
                    value=b"1",
                ),
                prolly.MutationRecord(
                    kind=prolly.MutationKind.UPSERT,
                    key=b"b",
                    value=b"2",
                ),
            ],
        )

        self.assertEqual(source.get(tree, b"a"), b"1")
        self.assertEqual(source.get_many(tree, [b"a", b"missing", b"b"]), [b"1", None, b"2"])
        self.assertTrue(source.publish_prefix_path_hint(tree, b"a"))
        self.assertTrue(source.hydrate_prefix_path_hint(tree, b"a"))

        source.publish_named_root_at_millis(b"main", tree, 7)
        self.assertEqual(source.load_named_root(b"main"), tree)
        self.assertEqual(len(source.list_named_roots()), 1)
        self.assertGreater(len(source.list_node_cids()), 0)
        self.assertEqual(source.plan_store_gc([tree]).reclaimable_nodes, 0)

        destination = prolly.ProllyEngine.custom_store(
            MemoryHostStore(),
            prolly.default_config(),
        )
        plan = source.plan_missing_nodes(tree, destination)
        self.assertGreater(plan.missing_nodes, 0)
        copied = source.copy_missing_nodes(tree, destination)
        self.assertEqual(copied.copied_nodes, plan.missing_nodes)
        self.assertEqual(destination.get(tree, b"b"), b"2")

        update = source.compare_and_swap_named_root(b"main", tree, None)
        self.assertTrue(update.applied)
        self.assertFalse(update.conflict)
        self.assertIsNone(source.load_named_root(b"main"))

    def test_sqlite_engine_reopens_tree_nodes(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            path = str(Path(tmp) / "prolly.db")
            first = prolly.ProllyEngine.sqlite(path, prolly.default_config())
            tree = first.create()
            tree = first.put(tree, b"k", b"v")
            del first

            reopened = prolly.ProllyEngine.sqlite(path, prolly.default_config())
            self.assertEqual(reopened.get(tree, b"k"), b"v")

    def test_paging_merge_named_roots_and_manifest_helpers(self) -> None:
        engine = prolly.ProllyEngine.memory(prolly.default_config())
        empty = engine.create()
        tree = empty
        for key, value in [(b"a", b"1"), (b"b", b"2"), (b"c", b"3")]:
            tree = engine.put(tree, key, value)

        first_page = engine.range_page(tree, None, None, 2)
        self.assertEqual(
            [(entry.key, entry.value) for entry in first_page.entries],
            [(b"a", b"1"), (b"b", b"2")],
        )
        self.assertIsNotNone(first_page.next_cursor)
        after_a = engine.range_after(tree, b"a", None)
        self.assertEqual([entry.key for entry in after_a], [b"b", b"c"])
        self.assertEqual((engine.first_entry(tree).key, engine.first_entry(tree).value), (b"a", b"1"))
        self.assertEqual((engine.last_entry(tree).key, engine.last_entry(tree).value), (b"c", b"3"))
        self.assertEqual((engine.lower_bound(tree, b"bb").key, engine.lower_bound(tree, b"bb").value), (b"c", b"3"))
        self.assertIsNone(engine.upper_bound(tree, b"c"))
        self.assertEqual([(entry.key, entry.value) for entry in engine.prefix(tree, b"b")], [(b"b", b"2")])
        prefix_page = engine.prefix_page(tree, b"b", None, 1)
        self.assertEqual([(entry.key, entry.value) for entry in prefix_page.entries], [(b"b", b"2")])
        self.assertIsNotNone(prefix_page.next_cursor)
        self.assertIsNone(prolly.range_cursor_start().after_key)
        after_a_cursor = prolly.range_cursor_after_key(b"a")
        self.assertEqual(after_a_cursor.after_key, b"a")
        from_cursor = engine.range_from_cursor(
            tree,
            after_a_cursor,
            None,
        )
        self.assertEqual(
            [entry.key for entry in from_cursor],
            [entry.key for entry in after_a],
        )
        window = engine.cursor_window(tree, b"bb", None, 1)
        self.assertEqual(window.position_key, b"b")
        self.assertEqual(window.position_value, b"2")
        self.assertFalse(window.found)
        self.assertEqual([(entry.key, entry.value) for entry in window.entries], [(b"c", b"3")])
        self.assertEqual(window.next_cursor.after_key, b"c")

        exact_probe = engine.cursor_window(tree, b"b", None, 0)
        self.assertTrue(exact_probe.found)
        self.assertEqual(exact_probe.position_key, b"b")
        self.assertEqual(exact_probe.entries, [])
        self.assertIsNone(exact_probe.next_cursor)

        second_page = engine.range_page(tree, first_page.next_cursor, None, 2)
        self.assertEqual(
            [(entry.key, entry.value) for entry in second_page.entries],
            [(b"c", b"3")],
        )
        self.assertIsNone(second_page.next_cursor)

        self.assertIsNone(prolly.reverse_cursor_end().before_key)
        before_c_cursor = prolly.reverse_cursor_before_key(b"c")
        self.assertEqual(before_c_cursor.before_key, b"c")
        reverse_first = engine.reverse_page(tree, None, b"", 2)
        self.assertEqual(
            [(entry.key, entry.value) for entry in reverse_first.entries],
            [(b"c", b"3"), (b"b", b"2")],
        )
        self.assertEqual(reverse_first.next_cursor.before_key, b"b")
        reverse_second = engine.reverse_page(tree, reverse_first.next_cursor, b"", 2)
        self.assertEqual(
            [(entry.key, entry.value) for entry in reverse_second.entries],
            [(b"a", b"1")],
        )
        self.assertIsNone(reverse_second.next_cursor)
        prefix_reverse = engine.prefix_reverse_page(tree, b"b", None, 8)
        self.assertEqual(
            [(entry.key, entry.value) for entry in prefix_reverse.entries],
            [(b"b", b"2")],
        )
        self.assertIsNone(prefix_reverse.next_cursor)

        diff_page = engine.diff_page(empty, tree, None, None, 1)
        self.assertEqual(len(diff_page.diffs), 1)
        self.assertIsNotNone(diff_page.next_cursor)

        changed_for_cursor = engine.batch(
            tree,
            [
                prolly.MutationRecord(
                    kind=prolly.MutationKind.UPSERT,
                    key=b"b",
                    value=b"22",
                ),
                prolly.MutationRecord(
                    kind=prolly.MutationKind.UPSERT,
                    key=b"c",
                    value=b"33",
                ),
            ],
        )
        resumed_diffs = engine.diff_from_cursor(
            tree,
            changed_for_cursor,
            prolly.RangeCursorRecord(after_key=b"a"),
            b"c",
        )
        self.assertEqual([(diff.kind, diff.key) for diff in resumed_diffs], [(prolly.DiffKind.CHANGED, b"b")])

        built = engine.build_from_entries(
            [
                prolly.EntryRecord(key=b"c", value=b"3"),
                prolly.EntryRecord(key=b"a", value=b"1"),
                prolly.EntryRecord(key=b"b", value=b"2"),
            ]
        )
        sorted_built = engine.build_from_sorted_entries(
            [
                prolly.EntryRecord(key=b"a", value=b"1"),
                prolly.EntryRecord(key=b"b", value=b"2"),
                prolly.EntryRecord(key=b"c", value=b"3"),
            ]
        )
        self.assertEqual(built.root, sorted_built.root)
        with self.assertRaises(Exception):
            engine.build_from_sorted_entries(
                [
                    prolly.EntryRecord(key=b"b", value=b"2"),
                    prolly.EntryRecord(key=b"a", value=b"1"),
                ]
            )
        appended = engine.append_batch(
            built,
            [
                prolly.MutationRecord(
                    kind=prolly.MutationKind.UPSERT,
                    key=b"d",
                    value=b"4",
                ),
                prolly.MutationRecord(
                    kind=prolly.MutationKind.UPSERT,
                    key=b"e",
                    value=b"5",
                ),
                prolly.MutationRecord(
                    kind=prolly.MutationKind.UPSERT,
                    key=b"d",
                    value=b"44",
                ),
            ],
        )
        self.assertEqual(engine.get(appended, b"d"), b"44")

        conflict_base = empty
        for key, value in [(b"a", b"base-a"), (b"b", b"base-b")]:
            conflict_base = engine.put(conflict_base, key, value)
        conflict_left = conflict_base
        for key, value in [(b"a", b"left-a"), (b"b", b"left-b")]:
            conflict_left = engine.put(conflict_left, key, value)
        conflict_right = conflict_base
        for key, value in [(b"a", b"right-a"), (b"b", b"right-b")]:
            conflict_right = engine.put(conflict_right, key, value)

        conflict_page = engine.conflict_page(
            conflict_base, conflict_left, conflict_right, None, 1
        )
        self.assertEqual(len(conflict_page.conflicts), 1)
        self.assertEqual(conflict_page.conflicts[0].key, b"a")
        self.assertEqual(conflict_page.conflicts[0].base, b"base-a")
        self.assertEqual(conflict_page.conflicts[0].left, b"left-a")
        self.assertEqual(conflict_page.conflicts[0].right, b"right-a")
        self.assertIsNotNone(conflict_page.next_cursor)

        second_conflict_page = engine.conflict_page(
            conflict_base,
            conflict_left,
            conflict_right,
            conflict_page.next_cursor,
            1,
        )
        self.assertEqual(len(second_conflict_page.conflicts), 1)
        self.assertEqual(second_conflict_page.conflicts[0].key, b"b")
        self.assertIsNone(second_conflict_page.next_cursor)

        parallel = engine.parallel_batch(
            tree,
            [
                prolly.MutationRecord(
                    kind=prolly.MutationKind.UPSERT,
                    key=b"d",
                    value=b"4",
                ),
                prolly.MutationRecord(
                    kind=prolly.MutationKind.UPSERT,
                    key=b"e",
                    value=b"5",
                ),
            ],
            prolly.ParallelConfigRecord(max_threads=1, parallelism_threshold=1),
        )
        self.assertEqual(engine.get(parallel, b"e"), b"5")
        parallel_stats = engine.parallel_batch_with_stats(
            tree,
            [
                prolly.MutationRecord(
                    kind=prolly.MutationKind.UPSERT,
                    key=b"f",
                    value=b"6",
                ),
                prolly.MutationRecord(
                    kind=prolly.MutationKind.UPSERT,
                    key=b"g",
                    value=b"7",
                ),
            ],
            prolly.ParallelConfigRecord(max_threads=1, parallelism_threshold=1),
        )
        self.assertEqual(engine.get(parallel_stats.tree, b"g"), b"7")
        self.assertEqual(parallel_stats.stats.input_mutations, 2)
        self.assertEqual(parallel_stats.stats.effective_mutations, 2)
        self.assertGreater(parallel_stats.stats.written_nodes, 0)

        base = engine.put(empty, b"k", b"base")
        left = engine.put(base, b"k", b"left")
        right = engine.put(base, b"k", b"right")
        explanation = engine.merge_explain(base, left, right, "prefer_right")
        self.assertIsNotNone(explanation.result)
        self.assertIsNone(explanation.error)
        self.assertIn("events", explanation.trace_json)
        self.assertGreater(len(explanation.trace.events), 0)
        self.assertTrue(
            any(
                event.kind == prolly.MergeTraceEventKind.RESOLVER_CALLED
                and event.resolution == prolly.MergeTraceResolutionKind.VALUE
                for event in explanation.trace.events
            )
        )
        merged = engine.merge(base, left, right, "prefer_right")
        self.assertEqual(engine.get(merged, b"k"), b"right")
        merged_range = engine.merge_range(base, left, right, b"k", None, "prefer_right")
        self.assertEqual(engine.get(merged_range, b"k"), b"right")
        merged_prefix = engine.merge_prefix(base, left, right, b"k", "prefer_right")
        self.assertEqual(engine.get(merged_prefix, b"k"), b"right")

        class JoinResolver(prolly.MergeResolverCallback):
            def resolve(self, conflict):
                if conflict.left is not None and conflict.right is not None:
                    return prolly.resolution_value(conflict.left + b"|" + conflict.right)
                if conflict.left is not None:
                    return prolly.resolution_value(conflict.left)
                if conflict.right is not None:
                    return prolly.resolution_value(conflict.right)
                return prolly.resolution_delete()

        resolver = JoinResolver()
        self.assertEqual(prolly.resolution_unresolved().kind, prolly.ResolutionKind.UNRESOLVED)
        update_conflict = prolly.ConflictRecord(
            key=b"k", base=b"base", left=b"left", right=b"right"
        )
        self.assertEqual(prolly.resolve_prefer_left(update_conflict).value, b"left")
        self.assertEqual(
            prolly.resolve_delete_wins(update_conflict).kind,
            prolly.ResolutionKind.UNRESOLVED,
        )
        delete_conflict = prolly.ConflictRecord(
            key=b"k", base=b"base", left=None, right=b"right"
        )
        self.assertEqual(
            prolly.resolve_delete_wins(delete_conflict).kind,
            prolly.ResolutionKind.DELETE,
        )
        self.assertEqual(prolly.resolve_update_wins(delete_conflict).value, b"right")

        class PreferRightResolver(prolly.MergeResolverCallback):
            def resolve(self, conflict):
                return prolly.resolve_prefer_right(conflict)

        prefer_right_merged = engine.merge_with_resolver(
            base, left, right, PreferRightResolver()
        )
        self.assertEqual(engine.get(prefer_right_merged, b"k"), b"right")
        callback_merged = engine.merge_with_resolver(base, left, right, resolver)
        self.assertEqual(engine.get(callback_merged, b"k"), b"left|right")
        callback_explanation = engine.merge_explain_with_resolver(
            base, left, right, resolver
        )
        self.assertIsNotNone(callback_explanation.result)
        self.assertIsNone(callback_explanation.error)
        callback_range = engine.merge_range_with_resolver(
            base, left, right, b"k", None, resolver
        )
        self.assertEqual(engine.get(callback_range, b"k"), b"left|right")
        callback_prefix = engine.merge_prefix_with_resolver(
            base, left, right, b"k", resolver
        )
        self.assertEqual(engine.get(callback_prefix, b"k"), b"left|right")

        policy_base = engine.batch(
            empty,
            [
                prolly.MutationRecord(
                    kind=prolly.MutationKind.UPSERT,
                    key=b"doc/title",
                    value=b"base-title",
                ),
                prolly.MutationRecord(
                    kind=prolly.MutationKind.UPSERT,
                    key=b"k",
                    value=b"base-k",
                ),
            ],
        )
        policy_left = engine.batch(
            policy_base,
            [
                prolly.MutationRecord(
                    kind=prolly.MutationKind.UPSERT,
                    key=b"doc/title",
                    value=b"left-title",
                ),
                prolly.MutationRecord(
                    kind=prolly.MutationKind.UPSERT,
                    key=b"k",
                    value=b"left-k",
                ),
            ],
        )
        policy_right = engine.batch(
            policy_base,
            [
                prolly.MutationRecord(
                    kind=prolly.MutationKind.UPSERT,
                    key=b"doc/title",
                    value=b"right-title",
                ),
                prolly.MutationRecord(
                    kind=prolly.MutationKind.UPSERT,
                    key=b"k",
                    value=b"right-k",
                ),
            ],
        )
        policy = prolly.MergePolicyRegistry()
        self.assertTrue(policy.is_empty())
        self.assertFalse(policy.has_default())
        policy.set_default_resolver_name("prefer_left")
        policy.push_prefix_resolver(b"doc/", resolver)
        policy.push_exact_resolver_name(b"k", "prefer_right")
        self.assertEqual(policy.len(), 2)
        self.assertTrue(policy.has_default())

        policy_merged = engine.merge_with_policy(
            policy_base,
            policy_left,
            policy_right,
            policy,
        )
        self.assertEqual(engine.get(policy_merged, b"doc/title"), b"left-title|right-title")
        self.assertEqual(engine.get(policy_merged, b"k"), b"right-k")
        policy_explanation = engine.merge_explain_with_policy(
            policy_base,
            policy_left,
            policy_right,
            policy,
        )
        self.assertIsNotNone(policy_explanation.result)
        self.assertIsNone(policy_explanation.error)
        policy_range = engine.merge_range_with_policy(
            policy_base,
            policy_left,
            policy_right,
            b"doc/",
            b"doc0",
            policy,
        )
        self.assertEqual(engine.get(policy_range, b"doc/title"), b"left-title|right-title")
        policy_prefix = engine.merge_prefix_with_policy(
            policy_base,
            policy_left,
            policy_right,
            b"doc/",
            policy,
        )
        self.assertEqual(engine.get(policy_prefix, b"doc/title"), b"left-title|right-title")

        engine.publish_named_root_at_millis(b"main", merged, 42)
        self.assertEqual(engine.load_named_root(b"main"), merged)
        self.assertEqual(len(engine.list_named_roots()), 1)
        manifests = engine.list_named_root_manifests()
        self.assertEqual(len(manifests), 1)
        self.assertEqual(manifests[0].name, b"main")
        self.assertEqual(manifests[0].manifest.tree, merged)
        self.assertEqual(manifests[0].manifest.created_at_millis, 42)
        self.assertEqual(manifests[0].manifest.updated_at_millis, 42)
        retention = prolly.retain_all_named_roots()
        self.assertEqual(retention.kind, prolly.NamedRootRetentionKind.ALL)
        exact_retention = prolly.retain_exact_named_roots([b"main", b"missing"])
        self.assertEqual(exact_retention.kind, prolly.NamedRootRetentionKind.EXACT)
        self.assertEqual(exact_retention.names, [b"main", b"missing"])
        prefix_retention = prolly.retain_named_root_prefix(b"ma")
        self.assertEqual(prefix_retention.kind, prolly.NamedRootRetentionKind.PREFIX)
        self.assertEqual(prefix_retention.prefix, b"ma")
        newest_retention = prolly.retain_newest_named_roots(b"checkpoint/", 2)
        self.assertEqual(newest_retention.kind, prolly.NamedRootRetentionKind.NEWEST_BY_NAME)
        self.assertEqual(newest_retention.prefix, b"checkpoint/")
        self.assertEqual(newest_retention.count, 2)
        updated_retention = prolly.retain_named_roots_updated_since(b"checkpoint/", 42)
        self.assertEqual(updated_retention.kind, prolly.NamedRootRetentionKind.UPDATED_SINCE)
        self.assertEqual(updated_retention.prefix, b"checkpoint/")
        self.assertEqual(updated_retention.min_updated_at_millis, 42)
        self.assertEqual(len(engine.load_retained_named_roots(retention).roots), 1)
        self.assertEqual(
            engine.plan_store_gc_for_retention(retention).reachability.live_nodes,
            1,
        )
        self.assertEqual(
            engine.sweep_store_gc_for_retention(retention).plan.reachability.live_nodes,
            1,
        )

        update = engine.compare_and_swap_named_root(b"main", merged, None)
        self.assertTrue(update.applied)
        self.assertFalse(update.conflict)
        self.assertIsNone(engine.load_named_root(b"main"))

        manifest = prolly.RootManifestRecord(
            tree=merged,
            created_at_millis=10,
            updated_at_millis=20,
        )
        manifest_bytes = prolly.root_manifest_to_bytes(manifest)
        self.assertEqual(prolly.root_manifest_from_bytes(manifest_bytes), manifest)

    def test_node_and_value_helpers_round_trip(self) -> None:
        self.assertEqual(prolly.encoding_raw().kind, prolly.EncodingKind.RAW)
        self.assertEqual(prolly.encoding_cbor().kind, prolly.EncodingKind.CBOR)
        self.assertEqual(prolly.encoding_json().kind, prolly.EncodingKind.JSON)
        custom_encoding = prolly.encoding_custom("postcard")
        self.assertEqual(custom_encoding.kind, prolly.EncodingKind.CUSTOM)
        self.assertEqual(custom_encoding.custom_name, "postcard")
        config = prolly.tree_config(2, 64, 32, 7, custom_encoding, 16, 4096)
        self.assertEqual(config.encoding, custom_encoding)
        self.assertEqual(config.node_cache_max_nodes, 16)
        with self.assertRaises(prolly.ProllyBindingError.InvalidArgument):
            prolly.tree_config(
                2,
                64,
                32,
                7,
                prolly.EncodingRecord(kind=prolly.EncodingKind.CUSTOM, custom_name=None),
                None,
                None,
            )
        self.assertEqual(prolly.large_value_config(8).inline_threshold, 8)
        self.assertEqual(prolly.parallel_config(2, 24).max_threads, 2)
        self.assertEqual(prolly.parallel_config_sequential().max_threads, 1)

        encoding = prolly.encoding_raw()
        node = prolly.NodeRecord(
            keys=[b"a"],
            vals=[b"1"],
            leaf=True,
            level=0,
            min_chunk_size=4,
            max_chunk_size=1024,
            chunking_factor=128,
            hash_seed=0,
            encoding=encoding,
        )

        node_bytes = prolly.node_to_bytes(node)
        decoded = prolly.node_from_bytes(node_bytes)
        self.assertEqual(decoded, node)
        self.assertEqual(prolly.node_cid(decoded), prolly.cid_from_bytes(node_bytes))

        value = prolly.VersionedValueRecord(
            schema="example",
            version=1,
            encoding=encoding,
            payload=b"payload",
        )
        value_bytes = prolly.versioned_value_to_bytes(value)
        self.assertEqual(prolly.versioned_value_from_bytes(value_bytes), value)
        self.assertTrue(prolly.versioned_value_matches_schema(value, "example", 1))
        self.assertFalse(prolly.versioned_value_matches_schema(value, "example", 2))
        prolly.versioned_value_require_schema(value, "example", 1)
        with self.assertRaises(prolly.ProllyBindingError.Serialization):
            prolly.versioned_value_require_schema(value, "other", 1)
        self.assertTrue(prolly.versioned_value_bytes_matches_schema(value_bytes, "example", 1))
        self.assertFalse(prolly.versioned_value_bytes_matches_schema(value_bytes, "example", 2))
        prolly.versioned_value_bytes_require_schema(value_bytes, "example", 1)
        with self.assertRaises(prolly.ProllyBindingError.Serialization):
            prolly.versioned_value_bytes_require_schema(value_bytes, "other", 1)

        value_ref = prolly.ValueRefRecord(
            kind=prolly.ValueRefKind.INLINE,
            value=b"plain",
            blob=None,
        )
        value_ref_bytes = prolly.value_ref_to_bytes(value_ref)
        self.assertEqual(prolly.value_ref_from_bytes(value_ref_bytes), value_ref)
        self.assertEqual(prolly.value_ref_from_stored_bytes(value_ref_bytes), value_ref)
        self.assertTrue(prolly.value_ref_inline_requires_escape(value_ref_bytes))
        raw_value_ref = prolly.value_ref_from_stored_bytes(b"plain")
        self.assertEqual(raw_value_ref.kind, prolly.ValueRefKind.INLINE)
        self.assertEqual(raw_value_ref.value, b"plain")
        self.assertFalse(prolly.value_ref_inline_requires_escape(b"plain"))

    def test_blob_store_and_large_value_helpers(self) -> None:
        engine = prolly.ProllyEngine.memory(prolly.default_config())
        blob_store = prolly.ProllyBlobStore.memory()

        direct_ref = blob_store.put_blob(b"direct")
        self.assertEqual(blob_store.get_blob(direct_ref), b"direct")
        prolly.blob_ref_validate_bytes(direct_ref, b"direct")
        with self.assertRaises(prolly.ProllyBindingError.Serialization):
            prolly.blob_ref_validate_bytes(direct_ref, b"wrong")
        blob_store.delete_blob(direct_ref)
        self.assertEqual(blob_store.blob_count(), 0)

        tree = engine.put_large_value(
            blob_store,
            engine.create(),
            b"big",
            b"large payload",
            prolly.LargeValueConfigRecord(inline_threshold=4),
        )
        self.assertEqual(engine.get_value_ref(tree, b"big").kind, prolly.ValueRefKind.BLOB)
        self.assertEqual(engine.get_large_value(blob_store, tree, b"big"), b"large payload")

        reachable = engine.mark_reachable_blobs([tree])
        self.assertEqual(reachable.live_blob_count, 1)
        self.assertEqual(
            engine.plan_blob_gc(blob_store, [tree], reachable.live_blobs)
            .reclaimable_blob_count,
            0,
        )

        orphan = blob_store.put_blob(b"orphan")
        self.assertIn(orphan, blob_store.list_blob_refs())
        self.assertEqual(
            engine.plan_blob_store_gc(blob_store, [tree]).reclaimable_blob_count,
            1,
        )
        self.assertEqual(
            engine.sweep_blob_store_gc(blob_store, [tree]).deleted_blobs,
            1,
        )
        self.assertEqual(blob_store.blob_count(), 1)

    def test_inspection_sync_gc_crdt_and_tombstone_helpers(self) -> None:
        engine = prolly.ProllyEngine.memory(prolly.default_config())
        empty = engine.create()

        base_value = prolly.timestamped_value_to_bytes(
            prolly.TimestampedValueRecord(value=b"base", timestamp=1)
        )
        left_value = prolly.timestamped_value_to_bytes(
            prolly.TimestampedValueRecord(value=b"left", timestamp=2)
        )
        right_value = prolly.timestamped_value_to_bytes(
            prolly.TimestampedValueRecord(value=b"right", timestamp=3)
        )

        base = engine.put(empty, b"k", base_value)
        left = engine.put(base, b"k", left_value)
        right = engine.put(base, b"k", right_value)
        merged = engine.crdt_merge(
            base,
            left,
            right,
            prolly.crdt_config_lww(prolly.CrdtDeletePolicyKind.UPDATE_WINS),
        )
        self.assertEqual(
            prolly.timestamped_value_from_bytes(engine.get(merged, b"k")),
            prolly.TimestampedValueRecord(value=b"right", timestamp=3),
        )

        class CrdtJoinResolver(prolly.CrdtResolverCallback):
            def resolve(self, conflict):
                if conflict.left is not None and conflict.right is not None:
                    return prolly.crdt_resolution_value(conflict.left + b"|" + conflict.right)
                if conflict.left is not None:
                    return prolly.crdt_resolution_value(conflict.left)
                if conflict.right is not None:
                    return prolly.crdt_resolution_value(conflict.right)
                return prolly.crdt_resolution_delete()

        callback_merged = engine.crdt_merge_with_resolver(
            base,
            left,
            right,
            prolly.CrdtDeletePolicyKind.UPDATE_WINS,
            CrdtJoinResolver(),
        )
        self.assertEqual(engine.get(callback_merged, b"k"), left_value + b"|" + right_value)

        page = engine.structural_diff_page(empty, merged, None, 1)
        self.assertEqual(len(page.diffs), 1)
        self.assertEqual(page.stats.emitted_diffs, 1)
        cursor_page = engine.structural_diff_page(empty, merged, None, 0)
        self.assertIsNotNone(cursor_page.next_cursor)
        self.assertIsNotNone(cursor_page.next_cursor_json)
        resumed_page = engine.structural_diff_page_with_cursor(
            empty,
            merged,
            cursor_page.next_cursor,
            1,
        )
        self.assertEqual(len(resumed_page.diffs), 1)
        self.assertEqual(len(engine.range_diff(empty, merged, b"k", b"l")), 1)
        self.assertEqual(
            engine.get_value_ref(merged, b"k").kind,
            prolly.ValueRefKind.INLINE,
        )

        self.assertGreater(json.loads(engine.collect_stats_json(merged).json)["num_nodes"], 0)
        typed_stats = engine.collect_stats(merged)
        self.assertEqual(typed_stats.total_key_value_pairs, 1)
        self.assertTrue(
            any(level.level == 0 and level.value > 0 for level in typed_stats.nodes_per_level)
        )
        typed_diff_stats = engine.stats_diff(empty, merged)
        self.assertEqual(typed_diff_stats.before.total_key_value_pairs, 0)
        self.assertEqual(typed_diff_stats.after.total_key_value_pairs, 1)
        self.assertEqual(typed_diff_stats.absolute.total_key_value_pairs_diff, 1)
        self.assertIn("level", engine.debug_tree_text(merged))
        debug_tree = engine.debug_tree(merged)
        self.assertGreater(len(debug_tree.levels), 0)
        self.assertTrue(any(level.nodes for level in debug_tree.levels))
        self.assertIn("right_only_nodes", engine.debug_compare_trees_json(empty, merged).json)
        debug_comparison = engine.debug_compare_trees(empty, merged)
        self.assertEqual(debug_comparison.left_only_nodes, 0)
        self.assertGreater(debug_comparison.right_only_nodes, 0)
        self.assertTrue(
            any(
                node.status == prolly.TreeDebugNodeStatusKind.RIGHT_ONLY
                for level in debug_comparison.levels
                for node in level.nodes
            )
        )

        reachable = engine.mark_reachable([merged])
        self.assertGreater(reachable.live_nodes, 0)
        self.assertGreater(len(engine.list_node_cids()), 0)
        self.assertEqual(engine.plan_gc([merged], reachable.live_cids).reclaimable_nodes, 0)

        destination = prolly.ProllyEngine.memory(prolly.default_config())
        plan = engine.plan_missing_nodes(merged, destination)
        self.assertGreater(plan.missing_nodes, 0)
        copied = engine.copy_missing_nodes(merged, destination)
        self.assertEqual(copied.copied_nodes, plan.missing_nodes)
        self.assertEqual(destination.get(merged, b"k"), engine.get(merged, b"k"))

        snapshot_bundle = engine.export_snapshot(merged)
        self.assertEqual(snapshot_bundle.format_version, 1)
        self.assertGreater(len(snapshot_bundle.nodes), 0)
        snapshot_bundle_bytes = prolly.snapshot_bundle_to_bytes(snapshot_bundle)
        snapshot_bundle_digest = prolly.snapshot_bundle_digest(snapshot_bundle)
        self.assertEqual(snapshot_bundle_digest, prolly.cid_from_bytes(snapshot_bundle_bytes))
        self.assertEqual(
            prolly.snapshot_bundle_digest_bytes(snapshot_bundle_bytes),
            snapshot_bundle_digest,
        )
        snapshot_summary = prolly.snapshot_bundle_summary(snapshot_bundle)
        self.assertEqual(snapshot_summary.format_version, 1)
        self.assertEqual(snapshot_summary.node_count, len(snapshot_bundle.nodes))
        self.assertGreater(snapshot_summary.byte_count, 0)
        self.assertEqual(
            prolly.snapshot_bundle_summary_from_bytes(snapshot_bundle_bytes),
            snapshot_summary,
        )
        snapshot_verification = prolly.verify_snapshot_bundle(snapshot_bundle)
        self.assertTrue(snapshot_verification.valid)
        self.assertEqual(snapshot_verification.summary, snapshot_summary)
        self.assertEqual(snapshot_verification.missing_cids, [])
        self.assertEqual(snapshot_verification.extra_cids, [])
        self.assertEqual(
            prolly.verify_snapshot_bundle_bytes(snapshot_bundle_bytes),
            snapshot_verification,
        )
        incomplete_snapshot_bundle = prolly.SnapshotBundleRecord(
            format_version=snapshot_bundle.format_version,
            tree=snapshot_bundle.tree,
            nodes=snapshot_bundle.nodes[:-1],
        )
        incomplete_verification = prolly.verify_snapshot_bundle(incomplete_snapshot_bundle)
        self.assertFalse(incomplete_verification.valid)
        self.assertGreater(len(incomplete_verification.missing_cids), 0)
        snapshot_bundle = prolly.snapshot_bundle_from_bytes(snapshot_bundle_bytes)
        snapshot_destination = prolly.ProllyEngine.memory(prolly.default_config())
        imported_tree = snapshot_destination.import_snapshot(snapshot_bundle)
        self.assertEqual(snapshot_destination.get(imported_tree, b"k"), engine.get(merged, b"k"))

        engine.pin_tree_root(merged)
        engine.pin_tree_path(merged, b"k")
        self.assertGreater(engine.cache_stats().cached_nodes, 0)
        self.assertGreater(engine.metrics().nodes_written, 0)
        engine.reset_metrics()
        self.assertEqual(engine.metrics().nodes_written, 0)
        self.assertFalse(engine.publish_prefix_path_hint(merged, b"k"))
        self.assertFalse(engine.hydrate_prefix_path_hint(merged, b"k"))
        self.assertEqual(prolly.changed_span_from_key(b"k").end, b"k\x00")
        self.assertEqual(prolly.changed_span_for_prefix(b"k").end, b"l")
        self.assertFalse(
            engine.publish_changed_spans_hint(
                empty,
                merged,
                [prolly.changed_span(b"k", b"l")],
            )
        )
        self.assertIsNone(engine.load_changed_spans_hint(empty, merged))

        tombstone = prolly.TombstoneRecord(
            actor=b"actor-1",
            timestamp_millis=123,
            causal_metadata=[
                prolly.TombstoneMetadataRecord(key="clock", value=b"7"),
            ],
        )
        tombstone_bytes = prolly.tombstone_to_bytes(tombstone)
        self.assertTrue(prolly.is_tombstone_value(tombstone_bytes))
        self.assertEqual(prolly.tombstone_from_bytes(tombstone_bytes), tombstone)
        self.assertEqual(prolly.tombstone_from_stored_bytes(tombstone_bytes), tombstone)
        self.assertEqual(
            prolly.tombstone_upsert_mutation(b"deleted", tombstone).kind,
            prolly.MutationKind.UPSERT,
        )
        self.assertEqual(
            prolly.tombstone_compaction_mutation(b"deleted", tombstone_bytes).kind,
            prolly.MutationKind.DELETE,
        )
        self.assertEqual(
            prolly.multi_value_set_from_bytes(
                prolly.multi_value_set_to_bytes([b"b", b"a", b"a"])
            ),
            [b"a", b"b"],
        )


if __name__ == "__main__":
    unittest.main()
