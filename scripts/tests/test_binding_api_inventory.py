from __future__ import annotations

import unittest

from scripts.binding_api_inventory import (
    ApiItem,
    CheckResult,
    build_classification_audit,
    check_manifest,
    extract_public_api,
    extract_public_api_items,
    generate_manifest,
    missing_feature_sentinels,
    review_abstraction_entries,
    validate_equivalence_catalog,
)


class ManifestCheckTests(unittest.TestCase):
    def test_abstraction_review_classifies_without_claiming_implementation(self) -> None:
        items = {
            "prolly::Store": ApiItem(
                rust="prolly::Store",
                kind="trait",
                owner=None,
                member_kind=None,
            ),
            "prolly::Store::Error": ApiItem(
                rust="prolly::Store::Error",
                kind="assoc_type",
                owner="prolly::Store",
                member_kind="trait-item",
            ),
            "prolly::KeyValue": ApiItem(
                rust="prolly::KeyValue",
                kind="type_alias",
                owner=None,
                member_kind=None,
            ),
            "prolly::resolver": ApiItem(
                rust="prolly::resolver",
                kind="module",
                owner=None,
                member_kind=None,
            ),
        }
        manifest = {
            "operations": [
                {
                    "rust": rust,
                    "classification": "portable",
                    "status": "planned",
                    "languages": {},
                    "tests": [],
                    "docs": [],
                }
                for rust in items
            ]
        }

        reviewed = review_abstraction_entries(items, manifest)
        entries = {entry["rust"]: entry for entry in reviewed["operations"]}

        self.assertEqual(entries["prolly::Store"]["equivalence"], "store-trait")
        self.assertEqual(entries["prolly::Store"]["classification"], "idiomatic")
        self.assertEqual(
            entries["prolly::Store::Error"]["equivalence"],
            "marker-and-associated-type",
        )
        self.assertEqual(
            entries["prolly::Store::Error"]["classification"],
            "rust-language-only",
        )
        self.assertEqual(entries["prolly::KeyValue"]["equivalence"], "record-alias")
        self.assertEqual(
            entries["prolly::resolver"]["equivalence"], "namespace-module"
        )
        for entry in entries.values():
            self.assertTrue(entry["reviewed"])
            self.assertEqual(entry["status"], "planned")
            self.assertEqual(entry["languages"], {})

    def test_release_rejects_unreviewed_idiomatic_mapping(self) -> None:
        languages = {
            language: f"{language}.iterable"
            for language in (
                "python",
                "go",
                "node",
                "kotlin",
                "java",
                "ruby",
                "swift",
                "wasm",
            )
        }
        result = check_manifest(
            rust_items={"prolly::DiffIter"},
            manifest={
                "operations": [
                    {
                        "rust": "prolly::DiffIter",
                        "classification": "idiomatic",
                        "status": "implemented",
                        "languages": languages,
                        "exclusions": {},
                        "tests": ["binding.iteration"],
                        "equivalence": "iterator-sequence",
                    }
                ]
            },
            release=True,
            equivalences={
                "iterator-sequence": self.complete_equivalence("idiomatic")
            },
        )

        self.assertEqual(result.incomplete, ("prolly::DiffIter",))

    def test_release_accepts_reviewed_idiomatic_equivalence(self) -> None:
        languages = {
            language: f"{language}.iterable"
            for language in (
                "python",
                "go",
                "node",
                "kotlin",
                "java",
                "ruby",
                "swift",
                "wasm",
            )
        }
        result = check_manifest(
            rust_items={"prolly::DiffIter"},
            manifest={
                "operations": [
                    {
                        "rust": "prolly::DiffIter",
                        "classification": "idiomatic",
                        "status": "implemented",
                        "languages": languages,
                        "exclusions": {},
                        "tests": ["binding.iteration"],
                        "docs": ["bindings/api/README.md#idiomatic-equivalents"],
                        "equivalence": "iterator-sequence",
                        "rationale": "Host sequences preserve ordered iteration.",
                        "reviewed": True,
                    }
                ]
            },
            release=True,
            equivalences={
                "iterator-sequence": self.complete_equivalence("idiomatic")
            },
        )

        self.assertTrue(result.ok)

    def test_release_rejects_unknown_equivalence_id(self) -> None:
        languages = {
            language: f"{language}.iterable"
            for language in (
                "python",
                "go",
                "node",
                "kotlin",
                "java",
                "ruby",
                "swift",
                "wasm",
            )
        }
        result = check_manifest(
            rust_items={"prolly::DiffIter"},
            manifest={
                "operations": [
                    {
                        "rust": "prolly::DiffIter",
                        "classification": "idiomatic",
                        "status": "implemented",
                        "languages": languages,
                        "exclusions": {},
                        "tests": ["binding.iteration"],
                        "docs": ["bindings/api/README.md#idiomatic-equivalents"],
                        "equivalence": "not-in-catalog",
                        "rationale": "Host sequences preserve ordered iteration.",
                        "reviewed": True,
                    }
                ]
            },
            release=True,
            equivalences={
                "iterator-sequence": self.complete_equivalence("idiomatic")
            },
        )

        self.assertEqual(result.incomplete, ("prolly::DiffIter",))

    def test_release_rejects_native_platform_exclusion(self) -> None:
        result = check_manifest(
            rust_items={"prolly::FileNodeStore::open"},
            manifest={
                "operations": [
                    {
                        "rust": "prolly::FileNodeStore::open",
                        "classification": "platform-excluded",
                        "status": "implemented",
                        "languages": {
                            "python": "python.FileNodeStore.open",
                            "node": "node.FileNodeStore.open",
                            "kotlin": "kotlin.FileNodeStore.open",
                            "java": "java.FileNodeStore.open",
                            "ruby": "ruby.FileNodeStore.open",
                            "swift": "swift.FileNodeStore.open",
                        },
                        "exclusions": {
                            "go": "not implemented",
                            "wasm": "browser runtimes have no filesystem",
                        },
                        "tests": ["binding.file-store"],
                        "docs": ["bindings/api/README.md#release-validation"],
                        "rationale": "Filesystem access is unavailable in browsers.",
                        "reviewed": True,
                    }
                ]
            },
            release=True,
        )

        self.assertEqual(result.incomplete, ("prolly::FileNodeStore::open",))

    def test_equivalence_catalog_requires_complete_language_patterns(self) -> None:
        equivalence = self.complete_equivalence("idiomatic")
        del equivalence["language_patterns"]["wasm"]

        self.assertEqual(
            validate_equivalence_catalog({"iterator-sequence": equivalence}),
            ("iterator-sequence",),
        )

    @staticmethod
    def complete_equivalence(classification: str) -> dict[str, object]:
        return {
            "classification": classification,
            "portable_semantics": "Ordered host iteration preserves Rust behavior.",
            "performance_contract": "Hot reads use bounded packed pages.",
            "language_patterns": {
                language: f"{language} sequence"
                for language in (
                    "python",
                    "go",
                    "node",
                    "kotlin",
                    "java",
                    "ruby",
                    "swift",
                    "wasm",
                )
            },
            "tests": ["binding.iteration"],
        }

    def test_audit_separates_release_evidence_from_unreviewed_item_kinds(self) -> None:
        items = {
            "prolly::VersionedMap::head": ApiItem(
                rust="prolly::VersionedMap::head",
                kind="function",
                owner="prolly::VersionedMap",
                member_kind="inherent-item",
            ),
            "prolly::VersionedMap::get": ApiItem(
                rust="prolly::VersionedMap::get",
                kind="function",
                owner="prolly::VersionedMap",
                member_kind="inherent-item",
            ),
            "prolly::VersionedValue::version": ApiItem(
                rust="prolly::VersionedValue::version",
                kind="struct_field",
                owner="prolly::VersionedValue",
                member_kind="field",
            ),
            "prolly::KeyCodec": ApiItem(
                rust="prolly::KeyCodec",
                kind="trait",
                owner=None,
                member_kind=None,
            ),
            "prolly::KeyCodec::Encoded": ApiItem(
                rust="prolly::KeyCodec::Encoded",
                kind="assoc_type",
                owner="prolly::KeyCodec",
                member_kind="trait-item",
            ),
        }
        languages = {
            language: f"{language}.VersionedMap.head"
            for language in (
                "python",
                "go",
                "node",
                "kotlin",
                "java",
                "ruby",
                "swift",
                "wasm",
            )
        }
        manifest = {
            "operations": [
                {
                    "rust": name,
                    "classification": "portable",
                    "status": "planned",
                    "family": "core",
                    "languages": {},
                    "exclusions": {},
                    "tests": [],
                }
                for name in items
            ]
        }
        manifest["operations"][0].update(
            status="implemented",
            languages=languages,
            tests=["binding.versioned.head"],
        )

        audit = build_classification_audit(items, manifest)

        self.assertEqual(
            audit["summary"],
            {
                "release_complete": 1,
                "reviewed_incomplete": 0,
                "unreviewed_runtime_candidate": 1,
                "unreviewed_data_model": 1,
                "unreviewed_rust_abstraction": 2,
            },
        )
        self.assertEqual(len(audit["rows"]), len(items))

    def test_inventory_requires_async_store_feature_provenance(self) -> None:
        result = check_manifest(
            rust_items=set(),
            manifest={
                "schema_version": 2,
                "rust_features": [],
                "operations": [],
            },
            release=False,
            required_rust_features=("async-store",),
        )

        self.assertEqual(result.incomplete, ("<manifest.rust_features>",))
        self.assertFalse(result.ok)

    def test_missing_rust_symbol_fails(self) -> None:
        result = check_manifest(
            rust_items={
                "prolly::VersionedMap",
                "prolly::VersionedMap::head",
            },
            manifest={
                "operations": [
                    {
                        "rust": "prolly::VersionedMap",
                        "classification": "portable",
                        "status": "planned",
                    }
                ]
            },
            release=False,
        )

        self.assertEqual(
            result,
            CheckResult(
                missing=("prolly::VersionedMap::head",),
                stale=(),
                incomplete=(),
            ),
        )
        self.assertFalse(result.ok)

    def test_stale_manifest_symbol_fails(self) -> None:
        result = check_manifest(
            rust_items={"prolly::VersionedMap"},
            manifest={
                "operations": [
                    {
                        "rust": "prolly::VersionedMap",
                        "classification": "portable",
                        "status": "planned",
                    },
                    {
                        "rust": "prolly::Removed",
                        "classification": "portable",
                        "status": "planned",
                    },
                ]
            },
            release=False,
        )

        self.assertEqual(result.stale, ("prolly::Removed",))
        self.assertFalse(result.ok)

    def test_inventory_mode_requires_a_valid_classification(self) -> None:
        result = check_manifest(
            rust_items={"prolly::VersionedMap"},
            manifest={
                "operations": [
                    {
                        "rust": "prolly::VersionedMap",
                        "classification": "unknown",
                        "status": "planned",
                    }
                ]
            },
            release=False,
        )

        self.assertEqual(result.incomplete, ("prolly::VersionedMap",))

    def test_release_requires_all_language_symbols_and_tests(self) -> None:
        result = check_manifest(
            rust_items={"prolly::VersionedMap::head"},
            manifest={
                "operations": [
                    {
                        "rust": "prolly::VersionedMap::head",
                        "classification": "portable",
                        "status": "planned",
                        "languages": {},
                        "tests": [],
                    }
                ]
            },
            release=True,
        )

        self.assertEqual(result.incomplete, ("prolly::VersionedMap::head",))
        self.assertFalse(result.ok)

    def test_release_accepts_complete_portable_mapping(self) -> None:
        languages = {
            language: f"{language}.VersionedMap.head"
            for language in (
                "python",
                "go",
                "node",
                "kotlin",
                "java",
                "ruby",
                "swift",
                "wasm",
            )
        }
        result = check_manifest(
            rust_items={"prolly::VersionedMap::head"},
            manifest={
                "operations": [
                    {
                        "rust": "prolly::VersionedMap::head",
                        "classification": "portable",
                        "status": "implemented",
                        "languages": languages,
                        "tests": ["binding.versioned.head"],
                    }
                ]
            },
            release=True,
        )

        self.assertTrue(result.ok)

    def test_release_requires_reason_and_test_for_platform_exclusion(self) -> None:
        result = check_manifest(
            rust_items={"prolly::FileNodeStore::open"},
            manifest={
                "operations": [
                    {
                        "rust": "prolly::FileNodeStore::open",
                        "classification": "platform-excluded",
                        "status": "implemented",
                        "languages": {
                            "python": "python.FileNodeStore.open",
                            "go": "go.OpenFileNodeStore",
                            "node": "node.FileNodeStore.open",
                            "kotlin": "kotlin.FileNodeStore.open",
                            "java": "java.FileNodeStore.open",
                            "ruby": "ruby.FileNodeStore.open",
                            "swift": "swift.FileNodeStore.open",
                        },
                        "exclusions": {"wasm": ""},
                        "tests": [],
                    }
                ]
            },
            release=True,
        )

        self.assertEqual(result.incomplete, ("prolly::FileNodeStore::open",))


class RustdocExtractionTests(unittest.TestCase):
    def test_async_feature_requires_async_public_api_sentinels(self) -> None:
        self.assertEqual(
            missing_feature_sentinels(
                {"prolly::AsyncProlly"},
                ("async-store",),
            ),
            ("prolly::AsyncVersionedMap",),
        )

    def test_extracts_public_root_item_and_associated_function(self) -> None:
        rustdoc = {
            "root": 1,
            "paths": {
                "2": {
                    "crate_id": 0,
                    "path": ["prolly", "VersionedMap"],
                    "kind": "struct",
                }
            },
            "index": {
                "1": {
                    "id": 1,
                    "crate_id": 0,
                    "name": "prolly",
                    "visibility": "public",
                    "inner": {
                        "module": {
                            "is_crate": True,
                            "items": [2],
                            "is_stripped": False,
                        }
                    },
                },
                "2": {
                    "id": 2,
                    "crate_id": 0,
                    "name": "VersionedMap",
                    "visibility": "public",
                    "inner": {
                        "struct": {
                            "kind": {
                                "plain": {
                                    "fields": [],
                                    "has_stripped_fields": False,
                                }
                            },
                            "generics": {"params": [], "where_predicates": []},
                            "impls": [3],
                        }
                    },
                },
                "3": {
                    "id": 3,
                    "crate_id": 0,
                    "name": None,
                    "visibility": "public",
                    "inner": {
                        "impl": {
                            "is_unsafe": False,
                            "generics": {"params": [], "where_predicates": []},
                            "provided_trait_methods": [],
                            "trait": None,
                            "for": {"resolved_path": {"name": "VersionedMap", "id": 2}},
                            "items": [4],
                            "is_negative": False,
                            "is_synthetic": False,
                            "blanket_impl": None,
                        }
                    },
                },
                "4": {
                    "id": 4,
                    "crate_id": 0,
                    "name": "head",
                    "visibility": "public",
                    "inner": {
                        "function": {
                            "sig": {
                                "inputs": [],
                                "output": None,
                                "is_c_variadic": False,
                            },
                            "generics": {"params": [], "where_predicates": []},
                            "header": {
                                "is_const": False,
                                "is_unsafe": False,
                                "is_async": False,
                                "abi": "Rust",
                            },
                            "has_body": True,
                        }
                    },
                },
            },
        }

        self.assertEqual(
            extract_public_api(rustdoc),
            {
                "prolly::VersionedMap",
                "prolly::VersionedMap::head",
            },
        )
        self.assertEqual(
            extract_public_api_items(rustdoc),
            {
                "prolly::VersionedMap": ApiItem(
                    rust="prolly::VersionedMap",
                    kind="struct",
                    owner=None,
                    member_kind=None,
                ),
                "prolly::VersionedMap::head": ApiItem(
                    rust="prolly::VersionedMap::head",
                    kind="function",
                    owner="prolly::VersionedMap",
                    member_kind="inherent-item",
                ),
            },
        )

    def test_extracts_field_variant_and_trait_item_metadata(self) -> None:
        rustdoc = {
            "root": 1,
            "index": {
                "1": {
                    "id": 1,
                    "crate_id": 0,
                    "name": "prolly",
                    "visibility": "public",
                    "inner": {"module": {"items": [2, 4, 6]}},
                },
                "2": {
                    "id": 2,
                    "crate_id": 0,
                    "name": "VersionedValue",
                    "visibility": "public",
                    "inner": {
                        "struct": {
                            "kind": {"plain": {"fields": [3]}},
                            "impls": [],
                        }
                    },
                },
                "3": {
                    "id": 3,
                    "crate_id": 0,
                    "name": "version",
                    "visibility": "public",
                    "inner": {"struct_field": {"primitive": "u64"}},
                },
                "4": {
                    "id": 4,
                    "crate_id": 0,
                    "name": "MergeChoice",
                    "visibility": "public",
                    "inner": {"enum": {"variants": [5], "impls": []}},
                },
                "5": {
                    "id": 5,
                    "crate_id": 0,
                    "name": "Left",
                    "visibility": "default",
                    "inner": {"variant": {"kind": "plain"}},
                },
                "6": {
                    "id": 6,
                    "crate_id": 0,
                    "name": "KeyCodec",
                    "visibility": "public",
                    "inner": {"trait": {"items": [7, 8]}},
                },
                "7": {
                    "id": 7,
                    "crate_id": 0,
                    "name": "Encoded",
                    "visibility": "default",
                    "inner": {"assoc_type": {"bounds": []}},
                },
                "8": {
                    "id": 8,
                    "crate_id": 0,
                    "name": "encode",
                    "visibility": "default",
                    "inner": {"function": {"sig": {}}},
                },
            },
        }

        items = extract_public_api_items(rustdoc)

        self.assertEqual(items["prolly::VersionedValue::version"].kind, "struct_field")
        self.assertEqual(items["prolly::VersionedValue::version"].member_kind, "field")
        self.assertEqual(items["prolly::MergeChoice::Left"].kind, "variant")
        self.assertEqual(items["prolly::MergeChoice::Left"].member_kind, "variant")
        self.assertEqual(items["prolly::KeyCodec::Encoded"].kind, "assoc_type")
        self.assertEqual(items["prolly::KeyCodec::Encoded"].member_kind, "trait-item")
        self.assertEqual(items["prolly::KeyCodec::encode"].kind, "function")
        self.assertEqual(items["prolly::KeyCodec::encode"].member_kind, "trait-item")

    def test_generation_is_idempotent_when_inventory_is_unchanged(self) -> None:
        previous = {
            "schema_version": 2,
            "generated_at": "2026-07-16T00:00:00+00:00",
            "rustdoc_format_version": 57,
            "rust_features": [],
            "languages": ["python"],
            "operations": [
                {
                    "rust": "prolly::VersionedMap",
                    "classification": "idiomatic",
                    "status": "planned",
                    "family": "versioned-map",
                    "languages": {},
                    "tests": [],
                }
            ],
        }

        generated = generate_manifest(
            {"prolly::VersionedMap"},
            previous,
            rustdoc_format_version=57,
        )

        self.assertEqual(
            generated["generated_at"],
            "2026-07-16T00:00:00+00:00",
        )
        self.assertEqual(
            generated["operations"][0]["classification"],
            "idiomatic",
        )


if __name__ == "__main__":
    unittest.main()
