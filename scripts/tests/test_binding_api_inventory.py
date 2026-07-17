from __future__ import annotations

import unittest

from scripts.binding_api_inventory import (
    CheckResult,
    check_manifest,
    extract_public_api,
    generate_manifest,
    missing_feature_sentinels,
)


class ManifestCheckTests(unittest.TestCase):
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
