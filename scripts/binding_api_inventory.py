#!/usr/bin/env python3
"""Generate and verify the portable language-binding API contract."""

from __future__ import annotations

import argparse
from dataclasses import dataclass
from datetime import datetime, timezone
import json
from pathlib import Path
import subprocess
import sys
from typing import Any, Iterable


LANGUAGES = (
    "python",
    "go",
    "node",
    "kotlin",
    "java",
    "ruby",
    "swift",
    "wasm",
)
CLASSIFICATIONS = (
    "portable",
    "idiomatic",
    "platform-excluded",
    "rust-language-only",
)
STATUSES = ("planned", "implemented")
MANIFEST_SCHEMA_VERSION = 2
REQUIRED_RUST_FEATURES = ("async-store",)
FEATURE_SENTINELS = {
    "async-store": (
        "prolly::AsyncProlly",
        "prolly::AsyncVersionedMap",
    ),
}


@dataclass(frozen=True)
class CheckResult:
    missing: tuple[str, ...]
    stale: tuple[str, ...]
    incomplete: tuple[str, ...]

    @property
    def ok(self) -> bool:
        return not (self.missing or self.stale or self.incomplete)


@dataclass(frozen=True, order=True)
class ApiItem:
    """A stable public Rust path plus the rustdoc shape needed for review."""

    rust: str
    kind: str
    owner: str | None
    member_kind: str | None


def _inner(item: dict[str, Any]) -> tuple[str, dict[str, Any]]:
    values = item.get("inner", {})
    if len(values) != 1:
        return "", {}
    kind, value = next(iter(values.items()))
    return kind, value if isinstance(value, dict) else {}


def _is_public(item: dict[str, Any]) -> bool:
    visibility = item.get("visibility")
    return visibility == "public" or (
        isinstance(visibility, dict) and "restricted" not in visibility
    )


def _item(index: dict[str, Any], item_id: Any) -> dict[str, Any] | None:
    return index.get(str(item_id))


def extract_public_api_items(rustdoc: dict[str, Any]) -> dict[str, ApiItem]:
    """Extract public Rust paths with enough shape data for classification."""

    index = rustdoc["index"]
    root = _item(index, rustdoc["root"])
    if root is None:
        raise ValueError("rustdoc root item is missing")
    kind, module = _inner(root)
    if kind != "module":
        raise ValueError("rustdoc root item is not a module")

    crate_name = root.get("name") or "crate"
    public_api: dict[str, ApiItem] = {}
    expanded_modules: set[tuple[str, str]] = set()

    def record(
        rust: str,
        kind: str,
        owner: str | None = None,
        member_kind: str | None = None,
    ) -> None:
        public_api[rust] = ApiItem(rust, kind, owner, member_kind)

    def add_named_item(item_id: Any, public_path: str) -> None:
        item = _item(index, item_id)
        if item is None or item.get("crate_id") != 0 or not _is_public(item):
            return
        item_kind, body = _inner(item)

        if item_kind == "use":
            target_id = body.get("id")
            public_name = body.get("name")
            if target_id is not None and public_name:
                parent = public_path.rsplit("::", 1)[0]
                add_named_item(target_id, f"{parent}::{public_name}")
            return

        record(public_path, item_kind)

        if item_kind == "module":
            module_key = (str(item_id), public_path)
            if module_key in expanded_modules:
                return
            expanded_modules.add(module_key)
            for child_id in body.get("items", []):
                child = _item(index, child_id)
                if child is None or not _is_public(child):
                    continue
                child_kind, child_body = _inner(child)
                child_name = (
                    child_body.get("name")
                    if child_kind == "use"
                    else child.get("name")
                )
                if child_name:
                    add_named_item(child_id, f"{public_path}::{child_name}")
            return

        child_ids: list[Any] = []
        impl_ids: list[Any] = []
        if item_kind == "struct":
            struct_kind = body.get("kind", {})
            if "plain" in struct_kind:
                child_ids.extend(struct_kind["plain"].get("fields", []))
            elif "tuple" in struct_kind:
                child_ids.extend(
                    value for value in struct_kind["tuple"] if value is not None
                )
            impl_ids.extend(body.get("impls", []))
        elif item_kind == "enum":
            child_ids.extend(body.get("variants", []))
            impl_ids.extend(body.get("impls", []))
        elif item_kind in {"union", "type_alias", "primitive"}:
            impl_ids.extend(body.get("impls", []))
        elif item_kind == "trait":
            child_ids.extend(body.get("items", []))

        for child_id in child_ids:
            child = _item(index, child_id)
            if child is None or child.get("crate_id") != 0:
                continue
            if item_kind not in {"enum", "trait"} and not _is_public(child):
                continue
            child_kind, _ = _inner(child)
            child_name = child.get("name")
            if child_name:
                if item_kind == "struct":
                    member_kind = "field"
                elif item_kind == "enum":
                    member_kind = "variant"
                else:
                    member_kind = "trait-item"
                child_path = f"{public_path}::{child_name}"
                record(child_path, child_kind, public_path, member_kind)

        for impl_id in impl_ids:
            impl_item = _item(index, impl_id)
            if impl_item is None:
                continue
            impl_kind, impl_body = _inner(impl_item)
            if impl_kind != "impl" or impl_body.get("trait") is not None:
                continue
            for associated_id in impl_body.get("items", []):
                associated = _item(index, associated_id)
                if associated is None or not _is_public(associated):
                    continue
                associated_kind, _ = _inner(associated)
                associated_name = associated.get("name")
                if associated_name and associated_kind in {
                    "function",
                    "assoc_const",
                    "assoc_type",
                }:
                    associated_path = f"{public_path}::{associated_name}"
                    record(
                        associated_path,
                        associated_kind,
                        public_path,
                        "inherent-item",
                    )

    for root_item_id in module.get("items", []):
        root_item = _item(index, root_item_id)
        if root_item is None or not _is_public(root_item):
            continue
        root_kind, root_body = _inner(root_item)
        root_name = (
            root_body.get("name")
            if root_kind == "use"
            else root_item.get("name")
        )
        if root_name:
            add_named_item(root_item_id, f"{crate_name}::{root_name}")

    return public_api


def extract_public_api(rustdoc: dict[str, Any]) -> set[str]:
    """Extract the crate-root public API and reachable associated items."""

    return set(extract_public_api_items(rustdoc))


def _nonempty_strings(values: Any) -> bool:
    return (
        isinstance(values, dict)
        and all(isinstance(key, str) for key in values)
        and all(isinstance(value, str) and bool(value.strip()) for value in values.values())
    )


def _valid_review_metadata(entry: dict[str, Any]) -> bool:
    return (
        entry.get("reviewed") is True
        and isinstance(entry.get("rationale"), str)
        and bool(entry["rationale"].strip())
        and isinstance(entry.get("docs"), list)
        and bool(entry["docs"])
        and all(isinstance(doc, str) and doc.strip() for doc in entry["docs"])
    )


def _valid_equivalence_record(record: Any) -> bool:
    if not isinstance(record, dict):
        return False
    if record.get("classification") not in {"idiomatic", "rust-language-only"}:
        return False
    for field in ("portable_semantics", "performance_contract"):
        if not isinstance(record.get(field), str) or not record[field].strip():
            return False
    if not _nonempty_strings(record.get("language_patterns")):
        return False
    if set(record["language_patterns"]) != set(LANGUAGES):
        return False
    tests = record.get("tests")
    return (
        isinstance(tests, list)
        and bool(tests)
        and all(isinstance(test, str) and test.strip() for test in tests)
    )


def validate_equivalence_catalog(equivalences: Any) -> tuple[str, ...]:
    """Return malformed equivalence IDs in deterministic order."""

    if not isinstance(equivalences, dict):
        return ("<catalog>",)
    return tuple(
        sorted(
            equivalence_id
            for equivalence_id, record in equivalences.items()
            if not isinstance(equivalence_id, str)
            or not equivalence_id.strip()
            or not _valid_equivalence_record(record)
        )
    )


def _complete_release_entry(
    entry: dict[str, Any], equivalences: dict[str, Any] | None = None
) -> bool:
    if entry.get("status") != "implemented":
        return False
    if not isinstance(entry.get("tests"), list) or not entry["tests"]:
        return False
    if not all(isinstance(test, str) and test.strip() for test in entry["tests"]):
        return False

    classification = entry.get("classification")
    languages = entry.get("languages", {})
    exclusions = entry.get("exclusions", {})
    if not isinstance(languages, dict) or not isinstance(exclusions, dict):
        return False
    if set(languages) & set(exclusions):
        return False
    if not _nonempty_strings(languages) and languages:
        return False
    if not _nonempty_strings(exclusions) and exclusions:
        return False

    covered = set(languages) | set(exclusions)
    if covered != set(LANGUAGES):
        return False
    if classification == "portable":
        return set(languages) == set(LANGUAGES) and not exclusions
    if classification in {"idiomatic", "rust-language-only"}:
        equivalence_id = entry.get("equivalence")
        if not isinstance(equivalence_id, str) or not equivalence_id.strip():
            return False
        record = (equivalences or {}).get(equivalence_id)
        if not _valid_equivalence_record(record):
            return False
        if record["classification"] != classification:
            return False
        if not set(entry["tests"]) & set(record["tests"]):
            return False
        return (
            set(languages) == set(LANGUAGES)
            and not exclusions
            and _valid_review_metadata(entry)
        )
    if classification == "platform-excluded":
        return set(exclusions) == {"wasm"} and _valid_review_metadata(entry)
    return False


_DATA_MODEL_KINDS = {
    "struct",
    "enum",
    "union",
    "struct_field",
    "variant",
    "constant",
    "static",
}
_RUST_ABSTRACTION_KINDS = {
    "trait",
    "type_alias",
    "primitive",
    "assoc_const",
    "assoc_type",
}

_STORE_TRAIT_OWNERS = {
    "prolly::AsyncBlobStore",
    "prolly::AsyncManifestStore",
    "prolly::AsyncManifestStoreScan",
    "prolly::AsyncStore",
    "prolly::AsyncTransactionalStore",
    "prolly::BlobStore",
    "prolly::BlobStoreScan",
    "prolly::ManifestStore",
    "prolly::ManifestStoreScan",
    "prolly::NodeStoreScan",
    "prolly::RemoteStoreBackend",
    "prolly::Store",
    "prolly::TransactionalStore",
}
_CODEC_TRAIT_OWNERS = {"prolly::KeyCodec", "prolly::ValueCodec"}
_CALLBACK_TRAIT_OWNERS = {
    "prolly::BorrowedMergeResolver",
    "prolly::ConflictFreeMerger",
    "prolly::SecondaryIndexExtractor",
    "prolly::StreamingSecondaryIndexExtractor",
}
_CALLBACK_TYPE_ALIASES = {
    "prolly::CustomMergeFn",
    "prolly::MergePolicyFn",
    "prolly::Resolver",
    "prolly::TimestampExtractor",
}


def _abstraction_equivalence(item: ApiItem) -> str | None:
    if item.kind == "module":
        return "namespace-module"
    if item.kind in {"assoc_type", "assoc_const"}:
        return "marker-and-associated-type"

    owner = item.owner or item.rust
    if owner in _STORE_TRAIT_OWNERS:
        return "store-trait"
    if owner in _CODEC_TRAIT_OWNERS:
        return "generic-codec"
    if owner in _CALLBACK_TRAIT_OWNERS:
        return "callback-protocol"
    if owner == "prolly::StreamingDiffer":
        return "iterator-sequence"
    if owner == "prolly::ParallelRebalancer":
        return "builder-typestate"
    if item.kind == "type_alias":
        if item.rust in _CALLBACK_TYPE_ALIASES:
            return "callback-protocol"
        return "record-alias"
    return None


def review_abstraction_entries(
    items: dict[str, ApiItem], manifest: dict[str, Any]
) -> dict[str, Any]:
    """Apply explicit abstraction classifications without claiming coverage."""

    reviewed = dict(manifest)
    operations: list[Any] = []
    for original in manifest.get("operations", []):
        if not isinstance(original, dict):
            operations.append(original)
            continue
        entry = dict(original)
        item = items.get(entry.get("rust"))
        if item is None:
            operations.append(entry)
            continue
        is_abstraction = (
            item.kind in _RUST_ABSTRACTION_KINDS
            or item.member_kind == "trait-item"
            or (item.kind not in _DATA_MODEL_KINDS and item.kind != "function")
        )
        if not is_abstraction:
            operations.append(entry)
            continue
        equivalence = _abstraction_equivalence(item)
        if equivalence is None:
            raise ValueError(f"no abstraction review rule for {item.rust}")
        classification = (
            "rust-language-only"
            if equivalence in {"marker-and-associated-type", "namespace-module"}
            else "idiomatic"
        )
        entry.update(
            classification=classification,
            equivalence=equivalence,
            rationale=(
                f"{item.rust} is represented by the reviewed "
                f"{equivalence} host-language contract instead of a literal "
                "Rust declaration."
            ),
            docs=[f"bindings/api/idiomatic-equivalents.json#{equivalence}"],
            reviewed=True,
        )
        operations.append(entry)
    reviewed["operations"] = operations
    return reviewed


def _audit_bucket(
    item: ApiItem,
    entry: dict[str, Any],
    equivalences: dict[str, Any] | None = None,
) -> str:
    if _complete_release_entry(entry, equivalences):
        return "release_complete"
    if entry.get("reviewed") is True:
        return "reviewed_incomplete"
    if item.kind in _DATA_MODEL_KINDS:
        return "unreviewed_data_model"
    if (
        item.kind in _RUST_ABSTRACTION_KINDS
        or item.member_kind == "trait-item"
        or item.kind != "function"
    ):
        return "unreviewed_rust_abstraction"
    return "unreviewed_runtime_candidate"


def build_classification_audit(
    items: dict[str, ApiItem],
    manifest: dict[str, Any],
    equivalences: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Build deterministic triage data without inferring parity completion."""

    entries = {
        entry["rust"]: entry
        for entry in manifest.get("operations", [])
        if isinstance(entry, dict) and isinstance(entry.get("rust"), str)
    }
    summary = {
        "release_complete": 0,
        "reviewed_incomplete": 0,
        "unreviewed_runtime_candidate": 0,
        "unreviewed_data_model": 0,
        "unreviewed_rust_abstraction": 0,
    }
    family_counts: dict[str, int] = {}
    owner_counts: dict[str, int] = {}
    rows: list[dict[str, Any]] = []
    for rust in sorted(items):
        item = items[rust]
        entry = entries.get(rust, {})
        bucket = _audit_bucket(item, entry, equivalences)
        summary[bucket] += 1
        family = entry.get("family") or _family(rust)
        family_counts[family] = family_counts.get(family, 0) + 1
        owner = item.owner or rust
        owner_counts[owner] = owner_counts.get(owner, 0) + 1
        rows.append(
            {
                "rust": rust,
                "kind": item.kind,
                "owner": item.owner,
                "member_kind": item.member_kind,
                "family": family,
                "classification": entry.get("classification"),
                "status": entry.get("status"),
                "bucket": bucket,
            }
        )
    return {
        "summary": summary,
        "family_counts": dict(sorted(family_counts.items())),
        "owner_counts": dict(
            sorted(owner_counts.items(), key=lambda pair: (-pair[1], pair[0]))
        ),
        "rows": rows,
    }


def check_manifest(
    rust_items: set[str],
    manifest: dict[str, Any],
    release: bool,
    required_rust_features: tuple[str, ...] = (),
    equivalences: dict[str, Any] | None = None,
) -> CheckResult:
    entries = manifest.get("operations")
    if not isinstance(entries, list):
        return CheckResult(
            missing=tuple(sorted(rust_items)),
            stale=(),
            incomplete=("<manifest.operations>",),
        )

    operations: dict[str, dict[str, Any]] = {}
    duplicates: set[str] = set()
    malformed: set[str] = set()
    for position, entry in enumerate(entries):
        if not isinstance(entry, dict) or not isinstance(entry.get("rust"), str):
            malformed.add(f"<operation:{position}>")
            continue
        name = entry["rust"]
        if name in operations:
            duplicates.add(name)
        operations[name] = entry

    missing = tuple(sorted(rust_items - operations.keys()))
    stale = tuple(sorted(operations.keys() - rust_items))
    incomplete = set(malformed) | duplicates
    if release and equivalences is not None:
        incomplete.update(
            f"<equivalence:{equivalence_id}>"
            for equivalence_id in validate_equivalence_catalog(equivalences)
        )
    if required_rust_features:
        if manifest.get("schema_version") != MANIFEST_SCHEMA_VERSION:
            incomplete.add("<manifest.schema_version>")
        features = manifest.get("rust_features")
        if not isinstance(features, list) or set(features) != set(
            required_rust_features
        ):
            incomplete.add("<manifest.rust_features>")
    for name in rust_items & operations.keys():
        entry = operations[name]
        if entry.get("classification") not in CLASSIFICATIONS:
            incomplete.add(name)
            continue
        if entry.get("status") not in STATUSES:
            incomplete.add(name)
            continue
        if release and not _complete_release_entry(entry, equivalences):
            incomplete.add(name)

    return CheckResult(missing, stale, tuple(sorted(incomplete)))


def _default_entry(rust_name: str) -> dict[str, Any]:
    return {
        "rust": rust_name,
        "classification": "portable",
        "status": "planned",
        "family": _family(rust_name),
        "performance": "owned",
        "languages": {},
        "exclusions": {},
        "tests": [],
        "docs": [],
    }


def _family(rust_name: str) -> str:
    lowered = rust_name.lower()
    if "secondaryindex" in lowered or "indexed" in lowered:
        return "indexed-map"
    if any(
        token in lowered
        for token in (
            "proximity",
            "hnsw",
            "quantiz",
            "accelerator",
            "neighbor",
            "searchbudget",
            "searchplan",
        )
    ):
        return "proximity-map"
    if "versionedmap" in lowered or "mapversion" in lowered:
        return "versioned-map"
    if "proof" in lowered:
        return "proof"
    if any(token in lowered for token in ("blob", "gc", "snapshot", "missingnode")):
        return "maintenance"
    if any(token in lowered for token in ("readsession", "writesession", "cursor")):
        return "session"
    return "core"


def generate_manifest(
    rust_items: Iterable[str],
    previous: dict[str, Any] | None,
    rustdoc_format_version: int | None,
    rust_features: tuple[str, ...] = (),
) -> dict[str, Any]:
    rust_names = sorted(set(rust_items))
    prior_entries = {
        entry["rust"]: entry
        for entry in (previous or {}).get("operations", [])
        if isinstance(entry, dict) and isinstance(entry.get("rust"), str)
    }
    operations = [
        prior_entries.get(name, _default_entry(name))
        for name in rust_names
    ]
    inventory_unchanged = (
        previous is not None
        and set(prior_entries) == set(rust_names)
        and previous.get("rustdoc_format_version") == rustdoc_format_version
        and previous.get("rust_features") == list(rust_features)
    )
    generated_at = (
        previous.get("generated_at")
        if inventory_unchanged
        else datetime.now(timezone.utc).replace(microsecond=0).isoformat()
    )
    return {
        "schema_version": MANIFEST_SCHEMA_VERSION,
        "generated_at": generated_at,
        "rustdoc_format_version": rustdoc_format_version,
        "rust_features": list(rust_features),
        "languages": list(LANGUAGES),
        "operations": operations,
    }


def _repo_root() -> Path:
    return Path(__file__).resolve().parents[1]


def _default_rustdoc_path(root: Path) -> Path:
    metadata = subprocess.run(
        ["cargo", "metadata", "--format-version", "1", "--no-deps"],
        cwd=root,
        check=True,
        capture_output=True,
        text=True,
    )
    target_directory = Path(json.loads(metadata.stdout)["target_directory"])
    return target_directory / "doc" / "prolly.json"


def _load_json(path: Path) -> dict[str, Any]:
    with path.open("r", encoding="utf-8") as stream:
        value = json.load(stream)
    if not isinstance(value, dict):
        raise ValueError(f"{path} must contain a JSON object")
    return value


def _write_json(path: Path, value: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    rendered = json.dumps(value, indent=2, sort_keys=False, ensure_ascii=False)
    path.write_text(rendered + "\n", encoding="utf-8")


def _print_result(result: CheckResult) -> None:
    for label, values in (
        ("missing", result.missing),
        ("stale", result.stale),
        ("incomplete", result.incomplete),
    ):
        for value in values:
            print(f"{label}: {value}", file=sys.stderr)


def missing_feature_sentinels(
    rust_items: set[str],
    rust_features: tuple[str, ...],
) -> tuple[str, ...]:
    """Return feature-gated symbols absent from the supplied rustdoc inventory."""

    return tuple(
        sentinel
        for feature in rust_features
        for sentinel in FEATURE_SENTINELS.get(feature, ())
        if sentinel not in rust_items
    )


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "command", choices=("generate", "check", "audit", "review-abstractions")
    )
    parser.add_argument("--release", action="store_true")
    parser.add_argument("--rustdoc", type=Path)
    parser.add_argument("--manifest", type=Path)
    parser.add_argument("--equivalences", type=Path)
    parser.add_argument("--output", type=Path)
    return parser


def main(argv: list[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    root = _repo_root()
    manifest_path = args.manifest or root / "bindings" / "api" / "parity.json"
    equivalences_path = (
        args.equivalences
        or root / "bindings" / "api" / "idiomatic-equivalents.json"
    )
    rustdoc_path = args.rustdoc or _default_rustdoc_path(root)
    if not rustdoc_path.exists():
        print(
            "rustdoc JSON is missing; run "
            "cargo +nightly rustdoc --lib --features async-store -- "
            "-Z unstable-options --output-format json",
            file=sys.stderr,
        )
        return 2

    rustdoc = _load_json(rustdoc_path)
    api_items = extract_public_api_items(rustdoc)
    rust_items = set(api_items)
    missing_sentinels = missing_feature_sentinels(rust_items, REQUIRED_RUST_FEATURES)
    if missing_sentinels:
        for sentinel in missing_sentinels:
            print(f"missing feature-gated symbol: {sentinel}", file=sys.stderr)
        print(
            "regenerate rustdoc JSON with: cargo +nightly rustdoc --lib "
            "--features async-store -- -Z unstable-options --output-format json",
            file=sys.stderr,
        )
        return 2
    if args.command == "generate":
        previous = _load_json(manifest_path) if manifest_path.exists() else None
        manifest = generate_manifest(
            rust_items,
            previous,
            rustdoc.get("format_version"),
            REQUIRED_RUST_FEATURES,
        )
        _write_json(manifest_path, manifest)
        print(f"wrote {len(rust_items)} operations to {manifest_path}")
        return 0

    if not manifest_path.exists():
        print(f"manifest is missing: {manifest_path}", file=sys.stderr)
        return 2
    manifest = _load_json(manifest_path)
    equivalence_document = (
        _load_json(equivalences_path) if equivalences_path.exists() else {}
    )
    equivalences = equivalence_document.get("equivalences", {})
    if args.command == "review-abstractions":
        reviewed = review_abstraction_entries(api_items, manifest)
        _write_json(manifest_path, reviewed)
        reviewed_count = sum(
            1
            for entry in reviewed["operations"]
            if isinstance(entry, dict) and entry.get("reviewed") is True
        )
        print(
            f"wrote {reviewed_count} reviewed abstraction classifications "
            f"to {manifest_path}"
        )
        return 0
    if args.command == "audit":
        audit = build_classification_audit(api_items, manifest, equivalences)
        audit = {
            "schema_version": 1,
            "rustdoc_format_version": rustdoc.get("format_version"),
            "rust_features": list(REQUIRED_RUST_FEATURES),
            **audit,
        }
        output_path = (
            args.output
            or root / "bindings" / "api" / "classification-audit.json"
        )
        _write_json(output_path, audit)
        print(
            f"wrote classification audit for {len(api_items)} operations "
            f"to {output_path}"
        )
        return 0

    result = check_manifest(
        rust_items,
        manifest,
        args.release,
        REQUIRED_RUST_FEATURES,
        equivalences,
    )
    if not result.ok:
        _print_result(result)
        return 1
    mode = "release" if args.release else "inventory"
    print(f"{mode} parity check passed for {len(rust_items)} operations")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
