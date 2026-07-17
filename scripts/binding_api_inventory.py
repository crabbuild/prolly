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
MANIFEST_SCHEMA_VERSION = 1


@dataclass(frozen=True)
class CheckResult:
    missing: tuple[str, ...]
    stale: tuple[str, ...]
    incomplete: tuple[str, ...]

    @property
    def ok(self) -> bool:
        return not (self.missing or self.stale or self.incomplete)


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


def extract_public_api(rustdoc: dict[str, Any]) -> set[str]:
    """Extract the crate-root public API and reachable associated items."""

    index = rustdoc["index"]
    root = _item(index, rustdoc["root"])
    if root is None:
        raise ValueError("rustdoc root item is missing")
    kind, module = _inner(root)
    if kind != "module":
        raise ValueError("rustdoc root item is not a module")

    crate_name = root.get("name") or "crate"
    public_api: set[str] = set()
    expanded_modules: set[tuple[str, str]] = set()

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

        public_api.add(public_path)

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
            if child is None or not _is_public(child):
                continue
            child_name = child.get("name")
            if child_name:
                public_api.add(f"{public_path}::{child_name}")

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
                    public_api.add(f"{public_path}::{associated_name}")

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


def _nonempty_strings(values: Any) -> bool:
    return (
        isinstance(values, dict)
        and all(isinstance(key, str) for key in values)
        and all(isinstance(value, str) and bool(value.strip()) for value in values.values())
    )


def _complete_release_entry(entry: dict[str, Any]) -> bool:
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
    if classification in {"portable", "idiomatic", "rust-language-only"}:
        return set(languages) == set(LANGUAGES) and not exclusions
    if classification == "platform-excluded":
        return bool(exclusions)
    return False


def check_manifest(
    rust_items: set[str],
    manifest: dict[str, Any],
    release: bool,
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
    for name in rust_items & operations.keys():
        entry = operations[name]
        if entry.get("classification") not in CLASSIFICATIONS:
            incomplete.add(name)
            continue
        if entry.get("status") not in STATUSES:
            incomplete.add(name)
            continue
        if release and not _complete_release_entry(entry):
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


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("command", choices=("generate", "check"))
    parser.add_argument("--release", action="store_true")
    parser.add_argument("--rustdoc", type=Path)
    parser.add_argument("--manifest", type=Path)
    return parser


def main(argv: list[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    root = _repo_root()
    manifest_path = args.manifest or root / "bindings" / "api" / "parity.json"
    rustdoc_path = args.rustdoc or _default_rustdoc_path(root)
    if not rustdoc_path.exists():
        print(
            "rustdoc JSON is missing; run "
            "cargo +nightly rustdoc --lib -- -Z unstable-options --output-format json",
            file=sys.stderr,
        )
        return 2

    rustdoc = _load_json(rustdoc_path)
    rust_items = extract_public_api(rustdoc)
    if args.command == "generate":
        previous = _load_json(manifest_path) if manifest_path.exists() else None
        manifest = generate_manifest(
            rust_items,
            previous,
            rustdoc.get("format_version"),
        )
        _write_json(manifest_path, manifest)
        print(f"wrote {len(rust_items)} operations to {manifest_path}")
        return 0

    if not manifest_path.exists():
        print(f"manifest is missing: {manifest_path}", file=sys.stderr)
        return 2
    result = check_manifest(rust_items, _load_json(manifest_path), args.release)
    if not result.ok:
        _print_result(result)
        return 1
    mode = "release" if args.release else "inventory"
    print(f"{mode} parity check passed for {len(rust_items)} operations")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
