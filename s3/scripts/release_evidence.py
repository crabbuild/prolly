#!/usr/bin/env python3
"""Create and verify a body-independent signed release evidence manifest."""

from __future__ import annotations

import argparse
import hashlib
import json
import pathlib
from typing import Any


SCHEMA = "prolly-s3-release-evidence/v1"
REQUIRED_METADATA = {
    "cargo_version",
    "created_at",
    "rustc_version",
    "rustfs_container",
    "rustfs_health",
    "rustfs_image",
    "rustfs_mount",
    "signer_fingerprint_sha256",
    "signer_mode",
    "source_revision",
    "source_state",
}


def sha256(path: pathlib.Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as source:
        for chunk in iter(lambda: source.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def safe_artifact(root: pathlib.Path, name: str) -> pathlib.Path:
    relative = pathlib.PurePosixPath(name)
    if relative.is_absolute() or not relative.parts or ".." in relative.parts:
        raise SystemExit(f"unsafe evidence artifact path: {name!r}")
    candidate = (root / pathlib.Path(*relative.parts)).resolve()
    try:
        candidate.relative_to(root)
    except ValueError as error:
        raise SystemExit(f"evidence artifact escapes root: {name!r}") from error
    if not candidate.is_file() or candidate.is_symlink():
        raise SystemExit(f"evidence artifact is not a regular file: {name!r}")
    return candidate


def parse_metadata(values: list[str]) -> dict[str, str]:
    metadata: dict[str, str] = {}
    for value in values:
        key, separator, item = value.partition("=")
        if not separator or not key or not item:
            raise SystemExit(f"metadata must be a nonempty key=value pair: {value!r}")
        if key in metadata:
            raise SystemExit(f"duplicate metadata key: {key}")
        metadata[key] = item
    missing = sorted(REQUIRED_METADATA - metadata.keys())
    if missing:
        raise SystemExit(f"missing required metadata: {', '.join(missing)}")
    return metadata


def create(args: argparse.Namespace) -> None:
    root = pathlib.Path(args.root).resolve()
    output = pathlib.Path(args.output)
    if output.is_absolute() or ".." in output.parts:
        raise SystemExit("manifest output must remain beneath the evidence root")
    manifest_path = root / output
    if manifest_path.exists():
        raise SystemExit(f"refusing to replace existing manifest: {manifest_path}")
    names = sorted(set(args.artifact))
    if len(names) != len(args.artifact):
        raise SystemExit("duplicate evidence artifact path")
    reserved = {str(output), "release-evidence.sig"}
    if reserved.intersection(names):
        raise SystemExit("manifest and signature cannot be self-listed artifacts")
    artifacts = []
    for name in names:
        path = safe_artifact(root, name)
        artifacts.append(
            {"path": name, "sha256": sha256(path), "size_bytes": path.stat().st_size}
        )
    manifest: dict[str, Any] = {
        "artifacts": artifacts,
        "metadata": parse_metadata(args.metadata),
        "schema": SCHEMA,
    }
    manifest_path.write_text(
        json.dumps(manifest, indent=2, sort_keys=True, ensure_ascii=True) + "\n",
        encoding="utf-8",
    )
    print(
        "RELEASE_EVIDENCE_MANIFEST_CREATED "
        f"artifacts={len(artifacts)} manifest={manifest_path}"
    )


def verify(args: argparse.Namespace) -> None:
    root = pathlib.Path(args.root).resolve()
    manifest_path = safe_artifact(root, args.manifest)
    try:
        manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    except (json.JSONDecodeError, UnicodeDecodeError) as error:
        raise SystemExit(f"invalid release evidence JSON: {error}") from error
    if manifest.get("schema") != SCHEMA:
        raise SystemExit("unsupported release evidence schema")
    metadata = manifest.get("metadata")
    if not isinstance(metadata, dict):
        raise SystemExit("release evidence metadata must be an object")
    missing = sorted(REQUIRED_METADATA - metadata.keys())
    if missing:
        raise SystemExit(f"manifest metadata is incomplete: {', '.join(missing)}")
    entries = manifest.get("artifacts")
    if not isinstance(entries, list) or not entries:
        raise SystemExit("release evidence must contain artifacts")
    names: list[str] = []
    for entry in entries:
        if not isinstance(entry, dict):
            raise SystemExit("release evidence artifact entry must be an object")
        if set(entry) != {"path", "sha256", "size_bytes"}:
            raise SystemExit("release evidence artifact entry has unknown fields")
        name = entry["path"]
        if not isinstance(name, str):
            raise SystemExit("release evidence artifact path must be a string")
        path = safe_artifact(root, name)
        if path.stat().st_size != entry["size_bytes"]:
            raise SystemExit(f"release evidence size mismatch: {name}")
        if sha256(path) != entry["sha256"]:
            raise SystemExit(f"release evidence digest mismatch: {name}")
        names.append(name)
    if names != sorted(set(names)):
        raise SystemExit("release evidence artifacts must be unique and sorted")
    actual: set[str] = set()
    for path in root.rglob("*"):
        if path.is_symlink():
            raise SystemExit(f"release evidence contains a symlink: {path.relative_to(root)}")
        if path.is_file():
            actual.add(path.relative_to(root).as_posix())
    allowed_unsigned = {args.manifest, "release-evidence.sig"}
    unexpected = sorted(actual - set(names) - allowed_unsigned)
    if unexpected:
        raise SystemExit(
            f"release evidence contains unsigned files: {', '.join(unexpected)}"
        )
    absent = sorted(set(names) - actual)
    if absent:
        raise SystemExit(f"release evidence is missing files: {', '.join(absent)}")
    print(
        "RELEASE_EVIDENCE_MANIFEST_VERIFIED "
        f"artifacts={len(entries)} manifest_sha256={sha256(manifest_path)}"
    )


def main() -> None:
    parser = argparse.ArgumentParser()
    commands = parser.add_subparsers(dest="command", required=True)
    create_parser = commands.add_parser("create")
    create_parser.add_argument("--root", required=True)
    create_parser.add_argument("--output", default="release-evidence.json")
    create_parser.add_argument("--artifact", action="append", required=True)
    create_parser.add_argument("--metadata", action="append", default=[])
    create_parser.set_defaults(handler=create)
    verify_parser = commands.add_parser("verify")
    verify_parser.add_argument("--root", required=True)
    verify_parser.add_argument("--manifest", default="release-evidence.json")
    verify_parser.set_defaults(handler=verify)
    args = parser.parse_args()
    args.handler(args)


if __name__ == "__main__":
    main()
