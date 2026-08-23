#!/usr/bin/env python3
"""Validate synchronized binding package metadata before packing or publishing."""

from __future__ import annotations

import argparse
import json
import re
import sys
import tomllib
import xml.etree.ElementTree as ET
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]


def load_json(path: str) -> dict:
    return json.loads((ROOT / path).read_text())


def load_toml(path: str) -> dict:
    return tomllib.loads((ROOT / path).read_text())


def fail(message: str) -> None:
    raise ValueError(message)


def require_equal(label: str, actual: str, expected: str) -> None:
    if actual != expected:
        fail(f"{label}: expected {expected!r}, found {actual!r}")


def gemspec_value(path: str, field: str) -> str:
    text = (ROOT / path).read_text()
    match = re.search(rf"spec\.{field}\s*=\s*['\"]([^'\"]+)['\"]", text)
    if not match:
        fail(f"{path}: missing spec.{field}")
    return match.group(1)


def maven_version(path: str) -> str:
    root = ET.parse(ROOT / path).getroot()
    namespace = {"m": "http://maven.apache.org/POM/4.0.0"}
    value = root.findtext("m:version", namespaces=namespace)
    if not value:
        fail(f"{path}: missing project version")
    return value.removesuffix("-SNAPSHOT")


def go_module(path: Path) -> str:
    first = path.read_text().splitlines()[0]
    if not first.startswith("module "):
        fail(f"{path.relative_to(ROOT)}: missing module directive")
    return first.removeprefix("module ")


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--tag", help="release tag to compare with bindings-v<version>")
    args = parser.parse_args()

    manifest = load_json("bindings/release-manifest.json")
    version = manifest["version"]
    require_equal("release manifest tag", manifest["tag"], f"bindings-v{version}")
    if args.tag:
        require_equal("release tag", args.tag, manifest["tag"])

    node = load_json("bindings/node/package.json")
    wasm = load_json("bindings/wasm/package.json")
    python = load_toml("bindings/python/pyproject.toml")
    uniffi = load_toml("bindings/uniffi/Cargo.toml")
    node_native = load_toml("bindings/node/native/Cargo.toml")
    wasm_cargo = load_toml("bindings/wasm/Cargo.toml")

    expected_npm = manifest["packages"]["npm"]
    require_equal("Node npm name", node["name"], expected_npm[0])
    require_equal("WASM npm name", wasm["name"], expected_npm[1])
    require_equal("PyPI name", python["project"]["name"], manifest["packages"]["pypi"][0])

    versions = {
        "Node npm": node["version"],
        "WASM npm": wasm["version"],
        "Python": python["project"]["version"],
        "UniFFI": uniffi["package"]["version"],
        "Node native": node_native["package"]["version"],
        "WASM Rust": wasm_cargo["package"]["version"],
        "Ruby": gemspec_value("bindings/ruby/prolly.gemspec", "version"),
        "JVM": maven_version("bindings/pom.xml"),
    }
    for label, actual in versions.items():
        require_equal(f"{label} version", actual, version)

    for label, package in (("Node", node), ("WASM", wasm)):
        if package.get("private") is True:
            fail(f"{label} npm package is private")
        require_equal(f"{label} npm publish access", package["publishConfig"]["access"], "public")

    go_files = [ROOT / "bindings/go/go.mod", *sorted((ROOT / "bindings/go/stores").glob("*/go.mod"))]
    go_modules = [go_module(path) for path in go_files]
    if go_modules != manifest["packages"]["go"]:
        fail(f"Go module list differs from release manifest: {go_modules!r}")
    for path in go_files[1:]:
        text = path.read_text()
        if "replace github.com/crabbuild/prolly/bindings/go" in text:
            fail(f"{path.relative_to(ROOT)} contains a local replace directive")
        if f"github.com/crabbuild/prolly/bindings/go v{version}" not in text:
            fail(f"{path.relative_to(ROOT)} does not require core v{version}")

    targets = node["napi"]["triples"]["additional"]
    if targets != manifest["native_targets"]:
        fail("Node native targets differ from the release manifest")

    print(f"binding release metadata is synchronized at {version}")
    print(f"npm: {', '.join(expected_npm)}")
    print(f"PyPI: {manifest['packages']['pypi'][0]}")
    print(f"Go modules: {len(go_modules)}")
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except (KeyError, TypeError, ValueError) as error:
        print(f"binding release metadata error: {error}", file=sys.stderr)
        raise SystemExit(1)
