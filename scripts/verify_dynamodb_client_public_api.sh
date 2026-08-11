#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
MANIFEST="$REPO_ROOT/extensions/dynamodb/client/Cargo.toml"
BASELINE="$REPO_ROOT/extensions/dynamodb/client/public-api.txt"
PUBLIC_API_VERSION="0.52.0"
RUSTDOC_TOOLCHAIN="nightly-2026-06-19"
MODE="check"

if [[ "${1:-}" == "--update" && "$#" == 1 ]]; then
  MODE="update"
elif [[ "$#" != 0 ]]; then
  echo "usage: $0 [--update]" >&2
  exit 64
fi

actual="$(mktemp "${TMPDIR:-/tmp}/prolly-dynamodb-public-api.XXXXXX")"
trap 'rm -f -- "$actual"' EXIT

installed_version="$(cargo public-api --version 2>/dev/null || true)"
if [[ "$installed_version" != "cargo-public-api $PUBLIC_API_VERSION" ]]; then
  echo "cargo-public-api $PUBLIC_API_VERSION is required; install it with:" >&2
  echo "  cargo install cargo-public-api --version $PUBLIC_API_VERSION --locked" >&2
  exit 69
fi
if ! rustc "+$RUSTDOC_TOOLCHAIN" --version >/dev/null 2>&1; then
  echo "Rust toolchain $RUSTDOC_TOOLCHAIN is required; install it with:" >&2
  echo "  rustup toolchain install $RUSTDOC_TOOLCHAIN --profile minimal" >&2
  exit 69
fi

cargo "+$RUSTDOC_TOOLCHAIN" public-api \
  --manifest-path "$MANIFEST" \
  --color never \
  --omit blanket-impls \
  > "$actual"

if [[ "$MODE" == "update" ]]; then
  install -m 0644 "$actual" "$BASELINE"
  printf 'updated_public_api baseline=%s lines=%s tool=%s rustdoc=%s\n' \
    "$BASELINE" "$(wc -l < "$BASELINE" | tr -d ' ')" \
    "$PUBLIC_API_VERSION" "$RUSTDOC_TOOLCHAIN"
  exit 0
fi

if [[ ! -f "$BASELINE" ]]; then
  echo "public API baseline is missing: $BASELINE" >&2
  echo "review the complete API, then run $0 --update" >&2
  exit 1
fi
if ! cmp -s "$BASELINE" "$actual"; then
  echo "Versioned DynamoDB client public API differs from the reviewed baseline:" >&2
  diff -u "$BASELINE" "$actual" >&2 || true
  echo "Review the complete diff and SemVer impact; update only with $0 --update" >&2
  exit 1
fi

if command -v sha256sum >/dev/null 2>&1; then
  digest="$(sha256sum "$BASELINE" | awk '{print $1}')"
else
  digest="$(shasum -a 256 "$BASELINE" | awk '{print $1}')"
fi
printf 'public_api_ok lines=%s sha256=%s tool=%s rustdoc=%s\n' \
  "$(wc -l < "$BASELINE" | tr -d ' ')" "$digest" \
  "$PUBLIC_API_VERSION" "$RUSTDOC_TOOLCHAIN"
