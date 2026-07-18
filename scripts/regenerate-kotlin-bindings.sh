#!/bin/sh
set -eu

SCRIPT_DIR=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd -P)
REPO_DIR=$(CDPATH= cd -- "$SCRIPT_DIR/.." && pwd -P)
TARGET_DIR=${CARGO_TARGET_DIR:-"$REPO_DIR/target"}
GENERATOR=${UNIFFI_BINDGEN:-uniffi-bindgen}
OUTPUT_FILE="$REPO_DIR/bindings/kotlin/src/main/kotlin/build/crab/prolly/generated/prolly.kt"
WORK_DIR=$(mktemp -d "${TMPDIR:-/tmp}/prolly-kotlin-bindings.XXXXXX")

cleanup() {
  rm -rf -- "$WORK_DIR"
}
trap cleanup EXIT HUP INT TERM

cargo build \
  --manifest-path "$REPO_DIR/bindings/uniffi/Cargo.toml" \
  --target-dir "$TARGET_DIR"

case "$(uname -s)" in
  Darwin) LIBRARY="$TARGET_DIR/debug/libprolly_bindings.dylib" ;;
  Linux) LIBRARY="$TARGET_DIR/debug/libprolly_bindings.so" ;;
  MINGW*|MSYS*|CYGWIN*) LIBRARY="$TARGET_DIR/debug/prolly_bindings.dll" ;;
  *)
    echo "unsupported platform for Kotlin binding generation: $(uname -s)" >&2
    exit 1
    ;;
esac

"$GENERATOR" generate "$LIBRARY" \
  --language kotlin \
  --out-dir "$WORK_DIR" \
  --config "$REPO_DIR/bindings/uniffi/uniffi.toml" \
  --no-format

GENERATED_FILE="$WORK_DIR/build/crab/prolly/prolly.kt"
test -f "$GENERATED_FILE"

assert_once() {
  PATTERN=$1
  COUNT=$(grep -F -c "$PATTERN" "$GENERATED_FILE" || true)
  if [ "$COUNT" -ne 1 ]; then
    echo "expected one generated occurrence of '$PATTERN', found $COUNT" >&2
    exit 1
  fi
}

assert_once "public interface AsyncProllyEngineInterface"
assert_once "open class AsyncProllyEngine:"
assert_once "public object FfiConverterTypeAsyncProllyEngine:"
assert_once "public interface AsyncProllyTransactionInterface"
assert_once "open class AsyncProllyTransaction:"
assert_once "public object FfiConverterTypeAsyncProllyTransaction:"
assert_once 'suspend fun `openRemoteProllyEngine`'

BEFORE_SYMBOLS=$(grep -o 'uniffi_prolly_bindings_[A-Za-z0-9_]*' "$GENERATED_FILE" | sort -u | shasum -a 256 | cut -d ' ' -f 1)

perl -0pi -e 's/AsyncProllyEngine/RemoteNativeProllyEngine/g; s/AsyncProllyTransaction/RemoteNativeProllyTransaction/g' "$GENERATED_FILE"
perl -0pi -e 's/[ \t]+(?=\r?$)//mg; s/\s+\z/\n/' "$GENERATED_FILE"

if grep -q 'AsyncProllyEngine\|AsyncProllyTransaction' "$GENERATED_FILE"; then
  echo "generated Kotlin still contains unrenamed async engine identifiers" >&2
  exit 1
fi

AFTER_SYMBOLS=$(grep -o 'uniffi_prolly_bindings_[A-Za-z0-9_]*' "$GENERATED_FILE" | sort -u | shasum -a 256 | cut -d ' ' -f 1)
if [ "$BEFORE_SYMBOLS" != "$AFTER_SYMBOLS" ]; then
  echo "Kotlin post-processing changed Rust FFI symbol names" >&2
  exit 1
fi

cp "$GENERATED_FILE" "$OUTPUT_FILE"
