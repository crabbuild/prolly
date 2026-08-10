#!/usr/bin/env bash
set -euo pipefail

rolling_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
fixture_manifest="$rolling_root/qualification/rolling-client/Cargo.toml"
cargo_toolchain="${PROLLY_S3_CARGO_TOOLCHAIN:-+1.94.1}"
default_build_root="${CARGO_TARGET_DIR:-${PROLLY_S3_CARGO_TARGET_DIR:-/Volumes/Workspace/prolly-build/versioned-s3}}/rolling-upgrade"
build_root="${PROLLY_S3_ROLLING_BUILD_ROOT:-$default_build_root}"
target_root="${PROLLY_S3_ROLLING_TARGET_ROOT:-$build_root}"
client_package="${PROLLY_S3_ROLLING_CLIENT_PACKAGE:-}"
core_package="${PROLLY_S3_ROLLING_CORE_PACKAGE:-}"

if [[ "${PROLLY_S3_RUSTFS:-}" != "1" ]]; then
  echo "set PROLLY_S3_RUSTFS=1 and the RustFS endpoint credentials" >&2
  exit 2
fi
if [[ ! -d "$build_root" ]]; then
  mkdir -p "$build_root"
fi
if [[ ! -d "$target_root" ]]; then
  mkdir -p "$target_root"
fi
if [[ -n "$client_package" || -n "$core_package" ]]; then
  if [[ ! -f "$client_package" || ! -f "$core_package" ]]; then
    echo "both PROLLY_S3_ROLLING_CLIENT_PACKAGE and PROLLY_S3_ROLLING_CORE_PACKAGE must name .crate archives" >&2
    exit 2
  fi
  package_source="$build_root/package-source"
  if [[ -e "$package_source" ]]; then
    echo "packaged rolling source already exists: $package_source" >&2
    exit 2
  fi
  mkdir -p "$package_source/client" "$package_source/core"
  tar -xzf "$client_package" -C "$package_source/client" --strip-components=1
  tar -xzf "$core_package" -C "$package_source/core" --strip-components=1
  package_config=(
    --config "patch.crates-io.prolly-s3-client.path='$package_source/client'"
    --config "patch.crates-io.prolly-s3-core.path='$package_source/core'"
  )
  source_mode="packaged"
else
  package_config=()
  source_mode="checkout"
fi

export PROLLY_S3_ROLLING_BUCKET="${PROLLY_S3_ROLLING_BUCKET:-prolly-versioned-s3-rolling}"
export PROLLY_S3_ROLLING_PREFIX="${PROLLY_S3_ROLLING_PREFIX:-integration/rolling/$(date +%s)-$$}"
export RUSTC_WRAPPER=""

new_target="$target_root/new"
legacy_target="$target_root/legacy"
CARGO_TARGET_DIR="$new_target" cargo "$cargo_toolchain" build \
  --manifest-path "$fixture_manifest" --locked "${package_config[@]}"
RUSTFLAGS="--cfg prolly_s3_legacy_v1_codec" \
  CARGO_TARGET_DIR="$legacy_target" cargo "$cargo_toolchain" build \
  --manifest-path "$fixture_manifest" --locked "${package_config[@]}"

new_client="$new_target/debug/prolly-s3-rolling-client"
legacy_client="$legacy_target/debug/prolly-s3-rolling-client"

"$new_client" init
"$legacy_client" legacy-write
"$new_client" new-write
"$legacy_client" verify
"$new_client" verify

for requirement in reader writer profile; do
  "$new_client" set-requirement "$requirement" 2
  before="$($new_client snapshot)"
  "$legacy_client" expect-incompatible
  "$new_client" expect-incompatible
  after="$($new_client snapshot)"
  if [[ "$after" != "$before" ]]; then
    echo "incompatible $requirement open changed physical repository versions" >&2
    exit 1
  fi
  "$new_client" set-requirement "$requirement" 1
  "$legacy_client" verify
  "$new_client" verify
done

final_snapshot="$($new_client snapshot)"
echo "RUSTFS_ROLLING_UPGRADE_COMPLETE source_mode=$source_mode bucket=$PROLLY_S3_ROLLING_BUCKET prefix=$PROLLY_S3_ROLLING_PREFIX requirements=reader,writer,profile final_snapshot=$final_snapshot"
