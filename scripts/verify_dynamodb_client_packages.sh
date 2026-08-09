#!/usr/bin/env bash
set -euo pipefail

# Fresh extracted-archive targets intentionally do not reuse workspace build
# state. Disable incremental objects and debug symbols so this clean-room
# verification remains practical on bounded CI volumes without changing code
# generation or test semantics.
export CARGO_INCREMENTAL=0
export CARGO_PROFILE_DEV_DEBUG=0
export CARGO_PROFILE_TEST_DEBUG=0

allow_dirty="${PROLLY_PACKAGE_ALLOW_DIRTY:-0}"
if [[ "${1:-}" == "--allow-dirty" ]]; then
  allow_dirty=1
  shift
fi
if [[ "$#" -ne 0 ]]; then
  echo "usage: $0 [--allow-dirty]" >&2
  exit 64
fi

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
work_dir="$(mktemp -d "${TMPDIR:-/tmp}/prolly-dynamodb-packages.XXXXXX")"
trap 'rm -rf -- "${work_dir}"' EXIT

package_target="${work_dir}/target"
mkdir -p "${package_target}"

cargo_package() {
  if [[ "${allow_dirty}" == "1" ]]; then
    cargo package "$@" --locked --allow-dirty
  else
    cargo package "$@" --locked
  fi
}

manifests=(
  "${repo_root}/Cargo.toml"
  "${repo_root}/stores/prolly-store-dynamodb/Cargo.toml"
  "${repo_root}/dynamodb/core/Cargo.toml"
  "${repo_root}/dynamodb/client/Cargo.toml"
)

for manifest in "${manifests[@]}"; do
  if [[ "${manifest}" == "${repo_root}/dynamodb/client/Cargo.toml" ]]; then
    # Cargo requires registry visibility of normal dependencies before it will
    # create an archive. During dependency-order release verification the core
    # archive exists locally but is intentionally not published yet.
    CARGO_TARGET_DIR="${package_target}" cargo_package \
      --manifest-path "${manifest}" \
      --no-verify \
      --config "patch.crates-io.prolly-dynamodb-core.path='${repo_root}/dynamodb/core'"
    continue
  fi
  CARGO_TARGET_DIR="${package_target}" cargo_package \
    --manifest-path "${manifest}" \
    --no-verify
done

archives=(
  "${package_target}/package/prolly-map-0.7.0.crate"
  "${package_target}/package/prolly-store-dynamodb-0.6.0.crate"
  "${package_target}/package/prolly-dynamodb-core-0.1.0.crate"
  "${package_target}/package/prolly-dynamodb-client-0.1.0.crate"
)
for archive in "${archives[@]}"; do
  test -s "${archive}"
  tar -xzf "${archive}" -C "${work_dir}"
done

# The durable-format oracle must travel with the semantic core. A repository
# test alone cannot protect downstream releases if cargo packaging omits it.
test -s "${work_dir}/prolly-dynamodb-core-0.1.0/tests/fixtures/database-format-10.json"
test -s "${work_dir}/prolly-dynamodb-core-0.1.0/tests/fixtures/database-format-11.json"
test -s "${work_dir}/prolly-dynamodb-core-0.1.0/tests/fixtures/database-format-12.json"
for fixture in canonical-v1.json validation-v1.json; do
  core_fixture="${work_dir}/prolly-dynamodb-core-0.1.0/tests/fixtures/${fixture}"
  client_fixture="${work_dir}/prolly-dynamodb-client-0.1.0/src/fixtures/${fixture}"
  test -s "${core_fixture}"
  test -s "${client_fixture}"
  cmp --silent "${core_fixture}" "${client_fixture}"
done

# The reviewed client API baseline is a release artifact. Packaging must not
# silently omit or substitute it.
packaged_public_api="${work_dir}/prolly-dynamodb-client-0.1.0/public-api.txt"
test -s "${packaged_public_api}"
cmp --silent "${repo_root}/dynamodb/client/public-api.txt" "${packaged_public_api}"

# Compile the test/example targets from the archives themselves. This catches
# package-local include paths and dev-only source omissions that a downstream
# library consumer cannot observe.
package_test_target="${work_dir}/package-test-target"
CARGO_NET_OFFLINE=true cargo update \
  --manifest-path "${work_dir}/prolly-dynamodb-core-0.1.0/Cargo.toml" \
  --offline \
  --package 'registry+https://github.com/rust-lang/crates.io-index#prolly-map@0.7.0' \
  --config "patch.crates-io.prolly-map.path='${work_dir}/prolly-map-0.7.0'"
CARGO_TARGET_DIR="${package_test_target}" cargo test \
  --manifest-path "${work_dir}/prolly-dynamodb-core-0.1.0/Cargo.toml" \
  --locked \
  --all-targets \
  --no-run \
  --config "patch.crates-io.prolly-map.path='${work_dir}/prolly-map-0.7.0'"
CARGO_NET_OFFLINE=true cargo update \
  --manifest-path "${work_dir}/prolly-dynamodb-client-0.1.0/Cargo.toml" \
  --offline \
  --package 'registry+https://github.com/rust-lang/crates.io-index#prolly-map@0.7.0' \
  --package 'registry+https://github.com/rust-lang/crates.io-index#prolly-store-dynamodb@0.6.0' \
  --config "patch.crates-io.prolly-map.path='${work_dir}/prolly-map-0.7.0'" \
  --config "patch.crates-io.prolly-store-dynamodb.path='${work_dir}/prolly-store-dynamodb-0.6.0'" \
  --config "patch.crates-io.prolly-dynamodb-core.path='${work_dir}/prolly-dynamodb-core-0.1.0'"
CARGO_TARGET_DIR="${package_test_target}" cargo test \
  --manifest-path "${work_dir}/prolly-dynamodb-client-0.1.0/Cargo.toml" \
  --locked \
  --all-targets \
  --no-run \
  --config "patch.crates-io.prolly-map.path='${work_dir}/prolly-map-0.7.0'" \
  --config "patch.crates-io.prolly-store-dynamodb.path='${work_dir}/prolly-store-dynamodb-0.6.0'" \
  --config "patch.crates-io.prolly-dynamodb-core.path='${work_dir}/prolly-dynamodb-core-0.1.0'"

consumer="${work_dir}/consumer"
mkdir -p "${consumer}/src"
cat >"${consumer}/Cargo.toml" <<EOF
[package]
name = "prolly-dynamodb-package-consumer"
version = "0.0.0"
edition = "2021"
publish = false

[dependencies]
prolly-dynamodb-client = { path = "${work_dir}/prolly-dynamodb-client-0.1.0" }

[patch.crates-io]
prolly-map = { path = "${work_dir}/prolly-map-0.7.0" }
prolly-store-dynamodb = { path = "${work_dir}/prolly-store-dynamodb-0.6.0" }
prolly-dynamodb-core = { path = "${work_dir}/prolly-dynamodb-core-0.1.0" }
EOF
cat >"${consumer}/src/main.rs" <<'EOF'
use prolly_dynamodb_client::{CancellationToken, Client};

fn main() {
    let _builder = Client::builder();
    let _cancellation = CancellationToken::new();
}
EOF

CARGO_TARGET_DIR="${work_dir}/consumer-target" cargo check \
  --manifest-path "${consumer}/Cargo.toml"

if command -v sha256sum >/dev/null 2>&1; then
  sha256sum "${archives[@]}"
else
  shasum -a 256 "${archives[@]}"
fi
