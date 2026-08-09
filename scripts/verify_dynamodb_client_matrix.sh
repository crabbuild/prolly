#!/usr/bin/env bash
set -euo pipefail

toolchain="1.91.1"
target=""
while [[ "$#" -gt 0 ]]; do
  case "$1" in
    --toolchain)
      [[ "$#" -ge 2 ]] || { echo "--toolchain requires a value" >&2; exit 64; }
      toolchain="$2"
      shift 2
      ;;
    --target)
      [[ "$#" -ge 2 ]] || { echo "--target requires a value" >&2; exit 64; }
      target="$2"
      shift 2
      ;;
    *)
      echo "usage: $0 [--toolchain TOOLCHAIN] [--target TARGET]" >&2
      exit 64
      ;;
  esac
done

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
matrix_target="$(mktemp -d "${TMPDIR:-/tmp}/prolly-dynamodb-matrix.XXXXXX")"
trap 'rm -rf -- "${matrix_target}"' EXIT

cargo_cmd=(cargo "+${toolchain}")
export CARGO_TARGET_DIR="${matrix_target}"

"${cargo_cmd[@]}" --version
rustc "+${toolchain}" --version --verbose
if [[ -n "${target}" ]]; then
  target_libdir="$(rustc "+${toolchain}" --print target-libdir --target "${target}" 2>/dev/null || true)"
  if [[ -z "${target_libdir}" || ! -d "${target_libdir}" ]]; then
    echo "Rust target ${target} is not installed for toolchain ${toolchain}; run: rustup target add --toolchain ${toolchain} ${target}" >&2
    exit 69
  fi
  target_env="$(printf '%s' "${target}" | tr '[:lower:]-' '[:upper:]_')"
  linker_var="CARGO_TARGET_${target_env}_LINKER"
  matrix_linker="$(printenv "${linker_var}" 2>/dev/null || true)"
  if [[ -n "${matrix_linker}" ]]; then
    matrix_linker_path="$(command -v "${matrix_linker}" 2>/dev/null || true)"
    [[ -n "${matrix_linker_path}" && -x "${matrix_linker_path}" ]] || {
      echo "${linker_var} does not name an executable: ${matrix_linker}" >&2
      exit 69
    }
    "${matrix_linker_path}" --version | sed -n '1p'
  fi
fi

run_check() {
  if [[ -n "${target}" ]]; then
    "${cargo_cmd[@]}" check --locked "$@" --target "${target}"
  else
    "${cargo_cmd[@]}" check --locked "$@"
  fi
}

# The root crate has a genuinely optional Tokio integration. Prove the minimal,
# default, and Tokio-enabled library surfaces separately.
run_check --manifest-path "${repo_root}/Cargo.toml" --lib --no-default-features
run_check --manifest-path "${repo_root}/Cargo.toml" --lib
run_check --manifest-path "${repo_root}/Cargo.toml" --lib --features tokio

# The Versioned DynamoDB crates intentionally have one semantic feature set.
# Running both forms proves that --no-default-features cannot silently remove
# validation, history, transaction, or worker behavior.
for manifest in \
  "${repo_root}/stores/prolly-store-dynamodb/Cargo.toml" \
  "${repo_root}/dynamodb/core/Cargo.toml" \
  "${repo_root}/dynamodb/client/Cargo.toml"
do
  run_check --manifest-path "${manifest}" --all-targets
  run_check --manifest-path "${manifest}" --all-targets --no-default-features
done

run_check --manifest-path "${repo_root}/dynamodb/admin/Cargo.toml" --all-targets

# Public AWS model types and the qualified TLS implementation are exact release
# inputs. These commands fail if the expected package is absent or ambiguous.
"${cargo_cmd[@]}" tree --locked --manifest-path "${repo_root}/dynamodb/client/Cargo.toml" \
  -i aws-sdk-dynamodb@1.73.0 >/dev/null
"${cargo_cmd[@]}" tree --locked --manifest-path "${repo_root}/dynamodb/client/Cargo.toml" \
  -i aws-lc-rs@1.17.3 >/dev/null
"${cargo_cmd[@]}" tree --locked --manifest-path "${repo_root}/dynamodb/client/Cargo.toml" \
  -i aws-lc-sys@0.43.0 >/dev/null

printf 'matrix_ok toolchain=%s target=%s\n' "${toolchain}" "${target:-host}"
