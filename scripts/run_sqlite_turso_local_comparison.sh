#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
MANIFEST="$REPO_ROOT/benchmarks/sqlite-turso-local/Cargo.toml"
PROFILE="${BENCH_PROFILE:-full}"
OUTPUT="${BENCH_OUT:-$REPO_ROOT/performance-results/sqlite-turso-local-$(date +%F)}"
FORWARDED=()
CLI_SIZES=""
CLI_RUNS=""

while (($#)); do
  case "$1" in
    --profile)
      PROFILE="${2:?--profile requires a value}"
      shift 2
      ;;
    --output)
      OUTPUT="${2:?--output requires a value}"
      shift 2
      ;;
    --sizes)
      CLI_SIZES="${2:?--sizes requires a value}"
      shift 2
      ;;
    --runs)
      CLI_RUNS="${2:?--runs requires a value}"
      shift 2
      ;;
    *)
      FORWARDED+=("$1")
      shift
      ;;
  esac
done

mkdir -p "$OUTPUT"
if [[ "$PROFILE" != "full" && "$PROFILE" != "smoke" ]]; then
  printf 'BENCH_PROFILE must be full or smoke\n' >&2
  exit 2
fi
if [[ "$PROFILE" == "smoke" ]]; then
  SIZES="${CLI_SIZES:-${BENCH_SIZES:-100}}"
  RUNS="${CLI_RUNS:-${BENCH_RUNS:-1}}"
else
  SIZES="${CLI_SIZES:-${BENCH_SIZES:-10000,50000,100000,500000,1000000,2000000}}"
  RUNS="${CLI_RUNS:-${BENCH_RUNS:-3}}"
fi
APIS="${BENCH_APIS:-put,batch,diff,merge}"
PATTERNS="${BENCH_PATTERNS:-append,random,clustered}"
ADAPTERS="${BENCH_ADAPTERS:-sqlite-sync,turso-async}"
TOKIO_WORKERS="${BENCH_TOKIO_WORKERS:-$(sysctl -n hw.logicalcpu 2>/dev/null || getconf _NPROCESSORS_ONLN 2>/dev/null || printf 1)}"
if [[ ! "$RUNS" =~ ^[1-9][0-9]*$ || ! "$TOKIO_WORKERS" =~ ^[1-9][0-9]*$ ]]; then
  printf 'BENCH_RUNS and BENCH_TOKIO_WORKERS must be positive integers\n' >&2
  exit 2
fi
REVISION="$(git -C "$REPO_ROOT" rev-parse HEAD 2>/dev/null || printf unknown)"
if [[ -n "$(git -C "$REPO_ROOT" status --porcelain 2>/dev/null || true)" ]]; then
  DIRTY_FLAG="--dirty"
else
  DIRTY_FLAG="--clean"
fi

{
  printf 'captured_utc=%s\n' "$(date -u +%Y-%m-%dT%H:%M:%SZ)"
  uname -a
  sysctl -n machdep.cpu.brand_string 2>/dev/null || true
  sysctl -n hw.memsize 2>/dev/null || true
  rustc --version
  cargo --version
  df -h "$OUTPUT"
} > "$OUTPUT/machine.txt"

if [[ "${PROLLY_BENCH_SKIP_BUILD:-0}" != "1" ]]; then
  cargo build --release --manifest-path "$MANIFEST" --bin prolly-sqlite-turso-local-bench
  cargo tree --manifest-path "$MANIFEST" > "$OUTPUT/dependencies.txt"
  cargo tree --manifest-path "$MANIFEST" -e features \
    -p prolly-sqlite-turso-local-bench > "$OUTPUT/dependency-features.txt"
  if grep -q 'prolly-store-turso feature "turso-cloud-sync"' \
    "$OUTPUT/dependency-features.txt"; then
    printf 'refusing to run: prolly-store-turso/turso-cloud-sync is enabled\n' >&2
    exit 2
  fi
fi

BENCH_ARGS=(
  --profile "$PROFILE"
  --output "$OUTPUT"
  --revision "$REVISION"
  "$DIRTY_FLAG"
  --sizes "$SIZES"
  --runs "$RUNS"
  --apis "$APIS"
  --patterns "$PATTERNS"
  --adapters "$ADAPTERS"
  --tokio-workers "$TOKIO_WORKERS"
)
if [[ -n "${BENCH_MAX_SECONDS:-}" ]]; then
  BENCH_ARGS+=(--max-seconds "$BENCH_MAX_SECONDS")
fi
if [[ -n "${BENCH_MIN_FREE_GB:-}" ]]; then
  BENCH_ARGS+=(--min-free-gb "$BENCH_MIN_FREE_GB")
fi
if [[ "${BENCH_KEEP_FIXTURES:-0}" == "1" ]]; then
  BENCH_ARGS+=(--keep-fixtures)
fi
if ((${#FORWARDED[@]})); then
  BENCH_ARGS+=("${FORWARDED[@]}")
fi

{
  printf 'started_utc=%s\n' "$(date -u +%Y-%m-%dT%H:%M:%SZ)"
  printf 'revision=%s\nprofile=%s\nsizes=%s\nruns=%s\napis=%s\npatterns=%s\nadapters=%s\ntokio_workers=%s\n' \
    "$REVISION" "$PROFILE" "$SIZES" "$RUNS" "$APIS" "$PATTERNS" "$ADAPTERS" "$TOKIO_WORKERS"
  printf 'benchmark_command='
  printf '%q ' "prolly-sqlite-turso-local-bench" "${BENCH_ARGS[@]}"
  printf '\n'
} > "$OUTPUT/driver-provenance.txt"

if [[ -n "${PROLLY_BENCH_EXECUTABLE:-}" ]]; then
  "$PROLLY_BENCH_EXECUTABLE" "${BENCH_ARGS[@]}"
else
  TARGET_DIR="$(
    cargo metadata --manifest-path "$MANIFEST" --format-version 1 --no-deps |
      sed -n 's/.*"target_directory"[[:space:]]*:[[:space:]]*"\([^"]*\)".*/\1/p'
  )"
  if [[ -z "$TARGET_DIR" ]]; then
    printf 'failed to resolve benchmark target directory from cargo metadata\n' >&2
    exit 2
  fi
  "$TARGET_DIR/release/prolly-sqlite-turso-local-bench" "${BENCH_ARGS[@]}"
fi

"${PYTHON_BIN:-python3}" \
  "$SCRIPT_DIR/summarize_sqlite_turso_local_comparison.py" \
  --input "$OUTPUT/raw-results.csv" \
  --fixtures "$OUTPUT/fixture-results.csv" \
  --output-dir "$OUTPUT" --sizes "$SIZES" --runs "$RUNS"

printf 'ended_utc=%s\n' "$(date -u +%Y-%m-%dT%H:%M:%SZ)" >> "$OUTPUT/driver-provenance.txt"

printf 'comparison complete: %s\n' "$OUTPUT"
