#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
MANIFEST="$REPO_ROOT/benchmarks/sqlite-scale/Cargo.toml"
PROFILE="${SQLITE_BENCH_PROFILE:-full}"
OUTPUT="${SQLITE_BENCH_OUT:-$REPO_ROOT/performance-results/sqlite/baseline}"

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
    *)
      printf 'unknown runner option: %s\n' "$1" >&2
      exit 2
      ;;
  esac
done

case "$PROFILE" in
  smoke)
    SIZES="${SQLITE_BENCH_SIZES:-100}"
    RUNS="${SQLITE_BENCH_RUNS:-1}"
    CHANGES="${SQLITE_BENCH_CHANGES:-10}"
    READ_SAMPLES="${SQLITE_BENCH_READ_SAMPLES:-10}"
    MIN_FREE_GB="${SQLITE_BENCH_MIN_FREE_GB:-0}"
    ;;
  full)
    SIZES="${SQLITE_BENCH_SIZES:-1000000}"
    RUNS="${SQLITE_BENCH_RUNS:-3}"
    CHANGES="${SQLITE_BENCH_CHANGES:-auto}"
    READ_SAMPLES="${SQLITE_BENCH_READ_SAMPLES:-10000}"
    MIN_FREE_GB="${SQLITE_BENCH_MIN_FREE_GB:-3}"
    ;;
  *)
    printf 'profile must be smoke or full\n' >&2
    exit 2
    ;;
esac

OPERATIONS="${SQLITE_BENCH_OPERATIONS:-put,batch,get_cold,get_warm,query,scan,full_scan,diff,merge}"
PATTERNS="${SQLITE_BENCH_PATTERNS:-append,random,clustered}"
REVISION="$(git -C "$REPO_ROOT" rev-parse HEAD 2>/dev/null || printf unknown)"
if [[ -n "$(git -C "$REPO_ROOT" status --porcelain 2>/dev/null || true)" ]]; then
  DIRTY=true
  DIRTY_ARG=--dirty
else
  DIRTY=false
  DIRTY_ARG=--clean
fi

mkdir -p "$OUTPUT"
STARTED_UTC="$(date -u +%Y-%m-%dT%H:%M:%SZ)"
{
  printf 'captured_utc=%s\n' "$STARTED_UTC"
  uname -a
  sysctl -n machdep.cpu.brand_string 2>/dev/null || true
  sysctl -n hw.logicalcpu 2>/dev/null || true
  sysctl -n hw.memsize 2>/dev/null || true
  rustc --version
  cargo --version
  df -h "$OUTPUT"
} > "$OUTPUT/machine.txt"

git -C "$REPO_ROOT" status --porcelain=v1 > "$OUTPUT/source-status.txt"
git -C "$REPO_ROOT" diff --binary HEAD | gzip -9 > "$OUTPUT/source-diff.patch.gz"
tar -czf "$OUTPUT/harness-source.tar.gz" -C "$REPO_ROOT" \
  benchmarks/sqlite-scale/Cargo.toml \
  benchmarks/sqlite-scale/Cargo.lock \
  benchmarks/sqlite-scale/src \
  benchmarks/sqlite-scale/tests \
  scripts/run_sqlite_scale_benchmark.sh \
  docs/sqlite-scale-benchmark.md
shasum -a 256 "$OUTPUT/harness-source.tar.gz" > "$OUTPUT/harness-source.sha256"

if [[ "${SQLITE_BENCH_SKIP_BUILD:-0}" != 1 ]]; then
  CARGO_INCREMENTAL=0 cargo build --release --manifest-path "$MANIFEST" \
    2>&1 | tee "$OUTPUT/build.log"
  cargo tree --manifest-path "$MANIFEST" > "$OUTPUT/dependencies.txt"
fi

if [[ -n "${SQLITE_BENCH_EXECUTABLE:-}" ]]; then
  EXECUTABLE="$SQLITE_BENCH_EXECUTABLE"
else
  EXECUTABLE="$REPO_ROOT/benchmarks/sqlite-scale/target/release/prolly-sqlite-scale-bench"
fi
if [[ ! -x "$EXECUTABLE" ]]; then
  printf 'benchmark executable is missing or not executable: %s\n' "$EXECUTABLE" >&2
  exit 1
fi
shasum -a 256 "$EXECUTABLE" > "$OUTPUT/binary.sha256"

ARGS=(
  --profile "$PROFILE"
  --output "$OUTPUT"
  --revision "$REVISION"
  "$DIRTY_ARG"
  --sizes "$SIZES"
  --runs "$RUNS"
  --operations "$OPERATIONS"
  --patterns "$PATTERNS"
  --changes "$CHANGES"
  --read-samples "$READ_SAMPLES"
  --min-free-gb "$MIN_FREE_GB"
)
if [[ "${SQLITE_BENCH_KEEP_FIXTURES:-0}" == 1 ]]; then
  ARGS+=(--keep-fixtures)
fi

{
  printf 'driver=%s\n' "$REPO_ROOT/scripts/run_sqlite_scale_benchmark.sh"
  printf 'revision=%s\n' "$REVISION"
  printf 'dirty=%s\n' "$DIRTY"
  printf 'started_utc=%s\n' "$STARTED_UTC"
  printf 'arguments='
  printf ' %q' "${ARGS[@]}"
  printf '\n'
} > "$OUTPUT/driver-provenance.txt"

"$EXECUTABLE" "${ARGS[@]}" 2>&1 | tee "$OUTPUT/run.log"
printf 'ended_utc=%s\n' "$(date -u +%Y-%m-%dT%H:%M:%SZ)" >> "$OUTPUT/driver-provenance.txt"
printf 'SQLite scale benchmark complete: %s\n' "$OUTPUT"
