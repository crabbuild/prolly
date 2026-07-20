#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
MANIFEST="$REPO_ROOT/benchmarks/turso-scale/Cargo.toml"
PROFILE="${TURSO_BENCH_PROFILE:-full}"
OUTPUT="${TURSO_BENCH_OUT:-$REPO_ROOT/performance-results/turso/baseline}"

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
    SIZES="${TURSO_BENCH_SIZES:-100}"
    RUNS="${TURSO_BENCH_RUNS:-1}"
    CHANGES="${TURSO_BENCH_CHANGES:-10}"
    READ_SAMPLES="${TURSO_BENCH_READ_SAMPLES:-10}"
    MIN_FREE_GB="${TURSO_BENCH_MIN_FREE_GB:-0}"
    ;;
  full)
    SIZES="${TURSO_BENCH_SIZES:-1000000}"
    RUNS="${TURSO_BENCH_RUNS:-3}"
    CHANGES="${TURSO_BENCH_CHANGES:-auto}"
    READ_SAMPLES="${TURSO_BENCH_READ_SAMPLES:-10000}"
    MIN_FREE_GB="${TURSO_BENCH_MIN_FREE_GB:-3}"
    ;;
  *)
    printf 'profile must be smoke or full\n' >&2
    exit 2
    ;;
esac

OPERATIONS="${TURSO_BENCH_OPERATIONS:-put,batch,get_cold,get_warm,query,scan,full_scan,diff,merge}"
PATTERNS="${TURSO_BENCH_PATTERNS:-append,random,clustered}"
TOKIO_WORKERS="${TURSO_BENCH_TOKIO_WORKERS:-4}"
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
  benchmarks/turso-scale/Cargo.toml \
  benchmarks/turso-scale/Cargo.lock \
  benchmarks/turso-scale/src \
  benchmarks/turso-scale/tests \
  stores/prolly-store-turso/Cargo.toml \
  scripts/run_turso_scale_benchmark.sh \
  docs/turso-scale-benchmark.md \
  docs/superpowers/specs/2026-07-19-turso-1m-30pct-baseline-design.md \
  docs/superpowers/plans/2026-07-19-turso-1m-30pct-baseline.md
(
  cd "$OUTPUT"
  shasum -a 256 harness-source.tar.gz > harness-source.sha256
)

if [[ "${TURSO_BENCH_SKIP_BUILD:-0}" != 1 ]]; then
  CARGO_INCREMENTAL=0 cargo build --release --manifest-path "$MANIFEST" \
    2>&1 | tee "$OUTPUT/build.log"
fi
cargo tree --manifest-path "$MANIFEST" > "$OUTPUT/dependencies.txt"
cargo tree --manifest-path "$MANIFEST" -e features > "$OUTPUT/dependency-features.txt"
if rg -q 'prolly-store-turso feature "turso-cloud-sync"' "$OUTPUT/dependency-features.txt"; then
  printf 'refusing to run: prolly-store-turso/turso-cloud-sync is enabled\n' >&2
  exit 2
fi

if [[ -n "${TURSO_BENCH_EXECUTABLE:-}" ]]; then
  EXECUTABLE="$TURSO_BENCH_EXECUTABLE"
else
  TARGET_DIR="$(cargo metadata --manifest-path "$MANIFEST" --format-version 1 --no-deps \
    | python3 -c 'import json,sys; print(json.load(sys.stdin)["target_directory"])')"
  EXECUTABLE="$TARGET_DIR/release/prolly-turso-scale-bench"
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
  --tokio-workers "$TOKIO_WORKERS"
)
if [[ "${TURSO_BENCH_KEEP_FIXTURES:-0}" == 1 ]]; then
  ARGS+=(--keep-fixtures)
fi

{
  printf 'driver=%s\n' "$REPO_ROOT/scripts/run_turso_scale_benchmark.sh"
  printf 'revision=%s\n' "$REVISION"
  printf 'dirty=%s\n' "$DIRTY"
  printf 'started_utc=%s\n' "$STARTED_UTC"
  printf 'arguments='
  printf ' %q' "${ARGS[@]}"
  printf '\n'
} > "$OUTPUT/driver-provenance.txt"

"$EXECUTABLE" "${ARGS[@]}" 2>&1 | tee "$OUTPUT/run.log"
printf 'ended_utc=%s\n' "$(date -u +%Y-%m-%dT%H:%M:%SZ)" >> "$OUTPUT/driver-provenance.txt"
printf 'Turso scale benchmark complete: %s\n' "$OUTPUT"
