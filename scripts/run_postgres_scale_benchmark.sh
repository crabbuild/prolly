#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
MANIFEST="$REPO_ROOT/benchmarks/postgres-scale/Cargo.toml"
COMPOSE_FILE="$REPO_ROOT/benchmarks/postgres-scale/docker-compose.yml"
PROJECT="prolly-postgres-scale-bench"
PROFILE="${BENCH_PROFILE:-full}"
CONFIG=""
SUITE=""
BASELINE=""
ALLOW_ENVIRONMENT_MISMATCH=0
OUTPUT="${BENCH_OUT:-$REPO_ROOT/performance-results/postgres-scale-$(date +%F)}"
PORT="${PROLLY_POSTGRES_BENCH_PORT:-55433}"
URL="${PROLLY_POSTGRES_BENCH_URL:-postgres://prolly:prolly@127.0.0.1:${PORT}/prolly}"

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
    --config)
      CONFIG="${2:?--config requires a value}"
      shift 2
      ;;
    --suite)
      SUITE="${2:?--suite requires a value}"
      shift 2
      ;;
    --baseline)
      BASELINE="${2:?--baseline requires a value}"
      shift 2
      ;;
    --allow-environment-mismatch)
      ALLOW_ENVIRONMENT_MISMATCH=1
      shift
      ;;
    --url)
      URL="${2:?--url requires a value}"
      shift 2
      ;;
    *)
      printf 'unknown option: %s\n' "$1" >&2
      exit 2
      ;;
  esac
done

case "$PROFILE" in
  smoke)
    SIZES="${BENCH_SIZES:-1000}"
    RUNS="${BENCH_RUNS:-1}"
    CHANGES="${BENCH_CHANGES:-100}"
    READ_SAMPLES="${BENCH_READ_SAMPLES:-100}"
    ;;
  full)
    SIZES="${BENCH_SIZES:-1000000,10000000}"
    RUNS="${BENCH_RUNS:-3}"
    CHANGES="${BENCH_CHANGES:-auto}"
    READ_SAMPLES="${BENCH_READ_SAMPLES:-10000}"
    ;;
  *)
    printf 'BENCH_PROFILE must be smoke or full\n' >&2
    exit 2
    ;;
esac

OPERATIONS="${BENCH_OPERATIONS:-put,batch,get_cold,get_warm,query,scan,full_scan,diff,merge}"
PATTERNS="${BENCH_PATTERNS:-append,random,clustered}"
VALUE_BYTES="${BENCH_VALUE_BYTES:-27}"
CONCURRENCY="${BENCH_CONCURRENCY:-32}"
MIN_FREE_GB="${BENCH_MIN_FREE_GB:-3}"
REVISION="$(git -C "$REPO_ROOT" rev-parse HEAD 2>/dev/null || printf unknown)"
if [[ -n "$(git -C "$REPO_ROOT" status --porcelain 2>/dev/null || true)" ]]; then
  DIRTY=true
  DIRTY_ARG=--dirty
else
  DIRTY=false
  DIRTY_ARG=--clean
fi

mkdir -p "$OUTPUT"
record_exit() {
  bench_exit_code=$?
  if [[ "$bench_exit_code" == 0 ]]; then
    rm -f "$OUTPUT/failure.txt"
  else
    {
      printf 'status=%s\n' "$bench_exit_code"
      printf 'failed_utc=%s\n' "$(date -u +%Y-%m-%dT%H:%M:%SZ)"
    } > "$OUTPUT/failure.txt"
  fi
}
trap record_exit EXIT

if [[ -n "$CONFIG" ]]; then
  DRIVER_MANIFEST="$OUTPUT/driver-manifest.txt"
else
  DRIVER_MANIFEST="$OUTPUT/run-manifest.txt"
fi
{
  printf 'captured_utc=%s\n' "$(date -u +%Y-%m-%dT%H:%M:%SZ)"
  uname -a
  sysctl -n machdep.cpu.brand_string 2>/dev/null || true
  sysctl -n hw.logicalcpu 2>/dev/null || true
  sysctl -n hw.memsize 2>/dev/null || true
  rustc --version
  cargo --version
  docker info --format 'docker_server={{.ServerVersion}} os={{.OperatingSystem}} cpus={{.NCPU}} memory={{.MemTotal}}' 2>/dev/null || true
  df -h "$OUTPUT"
} > "$OUTPUT/machine.txt"

if [[ "${PROLLY_BENCH_SKIP_DOCKER:-0}" != 1 ]]; then
  PROLLY_POSTGRES_BENCH_PORT="$PORT" docker compose -p "$PROJECT" -f "$COMPOSE_FILE" up -d postgres
  for attempt in $(seq 1 60); do
    bench_health="$(docker inspect --format '{{.State.Health.Status}}' "$PROJECT-postgres-1" 2>/dev/null || true)"
    if [[ "$bench_health" == healthy ]]; then
      break
    fi
    if [[ "$attempt" == 60 ]]; then
      docker logs "$PROJECT-postgres-1" >&2
      exit 1
    fi
    sleep 1
  done
  docker exec "$PROJECT-postgres-1" psql -U prolly -d prolly -X -A -t -c \
    "SELECT version(); SHOW shared_preload_libraries; SHOW track_io_timing; SHOW shared_buffers; SHOW work_mem; SHOW synchronous_commit;" \
    > "$OUTPUT/postgres.txt"
else
  printf 'docker capture skipped by PROLLY_BENCH_SKIP_DOCKER=1\n' > "$OUTPUT/postgres.txt"
fi

{
  if [[ -n "$CONFIG" ]]; then
    printf 'schema=postgres-benchmark-driver-v1\n'
    printf 'config=%s\n' "$CONFIG"
    printf 'suite=%s\n' "${SUITE:-configured}"
    printf 'baseline=%s\n' "$BASELINE"
    printf 'allow_environment_mismatch=%s\n' "$ALLOW_ENVIRONMENT_MISMATCH"
  else
    printf 'schema=postgres-scale-v1\n'
  fi
  printf 'revision=%s\n' "$REVISION"
  printf 'dirty=%s\n' "$DIRTY"
  printf 'seed=0x6a09e667f3bcc909\n'
  printf 'profile=%s\n' "$PROFILE"
  printf 'sizes=%s\n' "$SIZES"
  printf 'runs=%s\n' "$RUNS"
  printf 'operations=%s\n' "$OPERATIONS"
  printf 'patterns=%s\n' "$PATTERNS"
  printf 'value_bytes=%s\n' "$VALUE_BYTES"
  printf 'concurrency=%s\n' "$CONCURRENCY"
  printf 'changes=%s\n' "$CHANGES"
  printf 'read_samples=%s\n' "$READ_SAMPLES"
  printf 'merge_changes_semantics=total_split_evenly\n'
  printf 'random_merge_branch_distribution=interleaved\n'
  printf 'min_free_gb=%s\n' "$MIN_FREE_GB"
  printf 'started_utc=%s\n' "$(date -u +%Y-%m-%dT%H:%M:%SZ)"
} > "$DRIVER_MANIFEST"

if [[ "${PROLLY_BENCH_SKIP_BUILD:-0}" != 1 ]]; then
  cargo build --release --manifest-path "$MANIFEST" 2>&1 | tee -a "$OUTPUT/build.log"
  cargo tree --manifest-path "$MANIFEST" > "$OUTPUT/dependencies.txt"
fi

if [[ -n "${PROLLY_BENCH_EXECUTABLE:-}" ]]; then
  EXECUTABLE="$PROLLY_BENCH_EXECUTABLE"
else
  EXECUTABLE="$REPO_ROOT/benchmarks/postgres-scale/target/release/prolly-postgres-scale-bench"
fi
if [[ -f "$EXECUTABLE" ]]; then
  shasum -a 256 "$EXECUTABLE" > "$OUTPUT/binary.sha256"
fi

if [[ -n "$CONFIG" ]]; then
  ARGS=(
    --config "$CONFIG"
    --url "$URL"
    --output "$OUTPUT"
    --revision "$REVISION"
    "$DIRTY_ARG"
  )
  if [[ -n "$SUITE" ]]; then ARGS+=(--suite "$SUITE"); fi
  if [[ -n "$BASELINE" ]]; then ARGS+=(--baseline "$BASELINE"); fi
  if [[ "$ALLOW_ENVIRONMENT_MISMATCH" == 1 ]]; then
    ARGS+=(--allow-environment-mismatch)
  fi
else
  ARGS=(
    --profile "$PROFILE"
    --url "$URL"
    --output "$OUTPUT"
    --revision "$REVISION"
    "$DIRTY_ARG"
    --sizes "$SIZES"
    --runs "$RUNS"
    --operations "$OPERATIONS"
    --patterns "$PATTERNS"
    --value-bytes "$VALUE_BYTES"
    --concurrency "$CONCURRENCY"
    --changes "$CHANGES"
    --read-samples "$READ_SAMPLES"
    --min-free-gb "$MIN_FREE_GB"
  )
fi
"$EXECUTABLE" "${ARGS[@]}" 2>&1 | tee -a "$OUTPUT/run.log"

if [[ -n "$CONFIG" ]]; then
  SUMMARY_ARGS=(
    --manifest "$OUTPUT/run-manifest.txt"
    --resolved-config "$OUTPUT/resolved-workload.toml"
    --output-dir "$OUTPUT"
  )
  if [[ -f "$OUTPUT/service-raw.csv" ]]; then
    SUMMARY_ARGS+=(--service-input "$OUTPUT/service-raw.csv")
  fi
  if [[ -f "$OUTPUT/scale-raw.csv" ]]; then
    SUMMARY_ARGS+=(--scale-input "$OUTPUT/scale-raw.csv")
  fi
  if [[ -n "$BASELINE" ]]; then SUMMARY_ARGS+=(--baseline-dir "$BASELINE"); fi
  if [[ "$ALLOW_ENVIRONMENT_MISMATCH" == 1 ]]; then
    SUMMARY_ARGS+=(--allow-environment-mismatch)
  fi
  "${PYTHON_BIN:-python3}" "$SCRIPT_DIR/summarize_postgres_scale_benchmark.py" \
    "${SUMMARY_ARGS[@]}"
else
  "${PYTHON_BIN:-python3}" "$SCRIPT_DIR/summarize_postgres_scale_benchmark.py" \
    --input "$OUTPUT/raw-results.csv" \
    --manifest "$OUTPUT/run-manifest.txt" \
    --output-dir "$OUTPUT"
fi

printf 'ended_utc=%s\n' "$(date -u +%Y-%m-%dT%H:%M:%SZ)" >> "$OUTPUT/run-manifest.txt"

if [[ "${BENCH_CLEANUP:-0}" == 1 && "${PROLLY_BENCH_SKIP_DOCKER:-0}" != 1 ]]; then
  docker compose -p "$PROJECT" -f "$COMPOSE_FILE" down -v
fi

printf 'PostgreSQL scale benchmark complete: %s\n' "$OUTPUT"
