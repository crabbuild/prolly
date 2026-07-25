#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
MANIFEST="$REPO_ROOT/benchmarks/dynamodb-scale/Cargo.toml"
COMPOSE_FILE="$REPO_ROOT/docker-compose.store-services.yml"
PROJECT="${PROLLY_DYNAMODB_BENCH_PROJECT:-prolly-dynamodb-scale-bench}"
PORT="${PROLLY_DYNAMODB_BENCH_PORT:-58000}"
ENDPOINT="${PROLLY_DYNAMODB_BENCH_ENDPOINT:-http://127.0.0.1:${PORT}}"
TABLE="${PROLLY_DYNAMODB_BENCH_TABLE:-prolly_benchmark}"
PROFILE="${BENCH_PROFILE:-full}"
OUTPUT="${BENCH_OUT:-$REPO_ROOT/performance-results/dynamodb-local-$(date +%F)}"
BASELINE=""

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
    --baseline)
      BASELINE="${2:?--baseline requires a value}"
      shift 2
      ;;
    --endpoint)
      ENDPOINT="${2:?--endpoint requires a value}"
      shift 2
      ;;
    --table)
      TABLE="${2:?--table requires a value}"
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
    RECORDS="${BENCH_RECORDS:-10000}"
    RAW_ITEMS="${BENCH_RAW_ITEMS:-500}"
    SAMPLES="${BENCH_SAMPLES:-1000}"
    CHANGES="${BENCH_CHANGES:-1000}"
    ROOTS="${BENCH_ROOTS:-100}"
    CONFLICTS="${BENCH_CONFLICTS:-20}"
    CONCURRENT_OPERATIONS="${BENCH_CONCURRENT_OPERATIONS:-1000}"
    RUNS="${BENCH_RUNS:-1}"
    ;;
  full)
    RECORDS="${BENCH_RECORDS:-100000}"
    RAW_ITEMS="${BENCH_RAW_ITEMS:-2500}"
    SAMPLES="${BENCH_SAMPLES:-10000}"
    CHANGES="${BENCH_CHANGES:-10000}"
    ROOTS="${BENCH_ROOTS:-1000}"
    CONFLICTS="${BENCH_CONFLICTS:-100}"
    CONCURRENT_OPERATIONS="${BENCH_CONCURRENT_OPERATIONS:-10000}"
    RUNS="${BENCH_RUNS:-3}"
    ;;
  *)
    printf 'BENCH_PROFILE must be smoke or full\n' >&2
    exit 2
    ;;
esac

VALUE_BYTES="${BENCH_VALUE_BYTES:-256}"
CONCURRENCY="${BENCH_CONCURRENCY:-32}"
READ_PARALLELISM="${BENCH_READ_PARALLELISM:-16}"
BATCH_GET_PARALLELISM="${BENCH_BATCH_GET_PARALLELISM:-8}"
BATCH_WRITE_PARALLELISM="${BENCH_BATCH_WRITE_PARALLELISM:-8}"
SCAN_PARALLELISM="${BENCH_SCAN_PARALLELISM:-8}"
NAMESPACE_CLEANUP="${BENCH_NAMESPACE_CLEANUP:-1}"
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

{
  printf 'captured_utc=%s\n' "$(date -u +%Y-%m-%dT%H:%M:%SZ)"
  uname -a
  rustc --version
  cargo --version
  docker info --format 'docker_server={{.ServerVersion}} os={{.OperatingSystem}} cpus={{.NCPU}} memory={{.MemTotal}}' 2>/dev/null || true
} > "$OUTPUT/machine.txt"

if [[ "${PROLLY_BENCH_SKIP_DOCKER:-0}" != 1 ]]; then
  PROLLY_STORE_DYNAMODB_PORT="$PORT" \
    docker compose -p "$PROJECT" -f "$COMPOSE_FILE" up -d dynamodb
  for attempt in $(seq 1 60); do
    if curl --silent --output /dev/null "$ENDPOINT"; then
      break
    fi
    if [[ "$attempt" == 60 ]]; then
      docker compose -p "$PROJECT" -f "$COMPOSE_FILE" logs dynamodb >&2
      exit 1
    fi
    sleep 1
  done
fi

{
  printf 'schema=dynamodb-local-scale-v2\n'
  printf 'revision=%s\n' "$REVISION"
  printf 'dirty=%s\n' "$DIRTY"
  printf 'profile=%s\n' "$PROFILE"
  printf 'endpoint=%s\n' "$ENDPOINT"
  printf 'table=%s\n' "$TABLE"
  printf 'records=%s\n' "$RECORDS"
  printf 'value_bytes=%s\n' "$VALUE_BYTES"
  printf 'raw_items=%s\n' "$RAW_ITEMS"
  printf 'samples=%s\n' "$SAMPLES"
  printf 'changes=%s\n' "$CHANGES"
  printf 'roots=%s\n' "$ROOTS"
  printf 'conflicts=%s\n' "$CONFLICTS"
  printf 'concurrency=%s\n' "$CONCURRENCY"
  printf 'concurrent_operations=%s\n' "$CONCURRENT_OPERATIONS"
  printf 'read_parallelism=%s\n' "$READ_PARALLELISM"
  printf 'batch_get_parallelism=%s\n' "$BATCH_GET_PARALLELISM"
  printf 'batch_write_parallelism=%s\n' "$BATCH_WRITE_PARALLELISM"
  printf 'scan_parallelism=%s\n' "$SCAN_PARALLELISM"
  printf 'namespace_cleanup=%s\n' "$NAMESPACE_CLEANUP"
  printf 'runs=%s\n' "$RUNS"
  printf 'started_utc=%s\n' "$(date -u +%Y-%m-%dT%H:%M:%SZ)"
} > "$OUTPUT/run-manifest.txt"

if [[ "${PROLLY_BENCH_SKIP_BUILD:-0}" != 1 ]]; then
  cargo build --release --manifest-path "$MANIFEST" 2>&1 | tee -a "$OUTPUT/build.log"
  cargo tree --manifest-path "$MANIFEST" > "$OUTPUT/dependencies.txt"
fi

EXECUTABLE="${PROLLY_BENCH_EXECUTABLE:-$REPO_ROOT/benchmarks/dynamodb-scale/target/release/prolly-dynamodb-scale-bench}"
shasum -a 256 "$EXECUTABLE" > "$OUTPUT/binary.sha256"
BENCH_ARGS=(
  --endpoint "$ENDPOINT"
  --table "$TABLE"
  --output "$OUTPUT"
  --records "$RECORDS"
  --value-bytes "$VALUE_BYTES"
  --raw-items "$RAW_ITEMS"
  --samples "$SAMPLES"
  --changes "$CHANGES"
  --roots "$ROOTS"
  --conflicts "$CONFLICTS"
  --concurrency "$CONCURRENCY"
  --concurrent-operations "$CONCURRENT_OPERATIONS"
  --read-parallelism "$READ_PARALLELISM"
  --batch-get-parallelism "$BATCH_GET_PARALLELISM"
  --batch-write-parallelism "$BATCH_WRITE_PARALLELISM"
  --scan-parallelism "$SCAN_PARALLELISM"
  --runs "$RUNS"
  --revision "$REVISION"
  "$DIRTY_ARG"
)
if [[ "$NAMESPACE_CLEANUP" == 0 ]]; then
  BENCH_ARGS+=(--skip-namespace-cleanup)
fi
"$EXECUTABLE" "${BENCH_ARGS[@]}" 2>&1 | tee -a "$OUTPUT/run.log"

SUMMARY_ARGS=(
  --input "$OUTPUT/raw-results.csv"
  --output-dir "$OUTPUT"
)
if [[ -n "$BASELINE" ]]; then
  SUMMARY_ARGS+=(--baseline "$BASELINE")
fi
python3 "$SCRIPT_DIR/summarize_dynamodb_scale_benchmark.py" "${SUMMARY_ARGS[@]}"
printf 'ended_utc=%s\n' "$(date -u +%Y-%m-%dT%H:%M:%SZ)" >> "$OUTPUT/run-manifest.txt"

if [[ "${BENCH_CLEANUP:-0}" == 1 && "${PROLLY_BENCH_SKIP_DOCKER:-0}" != 1 ]]; then
  docker compose -p "$PROJECT" -f "$COMPOSE_FILE" down -v
fi

printf 'DynamoDB Local benchmark complete: %s\n' "$OUTPUT"
