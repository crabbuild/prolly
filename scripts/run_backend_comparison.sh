#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
OUTPUT="${BENCH_OUT:-$REPO_ROOT/performance-results/backend-comparison-$(date +%F)}"
RECORDS="${BENCH_RECORDS:-1000000}"
VALUE_BYTES="${BENCH_VALUE_BYTES:-27}"
CHANGES="${BENCH_CHANGES:-10000}"
SAMPLES="${BENCH_SAMPLES:-10000}"
RUNS="${BENCH_RUNS:-3}"
POSTGRES_COMPOSE="$REPO_ROOT/benchmarks/postgres-scale/docker-compose.yml"
DYNAMODB_COMPOSE="$REPO_ROOT/docker-compose.store-services.yml"

cleanup() {
  docker compose -p prolly-postgres-scale-bench -f "$POSTGRES_COMPOSE" down -v \
    >/dev/null 2>&1 || true
  docker compose -p prolly-dynamodb-scale-bench -f "$DYNAMODB_COMPOSE" down -v \
    >/dev/null 2>&1 || true
}
trap cleanup EXIT

if ((CHANGES % 2 != 0)); then
  printf 'BENCH_CHANGES must be even so merge can split work equally\n' >&2
  exit 2
fi

mkdir -p "$OUTPUT"

BENCH_SIZES="$RECORDS" \
BENCH_VALUE_BYTES="$VALUE_BYTES" \
BENCH_CHANGES="$CHANGES" \
BENCH_READ_SAMPLES="$SAMPLES" \
BENCH_RUNS="$RUNS" \
BENCH_OPERATIONS=batch,query,concurrent_query,diff,merge \
BENCH_PATTERNS=random \
BENCH_MIN_FREE_GB="${BENCH_MIN_FREE_GB:-0}" \
BENCH_CONCURRENCY="${BENCH_CONCURRENCY:-32}" \
BENCH_OUT="$OUTPUT/postgres" \
"$SCRIPT_DIR/run_postgres_scale_benchmark.sh" --profile full

docker compose -p prolly-postgres-scale-bench -f "$POSTGRES_COMPOSE" down -v

BENCH_RECORDS="$RECORDS" \
BENCH_VALUE_BYTES="$VALUE_BYTES" \
BENCH_CHANGES="$CHANGES" \
BENCH_SAMPLES="$SAMPLES" \
BENCH_RUNS="$RUNS" \
BENCH_RAW_ITEMS="${BENCH_RAW_ITEMS:-2500}" \
BENCH_ROOTS="${BENCH_ROOTS:-1000}" \
BENCH_CONFLICTS="${BENCH_CONFLICTS:-100}" \
BENCH_CONCURRENT_OPERATIONS="${BENCH_CONCURRENT_OPERATIONS:-$SAMPLES}" \
BENCH_CONCURRENCY="${BENCH_CONCURRENCY:-32}" \
BENCH_READ_PARALLELISM="${BENCH_READ_PARALLELISM:-16}" \
BENCH_BATCH_GET_PARALLELISM="${BENCH_BATCH_GET_PARALLELISM:-16}" \
BENCH_BATCH_WRITE_PARALLELISM="${BENCH_BATCH_WRITE_PARALLELISM:-16}" \
BENCH_SCAN_PARALLELISM="${BENCH_SCAN_PARALLELISM:-8}" \
BENCH_NAMESPACE_CLEANUP=0 \
BENCH_OUT="$OUTPUT/dynamodb" \
"$SCRIPT_DIR/run_dynamodb_scale_benchmark.sh" \
  --profile full \
  --table prolly_backend_comparison

SUMMARY_ARGS=(
  --postgres "$OUTPUT/postgres"
  --dynamodb "$OUTPUT/dynamodb"
  --output-dir "$OUTPUT"
)
if [[ -n "${BENCH_POSTGRES_BASELINE:-}" ]]; then
  SUMMARY_ARGS+=(--postgres-baseline "$BENCH_POSTGRES_BASELINE")
fi
if [[ -n "${BENCH_DYNAMODB_BASELINE:-}" ]]; then
  SUMMARY_ARGS+=(--dynamodb-baseline "$BENCH_DYNAMODB_BASELINE")
fi
python3 "$SCRIPT_DIR/summarize_backend_comparison.py" "${SUMMARY_ARGS[@]}"

printf 'Backend comparison complete: %s\n' "$OUTPUT"
