#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
PROFILE="${BENCH_MATRIX_PROFILE:-smoke}"
OUTPUT="${BENCH_MATRIX_OUT:-$REPO_ROOT/performance-results/dynamodb-client-matrix-$(date +%F)}"
PRINT_CONFIG=false

while (($#)); do
  case "$1" in
    --profile) PROFILE="${2:?--profile requires a value}"; shift 2 ;;
    --output) OUTPUT="${2:?--output requires a value}"; shift 2 ;;
    --print-config) PRINT_CONFIG=true; shift ;;
    *) printf 'unknown option: %s\n' "$1" >&2; exit 2 ;;
  esac
done

case "$PROFILE" in
  smoke)
    SAMPLES="${BENCH_SAMPLES:-1}"
    CASES=(
      "smoke-1k:100:1024:100:1,10,100"
      "smoke-64k:100:65536:63:1,10"
      "smoke-near400k:100:399000:10:1,10"
    )
    ;;
  qualification)
    SAMPLES="${BENCH_SAMPLES:-100}"
    CASES=(
      "10k-1k:10000:1024:100:1,10,100"
      "10k-16k:10000:16384:100:1,10,100"
      "10k-64k:10000:65536:63:1,10"
      "10k-near400k:10000:399000:10:1,10"
      "1m-1k:1000000:1024:100:1,10,100"
    )
    ;;
  *)
    printf 'profile must be smoke or qualification\n' >&2
    exit 2
    ;;
esac

NODE_CACHE_MAX_BYTES="${BENCH_NODE_CACHE_MAX_BYTES:-67108864}"
if ! [[ "$SAMPLES" =~ ^[1-9][0-9]*$ ]] || ! [[ "$NODE_CACHE_MAX_BYTES" =~ ^[0-9]+$ ]]; then
  printf 'BENCH_SAMPLES must be positive and BENCH_NODE_CACHE_MAX_BYTES must be non-negative\n' >&2
  exit 2
fi

CASE_NAMES=""
for specification in "${CASES[@]}"; do
  case_name="${specification%%:*}"
  CASE_NAMES="${CASE_NAMES:+$CASE_NAMES,}$case_name"
done
CASE_SPECIFICATIONS="$(IFS=';'; printf '%s' "${CASES[*]}")"

if [[ "$PRINT_CONFIG" == true ]]; then
  printf 'profile=%s cases=%s samples=%s node_cache_max_bytes=%s output=%s\n' \
    "$PROFILE" "$CASE_NAMES" "$SAMPLES" "$NODE_CACHE_MAX_BYTES" "$OUTPUT"
  exit 0
fi

if [[ "$PROFILE" == qualification \
  && "${PROLLY_BENCH_ALLOW_DIRTY:-0}" != 1 \
  && -n "$(git -C "$REPO_ROOT" status --porcelain 2>/dev/null || true)" ]]; then
  printf 'qualification requires a clean worktree; set PROLLY_BENCH_ALLOW_DIRTY=1 only for diagnostic runs\n' >&2
  exit 2
fi

mkdir -p "$OUTPUT"
REVISION="$(git -C "$REPO_ROOT" rev-parse HEAD 2>/dev/null || printf unknown)"
MATRIX_ROWS="$OUTPUT/matrix.csv.tmp"
HISTORY_DEPTH="${BENCH_HISTORY_DEPTH:-100}"
CONCURRENCY_WRITERS="${BENCH_CONCURRENCY_WRITERS:-1,4,8}"
CONCURRENCY_OPERATIONS_PER_WRITER="${BENCH_CONCURRENCY_OPERATIONS_PER_WRITER:-5}"
CONCURRENCY_RETRY_LIMIT="${BENCH_CONCURRENCY_RETRY_LIMIT:-7}"
COMPLETED_MANIFEST="$OUTPUT/matrix-manifest.txt"
COMPLETED_UTC=""

if [[ -e "$COMPLETED_MANIFEST" ]]; then
  if [[ "$(wc -l < "$COMPLETED_MANIFEST")" -ne 8 ]] \
    || ! rg -Fxq -- 'schema=dynamodb-client-matrix-v2' "$COMPLETED_MANIFEST" \
    || ! rg -Fxq -- "profile=$PROFILE" "$COMPLETED_MANIFEST" \
    || ! rg -Fxq -- "revision=$REVISION" "$COMPLETED_MANIFEST" \
    || ! rg -Fxq -- "case_names=$CASE_NAMES" "$COMPLETED_MANIFEST" \
    || ! rg -Fxq -- "cases=${#CASES[@]}" "$COMPLETED_MANIFEST" \
    || ! rg -Fxq -- "samples_per_case=$SAMPLES" "$COMPLETED_MANIFEST" \
    || ! rg -Fxq -- "node_cache_max_bytes=$NODE_CACHE_MAX_BYTES" "$COMPLETED_MANIFEST"; then
    printf 'completed benchmark matrix manifest does not match this exact run\n' >&2
    exit 1
  fi
  COMPLETED_UTC="$(sed -n 's/^completed_utc=//p' "$COMPLETED_MANIFEST")"
  if ! [[ "$COMPLETED_UTC" =~ ^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}Z$ ]]; then
    printf 'completed benchmark matrix manifest has an invalid completion timestamp\n' >&2
    exit 1
  fi
fi

trap 'rm -f "$MATRIX_ROWS"' EXIT

printf 'schema,matrix_profile,case,records,value_bytes,read_batch_items,history_depth,transaction_shapes,concurrency_writers,concurrency_operations_per_writer,concurrency_retry_limit,node_cache_max_bytes,samples,revision,result_dir,status\n' \
  > "$MATRIX_ROWS"

for specification in "${CASES[@]}"; do
  IFS=: read -r case_name records value_bytes read_batch_items transaction_shapes <<< "$specification"
  case_output="$OUTPUT/$case_name"
  if [[ -e "$case_output/raw-samples.csv" ]]; then
    if [[ -e "$case_output/failure.txt" ]] || ! rg -q '^ended_utc=' "$case_output/run-manifest.txt"; then
      printf 'case %s contains incomplete evidence; choose a new output directory\n' "$case_name" >&2
      exit 1
    fi
    python3 "$SCRIPT_DIR/validate_dynamodb_client_benchmark.py" \
      --input "$case_output/raw-samples.csv" \
      --manifest "$case_output/run-manifest.txt" \
      --samples "$SAMPLES" \
      --records "$records" \
      --value-bytes "$value_bytes" \
      --read-batch-items "$read_batch_items" \
      --history-depth "$HISTORY_DEPTH" \
      --workload full \
      --concurrency-writers "$CONCURRENCY_WRITERS" \
      --concurrency-operations-per-writer "$CONCURRENCY_OPERATIONS_PER_WRITER" \
      --concurrency-retry-limit "$CONCURRENCY_RETRY_LIMIT" \
      --revision "$REVISION" \
      --transaction-shapes "$transaction_shapes" \
      --node-cache-max-bytes "$NODE_CACHE_MAX_BYTES"
    python3 "$SCRIPT_DIR/summarize_dynamodb_client_benchmark.py" \
      --input "$case_output/raw-samples.csv" --output-dir "$case_output"
  else
    BENCH_RECORDS="$records" \
    BENCH_VALUE_BYTES="$value_bytes" \
    BENCH_READ_BATCH_ITEMS="$read_batch_items" \
    BENCH_HISTORY_DEPTH="$HISTORY_DEPTH" \
    BENCH_CONCURRENCY_WRITERS="$CONCURRENCY_WRITERS" \
    BENCH_CONCURRENCY_OPERATIONS_PER_WRITER="$CONCURRENCY_OPERATIONS_PER_WRITER" \
    BENCH_CONCURRENCY_RETRY_LIMIT="$CONCURRENCY_RETRY_LIMIT" \
    BENCH_NODE_CACHE_MAX_BYTES="$NODE_CACHE_MAX_BYTES" \
    BENCH_SAMPLES="$SAMPLES" \
    BENCH_TRANSACTION_SHAPES="$transaction_shapes" \
    BENCH_CLEANUP=1 \
      "$SCRIPT_DIR/run_dynamodb_client_benchmark.sh" \
        --profile repeatable \
        --output "$case_output" \
        --table "prolly_client_matrix_${case_name//-/_}"
  fi
  printf 'dynamodb-client-matrix-v2,%s,%s,%s,%s,%s,%s,"%s","%s",%s,%s,%s,%s,%s,%s,validated\n' \
    "$PROFILE" "$case_name" "$records" "$value_bytes" "$read_batch_items" "$HISTORY_DEPTH" "$transaction_shapes" "$CONCURRENCY_WRITERS" "$CONCURRENCY_OPERATIONS_PER_WRITER" "$CONCURRENCY_RETRY_LIMIT" \
    "$NODE_CACHE_MAX_BYTES" "$SAMPLES" "$REVISION" "$case_name" >> "$MATRIX_ROWS"
done

if [[ -e "$OUTPUT/matrix.csv" ]]; then
  if ! cmp -s "$MATRIX_ROWS" "$OUTPUT/matrix.csv"; then
    printf 'completed benchmark matrix CSV differs from revalidated cases\n' >&2
    exit 1
  fi
  rm -f "$MATRIX_ROWS"
else
  mv "$MATRIX_ROWS" "$OUTPUT/matrix.csv"
fi

if [[ -z "$COMPLETED_UTC" ]]; then
  {
    printf 'schema=dynamodb-client-matrix-v2\n'
    printf 'profile=%s\n' "$PROFILE"
    printf 'revision=%s\n' "$REVISION"
    printf 'case_names=%s\n' "$CASE_NAMES"
    printf 'cases=%s\n' "${#CASES[@]}"
    printf 'samples_per_case=%s\n' "$SAMPLES"
    printf 'node_cache_max_bytes=%s\n' "$NODE_CACHE_MAX_BYTES"
    printf 'completed_utc=%s\n' "$(date -u +%Y-%m-%dT%H:%M:%SZ)"
  } > "$COMPLETED_MANIFEST"
fi

python3 "$SCRIPT_DIR/validate_dynamodb_client_matrix.py" \
  --kind full \
  --input "$OUTPUT/matrix.csv" \
  --manifest "$COMPLETED_MANIFEST" \
  --profile "$PROFILE" \
  --revision "$REVISION" \
  --samples "$SAMPLES" \
  --node-cache-max-bytes "$NODE_CACHE_MAX_BYTES" \
  --expected-cases "$CASE_SPECIFICATIONS"

printf 'Versioned DynamoDB client matrix complete: %s\n' "$OUTPUT"
