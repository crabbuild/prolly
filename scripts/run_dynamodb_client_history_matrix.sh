#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
PROFILE="${BENCH_HISTORY_MATRIX_PROFILE:-smoke}"
OUTPUT="${BENCH_HISTORY_MATRIX_OUT:-$REPO_ROOT/performance-results/dynamodb-client-history-$(date +%F)}"
PRINT_CONFIG=false
NODE_CACHE_MAX_BYTES="${BENCH_NODE_CACHE_MAX_BYTES:-67108864}"

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
    HISTORY_DEPTHS="${BENCH_HISTORY_DEPTHS:-10,100,1000}"
    ;;
  qualification)
    SAMPLES="${BENCH_SAMPLES:-10}"
    HISTORY_DEPTHS="10,100,1000,10000,100000"
    if [[ -n "${BENCH_HISTORY_DEPTHS:-}" ]]; then
      printf 'qualification history depths are fixed and cannot be overridden\n' >&2
      exit 2
    fi
    ;;
  *)
    printf 'profile must be smoke or qualification\n' >&2
    exit 2
    ;;
esac

if ! [[ "$SAMPLES" =~ ^[1-9][0-9]*$ ]] || ! [[ "$NODE_CACHE_MAX_BYTES" =~ ^[0-9]+$ ]]; then
  printf 'BENCH_SAMPLES must be positive and BENCH_NODE_CACHE_MAX_BYTES must be non-negative\n' >&2
  exit 2
fi
if ! [[ "$HISTORY_DEPTHS" =~ ^[1-9][0-9]*(,[1-9][0-9]*)*$ ]]; then
  printf 'history depths must be comma-separated positive integers\n' >&2
  exit 2
fi

IFS=, read -r -a DEPTHS <<< "$HISTORY_DEPTHS"
previous=0
for depth in "${DEPTHS[@]}"; do
  if ((depth < 10 || depth > 100000 || depth <= previous)); then
    printf 'history depths must be strictly increasing values in 10..=100000\n' >&2
    exit 2
  fi
  previous="$depth"
done

if [[ "$PRINT_CONFIG" == true ]]; then
  printf 'profile=%s history_depths=%s samples=%s node_cache_max_bytes=%s output=%s\n' \
    "$PROFILE" "$HISTORY_DEPTHS" "$SAMPLES" "$NODE_CACHE_MAX_BYTES" "$OUTPUT"
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
MATRIX_ROWS="$OUTPUT/history-matrix.csv.tmp"
RECORDS="${BENCH_RECORDS:-100}"
VALUE_BYTES="${BENCH_VALUE_BYTES:-1024}"
READ_BATCH_ITEMS="${BENCH_READ_BATCH_ITEMS:-100}"
TRANSACTION_SHAPES="${BENCH_TRANSACTION_SHAPES:-1,10}"
CONCURRENCY_WRITERS="${BENCH_CONCURRENCY_WRITERS:-1}"
CONCURRENCY_OPERATIONS_PER_WRITER="${BENCH_CONCURRENCY_OPERATIONS_PER_WRITER:-1}"
CONCURRENCY_RETRY_LIMIT="${BENCH_CONCURRENCY_RETRY_LIMIT:-7}"
COMPLETED_MANIFEST="$OUTPUT/history-matrix-manifest.txt"
COMPLETED_UTC=""

if [[ -e "$COMPLETED_MANIFEST" ]]; then
  if [[ "$(wc -l < "$COMPLETED_MANIFEST")" -ne 8 ]] \
    || ! rg -Fxq -- 'schema=dynamodb-client-history-matrix-v2' "$COMPLETED_MANIFEST" \
    || ! rg -Fxq -- "profile=$PROFILE" "$COMPLETED_MANIFEST" \
    || ! rg -Fxq -- "revision=$REVISION" "$COMPLETED_MANIFEST" \
    || ! rg -Fxq -- "history_depths=$HISTORY_DEPTHS" "$COMPLETED_MANIFEST" \
    || ! rg -Fxq -- "cases=${#DEPTHS[@]}" "$COMPLETED_MANIFEST" \
    || ! rg -Fxq -- "samples_per_case=$SAMPLES" "$COMPLETED_MANIFEST" \
    || ! rg -Fxq -- "node_cache_max_bytes=$NODE_CACHE_MAX_BYTES" "$COMPLETED_MANIFEST"; then
    printf 'completed history matrix manifest does not match this exact run\n' >&2
    exit 1
  fi
  COMPLETED_UTC="$(sed -n 's/^completed_utc=//p' "$COMPLETED_MANIFEST")"
  if ! [[ "$COMPLETED_UTC" =~ ^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}Z$ ]]; then
    printf 'completed history matrix manifest has an invalid completion timestamp\n' >&2
    exit 1
  fi
fi

trap 'rm -f "$MATRIX_ROWS"' EXIT

printf 'schema,matrix_profile,history_depth,samples,records,value_bytes,read_batch_items,transaction_shapes,concurrency_writers,concurrency_operations_per_writer,concurrency_retry_limit,node_cache_max_bytes,revision,result_dir,status\n' \
  > "$MATRIX_ROWS"

for depth in "${DEPTHS[@]}"; do
  case_name="history-${depth}"
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
      --records "$RECORDS" \
      --value-bytes "$VALUE_BYTES" \
      --read-batch-items "$READ_BATCH_ITEMS" \
      --history-depth "$depth" \
      --workload history \
      --concurrency-writers "$CONCURRENCY_WRITERS" \
      --concurrency-operations-per-writer "$CONCURRENCY_OPERATIONS_PER_WRITER" \
      --concurrency-retry-limit "$CONCURRENCY_RETRY_LIMIT" \
      --revision "$REVISION" \
      --transaction-shapes "$TRANSACTION_SHAPES" \
      --node-cache-max-bytes "$NODE_CACHE_MAX_BYTES"
    python3 "$SCRIPT_DIR/summarize_dynamodb_client_benchmark.py" \
      --input "$case_output/raw-samples.csv" --output-dir "$case_output"
  else
    BENCH_RECORDS="$RECORDS" \
    BENCH_VALUE_BYTES="$VALUE_BYTES" \
    BENCH_READ_BATCH_ITEMS="$READ_BATCH_ITEMS" \
    BENCH_HISTORY_DEPTH="$depth" \
    BENCH_WORKLOAD=history \
    BENCH_CONCURRENCY_WRITERS="$CONCURRENCY_WRITERS" \
    BENCH_CONCURRENCY_OPERATIONS_PER_WRITER="$CONCURRENCY_OPERATIONS_PER_WRITER" \
    BENCH_CONCURRENCY_RETRY_LIMIT="$CONCURRENCY_RETRY_LIMIT" \
    BENCH_NODE_CACHE_MAX_BYTES="$NODE_CACHE_MAX_BYTES" \
    BENCH_SAMPLES="$SAMPLES" \
    BENCH_TRANSACTION_SHAPES="$TRANSACTION_SHAPES" \
    BENCH_CLEANUP=1 \
      "$SCRIPT_DIR/run_dynamodb_client_benchmark.sh" \
        --profile repeatable \
        --output "$case_output" \
        --table "prolly_client_history_${depth}"
  fi
  printf 'dynamodb-client-history-matrix-v2,%s,%s,%s,%s,%s,%s,"%s","%s",%s,%s,%s,%s,%s,validated\n' \
    "$PROFILE" "$depth" "$SAMPLES" "$RECORDS" "$VALUE_BYTES" "$READ_BATCH_ITEMS" \
    "$TRANSACTION_SHAPES" "$CONCURRENCY_WRITERS" "$CONCURRENCY_OPERATIONS_PER_WRITER" \
    "$CONCURRENCY_RETRY_LIMIT" "$NODE_CACHE_MAX_BYTES" "$REVISION" "$case_name" >> "$MATRIX_ROWS"
done

if [[ -e "$OUTPUT/history-matrix.csv" ]]; then
  if ! cmp -s "$MATRIX_ROWS" "$OUTPUT/history-matrix.csv"; then
    printf 'completed history matrix CSV differs from revalidated cases\n' >&2
    exit 1
  fi
  rm -f "$MATRIX_ROWS"
else
  mv "$MATRIX_ROWS" "$OUTPUT/history-matrix.csv"
fi

if [[ -z "$COMPLETED_UTC" ]]; then
  {
    printf 'schema=dynamodb-client-history-matrix-v2\n'
    printf 'profile=%s\n' "$PROFILE"
    printf 'revision=%s\n' "$REVISION"
    printf 'history_depths=%s\n' "$HISTORY_DEPTHS"
    printf 'cases=%s\n' "${#DEPTHS[@]}"
    printf 'samples_per_case=%s\n' "$SAMPLES"
    printf 'node_cache_max_bytes=%s\n' "$NODE_CACHE_MAX_BYTES"
    printf 'completed_utc=%s\n' "$(date -u +%Y-%m-%dT%H:%M:%SZ)"
  } > "$COMPLETED_MANIFEST"
fi

python3 "$SCRIPT_DIR/validate_dynamodb_client_matrix.py" \
  --kind history \
  --input "$OUTPUT/history-matrix.csv" \
  --manifest "$COMPLETED_MANIFEST" \
  --profile "$PROFILE" \
  --revision "$REVISION" \
  --samples "$SAMPLES" \
  --node-cache-max-bytes "$NODE_CACHE_MAX_BYTES" \
  --expected-cases "$HISTORY_DEPTHS" \
  --expected-history-shape "$RECORDS:$VALUE_BYTES:$READ_BATCH_ITEMS:$TRANSACTION_SHAPES:$CONCURRENCY_WRITERS:$CONCURRENCY_OPERATIONS_PER_WRITER:$CONCURRENCY_RETRY_LIMIT"

printf 'Versioned DynamoDB client history matrix complete: %s\n' "$OUTPUT"
