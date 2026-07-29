#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
DRIVER="$SCRIPT_DIR/run_mysql_postgres_comparison.sh"
SUMMARIZER="$SCRIPT_DIR/summarize_mysql_postgres_service_matrix.py"
OUTPUT="${BENCH_OUT:-$REPO_ROOT/performance-results/mysql-postgres-service-$(date -u +%Y%m%dT%H%M%SZ)}"
CLIENTS="${BENCH_CLIENTS:-1,8,32}"
POOL_SIZES="${BENCH_POOL_SIZES:-4,16}"
RECORDS="${BENCH_RECORDS:-100000}"
CHANGES="${BENCH_CHANGES:-1000}"
SAMPLES="${BENCH_SAMPLES:-10000}"

[[ ! -e "$OUTPUT" ]] || {
  printf 'refusing to overwrite service matrix output: %s\n' "$OUTPUT" >&2
  exit 2
}
mkdir -p "$OUTPUT/cells"

IFS=',' read -r -a client_values <<< "$CLIENTS"
IFS=',' read -r -a pool_values <<< "$POOL_SIZES"
first=true
for clients in "${client_values[@]}"; do
  for pool_size in "${pool_values[@]}"; do
    [[ "$clients" =~ ^[1-9][0-9]*$ && "$pool_size" =~ ^[1-9][0-9]*$ ]] || {
      printf 'BENCH_CLIENTS and BENCH_POOL_SIZES must contain positive integers\n' >&2
      exit 2
    }
    cell="$OUTPUT/cells/clients-${clients}-pool-${pool_size}"
    if [[ "$first" == true ]]; then
      skip_build="${BENCH_SKIP_BUILD:-0}"
      first=false
    else
      skip_build=1
    fi
    BENCH_OUT="$cell" \
    BENCH_RECORDS="$RECORDS" \
    BENCH_CHANGES="$CHANGES" \
    BENCH_SAMPLES="$SAMPLES" \
    BENCH_CONCURRENCY="$clients" \
    BENCH_POOL_SIZE="$pool_size" \
    BENCH_SKIP_BUILD="$skip_build" \
      "$DRIVER"
  done
done

python3 "$SUMMARIZER" --input "$OUTPUT/cells" --output "$OUTPUT"
printf 'MySQL/PostgreSQL service matrix complete: %s/report.md\n' "$OUTPUT"
