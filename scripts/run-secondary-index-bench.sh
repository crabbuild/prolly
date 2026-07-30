#!/usr/bin/env bash
set -euo pipefail

repository_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
samples="${PROLLY_INDEX_BENCH_SAMPLES:-7}"
scale="${PROLLY_INDEX_BENCH_SCALE:-10000}"
batch="${PROLLY_INDEX_BENCH_BATCH:-256}"
output="${1:-$repository_root/target/secondary-index-bench.csv}"

if (( samples < 5 )); then
  echo "PROLLY_INDEX_BENCH_SAMPLES must be at least 5" >&2
  exit 2
fi

mkdir -p "$(dirname "$output")"
temporary="$(mktemp)"
trap 'unlink "$temporary" 2>/dev/null || true' EXIT
printf 'sample,operation,scale,work_items,total_ms,items_per_sec,verified\n' >"$output"

for sample in $(seq 1 "$samples"); do
  (
    cd "$repository_root"
    PROLLY_INDEX_BENCH_SCALE="$scale" \
      PROLLY_INDEX_BENCH_BATCH="$batch" \
      cargo bench --bench prolly_secondary_index_bench --quiet
  ) >"$temporary"
  awk -F, -v sample="$sample" 'NR > 1 { print sample "," $0 }' "$temporary" >>"$output"
done

"$repository_root/scripts/summarize-secondary-index-bench.sh" "$output"
