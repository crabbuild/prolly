#!/bin/sh
set -eu

ROOT=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
OUT=${BENCH_OUT:-"$ROOT/performance-results/dolt-current-rust-canonical-2026-07-17"}
SIZES=${BENCH_SIZES:-"10000 50000 1000000 5000000 10000000"}
RUNS=${BENCH_RUNS:-3}
LARGE_RUNS=${BENCH_LARGE_RUNS:-3}
DOLT_REPO_URL=${DOLT_REPO_URL:-"https://github.com/dolthub/dolt.git"}
DOLT_CACHE=${DOLT_CACHE:-"$ROOT/target/dolt-benchmark"}
DOLT_REQUESTED_REV=${DOLT_REV:-}
TIME_BIN=${BENCH_TIME_BIN:-/usr/bin/time}
HISTORY_SUMMARY=${BENCH_HISTORY_SUMMARY-"$ROOT/performance-results/zero-copy-final-rerun-2026-07-16/summary.csv"}
RUNNER_SOURCE="$ROOT/benchmarks/dolt-prolly-compare"
RUNNER_HEADER='implementation,revision,contract_version,records,phase,workload,operation,operations,elapsed_ns,ns_per_op,ops_per_sec,workload_digest,result_count,validated'
RESULT_HEADER="$RUNNER_HEADER,repetition,peak_rss_bytes"

case "$OUT" in
    /*) ;;
    *) OUT="$ROOT/$OUT" ;;
esac
case "$DOLT_CACHE" in
    /*) ;;
    *) DOLT_CACHE="$ROOT/$DOLT_CACHE" ;;
esac

if [ "$RUNS" -le 0 ] || [ "$LARGE_RUNS" -le 0 ]; then
    printf 'benchmark repetitions must be positive\n' >&2
    exit 2
fi
if [ "$LARGE_RUNS" -ne "$RUNS" ]; then
    printf 'BENCH_LARGE_RUNS must equal BENCH_RUNS; every requested size requires the same repetition count\n' >&2
    exit 2
fi
if [ -e "$OUT/results.csv" ]; then
    printf 'benchmark output already contains results: %s\n' "$OUT/results.csv" >&2
    exit 2
fi
if [ ! -f "$RUNNER_SOURCE/main.go" ] || [ ! -f "$RUNNER_SOURCE/main_test.go" ]; then
    printf 'checked-in Dolt runner is incomplete: %s\n' "$RUNNER_SOURCE" >&2
    exit 2
fi

mkdir -p "$OUT/bin" "$OUT/raw" "$OUT/smoke"

sha256_file() {
    shasum -a 256 "$1" | awk '{ print $1 }'
}

source_tree_hash() {
    find "$ROOT/src" "$ROOT/benches" "$ROOT/Cargo.toml" -type f |
        LC_ALL=C sort |
        while IFS= read -r source_file; do
            shasum -a 256 "$source_file"
        done |
        shasum -a 256 |
        awk '{ print $1 }'
}

if [ ! -d "$DOLT_CACHE/.git" ]; then
    if [ -e "$DOLT_CACHE" ]; then
        printf 'DOLT_CACHE exists but is not a git checkout: %s\n' "$DOLT_CACHE" >&2
        exit 2
    fi
    mkdir -p "$(dirname -- "$DOLT_CACHE")"
    git clone --filter=blob:none --no-checkout "$DOLT_REPO_URL" "$DOLT_CACHE"
fi

git -C "$DOLT_CACHE" fetch --prune origin main
if [ -n "$DOLT_REQUESTED_REV" ]; then
    if ! git -C "$DOLT_CACHE" rev-parse --verify "${DOLT_REQUESTED_REV}^{commit}" >/dev/null 2>&1; then
        git -C "$DOLT_CACHE" fetch origin "$DOLT_REQUESTED_REV"
    fi
    DOLT_SHA=$(git -C "$DOLT_CACHE" rev-parse --verify "${DOLT_REQUESTED_REV}^{commit}")
else
    DOLT_SHA=$(git -C "$DOLT_CACHE" rev-parse origin/main)
fi
git -C "$DOLT_CACHE" checkout --detach "$DOLT_SHA"

mkdir -p "$DOLT_CACHE/go/cmd/prolly-compare"
cp "$RUNNER_SOURCE/main.go" "$DOLT_CACHE/go/cmd/prolly-compare/main.go"
cp "$RUNNER_SOURCE/main_test.go" "$DOLT_CACHE/go/cmd/prolly-compare/main_test.go"

RUST_COMMIT=$(git -C "$ROOT" rev-parse HEAD 2>/dev/null || printf unknown)
RUST_SOURCE_HASH=$(source_tree_hash)
DOLT_RUNNER_HASH=$(sha256_file "$RUNNER_SOURCE/main.go")
RUST_REV="$(printf '%s' "$RUST_COMMIT" | cut -c1-12)+src.$(printf '%s' "$RUST_SOURCE_HASH" | cut -c1-12)"
DOLT_BENCH_REV="$(printf '%s' "$DOLT_SHA" | cut -c1-12)+runner.$(printf '%s' "$DOLT_RUNNER_HASH" | cut -c1-12)"

RUST_TARGET=$(cargo metadata --manifest-path "$ROOT/Cargo.toml" --no-deps --format-version 1 |
    sed -n 's/.*"target_directory":"\([^"]*\)".*/\1/p')
if [ -z "$RUST_TARGET" ]; then
    printf 'cargo metadata did not return a target directory\n' >&2
    exit 2
fi
cargo build --manifest-path "$ROOT/Cargo.toml" --release --bin prolly_compare
cp "$RUST_TARGET/release/prolly_compare" "$OUT/bin/rust-prolly-compare"

(
    cd "$DOLT_CACHE/go"
    go test ./cmd/prolly-compare
    go build -trimpath -o "$OUT/bin/dolt-go-prolly-compare" ./cmd/prolly-compare
)

RUST_BINARY_HASH=$(sha256_file "$OUT/bin/rust-prolly-compare")
DOLT_BINARY_HASH=$(sha256_file "$OUT/bin/dolt-go-prolly-compare")

{
    printf 'generated_utc=%s\n' "$(date -u +%Y-%m-%dT%H:%M:%SZ)"
    printf 'rust_commit=%s\n' "$RUST_COMMIT"
    printf 'rust_source_sha256=%s\n' "$RUST_SOURCE_HASH"
    printf 'rust_revision=%s\n' "$RUST_REV"
    printf 'dolt_repository=%s\n' "$DOLT_REPO_URL"
    printf 'dolt_commit=%s\n' "$DOLT_SHA"
    printf 'dolt_runner_sha256=%s\n' "$DOLT_RUNNER_HASH"
    printf 'dolt_revision=%s\n' "$DOLT_BENCH_REV"
    printf 'rust_binary_sha256=%s\n' "$RUST_BINARY_HASH"
    printf 'dolt_binary_sha256=%s\n' "$DOLT_BINARY_HASH"
    printf 'sizes=%s\n' "$SIZES"
    printf 'runs=%s\n' "$RUNS"
    printf 'worker_threads=1\n'
    printf 'contract_version=prolly-compare-v1\n'
    printf 'mutation_ratio=30%%\n'
    printf 'mutation_insert_update_mix=50/50\n'
    printf 'point_read_cap=100000\n'
} >"$OUT/manifest.txt"

{
    printf 'generated_utc=%s\n' "$(date -u +%Y-%m-%dT%H:%M:%SZ)"
    printf 'host=%s\n' "$(hostname)"
    printf 'os=%s\n' "$(uname -a)"
    if command -v sysctl >/dev/null 2>&1; then
        printf 'cpu=%s\n' "$(sysctl -n machdep.cpu.brand_string 2>/dev/null || true)"
        printf 'memory_bytes=%s\n' "$(sysctl -n hw.memsize 2>/dev/null || true)"
    elif [ -r /proc/cpuinfo ]; then
        printf 'cpu=%s\n' "$(sed -n 's/^model name[[:space:]]*: //p' /proc/cpuinfo | sed -n '1p')"
        printf 'memory=%s\n' "$(sed -n 's/^MemTotal:[[:space:]]*//p' /proc/meminfo)"
    fi
    rustc -Vv
    go version
} >"$OUT/machine.txt"

validate_runner_csv() {
    csv_path=$1
    expected_implementation=$2
    python3 - "$csv_path" "$expected_implementation" "$RUNNER_HEADER" <<'PY'
import csv
import sys

path, implementation, expected_header = sys.argv[1:]
with open(path, newline="", encoding="utf-8") as handle:
    header = handle.readline().rstrip("\r\n")
    handle.seek(0)
    rows = list(csv.DictReader(handle))
if header != expected_header:
    raise SystemExit(f"header mismatch: {header!r}")
if len(rows) != 3:
    raise SystemExit(f"expected 3 data rows, found {len(rows)}")
if [row.get("implementation") for row in rows] != [implementation] * 3:
    raise SystemExit("implementation mismatch")
if [row.get("operation") for row in rows] != ["write", "point_read", "range_scan"]:
    raise SystemExit("operation sequence mismatch")
if any(row.get("contract_version") != "prolly-compare-v1" for row in rows):
    raise SystemExit("contract version mismatch")
if any(row.get("validated") != "true" for row in rows):
    raise SystemExit("runner emitted an unvalidated row")
PY
}

append_normalized_rows() {
    csv_path=$1
    repetition=$2
    peak_rss=$3
    awk -v repetition="$repetition" -v peak_rss="$peak_rss" \
        'NR > 1 { print $0 "," repetition "," peak_rss }' "$csv_path"
}

run_smoke_one() {
    implementation=$1
    if [ "$implementation" = rust ]; then
        smoke_binary="$OUT/bin/rust-prolly-compare"
        smoke_revision=$RUST_REV
    else
        smoke_binary="$OUT/bin/dolt-go-prolly-compare"
        smoke_revision=$DOLT_BENCH_REV
    fi
    smoke_stdout="$OUT/smoke/${implementation}.csv"
    smoke_stderr="$OUT/smoke/${implementation}.stderr"
    set +e
    RAYON_NUM_THREADS=1 GOMAXPROCS=1 BENCH_REVISION="$smoke_revision" \
        "$smoke_binary" --records 10000 --phase fresh --workload random \
        >"$smoke_stdout" 2>"$smoke_stderr"
    smoke_exit=$?
    set -e
    if [ "$smoke_exit" -ne 0 ]; then
        printf 'smoke benchmark failed: %s; see %s\n' "$implementation" "$smoke_stderr" >&2
        return "$smoke_exit"
    fi
    if ! validate_runner_csv "$smoke_stdout" "$implementation"; then
        printf 'malformed runner CSV during smoke: %s\n' "$smoke_stdout" >&2
        return 65
    fi
    append_normalized_rows "$smoke_stdout" 1 1 >>"$OUT/smoke/results.csv"
}

printf '%s,repetition,peak_rss_bytes\n' "$RUNNER_HEADER" >"$OUT/smoke/results.csv"
run_smoke_one rust
run_smoke_one dolt-go
python3 "$ROOT/scripts/summarize_prolly_comparison.py" \
    --input "$OUT/smoke/results.csv" \
    --output-dir "$OUT/smoke" \
    --expected-runs 1 \
    --expected-sizes 10000 \
    --allow-partial

if "$TIME_BIN" -l -o "$OUT/time-probe.txt" /usr/bin/true >/dev/null 2>"$OUT/time-probe.stderr"; then
    TIME_MODE=-l
else
    TIME_MODE=-v
    "$TIME_BIN" -v -o "$OUT/time-probe.txt" /usr/bin/true >/dev/null 2>"$OUT/time-probe.stderr"
fi

printf '%s\n' "$RESULT_HEADER" >"$OUT/results.csv"
printf 'repetition,implementation,records,phase,workload,exit_status,peak_rss_bytes,stdout,stderr,time\n' >"$OUT/manifest.csv"

run_one() {
    implementation=$1
    records=$2
    selected_phase=$3
    selected_workload=$4
    repetition=$5
    prefix="$OUT/raw/${implementation}-${records}-${selected_phase}-${selected_workload}-run${repetition}"

    if [ "$implementation" = rust ]; then
        binary="$OUT/bin/rust-prolly-compare"
        revision=$RUST_REV
    else
        binary="$OUT/bin/dolt-go-prolly-compare"
        revision=$DOLT_BENCH_REV
    fi

    printf 'running repetition=%s implementation=%s records=%s phase=%s workload=%s\n' \
        "$repetition" "$implementation" "$records" "$selected_phase" "$selected_workload" >&2
    set +e
    RAYON_NUM_THREADS=1 GOMAXPROCS=1 BENCH_REVISION="$revision" \
        "$TIME_BIN" "$TIME_MODE" -o "$prefix.time" \
        "$binary" --records "$records" --phase "$selected_phase" --workload "$selected_workload" \
        >"$prefix.csv" 2>"$prefix.stderr"
    process_exit=$?
    set -e

    peak_rss=$(python3 "$ROOT/scripts/prolly_process_metrics.py" "$prefix.time" 2>>"$prefix.stderr") || peak_rss=
    printf '%s,%s,%s,%s,%s,%s,%s,%s,%s,%s\n' \
        "$repetition" "$implementation" "$records" "$selected_phase" "$selected_workload" \
        "$process_exit" "$peak_rss" "$prefix.csv" "$prefix.stderr" "$prefix.time" \
        >>"$OUT/manifest.csv"
    if [ "$process_exit" -ne 0 ]; then
        printf 'benchmark failed: %s %s %s %s repetition %s; see %s\n' \
            "$implementation" "$records" "$selected_phase" "$selected_workload" \
            "$repetition" "$prefix.stderr" >&2
        return "$process_exit"
    fi
    if [ -z "$peak_rss" ]; then
        printf 'benchmark failed: peak RSS missing for %s\n' "$prefix.time" >&2
        return 66
    fi
    if ! validate_runner_csv "$prefix.csv" "$implementation"; then
        printf 'malformed runner CSV: %s\n' "$prefix.csv" >&2
        return 65
    fi
    append_normalized_rows "$prefix.csv" "$repetition" "$peak_rss" >>"$OUT/results.csv"
}

for records in $SIZES; do
    repetition=1
    while [ "$repetition" -le "$RUNS" ]; do
        for selected_phase in fresh mutation; do
            for selected_workload in append random clustered; do
                if [ $(((repetition + records + ${#selected_phase} + ${#selected_workload}) % 2)) -eq 0 ]; then
                    run_one rust "$records" "$selected_phase" "$selected_workload" "$repetition"
                    run_one dolt-go "$records" "$selected_phase" "$selected_workload" "$repetition"
                else
                    run_one dolt-go "$records" "$selected_phase" "$selected_workload" "$repetition"
                    run_one rust "$records" "$selected_phase" "$selected_workload" "$repetition"
                fi
            done
        done
        repetition=$((repetition + 1))
    done
done

EXPECTED_SIZES=
for records in $SIZES; do
    if [ -z "$EXPECTED_SIZES" ]; then
        EXPECTED_SIZES=$records
    else
        EXPECTED_SIZES="$EXPECTED_SIZES,$records"
    fi
done

set -- python3 "$ROOT/scripts/summarize_prolly_comparison.py" \
    --input "$OUT/results.csv" \
    --output-dir "$OUT" \
    --expected-runs "$RUNS" \
    --expected-sizes "$EXPECTED_SIZES"
if [ -n "$HISTORY_SUMMARY" ]; then
    set -- "$@" --history-summary "$HISTORY_SUMMARY"
fi
"$@"
printf 'comparison complete: %s/report.md\n' "$OUT"
