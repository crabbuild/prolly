#!/usr/bin/env bash
set -euo pipefail

ROOT=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
PROFILE=${BENCH_PROFILE:-full}
PYTHON_BIN=${PYTHON_BIN:-python3}
TIME_BIN=${BENCH_TIME_BIN:-/usr/bin/time}
OUT=${BENCH_OUT:-"$ROOT/performance-results/dolt-rust-sqlite-$(date -u +%Y%m%dT%H%M%SZ)"}
DOLT_REPO_URL=${DOLT_REPO_URL:-https://github.com/dolthub/dolt.git}
DOLT_CACHE=${DOLT_CACHE:-"$ROOT/target/dolt-sqlite-benchmark"}
SQLITE_DRIVER_VERSION=1.14.7
OPERATIONS=${BENCH_OPERATIONS:-put,batch,get_cold,get_warm,query,scan,full_scan,diff,merge}
PATTERNS=${BENCH_PATTERNS:-append,random,clustered}

case "$OUT" in /*) ;; *) OUT="$ROOT/$OUT" ;; esac
case "$DOLT_CACHE" in /*) ;; *) DOLT_CACHE="$ROOT/$DOLT_CACHE" ;; esac

case "$PROFILE" in
    smoke)
        SIZES=${BENCH_SIZES:-100}
        RUNS=${BENCH_RUNS:-1}
        READ_SAMPLES=${BENCH_READ_SAMPLES:-10}
        CHANGES_OVERRIDE=${BENCH_CHANGES:-10}
        ;;
    full)
        SIZES=${BENCH_SIZES:-1000000}
        RUNS=${BENCH_RUNS:-3}
        READ_SAMPLES=${BENCH_READ_SAMPLES:-10000}
        CHANGES_OVERRIDE=${BENCH_CHANGES:-}
        ;;
    *) printf 'unknown BENCH_PROFILE: %s\n' "$PROFILE" >&2; exit 2 ;;
esac

if [ -e "$OUT" ]; then
    printf 'refusing to overwrite comparison output: %s\n' "$OUT" >&2
    exit 2
fi
mkdir -p "$OUT/bin" "$OUT/raw" "$OUT/work"
printf 'running\n' >"$OUT/run-status.txt"
trap 'status=$?; if [ "$status" -ne 0 ]; then printf "failed\n" >"$OUT/run-status.txt"; fi' EXIT

if "$TIME_BIN" -l -o "$OUT/time-probe.txt" /usr/bin/true >/dev/null 2>"$OUT/time-probe.stderr"; then
    TIME_ARGS=(-l)
else
    TIME_ARGS=(-v)
    "$TIME_BIN" -v -o "$OUT/time-probe.txt" /usr/bin/true >/dev/null 2>"$OUT/time-probe.stderr"
fi

sha256_file() { shasum -a 256 "$1" | awk '{print $1}'; }
source_hash() {
    find "$1" -type f | LC_ALL=C sort | while IFS= read -r file; do shasum -a 256 "$file"; done | shasum -a 256 | awk '{print $1}'
}

if [ "${DOLT_SQLITE_SKIP_BUILD:-0}" = 1 ]; then
    GO_BIN=${DOLT_SQLITE_GO_BIN:?DOLT_SQLITE_GO_BIN is required when skipping builds}
    RUST_BIN=${DOLT_SQLITE_RUST_BIN:?DOLT_SQLITE_RUST_BIN is required when skipping builds}
    DOLT_SHA=${DOLT_SQLITE_DOLT_REVISION:-unknown}
    RUST_REVISION=${DOLT_SQLITE_RUST_REVISION:-unknown}
    GO_SOURCE_HASH=$(source_hash "$ROOT/benchmarks/dolt-prolly-sqlite-compare")
else
    if [ ! -d "$DOLT_CACHE/.git" ]; then
        if [ -e "$DOLT_CACHE" ]; then
            printf 'DOLT_CACHE is not a git checkout: %s\n' "$DOLT_CACHE" >&2
            exit 2
        fi
        mkdir -p "$(dirname -- "$DOLT_CACHE")"
        git clone --filter=blob:none --no-checkout "$DOLT_REPO_URL" "$DOLT_CACHE"
    fi
    if [ "${DOLT_SQLITE_SKIP_FETCH:-0}" != 1 ]; then
        git -C "$DOLT_CACHE" fetch --prune origin main
        if [ -n "${DOLT_REV:-}" ] && ! git -C "$DOLT_CACHE" rev-parse --verify "${DOLT_REV}^{commit}" >/dev/null 2>&1; then
            git -C "$DOLT_CACHE" fetch origin "$DOLT_REV"
        fi
        DOLT_SHA=$(git -C "$DOLT_CACHE" rev-parse "${DOLT_REV:-origin/main}^{commit}")
        git -C "$DOLT_CACHE" checkout --detach "$DOLT_SHA"
    else
        DOLT_SHA=$(git -C "$DOLT_CACHE" rev-parse "${DOLT_REV:-HEAD}^{commit}")
    fi
    RUNNER_DEST="$DOLT_CACHE/go/cmd/prolly-sqlite-compare"
    if [ -e "$RUNNER_DEST" ]; then rm -rf "$RUNNER_DEST"; fi
    mkdir -p "$RUNNER_DEST"
    cp "$ROOT"/benchmarks/dolt-prolly-sqlite-compare/*.go "$RUNNER_DEST/"
    GO_SOURCE_HASH=$(source_hash "$ROOT/benchmarks/dolt-prolly-sqlite-compare")
    cp "$DOLT_CACHE/go/go.mod" "$OUT/dolt-benchmark.mod"
    cp "$DOLT_CACHE/go/go.sum" "$OUT/dolt-benchmark.sum"
    (
        cd "$DOLT_CACHE/go"
        go mod edit -modfile="$OUT/dolt-benchmark.mod" -require="github.com/mattn/go-sqlite3@v$SQLITE_DRIVER_VERSION"
        go test -mod=mod -modfile="$OUT/dolt-benchmark.mod" ./cmd/prolly-sqlite-compare
        go build -mod=mod -modfile="$OUT/dolt-benchmark.mod" -trimpath -o "$OUT/bin/dolt-go-prolly-sqlite" ./cmd/prolly-sqlite-compare
    ) >"$OUT/go-build.log" 2>&1
    GO_BIN="$OUT/bin/dolt-go-prolly-sqlite"
    cargo test --manifest-path "$ROOT/benchmarks/sqlite-scale/Cargo.toml" >"$OUT/rust-test.log" 2>&1
    cargo build --release --manifest-path "$ROOT/benchmarks/sqlite-scale/Cargo.toml" --bin prolly-sqlite-cell-runner >"$OUT/rust-build.log" 2>&1
    RUST_TARGET=$(
        cargo metadata --manifest-path "$ROOT/benchmarks/sqlite-scale/Cargo.toml" --no-deps --format-version 1 |
            "$PYTHON_BIN" -c 'import json,sys; print(json.load(sys.stdin)["target_directory"])'
    )
    cp "$RUST_TARGET/release/prolly-sqlite-cell-runner" "$OUT/bin/rust-prolly-sqlite"
    RUST_BIN="$OUT/bin/rust-prolly-sqlite"
    RUST_COMMIT=$(git -C "$ROOT" rev-parse HEAD 2>/dev/null || printf unknown)
    RUST_SOURCE_HASH=$(source_hash "$ROOT/benchmarks/sqlite-scale")
    RUST_REVISION="$(printf '%s' "$RUST_COMMIT" | cut -c1-12)+src.$(printf '%s' "$RUST_SOURCE_HASH" | cut -c1-12)"
fi

GO_REVISION="$(printf '%s' "$DOLT_SHA" | cut -c1-12)+runner.$(printf '%s' "$GO_SOURCE_HASH" | cut -c1-12)"
GO_BINARY_HASH=$(sha256_file "$GO_BIN")
RUST_BINARY_HASH=$(sha256_file "$RUST_BIN")

{
    printf 'started_utc=%s\n' "$(date -u +%Y-%m-%dT%H:%M:%SZ)"
    printf 'profile=%s\nsizes=%s\nruns=%s\noperations=%s\npatterns=%s\nread_samples=%s\nchanges=%s\n' "$PROFILE" "$SIZES" "$RUNS" "$OPERATIONS" "$PATTERNS" "$READ_SAMPLES" "${CHANGES_OVERRIDE:-auto-30-percent}"
    printf 'contract_version=sqlite-scale-v2\nsqlite_driver=github.com/mattn/go-sqlite3@v%s\n' "$SQLITE_DRIVER_VERSION"
    printf 'sqlite_journal=WAL\nsqlite_synchronous=NORMAL\nsqlite_busy_timeout_ms=5000\nsqlite_temp_store=MEMORY\n'
    printf 'dolt_commit=%s\ndolt_runner_sha256=%s\ndolt_binary_sha256=%s\n' "$DOLT_SHA" "$GO_SOURCE_HASH" "$GO_BINARY_HASH"
    printf 'rust_revision=%s\nrust_binary_sha256=%s\nworker_threads=1\n' "$RUST_REVISION" "$RUST_BINARY_HASH"
} >"$OUT/manifest.txt"
{
    printf 'host=%s\nos=%s\n' "$(hostname)" "$(uname -a)"
    command -v sysctl >/dev/null && printf 'cpu=%s\nmemory_bytes=%s\n' "$(sysctl -n machdep.cpu.brand_string 2>/dev/null || true)" "$(sysctl -n hw.memsize 2>/dev/null || true)"
    go version
    rustc -Vv
    command -v sqlite3 >/dev/null && sqlite3 --version || true
} >"$OUT/machine.txt"

printf 'implementation,kind,records,repetition,operation,pattern,cache_state,exit_status,validation,peak_rss_bytes,json,stderr,time\n' >"$OUT/process-manifest.csv"
: >"$OUT/raw-results.jsonl"

run_one() {
    local implementation=$1 kind=$2 records=$3 repetition=$4 operation=$5 selected_pattern=$6 cache_state=$7 binary=$8 revision=$9 output_root=${10}
    shift 10
    local safe_pattern=${selected_pattern//\//-}
    local slug="$implementation-$kind-$records-r$repetition-$operation-$safe_pattern"
    local prefix="$OUT/raw/$slug"
    set +e
    GOMAXPROCS=1 RAYON_NUM_THREADS=1 "$TIME_BIN" "${TIME_ARGS[@]}" -o "$prefix.time" "$binary" "$kind" --output "$output_root" --records "$records" --repetition "$repetition" --revision "$revision" "$@" >"$prefix.json" 2>"$prefix.stderr"
    local status=$?
    set -e
    local peak_rss validation=failed
    peak_rss=$("$PYTHON_BIN" "$ROOT/scripts/prolly_process_metrics.py" "$prefix.time" 2>>"$prefix.stderr") || peak_rss=
    if "$PYTHON_BIN" -c 'import json,sys; r=json.load(open(sys.argv[1])); assert r["validated"] is True' "$prefix.json" >/dev/null 2>&1 && [ "$status" -eq 0 ] && [ -n "$peak_rss" ]; then
        validation=ok
    fi
    printf '%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s\n' "$implementation" "$kind" "$records" "$repetition" "$operation" "$selected_pattern" "$cache_state" "$status" "$validation" "$peak_rss" "raw/$slug.json" "raw/$slug.stderr" "raw/$slug.time" >>"$OUT/process-manifest.csv"
    if "$PYTHON_BIN" -m json.tool "$prefix.json" >/dev/null 2>&1; then cat "$prefix.json" >>"$OUT/raw-results.jsonl"; fi
    if [ "$validation" != ok ]; then
        printf 'benchmark process failed: %s; see %s\n' "$slug" "$prefix.stderr" >&2
        return 1
    fi
}

sequence=0
run_pair() {
    local kind=$1 records=$2 repetition=$3 operation=$4 selected_pattern=$5 cache_state=$6 changes=$7
    local common=()
    if [ "$kind" = cell ]; then common=(--operation "$operation" --pattern "$selected_pattern" --changes "$changes" --read-samples "$READ_SAMPLES"); fi
    local first=rust second=dolt-go
    if [ $((sequence % 2)) -eq 1 ]; then first=dolt-go; second=rust; fi
    sequence=$((sequence + 1))
    for implementation in "$first" "$second"; do
        if [ "$implementation" = rust ]; then
            if [ "$kind" = cell ]; then
                run_one rust "$kind" "$records" "$repetition" "$operation" "$selected_pattern" "$cache_state" "$RUST_BIN" "$RUST_REVISION" "$OUT/work/rust" "${common[@]}"
            else
                run_one rust "$kind" "$records" "$repetition" "$operation" "$selected_pattern" "$cache_state" "$RUST_BIN" "$RUST_REVISION" "$OUT/work/rust"
            fi
        else
            if [ "$kind" = cell ]; then
                run_one dolt-go "$kind" "$records" "$repetition" "$operation" "$selected_pattern" "$cache_state" "$GO_BIN" "$GO_REVISION" "$OUT/work/dolt-go" "${common[@]}"
            else
                run_one dolt-go "$kind" "$records" "$repetition" "$operation" "$selected_pattern" "$cache_state" "$GO_BIN" "$GO_REVISION" "$OUT/work/dolt-go"
            fi
        fi
    done
}

IFS=',' read -r -a operation_array <<<"$OPERATIONS"
IFS=',' read -r -a pattern_array <<<"$PATTERNS"
expected_cells=0
for selected_operation in "${operation_array[@]}"; do
    if [ "$selected_operation" = full_scan ]; then expected_cells=$((expected_cells + 1)); else expected_cells=$((expected_cells + ${#pattern_array[@]})); fi
done

for records in $SIZES; do
    changes=$CHANGES_OVERRIDE
    if [ -z "$changes" ]; then changes=$(((records * 30 + 99) / 100)); fi
    if [[ ",$OPERATIONS," == *,merge,* ]] && [ $((changes % 2)) -ne 0 ]; then printf 'merge changes must be even: %s\n' "$changes" >&2; exit 2; fi
    if [ "$READ_SAMPLES" -gt "$records" ] || [ "$changes" -gt "$records" ]; then printf 'changes/read samples exceed records\n' >&2; exit 2; fi
    for repetition in $(seq 1 "$RUNS"); do
        run_pair fixture "$records" "$repetition" build n/a n/a "$changes"
        for selected_operation in "${operation_array[@]}"; do
            if [ "$selected_operation" = full_scan ]; then selected_patterns=(append); else selected_patterns=("${pattern_array[@]}"); fi
            for selected_pattern in "${selected_patterns[@]}"; do
                cache_state=n/a
                [ "$selected_operation" = get_cold ] && cache_state=cold-manager
                [ "$selected_operation" = get_warm ] && cache_state=warm-manager
                run_pair cell "$records" "$repetition" "$selected_operation" "$selected_pattern" "$cache_state" "$changes"
            done
        done
    done
done

expected_sizes=$(printf '%s' "$SIZES" | tr ' ' ',')
"$PYTHON_BIN" "$ROOT/scripts/summarize_dolt_sqlite_comparison.py" --input "$OUT/raw-results.jsonl" --manifest "$OUT/process-manifest.csv" --output-dir "$OUT" --expected-runs "$RUNS" --expected-sizes "$expected_sizes" --expected-cells "$expected_cells"
printf 'ended_utc=%s\n' "$(date -u +%Y-%m-%dT%H:%M:%SZ)" >>"$OUT/manifest.txt"
printf 'complete\n' >"$OUT/run-status.txt"
printf 'Dolt Go vs Rust SQLite comparison complete: %s\n' "$OUT"
