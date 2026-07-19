#!/bin/sh
set -eu

ROOT=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
OUT=${BENCH_OUT:-"$ROOT/performance-results/prolly-version-2026-07-18"}
SIZES=${BENCH_SIZES:-"10000 50000 1000000 5000000 10000000"}
RUNS=${BENCH_RUNS:-3}
DENSITIES=${BENCH_DENSITIES:-"0 1 30"}
LOCALITIES=${BENCH_LOCALITIES:-"append random clustered"}
RUN_LIFECYCLE=${BENCH_LIFECYCLE:-1}
DOLT_REPO_URL=${DOLT_REPO_URL:-"https://github.com/dolthub/dolt.git"}
DOLT_CACHE=${DOLT_CACHE:-"$ROOT/target/dolt-version-benchmark"}
DOLT_REQUESTED_REV=${DOLT_REV:-}
TIME_BIN=${BENCH_TIME_BIN:-/usr/bin/time}
GO_RUNNER_SOURCE="$ROOT/benchmarks/dolt-prolly-version-compare"
RUNNER_HEADER='implementation,revision,contract_version,records,density,locality,operation,relationship,operations,elapsed_ns,ns_per_op,ops_per_sec,workload_digest,result_digest,result_count,base_count,target_count,conflict_count,validated'
RESULT_HEADER="$RUNNER_HEADER,repetition,peak_rss_bytes"

case "$OUT" in
    /*) ;;
    *) OUT="$ROOT/$OUT" ;;
esac
case "$DOLT_CACHE" in
    /*) ;;
    *) DOLT_CACHE="$ROOT/$DOLT_CACHE" ;;
esac

if [ "$RUNS" -le 0 ]; then
    printf 'BENCH_RUNS must be positive\n' >&2
    exit 2
fi
if [ -e "$OUT/results-common.csv" ] || [ -e "$OUT/results-lifecycle.csv" ]; then
    printf 'benchmark output already contains results: %s\n' "$OUT" >&2
    exit 2
fi
if [ ! -f "$GO_RUNNER_SOURCE/main.go" ] || [ ! -f "$GO_RUNNER_SOURCE/workload.go" ]; then
    printf 'checked-in Dolt version runner is incomplete: %s\n' "$GO_RUNNER_SOURCE" >&2
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

go_runner_hash() {
    find "$GO_RUNNER_SOURCE" -type f -name '*.go' |
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

mkdir -p "$DOLT_CACHE/go/cmd/prolly-version-compare"
cp "$GO_RUNNER_SOURCE/main.go" "$DOLT_CACHE/go/cmd/prolly-version-compare/main.go"
cp "$GO_RUNNER_SOURCE/workload.go" "$DOLT_CACHE/go/cmd/prolly-version-compare/workload.go"

RUST_COMMIT=$(git -C "$ROOT" rev-parse HEAD 2>/dev/null || printf unknown)
RUST_SOURCE_HASH=$(source_tree_hash)
DOLT_RUNNER_HASH=$(go_runner_hash)
RUST_REV="$(printf '%s' "$RUST_COMMIT" | cut -c1-12)+src.$(printf '%s' "$RUST_SOURCE_HASH" | cut -c1-12)"
DOLT_BENCH_REV="$(printf '%s' "$DOLT_SHA" | cut -c1-12)+runner.$(printf '%s' "$DOLT_RUNNER_HASH" | cut -c1-12)"

RUST_TARGET=$(cargo metadata --manifest-path "$ROOT/Cargo.toml" --no-deps --format-version 1 |
    sed -n 's/.*"target_directory":"\([^"]*\)".*/\1/p')
if [ -z "$RUST_TARGET" ]; then
    printf 'cargo metadata did not return a target directory\n' >&2
    exit 2
fi
cargo build --manifest-path "$ROOT/Cargo.toml" --release \
    --bin prolly_version_compare --bin prolly_version_lifecycle
cp "$RUST_TARGET/release/prolly_version_compare" "$OUT/bin/rust-prolly-version-compare"
cp "$RUST_TARGET/release/prolly_version_lifecycle" "$OUT/bin/rust-prolly-version-lifecycle"

(
    cd "$DOLT_CACHE/go"
    go build -trimpath -o "$OUT/bin/dolt-go-prolly-version-compare" ./cmd/prolly-version-compare
)

RUST_COMMON_BINARY_HASH=$(sha256_file "$OUT/bin/rust-prolly-version-compare")
RUST_LIFECYCLE_BINARY_HASH=$(sha256_file "$OUT/bin/rust-prolly-version-lifecycle")
DOLT_BINARY_HASH=$(sha256_file "$OUT/bin/dolt-go-prolly-version-compare")

{
    printf 'generated_utc=%s\n' "$(date -u +%Y-%m-%dT%H:%M:%SZ)"
    printf 'rust_commit=%s\n' "$RUST_COMMIT"
    printf 'rust_source_sha256=%s\n' "$RUST_SOURCE_HASH"
    printf 'rust_revision=%s\n' "$RUST_REV"
    printf 'dolt_repository=%s\n' "$DOLT_REPO_URL"
    printf 'dolt_commit=%s\n' "$DOLT_SHA"
    printf 'dolt_runner_sha256=%s\n' "$DOLT_RUNNER_HASH"
    printf 'dolt_revision=%s\n' "$DOLT_BENCH_REV"
    printf 'rust_common_binary_sha256=%s\n' "$RUST_COMMON_BINARY_HASH"
    printf 'rust_lifecycle_binary_sha256=%s\n' "$RUST_LIFECYCLE_BINARY_HASH"
    printf 'dolt_binary_sha256=%s\n' "$DOLT_BINARY_HASH"
    printf 'sizes=%s\n' "$SIZES"
    printf 'runs=%s\n' "$RUNS"
    printf 'worker_threads=1\n'
    printf 'contract_version=prolly-version-compare-v3\n'
    printf 'densities=%s\n' "$DENSITIES"
    printf 'localities=%s\n' "$LOCALITIES"
    printf 'lifecycle=%s\n' "$RUN_LIFECYCLE"
    printf 'history_depth=100\n'
    printf 'storage=in-memory\n'
} >"$OUT/manifest.txt"

{
    printf 'generated_utc=%s\n' "$(date -u +%Y-%m-%dT%H:%M:%SZ)"
    printf 'host=%s\n' "$(hostname)"
    printf 'os=%s\n' "$(uname -a)"
    if command -v sysctl >/dev/null 2>&1; then
        printf 'cpu=%s\n' "$(sysctl -n machdep.cpu.brand_string 2>/dev/null || true)"
        printf 'memory_bytes=%s\n' "$(sysctl -n hw.memsize 2>/dev/null || true)"
    fi
    rustc -Vv
    go version
} >"$OUT/machine.txt"

if "$TIME_BIN" -l -o "$OUT/time-probe.txt" /usr/bin/true >/dev/null 2>"$OUT/time-probe.stderr"; then
    TIME_MODE=-l
else
    TIME_MODE=-v
    "$TIME_BIN" -v -o "$OUT/time-probe.txt" /usr/bin/true >/dev/null 2>"$OUT/time-probe.stderr"
fi

validate_csv() {
    csv_path=$1
    expected_implementation=$2
    expected_operations=$3
    python3 - "$csv_path" "$expected_implementation" "$expected_operations" "$RUNNER_HEADER" <<'PY'
import csv
import math
import sys

path, implementation, operation_text, expected_header = sys.argv[1:]
expected_operations = operation_text.split()
with open(path, newline="", encoding="utf-8") as handle:
    header = handle.readline().rstrip("\r\n")
    handle.seek(0)
    rows = list(csv.DictReader(handle))
if header != expected_header:
    raise SystemExit(f"header mismatch: {header!r}")
if [row.get("operation") for row in rows] != expected_operations:
    raise SystemExit(f"operation sequence mismatch: {[row.get('operation') for row in rows]!r}")
for row in rows:
    if row.get("implementation") != implementation:
        raise SystemExit("implementation mismatch")
    if row.get("contract_version") != "prolly-version-compare-v3":
        raise SystemExit("contract version mismatch")
    if row.get("validated") != "true":
        raise SystemExit("runner emitted an unvalidated row")
    for field in ("operations", "elapsed_ns", "result_count", "base_count", "target_count", "conflict_count"):
        if int(row[field]) < 0:
            raise SystemExit(f"negative {field}")
    for field in ("ns_per_op", "ops_per_sec"):
        if not math.isfinite(float(row[field])):
            raise SystemExit(f"non-finite {field}")
PY
}

validate_pair() {
    rust_csv=$1
    go_csv=$2
    python3 - "$rust_csv" "$go_csv" <<'PY'
import csv
import sys

rust = list(csv.DictReader(open(sys.argv[1], newline="", encoding="utf-8")))
go = list(csv.DictReader(open(sys.argv[2], newline="", encoding="utf-8")))
if [row["operation"] for row in rust] != [row["operation"] for row in go]:
    raise SystemExit("paired operation sequence differs")
fields = (
    "contract_version", "records", "density", "locality", "operation", "relationship",
    "operations", "workload_digest", "result_digest", "base_count", "target_count",
    "conflict_count", "validated",
)
for rust_row, go_row in zip(rust, go):
    for field in fields:
        if rust_row[field] != go_row[field]:
            raise SystemExit(
                f"pair mismatch operation={rust_row['operation']} field={field}: "
                f"rust={rust_row[field]!r} go={go_row[field]!r}"
            )
    if rust_row["operation"] != "patch_generate" and rust_row["result_count"] != go_row["result_count"]:
        raise SystemExit(f"pair result_count mismatch for {rust_row['operation']}")
PY
}

append_rows() {
    csv_path=$1
    repetition=$2
    peak_rss=$3
    output=$4
    awk -v repetition="$repetition" -v peak_rss="$peak_rss" \
        'NR > 1 { print $0 "," repetition "," peak_rss }' "$csv_path" >>"$output"
}

peak_rss() {
    python3 "$ROOT/scripts/prolly_process_metrics.py" "$1"
}

printf '%s\n' "$RESULT_HEADER" >"$OUT/results-common.csv"
printf '%s\n' "$RESULT_HEADER" >"$OUT/results-lifecycle.csv"
printf 'repetition,implementation,records,density,locality,scenario,exit_status,peak_rss_bytes,stdout,stderr,time\n' >"$OUT/manifest.csv"

run_process() {
    implementation=$1
    records=$2
    density=$3
    locality=$4
    scenario=$5
    repetition=$6
    prefix=$7

    case "$implementation" in
        rust)
            binary="$OUT/bin/rust-prolly-version-compare"
            revision=$RUST_REV
            set -- "$binary" --records "$records" --density "$density" --locality "$locality"
            ;;
        dolt-go)
            binary="$OUT/bin/dolt-go-prolly-version-compare"
            revision=$DOLT_BENCH_REV
            set -- "$binary" --records "$records" --density "$density" --locality "$locality"
            ;;
        rust-lifecycle)
            binary="$OUT/bin/rust-prolly-version-lifecycle"
            revision=$RUST_REV
            set -- "$binary" --records "$records" --scenario "$scenario"
            if [ "$scenario" = publish ]; then
                set -- "$@" --density "$density" --locality "$locality"
            fi
            ;;
        *)
            printf 'unknown implementation: %s\n' "$implementation" >&2
            return 2
            ;;
    esac

    printf 'running repetition=%s implementation=%s records=%s density=%s locality=%s scenario=%s\n' \
        "$repetition" "$implementation" "$records" "$density" "$locality" "$scenario" >&2
    set +e
    RAYON_NUM_THREADS=1 GOMAXPROCS=1 BENCH_REVISION="$revision" \
        "$TIME_BIN" "$TIME_MODE" -o "$prefix.time" "$@" >"$prefix.csv" 2>"$prefix.stderr"
    process_exit=$?
    set -e
    rss=$(peak_rss "$prefix.time" 2>>"$prefix.stderr" || true)
    printf '%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s\n' \
        "$repetition" "$implementation" "$records" "$density" "$locality" "$scenario" \
        "$process_exit" "$rss" "$prefix.csv" "$prefix.stderr" "$prefix.time" >>"$OUT/manifest.csv"
    if [ "$process_exit" -ne 0 ] || [ -z "$rss" ]; then
        printf 'benchmark process failed: %s; see %s\n' "$prefix" "$prefix.stderr" >&2
        return 1
    fi
}

common_operations() {
    if [ "$1" -eq 0 ]; then
        printf 'full_diff range_diff patch_generate patch_apply merge_noop'
    else
        printf 'full_diff range_diff patch_generate patch_apply merge_disjoint merge_convergent merge_conflict'
    fi
}

run_common_pair() {
    records=$1
    density=$2
    locality=$3
    repetition=$4
    directory=$5
    rust_prefix="$directory/rust-${records}-${density}-${locality}-run${repetition}"
    go_prefix="$directory/dolt-go-${records}-${density}-${locality}-run${repetition}"
    if [ $(((records + density + repetition + ${#locality}) % 2)) -eq 0 ]; then
        run_process rust "$records" "$density" "$locality" common "$repetition" "$rust_prefix"
        run_process dolt-go "$records" "$density" "$locality" common "$repetition" "$go_prefix"
    else
        run_process dolt-go "$records" "$density" "$locality" common "$repetition" "$go_prefix"
        run_process rust "$records" "$density" "$locality" common "$repetition" "$rust_prefix"
    fi
    operations=$(common_operations "$density")
    validate_csv "$rust_prefix.csv" rust "$operations"
    validate_csv "$go_prefix.csv" dolt-go "$operations"
    validate_pair "$rust_prefix.csv" "$go_prefix.csv"
    append_rows "$rust_prefix.csv" "$repetition" "$(peak_rss "$rust_prefix.time")" "$OUT/results-common.csv"
    append_rows "$go_prefix.csv" "$repetition" "$(peak_rss "$go_prefix.time")" "$OUT/results-common.csv"
}

run_lifecycle() {
    records=$1
    scenario=$2
    density=$3
    locality=$4
    repetition=$5
    directory=$6
    prefix="$directory/rust-lifecycle-${records}-${scenario}-${density}-${locality}-run${repetition}"
    run_process rust-lifecycle "$records" "$density" "$locality" "$scenario" "$repetition" "$prefix"
    case "$scenario" in
        publish) operations='version_publish' ;;
        read) operations='head_resolve snapshot_resolve historical_point_read historical_range_scan version_list' ;;
        rollback) operations='rollback' ;;
        prune) operations='retention_prune' ;;
    esac
    validate_csv "$prefix.csv" rust-lifecycle "$operations"
    append_rows "$prefix.csv" "$repetition" "$(peak_rss "$prefix.time")" "$OUT/results-lifecycle.csv"
}

# Fail fast on contract mismatches before the requested matrix.
run_common_pair 10000 0 none 1 "$OUT/smoke"
run_common_pair 10000 1 random 1 "$OUT/smoke"
if [ "$RUN_LIFECYCLE" -eq 1 ]; then
    run_lifecycle 10000 prune 0 none 1 "$OUT/smoke"
fi

# Remove smoke rows from the full result streams while retaining smoke artifacts.
printf '%s\n' "$RESULT_HEADER" >"$OUT/results-common.csv"
printf '%s\n' "$RESULT_HEADER" >"$OUT/results-lifecycle.csv"
printf 'repetition,implementation,records,density,locality,scenario,exit_status,peak_rss_bytes,stdout,stderr,time\n' >"$OUT/manifest.csv"

for records in $SIZES; do
    repetition=1
    while [ "$repetition" -le "$RUNS" ]; do
        for density in $DENSITIES; do
            if [ "$density" -eq 0 ]; then
                run_common_pair "$records" 0 none "$repetition" "$OUT/raw"
                continue
            fi
            for locality in $LOCALITIES; do
                run_common_pair "$records" "$density" "$locality" "$repetition" "$OUT/raw"
                if [ "$RUN_LIFECYCLE" -eq 1 ]; then
                    run_lifecycle "$records" publish "$density" "$locality" "$repetition" "$OUT/raw"
                fi
            done
        done
        if [ "$RUN_LIFECYCLE" -eq 1 ]; then
            run_lifecycle "$records" read 0 none "$repetition" "$OUT/raw"
            run_lifecycle "$records" rollback 0 none "$repetition" "$OUT/raw"
            run_lifecycle "$records" prune 0 none "$repetition" "$OUT/raw"
        fi
        repetition=$((repetition + 1))
    done
done

python3 "$ROOT/scripts/summarize_prolly_version_comparison.py" --output-dir "$OUT"
printf 'version comparison complete: %s/report.md\n' "$OUT"
