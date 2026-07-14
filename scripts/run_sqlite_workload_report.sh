#!/bin/sh
set -eu

ROOT=$(CDPATH= cd -- "$(dirname "$0")/.." && pwd)
BASELINE_REV=${SQLITE_BENCH_BASELINE_REV:-fa7c219}
SIZES=${SQLITE_BENCH_SIZES:-"1000 10000 50000 100000 1000000 10000000"}
RUNS=${SQLITE_BENCH_RUNS:-5}
PROFILES=${SQLITE_BENCH_PROFILES:-"full normal"}
WORKLOADS=${SQLITE_BENCH_WORKLOADS:-"sorted_stream_build shuffled_batch_build random_reads_cold_manager random_reads_warm_manager clustered_reads_cold_manager clustered_reads_warm_manager right_edge_reads_cold_manager right_edge_reads_warm_manager append_batch_upserts random_batch_updates clustered_batch_updates random_batch_deletes clustered_batch_deletes identical_diff append_sparse_diff random_sparse_diff clustered_sparse_diff random_delete_diff clustered_delete_diff append_disjoint_sparse_merge random_disjoint_sparse_merge clustered_disjoint_sparse_merge random_conflict_resolved_merge clustered_conflict_resolved_merge"}
RESULT_DIR=${SQLITE_BENCH_RESULT_DIR:-"$ROOT/performance-results/sqlite-workloads-2026-07-14"}
TMP_ROOT=$(mktemp -d "${TMPDIR:-/tmp}/prolly-sqlite-workloads.XXXXXX")
BASELINE_ROOT="$TMP_ROOT/baseline"
CURRENT_TARGET="$TMP_ROOT/current-target"
BASELINE_TARGET="$TMP_ROOT/baseline-target"
FIXTURES="$TMP_ROOT/fixtures"
RAW="$RESULT_DIR/raw"
MANIFEST="$RESULT_DIR/run-manifest.csv"
ROWS="$RESULT_DIR/raw-results.csv"
MACHINE="$RESULT_DIR/machine.txt"
COPY_METHOD=copy

cleanup() {
    git -C "$ROOT" worktree remove --force "$BASELINE_ROOT" >/dev/null 2>&1 || true
    rm -rf "$TMP_ROOT"
}
trap cleanup EXIT INT TERM

mkdir -p "$RAW" "$FIXTURES"
rm -f "$MANIFEST" "$ROWS" "$MACHINE"
printf '%s\n' 'version,profile,records,run,workload,order,exit_status,validation,stdout,stderr,timing' >"$MANIFEST"

git -C "$ROOT" worktree add --detach "$BASELINE_ROOT" "$BASELINE_REV" >/dev/null
mkdir -p "$BASELINE_ROOT/stores/prolly-store-sqlite/benches"
cp "$ROOT/stores/prolly-store-sqlite/benches/sqlite_workload_bench.rs" "$BASELINE_ROOT/stores/prolly-store-sqlite/benches/sqlite_workload_bench.rs"
cp "$ROOT/stores/prolly-store-sqlite/benches/sqlite_workload_support.rs" "$BASELINE_ROOT/stores/prolly-store-sqlite/benches/sqlite_workload_support.rs"
if ! grep -q 'name = "sqlite_workload_bench"' "$BASELINE_ROOT/stores/prolly-store-sqlite/Cargo.toml"; then
    printf '\n[[bench]]\nname = "sqlite_workload_bench"\nharness = false\n' >>"$BASELINE_ROOT/stores/prolly-store-sqlite/Cargo.toml"
fi

current_harness_hash=$(shasum -a 256 "$ROOT/stores/prolly-store-sqlite/benches/sqlite_workload_bench.rs" "$ROOT/stores/prolly-store-sqlite/benches/sqlite_workload_support.rs" | shasum -a 256 | awk '{print $1}')
baseline_harness_hash=$(shasum -a 256 "$BASELINE_ROOT/stores/prolly-store-sqlite/benches/sqlite_workload_bench.rs" "$BASELINE_ROOT/stores/prolly-store-sqlite/benches/sqlite_workload_support.rs" | shasum -a 256 | awk '{print $1}')
if [ "$current_harness_hash" != "$baseline_harness_hash" ]; then
    printf '%s\n' 'shared harness hash mismatch' >&2
    exit 1
fi

CARGO_INCREMENTAL=0 cargo build --release --manifest-path "$ROOT/stores/prolly-store-sqlite/Cargo.toml" --target-dir "$CURRENT_TARGET" --bench sqlite_workload_bench
CARGO_INCREMENTAL=0 cargo build --release --manifest-path "$BASELINE_ROOT/stores/prolly-store-sqlite/Cargo.toml" --target-dir "$BASELINE_TARGET" --bench sqlite_workload_bench
CURRENT_BIN=$(find "$CURRENT_TARGET/release/deps" -type f -perm +111 -name 'sqlite_workload_bench-*' | head -n 1)
BASELINE_BIN=$(find "$BASELINE_TARGET/release/deps" -type f -perm +111 -name 'sqlite_workload_bench-*' | head -n 1)
test -n "$CURRENT_BIN"
test -n "$BASELINE_BIN"

if cp -c /dev/null "$TMP_ROOT/copy-test" 2>/dev/null; then
    COPY_METHOD=clonefile
elif cp --reflink=auto /dev/null "$TMP_ROOT/copy-test" 2>/dev/null; then
    COPY_METHOD=reflink-auto
fi
rm -f "$TMP_ROOT/copy-test"

{
    printf 'timestamp_utc=%s\n' "$(date -u +%Y-%m-%dT%H:%M:%SZ)"
    printf 'current_revision=%s\n' "$(git -C "$ROOT" rev-parse HEAD)"
    printf 'baseline_revision=%s\n' "$(git -C "$BASELINE_ROOT" rev-parse HEAD)"
    printf 'current_dirty=%s\n' "$(test -n "$(git -C "$ROOT" status --porcelain --untracked-files=no)" && printf true || printf false)"
    printf 'harness_sha256=%s\n' "$current_harness_hash"
    printf 'current_binary_sha256=%s\n' "$(shasum -a 256 "$CURRENT_BIN" | awk '{print $1}')"
    printf 'baseline_binary_sha256=%s\n' "$(shasum -a 256 "$BASELINE_BIN" | awk '{print $1}')"
    printf 'rustc=%s\n' "$(rustc -Vv | tr '\n' ';')"
    printf 'cargo=%s\n' "$(cargo -V)"
    printf 'sqlite_cli=%s\n' "$(sqlite3 --version)"
    printf 'uname=%s\n' "$(uname -a)"
    printf 'cpu_count=%s\n' "$(sysctl -n hw.ncpu 2>/dev/null || getconf _NPROCESSORS_ONLN)"
    printf 'memory_bytes=%s\n' "$(sysctl -n hw.memsize 2>/dev/null || printf unknown)"
    printf 'filesystem=%s\n' "$(df -h "$ROOT" | tail -n 1 | tr -s ' ')"
    printf 'copy_method=%s\n' "$COPY_METHOD"
    printf 'sizes=%s\n' "$SIZES"
    printf 'runs=%s\n' "$RUNS"
    printf 'profiles=%s\n' "$PROFILES"
    printf 'workloads=%s\n' "$WORKLOADS"
} >"$MACHINE"

copy_fixture() {
    source_path=$1
    destination_path=$2
    case "$COPY_METHOD" in
        clonefile) cp -c "$source_path" "$destination_path" ;;
        reflink-auto) cp --reflink=auto "$source_path" "$destination_path" ;;
        *) cp "$source_path" "$destination_path" ;;
    esac
}

run_process() {
    version=$1
    profile=$2
    records=$3
    run=$4
    workload=$5
    order=$6
    database=$7
    binary=$8
    slug="${records}-${profile}-r${run}-${workload}-${version}"
    stdout="$RAW/$slug.stdout"
    stderr="$RAW/$slug.stderr"
    timing="$RAW/$slug.time"
    set +e
    /usr/bin/time -l -o "$timing" env \
        PROLLY_SQLITE_WORKLOAD="$workload" \
        PROLLY_SQLITE_RECORDS="$records" \
        PROLLY_SQLITE_PROFILE="$profile" \
        PROLLY_SQLITE_VERSION="$version" \
        PROLLY_SQLITE_RUN="$run" \
        PROLLY_SQLITE_DB="$database" \
        "$binary" >"$stdout" 2>"$stderr"
    exit_status=$?
    set -e
    validation=failed
    if [ "$exit_status" -eq 0 ] && tail -n 1 "$stdout" | grep -q ',true,ok$'; then
        validation=ok
        if [ ! -s "$ROWS" ]; then
            head -n 1 "$stdout" >"$ROWS"
        fi
        tail -n 1 "$stdout" >>"$ROWS"
    fi
    printf '%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s\n' \
        "$version" "$profile" "$records" "$run" "$workload" "$order" "$exit_status" "$validation" \
        "raw/$slug.stdout" "raw/$slug.stderr" "raw/$slug.time" >>"$MANIFEST"
}

run_number=1
while [ "$run_number" -le "$RUNS" ]; do
    for records in $SIZES; do
        if [ $((run_number % 2)) -eq 1 ]; then
            profile_order=$PROFILES
        else
            profile_order=$(printf '%s\n' $PROFILES | awk '{line[NR]=$0} END {for (i=NR;i>=1;i--) printf "%s%s", line[i], (i>1?" ":"\n")}')
        fi
        for profile in $profile_order; do
            if [ $(((run_number + records) % 2)) -eq 1 ]; then
                versions="original current"
            else
                versions="current original"
            fi
            order=0
            for version in $versions; do
                order=$((order + 1))
                if [ "$version" = current ]; then
                    binary=$CURRENT_BIN
                else
                    binary=$BASELINE_BIN
                fi
                base="$FIXTURES/${records}-${profile}-r${run_number}-${version}-base.sqlite"
                rm -f "$base" "$base-wal" "$base-shm"
                run_process "$version" "$profile" "$records" "$run_number" sorted_stream_build "$order" "$base" "$binary"
                if ! tail -n 1 "$RAW/${records}-${profile}-r${run_number}-sorted_stream_build-${version}.stdout" | grep -q ',true,ok$'; then
                    rm -f "$base" "$base-wal" "$base-shm"
                    continue
                fi
                checkpoint=$(sqlite3 "$base" 'PRAGMA wal_checkpoint(TRUNCATE); PRAGMA integrity_check;')
                if ! printf '%s\n' "$checkpoint" | tail -n 1 | grep -qx ok; then
                    printf 'checkpoint or integrity validation failed for %s\n' "$base" >&2
                    exit 1
                fi
                for workload in $WORKLOADS; do
                    if [ "$workload" = sorted_stream_build ]; then
                        continue
                    fi
                    fixture="$FIXTURES/${records}-${profile}-r${run_number}-${workload}-${version}.sqlite"
                    rm -f "$fixture" "$fixture-wal" "$fixture-shm"
                    if [ "$workload" != shuffled_batch_build ]; then
                        copy_fixture "$base" "$fixture"
                    fi
                    run_process "$version" "$profile" "$records" "$run_number" "$workload" "$order" "$fixture" "$binary"
                    rm -f "$fixture" "$fixture-wal" "$fixture-shm"
                done
                rm -f "$base" "$base-wal" "$base-shm"
            done
        done
    done
    run_number=$((run_number + 1))
done

printf 'Raw results: %s\n' "$ROWS"
printf 'Run manifest: %s\n' "$MANIFEST"
