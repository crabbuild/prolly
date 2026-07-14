#!/bin/sh
set -eu

ROOT=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
BASELINE_DIR=${SCALE_BASELINE_DIR:-/tmp/prolly-scale-baseline}
BASELINE_TARGET=${SCALE_BASELINE_TARGET:-/tmp/prolly-scale-baseline-target}
CURRENT_TARGET=${SCALE_CURRENT_TARGET:-/tmp/prolly-scale-current-target}
OUTPUT_DIR=${SCALE_OUTPUT_DIR:-$ROOT/performance-results/scale-2026-07-14}
SIZES=${SCALE_SIZES:-"1000 10000 50000 100000 1000000 10000000"}
RUNS=${SCALE_RUNS:-3}
RUNS_100K=${SCALE_100K_RUNS:-$RUNS}
RUNS_10M=${SCALE_10M_RUNS:-2}
BASELINE_REVISION=${SCALE_BASELINE_REVISION:-fa7c219}

mkdir -p "$OUTPUT_DIR/raw"
rm -f "$OUTPUT_DIR/raw"/*.csv "$OUTPUT_DIR/raw"/*.time "$OUTPUT_DIR/raw"/*.stderr

if [ ! -d "$BASELINE_DIR/.git" ] && [ ! -f "$BASELINE_DIR/.git" ]; then
    git -C "$ROOT" worktree add --detach "$BASELINE_DIR" "$BASELINE_REVISION"
fi

cp "$ROOT/benches/scale_workloads.rs" "$BASELINE_DIR/benches/scale_workloads.rs"
if ! grep -q 'name = "scale_workloads"' "$BASELINE_DIR/Cargo.toml"; then
    printf '\n[[bench]]\nname = "scale_workloads"\nharness = false\n' >> "$BASELINE_DIR/Cargo.toml"
fi
if [ -f "$ROOT/Cargo.lock" ]; then
    cp "$ROOT/Cargo.lock" "$BASELINE_DIR/Cargo.lock"
fi

CARGO_TARGET_DIR="$BASELINE_TARGET" cargo bench \
    --manifest-path "$BASELINE_DIR/Cargo.toml" --bench scale_workloads --no-run
CARGO_TARGET_DIR="$CURRENT_TARGET" cargo bench \
    --manifest-path "$ROOT/Cargo.toml" --bench scale_workloads --no-run

BASELINE_BINARY=$(find "$BASELINE_TARGET/release/deps" -maxdepth 1 -type f -perm -111 -name 'scale_workloads-*' -exec ls -t {} + | head -1)
CURRENT_BINARY=$(find "$CURRENT_TARGET/release/deps" -maxdepth 1 -type f -perm -111 -name 'scale_workloads-*' -exec ls -t {} + | head -1)
test -n "$BASELINE_BINARY"
test -n "$CURRENT_BINARY"
if [ -f "$ROOT/Cargo.lock" ]; then
    cp "$ROOT/Cargo.lock" "$OUTPUT_DIR/dependency-lock.Cargo.lock"
fi

{
    date -u '+utc_started=%Y-%m-%dT%H:%M:%SZ'
    printf 'root=%s\n' "$ROOT"
    printf 'baseline_revision=%s\n' "$(git -C "$BASELINE_DIR" rev-parse HEAD)"
    printf 'improved_revision=%s\n' "$(git -C "$ROOT" rev-parse HEAD)"
    printf 'baseline_binary=%s\n' "$BASELINE_BINARY"
    printf 'improved_binary=%s\n' "$CURRENT_BINARY"
    printf 'baseline_binary_sha256=%s\n' "$(shasum -a 256 "$BASELINE_BINARY" | awk '{print $1}')"
    printf 'improved_binary_sha256=%s\n' "$(shasum -a 256 "$CURRENT_BINARY" | awk '{print $1}')"
    printf 'shared_harness_sha256=%s\n' "$(shasum -a 256 "$ROOT/benches/scale_workloads.rs" | awk '{print $1}')"
    if git -C "$ROOT" diff --quiet && git -C "$ROOT" diff --cached --quiet; then
        printf 'improved_worktree_dirty=no\n'
    else
        printf 'improved_worktree_dirty=yes\n'
    fi
    printf 'hardware_memory_bytes=%s\n' "$(sysctl -n hw.memsize)"
    printf 'hardware_logical_cpus=%s\n' "$(sysctl -n hw.ncpu)"
    printf 'os=%s\n' "$(sw_vers -productVersion)"
    rustc -Vv
} > "$OUTPUT_DIR/machine.txt"

printf 'version,records,run,exit_status,csv,time,stderr\n' > "$OUTPUT_DIR/run-manifest.csv"

run_one() {
    version=$1
    records=$2
    run=$3
    binary=$4
    stem="$version-$records-$run"
    csv="$OUTPUT_DIR/raw/$stem.csv"
    timing="$OUTPUT_DIR/raw/$stem.time"
    stderr="$OUTPUT_DIR/raw/$stem.stderr"

    set +e
    /usr/bin/time -l -o "$timing" env \
        SCALE_VERSION="$version" SCALE_RECORDS="$records" \
        "$binary" > "$csv" 2> "$stderr"
    status=$?
    set -e

    printf '%s,%s,%s,%s,%s,%s,%s\n' \
        "$version" "$records" "$run" "$status" "$csv" "$timing" "$stderr" \
        >> "$OUTPUT_DIR/run-manifest.csv"
}

for records in $SIZES; do
    repetitions=$RUNS
    if [ "$records" -eq 100000 ]; then
        repetitions=$RUNS_100K
    fi
    if [ "$records" -eq 10000000 ]; then
        repetitions=$RUNS_10M
    fi
    run=1
    while [ "$run" -le "$repetitions" ]; do
        if [ $((run % 2)) -eq 1 ]; then
            run_one original "$records" "$run" "$BASELINE_BINARY"
            run_one improved "$records" "$run" "$CURRENT_BINARY"
        else
            run_one improved "$records" "$run" "$CURRENT_BINARY"
            run_one original "$records" "$run" "$BASELINE_BINARY"
        fi
        run=$((run + 1))
    done
done

awk -F, 'NR > 1 && $4 != 0 { failed = 1 } END { exit failed }' "$OUTPUT_DIR/run-manifest.csv"

CARGO_TARGET_DIR="$CURRENT_TARGET" cargo run --release \
    --manifest-path "$ROOT/Cargo.toml" --bin prolly-scale-report -- "$OUTPUT_DIR"
