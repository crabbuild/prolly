#!/bin/sh
set -eu

ROOT=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
OUT=${BENCH_OUT:-"$ROOT/performance-results/dolt-rust"}
SIZES=${BENCH_SIZES:-"10000 50000 1000000 5000000 10000000"}
RUNS=${BENCH_RUNS:-3}
LARGE_RUNS=${BENCH_LARGE_RUNS:-2}

case "$OUT" in
    /*) ;;
    *) OUT="$ROOT/$OUT" ;;
esac

mkdir -p "$OUT/bin" "$OUT/raw"

RUST_HEAD=$(git -C "$ROOT" rev-parse --short=12 HEAD 2>/dev/null || printf unknown)
RUST_SOURCE_HASH=$(
    find "$ROOT/src" "$ROOT/benches" "$ROOT/Cargo.toml" -type f |
        LC_ALL=C sort |
        while IFS= read -r file; do shasum "$file"; done |
        shasum | awk '{ print substr($1, 1, 12) }'
)
RUST_REV="${RUST_HEAD}+src.${RUST_SOURCE_HASH}"
GO_HEAD=$(git -C "$ROOT/dolt" rev-parse --short=12 HEAD 2>/dev/null || printf unknown)
GO_RUNNER_HASH=$(shasum "$ROOT/dolt/go/cmd/prolly-compare/main.go" | awk '{ print substr($1, 1, 12) }')
GO_REV="${GO_HEAD}+runner.${GO_RUNNER_HASH}"

RUST_TARGET=$(cargo metadata --manifest-path "$ROOT/Cargo.toml" --no-deps --format-version 1 |
    sed -n 's/.*"target_directory":"\([^"]*\)".*/\1/p')
cargo build --manifest-path "$ROOT/Cargo.toml" --release --bin prolly_compare
cp "$RUST_TARGET/release/prolly_compare" "$OUT/bin/rust-prolly-compare"

(cd "$ROOT/dolt/go" && go build -trimpath -o "$OUT/bin/dolt-go-prolly-compare" ./cmd/prolly-compare)

{
    printf 'generated_utc=%s\n' "$(date -u +%Y-%m-%dT%H:%M:%SZ)"
    printf 'host=%s\n' "$(hostname)"
    printf 'os=%s\n' "$(uname -a)"
    printf 'rust_revision=%s\n' "$RUST_REV"
    printf 'go_revision=%s\n' "$GO_REV"
    printf 'rust_binary_sha256=%s\n' "$(shasum -a 256 "$OUT/bin/rust-prolly-compare" | awk '{ print $1 }')"
    printf 'go_binary_sha256=%s\n' "$(shasum -a 256 "$OUT/bin/dolt-go-prolly-compare" | awk '{ print $1 }')"
    rustc -Vv
    go version
    printf 'sizes=%s\n' "$SIZES"
    printf 'runs=%s\n' "$RUNS"
    printf 'large_runs=%s\n' "$LARGE_RUNS"
    printf 'worker_threads=1\n'
    printf 'batch_size=whole_workload\n'
    printf 'mutation_ratio=30%%\n'
    printf 'mutation_insert_update_mix=50/50\n'
} >"$OUT/machine.txt"

printf 'implementation,revision,records,phase,workload,operation,operations,elapsed_ns,ns_per_op,ops_per_sec,workload_digest,result_count,validated,run\n' >"$OUT/results.csv"
printf 'run,implementation,records,phase,workload,exit_status,stdout,stderr,time\n' >"$OUT/manifest.csv"

run_one() {
    implementation=$1
    records=$2
    phase=$3
    workload=$4
    run=$5
    prefix="$OUT/raw/${implementation}-${records}-${phase}-${workload}-run${run}"

    if [ "$implementation" = rust ]; then
        binary="$OUT/bin/rust-prolly-compare"
        revision=$RUST_REV
    else
        binary="$OUT/bin/dolt-go-prolly-compare"
        revision=$GO_REV
    fi

    set +e
    if /usr/bin/time -l true >/dev/null 2>&1; then
        RAYON_NUM_THREADS=1 GOMAXPROCS=1 BENCH_REVISION="$revision" \
            /usr/bin/time -l "$binary" --records "$records" --phase "$phase" --workload "$workload" \
            >"$prefix.csv" 2>"$prefix.time"
        status=$?
    else
        RAYON_NUM_THREADS=1 GOMAXPROCS=1 BENCH_REVISION="$revision" \
            /usr/bin/time -v "$binary" --records "$records" --phase "$phase" --workload "$workload" \
            >"$prefix.csv" 2>"$prefix.time"
        status=$?
    fi
    set -e

    : >"$prefix.stderr"
    printf '%s,%s,%s,%s,%s,%s,%s,%s,%s\n' \
        "$run" "$implementation" "$records" "$phase" "$workload" "$status" \
        "$prefix.csv" "$prefix.stderr" "$prefix.time" >>"$OUT/manifest.csv"
    if [ "$status" -ne 0 ]; then
        printf 'benchmark failed: %s %s %s %s run %s; see %s\n' \
            "$implementation" "$records" "$phase" "$workload" "$run" "$prefix.time" >&2
        return "$status"
    fi
    awk -v run="$run" 'NR > 1 { print $0 "," run }' "$prefix.csv" >>"$OUT/results.csv"
}

for records in $SIZES; do
    repetitions=$RUNS
    if [ "$records" -ge 5000000 ]; then
        repetitions=$LARGE_RUNS
    fi
    run=1
    while [ "$run" -le "$repetitions" ]; do
        for phase in fresh mutation; do
            for workload in append random clustered; do
                if [ $(((run + records + ${#phase} + ${#workload}) % 2)) -eq 0 ]; then
                    run_one rust "$records" "$phase" "$workload" "$run"
                    run_one dolt-go "$records" "$phase" "$workload" "$run"
                else
                    run_one dolt-go "$records" "$phase" "$workload" "$run"
                    run_one rust "$records" "$phase" "$workload" "$run"
                fi
            done
        done
        run=$((run + 1))
    done
done

python3 "$ROOT/scripts/summarize_prolly_comparison.py" "$OUT/results.csv" "$OUT"
printf 'comparison complete: %s/report.md\n' "$OUT"
