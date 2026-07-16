#!/bin/sh
set -eu

ROOT=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
OUT=${BENCH_OUT:-"$ROOT/performance-results/go-binding-vs-native-go"}
SIZES=${BENCH_SIZES:-"10000 50000 1000000 5000000 10000000"}
RUNS=${BENCH_RUNS:-3}
LARGE_RUNS=${BENCH_LARGE_RUNS:-3}
RESUME=${BENCH_RESUME:-0}

case "$OUT" in
    /*) ;;
    *) OUT="$ROOT/$OUT" ;;
esac

if [ ! -f "$ROOT/dolt/go/cmd/prolly-compare/main.go" ]; then
    printf 'missing native Go runner: initialize the Dolt checkout at %s/dolt first\n' "$ROOT" >&2
    exit 1
fi

mkdir -p "$OUT/bin" "$OUT/raw"

RUST_HEAD=$(git -C "$ROOT" rev-parse --short=12 HEAD 2>/dev/null || printf unknown)
BINDING_SOURCE_HASH=$(
    find "$ROOT/src" "$ROOT/bindings/uniffi/src" "$ROOT/bindings/go" \
        "$ROOT/Cargo.toml" "$ROOT/bindings/uniffi/Cargo.toml" -type f \
        ! -path '*/target/*' |
        LC_ALL=C sort |
        while IFS= read -r file; do shasum "$file"; done |
        shasum | awk '{ print substr($1, 1, 12) }'
)
BINDING_REV="${RUST_HEAD}+binding.${BINDING_SOURCE_HASH}"
GO_HEAD=$(git -C "$ROOT/dolt" rev-parse --short=12 HEAD 2>/dev/null || printf unknown)
GO_RUNNER_HASH=$(shasum "$ROOT/dolt/go/cmd/prolly-compare/main.go" | awk '{ print substr($1, 1, 12) }')
GO_REV="${GO_HEAD}+runner.${GO_RUNNER_HASH}"

if [ "$RESUME" = 1 ] && [ -f "$OUT/machine.txt" ]; then
    BINDING_REV=$(sed -n 's/^binding_revision=//p' "$OUT/machine.txt")
    GO_REV=$(sed -n 's/^native_go_revision=//p' "$OUT/machine.txt")
fi

cargo build --manifest-path "$ROOT/Cargo.toml" --release -p prolly-bindings --target-dir "$ROOT/target"
(cd "$ROOT/bindings/go" && go build -tags prolly_release -trimpath \
    -o "$OUT/bin/rust-go-binding-prolly-compare" ./cmd/prolly-compare)
(cd "$ROOT/dolt/go" && go build -trimpath -o "$OUT/bin/native-go-prolly-compare" ./cmd/prolly-compare)

if [ "$RESUME" = 1 ] && [ -f "$OUT/machine.txt" ]; then
    EXPECTED_BINDING_SHA=$(sed -n 's/^binding_binary_sha256=//p' "$OUT/machine.txt")
    EXPECTED_NATIVE_SHA=$(sed -n 's/^native_go_binary_sha256=//p' "$OUT/machine.txt")
    ACTUAL_BINDING_SHA=$(shasum -a 256 "$OUT/bin/rust-go-binding-prolly-compare" | awk '{ print $1 }')
    ACTUAL_NATIVE_SHA=$(shasum -a 256 "$OUT/bin/native-go-prolly-compare" | awk '{ print $1 }')
    if [ "$ACTUAL_BINDING_SHA" != "$EXPECTED_BINDING_SHA" ] || [ "$ACTUAL_NATIVE_SHA" != "$EXPECTED_NATIVE_SHA" ]; then
        printf 'refusing resume: rebuilt benchmark binary hash differs from machine.txt\n' >&2
        exit 1
    fi
fi

BINDING_LIBRARY="$ROOT/target/release/libprolly_bindings.dylib"
if [ "$(uname -s)" = Linux ]; then
    BINDING_LIBRARY="$ROOT/target/release/libprolly_bindings.so"
fi
if [ ! -f "$BINDING_LIBRARY" ]; then
    printf 'release binding library not found: %s\n' "$BINDING_LIBRARY" >&2
    exit 1
fi

if [ "$RESUME" != 1 ] || [ ! -f "$OUT/machine.txt" ]; then
{
    printf 'generated_utc=%s\n' "$(date -u +%Y-%m-%dT%H:%M:%SZ)"
    printf 'host=%s\n' "$(hostname)"
    printf 'os=%s\n' "$(uname -a)"
    printf 'binding_revision=%s\n' "$BINDING_REV"
    printf 'native_go_revision=%s\n' "$GO_REV"
    printf 'binding_binary_sha256=%s\n' "$(shasum -a 256 "$OUT/bin/rust-go-binding-prolly-compare" | awk '{ print $1 }')"
    printf 'binding_library=%s\n' "$BINDING_LIBRARY"
    printf 'binding_library_sha256=%s\n' "$(shasum -a 256 "$BINDING_LIBRARY" | awk '{ print $1 }')"
    printf 'native_go_binary_sha256=%s\n' "$(shasum -a 256 "$OUT/bin/native-go-prolly-compare" | awk '{ print $1 }')"
    if command -v otool >/dev/null 2>&1; then
        printf 'binding_dynamic_dependency=%s\n' "$(otool -L "$OUT/bin/rust-go-binding-prolly-compare" | sed -n '2p' | awk '{$1=$1; print}')"
    elif command -v ldd >/dev/null 2>&1; then
        printf 'binding_dynamic_dependency=%s\n' "$(ldd "$OUT/bin/rust-go-binding-prolly-compare" | sed -n '/prolly_bindings/{p;q;}')"
    fi
    rustc -Vv
    go version
    printf 'sizes=%s\n' "$SIZES"
    printf 'runs=%s\n' "$RUNS"
    printf 'large_runs=%s\n' "$LARGE_RUNS"
    printf 'worker_threads=1\n'
    printf 'storage=in-memory\n'
    printf 'binding_build=release\n'
    printf 'binding_point_api=Engine.Get per key\n'
    printf 'binding_scan_api=Engine.ScanRange with 1024-entry pages\n'
    printf 'batch_size=whole_workload\n'
    printf 'mutation_ratio=30%%\n'
    printf 'mutation_insert_update_mix=50/50\n'
} >"$OUT/machine.txt"
fi

if [ "$RESUME" != 1 ] || [ ! -f "$OUT/results.csv" ] || [ ! -f "$OUT/manifest.csv" ]; then
    printf 'implementation,revision,records,phase,workload,operation,operations,elapsed_ns,ns_per_op,ops_per_sec,workload_digest,result_count,validated,run\n' >"$OUT/results.csv"
    printf 'run,implementation,records,phase,workload,exit_status,stdout,stderr,time\n' >"$OUT/manifest.csv"
fi

run_one() {
    implementation=$1
    records=$2
    phase=$3
    workload=$4
    run=$5
    prefix="$OUT/raw/${implementation}-${records}-${phase}-${workload}-run${run}"

    if [ "$RESUME" = 1 ] && awk -F, \
        -v run="$run" -v implementation="$implementation" -v records="$records" \
        -v phase="$phase" -v workload="$workload" \
        'NR > 1 && $1 == run && $2 == implementation && $3 == records && $4 == phase && $5 == workload && $6 == 0 { found=1 } END { exit !found }' \
        "$OUT/manifest.csv"; then
        return 0
    fi

    if [ "$implementation" = rust-go-binding ]; then
        binary="$OUT/bin/rust-go-binding-prolly-compare"
        revision=$BINDING_REV
    else
        binary="$OUT/bin/native-go-prolly-compare"
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
    awk -F, -v OFS=, -v run="$run" -v implementation="$implementation" \
        'NR > 1 { $1=implementation; print $0 "," run }' "$prefix.csv" >>"$OUT/results.csv"
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
                    run_one rust-go-binding "$records" "$phase" "$workload" "$run"
                    run_one native-go "$records" "$phase" "$workload" "$run"
                else
                    run_one native-go "$records" "$phase" "$workload" "$run"
                    run_one rust-go-binding "$records" "$phase" "$workload" "$run"
                fi
            done
        done
        run=$((run + 1))
    done
done

python3 "$ROOT/scripts/summarize_go_binding_comparison.py" "$OUT/results.csv" "$OUT"
printf 'comparison complete: %s/report.md\n' "$OUT"
