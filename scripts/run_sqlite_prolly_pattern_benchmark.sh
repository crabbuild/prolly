#!/bin/sh
set -eu

ROOT=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
PROFILE=${SQLITE_PATTERN_PROFILE:-full}
OUT=${SQLITE_PATTERN_OUT:-"$ROOT/performance-results/sqlite-prolly-patterns-$(date +%Y-%m-%d)"}
RUNS=${SQLITE_PATTERN_RUNS:-}
SIZES=${SQLITE_PATTERN_SIZES:-}
KEEP=${SQLITE_PATTERN_KEEP_FIXTURES:-0}

previous=
has_output=0
has_profile=0
for argument in "$@"; do
    if [ "$previous" = "--output" ]; then
        OUT=$argument
        has_output=1
    elif [ "$previous" = "--profile" ]; then
        PROFILE=$argument
        has_profile=1
    fi
    previous=$argument
done

revision=$(git -C "$ROOT" rev-parse HEAD)
if test -n "$(git -C "$ROOT" status --porcelain)"; then
    dirty_flag=--dirty
else
    dirty_flag=--clean
fi

CARGO_INCREMENTAL=0 cargo build --release \
    --manifest-path "$ROOT/benchmarks/sqlite-prolly-patterns/Cargo.toml"

mkdir -p "$OUT"
{
    uname -a
    sysctl -n machdep.cpu.brand_string 2>/dev/null || true
    sysctl -n hw.memsize 2>/dev/null || true
    df -h "$OUT"
    rustc --version
    cargo --version
} >"$OUT/machine.txt"
cargo tree --manifest-path "$ROOT/benchmarks/sqlite-prolly-patterns/Cargo.toml" \
    >"$OUT/dependencies.txt"

set -- "$@" --revision "$revision" "$dirty_flag"
if [ "$has_output" -eq 0 ]; then
    set -- "$@" --output "$OUT"
fi
if [ "$has_profile" -eq 0 ]; then
    set -- "$@" --profile "$PROFILE"
fi
if [ -n "$RUNS" ]; then
    set -- "$@" --runs "$RUNS"
fi
if [ -n "$SIZES" ]; then
    set -- "$@" --sizes "$SIZES"
fi
if [ "$KEEP" = "1" ]; then
    set -- "$@" --keep-fixtures
fi

{
    printf 'driver=%s\n' "$ROOT/scripts/run_sqlite_prolly_pattern_benchmark.sh"
    printf 'revision=%s\n' "$revision"
    printf 'dirty_flag=%s\n' "$dirty_flag"
    printf 'arguments='
    printf ' %s' "$@"
    printf '\n'
} >"$OUT/driver-provenance.txt"

exec "$ROOT/benchmarks/sqlite-prolly-patterns/target/release/prolly-sqlite-pattern-bench" "$@"
