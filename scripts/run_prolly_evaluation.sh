#!/bin/sh
set -eu

ROOT=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
PROFILE=${BENCH_PROFILE:-canonical}
OUT=${BENCH_OUT:-"$ROOT/performance-results/prolly-evaluation-$(date -u +%Y%m%dT%H%M%SZ)"}
RESUME=${BENCH_RESUME:-0}
SKIP_BUILD=${BENCH_SKIP_BUILD:-0}
SKIP_SMOKE=${BENCH_SKIP_SMOKE:-0}
TIME_BIN=${BENCH_TIME_BIN:-/usr/bin/time}
DOLT_REPO_URL=${DOLT_REPO_URL:-https://github.com/dolthub/dolt.git}
DOLT_CACHE=${DOLT_CACHE:-"$ROOT/target/dolt-benchmark"}
DOLT_REQUESTED_REV=${DOLT_REV:-}
RUST_CACHE_PROFILES=${BENCH_RUST_CACHE_PROFILES:-"bounded unbounded"}
RUN_LIFECYCLE=${BENCH_LIFECYCLE:-1}

case "$PROFILE" in
    smoke)
        SIZES=${BENCH_SIZES:-10000}
        RUNS=${BENCH_RUNS:-1}
        DENSITIES=${BENCH_DENSITIES:-1}
        LOCALITIES=${BENCH_LOCALITIES:-random}
        RUN_LIFECYCLE=${BENCH_LIFECYCLE:-0}
        ;;
    canonical)
        SIZES=${BENCH_SIZES:-"10000 50000 1000000 5000000 10000000"}
        RUNS=${BENCH_RUNS:-3}
        DENSITIES=${BENCH_DENSITIES:-"0 1 30"}
        LOCALITIES=${BENCH_LOCALITIES:-"append random clustered"}
        ;;
    *)
        printf 'BENCH_PROFILE must be smoke or canonical: %s\n' "$PROFILE" >&2
        exit 2
        ;;
esac

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
if [ "$RESUME" != 0 ] && [ "$RESUME" != 1 ]; then
    printf 'BENCH_RESUME must be 0 or 1\n' >&2
    exit 2
fi
if [ "$RUN_LIFECYCLE" != 0 ] && [ "$RUN_LIFECYCLE" != 1 ]; then
    printf 'BENCH_LIFECYCLE must be 0 or 1\n' >&2
    exit 2
fi

sha256_file() {
    shasum -a 256 "$1" | awk '{ print $1 }'
}

source_tree_hash() {
    (
        cd "$ROOT"
        set -- src Cargo.toml
        if [ -f Cargo.lock ]; then
            set -- "$@" Cargo.lock
        fi
        find "$@" -type f |
            LC_ALL=C sort |
            while IFS= read -r source_file; do
                printf '%s\t%s\n' "$source_file" "$(sha256_file "$source_file")"
            done |
            shasum -a 256 |
            awk '{ print $1 }'
    )
}

directory_hash() {
    (
        cd "$1"
        find . -type f |
            LC_ALL=C sort |
            while IFS= read -r source_file; do
                printf '%s\t%s\n' "$source_file" "$(sha256_file "$source_file")"
            done |
            shasum -a 256 |
            awk '{ print $1 }'
    )
}

framework_hash() {
    for source_file in \
        scripts/run_prolly_evaluation.sh \
        scripts/prolly_evaluation_runner.py \
        scripts/summarize_prolly_evaluation.py \
        scripts/prolly_process_metrics.py; do
        printf '%s\t%s\n' "$source_file" "$(sha256_file "$ROOT/$source_file")"
    done | shasum -a 256 | awk '{ print $1 }'
}

require_executable() {
    if [ ! -x "$1" ]; then
        printf 'required executable is missing: %s\n' "$1" >&2
        exit 2
    fi
}

manifest_value() {
    sed -n "s/^$1=//p" "$OUT/manifest.txt"
}

TREE_GO_SOURCE="$ROOT/benchmarks/dolt-prolly-compare"
VERSION_GO_SOURCE="$ROOT/benchmarks/dolt-prolly-version-compare"

RESUME_FROM_ARTIFACTS=0
if [ "$RESUME" -eq 1 ] && [ -f "$OUT/manifest.txt" ] && [ "${BENCH_REBUILD_ON_RESUME:-0}" -ne 1 ]; then
    RESUME_FROM_ARTIFACTS=1
fi

if [ "$RESUME_FROM_ARTIFACTS" -eq 1 ]; then
    RUST_TREE_BIN="$OUT/bin/rust-prolly-compare"
    RUST_VERSION_BIN="$OUT/bin/rust-prolly-version-compare"
    RUST_LIFECYCLE_BIN="$OUT/bin/rust-prolly-version-lifecycle"
    GO_TREE_BIN="$OUT/bin/dolt-go-prolly-compare"
    GO_VERSION_BIN="$OUT/bin/dolt-go-prolly-version-compare"
    RUST_COMMIT=$(manifest_value rust_commit)
    DOLT_SHA=$(manifest_value dolt_commit)
elif [ "$SKIP_BUILD" -eq 1 ]; then
    RUST_TREE_BIN=${PROLLY_EVAL_RUST_TREE_BIN:?PROLLY_EVAL_RUST_TREE_BIN is required with BENCH_SKIP_BUILD=1}
    RUST_VERSION_BIN=${PROLLY_EVAL_RUST_VERSION_BIN:?PROLLY_EVAL_RUST_VERSION_BIN is required with BENCH_SKIP_BUILD=1}
    RUST_LIFECYCLE_BIN=${PROLLY_EVAL_RUST_LIFECYCLE_BIN:?PROLLY_EVAL_RUST_LIFECYCLE_BIN is required with BENCH_SKIP_BUILD=1}
    GO_TREE_BIN=${PROLLY_EVAL_GO_TREE_BIN:?PROLLY_EVAL_GO_TREE_BIN is required with BENCH_SKIP_BUILD=1}
    GO_VERSION_BIN=${PROLLY_EVAL_GO_VERSION_BIN:?PROLLY_EVAL_GO_VERSION_BIN is required with BENCH_SKIP_BUILD=1}
    RUST_COMMIT=$(git -C "$ROOT" rev-parse HEAD 2>/dev/null || printf unknown)
    DOLT_SHA=${DOLT_REQUESTED_REV:-external}
else
    if [ ! -d "$DOLT_CACHE/.git" ]; then
        if [ -e "$DOLT_CACHE" ]; then
            printf 'DOLT_CACHE exists but is not a git checkout: %s\n' "$DOLT_CACHE" >&2
            exit 2
        fi
        mkdir -p "$(dirname -- "$DOLT_CACHE")"
        git clone --filter=blob:none --no-checkout "$DOLT_REPO_URL" "$DOLT_CACHE"
    fi
    if [ -n "$DOLT_REQUESTED_REV" ]; then
        if ! git -C "$DOLT_CACHE" rev-parse --verify "${DOLT_REQUESTED_REV}^{commit}" >/dev/null 2>&1; then
            git -C "$DOLT_CACHE" fetch origin "$DOLT_REQUESTED_REV"
        fi
        DOLT_SHA=$(git -C "$DOLT_CACHE" rev-parse --verify "${DOLT_REQUESTED_REV}^{commit}")
    else
        git -C "$DOLT_CACHE" fetch --prune origin main
        DOLT_SHA=$(git -C "$DOLT_CACHE" rev-parse origin/main)
    fi
    git -C "$DOLT_CACHE" checkout --detach "$DOLT_SHA"

    mkdir -p "$DOLT_CACHE/go/cmd/prolly-compare"
    cp "$TREE_GO_SOURCE/main.go" "$DOLT_CACHE/go/cmd/prolly-compare/main.go"
    cp "$TREE_GO_SOURCE/main_test.go" "$DOLT_CACHE/go/cmd/prolly-compare/main_test.go"
    mkdir -p "$DOLT_CACHE/go/cmd/prolly-version-compare"
    cp "$VERSION_GO_SOURCE/main.go" "$DOLT_CACHE/go/cmd/prolly-version-compare/main.go"
    cp "$VERSION_GO_SOURCE/workload.go" "$DOLT_CACHE/go/cmd/prolly-version-compare/workload.go"

    RUST_TARGET=$(cargo metadata --manifest-path "$ROOT/Cargo.toml" --no-deps --format-version 1 |
        sed -n 's/.*"target_directory":"\([^"]*\)".*/\1/p')
    if [ -z "$RUST_TARGET" ]; then
        printf 'cargo metadata did not return a target directory\n' >&2
        exit 2
    fi
    cargo build --manifest-path "$ROOT/Cargo.toml" --release \
        --bin prolly_compare --bin prolly_version_compare --bin prolly_version_lifecycle
    RUST_TREE_BIN="$RUST_TARGET/release/prolly_compare"
    RUST_VERSION_BIN="$RUST_TARGET/release/prolly_version_compare"
    RUST_LIFECYCLE_BIN="$RUST_TARGET/release/prolly_version_lifecycle"
    (
        cd "$DOLT_CACHE/go"
        go test ./cmd/prolly-compare ./cmd/prolly-version-compare
        go build -trimpath -o "$DOLT_CACHE/go/prolly-compare" ./cmd/prolly-compare
        go build -trimpath -o "$DOLT_CACHE/go/prolly-version-compare" ./cmd/prolly-version-compare
    )
    GO_TREE_BIN="$DOLT_CACHE/go/prolly-compare"
    GO_VERSION_BIN="$DOLT_CACHE/go/prolly-version-compare"
    RUST_COMMIT=$(git -C "$ROOT" rev-parse HEAD 2>/dev/null || printf unknown)
fi

for binary in "$RUST_TREE_BIN" "$RUST_VERSION_BIN" "$RUST_LIFECYCLE_BIN" "$GO_TREE_BIN" "$GO_VERSION_BIN" "$TIME_BIN"; do
    require_executable "$binary"
done

RUST_SOURCE_HASH=$(source_tree_hash)
FRAMEWORK_HASH=$(framework_hash)
TREE_GO_SOURCE_HASH=$(directory_hash "$TREE_GO_SOURCE")
VERSION_GO_SOURCE_HASH=$(directory_hash "$VERSION_GO_SOURCE")
RUST_REVISION="$(printf '%s' "$RUST_COMMIT" | cut -c1-12)+src.$(printf '%s' "$RUST_SOURCE_HASH" | cut -c1-12)"
DOLT_REVISION="$(printf '%s' "$DOLT_SHA" | cut -c1-12)+tree.$(printf '%s' "$TREE_GO_SOURCE_HASH" | cut -c1-8)+version.$(printf '%s' "$VERSION_GO_SOURCE_HASH" | cut -c1-8)"

if "$TIME_BIN" -l -o /dev/null /usr/bin/true >/dev/null 2>&1; then
    TIME_MODE=-l
else
    TIME_MODE=-v
fi

CONFIG_FILE=$(mktemp "${TMPDIR:-/tmp}/prolly-evaluation-config.XXXXXX")
cleanup_config() {
    rm -f "$CONFIG_FILE"
}
trap cleanup_config EXIT HUP INT TERM
{
    printf 'schema=prolly-evaluation-v1\n'
    printf 'profile=%s\n' "$PROFILE"
    printf 'rust_commit=%s\n' "$RUST_COMMIT"
    printf 'rust_source_sha256=%s\n' "$RUST_SOURCE_HASH"
    printf 'rust_revision=%s\n' "$RUST_REVISION"
    printf 'dolt_repository=%s\n' "$DOLT_REPO_URL"
    printf 'dolt_commit=%s\n' "$DOLT_SHA"
    printf 'dolt_revision=%s\n' "$DOLT_REVISION"
    printf 'tree_go_source_sha256=%s\n' "$TREE_GO_SOURCE_HASH"
    printf 'version_go_source_sha256=%s\n' "$VERSION_GO_SOURCE_HASH"
    printf 'framework_sha256=%s\n' "$FRAMEWORK_HASH"
    printf 'rust_tree_binary_sha256=%s\n' "$(sha256_file "$RUST_TREE_BIN")"
    printf 'rust_version_binary_sha256=%s\n' "$(sha256_file "$RUST_VERSION_BIN")"
    printf 'rust_lifecycle_binary_sha256=%s\n' "$(sha256_file "$RUST_LIFECYCLE_BIN")"
    printf 'go_tree_binary_sha256=%s\n' "$(sha256_file "$GO_TREE_BIN")"
    printf 'go_version_binary_sha256=%s\n' "$(sha256_file "$GO_VERSION_BIN")"
    printf 'time_binary_sha256=%s\n' "$(sha256_file "$TIME_BIN")"
    printf 'sizes=%s\n' "$SIZES"
    printf 'runs=%s\n' "$RUNS"
    printf 'densities=%s\n' "$DENSITIES"
    printf 'localities=%s\n' "$LOCALITIES"
    printf 'rust_cache_profiles=%s\n' "$RUST_CACHE_PROFILES"
    printf 'lifecycle=%s\n' "$RUN_LIFECYCLE"
    printf 'worker_threads=1\n'
    printf 'storage=in-memory\n'
    printf 'time_mode=%s\n' "$TIME_MODE"
} >"$CONFIG_FILE"
FINGERPRINT=$(sha256_file "$CONFIG_FILE")
printf 'fingerprint=%s\n' "$FINGERPRINT" >>"$CONFIG_FILE"

if [ -e "$OUT" ] && [ "$RESUME" -ne 1 ]; then
    printf 'output already exists; use a new BENCH_OUT or BENCH_RESUME=1: %s\n' "$OUT" >&2
    exit 2
fi
mkdir -p "$OUT"
LOCK_DIR="$OUT/.lock"
if ! mkdir "$LOCK_DIR" 2>/dev/null; then
    if [ "$RESUME" -eq 1 ] && [ -f "$LOCK_DIR/pid" ]; then
        lock_pid=$(sed -n '1p' "$LOCK_DIR/pid")
        if ! kill -0 "$lock_pid" 2>/dev/null; then
            rm -rf "$LOCK_DIR"
            mkdir "$LOCK_DIR"
        else
            printf 'evaluation output is locked by pid %s: %s\n' "$lock_pid" "$OUT" >&2
            exit 2
        fi
    else
        printf 'evaluation output is locked: %s\n' "$OUT" >&2
        exit 2
    fi
fi
printf '%s\n' "$$" >"$LOCK_DIR/pid"
cleanup_all() {
    rm -rf "$LOCK_DIR"
    cleanup_config
}
trap cleanup_all EXIT HUP INT TERM

if [ "$RESUME" -eq 1 ]; then
    if [ ! -f "$OUT/manifest.txt" ]; then
        printf 'cannot resume without manifest.txt: %s\n' "$OUT" >&2
        exit 2
    fi
    if ! cmp -s "$CONFIG_FILE" "$OUT/manifest.txt"; then
        printf 'resume provenance mismatch; configuration or binaries changed: %s\n' "$OUT" >&2
        diff -u "$OUT/manifest.txt" "$CONFIG_FILE" >&2 || true
        exit 2
    fi
else
    cp "$CONFIG_FILE" "$OUT/manifest.txt"
    mkdir -p "$OUT/bin"
    cp "$RUST_TREE_BIN" "$OUT/bin/rust-prolly-compare"
    cp "$RUST_VERSION_BIN" "$OUT/bin/rust-prolly-version-compare"
    cp "$RUST_LIFECYCLE_BIN" "$OUT/bin/rust-prolly-version-lifecycle"
    cp "$GO_TREE_BIN" "$OUT/bin/dolt-go-prolly-compare"
    cp "$GO_VERSION_BIN" "$OUT/bin/dolt-go-prolly-version-compare"
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
fi

# COMPLETE is a commit marker for the fully validated artifact set, not merely a
# record that an earlier invocation once reached the end.
rm -f "$OUT/COMPLETE"

set -- python3 "$ROOT/scripts/prolly_evaluation_runner.py" \
    --output "$OUT" \
    --fingerprint "$FINGERPRINT" \
    --rust-revision "$RUST_REVISION" \
    --go-revision "$DOLT_REVISION" \
    --rust-tree "$OUT/bin/rust-prolly-compare" \
    --go-tree "$OUT/bin/dolt-go-prolly-compare" \
    --rust-version "$OUT/bin/rust-prolly-version-compare" \
    --go-version "$OUT/bin/dolt-go-prolly-version-compare" \
    --rust-lifecycle "$OUT/bin/rust-prolly-version-lifecycle" \
    --sizes "$SIZES" \
    --runs "$RUNS" \
    --densities "$DENSITIES" \
    --localities "$LOCALITIES" \
    --rust-cache-profiles "$RUST_CACHE_PROFILES" \
    --lifecycle "$RUN_LIFECYCLE" \
    --time-bin "$TIME_BIN" \
    --time-mode="$TIME_MODE"
if [ "$RESUME" -eq 1 ]; then
    set -- "$@" --resume
fi
if [ "$SKIP_SMOKE" -eq 1 ]; then
    set -- "$@" --skip-smoke
fi
"$@"
python3 "$ROOT/scripts/summarize_prolly_evaluation.py" --output "$OUT"
printf 'evaluation complete: %s/report.md\n' "$OUT"
