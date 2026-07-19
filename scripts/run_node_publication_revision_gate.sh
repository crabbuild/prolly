#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
CANDIDATE_SOURCE="$(cd "$SCRIPT_DIR/.." && pwd)"
SUITE=""
BASELINE_REPO=""
CANDIDATE_REPO=""
OUTPUT=""
SIZES=""
RUNS=""
CHANGES=""
APIS=""
PATTERNS=""
ADAPTERS=""
ISOLATE_CELLS=false
MEASUREMENT_SAMPLES=1
MINIMUM_PAIRS=5

usage() {
  printf '%s\n' 'usage: run_node_publication_revision_gate.sh --suite foundation|sqlite-turso --baseline-repo PATH --candidate-repo PATH --output PATH --sizes CSV --runs N --changes N|auto --apis CSV --patterns CSV --adapters CSV [--measurement-samples N] [--minimum-pairs N] [--isolate-cells]'
}

while (($#)); do
  case "$1" in
    --suite) SUITE="${2:?--suite requires a value}"; shift 2 ;;
    --baseline-repo) BASELINE_REPO="${2:?--baseline-repo requires a value}"; shift 2 ;;
    --candidate-repo) CANDIDATE_REPO="${2:?--candidate-repo requires a value}"; shift 2 ;;
    --output) OUTPUT="${2:?--output requires a value}"; shift 2 ;;
    --sizes) SIZES="${2:?--sizes requires a value}"; shift 2 ;;
    --runs) RUNS="${2:?--runs requires a value}"; shift 2 ;;
    --changes) CHANGES="${2:?--changes requires a value}"; shift 2 ;;
    --apis) APIS="${2:?--apis requires a value}"; shift 2 ;;
    --patterns) PATTERNS="${2:?--patterns requires a value}"; shift 2 ;;
    --adapters) ADAPTERS="${2:?--adapters requires a value}"; shift 2 ;;
    --measurement-samples) MEASUREMENT_SAMPLES="${2:?--measurement-samples requires a value}"; shift 2 ;;
    --minimum-pairs) MINIMUM_PAIRS="${2:?--minimum-pairs requires a value}"; shift 2 ;;
    --isolate-cells) ISOLATE_CELLS=true; shift ;;
    --help|-h) usage; exit 0 ;;
    *) printf 'unknown argument: %s\n' "$1" >&2; usage >&2; exit 2 ;;
  esac
done

if [[ "$SUITE" != "foundation" && "$SUITE" != "sqlite-turso" ]]; then
  printf 'suite must be foundation or sqlite-turso\n' >&2
  exit 2
fi
for required in BASELINE_REPO CANDIDATE_REPO OUTPUT SIZES RUNS CHANGES APIS PATTERNS ADAPTERS; do
  if [[ -z "${!required}" ]]; then
    printf 'missing required argument: %s\n' "$required" >&2
    usage >&2
    exit 2
  fi
done
if [[ ! "$RUNS" =~ ^[1-9][0-9]*$ ]]; then
  printf 'runs must be a positive integer\n' >&2
  exit 2
fi
if [[ "$CHANGES" != "auto" && ! "$CHANGES" =~ ^[1-9][0-9]*$ ]]; then
  printf 'changes must be a positive integer or auto\n' >&2
  exit 2
fi
if [[ ! "$MEASUREMENT_SAMPLES" =~ ^[1-9][0-9]*$ ]] || ((MEASUREMENT_SAMPLES > 254)); then
  printf 'measurement samples must be between 1 and 254\n' >&2
  exit 2
fi
if [[ ! "$MINIMUM_PAIRS" =~ ^[1-9][0-9]*$ ]]; then
  printf 'minimum pairs must be a positive integer\n' >&2
  exit 2
fi
if [[ -n "$(git -C "$BASELINE_REPO" status --porcelain)" ]]; then
  printf 'refusing dirty baseline repository: %s\n' "$BASELINE_REPO" >&2
  exit 2
fi
if [[ -e "$OUTPUT/raw-results.csv" ]]; then
  printf 'refusing existing combined result: %s\n' "$OUTPUT/raw-results.csv" >&2
  exit 2
fi

WORK_ROOT="$(mktemp -d "${TMPDIR:-/tmp}/prolly-node-publication-gate.XXXXXX")"
cleanup() {
  rm -rf -- "$WORK_ROOT"
}
trap cleanup EXIT
TARGET_ROOT="${PROLLY_NODE_GATE_TARGET_ROOT:-$WORK_ROOT/targets}"
mkdir -p "$OUTPUT" "$WORK_ROOT/build" "$TARGET_ROOT" "$WORK_ROOT/invocations"
BASELINE_REVISION="$(git -C "$BASELINE_REPO" rev-parse HEAD)"
CANDIDATE_REVISION="$(git -C "$CANDIDATE_REPO" rev-parse HEAD)"
if [[ -n "$(git -C "$CANDIDATE_REPO" status --porcelain)" ]]; then
  CANDIDATE_DIRTY="--dirty"
else
  CANDIDATE_DIRTY="--clean"
fi

capture_machine() {
  {
    printf 'captured_utc=%s\n' "$(date -u +%Y-%m-%dT%H:%M:%SZ)"
    uname -a
    rustc --version
    cargo --version
    sysctl -n machdep.cpu.brand_string 2>/dev/null || true
    sysctl -n hw.memsize 2>/dev/null || true
    df -h "$OUTPUT"
  } > "$OUTPUT/machine.txt"
  {
    printf 'suite=%s\n' "$SUITE"
    printf 'baseline_revision=%s\n' "$BASELINE_REVISION"
    printf 'candidate_revision=%s\n' "$CANDIDATE_REVISION"
    printf 'sizes=%s\nruns=%s\nchanges=%s\napis=%s\npatterns=%s\nadapters=%s\n' \
      "$SIZES" "$RUNS" "$CHANGES" "$APIS" "$PATTERNS" "$ADAPTERS"
    printf 'execution=alternating-local-only\nisolate_cells=%s\nmeasurement_samples=%s\nminimum_pairs=%s\ncloud_sync=disabled\n' \
      "$ISOLATE_CELLS" "$MEASUREMENT_SAMPLES" "$MINIMUM_PAIRS"
    if [[ "$SUITE" == "foundation" ]]; then
      printf 'foundation_samples=%s\n' "${PROLLY_FOUNDATION_SAMPLES:-30}"
    fi
  } > "$OUTPUT/provenance.txt"
}

build_foundation() {
  local role="$1"
  local repo="$2"
  local stage="$WORK_ROOT/build/foundation-$role"
  local target="$TARGET_ROOT/foundation-$role"
  mkdir -p "$stage/src" "$target"
  cp "$CANDIDATE_SOURCE/benches/async_first_foundation_bench.rs" "$stage/src/main.rs"
  {
    printf '[package]\nname = "prolly-foundation-revision-bench"\nversion = "0.0.0"\nedition = "2021"\npublish = false\n\n'
    printf '[dependencies]\n'
    printf 'prolly = { package = "prolly-map", path = "%s", features = ["async-store"] }\n' "$repo"
    printf 'futures-util = "0.3"\n'
    printf '\n[features]\nbaseline-contract = []\n'
  } > "$stage/Cargo.toml"
  if [[ "$role" == "baseline" ]]; then
    CARGO_TARGET_DIR="$target" CARGO_INCREMENTAL=0 CARGO_PROFILE_RELEASE_DEBUG=0 \
      cargo build --release --manifest-path "$stage/Cargo.toml" \
        --features baseline-contract
  else
    CARGO_TARGET_DIR="$target" CARGO_INCREMENTAL=0 CARGO_PROFILE_RELEASE_DEBUG=0 \
      cargo build --release --manifest-path "$stage/Cargo.toml"
  fi
  printf '%s\n' "$target/release/prolly-foundation-revision-bench"
}

build_sqlite_turso() {
  local role="$1"
  local repo="$2"
  local stage="$WORK_ROOT/build/sqlite-turso-$role"
  local manifest="$stage/Cargo.toml"
  local target="$TARGET_ROOT/sqlite-turso-$role"
  if [[ ! -f "$repo/stores/prolly-store-sqlite/Cargo.toml" \
    || ! -f "$repo/stores/prolly-store-turso/Cargo.toml" ]]; then
    printf 'revision %s lacks a required SQLite/Turso adapter\n' "$repo" >&2
    exit 2
  fi
  mkdir -p "$stage" "$target"
  cp -R "$CANDIDATE_SOURCE/benchmarks/sqlite-turso-local/src" "$stage/src"
  cp "$CANDIDATE_SOURCE/benchmarks/sqlite-turso-local/Cargo.lock" "$stage/Cargo.lock"
  {
    printf '[package]\nname = "prolly-sqlite-turso-local-bench"\nversion = "0.0.0"\nedition = "2021"\nrust-version = "1.88"\npublish = false\n\n'
    printf '[dependencies]\n'
    printf 'csv = "1.3"\nfs2 = "0.4"\n'
    printf 'prolly = { package = "prolly-map", path = "%s", features = ["async-store"] }\n' "$repo"
    printf 'prolly-store-sqlite = { path = "%s/stores/prolly-store-sqlite" }\n' "$repo"
    printf 'prolly-store-turso = { path = "%s/stores/prolly-store-turso" }\n' "$repo"
    printf 'serde = { version = "1.0", features = ["derive"] }\n'
    printf 'tempfile = "3.20"\n'
    printf 'tokio = { version = "1.45", features = ["macros", "rt-multi-thread"] }\n\n'
    printf '[lints.rust]\nunsafe_code = "forbid"\n'
  } > "$manifest"
  CARGO_TARGET_DIR="$target" CARGO_INCREMENTAL=0 CARGO_PROFILE_RELEASE_DEBUG=0 \
    cargo build --release --locked --manifest-path "$manifest" \
      --bin prolly-sqlite-turso-local-bench
  CARGO_TARGET_DIR="$target" cargo tree --manifest-path "$manifest" -e features \
    > "$OUTPUT/dependency-features-$role.txt"
  if rg -q 'prolly-store-turso feature "turso-cloud-sync"' \
    "$OUTPUT/dependency-features-$role.txt"; then
    printf 'refusing %s: turso-cloud-sync is enabled\n' "$role" >&2
    exit 2
  fi
  printf '%s\n' "$target/release/prolly-sqlite-turso-local-bench"
}

append_csv() {
  local suite="$1"
  local role="$2"
  local pair="$3"
  local source="$4"
  local combined="$OUTPUT/raw-results.csv"
  if [[ ! -s "$source" ]]; then
    printf 'benchmark emitted no CSV: %s\n' "$source" >&2
    exit 2
  fi
  if [[ ! -e "$combined" ]]; then
    {
      printf 'suite,revision_role,pair,'
      head -n 1 "$source"
    } > "$combined"
  fi
  awk -v suite="$suite" -v role="$role" -v pair="$pair" \
    'NR > 1 { print suite "," role "," pair "," $0 }' "$source" >> "$combined"
}

resolved_changes() {
  local records="$1"
  if [[ "$CHANGES" != "auto" ]]; then
    printf '%s\n' "$CHANGES"
    return
  fi
  local value=$((records / 100))
  if ((value < 100)); then value=100; fi
  if ((value > 10000)); then value=10000; fi
  if ((value > records)); then value="$records"; fi
  printf '%s\n' "$value"
}

capture_machine
if [[ "$SUITE" == "foundation" ]]; then
  BASELINE_BIN="$(build_foundation baseline "$BASELINE_REPO")"
  CANDIDATE_BIN="$(build_foundation candidate "$CANDIDATE_REPO")"
else
  BASELINE_BIN="$(build_sqlite_turso baseline "$BASELINE_REPO")"
  CANDIDATE_BIN="$(build_sqlite_turso candidate "$CANDIDATE_REPO")"
fi

run_role() {
  local role="$1"
  local pair="$2"
  local records="$3"
  local cell_changes="$4"
  local cell_apis="$5"
  local cell_patterns="$6"
  local cell_adapters="$7"
  local cell_tag="$8"
  local binary revision dirty_flag invocation

  if [[ "$role" == "baseline" ]]; then
    binary="$BASELINE_BIN"
    revision="$BASELINE_REVISION"
    dirty_flag="--clean"
  else
    binary="$CANDIDATE_BIN"
    revision="$CANDIDATE_REVISION"
    dirty_flag="$CANDIDATE_DIRTY"
  fi
  invocation="$WORK_ROOT/invocations/pair-$pair/records-$records/$cell_tag/$role"
  mkdir -p "$invocation"
  if [[ "$SUITE" == "foundation" ]]; then
    BENCH_REVISION="$revision" \
      PROLLY_FOUNDATION_RECORDS="$records" \
      PROLLY_FOUNDATION_CHANGES="$cell_changes" \
      PROLLY_FOUNDATION_APIS="$cell_apis" \
      "$binary" > "$invocation/raw.csv"
    append_csv foundation "$role" "$pair" "$invocation/raw.csv"
  else
    "$binary" \
      --profile smoke \
      --output "$invocation" \
      --revision "$revision" \
      "$dirty_flag" \
      --sizes "$records" \
      --runs 1 \
      --changes "$cell_changes" \
      --measurement-samples "$MEASUREMENT_SAMPLES" \
      --apis "$cell_apis" \
      --patterns "$cell_patterns" \
      --adapters "$cell_adapters"
    append_csv sqlite-turso "$role" "$pair" "$invocation/raw-results.csv"
  fi
}

IFS=',' read -r -a SIZE_VALUES <<< "$SIZES"
for pair in $(seq 1 "$RUNS"); do
  if ((pair % 2 == 1)); then
    ORDER=(baseline candidate)
  else
    ORDER=(candidate baseline)
  fi
  for records in "${SIZE_VALUES[@]}"; do
    if [[ ! "$records" =~ ^[1-9][0-9]*$ ]]; then
      printf 'invalid record size: %s\n' "$records" >&2
      exit 2
    fi
    cell_changes="$(resolved_changes "$records")"
    if [[ "$SUITE" == "sqlite-turso" && "$ISOLATE_CELLS" == true ]]; then
      IFS=',' read -r -a API_VALUES <<< "$APIS"
      IFS=',' read -r -a PATTERN_VALUES <<< "$PATTERNS"
      IFS=',' read -r -a ADAPTER_VALUES <<< "$ADAPTERS"
      for adapter in "${ADAPTER_VALUES[@]}"; do
        for api in "${API_VALUES[@]}"; do
          for pattern in "${PATTERN_VALUES[@]}"; do
            cell_tag="$adapter/$api/$pattern"
            for role in "${ORDER[@]}"; do
              run_role "$role" "$pair" "$records" "$cell_changes" \
                "$api" "$pattern" "$adapter" "$cell_tag"
            done
          done
        done
      done
    else
      for role in "${ORDER[@]}"; do
        run_role "$role" "$pair" "$records" "$cell_changes" \
          "$APIS" "$PATTERNS" "$ADAPTERS" all-cells
      done
    fi
  done
done

python3 "$SCRIPT_DIR/summarize_node_publication_revision_gate.py" \
  --input "$OUTPUT/raw-results.csv" \
  --output-dir "$OUTPUT" \
  --minimum-pairs "$MINIMUM_PAIRS"

printf 'node-publication revision gate complete: %s\n' "$OUTPUT"
