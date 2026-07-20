#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
CANDIDATE_SOURCE="$(cd "$SCRIPT_DIR/.." && pwd)"
BASELINE_REPO=""
CANDIDATE_REPO=""
OUTPUT=""
RECORDS=""
CHANGES=""
RUNS=""
APIS=""
PATTERNS=""
ADAPTERS=""
MEASUREMENT_SAMPLES="1"

usage() {
  printf '%s\n' 'usage: run_local_store_publication_revision_gate.sh --baseline-repo PATH --candidate-repo PATH --output PATH --records N --changes N --runs N --apis CSV --patterns CSV --adapters CSV [--measurement-samples N]'
}

while (($#)); do
  case "$1" in
    --baseline-repo) BASELINE_REPO="${2:?--baseline-repo requires a value}"; shift 2 ;;
    --candidate-repo) CANDIDATE_REPO="${2:?--candidate-repo requires a value}"; shift 2 ;;
    --output) OUTPUT="${2:?--output requires a value}"; shift 2 ;;
    --records) RECORDS="${2:?--records requires a value}"; shift 2 ;;
    --changes) CHANGES="${2:?--changes requires a value}"; shift 2 ;;
    --runs) RUNS="${2:?--runs requires a value}"; shift 2 ;;
    --apis) APIS="${2:?--apis requires a value}"; shift 2 ;;
    --patterns) PATTERNS="${2:?--patterns requires a value}"; shift 2 ;;
    --adapters) ADAPTERS="${2:?--adapters requires a value}"; shift 2 ;;
    --measurement-samples) MEASUREMENT_SAMPLES="${2:?--measurement-samples requires a value}"; shift 2 ;;
    --help|-h) usage; exit 0 ;;
    *) printf 'unknown argument: %s\n' "$1" >&2; usage >&2; exit 2 ;;
  esac
done

for required in BASELINE_REPO CANDIDATE_REPO OUTPUT RECORDS CHANGES RUNS APIS PATTERNS ADAPTERS; do
  if [[ -z "${!required}" ]]; then
    printf 'missing required argument: %s\n' "$required" >&2
    usage >&2
    exit 2
  fi
done
for integer in RECORDS CHANGES RUNS MEASUREMENT_SAMPLES; do
  if [[ ! "${!integer}" =~ ^[1-9][0-9]*$ ]]; then
    printf '%s must be a positive integer\n' "$integer" >&2
    exit 2
  fi
done
if ((CHANGES * 2 > RECORDS)); then
  printf 'changes must leave two disjoint merge branches\n' >&2
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

WORK_ROOT="$(mktemp -d "${TMPDIR:-/tmp}/prolly-local-publication-gate.XXXXXX")"
cleanup() {
  rm -rf -- "$WORK_ROOT"
}
trap cleanup EXIT
TARGET_ROOT="${PROLLY_NODE_GATE_TARGET_ROOT:-$WORK_ROOT/targets}"
mkdir -p "$OUTPUT" "$WORK_ROOT/build" "$TARGET_ROOT" "$WORK_ROOT/invocations"
LIMITATIONS="$OUTPUT/environment-limitations.csv"
printf 'adapter,reason\n' > "$LIMITATIONS"
BASELINE_REVISION="$(git -C "$BASELINE_REPO" rev-parse HEAD)"
CANDIDATE_REVISION="$(git -C "$CANDIDATE_REPO" rev-parse HEAD)"

remove_adapter() {
  local list="$1"
  local removed="$2"
  local output=""
  local item
  IFS=',' read -r -a items <<< "$list"
  for item in "${items[@]}"; do
    if [[ "$item" == "$removed" ]]; then continue; fi
    if [[ -n "$output" ]]; then output+=","; fi
    output+="$item"
  done
  printf '%s\n' "$output"
}

if [[ ",$ADAPTERS," == *",pglite-sync,"* ]]; then
  if ! command -v node >/dev/null 2>&1; then
    printf 'pglite-sync,Node.js executable is unavailable\n' >> "$LIMITATIONS"
    ADAPTERS="$(remove_adapter "$ADAPTERS" pglite-sync)"
  elif ! (cd "$CANDIDATE_REPO" && node --input-type=module -e "import('@electric-sql/pglite')" >/dev/null 2>&1); then
    printf 'pglite-sync,@electric-sql/pglite package is unavailable\n' >> "$LIMITATIONS"
    ADAPTERS="$(remove_adapter "$ADAPTERS" pglite-sync)"
  fi
fi
if [[ -z "$ADAPTERS" ]]; then
  printf 'no runnable adapters remain after prerequisite checks\n' >&2
  exit 2
fi

build_harness() {
  local role="$1"
  local repo="$2"
  local stage="$WORK_ROOT/build/local-$role"
  local target="$TARGET_ROOT/local-$role"
  mkdir -p "$stage/src" "$target"
  cp "$CANDIDATE_SOURCE/benchmarks/local-store-publication/src/"*.rs "$stage/src/"
  {
    printf '[package]\nname = "prolly-local-store-publication-bench"\nversion = "0.0.0"\nedition = "2021"\npublish = false\n\n'
    printf '[dependencies]\n'
    printf 'csv = "1.3"\nserde = { version = "1.0", features = ["derive"] }\nslatedb = "0.14.0"\ntokio = { version = "1.45", features = ["macros", "rt-multi-thread"] }\n'
    printf 'prolly = { package = "prolly-map", path = "%s", features = ["async-store"] }\n' "$repo"
    printf 'prolly-store-pglite = { path = "%s/stores/prolly-store-pglite" }\n' "$repo"
    printf 'prolly-store-rocksdb = { path = "%s/stores/prolly-store-rocksdb" }\n' "$repo"
    printf 'prolly-store-slatedb = { path = "%s/stores/prolly-store-slatedb" }\n' "$repo"
    printf 'prolly-store-sqlite = { path = "%s/stores/prolly-store-sqlite" }\n' "$repo"
    printf 'prolly-store-turso = { path = "%s/stores/prolly-store-turso" }\n' "$repo"
  } > "$stage/Cargo.toml"
  CARGO_TARGET_DIR="$target" CARGO_INCREMENTAL=0 CARGO_PROFILE_RELEASE_DEBUG=0 \
    cargo build --release --manifest-path "$stage/Cargo.toml"
  CARGO_TARGET_DIR="$target" cargo tree --manifest-path "$stage/Cargo.toml" -e features \
    > "$OUTPUT/dependency-features-$role.txt"
  if rg -q 'prolly-store-turso feature "turso-cloud-sync"' \
    "$OUTPUT/dependency-features-$role.txt"; then
    printf 'refusing %s: turso-cloud-sync is enabled\n' "$role" >&2
    exit 2
  fi
  printf '%s\n' "$target/release/prolly-local-store-publication-bench"
}

append_csv() {
  local role="$1"
  local pair="$2"
  local source="$3"
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
  awk -v role="$role" -v pair="$pair" \
    'NR > 1 { print "local-adapters," role "," pair "," $0 }' "$source" >> "$combined"
}

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
  printf 'baseline_revision=%s\ncandidate_revision=%s\n' "$BASELINE_REVISION" "$CANDIDATE_REVISION"
  printf 'records=%s\nchanges=%s\nruns=%s\nmeasurement_samples=%s\napis=%s\npatterns=%s\nadapters=%s\n' \
    "$RECORDS" "$CHANGES" "$RUNS" "$MEASUREMENT_SAMPLES" "$APIS" "$PATTERNS" "$ADAPTERS"
  printf 'execution=alternating-fresh-local-store-cells\ncloud_sync=disabled\n'
} > "$OUTPUT/provenance.txt"

BASELINE_BIN="$(build_harness baseline "$BASELINE_REPO")"
CANDIDATE_BIN="$(build_harness candidate "$CANDIDATE_REPO")"

for pair in $(seq 1 "$RUNS"); do
  if ((pair % 2 == 1)); then
    ORDER=(baseline candidate)
  else
    ORDER=(candidate baseline)
  fi
  for role in "${ORDER[@]}"; do
    if [[ "$role" == "baseline" ]]; then
      binary="$BASELINE_BIN"
      revision="$BASELINE_REVISION"
    else
      binary="$CANDIDATE_BIN"
      revision="$CANDIDATE_REVISION"
    fi
    invocation="$WORK_ROOT/invocations/pair-$pair/$role"
    mkdir -p "$invocation"
    "$binary" \
      --output "$invocation/raw.csv" \
      --records "$RECORDS" \
      --changes "$CHANGES" \
      --runs "$MEASUREMENT_SAMPLES" \
      --adapters "$ADAPTERS" \
      --apis "$APIS" \
      --patterns "$PATTERNS" \
      --revision "$revision"
    append_csv "$role" "$pair" "$invocation/raw.csv"
  done
done

python3 "$SCRIPT_DIR/summarize_node_publication_revision_gate.py" \
  --input "$OUTPUT/raw-results.csv" \
  --output-dir "$OUTPUT" \
  --environment-limitations "$LIMITATIONS" \
  --minimum-pairs 5

printf 'local-store publication revision gate complete: %s\n' "$OUTPUT"
