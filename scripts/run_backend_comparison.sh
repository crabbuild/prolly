#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
MANIFEST="$REPO_ROOT/benchmarks/backend-comparison/Cargo.toml"
LOCKFILE="$REPO_ROOT/benchmarks/backend-comparison/Cargo.lock"
COMPOSE_FILE="$REPO_ROOT/benchmarks/backend-comparison/docker-compose.yml"
OUTPUT="${BENCH_OUT:-$REPO_ROOT/performance-results/backend-comparison-$(date -u +%Y%m%dT%H%M%SZ)}"
RECORDS="${BENCH_RECORDS:-1000000}"
VALUE_BYTES="${BENCH_VALUE_BYTES:-27}"
CHANGES="${BENCH_CHANGES:-10000}"
SAMPLES="${BENCH_SAMPLES:-10000}"
CONCURRENCY="${BENCH_CONCURRENCY:-32}"
RUNS="${BENCH_RUNS:-7}"
SEED="${BENCH_SEED:-0x6a09e667f3bcc909}"
READ_PARALLELISM="${BENCH_READ_PARALLELISM:-16}"
BATCH_GET_PARALLELISM="${BENCH_BATCH_GET_PARALLELISM:-16}"
BATCH_WRITE_PARALLELISM="${BENCH_BATCH_WRITE_PARALLELISM:-16}"
SCAN_PARALLELISM="${BENCH_SCAN_PARALLELISM:-8}"
POSTGRES_PORT="${PROLLY_BACKEND_POSTGRES_PORT:-55433}"
DYNAMODB_PORT="${PROLLY_BACKEND_DYNAMODB_PORT:-58000}"
POSTGRES_URL="${PROLLY_BACKEND_POSTGRES_URL:-postgres://prolly:prolly@127.0.0.1:${POSTGRES_PORT}/prolly}"
DYNAMODB_ENDPOINT="${PROLLY_BACKEND_DYNAMODB_ENDPOINT:-http://127.0.0.1:${DYNAMODB_PORT}}"
DYNAMODB_TABLE="${PROLLY_BACKEND_DYNAMODB_TABLE:-prolly_backend_comparison}"
POSTGRES_IMAGE="postgres@sha256:57c72fd2a128e416c7fcc499958864df5301e940bca0a56f58fddf30ffc07777"
DYNAMODB_IMAGE="amazon/dynamodb-local@sha256:d89f8fcc6b1a39cb35976c248ed42a28c66ae00dc043099210f5571e42648ab4"
POSTGRES_PROJECT="prolly-backend-comparison-postgres"
DYNAMODB_PROJECT="prolly-backend-comparison-dynamodb"
GIT_BIN="${BENCH_GIT_BIN:-git}"
DOCKER_BIN="${BENCH_DOCKER_BIN:-docker}"
CURL_BIN="${BENCH_CURL_BIN:-curl}"
CARGO_BIN="${BENCH_CARGO_BIN:-cargo}"
SHASUM_BIN="${BENCH_SHASUM_BIN:-shasum}"
MEASUREMENT_COMMANDS=""
RUN_STARTED=false

cleanup() {
  "$DOCKER_BIN" compose -p "$POSTGRES_PROJECT" -f "$COMPOSE_FILE" down -v \
    >/dev/null 2>&1 || true
  "$DOCKER_BIN" compose -p "$DYNAMODB_PROJECT" -f "$COMPOSE_FILE" down -v \
    >/dev/null 2>&1 || true
}

finish() {
  code=$?
  trap - EXIT
  cleanup
  if [[ "$code" != 0 && "$RUN_STARTED" == true && -d "$OUTPUT" ]]; then
    {
      printf 'status=failed\n'
      printf 'exit_code=%s\n' "$code"
      printf 'failed_utc=%s\n' "$(date -u +%Y-%m-%dT%H:%M:%SZ)"
    } > "$OUTPUT/failure.txt"
  fi
  exit "$code"
}
trap finish EXIT

fail() {
  printf '%s\n' "$*" >&2
  exit 2
}

is_positive_integer() {
  [[ "$1" =~ ^[1-9][0-9]*$ ]]
}

sha256_file() {
  "$SHASUM_BIN" -a 256 "$1" | awk '{print $1}'
}

record_command() {
  {
    printf '%q ' "$@"
    printf '\n'
  } >> "$MEASUREMENT_COMMANDS"
  "$@"
}

if [[ -e "$OUTPUT" ]]; then
  fail "refusing to overwrite comparison output: $OUTPUT"
fi
for pair in \
  "BENCH_RECORDS:$RECORDS" \
  "BENCH_VALUE_BYTES:$VALUE_BYTES" \
  "BENCH_CHANGES:$CHANGES" \
  "BENCH_SAMPLES:$SAMPLES" \
  "BENCH_CONCURRENCY:$CONCURRENCY" \
  "BENCH_RUNS:$RUNS"; do
  name="${pair%%:*}"
  value="${pair#*:}"
  is_positive_integer "$value" || fail "$name must be a positive integer"
done
((RUNS >= 7)) || fail "BENCH_RUNS must be at least 7"
((CHANGES % 2 == 0)) || fail "BENCH_CHANGES must be even"
((CHANGES <= RECORDS)) || fail "BENCH_CHANGES cannot exceed BENCH_RECORDS"
((SAMPLES <= RECORDS)) || fail "BENCH_SAMPLES cannot exceed BENCH_RECORDS"

REVISION="$("$GIT_BIN" -C "$REPO_ROOT" rev-parse HEAD)"
TREE_HASH="$("$GIT_BIN" -C "$REPO_ROOT" rev-parse 'HEAD^{tree}')"
[[ "$REVISION" =~ ^[0-9a-f]{40}$ ]] || fail "HEAD is not a committed Git revision"
[[ "$TREE_HASH" =~ ^[0-9a-f]{40}$ ]] || fail "HEAD tree hash is invalid"
TRACKED_STATUS="$("$GIT_BIN" -C "$REPO_ROOT" status --porcelain --untracked-files=no)"
[[ -z "$TRACKED_STATUS" ]] || fail "tracked worktree must be clean before comparison"

mkdir -p "$OUTPUT/bin" "$OUTPUT/invocations" "$OUTPUT/warmup"
RUN_STARTED=true
MEASUREMENT_COMMANDS="$OUTPUT/measurement-commands.txt"
: > "$MEASUREMENT_COMMANDS"
RUN_ID="backend-${REVISION:0:12}-$(date -u +%Y%m%dT%H%M%SZ)-$$"

{
  printf 'records=%s\n' "$RECORDS"
  printf 'value_bytes=%s\n' "$VALUE_BYTES"
  printf 'changes=%s\n' "$CHANGES"
  printf 'samples=%s\n' "$SAMPLES"
  printf 'concurrency=%s\n' "$CONCURRENCY"
  printf 'repetitions=%s\n' "$RUNS"
  printf 'seed=%s\n' "$SEED"
  printf 'read_parallelism=%s\n' "$READ_PARALLELISM"
  printf 'batch_get_parallelism=%s\n' "$BATCH_GET_PARALLELISM"
  printf 'batch_write_parallelism=%s\n' "$BATCH_WRITE_PARALLELISM"
  printf 'scan_parallelism=%s\n' "$SCAN_PARALLELISM"
} > "$OUTPUT/config.txt"
CONFIG_SHA256="$(sha256_file "$OUTPUT/config.txt")"

{
  printf 'captured_utc=%s\n' "$(date -u +%Y-%m-%dT%H:%M:%SZ)"
  uname -a
  sysctl -n machdep.cpu.brand_string 2>/dev/null || true
  sysctl -n hw.logicalcpu 2>/dev/null || true
  sysctl -n hw.memsize 2>/dev/null || true
  rustc --version
  "$CARGO_BIN" --version
  "$DOCKER_BIN" info --format 'docker_server={{.ServerVersion}} os={{.OperatingSystem}} cpus={{.NCPU}} memory={{.MemTotal}}' 2>/dev/null || true
} > "$OUTPUT/machine.txt"

if [[ "${BENCH_SKIP_BUILD:-0}" != 1 ]]; then
  "$CARGO_BIN" build --release --manifest-path "$MANIFEST" 2>&1 | tee "$OUTPUT/build.log"
  "$CARGO_BIN" tree --manifest-path "$MANIFEST" > "$OUTPUT/dependencies.txt"
fi
[[ "$("$GIT_BIN" -C "$REPO_ROOT" rev-parse HEAD)" == "$REVISION" ]] \
  || fail "HEAD changed during benchmark build"
[[ -z "$("$GIT_BIN" -C "$REPO_ROOT" status --porcelain --untracked-files=no)" ]] \
  || fail "tracked worktree changed during benchmark build"
LOCKFILE_SHA256="$(sha256_file "$LOCKFILE")"

POSTGRES_SOURCE="${BENCH_POSTGRES_EXECUTABLE:-$REPO_ROOT/benchmarks/backend-comparison/target/release/prolly-backend-postgres}"
DYNAMODB_SOURCE="${BENCH_DYNAMODB_EXECUTABLE:-$REPO_ROOT/benchmarks/backend-comparison/target/release/prolly-backend-dynamodb}"
SUMMARIZER_SOURCE="${BENCH_SUMMARIZER_EXECUTABLE:-$REPO_ROOT/benchmarks/backend-comparison/target/release/prolly-backend-summarize}"
for executable in "$POSTGRES_SOURCE" "$DYNAMODB_SOURCE" "$SUMMARIZER_SOURCE"; do
  [[ -x "$executable" ]] || fail "benchmark executable is missing or not executable: $executable"
done
cp "$POSTGRES_SOURCE" "$OUTPUT/bin/prolly-backend-postgres"
cp "$DYNAMODB_SOURCE" "$OUTPUT/bin/prolly-backend-dynamodb"
cp "$SUMMARIZER_SOURCE" "$OUTPUT/bin/prolly-backend-summarize"
POSTGRES_EXECUTABLE="$OUTPUT/bin/prolly-backend-postgres"
DYNAMODB_EXECUTABLE="$OUTPUT/bin/prolly-backend-dynamodb"
SUMMARIZER_EXECUTABLE="$OUTPUT/bin/prolly-backend-summarize"
POSTGRES_BINARY_SHA256="$(sha256_file "$POSTGRES_EXECUTABLE")"
DYNAMODB_BINARY_SHA256="$(sha256_file "$DYNAMODB_EXECUTABLE")"
SUMMARIZER_BINARY_SHA256="$(sha256_file "$SUMMARIZER_EXECUTABLE")"
{
  printf '%s  %s\n' "$POSTGRES_BINARY_SHA256" "bin/prolly-backend-postgres"
  printf '%s  %s\n' "$DYNAMODB_BINARY_SHA256" "bin/prolly-backend-dynamodb"
  printf '%s  %s\n' "$SUMMARIZER_BINARY_SHA256" "bin/prolly-backend-summarize"
} > "$OUTPUT/binaries.sha256"

if [[ "${BENCH_SKIP_IMAGE_PULL:-0}" != 1 ]]; then
  {
    "$DOCKER_BIN" pull "$POSTGRES_IMAGE"
    "$DOCKER_BIN" pull "$DYNAMODB_IMAGE"
  } > "$OUTPUT/image-pull.log"
fi
POSTGRES_IMAGE_ID="$("$DOCKER_BIN" image inspect "$POSTGRES_IMAGE" --format '{{.Id}}')"
DYNAMODB_IMAGE_ID="$("$DOCKER_BIN" image inspect "$DYNAMODB_IMAGE" --format '{{.Id}}')"
[[ -n "$POSTGRES_IMAGE_ID" && -n "$DYNAMODB_IMAGE_ID" ]] \
  || fail "pinned service images are not available"
{
  printf 'postgres_image=%s\n' "$POSTGRES_IMAGE"
  printf 'postgres_image_id=%s\n' "$POSTGRES_IMAGE_ID"
  printf 'dynamodb_image=%s\n' "$DYNAMODB_IMAGE"
  printf 'dynamodb_image_id=%s\n' "$DYNAMODB_IMAGE_ID"
} > "$OUTPUT/images.txt"

wait_for_postgres() {
  for attempt in $(seq 1 60); do
    health="$("$DOCKER_BIN" inspect --format '{{.State.Health.Status}}' "$POSTGRES_PROJECT-postgres-1" 2>/dev/null || true)"
    [[ "$health" == healthy ]] && return 0
    sleep 1
  done
  "$DOCKER_BIN" compose -p "$POSTGRES_PROJECT" -f "$COMPOSE_FILE" logs postgres >&2 || true
  return 1
}

wait_for_dynamodb() {
  for attempt in $(seq 1 60); do
    "$CURL_BIN" --silent --output /dev/null "$DYNAMODB_ENDPOINT" && return 0
    sleep 1
  done
  "$DOCKER_BIN" compose -p "$DYNAMODB_PROJECT" -f "$COMPOSE_FILE" logs dynamodb >&2 || true
  return 1
}

run_backend() {
  local backend="$1"
  local repetition="$2"
  local output="$3"
  local invocation_run_id="$4"
  local project service executable binary_sha256 health
  local -a args
  if [[ "$backend" == postgres ]]; then
    project="$POSTGRES_PROJECT"
    service=postgres
    executable="$POSTGRES_EXECUTABLE"
    binary_sha256="$POSTGRES_BINARY_SHA256"
  else
    project="$DYNAMODB_PROJECT"
    service=dynamodb
    executable="$DYNAMODB_EXECUTABLE"
    binary_sha256="$DYNAMODB_BINARY_SHA256"
  fi
  record_command "$DOCKER_BIN" compose -p "$project" -f "$COMPOSE_FILE" down -v
  if [[ "$backend" == postgres ]]; then
    record_command env PROLLY_BACKEND_POSTGRES_PORT="$POSTGRES_PORT" \
      "$DOCKER_BIN" compose -p "$project" -f "$COMPOSE_FILE" up -d "$service"
    wait_for_postgres
  else
    record_command env PROLLY_BACKEND_DYNAMODB_PORT="$DYNAMODB_PORT" \
      "$DOCKER_BIN" compose -p "$project" -f "$COMPOSE_FILE" up -d "$service"
    wait_for_dynamodb
  fi
  args=(
    --output "$output"
    --run-id "$invocation_run_id"
    --repetition "$repetition"
    --revision "$REVISION"
    --tree-hash "$TREE_HASH"
    --binary-sha256 "$binary_sha256"
    --records "$RECORDS"
    --value-bytes "$VALUE_BYTES"
    --changes "$CHANGES"
    --samples "$SAMPLES"
    --concurrency "$CONCURRENCY"
    --seed "$SEED"
  )
  if [[ "$backend" == postgres ]]; then
    args+=(--url "$POSTGRES_URL")
  else
    args+=(
      --endpoint "$DYNAMODB_ENDPOINT"
      --table "$DYNAMODB_TABLE"
      --read-parallelism "$READ_PARALLELISM"
      --batch-get-parallelism "$BATCH_GET_PARALLELISM"
      --batch-write-parallelism "$BATCH_WRITE_PARALLELISM"
      --scan-parallelism "$SCAN_PARALLELISM"
    )
  fi
  record_command "$executable" "${args[@]}"
  record_command "$DOCKER_BIN" compose -p "$project" -f "$COMPOSE_FILE" down -v
}

run_backend postgres 1 "$OUTPUT/warmup/postgres.csv" "$RUN_ID-warmup-postgres"
run_backend dynamodb 1 "$OUTPUT/warmup/dynamodb.csv" "$RUN_ID-warmup-dynamodb"

measured_files=()
for repetition in $(seq 1 "$RUNS"); do
  if ((repetition % 2 == 1)); then
    order=(postgres dynamodb)
  else
    order=(dynamodb postgres)
  fi
  for backend in "${order[@]}"; do
    file="$OUTPUT/invocations/${repetition}-${backend}.csv"
    run_backend "$backend" "$repetition" "$file" "$RUN_ID"
    measured_files+=("$file")
  done
done

RAW_RESULTS="$OUTPUT/raw-results.csv"
first=true
header=""
for file in "${measured_files[@]}"; do
  current_header="$(head -n 1 "$file")"
  if [[ "$first" == true ]]; then
    cp "$file" "$RAW_RESULTS"
    header="$current_header"
    first=false
  else
    [[ "$current_header" == "$header" ]] || fail "evidence headers differ: $file"
    tail -n +2 "$file" >> "$RAW_RESULTS"
  fi
done

COMMANDS_SHA256="$(sha256_file "$MEASUREMENT_COMMANDS")"
{
  printf 'schema=backend-comparison-manifest-v1\n'
  printf 'status=complete\n'
  printf 'resumed=false\n'
  printf 'dirty=false\n'
  printf 'run_id=%s\n' "$RUN_ID"
  printf 'revision=%s\n' "$REVISION"
  printf 'tree_hash=%s\n' "$TREE_HASH"
  printf 'contract_version=backend-workload-v1\n'
  printf 'timed_scope_version=public-prolly-operation-v1\n'
  printf 'result_schema=backend-comparison-v1\n'
  printf 'repetitions=%s\n' "$RUNS"
  printf 'lockfile_sha256=%s\n' "$LOCKFILE_SHA256"
  printf 'config_sha256=%s\n' "$CONFIG_SHA256"
  printf 'commands_sha256=%s\n' "$COMMANDS_SHA256"
  printf 'postgres_binary_sha256=%s\n' "$POSTGRES_BINARY_SHA256"
  printf 'dynamodb_binary_sha256=%s\n' "$DYNAMODB_BINARY_SHA256"
  printf 'summarizer_binary_sha256=%s\n' "$SUMMARIZER_BINARY_SHA256"
  printf 'postgres_image=%s\n' "$POSTGRES_IMAGE"
  printf 'postgres_image_id=%s\n' "$POSTGRES_IMAGE_ID"
  printf 'dynamodb_image=%s\n' "$DYNAMODB_IMAGE"
  printf 'dynamodb_image_id=%s\n' "$DYNAMODB_IMAGE_ID"
} > "$OUTPUT/manifest.txt"

"$SUMMARIZER_EXECUTABLE" \
  --input "$RAW_RESULTS" \
  --manifest "$OUTPUT/manifest.txt" \
  --output-dir "$OUTPUT"
printf 'status=complete\nended_utc=%s\n' "$(date -u +%Y-%m-%dT%H:%M:%SZ)" \
  > "$OUTPUT/run-state.txt"
printf 'Backend comparison complete: %s\n' "$OUTPUT"
