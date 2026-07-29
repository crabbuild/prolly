#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
MANIFEST="$REPO_ROOT/benchmarks/backend-comparison/Cargo.toml"
LOCKFILE="$REPO_ROOT/benchmarks/backend-comparison/Cargo.lock"
COMPOSE_FILE="$REPO_ROOT/benchmarks/backend-comparison/docker-compose.yml"
OUTPUT="${BENCH_OUT:-$REPO_ROOT/performance-results/mysql-postgres-$(date -u +%Y%m%dT%H%M%SZ)}"
MODE="${BENCH_MODE:-local}"
RECORDS="${BENCH_RECORDS:-1000000}"
VALUE_BYTES="${BENCH_VALUE_BYTES:-27}"
CHANGES="${BENCH_CHANGES:-10000}"
SAMPLES="${BENCH_SAMPLES:-10000}"
CONCURRENCY="${BENCH_CONCURRENCY:-32}"
POOL_SIZE="${BENCH_POOL_SIZE:-16}"
ADAPTER_BATCH_ITEMS="${BENCH_ADAPTER_BATCH_ITEMS:-1000}"
RUNS="${BENCH_RUNS:-7}"
SEED="${BENCH_SEED:-0x6a09e667f3bcc909}"
POSTGRES_PORT="${PROLLY_BACKEND_POSTGRES_PORT:-55433}"
MYSQL_PORT="${PROLLY_BACKEND_MYSQL_PORT:-53307}"
POSTGRES_URL="${PROLLY_BACKEND_POSTGRES_URL:-postgres://prolly:prolly@127.0.0.1:${POSTGRES_PORT}/prolly}"
MYSQL_URL="${PROLLY_BACKEND_MYSQL_URL:-mysql://prolly:prolly@127.0.0.1:${MYSQL_PORT}/prolly}"
POSTGRES_IMAGE="postgres@sha256:57c72fd2a128e416c7fcc499958864df5301e940bca0a56f58fddf30ffc07777"
MYSQL_IMAGE="mysql@sha256:7dcddc01f13bab2f15cde676d44d01f61fc9f99fe7785e86196dfc07d358ae2b"
POSTGRES_PROJECT="prolly-sql-comparison-postgres"
MYSQL_PROJECT="prolly-sql-comparison-mysql"
GIT_BIN="${BENCH_GIT_BIN:-git}"
DOCKER_BIN="${BENCH_DOCKER_BIN:-docker}"
CARGO_BIN="${BENCH_CARGO_BIN:-cargo}"
SHASUM_BIN="${BENCH_SHASUM_BIN:-shasum}"
MEASUREMENT_COMMANDS=""
RUN_STARTED=false

cleanup() {
  if [[ "$MODE" == local ]]; then
    "$DOCKER_BIN" compose -p "$POSTGRES_PROJECT" -f "$COMPOSE_FILE" down -v \
      >/dev/null 2>&1 || true
    "$DOCKER_BIN" compose -p "$MYSQL_PROJECT" -f "$COMPOSE_FILE" down -v \
      >/dev/null 2>&1 || true
  fi
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

redact_url() {
  printf '%s' "$1" | sed -E 's#(://)[^/@]+@#\1REDACTED@#'
}

record_command() {
  write_shell_command "$@" >> "$MEASUREMENT_COMMANDS"
  "$@"
}

write_shell_command() {
  local separator=""
  local value
  for value in "$@"; do
    printf '%s' "$separator"
    printf '%q' "$value"
    separator=" "
  done
  printf '\n'
}

record_runner_command() {
  local -a actual=("$@")
  local -a redacted=()
  local redact_next=false
  local value
  for value in "${actual[@]}"; do
    if [[ "$redact_next" == true ]]; then
      redacted+=("$(redact_url "$value")")
      redact_next=false
    else
      redacted+=("$value")
      [[ "$value" == --url ]] && redact_next=true
    fi
  done
  write_shell_command "${redacted[@]}" >> "$MEASUREMENT_COMMANDS"
  "${actual[@]}"
}

[[ "$MODE" == local || "$MODE" == external ]] \
  || fail "BENCH_MODE must be local or external"
if [[ "$MODE" == external ]]; then
  [[ "${BENCH_EXTERNAL_RESET_ACK:-}" == "I_UNDERSTAND_BENCHMARK_DATA_WILL_BE_DELETED" ]] \
    || fail "external mode requires BENCH_EXTERNAL_RESET_ACK=I_UNDERSTAND_BENCHMARK_DATA_WILL_BE_DELETED"
  [[ -n "${BENCH_EXTERNAL_POSTGRES_IDENTITY:-}" ]] \
    || fail "external mode requires BENCH_EXTERNAL_POSTGRES_IDENTITY"
  [[ -n "${BENCH_EXTERNAL_MYSQL_IDENTITY:-}" ]] \
    || fail "external mode requires BENCH_EXTERNAL_MYSQL_IDENTITY"
fi
[[ ! -e "$OUTPUT" ]] || fail "refusing to overwrite comparison output: $OUTPUT"
for pair in \
  "BENCH_RECORDS:$RECORDS" \
  "BENCH_VALUE_BYTES:$VALUE_BYTES" \
  "BENCH_CHANGES:$CHANGES" \
  "BENCH_SAMPLES:$SAMPLES" \
  "BENCH_CONCURRENCY:$CONCURRENCY" \
  "BENCH_POOL_SIZE:$POOL_SIZE" \
  "BENCH_ADAPTER_BATCH_ITEMS:$ADAPTER_BATCH_ITEMS" \
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
RUN_ID="sql-${REVISION:0:12}-$(date -u +%Y%m%dT%H%M%SZ)-$$"

{
  printf 'environment_class=%s\n' "$([[ "$MODE" == local ]] && printf controlled_local || printf external)"
  printf 'records=%s\n' "$RECORDS"
  printf 'value_bytes=%s\n' "$VALUE_BYTES"
  printf 'changes=%s\n' "$CHANGES"
  printf 'samples=%s\n' "$SAMPLES"
  printf 'concurrency=%s\n' "$CONCURRENCY"
  printf 'pool_size=%s\n' "$POOL_SIZE"
  printf 'adapter_batch_items=%s\n' "$ADAPTER_BATCH_ITEMS"
  printf 'repetitions=%s\n' "$RUNS"
  printf 'seed=%s\n' "$SEED"
} > "$OUTPUT/config.txt"
CONFIG_SHA256="$(sha256_file "$OUTPUT/config.txt")"

{
  printf 'captured_utc=%s\n' "$(date -u +%Y-%m-%dT%H:%M:%SZ)"
  uname -a
  rustc --version
  "$CARGO_BIN" --version
  if [[ "$MODE" == local ]]; then
    "$DOCKER_BIN" info --format 'docker_server={{.ServerVersion}} os={{.OperatingSystem}} cpus={{.NCPU}} memory={{.MemTotal}}'
  else
    printf 'external_postgres_identity=%s\n' "${BENCH_EXTERNAL_POSTGRES_IDENTITY}"
    printf 'external_mysql_identity=%s\n' "${BENCH_EXTERNAL_MYSQL_IDENTITY}"
  fi
} > "$OUTPUT/machine.txt"

if [[ "${BENCH_SKIP_BUILD:-0}" != 1 ]]; then
  "$CARGO_BIN" build --release --no-default-features --bins \
    --manifest-path "$MANIFEST" 2>&1 | tee "$OUTPUT/build.log"
  "$CARGO_BIN" tree --no-default-features --manifest-path "$MANIFEST" \
    > "$OUTPUT/dependencies.txt"
fi
[[ "$("$GIT_BIN" -C "$REPO_ROOT" rev-parse HEAD)" == "$REVISION" ]] \
  || fail "HEAD changed during benchmark build"
[[ -z "$("$GIT_BIN" -C "$REPO_ROOT" status --porcelain --untracked-files=no)" ]] \
  || fail "tracked worktree changed during benchmark build"
LOCKFILE_SHA256="$(sha256_file "$LOCKFILE")"

POSTGRES_SOURCE="${BENCH_POSTGRES_EXECUTABLE:-$REPO_ROOT/benchmarks/backend-comparison/target/release/prolly-backend-postgres}"
MYSQL_SOURCE="${BENCH_MYSQL_EXECUTABLE:-$REPO_ROOT/benchmarks/backend-comparison/target/release/prolly-backend-mysql}"
SUMMARIZER_SOURCE="${BENCH_SUMMARIZER_EXECUTABLE:-$REPO_ROOT/benchmarks/backend-comparison/target/release/prolly-backend-summarize}"
SERVICE_SUMMARIZER="${BENCH_SERVICE_SUMMARIZER:-$SCRIPT_DIR/summarize_mysql_postgres_service.py}"
for executable in "$POSTGRES_SOURCE" "$MYSQL_SOURCE" "$SUMMARIZER_SOURCE"; do
  [[ -x "$executable" ]] || fail "benchmark executable is missing or not executable: $executable"
done
cp "$POSTGRES_SOURCE" "$OUTPUT/bin/prolly-backend-postgres"
cp "$MYSQL_SOURCE" "$OUTPUT/bin/prolly-backend-mysql"
cp "$SUMMARIZER_SOURCE" "$OUTPUT/bin/prolly-backend-summarize"
POSTGRES_EXECUTABLE="$OUTPUT/bin/prolly-backend-postgres"
MYSQL_EXECUTABLE="$OUTPUT/bin/prolly-backend-mysql"
SUMMARIZER_EXECUTABLE="$OUTPUT/bin/prolly-backend-summarize"
POSTGRES_BINARY_SHA256="$(sha256_file "$POSTGRES_EXECUTABLE")"
MYSQL_BINARY_SHA256="$(sha256_file "$MYSQL_EXECUTABLE")"
SUMMARIZER_BINARY_SHA256="$(sha256_file "$SUMMARIZER_EXECUTABLE")"
{
  printf '%s  %s\n' "$POSTGRES_BINARY_SHA256" "bin/prolly-backend-postgres"
  printf '%s  %s\n' "$MYSQL_BINARY_SHA256" "bin/prolly-backend-mysql"
  printf '%s  %s\n' "$SUMMARIZER_BINARY_SHA256" "bin/prolly-backend-summarize"
} > "$OUTPUT/binaries.sha256"

if [[ "$MODE" == local ]]; then
  if [[ "${BENCH_SKIP_IMAGE_PULL:-0}" != 1 ]]; then
    {
      "$DOCKER_BIN" pull "$POSTGRES_IMAGE"
      "$DOCKER_BIN" pull "$MYSQL_IMAGE"
    } > "$OUTPUT/image-pull.log"
  fi
  POSTGRES_IMAGE_ID="$("$DOCKER_BIN" image inspect "$POSTGRES_IMAGE" --format '{{.Id}}')"
  MYSQL_IMAGE_ID="$("$DOCKER_BIN" image inspect "$MYSQL_IMAGE" --format '{{.Id}}')"
else
  POSTGRES_IMAGE="external"
  MYSQL_IMAGE="external"
  POSTGRES_IMAGE_ID="${BENCH_EXTERNAL_POSTGRES_IDENTITY}"
  MYSQL_IMAGE_ID="${BENCH_EXTERNAL_MYSQL_IDENTITY}"
fi
[[ -n "$POSTGRES_IMAGE_ID" && -n "$MYSQL_IMAGE_ID" ]] \
  || fail "service identities are unavailable"
{
  printf 'postgres_image=%s\n' "$POSTGRES_IMAGE"
  printf 'postgres_image_id=%s\n' "$POSTGRES_IMAGE_ID"
  printf 'mysql_image=%s\n' "$MYSQL_IMAGE"
  printf 'mysql_image_id=%s\n' "$MYSQL_IMAGE_ID"
} > "$OUTPUT/images.txt"

wait_for_service() {
  local project="$1"
  local service="$2"
  for _ in $(seq 1 120); do
    health="$("$DOCKER_BIN" inspect --format '{{.State.Health.Status}}' "$project-$service-1" 2>/dev/null || true)"
    [[ "$health" == healthy ]] && return 0
    sleep 1
  done
  "$DOCKER_BIN" compose -p "$project" -f "$COMPOSE_FILE" logs "$service" >&2 || true
  return 1
}

run_backend() {
  local backend="$1"
  local repetition="$2"
  local output="$3"
  local service_output="$4"
  local invocation_run_id="$5"
  local project service executable binary_sha256 url port_env port
  if [[ "$backend" == postgres ]]; then
    project="$POSTGRES_PROJECT"
    service=postgres
    executable="$POSTGRES_EXECUTABLE"
    binary_sha256="$POSTGRES_BINARY_SHA256"
    url="$POSTGRES_URL"
    port_env=PROLLY_BACKEND_POSTGRES_PORT
    port="$POSTGRES_PORT"
  else
    project="$MYSQL_PROJECT"
    service=mysql
    executable="$MYSQL_EXECUTABLE"
    binary_sha256="$MYSQL_BINARY_SHA256"
    url="$MYSQL_URL"
    port_env=PROLLY_BACKEND_MYSQL_PORT
    port="$MYSQL_PORT"
  fi
  if [[ "$MODE" == local ]]; then
    record_command "$DOCKER_BIN" compose -p "$project" -f "$COMPOSE_FILE" down -v
    record_command env "$port_env=$port" \
      "$DOCKER_BIN" compose -p "$project" -f "$COMPOSE_FILE" up -d "$service"
    wait_for_service "$project" "$service"
  fi
  record_runner_command "$executable" \
    --output "$output" \
    --run-id "$invocation_run_id" \
    --repetition "$repetition" \
    --revision "$REVISION" \
    --tree-hash "$TREE_HASH" \
    --binary-sha256 "$binary_sha256" \
    --records "$RECORDS" \
    --value-bytes "$VALUE_BYTES" \
    --changes "$CHANGES" \
    --samples "$SAMPLES" \
    --concurrency "$CONCURRENCY" \
    --pool-size "$POOL_SIZE" \
    --adapter-batch-items "$ADAPTER_BATCH_ITEMS" \
    --seed "$SEED" \
    --url "$url" \
    --suite end-to-end
  record_runner_command "$executable" \
    --output "$service_output" \
    --run-id "$invocation_run_id" \
    --repetition "$repetition" \
    --revision "$REVISION" \
    --tree-hash "$TREE_HASH" \
    --binary-sha256 "$binary_sha256" \
    --records "$RECORDS" \
    --value-bytes "$VALUE_BYTES" \
    --changes "$CHANGES" \
    --samples "$SAMPLES" \
    --concurrency "$CONCURRENCY" \
    --pool-size "$POOL_SIZE" \
    --adapter-batch-items "$ADAPTER_BATCH_ITEMS" \
    --seed "$SEED" \
    --url "$url" \
    --suite service
  if [[ "$MODE" == local ]]; then
    record_command "$DOCKER_BIN" compose -p "$project" -f "$COMPOSE_FILE" down -v
  fi
}

run_backend postgres 1 "$OUTPUT/warmup/postgres.csv" \
  "$OUTPUT/warmup/postgres-service.csv" "$RUN_ID-warmup-postgres"
run_backend mysql 1 "$OUTPUT/warmup/mysql.csv" \
  "$OUTPUT/warmup/mysql-service.csv" "$RUN_ID-warmup-mysql"

measured_files=()
measured_service_files=()
for repetition in $(seq 1 "$RUNS"); do
  if ((repetition % 2 == 1)); then
    order=(postgres mysql)
  else
    order=(mysql postgres)
  fi
  for backend in "${order[@]}"; do
    file="$OUTPUT/invocations/${repetition}-${backend}.csv"
    service_file="$OUTPUT/invocations/${repetition}-${backend}-service.csv"
    run_backend "$backend" "$repetition" "$file" "$service_file" "$RUN_ID"
    measured_files+=("$file")
    measured_service_files+=("$service_file")
  done
done

RAW_SERVICE_RESULTS="$OUTPUT/raw-service-results.csv"
first=true
header=""
for file in "${measured_service_files[@]}"; do
  current_header="$(head -n 1 "$file")"
  if [[ "$first" == true ]]; then
    cp "$file" "$RAW_SERVICE_RESULTS"
    header="$current_header"
    first=false
  else
    [[ "$current_header" == "$header" ]] || fail "service evidence headers differ: $file"
    tail -n +2 "$file" >> "$RAW_SERVICE_RESULTS"
  fi
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
  printf 'environment_class=%s\n' "$([[ "$MODE" == local ]] && printf controlled_local || printf external)"
  printf 'backend_a=postgres\n'
  printf 'backend_b=mysql\n'
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
  printf 'mysql_binary_sha256=%s\n' "$MYSQL_BINARY_SHA256"
  printf 'summarizer_binary_sha256=%s\n' "$SUMMARIZER_BINARY_SHA256"
  printf 'postgres_image=%s\n' "$POSTGRES_IMAGE"
  printf 'postgres_image_id=%s\n' "$POSTGRES_IMAGE_ID"
  printf 'mysql_image=%s\n' "$MYSQL_IMAGE"
  printf 'mysql_image_id=%s\n' "$MYSQL_IMAGE_ID"
} > "$OUTPUT/manifest.txt"

record_command "$SUMMARIZER_EXECUTABLE" \
  --input "$RAW_RESULTS" \
  --manifest "$OUTPUT/manifest.txt" \
  --output-dir "$OUTPUT"
record_command "$SERVICE_SUMMARIZER" \
  --input "$RAW_SERVICE_RESULTS" \
  --manifest "$OUTPUT/manifest.txt" \
  --output-dir "$OUTPUT"

{
  printf 'status=complete\n'
  printf 'completed_utc=%s\n' "$(date -u +%Y-%m-%dT%H:%M:%SZ)"
} > "$OUTPUT/run-state.txt"
printf 'MySQL/PostgreSQL comparison complete: %s/report.md\n' "$OUTPUT"
