#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
MANIFEST="$REPO_ROOT/benchmarks/backend-comparison/Cargo.toml"
LOCKFILE="$REPO_ROOT/benchmarks/backend-comparison/Cargo.lock"
COMPOSE_FILE="$REPO_ROOT/benchmarks/backend-comparison/docker-compose.yml"
OUTPUT="${BENCH_OUT:-$REPO_ROOT/performance-results/spanner-postgres-$(date -u +%Y%m%dT%H%M%SZ)}"
MODE="${BENCH_MODE:-local}"
RECORDS="${BENCH_RECORDS:-10000}"
VALUE_BYTES="${BENCH_VALUE_BYTES:-128}"
CHANGES="${BENCH_CHANGES:-1000}"
SAMPLES="${BENCH_SAMPLES:-1000}"
CONCURRENCY="${BENCH_CONCURRENCY:-16}"
POOL_SIZE="${BENCH_POOL_SIZE:-16}"
ADAPTER_BATCH_ITEMS="${BENCH_ADAPTER_BATCH_ITEMS:-1000}"
RUNS="${BENCH_RUNS:-7}"
SEED="${BENCH_SEED:-0x6a09e667f3bcc909}"
POSTGRES_PORT="${PROLLY_BACKEND_POSTGRES_PORT:-55433}"
SPANNER_GRPC_PORT="${PROLLY_BACKEND_SPANNER_GRPC_PORT:-59010}"
SPANNER_REST_PORT="${PROLLY_BACKEND_SPANNER_REST_PORT:-59020}"
POSTGRES_URL="${PROLLY_BACKEND_POSTGRES_URL:-postgres://prolly:prolly@127.0.0.1:${POSTGRES_PORT}/prolly}"
SPANNER_DATABASE="${PROLLY_BACKEND_SPANNER_DATABASE:-projects/prolly-local/instances/prolly-instance/databases/prolly}"
POSTGRES_IMAGE="postgres@sha256:57c72fd2a128e416c7fcc499958864df5301e940bca0a56f58fddf30ffc07777"
SPANNER_IMAGE="gcr.io/cloud-spanner-emulator/emulator@sha256:ad54472fe7b161b9214f7f816f304b649a4779e348229c375ac067f5ed5a6422"
POSTGRES_PROJECT="prolly-spanner-postgres-postgres"
SPANNER_PROJECT="prolly-spanner-postgres-spanner"
GIT_BIN="${BENCH_GIT_BIN:-git}"
DOCKER_BIN="${BENCH_DOCKER_BIN:-docker}"
CARGO_BIN="${BENCH_CARGO_BIN:-cargo}"
CURL_BIN="${BENCH_CURL_BIN:-curl}"
SHASUM_BIN="${BENCH_SHASUM_BIN:-shasum}"
MEASUREMENT_COMMANDS=""
RUN_STARTED=false

compose() {
  local project="$1"
  shift
  "$DOCKER_BIN" compose -p "$project" -f "$COMPOSE_FILE" "$@"
}

cleanup() {
  if [[ "$MODE" == local ]]; then
    compose "$POSTGRES_PROJECT" down -v >/dev/null 2>&1 || true
    compose "$SPANNER_PROJECT" down -v >/dev/null 2>&1 || true
  fi
}

finish() {
  local code=$?
  trap - EXIT
  cleanup
  if [[ "$code" != 0 && "$RUN_STARTED" == true && -d "$OUTPUT" ]]; then
    {
      printf 'status=failed\n'
      printf 'exit_code=%s\n' "$code"
      printf 'failed_utc=%s\n' "$(date -u +%Y-%m-%dT%H:%M:%SZ)"
    } >"$OUTPUT/failure.txt"
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

record_command() {
  write_shell_command "$@" >>"$MEASUREMENT_COMMANDS"
  "$@"
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
  write_shell_command "${redacted[@]}" >>"$MEASUREMENT_COMMANDS"
  "${actual[@]}"
}

wait_for_postgres() {
  local consecutive_ready=0
  for _ in $(seq 1 120); do
    local health
    health="$("$DOCKER_BIN" inspect --format '{{.State.Health.Status}}' \
      "$POSTGRES_PROJECT-postgres-1" 2>/dev/null || true)"
    if [[ "$health" == healthy ]] &&
      "$DOCKER_BIN" exec "$POSTGRES_PROJECT-postgres-1" \
        psql -U prolly -d prolly -tAc "SELECT 1" 2>/dev/null |
        tr -d '[:space:]' | grep -qx 1; then
      consecutive_ready=$((consecutive_ready + 1))
      [[ "$consecutive_ready" -ge 3 ]] && return 0
    else
      consecutive_ready=0
    fi
    sleep 1
  done
  compose "$POSTGRES_PROJECT" logs postgres >&2 || true
  return 1
}

wait_for_spanner() {
  for _ in $(seq 1 120); do
    if (echo >"/dev/tcp/127.0.0.1/$SPANNER_GRPC_PORT") >/dev/null 2>&1; then
      return 0
    fi
    sleep 1
  done
  compose "$SPANNER_PROJECT" logs spanner >&2 || true
  return 1
}

initialize_spanner() {
  local endpoint="http://127.0.0.1:$SPANNER_REST_PORT"
  local instance_ready=false
  local database_ready=false
  for _ in $(seq 1 60); do
    if "$CURL_BIN" --silent --fail-with-body \
      -H "content-type: application/json" \
      -X POST "$endpoint/v1/projects/prolly-local/instances" \
      --data-binary '{
        "instanceId": "prolly-instance",
        "instance": {
          "config": "projects/prolly-local/instanceConfigs/emulator-config",
          "displayName": "Prolly Local",
          "nodeCount": 1
        }
      }' >/dev/null 2>&1; then
      instance_ready=true
      break
    fi
    sleep 1
  done
  [[ "$instance_ready" == true ]] || return 1
  for _ in $(seq 1 60); do
    if "$CURL_BIN" --silent --fail-with-body \
      -H "content-type: application/json" \
      -X POST "$endpoint/v1/projects/prolly-local/instances/prolly-instance/databases" \
      --data-binary '{
        "createStatement": "CREATE DATABASE `prolly`",
        "extraStatements": [
          "CREATE TABLE ProllyNodes (Cid BYTES(32) NOT NULL, Node BYTES(MAX) NOT NULL) PRIMARY KEY (Cid)",
          "CREATE TABLE ProllyHints (Namespace BYTES(MAX) NOT NULL, HintKey BYTES(MAX) NOT NULL, Value BYTES(MAX) NOT NULL) PRIMARY KEY (Namespace, HintKey)",
          "CREATE TABLE ProllyRoots (Name BYTES(MAX) NOT NULL, Manifest BYTES(MAX) NOT NULL) PRIMARY KEY (Name)"
        ]
      }' >/dev/null 2>&1; then
      database_ready=true
      break
    fi
    sleep 1
  done
  [[ "$database_ready" == true ]] || return 1
  for _ in $(seq 1 60); do
    if "$CURL_BIN" --silent --fail \
      "$endpoint/v1/projects/prolly-local/instances/prolly-instance/databases/prolly" \
      >/dev/null 2>&1; then
      return 0
    fi
    sleep 1
  done
  return 1
}

[[ "$MODE" == local || "$MODE" == external ]] ||
  fail "BENCH_MODE must be local or external"
if [[ "$MODE" == external ]]; then
  [[ "${BENCH_EXTERNAL_RESET_ACK:-}" == "I_UNDERSTAND_BENCHMARK_DATA_WILL_BE_DELETED" ]] ||
    fail "external mode requires BENCH_EXTERNAL_RESET_ACK=I_UNDERSTAND_BENCHMARK_DATA_WILL_BE_DELETED"
  [[ -n "${BENCH_EXTERNAL_POSTGRES_IDENTITY:-}" ]] ||
    fail "external mode requires BENCH_EXTERNAL_POSTGRES_IDENTITY"
  [[ -n "${BENCH_EXTERNAL_SPANNER_IDENTITY:-}" ]] ||
    fail "external mode requires BENCH_EXTERNAL_SPANNER_IDENTITY"
  [[ -n "${PROLLY_BACKEND_POSTGRES_URL:-}" ]] ||
    fail "external mode requires PROLLY_BACKEND_POSTGRES_URL"
  [[ -n "${PROLLY_BACKEND_SPANNER_DATABASE:-}" ]] ||
    fail "external mode requires PROLLY_BACKEND_SPANNER_DATABASE"
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
[[ -z "$("$GIT_BIN" -C "$REPO_ROOT" status --porcelain --untracked-files=no)" ]] ||
  fail "tracked worktree must be clean before comparison"

mkdir -p "$OUTPUT/bin" "$OUTPUT/invocations" "$OUTPUT/warmup"
RUN_STARTED=true
MEASUREMENT_COMMANDS="$OUTPUT/measurement-commands.txt"
: >"$MEASUREMENT_COMMANDS"
RUN_ID="spanner-postgres-${REVISION:0:12}-$(date -u +%Y%m%dT%H%M%SZ)-$$"

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
} >"$OUTPUT/config.txt"
CONFIG_SHA256="$(sha256_file "$OUTPUT/config.txt")"

{
  printf 'captured_utc=%s\n' "$(date -u +%Y-%m-%dT%H:%M:%SZ)"
  uname -a
  rustc --version
  "$CARGO_BIN" --version
  if [[ "$MODE" == local ]]; then
    "$DOCKER_BIN" info --format \
      'docker_server={{.ServerVersion}} os={{.OperatingSystem}} cpus={{.NCPU}} memory={{.MemTotal}}'
  else
    printf 'external_postgres_identity=%s\n' "$BENCH_EXTERNAL_POSTGRES_IDENTITY"
    printf 'external_spanner_identity=%s\n' "$BENCH_EXTERNAL_SPANNER_IDENTITY"
  fi
} >"$OUTPUT/machine.txt"

if [[ "${BENCH_SKIP_BUILD:-0}" != 1 ]]; then
  "$CARGO_BIN" build --release --no-default-features --features spanner --bins \
    --manifest-path "$MANIFEST" 2>&1 | tee "$OUTPUT/build.log"
  "$CARGO_BIN" tree --no-default-features --features spanner \
    --manifest-path "$MANIFEST" >"$OUTPUT/dependencies.txt"
fi
[[ "$("$GIT_BIN" -C "$REPO_ROOT" rev-parse HEAD)" == "$REVISION" ]] ||
  fail "HEAD changed during benchmark build"
[[ -z "$("$GIT_BIN" -C "$REPO_ROOT" status --porcelain --untracked-files=no)" ]] ||
  fail "tracked worktree changed during benchmark build"
LOCKFILE_SHA256="$(sha256_file "$LOCKFILE")"

POSTGRES_SOURCE="${BENCH_POSTGRES_EXECUTABLE:-$REPO_ROOT/benchmarks/backend-comparison/target/release/prolly-backend-postgres}"
SPANNER_SOURCE="${BENCH_SPANNER_EXECUTABLE:-$REPO_ROOT/benchmarks/backend-comparison/target/release/prolly-backend-spanner}"
SUMMARIZER_SOURCE="${BENCH_SUMMARIZER_EXECUTABLE:-$REPO_ROOT/benchmarks/backend-comparison/target/release/prolly-backend-summarize}"
SERVICE_SUMMARIZER="${BENCH_SERVICE_SUMMARIZER:-$SCRIPT_DIR/summarize_mysql_postgres_service.py}"
for executable in "$POSTGRES_SOURCE" "$SPANNER_SOURCE" "$SUMMARIZER_SOURCE"; do
  [[ -x "$executable" ]] ||
    fail "benchmark executable is missing or not executable: $executable"
done
cp "$POSTGRES_SOURCE" "$OUTPUT/bin/prolly-backend-postgres"
cp "$SPANNER_SOURCE" "$OUTPUT/bin/prolly-backend-spanner"
cp "$SUMMARIZER_SOURCE" "$OUTPUT/bin/prolly-backend-summarize"
POSTGRES_EXECUTABLE="$OUTPUT/bin/prolly-backend-postgres"
SPANNER_EXECUTABLE="$OUTPUT/bin/prolly-backend-spanner"
SUMMARIZER_EXECUTABLE="$OUTPUT/bin/prolly-backend-summarize"
POSTGRES_BINARY_SHA256="$(sha256_file "$POSTGRES_EXECUTABLE")"
SPANNER_BINARY_SHA256="$(sha256_file "$SPANNER_EXECUTABLE")"
SUMMARIZER_BINARY_SHA256="$(sha256_file "$SUMMARIZER_EXECUTABLE")"
{
  printf '%s  %s\n' "$POSTGRES_BINARY_SHA256" "bin/prolly-backend-postgres"
  printf '%s  %s\n' "$SPANNER_BINARY_SHA256" "bin/prolly-backend-spanner"
  printf '%s  %s\n' "$SUMMARIZER_BINARY_SHA256" "bin/prolly-backend-summarize"
} >"$OUTPUT/binaries.sha256"

if [[ "$MODE" == local ]]; then
  if [[ "${BENCH_SKIP_IMAGE_PULL:-0}" != 1 ]]; then
    {
      "$DOCKER_BIN" pull "$POSTGRES_IMAGE"
      "$DOCKER_BIN" pull "$SPANNER_IMAGE"
    } >"$OUTPUT/image-pull.log"
  fi
  POSTGRES_IMAGE_ID="$("$DOCKER_BIN" image inspect "$POSTGRES_IMAGE" --format '{{.Id}}')"
  SPANNER_IMAGE_ID="$("$DOCKER_BIN" image inspect "$SPANNER_IMAGE" --format '{{.Id}}')"
else
  POSTGRES_IMAGE="external"
  SPANNER_IMAGE="external"
  POSTGRES_IMAGE_ID="$BENCH_EXTERNAL_POSTGRES_IDENTITY"
  SPANNER_IMAGE_ID="$BENCH_EXTERNAL_SPANNER_IDENTITY"
fi
[[ -n "$POSTGRES_IMAGE_ID" && -n "$SPANNER_IMAGE_ID" ]] ||
  fail "service identities are unavailable"
{
  printf 'postgres_image=%s\n' "$POSTGRES_IMAGE"
  printf 'postgres_image_id=%s\n' "$POSTGRES_IMAGE_ID"
  printf 'spanner_image=%s\n' "$SPANNER_IMAGE"
  printf 'spanner_image_id=%s\n' "$SPANNER_IMAGE_ID"
} >"$OUTPUT/images.txt"

run_backend() {
  local backend="$1"
  local repetition="$2"
  local output="$3"
  local service_output="$4"
  local invocation_run_id="$5"
  local executable binary_sha256
  local -a connection_args environment_args

  if [[ "$backend" == postgres ]]; then
    executable="$POSTGRES_EXECUTABLE"
    binary_sha256="$POSTGRES_BINARY_SHA256"
    connection_args=(--url "$POSTGRES_URL")
    environment_args=(env)
    if [[ "$MODE" == local ]]; then
      record_command "$DOCKER_BIN" compose -p "$POSTGRES_PROJECT" \
        -f "$COMPOSE_FILE" down -v
      record_command env PROLLY_BACKEND_POSTGRES_PORT="$POSTGRES_PORT" \
        "$DOCKER_BIN" compose -p "$POSTGRES_PROJECT" -f "$COMPOSE_FILE" up -d postgres
      wait_for_postgres
    fi
  else
    executable="$SPANNER_EXECUTABLE"
    binary_sha256="$SPANNER_BINARY_SHA256"
    connection_args=(--database "$SPANNER_DATABASE")
    if [[ "$MODE" == local ]]; then
      environment_args=(env "SPANNER_EMULATOR_HOST=127.0.0.1:$SPANNER_GRPC_PORT")
      record_command "$DOCKER_BIN" compose -p "$SPANNER_PROJECT" \
        -f "$COMPOSE_FILE" down -v
      record_command env \
        PROLLY_BACKEND_SPANNER_GRPC_PORT="$SPANNER_GRPC_PORT" \
        PROLLY_BACKEND_SPANNER_REST_PORT="$SPANNER_REST_PORT" \
        "$DOCKER_BIN" compose -p "$SPANNER_PROJECT" -f "$COMPOSE_FILE" up -d spanner
      wait_for_spanner
      initialize_spanner
    else
      environment_args=(env -u SPANNER_EMULATOR_HOST)
    fi
  fi

  local -a common_args=(
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
    --pool-size "$POOL_SIZE"
    --adapter-batch-items "$ADAPTER_BATCH_ITEMS"
    --seed "$SEED"
  )
  record_runner_command "${environment_args[@]}" "$executable" \
    --output "$output" "${common_args[@]}" "${connection_args[@]}" --suite end-to-end
  record_runner_command "${environment_args[@]}" "$executable" \
    --output "$service_output" "${common_args[@]}" "${connection_args[@]}" --suite service

  if [[ "$MODE" == local ]]; then
    if [[ "$backend" == postgres ]]; then
      record_command "$DOCKER_BIN" compose -p "$POSTGRES_PROJECT" \
        -f "$COMPOSE_FILE" down -v
    else
      record_command "$DOCKER_BIN" compose -p "$SPANNER_PROJECT" \
        -f "$COMPOSE_FILE" down -v
    fi
  fi
}

run_backend postgres 1 "$OUTPUT/warmup/postgres.csv" \
  "$OUTPUT/warmup/postgres-service.csv" "$RUN_ID-warmup-postgres"
run_backend spanner 1 "$OUTPUT/warmup/spanner.csv" \
  "$OUTPUT/warmup/spanner-service.csv" "$RUN_ID-warmup-spanner"

measured_files=()
measured_service_files=()
for repetition in $(seq 1 "$RUNS"); do
  if ((repetition % 2 == 1)); then
    order=(postgres spanner)
  else
    order=(spanner postgres)
  fi
  for backend in "${order[@]}"; do
    file="$OUTPUT/invocations/${repetition}-${backend}.csv"
    service_file="$OUTPUT/invocations/${repetition}-${backend}-service.csv"
    run_backend "$backend" "$repetition" "$file" "$service_file" "$RUN_ID"
    measured_files+=("$file")
    measured_service_files+=("$service_file")
  done
done

combine_csv() {
  local destination="$1"
  shift
  local first=true
  local header=""
  local file current_header
  for file in "$@"; do
    current_header="$(head -n 1 "$file")"
    if [[ "$first" == true ]]; then
      cp "$file" "$destination"
      header="$current_header"
      first=false
    else
      [[ "$current_header" == "$header" ]] ||
        fail "evidence headers differ: $file"
      tail -n +2 "$file" >>"$destination"
    fi
  done
}

RAW_RESULTS="$OUTPUT/raw-results.csv"
RAW_SERVICE_RESULTS="$OUTPUT/raw-service-results.csv"
combine_csv "$RAW_RESULTS" "${measured_files[@]}"
combine_csv "$RAW_SERVICE_RESULTS" "${measured_service_files[@]}"

write_shell_command "$SUMMARIZER_EXECUTABLE" \
  --input "$RAW_RESULTS" --manifest "$OUTPUT/manifest.txt" --output-dir "$OUTPUT" \
  >>"$MEASUREMENT_COMMANDS"
write_shell_command "$SERVICE_SUMMARIZER" \
  --input "$RAW_SERVICE_RESULTS" --manifest "$OUTPUT/manifest.txt" --output-dir "$OUTPUT" \
  >>"$MEASUREMENT_COMMANDS"
COMMANDS_SHA256="$(sha256_file "$MEASUREMENT_COMMANDS")"
{
  printf 'schema=backend-comparison-manifest-v1\n'
  printf 'status=complete\n'
  printf 'resumed=false\n'
  printf 'dirty=false\n'
  printf 'environment_class=%s\n' "$([[ "$MODE" == local ]] && printf controlled_local || printf external)"
  printf 'backend_a=postgres\n'
  printf 'backend_b=spanner\n'
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
  printf 'spanner_binary_sha256=%s\n' "$SPANNER_BINARY_SHA256"
  printf 'summarizer_binary_sha256=%s\n' "$SUMMARIZER_BINARY_SHA256"
  printf 'postgres_image=%s\n' "$POSTGRES_IMAGE"
  printf 'postgres_image_id=%s\n' "$POSTGRES_IMAGE_ID"
  printf 'spanner_image=%s\n' "$SPANNER_IMAGE"
  printf 'spanner_image_id=%s\n' "$SPANNER_IMAGE_ID"
} >"$OUTPUT/manifest.txt"

"$SUMMARIZER_EXECUTABLE" \
  --input "$RAW_RESULTS" --manifest "$OUTPUT/manifest.txt" --output-dir "$OUTPUT"
"$SERVICE_SUMMARIZER" \
  --input "$RAW_SERVICE_RESULTS" --manifest "$OUTPUT/manifest.txt" --output-dir "$OUTPUT"
[[ "$(sha256_file "$MEASUREMENT_COMMANDS")" == "$COMMANDS_SHA256" ]] ||
  fail "measurement command provenance changed after the manifest was sealed"
{
  printf 'status=complete\n'
  printf 'completed_utc=%s\n' "$(date -u +%Y-%m-%dT%H:%M:%SZ)"
} >"$OUTPUT/run-state.txt"
printf 'Spanner/PostgreSQL comparison complete: %s/report.md\n' "$OUTPUT"
