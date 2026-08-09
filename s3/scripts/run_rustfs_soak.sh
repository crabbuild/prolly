#!/usr/bin/env bash
set -euo pipefail

soak_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
repository_root="$(cd "$soak_root/.." && pwd)"
duration_seconds="${PROLLY_S3_SOAK_SECONDS:-86400}"
cargo_toolchain="${PROLLY_S3_CARGO_TOOLCHAIN:-+1.94.1}"
container="${PROLLY_RUSTFS_CONTAINER:-prolly-rustfs}"
data_dir="${PROLLY_RUSTFS_DATA_DIR:-/Volumes/Workspace/prolly-data}"
build_dir="${CARGO_TARGET_DIR:-${PROLLY_S3_CARGO_TARGET_DIR:-/Volumes/Workspace/prolly-build/versioned-s3}}"
run_id="${PROLLY_S3_SOAK_RUN_ID:-$(date -u +%Y%m%dT%H%M%SZ)-$$}"
evidence_dir="${PROLLY_S3_SOAK_EVIDENCE_DIR:-$build_dir/soak-evidence/$run_id}"
max_memory_bytes="${PROLLY_S3_SOAK_MAX_RUSTFS_MEMORY_BYTES:-1073741824}"
max_iteration_growth_kib="${PROLLY_S3_SOAK_MAX_DATA_GROWTH_KIB_PER_ITERATION:-32768}"
max_total_growth_kib="${PROLLY_S3_SOAK_MAX_TOTAL_DATA_GROWTH_KIB:-8388608}"
max_repository_storage_bytes="${PROLLY_S3_SOAK_MAX_REPOSITORY_BYTES_PER_WORKFLOW:-16777216}"
max_build_growth_kib="${PROLLY_S3_SOAK_MAX_BUILD_GROWTH_KIB:-65536}"
iteration_interval_seconds="${PROLLY_S3_SOAK_ITERATION_INTERVAL_SECONDS:-60}"

positive_integer() {
  [[ "$1" =~ ^[1-9][0-9]*$ ]]
}

nonnegative_integer() {
  [[ "$1" =~ ^[0-9]+$ ]]
}

if ! positive_integer "$duration_seconds"; then
  echo "PROLLY_S3_SOAK_SECONDS must be a positive integer" >&2
  exit 2
fi
if ! positive_integer "$max_memory_bytes"; then
  echo "PROLLY_S3_SOAK_MAX_RUSTFS_MEMORY_BYTES must be a positive integer" >&2
  exit 2
fi
if ! positive_integer "$max_iteration_growth_kib"; then
  echo "PROLLY_S3_SOAK_MAX_DATA_GROWTH_KIB_PER_ITERATION must be a positive integer" >&2
  exit 2
fi
if ! positive_integer "$max_total_growth_kib"; then
  echo "PROLLY_S3_SOAK_MAX_TOTAL_DATA_GROWTH_KIB must be a positive integer" >&2
  exit 2
fi
if ! positive_integer "$max_repository_storage_bytes"; then
  echo "PROLLY_S3_SOAK_MAX_REPOSITORY_BYTES_PER_WORKFLOW must be a positive integer" >&2
  exit 2
fi
if ! nonnegative_integer "$max_build_growth_kib"; then
  echo "PROLLY_S3_SOAK_MAX_BUILD_GROWTH_KIB must be a nonnegative integer" >&2
  exit 2
fi
if ! positive_integer "$iteration_interval_seconds"; then
  echo "PROLLY_S3_SOAK_ITERATION_INTERVAL_SECONDS must be a positive integer" >&2
  exit 2
fi
if [[ ! "$run_id" =~ ^[A-Za-z0-9._-]+$ ]]; then
  echo "PROLLY_S3_SOAK_RUN_ID must contain only letters, digits, dot, underscore, or hyphen" >&2
  exit 2
fi
if [[ "${PROLLY_S3_RUSTFS:-}" != "1" ]]; then
  echo "set PROLLY_S3_RUSTFS=1 and the RustFS endpoint credentials" >&2
  exit 2
fi
if [[ ! -d "$data_dir" ]]; then
  echo "RustFS data directory does not exist: $data_dir" >&2
  exit 2
fi
if ! docker inspect "$container" >/dev/null 2>&1; then
  echo "RustFS container $container does not exist" >&2
  exit 2
fi
mounted_data_dir="$(docker inspect --format '{{range .Mounts}}{{if eq .Destination "/data"}}{{.Source}}{{end}}{{end}}' "$container")"
if [[ "$mounted_data_dir" != "$data_dir" ]]; then
  echo "RustFS /data mount mismatch: expected=$data_dir actual=$mounted_data_dir" >&2
  exit 2
fi
health="$(docker inspect --format '{{if .State.Health}}{{.State.Health.Status}}{{else}}{{.State.Status}}{{end}}' "$container")"
if [[ "$health" != "healthy" ]]; then
  echo "RustFS must be healthy before the soak: health=$health" >&2
  exit 2
fi
if [[ -e "$evidence_dir" ]]; then
  echo "soak evidence directory already exists: $evidence_dir" >&2
  exit 2
fi
mkdir -p "$(dirname "$evidence_dir")"
mkdir "$evidence_dir"
soak_log="$evidence_dir/soak.log"
verification_log="$evidence_dir/verification.log"
checksums="$evidence_dir/checksums.sha256"

export CARGO_TARGET_DIR="$build_dir"
export RUSTC_WRAPPER=""

# Build and resolve one immutable test executable before the measured interval
# so per-iteration environment markers cannot trigger Cargo recompilation.
test_binary="$(
  cargo "$cargo_toolchain" test --manifest-path "$soak_root/Cargo.toml" \
    -p prolly-s3-client --all-features --test rustfs_repository --no-run \
    --message-format=json |
    python3 -c 'import json, sys
executables = []
for line in sys.stdin:
    try:
        message = json.loads(line)
    except json.JSONDecodeError:
        continue
    if (
        message.get("reason") == "compiler-artifact"
        and message.get("target", {}).get("name") == "rustfs_repository"
        and message.get("executable")
    ):
        executables.append(message["executable"])
if len(executables) != 1:
    raise SystemExit(f"expected one rustfs_repository executable, found {len(executables)}")
print(executables[0])'
)"
if [[ ! -x "$test_binary" ]]; then
  echo "resolved RustFS test binary is not executable: $test_binary" >&2
  exit 1
fi
test_binary_sha256="$(shasum -a 256 "$test_binary" | awk '{print $1}')"

epoch_millis() {
  python3 -c 'import time; print(time.time_ns() // 1_000_000)'
}

directory_kib() {
  du -sk "$1" | awk '{print $1}'
}

rustfs_memory_bytes() {
  local usage
  usage="$(docker stats --no-stream --format '{{.MemUsage}}' "$container" | awk '{print $1}')"
  python3 -c 'import re, sys
value = sys.argv[1]
match = re.fullmatch(r"([0-9]+(?:[.][0-9]+)?)(B|KiB|MiB|GiB|TiB)", value)
if match is None:
    raise SystemExit(f"unsupported docker memory value: {value}")
units = {"B": 1, "KiB": 1024, "MiB": 1024**2, "GiB": 1024**3, "TiB": 1024**4}
print(int(float(match.group(1)) * units[match.group(2)]))' "$usage"
}

run_test() {
  local iteration="$1"
  local name="$2"
  local test_name="$3"
  local test_started test_finished status
  test_started="$(epoch_millis)"
  set +e
  PROLLY_S3_SOAK_RUN_ID="$run_id" \
  PROLLY_S3_SOAK_ITERATION="$iteration" \
    "$test_binary" "$test_name" --exact --nocapture
  status="$?"
  set -e
  test_finished="$(epoch_millis)"
  if [[ "$status" == "0" ]]; then
    echo "SOAK_TEST run_id=$run_id iteration=$iteration name=$name status=passed elapsed_millis=$((test_finished - test_started))"
    return 0
  fi
  echo "SOAK_TEST run_id=$run_id iteration=$iteration name=$name status=failed elapsed_millis=$((test_finished - test_started)) exit_code=$status"
  return "$status"
}

run_soak() {
  local source_revision source_state rustfs_image restart_count
  local initial_data_kib initial_build_kib started_at deadline
  local iterations=0 test_runs=0 max_observed_memory_bytes=0
  local max_iteration_millis=0 iteration_started iteration_finished now
  local interval_wait remaining_seconds current_epoch iteration_elapsed_seconds
  local data_kib build_kib memory_bytes total_growth

  source_revision="$(git -C "$repository_root" rev-parse HEAD 2>/dev/null || echo unversioned)"
  if [[ -n "$(git -C "$repository_root" status --porcelain --untracked-files=normal)" ]]; then
    source_state="dirty"
  else
    source_state="clean"
  fi
  rustfs_image="$(docker inspect --format '{{.Config.Image}}@{{.Image}}' "$container")"
  restart_count="$(docker inspect --format '{{.RestartCount}}' "$container")"
  initial_data_kib="$(directory_kib "$data_dir")"
  initial_build_kib="$(directory_kib "$build_dir")"
  started_at="$(date +%s)"
  deadline="$((started_at + duration_seconds))"

  echo "SOAK_START schema=prolly-s3-soak/v2 run_id=$run_id epoch=$started_at duration_seconds=$duration_seconds iteration_interval_seconds=$iteration_interval_seconds initial_data_kib=$initial_data_kib initial_build_kib=$initial_build_kib container=$container health=healthy image=$rustfs_image mount=$data_dir:/data restart_count=$restart_count source_revision=$source_revision source_state=$source_state test_binary_sha256=$test_binary_sha256 cargo_version=$(cargo "$cargo_toolchain" --version | tr ' ' '_') rustc_version=$(rustc "$cargo_toolchain" --version | tr ' ' '_') max_rustfs_memory_bytes=$max_memory_bytes max_data_growth_kib_per_iteration=$max_iteration_growth_kib max_total_data_growth_kib=$max_total_growth_kib max_repository_bytes_per_workflow=$max_repository_storage_bytes max_build_growth_kib=$max_build_growth_kib"

  while (( iterations == 0 || $(date +%s) < deadline )); do
    iterations="$((iterations + 1))"
    iteration_started="$(epoch_millis)"
    run_test "$iterations" ref-contention \
      rustfs_branch_tag_and_merge_contend_across_independent_processes
    test_runs="$((test_runs + 1))"
    run_test "$iterations" multipart-recovery \
      rustfs_completing_upload_resumes_in_independent_process
    test_runs="$((test_runs + 1))"
    iteration_finished="$(epoch_millis)"

    health="$(docker inspect --format '{{if .State.Health}}{{.State.Health.Status}}{{else}}{{.State.Status}}{{end}}' "$container")"
    if [[ "$health" != "healthy" ]]; then
      echo "SOAK_INVARIANT_FAILED run_id=$run_id iteration=$iterations invariant=rustfs_health actual=$health"
      return 1
    fi
    now="$(docker inspect --format '{{.RestartCount}}' "$container")"
    if [[ "$now" != "$restart_count" ]]; then
      echo "SOAK_INVARIANT_FAILED run_id=$run_id iteration=$iterations invariant=restart_count expected=$restart_count actual=$now"
      return 1
    fi

    memory_bytes="$(rustfs_memory_bytes)"
    now="$((iteration_finished - iteration_started))"
    if (( memory_bytes <= 0 || memory_bytes > max_memory_bytes )); then
      echo "SOAK_INVARIANT_FAILED run_id=$run_id iteration=$iterations invariant=rustfs_memory_bytes expected_max=$max_memory_bytes actual=$memory_bytes"
      return 1
    fi
    if (( memory_bytes > max_observed_memory_bytes )); then
      max_observed_memory_bytes="$memory_bytes"
    fi
    if (( now > max_iteration_millis )); then
      max_iteration_millis="$now"
    fi
    echo "SOAK_ITERATION run_id=$run_id epoch=$(date +%s) iteration=$iterations elapsed_millis=$now rustfs_memory_bytes=$memory_bytes health=$health restart_count=$restart_count"

    current_epoch="$(date +%s)"
    iteration_elapsed_seconds="$(((now + 999) / 1000))"
    if (( current_epoch < deadline && iteration_elapsed_seconds < iteration_interval_seconds )); then
      interval_wait="$((iteration_interval_seconds - iteration_elapsed_seconds))"
      remaining_seconds="$((deadline - current_epoch))"
      if (( interval_wait > remaining_seconds )); then
        interval_wait="$remaining_seconds"
      fi
      if (( interval_wait > 0 )); then
        sleep "$interval_wait"
      fi
    fi
  done

  now="$(date +%s)"
  if [[ "$(shasum -a 256 "$test_binary" | awk '{print $1}')" != "$test_binary_sha256" ]]; then
    echo "SOAK_INVARIANT_FAILED run_id=$run_id iteration=$iterations invariant=test_binary_sha256"
    return 1
  fi
  data_kib="$(directory_kib "$data_dir")"
  total_growth="$((data_kib - initial_data_kib))"
  if (( total_growth > iterations * max_iteration_growth_kib )); then
    echo "SOAK_INVARIANT_FAILED run_id=$run_id iteration=$iterations invariant=total_data_growth_kib expected_max=$((iterations * max_iteration_growth_kib)) actual=$total_growth"
    return 1
  fi
  if (( total_growth > max_total_growth_kib )); then
    echo "SOAK_INVARIANT_FAILED run_id=$run_id iteration=$iterations invariant=total_data_growth_kib_absolute expected_max=$max_total_growth_kib actual=$total_growth"
    return 1
  fi
  build_kib="$(directory_kib "$build_dir")"
  if (( build_kib - initial_build_kib > max_build_growth_kib )); then
    echo "SOAK_INVARIANT_FAILED run_id=$run_id iteration=$iterations invariant=build_growth_kib expected_max=$max_build_growth_kib actual=$((build_kib - initial_build_kib))"
    return 1
  fi
  health="$(docker inspect --format '{{if .State.Health}}{{.State.Health.Status}}{{else}}{{.State.Status}}{{end}}' "$container")"
  echo "SOAK_COMPLETE schema=prolly-s3-soak/v2 run_id=$run_id epoch=$now elapsed_seconds=$((now - started_at)) iterations=$iterations test_runs=$test_runs final_data_kib=$data_kib data_growth_kib=$total_growth final_build_kib=$build_kib build_growth_kib=$((build_kib - initial_build_kib)) max_rustfs_memory_bytes_observed=$max_observed_memory_bytes max_iteration_millis=$max_iteration_millis health=$health restart_count=$(docker inspect --format '{{.RestartCount}}' "$container")"
}

set +e
run_soak 2>&1 | tee "$soak_log"
soak_status="${PIPESTATUS[0]}"
set -e
if [[ "$soak_status" != "0" ]]; then
  echo "soak failed; incomplete evidence preserved at $evidence_dir" >&2
  exit "$soak_status"
fi

python3 "$soak_root/scripts/verify_soak_evidence.py" \
  --test-log "$soak_log" --minimum-seconds "$duration_seconds" | tee "$verification_log"
shasum -a 256 "$soak_log" "$verification_log" | tee "$checksums"
echo "RUSTFS_SOAK_EVIDENCE_COMPLETE evidence=$evidence_dir run_id=$run_id"
