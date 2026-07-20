#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
BENCH_DIR="$REPO_ROOT/benchmarks/redis-scale"
MANIFEST="$BENCH_DIR/Cargo.toml"
COMPOSE_FILE="$BENCH_DIR/docker-compose.yml"
PROFILE="${REDIS_BENCH_PROFILE:-full}"
OUTPUT="${REDIS_BENCH_OUT:-$REPO_ROOT/performance-results/redis/baseline}"

while (($#)); do
  case "$1" in
    --profile)
      PROFILE="${2:?--profile requires a value}"
      shift 2
      ;;
    --output)
      OUTPUT="${2:?--output requires a value}"
      shift 2
      ;;
    *)
      printf 'unknown runner option: %s\n' "$1" >&2
      exit 2
      ;;
  esac
done

case "$PROFILE" in
  smoke)
    SIZES="${REDIS_BENCH_SIZES:-100}"
    RUNS="${REDIS_BENCH_RUNS:-1}"
    CHANGES="${REDIS_BENCH_CHANGES:-10}"
    READ_SAMPLES="${REDIS_BENCH_READ_SAMPLES:-10}"
    MIN_FREE_GB="${REDIS_BENCH_MIN_FREE_GB:-0}"
    ;;
  full)
    SIZES="${REDIS_BENCH_SIZES:-1000000}"
    RUNS="${REDIS_BENCH_RUNS:-3}"
    CHANGES="${REDIS_BENCH_CHANGES:-auto}"
    READ_SAMPLES="${REDIS_BENCH_READ_SAMPLES:-10000}"
    MIN_FREE_GB="${REDIS_BENCH_MIN_FREE_GB:-3}"
    ;;
  *)
    printf 'profile must be smoke or full\n' >&2
    exit 2
    ;;
esac

OPERATIONS="${REDIS_BENCH_OPERATIONS:-put,batch,get_cold,get_warm,query,scan,full_scan,diff,merge}"
PATTERNS="${REDIS_BENCH_PATTERNS:-append,random,clustered}"
TOKIO_WORKERS="${REDIS_BENCH_TOKIO_WORKERS:-4}"
PROJECT="prolly-redis-bench-$$"
COMPOSE=(docker compose -p "$PROJECT" -f "$COMPOSE_FILE")

cleanup() {
  if [[ "${REDIS_BENCH_KEEP_CONTAINER:-0}" == 1 ]]; then
    printf 'Redis benchmark container retained for compose project %s\n' "$PROJECT"
  else
    "${COMPOSE[@]}" down --volumes --remove-orphans >/dev/null 2>&1 || true
  fi
}
trap cleanup EXIT INT TERM

docker info >/dev/null
mkdir -p "$OUTPUT"
"${COMPOSE[@]}" up -d
CONTAINER_ID="$("${COMPOSE[@]}" ps -q redis)"
if [[ -z "$CONTAINER_ID" ]]; then
  printf 'Redis benchmark container did not start\n' >&2
  exit 1
fi
for _ in $(seq 1 60); do
  if docker exec "$CONTAINER_ID" redis-cli ping 2>/dev/null | rg -q '^PONG$'; then
    break
  fi
  sleep 1
done
if ! docker exec "$CONTAINER_ID" redis-cli ping | rg -q '^PONG$'; then
  printf 'Redis did not become ready\n' >&2
  exit 1
fi

PORT_BINDING="$("${COMPOSE[@]}" port redis 6379)"
REDIS_PORT="${PORT_BINDING##*:}"
if [[ ! "$REDIS_PORT" =~ ^[0-9]+$ ]]; then
  printf 'could not determine Redis host port from %s\n' "$PORT_BINDING" >&2
  exit 1
fi
REDIS_URL="redis://127.0.0.1:$REDIS_PORT/"

REVISION="$(git -C "$REPO_ROOT" rev-parse HEAD 2>/dev/null || printf unknown)"
if [[ -n "$(git -C "$REPO_ROOT" status --porcelain 2>/dev/null || true)" ]]; then
  DIRTY=true
  DIRTY_ARG=--dirty
else
  DIRTY=false
  DIRTY_ARG=--clean
fi
STARTED_UTC="$(date -u +%Y-%m-%dT%H:%M:%SZ)"

{
  printf 'captured_utc=%s\n' "$STARTED_UTC"
  uname -a
  sysctl -n machdep.cpu.brand_string 2>/dev/null || true
  sysctl -n hw.logicalcpu 2>/dev/null || true
  sysctl -n hw.memsize 2>/dev/null || true
  rustc --version
  cargo --version
  docker version
  docker compose version
  df -h "$OUTPUT"
} > "$OUTPUT/machine.txt"

"${COMPOSE[@]}" config > "$OUTPUT/docker-compose-resolved.yml"
docker inspect "$CONTAINER_ID" > "$OUTPUT/redis-container-inspect.json"
docker image inspect redis:7.4.2-bookworm > "$OUTPUT/redis-image-inspect.json"
{
  docker exec "$CONTAINER_ID" redis-server --version
  docker exec "$CONTAINER_ID" redis-cli --raw CONFIG GET appendonly appendfsync save auto-aof-rewrite-percentage no-appendfsync-on-rewrite
} > "$OUTPUT/redis-config.txt"
docker exec "$CONTAINER_ID" redis-cli INFO memory persistence > "$OUTPUT/redis-info-before.txt"

git -C "$REPO_ROOT" status --porcelain=v1 > "$OUTPUT/source-status.txt"
git -C "$REPO_ROOT" diff --binary HEAD | gzip -9 > "$OUTPUT/source-diff.patch.gz"

if [[ "${REDIS_BENCH_SKIP_BUILD:-0}" != 1 ]]; then
  CARGO_INCREMENTAL=0 cargo build --release --manifest-path "$MANIFEST" \
    2>&1 | tee "$OUTPUT/build.log"
fi
cargo tree --manifest-path "$MANIFEST" > "$OUTPUT/dependencies.txt"
cargo tree --manifest-path "$MANIFEST" -e features > "$OUTPUT/dependency-features.txt"

if [[ -n "${REDIS_BENCH_EXECUTABLE:-}" ]]; then
  EXECUTABLE="$REDIS_BENCH_EXECUTABLE"
else
  TARGET_DIR="$(cargo metadata --manifest-path "$MANIFEST" --format-version 1 --no-deps \
    | python3 -c 'import json,sys; print(json.load(sys.stdin)["target_directory"])')"
  EXECUTABLE="$TARGET_DIR/release/prolly-redis-scale-bench"
fi
if [[ ! -x "$EXECUTABLE" ]]; then
  printf 'benchmark executable is missing or not executable: %s\n' "$EXECUTABLE" >&2
  exit 1
fi
shasum -a 256 "$EXECUTABLE" > "$OUTPUT/binary.sha256"

ARGS=(
  --profile "$PROFILE"
  --redis-url "$REDIS_URL"
  --output "$OUTPUT"
  --revision "$REVISION"
  "$DIRTY_ARG"
  --sizes "$SIZES"
  --runs "$RUNS"
  --operations "$OPERATIONS"
  --patterns "$PATTERNS"
  --changes "$CHANGES"
  --read-samples "$READ_SAMPLES"
  --min-free-gb "$MIN_FREE_GB"
  --tokio-workers "$TOKIO_WORKERS"
)
if [[ "${REDIS_BENCH_KEEP_FIXTURES:-0}" == 1 ]]; then
  ARGS+=(--keep-fixtures)
fi

{
  printf 'driver=%s\n' "$REPO_ROOT/scripts/run_redis_scale_benchmark.sh"
  printf 'revision=%s\n' "$REVISION"
  printf 'dirty=%s\n' "$DIRTY"
  printf 'started_utc=%s\n' "$STARTED_UTC"
  printf 'redis_url=%s\n' "$REDIS_URL"
  printf 'redis_image=redis:7.4.2-bookworm\n'
  printf 'durability=appendonly=yes,appendfsync=always,save=disabled,auto-aof-rewrite=disabled\n'
  printf 'arguments='
  printf ' %q' "${ARGS[@]}"
  printf '\n'
} > "$OUTPUT/driver-provenance.txt"

"$EXECUTABLE" "${ARGS[@]}" 2>&1 | tee "$OUTPUT/run.log"
docker exec "$CONTAINER_ID" redis-cli INFO memory persistence > "$OUTPUT/redis-info-after.txt"
printf 'ended_utc=%s\n' "$(date -u +%Y-%m-%dT%H:%M:%SZ)" >> "$OUTPUT/driver-provenance.txt"

tar -czf "$OUTPUT/harness-source.tar.gz" -C "$REPO_ROOT" \
  benchmarks/redis-scale/Cargo.toml \
  benchmarks/redis-scale/Cargo.lock \
  benchmarks/redis-scale/docker-compose.yml \
  benchmarks/redis-scale/redis.conf \
  benchmarks/redis-scale/src \
  benchmarks/redis-scale/tests \
  stores/prolly-store-redis/Cargo.toml \
  scripts/run_redis_scale_benchmark.sh \
  docs/redis-scale-benchmark.md
(
  cd "$OUTPUT"
  shasum -a 256 harness-source.tar.gz > harness-source.sha256
)
printf 'Redis scale benchmark complete: %s\n' "$OUTPUT"
