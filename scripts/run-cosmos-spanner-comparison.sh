#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
COMPOSE_FILE="$ROOT_DIR/docker-compose.store-services.yml"
PROJECT_NAME="${PROLLY_CLOUD_COMPOSE_PROJECT:-prolly-cosmos-spanner-comparison}"
COSMOS_PORT="${PROLLY_STORE_COSMOS_PORT:-58081}"
COSMOS_HEALTH_PORT="${PROLLY_STORE_COSMOS_HEALTH_PORT:-58080}"
SPANNER_GRPC_PORT="${PROLLY_STORE_SPANNER_GRPC_PORT:-9010}"
SPANNER_REST_PORT="${PROLLY_STORE_SPANNER_REST_PORT:-9020}"
OUTPUT="${PROLLY_CLOUD_BENCH_OUTPUT:-$ROOT_DIR/performance-results/cosmos-spanner-$(date -u +%Y%m%dT%H%M%SZ)}"
LOCAL_COSMOS=0
LOCAL_SPANNER=0

compose() {
  docker compose -p "$PROJECT_NAME" -f "$COMPOSE_FILE" "$@"
}

cleanup() {
  if [[ "${KEEP_PROLLY_CLOUD_SERVICES:-0}" != "1" ]] &&
    ((LOCAL_COSMOS == 1 || LOCAL_SPANNER == 1)); then
    compose down -v >/dev/null 2>&1 || true
  fi
}
trap cleanup EXIT

wait_for_url() {
  local name="$1"
  local url="$2"
  for _ in $(seq 1 180); do
    if curl --silent --fail "$url" >/dev/null 2>&1; then
      printf '%s is ready\n' "$name"
      return 0
    fi
    sleep 1
  done
  printf '%s did not become ready: %s\n' "$name" "$url" >&2
  compose ps >&2 || true
  return 1
}

wait_for_tcp() {
  local name="$1"
  local port="$2"
  for _ in $(seq 1 120); do
    if (echo >"/dev/tcp/127.0.0.1/$port") >/dev/null 2>&1; then
      printf '%s is listening on port %s\n' "$name" "$port"
      return 0
    fi
    sleep 1
  done
  printf '%s did not start on port %s\n' "$name" "$port" >&2
  return 1
}

wait_for_cosmos_init() {
  for _ in $(seq 1 120); do
    if compose logs --no-color cosmosdb 2>/dev/null |
      grep -q "All initialization scripts processed successfully"; then
      printf 'Cosmos DB emulator schema is ready\n'
      return 0
    fi
    sleep 1
  done
  printf 'Cosmos DB initialization did not complete\n' >&2
  compose logs cosmosdb >&2 || true
  return 1
}

services=()
if [[ -z "${PROLLY_STORE_COSMOS_ENDPOINT:-}" ]]; then
  LOCAL_COSMOS=1
  services+=(cosmosdb)
fi
if [[ -z "${PROLLY_STORE_SPANNER_DATABASE:-}" ]]; then
  LOCAL_SPANNER=1
  services+=(spanner)
fi

if ((${#services[@]} > 0)); then
  export PROLLY_STORE_COSMOS_PORT="$COSMOS_PORT"
  export PROLLY_STORE_COSMOS_HEALTH_PORT="$COSMOS_HEALTH_PORT"
  export PROLLY_STORE_SPANNER_GRPC_PORT="$SPANNER_GRPC_PORT"
  export PROLLY_STORE_SPANNER_REST_PORT="$SPANNER_REST_PORT"
  compose up -d "${services[@]}"
fi

if ((LOCAL_COSMOS == 1)); then
  wait_for_url "Cosmos DB emulator" "http://127.0.0.1:$COSMOS_HEALTH_PORT/ready"
  wait_for_cosmos_init
  export PROLLY_STORE_COSMOS_ENDPOINT="http://127.0.0.1:$COSMOS_PORT"
  export PROLLY_STORE_COSMOS_KEY="C2y6yDjf5/R+ob0N8A7Cgv30VRDJIWEHLM+4QDU5DE2nQ9nDuVTqobD4b8mGGyPMbIZnqyMsEcaGQy67XIw/Jw=="
  export PROLLY_STORE_COSMOS_DATABASE="prolly"
  export PROLLY_STORE_COSMOS_CONTAINER="prolly_store"
else
  : "${PROLLY_STORE_COSMOS_KEY:?managed Cosmos comparison requires PROLLY_STORE_COSMOS_KEY}"
  : "${PROLLY_STORE_COSMOS_DATABASE:?managed Cosmos comparison requires PROLLY_STORE_COSMOS_DATABASE}"
  : "${PROLLY_STORE_COSMOS_CONTAINER:?managed Cosmos comparison requires PROLLY_STORE_COSMOS_CONTAINER}"
fi

if ((LOCAL_SPANNER == 1)); then
  wait_for_tcp "Spanner emulator" "$SPANNER_GRPC_PORT"
  spanner_rest="http://127.0.0.1:$SPANNER_REST_PORT"
  curl --silent --fail-with-body \
    -H "content-type: application/json" \
    -X POST "$spanner_rest/v1/projects/prolly-local/instances" \
    --data-binary '{
      "instanceId": "prolly-instance",
      "instance": {
        "config": "projects/prolly-local/instanceConfigs/emulator-config",
        "displayName": "Prolly Local",
        "nodeCount": 1
      }
    }' >/dev/null
  curl --silent --fail-with-body \
    -H "content-type: application/json" \
    -X POST "$spanner_rest/v1/projects/prolly-local/instances/prolly-instance/databases" \
    --data-binary '{
      "createStatement": "CREATE DATABASE `prolly`",
      "extraStatements": [
        "CREATE TABLE ProllyNodes (Cid BYTES(32) NOT NULL, Node BYTES(MAX) NOT NULL) PRIMARY KEY (Cid)",
        "CREATE TABLE ProllyHints (Namespace BYTES(MAX) NOT NULL, HintKey BYTES(MAX) NOT NULL, Value BYTES(MAX) NOT NULL) PRIMARY KEY (Namespace, HintKey)",
        "CREATE TABLE ProllyRoots (Name BYTES(MAX) NOT NULL, Manifest BYTES(MAX) NOT NULL) PRIMARY KEY (Name)"
      ]
    }' >/dev/null
  wait_for_url \
    "Spanner test database" \
    "$spanner_rest/v1/projects/prolly-local/instances/prolly-instance/databases/prolly"
  export SPANNER_EMULATOR_HOST="127.0.0.1:$SPANNER_GRPC_PORT"
  export PROLLY_STORE_SPANNER_DATABASE="projects/prolly-local/instances/prolly-instance/databases/prolly"
elif [[ -z "${PROLLY_STORE_SPANNER_AUTH:-}" ]]; then
  export PROLLY_STORE_SPANNER_AUTH=1
fi

mkdir -p "$OUTPUT"
{
  printf 'captured_utc=%s\n' "$(date -u +%Y-%m-%dT%H:%M:%SZ)"
  printf 'git_revision=%s\n' "$(git -C "$ROOT_DIR" rev-parse HEAD)"
  rustc --version
  cargo --version
  docker --version 2>/dev/null || true
  if ((LOCAL_COSMOS == 1)); then
    compose images cosmosdb
  fi
  if ((LOCAL_SPANNER == 1)); then
    compose images spanner
  fi
} >"$OUTPUT/environment.txt"

cargo test --manifest-path "$ROOT_DIR/stores/prolly-store-cosmosdb/Cargo.toml" \
  2>&1 | tee "$OUTPUT/cosmos-conformance.log"
cargo test --manifest-path "$ROOT_DIR/stores/prolly-store-spanner/Cargo.toml" \
  2>&1 | tee "$OUTPUT/spanner-conformance.log"

cargo run --release \
  --manifest-path "$ROOT_DIR/benchmarks/cosmos-spanner-comparison/Cargo.toml" \
  -- cosmos | tee "$OUTPUT/cosmos.json"
cargo run --release \
  --manifest-path "$ROOT_DIR/benchmarks/cosmos-spanner-comparison/Cargo.toml" \
  -- spanner | tee "$OUTPUT/spanner.json"

printf 'comparison artifacts: %s\n' "$OUTPUT"
