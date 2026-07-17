#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
COMPOSE_FILE="$ROOT_DIR/docker-compose.store-services.yml"
PROJECT_NAME="${PROLLY_STORE_COMPOSE_PROJECT:-prolly-store-services}"
SERVICES_RUNNING=0
if [[ "${1:-}" == "--services-running" ]]; then
  SERVICES_RUNNING=1
elif [[ -n "${1:-}" ]]; then
  echo "usage: $0 [--services-running]" >&2
  exit 2
fi

DYNAMODB_PORT="${PROLLY_STORE_DYNAMODB_PORT:-8000}"
REDIS_PORT="${PROLLY_STORE_REDIS_PORT:-56379}"
POSTGRES_PORT="${PROLLY_STORE_POSTGRES_PORT:-55432}"
MYSQL_PORT="${PROLLY_STORE_MYSQL_PORT:-53306}"
SPANNER_GRPC_PORT="${PROLLY_STORE_SPANNER_GRPC_PORT:-9010}"
SPANNER_REST_PORT="${PROLLY_STORE_SPANNER_REST_PORT:-9020}"

export PROLLY_STORE_DYNAMODB_PORT="$DYNAMODB_PORT"
export PROLLY_STORE_REDIS_PORT="$REDIS_PORT"
export PROLLY_STORE_POSTGRES_PORT="$POSTGRES_PORT"
export PROLLY_STORE_MYSQL_PORT="$MYSQL_PORT"
export PROLLY_STORE_SPANNER_GRPC_PORT="$SPANNER_GRPC_PORT"
export PROLLY_STORE_SPANNER_REST_PORT="$SPANNER_REST_PORT"

compose() {
  docker compose -p "$PROJECT_NAME" -f "$COMPOSE_FILE" "$@"
}

cleanup() {
  if [[ "$SERVICES_RUNNING" == "0" && "${KEEP_PROLLY_STORE_SERVICES:-0}" != "1" ]]; then
    compose down -v
  fi
}
trap cleanup EXIT

if [[ "$SERVICES_RUNNING" == "0" ]]; then
  compose up -d postgres mysql redis dynamodb spanner
fi

wait_for_tcp() {
  local name="$1"
  local port="$2"
  for _ in $(seq 1 120); do
    if (echo >"/dev/tcp/127.0.0.1/$port") >/dev/null 2>&1; then
      echo "$name is listening on 127.0.0.1:$port"
      return 0
    fi
    sleep 1
  done
  echo "$name did not start on 127.0.0.1:$port" >&2
  compose ps
  return 1
}

wait_for_tcp "PostgreSQL" "$POSTGRES_PORT"
wait_for_tcp "MySQL" "$MYSQL_PORT"
wait_for_tcp "Redis" "$REDIS_PORT"
wait_for_tcp "DynamoDB Local" "$DYNAMODB_PORT"
wait_for_tcp "Spanner emulator" "$SPANNER_GRPC_PORT"

export AWS_ACCESS_KEY_ID="${AWS_ACCESS_KEY_ID:-test}"
export AWS_REGION="${AWS_REGION:-us-west-2}"
export AWS_SECRET_ACCESS_KEY="${AWS_SECRET_ACCESS_KEY:-test}"
export PROLLY_POSTGRES_URL="postgres://prolly:prolly@127.0.0.1:$POSTGRES_PORT/prolly?sslmode=disable"
export PROLLY_MYSQL_URL="mysql://prolly:prolly@127.0.0.1:$MYSQL_PORT/prolly"
export PROLLY_REDIS_URL="redis://127.0.0.1:$REDIS_PORT"
export PROLLY_DYNAMODB_ENDPOINT="http://127.0.0.1:$DYNAMODB_PORT"
export SPANNER_EMULATOR_HOST="127.0.0.1:$SPANNER_GRPC_PORT"

node "$ROOT_DIR/scripts/verify-store-compatibility.mjs"

for module in sqlite postgres mysql redis dynamodb cosmosdb spanner pglite; do
  echo "checking Node store: $module"
  npm --prefix "$ROOT_DIR/bindings/node/stores/$module" run check
  echo "testing Node store: $module"
  if [[ "$module" == "cosmosdb" && "${RUN_PROLLY_COSMOS_LIVE:-0}" != "1" ]]; then
    (
      unset PROLLY_COSMOS_ENDPOINT PROLLY_COSMOS_KEY PROLLY_COSMOS_DATABASE
      npm --prefix "$ROOT_DIR/bindings/node/stores/$module" test
    )
  else
    npm --prefix "$ROOT_DIR/bindings/node/stores/$module" test
  fi
done

if [[ "${RUN_PROLLY_COSMOS_LIVE:-0}" == "1" ]]; then
  : "${PROLLY_COSMOS_ENDPOINT:?RUN_PROLLY_COSMOS_LIVE=1 requires PROLLY_COSMOS_ENDPOINT}"
  : "${PROLLY_COSMOS_KEY:?RUN_PROLLY_COSMOS_LIVE=1 requires PROLLY_COSMOS_KEY}"
  : "${PROLLY_COSMOS_DATABASE:?RUN_PROLLY_COSMOS_LIVE=1 requires PROLLY_COSMOS_DATABASE}"
  echo "Cosmos DB SDK-contract and live gates enabled"
else
  echo "Cosmos DB SDK-contract gates passed; live gate skipped (set RUN_PROLLY_COSMOS_LIVE=1 with credentials)"
fi

if [[ -n "${PROLLY_STORE_JAVA_HOME:-}" ]]; then
  export JAVA_HOME="$PROLLY_STORE_JAVA_HOME"
fi
mvn -f "$ROOT_DIR/bindings/pom.xml" -Dstyle.color=never test

echo "all Node, Kotlin, and Java remote-store providers passed"
