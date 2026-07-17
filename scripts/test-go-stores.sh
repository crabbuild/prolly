#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
COMPOSE_FILE="$ROOT_DIR/docker-compose.store-services.yml"
PROJECT_NAME="${PROLLY_STORE_COMPOSE_PROJECT:-prolly-store-services}"
SERVICES_RUNNING=0
if [[ "${1:-}" == "--services-running" ]]; then
  SERVICES_RUNNING=1
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

if [[ "$SERVICES_RUNNING" == "0" ]]; then
  compose up -d postgres mysql redis dynamodb spanner
fi
wait_for_tcp "PostgreSQL" "$POSTGRES_PORT"
wait_for_tcp "MySQL" "$MYSQL_PORT"
wait_for_tcp "Redis" "$REDIS_PORT"
wait_for_tcp "DynamoDB Local" "$DYNAMODB_PORT"
wait_for_tcp "Spanner emulator" "$SPANNER_GRPC_PORT"

export PROLLY_POSTGRES_URL="postgres://prolly:prolly@127.0.0.1:$POSTGRES_PORT/prolly?sslmode=disable"
export PROLLY_MYSQL_DSN="prolly:prolly@tcp(127.0.0.1:$MYSQL_PORT)/prolly?parseTime=true"
export PROLLY_REDIS_ADDR="127.0.0.1:$REDIS_PORT"
export PROLLY_DYNAMODB_ENDPOINT="http://127.0.0.1:$DYNAMODB_PORT"
export SPANNER_EMULATOR_HOST="127.0.0.1:$SPANNER_GRPC_PORT"

cd "$ROOT_DIR/bindings/go"
go test -race ./...
go run ./internal/verifycompat ../../conformance/store-protocol-v1/compatibility.json

core_deps="$(go list -deps ./...)"
if grep -Eq 'modernc.org/sqlite|github.com/jackc/pgx|github.com/go-sql-driver/mysql|github.com/redis/go-redis|github.com/aws/aws-sdk-go-v2/service/dynamodb|github.com/Azure/azure-sdk-for-go/sdk/data/azcosmos|cloud.google.com/go/spanner' <<<"$core_deps"; then
  echo "core Go module unexpectedly depends on a provider SDK" >&2
  exit 1
fi

for module in sqlite postgres mysql redis dynamodb cosmosdb spanner; do
  echo "testing Go store: $module"
  if [[ "$module" == "cosmosdb" && "${RUN_PROLLY_COSMOS_LIVE:-0}" != "1" ]]; then
    (
      unset PROLLY_COSMOS_ENDPOINT PROLLY_COSMOS_KEY PROLLY_COSMOS_DATABASE
      cd "$ROOT_DIR/bindings/go/stores/$module"
      go test -race ./...
    )
  else
    (
      cd "$ROOT_DIR/bindings/go/stores/$module"
      go test -race ./...
    )
  fi
done

echo "all Go remote-store providers passed"
