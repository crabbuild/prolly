#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
COMPOSE_FILE="$ROOT_DIR/docker-compose.store-services.yml"

env_or_default() {
  local primary="$1"
  local legacy="$2"
  local default="$3"

  if [[ -n "${!primary:-}" ]]; then
    printf '%s\n' "${!primary}"
  elif [[ -n "${!legacy:-}" ]]; then
    printf '%s\n' "${!legacy}"
  else
    printf '%s\n' "$default"
  fi
}

PROJECT_NAME="$(env_or_default PROLLY_STORE_COMPOSE_PROJECT PROLLY_ADAPTERS_COMPOSE_PROJECT prolly-store-services)"

DYNAMODB_PORT="$(env_or_default PROLLY_STORE_DYNAMODB_PORT PROLLY_ADAPTERS_DYNAMODB_PORT 8000)"
REDIS_PORT="$(env_or_default PROLLY_STORE_REDIS_PORT PROLLY_ADAPTERS_REDIS_PORT 56379)"
POSTGRES_PORT="$(env_or_default PROLLY_STORE_POSTGRES_PORT PROLLY_ADAPTERS_POSTGRES_PORT 55432)"
MYSQL_PORT="$(env_or_default PROLLY_STORE_MYSQL_PORT PROLLY_ADAPTERS_MYSQL_PORT 53306)"

export PROLLY_STORE_DYNAMODB_PORT="$DYNAMODB_PORT"
export PROLLY_STORE_REDIS_PORT="$REDIS_PORT"
export PROLLY_STORE_POSTGRES_PORT="$POSTGRES_PORT"
export PROLLY_STORE_MYSQL_PORT="$MYSQL_PORT"

compose() {
  docker compose -p "$PROJECT_NAME" -f "$COMPOSE_FILE" "$@"
}

cleanup() {
  if [[ "${KEEP_PROLLY_STORE_SERVICES:-${KEEP_PROLLY_ADAPTER_SERVICES:-0}}" != "1" ]]; then
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

wait_for_service() {
  local name="$1"
  shift

  for _ in $(seq 1 120); do
    if compose exec -T "$@" >/dev/null 2>&1; then
      echo "$name is ready"
      return 0
    fi
    sleep 1
  done

  echo "$name did not become ready" >&2
  compose ps
  return 1
}

compose up -d

wait_for_tcp "DynamoDB Local" "$DYNAMODB_PORT"
wait_for_service "Redis" redis redis-cli ping
wait_for_service "Postgres" postgres pg_isready -U prolly -d prolly
wait_for_service "MySQL" mysql mysqladmin ping -h 127.0.0.1 -uprolly -pprolly --silent

export AWS_ACCESS_KEY_ID="${AWS_ACCESS_KEY_ID:-test}"
export AWS_REGION="${AWS_REGION:-us-west-2}"
export AWS_SECRET_ACCESS_KEY="${AWS_SECRET_ACCESS_KEY:-test}"
export PROLLY_STORE_DYNAMODB_ENDPOINT="http://127.0.0.1:$DYNAMODB_PORT"
export PROLLY_STORE_DYNAMODB_TABLE="${PROLLY_STORE_DYNAMODB_TABLE:-${PROLLY_ADAPTERS_DYNAMODB_TABLE:-prolly_store_local}}"
export PROLLY_STORE_MYSQL_URL="mysql://prolly:prolly@127.0.0.1:$MYSQL_PORT/prolly"
export PROLLY_STORE_POSTGRES_URL="postgres://prolly:prolly@127.0.0.1:$POSTGRES_PORT/prolly"
export PROLLY_STORE_REDIS_URL="redis://127.0.0.1:$REDIS_PORT/"

cd "$ROOT_DIR"
cargo test \
  -p prolly-map \
  --features async-store

cargo test \
  -p prolly-store-dynamodb \
  -p prolly-store-mysql \
  -p prolly-store-postgres \
  -p prolly-store-redis
