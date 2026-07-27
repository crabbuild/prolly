#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
COMPOSE_FILE="$REPO_ROOT/docker-compose.store-services.yml"
PROJECT="${PROLLY_CORRECTNESS_PROJECT:-prolly-backend-correctness}"
MANIFEST="$REPO_ROOT/benchmarks/backend-correctness/Cargo.toml"
OUTPUT="${PROLLY_CORRECTNESS_OUTPUT:-$REPO_ROOT/performance-results/backend-correctness-$(date +%F)}"
POSTGRES_PORT="${PROLLY_STORE_POSTGRES_PORT:-55432}"
DYNAMODB_PORT="${PROLLY_STORE_DYNAMODB_PORT:-8000}"

cleanup() {
  docker compose -p "$PROJECT" -f "$COMPOSE_FILE" down -v >/dev/null 2>&1 || true
}
trap cleanup EXIT

mkdir -p "$OUTPUT"
PROLLY_STORE_POSTGRES_PORT="$POSTGRES_PORT" \
PROLLY_STORE_DYNAMODB_PORT="$DYNAMODB_PORT" \
  docker compose -p "$PROJECT" -f "$COMPOSE_FILE" up -d postgres dynamodb

for attempt in $(seq 1 60); do
  postgres_health="$(docker inspect --format '{{.State.Health.Status}}' "$PROJECT-postgres-1" 2>/dev/null || true)"
  dynamodb_ready=0
  if curl --silent --output /dev/null "http://127.0.0.1:${DYNAMODB_PORT}"; then
    dynamodb_ready=1
  fi
  if [[ "$postgres_health" == healthy && "$dynamodb_ready" == 1 ]]; then
    break
  fi
  if [[ "$attempt" == 60 ]]; then
    docker compose -p "$PROJECT" -f "$COMPOSE_FILE" logs postgres dynamodb >&2
    exit 1
  fi
  sleep 1
done

PROLLY_STORE_POSTGRES_URL="postgres://prolly:prolly@127.0.0.1:${POSTGRES_PORT}/prolly" \
PROLLY_STORE_DYNAMODB_ENDPOINT="http://127.0.0.1:${DYNAMODB_PORT}" \
  cargo run --quiet --release --manifest-path "$MANIFEST" 2>&1 | tee "$OUTPUT/report.txt"
