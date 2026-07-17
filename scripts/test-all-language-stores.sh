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
export PROLLY_STORE_DYNAMODB_PORT="$DYNAMODB_PORT" PROLLY_STORE_REDIS_PORT="$REDIS_PORT"
export PROLLY_STORE_POSTGRES_PORT="$POSTGRES_PORT" PROLLY_STORE_MYSQL_PORT="$MYSQL_PORT"
export PROLLY_STORE_SPANNER_GRPC_PORT="$SPANNER_GRPC_PORT" PROLLY_STORE_SPANNER_REST_PORT="$SPANNER_REST_PORT"

compose() { docker compose -p "$PROJECT_NAME" -f "$COMPOSE_FILE" "$@"; }
cleanup() {
  if [[ "$SERVICES_RUNNING" == "0" && "${KEEP_PROLLY_STORE_SERVICES:-0}" != "1" ]]; then compose down -v; fi
}
trap cleanup EXIT

wait_for_tcp() {
  local name="$1" port="$2"
  for _ in $(seq 1 120); do
    if (echo >"/dev/tcp/127.0.0.1/$port") >/dev/null 2>&1; then echo "$name is listening on 127.0.0.1:$port"; return; fi
    sleep 1
  done
  echo "$name did not start on 127.0.0.1:$port" >&2; compose ps; return 1
}

if [[ "$SERVICES_RUNNING" == "0" ]]; then compose up -d postgres mysql redis dynamodb spanner; fi
wait_for_tcp "PostgreSQL" "$POSTGRES_PORT"
wait_for_tcp "MySQL" "$MYSQL_PORT"
wait_for_tcp "Redis" "$REDIS_PORT"
wait_for_tcp "DynamoDB Local" "$DYNAMODB_PORT"
wait_for_tcp "Spanner emulator" "$SPANNER_GRPC_PORT"

export AWS_ACCESS_KEY_ID="${AWS_ACCESS_KEY_ID:-test}" AWS_SECRET_ACCESS_KEY="${AWS_SECRET_ACCESS_KEY:-test}"
export AWS_REGION="${AWS_REGION:-us-west-2}"
export PROLLY_STORE_DYNAMODB_TABLE="${PROLLY_STORE_DYNAMODB_TABLE:-prolly_store_all_language_gate}"
export PROLLY_POSTGRES_URL="postgresql://prolly:prolly@127.0.0.1:$POSTGRES_PORT/prolly?sslmode=disable"
export PROLLY_MYSQL_URL="mysql://prolly:prolly@127.0.0.1:$MYSQL_PORT/prolly"
export PROLLY_MYSQL_DSN="prolly:prolly@tcp(127.0.0.1:$MYSQL_PORT)/prolly?parseTime=true"
export PROLLY_REDIS_URL="redis://127.0.0.1:$REDIS_PORT" PROLLY_REDIS_ADDR="127.0.0.1:$REDIS_PORT"
export PROLLY_DYNAMODB_ENDPOINT="http://127.0.0.1:$DYNAMODB_PORT"
export PROLLY_STORE_DYNAMODB_ENDPOINT="$PROLLY_DYNAMODB_ENDPOINT"
export SPANNER_EMULATOR_HOST="127.0.0.1:$SPANNER_GRPC_PORT"
export PROLLY_BINDINGS_LIBRARY="${PROLLY_BINDINGS_LIBRARY:-$ROOT_DIR/target/debug/libprolly_bindings.dylib}"
export PROLLY_BINDINGS_LIBRARY_DIR="${PROLLY_BINDINGS_LIBRARY_DIR:-$ROOT_DIR/target/debug}"
export DYLD_LIBRARY_PATH="${DYLD_LIBRARY_PATH:-$ROOT_DIR/target/debug}"

cd "$ROOT_DIR"
cargo build --manifest-path bindings/uniffi/Cargo.toml --target-dir target
cargo test --features async-store
for provider in sqlite postgres mysql redis dynamodb cosmosdb spanner; do
  echo "testing Rust store: $provider"
  cargo test --manifest-path "stores/prolly-store-$provider/Cargo.toml" --target-dir target
done
node scripts/verify-store-compatibility.mjs
scripts/test-go-stores.sh --services-running
if [[ -z "${PROLLY_STORE_JAVA_HOME:-}" && -x /usr/libexec/java_home ]]; then
  PROLLY_STORE_JAVA_HOME="$(/usr/libexec/java_home -v 17 2>/dev/null || true)"
  export PROLLY_STORE_JAVA_HOME
fi
scripts/test-node-jvm-stores.sh --services-running

PYTHON_ENV="${PROLLY_PYTHON_STORE_VENV:-/tmp/prolly-python-store-gate}"
if [[ ! -x "$PYTHON_ENV/bin/python" ]]; then python3 -m venv "$PYTHON_ENV"; fi
if [[ "${PROLLY_STORE_SKIP_INSTALL:-0}" != "1" ]]; then
  "$PYTHON_ENV/bin/python" -m pip install --disable-pip-version-check -q \
    'psycopg[binary,pool]==3.3.4' aiomysql==0.3.2 redis==8.0.1 aioboto3==15.5.0 \
    azure-cosmos==4.16.1 google-cloud-spanner==3.69.0
fi
for provider in sqlite postgres mysql redis dynamodb cosmosdb spanner; do
  echo "testing Python store: $provider"
  PYTHONPATH="$ROOT_DIR/bindings/python:$ROOT_DIR/bindings/python/stores/$provider" \
    "$PYTHON_ENV/bin/python" -m unittest discover -s "$ROOT_DIR/bindings/python/stores/$provider/tests" -v
done

RUBY_BIN_DIR="${PROLLY_STORE_RUBY_BIN_DIR:-}"
if [[ -z "$RUBY_BIN_DIR" && -x /opt/homebrew/Library/Homebrew/vendor/portable-ruby/current/bin/ruby ]]; then
  RUBY_BIN_DIR="/opt/homebrew/Library/Homebrew/vendor/portable-ruby/current/bin"
fi
if [[ -n "$RUBY_BIN_DIR" ]]; then export PATH="$RUBY_BIN_DIR:$PATH"; fi
RUBY_BUNDLE_PATH="${PROLLY_RUBY_STORE_BUNDLE:-/tmp/prolly-ruby-store-gate-portable}"
for provider in sqlite postgres mysql redis dynamodb spanner; do
  echo "testing Ruby store: $provider"
  export BUNDLE_GEMFILE="$ROOT_DIR/bindings/ruby/stores/$provider/Gemfile" BUNDLE_PATH="$RUBY_BUNDLE_PATH"
  if [[ "${PROLLY_STORE_SKIP_INSTALL:-0}" != "1" ]]; then bundle install --quiet; fi
  bundle exec ruby -I"$ROOT_DIR/bindings/ruby/stores/$provider/lib" "$ROOT_DIR/bindings/ruby/stores/$provider/test/${provider}_store_test.rb"
done

for provider in sqlite postgres mysql redis dynamodb; do
  echo "testing Swift store: $provider"
  swift run --package-path bindings/swift "prolly-store-$provider-check"
done

for provider in indexeddb opfs pglite; do
  echo "testing browser store: $provider"
  if [[ "${PROLLY_STORE_SKIP_INSTALL:-0}" != "1" ]]; then npm --prefix "bindings/wasm/stores/$provider" ci --silent; fi
  npm --prefix "bindings/wasm/stores/$provider" test
  npm --prefix "bindings/wasm/stores/$provider" run check
done

echo "all Rust, Go, Node, JVM, Python, Ruby, Swift, and browser remote-store providers passed"
