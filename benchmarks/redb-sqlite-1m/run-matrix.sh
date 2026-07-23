#!/usr/bin/env bash
set -euo pipefail

repetitions="${1:-3}"
manifest="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/Cargo.toml"

if ! [[ "${repetitions}" =~ ^[1-9][0-9]*$ ]]; then
  echo "usage: $0 [positive-repetition-count]" >&2
  exit 2
fi

for ((repetition = 1; repetition <= repetitions; repetition++)); do
  cargo run --release --quiet --manifest-path "${manifest}" -- redb "${repetition}"
  cargo run --release --quiet --manifest-path "${manifest}" -- sqlite "${repetition}"
done
