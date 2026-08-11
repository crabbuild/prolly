#!/usr/bin/env bash
set -euo pipefail

security_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

cargo deny --manifest-path "$security_root/Cargo.toml" \
  --config "$security_root/deny.toml" check advisories

for lockfile in \
  "$security_root/Cargo.lock" \
  "$security_root/qualification/downstream-client/Cargo.lock" \
  "$security_root/qualification/rolling-client/Cargo.lock"
do
  if rg -n \
    '^name = "(bincode|paste|foyer|foyer-common|foyer-memory|foyer-storage|prolly-store-slatedb)"$|version = "0\.21\.12"|version = "0\.101\.7"|version = "0\.24\.2"' \
    "$lockfile"
  then
    echo "forbidden legacy TLS or unused cache dependency in $lockfile" >&2
    exit 1
  fi
done

cargo tree --manifest-path "$security_root/Cargo.toml" --workspace --all-features \
  -i rustls-webpki@0.103.13 >/dev/null

echo "DEPENDENCY_SECURITY_COMPLETE advisories=approved-only tls=rustls-0.23 cache=foyer-absent"
