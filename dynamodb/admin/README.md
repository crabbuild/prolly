# prolly-dynamodb-admin

Explicit operational CLI for `prolly-dynamodb-client`. It executes in-process
against the configured `prolly-store-dynamodb` namespace and never uses or
deploys a compatibility service.

Safety properties:

- `verify-archive` is offline and requires no AWS configuration;
- all provider commands require an explicit nonempty namespace key prefix;
- only `bootstrap` creates physical schema;
- backup and plan files use create-new semantics and are never overwritten;
- archive node, blob, and encoded-byte limits are mandatory and nonzero;
- import and retention separate read-only planning from attributed application;
- plan identities cover the complete canonical operation and are revalidated;
- every command emits structured JSON on standard output;
- import/retention application records a durable actor, reason, and optional
  change ticket in the same logical transaction as the mutation.

Provider configuration can be supplied by flags or the corresponding
environment variables:

```sh
export PROLLY_STORE_DYNAMODB_TABLE=prolly-versioned
export PROLLY_STORE_DYNAMODB_ROOT_TABLE=prolly-versioned-roots
export PROLLY_STORE_DYNAMODB_KEY_PREFIX=legal-prod:

cargo run --manifest-path dynamodb/admin/Cargo.toml -- verify
```

Physical provisioning is deliberately separate:

```sh
cargo run --manifest-path dynamodb/admin/Cargo.toml -- bootstrap
```

Back up an exact retained version, then verify it offline:

```sh
cargo run --manifest-path dynamodb/admin/Cargo.toml -- \
  backup --table Evidence --version "$VERSION_ID" --output evidence.ddba

cargo run --manifest-path dynamodb/admin/Cargo.toml -- \
  verify-archive --input evidence.ddba
```

Import is a reviewed two-command workflow. Output files must not already exist:

```sh
cargo run --manifest-path dynamodb/admin/Cargo.toml -- \
  import-plan --archive evidence.ddba --target-table EvidenceRecovered \
  --output import-plan.json

cargo run --manifest-path dynamodb/admin/Cargo.toml -- \
  import-apply --archive evidence.ddba --plan import-plan.json \
  --actor records-officer --reason "approved recovery" \
  --change-ticket LEGAL-2026-0088
```

Retention follows the same separation:

```sh
cargo run --manifest-path dynamodb/admin/Cargo.toml -- \
  retention-plan --table Evidence --keep-last 365 \
  --output retention-plan.json

cargo run --manifest-path dynamodb/admin/Cargo.toml -- \
  retention-apply --plan retention-plan.json \
  --actor records-officer --reason "approved annual schedule" \
  --change-ticket LEGAL-2026-0042
```

Acquire and explicitly release the global writer fence around approved
physical maintenance:

```sh
cargo run --manifest-path dynamodb/admin/Cargo.toml -- \
  lease-acquire --duration-millis 3600000 \
  --actor gc-worker --reason "verified global sweep" --change-ticket OPS-42

cargo run --manifest-path dynamodb/admin/Cargo.toml -- lease-status

cargo run --manifest-path dynamodb/admin/Cargo.toml -- \
  lease-release --lease-id "$LEASE_ID" \
  --actor gc-worker --reason "sweep completed" --change-ticket OPS-42
```

Expiry is not an automatic release. `lease-break-expired` is the explicit,
audited recovery command for a crashed holder and fails before the recorded
expiry.

With the fence held, `gc-plan` scans one bounded provider candidate page and
writes its exact dry-run result with the active lease identity and complete
named-root digest. Node/blob reachability and root enumeration have independent
hard limits. Provider cursors may yield empty pages because DynamoDB `Scan`
limits evaluated physical items before applying the namespace/family filter;
continue with `next_cursor` until it is absent.

```sh
cargo run --manifest-path dynamodb/admin/Cargo.toml -- \
  gc-plan --lease-id "$LEASE_ID" --output gc-plan-0001.json

cargo run --manifest-path dynamodb/admin/Cargo.toml -- \
  gc-apply --plan gc-plan-0001.json \
  --actor gc-worker --reason "apply reviewed sweep page" --change-ticket OPS-42
```

`gc-apply` revalidates reachability, writes durable in-progress evidence before
physical deletion, and is idempotently resumable after partial failure. It
leaves the lease held even after completion; release it explicitly only after
all reviewed pages finish. A release or expired-lease break fails while a GC
execution is still in progress.
