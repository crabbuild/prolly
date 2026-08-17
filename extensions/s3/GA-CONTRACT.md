# Prolly S3 general-availability contract

Status: proposed contract for the first stable release. The `0.1.x` crates are
still a preview and do not yet carry the GA compatibility promise below.

## Storage boundary

One logical live file is one complete immutable provider object. Prolly S3
does not pack payloads, split payloads into chunks, or own multipart-upload
state. Prolly metadata nodes may share a range-addressable commit object;
logical payloads never do.

## Repository-format compatibility

The create-once `format/repository.cbor` marker is the repository's canonical
format identity. Its tree descriptor, canonical limits, idempotency retention,
and provider version-limit profile are immutable for the lifetime of the
repository prefix.

The first stable release will apply these rules:

- patch and minor client upgrades must open and write every repository created
  by an earlier release in the same major series;
- new readers may supply documented defaults for fields absent from older
  objects, while canonical decoding rejects unknown fields, object magic, and
  version values rather than silently changing their meaning;
- a writer must fail closed with `RepositoryFormatConflict` or
  `UnsupportedRepositoryFormat` before publishing when it cannot reproduce the
  stored canonical format exactly;
- caches, journal-derived indexes, rightmost-path hints, and prewarm state are
  advisory and may be deleted/rebuilt by any compatible release;
- commit IDs, object-version IDs, payload bindings, authority epochs, branch
  generations, and published state roots are durable protocol state and may
  not be rewritten during an upgrade;
- a future incompatible format uses a new major protocol generation and a new
  repository prefix. It is transferred through the verified history-transfer
  API, never migrated in place.

The repository core already enforces exact create-once canonical settings on
open. Commit metadata and node packs use explicit versioned wire magic and
content hashes; unsupported or corrupt encodings fail before becoming visible.

## Upgrade, downgrade, and recovery guarantees

Before an upgrade:

1. complete or cancel active merges, restores, transfers, fsck, and GC jobs;
2. retain a provider-versioned backup or verified replica and the provider
   attestation;
3. record every branch/tag/pin target and run shallow fsck;
4. open the repository read-only with the candidate release and rebuild
   advisory indexes/cache from an empty local directory;
5. run the provider- and cardinality-specific qualification gate before
   enabling writers.

A downgrade is supported only when the older binary declares support for the
exact stored repository format and every required feature. Canonical derived
indexes written by a newer binary can intentionally fail closed in an older
binary; downgrade preparation must reset those advisory index heads with the
newer release before the older release rebuilds them. Otherwise restore the
pre-upgrade provider snapshot or transfer verified history into a new prefix.
Downgrade code must never delete newer immutable objects or rewrite the format
marker. If a release does not publish and test that reset procedure, downgrade
from that release is explicitly unsupported.

Recovery guarantees are based on immutable closure and fenced mutable
controls: branch publication is CAS-protected, retry outcomes are reconciled,
authority takeover fences old epochs, commit sessions and maintenance cursors
are restartable, and exact physical versions are deleted only after the GC
safety protocol. Recovery does not guarantee retention of unreachable payloads
outside the configured GC policy.

## Production cache profile

`ProductionCacheProfile` is the supported production path. It requires a
single-owner persistent Foyer directory, chooses memory/disk/location bounds
from expected repository cardinality, enables bounded sibling prefetch, and
prewarms and pins root/upper-level metadata nodes. New repositories initialized
with the profile use encoded-byte-bounded metadata nodes; existing repositories
retain their create-once tree format and still use exact node-pack range reads.

The profile is a latency requirement, never a correctness dependency. Removing
the cache must leave every repository operation correct, although cold SLOs may
fail until prewarming completes.

## OpenTelemetry and reference alerts

Build `prolly-s3-client` with `opentelemetry` and attach an
`OpenTelemetryClientMetrics` sink through `ClientBuilder::telemetry`. The
application owns the `MeterProvider`, exporter, resource attributes, sampling,
and shutdown. The client exports bounded-dimension metrics for:

- cache hits/misses, admissions, errors, corruptions, and singleflight waits;
- requested, provider-fetched, and cache-avoided metadata bytes;
- predictive prefetch batches/nodes;
- S3 operations and transferred bytes;
- total open, index catch-up, and prewarm duration/failure.

Recommended initial alerts, tuned after a representative baseline:

| Condition | Initial threshold | Window |
| --- | ---: | ---: |
| Metadata cache hit ratio | below 90% after warmup | 15 minutes |
| Provider-fetched / requested metadata bytes | above 4x | 10 minutes |
| Cache admission rejects | above 1% of node requests | 10 minutes |
| Cache corruption | any sustained nonzero value | 5 minutes |
| Startup prewarm timeout/failure | any occurrence | immediate |
| Branch-index lag | above 100 generations or not ready | 5 minutes |
| S3 429/503 wire attempts | above provider-specific error budget | 5 minutes |
| GC/fsck checkpoint progress | no progress during an active job | 15 minutes |

SDK operation counters do not include SDK-internal retries. Attach
`S3WireAttemptInterceptor` and provider-side request metrics for retry/error
alerts.

## Explicit support envelope

`SupportedEnvelope::for_deployment` exposes the same policy to applications.
The current evidence supports only a controlled RustFS/local pilot through
100K objects after its release gates pass. AWS always requires workload-specific
qualification. Repositories above 100K require cardinality-matched maintenance
and performance qualification, and one-million-object production support is
not claimed while the published cold and ingestion gates remain below target.

Promotion requires all of the following at the intended provider, region,
cardinality, key distribution, concurrency, retention, and cache size:

- provider capability and lifecycle/Object Lock/replication attestation;
- cold, prewarmed, steady-state, and cache-loss read/list SLOs;
- ingest throughput and request/byte amplification budgets;
- branch, arbitrary-snapshot diff, and merge SLOs;
- authority expiry/takeover and process-loss fault injection;
- fsck, journal-driven GC, restart, backup, and restore drills;
- operator dashboards, alerts, runbooks, IAM/KMS review, and cost approval.

RustFS conformance proves protocol behavior, not AWS latency, throttling, cost,
or operational readiness.
