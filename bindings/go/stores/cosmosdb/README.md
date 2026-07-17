# Cosmos DB store for Go

This module implements the shared asynchronous store protocol with the
official `azcosmos` SDK. The container must use `/kind` as its partition key.
Every store instance keeps nodes, roots, and hints in one configurable `kind`
partition, enabling 100-operation transactional batches.

The schema-version-1 documents match Rust exactly: deterministic `k<hex>` IDs,
`kind`, `family`, hex `key`, and base64 `value` fields. Root compare-and-swap
uses ETags. Ordinary batches and the combined nodes-and-hint helper are not
advertised as atomic; strict commits are single-partition transactions.

`EnsureDatabaseAndContainer` performs non-destructive resource initialization
and validates `/kind`. Applications still own account provisioning, credentials,
throughput policy, and resource deletion.
