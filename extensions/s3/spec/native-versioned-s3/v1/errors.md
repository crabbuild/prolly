# Stable errors and retry contract

Portable errors carry `code`, `retry`, human message, optional operation ID,
and optional provider code/message/request ID. Programs branch only on `code`
and `retry`; messages are not stable.

## Retry advice

| Advice | Meaning |
|---|---|
| `Never` | request is invalid, unsupported, corrupt, or definitively failed |
| `Safe` | same request and operation ID may be retried with backoff |
| `After(ms)` | retry same request no earlier than the duration |
| `ReloadHead` | reload branch/ref state, recompute, and use policy to retry |
| `ReconcileOperation` | outcome may have committed; inspect ref, operation record, and provider metadata before any mutation |

## Codes

The v1 codes are:

```text
InvalidRequest UnsupportedParameter InvalidBucket InvalidKey InvalidBranch
InvalidRevision InvalidLimit EntityTooLarge IncompleteBody
RepositoryNotInitialized RepositoryFormatConflict UnsupportedRepositoryFormat
ProviderNotQualified MissingCapability NoSuchKey NoSuchVersion NoSuchBranch
NoSuchUpload UploadConflict NoSuchBatch BatchExpired BatchConflict
PreconditionFailed NotModified RefConflict IdempotencyConflict NoMergeBase
AmbiguousMergeBase MergeConflict HistoryLimitExceeded InvalidContinuationToken
InvalidRange ChecksumMismatch CorruptNode CorruptContent CorruptCommit
MissingClosure PermissionDenied Throttled Timeout OperationCanceled
OutcomeUnknown Transport InternalInvariant
```

Default advice is `Never`. Adapters normally map throttling to `After`,
pre-mutation transient transport/timeouts to `Safe`, ref CAS conflict to
`ReloadHead`, and any post-mutation ambiguous result to `ReconcileOperation`.
`OutcomeUnknown` MUST NOT be changed to `Safe` merely because the provider SDK
labels an error retryable.

Validation, unsupported format, corruption, checksum mismatch, idempotency
conflict, and permission failures are never automatically retried. A caller may
explicitly invoke repair or administrative recovery as a separate operation.
