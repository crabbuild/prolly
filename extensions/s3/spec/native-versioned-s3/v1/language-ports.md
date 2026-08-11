# Implementing another language

Use five layers with no host-language types crossing persistence boundaries:

```text
public SDK → v1 state machines → Prolly/record codec → object-plane adapter → native S3 SDK
```

Start with a Reader implementation. Implement canonical CBOR and the hash/path
vectors before S3 I/O, then immutable record loading and closure validation,
then ref resolution and exact-VersionId reads. Add qualification, CAS/fencing,
and idempotency before enabling writes.

Recommended representations:

- Java: `long` only after checking v1 values ≤ 2^63−1; `byte[]` for digests;
  sealed interfaces for enums; AWS SDK v2 async client.
- Go: `uint64`, fixed `[32]byte`, explicit tagged-union structs, AWS SDK for Go
  v2; never rely on `map` iteration during encoding.
- TypeScript: `bigint` for uint64 and `Uint8Array` for bytes/digests; model enums
  as discriminated unions; do not route CBOR through JSON or `number`.

Generated Smithy types are an API convenience, not a wire codec. Persisted
records MUST be generated or hand-written from CDDL with explicit ordinal tags.
Every port should expose an `inspect` mode that prints canonical hex, computed
IDs, resolved physical VersionIds, ref generation, and closure validation.

The minimum release gate is:

1. pass `conformance/verify.py` and all vectors;
2. decode/re-encode fixtures byte-identically;
3. reject every negative CBOR and state-machine case;
4. read a Rust-created repository;
5. for Writer, create a repository that Rust reads and mutates;
6. pass injected timeout/CAS/crash scenarios without duplicate visibility.
