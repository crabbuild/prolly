# Deterministic CBOR profile

The media type is `application/vnd.prolly.native-s3.v1+cbor`. All persisted
records in [schema.cddl](schema.cddl) use the `prolly-packed-cbor/v1` profile.

Commit paths contain a range-readable envelope instead of bare CBOR:

```text
"PLYCOM01" || u32be(commit_length) || u64be(node_pack_length) ||
canonical BucketCommitV1 || optional NodePackV1 frame
```

`node_pack_length` is zero exactly when `BucketCommitV1.node_pack` is null.
Otherwise the appended frame is
`"PLYPACK1" || u32be(toc_length) || canonical NodePackTocV1 || payload` and its
derived `NodePackRefV1` MUST equal the reference in the commit. The envelope
path is still derived from the logical `BucketCommitV1`, not from the physical
envelope bytes. Decoders reject length mismatches and trailing bytes.

## Encoder requirements

An encoder MUST:

1. emit definite-length maps, arrays, byte strings, and text strings;
2. use the shortest CBOR argument encoding for every integer and length;
3. encode signed concepts only when the schema permits them (v1 permits no
   negative integer values);
4. encode structs as maps whose keys are zero-based unsigned field ordinals;
5. encode unit enum variants as their zero-based unsigned variant ordinal;
6. encode payload enum variants as a one-entry map from the variant ordinal to
   its payload; struct-variant payloads are ordinal-keyed maps;
7. sort map keys by RFC 8949 length-first deterministic order: shorter encoded
   key first, then lexicographic order of the encoded key bytes;
8. preserve byte and text content exactly; v1 performs no Unicode
   normalization;
9. include every declared struct field, including `null`, `false`, zero, empty
   collections, and fields whose Rust implementation has a decode default;
10. represent fixed digest arrays using the CDDL `digest16` and `digest32`
    array forms, and UUIDs as a 16-byte byte string.

CBOR floating-point values, semantic tags, negative integers, undefined, and
simple values other than `false`, `true`, and `null` are forbidden.

## Decoder requirements

A decoder MUST reject:

- duplicate or unknown map keys;
- missing fields;
- unknown enum variants;
- indefinite-length items;
- non-minimal integer or length encodings;
- map keys in a noncanonical order;
- trailing bytes;
- invalid UTF-8;
- values outside their CDDL range;
- any document that does not round-trip to byte-identical canonical bytes.

Decoders MUST enforce semantic validation after structural validation. A
structurally valid record with a mismatched content ID, malformed binding,
unsorted node index, or invalid branch name is corrupt.

## Collection order

Arrays used as sets or ordered indexes have record-specific rules in CDDL
comments and the state-machine document. General maps are sorted by canonical
encoded key bytes, not by a host language's default string comparator.

## Content hashing

Content identifiers hash the exact canonical bytes produced by this profile.
Parsing and reserializing through a lossy dynamic number type is forbidden.
TypeScript implementations SHOULD use `bigint` for all `uint64` values and
MUST reject values that cannot be represented without loss by their chosen
runtime.
