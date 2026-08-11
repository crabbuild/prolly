# Compatibility and evolution

V1 is closed and frozen. A released v1 implementation MUST continue to emit
byte-identical records and MUST reject extensions that v1 does not define.

The following changes require a new major protocol and namespace:

- adding, removing, reordering, or changing any persisted field or variant;
- changing canonical CBOR, hashing, ID text, path derivation, tree-key encoding,
  state-machine visibility, lease fencing, or S3 preconditions;
- weakening qualification or integrity checks;
- changing a default that affects persisted or observable behavior.

A v2 design MUST use a new format object (for example `format/v2.cbor`), new
content-ID domains and prefixes where bytes or meaning changed, and explicit
reader/writer negotiation. It MUST NOT reinterpret v1 bytes in place.

Additive public SDK conveniences that translate entirely into existing v1
operations MAY ship without changing the protocol. New diagnostics MAY be
added if they do not alter retry categories or persisted data.

Readers compare `min_reader_version` to their supported reader version;
writers compare both minimums. Zero and values greater than supported are
invalid. For v1, the format version, required capability profile, reader
minimum, writer minimum, and every implementation default are exactly `1`.

Cross-language releases MUST run the same conformance corpus in both
directions: each implementation decodes every canonical vector, reproduces its
bytes and IDs, rejects every negative vector, and interoperates against at
least one independently implemented writer before claiming Writer or Full.
