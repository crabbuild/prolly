# V1 conformance suite

`cases.json` is language-neutral input. `verify.py` is a dependency-free
reference verifier for registry, hashing, paths, and repository source defaults.
Run it from any directory:

```sh
python3 extensions/s3/spec/prolly-s3/v1/conformance/verify.py
```

Each language implementation MUST additionally feed `invalid_cbor` to its v1
decoder, decode/re-encode every canonical fixture byte-identically, recompute
all content IDs, and execute the state scenarios. Rust wires these requirements
into `core/tests/protocol_v1.rs`.

Fixture JSON uses hex for bytes and decimal strings for unsigned 64-bit values,
so JavaScript runners do not lose precision. A runner MUST report the case name
and expected/actual bytes or state on failure.
