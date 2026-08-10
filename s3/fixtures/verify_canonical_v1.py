#!/usr/bin/env python3
"""Dependency-free, non-Rust verifier for the v1 canonical fixture."""

import base64
import hashlib
import json
import pathlib
import sys


def _length(major, value):
    prefix = major << 5
    if value < 24:
        return bytes([prefix | value])
    if value <= 0xFF:
        return bytes([prefix | 24, value])
    if value <= 0xFFFF:
        return bytes([prefix | 25]) + value.to_bytes(2, "big")
    if value <= 0xFFFFFFFF:
        return bytes([prefix | 26]) + value.to_bytes(4, "big")
    return bytes([prefix | 27]) + value.to_bytes(8, "big")


def encode(value):
    if value is None:
        return b"\xf6"
    if value is False:
        return b"\xf4"
    if value is True:
        return b"\xf5"
    if isinstance(value, int) and value >= 0:
        return _length(0, value)
    if isinstance(value, int):
        return _length(1, -1 - value)
    if isinstance(value, bytes):
        return _length(2, len(value)) + value
    if isinstance(value, str):
        raw = value.encode("utf-8")
        return _length(3, len(raw)) + raw
    if isinstance(value, list):
        return _length(4, len(value)) + b"".join(map(encode, value))
    if isinstance(value, dict):
        return _length(5, len(value)) + b"".join(
            encode(key) + encode(item) for key, item in value.items()
        )
    raise TypeError(f"unsupported fixture value: {type(value)!r}")


def decode(raw):
    offset = 0

    def read_length(additional):
        nonlocal offset
        if additional < 24:
            return additional
        widths = {24: 1, 25: 2, 26: 4, 27: 8}
        width = widths.get(additional)
        if width is None:
            raise ValueError("indefinite/reserved CBOR length is not canonical v1")
        end = offset + width
        value = int.from_bytes(raw[offset:end], "big")
        offset = end
        return value

    def item():
        nonlocal offset
        if offset >= len(raw):
            raise ValueError("truncated CBOR")
        initial = raw[offset]
        offset += 1
        major, additional = initial >> 5, initial & 31
        if major in (0, 1):
            value = read_length(additional)
            return value if major == 0 else -1 - value
        if major in (2, 3):
            length = read_length(additional)
            end = offset + length
            value = raw[offset:end]
            if len(value) != length:
                raise ValueError("truncated CBOR string")
            offset = end
            return value if major == 2 else value.decode("utf-8")
        if major == 4:
            return [item() for _ in range(read_length(additional))]
        if major == 5:
            return {item(): item() for _ in range(read_length(additional))}
        if major == 7 and additional in (20, 21, 22):
            return {20: False, 21: True, 22: None}[additional]
        raise ValueError(f"unsupported CBOR major/additional {major}/{additional}")

    value = item()
    if offset != len(raw):
        raise ValueError("trailing CBOR data")
    return value


def domain_hash(domain, *parts):
    digest = hashlib.sha256()
    digest.update(len(domain).to_bytes(4, "big"))
    digest.update(domain)
    for part in parts:
        digest.update(len(part).to_bytes(8, "big"))
        digest.update(part)
    return digest.digest()


def identifier(prefix, digest):
    return prefix + base64.b32encode(digest).decode("ascii").lower().rstrip("=")


def main():
    fixture_path = pathlib.Path(__file__).with_name("canonical-v1.json")
    fixture = json.loads(fixture_path.read_text(encoding="utf-8"))
    version_raw = bytes.fromhex(fixture["object_version_cbor_hex"])
    delta_raw = bytes.fromhex(fixture["delta_cbor_hex"])
    format_raw = bytes.fromhex(fixture["repository_format_cbor_hex"])
    state_raw = bytes.fromhex(fixture["initial_state_cbor_hex"])
    initial_delta_raw = bytes.fromhex(fixture["initial_delta_cbor_hex"])
    initial_commit_raw = bytes.fromhex(fixture["initial_commit_cbor_hex"])
    initial_reflog_raw = bytes.fromhex(fixture["initial_reflog_cbor_hex"])
    initial_ref_raw = bytes.fromhex(fixture["initial_ref_cbor_hex"])
    version = decode(version_raw)
    delta = decode(delta_raw)
    format_marker = decode(format_raw)
    initial_state = decode(state_raw)
    initial_delta = decode(initial_delta_raw)
    initial_commit = decode(initial_commit_raw)
    initial_reflog = decode(initial_reflog_raw)
    initial_ref = decode(initial_ref_raw)
    for value, raw in [
        (version, version_raw),
        (delta, delta_raw),
        (format_marker, format_raw),
        (initial_state, state_raw),
        (initial_delta, initial_delta_raw),
        (initial_commit, initial_commit_raw),
        (initial_reflog, initial_reflog_raw),
        (initial_ref, initial_ref_raw),
    ]:
        assert encode(value) == raw
    version_digest = domain_hash(
        b"prolly-s3/object-version/v1",
        bytes([0x11]) * 32,
        b"fixtures/object.txt",
        bytes.fromhex("00112233445566778899aabbccddeeff"),
        encode(version[1]),
    )
    assert identifier("pov1_", version_digest) == fixture["object_version_id"]
    assert identifier(
        "pdl1_", domain_hash(b"prolly-s3/delta/v1", delta_raw)
    ) == fixture["delta_id"]
    initial_delta_digest = domain_hash(b"prolly-s3/delta/v1", initial_delta_raw)
    initial_commit_digest = domain_hash(b"prolly-s3/commit/v1", initial_commit_raw)
    initial_reflog_digest = domain_hash(b"prolly-s3/reflog/v1", initial_reflog_raw)
    assert identifier("pdl1_", initial_delta_digest) == fixture["initial_delta_id"]
    assert identifier("pbc1_", initial_commit_digest) == fixture["initial_commit_id"]
    assert identifier("prl1_", initial_reflog_digest) == fixture["initial_reflog_id"]
    assert bytes(initial_commit[3]) == initial_delta_digest
    assert bytes(initial_reflog[2]) == initial_commit_digest
    assert bytes(initial_ref[0]) == initial_commit_digest
    assert bytes(initial_ref[4]) == initial_reflog_digest
    assert format_marker[1] == 1
    assert format_marker[5] == 1
    assert format_marker[6] == 1
    # Profile 1 is intentionally omitted/defaulted so the original v1 marker
    # stays byte-identical and remains readable by pre-profile clients.
    assert 8 not in format_marker
    print("canonical-v1: independent CBOR round-trip and IDs verified")


if __name__ == "__main__":
    try:
        main()
    except Exception as error:
        print(f"canonical-v1 verification failed: {error}", file=sys.stderr)
        raise
