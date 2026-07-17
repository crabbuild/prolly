"""Scoped decoders for Prolly's leased PRPG v2 transport."""

from __future__ import annotations

import ctypes
import struct
from dataclasses import dataclass
from typing import Callable, Sequence, TypeVar

from .uniffi import prolly as _native


class _FastPageResult(ctypes.Structure):
    _fields_ = [
        ("status", ctypes.c_int32),
        ("terminal", ctypes.c_uint8),
        ("reserved", ctypes.c_uint8 * 3),
        ("record_count", ctypes.c_uint32),
        ("lease_handle", ctypes.c_uint64),
        ("data_ptr", ctypes.POINTER(ctypes.c_uint8)),
        ("data_len", ctypes.c_uint64),
    ]


class _FastValueLeaseResult(ctypes.Structure):
    _fields_ = [
        ("status", ctypes.c_int32),
        ("found", ctypes.c_uint8),
        ("reserved", ctypes.c_uint8 * 3),
        ("lease_handle", ctypes.c_uint64),
        ("data_ptr", ctypes.POINTER(ctypes.c_uint8)),
        ("data_len", ctypes.c_uint64),
    ]


class _Scope:
    def __init__(self) -> None:
        self.alive = True

    def close(self) -> None:
        self.alive = False

    def check(self) -> None:
        if not self.alive:
            raise RuntimeError("packed page view escaped its callback scope")


class ScopedBytes:
    """Read-only bytes backed by a native page and valid only in its callback."""

    def __init__(self, view: memoryview, scope: _Scope):
        self._view = view
        self._scope = scope

    def __len__(self) -> int:
        self._scope.check()
        return len(self._view)

    def __getitem__(self, index):
        self._scope.check()
        return self._view[index]

    def __bytes__(self) -> bytes:
        self._scope.check()
        return self._view.tobytes()

    def subview(self, start: int, end: int | None = None) -> "ScopedBytes":
        self._scope.check()
        return ScopedBytes(self._view[start:end], self._scope)


@dataclass(frozen=True)
class ValueRefView:
    kind: str
    inline: ScopedBytes | None = None
    cid: bytes | None = None
    length: int | None = None


@dataclass(frozen=True)
class NeighborView:
    key: ScopedBytes
    distance: float
    rank: int
    value: ScopedBytes | None
    proof: ScopedBytes | None


@dataclass(frozen=True)
class EntryView:
    key: ScopedBytes
    value: ScopedBytes


@dataclass(frozen=True)
class ScanOutcome:
    visited: int
    stopped: bool


class _FastScanOpenResult(ctypes.Structure):
    _fields_ = [
        ("status", ctypes.c_int32),
        ("reserved", ctypes.c_uint32),
        ("scan_handle", ctypes.c_uint64),
    ]


def _slice(arena: memoryview, offset: int, length: int) -> memoryview:
    end = offset + length
    if offset < 0 or end > len(arena):
        raise ValueError("packed page field is outside the arena")
    return arena[offset:end]


def _decode_neighbors(page: memoryview, scope: _Scope) -> tuple[NeighborView, ...]:
    if len(page) < 28 or bytes(page[:4]) != b"PRPG":
        raise ValueError("invalid packed page header")
    version, kind, _flags, count, table_bytes, arena_bytes = struct.unpack_from(
        "<HHIIIQ", page, 4
    )
    if version != 2 or kind != 7 or table_bytes != count * 40:
        raise ValueError("packed page is not a v2 proximity-neighbor page")
    arena_start = 28 + table_bytes
    if arena_start + arena_bytes != len(page):
        raise ValueError("packed page length mismatch")
    arena = page[arena_start:]
    rows: list[NeighborView] = []
    for index in range(count):
        base = 28 + index * 40
        flags, key_offset, key_len = struct.unpack_from("<III", page, base)
        distance = struct.unpack_from("<d", page, base + 12)[0]
        rank, value_offset, value_len, proof_offset, proof_len = struct.unpack_from(
            "<IIIII", page, base + 20
        )
        rows.append(
            NeighborView(
                key=ScopedBytes(_slice(arena, key_offset, key_len), scope),
                distance=distance,
                rank=rank,
                value=ScopedBytes(_slice(arena, value_offset, value_len), scope) if flags & 1 else None,
                proof=ScopedBytes(_slice(arena, proof_offset, proof_len), scope) if flags & 2 else None,
            )
        )
    return tuple(rows)


def _decode_entries(
    page: memoryview,
    scope: _Scope,
    record_count: int,
    terminal: bool,
    previous_key: bytes | None,
) -> tuple[EntryView, ...]:
    if len(page) < 28 or bytes(page[:4]) != b"PRPG":
        raise ValueError("invalid packed scan page header")
    version, kind, flags, count, table_bytes, arena_bytes = struct.unpack_from(
        "<HHIIIQ", page, 4
    )
    if (
        version != 1
        or kind != 1
        or count != record_count
        or table_bytes < count * 16
        or table_bytes % 16 != 0
        or bool(flags & 1) != terminal
    ):
        raise ValueError("inconsistent packed scan page metadata")
    arena_start = 28 + table_bytes
    if arena_start + arena_bytes != len(page):
        raise ValueError("packed scan page length mismatch")
    arena = page[arena_start:]
    rows: list[EntryView] = []
    prior = previous_key
    for index in range(count):
        key_offset, key_len, value_offset, value_len = struct.unpack_from(
            "<IIII", page, 28 + index * 16
        )
        key_view = _slice(arena, key_offset, key_len)
        value_view = _slice(arena, value_offset, value_len)
        key = key_view.tobytes()
        if prior is not None and prior >= key:
            raise ValueError("packed scan page keys are not strictly ordered")
        prior = key
        rows.append(
            EntryView(
                key=ScopedBytes(key_view, scope),
                value=ScopedBytes(value_view, scope),
            )
        )
    return tuple(rows)


_R = TypeVar("_R")


def point_read_view(
    session_handle: int,
    key: bytes,
    visit: Callable[[ScopedBytes], _R],
) -> tuple[bool, _R | None]:
    """Expose one retained value only for the synchronous callback scope."""

    if not callable(visit):
        raise TypeError("point-read visitor must be callable")
    library = _native._UniffiLib
    get_lease = library.prolly_fast_read_session_get_lease
    get_lease.argtypes = [
        ctypes.c_uint64,
        ctypes.POINTER(ctypes.c_uint8),
        ctypes.c_size_t,
    ]
    get_lease.restype = _FastValueLeaseResult
    release = library.prolly_fast_value_release
    release.argtypes = [ctypes.c_uint64]
    release.restype = None
    key_bytes = bytes(key)
    key_buffer = (ctypes.c_uint8 * len(key_bytes)).from_buffer_copy(key_bytes)
    result = get_lease(
        session_handle, key_buffer if key_bytes else None, len(key_bytes)
    )
    if result.status != 0:
        raise RuntimeError(f"native retained point read failed with status {result.status}")
    if not result.found:
        if result.lease_handle:
            release(result.lease_handle)
            raise RuntimeError("missing point read returned a value lease")
        return False, None
    if not result.lease_handle or (result.data_len and not result.data_ptr):
        if result.lease_handle:
            release(result.lease_handle)
        raise RuntimeError("native point read returned an invalid value lease")
    scope = _Scope()
    try:
        view = memoryview(b"")
        if result.data_len:
            raw = (ctypes.c_uint8 * result.data_len).from_address(
                ctypes.addressof(result.data_ptr.contents)
            )
            view = memoryview(raw).cast("B")
        return True, visit(ScopedBytes(view, scope))
    finally:
        scope.close()
        release(result.lease_handle)


def decode_value_ref_view(value: ScopedBytes) -> ValueRefView:
    if len(value) < 4 or bytes(value.subview(0, 4)) != b"PLVB":
        return ValueRefView(kind="inline", inline=value)
    if len(value) < 6 or value[4] != 1:
        raise ValueError("invalid or unsupported value reference header")
    tag = value[5]
    if tag == 0:
        if len(value) < 14:
            raise ValueError("inline value reference is truncated")
        length = int.from_bytes(bytes(value.subview(6, 14)), "big")
        if len(value) != 14 + length:
            raise ValueError("inline value reference length does not match payload")
        return ValueRefView(kind="inline", inline=value.subview(14))
    if tag == 1:
        if len(value) != 46:
            raise ValueError("blob value reference length is invalid")
        return ValueRefView(
            kind="blob",
            cid=bytes(value.subview(6, 38)),
            length=int.from_bytes(bytes(value.subview(38, 46)), "big"),
        )
    raise ValueError(f"unknown value reference tag {tag}")


def proximity_search_view(
    map_handle: int,
    query: Sequence[float],
    k: int,
    visit: Callable[[tuple[NeighborView, ...]], _R],
    *,
    max_arena_bytes: int = 64 * 1024 * 1024,
) -> _R:
    """Run one native search and expose views only for the callback's scope."""

    values = (ctypes.c_float * len(query))(*(float(value) for value in query))
    library = _native._UniffiLib
    search = library.prolly_fast_proximity_search
    search.argtypes = [
        ctypes.c_uint64,
        ctypes.POINTER(ctypes.c_float),
        ctypes.c_size_t,
        ctypes.c_uint32,
        ctypes.c_uint64,
    ]
    search.restype = _FastPageResult
    release = library.prolly_fast_page_release
    release.argtypes = [ctypes.c_uint64]
    release.restype = None
    result = search(map_handle, values, len(query), k, max_arena_bytes)
    if result.status != 0:
        raise RuntimeError(f"native proximity search failed with status {result.status}")
    scope = _Scope()
    try:
        raw = (ctypes.c_uint8 * result.data_len).from_address(
            ctypes.addressof(result.data_ptr.contents)
        )
        page = memoryview(raw).cast("B")
        return visit(_decode_neighbors(page, scope))
    finally:
        scope.close()
        release(result.lease_handle)


def scan_range_view(
    session_handle: int,
    start: bytes,
    end: bytes | None,
    visit: Callable[[EntryView], bool],
    *,
    max_records: int = 4096,
    max_arena_bytes: int = 4 * 1024 * 1024,
) -> ScanOutcome:
    """Visit a retained scan through callback-scoped views into native pages."""

    if not callable(visit):
        raise TypeError("visit must be callable")
    if max_records <= 0 or max_arena_bytes <= 0:
        raise ValueError("packed scan limits must be positive")
    library = _native._UniffiLib
    open_scan = library.prolly_fast_read_session_scan_open
    open_scan.argtypes = [
        ctypes.c_uint64,
        ctypes.POINTER(ctypes.c_uint8),
        ctypes.c_size_t,
        ctypes.POINTER(ctypes.c_uint8),
        ctypes.c_size_t,
        ctypes.c_uint8,
    ]
    open_scan.restype = _FastScanOpenResult
    next_page = library.prolly_fast_read_session_scan_next
    next_page.argtypes = [ctypes.c_uint64, ctypes.c_uint64, ctypes.c_uint32, ctypes.c_uint64]
    next_page.restype = _FastPageResult
    close_scan = library.prolly_fast_scan_close
    close_scan.argtypes = [ctypes.c_uint64]
    close_scan.restype = None
    release = library.prolly_fast_page_release
    release.argtypes = [ctypes.c_uint64]
    release.restype = None

    start_bytes = bytes(start)
    end_bytes = None if end is None else bytes(end)
    start_buffer = (ctypes.c_uint8 * len(start_bytes)).from_buffer_copy(start_bytes)
    end_buffer = (
        None
        if end_bytes is None
        else (ctypes.c_uint8 * len(end_bytes)).from_buffer_copy(end_bytes)
    )
    opened = open_scan(
        session_handle,
        start_buffer if start_bytes else None,
        len(start_bytes),
        end_buffer if end_bytes else None,
        0 if end_bytes is None else len(end_bytes),
        0 if end_bytes is None else 1,
    )
    if opened.status != 0:
        raise RuntimeError(f"native retained scan open failed with status {opened.status}")

    visited = 0
    previous_key: bytes | None = None
    try:
        while True:
            result = next_page(
                session_handle, opened.scan_handle, max_records, max_arena_bytes
            )
            if result.status != 0:
                raise RuntimeError(
                    f"native retained scan read failed with status {result.status}"
                )
            scope = _Scope()
            try:
                if not result.data_ptr:
                    raise ValueError("native packed scan page pointer was null")
                raw = (ctypes.c_uint8 * result.data_len).from_address(
                    ctypes.addressof(result.data_ptr.contents)
                )
                rows = _decode_entries(
                    memoryview(raw).cast("B"),
                    scope,
                    result.record_count,
                    bool(result.terminal),
                    previous_key,
                )
                for row in rows:
                    visited += 1
                    if not visit(row):
                        return ScanOutcome(visited=visited, stopped=True)
                if rows:
                    previous_key = bytes(rows[-1].key)
                elif not result.terminal:
                    raise RuntimeError("non-terminal packed scan page made no progress")
            finally:
                scope.close()
                release(result.lease_handle)
            if result.terminal:
                return ScanOutcome(visited=visited, stopped=False)
    finally:
        close_scan(opened.scan_handle)
