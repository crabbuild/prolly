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


@dataclass(frozen=True)
class NeighborView:
    key: ScopedBytes
    distance: float
    rank: int
    value: ScopedBytes | None
    proof: ScopedBytes | None


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


_R = TypeVar("_R")


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
