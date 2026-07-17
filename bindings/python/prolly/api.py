"""Hard-cutover, application-facing Prolly API."""

from __future__ import annotations

import asyncio
from dataclasses import dataclass
from typing import Any, Callable, Iterable, Sequence

from .packed import (
    EntryView,
    NeighborView,
    ScanOutcome,
    ScopedBytes,
    proximity_search_view,
    scan_range_view,
)
from .uniffi import prolly as _native


def _background(call):
    """Run an owned blocking binding call without blocking the event loop."""

    async def run():
        return await asyncio.to_thread(call)

    return run()


class _Scoped:
    def __init__(self) -> None:
        self._closed = False

    def _open(self) -> None:
        if self._closed:
            raise RuntimeError(f"{type(self).__name__} is closed")

    def close(self) -> None:
        self._closed = True

    def __enter__(self):
        self._open()
        return self

    def __exit__(self, _kind, _value, _traceback) -> None:
        self.close()


@dataclass(frozen=True)
class ProximityRecord:
    key: bytes
    vector: Sequence[float]
    value: bytes


@dataclass(frozen=True)
class HnswBuildResult:
    index: "HnswIndex"
    stats: _native.HnswBuildStatsRecord


@dataclass(frozen=True)
class ProductQuantizationBuildResult:
    index: "ProductQuantizer"
    stats: _native.ProductQuantizationBuildStatsRecord


@dataclass(frozen=True)
class CompositeBuildOutcome:
    accelerator: "CompositeAccelerator | None"
    reasons: Sequence[_native.FullRebuildReasonRecord]
    stats: _native.CompositeBuildStatsRecord


@dataclass(frozen=True)
class CompositeBuildOrRebuildOutcome:
    kind: _native.CompositeBuildOrRebuildKindRecord
    composite: "CompositeAccelerator | None"
    hnsw: "HnswIndex | None"
    pq: "ProductQuantizer | None"
    reasons: Sequence[_native.FullRebuildReasonRecord]
    composite_stats: _native.CompositeBuildStatsRecord
    hnsw_stats: _native.HnswBuildStatsRecord | None
    pq_stats: _native.ProductQuantizationBuildStatsRecord | None


IndexProjection = _native.IndexProjectionRecord
ProximitySearchRuntimePolicy = _native.ProximitySearchRuntimePolicyRecord
ProximitySearchRuntimeStats = _native.ProximitySearchRuntimeStatsRecord


class ProximityCancellationToken(_Scoped):
    def __init__(self):
        super().__init__()
        self._inner = _native.BindingProximityCancellationToken()

    def cancel(self) -> None:
        self._open()
        self._inner.cancel()

    @property
    def is_cancelled(self) -> bool:
        self._open()
        return self._inner.is_cancelled()


class Engine(_Scoped):
    def __init__(self, inner: _native.ProllyEngine):
        super().__init__()
        self._inner = inner

    @classmethod
    def memory(cls, config: _native.ConfigRecord | None = None) -> "Engine":
        return cls(_native.ProllyEngine.memory(config or _native.default_config()))

    @classmethod
    def file(cls, path: str, config: _native.ConfigRecord | None = None) -> "Engine":
        return cls(_native.ProllyEngine.file(path, config or _native.default_config()))

    def versioned_map(self, map_id: bytes) -> "VersionedMap":
        self._open()
        return VersionedMap(self._inner.versioned_map(bytes(map_id)))

    def begin_versioned_transaction(self) -> "VersionedTransaction":
        self._open()
        return VersionedTransaction(self._inner.begin_versioned_transaction())

    def index_registry(self) -> "IndexRegistry":
        self._open()
        return IndexRegistry(_native.BindingIndexRegistry())

    def indexed_map(self, map_id: bytes, registry: "IndexRegistry") -> "IndexedMap":
        self._open()
        return IndexedMap(self._inner.indexed_map(bytes(map_id), registry._inner))

    def build_proximity(
        self,
        dimensions: int,
        records: Iterable[ProximityRecord],
        *,
        config: _native.ProximityConfigRecord | None = None,
        threads: int | None = None,
    ) -> "ProximityMap":
        self._open()
        native_records = [
            _native.ProximityRecordRecord(
                key=bytes(record.key),
                vector=[float(value) for value in record.vector],
                value=bytes(record.value),
            )
            for record in records
        ]
        return ProximityMap(
            self._inner.build_proximity_map(
                config or _native.default_proximity_config(dimensions),
                native_records,
                threads,
            )
        )

    def load_proximity(self, descriptor: bytes) -> "ProximityMap":
        self._open()
        return ProximityMap(self._inner.load_proximity_map(bytes(descriptor)))

    def proximity_search_runtime(
        self,
        policy: _native.ProximitySearchRuntimePolicyRecord | None = None,
    ) -> "ProximitySearchRuntime":
        self._open()
        return ProximitySearchRuntime(
            self._inner.proximity_search_runtime(
                policy or _native.default_proximity_search_runtime_policy()
            )
        )


def _owned_mutations(mutations):
    return [
        _native.MutationRecord(
            kind=mutation.kind,
            key=bytes(mutation.key),
            value=None if mutation.value is None else bytes(mutation.value),
        )
        for mutation in mutations
    ]


def _owned_entries(entries):
    return [
        _native.EntryRecord(key=bytes(entry.key), value=bytes(entry.value))
        for entry in entries
    ]


def _owned_proximity_search_request(request):
    budget = request.budget
    filter_record = request.filter
    return _native.ProximitySearchRequestRecord(
        query=[float(value) for value in request.query],
        k=int(request.k),
        policy=request.policy,
        adaptive_quality=request.adaptive_quality,
        budget=_native.SearchBudgetRecord(
            max_nodes=budget.max_nodes,
            max_committed_bytes=budget.max_committed_bytes,
            max_distance_evaluations=budget.max_distance_evaluations,
            max_frontier_entries=budget.max_frontier_entries,
        ),
        filter=_native.ProximityFilterRecord(
            kind=filter_record.kind,
            start=None if filter_record.start is None else bytes(filter_record.start),
            range_end=(
                None if filter_record.range_end is None else bytes(filter_record.range_end)
            ),
            prefix=None if filter_record.prefix is None else bytes(filter_record.prefix),
            eligible_keys=[bytes(key) for key in filter_record.eligible_keys],
        ),
        kernel=request.kernel,
        backend=request.backend,
        hnsw_ef_search=request.hnsw_ef_search,
        pq_rerank_multiplier=request.pq_rerank_multiplier,
    )


class VersionedMap(_Scoped):
    def __init__(self, inner: _native.BindingVersionedMap):
        super().__init__()
        self._inner = inner

    def initialize(self):
        self._open()
        return self._inner.initialize()

    def initialize_sorted(self, entries):
        self._open()
        return self._inner.initialize_sorted(_owned_entries(entries))

    @property
    def id(self) -> bytes:
        self._open()
        return self._inner.id()

    def is_initialized(self) -> bool:
        self._open()
        return self._inner.is_initialized()

    def head(self):
        self._open()
        return self._inner.head()

    def head_id(self):
        self._open()
        return self._inner.head_id()

    def version(self, version_id: bytes):
        self._open()
        return self._inner.version(bytes(version_id))

    def versions(self):
        self._open()
        return self._inner.versions()

    def get(self, key: bytes):
        self._open()
        return self._inner.get(bytes(key))

    def contains(self, key: bytes) -> bool:
        self._open()
        return self._inner.contains_key(bytes(key))

    def get_many(self, keys: Iterable[bytes]):
        self._open()
        return self._inner.get_many([bytes(key) for key in keys])

    def get_at(self, version_id: bytes, key: bytes):
        self._open()
        return self._inner.get_at(bytes(version_id), bytes(key))

    def get_many_at(self, version_id: bytes, keys: Iterable[bytes]):
        self._open()
        return self._inner.get_many_at(bytes(version_id), [bytes(key) for key in keys])

    def range(self, start: bytes = b"", end: bytes | None = None):
        self._open()
        return self._inner.range(bytes(start), None if end is None else bytes(end))

    def prefix(self, prefix: bytes):
        self._open()
        return self._inner.prefix(bytes(prefix))

    def range_at(self, version_id: bytes, start: bytes = b"", end: bytes | None = None):
        self._open()
        return self._inner.range_at(
            bytes(version_id), bytes(start), None if end is None else bytes(end)
        )

    def prefix_at(self, version_id: bytes, prefix: bytes):
        self._open()
        return self._inner.prefix_at(bytes(version_id), bytes(prefix))

    def range_page(self, cursor=None, end: bytes | None = None, limit: int = 256):
        self._open()
        return self._inner.range_page(cursor, None if end is None else bytes(end), limit)

    def prefix_page(self, prefix: bytes, cursor=None, limit: int = 256):
        self._open()
        return self._inner.prefix_page(bytes(prefix), cursor, limit)

    def range_page_at(
        self, version_id: bytes, cursor=None, end: bytes | None = None, limit: int = 256
    ):
        self._open()
        return self._inner.range_page_at(
            bytes(version_id), cursor, None if end is None else bytes(end), limit
        )

    def prefix_page_at(
        self, version_id: bytes, prefix: bytes, cursor=None, limit: int = 256
    ):
        self._open()
        return self._inner.prefix_page_at(bytes(version_id), bytes(prefix), cursor, limit)

    def diff(self, base: bytes, target: bytes):
        self._open()
        return self._inner.diff(bytes(base), bytes(target))

    def changes_since(self, base: bytes):
        self._open()
        return self._inner.changes_since(bytes(base))

    def rollback_to(self, version_id: bytes):
        self._open()
        return self._inner.rollback_to(bytes(version_id))

    def put(self, key: bytes, value: bytes):
        self._open()
        return self._inner.put(bytes(key), bytes(value))

    def delete(self, key: bytes):
        self._open()
        return self._inner.delete(bytes(key))

    def apply(self, mutations):
        self._open()
        return self._inner.apply(_owned_mutations(mutations))

    def append(self, mutations):
        self._open()
        return self._inner.append(_owned_mutations(mutations))

    def parallel_apply(self, mutations, config):
        self._open()
        owned_config = _native.ParallelConfigRecord(
            max_threads=config.max_threads,
            parallelism_threshold=config.parallelism_threshold,
        )
        return self._inner.parallel_apply(_owned_mutations(mutations), owned_config)

    def rebuild_sorted_if(self, expected: bytes | None, entries):
        self._open()
        return self._inner.rebuild_sorted_if(
            None if expected is None else bytes(expected), _owned_entries(entries)
        )

    def rebuild_from_entries_if(self, expected: bytes | None, entries):
        self._open()
        return self._inner.rebuild_from_entries_if(
            None if expected is None else bytes(expected), _owned_entries(entries)
        )

    def rebuild_from_iter_if(self, expected: bytes | None, entries):
        return self.rebuild_from_entries_if(expected, entries)

    def apply_at_millis(self, mutations, timestamp_millis: int):
        self._open()
        return self._inner.apply_at_millis(
            _owned_mutations(mutations), timestamp_millis
        )

    def apply_if(self, expected: bytes | None, mutations):
        self._open()
        return self._inner.apply_if(
            None if expected is None else bytes(expected), _owned_mutations(mutations)
        )

    def apply_if_at_millis(
        self, expected: bytes | None, mutations, timestamp_millis: int
    ):
        self._open()
        return self._inner.apply_if_at_millis(
            None if expected is None else bytes(expected),
            _owned_mutations(mutations),
            timestamp_millis,
        )

    def put_if(self, expected: bytes | None, key: bytes, value: bytes):
        self._open()
        return self._inner.put_if(
            None if expected is None else bytes(expected), bytes(key), bytes(value)
        )

    def delete_if(self, expected: bytes | None, key: bytes):
        self._open()
        return self._inner.delete_if(
            None if expected is None else bytes(expected), bytes(key)
        )

    def snapshot(self) -> "MapSnapshot | None":
        self._open()
        value = self._inner.snapshot()
        return None if value is None else MapSnapshot(value)

    def snapshot_at(self, version_id: bytes) -> "MapSnapshot | None":
        self._open()
        value = self._inner.snapshot_at(bytes(version_id))
        return None if value is None else MapSnapshot(value)

    def compare(self, base: bytes, target: bytes) -> "MapComparison":
        self._open()
        return MapComparison(self._inner.compare(bytes(base), bytes(target)))

    def compare_to_head(self, base: bytes) -> "MapComparison":
        self._open()
        return MapComparison(self._inner.compare_to_head(bytes(base)))

    def subscribe(self) -> "MapSubscription":
        self._open()
        return MapSubscription(self._inner.subscribe())

    def subscribe_from(self, last_seen: bytes | None = None) -> "MapSubscription":
        self._open()
        return MapSubscription(
            self._inner.subscribe_from(None if last_seen is None else bytes(last_seen))
        )

    def prepare_merge(self, base: bytes, candidate: bytes) -> "MapMerge":
        self._open()
        return MapMerge(self._inner.prepare_merge(bytes(base), bytes(candidate)))

    def keep_last(self, count: int):
        self._open()
        return self._inner.keep_last(count)

    def prune_versions(self, keep_latest: int):
        self._open()
        return self._inner.prune_versions(keep_latest)

    def keep_for_at(self, now_millis: int, max_age_millis: int):
        self._open()
        return self._inner.keep_for_at(now_millis, max_age_millis)

    def keep_for(self, max_age_millis: int):
        self._open()
        return self._inner.keep_for(max_age_millis)

    def keep_versions(self, version_ids: Iterable[bytes]):
        self._open()
        return self._inner.keep_versions([bytes(version_id) for version_id in version_ids])

    def retention_policy(self):
        self._open()
        return self._inner.retention_policy()

    def verify_catalog(self):
        self._open()
        return self._inner.verify_catalog()

    def backup(self) -> bytes:
        self._open()
        return self._inner.backup()

    def restore_backup(self, bundle: bytes):
        self._open()
        return self._inner.restore_backup(bytes(bundle))

    def import_as_head(self, bundle):
        self._open()
        return self._inner.import_as_head(bundle)

    def plan_gc(self):
        self._open()
        return self._inner.plan_gc()

    def sweep_gc(self):
        self._open()
        return self._inner.sweep_gc()

    def put_async(self, key: bytes, value: bytes):
        copied_key, copied_value = bytes(key), bytes(value)
        return _background(lambda: self.put(copied_key, copied_value))

    def initialize_async(self):
        return _background(self.initialize)

    def head_async(self):
        return _background(self.head)

    def version_async(self, version_id: bytes):
        owned_id = bytes(version_id)
        return _background(lambda: self.version(owned_id))

    def get_async(self, key: bytes):
        owned_key = bytes(key)
        return _background(lambda: self.get(owned_key))

    def apply_async(self, mutations):
        owned = tuple(mutations)
        return _background(lambda: self.apply(owned))

    def delete_async(self, key: bytes):
        owned_key = bytes(key)
        return _background(lambda: self.delete(owned_key))

    def snapshot_async(self):
        return _background(self.snapshot)

    def snapshot_at_async(self, version_id: bytes):
        owned_id = bytes(version_id)
        return _background(lambda: self.snapshot_at(owned_id))

    def subscribe_async(self):
        return _background(self.subscribe)

    def subscribe_from_async(self, last_seen: bytes | None = None):
        owned = None if last_seen is None else bytes(last_seen)
        return _background(lambda: self.subscribe_from(owned))


class VersionedTransaction(_Scoped):
    def __init__(self, inner: _native.BindingVersionedTransaction):
        super().__init__()
        self._inner = inner

    def head(self, map_id: bytes):
        self._open()
        return self._inner.head(bytes(map_id))

    def get(self, map_id: bytes, key: bytes):
        self._open()
        return self._inner.get(bytes(map_id), bytes(key))

    def apply(self, map_id: bytes, mutations):
        self._open()
        return self._inner.apply(bytes(map_id), _owned_mutations(mutations))

    def apply_if(self, map_id: bytes, expected: bytes | None, mutations):
        self._open()
        return self._inner.apply_if(
            bytes(map_id), None if expected is None else bytes(expected),
            _owned_mutations(mutations),
        )

    def put(self, map_id: bytes, key: bytes, value: bytes):
        self._open()
        return self._inner.put(bytes(map_id), bytes(key), bytes(value))

    def delete(self, map_id: bytes, key: bytes):
        self._open()
        return self._inner.delete(bytes(map_id), bytes(key))

    def commit(self):
        self._open()
        result = self._inner.commit()
        self.close()
        return result

    def rollback(self) -> None:
        self._open()
        self._inner.rollback()
        self.close()


class MapComparison(_Scoped):
    def __init__(self, inner: _native.BindingMapComparison):
        super().__init__()
        self._inner = inner

    @property
    def base(self):
        self._open()
        return self._inner.base()

    @property
    def target(self):
        self._open()
        return self._inner.target()

    def diff(self):
        self._open()
        return self._inner.diff()

    def diff_page(self, cursor=None, end: bytes | None = None, limit: int = 256):
        self._open()
        return self._inner.diff_page(cursor, None if end is None else bytes(end), limit)


class MapSubscription(_Scoped):
    def __init__(self, inner: _native.BindingMapSubscription):
        super().__init__()
        self._inner = inner

    @property
    def last_seen(self) -> bytes | None:
        self._open()
        return self._inner.last_seen()

    def poll(self):
        self._open()
        return self._inner.poll()

    def poll_async(self):
        return _background(self.poll)


class MapMerge(_Scoped):
    def __init__(self, inner: _native.BindingMapMerge):
        super().__init__()
        self._inner = inner

    @property
    def base(self):
        self._open()
        return self._inner.base()

    @property
    def head(self):
        self._open()
        return self._inner.head()

    @property
    def candidate(self):
        self._open()
        return self._inner.candidate()

    def merge(self, resolver: str | None = None):
        self._open()
        return self._inner.merge(resolver)

    def conflict_page(self, cursor=None, limit: int = 256):
        self._open()
        return self._inner.conflict_page(cursor, limit)

    def publish(self, resolver: str | None = None):
        self._open()
        return self._inner.publish(resolver)


class MapSnapshot(_Scoped):
    def __init__(self, inner: _native.BindingMapSnapshot):
        super().__init__()
        self._inner = inner

    @property
    def id(self) -> bytes:
        self._open()
        return self._inner.id()

    @property
    def version(self):
        self._open()
        return self._inner.version()

    def get(self, key: bytes):
        self._open()
        return self._inner.get(bytes(key))

    def get_many(self, keys: Iterable[bytes]):
        self._open()
        return self._inner.get_many([bytes(key) for key in keys])

    def contains(self, key: bytes) -> bool:
        self._open()
        return self._inner.contains_key(bytes(key))

    def first_entry(self):
        self._open()
        return self._inner.first_entry()

    def last_entry(self):
        self._open()
        return self._inner.last_entry()

    def lower_bound(self, key: bytes):
        self._open()
        return self._inner.lower_bound(bytes(key))

    def upper_bound(self, key: bytes):
        self._open()
        return self._inner.upper_bound(bytes(key))

    def range(self, start: bytes = b"", end: bytes | None = None):
        self._open()
        return self._inner.range(bytes(start), None if end is None else bytes(end))

    def prefix(self, prefix: bytes):
        self._open()
        return self._inner.prefix(bytes(prefix))

    def range_page(self, cursor=None, end: bytes | None = None, limit: int = 256):
        self._open()
        return self._inner.range_page(cursor, None if end is None else bytes(end), limit)

    def prefix_page(self, prefix: bytes, cursor=None, limit: int = 256):
        self._open()
        return self._inner.prefix_page(bytes(prefix), cursor, limit)

    def reverse_page(self, cursor=None, start: bytes = b"", limit: int = 256):
        self._open()
        return self._inner.reverse_page(cursor, bytes(start), limit)

    def prefix_reverse_page(self, prefix: bytes, cursor=None, limit: int = 256):
        self._open()
        return self._inner.prefix_reverse_page(bytes(prefix), cursor, limit)

    def prove_key(self, key: bytes):
        self._open()
        return self._inner.prove_key(bytes(key))

    def prove_keys(self, keys: Iterable[bytes]):
        self._open()
        return self._inner.prove_keys([bytes(key) for key in keys])

    def prove_range(self, start: bytes = b"", end: bytes | None = None):
        self._open()
        return self._inner.prove_range(bytes(start), None if end is None else bytes(end))

    def prove_prefix(self, prefix: bytes):
        self._open()
        return self._inner.prove_prefix(bytes(prefix))

    def prove_range_page(self, cursor=None, end: bytes | None = None, limit: int = 256):
        self._open()
        return self._inner.prove_range_page(
            cursor, None if end is None else bytes(end), limit
        )

    def stats(self):
        self._open()
        return self._inner.stats()

    def export(self):
        self._open()
        return self._inner.export()

    def read(self) -> "ReadSession":
        self._open()
        return ReadSession(self._inner.read_session())

    def get_async(self, key: bytes):
        owned_key = bytes(key)
        return _background(lambda: self.get(owned_key))

    def get_many_async(self, keys: Iterable[bytes]):
        owned_keys = tuple(bytes(key) for key in keys)
        return _background(lambda: self.get_many(owned_keys))

    def range_async(self, start: bytes = b"", end: bytes | None = None):
        owned_start = bytes(start)
        owned_end = None if end is None else bytes(end)
        return _background(lambda: self.range(owned_start, owned_end))

    def prefix_async(self, prefix: bytes):
        owned_prefix = bytes(prefix)
        return _background(lambda: self.prefix(owned_prefix))

    def range_page_async(self, cursor=None, end: bytes | None = None, limit: int = 256):
        owned_end = None if end is None else bytes(end)
        return _background(lambda: self.range_page(cursor, owned_end, limit))

    def prefix_page_async(self, prefix: bytes, cursor=None, limit: int = 256):
        owned_prefix = bytes(prefix)
        return _background(lambda: self.prefix_page(owned_prefix, cursor, limit))

    def prove_key_async(self, key: bytes):
        owned_key = bytes(key)
        return _background(lambda: self.prove_key(owned_key))

    def prove_keys_async(self, keys: Iterable[bytes]):
        owned_keys = tuple(bytes(key) for key in keys)
        return _background(lambda: self.prove_keys(owned_keys))

    def prove_range_async(self, start: bytes = b"", end: bytes | None = None):
        owned_start = bytes(start)
        owned_end = None if end is None else bytes(end)
        return _background(lambda: self.prove_range(owned_start, owned_end))

    def prove_prefix_async(self, prefix: bytes):
        owned_prefix = bytes(prefix)
        return _background(lambda: self.prove_prefix(owned_prefix))

    def stats_async(self):
        return _background(self.stats)


class ReadSession(_Scoped):
    def __init__(self, inner: _native.ProllyReadSession):
        super().__init__()
        self._inner = inner

    def get(self, key: bytes):
        self._open()
        return self._inner.get(bytes(key))

    def get_many(self, keys: Iterable[bytes]):
        self._open()
        return self._inner.get_many([bytes(key) for key in keys])

    def scan_range_view(
        self,
        start: bytes = b"",
        end: bytes | None = None,
        visit: Callable[[EntryView], bool] | None = None,
    ) -> ScanOutcome:
        self._open()
        if visit is None:
            raise TypeError("visit must be provided")
        return scan_range_view(
            self._inner.fast_handle(), bytes(start), None if end is None else bytes(end), visit
        )


class IndexRegistry:
    def __init__(self, inner: _native.BindingIndexRegistry):
        self._inner = inner

    def register(
        self,
        name: bytes,
        generation: int,
        extractor_id: str,
        projection: _native.IndexProjectionRecord,
        extractor: Callable[[bytes, bytes], Iterable[tuple[bytes, bytes | None]]],
        limits=None,
    ) -> None:
        class Adapter(_native.SecondaryIndexExtractorCallback):
            def extract(self, primary_key: bytes, source_value: bytes):
                return [
                    _native.IndexEntryRecord(term=bytes(term), projection=projection_value)
                    for term, projection_value in extractor(primary_key, source_value)
                ]

        self._inner.register(
            bytes(name), generation, extractor_id, projection, limits, Adapter()
        )


class IndexedMap(_Scoped):
    def __init__(self, inner: _native.BindingIndexedMap):
        super().__init__()
        self._inner = inner

    @property
    def id(self) -> bytes:
        self._open()
        return self._inner.id()

    def ensure_index(self, name: bytes):
        self._open()
        return self._inner.ensure_index(bytes(name))

    def put(self, key: bytes, value: bytes):
        self._open()
        return self._inner.put(bytes(key), bytes(value))

    def apply(self, mutations):
        self._open()
        return self._inner.apply(mutations)

    def apply_if(self, expected_source: bytes | None, mutations):
        self._open()
        return self._inner.apply_if(
            None if expected_source is None else bytes(expected_source), mutations
        )

    def delete(self, key: bytes):
        self._open()
        return self._inner.delete(bytes(key))

    def get(self, key: bytes):
        self._open()
        return self._inner.get(bytes(key))

    def snapshot(self) -> "IndexedSnapshot":
        self._open()
        return IndexedSnapshot(self._inner.snapshot())

    def snapshot_at(self, source_version: bytes) -> "IndexedSnapshot":
        self._open()
        return IndexedSnapshot(self._inner.snapshot_at(bytes(source_version)))

    def snapshot_by_id(self, snapshot_id) -> "IndexedSnapshot":
        self._open()
        return IndexedSnapshot(self._inner.snapshot_by_id(snapshot_id))

    def health(self):
        self._open()
        return self._inner.health()

    def verify_index(self, name: bytes, source_version: bytes):
        self._open()
        return self._inner.verify_index(bytes(name), bytes(source_version))

    def verify_all(self, source_version: bytes):
        self._open()
        return self._inner.verify_all(bytes(source_version))

    def repair_index(self, name: bytes, source_version: bytes):
        self._open()
        return self._inner.repair_index(bytes(name), bytes(source_version))

    def deactivate_index(self, name: bytes):
        self._open()
        return self._inner.deactivate_index(bytes(name))

    def metrics(self):
        self._open()
        return self._inner.metrics()

    def export_current(self) -> bytes:
        self._open()
        return self._inner.export_current()

    def import_current(self, bundle: bytes, expected_source: bytes | None = None):
        self._open()
        return self._inner.import_current(
            bytes(bundle), None if expected_source is None else bytes(expected_source)
        )

    def keep_last(self, count: int):
        self._open()
        return self._inner.keep_last(count)

    def get_async(self, key: bytes):
        owned_key = bytes(key)
        return _background(lambda: self.get(owned_key))

    def put_async(self, key: bytes, value: bytes):
        owned_key, owned_value = bytes(key), bytes(value)
        return _background(lambda: self.put(owned_key, owned_value))

    def apply_async(self, mutations):
        owned = tuple(mutations)
        return _background(lambda: self.apply(owned))

    def delete_async(self, key: bytes):
        owned_key = bytes(key)
        return _background(lambda: self.delete(owned_key))

    def ensure_index_async(self, name: bytes):
        owned_name = bytes(name)
        return _background(lambda: self.ensure_index(owned_name))

    def snapshot_async(self):
        return _background(self.snapshot)

    def snapshot_at_async(self, source_version: bytes):
        owned_version = bytes(source_version)
        return _background(lambda: self.snapshot_at(owned_version))


class IndexedSnapshot(_Scoped):
    def __init__(self, inner: _native.BindingIndexedSnapshot):
        super().__init__()
        self._inner = inner

    @property
    def id(self):
        self._open()
        return self._inner.id()

    def index(self, name: bytes) -> "SecondaryIndex":
        self._open()
        return SecondaryIndex(self._inner.index(bytes(name)))


class SecondaryIndex(_Scoped):
    def __init__(self, inner: _native.BindingSecondaryIndexSnapshot):
        super().__init__()
        self._inner = inner

    @property
    def name(self) -> bytes:
        self._open()
        return self._inner.name()

    def exact(self, term: bytes):
        self._open()
        return self._inner.exact(bytes(term))

    def prefix(self, prefix: bytes):
        self._open()
        return self._inner.prefix(bytes(prefix))

    def range(self, start: bytes, end: bytes | None = None):
        self._open()
        return self._inner.range(bytes(start), None if end is None else bytes(end))

    def records(self, term: bytes):
        self._open()
        return self._inner.records(bytes(term))

    def exact_page(self, term: bytes, cursor: bytes | None = None, limit: int = 256):
        self._open()
        return self._inner.exact_page(bytes(term), cursor, limit)

    def exact_reverse_page(self, term: bytes, cursor: bytes | None = None, limit: int = 256):
        self._open()
        return self._inner.exact_reverse_page(bytes(term), cursor, limit)

    def prefix_page(self, prefix: bytes, cursor: bytes | None = None, limit: int = 256):
        self._open()
        return self._inner.prefix_page(bytes(prefix), cursor, limit)

    def prefix_reverse_page(self, prefix: bytes, cursor: bytes | None = None, limit: int = 256):
        self._open()
        return self._inner.prefix_reverse_page(bytes(prefix), cursor, limit)

    def range_page(self, start: bytes, end: bytes | None = None, cursor: bytes | None = None, limit: int = 256):
        self._open()
        return self._inner.range_page(bytes(start), None if end is None else bytes(end), cursor, limit)

    def range_reverse_page(self, start: bytes, end: bytes | None = None, cursor: bytes | None = None, limit: int = 256):
        self._open()
        return self._inner.range_reverse_page(bytes(start), None if end is None else bytes(end), cursor, limit)


class ProximityMap(_Scoped):
    def __init__(self, inner: _native.BindingProximityMap):
        super().__init__()
        self._inner = inner

    @property
    def descriptor(self) -> bytes:
        self._open()
        return self._inner.descriptor()

    def get(self, key: bytes):
        self._open()
        return self._inner.get(bytes(key))

    def contains(self, key: bytes) -> bool:
        self._open()
        return self._inner.contains_key(bytes(key))

    @property
    def count(self) -> int:
        self._open()
        return self._inner.count()

    @property
    def config(self):
        self._open()
        return self._inner.config()

    def build_hnsw(
        self,
        config: _native.HnswConfigRecord | None = None,
        limits: _native.HnswBuildLimitsRecord | None = None,
    ) -> HnswBuildResult:
        self._open()
        result = self._inner.build_hnsw(
            config or _native.default_hnsw_config(),
            limits or _native.default_hnsw_build_limits(),
        )
        return HnswBuildResult(HnswIndex(result.index), result.stats)

    def load_hnsw(self, manifest: bytes) -> "HnswIndex":
        self._open()
        return HnswIndex(self._inner.load_hnsw(bytes(manifest)))

    def build_pq(
        self,
        config: _native.ProductQuantizationConfigRecord | None = None,
        *,
        worker_threads: int = 1,
        limits: _native.ProductQuantizationBuildLimitsRecord | None = None,
    ) -> ProductQuantizationBuildResult:
        self._open()
        result = self._inner.build_pq(
            config or _native.default_pq_config(),
            worker_threads,
            limits or _native.default_pq_build_limits(),
        )
        return ProductQuantizationBuildResult(
            ProductQuantizer(result.index), result.stats
        )

    def load_pq(self, manifest: bytes) -> "ProductQuantizer":
        self._open()
        return ProductQuantizer(self._inner.load_pq(bytes(manifest)))

    def build_composite_hnsw(
        self,
        base_map: "ProximityMap",
        base: "HnswIndex",
        config: _native.CompositeAcceleratorConfigRecord | None = None,
        limits: _native.CompositeBuildLimitsRecord | None = None,
    ) -> CompositeBuildOutcome:
        self._open()
        base_map._open()
        base._open()
        result = self._inner.build_composite_hnsw(
            base_map._inner,
            base._inner,
            config or _native.default_composite_accelerator_config(),
            limits or _native.default_composite_build_limits(),
        )
        return CompositeBuildOutcome(
            None if result.accelerator is None else CompositeAccelerator(result.accelerator),
            result.reasons,
            result.stats,
        )

    def build_composite_pq(
        self,
        base_map: "ProximityMap",
        base: "ProductQuantizer",
        config: _native.CompositeAcceleratorConfigRecord | None = None,
        limits: _native.CompositeBuildLimitsRecord | None = None,
    ) -> CompositeBuildOutcome:
        self._open()
        base_map._open()
        base._open()
        result = self._inner.build_composite_pq(
            base_map._inner,
            base._inner,
            config or _native.default_composite_accelerator_config(),
            limits or _native.default_composite_build_limits(),
        )
        return CompositeBuildOutcome(
            None if result.accelerator is None else CompositeAccelerator(result.accelerator),
            result.reasons,
            result.stats,
        )

    @staticmethod
    def _rebuild_outcome(result) -> CompositeBuildOrRebuildOutcome:
        return CompositeBuildOrRebuildOutcome(
            result.kind,
            None if result.composite is None else CompositeAccelerator(result.composite),
            None if result.hnsw is None else HnswIndex(result.hnsw),
            None if result.pq is None else ProductQuantizer(result.pq),
            result.reasons,
            result.composite_stats,
            result.hnsw_stats,
            result.pq_stats,
        )

    def build_or_rebuild_composite_hnsw(
        self,
        base_map: "ProximityMap",
        base: "HnswIndex",
        config: _native.CompositeAcceleratorConfigRecord | None = None,
        limits: _native.CompositeBuildLimitsRecord | None = None,
        rebuild: _native.CompositeRebuildOptionsRecord | None = None,
    ) -> CompositeBuildOrRebuildOutcome:
        self._open()
        base_map._open()
        base._open()
        return self._rebuild_outcome(
            self._inner.build_or_rebuild_composite_hnsw(
                base_map._inner,
                base._inner,
                config or _native.default_composite_accelerator_config(),
                limits or _native.default_composite_build_limits(),
                rebuild or _native.default_composite_rebuild_options(),
            )
        )

    def build_or_rebuild_composite_pq(
        self,
        base_map: "ProximityMap",
        base: "ProductQuantizer",
        config: _native.CompositeAcceleratorConfigRecord | None = None,
        limits: _native.CompositeBuildLimitsRecord | None = None,
        rebuild: _native.CompositeRebuildOptionsRecord | None = None,
    ) -> CompositeBuildOrRebuildOutcome:
        self._open()
        base_map._open()
        base._open()
        return self._rebuild_outcome(
            self._inner.build_or_rebuild_composite_pq(
                base_map._inner,
                base._inner,
                config or _native.default_composite_accelerator_config(),
                limits or _native.default_composite_build_limits(),
                rebuild or _native.default_composite_rebuild_options(),
            )
        )

    def load_composite(self, manifest: bytes) -> "CompositeAccelerator":
        self._open()
        return CompositeAccelerator(self._inner.load_composite(bytes(manifest)))

    def build_accelerator_catalog(
        self,
        *,
        hnsw: "HnswIndex | None" = None,
        pq: "ProductQuantizer | None" = None,
        composite: "CompositeAccelerator | None" = None,
    ) -> "AcceleratorCatalog":
        self._open()
        for value in (hnsw, pq, composite):
            if value is not None:
                value._open()
        return AcceleratorCatalog(
            self._inner.build_accelerator_catalog(
                None if hnsw is None else hnsw._inner,
                None if pq is None else pq._inner,
                None if composite is None else composite._inner,
            )
        )

    def load_accelerator_catalog(self, manifest: bytes) -> "AcceleratorCatalog":
        self._open()
        return AcceleratorCatalog(
            self._inner.load_accelerator_catalog(bytes(manifest))
        )

    def search(self, request: _native.ProximitySearchRequestRecord):
        with self.read() as session:
            return session.search(request)

    def search_with_runtime(
        self,
        request: _native.ProximitySearchRequestRecord,
        runtime: "ProximitySearchRuntime",
    ):
        self._open()
        runtime._open()
        return self._inner.search_with_runtime(
            _owned_proximity_search_request(request), runtime._inner
        )

    def search_cancellable(
        self,
        request: _native.ProximitySearchRequestRecord,
        *,
        runtime: "ProximitySearchRuntime | None" = None,
        cancellation: ProximityCancellationToken,
    ):
        self._open()
        cancellation._open()
        if runtime is not None:
            runtime._open()
        return self._inner.search_cancellable(
            _owned_proximity_search_request(request),
            None if runtime is None else runtime._inner,
            cancellation._inner,
        )

    async def search_async(
        self,
        request: _native.ProximitySearchRequestRecord,
        *,
        runtime: "ProximitySearchRuntime | None" = None,
        cancellation: "ProximityCancellationToken | None" = None,
    ):
        owned_request = _owned_proximity_search_request(request)
        token = cancellation or ProximityCancellationToken()
        try:
            return await asyncio.to_thread(
                self.search_cancellable,
                owned_request,
                runtime=runtime,
                cancellation=token,
            )
        except asyncio.CancelledError:
            token.cancel()
            raise

    def search_exact(self, query: Sequence[float], k: int):
        with self.read() as session:
            return session.search_exact(query, k)

    def scan_records(
        self, visitor: Callable[[_native.ProximityRecordRecord], bool]
    ) -> int:
        self._open()
        return self._inner.scan_records(_ProximityRecordVisitor(visitor))

    def read(self) -> "ProximityReadSession":
        self._open()
        return ProximityReadSession(self._inner.read_session())

    def search_view(
        self,
        query: Sequence[float],
        k: int,
        visit: Callable[[tuple[NeighborView, ...]], Any],
    ):
        with self.read() as session:
            return session.search_view(query, k, visit)

    def mutate(self, mutations):
        self._open()
        result = self._inner.mutate(mutations)
        return ProximityMap(result.map), result.stats

    def rebuild(self, mutations):
        self._open()
        return ProximityMap(self._inner.rebuild(mutations))

    def verify(self):
        self._open()
        return self._inner.verify()

    def prove_membership(self, key: bytes):
        self._open()
        return self._inner.prove_membership(bytes(key))

    def prove_search(self, request, limits=None) -> "ProximitySearchProof":
        self._open()
        return ProximitySearchProof(
            self._inner.prove_search(
                _owned_proximity_search_request(request),
                limits or _native.default_content_graph_limits(),
            )
        )

    def prove_search_exact(
        self, query: Sequence[float], k: int, limits=None
    ) -> "ProximitySearchProof":
        return self.prove_search(
            _native.exact_proximity_search_request(
                [float(value) for value in query], k
            ),
            limits,
        )

    def prove_structure(self, limits=None):
        self._open()
        return self._inner.prove_structure(
            limits or _native.default_content_graph_limits()
        )

    def clear_cache(self) -> None:
        self._open()
        self._inner.clear_content_cache()


class ProximitySearchRuntime(_Scoped):
    def __init__(self, inner: _native.BindingProximitySearchRuntime):
        super().__init__()
        self._inner = inner

    @property
    def policy(self) -> _native.ProximitySearchRuntimePolicyRecord:
        self._open()
        return self._inner.policy()

    @property
    def stats(self) -> _native.ProximitySearchRuntimeStatsRecord:
        self._open()
        return self._inner.stats()

    def clear(self) -> None:
        self._open()
        self._inner.clear()


class HnswIndex(_Scoped):
    def __init__(self, inner: _native.BindingHnswIndex):
        super().__init__()
        self._inner = inner

    @property
    def manifest(self) -> bytes:
        self._open()
        return self._inner.manifest()

    @property
    def source_descriptor(self) -> bytes:
        self._open()
        return self._inner.source_descriptor()

    @property
    def config(self) -> _native.HnswConfigRecord:
        self._open()
        return self._inner.config()

    @property
    def is_canonical(self) -> bool:
        self._open()
        return self._inner.is_canonical()

    def search(
        self,
        map: ProximityMap,
        request: _native.ProximitySearchRequestRecord,
    ) -> _native.ProximitySearchResultRecord:
        self._open()
        map._open()
        return self._inner.search(
            map._inner,
            _owned_proximity_search_request(request),
        )

    def search_with_runtime(
        self,
        map: ProximityMap,
        request: _native.ProximitySearchRequestRecord,
        runtime: ProximitySearchRuntime,
    ) -> _native.ProximitySearchResultRecord:
        self._open()
        map._open()
        runtime._open()
        return self._inner.search_with_runtime(
            map._inner,
            _owned_proximity_search_request(request),
            runtime._inner,
        )

    def search_cancellable(
        self,
        map: ProximityMap,
        request: _native.ProximitySearchRequestRecord,
        *,
        runtime: ProximitySearchRuntime | None = None,
        cancellation: ProximityCancellationToken,
    ) -> _native.ProximitySearchResultRecord:
        self._open()
        map._open()
        cancellation._open()
        if runtime is not None:
            runtime._open()
        return self._inner.search_cancellable(
            map._inner,
            _owned_proximity_search_request(request),
            None if runtime is None else runtime._inner,
            cancellation._inner,
        )

    async def search_async(
        self,
        map: ProximityMap,
        request: _native.ProximitySearchRequestRecord,
        *,
        runtime: ProximitySearchRuntime | None = None,
        cancellation: ProximityCancellationToken | None = None,
    ) -> _native.ProximitySearchResultRecord:
        token = cancellation or ProximityCancellationToken()
        try:
            return await asyncio.to_thread(
                self.search_cancellable,
                map,
                _owned_proximity_search_request(request),
                runtime=runtime,
                cancellation=token,
            )
        except asyncio.CancelledError:
            token.cancel()
            raise

    def prove_search(
        self,
        map: ProximityMap,
        request: _native.ProximitySearchRequestRecord,
        limits: _native.ContentGraphLimitsRecord | None = None,
    ) -> "ProximitySearchProof":
        self._open()
        map._open()
        return ProximitySearchProof(
            self._inner.prove_search(
                map._inner,
                _owned_proximity_search_request(request),
                limits or _native.default_content_graph_limits(),
            )
        )


class ProductQuantizer(_Scoped):
    def __init__(self, inner: _native.BindingProductQuantizer):
        super().__init__()
        self._inner = inner

    @property
    def manifest(self) -> bytes:
        self._open()
        return self._inner.manifest()

    @property
    def source_descriptor(self) -> bytes:
        self._open()
        return self._inner.source_descriptor()

    @property
    def config(self) -> _native.ProductQuantizationConfigRecord:
        self._open()
        return self._inner.config()

    @property
    def quality(self) -> _native.ProductQuantizationQualityRecord:
        self._open()
        return self._inner.quality()

    def search(
        self,
        map: ProximityMap,
        request: _native.ProximitySearchRequestRecord,
    ) -> _native.ProximitySearchResultRecord:
        self._open()
        map._open()
        return self._inner.search(
            map._inner,
            _owned_proximity_search_request(request),
        )

    def search_with_runtime(
        self,
        map: ProximityMap,
        request: _native.ProximitySearchRequestRecord,
        runtime: ProximitySearchRuntime,
    ) -> _native.ProximitySearchResultRecord:
        self._open()
        map._open()
        runtime._open()
        return self._inner.search_with_runtime(
            map._inner,
            _owned_proximity_search_request(request),
            runtime._inner,
        )

    def search_cancellable(
        self,
        map: ProximityMap,
        request: _native.ProximitySearchRequestRecord,
        *,
        runtime: ProximitySearchRuntime | None = None,
        cancellation: ProximityCancellationToken,
    ) -> _native.ProximitySearchResultRecord:
        self._open()
        map._open()
        cancellation._open()
        if runtime is not None:
            runtime._open()
        return self._inner.search_cancellable(
            map._inner,
            _owned_proximity_search_request(request),
            None if runtime is None else runtime._inner,
            cancellation._inner,
        )

    async def search_async(
        self,
        map: ProximityMap,
        request: _native.ProximitySearchRequestRecord,
        *,
        runtime: ProximitySearchRuntime | None = None,
        cancellation: ProximityCancellationToken | None = None,
    ) -> _native.ProximitySearchResultRecord:
        token = cancellation or ProximityCancellationToken()
        try:
            return await asyncio.to_thread(
                self.search_cancellable,
                map,
                _owned_proximity_search_request(request),
                runtime=runtime,
                cancellation=token,
            )
        except asyncio.CancelledError:
            token.cancel()
            raise

    def prove_search(
        self,
        map: ProximityMap,
        request: _native.ProximitySearchRequestRecord,
        limits: _native.ContentGraphLimitsRecord | None = None,
    ) -> "ProximitySearchProof":
        self._open()
        map._open()
        return ProximitySearchProof(
            self._inner.prove_search(
                map._inner,
                _owned_proximity_search_request(request),
                limits or _native.default_content_graph_limits(),
            )
        )


class CompositeAccelerator(_Scoped):
    def __init__(self, inner: _native.BindingCompositeAccelerator):
        super().__init__()
        self._inner = inner

    @property
    def manifest(self) -> bytes:
        self._open()
        return self._inner.manifest()

    @property
    def current_source_descriptor(self) -> bytes:
        self._open()
        return self._inner.current_source_descriptor()

    @property
    def base_source_descriptor(self) -> bytes:
        self._open()
        return self._inner.base_source_descriptor()

    @property
    def base_kind(self):
        self._open()
        return self._inner.base_kind()

    @property
    def delta_count(self) -> int:
        self._open()
        return self._inner.delta_count()

    @property
    def shadow_count(self) -> int:
        self._open()
        return self._inner.shadow_count()

    @property
    def config(self):
        self._open()
        return self._inner.config()

    @property
    def build_stats(self):
        self._open()
        return self._inner.build_stats()

    def search(self, map: ProximityMap, request):
        self._open()
        map._open()
        return self._inner.search(map._inner, _owned_proximity_search_request(request))

    def search_with_runtime(
        self,
        map: ProximityMap,
        request,
        runtime: ProximitySearchRuntime,
    ):
        self._open()
        map._open()
        runtime._open()
        return self._inner.search_with_runtime(
            map._inner,
            _owned_proximity_search_request(request),
            runtime._inner,
        )

    def search_cancellable(
        self,
        map: ProximityMap,
        request,
        *,
        runtime: ProximitySearchRuntime | None = None,
        cancellation: ProximityCancellationToken,
    ):
        self._open()
        map._open()
        cancellation._open()
        if runtime is not None:
            runtime._open()
        return self._inner.search_cancellable(
            map._inner,
            _owned_proximity_search_request(request),
            None if runtime is None else runtime._inner,
            cancellation._inner,
        )

    async def search_async(
        self,
        map: ProximityMap,
        request,
        *,
        runtime: ProximitySearchRuntime | None = None,
        cancellation: ProximityCancellationToken | None = None,
    ):
        token = cancellation or ProximityCancellationToken()
        try:
            return await asyncio.to_thread(
                self.search_cancellable,
                map,
                _owned_proximity_search_request(request),
                runtime=runtime,
                cancellation=token,
            )
        except asyncio.CancelledError:
            token.cancel()
            raise

    def prove_search(self, map: ProximityMap, request, limits=None) -> "ProximitySearchProof":
        self._open()
        map._open()
        return ProximitySearchProof(
            self._inner.prove_search(
                map._inner,
                _owned_proximity_search_request(request),
                limits or _native.default_content_graph_limits(),
            )
        )


class AcceleratorCatalog(_Scoped):
    def __init__(self, inner: _native.BindingAcceleratorCatalog):
        super().__init__()
        self._inner = inner

    @property
    def manifest(self) -> bytes:
        self._open()
        return self._inner.manifest()

    @property
    def source_descriptor(self) -> bytes:
        self._open()
        return self._inner.source_descriptor()

    @property
    def entries(self):
        self._open()
        return self._inner.entries()

    def search(self, map: ProximityMap, request):
        self._open()
        map._open()
        return self._inner.search(map._inner, _owned_proximity_search_request(request))

    def search_with_runtime(
        self,
        map: ProximityMap,
        request,
        runtime: ProximitySearchRuntime,
    ):
        self._open()
        map._open()
        runtime._open()
        return self._inner.search_with_runtime(
            map._inner,
            _owned_proximity_search_request(request),
            runtime._inner,
        )

    def search_cancellable(
        self,
        map: ProximityMap,
        request,
        *,
        runtime: ProximitySearchRuntime | None = None,
        cancellation: ProximityCancellationToken,
    ):
        self._open()
        map._open()
        cancellation._open()
        if runtime is not None:
            runtime._open()
        return self._inner.search_cancellable(
            map._inner,
            _owned_proximity_search_request(request),
            None if runtime is None else runtime._inner,
            cancellation._inner,
        )

    async def search_async(
        self,
        map: ProximityMap,
        request,
        *,
        runtime: ProximitySearchRuntime | None = None,
        cancellation: ProximityCancellationToken | None = None,
    ):
        token = cancellation or ProximityCancellationToken()
        try:
            return await asyncio.to_thread(
                self.search_cancellable,
                map,
                _owned_proximity_search_request(request),
                runtime=runtime,
                cancellation=token,
            )
        except asyncio.CancelledError:
            token.cancel()
            raise

    def prove_search(self, map: ProximityMap, request, limits=None) -> "ProximitySearchProof":
        self._open()
        map._open()
        return ProximitySearchProof(
            self._inner.prove_search(
                map._inner,
                _owned_proximity_search_request(request),
                limits or _native.default_content_graph_limits(),
            )
        )


class ProximityReadSession(_Scoped):
    def __init__(self, inner: _native.BindingProximityReadSession):
        super().__init__()
        self._inner = inner

    def get(self, key: bytes):
        self._open()
        return self._inner.get(bytes(key))

    def contains(self, key: bytes) -> bool:
        self._open()
        return self._inner.contains_key(bytes(key))

    def search(self, request: _native.ProximitySearchRequestRecord):
        self._open()
        return self._inner.search(_owned_proximity_search_request(request))

    def search_with_runtime(
        self,
        request: _native.ProximitySearchRequestRecord,
        runtime: ProximitySearchRuntime,
    ):
        self._open()
        runtime._open()
        return self._inner.search_with_runtime(
            _owned_proximity_search_request(request), runtime._inner
        )

    def search_cancellable(
        self,
        request: _native.ProximitySearchRequestRecord,
        *,
        runtime: ProximitySearchRuntime | None = None,
        cancellation: ProximityCancellationToken,
    ):
        self._open()
        cancellation._open()
        if runtime is not None:
            runtime._open()
        return self._inner.search_cancellable(
            _owned_proximity_search_request(request),
            None if runtime is None else runtime._inner,
            cancellation._inner,
        )

    async def search_async(
        self,
        request: _native.ProximitySearchRequestRecord,
        *,
        runtime: ProximitySearchRuntime | None = None,
        cancellation: ProximityCancellationToken | None = None,
    ):
        token = cancellation or ProximityCancellationToken()
        try:
            return await asyncio.to_thread(
                self.search_cancellable,
                _owned_proximity_search_request(request),
                runtime=runtime,
                cancellation=token,
            )
        except asyncio.CancelledError:
            token.cancel()
            raise

    def search_exact(self, query: Sequence[float], k: int):
        return self.search(
            _native.exact_proximity_search_request(
                [float(value) for value in query], k
            )
        )

    def scan_records(
        self, visitor: Callable[[_native.ProximityRecordRecord], bool]
    ) -> int:
        self._open()
        return self._inner.scan_records(_ProximityRecordVisitor(visitor))

    def search_view(
        self,
        query: Sequence[float],
        k: int,
        visit: Callable[[tuple[NeighborView, ...]], Any],
    ):
        self._open()
        return proximity_search_view(self._inner.fast_handle(), query, k, visit)


class _ProximityRecordVisitor(_native.ProximityRecordVisitorCallback):
    def __init__(
        self, visitor: Callable[[_native.ProximityRecordRecord], bool]
    ) -> None:
        self._visitor = visitor

    def visit(self, record: _native.ProximityRecordRecord) -> bool:
        return bool(self._visitor(record))


class ProximitySearchProof(_Scoped):
    def __init__(self, inner: _native.BindingProximitySearchProof):
        super().__init__()
        self._inner = inner

    @property
    def source_descriptor(self) -> bytes:
        self._open()
        return self._inner.source_descriptor()

    def verify(self, expected_descriptor: bytes | None = None, limits=None):
        self._open()
        return self._inner.verify(
            None if expected_descriptor is None else bytes(expected_descriptor),
            limits or _native.default_content_graph_limits(),
        )


def verify_key_proof(proof):
    return _native.verify_key_proof(proof)


def verify_proximity_membership_proof(proof, expected_descriptor: bytes | None = None):
    return _native.verify_proximity_membership_proof(
        proof,
        None if expected_descriptor is None else bytes(expected_descriptor),
    )


def verify_proximity_structure_proof(
    proof, expected_descriptor: bytes | None = None, limits=None
):
    return _native.verify_proximity_structure_proof(
        proof,
        None if expected_descriptor is None else bytes(expected_descriptor),
        limits or _native.default_content_graph_limits(),
    )


__all__ = [
    "AcceleratorCatalog",
    "CompositeAccelerator",
    "CompositeBuildOrRebuildOutcome",
    "CompositeBuildOutcome",
    "Engine",
    "EntryView",
    "HnswBuildResult",
    "HnswIndex",
    "IndexProjection",
    "IndexRegistry",
    "IndexedMap",
    "IndexedSnapshot",
    "MapComparison",
    "MapMerge",
    "MapSubscription",
    "MapSnapshot",
    "NeighborView",
    "ProximityMap",
    "ProximityCancellationToken",
    "ProximityReadSession",
    "ProximityRecord",
    "ProximitySearchRuntime",
    "ProximitySearchRuntimePolicy",
    "ProximitySearchRuntimeStats",
    "ProximitySearchProof",
    "ProductQuantizationBuildResult",
    "ProductQuantizer",
    "SecondaryIndex",
    "ScopedBytes",
    "ReadSession",
    "ScanOutcome",
    "VersionedMap",
    "VersionedTransaction",
    "verify_key_proof",
    "verify_proximity_membership_proof",
    "verify_proximity_structure_proof",
]
