package prolly

import (
	"bytes"
	"errors"
	"runtime"
	"sync"
	"sync/atomic"
)

type IndexProjection int32

const (
	IndexProjectionKeysOnly IndexProjection = 1
	IndexProjectionInclude  IndexProjection = 2
	IndexProjectionAll      IndexProjection = 3
)

type IndexEntry struct {
	Term       []byte
	Projection []byte
}

type IndexExtractor interface {
	Extract(primaryKey, sourceValue []byte) ([]IndexEntry, error)
}

type IndexExtractorFunc func(primaryKey, sourceValue []byte) ([]IndexEntry, error)

func (f IndexExtractorFunc) Extract(primaryKey, sourceValue []byte) ([]IndexEntry, error) {
	return f(primaryKey, sourceValue)
}

type SecondaryIndexLimits struct {
	MaxTermBytes                      uint64
	MaxProjectionBytes                uint64
	MaxAllValueBytes                  uint64
	MaxTermsPerRecord                 uint64
	MaxProjectedBytesPerRecord        uint64
	MaxDerivedMutationsPerTransaction uint64
	MaxProjectedBytesPerTransaction   uint64
	MaxIndexes                        uint64
	BuildPageSize                     uint64
	MaxTemporarySortBytes             uint64
	MaxBundleNodes                    uint64
	MaxBundleBytes                    uint64
	MaxVerificationEntries            uint64
	MaxWriteRetries                   uint64
	MaxBuildRetries                   uint64
}

type IndexRegistry struct {
	handle uint64
	closed atomic.Bool
	mu     sync.RWMutex
}

var indexExtractorVtableOnce sync.Once

func NewIndexRegistry() (*IndexRegistry, error) {
	indexExtractorVtableOnce.Do(ffiRegisterIndexExtractorVtable)
	handle, err := ffiNewIndexRegistry()
	if err != nil {
		return nil, err
	}
	registry := &IndexRegistry{handle: handle}
	runtime.SetFinalizer(registry, (*IndexRegistry).Close)
	return registry, nil
}

func (r *IndexRegistry) Close() {
	if r == nil || r.closed.Swap(true) {
		return
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	runtime.SetFinalizer(r, nil)
	if r.handle != 0 {
		ffiFreeIndexRegistry(r.handle)
		r.handle = 0
	}
}

func (r *IndexRegistry) withHandle() (uint64, func(), error) {
	if r == nil || r.closed.Load() {
		return 0, nil, errors.New("index registry is closed")
	}
	r.mu.RLock()
	if r.closed.Load() || r.handle == 0 {
		r.mu.RUnlock()
		return 0, nil, errors.New("index registry is closed")
	}
	return r.handle, r.mu.RUnlock, nil
}

func (r *IndexRegistry) Register(name []byte, generation uint64, extractorID string, projection IndexProjection, limits *SecondaryIndexLimits, extractor IndexExtractor) error {
	if extractor == nil {
		return errors.New("nil secondary index extractor")
	}
	if projection < IndexProjectionKeysOnly || projection > IndexProjectionAll {
		return errors.New("invalid index projection")
	}
	handle, unlock, err := r.withHandle()
	if err != nil {
		return err
	}
	defer unlock()
	callback := registerGoIndexExtractor(extractor)
	err = ffiIndexRegistryRegister(handle, append([]byte(nil), name...), generation, extractorID, int32(projection), encodeOptionalSecondaryIndexLimits(limits), callback)
	if err != nil {
		removeGoIndexExtractor(callback)
	}
	return err
}

func encodeOptionalSecondaryIndexLimits(limits *SecondaryIndexLimits) []byte {
	if limits == nil {
		return []byte{0}
	}
	var out bytes.Buffer
	out.WriteByte(1)
	values := [...]uint64{
		limits.MaxTermBytes, limits.MaxProjectionBytes, limits.MaxAllValueBytes,
		limits.MaxTermsPerRecord, limits.MaxProjectedBytesPerRecord,
		limits.MaxDerivedMutationsPerTransaction, limits.MaxProjectedBytesPerTransaction,
		limits.MaxIndexes, limits.BuildPageSize, limits.MaxTemporarySortBytes,
		limits.MaxBundleNodes, limits.MaxBundleBytes, limits.MaxVerificationEntries,
		limits.MaxWriteRetries, limits.MaxBuildRetries,
	}
	for _, value := range values {
		writeU64(&out, value)
	}
	return out.Bytes()
}

type IndexedVersion struct {
	SourceVersion  []byte
	CatalogVersion []byte
	IndexCount     uint64
}

type IndexedSnapshotID struct {
	SourceVersion  []byte
	CatalogVersion []byte
}

type IndexedUpdateKind int32

const (
	IndexedUpdateApplied   IndexedUpdateKind = 1
	IndexedUpdateUnchanged IndexedUpdateKind = 2
	IndexedUpdateConflict  IndexedUpdateKind = 3
)

type IndexedUpdate struct {
	Kind                  IndexedUpdateKind
	PreviousSourceVersion []byte
	Current               *IndexedVersion
}

type IndexBuildResult struct {
	SourceVersion  []byte
	IndexVersion   []byte
	CatalogVersion []byte
	Generation     uint64
	Entries        uint64
	Attempts       uint64
	Activated      bool
}

type IndexVerification struct {
	Name                 []byte
	SourceVersion        []byte
	ExpectedIndexVersion []byte
	ActualIndexVersion   []byte
	ExpectedEntries      uint64
	ActualEntries        uint64
	SemanticDifferences  uint64
	Valid                bool
	Canonical            bool
}

type ActiveIndexHealth struct {
	Name         []byte
	Generation   uint64
	Fingerprint  []byte
	Projection   IndexProjection
	IndexMapID   []byte
	IndexVersion []byte
}

type IndexedMapHealth struct {
	SourceMapID          []byte
	SourceVersion        []byte
	CatalogVersion       []byte
	ActiveIndexes        []ActiveIndexHealth
	SupportsTransactions bool
}

type IndexedMapMetrics struct {
	NormalizedSourceMutations uint64
	RecordsExtracted          uint64
	TermsEmitted              uint64
	ProjectedBytes            uint64
	PhysicalUpserts           uint64
	PhysicalDeletes           uint64
	UnchangedEmissionsSkipped uint64
	SourceNodesWritten        uint64
	IndexNodesWritten         uint64
	CatalogNodesWritten       uint64
	Retries                   uint64
	BuildAttempts             uint64
	VerificationOutcomes      uint64
	RetainedRoots             uint64
}

type IndexedRetention struct {
	RetainedSourceVersions   [][]byte
	RemovedSourceVersions    [][]byte
	RetainedIndexVersions    [][]byte
	RemovedIndexVersions     [][]byte
	RemovedCatalogVersions   [][]byte
	RemovedCheckpointRecords uint64
	RemovedNamedRoots        [][]byte
}

type IndexedMap struct {
	handle uint64
	closed atomic.Bool
	mu     sync.RWMutex
}

func (e *Engine) IndexedMap(id []byte, registry *IndexRegistry) (*IndexedMap, error) {
	registryHandle, unlock, err := registry.withHandle()
	if err != nil {
		return nil, err
	}
	defer unlock()
	handle, err := ffiEngineIndexedMap(e, append([]byte(nil), id...), registryHandle)
	if err != nil {
		return nil, err
	}
	result := &IndexedMap{handle: handle}
	runtime.SetFinalizer(result, (*IndexedMap).Close)
	return result, nil
}

func (m *IndexedMap) Close() {
	if m == nil || m.closed.Swap(true) {
		return
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	runtime.SetFinalizer(m, nil)
	if m.handle != 0 {
		ffiFreeIndexedMap(m.handle)
		m.handle = 0
	}
}
func (m *IndexedMap) withHandle() (uint64, func(), error) {
	if m == nil || m.closed.Load() {
		return 0, nil, errors.New("indexed map is closed")
	}
	m.mu.RLock()
	if m.closed.Load() || m.handle == 0 {
		m.mu.RUnlock()
		return 0, nil, errors.New("indexed map is closed")
	}
	return m.handle, m.mu.RUnlock, nil
}

func (m *IndexedMap) ID() ([]byte, error) {
	handle, unlock, err := m.withHandle()
	if err != nil {
		return nil, err
	}
	defer unlock()
	raw, err := ffiIndexedMapID(handle)
	if err != nil {
		return nil, err
	}
	d := byteDecoder{data: raw}
	id, err := d.readByteArray()
	if err != nil {
		return nil, err
	}
	return id, d.done()
}

func (m *IndexedMap) Apply(mutations []Mutation) (IndexedVersion, error) {
	handle, unlock, err := m.withHandle()
	if err != nil {
		return IndexedVersion{}, err
	}
	defer unlock()
	encoded, err := encodeMutations(mutations)
	if err != nil {
		return IndexedVersion{}, err
	}
	raw, err := ffiIndexedMapApply(handle, encoded)
	if err != nil {
		return IndexedVersion{}, err
	}
	return decodeIndexedVersion(raw)
}

func (m *IndexedMap) ApplyIf(expectedSource []byte, mutations []Mutation) (IndexedUpdate, error) {
	handle, unlock, err := m.withHandle()
	if err != nil {
		return IndexedUpdate{}, err
	}
	defer unlock()
	encoded, err := encodeMutations(mutations)
	if err != nil {
		return IndexedUpdate{}, err
	}
	raw, err := ffiIndexedMapApplyIf(handle, append([]byte(nil), expectedSource...), encoded)
	if err != nil {
		return IndexedUpdate{}, err
	}
	return decodeIndexedUpdate(raw)
}

func (m *IndexedMap) Put(key, value []byte) (IndexedVersion, error) {
	handle, unlock, err := m.withHandle()
	if err != nil {
		return IndexedVersion{}, err
	}
	defer unlock()
	raw, err := ffiIndexedMapPut(handle, append([]byte(nil), key...), append([]byte(nil), value...))
	if err != nil {
		return IndexedVersion{}, err
	}
	return decodeIndexedVersion(raw)
}
func (m *IndexedMap) Delete(key []byte) (IndexedVersion, error) {
	handle, unlock, err := m.withHandle()
	if err != nil {
		return IndexedVersion{}, err
	}
	defer unlock()
	raw, err := ffiIndexedMapDelete(handle, append([]byte(nil), key...))
	if err != nil {
		return IndexedVersion{}, err
	}
	return decodeIndexedVersion(raw)
}
func (m *IndexedMap) Get(key []byte) ([]byte, bool, error) {
	handle, unlock, err := m.withHandle()
	if err != nil {
		return nil, false, err
	}
	defer unlock()
	raw, err := ffiIndexedMapGet(handle, append([]byte(nil), key...))
	if err != nil {
		return nil, false, err
	}
	decoder := byteDecoder{data: raw}
	value, ok, err := decoder.readOptionalByteArray()
	if err != nil {
		return nil, false, err
	}
	return value, ok, decoder.done()
}
func (m *IndexedMap) EnsureIndex(name []byte) (IndexBuildResult, error) {
	handle, unlock, err := m.withHandle()
	if err != nil {
		return IndexBuildResult{}, err
	}
	defer unlock()
	raw, err := ffiIndexedMapEnsureIndex(handle, append([]byte(nil), name...))
	if err != nil {
		return IndexBuildResult{}, err
	}
	return decodeIndexBuildResult(raw)
}

// ReplaceIndex shadow-builds a newer generation and atomically activates it.
// Retired extractor generations remain retained by the native map so exact
// historical snapshots can still be reopened through this handle.
func (m *IndexedMap) ReplaceIndex(name []byte, generation uint64, extractorID string, projection IndexProjection, limits *SecondaryIndexLimits, extractor IndexExtractor) (IndexBuildResult, error) {
	if extractor == nil {
		return IndexBuildResult{}, errors.New("nil secondary index extractor")
	}
	if projection < IndexProjectionKeysOnly || projection > IndexProjectionAll {
		return IndexBuildResult{}, errors.New("invalid index projection")
	}
	handle, unlock, err := m.withHandle()
	if err != nil {
		return IndexBuildResult{}, err
	}
	defer unlock()
	callback := registerGoIndexExtractor(extractor)
	raw, err := ffiIndexedMapReplaceIndex(
		handle, append([]byte(nil), name...), generation, extractorID,
		int32(projection), encodeOptionalSecondaryIndexLimits(limits), callback,
	)
	if err != nil {
		removeGoIndexExtractor(callback)
		return IndexBuildResult{}, err
	}
	return decodeIndexBuildResult(raw)
}

func (m *IndexedMap) Health() (IndexedMapHealth, error) {
	handle, unlock, err := m.withHandle()
	if err != nil {
		return IndexedMapHealth{}, err
	}
	defer unlock()
	raw, err := ffiIndexedMapHealth(handle)
	if err != nil {
		return IndexedMapHealth{}, err
	}
	return decodeIndexedMapHealth(raw)
}

func (m *IndexedMap) VerifyIndex(name, sourceVersion []byte) (IndexVerification, error) {
	handle, unlock, err := m.withHandle()
	if err != nil {
		return IndexVerification{}, err
	}
	defer unlock()
	raw, err := ffiIndexedMapVerifyIndex(handle, append([]byte(nil), name...), append([]byte(nil), sourceVersion...))
	if err != nil {
		return IndexVerification{}, err
	}
	return decodeIndexVerification(raw)
}

func (m *IndexedMap) VerifyAll(sourceVersion []byte) ([]IndexVerification, error) {
	handle, unlock, err := m.withHandle()
	if err != nil {
		return nil, err
	}
	defer unlock()
	raw, err := ffiIndexedMapVerifyAll(handle, append([]byte(nil), sourceVersion...))
	if err != nil {
		return nil, err
	}
	return decodeIndexVerifications(raw)
}

func (m *IndexedMap) RepairIndex(name, sourceVersion []byte) (IndexVerification, error) {
	handle, unlock, err := m.withHandle()
	if err != nil {
		return IndexVerification{}, err
	}
	defer unlock()
	raw, err := ffiIndexedMapRepairIndex(handle, append([]byte(nil), name...), append([]byte(nil), sourceVersion...))
	if err != nil {
		return IndexVerification{}, err
	}
	return decodeIndexVerification(raw)
}

func (m *IndexedMap) DeactivateIndex(name []byte) (IndexedVersion, error) {
	handle, unlock, err := m.withHandle()
	if err != nil {
		return IndexedVersion{}, err
	}
	defer unlock()
	raw, err := ffiIndexedMapDeactivateIndex(handle, append([]byte(nil), name...))
	if err != nil {
		return IndexedVersion{}, err
	}
	return decodeIndexedVersion(raw)
}

func (m *IndexedMap) Metrics() (IndexedMapMetrics, error) {
	handle, unlock, err := m.withHandle()
	if err != nil {
		return IndexedMapMetrics{}, err
	}
	defer unlock()
	raw, err := ffiIndexedMapMetrics(handle)
	if err != nil {
		return IndexedMapMetrics{}, err
	}
	return decodeIndexedMapMetrics(raw)
}

func (m *IndexedMap) ExportCurrent() ([]byte, error) {
	handle, unlock, err := m.withHandle()
	if err != nil {
		return nil, err
	}
	defer unlock()
	raw, err := ffiIndexedMapExportCurrent(handle)
	if err != nil {
		return nil, err
	}
	d := byteDecoder{data: raw}
	bundle, err := d.readByteArray()
	if err != nil {
		return nil, err
	}
	return bundle, d.done()
}

func (m *IndexedMap) ImportCurrent(bundle, expectedSource []byte) (IndexedVersion, error) {
	handle, unlock, err := m.withHandle()
	if err != nil {
		return IndexedVersion{}, err
	}
	defer unlock()
	raw, err := ffiIndexedMapImportCurrent(handle, append([]byte(nil), bundle...), append([]byte(nil), expectedSource...))
	if err != nil {
		return IndexedVersion{}, err
	}
	return decodeIndexedVersion(raw)
}

func (m *IndexedMap) KeepLast(count uint64) (IndexedRetention, error) {
	handle, unlock, err := m.withHandle()
	if err != nil {
		return IndexedRetention{}, err
	}
	defer unlock()
	raw, err := ffiIndexedMapKeepLast(handle, count)
	if err != nil {
		return IndexedRetention{}, err
	}
	return decodeIndexedRetention(raw)
}

func (m *IndexedMap) PlanGC() (GcPlan, error) {
	handle, unlock, err := m.withHandle()
	if err != nil {
		return GcPlan{}, err
	}
	defer unlock()
	raw, err := ffiIndexedMapPlanGC(handle)
	if err != nil {
		return GcPlan{}, err
	}
	return decodeGcPlan(raw)
}
func (m *IndexedMap) Snapshot() (*IndexedSnapshot, error) {
	handle, unlock, err := m.withHandle()
	if err != nil {
		return nil, err
	}
	defer unlock()
	snapshot, err := ffiIndexedMapSnapshot(handle)
	if err != nil {
		return nil, err
	}
	return adoptIndexedSnapshot(snapshot), nil
}

func (m *IndexedMap) SnapshotAt(sourceVersion []byte) (*IndexedSnapshot, error) {
	handle, unlock, err := m.withHandle()
	if err != nil {
		return nil, err
	}
	defer unlock()
	snapshot, err := ffiIndexedMapSnapshotAt(handle, append([]byte(nil), sourceVersion...))
	if err != nil {
		return nil, err
	}
	return adoptIndexedSnapshot(snapshot), nil
}

func (m *IndexedMap) SnapshotByID(id IndexedSnapshotID) (*IndexedSnapshot, error) {
	handle, unlock, err := m.withHandle()
	if err != nil {
		return nil, err
	}
	defer unlock()
	snapshot, err := ffiIndexedMapSnapshotByID(handle, encodeIndexedSnapshotID(id))
	if err != nil {
		return nil, err
	}
	return adoptIndexedSnapshot(snapshot), nil
}

type IndexedSnapshot struct {
	handle uint64
	closed atomic.Bool
	mu     sync.RWMutex
}

func adoptIndexedSnapshot(handle uint64) *IndexedSnapshot {
	result := &IndexedSnapshot{handle: handle}
	runtime.SetFinalizer(result, (*IndexedSnapshot).Close)
	return result
}

func (s *IndexedSnapshot) Close() {
	if s == nil || s.closed.Swap(true) {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	runtime.SetFinalizer(s, nil)
	if s.handle != 0 {
		ffiFreeIndexedSnapshot(s.handle)
		s.handle = 0
	}
}
func (s *IndexedSnapshot) withHandle() (uint64, func(), error) {
	if s == nil || s.closed.Load() {
		return 0, nil, errors.New("indexed snapshot is closed")
	}
	s.mu.RLock()
	if s.closed.Load() || s.handle == 0 {
		s.mu.RUnlock()
		return 0, nil, errors.New("indexed snapshot is closed")
	}
	return s.handle, s.mu.RUnlock, nil
}

func (s *IndexedSnapshot) ID() (IndexedSnapshotID, error) {
	handle, unlock, err := s.withHandle()
	if err != nil {
		return IndexedSnapshotID{}, err
	}
	defer unlock()
	raw, err := ffiIndexedSnapshotID(handle)
	if err != nil {
		return IndexedSnapshotID{}, err
	}
	return decodeIndexedSnapshotID(raw)
}
func (s *IndexedSnapshot) Index(name []byte) (*SecondaryIndex, error) {
	handle, unlock, err := s.withHandle()
	if err != nil {
		return nil, err
	}
	defer unlock()
	index, err := ffiIndexedSnapshotIndex(handle, append([]byte(nil), name...))
	if err != nil {
		return nil, err
	}
	fast, err := ffiSecondaryIndexFastHandle(index)
	if err != nil {
		ffiFreeSecondaryIndex(index)
		return nil, err
	}
	result := &SecondaryIndex{handle: index, fast: fast}
	runtime.SetFinalizer(result, (*SecondaryIndex).Close)
	return result, nil
}

type SecondaryIndex struct {
	handle uint64
	fast   uint64
	closed atomic.Bool
	mu     sync.RWMutex
}

func (i *SecondaryIndex) Close() {
	if i == nil || i.closed.Swap(true) {
		return
	}
	i.mu.Lock()
	defer i.mu.Unlock()
	runtime.SetFinalizer(i, nil)
	if i.handle != 0 {
		ffiFreeSecondaryIndex(i.handle)
		i.handle = 0
		i.fast = 0
	}
}
func (i *SecondaryIndex) withHandle() (uint64, uint64, func(), error) {
	if i == nil || i.closed.Load() {
		return 0, 0, nil, errors.New("secondary index is closed")
	}
	i.mu.RLock()
	if i.closed.Load() || i.handle == 0 || i.fast == 0 {
		i.mu.RUnlock()
		return 0, 0, nil, errors.New("secondary index is closed")
	}
	return i.handle, i.fast, i.mu.RUnlock, nil
}

type IndexMatch struct {
	Term       []byte
	PrimaryKey []byte
	Projection []byte
}

type IndexPage struct {
	Matches    []IndexMatch
	NextCursor []byte
}

func (i *SecondaryIndex) Name() ([]byte, error) {
	handle, _, unlock, err := i.withHandle()
	if err != nil {
		return nil, err
	}
	defer unlock()
	raw, err := ffiSecondaryIndexName(handle)
	if err != nil {
		return nil, err
	}
	d := byteDecoder{data: raw}
	name, err := d.readByteArray()
	if err != nil {
		return nil, err
	}
	return name, d.done()
}

func (i *SecondaryIndex) Exact(term []byte) ([]IndexMatch, error) {
	handle, _, unlock, err := i.withHandle()
	if err != nil {
		return nil, err
	}
	defer unlock()
	raw, err := ffiSecondaryIndexExact(handle, append([]byte(nil), term...))
	if err != nil {
		return nil, err
	}
	return decodeIndexMatches(raw)
}

func (i *SecondaryIndex) Prefix(prefix []byte) ([]IndexMatch, error) {
	handle, _, unlock, err := i.withHandle()
	if err != nil {
		return nil, err
	}
	defer unlock()
	raw, err := ffiSecondaryIndexPrefix(handle, append([]byte(nil), prefix...))
	if err != nil {
		return nil, err
	}
	return decodeIndexMatches(raw)
}

func (i *SecondaryIndex) Range(start, end []byte) ([]IndexMatch, error) {
	handle, _, unlock, err := i.withHandle()
	if err != nil {
		return nil, err
	}
	defer unlock()
	raw, err := ffiSecondaryIndexRange(handle, append([]byte(nil), start...), append([]byte(nil), end...))
	if err != nil {
		return nil, err
	}
	return decodeIndexMatches(raw)
}

func (i *SecondaryIndex) ExactPage(term, cursor []byte, limit uint64) (IndexPage, error) {
	return i.page(func(handle uint64) ([]byte, error) {
		return ffiSecondaryIndexExactPage(handle, append([]byte(nil), term...), append([]byte(nil), cursor...), limit, false)
	})
}

func (i *SecondaryIndex) ExactReversePage(term, cursor []byte, limit uint64) (IndexPage, error) {
	return i.page(func(handle uint64) ([]byte, error) {
		return ffiSecondaryIndexExactPage(handle, append([]byte(nil), term...), append([]byte(nil), cursor...), limit, true)
	})
}

func (i *SecondaryIndex) PrefixPage(prefix, cursor []byte, limit uint64) (IndexPage, error) {
	return i.page(func(handle uint64) ([]byte, error) {
		return ffiSecondaryIndexPrefixPage(handle, append([]byte(nil), prefix...), append([]byte(nil), cursor...), limit, false)
	})
}

func (i *SecondaryIndex) PrefixReversePage(prefix, cursor []byte, limit uint64) (IndexPage, error) {
	return i.page(func(handle uint64) ([]byte, error) {
		return ffiSecondaryIndexPrefixPage(handle, append([]byte(nil), prefix...), append([]byte(nil), cursor...), limit, true)
	})
}

func (i *SecondaryIndex) RangePage(start, end, cursor []byte, limit uint64) (IndexPage, error) {
	return i.page(func(handle uint64) ([]byte, error) {
		return ffiSecondaryIndexRangePage(handle, append([]byte(nil), start...), append([]byte(nil), end...), append([]byte(nil), cursor...), limit, false)
	})
}

func (i *SecondaryIndex) RangeReversePage(start, end, cursor []byte, limit uint64) (IndexPage, error) {
	return i.page(func(handle uint64) ([]byte, error) {
		return ffiSecondaryIndexRangePage(handle, append([]byte(nil), start...), append([]byte(nil), end...), append([]byte(nil), cursor...), limit, true)
	})
}

func (i *SecondaryIndex) page(call func(uint64) ([]byte, error)) (IndexPage, error) {
	handle, _, unlock, err := i.withHandle()
	if err != nil {
		return IndexPage{}, err
	}
	defer unlock()
	raw, err := call(handle)
	if err != nil {
		return IndexPage{}, err
	}
	return decodeIndexPage(raw)
}

type IndexedSourceRecord struct {
	Term        []byte
	PrimaryKey  []byte
	Projection  []byte
	SourceValue []byte
}

func (i *SecondaryIndex) Records(term []byte) ([]IndexedSourceRecord, error) {
	handle, _, unlock, err := i.withHandle()
	if err != nil {
		return nil, err
	}
	defer unlock()
	raw, err := ffiSecondaryIndexRecords(handle, append([]byte(nil), term...))
	if err != nil {
		return nil, err
	}
	return decodeIndexedSourceRecords(raw)
}

func decodeIndexedVersionFrom(d *byteDecoder) (IndexedVersion, error) {
	source, err := d.readByteArray()
	if err != nil {
		return IndexedVersion{}, err
	}
	catalog, _, err := d.readOptionalByteArray()
	if err != nil {
		return IndexedVersion{}, err
	}
	count, err := d.readUint64()
	if err != nil {
		return IndexedVersion{}, err
	}
	return IndexedVersion{source, catalog, count}, nil
}

func decodeIndexedVersion(raw []byte) (IndexedVersion, error) {
	d := byteDecoder{data: raw}
	value, err := decodeIndexedVersionFrom(&d)
	if err != nil {
		return IndexedVersion{}, err
	}
	return value, d.done()
}

func decodeIndexedUpdate(raw []byte) (IndexedUpdate, error) {
	d := byteDecoder{data: raw}
	kind, err := d.readInt32()
	if err != nil || kind < int32(IndexedUpdateApplied) || kind > int32(IndexedUpdateConflict) {
		if err == nil {
			err = errors.New("invalid indexed update kind")
		}
		return IndexedUpdate{}, err
	}
	previous, _, err := d.readOptionalByteArray()
	if err != nil {
		return IndexedUpdate{}, err
	}
	present, err := d.readByte()
	if err != nil {
		return IndexedUpdate{}, err
	}
	var current *IndexedVersion
	if present != 0 {
		value, err := decodeIndexedVersionFrom(&d)
		if err != nil {
			return IndexedUpdate{}, err
		}
		current = &value
	}
	return IndexedUpdate{IndexedUpdateKind(kind), previous, current}, d.done()
}

func encodeIndexedSnapshotID(id IndexedSnapshotID) []byte {
	var out bytes.Buffer
	encodeByteArrayInto(&out, id.SourceVersion)
	encodeByteArrayInto(&out, id.CatalogVersion)
	return out.Bytes()
}

func decodeIndexedSnapshotID(raw []byte) (IndexedSnapshotID, error) {
	d := byteDecoder{data: raw}
	source, err := d.readByteArray()
	if err != nil {
		return IndexedSnapshotID{}, err
	}
	catalog, err := d.readByteArray()
	if err != nil {
		return IndexedSnapshotID{}, err
	}
	return IndexedSnapshotID{source, catalog}, d.done()
}
func decodeIndexBuildResult(raw []byte) (IndexBuildResult, error) {
	d := byteDecoder{data: raw}
	source, err := d.readByteArray()
	if err != nil {
		return IndexBuildResult{}, err
	}
	index, err := d.readByteArray()
	if err != nil {
		return IndexBuildResult{}, err
	}
	catalog, err := d.readByteArray()
	if err != nil {
		return IndexBuildResult{}, err
	}
	generation, err := d.readUint64()
	if err != nil {
		return IndexBuildResult{}, err
	}
	entries, err := d.readUint64()
	if err != nil {
		return IndexBuildResult{}, err
	}
	attempts, err := d.readUint64()
	if err != nil {
		return IndexBuildResult{}, err
	}
	activated, err := d.readBool()
	if err != nil {
		return IndexBuildResult{}, err
	}
	return IndexBuildResult{source, index, catalog, generation, entries, attempts, activated}, d.done()
}

func decodeActiveIndexHealth(d *byteDecoder) (ActiveIndexHealth, error) {
	var value ActiveIndexHealth
	var err error
	if value.Name, err = d.readByteArray(); err != nil {
		return value, err
	}
	if value.Generation, err = d.readUint64(); err != nil {
		return value, err
	}
	if value.Fingerprint, err = d.readByteArray(); err != nil {
		return value, err
	}
	projection, err := d.readInt32()
	if err != nil || projection < int32(IndexProjectionKeysOnly) || projection > int32(IndexProjectionAll) {
		if err == nil {
			err = errors.New("invalid index projection")
		}
		return value, err
	}
	value.Projection = IndexProjection(projection)
	if value.IndexMapID, err = d.readByteArray(); err != nil {
		return value, err
	}
	value.IndexVersion, err = d.readByteArray()
	return value, err
}

func decodeIndexedMapHealth(raw []byte) (IndexedMapHealth, error) {
	d := byteDecoder{data: raw}
	var value IndexedMapHealth
	var err error
	if value.SourceMapID, err = d.readByteArray(); err != nil {
		return value, err
	}
	if value.SourceVersion, _, err = d.readOptionalByteArray(); err != nil {
		return value, err
	}
	if value.CatalogVersion, _, err = d.readOptionalByteArray(); err != nil {
		return value, err
	}
	count, err := d.readInt32()
	if err != nil || count < 0 {
		if err == nil {
			err = errors.New("negative active index count")
		}
		return value, err
	}
	value.ActiveIndexes = make([]ActiveIndexHealth, 0, count)
	for range count {
		index, err := decodeActiveIndexHealth(&d)
		if err != nil {
			return value, err
		}
		value.ActiveIndexes = append(value.ActiveIndexes, index)
	}
	if value.SupportsTransactions, err = d.readBool(); err != nil {
		return value, err
	}
	return value, d.done()
}

func decodeIndexVerificationFrom(d *byteDecoder) (IndexVerification, error) {
	var value IndexVerification
	var err error
	if value.Name, err = d.readByteArray(); err != nil {
		return value, err
	}
	if value.SourceVersion, err = d.readByteArray(); err != nil {
		return value, err
	}
	if value.ExpectedIndexVersion, err = d.readByteArray(); err != nil {
		return value, err
	}
	if value.ActualIndexVersion, err = d.readByteArray(); err != nil {
		return value, err
	}
	if value.ExpectedEntries, err = d.readUint64(); err != nil {
		return value, err
	}
	if value.ActualEntries, err = d.readUint64(); err != nil {
		return value, err
	}
	if value.SemanticDifferences, err = d.readUint64(); err != nil {
		return value, err
	}
	if value.Valid, err = d.readBool(); err != nil {
		return value, err
	}
	value.Canonical, err = d.readBool()
	return value, err
}

func decodeIndexVerification(raw []byte) (IndexVerification, error) {
	d := byteDecoder{data: raw}
	value, err := decodeIndexVerificationFrom(&d)
	if err != nil {
		return IndexVerification{}, err
	}
	return value, d.done()
}

func decodeIndexVerifications(raw []byte) ([]IndexVerification, error) {
	d := byteDecoder{data: raw}
	count, err := d.readInt32()
	if err != nil || count < 0 {
		if err == nil {
			err = errors.New("negative index verification count")
		}
		return nil, err
	}
	values := make([]IndexVerification, 0, count)
	for range count {
		value, err := decodeIndexVerificationFrom(&d)
		if err != nil {
			return nil, err
		}
		values = append(values, value)
	}
	return values, d.done()
}

func decodeIndexedMapMetrics(raw []byte) (IndexedMapMetrics, error) {
	d := byteDecoder{data: raw}
	fields := []*uint64{
		new(uint64), new(uint64), new(uint64), new(uint64), new(uint64), new(uint64), new(uint64),
		new(uint64), new(uint64), new(uint64), new(uint64), new(uint64), new(uint64), new(uint64),
	}
	for _, field := range fields {
		value, err := d.readUint64()
		if err != nil {
			return IndexedMapMetrics{}, err
		}
		*field = value
	}
	result := IndexedMapMetrics{
		NormalizedSourceMutations: *fields[0], RecordsExtracted: *fields[1], TermsEmitted: *fields[2],
		ProjectedBytes: *fields[3], PhysicalUpserts: *fields[4], PhysicalDeletes: *fields[5],
		UnchangedEmissionsSkipped: *fields[6], SourceNodesWritten: *fields[7], IndexNodesWritten: *fields[8],
		CatalogNodesWritten: *fields[9], Retries: *fields[10], BuildAttempts: *fields[11],
		VerificationOutcomes: *fields[12], RetainedRoots: *fields[13],
	}
	return result, d.done()
}

func decodeIndexedRetention(raw []byte) (IndexedRetention, error) {
	d := byteDecoder{data: raw}
	var result IndexedRetention
	var err error
	if result.RetainedSourceVersions, err = d.readByteArraySequence(); err != nil {
		return result, err
	}
	if result.RemovedSourceVersions, err = d.readByteArraySequence(); err != nil {
		return result, err
	}
	if result.RetainedIndexVersions, err = d.readByteArraySequence(); err != nil {
		return result, err
	}
	if result.RemovedIndexVersions, err = d.readByteArraySequence(); err != nil {
		return result, err
	}
	if result.RemovedCatalogVersions, err = d.readByteArraySequence(); err != nil {
		return result, err
	}
	if result.RemovedCheckpointRecords, err = d.readUint64(); err != nil {
		return result, err
	}
	if result.RemovedNamedRoots, err = d.readByteArraySequence(); err != nil {
		return result, err
	}
	return result, d.done()
}

func decodeIndexMatch(d *byteDecoder) (IndexMatch, error) {
	term, err := d.readByteArray()
	if err != nil {
		return IndexMatch{}, err
	}
	key, err := d.readByteArray()
	if err != nil {
		return IndexMatch{}, err
	}
	projection, _, err := d.readOptionalByteArray()
	if err != nil {
		return IndexMatch{}, err
	}
	return IndexMatch{term, key, projection}, nil
}

func decodeIndexMatchesFrom(d *byteDecoder) ([]IndexMatch, error) {
	count, err := d.readInt32()
	if err != nil || count < 0 {
		if err == nil {
			err = errors.New("negative index match count")
		}
		return nil, err
	}
	rows := make([]IndexMatch, 0, count)
	for range count {
		row, err := decodeIndexMatch(d)
		if err != nil {
			return nil, err
		}
		rows = append(rows, row)
	}
	return rows, nil
}

func decodeIndexMatches(raw []byte) ([]IndexMatch, error) {
	d := byteDecoder{data: raw}
	rows, err := decodeIndexMatchesFrom(&d)
	if err != nil {
		return nil, err
	}
	return rows, d.done()
}

func decodeIndexPage(raw []byte) (IndexPage, error) {
	d := byteDecoder{data: raw}
	matches, err := decodeIndexMatchesFrom(&d)
	if err != nil {
		return IndexPage{}, err
	}
	cursor, _, err := d.readOptionalByteArray()
	if err != nil {
		return IndexPage{}, err
	}
	return IndexPage{matches, cursor}, d.done()
}

func decodeIndexedSourceRecords(raw []byte) ([]IndexedSourceRecord, error) {
	d := byteDecoder{data: raw}
	count, err := d.readInt32()
	if err != nil {
		return nil, err
	}
	if count < 0 {
		return nil, errors.New("negative indexed source record count")
	}
	rows := make([]IndexedSourceRecord, 0, count)
	for range count {
		term, err := d.readByteArray()
		if err != nil {
			return nil, err
		}
		key, err := d.readByteArray()
		if err != nil {
			return nil, err
		}
		projection, _, err := d.readOptionalByteArray()
		if err != nil {
			return nil, err
		}
		value, err := d.readByteArray()
		if err != nil {
			return nil, err
		}
		rows = append(rows, IndexedSourceRecord{term, key, projection, value})
	}
	return rows, d.done()
}
