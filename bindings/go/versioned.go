package prolly

import (
	"bytes"
	"errors"
	"runtime"
	"sync"
	"sync/atomic"
)

// NewMemoryEngine constructs the hard-cutover application engine.
func NewMemoryEngine(config Config) (*Engine, error) { return Memory(config) }

type MapVersion struct {
	ID              []byte
	Tree            Tree
	CreatedAtMillis *uint64
	IsHead          bool
}

type MapUpdateKind string

const (
	MapUpdateApplied   MapUpdateKind = "applied"
	MapUpdateUnchanged MapUpdateKind = "unchanged"
	MapUpdateConflict  MapUpdateKind = "conflict"
)

type MapUpdate struct {
	Kind     MapUpdateKind
	Previous []byte
	Current  *MapVersion
}

type VersionPrune struct {
	Retained [][]byte
	Removed  [][]byte
}

type VersionedMap struct {
	engine *Engine
	handle uint64
	closed atomic.Bool
	mu     sync.RWMutex
}

type CatalogVerification struct {
	Head                                         []byte
	VersionCount, ReachableNodes, ReachableBytes uint64
}

func (e *Engine) VersionedMap(id []byte) (*VersionedMap, error) {
	handle, err := ffiEngineVersionedMap(e, append([]byte(nil), id...))
	if err != nil {
		return nil, err
	}
	result := &VersionedMap{engine: e, handle: handle}
	runtime.SetFinalizer(result, (*VersionedMap).Close)
	return result, nil
}

// MapComparison pins two cataloged versions so later head changes cannot alter
// the comparison inputs.
type MapComparison struct {
	engine *Engine
	base   MapVersion
	target MapVersion
	closed atomic.Bool
}

type MapChangeEvent struct {
	Previous []byte
	Current  MapVersion
	Diffs    []Diff
}

// MapSubscription is a resumable explicitly-polled change stream.
type MapSubscription struct {
	engine   *Engine
	mapID    []byte
	lastSeen []byte
	closed   atomic.Bool
	mu       sync.Mutex
}

func (m *VersionedMap) Subscribe() (*MapSubscription, error) {
	id, err := m.ID()
	if err != nil {
		return nil, err
	}
	lastSeen, ok, err := m.HeadID()
	if err != nil {
		return nil, err
	}
	if !ok {
		lastSeen = nil
	}
	return &MapSubscription{engine: m.engine, mapID: id, lastSeen: bytes.Clone(lastSeen)}, nil
}

func (m *VersionedMap) SubscribeFrom(lastSeen []byte) (*MapSubscription, error) {
	id, err := m.ID()
	if err != nil {
		return nil, err
	}
	return &MapSubscription{engine: m.engine, mapID: id, lastSeen: bytes.Clone(lastSeen)}, nil
}

func (s *MapSubscription) Close() {
	if s != nil {
		s.closed.Store(true)
	}
}

func (s *MapSubscription) LastSeen() ([]byte, bool, error) {
	if s == nil || s.closed.Load() {
		return nil, false, errors.New("map subscription is closed")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.lastSeen == nil {
		return nil, false, nil
	}
	return bytes.Clone(s.lastSeen), true, nil
}

func (s *MapSubscription) Poll() (*MapChangeEvent, error) {
	if s == nil || s.closed.Load() {
		return nil, errors.New("map subscription is closed")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed.Load() {
		return nil, errors.New("map subscription is closed")
	}
	mapHandle, err := s.engine.VersionedMap(s.mapID)
	if err != nil {
		return nil, err
	}
	defer mapHandle.Close()
	current, err := mapHandle.Head()
	if err != nil || current == nil {
		return nil, err
	}
	if bytes.Equal(s.lastSeen, current.ID) {
		return nil, nil
	}
	var previousTree Tree
	if s.lastSeen == nil {
		previousTree, err = s.engine.Create()
	} else {
		previous, loadErr := mapHandle.Version(s.lastSeen)
		if loadErr != nil {
			return nil, loadErr
		}
		if previous == nil {
			return nil, errors.New("subscription resume version was pruned")
		}
		previousTree = previous.Tree
	}
	if err != nil {
		return nil, err
	}
	diffs, err := s.engine.Diff(previousTree, current.Tree)
	if err != nil {
		return nil, err
	}
	previous := bytes.Clone(s.lastSeen)
	s.lastSeen = bytes.Clone(current.ID)
	return &MapChangeEvent{Previous: previous, Current: *current, Diffs: diffs}, nil
}

func (m *VersionedMap) Compare(baseID, targetID []byte) (*MapComparison, error) {
	base, err := m.Version(append([]byte(nil), baseID...))
	if err != nil {
		return nil, err
	}
	if base == nil {
		return nil, errors.New("base map version is not cataloged")
	}
	target, err := m.Version(append([]byte(nil), targetID...))
	if err != nil {
		return nil, err
	}
	if target == nil {
		return nil, errors.New("target map version is not cataloged")
	}
	return &MapComparison{engine: m.engine, base: *base, target: *target}, nil
}

func (m *VersionedMap) CompareToHead(baseID []byte) (*MapComparison, error) {
	head, err := m.Head()
	if err != nil {
		return nil, err
	}
	if head == nil {
		return nil, errors.New("versioned map has not been initialized")
	}
	return m.Compare(baseID, head.ID)
}

func (c *MapComparison) Close() {
	if c != nil {
		c.closed.Store(true)
	}
}

func (c *MapComparison) open() error {
	if c == nil || c.closed.Load() {
		return errors.New("map comparison is closed")
	}
	return nil
}

func (c *MapComparison) Base() (MapVersion, error) {
	if err := c.open(); err != nil {
		return MapVersion{}, err
	}
	return c.base, nil
}

func (c *MapComparison) Target() (MapVersion, error) {
	if err := c.open(); err != nil {
		return MapVersion{}, err
	}
	return c.target, nil
}

func (c *MapComparison) Diff() ([]Diff, error) {
	if err := c.open(); err != nil {
		return nil, err
	}
	return c.engine.Diff(c.base.Tree, c.target.Tree)
}

func (c *MapComparison) DiffPage(cursor *RangeCursor, end []byte, limit uint64) (DiffPage, error) {
	if err := c.open(); err != nil {
		return DiffPage{}, err
	}
	return c.engine.DiffPage(c.base.Tree, c.target.Tree, cursor, append([]byte(nil), end...), limit)
}

func (m *VersionedMap) Close() {
	if m == nil || m.closed.Swap(true) {
		return
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	runtime.SetFinalizer(m, nil)
	if m.handle != 0 {
		ffiFreeVersioned(m.handle)
		m.handle = 0
	}
}

func (m *VersionedMap) withHandle() (uint64, func(), error) {
	if m == nil || m.closed.Load() {
		return 0, nil, errors.New("versioned map is closed")
	}
	m.mu.RLock()
	if m.closed.Load() || m.handle == 0 {
		m.mu.RUnlock()
		return 0, nil, errors.New("versioned map is closed")
	}
	return m.handle, m.mu.RUnlock, nil
}

// ID returns an owned copy of the application map identifier.
func (m *VersionedMap) ID() ([]byte, error) {
	handle, unlock, err := m.withHandle()
	if err != nil {
		return nil, err
	}
	defer unlock()
	raw, err := ffiVersionedID(handle)
	if err != nil {
		return nil, err
	}
	d := byteDecoder{data: raw}
	value, err := d.readByteArray()
	if err != nil {
		return nil, err
	}
	return value, d.done()
}

func (m *VersionedMap) IsInitialized() (bool, error) {
	handle, unlock, err := m.withHandle()
	if err != nil {
		return false, err
	}
	defer unlock()
	return ffiVersionedIsInitialized(handle)
}

func (m *VersionedMap) Initialize() (MapVersion, error) {
	handle, unlock, err := m.withHandle()
	if err != nil {
		return MapVersion{}, err
	}
	defer unlock()
	raw, err := ffiVersionedInitialize(handle)
	if err != nil {
		return MapVersion{}, err
	}
	return decodePortableMapVersion(raw)
}

func (m *VersionedMap) Head() (*MapVersion, error) {
	handle, unlock, err := m.withHandle()
	if err != nil {
		return nil, err
	}
	defer unlock()
	raw, err := ffiVersionedHead(handle)
	if err != nil {
		return nil, err
	}
	return decodeOptionalPortableMapVersion(raw)
}

func (m *VersionedMap) HeadID() ([]byte, bool, error) {
	handle, unlock, err := m.withHandle()
	if err != nil {
		return nil, false, err
	}
	defer unlock()
	raw, err := ffiVersionedHeadID(handle)
	if err != nil {
		return nil, false, err
	}
	d := byteDecoder{data: raw}
	value, ok, err := d.readOptionalByteArray()
	if err != nil {
		return nil, false, err
	}
	return value, ok, d.done()
}

func (m *VersionedMap) Version(id []byte) (*MapVersion, error) {
	handle, unlock, err := m.withHandle()
	if err != nil {
		return nil, err
	}
	defer unlock()
	raw, err := ffiVersionedVersion(handle, append([]byte(nil), id...))
	if err != nil {
		return nil, err
	}
	return decodeOptionalPortableMapVersion(raw)
}

func (m *VersionedMap) Versions() ([]MapVersion, error) {
	handle, unlock, err := m.withHandle()
	if err != nil {
		return nil, err
	}
	defer unlock()
	raw, err := ffiVersionedVersions(handle)
	if err != nil {
		return nil, err
	}
	return decodePortableMapVersions(raw)
}

func (m *VersionedMap) Get(key []byte) ([]byte, bool, error) {
	handle, unlock, err := m.withHandle()
	if err != nil {
		return nil, false, err
	}
	defer unlock()
	raw, err := ffiVersionedGet(handle, append([]byte(nil), key...))
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

func (m *VersionedMap) ContainsKey(key []byte) (bool, error) {
	handle, unlock, err := m.withHandle()
	if err != nil {
		return false, err
	}
	defer unlock()
	return ffiVersionedContainsKey(handle, append([]byte(nil), key...))
}

func (m *VersionedMap) GetMany(keys [][]byte) ([][]byte, error) {
	handle, unlock, err := m.withHandle()
	if err != nil {
		return nil, err
	}
	defer unlock()
	owned := cloneByteSlices(keys)
	raw, err := ffiVersionedGetMany(handle, owned)
	if err != nil {
		return nil, err
	}
	values, _, err := decodeOptionalByteArraySequence(raw)
	return values, err
}

func (m *VersionedMap) GetAt(id, key []byte) ([]byte, bool, error) {
	handle, unlock, err := m.withHandle()
	if err != nil {
		return nil, false, err
	}
	defer unlock()
	raw, err := ffiVersionedGetAt(handle, append([]byte(nil), id...), append([]byte(nil), key...))
	if err != nil {
		return nil, false, err
	}
	d := byteDecoder{data: raw}
	value, ok, err := d.readOptionalByteArray()
	if err != nil {
		return nil, false, err
	}
	return value, ok, d.done()
}

func (m *VersionedMap) GetManyAt(id []byte, keys [][]byte) ([][]byte, error) {
	handle, unlock, err := m.withHandle()
	if err != nil {
		return nil, err
	}
	defer unlock()
	raw, err := ffiVersionedGetManyAt(handle, append([]byte(nil), id...), cloneByteSlices(keys))
	if err != nil {
		return nil, err
	}
	values, _, err := decodeOptionalByteArraySequence(raw)
	return values, err
}

func (m *VersionedMap) Put(key, value []byte) (MapVersion, error) {
	handle, unlock, err := m.withHandle()
	if err != nil {
		return MapVersion{}, err
	}
	defer unlock()
	raw, err := ffiVersionedPut(handle, append([]byte(nil), key...), append([]byte(nil), value...))
	if err != nil {
		return MapVersion{}, err
	}
	return decodePortableMapVersion(raw)
}

func (m *VersionedMap) Apply(mutations []Mutation) (MapVersion, error) {
	handle, unlock, err := m.withHandle()
	if err != nil {
		return MapVersion{}, err
	}
	defer unlock()
	encoded, err := encodeMutations(cloneMutations(mutations))
	if err != nil {
		return MapVersion{}, err
	}
	raw, err := ffiVersionedApply(handle, encoded)
	if err != nil {
		return MapVersion{}, err
	}
	return decodePortableMapVersion(raw)
}

func (m *VersionedMap) ApplyIf(expected []byte, mutations []Mutation) (MapUpdate, error) {
	handle, unlock, err := m.withHandle()
	if err != nil {
		return MapUpdate{}, err
	}
	defer unlock()
	encoded, err := encodeMutations(cloneMutations(mutations))
	if err != nil {
		return MapUpdate{}, err
	}
	raw, err := ffiVersionedApplyIf(handle, append([]byte(nil), expected...), encoded)
	if err != nil {
		return MapUpdate{}, err
	}
	return decodePortableMapUpdate(raw)
}

func (m *VersionedMap) PutIf(expected, key, value []byte) (MapUpdate, error) {
	handle, unlock, err := m.withHandle()
	if err != nil {
		return MapUpdate{}, err
	}
	defer unlock()
	raw, err := ffiVersionedPutIf(handle, append([]byte(nil), expected...), append([]byte(nil), key...), append([]byte(nil), value...))
	if err != nil {
		return MapUpdate{}, err
	}
	return decodePortableMapUpdate(raw)
}

func (m *VersionedMap) DeleteIf(expected, key []byte) (MapUpdate, error) {
	handle, unlock, err := m.withHandle()
	if err != nil {
		return MapUpdate{}, err
	}
	defer unlock()
	raw, err := ffiVersionedDeleteIf(handle, append([]byte(nil), expected...), append([]byte(nil), key...))
	if err != nil {
		return MapUpdate{}, err
	}
	return decodePortableMapUpdate(raw)
}

func (m *VersionedMap) Delete(key []byte) (MapVersion, error) {
	handle, unlock, err := m.withHandle()
	if err != nil {
		return MapVersion{}, err
	}
	defer unlock()
	raw, err := ffiVersionedDelete(handle, append([]byte(nil), key...))
	if err != nil {
		return MapVersion{}, err
	}
	return decodePortableMapVersion(raw)
}

func (m *VersionedMap) Snapshot() (*MapSnapshot, error) {
	handle, unlock, err := m.withHandle()
	if err != nil {
		return nil, err
	}
	defer unlock()
	snapshot, err := ffiVersionedSnapshot(handle)
	if err != nil {
		return nil, err
	}
	if snapshot == 0 {
		return nil, nil
	}
	return adoptMapSnapshot(snapshot), nil
}

func (m *VersionedMap) SnapshotAt(id []byte) (*MapSnapshot, error) {
	handle, unlock, err := m.withHandle()
	if err != nil {
		return nil, err
	}
	defer unlock()
	snapshot, err := ffiVersionedSnapshotAt(handle, append([]byte(nil), id...))
	if err != nil {
		return nil, err
	}
	if snapshot == 0 {
		return nil, nil
	}
	return adoptMapSnapshot(snapshot), nil
}
func (m *VersionedMap) Backup() ([]byte, error) {
	handle, unlock, err := m.withHandle()
	if err != nil {
		return nil, err
	}
	defer unlock()
	raw, err := ffiVersionedBackup(handle)
	if err != nil {
		return nil, err
	}
	d := byteDecoder{data: raw}
	value, err := d.readByteArray()
	if err != nil {
		return nil, err
	}
	return value, d.done()
}

func (m *VersionedMap) RestoreBackup(bytes []byte) (MapVersion, error) {
	handle, unlock, err := m.withHandle()
	if err != nil {
		return MapVersion{}, err
	}
	defer unlock()
	raw, err := ffiVersionedRestoreBackup(handle, append([]byte(nil), bytes...))
	if err != nil {
		return MapVersion{}, err
	}
	return decodePortableMapVersion(raw)
}

func (m *VersionedMap) KeepLast(count uint64) (VersionPrune, error) {
	handle, unlock, err := m.withHandle()
	if err != nil {
		return VersionPrune{}, err
	}
	defer unlock()
	raw, err := ffiVersionedKeepLast(handle, count)
	if err != nil {
		return VersionPrune{}, err
	}
	d := byteDecoder{data: raw}
	retained, err := d.readByteArraySequence()
	if err != nil {
		return VersionPrune{}, err
	}
	removed, err := d.readByteArraySequence()
	if err != nil {
		return VersionPrune{}, err
	}
	return VersionPrune{Retained: retained, Removed: removed}, d.done()
}
func (m *VersionedMap) VerifyCatalog() (CatalogVerification, error) {
	handle, unlock, err := m.withHandle()
	if err != nil {
		return CatalogVerification{}, err
	}
	defer unlock()
	raw, err := ffiVersionedVerifyCatalog(handle)
	if err != nil {
		return CatalogVerification{}, err
	}
	d := byteDecoder{data: raw}
	head, err := d.readByteArray()
	if err != nil {
		return CatalogVerification{}, err
	}
	count, err := d.readUint64()
	if err != nil {
		return CatalogVerification{}, err
	}
	nodes, err := d.readUint64()
	if err != nil {
		return CatalogVerification{}, err
	}
	size, err := d.readUint64()
	if err != nil {
		return CatalogVerification{}, err
	}
	return CatalogVerification{head, count, nodes, size}, d.done()
}

type MapSnapshot struct {
	handle uint64
	closed atomic.Bool
	mu     sync.RWMutex
}

func adoptMapSnapshot(handle uint64) *MapSnapshot {
	result := &MapSnapshot{handle: handle}
	runtime.SetFinalizer(result, (*MapSnapshot).Close)
	return result
}

func (s *MapSnapshot) Close() {
	if s == nil || s.closed.Swap(true) {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	runtime.SetFinalizer(s, nil)
	if s.handle != 0 {
		ffiFreeMapSnapshot(s.handle)
		s.handle = 0
	}
}
func (s *MapSnapshot) withHandle() (uint64, func(), error) {
	if s == nil || s.closed.Load() {
		return 0, nil, errors.New("map snapshot is closed")
	}
	s.mu.RLock()
	if s.closed.Load() || s.handle == 0 {
		s.mu.RUnlock()
		return 0, nil, errors.New("map snapshot is closed")
	}
	return s.handle, s.mu.RUnlock, nil
}

func (s *MapSnapshot) ID() ([]byte, error) {
	handle, unlock, err := s.withHandle()
	if err != nil {
		return nil, err
	}
	defer unlock()
	raw, err := ffiMapSnapshotID(handle)
	if err != nil {
		return nil, err
	}
	d := byteDecoder{data: raw}
	value, err := d.readByteArray()
	if err != nil {
		return nil, err
	}
	return value, d.done()
}

func (s *MapSnapshot) Version() (MapVersion, error) {
	handle, unlock, err := s.withHandle()
	if err != nil {
		return MapVersion{}, err
	}
	defer unlock()
	raw, err := ffiMapSnapshotVersion(handle)
	if err != nil {
		return MapVersion{}, err
	}
	return decodePortableMapVersion(raw)
}
func (s *MapSnapshot) Get(key []byte) ([]byte, bool, error) {
	handle, unlock, err := s.withHandle()
	if err != nil {
		return nil, false, err
	}
	defer unlock()
	raw, err := ffiMapSnapshotGet(handle, append([]byte(nil), key...))
	if err != nil {
		return nil, false, err
	}
	d := byteDecoder{data: raw}
	value, ok, err := d.readOptionalByteArray()
	if err != nil {
		return nil, false, err
	}
	return value, ok, d.done()
}
func (s *MapSnapshot) GetMany(keys [][]byte) ([][]byte, error) {
	handle, unlock, err := s.withHandle()
	if err != nil {
		return nil, err
	}
	defer unlock()
	owned := make([][]byte, len(keys))
	for index, key := range keys {
		owned[index] = append([]byte{}, key...)
	}
	raw, err := ffiMapSnapshotGetMany(handle, owned)
	if err != nil {
		return nil, err
	}
	values, _, err := decodeOptionalByteArraySequence(raw)
	return values, err
}
func (s *MapSnapshot) ContainsKey(key []byte) (bool, error) {
	handle, unlock, err := s.withHandle()
	if err != nil {
		return false, err
	}
	defer unlock()
	return ffiMapSnapshotContainsKey(handle, append([]byte{}, key...))
}
func (s *MapSnapshot) FirstEntry() (*Entry, error) {
	handle, unlock, err := s.withHandle()
	if err != nil {
		return nil, err
	}
	defer unlock()
	raw, err := ffiMapSnapshotFirstEntry(handle)
	if err != nil {
		return nil, err
	}
	return decodeOptionalEntry(raw)
}
func (s *MapSnapshot) LastEntry() (*Entry, error) {
	handle, unlock, err := s.withHandle()
	if err != nil {
		return nil, err
	}
	defer unlock()
	raw, err := ffiMapSnapshotLastEntry(handle)
	if err != nil {
		return nil, err
	}
	return decodeOptionalEntry(raw)
}
func (s *MapSnapshot) LowerBound(key []byte) (*Entry, error) {
	handle, unlock, err := s.withHandle()
	if err != nil {
		return nil, err
	}
	defer unlock()
	raw, err := ffiMapSnapshotLowerBound(handle, append([]byte{}, key...))
	if err != nil {
		return nil, err
	}
	return decodeOptionalEntry(raw)
}
func (s *MapSnapshot) UpperBound(key []byte) (*Entry, error) {
	handle, unlock, err := s.withHandle()
	if err != nil {
		return nil, err
	}
	defer unlock()
	raw, err := ffiMapSnapshotUpperBound(handle, append([]byte{}, key...))
	if err != nil {
		return nil, err
	}
	return decodeOptionalEntry(raw)
}
func (s *MapSnapshot) Range(start, end []byte) ([]Entry, error) {
	handle, unlock, err := s.withHandle()
	if err != nil {
		return nil, err
	}
	defer unlock()
	raw, err := ffiMapSnapshotRange(handle, append([]byte{}, start...), cloneOptionalBytes(end))
	if err != nil {
		return nil, err
	}
	return decodeEntries(raw)
}
func (s *MapSnapshot) Prefix(prefix []byte) ([]Entry, error) {
	handle, unlock, err := s.withHandle()
	if err != nil {
		return nil, err
	}
	defer unlock()
	raw, err := ffiMapSnapshotPrefix(handle, append([]byte{}, prefix...))
	if err != nil {
		return nil, err
	}
	return decodeEntries(raw)
}
func (s *MapSnapshot) RangePage(cursor *RangeCursor, end []byte, limit uint64) (RangePage, error) {
	handle, unlock, err := s.withHandle()
	if err != nil {
		return RangePage{}, err
	}
	defer unlock()
	raw, err := ffiMapSnapshotRangePage(handle, cloneRangeCursor(cursor), cloneOptionalBytes(end), limit)
	if err != nil {
		return RangePage{}, err
	}
	return decodeRangePage(raw)
}
func (s *MapSnapshot) PrefixPage(prefix []byte, cursor *RangeCursor, limit uint64) (RangePage, error) {
	handle, unlock, err := s.withHandle()
	if err != nil {
		return RangePage{}, err
	}
	defer unlock()
	raw, err := ffiMapSnapshotPrefixPage(handle, append([]byte{}, prefix...), cloneRangeCursor(cursor), limit)
	if err != nil {
		return RangePage{}, err
	}
	return decodeRangePage(raw)
}
func (s *MapSnapshot) ReversePage(cursor *ReverseCursor, start []byte, limit uint64) (ReversePage, error) {
	handle, unlock, err := s.withHandle()
	if err != nil {
		return ReversePage{}, err
	}
	defer unlock()
	raw, err := ffiMapSnapshotReversePage(handle, cloneReverseCursor(cursor), append([]byte{}, start...), limit)
	if err != nil {
		return ReversePage{}, err
	}
	return decodeReversePage(raw)
}
func (s *MapSnapshot) PrefixReversePage(prefix []byte, cursor *ReverseCursor, limit uint64) (ReversePage, error) {
	handle, unlock, err := s.withHandle()
	if err != nil {
		return ReversePage{}, err
	}
	defer unlock()
	raw, err := ffiMapSnapshotPrefixReversePage(handle, append([]byte{}, prefix...), cloneReverseCursor(cursor), limit)
	if err != nil {
		return ReversePage{}, err
	}
	return decodeReversePage(raw)
}

func cloneOptionalBytes(value []byte) []byte {
	if value == nil {
		return nil
	}
	return append([]byte{}, value...)
}

func cloneRangeCursor(cursor *RangeCursor) *RangeCursor {
	if cursor == nil {
		return nil
	}
	return &RangeCursor{AfterKey: cloneOptionalBytes(cursor.AfterKey)}
}

func cloneReverseCursor(cursor *ReverseCursor) *ReverseCursor {
	if cursor == nil {
		return nil
	}
	return &ReverseCursor{BeforeKey: cloneOptionalBytes(cursor.BeforeKey)}
}
func (s *MapSnapshot) ProveKey(key []byte) (KeyProof, error) {
	handle, unlock, err := s.withHandle()
	if err != nil {
		return KeyProof{}, err
	}
	defer unlock()
	raw, err := ffiMapSnapshotProveKey(handle, append([]byte(nil), key...))
	if err != nil {
		return KeyProof{}, err
	}
	return decodeKeyProof(raw)
}

// ProveKeys creates a compact proof for the requested keys. Input keys are
// copied before crossing the FFI boundary so callers may safely reuse them.
func (s *MapSnapshot) ProveKeys(keys [][]byte) (MultiKeyProof, error) {
	handle, unlock, err := s.withHandle()
	if err != nil {
		return MultiKeyProof{}, err
	}
	defer unlock()
	owned := make([][]byte, len(keys))
	for index, key := range keys {
		owned[index] = append([]byte(nil), key...)
	}
	raw, err := ffiMapSnapshotProveKeys(handle, owned)
	if err != nil {
		return MultiKeyProof{}, err
	}
	return decodeMultiKeyProof(raw)
}

// ProveRange creates a proof for the half-open interval [start, end).
func (s *MapSnapshot) ProveRange(start, end []byte) (RangeProof, error) {
	handle, unlock, err := s.withHandle()
	if err != nil {
		return RangeProof{}, err
	}
	defer unlock()
	raw, err := ffiMapSnapshotProveRange(handle, append([]byte(nil), start...), cloneOptionalBytes(end))
	if err != nil {
		return RangeProof{}, err
	}
	return decodeRangeProof(raw)
}

// ProvePrefix creates a proof covering every key with prefix.
func (s *MapSnapshot) ProvePrefix(prefix []byte) (RangeProof, error) {
	handle, unlock, err := s.withHandle()
	if err != nil {
		return RangeProof{}, err
	}
	defer unlock()
	raw, err := ffiMapSnapshotProvePrefix(handle, append([]byte(nil), prefix...))
	if err != nil {
		return RangeProof{}, err
	}
	return decodeRangeProof(raw)
}

// ProveRangePage returns a bounded page together with its independently
// verifiable proof.
func (s *MapSnapshot) ProveRangePage(cursor *RangeCursor, end []byte, limit uint64) (ProvedRangePage, error) {
	handle, unlock, err := s.withHandle()
	if err != nil {
		return ProvedRangePage{}, err
	}
	defer unlock()
	raw, err := ffiMapSnapshotProveRangePage(handle, cloneRangeCursor(cursor), cloneOptionalBytes(end), limit)
	if err != nil {
		return ProvedRangePage{}, err
	}
	return decodeProvedRangePage(raw)
}
func (s *MapSnapshot) Read() (*ReadSession, error) {
	handle, unlock, err := s.withHandle()
	if err != nil {
		return nil, err
	}
	defer unlock()
	native, err := ffiMapSnapshotRead(handle)
	if err != nil {
		return nil, err
	}
	return ffiAdoptReadSession(native)
}

func decodePortableMapVersion(raw []byte) (MapVersion, error) {
	decoder := byteDecoder{data: raw}
	value, err := readPortableMapVersion(&decoder)
	if err != nil {
		return MapVersion{}, err
	}
	return value, decoder.done()
}

func readPortableMapVersion(decoder *byteDecoder) (MapVersion, error) {
	id, err := decoder.readByteArray()
	if err != nil {
		return MapVersion{}, err
	}
	tree, err := decoder.readTree()
	if err != nil {
		return MapVersion{}, err
	}
	created, err := decoder.readOptionalUint64()
	if err != nil {
		return MapVersion{}, err
	}
	head, err := decoder.readBool()
	if err != nil {
		return MapVersion{}, err
	}
	return MapVersion{ID: id, Tree: tree, CreatedAtMillis: created, IsHead: head}, nil
}

func decodeOptionalPortableMapVersion(raw []byte) (*MapVersion, error) {
	decoder := byteDecoder{data: raw}
	present, err := decoder.readByte()
	if err != nil {
		return nil, err
	}
	if present == 0 {
		return nil, decoder.done()
	}
	value, err := readPortableMapVersion(&decoder)
	if err != nil {
		return nil, err
	}
	return &value, decoder.done()
}

func decodePortableMapVersions(raw []byte) ([]MapVersion, error) {
	decoder := byteDecoder{data: raw}
	count, err := decoder.readInt32()
	if err != nil {
		return nil, err
	}
	if count < 0 {
		return nil, errors.New("invalid version sequence length")
	}
	versions := make([]MapVersion, 0, count)
	for range count {
		version, err := readPortableMapVersion(&decoder)
		if err != nil {
			return nil, err
		}
		versions = append(versions, version)
	}
	return versions, decoder.done()
}

func decodePortableMapUpdate(raw []byte) (MapUpdate, error) {
	d := byteDecoder{data: raw}
	kind, err := d.readInt32()
	if err != nil {
		return MapUpdate{}, err
	}
	var result MapUpdate
	switch kind {
	case 1:
		result.Kind = MapUpdateApplied
	case 2:
		result.Kind = MapUpdateUnchanged
	case 3:
		result.Kind = MapUpdateConflict
	default:
		return MapUpdate{}, errors.New("invalid map update kind")
	}
	previous, present, err := d.readOptionalByteArray()
	if err != nil {
		return MapUpdate{}, err
	}
	if present {
		result.Previous = previous
	}
	presentByte, err := d.readByte()
	if err != nil {
		return MapUpdate{}, err
	}
	if presentByte != 0 {
		current, err := readPortableMapVersion(&d)
		if err != nil {
			return MapUpdate{}, err
		}
		result.Current = &current
	}
	return result, d.done()
}

func cloneByteSlices(values [][]byte) [][]byte {
	owned := make([][]byte, len(values))
	for index, value := range values {
		owned[index] = append([]byte(nil), value...)
	}
	return owned
}

func cloneMutations(values []Mutation) []Mutation {
	owned := make([]Mutation, len(values))
	for index, value := range values {
		owned[index] = Mutation{
			Kind:  value.Kind,
			Key:   append([]byte(nil), value.Key...),
			Value: append([]byte(nil), value.Value...),
		}
	}
	return owned
}
