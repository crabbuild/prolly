package prolly

import (
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

type VersionedMap struct {
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
	result := &VersionedMap{handle: handle}
	runtime.SetFinalizer(result, (*VersionedMap).Close)
	return result, nil
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
