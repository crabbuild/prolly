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

func decodePortableMapVersion(raw []byte) (MapVersion, error) {
	decoder := byteDecoder{data: raw}
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
	return MapVersion{ID: id, Tree: tree, CreatedAtMillis: created, IsHead: head}, decoder.done()
}
