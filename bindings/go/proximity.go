package prolly

import (
	"bytes"
	"context"
	"errors"
	"math"
	"runtime"
	"sync"
	"sync/atomic"
)

type ProximityRecord struct {
	Key    []byte
	Vector []float32
	Value  []byte
}

type ExactProximityRecord struct {
	Vector []float32
	Value  []byte
}

// ProximityMembershipProof is an opaque, portable proof produced and verified
// by the Rust core. Keeping its UniFFI representation opaque avoids a second
// allocation-heavy public wire model while preserving cross-language proof
// semantics.
type ProximityMembershipProof struct {
	encoded []byte
}

type ProximityMembershipVerification struct {
	Descriptor []byte
	Key        []byte
	Record     *ExactProximityRecord
}

type ProximityVerification struct {
	RecordCount            uint64
	ProximityNodeCount     uint64
	ExternalVectorCount    uint64
	QuantizedNodeCount     uint64
	ScalarQuantizerCount   uint64
	OverflowPageCount      uint64
	OverflowDirectoryCount uint64
	MaximumLevel           uint8
	MaximumNodeBytes       uint64
	DistanceChecks         uint64
}

type SearchRequest struct {
	Query []float32
	K     uint32
}

func ExactSearch(query []float32, k uint32) SearchRequest {
	return SearchRequest{Query: append([]float32(nil), query...), K: k}
}

type Neighbor struct {
	Key      []byte
	Value    []byte
	Distance float64
	Rank     uint32
}
type SearchResult struct {
	Neighbors  []Neighbor
	Completion string
	Backend    string
}

type ProximityMap struct {
	handle uint64
	fast   uint64
	closed atomic.Bool
	mu     sync.RWMutex
}

func (e *Engine) BuildProximity(dimensions uint32, records []ProximityRecord) (*ProximityMap, error) {
	if dimensions == 0 {
		return nil, errors.New("proximity dimensions must be positive")
	}
	config, err := ffiDefaultProximityConfig(dimensions)
	if err != nil {
		return nil, err
	}
	encoded, err := encodeProximityRecords(dimensions, records)
	if err != nil {
		return nil, err
	}
	handle, err := ffiEngineBuildProximity(e, config, encoded)
	if err != nil {
		return nil, err
	}
	fast, err := ffiProximityFastHandle(handle)
	if err != nil {
		ffiFreeProximity(handle)
		return nil, err
	}
	result := &ProximityMap{handle: handle, fast: fast}
	runtime.SetFinalizer(result, (*ProximityMap).Close)
	return result, nil
}

func encodeProximityRecords(dimensions uint32, records []ProximityRecord) ([]byte, error) {
	var out bytes.Buffer
	writeI32(&out, int32(len(records)))
	for _, record := range records {
		if len(record.Vector) != int(dimensions) {
			return nil, errors.New("proximity vector dimension mismatch")
		}
		encodeByteArrayInto(&out, record.Key)
		writeI32(&out, int32(len(record.Vector)))
		for _, value := range record.Vector {
			writeU32(&out, math.Float32bits(value))
		}
		encodeByteArrayInto(&out, record.Value)
	}
	return out.Bytes(), nil
}

func (m *ProximityMap) Close() {
	if m == nil || m.closed.Swap(true) {
		return
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	runtime.SetFinalizer(m, nil)
	if m.handle != 0 {
		ffiFreeProximity(m.handle)
		m.handle = 0
		m.fast = 0
	}
}
func (m *ProximityMap) withHandle() (uint64, uint64, func(), error) {
	if m == nil || m.closed.Load() {
		return 0, 0, nil, errors.New("proximity map is closed")
	}
	m.mu.RLock()
	if m.closed.Load() || m.handle == 0 || m.fast == 0 {
		m.mu.RUnlock()
		return 0, 0, nil, errors.New("proximity map is closed")
	}
	return m.handle, m.fast, m.mu.RUnlock, nil
}

func (m *ProximityMap) Descriptor() ([]byte, error) {
	handle, _, unlock, err := m.withHandle()
	if err != nil {
		return nil, err
	}
	defer unlock()
	raw, err := ffiProximityDescriptor(handle)
	if err != nil {
		return nil, err
	}
	d := byteDecoder{data: raw}
	descriptor, err := d.readByteArray()
	if err != nil {
		return nil, err
	}
	return descriptor, d.done()
}

func (m *ProximityMap) Count() (uint64, error) {
	handle, _, unlock, err := m.withHandle()
	if err != nil {
		return 0, err
	}
	defer unlock()
	return ffiProximityCount(handle)
}

func (m *ProximityMap) Contains(key []byte) (bool, error) {
	handle, _, unlock, err := m.withHandle()
	if err != nil {
		return false, err
	}
	defer unlock()
	return ffiProximityContains(handle, append([]byte(nil), key...))
}

func (m *ProximityMap) Get(key []byte) (ExactProximityRecord, bool, error) {
	handle, _, unlock, err := m.withHandle()
	if err != nil {
		return ExactProximityRecord{}, false, err
	}
	defer unlock()
	raw, err := ffiProximityGet(handle, append([]byte(nil), key...))
	if err != nil {
		return ExactProximityRecord{}, false, err
	}
	d := byteDecoder{data: raw}
	record, ok, err := decodeOptionalExactProximityRecord(&d)
	if err != nil {
		return ExactProximityRecord{}, false, err
	}
	if err := d.done(); err != nil {
		return ExactProximityRecord{}, false, err
	}
	return record, ok, nil
}

func (m *ProximityMap) ProveMembership(key []byte) (ProximityMembershipProof, error) {
	handle, _, unlock, err := m.withHandle()
	if err != nil {
		return ProximityMembershipProof{}, err
	}
	defer unlock()
	raw, err := ffiProximityProveMembership(handle, append([]byte(nil), key...))
	if err != nil {
		return ProximityMembershipProof{}, err
	}
	return ProximityMembershipProof{encoded: raw}, nil
}

func VerifyProximityMembershipProof(proof ProximityMembershipProof, expectedDescriptor []byte) (ProximityMembershipVerification, error) {
	if len(proof.encoded) == 0 {
		return ProximityMembershipVerification{}, errors.New("empty proximity membership proof")
	}
	raw, err := ffiVerifyProximityMembershipProof(proof.encoded, append([]byte(nil), expectedDescriptor...))
	if err != nil {
		return ProximityMembershipVerification{}, err
	}
	return decodeProximityMembershipVerification(raw)
}

func (m *ProximityMap) Verify() (ProximityVerification, error) {
	handle, _, unlock, err := m.withHandle()
	if err != nil {
		return ProximityVerification{}, err
	}
	defer unlock()
	raw, err := ffiProximityVerify(handle)
	if err != nil {
		return ProximityVerification{}, err
	}
	return decodeProximityVerification(raw)
}

func (m *ProximityMap) ClearContentCache() error {
	handle, _, unlock, err := m.withHandle()
	if err != nil {
		return err
	}
	defer unlock()
	return ffiProximityClearContentCache(handle)
}

type ProximitySession struct {
	handle uint64
	fast   uint64
	closed atomic.Bool
	mu     sync.RWMutex
}

func (m *ProximityMap) Read() (*ProximitySession, error) {
	handle, _, unlock, err := m.withHandle()
	if err != nil {
		return nil, err
	}
	defer unlock()
	clone, err := ffiCloneProximity(handle)
	if err != nil {
		return nil, err
	}
	fast, err := ffiProximityFastHandle(clone)
	if err != nil {
		ffiFreeProximity(clone)
		return nil, err
	}
	session := &ProximitySession{handle: clone, fast: fast}
	runtime.SetFinalizer(session, (*ProximitySession).Close)
	return session, nil
}
func (s *ProximitySession) Close() {
	if s == nil || s.closed.Swap(true) {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	runtime.SetFinalizer(s, nil)
	if s.handle != 0 {
		ffiFreeProximity(s.handle)
		s.handle = 0
		s.fast = 0
	}
}
func (s *ProximitySession) withFast() (uint64, func(), error) {
	if s == nil || s.closed.Load() {
		return 0, nil, errors.New("proximity session is closed")
	}
	s.mu.RLock()
	if s.closed.Load() || s.fast == 0 {
		s.mu.RUnlock()
		return 0, nil, errors.New("proximity session is closed")
	}
	return s.fast, s.mu.RUnlock, nil
}

func (s *ProximitySession) Search(ctx context.Context, request SearchRequest) (SearchResult, error) {
	var result SearchResult
	err := s.WithSearchView(ctx, request, func(rows []NeighborView) error {
		result.Neighbors = make([]Neighbor, 0, len(rows))
		for _, row := range rows {
			key, err := row.Key.Copy()
			if err != nil {
				return err
			}
			var value []byte
			if row.Value != nil {
				value, err = row.Value.Copy()
				if err != nil {
					return err
				}
			}
			result.Neighbors = append(result.Neighbors, Neighbor{key, value, row.Distance, row.Rank})
		}
		return nil
	})
	if err != nil {
		return SearchResult{}, err
	}
	result.Completion = "exact"
	result.Backend = "native"
	return result, nil
}

func (s *ProximitySession) WithSearchView(ctx context.Context, request SearchRequest, visit func([]NeighborView) error) error {
	if ctx == nil {
		ctx = context.Background()
	}
	if visit == nil {
		return errors.New("nil proximity view visitor")
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if len(request.Query) == 0 || request.K == 0 {
		return errors.New("proximity query and k must be non-empty")
	}
	fast, unlock, err := s.withFast()
	if err != nil {
		return err
	}
	defer unlock()
	query := append([]float32(nil), request.Query...)
	page, err := ffiProximitySearch(fast, query, request.K)
	if err != nil {
		return err
	}
	defer page.Close()
	if err := ctx.Err(); err != nil {
		return err
	}
	scope := &viewScope{}
	defer scope.expired.Store(true)
	rows, err := decodeNeighborViews(page.data, scope)
	if err != nil {
		return err
	}
	return visit(rows)
}

func decodeFloat32Sequence(d *byteDecoder) ([]float32, error) {
	count, err := d.readInt32()
	if err != nil {
		return nil, err
	}
	if count < 0 {
		return nil, errors.New("negative proximity vector length")
	}
	values := make([]float32, 0, count)
	for range count {
		bits, err := d.readUint32()
		if err != nil {
			return nil, err
		}
		values = append(values, math.Float32frombits(bits))
	}
	return values, nil
}

func decodeOptionalExactProximityRecord(d *byteDecoder) (ExactProximityRecord, bool, error) {
	present, err := d.readByte()
	if err != nil {
		return ExactProximityRecord{}, false, err
	}
	if present == 0 {
		return ExactProximityRecord{}, false, nil
	}
	vector, err := decodeFloat32Sequence(d)
	if err != nil {
		return ExactProximityRecord{}, false, err
	}
	value, err := d.readByteArray()
	if err != nil {
		return ExactProximityRecord{}, false, err
	}
	return ExactProximityRecord{Vector: vector, Value: value}, true, nil
}

func decodeProximityMembershipVerification(raw []byte) (ProximityMembershipVerification, error) {
	d := byteDecoder{data: raw}
	var result ProximityMembershipVerification
	var err error
	if result.Descriptor, err = d.readByteArray(); err != nil {
		return result, err
	}
	if result.Key, err = d.readByteArray(); err != nil {
		return result, err
	}
	record, ok, err := decodeOptionalExactProximityRecord(&d)
	if err != nil {
		return result, err
	}
	if ok {
		result.Record = &record
	}
	return result, d.done()
}

func decodeProximityVerification(raw []byte) (ProximityVerification, error) {
	d := byteDecoder{data: raw}
	var result ProximityVerification
	fields := []*uint64{
		&result.RecordCount, &result.ProximityNodeCount, &result.ExternalVectorCount,
		&result.QuantizedNodeCount, &result.ScalarQuantizerCount, &result.OverflowPageCount,
		&result.OverflowDirectoryCount,
	}
	for _, field := range fields {
		value, err := d.readUint64()
		if err != nil {
			return ProximityVerification{}, err
		}
		*field = value
	}
	level, err := d.readByte()
	if err != nil {
		return ProximityVerification{}, err
	}
	result.MaximumLevel = level
	if result.MaximumNodeBytes, err = d.readUint64(); err != nil {
		return ProximityVerification{}, err
	}
	if result.DistanceChecks, err = d.readUint64(); err != nil {
		return ProximityVerification{}, err
	}
	return result, d.done()
}
