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
