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

type ProximityConfig struct {
	Dimensions                  uint32
	Metric                      string
	LogChunkSize                uint8
	LevelHashSeed               uint64
	MinPageBytes                uint32
	TargetPageBytes             uint32
	MaxPageBytes                uint32
	OverflowHashSeed            uint64
	InlineThresholdBytes        uint32
	ScalarQuantizationGroupSize *uint32
}

type HNSWRoutingVectorEncoding int32

const (
	HNSWRoutingVectorFullF32 HNSWRoutingVectorEncoding = 1
)

type HNSWConfig struct {
	MaxConnections        uint16
	EFConstruction        uint32
	EFSearch              uint32
	LevelBits             uint8
	OverfetchMultiplier   uint32
	Seed                  uint64
	RoutingVectorEncoding HNSWRoutingVectorEncoding
}

type HNSWBuildLimits struct {
	MaxRecords             *uint64
	MaxOwnedBytes          *uint64
	MaxDistanceEvaluations *uint64
	WorkerThreads          uint64
	MaxEncodedGraphBytes   *uint64
}

type HNSWBuildStats struct {
	Records             uint64
	DistanceEvaluations uint64
	DirectedEdges       uint64
	MaximumLevel        uint8
	OwnedBytes          uint64
	EncodedGraphBytes   uint64
}

type HNSWBuildResult struct {
	Index *HNSWIndex
	Stats HNSWBuildStats
}

func decodeHNSWConfig(raw []byte) (HNSWConfig, error) {
	d := byteDecoder{data: raw}
	high, err := d.readByte()
	if err != nil {
		return HNSWConfig{}, err
	}
	low, err := d.readByte()
	if err != nil {
		return HNSWConfig{}, err
	}
	config := HNSWConfig{MaxConnections: uint16(high)<<8 | uint16(low)}
	if config.EFConstruction, err = d.readUint32(); err != nil {
		return HNSWConfig{}, err
	}
	if config.EFSearch, err = d.readUint32(); err != nil {
		return HNSWConfig{}, err
	}
	if config.LevelBits, err = d.readByte(); err != nil {
		return HNSWConfig{}, err
	}
	if config.OverfetchMultiplier, err = d.readUint32(); err != nil {
		return HNSWConfig{}, err
	}
	if config.Seed, err = d.readUint64(); err != nil {
		return HNSWConfig{}, err
	}
	encoding, err := d.readInt32()
	if err != nil {
		return HNSWConfig{}, err
	}
	config.RoutingVectorEncoding = HNSWRoutingVectorEncoding(encoding)
	if config.RoutingVectorEncoding != HNSWRoutingVectorFullF32 {
		return HNSWConfig{}, errors.New("unknown HNSW routing-vector encoding")
	}
	return config, d.done()
}

func encodeHNSWConfig(config HNSWConfig) ([]byte, error) {
	if config.RoutingVectorEncoding != HNSWRoutingVectorFullF32 {
		return nil, errors.New("unknown HNSW routing-vector encoding")
	}
	var out bytes.Buffer
	out.WriteByte(byte(config.MaxConnections >> 8))
	out.WriteByte(byte(config.MaxConnections))
	writeU32(&out, config.EFConstruction)
	writeU32(&out, config.EFSearch)
	out.WriteByte(config.LevelBits)
	writeU32(&out, config.OverfetchMultiplier)
	writeU64(&out, config.Seed)
	writeI32(&out, int32(config.RoutingVectorEncoding))
	return out.Bytes(), nil
}

func decodeHNSWBuildLimits(raw []byte) (HNSWBuildLimits, error) {
	d := byteDecoder{data: raw}
	var result HNSWBuildLimits
	var err error
	if result.MaxRecords, err = d.readOptionalUint64(); err != nil {
		return HNSWBuildLimits{}, err
	}
	if result.MaxOwnedBytes, err = d.readOptionalUint64(); err != nil {
		return HNSWBuildLimits{}, err
	}
	if result.MaxDistanceEvaluations, err = d.readOptionalUint64(); err != nil {
		return HNSWBuildLimits{}, err
	}
	if result.WorkerThreads, err = d.readUint64(); err != nil {
		return HNSWBuildLimits{}, err
	}
	if result.MaxEncodedGraphBytes, err = d.readOptionalUint64(); err != nil {
		return HNSWBuildLimits{}, err
	}
	return result, d.done()
}

func encodeHNSWBuildLimits(limits HNSWBuildLimits) []byte {
	var out bytes.Buffer
	encodeOptionalU64(&out, limits.MaxRecords)
	encodeOptionalU64(&out, limits.MaxOwnedBytes)
	encodeOptionalU64(&out, limits.MaxDistanceEvaluations)
	writeU64(&out, limits.WorkerThreads)
	encodeOptionalU64(&out, limits.MaxEncodedGraphBytes)
	return out.Bytes()
}

func decodeHNSWBuildStats(d *byteDecoder) (HNSWBuildStats, error) {
	var result HNSWBuildStats
	var err error
	if result.Records, err = d.readUint64(); err != nil {
		return HNSWBuildStats{}, err
	}
	if result.DistanceEvaluations, err = d.readUint64(); err != nil {
		return HNSWBuildStats{}, err
	}
	if result.DirectedEdges, err = d.readUint64(); err != nil {
		return HNSWBuildStats{}, err
	}
	if result.MaximumLevel, err = d.readByte(); err != nil {
		return HNSWBuildStats{}, err
	}
	if result.OwnedBytes, err = d.readUint64(); err != nil {
		return HNSWBuildStats{}, err
	}
	if result.EncodedGraphBytes, err = d.readUint64(); err != nil {
		return HNSWBuildStats{}, err
	}
	return result, nil
}

func DefaultHNSWConfig() (HNSWConfig, error) {
	raw, err := ffiDefaultHNSWConfig()
	if err != nil {
		return HNSWConfig{}, err
	}
	return decodeHNSWConfig(raw)
}

func DefaultHNSWBuildLimits() (HNSWBuildLimits, error) {
	raw, err := ffiDefaultHNSWBuildLimits()
	if err != nil {
		return HNSWBuildLimits{}, err
	}
	return decodeHNSWBuildLimits(raw)
}

type ProximityMutation struct {
	Key    []byte
	Vector []float32
	Value  []byte
	Delete bool
}

func UpsertProximity(key []byte, vector []float32, value []byte) ProximityMutation {
	return ProximityMutation{Key: append([]byte(nil), key...), Vector: append([]float32(nil), vector...), Value: append([]byte(nil), value...)}
}

func DeleteProximity(key []byte) ProximityMutation {
	return ProximityMutation{Key: append([]byte(nil), key...), Delete: true}
}

type ProximityMutationStats struct {
	DirectoryEntriesScanned   uint64
	DirectoryNodesRead        uint64
	DirectoryNodesRebuilt     uint64
	DirectoryNodesWritten     uint64
	DirectoryNodesReused      uint64
	DirectoryLevelsRebuilt    uint64
	DirectoryRightEdgeRebuilt bool
	NodesRead                 uint64
	NodesWritten              uint64
	NodesReused               uint64
	RecordsRebuilt            uint64
	DistanceEvaluations       uint64
	FullProximityRebuild      bool
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

type ProximityStructuralProof struct {
	encoded []byte
}

type ProximityStructuralVerification struct {
	Descriptor  []byte
	ObjectCount uint64
	Summary     ProximityVerification
}

type ProximityMembershipVerification struct {
	Descriptor []byte
	Key        []byte
	Record     *ExactProximityRecord
}

type ProximitySearchClaim struct {
	Kind               string
	TerminalLowerBound *float64
}

type ProximitySearchVerification struct {
	Result         SearchResult
	Claim          ProximitySearchClaim
	ReplayedEvents uint64
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

type SearchPolicy int32

const (
	SearchPolicyExact       SearchPolicy = 1
	SearchPolicyFixedBudget SearchPolicy = 2
	SearchPolicyAdaptive    SearchPolicy = 3
)

type AdaptiveQuality int32

const (
	AdaptiveQualityFast       AdaptiveQuality = 1
	AdaptiveQualityBalanced   AdaptiveQuality = 2
	AdaptiveQualityHighRecall AdaptiveQuality = 3
)

type QueryKernel int32

const (
	QueryKernelScalarDeterministic QueryKernel = 1
	QueryKernelSIMDDeterministic   QueryKernel = 2
	QueryKernelAutoDeterministic   QueryKernel = 3
)

type SearchBackend int32

const (
	SearchBackendNative           SearchBackend = 1
	SearchBackendProductQuantized SearchBackend = 2
	SearchBackendHNSW             SearchBackend = 3
	SearchBackendComposite        SearchBackend = 4
	SearchBackendAuto             SearchBackend = 5
)

type ProximityFilterKind int32

const (
	ProximityFilterAll          ProximityFilterKind = 1
	ProximityFilterKeyRange     ProximityFilterKind = 2
	ProximityFilterPrefix       ProximityFilterKind = 3
	ProximityFilterEligibleKeys ProximityFilterKind = 4
)

type SearchBudget struct {
	MaxNodes               *uint64
	MaxCommittedBytes      *uint64
	MaxDistanceEvaluations *uint64
	MaxFrontierEntries     *uint64
}

type ProximityFilter struct {
	Kind         ProximityFilterKind
	Start        []byte
	End          []byte
	Prefix       []byte
	EligibleKeys [][]byte
}

func AllFilter() ProximityFilter { return ProximityFilter{Kind: ProximityFilterAll} }
func KeyRangeFilter(start, end []byte) ProximityFilter {
	return ProximityFilter{Kind: ProximityFilterKeyRange, Start: bytes.Clone(start), End: bytes.Clone(end)}
}
func PrefixFilter(prefix []byte) ProximityFilter {
	return ProximityFilter{Kind: ProximityFilterPrefix, Prefix: bytes.Clone(prefix)}
}
func EligibleKeysFilter(keys [][]byte) ProximityFilter {
	return ProximityFilter{Kind: ProximityFilterEligibleKeys, EligibleKeys: cloneByteSlices(keys)}
}

type SearchRequest struct {
	Query              []float32
	K                  uint32
	Policy             SearchPolicy
	AdaptiveQuality    *AdaptiveQuality
	Budget             SearchBudget
	Filter             ProximityFilter
	Kernel             QueryKernel
	Backend            SearchBackend
	HNSWEFSearch       *uint32
	PQRerankMultiplier *uint16
}

func ExactSearch(query []float32, k uint32) SearchRequest {
	return SearchRequest{
		Query: append([]float32(nil), query...), K: k, Policy: SearchPolicyExact,
		Filter: AllFilter(), Kernel: QueryKernelAutoDeterministic, Backend: SearchBackendNative,
	}
}

func cloneSearchRequest(request SearchRequest) SearchRequest {
	request.Query = append([]float32(nil), request.Query...)
	request.Filter.Start = bytes.Clone(request.Filter.Start)
	request.Filter.End = bytes.Clone(request.Filter.End)
	request.Filter.Prefix = bytes.Clone(request.Filter.Prefix)
	request.Filter.EligibleKeys = cloneByteSlices(request.Filter.EligibleKeys)
	if request.AdaptiveQuality != nil {
		value := *request.AdaptiveQuality
		request.AdaptiveQuality = &value
	}
	cloneU64 := func(value *uint64) *uint64 {
		if value == nil {
			return nil
		}
		copy := *value
		return &copy
	}
	request.Budget.MaxNodes = cloneU64(request.Budget.MaxNodes)
	request.Budget.MaxCommittedBytes = cloneU64(request.Budget.MaxCommittedBytes)
	request.Budget.MaxDistanceEvaluations = cloneU64(request.Budget.MaxDistanceEvaluations)
	request.Budget.MaxFrontierEntries = cloneU64(request.Budget.MaxFrontierEntries)
	if request.HNSWEFSearch != nil {
		value := *request.HNSWEFSearch
		request.HNSWEFSearch = &value
	}
	if request.PQRerankMultiplier != nil {
		value := *request.PQRerankMultiplier
		request.PQRerankMultiplier = &value
	}
	return request
}

func (request SearchRequest) validate() error {
	if len(request.Query) == 0 || request.K == 0 {
		return errors.New("proximity query and k must be non-empty")
	}
	if request.Policy < SearchPolicyExact || request.Policy > SearchPolicyAdaptive {
		return errors.New("invalid proximity search policy")
	}
	if request.Policy == SearchPolicyAdaptive && request.AdaptiveQuality == nil {
		return errors.New("adaptive proximity search requires adaptive quality")
	}
	if request.AdaptiveQuality != nil && (*request.AdaptiveQuality < AdaptiveQualityFast || *request.AdaptiveQuality > AdaptiveQualityHighRecall) {
		return errors.New("invalid adaptive quality")
	}
	if request.Filter.Kind < ProximityFilterAll || request.Filter.Kind > ProximityFilterEligibleKeys {
		return errors.New("invalid proximity filter")
	}
	if request.Filter.Kind == ProximityFilterPrefix && request.Filter.Prefix == nil {
		return errors.New("prefix filter requires prefix")
	}
	if request.Kernel < QueryKernelScalarDeterministic || request.Kernel > QueryKernelAutoDeterministic {
		return errors.New("invalid query kernel")
	}
	if request.Backend < SearchBackendNative || request.Backend > SearchBackendAuto {
		return errors.New("invalid search backend")
	}
	return nil
}

func (request SearchRequest) usesPackedExactPath() bool {
	return request.Policy == SearchPolicyExact && request.AdaptiveQuality == nil &&
		request.Budget.MaxNodes == nil && request.Budget.MaxCommittedBytes == nil &&
		request.Budget.MaxDistanceEvaluations == nil && request.Budget.MaxFrontierEntries == nil &&
		request.Filter.Kind == ProximityFilterAll && request.Kernel == QueryKernelAutoDeterministic &&
		request.Backend == SearchBackendNative && request.HNSWEFSearch == nil && request.PQRerankMultiplier == nil
}

type Neighbor struct {
	Key      []byte
	Value    []byte
	Distance float64
	Rank     uint32
}
type SearchResult struct {
	Neighbors         []Neighbor
	Stats             SearchStats
	Completion        string
	Backend           string
	PlanFormatVersion uint8
}

type SearchStats struct {
	LevelsVisited                uint64
	NodesRead                    uint64
	BytesRead                    uint64
	PhysicalBytesRead            uint64
	CommittedBytes               uint64
	DistanceEvaluations          uint64
	QuantizedDistanceEvaluations uint64
	RerankedCandidates           uint64
	FrontierPeak                 uint64
	CandidateHandlesPeak         uint64
	CandidateRetainedBytesPeak   uint64
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
	return newProximityMap(handle)
}

func newProximityMap(handle uint64) (*ProximityMap, error) {
	fast, err := ffiProximityFastHandle(handle)
	if err != nil {
		ffiFreeProximity(handle)
		return nil, err
	}
	result := &ProximityMap{handle: handle, fast: fast}
	runtime.SetFinalizer(result, (*ProximityMap).Close)
	return result, nil
}

type HNSWIndex struct {
	handle uint64
	closed atomic.Bool
	mu     sync.RWMutex
}

func newHNSWIndex(handle uint64) *HNSWIndex {
	index := &HNSWIndex{handle: handle}
	runtime.SetFinalizer(index, (*HNSWIndex).Close)
	return index
}

func (i *HNSWIndex) Close() {
	if i == nil || i.closed.Swap(true) {
		return
	}
	i.mu.Lock()
	defer i.mu.Unlock()
	runtime.SetFinalizer(i, nil)
	if i.handle != 0 {
		ffiFreeHNSWIndex(i.handle)
		i.handle = 0
	}
}

func (i *HNSWIndex) withHandle() (uint64, func(), error) {
	if i == nil || i.closed.Load() {
		return 0, nil, errors.New("HNSW index is closed")
	}
	i.mu.RLock()
	if i.closed.Load() || i.handle == 0 {
		i.mu.RUnlock()
		return 0, nil, errors.New("HNSW index is closed")
	}
	return i.handle, i.mu.RUnlock, nil
}

func (m *ProximityMap) BuildHNSW(config HNSWConfig, limits HNSWBuildLimits) (HNSWBuildResult, error) {
	encodedConfig, err := encodeHNSWConfig(config)
	if err != nil {
		return HNSWBuildResult{}, err
	}
	encodedLimits := encodeHNSWBuildLimits(limits)
	handle, _, unlock, err := m.withHandle()
	if err != nil {
		return HNSWBuildResult{}, err
	}
	defer unlock()
	raw, err := ffiProximityBuildHNSW(handle, encodedConfig, encodedLimits)
	if err != nil {
		return HNSWBuildResult{}, err
	}
	d := byteDecoder{data: raw}
	indexHandle, err := d.readUint64()
	if err != nil {
		return HNSWBuildResult{}, err
	}
	stats, err := decodeHNSWBuildStats(&d)
	if err != nil {
		ffiFreeHNSWIndex(indexHandle)
		return HNSWBuildResult{}, err
	}
	if err := d.done(); err != nil {
		ffiFreeHNSWIndex(indexHandle)
		return HNSWBuildResult{}, err
	}
	return HNSWBuildResult{Index: newHNSWIndex(indexHandle), Stats: stats}, nil
}

func (m *ProximityMap) LoadHNSW(manifest []byte) (*HNSWIndex, error) {
	handle, _, unlock, err := m.withHandle()
	if err != nil {
		return nil, err
	}
	defer unlock()
	indexHandle, err := ffiProximityLoadHNSW(handle, append([]byte(nil), manifest...))
	if err != nil {
		return nil, err
	}
	return newHNSWIndex(indexHandle), nil
}

func decodeHNSWByteArray(raw []byte) ([]byte, error) {
	d := byteDecoder{data: raw}
	value, err := d.readByteArray()
	if err != nil {
		return nil, err
	}
	return value, d.done()
}

func (i *HNSWIndex) Manifest() ([]byte, error) {
	handle, unlock, err := i.withHandle()
	if err != nil {
		return nil, err
	}
	defer unlock()
	raw, err := ffiHNSWIndexManifest(handle)
	if err != nil {
		return nil, err
	}
	return decodeHNSWByteArray(raw)
}

func (i *HNSWIndex) SourceDescriptor() ([]byte, error) {
	handle, unlock, err := i.withHandle()
	if err != nil {
		return nil, err
	}
	defer unlock()
	raw, err := ffiHNSWIndexSourceDescriptor(handle)
	if err != nil {
		return nil, err
	}
	return decodeHNSWByteArray(raw)
}

func (i *HNSWIndex) Config() (HNSWConfig, error) {
	handle, unlock, err := i.withHandle()
	if err != nil {
		return HNSWConfig{}, err
	}
	defer unlock()
	raw, err := ffiHNSWIndexConfig(handle)
	if err != nil {
		return HNSWConfig{}, err
	}
	return decodeHNSWConfig(raw)
}

func (i *HNSWIndex) IsCanonical() (bool, error) {
	handle, unlock, err := i.withHandle()
	if err != nil {
		return false, err
	}
	defer unlock()
	return ffiHNSWIndexIsCanonical(handle)
}

func (i *HNSWIndex) Search(ctx context.Context, proximity *ProximityMap, request SearchRequest) (SearchResult, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return SearchResult{}, err
	}
	request = cloneSearchRequest(request)
	encodedRequest, err := encodeProximitySearchRequest(request)
	if err != nil {
		return SearchResult{}, err
	}
	indexHandle, indexUnlock, err := i.withHandle()
	if err != nil {
		return SearchResult{}, err
	}
	defer indexUnlock()
	mapHandle, _, mapUnlock, err := proximity.withHandle()
	if err != nil {
		return SearchResult{}, err
	}
	defer mapUnlock()
	raw, err := ffiHNSWIndexSearch(indexHandle, mapHandle, encodedRequest)
	if err != nil {
		return SearchResult{}, err
	}
	if err := ctx.Err(); err != nil {
		return SearchResult{}, err
	}
	return decodeProximitySearchResultBytes(raw)
}

func (i *HNSWIndex) ProveSearch(proximity *ProximityMap, request SearchRequest) (*ProximitySearchProof, error) {
	request = cloneSearchRequest(request)
	encodedRequest, err := encodeProximitySearchRequest(request)
	if err != nil {
		return nil, err
	}
	limits, err := ffiDefaultContentGraphLimits()
	if err != nil {
		return nil, err
	}
	indexHandle, indexUnlock, err := i.withHandle()
	if err != nil {
		return nil, err
	}
	defer indexUnlock()
	mapHandle, _, mapUnlock, err := proximity.withHandle()
	if err != nil {
		return nil, err
	}
	defer mapUnlock()
	proofHandle, err := ffiHNSWIndexProveSearch(indexHandle, mapHandle, encodedRequest, limits)
	if err != nil {
		return nil, err
	}
	proof := &ProximitySearchProof{handle: proofHandle}
	runtime.SetFinalizer(proof, (*ProximitySearchProof).Close)
	return proof, nil
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

func encodeFloat32Sequence(values []float32) []byte {
	var out bytes.Buffer
	writeI32(&out, int32(len(values)))
	for _, value := range values {
		writeU32(&out, math.Float32bits(value))
	}
	return out.Bytes()
}

func encodeProximitySearchRequest(request SearchRequest) ([]byte, error) {
	request = cloneSearchRequest(request)
	if err := request.validate(); err != nil {
		return nil, err
	}
	var out bytes.Buffer
	out.Write(encodeFloat32Sequence(request.Query))
	writeU64(&out, uint64(request.K))
	writeI32(&out, int32(request.Policy))
	if request.AdaptiveQuality == nil {
		out.WriteByte(0)
	} else {
		out.WriteByte(1)
		writeI32(&out, int32(*request.AdaptiveQuality))
	}
	encodeOptionalU64(&out, request.Budget.MaxNodes)
	encodeOptionalU64(&out, request.Budget.MaxCommittedBytes)
	encodeOptionalU64(&out, request.Budget.MaxDistanceEvaluations)
	encodeOptionalU64(&out, request.Budget.MaxFrontierEntries)
	writeI32(&out, int32(request.Filter.Kind))
	encodeOptionalByteArrayInto(&out, request.Filter.Start)
	encodeOptionalByteArrayInto(&out, request.Filter.End)
	encodeOptionalByteArrayInto(&out, request.Filter.Prefix)
	out.Write(encodeByteArraySequence(request.Filter.EligibleKeys))
	writeI32(&out, int32(request.Kernel))
	writeI32(&out, int32(request.Backend))
	if request.HNSWEFSearch == nil {
		out.WriteByte(0)
	} else {
		out.WriteByte(1)
		writeU32(&out, *request.HNSWEFSearch)
	}
	if request.PQRerankMultiplier == nil {
		out.WriteByte(0)
	} else {
		out.WriteByte(1)
		out.WriteByte(byte(*request.PQRerankMultiplier >> 8))
		out.WriteByte(byte(*request.PQRerankMultiplier))
	}
	return out.Bytes(), nil
}

func encodeProximityMutations(mutations []ProximityMutation) ([]byte, error) {
	var out bytes.Buffer
	writeI32(&out, int32(len(mutations)))
	for _, mutation := range mutations {
		encodeByteArrayInto(&out, mutation.Key)
		if mutation.Delete {
			out.WriteByte(0)
			out.WriteByte(0)
			continue
		}
		if len(mutation.Vector) == 0 {
			return nil, errors.New("proximity upsert vector must be non-empty")
		}
		out.WriteByte(1)
		out.Write(encodeFloat32Sequence(mutation.Vector))
		out.WriteByte(1)
		encodeByteArrayInto(&out, mutation.Value)
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

func (m *ProximityMap) Config() (ProximityConfig, error) {
	handle, _, unlock, err := m.withHandle()
	if err != nil {
		return ProximityConfig{}, err
	}
	defer unlock()
	raw, err := ffiProximityConfig(handle)
	if err != nil {
		return ProximityConfig{}, err
	}
	d := byteDecoder{data: raw}
	var config ProximityConfig
	if config.Dimensions, err = d.readUint32(); err != nil {
		return ProximityConfig{}, err
	}
	metric, err := d.readInt32()
	if err != nil {
		return ProximityConfig{}, err
	}
	config.Metric = map[int32]string{1: "l2-squared", 2: "cosine", 3: "inner-product"}[metric]
	if config.Metric == "" {
		return ProximityConfig{}, errors.New("unknown proximity distance metric")
	}
	if config.LogChunkSize, err = d.readByte(); err != nil {
		return ProximityConfig{}, err
	}
	if config.LevelHashSeed, err = d.readUint64(); err != nil {
		return ProximityConfig{}, err
	}
	if config.MinPageBytes, err = d.readUint32(); err != nil {
		return ProximityConfig{}, err
	}
	if config.TargetPageBytes, err = d.readUint32(); err != nil {
		return ProximityConfig{}, err
	}
	if config.MaxPageBytes, err = d.readUint32(); err != nil {
		return ProximityConfig{}, err
	}
	if config.OverflowHashSeed, err = d.readUint64(); err != nil {
		return ProximityConfig{}, err
	}
	if config.InlineThresholdBytes, err = d.readUint32(); err != nil {
		return ProximityConfig{}, err
	}
	present, err := d.readByte()
	if err != nil {
		return ProximityConfig{}, err
	}
	if present != 0 {
		value, err := d.readUint32()
		if err != nil {
			return ProximityConfig{}, err
		}
		config.ScalarQuantizationGroupSize = &value
	}
	return config, d.done()
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

func (m *ProximityMap) Mutate(mutations []ProximityMutation) (*ProximityMap, ProximityMutationStats, error) {
	handle, _, unlock, err := m.withHandle()
	if err != nil {
		return nil, ProximityMutationStats{}, err
	}
	defer unlock()
	encoded, err := encodeProximityMutations(mutations)
	if err != nil {
		return nil, ProximityMutationStats{}, err
	}
	raw, err := ffiProximityMutate(handle, encoded)
	if err != nil {
		return nil, ProximityMutationStats{}, err
	}
	d := byteDecoder{data: raw}
	updatedHandle, err := d.readUint64()
	if err != nil {
		return nil, ProximityMutationStats{}, err
	}
	stats, err := decodeProximityMutationStats(&d)
	if err != nil {
		ffiFreeProximity(updatedHandle)
		return nil, ProximityMutationStats{}, err
	}
	if err := d.done(); err != nil {
		ffiFreeProximity(updatedHandle)
		return nil, ProximityMutationStats{}, err
	}
	updated, err := newProximityMap(updatedHandle)
	return updated, stats, err
}

func (m *ProximityMap) Rebuild(mutations []ProximityMutation) (*ProximityMap, error) {
	handle, _, unlock, err := m.withHandle()
	if err != nil {
		return nil, err
	}
	defer unlock()
	encoded, err := encodeProximityMutations(mutations)
	if err != nil {
		return nil, err
	}
	updatedHandle, err := ffiProximityRebuild(handle, encoded)
	if err != nil {
		return nil, err
	}
	return newProximityMap(updatedHandle)
}

func (m *ProximityMap) ProveStructure() (ProximityStructuralProof, error) {
	handle, _, unlock, err := m.withHandle()
	if err != nil {
		return ProximityStructuralProof{}, err
	}
	defer unlock()
	limits, err := ffiDefaultContentGraphLimits()
	if err != nil {
		return ProximityStructuralProof{}, err
	}
	raw, err := ffiProximityProveStructure(handle, limits)
	if err != nil {
		return ProximityStructuralProof{}, err
	}
	return ProximityStructuralProof{encoded: raw}, nil
}

func VerifyProximityStructuralProof(proof ProximityStructuralProof, expectedDescriptor []byte) (ProximityStructuralVerification, error) {
	limits, err := ffiDefaultContentGraphLimits()
	if err != nil {
		return ProximityStructuralVerification{}, err
	}
	raw, err := ffiVerifyProximityStructureProof(proof.encoded, append([]byte(nil), expectedDescriptor...), limits)
	if err != nil {
		return ProximityStructuralVerification{}, err
	}
	d := byteDecoder{data: raw}
	descriptor, err := d.readByteArray()
	if err != nil {
		return ProximityStructuralVerification{}, err
	}
	objectCount, err := d.readUint64()
	if err != nil {
		return ProximityStructuralVerification{}, err
	}
	summary, err := decodeProximityVerificationFrom(&d)
	if err != nil {
		return ProximityStructuralVerification{}, err
	}
	return ProximityStructuralVerification{Descriptor: descriptor, ObjectCount: objectCount, Summary: summary}, d.done()
}

type ProximitySearchProof struct {
	handle uint64
	closed atomic.Bool
	mu     sync.RWMutex
}

func (m *ProximityMap) ProveSearch(request SearchRequest) (*ProximitySearchProof, error) {
	request = cloneSearchRequest(request)
	nativeRequest, err := encodeProximitySearchRequest(request)
	if err != nil {
		return nil, err
	}
	handle, _, unlock, err := m.withHandle()
	if err != nil {
		return nil, err
	}
	defer unlock()
	limits, err := ffiDefaultContentGraphLimits()
	if err != nil {
		return nil, err
	}
	proofHandle, err := ffiProximityProveSearch(handle, nativeRequest, limits)
	if err != nil {
		return nil, err
	}
	proof := &ProximitySearchProof{handle: proofHandle}
	runtime.SetFinalizer(proof, (*ProximitySearchProof).Close)
	return proof, nil
}

func (m *ProximityMap) Search(ctx context.Context, request SearchRequest) (SearchResult, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return SearchResult{}, err
	}
	request = cloneSearchRequest(request)
	nativeRequest, err := encodeProximitySearchRequest(request)
	if err != nil {
		return SearchResult{}, err
	}
	handle, _, unlock, err := m.withHandle()
	if err != nil {
		return SearchResult{}, err
	}
	defer unlock()
	raw, err := ffiProximitySearchRecord(handle, nativeRequest)
	if err != nil {
		return SearchResult{}, err
	}
	if err := ctx.Err(); err != nil {
		return SearchResult{}, err
	}
	return decodeProximitySearchResultBytes(raw)
}

func (p *ProximitySearchProof) Close() {
	if p == nil || p.closed.Swap(true) {
		return
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	runtime.SetFinalizer(p, nil)
	if p.handle != 0 {
		ffiFreeProximitySearchProof(p.handle)
		p.handle = 0
	}
}

func (p *ProximitySearchProof) withHandle() (uint64, func(), error) {
	if p == nil || p.closed.Load() {
		return 0, nil, errors.New("proximity search proof is closed")
	}
	p.mu.RLock()
	if p.closed.Load() || p.handle == 0 {
		p.mu.RUnlock()
		return 0, nil, errors.New("proximity search proof is closed")
	}
	return p.handle, p.mu.RUnlock, nil
}

func (p *ProximitySearchProof) Verify(expectedDescriptor []byte) (ProximitySearchVerification, error) {
	handle, unlock, err := p.withHandle()
	if err != nil {
		return ProximitySearchVerification{}, err
	}
	defer unlock()
	limits, err := ffiDefaultContentGraphLimits()
	if err != nil {
		return ProximitySearchVerification{}, err
	}
	raw, err := ffiProximitySearchProofVerify(handle, append([]byte(nil), expectedDescriptor...), limits)
	if err != nil {
		return ProximitySearchVerification{}, err
	}
	return decodeProximitySearchVerification(raw)
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
	sessionHandle, err := ffiProximityReadSession(handle)
	if err != nil {
		return nil, err
	}
	fast, err := ffiProximityReadSessionFastHandle(sessionHandle)
	if err != nil {
		ffiFreeProximityReadSession(sessionHandle)
		return nil, err
	}
	session := &ProximitySession{handle: sessionHandle, fast: fast}
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
		ffiFreeProximityReadSession(s.handle)
		s.handle = 0
		s.fast = 0
	}
}

func (s *ProximitySession) withHandle() (uint64, func(), error) {
	if s == nil || s.closed.Load() {
		return 0, nil, errors.New("proximity session is closed")
	}
	s.mu.RLock()
	if s.closed.Load() || s.handle == 0 {
		s.mu.RUnlock()
		return 0, nil, errors.New("proximity session is closed")
	}
	return s.handle, s.mu.RUnlock, nil
}

func (s *ProximitySession) Contains(key []byte) (bool, error) {
	handle, unlock, err := s.withHandle()
	if err != nil {
		return false, err
	}
	defer unlock()
	return ffiProximityReadSessionContains(handle, append([]byte(nil), key...))
}

func (s *ProximitySession) Get(key []byte) (ExactProximityRecord, bool, error) {
	handle, unlock, err := s.withHandle()
	if err != nil {
		return ExactProximityRecord{}, false, err
	}
	defer unlock()
	raw, err := ffiProximityReadSessionGet(handle, append([]byte(nil), key...))
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
	request = cloneSearchRequest(request)
	if err := request.validate(); err != nil {
		return SearchResult{}, err
	}
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return SearchResult{}, err
	}
	nativeRequest, err := encodeProximitySearchRequest(request)
	if err != nil {
		return SearchResult{}, err
	}
	handle, unlock, err := s.withHandle()
	if err != nil {
		return SearchResult{}, err
	}
	defer unlock()
	raw, err := ffiProximityReadSessionSearch(handle, nativeRequest)
	if err != nil {
		return SearchResult{}, err
	}
	if err := ctx.Err(); err != nil {
		return SearchResult{}, err
	}
	return decodeProximitySearchResultBytes(raw)
}

func (s *ProximitySession) WithSearchView(ctx context.Context, request SearchRequest, visit func([]NeighborView) error) error {
	request = cloneSearchRequest(request)
	if ctx == nil {
		ctx = context.Background()
	}
	if visit == nil {
		return errors.New("nil proximity view visitor")
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := request.validate(); err != nil {
		return err
	}
	if !request.usesPackedExactPath() {
		return errors.New("zero-copy proximity views require an exact native request")
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
	scope := newViewScope()
	defer scope.close()
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

func decodeProximitySearchResult(d *byteDecoder) (SearchResult, error) {
	count, err := d.readInt32()
	if err != nil || count < 0 {
		if err == nil {
			err = errors.New("negative proximity neighbor count")
		}
		return SearchResult{}, err
	}
	result := SearchResult{Neighbors: make([]Neighbor, 0, count)}
	for index := int32(0); index < count; index++ {
		key, err := d.readByteArray()
		if err != nil {
			return SearchResult{}, err
		}
		value, err := d.readByteArray()
		if err != nil {
			return SearchResult{}, err
		}
		distanceBits, err := d.readUint64()
		if err != nil {
			return SearchResult{}, err
		}
		result.Neighbors = append(result.Neighbors, Neighbor{
			Key: key, Value: value, Distance: math.Float64frombits(distanceBits), Rank: uint32(index),
		})
	}
	stats := []*uint64{
		&result.Stats.LevelsVisited,
		&result.Stats.NodesRead,
		&result.Stats.BytesRead,
		&result.Stats.PhysicalBytesRead,
		&result.Stats.CommittedBytes,
		&result.Stats.DistanceEvaluations,
		&result.Stats.QuantizedDistanceEvaluations,
		&result.Stats.RerankedCandidates,
		&result.Stats.FrontierPeak,
		&result.Stats.CandidateHandlesPeak,
		&result.Stats.CandidateRetainedBytesPeak,
	}
	for _, field := range stats {
		value, err := d.readUint64()
		if err != nil {
			return SearchResult{}, err
		}
		*field = value
	}
	completion, err := d.readInt32()
	if err != nil {
		return SearchResult{}, err
	}
	result.Completion = map[int32]string{
		1: "exact", 2: "approximate-policy-satisfied", 3: "budget-exhausted",
		4: "cancelled", 5: "deadline-exceeded",
	}[completion]
	if result.Completion == "" {
		return SearchResult{}, errors.New("unknown proximity search completion")
	}
	backend, err := d.readInt32()
	if err != nil {
		return SearchResult{}, err
	}
	result.Backend = map[int32]string{
		1: "native", 2: "product-quantized", 3: "hnsw", 4: "composite", 5: "auto",
	}[backend]
	if result.Backend == "" {
		return SearchResult{}, errors.New("unknown proximity search backend")
	}
	result.PlanFormatVersion, err = d.readByte()
	if err != nil {
		return SearchResult{}, err
	}
	return result, nil
}

func decodeProximitySearchResultBytes(raw []byte) (SearchResult, error) {
	d := byteDecoder{data: raw}
	result, err := decodeProximitySearchResult(&d)
	if err != nil {
		return SearchResult{}, err
	}
	return result, d.done()
}

func decodeProximitySearchVerification(raw []byte) (ProximitySearchVerification, error) {
	d := byteDecoder{data: raw}
	result, err := decodeProximitySearchResult(&d)
	if err != nil {
		return ProximitySearchVerification{}, err
	}
	kind, err := d.readInt32()
	if err != nil {
		return ProximitySearchVerification{}, err
	}
	claim := ProximitySearchClaim{}
	switch kind {
	case 1:
		claim.Kind = "exact-l2-optimal"
	case 2:
		claim.Kind = "honest-execution"
	default:
		return ProximitySearchVerification{}, errors.New("unknown proximity search claim")
	}
	present, err := d.readByte()
	if err != nil {
		return ProximitySearchVerification{}, err
	}
	if present != 0 {
		bits, err := d.readUint64()
		if err != nil {
			return ProximitySearchVerification{}, err
		}
		value := math.Float64frombits(bits)
		claim.TerminalLowerBound = &value
	}
	replayed, err := d.readUint64()
	if err != nil {
		return ProximitySearchVerification{}, err
	}
	verification := ProximitySearchVerification{Result: result, Claim: claim, ReplayedEvents: replayed}
	return verification, d.done()
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
	result, err := decodeProximityVerificationFrom(&d)
	if err != nil {
		return ProximityVerification{}, err
	}
	return result, d.done()
}

func decodeProximityVerificationFrom(d *byteDecoder) (ProximityVerification, error) {
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
	return result, nil
}

func decodeProximityMutationStats(d *byteDecoder) (ProximityMutationStats, error) {
	var result ProximityMutationStats
	fields := []*uint64{
		&result.DirectoryEntriesScanned, &result.DirectoryNodesRead,
		&result.DirectoryNodesRebuilt, &result.DirectoryNodesWritten,
		&result.DirectoryNodesReused, &result.DirectoryLevelsRebuilt,
	}
	for _, field := range fields {
		value, err := d.readUint64()
		if err != nil {
			return ProximityMutationStats{}, err
		}
		*field = value
	}
	rightEdge, err := d.readByte()
	if err != nil {
		return ProximityMutationStats{}, err
	}
	result.DirectoryRightEdgeRebuilt = rightEdge != 0
	fields = []*uint64{
		&result.NodesRead, &result.NodesWritten, &result.NodesReused,
		&result.RecordsRebuilt, &result.DistanceEvaluations,
	}
	for _, field := range fields {
		value, err := d.readUint64()
		if err != nil {
			return ProximityMutationStats{}, err
		}
		*field = value
	}
	full, err := d.readByte()
	if err != nil {
		return ProximityMutationStats{}, err
	}
	result.FullProximityRebuild = full != 0
	return result, nil
}
