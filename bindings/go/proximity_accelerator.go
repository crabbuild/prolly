package prolly

import (
	"bytes"
	"context"
	"errors"
	"runtime"
	"sync"
	"sync/atomic"
)

type CompositeBaseKind int32

const (
	CompositeBaseHNSW             CompositeBaseKind = 1
	CompositeBaseProductQuantized CompositeBaseKind = 2
)

type CompositeAcceleratorConfig struct {
	MaxDeltaRecords         uint64
	MaxShadowRecords        uint64
	MaxDeltaRatioPPM        uint32
	MaxShadowRatioPPM       uint32
	BaseOverfetchMultiplier uint32
}

type CompositeBuildLimits struct {
	MaxDiffEntries         *uint64
	MaxOwnedBytes          *uint64
	MaxEncodedOutputBytes  *uint64
	MaxDistanceEvaluations *uint64
}

type CompositeBuildStats struct {
	DiffEntries, InsertedRecords, VectorUpdatedRecords, ValueOnlyRecords uint64
	DeletedRecords, DeltaRecords, ShadowRecords, OwnedBytesPeak          uint64
	EncodedOutputBytes, DistanceEvaluations                              uint64
}

type FullRebuildReasonKind int32

const (
	FullRebuildDeltaRecords  FullRebuildReasonKind = 1
	FullRebuildShadowRecords FullRebuildReasonKind = 2
	FullRebuildDeltaRatio    FullRebuildReasonKind = 3
	FullRebuildShadowRatio   FullRebuildReasonKind = 4
)

type FullRebuildReason struct {
	Kind            FullRebuildReasonKind
	Actual, Maximum uint64
}

type CompositeRebuildOptions struct {
	HNSWLimits      HNSWBuildLimits
	PQWorkerThreads uint64
	PQLimits        ProductQuantizationBuildLimits
}

type CompositeBuildOutcome struct {
	Accelerator *CompositeAccelerator
	Reasons     []FullRebuildReason
	Stats       CompositeBuildStats
}

type CompositeBuildOrRebuildKind int32

const (
	CompositeBuilt                   CompositeBuildOrRebuildKind = 1
	CompositeNoAcceleratorRequired   CompositeBuildOrRebuildKind = 2
	CompositeHNSWRebuilt             CompositeBuildOrRebuildKind = 3
	CompositeProductQuantizedRebuilt CompositeBuildOrRebuildKind = 4
)

type CompositeBuildOrRebuildOutcome struct {
	Kind           CompositeBuildOrRebuildKind
	Composite      *CompositeAccelerator
	HNSW           *HNSWIndex
	PQ             *ProductQuantizer
	Reasons        []FullRebuildReason
	CompositeStats CompositeBuildStats
	HNSWStats      *HNSWBuildStats
	PQStats        *ProductQuantizationBuildStats
}

type CatalogAcceleratorKind int32

const (
	CatalogHNSW             CatalogAcceleratorKind = 1
	CatalogProductQuantized CatalogAcceleratorKind = 2
	CatalogComposite        CatalogAcceleratorKind = 3
)

type AcceleratorCatalogEntry struct {
	Kind                     CatalogAcceleratorKind
	ConfigurationFingerprint []byte
	Manifest                 []byte
}

func encodeCompositeConfig(value CompositeAcceleratorConfig) []byte {
	var out bytes.Buffer
	writeU64(&out, value.MaxDeltaRecords)
	writeU64(&out, value.MaxShadowRecords)
	writeU32(&out, value.MaxDeltaRatioPPM)
	writeU32(&out, value.MaxShadowRatioPPM)
	writeU32(&out, value.BaseOverfetchMultiplier)
	return out.Bytes()
}

func decodeCompositeConfig(raw []byte) (CompositeAcceleratorConfig, error) {
	d := byteDecoder{data: raw}
	var value CompositeAcceleratorConfig
	var err error
	if value.MaxDeltaRecords, err = d.readUint64(); err != nil {
		return value, err
	}
	if value.MaxShadowRecords, err = d.readUint64(); err != nil {
		return value, err
	}
	if value.MaxDeltaRatioPPM, err = d.readUint32(); err != nil {
		return value, err
	}
	if value.MaxShadowRatioPPM, err = d.readUint32(); err != nil {
		return value, err
	}
	if value.BaseOverfetchMultiplier, err = d.readUint32(); err != nil {
		return value, err
	}
	return value, d.done()
}

func encodeCompositeBuildLimits(value CompositeBuildLimits) []byte {
	var out bytes.Buffer
	encodeOptionalU64(&out, value.MaxDiffEntries)
	encodeOptionalU64(&out, value.MaxOwnedBytes)
	encodeOptionalU64(&out, value.MaxEncodedOutputBytes)
	encodeOptionalU64(&out, value.MaxDistanceEvaluations)
	return out.Bytes()
}

func decodeCompositeBuildLimits(raw []byte) (CompositeBuildLimits, error) {
	d := byteDecoder{data: raw}
	var value CompositeBuildLimits
	var err error
	if value.MaxDiffEntries, err = d.readOptionalUint64(); err != nil {
		return value, err
	}
	if value.MaxOwnedBytes, err = d.readOptionalUint64(); err != nil {
		return value, err
	}
	if value.MaxEncodedOutputBytes, err = d.readOptionalUint64(); err != nil {
		return value, err
	}
	if value.MaxDistanceEvaluations, err = d.readOptionalUint64(); err != nil {
		return value, err
	}
	return value, d.done()
}

func decodeCompositeBuildStats(d *byteDecoder) (CompositeBuildStats, error) {
	var value CompositeBuildStats
	targets := []*uint64{
		&value.DiffEntries, &value.InsertedRecords, &value.VectorUpdatedRecords, &value.ValueOnlyRecords,
		&value.DeletedRecords, &value.DeltaRecords, &value.ShadowRecords, &value.OwnedBytesPeak,
		&value.EncodedOutputBytes, &value.DistanceEvaluations,
	}
	for _, target := range targets {
		next, err := d.readUint64()
		if err != nil {
			return value, err
		}
		*target = next
	}
	return value, nil
}

func decodeFullRebuildReasons(d *byteDecoder) ([]FullRebuildReason, error) {
	count, err := d.readInt32()
	if err != nil {
		return nil, err
	}
	if count < 0 {
		return nil, errors.New("invalid rebuild reason count")
	}
	values := make([]FullRebuildReason, 0, count)
	for range count {
		kind, err := d.readInt32()
		if err != nil {
			return nil, err
		}
		actual, err := d.readUint64()
		if err != nil {
			return nil, err
		}
		maximum, err := d.readUint64()
		if err != nil {
			return nil, err
		}
		if kind < 1 || kind > 4 {
			return nil, errors.New("unknown full rebuild reason")
		}
		values = append(values, FullRebuildReason{Kind: FullRebuildReasonKind(kind), Actual: actual, Maximum: maximum})
	}
	return values, nil
}

func encodeCompositeRebuildOptions(value CompositeRebuildOptions) []byte {
	var out bytes.Buffer
	out.Write(encodeHNSWBuildLimits(value.HNSWLimits))
	writeU64(&out, value.PQWorkerThreads)
	out.Write(encodeProductQuantizationBuildLimits(value.PQLimits))
	return out.Bytes()
}

func decodeCompositeRebuildOptions(raw []byte) (CompositeRebuildOptions, error) {
	d := byteDecoder{data: raw}
	var value CompositeRebuildOptions
	var err error
	if value.HNSWLimits.MaxRecords, err = d.readOptionalUint64(); err != nil {
		return value, err
	}
	if value.HNSWLimits.MaxOwnedBytes, err = d.readOptionalUint64(); err != nil {
		return value, err
	}
	if value.HNSWLimits.MaxDistanceEvaluations, err = d.readOptionalUint64(); err != nil {
		return value, err
	}
	if value.HNSWLimits.WorkerThreads, err = d.readUint64(); err != nil {
		return value, err
	}
	if value.HNSWLimits.MaxEncodedGraphBytes, err = d.readOptionalUint64(); err != nil {
		return value, err
	}
	if value.PQWorkerThreads, err = d.readUint64(); err != nil {
		return value, err
	}
	if value.PQLimits.MaxTrainingVectors, err = d.readOptionalUint64(); err != nil {
		return value, err
	}
	if value.PQLimits.MaxTrainingBytes, err = d.readOptionalUint64(); err != nil {
		return value, err
	}
	if value.PQLimits.MaxTemporaryCodeBytes, err = d.readOptionalUint64(); err != nil {
		return value, err
	}
	if value.PQLimits.MaxDistanceEvaluations, err = d.readOptionalUint64(); err != nil {
		return value, err
	}
	if value.PQLimits.MaxEncodedOutputBytes, err = d.readOptionalUint64(); err != nil {
		return value, err
	}
	if value.PQLimits.MaxWorkerThreads, err = d.readOptionalUint64(); err != nil {
		return value, err
	}
	return value, d.done()
}

func DefaultCompositeAcceleratorConfig() (CompositeAcceleratorConfig, error) {
	raw, err := ffiDefaultCompositeConfig()
	if err != nil {
		return CompositeAcceleratorConfig{}, err
	}
	return decodeCompositeConfig(raw)
}
func DefaultCompositeBuildLimits() (CompositeBuildLimits, error) {
	raw, err := ffiDefaultCompositeBuildLimits()
	if err != nil {
		return CompositeBuildLimits{}, err
	}
	return decodeCompositeBuildLimits(raw)
}
func DefaultCompositeRebuildOptions() (CompositeRebuildOptions, error) {
	raw, err := ffiDefaultCompositeRebuildOptions()
	if err != nil {
		return CompositeRebuildOptions{}, err
	}
	return decodeCompositeRebuildOptions(raw)
}

func readOptionalObject(d *byteDecoder) (uint64, error) {
	present, err := d.readByte()
	if err != nil || present == 0 {
		return 0, err
	}
	return d.readUint64()
}

func decodeCompositeBuildOutcome(raw []byte) (CompositeBuildOutcome, error) {
	d := byteDecoder{data: raw}
	handle, err := readOptionalObject(&d)
	if err != nil {
		return CompositeBuildOutcome{}, err
	}
	cleanup := func() {
		if handle != 0 {
			ffiFreeComposite(handle)
		}
	}
	reasons, err := decodeFullRebuildReasons(&d)
	if err != nil {
		cleanup()
		return CompositeBuildOutcome{}, err
	}
	stats, err := decodeCompositeBuildStats(&d)
	if err != nil {
		cleanup()
		return CompositeBuildOutcome{}, err
	}
	if err := d.done(); err != nil {
		cleanup()
		return CompositeBuildOutcome{}, err
	}
	var accelerator *CompositeAccelerator
	if handle != 0 {
		accelerator = newCompositeAccelerator(handle)
	}
	return CompositeBuildOutcome{Accelerator: accelerator, Reasons: reasons, Stats: stats}, nil
}

func decodeOptionalHNSWStats(d *byteDecoder) (*HNSWBuildStats, error) {
	present, err := d.readByte()
	if err != nil || present == 0 {
		return nil, err
	}
	value, err := decodeHNSWBuildStats(d)
	return &value, err
}
func decodeOptionalPQStats(d *byteDecoder) (*ProductQuantizationBuildStats, error) {
	present, err := d.readByte()
	if err != nil || present == 0 {
		return nil, err
	}
	value, err := decodeProductQuantizationBuildStats(d)
	return &value, err
}

func decodeCompositeRebuildOutcome(raw []byte) (CompositeBuildOrRebuildOutcome, error) {
	d := byteDecoder{data: raw}
	kind, err := d.readInt32()
	if err != nil {
		return CompositeBuildOrRebuildOutcome{}, err
	}
	composite, err := readOptionalObject(&d)
	if err != nil {
		return CompositeBuildOrRebuildOutcome{}, err
	}
	hnsw, err := readOptionalObject(&d)
	if err != nil {
		if composite != 0 {
			ffiFreeComposite(composite)
		}
		return CompositeBuildOrRebuildOutcome{}, err
	}
	pq, err := readOptionalObject(&d)
	if err != nil {
		if composite != 0 {
			ffiFreeComposite(composite)
		}
		if hnsw != 0 {
			ffiFreeHNSWIndex(hnsw)
		}
		return CompositeBuildOrRebuildOutcome{}, err
	}
	cleanup := func() {
		if composite != 0 {
			ffiFreeComposite(composite)
		}
		if hnsw != 0 {
			ffiFreeHNSWIndex(hnsw)
		}
		if pq != 0 {
			ffiFreeProductQuantizer(pq)
		}
	}
	reasons, err := decodeFullRebuildReasons(&d)
	if err != nil {
		cleanup()
		return CompositeBuildOrRebuildOutcome{}, err
	}
	stats, err := decodeCompositeBuildStats(&d)
	if err != nil {
		cleanup()
		return CompositeBuildOrRebuildOutcome{}, err
	}
	hnswStats, err := decodeOptionalHNSWStats(&d)
	if err != nil {
		cleanup()
		return CompositeBuildOrRebuildOutcome{}, err
	}
	pqStats, err := decodeOptionalPQStats(&d)
	if err != nil {
		cleanup()
		return CompositeBuildOrRebuildOutcome{}, err
	}
	if err := d.done(); err != nil {
		cleanup()
		return CompositeBuildOrRebuildOutcome{}, err
	}
	if kind < 1 || kind > 4 {
		cleanup()
		return CompositeBuildOrRebuildOutcome{}, errors.New("unknown composite rebuild outcome")
	}
	value := CompositeBuildOrRebuildOutcome{Kind: CompositeBuildOrRebuildKind(kind), Reasons: reasons, CompositeStats: stats, HNSWStats: hnswStats, PQStats: pqStats}
	if composite != 0 {
		value.Composite = newCompositeAccelerator(composite)
	}
	if hnsw != 0 {
		value.HNSW = newHNSWIndex(hnsw)
	}
	if pq != 0 {
		value.PQ = newProductQuantizer(pq)
	}
	return value, nil
}

func (m *ProximityMap) BuildCompositeHNSW(baseMap *ProximityMap, base *HNSWIndex, config CompositeAcceleratorConfig, limits CompositeBuildLimits) (CompositeBuildOutcome, error) {
	current, _, currentUnlock, err := m.withHandle()
	if err != nil {
		return CompositeBuildOutcome{}, err
	}
	defer currentUnlock()
	baseMapHandle, _, mapUnlock, err := baseMap.withHandle()
	if err != nil {
		return CompositeBuildOutcome{}, err
	}
	defer mapUnlock()
	baseHandle, baseUnlock, err := base.withHandle()
	if err != nil {
		return CompositeBuildOutcome{}, err
	}
	defer baseUnlock()
	raw, err := ffiProximityBuildCompositeHNSW(current, baseMapHandle, baseHandle, encodeCompositeConfig(config), encodeCompositeBuildLimits(limits))
	if err != nil {
		return CompositeBuildOutcome{}, err
	}
	return decodeCompositeBuildOutcome(raw)
}

func (m *ProximityMap) BuildCompositePQ(baseMap *ProximityMap, base *ProductQuantizer, config CompositeAcceleratorConfig, limits CompositeBuildLimits) (CompositeBuildOutcome, error) {
	current, _, currentUnlock, err := m.withHandle()
	if err != nil {
		return CompositeBuildOutcome{}, err
	}
	defer currentUnlock()
	baseMapHandle, _, mapUnlock, err := baseMap.withHandle()
	if err != nil {
		return CompositeBuildOutcome{}, err
	}
	defer mapUnlock()
	baseHandle, baseUnlock, err := base.withHandle()
	if err != nil {
		return CompositeBuildOutcome{}, err
	}
	defer baseUnlock()
	raw, err := ffiProximityBuildCompositePQ(current, baseMapHandle, baseHandle, encodeCompositeConfig(config), encodeCompositeBuildLimits(limits))
	if err != nil {
		return CompositeBuildOutcome{}, err
	}
	return decodeCompositeBuildOutcome(raw)
}

func (m *ProximityMap) BuildOrRebuildCompositeHNSW(baseMap *ProximityMap, base *HNSWIndex, config CompositeAcceleratorConfig, limits CompositeBuildLimits, rebuild CompositeRebuildOptions) (CompositeBuildOrRebuildOutcome, error) {
	current, _, currentUnlock, err := m.withHandle()
	if err != nil {
		return CompositeBuildOrRebuildOutcome{}, err
	}
	defer currentUnlock()
	baseMapHandle, _, mapUnlock, err := baseMap.withHandle()
	if err != nil {
		return CompositeBuildOrRebuildOutcome{}, err
	}
	defer mapUnlock()
	baseHandle, baseUnlock, err := base.withHandle()
	if err != nil {
		return CompositeBuildOrRebuildOutcome{}, err
	}
	defer baseUnlock()
	raw, err := ffiProximityRebuildCompositeHNSW(current, baseMapHandle, baseHandle, encodeCompositeConfig(config), encodeCompositeBuildLimits(limits), encodeCompositeRebuildOptions(rebuild))
	if err != nil {
		return CompositeBuildOrRebuildOutcome{}, err
	}
	return decodeCompositeRebuildOutcome(raw)
}

func (m *ProximityMap) BuildOrRebuildCompositePQ(baseMap *ProximityMap, base *ProductQuantizer, config CompositeAcceleratorConfig, limits CompositeBuildLimits, rebuild CompositeRebuildOptions) (CompositeBuildOrRebuildOutcome, error) {
	current, _, currentUnlock, err := m.withHandle()
	if err != nil {
		return CompositeBuildOrRebuildOutcome{}, err
	}
	defer currentUnlock()
	baseMapHandle, _, mapUnlock, err := baseMap.withHandle()
	if err != nil {
		return CompositeBuildOrRebuildOutcome{}, err
	}
	defer mapUnlock()
	baseHandle, baseUnlock, err := base.withHandle()
	if err != nil {
		return CompositeBuildOrRebuildOutcome{}, err
	}
	defer baseUnlock()
	raw, err := ffiProximityRebuildCompositePQ(current, baseMapHandle, baseHandle, encodeCompositeConfig(config), encodeCompositeBuildLimits(limits), encodeCompositeRebuildOptions(rebuild))
	if err != nil {
		return CompositeBuildOrRebuildOutcome{}, err
	}
	return decodeCompositeRebuildOutcome(raw)
}

type CompositeAccelerator struct {
	handle uint64
	closed atomic.Bool
	mu     sync.RWMutex
}

func newCompositeAccelerator(handle uint64) *CompositeAccelerator {
	value := &CompositeAccelerator{handle: handle}
	runtime.SetFinalizer(value, (*CompositeAccelerator).Close)
	return value
}
func (a *CompositeAccelerator) Close() {
	if a == nil || a.closed.Swap(true) {
		return
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	runtime.SetFinalizer(a, nil)
	if a.handle != 0 {
		ffiFreeComposite(a.handle)
		a.handle = 0
	}
}
func (a *CompositeAccelerator) withHandle() (uint64, func(), error) {
	if a == nil || a.closed.Load() {
		return 0, nil, errors.New("composite accelerator is closed")
	}
	a.mu.RLock()
	if a.closed.Load() || a.handle == 0 {
		a.mu.RUnlock()
		return 0, nil, errors.New("composite accelerator is closed")
	}
	return a.handle, a.mu.RUnlock, nil
}
func (m *ProximityMap) LoadComposite(manifest []byte) (*CompositeAccelerator, error) {
	handle, _, unlock, err := m.withHandle()
	if err != nil {
		return nil, err
	}
	defer unlock()
	value, err := ffiProximityLoadComposite(handle, append([]byte(nil), manifest...))
	if err != nil {
		return nil, err
	}
	return newCompositeAccelerator(value), nil
}
func (a *CompositeAccelerator) Manifest() ([]byte, error) {
	handle, unlock, err := a.withHandle()
	if err != nil {
		return nil, err
	}
	defer unlock()
	raw, err := ffiCompositeManifest(handle)
	if err != nil {
		return nil, err
	}
	return decodeHNSWByteArray(raw)
}
func (a *CompositeAccelerator) CurrentSourceDescriptor() ([]byte, error) {
	handle, unlock, err := a.withHandle()
	if err != nil {
		return nil, err
	}
	defer unlock()
	raw, err := ffiCompositeCurrentSource(handle)
	if err != nil {
		return nil, err
	}
	return decodeHNSWByteArray(raw)
}
func (a *CompositeAccelerator) BaseSourceDescriptor() ([]byte, error) {
	handle, unlock, err := a.withHandle()
	if err != nil {
		return nil, err
	}
	defer unlock()
	raw, err := ffiCompositeBaseSource(handle)
	if err != nil {
		return nil, err
	}
	return decodeHNSWByteArray(raw)
}
func (a *CompositeAccelerator) BaseKind() (CompositeBaseKind, error) {
	handle, unlock, err := a.withHandle()
	if err != nil {
		return 0, err
	}
	defer unlock()
	raw, err := ffiCompositeBaseKind(handle)
	if err != nil {
		return 0, err
	}
	d := byteDecoder{data: raw}
	value, err := d.readInt32()
	if err != nil {
		return 0, err
	}
	if err := d.done(); err != nil {
		return 0, err
	}
	return CompositeBaseKind(value), nil
}
func (a *CompositeAccelerator) DeltaCount() (uint64, error) {
	handle, unlock, err := a.withHandle()
	if err != nil {
		return 0, err
	}
	defer unlock()
	return ffiCompositeCount(handle, false)
}
func (a *CompositeAccelerator) ShadowCount() (uint64, error) {
	handle, unlock, err := a.withHandle()
	if err != nil {
		return 0, err
	}
	defer unlock()
	return ffiCompositeCount(handle, true)
}
func (a *CompositeAccelerator) Config() (CompositeAcceleratorConfig, error) {
	handle, unlock, err := a.withHandle()
	if err != nil {
		return CompositeAcceleratorConfig{}, err
	}
	defer unlock()
	raw, err := ffiCompositeConfig(handle)
	if err != nil {
		return CompositeAcceleratorConfig{}, err
	}
	return decodeCompositeConfig(raw)
}
func (a *CompositeAccelerator) BuildStats() (CompositeBuildStats, error) {
	handle, unlock, err := a.withHandle()
	if err != nil {
		return CompositeBuildStats{}, err
	}
	defer unlock()
	raw, err := ffiCompositeBuildStats(handle)
	if err != nil {
		return CompositeBuildStats{}, err
	}
	d := byteDecoder{data: raw}
	value, err := decodeCompositeBuildStats(&d)
	if err != nil {
		return value, err
	}
	return value, d.done()
}
func (a *CompositeAccelerator) Search(ctx context.Context, proximity *ProximityMap, request SearchRequest) (SearchResult, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return SearchResult{}, err
	}
	encoded, err := encodeProximitySearchRequest(cloneSearchRequest(request))
	if err != nil {
		return SearchResult{}, err
	}
	index, indexUnlock, err := a.withHandle()
	if err != nil {
		return SearchResult{}, err
	}
	defer indexUnlock()
	source, _, sourceUnlock, err := proximity.withHandle()
	if err != nil {
		return SearchResult{}, err
	}
	defer sourceUnlock()
	raw, err := ffiCompositeSearch(index, source, encoded)
	if err != nil {
		return SearchResult{}, err
	}
	if err := ctx.Err(); err != nil {
		return SearchResult{}, err
	}
	return decodeProximitySearchResultBytes(raw)
}
func (a *CompositeAccelerator) SearchWithRuntime(
	ctx context.Context, proximity *ProximityMap, request SearchRequest, searchRuntime *ProximitySearchRuntime,
) (SearchResult, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return SearchResult{}, err
	}
	encoded, err := encodeProximitySearchRequest(cloneSearchRequest(request))
	if err != nil {
		return SearchResult{}, err
	}
	index, indexUnlock, err := a.withHandle()
	if err != nil {
		return SearchResult{}, err
	}
	defer indexUnlock()
	source, _, sourceUnlock, err := proximity.withHandle()
	if err != nil {
		return SearchResult{}, err
	}
	defer sourceUnlock()
	runtimeHandle, runtimeUnlock, err := searchRuntime.withHandle()
	if err != nil {
		return SearchResult{}, err
	}
	defer runtimeUnlock()
	raw, err := ffiCompositeSearchWithRuntime(index, source, encoded, runtimeHandle)
	if err != nil {
		return SearchResult{}, err
	}
	if err := ctx.Err(); err != nil {
		return SearchResult{}, err
	}
	return decodeProximitySearchResultBytes(raw)
}
func (a *CompositeAccelerator) ProveSearch(proximity *ProximityMap, request SearchRequest) (*ProximitySearchProof, error) {
	encoded, err := encodeProximitySearchRequest(cloneSearchRequest(request))
	if err != nil {
		return nil, err
	}
	limits, err := ffiDefaultContentGraphLimits()
	if err != nil {
		return nil, err
	}
	index, indexUnlock, err := a.withHandle()
	if err != nil {
		return nil, err
	}
	defer indexUnlock()
	source, _, sourceUnlock, err := proximity.withHandle()
	if err != nil {
		return nil, err
	}
	defer sourceUnlock()
	handle, err := ffiCompositeProveSearch(index, source, encoded, limits)
	if err != nil {
		return nil, err
	}
	proof := &ProximitySearchProof{handle: handle}
	runtime.SetFinalizer(proof, (*ProximitySearchProof).Close)
	return proof, nil
}

type AcceleratorCatalog struct {
	handle uint64
	closed atomic.Bool
	mu     sync.RWMutex
}

func newAcceleratorCatalog(handle uint64) *AcceleratorCatalog {
	value := &AcceleratorCatalog{handle: handle}
	runtime.SetFinalizer(value, (*AcceleratorCatalog).Close)
	return value
}
func (a *AcceleratorCatalog) Close() {
	if a == nil || a.closed.Swap(true) {
		return
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	runtime.SetFinalizer(a, nil)
	if a.handle != 0 {
		ffiFreeCatalog(a.handle)
		a.handle = 0
	}
}
func (a *AcceleratorCatalog) withHandle() (uint64, func(), error) {
	if a == nil || a.closed.Load() {
		return 0, nil, errors.New("accelerator catalog is closed")
	}
	a.mu.RLock()
	if a.closed.Load() || a.handle == 0 {
		a.mu.RUnlock()
		return 0, nil, errors.New("accelerator catalog is closed")
	}
	return a.handle, a.mu.RUnlock, nil
}

func (m *ProximityMap) BuildAcceleratorCatalog(hnsw *HNSWIndex, pq *ProductQuantizer, composite *CompositeAccelerator) (*AcceleratorCatalog, error) {
	mapHandle, _, mapUnlock, err := m.withHandle()
	if err != nil {
		return nil, err
	}
	defer mapUnlock()
	var hnswHandle, pqHandle, compositeHandle *uint64
	var unlocks []func()
	defer func() {
		for i := len(unlocks) - 1; i >= 0; i-- {
			unlocks[i]()
		}
	}()
	if hnsw != nil {
		value, unlock, err := hnsw.withHandle()
		if err != nil {
			return nil, err
		}
		hnswHandle = &value
		unlocks = append(unlocks, unlock)
	}
	if pq != nil {
		value, unlock, err := pq.withHandle()
		if err != nil {
			return nil, err
		}
		pqHandle = &value
		unlocks = append(unlocks, unlock)
	}
	if composite != nil {
		value, unlock, err := composite.withHandle()
		if err != nil {
			return nil, err
		}
		compositeHandle = &value
		unlocks = append(unlocks, unlock)
	}
	handle, err := ffiProximityBuildCatalog(mapHandle, hnswHandle, pqHandle, compositeHandle)
	if err != nil {
		return nil, err
	}
	return newAcceleratorCatalog(handle), nil
}
func (m *ProximityMap) LoadAcceleratorCatalog(manifest []byte) (*AcceleratorCatalog, error) {
	handle, _, unlock, err := m.withHandle()
	if err != nil {
		return nil, err
	}
	defer unlock()
	value, err := ffiProximityLoadCatalog(handle, append([]byte(nil), manifest...))
	if err != nil {
		return nil, err
	}
	return newAcceleratorCatalog(value), nil
}
func (a *AcceleratorCatalog) Manifest() ([]byte, error) {
	handle, unlock, err := a.withHandle()
	if err != nil {
		return nil, err
	}
	defer unlock()
	raw, err := ffiCatalogManifest(handle)
	if err != nil {
		return nil, err
	}
	return decodeHNSWByteArray(raw)
}
func (a *AcceleratorCatalog) SourceDescriptor() ([]byte, error) {
	handle, unlock, err := a.withHandle()
	if err != nil {
		return nil, err
	}
	defer unlock()
	raw, err := ffiCatalogSource(handle)
	if err != nil {
		return nil, err
	}
	return decodeHNSWByteArray(raw)
}
func (a *AcceleratorCatalog) Entries() ([]AcceleratorCatalogEntry, error) {
	handle, unlock, err := a.withHandle()
	if err != nil {
		return nil, err
	}
	defer unlock()
	raw, err := ffiCatalogEntries(handle)
	if err != nil {
		return nil, err
	}
	d := byteDecoder{data: raw}
	count, err := d.readInt32()
	if err != nil || count < 0 {
		return nil, errors.New("invalid accelerator catalog entries")
	}
	values := make([]AcceleratorCatalogEntry, 0, count)
	for range count {
		kind, err := d.readInt32()
		if err != nil {
			return nil, err
		}
		fingerprint, err := d.readByteArray()
		if err != nil {
			return nil, err
		}
		manifest, err := d.readByteArray()
		if err != nil {
			return nil, err
		}
		values = append(values, AcceleratorCatalogEntry{Kind: CatalogAcceleratorKind(kind), ConfigurationFingerprint: fingerprint, Manifest: manifest})
	}
	return values, d.done()
}
func (a *AcceleratorCatalog) Search(ctx context.Context, proximity *ProximityMap, request SearchRequest) (SearchResult, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return SearchResult{}, err
	}
	encoded, err := encodeProximitySearchRequest(cloneSearchRequest(request))
	if err != nil {
		return SearchResult{}, err
	}
	catalog, catalogUnlock, err := a.withHandle()
	if err != nil {
		return SearchResult{}, err
	}
	defer catalogUnlock()
	source, _, sourceUnlock, err := proximity.withHandle()
	if err != nil {
		return SearchResult{}, err
	}
	defer sourceUnlock()
	raw, err := ffiCatalogSearch(catalog, source, encoded)
	if err != nil {
		return SearchResult{}, err
	}
	if err := ctx.Err(); err != nil {
		return SearchResult{}, err
	}
	return decodeProximitySearchResultBytes(raw)
}
func (a *AcceleratorCatalog) SearchWithRuntime(
	ctx context.Context, proximity *ProximityMap, request SearchRequest, searchRuntime *ProximitySearchRuntime,
) (SearchResult, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return SearchResult{}, err
	}
	encoded, err := encodeProximitySearchRequest(cloneSearchRequest(request))
	if err != nil {
		return SearchResult{}, err
	}
	catalog, catalogUnlock, err := a.withHandle()
	if err != nil {
		return SearchResult{}, err
	}
	defer catalogUnlock()
	source, _, sourceUnlock, err := proximity.withHandle()
	if err != nil {
		return SearchResult{}, err
	}
	defer sourceUnlock()
	runtimeHandle, runtimeUnlock, err := searchRuntime.withHandle()
	if err != nil {
		return SearchResult{}, err
	}
	defer runtimeUnlock()
	raw, err := ffiCatalogSearchWithRuntime(catalog, source, encoded, runtimeHandle)
	if err != nil {
		return SearchResult{}, err
	}
	if err := ctx.Err(); err != nil {
		return SearchResult{}, err
	}
	return decodeProximitySearchResultBytes(raw)
}
func (a *AcceleratorCatalog) ProveSearch(proximity *ProximityMap, request SearchRequest) (*ProximitySearchProof, error) {
	encoded, err := encodeProximitySearchRequest(cloneSearchRequest(request))
	if err != nil {
		return nil, err
	}
	limits, err := ffiDefaultContentGraphLimits()
	if err != nil {
		return nil, err
	}
	catalog, catalogUnlock, err := a.withHandle()
	if err != nil {
		return nil, err
	}
	defer catalogUnlock()
	source, _, sourceUnlock, err := proximity.withHandle()
	if err != nil {
		return nil, err
	}
	defer sourceUnlock()
	handle, err := ffiCatalogProveSearch(catalog, source, encoded, limits)
	if err != nil {
		return nil, err
	}
	proof := &ProximitySearchProof{handle: handle}
	runtime.SetFinalizer(proof, (*ProximitySearchProof).Close)
	return proof, nil
}
