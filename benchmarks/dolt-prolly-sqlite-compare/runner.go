package main

import (
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"time"

	"github.com/dolthub/dolt/go/store/hash"
	"github.com/dolthub/dolt/go/store/prolly"
	"github.com/dolthub/dolt/go/store/prolly/tree"
	"github.com/dolthub/dolt/go/store/val"
)

func buildFixture(ctx context.Context, spec fixtureSpec) (protocolRow, error) {
	row := baseRow("fixture", spec.revision, spec.records, spec.repetition, "build", "n/a", "n/a")
	row.LogicalOperations = spec.records
	row.ExpectedEntries = spec.records
	layout := fixtureLayout{output: spec.output, records: spec.records, repetition: spec.repetition}
	if _, err := os.Lstat(layout.sourceDir()); !os.IsNotExist(err) {
		return failed(row, fmt.Errorf("fixture already exists: %s", layout.sourceDir()))
	}
	if err := os.MkdirAll(layout.sourceDir(), 0o755); err != nil {
		return failed(row, err)
	}
	store, err := openSQLiteChunkStore(layout.sourceDB())
	if err != nil {
		return failed(row, err)
	}
	ns := tree.NewNodeStore(store)
	codec := newMapCodec(ns)
	tuples, err := codec.tuples(ctx, spec.records)
	if err != nil {
		store.Close()
		return failed(row, err)
	}
	store.resetMetrics()
	started := time.Now()
	m, err := prolly.NewMapFromTuples(ctx, ns, codec.keyDesc, codec.valueDesc, tuples...)
	if err == nil {
		err = store.flushPending(ctx)
	}
	row.TotalNS = elapsedNS(started)
	metrics := store.snapshotMetrics()
	if err != nil {
		store.Close()
		return failed(row, err)
	}
	if err := publishMap(ctx, store, m, hash.Hash{}); err != nil {
		store.Close()
		return failed(row, err)
	}
	if err := validateMap(ctx, m, codec, spec.records, nil); err != nil {
		store.Close()
		return failed(row, err)
	}
	if err := store.checkpoint(ctx); err != nil {
		store.Close()
		return failed(row, err)
	}
	if err := store.Close(); err != nil {
		return failed(row, err)
	}
	reopened, err := openSQLiteChunkStore(layout.sourceDB())
	if err != nil {
		return failed(row, err)
	}
	loaded, reopenedCodec, err := loadMap(ctx, reopened)
	if err == nil {
		err = validateMap(ctx, loaded, reopenedCodec, spec.records, nil)
	}
	closeErr := reopened.Close()
	if err != nil {
		return failed(row, err)
	}
	if closeErr != nil {
		return failed(row, closeErr)
	}
	row.ObservedItems = spec.records
	row.ObservedEntries = spec.records
	row.ResultEntries = spec.records
	row.NSPerOperation = float64(row.TotalNS) / float64(max(spec.records, 1))
	row.OperationsPerSecond = rate(spec.records, row.TotalNS)
	setMetrics(&row, metrics)
	row.DBBytes, row.WALBytes, row.SHMBytes, row.TotalDatabaseBytes, err = sqliteFileBytes(layout.sourceDB())
	if err != nil {
		return failed(row, err)
	}
	row.Validated = true
	return row, nil
}

type cellOutcome struct {
	result        prolly.Map
	changed       map[int]byte
	observedItems int
	latencies     []uint64
	totalNS       uint64
	queryStrategy *string
}

func runCell(ctx context.Context, spec cellSpec) (protocolRow, error) {
	row := baseRow("cell", spec.revision, spec.records, spec.repetition, string(spec.operation), string(spec.pattern), string(spec.cacheState))
	row.LogicalOperations = spec.logicalOperations()
	row.ExpectedEntries = spec.expectedEntries()
	layout := fixtureLayout{output: spec.output, records: spec.records, repetition: spec.repetition}
	cellDir := layout.cellDir(spec)
	if err := cloneFixture(layout.sourceDir(), cellDir); err != nil {
		return failed(row, err)
	}
	database := layout.cellDB(spec)
	store, err := openSQLiteChunkStore(database)
	if err != nil {
		return failed(row, err)
	}
	baseRoot, err := store.Root(ctx)
	if err != nil {
		store.Close()
		return failed(row, err)
	}
	base, codec, err := loadMap(ctx, store)
	if err != nil {
		store.Close()
		return failed(row, err)
	}
	outcome, err := executeCell(ctx, store, base, codec, spec)
	metrics := store.snapshotMetrics()
	if err != nil {
		store.Close()
		return failed(row, fmt.Errorf("%s/%s: %w", spec.operation, spec.pattern, err))
	}
	observedEntries, err := outcome.result.Count()
	if err != nil {
		store.Close()
		return failed(row, err)
	}
	if observedEntries != spec.expectedEntries() {
		store.Close()
		return failed(row, fmt.Errorf("observed %d entries, expected %d", observedEntries, spec.expectedEntries()))
	}
	if err := validateMap(ctx, outcome.result, codec, spec.expectedEntries(), outcome.changed); err != nil {
		store.Close()
		return failed(row, err)
	}
	mutating := spec.operation == opPut || spec.operation == opBatch || spec.operation == opMerge
	if mutating {
		if err := publishMap(ctx, store, outcome.result, baseRoot); err != nil {
			store.Close()
			return failed(row, err)
		}
	}
	row.TotalNS = outcome.totalNS
	row.NSPerOperation = float64(row.TotalNS) / float64(max(spec.logicalOperations(), 1))
	row.OperationsPerSecond = rate(spec.logicalOperations(), row.TotalNS)
	row.ObservedItems = outcome.observedItems
	row.ObservedEntries = observedEntries
	row.ResultEntries = observedEntries
	row.QueryStrategy = outcome.queryStrategy
	setMetrics(&row, metrics)
	if len(outcome.latencies) > 0 {
		row.P50NS = nearestRank(outcome.latencies, .50)
		row.P95NS = nearestRank(outcome.latencies, .95)
		row.P99NS = nearestRank(outcome.latencies, .99)
		row.MaxNS = nearestRank(outcome.latencies, 1)
	}
	row.DBBytes, row.WALBytes, row.SHMBytes, row.TotalDatabaseBytes, err = sqliteFileBytes(database)
	if err != nil {
		store.Close()
		return failed(row, err)
	}
	if err := store.Close(); err != nil {
		return failed(row, err)
	}
	if mutating {
		reopened, openErr := openSQLiteChunkStore(database)
		if openErr != nil {
			return failed(row, openErr)
		}
		loaded, reopenedCodec, openErr := loadMap(ctx, reopened)
		if openErr == nil {
			openErr = validateMap(ctx, loaded, reopenedCodec, spec.expectedEntries(), outcome.changed)
		}
		closeErr := reopened.Close()
		if openErr != nil {
			return failed(row, openErr)
		}
		if closeErr != nil {
			return failed(row, closeErr)
		}
	}
	row.Validated = true
	if err := safeRemove(filepath.Join(spec.output, "cells"), cellDir); err != nil {
		return failed(row, err)
	}
	return row, nil
}

func executeCell(ctx context.Context, store *sqliteChunkStore, base prolly.Map, codec *mapCodec, spec cellSpec) (cellOutcome, error) {
	switch spec.operation {
	case opPut, opBatch:
		count := spec.changes
		if spec.operation == opPut {
			count = 1
		}
		ids := mutationIDs(spec.pattern, spec.records, count, 1)
		store.resetMetrics()
		started := time.Now()
		result, err := applyBatch(ctx, base, codec, ids, 1)
		if err == nil {
			err = store.flushPending(ctx)
		}
		elapsed := elapsedNS(started)
		if err != nil {
			return cellOutcome{}, err
		}
		changed := make(map[int]byte, len(ids))
		for _, id := range ids {
			changed[id] = 1
		}
		return cellOutcome{result: result, changed: changed, observedItems: len(ids), totalNS: elapsed}, nil
	case opGetCold, opGetWarm, opQuery:
		ids := readIDs(spec.pattern, spec.records, spec.readSamples)
		if spec.operation == opGetWarm {
			for _, id := range ids {
				if err := codec.assertValue(ctx, base, id, 0); err != nil {
					return cellOutcome{}, err
				}
			}
		}
		store.resetMetrics()
		latencies := make([]uint64, 0, len(ids))
		startedAll := time.Now()
		for _, id := range ids {
			if spec.operation == opGetCold {
				base.NodeStore().PurgeCaches()
			}
			started := time.Now()
			if err := codec.assertValue(ctx, base, id, 0); err != nil {
				return cellOutcome{}, err
			}
			if spec.operation != opQuery {
				latencies = append(latencies, elapsedNS(started))
			}
		}
		var strategy *string
		if spec.operation == opQuery {
			value := "repeated_map_get"
			strategy = &value
		}
		return cellOutcome{result: base, observedItems: len(ids), latencies: latencies, totalNS: elapsedNS(startedAll), queryStrategy: strategy}, nil
	case opScan, opFullScan:
		ids := rangeIDs(spec.pattern, spec.records, spec.readSamples)
		if spec.operation == opFullScan {
			ids = make([]int, spec.records)
			for i := range ids {
				ids[i] = i
			}
		}
		store.resetMetrics()
		started := time.Now()
		observed, err := consumeScan(ctx, base, codec, ids, spec.operation == opFullScan)
		return cellOutcome{result: base, observedItems: observed, totalNS: elapsedNS(started)}, err
	case opDiff:
		ids := mutationIDs(spec.pattern, spec.records, spec.changes, 2)
		changedMap, err := applyBatch(ctx, base, codec, ids, 1)
		if err == nil {
			err = store.flushPending(ctx)
		}
		if err != nil {
			return cellOutcome{}, err
		}
		expected := make(map[string]struct{}, len(ids))
		for _, id := range ids {
			expected[string(key(id))] = struct{}{}
		}
		store.resetMetrics()
		observed := 0
		started := time.Now()
		err = prolly.DiffMaps(ctx, base, changedMap, false, func(_ context.Context, diff tree.Diff) error {
			logical, ok := codec.keyDesc.GetBytes(0, val.Tuple(diff.Key))
			if !ok {
				return fmt.Errorf("diff key is not bytes")
			}
			if _, ok := expected[string(logical)]; !ok {
				return fmt.Errorf("unexpected diff key %q", logical)
			}
			delete(expected, string(logical))
			observed++
			return nil
		})
		elapsed := elapsedNS(started)
		if err == io.EOF {
			err = nil
		}
		if err != nil {
			return cellOutcome{}, err
		}
		if len(expected) != 0 || observed != len(ids) {
			return cellOutcome{}, fmt.Errorf("diff count mismatch: observed %d expected %d", observed, len(ids))
		}
		changed := make(map[int]byte, len(ids))
		for _, id := range ids {
			changed[id] = 1
		}
		return cellOutcome{result: changedMap, changed: changed, observedItems: observed, totalNS: elapsed}, nil
	case opMerge:
		leftIDs, rightIDs, err := mergeIDs(spec.records, spec.changes, spec.pattern)
		if err != nil {
			return cellOutcome{}, err
		}
		left, err := applyBatch(ctx, base, codec, leftIDs, 1)
		if err == nil {
			err = store.flushPending(ctx)
		}
		if err != nil {
			return cellOutcome{}, err
		}
		right, err := applyBatch(ctx, base, codec, rightIDs, 2)
		if err == nil {
			err = store.flushPending(ctx)
		}
		if err != nil {
			return cellOutcome{}, err
		}
		collision := false
		store.resetMetrics()
		started := time.Now()
		merged, _, err := prolly.MergeMaps(ctx, left, right, base, func(_, _ tree.Diff) (tree.Diff, bool) { collision = true; return tree.Diff{}, false })
		if err == nil {
			err = store.flushPending(ctx)
		}
		elapsed := elapsedNS(started)
		if err != nil {
			return cellOutcome{}, err
		}
		if collision {
			return cellOutcome{}, fmt.Errorf("disjoint merge invoked collision resolver")
		}
		changed := make(map[int]byte, len(leftIDs)+len(rightIDs))
		for _, id := range leftIDs {
			changed[id] = 1
		}
		for _, id := range rightIDs {
			changed[id] = 2
		}
		return cellOutcome{result: merged, changed: changed, observedItems: len(changed), totalNS: elapsed}, nil
	default:
		return cellOutcome{}, fmt.Errorf("unsupported operation %q", spec.operation)
	}
}

func consumeScan(ctx context.Context, m prolly.Map, codec *mapCodec, ids []int, full bool) (int, error) {
	var iterator prolly.MapIter
	var err error
	if full {
		iterator, err = m.IterAll(ctx)
	} else {
		startTuple, tupleErr := codec.keyTuple(ctx, ids[0])
		if tupleErr != nil {
			return 0, tupleErr
		}
		stopTuple, tupleErr := codec.keyTuple(ctx, ids[0]+len(ids))
		if tupleErr != nil {
			return 0, tupleErr
		}
		iterator, err = m.IterKeyRange(ctx, startTuple, stopTuple)
	}
	if err != nil {
		return 0, err
	}
	observed := 0
	for {
		keyTuple, valueTuple, nextErr := iterator.Next(ctx)
		if nextErr == io.EOF {
			break
		}
		if nextErr != nil {
			return observed, nextErr
		}
		if observed >= len(ids) {
			return observed, fmt.Errorf("scan returned too many rows")
		}
		logicalKey, keyOK := codec.keyDesc.GetBytes(0, keyTuple)
		logicalValue, valueOK := codec.valueDesc.GetBytes(0, valueTuple)
		id := ids[observed]
		if !keyOK || !valueOK || string(logicalKey) != string(key(id)) || string(logicalValue) != string(value(id, 0)) {
			return observed, fmt.Errorf("scan returned wrong record at index %d", observed)
		}
		observed++
	}
	if observed != len(ids) {
		return observed, fmt.Errorf("scan returned %d rows, expected %d", observed, len(ids))
	}
	return observed, nil
}

func validateMap(ctx context.Context, m prolly.Map, codec *mapCodec, expected int, changed map[int]byte) error {
	count, err := m.Count()
	if err != nil {
		return err
	}
	if count != expected {
		return fmt.Errorf("map has %d records, expected %d", count, expected)
	}
	if expected > 0 {
		for _, id := range []int{0, expected / 2, expected - 1} {
			generation := byte(0)
			if changed != nil {
				generation = changed[id]
			}
			if err := codec.assertValue(ctx, m, id, generation); err != nil {
				return err
			}
		}
	}
	for id, generation := range changed {
		if err := codec.assertValue(ctx, m, id, generation); err != nil {
			return err
		}
	}
	return nil
}

func baseRow(kind, revision string, records, repetition int, op, selectedPattern, cache string) protocolRow {
	return protocolRow{ContractVersion: contractVersion, Kind: kind, Implementation: "dolt-go", Revision: revision, Records: records, Repetition: repetition, Operation: op, Pattern: selectedPattern, CacheState: cache}
}

func failed(row protocolRow, err error) (protocolRow, error) {
	row.Error = err.Error()
	return row, err
}
func elapsedNS(started time.Time) uint64 {
	value := time.Since(started).Nanoseconds()
	if value < 1 {
		return 1
	}
	return uint64(value)
}
func setMetrics(row *protocolRow, metrics storeMetrics) {
	row.ChunkReads = pointer(metrics.chunkReads)
	row.ChunkWrites = pointer(metrics.chunkWrites)
	row.BytesRead = pointer(metrics.bytesRead)
	row.BytesWritten = pointer(metrics.bytesWritten)
}
