package main

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"runtime"
	"strconv"
	"time"

	"github.com/dolthub/dolt/go/store/chunks"
	"github.com/dolthub/dolt/go/store/prolly"
	"github.com/dolthub/dolt/go/store/prolly/tree"
	"github.com/dolthub/dolt/go/store/val"
)

const (
	clusterSize       uint64 = 1_000
	contractVersion          = "prolly-compare-v1"
	defaultPointReads uint64 = 100_000
	randomSeed        uint64 = 0x6a09e667f3bcc909
	fnvOffset         uint64 = 0xcbf29ce484222325
	fnvPrime          uint64 = 0x00000100000001b3
)

type phase string

const (
	phaseFresh    phase = "fresh"
	phaseMutation phase = "mutation"
)

type workload string

const (
	workloadAppend    workload = "append"
	workloadRandom    workload = "random"
	workloadClustered workload = "clustered"
)

type arguments struct {
	records  uint64
	phase    phase
	workload workload
}

type operation struct {
	key   val.Tuple
	value val.Tuple
}

type readTarget struct {
	key      val.Tuple
	expected []byte
}

type scenarioResult struct {
	writeOperations uint64
	writeElapsed    time.Duration
	readOperations  uint64
	readElapsed     time.Duration
	scanOperations  uint64
	scanElapsed     time.Duration
	digest          uint64
	resultCount     uint64
}

var benchmarkSink uint64

func main() {
	args, err := parseArgs(os.Args[1:])
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(2)
	}
	result, err := runScenario(context.Background(), args)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	revision := os.Getenv("BENCH_REVISION")
	if revision == "" {
		revision = "unknown"
	}
	fmt.Println(csvHeader())
	emit(revision, args, "write", result.writeOperations, result.writeElapsed, result)
	emit(revision, args, "point_read", result.readOperations, result.readElapsed, result)
	emit(revision, args, "range_scan", result.scanOperations, result.scanElapsed, result)
}

func parseArgs(argv []string) (arguments, error) {
	flags := flag.NewFlagSet("prolly-compare", flag.ContinueOnError)
	flags.SetOutput(io.Discard)
	records := flags.Uint64("records", 0, "base record count")
	phaseName := flags.String("phase", "", "fresh or mutation")
	workloadName := flags.String("workload", "", "append, random, or clustered")
	if err := flags.Parse(argv); err != nil {
		return arguments{}, err
	}
	if flags.NArg() != 0 {
		return arguments{}, fmt.Errorf("unexpected arguments: %v", flags.Args())
	}
	if *records < clusterSize || *records%clusterSize != 0 {
		return arguments{}, fmt.Errorf("records must be a positive multiple of %d", clusterSize)
	}
	parsedPhase := phase(*phaseName)
	if parsedPhase != phaseFresh && parsedPhase != phaseMutation {
		return arguments{}, fmt.Errorf("invalid phase %q; expected fresh or mutation", *phaseName)
	}
	parsedWorkload := workload(*workloadName)
	if parsedWorkload != workloadAppend && parsedWorkload != workloadRandom && parsedWorkload != workloadClustered {
		return arguments{}, fmt.Errorf("invalid workload %q; expected append, random, or clustered", *workloadName)
	}
	return arguments{records: *records, phase: parsedPhase, workload: parsedWorkload}, nil
}

func runScenario(ctx context.Context, args arguments) (scenarioResult, error) {
	storage := &chunks.TestStorage{}
	ns := tree.NewNodeStore(storage.NewView())
	keyDesc := val.NewTupleDescriptor(val.Type{Enc: val.ByteStringEnc, Nullable: false})
	valueDesc := val.NewTupleDescriptor(val.Type{Enc: val.ByteStringEnc, Nullable: false})

	var (
		resultMap     prolly.Map
		writeElapsed  time.Duration
		writeCount    uint64
		digest        uint64
		expectedCount uint64
		err           error
	)
	switch args.phase {
	case phaseFresh:
		operations, preparedDigest, prepErr := prepareOperations(ctx, ns, keyDesc, valueDesc, phaseFresh, args.workload, args.records)
		if prepErr != nil {
			return scenarioResult{}, prepErr
		}
		resultMap, writeElapsed, err = applyOperations(ctx, ns, keyDesc, valueDesc, prolly.Map{}, operations, true)
		writeCount = args.records
		digest = preparedDigest
		expectedCount = args.records
	case phaseMutation:
		baseOperations, _, prepErr := prepareOperations(ctx, ns, keyDesc, valueDesc, phaseFresh, workloadAppend, args.records)
		if prepErr != nil {
			return scenarioResult{}, prepErr
		}
		baseMap, _, applyErr := applyOperations(ctx, ns, keyDesc, valueDesc, prolly.Map{}, baseOperations, true)
		if applyErr != nil {
			return scenarioResult{}, fmt.Errorf("build base map: %w", applyErr)
		}
		baseOperations = nil
		runtime.GC()

		writeCount = args.records * 30 / 100
		operations, preparedDigest, prepErr := prepareOperations(ctx, ns, keyDesc, valueDesc, phaseMutation, args.workload, args.records)
		if prepErr != nil {
			return scenarioResult{}, prepErr
		}
		if err := validateMutationPositions(args.workload, args.records, writeCount); err != nil {
			return scenarioResult{}, err
		}
		resultMap, writeElapsed, err = applyOperations(ctx, ns, keyDesc, valueDesc, baseMap, operations, false)
		digest = preparedDigest
		if args.workload == workloadAppend {
			expectedCount = args.records + writeCount
		} else {
			expectedCount = args.records + writeCount/2
		}
	default:
		return scenarioResult{}, fmt.Errorf("unsupported phase %q", args.phase)
	}
	if err != nil {
		return scenarioResult{}, fmt.Errorf("apply %s/%s operations: %w", args.phase, args.workload, err)
	}
	if digest != workloadDigest(args.phase, args.workload, args.records) {
		return scenarioResult{}, errors.New("prepared operations disagree with workload digest")
	}

	count, err := resultMap.Count()
	if err != nil {
		return scenarioResult{}, fmt.Errorf("count result map: %w", err)
	}
	if uint64(count) != expectedCount {
		return scenarioResult{}, fmt.Errorf("post-write cardinality = %d, want %d", count, expectedCount)
	}
	if err := validateOrderedScan(ctx, resultMap, keyDesc, expectedCount); err != nil {
		return scenarioResult{}, err
	}

	pointReadLimit, err := pointReadLimit()
	if err != nil {
		return scenarioResult{}, err
	}
	targets, err := prepareReadTargets(ctx, ns, keyDesc, args, writeCount, pointReadLimit)
	if err != nil {
		return scenarioResult{}, err
	}
	for _, target := range targets {
		if err := readAndValidate(ctx, resultMap, valueDesc, target, false); err != nil {
			return scenarioResult{}, fmt.Errorf("warm point read: %w", err)
		}
	}

	readStarted := time.Now()
	for _, target := range targets {
		if err := readAndValidate(ctx, resultMap, valueDesc, target, true); err != nil {
			return scenarioResult{}, fmt.Errorf("timed point read: %w", err)
		}
	}
	readElapsed := time.Since(readStarted)

	scanStarted := time.Now()
	scanCount, scannedBytes, err := scanAndConsume(ctx, resultMap, keyDesc, valueDesc)
	scanElapsed := time.Since(scanStarted)
	if err != nil {
		return scenarioResult{}, fmt.Errorf("timed range scan: %w", err)
	}
	if scanCount != expectedCount {
		return scenarioResult{}, fmt.Errorf("timed range scan count = %d, want %d", scanCount, expectedCount)
	}
	benchmarkSink ^= scannedBytes
	runtime.KeepAlive(benchmarkSink)

	return scenarioResult{
		writeOperations: writeCount,
		writeElapsed:    writeElapsed,
		readOperations:  uint64(len(targets)),
		readElapsed:     readElapsed,
		scanOperations:  scanCount,
		scanElapsed:     scanElapsed,
		digest:          digest,
		resultCount:     expectedCount,
	}, nil
}

func applyOperations(
	ctx context.Context,
	ns tree.NodeStore,
	keyDesc, valueDesc *val.TupleDesc,
	base prolly.Map,
	operations []operation,
	empty bool,
) (prolly.Map, time.Duration, error) {
	if empty {
		var err error
		base, err = prolly.NewMapFromTuples(ctx, ns, keyDesc, valueDesc)
		if err != nil {
			return prolly.Map{}, 0, err
		}
	}
	mutable := base.Mutate()
	started := time.Now()
	for _, item := range operations {
		if err := mutable.Put(ctx, item.key, item.value); err != nil {
			return prolly.Map{}, 0, err
		}
	}
	result, err := mutable.Map(ctx)
	elapsed := time.Since(started)
	return result, elapsed, err
}

func prepareOperations(
	ctx context.Context,
	ns tree.NodeStore,
	keyDesc, valueDesc *val.TupleDesc,
	selectedPhase phase,
	selectedWorkload workload,
	records uint64,
) ([]operation, uint64, error) {
	count := records
	if selectedPhase == phaseMutation {
		count = records * 30 / 100
	}
	operations := make([]operation, 0, count)
	keyBuilder := val.NewTupleBuilder(keyDesc, ns)
	valueBuilder := val.NewTupleBuilder(valueDesc, ns)
	digest := fnvOffset
	for index := uint64(0); index < count; index++ {
		var position, generation uint64
		if selectedPhase == phaseFresh {
			position = freshID(selectedWorkload, index, records) * 2
		} else {
			position = mutationPosition(selectedWorkload, index, records, count)
			generation = 1
		}
		logicalKey := keyForPosition(position)
		logicalValue := valueForPosition(position, generation)
		digest = digestOperation(digest, logicalKey, logicalValue)
		keyTuple, err := buildTuple(ctx, keyBuilder, ns, logicalKey)
		if err != nil {
			return nil, 0, fmt.Errorf("build key tuple: %w", err)
		}
		valueTuple, err := buildTuple(ctx, valueBuilder, ns, logicalValue)
		if err != nil {
			return nil, 0, fmt.Errorf("build value tuple: %w", err)
		}
		operations = append(operations, operation{key: keyTuple, value: valueTuple})
	}
	return operations, digest, nil
}

func buildTuple(ctx context.Context, builder *val.TupleBuilder, ns tree.NodeStore, logical []byte) (val.Tuple, error) {
	builder.PutByteString(0, logical)
	return builder.Build(ctx, ns.Pool())
}

func validateMutationPositions(selected workload, records, writes uint64) error {
	maxPosition := records*2 + writes + 1
	seen := make([]byte, (maxPosition+7)/8)
	var updates, inserts uint64
	for index := uint64(0); index < writes; index++ {
		position := mutationPosition(selected, index, records, writes)
		if position >= maxPosition {
			return fmt.Errorf("mutation position %d exceeds validation bound %d", position, maxPosition)
		}
		byteIndex, bitIndex := position/8, position%8
		mask := byte(1 << bitIndex)
		if seen[byteIndex]&mask != 0 {
			return fmt.Errorf("duplicate mutation position %d", position)
		}
		seen[byteIndex] |= mask
		if selected != workloadAppend {
			if position%2 == 0 {
				updates++
			} else {
				inserts++
			}
		}
	}
	if selected != workloadAppend && (updates != writes/2 || inserts != writes-writes/2) {
		return fmt.Errorf("mutation mix updates/inserts = %d/%d, want %d/%d", updates, inserts, writes/2, writes-writes/2)
	}
	return nil
}

func validateOrderedScan(ctx context.Context, m prolly.Map, keyDesc *val.TupleDesc, expected uint64) error {
	iter, err := m.IterAll(ctx)
	if err != nil {
		return fmt.Errorf("open validation scan: %w", err)
	}
	var previous []byte
	var count uint64
	for {
		keyTuple, _, err := iter.Next(ctx)
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return fmt.Errorf("validation scan: %w", err)
		}
		logicalKey, ok := keyDesc.GetBytes(0, keyTuple)
		if !ok {
			return errors.New("validation scan found NULL key")
		}
		if previous != nil && bytes.Compare(previous, logicalKey) >= 0 {
			return fmt.Errorf("range keys are not strictly sorted: %q then %q", previous, logicalKey)
		}
		previous = append(previous[:0], logicalKey...)
		count++
	}
	if count != expected {
		return fmt.Errorf("validation scan count = %d, want %d", count, expected)
	}
	return nil
}

func pointReadLimit() (uint64, error) {
	raw := os.Getenv("PROLLY_COMPARE_POINT_READS")
	if raw == "" {
		return defaultPointReads, nil
	}
	value, err := strconv.ParseUint(raw, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("PROLLY_COMPARE_POINT_READS must be an integer: %w", err)
	}
	return value, nil
}

func prepareReadTargets(
	ctx context.Context,
	ns tree.NodeStore,
	keyDesc *val.TupleDesc,
	args arguments,
	writes, pointReads uint64,
) ([]readTarget, error) {
	available := args.records
	if args.phase == phaseMutation {
		available += writes
	}
	count := min(pointReads, available)
	targets := make([]readTarget, 0, count)
	keyBuilder := val.NewTupleBuilder(keyDesc, ns)
	for index := uint64(0); index < count; index++ {
		var position, generation uint64
		if args.phase == phaseFresh {
			id := permute(index%args.records, args.records, randomSeed^0x5ead0001)
			position = id * 2
		} else {
			position, generation = mutationReadTarget(args.workload, index, args.records, writes)
		}
		key, err := buildTuple(ctx, keyBuilder, ns, keyForPosition(position))
		if err != nil {
			return nil, fmt.Errorf("build read key tuple: %w", err)
		}
		targets = append(targets, readTarget{key: key, expected: valueForPosition(position, generation)})
	}
	return targets, nil
}

func readAndValidate(ctx context.Context, m prolly.Map, valueDesc *val.TupleDesc, target readTarget, consume bool) error {
	found := false
	err := m.Get(ctx, target.key, func(key, value val.Tuple) error {
		if key == nil || value == nil {
			return nil
		}
		logicalValue, ok := valueDesc.GetBytes(0, value)
		if !ok {
			return errors.New("point read found NULL value")
		}
		if !bytes.Equal(logicalValue, target.expected) {
			return errors.New("point-read value mismatch")
		}
		found = true
		if consume {
			benchmarkSink += uint64(len(logicalValue))
		}
		return nil
	})
	if err != nil {
		return err
	}
	if !found {
		return errors.New("point-read key does not exist")
	}
	return nil
}

func scanAndConsume(ctx context.Context, m prolly.Map, keyDesc, valueDesc *val.TupleDesc) (uint64, uint64, error) {
	iter, err := m.IterAll(ctx)
	if err != nil {
		return 0, 0, err
	}
	var count, scannedBytes uint64
	for {
		keyTuple, valueTuple, err := iter.Next(ctx)
		if errors.Is(err, io.EOF) {
			return count, scannedBytes, nil
		}
		if err != nil {
			return 0, 0, err
		}
		key, keyOK := keyDesc.GetBytes(0, keyTuple)
		value, valueOK := valueDesc.GetBytes(0, valueTuple)
		if !keyOK || !valueOK {
			return 0, 0, errors.New("range scan found NULL key or value")
		}
		scannedBytes += uint64(len(key) + len(value))
		count++
	}
}

func freshID(selected workload, index, records uint64) uint64 {
	switch selected {
	case workloadAppend:
		return index
	case workloadRandom:
		return permute(index, records, randomSeed^records)
	case workloadClustered:
		blocks := records / clusterSize
		block := index / clusterSize
		offset := index % clusterSize
		return permute(block, blocks, randomSeed^0xc1a57e2d)*clusterSize + offset
	default:
		panic("unsupported workload")
	}
}

func mutationPosition(selected workload, index, records, writes uint64) uint64 {
	switch selected {
	case workloadAppend:
		return records*2 + index
	case workloadRandom:
		ordinal := index / 2
		if index%2 == 0 {
			return permute(ordinal, records, randomSeed^0xa11ce001) * 2
		}
		return permute(ordinal, records, randomSeed^0x1a5e2701)*2 + 1
	case workloadClustered:
		updates := writes / 2
		inserts := writes - updates
		width := max(updates, inserts)
		start := (records - width) / 2
		ordinal := index / 2
		if index%2 == 0 {
			return (start + ordinal) * 2
		}
		return (start+ordinal)*2 + 1
	default:
		panic("unsupported workload")
	}
}

func mutationReadTarget(selected workload, index, records, writes uint64) (uint64, uint64) {
	switch selected {
	case workloadAppend:
		if index%2 == 0 {
			return ((index / 2) % records) * 2, 0
		}
		return records*2 + (index/2)%writes, 1
	case workloadRandom, workloadClustered:
		updates := writes / 2
		inserts := writes - updates
		switch index % 3 {
		case 0:
			op := 2 * ((index / 3) % updates)
			return mutationPosition(selected, op, records, writes), 1
		case 1:
			op := 2*((index/3)%inserts) + 1
			return mutationPosition(selected, op, records, writes), 1
		default:
			unchangedOrdinal := (index / 3) % (records - updates)
			if selected == workloadRandom {
				id := permute(updates+unchangedOrdinal, records, randomSeed^0xa11ce001)
				return id * 2, 0
			}
			width := max(updates, inserts)
			start := (records - width) / 2
			return (unchangedOrdinal % start) * 2, 0
		}
	default:
		panic("unsupported workload")
	}
}

func keyForPosition(position uint64) []byte {
	return []byte(fmt.Sprintf("key-%020d", position))
}

func valueForPosition(position, generation uint64) []byte {
	state := mix64(position ^ generation*0x9e3779b97f4a7c15)
	length := state%100 + 1
	value := make([]byte, length)
	for index := range value {
		state = mix64(state + uint64(index) + 0x9e3779b9)
		value[index] = byte(state)
	}
	return value
}

func permute(index, count, seed uint64) uint64 {
	if count <= 1 {
		return 0
	}
	multiplier := (mix64(seed) % count) | 1
	for gcd(multiplier, count) != 1 {
		multiplier = (multiplier + 2) % count
		if multiplier == 0 {
			multiplier = 1
		}
	}
	offset := mix64(seed^0xd1b54a32d192ed03) % count
	return (multiplier*index + offset) % count
}

func gcd(left, right uint64) uint64 {
	for right != 0 {
		left, right = right, left%right
	}
	return left
}

func mix64(value uint64) uint64 {
	value = (value ^ (value >> 30)) * 0xbf58476d1ce4e5b9
	value = (value ^ (value >> 27)) * 0x94d049bb133111eb
	return value ^ (value >> 31)
}

func workloadDigest(selectedPhase phase, selectedWorkload workload, records uint64) uint64 {
	count := records
	if selectedPhase == phaseMutation {
		count = records * 30 / 100
	}
	digest := uint64(fnvOffset)
	for index := uint64(0); index < count; index++ {
		var position, generation uint64
		if selectedPhase == phaseFresh {
			position = freshID(selectedWorkload, index, records) * 2
		} else {
			position = mutationPosition(selectedWorkload, index, records, count)
			generation = 1
		}
		digest = digestOperation(digest, keyForPosition(position), valueForPosition(position, generation))
	}
	return digest
}

func digestOperation(digest uint64, key, value []byte) uint64 {
	var length [4]byte
	binary.BigEndian.PutUint32(length[:], uint32(len(key)))
	digest = digestBytes(digest, length[:])
	digest = digestBytes(digest, key)
	binary.BigEndian.PutUint32(length[:], uint32(len(value)))
	digest = digestBytes(digest, length[:])
	return digestBytes(digest, value)
}

func digestBytes(digest uint64, data []byte) uint64 {
	for _, value := range data {
		digest ^= uint64(value)
		digest *= fnvPrime
	}
	return digest
}

func csvHeader() string {
	return "implementation,revision,contract_version,records,phase,workload,operation,operations,elapsed_ns,ns_per_op,ops_per_sec,workload_digest,result_count,validated"
}

func emit(revision string, args arguments, operationName string, operations uint64, elapsed time.Duration, result scenarioResult) {
	elapsedNS := elapsed.Nanoseconds()
	nsPerOp := float64(elapsedNS) / float64(max(operations, 1))
	opsPerSecond := float64(operations) * 1_000_000_000 / float64(max(elapsedNS, 1))
	fmt.Printf(
		"dolt-go,%s,%s,%d,%s,%s,%s,%d,%d,%.3f,%.3f,%016x,%d,true\n",
		revision,
		contractVersion,
		args.records,
		args.phase,
		args.workload,
		operationName,
		operations,
		elapsedNS,
		nsPerOp,
		opsPerSecond,
		result.digest,
		result.resultCount,
	)
}
