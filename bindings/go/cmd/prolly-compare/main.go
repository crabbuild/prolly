// Command prolly-compare benchmarks the Rust prolly tree through its public Go
// binding. Its deterministic workload contract matches src/bin/prolly_compare.rs
// and dolt/go/cmd/prolly-compare/main.go.
package main

import (
	"bytes"
	"flag"
	"fmt"
	"os"
	"runtime"
	"strconv"
	"time"

	prolly "build.crab/prolly-go"
)

const (
	clusterSize       = 1_000
	defaultPointReads = 100_000
	randomSeed        = uint64(0x6a09e667f3bcc909)
	fnvOffset         = uint64(0xcbf29ce484222325)
	fnvPrime          = uint64(0x00000100000001b3)
)

type phase string

const (
	freshPhase    phase = "fresh"
	mutationPhase phase = "mutation"
)

type workload string

const (
	appendWorkload    workload = "append"
	randomWorkload    workload = "random"
	clusteredWorkload workload = "clustered"
)

type arguments struct {
	records  int
	phase    phase
	workload workload
}

type scenarioResult struct {
	writeOperations int
	writeElapsed    time.Duration
	readOperations  int
	readElapsed     time.Duration
	scanOperations  int
	scanElapsed     time.Duration
	digest          uint64
	resultCount     int
}

type readTarget struct {
	key      []byte
	expected []byte
}

func main() {
	runtime.GOMAXPROCS(1)
	args := parseArgs()
	if args.records < clusterSize || args.records%clusterSize != 0 {
		panic(fmt.Sprintf("records must be a positive multiple of %d", clusterSize))
	}

	result := runScenario(args)
	revision := os.Getenv("BENCH_REVISION")
	if revision == "" {
		revision = "unknown"
	}

	fmt.Println("implementation,revision,records,phase,workload,operation,operations,elapsed_ns,ns_per_op,ops_per_sec,workload_digest,result_count,validated")
	emit(revision, args, "write", result.writeOperations, result.writeElapsed, result.digest, result.resultCount)
	emit(revision, args, "point_read", result.readOperations, result.readElapsed, result.digest, result.resultCount)
	emit(revision, args, "range_scan", result.scanOperations, result.scanElapsed, result.digest, result.resultCount)
}

func parseArgs() arguments {
	records := flag.Int("records", 0, "base record count")
	phaseValue := flag.String("phase", "", "fresh or mutation")
	workloadValue := flag.String("workload", "", "append, random, or clustered")
	flag.Parse()
	p := phase(*phaseValue)
	if p != freshPhase && p != mutationPhase {
		panic("--phase must be fresh or mutation")
	}
	w := workload(*workloadValue)
	if w != appendWorkload && w != randomWorkload && w != clusteredWorkload {
		panic("--workload must be append, random, or clustered")
	}
	if *records <= 0 {
		panic("--records is required")
	}
	return arguments{records: *records, phase: p, workload: w}
}

func runScenario(args arguments) scenarioResult {
	config, err := prolly.DefaultConfig()
	must(err)
	engine, err := prolly.Memory(config)
	must(err)
	defer engine.Close()

	var tree prolly.Tree
	var writeOperations, resultCount int
	var writeElapsed time.Duration
	var digest uint64
	if args.phase == freshPhase {
		writeOperations = args.records
		tree, writeElapsed, digest = buildFresh(engine, args.records, args.workload)
		resultCount = args.records
	} else {
		tree, _, _ = buildFresh(engine, args.records, appendWorkload)
		writeOperations = args.records * 30 / 100
		tree, writeElapsed, digest = applyMutations(engine, tree, args.records, writeOperations, args.workload)
		inserts := writeOperations
		if args.workload != appendWorkload {
			inserts = writeOperations - writeOperations/2
		}
		resultCount = args.records + inserts
	}

	targets := makeReadTargets(args.phase, args.workload, args.records, writeOperations)
	for _, target := range targets {
		validateGet(engine, tree, target)
	}

	readStarted := time.Now()
	observedBytes := 0
	for _, target := range targets {
		observedBytes += validateGet(engine, tree, target)
	}
	readElapsed := time.Since(readStarted)
	runtime.KeepAlive(observedBytes)

	validateRange(engine, tree, resultCount)
	scanCount := 0
	scannedBytes := 0
	scanStarted := time.Now()
	outcome, err := engine.ScanRange(tree, []byte{}, nil, func(entry prolly.Entry) bool {
		scannedBytes += len(entry.Key) + len(entry.Value)
		scanCount++
		return true
	})
	scanElapsed := time.Since(scanStarted)
	must(err)
	if outcome.Stopped || int(outcome.Visited) != resultCount || scanCount != resultCount {
		panic(fmt.Sprintf("range scan cardinality mismatch: visited=%d callback=%d want=%d", outcome.Visited, scanCount, resultCount))
	}
	runtime.KeepAlive(scannedBytes)

	return scenarioResult{
		writeOperations: writeOperations,
		writeElapsed:    writeElapsed,
		readOperations:  len(targets),
		readElapsed:     readElapsed,
		scanOperations:  scanCount,
		scanElapsed:     scanElapsed,
		digest:          digest,
		resultCount:     resultCount,
	}
}

func buildFresh(engine *prolly.Engine, records int, w workload) (prolly.Tree, time.Duration, uint64) {
	tree, err := engine.Create()
	must(err)
	mutations := make([]prolly.Mutation, 0, records)
	digest := fnvOffset
	for index := 0; index < records; index++ {
		id := freshID(w, index, records)
		position := id * 2
		key := keyForPosition(position)
		value := valueForPosition(position, 0)
		digest = digestOperation(digest, key, value)
		mutations = append(mutations, prolly.UpsertMutation(key, value))
	}
	started := time.Now()
	tree, err = engine.Batch(tree, mutations)
	elapsed := time.Since(started)
	must(err)
	runtime.KeepAlive(mutations)
	return tree, elapsed, digest
}

func applyMutations(engine *prolly.Engine, tree prolly.Tree, records, writes int, w workload) (prolly.Tree, time.Duration, uint64) {
	mutations := make([]prolly.Mutation, 0, writes)
	digest := fnvOffset
	for index := 0; index < writes; index++ {
		position := mutationPosition(w, index, records, writes)
		key := keyForPosition(position)
		value := valueForPosition(position, 1)
		digest = digestOperation(digest, key, value)
		mutations = append(mutations, prolly.UpsertMutation(key, value))
	}
	started := time.Now()
	var err error
	if w == appendWorkload {
		tree, err = engine.AppendBatch(tree, mutations)
	} else {
		tree, err = engine.Batch(tree, mutations)
	}
	elapsed := time.Since(started)
	must(err)
	runtime.KeepAlive(mutations)
	return tree, elapsed, digest
}

func validateGet(engine *prolly.Engine, tree prolly.Tree, target readTarget) int {
	actual, found, err := engine.Get(tree, target.key)
	must(err)
	if !found || !bytes.Equal(actual, target.expected) {
		panic(fmt.Sprintf("point-read mismatch for %q", target.key))
	}
	return len(actual)
}

func validateRange(engine *prolly.Engine, tree prolly.Tree, resultCount int) {
	var previous []byte
	count := 0
	checksum := fnvOffset
	outcome, err := engine.ScanRange(tree, []byte{}, nil, func(entry prolly.Entry) bool {
		if previous != nil && bytes.Compare(previous, entry.Key) >= 0 {
			panic("range keys are not strictly sorted")
		}
		checksum = digestBytes(checksum, entry.Key)
		checksum = digestBytes(checksum, entry.Value)
		previous = append(previous[:0], entry.Key...)
		count++
		return true
	})
	must(err)
	if outcome.Stopped || int(outcome.Visited) != resultCount || count != resultCount {
		panic(fmt.Sprintf("range validation cardinality mismatch: got=%d want=%d", count, resultCount))
	}
	runtime.KeepAlive(checksum)
}

func makeReadTargets(p phase, w workload, records, writes int) []readTarget {
	count := records
	if p == mutationPhase {
		count = records + writes
	}
	pointReads := defaultPointReads
	if value := os.Getenv("PROLLY_COMPARE_POINT_READS"); value != "" {
		parsed, err := strconv.Atoi(value)
		must(err)
		pointReads = parsed
	}
	if count > pointReads {
		count = pointReads
	}
	targets := make([]readTarget, 0, count)
	for index := 0; index < count; index++ {
		position, generation := 0, uint64(0)
		if p == freshPhase {
			id := permute(index%records, records, randomSeed^0x5ead0001)
			position = id * 2
		} else {
			position, generation = mutationReadTarget(w, index, records, writes)
		}
		targets = append(targets, readTarget{
			key:      keyForPosition(position),
			expected: valueForPosition(position, generation),
		})
	}
	return targets
}

func freshID(w workload, index, records int) int {
	switch w {
	case appendWorkload:
		return index
	case randomWorkload:
		return permute(index, records, randomSeed^uint64(records))
	case clusteredWorkload:
		blocks := records / clusterSize
		return permute(index/clusterSize, blocks, randomSeed^0xc1a57e2d)*clusterSize + index%clusterSize
	default:
		panic("unreachable workload")
	}
}

func mutationPosition(w workload, index, records, writes int) int {
	switch w {
	case appendWorkload:
		return records*2 + index
	case randomWorkload:
		ordinal := index / 2
		if index%2 == 0 {
			return permute(ordinal, records, randomSeed^0xa11ce001) * 2
		}
		return permute(ordinal, records, randomSeed^0x1a5e2701)*2 + 1
	case clusteredWorkload:
		updates, inserts := writes/2, writes-writes/2
		width := updates
		if inserts > width {
			width = inserts
		}
		start, ordinal := (records-width)/2, index/2
		if index%2 == 0 {
			return (start + ordinal) * 2
		}
		return (start+ordinal)*2 + 1
	default:
		panic("unreachable workload")
	}
}

func mutationReadTarget(w workload, index, records, writes int) (int, uint64) {
	if w == appendWorkload {
		if index%2 == 0 {
			return ((index / 2) % records) * 2, 0
		}
		return records*2 + (index/2)%writes, 1
	}
	updates, inserts := writes/2, writes-writes/2
	switch index % 3 {
	case 0:
		return mutationPosition(w, 2*((index/3)%updates), records, writes), 1
	case 1:
		return mutationPosition(w, 2*((index/3)%inserts)+1, records, writes), 1
	default:
		unchangedOrdinal := (index / 3) % (records - updates)
		if w == randomWorkload {
			return permute(updates+unchangedOrdinal, records, randomSeed^0xa11ce001) * 2, 0
		}
		width := updates
		if inserts > width {
			width = inserts
		}
		start := (records - width) / 2
		return (unchangedOrdinal % start) * 2, 0
	}
}

func keyForPosition(position int) []byte {
	return []byte(fmt.Sprintf("key-%020d", position))
}

func valueForPosition(position int, generation uint64) []byte {
	state := mix64(uint64(position) ^ generation*0x9e3779b97f4a7c15)
	value := make([]byte, int(state%100)+1)
	for index := range value {
		state = mix64(state + uint64(index) + 0x9e3779b9)
		value[index] = byte(state)
	}
	return value
}

func permute(index, count int, seed uint64) int {
	if count <= 1 {
		return 0
	}
	multiplier := int(mix64(seed)%uint64(count)) | 1
	for gcd(multiplier, count) != 1 {
		multiplier = (multiplier + 2) % count
		if multiplier == 0 {
			multiplier = 1
		}
	}
	offset := int(mix64(seed^0xd1b54a32d192ed03) % uint64(count))
	return (multiplier*index + offset) % count
}

func gcd(left, right int) int {
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

func digestOperation(digest uint64, key, value []byte) uint64 {
	digest = digestBytes(digest, []byte{byte(len(key) >> 24), byte(len(key) >> 16), byte(len(key) >> 8), byte(len(key))})
	digest = digestBytes(digest, key)
	digest = digestBytes(digest, []byte{byte(len(value) >> 24), byte(len(value) >> 16), byte(len(value) >> 8), byte(len(value))})
	return digestBytes(digest, value)
}

func digestBytes(digest uint64, data []byte) uint64 {
	for _, value := range data {
		digest ^= uint64(value)
		digest *= fnvPrime
	}
	return digest
}

func emit(revision string, args arguments, operation string, operations int, elapsed time.Duration, digest uint64, resultCount int) {
	elapsedNS := elapsed.Nanoseconds()
	nsPerOp := float64(elapsedNS) / float64(max(operations, 1))
	opsPerSecond := float64(operations) * 1_000_000_000 / float64(max64(elapsedNS, 1))
	fmt.Printf("rust-go-binding,%s,%d,%s,%s,%s,%d,%d,%.3f,%.3f,%016x,%d,true\n",
		revision, args.records, args.phase, args.workload, operation, operations, elapsedNS, nsPerOp, opsPerSecond, digest, resultCount)
}

func must(err error) {
	if err != nil {
		panic(err)
	}
}

func max(left, right int) int {
	if left > right {
		return left
	}
	return right
}

func max64(left, right int64) int64 {
	if left > right {
		return left
	}
	return right
}
