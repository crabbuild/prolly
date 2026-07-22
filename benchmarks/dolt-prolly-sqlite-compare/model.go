package main

import (
	"bytes"
	"fmt"
	"math/bits"
	"sort"
)

const (
	contractVersion = "sqlite-scale-v2"
	randomSeed      = uint64(0x6a09e667f3bcc909)
)

type operation string

const (
	opPut      operation = "put"
	opBatch    operation = "batch"
	opGetCold  operation = "get_cold"
	opGetWarm  operation = "get_warm"
	opQuery    operation = "query"
	opScan     operation = "scan"
	opFullScan operation = "full_scan"
	opDiff     operation = "diff"
	opMerge    operation = "merge"
)

var allOperations = []operation{opPut, opBatch, opGetCold, opGetWarm, opQuery, opScan, opFullScan, opDiff, opMerge}

type pattern string

const (
	patternAppend    pattern = "append"
	patternRandom    pattern = "random"
	patternClustered pattern = "clustered"
)

var allPatterns = []pattern{patternAppend, patternRandom, patternClustered}

type cacheState string

const (
	cacheNA   cacheState = "n/a"
	cacheCold cacheState = "cold-manager"
	cacheWarm cacheState = "warm-manager"
)

type fixtureSpec struct {
	output     string
	records    int
	repetition int
	revision   string
}

type cellSpec struct {
	output      string
	records     int
	repetition  int
	operation   operation
	pattern     pattern
	cacheState  cacheState
	changes     int
	readSamples int
	revision    string
}

func (s cellSpec) logicalOperations() int {
	switch s.operation {
	case opPut:
		return 1
	case opBatch, opDiff, opMerge:
		return s.changes
	case opGetCold, opGetWarm, opQuery, opScan:
		return s.readSamples
	case opFullScan:
		return s.records
	default:
		return 0
	}
}

func (s cellSpec) expectedEntries() int {
	if s.pattern != patternAppend {
		return s.records
	}
	switch s.operation {
	case opPut:
		return s.records + 1
	case opBatch, opDiff, opMerge:
		return s.records + s.changes
	default:
		return s.records
	}
}

func key(id int) []byte { return []byte(fmt.Sprintf("key-%020d", id)) }

func value(id int, generation byte) []byte {
	prefix := []byte(fmt.Sprintf("value-%020d-%02d-", id, generation))
	return append(prefix, bytes.Repeat([]byte{'x'}, 100-len(prefix))...)
}

func changeCount(records int) int {
	count := (records*30 + 99) / 100
	if count < 1 {
		count = 1
	}
	if count > records {
		count = records
	}
	return count
}

func mutationIDs(selected pattern, records, count int, salt uint64) []int {
	switch selected {
	case patternAppend:
		ids := make([]int, count)
		for i := range ids {
			ids[i] = records + i
		}
		return ids
	case patternRandom:
		return randomIDs(records, count, salt)
	case patternClustered:
		return clusteredIDs(records, count)
	default:
		panic("unsupported pattern")
	}
}

func mergeIDs(records, count int, selected pattern) ([]int, []int, error) {
	if count%2 != 0 {
		return nil, nil, fmt.Errorf("merge changes must be even")
	}
	branchCount := count / 2
	switch selected {
	case patternAppend:
		left := make([]int, branchCount)
		right := make([]int, branchCount)
		for i := range left {
			left[i] = records + i
			right[i] = records + branchCount + i
		}
		return left, right, nil
	case patternClustered:
		ids := clusteredIDs(records, count)
		return append([]int(nil), ids[:branchCount]...), append([]int(nil), ids[branchCount:]...), nil
	case patternRandom:
		ids := randomIDs(records, count, 0x006d65726765)
		left := make([]int, 0, branchCount)
		right := make([]int, 0, branchCount)
		for i, id := range ids {
			if i%2 == 0 {
				left = append(left, id)
			} else {
				right = append(right, id)
			}
		}
		return left, right, nil
	default:
		return nil, nil, fmt.Errorf("unsupported pattern %q", selected)
	}
}

func readIDs(selected pattern, records, count int) []int {
	switch selected {
	case patternAppend:
		return rightEdgeIDs(records, count)
	case patternRandom:
		return randomIDs(records, count, 0x72656164)
	case patternClustered:
		return clusteredIDs(records, count)
	default:
		panic("unsupported pattern")
	}
}

func rangeIDs(selected pattern, records, count int) []int {
	wanted := min(count, records)
	if wanted == 0 {
		return nil
	}
	start := 0
	switch selected {
	case patternAppend:
		start = records - wanted
	case patternClustered:
		start = (records - wanted) / 2
	case patternRandom:
		state := randomSeed ^ 0x7363616e
		start = int(nextRandom(&state) % uint64(records-wanted+1))
	}
	ids := make([]int, wanted)
	for i := range ids {
		ids[i] = start + i
	}
	return ids
}

func rangeBounds(selected pattern, records, count int) ([]byte, []byte) {
	ids := rangeIDs(selected, records, count)
	start := 0
	if len(ids) > 0 {
		start = ids[0]
	}
	return key(start), key(start + len(ids))
}

func clusteredIDs(records, count int) []int {
	wanted := min(count, records)
	start := (records - wanted) / 2
	ids := make([]int, wanted)
	for i := range ids {
		ids[i] = start + i
	}
	return ids
}

func rightEdgeIDs(records, count int) []int {
	wanted := min(count, records)
	ids := make([]int, wanted)
	for i := range ids {
		ids[i] = records - wanted + i
	}
	return ids
}

func randomIDs(records, count int, salt uint64) []int {
	wanted := min(count, records)
	state := randomSeed ^ bits.RotateLeft64(uint64(records), 29) ^ bits.RotateLeft64(salt, 11)
	set := make(map[int]struct{}, wanted)
	for len(set) < wanted {
		set[int(nextRandom(&state)%uint64(records))] = struct{}{}
	}
	ids := make([]int, 0, wanted)
	for id := range set {
		ids = append(ids, id)
	}
	sort.Ints(ids)
	return ids
}

func nextRandom(state *uint64) uint64 {
	*state ^= *state << 13
	*state ^= *state >> 7
	*state ^= *state << 17
	return *state
}

func parseOperation(input string) (operation, error) {
	for _, candidate := range allOperations {
		if input == string(candidate) {
			return candidate, nil
		}
	}
	return "", fmt.Errorf("unknown operation %q", input)
}

func parsePattern(input string) (pattern, error) {
	for _, candidate := range allPatterns {
		if input == string(candidate) {
			return candidate, nil
		}
	}
	return "", fmt.Errorf("unknown pattern %q", input)
}
