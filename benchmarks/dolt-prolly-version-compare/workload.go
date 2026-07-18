package main

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"sort"
	"strconv"
)

const (
	contractVersion = "prolly-version-compare-v2"
	randomSeed      = uint64(0x6a09e667f3bcc909)
	fnvOffset       = uint64(0xcbf29ce484222325)
	fnvPrime        = uint64(0x00000100000001b3)
	clusterSize     = uint64(1000)
)

type locality string

const (
	localityNone      locality = "none"
	localityAppend    locality = "append"
	localityRandom    locality = "random"
	localityClustered locality = "clustered"
)

type logicalMutation struct {
	key    []byte
	value  []byte
	delete bool
}

func baseMutations(records uint64) []logicalMutation {
	result := make([]logicalMutation, 0, records)
	for position := uint64(0); position < records; position++ {
		result = append(result, logicalMutation{
			key:   keyForID(position * 2),
			value: valueFor(position, 0),
		})
	}
	return result
}

func changeCount(records uint64, density uint64) uint64 {
	return records * density / 100
}

func branchMutations(records, density uint64, selected locality, disjointOffset, generation uint64) []logicalMutation {
	count := changeCount(records, density)
	if count == 0 {
		return nil
	}
	result := make([]logicalMutation, 0, count)
	if selected == localityAppend {
		for ordinal := uint64(0); ordinal < count; ordinal++ {
			appendOrdinal := ordinal + disjointOffset
			id := records*2 + appendOrdinal*2
			result = append(result, logicalMutation{key: keyForID(id), value: valueFor(id, generation)})
		}
		return result
	}

	updates := count * 40 / 100
	inserts := count * 30 / 100
	for ordinal := uint64(0); ordinal < count; ordinal++ {
		position := selectedPosition(ordinal+disjointOffset, records, selected)
		switch {
		case ordinal < updates:
			id := position * 2
			result = append(result, logicalMutation{key: keyForID(id), value: valueFor(id, generation)})
		case ordinal < updates+inserts:
			id := position*2 + 1
			result = append(result, logicalMutation{key: keyForID(id), value: valueFor(id, generation)})
		default:
			result = append(result, logicalMutation{key: keyForID(position * 2), delete: true})
		}
	}
	sort.Slice(result, func(i, j int) bool { return bytes.Compare(result[i].key, result[j].key) < 0 })
	for i := 1; i < len(result); i++ {
		if bytes.Compare(result[i-1].key, result[i].key) >= 0 {
			panic("mutation keys are not unique and ordered")
		}
	}
	return result
}

func conflictingMutations(left []logicalMutation) []logicalMutation {
	result := make([]logicalMutation, 0, len(left))
	for _, mutation := range left {
		result = append(result, logicalMutation{
			key:   append([]byte(nil), mutation.key...),
			value: valueForKey(mutation.key, 3),
		})
	}
	return result
}

func rangeBounds(records, density uint64, selected locality, left []logicalMutation) ([]byte, []byte) {
	if density == 0 {
		return keyForID(0), keyForID(max(records/10, 1) * 2)
	}
	inserts := changeCount(records, density) * 30 / 100
	if selected == localityAppend {
		inserts = changeCount(records, density)
	}
	union := records + inserts
	widthIDs := max(union/10, 1) * 2
	if selected == localityAppend {
		endID := records*2 + changeCount(records, density)*2 + 1
		startID := uint64(0)
		if endID > widthIDs {
			startID = endID - widthIDs
		}
		return keyForID(startID), keyForID(endID)
	}
	first := ^uint64(0)
	for _, mutation := range left {
		id, err := strconv.ParseUint(string(mutation.key[1:]), 10, 64)
		if err != nil {
			panic(err)
		}
		if id < first {
			first = id
		}
	}
	maxID := records*2 + 2
	start := first
	if maxID > widthIDs && start > maxID-widthIDs {
		start = maxID - widthIDs
	}
	end := start + widthIDs
	if end > maxID {
		end = maxID
	}
	return keyForID(start), keyForID(end)
}

func workloadDigest(baseCount uint64, relationship string, groups ...[]logicalMutation) uint64 {
	digest := digestBytes(fnvOffset, []byte(contractVersion))
	digest = digestUint64(digest, baseCount)
	digest = digestBytes(digest, []byte(relationship))
	for _, group := range groups {
		digest = digestUint64(digest, uint64(len(group)))
		for _, mutation := range group {
			if mutation.delete {
				digest = digestBytes(digest, []byte{2})
				digest = digestBytes(digest, mutation.key)
			} else {
				digest = digestBytes(digest, []byte{1})
				digest = digestBytes(digest, mutation.key)
				digest = digestBytes(digest, mutation.value)
			}
		}
	}
	return digest
}

func digestEntry(digest uint64, key, value []byte) uint64 {
	digest = digestBytes(digest, key)
	return digestBytes(digest, value)
}

func digestUint64(digest, value uint64) uint64 {
	var encoded [8]byte
	binary.LittleEndian.PutUint64(encoded[:], value)
	return digestBytes(digest, encoded[:])
}

func digestBytes(digest uint64, data []byte) uint64 {
	var length [8]byte
	binary.LittleEndian.PutUint64(length[:], uint64(len(data)))
	for _, b := range append(length[:], data...) {
		digest ^= uint64(b)
		digest *= fnvPrime
	}
	return digest
}

func selectedPosition(ordinal, records uint64, selected locality) uint64 {
	switch selected {
	case localityRandom:
		return permute(ordinal, records, randomSeed^records)
	case localityClustered:
		blocks := (records + clusterSize - 1) / clusterSize
		logicalBlock := ordinal / clusterSize
		offset := ordinal % clusterSize
		block := permute(logicalBlock, blocks, randomSeed^0xc1a57e2d)
		return (block*clusterSize + offset) % records
	default:
		panic("selected position requires random or clustered locality")
	}
}

func permute(index, count, seed uint64) uint64 {
	if count <= 1 {
		return 0
	}
	step := (mix64(seed) | 1) % count
	if step == 0 {
		step = 1
	}
	for gcd(step, count) != 1 {
		step = (step + 2) % count
		if step == 0 {
			step = 1
		}
	}
	offset := mix64(seed^0x9e3779b97f4a7c15) % count
	return ((index%count)*step + offset) % count
}

func gcd(left, right uint64) uint64 {
	for right != 0 {
		left, right = right, left%right
	}
	return left
}

func mix64(value uint64) uint64 {
	value ^= value >> 30
	value *= 0xbf58476d1ce4e5b9
	value ^= value >> 27
	value *= 0x94d049bb133111eb
	return value ^ (value >> 31)
}

func keyForID(id uint64) []byte {
	return []byte(fmt.Sprintf("k%020d", id))
}

func valueFor(position, generation uint64) []byte {
	seed := mix64(position ^ generation*0xd1b54a32d192ed03 ^ randomSeed)
	length := 16 + seed%84
	value := make([]byte, 0, length)
	state := seed
	for uint64(len(value)) < length {
		state = mix64(state + 0x9e3779b97f4a7c15)
		var encoded [8]byte
		binary.LittleEndian.PutUint64(encoded[:], state)
		value = append(value, encoded[:]...)
	}
	return value[:length]
}

func valueForKey(key []byte, generation uint64) []byte {
	return valueFor(digestBytes(fnvOffset, key), generation)
}

func max(left, right uint64) uint64 {
	if left > right {
		return left
	}
	return right
}
