package main

import "testing"

func TestWorkloadContractStableDigest(t *testing.T) {
	digest := uint64(fnvOffset)
	for index := 0; index < 10_000; index++ {
		id := freshID(randomWorkload, index, 10_000)
		key := keyForPosition(id * 2)
		value := valueForPosition(id*2, 0)
		digest = digestOperation(digest, key, value)
	}
	if digest != 0x004197dd790a1245 {
		t.Fatalf("workload digest changed: got %016x", digest)
	}
}

func TestMutationMixHasEqualUniqueUpdatesAndInserts(t *testing.T) {
	const records = 10_000
	writes := records * 30 / 100
	positions := make(map[int]struct{}, writes)
	updates, inserts := 0, 0
	for index := 0; index < writes; index++ {
		position := mutationPosition(randomWorkload, index, records, writes)
		positions[position] = struct{}{}
		if position%2 == 0 {
			updates++
		} else {
			inserts++
		}
	}
	if updates != 1_500 || inserts != 1_500 || len(positions) != writes {
		t.Fatalf("unexpected mutation mix: updates=%d inserts=%d unique=%d", updates, inserts, len(positions))
	}
}

func TestValuesAreDeterministicAndBounded(t *testing.T) {
	first := valueForPosition(42, 0)
	second := valueForPosition(42, 0)
	changed := valueForPosition(42, 1)
	if string(first) != string(second) || string(first) == string(changed) {
		t.Fatal("value generation is not deterministic across generations")
	}
	if len(first) < 1 || len(first) > 100 {
		t.Fatalf("value length is outside 1-100 bytes: %d", len(first))
	}
}
