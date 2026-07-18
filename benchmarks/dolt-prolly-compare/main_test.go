package main

import (
	"bytes"
	"testing"
)

func TestWorkloadContractStableDigests(t *testing.T) {
	tests := []struct {
		phase    phase
		workload workload
		want     uint64
	}{
		{phaseFresh, workloadAppend, 0x51f55fcd59187cbf},
		{phaseFresh, workloadRandom, 0x004197dd790a1245},
		{phaseFresh, workloadClustered, 0x86e38047f6ae04b3},
		{phaseMutation, workloadAppend, 0x2ef1df79e1226620},
		{phaseMutation, workloadRandom, 0x3bc7e45ef276a1c5},
		{phaseMutation, workloadClustered, 0x5caed8dbd3056277},
	}
	for _, tt := range tests {
		t.Run(string(tt.phase)+"/"+string(tt.workload), func(t *testing.T) {
			if got := workloadDigest(tt.phase, tt.workload, 10_000); got != tt.want {
				t.Fatalf("workload digest = %016x, want %016x", got, tt.want)
			}
		})
	}
}

func TestPermutationUniqueForRequestedScales(t *testing.T) {
	for _, count := range []uint64{10_000, 50_000, 1_000_000} {
		seen := make([]bool, count)
		for index := uint64(0); index < count; index++ {
			position := permute(index, count, randomSeed)
			if position >= count {
				t.Fatalf("permute(%d, %d) = %d", index, count, position)
			}
			if seen[position] {
				t.Fatalf("duplicate position %d for count %d", position, count)
			}
			seen[position] = true
		}
	}
}

func TestKeysPreserveNumericOrder(t *testing.T) {
	positions := []uint64{0, 1, 9, 10, 999, 1_000, 19_999_999}
	for index := 1; index < len(positions); index++ {
		left := keyForPosition(positions[index-1])
		right := keyForPosition(positions[index])
		if len(left) != len(right) {
			t.Fatalf("key widths differ: %q and %q", left, right)
		}
		if bytes.Compare(left, right) >= 0 {
			t.Fatalf("keys are not ordered: %q >= %q", left, right)
		}
	}
}

func TestValuesDeterministicAndBounded(t *testing.T) {
	first := valueForPosition(42, 0)
	if !bytes.Equal(first, valueForPosition(42, 0)) {
		t.Fatal("same position and generation produced different values")
	}
	if bytes.Equal(first, valueForPosition(42, 1)) {
		t.Fatal("different generations produced the same value")
	}
	if len(first) < 1 || len(first) > 100 {
		t.Fatalf("value length = %d, want 1..100", len(first))
	}
}

func TestMutationMixUniqueAndBalanced(t *testing.T) {
	const records uint64 = 10_000
	const writes uint64 = records * 30 / 100
	for _, pattern := range []workload{workloadRandom, workloadClustered} {
		t.Run(string(pattern), func(t *testing.T) {
			seen := make(map[uint64]struct{}, writes)
			var updates, inserts uint64
			for index := uint64(0); index < writes; index++ {
				position := mutationPosition(pattern, index, records, writes)
				if _, ok := seen[position]; ok {
					t.Fatalf("duplicate mutation position %d", position)
				}
				seen[position] = struct{}{}
				if position%2 == 0 {
					updates++
				} else {
					inserts++
				}
			}
			if updates != writes/2 || inserts != writes/2 {
				t.Fatalf("updates/inserts = %d/%d, want %d/%d", updates, inserts, writes/2, writes/2)
			}
		})
	}
}

func TestCSVSchemaIncludesContractVersion(t *testing.T) {
	if contractVersion != "prolly-compare-v1" {
		t.Fatalf("contract version = %q", contractVersion)
	}
	want := "implementation,revision,contract_version,records,phase,workload,operation,operations,elapsed_ns,ns_per_op,ops_per_sec,workload_digest,result_count,validated"
	if got := csvHeader(); got != want {
		t.Fatalf("CSV header = %q, want %q", got, want)
	}
}
