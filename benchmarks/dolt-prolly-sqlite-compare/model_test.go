package main

import (
	"bytes"
	"reflect"
	"testing"
)

func TestLogicalWorkloadMatchesSQLiteScaleContract(t *testing.T) {
	if got := key(42); len(got) != 24 || string(got) != "key-00000000000000000042" {
		t.Fatalf("key(42) = %q (%d bytes)", got, len(got))
	}
	if len(value(42, 0)) != 100 || bytes.Equal(value(42, 0), value(42, 1)) {
		t.Fatal("values must be 100 bytes and generation-sensitive")
	}
	if got := mutationIDs(patternAppend, 10_000, 3, 0); !reflect.DeepEqual(got, []int{10_000, 10_001, 10_002}) {
		t.Fatalf("append IDs = %v", got)
	}
	if got := mutationIDs(patternClustered, 10_000, 4, 0); !reflect.DeepEqual(got, []int{4_998, 4_999, 5_000, 5_001}) {
		t.Fatalf("clustered IDs = %v", got)
	}
}

func TestMergeIDsAreEvenAndDisjoint(t *testing.T) {
	for _, selected := range allPatterns {
		left, right, err := mergeIDs(100_000, 1_000, selected)
		if err != nil || len(left) != 500 || len(right) != 500 {
			t.Fatalf("%s: left=%d right=%d err=%v", selected, len(left), len(right), err)
		}
		seen := map[int]struct{}{}
		for _, id := range left {
			seen[id] = struct{}{}
		}
		for _, id := range right {
			if _, ok := seen[id]; ok {
				t.Fatalf("%s repeats ID %d", selected, id)
			}
		}
	}
	if _, _, err := mergeIDs(100, 11, patternRandom); err == nil {
		t.Fatal("odd merge count must fail")
	}
}
