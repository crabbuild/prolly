package main

import (
	"bytes"
	"fmt"
	"log"

	prolly "build.crab/prolly-go"
)

func main() {
	engine, err := prolly.OpenMemory()
	must(err)
	defer engine.Close()
	blobStore, err := prolly.MemoryBlobStore()
	must(err)
	defer blobStore.Close()

	tree, err := engine.Create()
	must(err)
	policy := prolly.LargeValueConfig{InlineThreshold: 8}
	tree, err = engine.PutLargeValue(blobStore, tree, []byte("doc/body"), bytes.Repeat([]byte{7}, 64), policy)
	must(err)
	valueRef, err := engine.GetValueRef(tree, []byte("doc/body"))
	must(err)
	check(valueRef != nil && valueRef.Kind == "blob", "expected blob value ref")

	updated, err := engine.PutLargeValue(blobStore, tree, []byte("doc/body"), bytes.Repeat([]byte{9}, 64), policy)
	must(err)
	loaded, ok, err := engine.GetLargeValue(blobStore, updated, []byte("doc/body"))
	must(err)
	check(ok && bytes.Equal(loaded, bytes.Repeat([]byte{9}, 64)), "expected updated large value")

	plan, err := engine.PlanBlobStoreGC(blobStore, []prolly.Tree{updated})
	must(err)
	check(plan.ReclaimableBlobCount == 1, "expected one reclaimable blob")
	sweep, err := engine.SweepBlobStoreGC(blobStore, []prolly.Tree{updated})
	must(err)
	check(sweep.DeletedBlobs == 1, "expected one deleted blob")

	fmt.Printf("file_blob_store: reclaimed %d bytes\n", sweep.DeletedBlobBytes)
}

func must(err error) {
	if err != nil {
		log.Fatal(err)
	}
}

func check(ok bool, message string) {
	if !ok {
		log.Fatal(message)
	}
}
