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

	base, err := engine.Create()
	must(err)
	base, err = engine.Put(base, []byte("doc:title"), []byte("Draft"))
	must(err)
	left, err := engine.Put(base, []byte("doc:body"), []byte("Hello"))
	must(err)
	right, err := engine.Put(base, []byte("doc:tags"), []byte("example"))
	must(err)

	leftChanges, err := engine.Diff(base, left)
	must(err)
	check(len(leftChanges) == 1 && bytes.Equal(leftChanges[0].Key, []byte("doc:body")), "expected body diff")

	merged, err := engine.Merge(base, left, right, "prefer_right")
	must(err)
	body, ok, err := engine.Get(merged, []byte("doc:body"))
	must(err)
	check(ok && bytes.Equal(body, []byte("Hello")), "expected merged body")
	tags, ok, err := engine.Get(merged, []byte("doc:tags"))
	must(err)
	check(ok && bytes.Equal(tags, []byte("example")), "expected merged tags")

	fmt.Printf("diff_merge: merged %d left-side change\n", len(leftChanges))
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
