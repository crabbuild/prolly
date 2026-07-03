package main

import (
	prolly "build.crab/prolly-go"
	"fmt"
	"log"
	"sort"
	"strings"
)

func main() {
	if err := VectorSidecar(); err != nil {
		log.Fatal(err)
	}
}

func VectorSidecar() error {
	engine, err := prolly.OpenMemory()
	if err != nil {
		return err
	}
	defer engine.Close()
	sidecar := map[string][]float64{"vec-1": {0.9, 0.1}, "vec-2": {0.8, 0.2}, "vec-stale": {1.0, 0.0}}
	tree, err := engine.Batch(mustCreate(engine), []prolly.Mutation{
		upsertText("vector-sidecar/corpus/docs/chunk/doc-1/0001", "vec-1|doc-1|parser-v1"),
		upsertText("vector-sidecar/corpus/docs/chunk/doc-2/0001", "vec-2|doc-2|parser-v1"),
	})
	if err != nil {
		return err
	}
	allowed := map[string]bool{}
	rows, err := engine.Range(tree, bytesOf("vector-sidecar/corpus/docs/chunk/"), bytesOf("vector-sidecar/corpus/docs/chunk0"))
	if err != nil {
		return err
	}
	for _, row := range rows {
		allowed[strings.SplitN(string(row.Value), "|", 2)[0]] = true
	}
	var hits []string
	for vectorID := range sidecar {
		if allowed[vectorID] {
			hits = append(hits, vectorID)
		}
	}
	sort.Strings(hits)
	if strings.Join(hits, ",") != "vec-1,vec-2" {
		return fmt.Errorf("unexpected sidecar hits %v", hits)
	}
	fmt.Printf("vector_sidecar: filtered sidecar hits to %d snapshot vectors\n", len(hits))
	return nil
}

func bytesOf(value string) []byte {
	return []byte(value)
}

func upsert(key string, value []byte) prolly.Mutation {
	return prolly.Mutation{Kind: "upsert", Key: bytesOf(key), Value: value}
}

func upsertText(key string, value string) prolly.Mutation {
	return upsert(key, bytesOf(value))
}

func deleteMutation(key string) prolly.Mutation {
	return prolly.Mutation{Kind: "delete", Key: bytesOf(key)}
}

func mustCreate(engine *prolly.Engine) prolly.Tree {
	tree, err := engine.Create()
	if err != nil {
		panic(err)
	}
	return tree
}
