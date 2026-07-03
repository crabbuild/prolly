package main

import (
	prolly "build.crab/prolly-go"
	"bytes"
	"fmt"
	"log"
	"strings"
)

func main() {
	if err := BatchBuild(); err != nil {
		log.Fatal(err)
	}
}

func BatchBuild() error {
	engine, err := prolly.OpenMemory()
	if err != nil {
		return err
	}
	defer engine.Close()

	entries := make([]prolly.Entry, 0, 64)
	for idx := 64; idx >= 1; idx-- {
		entries = append(entries, prolly.Entry{
			Key:   bytesOf(fmt.Sprintf("event/%04d", idx)),
			Value: bytesOf(fmt.Sprintf("payload-%d", idx)),
		})
	}
	tree, err := engine.BuildFromEntries(entries)
	if err != nil {
		return err
	}
	rows, err := engine.Range(tree, bytesOf("event/"), bytesOf("event0"))
	if err != nil {
		return err
	}
	stats, err := engine.CollectStatsJSON(tree)
	if err != nil {
		return err
	}
	if len(rows) != 64 || !bytes.Equal(rows[0].Key, bytesOf("event/0001")) || !strings.Contains(stats, "num_nodes") {
		return fmt.Errorf("batch build validation failed")
	}
	fmt.Printf("batch_build: imported %d events\n", len(rows))
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
