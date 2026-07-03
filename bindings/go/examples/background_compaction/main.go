package main

import (
	prolly "build.crab/prolly-go"
	"fmt"
	"log"
)

func main() {
	if err := BackgroundCompaction(); err != nil {
		log.Fatal(err)
	}
}

func BackgroundCompaction() error {
	engine, err := prolly.OpenMemory()
	if err != nil {
		return err
	}
	defer engine.Close()

	mutations := make([]prolly.Mutation, 0, 6)
	for idx := 1; idx <= 6; idx++ {
		mutations = append(mutations, upsertText(fmt.Sprintf("event/%04d", idx), fmt.Sprintf("raw-event-%d", idx)))
	}
	events, err := engine.Batch(mustCreate(engine), mutations)
	if err != nil {
		return err
	}
	if err := engine.PublishNamedRoot(bytesOf("compaction/run/r7/root/events/0001"), events); err != nil {
		return err
	}
	compacted, err := engine.Batch(events, []prolly.Mutation{
		deleteMutation("event/0001"),
		deleteMutation("event/0002"),
		deleteMutation("event/0003"),
		deleteMutation("event/0004"),
		upsertText("event/0004-summary", "summary of events 1..4"),
	})
	if err != nil {
		return err
	}
	if err := engine.PublishNamedRoot(bytesOf("compaction/run/r7/root/events/current"), compacted); err != nil {
		return err
	}
	plan, err := engine.PlanStoreGC([]prolly.Tree{events, compacted})
	if err != nil {
		return err
	}
	remaining, err := engine.Range(compacted, bytesOf("event/"), bytesOf("event0"))
	if err != nil {
		return err
	}
	if len(remaining) != 3 || plan.ReclaimableNodes < 0 {
		return fmt.Errorf("compaction validation failed")
	}
	fmt.Printf("background_compaction: compacted log to %d records\n", len(remaining))
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
