package main

import (
	prolly "build.crab/prolly-go"
	"bytes"
	"fmt"
	"log"
)

func main() {
	if err := CrdtMerge(); err != nil {
		log.Fatal(err)
	}
}

func CrdtMerge() error {
	engine, err := prolly.OpenMemory()
	if err != nil {
		return err
	}
	defer engine.Close()

	baseValue, err := prolly.TimestampedValueToBytes(prolly.TimestampedValue{Value: bytesOf("base"), Timestamp: 1})
	if err != nil {
		return err
	}
	leftValue, err := prolly.TimestampedValueToBytes(prolly.TimestampedValue{Value: bytesOf("left"), Timestamp: 2})
	if err != nil {
		return err
	}
	rightValue, err := prolly.TimestampedValueToBytes(prolly.TimestampedValue{Value: bytesOf("right"), Timestamp: 3})
	if err != nil {
		return err
	}
	base, err := engine.Put(mustCreate(engine), bytesOf("counter/global"), baseValue)
	if err != nil {
		return err
	}
	left, err := engine.Put(base, bytesOf("counter/global"), leftValue)
	if err != nil {
		return err
	}
	right, err := engine.Put(base, bytesOf("counter/global"), rightValue)
	if err != nil {
		return err
	}
	config, err := prolly.CrdtConfigLWW("update_wins")
	if err != nil {
		return err
	}
	merged, err := engine.CrdtMerge(base, left, right, config)
	if err != nil {
		return err
	}
	value, ok, err := engine.Get(merged, bytesOf("counter/global"))
	if err != nil {
		return err
	}
	if !ok {
		return fmt.Errorf("missing CRDT value")
	}
	decoded, err := prolly.TimestampedValueFromBytes(value)
	if err != nil {
		return err
	}
	mergedSet, err := prolly.MultiValueSetMerge([][]byte{bytesOf("candidate-b")}, [][]byte{bytesOf("candidate-a"), bytesOf("candidate-b")})
	if err != nil {
		return err
	}
	if !bytes.Equal(decoded.Value, bytesOf("right")) || decoded.Timestamp != 3 || len(mergedSet) != 2 || !bytes.Equal(mergedSet[0], bytesOf("candidate-a")) {
		return fmt.Errorf("CRDT validation failed")
	}
	fmt.Println("crdt_merge: last-writer-wins and multi-value helpers passed")
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
