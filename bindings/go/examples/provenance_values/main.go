package main

import (
	prolly "build.crab/prolly-go"
	"bytes"
	"encoding/hex"
	"fmt"
	"log"
)

func main() {
	if err := ProvenanceValues(); err != nil {
		log.Fatal(err)
	}
}

func ProvenanceValues() error {
	engine, err := prolly.OpenMemory()
	if err != nil {
		return err
	}
	defer engine.Close()
	source := bytesOf("CrabDB language bindings design")
	sourceCid, err := prolly.CidFromBytes(source)
	if err != nil {
		return err
	}
	chunkCid, err := prolly.CidFromBytes(source[:16])
	if err != nil {
		return err
	}
	tree, err := engine.Batch(mustCreate(engine), []prolly.Mutation{
		upsertText("provenance/chunk/file-1/chunk-1", "source="+hex.EncodeToString(sourceCid)+"|chunk="+hex.EncodeToString(chunkCid)+"|parser=v1"),
		upsertText("provenance/claim/file-1/claim-1", "CrabDB uses Rust-backed bindings|chunk=file-1/chunk-1"),
	})
	if err != nil {
		return err
	}
	claims, err := engine.Range(tree, bytesOf("provenance/claim/file-1/"), bytesOf("provenance/claim/file-10"))
	if err != nil {
		return err
	}
	if len(claims) != 1 || !bytes.Contains(claims[0].Value, bytesOf("Rust-backed")) {
		return fmt.Errorf("provenance validation failed")
	}
	fmt.Println("provenance_values: claim links back to source and chunk CIDs")
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
