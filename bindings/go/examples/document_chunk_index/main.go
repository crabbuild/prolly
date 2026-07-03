package main

import (
	prolly "build.crab/prolly-go"
	"bytes"
	"fmt"
	"log"
)

func main() {
	if err := DocumentChunkIndex(); err != nil {
		log.Fatal(err)
	}
}

func DocumentChunkIndex() error {
	engine, err := prolly.OpenMemory()
	if err != nil {
		return err
	}
	defer engine.Close()
	blobStore, err := prolly.MemoryBlobStore()
	if err != nil {
		return err
	}
	defer blobStore.Close()

	textKey := bytesOf("doc-index/corpus/text/parser-v1/doc-1/chunk-0001")
	metadataKey := bytesOf("doc-index/corpus/parser/parser-v1/document/doc-1/chunk/000000")
	tree, err := engine.PutLargeValue(blobStore, mustCreate(engine), textKey, bytes.Repeat(bytesOf("CrabDB stores large chunk text outside prolly leaves."), 8), prolly.LargeValueConfig{InlineThreshold: 32})
	if err != nil {
		return err
	}
	tree, err = engine.Put(tree, metadataKey, bytesOf("doc-1|chunk-0001|0|384|vector-0001"))
	if err != nil {
		return err
	}
	metadata, err := engine.Range(tree, bytesOf("doc-index/corpus/parser/"), bytesOf("doc-index/corpus/parser0"))
	if err != nil {
		return err
	}
	loaded, ok, err := engine.GetLargeValue(blobStore, tree, textKey)
	if err != nil {
		return err
	}
	if len(metadata) != 1 || !ok || !bytes.HasPrefix(loaded, bytesOf("CrabDB stores")) {
		return fmt.Errorf("document chunk validation failed")
	}
	fmt.Println("document_chunk_index: metadata and blob-backed chunk text are linked")
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
