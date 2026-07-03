package main

import (
	prolly "build.crab/prolly-go"
	"encoding/hex"
	"fmt"
	"log"
)

func main() {
	if err := DeterministicRagSnapshot(); err != nil {
		log.Fatal(err)
	}
}

func DeterministicRagSnapshot() error {
	engine, err := prolly.OpenMemory()
	if err != nil {
		return err
	}
	defer engine.Close()

	indexRoot := bytesOf("rag/corpus/docs/root/index/current")
	indexV1, err := engine.Batch(mustCreate(engine), []prolly.Mutation{
		upsertText("rag/corpus/docs/chunk/doc-1/0001", "vector:v1|CrabDB stores deterministic roots"),
		upsertText("rag/corpus/docs/chunk/doc-2/0001", "vector:v2|Prolly trees diff by key"),
	})
	if err != nil {
		return err
	}
	if err := engine.PublishNamedRoot(indexRoot, indexV1); err != nil {
		return err
	}
	indexRootBytes, _, err := indexV1.Root()
	if err != nil {
		return err
	}
	answerValue := []byte("query:q1|snapshot:" + hex.EncodeToString(indexRootBytes) + "|citation:doc-1/0001")
	answers, err := engine.Put(mustCreate(engine), bytesOf("rag/answer/q1"), answerValue)
	if err != nil {
		return err
	}
	if err := engine.PublishNamedRoot(bytesOf("rag/corpus/docs/root/answers"), answers); err != nil {
		return err
	}
	indexV2, err := engine.Put(indexV1, bytesOf("rag/corpus/docs/chunk/doc-3/0001"), bytesOf("vector:v3|New content"))
	if err != nil {
		return err
	}
	if err := engine.PublishNamedRoot(indexRoot, indexV2); err != nil {
		return err
	}
	replay, err := engine.Range(indexV1, bytesOf("rag/corpus/docs/chunk/"), bytesOf("rag/corpus/docs/chunk0"))
	if err != nil {
		return err
	}
	currentRoot, err := engine.LoadNamedRoot(indexRoot)
	if err != nil {
		return err
	}
	current, err := engine.Range(*currentRoot, bytesOf("rag/corpus/docs/chunk/"), bytesOf("rag/corpus/docs/chunk0"))
	if err != nil {
		return err
	}
	if len(replay) != 2 || len(current) != 3 {
		return fmt.Errorf("RAG snapshot validation failed")
	}
	fmt.Println("deterministic_rag_snapshot: replay kept original index root")
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
