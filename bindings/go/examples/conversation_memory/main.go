package main

import (
	prolly "build.crab/prolly-go"
	"fmt"
	"log"
)

func main() {
	if err := ConversationMemory(); err != nil {
		log.Fatal(err)
	}
}

func ConversationMemory() error {
	engine, err := prolly.OpenMemory()
	if err != nil {
		return err
	}
	defer engine.Close()

	main := bytesOf("conversation/c42/root/main")
	attemptName := bytesOf("conversation/c42/attempt/extractor/a1")
	base, err := engine.Put(mustCreate(engine), bytesOf("conversation/c42/memory/m001"), bytesOf("user|likes terse summaries|0.91"))
	if err != nil {
		return err
	}
	if err := engine.PublishNamedRoot(main, base); err != nil {
		return err
	}
	attempt, err := engine.Put(base, bytesOf("conversation/c42/memory/m002"), bytesOf("user|uses Go|0.87"))
	if err != nil {
		return err
	}
	if err := engine.PublishNamedRoot(attemptName, attempt); err != nil {
		return err
	}
	canonical, err := engine.Put(base, bytesOf("conversation/c42/memory/m003"), bytesOf("user|prefers local-first apps|0.82"))
	if err != nil {
		return err
	}
	if err := engine.PublishNamedRoot(main, canonical); err != nil {
		return err
	}
	current, err := engine.LoadNamedRoot(main)
	if err != nil {
		return err
	}
	attemptRoot, err := engine.LoadNamedRoot(attemptName)
	if err != nil {
		return err
	}
	merged, err := engine.Merge(base, *current, *attemptRoot, "prefer_right")
	if err != nil {
		return err
	}
	update, err := engine.CompareAndSwapNamedRoot(main, &canonical, &merged)
	if err != nil {
		return err
	}
	rows, err := engine.Range(merged, bytesOf("conversation/c42/memory/"), bytesOf("conversation/c42/memory0"))
	if err != nil {
		return err
	}
	if !update.Applied || len(rows) != 3 {
		return fmt.Errorf("conversation memory merge failed")
	}
	fmt.Println("conversation_memory: accepted extractor attempt into canonical memory")
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
