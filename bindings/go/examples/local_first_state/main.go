package main

import (
	prolly "build.crab/prolly-go"
	"bytes"
	"fmt"
	"log"
)

func main() {
	if err := LocalFirstState(); err != nil {
		log.Fatal(err)
	}
}

func LocalFirstState() error {
	engine, err := prolly.OpenMemory()
	if err != nil {
		return err
	}
	defer engine.Close()

	main := bytesOf("app/demo/root/main")
	base, err := engine.Batch(mustCreate(engine), []prolly.Mutation{
		upsertText("entity/user/001", "Ada"),
		upsert("index/user/name/Ada/001", []byte{}),
	})
	if err != nil {
		return err
	}
	if err := engine.PublishNamedRoot(main, base); err != nil {
		return err
	}

	device, err := engine.Batch(base, []prolly.Mutation{
		upsertText("entity/task/900", "offline draft"),
		upsert("index/task/status/open/900", []byte{}),
	})
	if err != nil {
		return err
	}
	canonical, err := engine.Put(base, bytesOf("entity/user/002"), bytesOf("Grace"))
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
	merged, err := engine.Merge(base, *current, device, "prefer_right")
	if err != nil {
		return err
	}
	update, err := engine.CompareAndSwapNamedRoot(main, current, &merged)
	if err != nil {
		return err
	}
	if !update.Applied {
		return fmt.Errorf("main root CAS failed")
	}
	if err := requireGet(engine, merged, "entity/user/002", "Grace"); err != nil {
		return err
	}
	if err := requireGet(engine, merged, "entity/task/900", "offline draft"); err != nil {
		return err
	}

	fmt.Println("local_first_state: merged offline branch into main")
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

func mustBytes(label string, expected []byte, actual []byte, ok bool) error {
	if !ok || !bytes.Equal(expected, actual) {
		return fmt.Errorf("%s: expected %q, got %q", label, expected, actual)
	}
	return nil
}

func requireGet(engine *prolly.Engine, tree prolly.Tree, key string, expected string) error {
	value, ok, err := engine.Get(tree, bytesOf(key))
	if err != nil {
		return err
	}
	return mustBytes(key, bytesOf(expected), value, ok)
}
