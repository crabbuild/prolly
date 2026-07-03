package main

import (
	prolly "build.crab/prolly-go"
	"bytes"
	"fmt"
	"log"
	"os"
	"path/filepath"
)

func main() {
	if err := DurableSQLite(); err != nil {
		log.Fatal(err)
	}
}

func DurableSQLite() error {
	dir, err := os.MkdirTemp("", "prolly-go-")
	if err != nil {
		return err
	}
	defer os.RemoveAll(dir)
	engine, err := prolly.OpenSQLite(filepath.Join(dir, "app.prolly.sqlite"))
	if err != nil {
		return err
	}
	defer engine.Close()
	tree, err := engine.Batch(mustCreate(engine), []prolly.Mutation{upsertText("user/1", "Ada"), upsertText("user/2", "Grace")})
	if err != nil {
		return err
	}
	if err := engine.PublishNamedRoot(bytesOf("users/main"), tree); err != nil {
		return err
	}
	loaded, err := engine.LoadNamedRoot(bytesOf("users/main"))
	if err != nil {
		return err
	}
	loadedRoot, _, err := loaded.Root()
	if err != nil {
		return err
	}
	treeRoot, _, err := tree.Root()
	if err != nil {
		return err
	}
	if !bytes.Equal(loadedRoot, treeRoot) {
		return fmt.Errorf("loaded SQLite root mismatch")
	}
	if err := requireGet(engine, *loaded, "user/1", "Ada"); err != nil {
		return err
	}
	fmt.Println("durable_sqlite: named root survived through SQLite store API")
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
