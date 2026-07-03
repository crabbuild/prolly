package main

import (
	prolly "build.crab/prolly-go"
	"bytes"
	"fmt"
	"log"
)

func main() {
	if err := Resolver(); err != nil {
		log.Fatal(err)
	}
}

func Resolver() error {
	engine, err := prolly.OpenMemory()
	if err != nil {
		return err
	}
	defer engine.Close()

	base, err := engine.Put(mustCreate(engine), bytesOf("settings/theme"), bytesOf("light"))
	if err != nil {
		return err
	}
	leftDelete, err := engine.Delete(base, bytesOf("settings/theme"))
	if err != nil {
		return err
	}
	rightUpdate, err := engine.Put(base, bytesOf("settings/theme"), bytesOf("dark"))
	if err != nil {
		return err
	}
	updateWins, err := engine.Merge(base, leftDelete, rightUpdate, "update_wins")
	if err != nil {
		return err
	}
	deleteWins, err := engine.Merge(base, leftDelete, rightUpdate, "delete_wins")
	if err != nil {
		return err
	}
	if err := requireGet(engine, updateWins, "settings/theme", "dark"); err != nil {
		return err
	}
	_, ok, err := engine.Get(deleteWins, bytesOf("settings/theme"))
	if err != nil {
		return err
	}
	if ok {
		return fmt.Errorf("delete_wins should remove settings/theme")
	}
	fmt.Println("resolver: demonstrated update-wins and delete-wins policies")
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
