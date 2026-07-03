package main

import (
	prolly "build.crab/prolly-go"
	"bytes"
	"fmt"
	"log"
)

func main() {
	if err := FilesystemSnapshot(); err != nil {
		log.Fatal(err)
	}
}

func FilesystemSnapshot() error {
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
	tree := mustCreate(engine)
	for path, contents := range map[string][]byte{
		"README.md":  bytesOf("# Demo\n"),
		"src/lib.rs": bytesOf("pub fn answer() -> u8 { 42 }\n"),
	} {
		tree, err = engine.PutLargeValue(blobStore, tree, bytesOf("path/"+path), contents, prolly.LargeValueConfig{InlineThreshold: 4})
		if err != nil {
			return err
		}
	}
	if err := engine.PublishNamedRoot(bytesOf("refs/heads/main"), tree); err != nil {
		return err
	}
	loaded, err := engine.LoadNamedRoot(bytesOf("refs/heads/main"))
	if err != nil {
		return err
	}
	readme, ok, err := engine.GetLargeValue(blobStore, *loaded, bytesOf("path/README.md"))
	if err != nil {
		return err
	}
	if err := mustBytes("README.md", bytesOf("# Demo\n"), readme, ok); err != nil {
		return err
	}
	fmt.Println("filesystem_snapshot: published branch with blob-backed file contents")
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
