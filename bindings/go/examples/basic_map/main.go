package main

import (
	"bytes"
	"fmt"
	"log"

	prolly "build.crab/prolly-go"
)

func main() {
	engine, err := prolly.OpenMemory()
	must(err)
	defer engine.Close()

	tree, err := engine.Create()
	must(err)
	tree, err = engine.Put(tree, []byte("user:001"), []byte("Ada"))
	must(err)
	tree, err = engine.Put(tree, []byte("user:002"), []byte("Grace"))
	must(err)
	tree, err = engine.Put(tree, []byte("user:003"), []byte("Linus"))
	must(err)

	value, ok, err := engine.Get(tree, []byte("user:001"))
	must(err)
	check(ok && bytes.Equal(value, []byte("Ada")), "expected user:001 to be Ada")

	tree, err = engine.Delete(tree, []byte("user:003"))
	must(err)
	_, ok, err = engine.Get(tree, []byte("user:003"))
	must(err)
	check(!ok, "expected user:003 to be deleted")

	users, err := engine.Range(tree, []byte("user:"), []byte("user;"))
	must(err)
	check(len(users) == 2, "expected two users in range")

	fmt.Printf("basic_map: %d users in range\n", len(users))
}

func must(err error) {
	if err != nil {
		log.Fatal(err)
	}
}

func check(ok bool, message string) {
	if !ok {
		log.Fatal(message)
	}
}
