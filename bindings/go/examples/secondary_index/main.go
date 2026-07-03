package main

import (
	"bytes"
	"fmt"
	"log"
	"strings"

	prolly "build.crab/prolly-go"
)

type user struct {
	tenant, id, status, name string
}

func main() {
	engine, err := prolly.OpenMemory()
	must(err)
	defer engine.Close()

	empty, err := engine.Create()
	must(err)

	sourceV1 := putUser(engine, empty, user{"acme", "u001", "active", "Ada"})
	sourceV1 = putUser(engine, sourceV1, user{"acme", "u002", "invited", "Grace"})
	indexV1 := buildStatusIndex(engine, sourceV1)

	sourceV2 := putUser(engine, sourceV1, user{"acme", "u002", "active", "Grace"})
	sourceV2 = putUser(engine, sourceV2, user{"globex", "u003", "active", "Linus"})

	sourceChanges, err := engine.Diff(sourceV1, sourceV2)
	must(err)
	check(len(sourceChanges) == 2, "expected two source changes")

	indexV2 := applySourceDiff(engine, indexV1, sourceChanges)
	rebuiltIndexV2 := buildStatusIndex(engine, sourceV2)
	equalTreeRoots(indexV2, rebuiltIndexV2)

	check(len(usersByStatus(engine, indexV2, "acme", "active")) == 2, "expected two acme active users")
	check(len(usersByStatus(engine, indexV2, "acme", "invited")) == 0, "expected no acme invited users")
	check(len(usersByStatus(engine, indexV2, "globex", "active")) == 1, "expected one globex active user")

	fmt.Printf("secondary_index: applied %d source diffs\n", len(sourceChanges))
}

func userKey(u user) []byte {
	return []byte(fmt.Sprintf("source/tenant/%s/user/%s", u.tenant, u.id))
}

func encodeUser(u user) []byte {
	return []byte(strings.Join([]string{u.tenant, u.id, u.status, u.name}, "|"))
}

func decodeUser(value []byte) user {
	parts := strings.SplitN(string(value), "|", 4)
	return user{parts[0], parts[1], parts[2], parts[3]}
}

func statusIndexPrefix(tenant, status string) []byte {
	return []byte(fmt.Sprintf("index/user-by-status/tenant/%s/status/%s/", tenant, status))
}

func statusIndexKey(u user) []byte {
	return append(statusIndexPrefix(u.tenant, u.status), []byte(u.id)...)
}

func putUser(engine *prolly.Engine, tree prolly.Tree, u user) prolly.Tree {
	next, err := engine.Put(tree, userKey(u), encodeUser(u))
	must(err)
	return next
}

func buildStatusIndex(engine *prolly.Engine, source prolly.Tree) prolly.Tree {
	index, err := engine.Create()
	must(err)
	entries, err := engine.Range(source, []byte("source/"), []byte("source0"))
	must(err)
	for _, entry := range entries {
		index, err = engine.Put(index, statusIndexKey(decodeUser(entry.Value)), []byte("1"))
		must(err)
	}
	return index
}

func applySourceDiff(engine *prolly.Engine, index prolly.Tree, changes []prolly.Diff) prolly.Tree {
	var err error
	for _, change := range changes {
		switch change.Kind {
		case "added":
			index, err = engine.Put(index, statusIndexKey(decodeUser(change.Value)), []byte("1"))
		case "removed":
			index, err = engine.Delete(index, statusIndexKey(decodeUser(change.Value)))
		case "changed":
			oldKey := statusIndexKey(decodeUser(change.OldValue))
			newKey := statusIndexKey(decodeUser(change.NewValue))
			if bytes.Equal(oldKey, newKey) {
				continue
			}
			index, err = engine.Delete(index, oldKey)
			must(err)
			index, err = engine.Put(index, newKey, []byte("1"))
		}
		must(err)
	}
	return index
}

func usersByStatus(engine *prolly.Engine, index prolly.Tree, tenant, status string) []prolly.Entry {
	start := statusIndexPrefix(tenant, status)
	end, ok, err := prolly.PrefixEnd(start)
	must(err)
	if !ok {
		end = nil
	}
	entries, err := engine.Range(index, start, end)
	must(err)
	return entries
}

func equalTreeRoots(left, right prolly.Tree) {
	leftRoot, leftPresent, err := left.Root()
	must(err)
	rightRoot, rightPresent, err := right.Root()
	must(err)
	check(leftPresent == rightPresent && bytes.Equal(leftRoot, rightRoot), "incremental index does not match rebuilt index")
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
