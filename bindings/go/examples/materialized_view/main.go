package main

import (
	prolly "build.crab/prolly-go"
	"bytes"
	"fmt"
	"log"
	"sort"
	"strconv"
	"strings"
)

func main() {
	if err := MaterializedView(); err != nil {
		log.Fatal(err)
	}
}

func MaterializedView() error {
	engine, err := prolly.OpenMemory()
	if err != nil {
		return err
	}
	defer engine.Close()
	o1 := order{"acme", "o1", "paid", 1200}
	o2 := order{"acme", "o2", "open", 500}
	sourceV1, err := engine.Batch(mustCreate(engine), []prolly.Mutation{
		{Kind: "upsert", Key: orderKey(o1), Value: encodeOrder(o1)},
		{Kind: "upsert", Key: orderKey(o2), Value: encodeOrder(o2)},
	})
	if err != nil {
		return err
	}
	paidO2 := order{"acme", "o2", "paid", 500}
	sourceV2, err := engine.Put(sourceV1, orderKey(paidO2), encodeOrder(paidO2))
	if err != nil {
		return err
	}
	viewV2, err := buildRevenueView(engine, sourceV2)
	if err != nil {
		return err
	}
	if err := requireGet(engine, viewV2, viewKeyString("acme", "paid"), "1700"); err != nil {
		return err
	}
	_, ok, err := engine.Get(viewV2, viewKey("acme", "open"))
	if err != nil {
		return err
	}
	if ok {
		return fmt.Errorf("open aggregate should be absent")
	}
	diff, err := engine.Diff(sourceV1, sourceV2)
	if err != nil {
		return err
	}
	fmt.Printf("materialized_view: folded %d source diff\n", len(diff))
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

type order struct {
	tenant string
	id     string
	status string
	cents  int
}

func orderKey(order order) []byte {
	return bytesOf("orders/source/tenant/" + order.tenant + "/order/" + order.id)
}

func encodeOrder(order order) []byte {
	return bytesOf(fmt.Sprintf("%s|%s|%s|%d", order.tenant, order.id, order.status, order.cents))
}

func decodeOrder(value []byte) (order, error) {
	parts := strings.Split(string(value), "|")
	if len(parts) != 4 {
		return order{}, fmt.Errorf("invalid order value %q", value)
	}
	cents, err := strconv.Atoi(parts[3])
	if err != nil {
		return order{}, err
	}
	return order{tenant: parts[0], id: parts[1], status: parts[2], cents: cents}, nil
}

func viewKey(tenant string, status string) []byte {
	return bytesOf("orders/view/by-status/tenant/" + tenant + "/status/" + status)
}

func buildRevenueView(engine *prolly.Engine, source prolly.Tree) (prolly.Tree, error) {
	rows, err := engine.Range(source, bytesOf("orders/source/"), bytesOf("orders/source0"))
	if err != nil {
		return prolly.Tree{}, err
	}
	totals := map[string]int{}
	for _, row := range rows {
		order, err := decodeOrder(row.Value)
		if err != nil {
			return prolly.Tree{}, err
		}
		totals[order.tenant+"|"+order.status] += order.cents
	}
	keys := make([]string, 0, len(totals))
	for key := range totals {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	mutations := make([]prolly.Mutation, 0, len(keys))
	for _, key := range keys {
		parts := strings.Split(key, "|")
		mutations = append(mutations, upsert(viewKeyString(parts[0], parts[1]), []byte(strconv.Itoa(totals[key]))))
	}
	return engine.Batch(mustCreate(engine), mutations)
}

func viewKeyString(tenant string, status string) string {
	return "orders/view/by-status/tenant/" + tenant + "/status/" + status
}
