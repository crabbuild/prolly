package main

import (
	prolly "build.crab/prolly-go"
	"fmt"
	"log"
)

func main() {
	if err := AgentEventLog(); err != nil {
		log.Fatal(err)
	}
}

func AgentEventLog() error {
	engine, err := prolly.OpenMemory()
	if err != nil {
		return err
	}
	defer engine.Close()

	root := bytesOf("agent-log/run-7/root/events/current")
	tree, err := engine.Batch(mustCreate(engine), []prolly.Mutation{
		upsertText("agent-log/run-7/event/1783036805000/0001", "user|Summarize the plan"),
		upsertText("agent-log/run-7/event/1783036805000/0002", "tool-call|search-docs"),
		upsertText("agent-log/run-7/event/1783036806000/0003", "assistant|Plan ready"),
	})
	if err != nil {
		return err
	}
	if err := engine.PublishNamedRoot(root, tree); err != nil {
		return err
	}
	loaded, err := engine.LoadNamedRoot(root)
	if err != nil {
		return err
	}
	page, err := engine.RangePage(*loaded, nil, nil, 2)
	if err != nil {
		return err
	}
	if len(page.Entries) != 2 || page.NextCursor == nil {
		return fmt.Errorf("expected first event page and cursor")
	}
	fmt.Printf("agent_event_log: first page has %d events\n", len(page.Entries))
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
