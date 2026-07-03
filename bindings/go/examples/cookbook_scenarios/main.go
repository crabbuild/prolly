package main

import (
	"log"
	"os"
	"os/exec"
)

var scenarios = []string{
	"./examples/batch_build",
	"./examples/local_first_state",
	"./examples/resolver",
	"./examples/crdt_merge",
	"./examples/conversation_memory",
	"./examples/agent_event_log",
	"./examples/background_compaction",
	"./examples/deterministic_rag_snapshot",
	"./examples/document_chunk_index",
	"./examples/vector_sidecar",
	"./examples/provenance_values",
	"./examples/materialized_view",
	"./examples/filesystem_snapshot",
	"./examples/durable_sqlite",
}

func main() {
	for _, scenario := range scenarios {
		cmd := exec.Command("go", "run", scenario)
		cmd.Stdout = os.Stdout
		cmd.Stderr = os.Stderr
		if err := cmd.Run(); err != nil {
			log.Fatal(err)
		}
	}
}
