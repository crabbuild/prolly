package main

import (
	"encoding/json"
	"fmt"
	"os"
	"regexp"
	"sort"
)

var requiredProviders = []string{"cosmosdb", "dynamodb", "mysql", "postgresql", "redis", "spanner", "sqlite"}
var goVersion = regexp.MustCompile(`^1\.(2[4-9]|[3-9][0-9])\.0$`)

type manifest struct {
	ProtocolMajor int                           `json:"protocol_major"`
	Languages     map[string]map[string]adapter `json:"languages"`
}

type adapter struct {
	Status        string       `json:"status"`
	Reason        string       `json:"reason"`
	Module        string       `json:"module"`
	SDKModule     string       `json:"sdk_module"`
	SDKVersion    string       `json:"sdk_version"`
	MinimumGo     string       `json:"minimum_go"`
	ProtocolMajor int          `json:"protocol_major"`
	SchemaVersion int          `json:"schema_version"`
	Capabilities  capabilities `json:"capabilities"`
	Limits        limits       `json:"limits"`
	Evidence      []string     `json:"evidence"`
}

type capabilities struct {
	NativeBatchReads   bool `json:"native_batch_reads"`
	AtomicBatchWrites  bool `json:"atomic_batch_writes"`
	NodeScan           bool `json:"node_scan"`
	Hints              bool `json:"hints"`
	AtomicNodesAndHint bool `json:"atomic_nodes_and_hint"`
	RootScan           bool `json:"root_scan"`
	RootCompareAndSwap bool `json:"root_compare_and_swap"`
	Transactions       bool `json:"transactions"`
	ReadParallelism    int  `json:"read_parallelism"`
}

type limits struct {
	MaxBatchReadItems        *int `json:"max_batch_read_items"`
	MaxBatchWriteItems       *int `json:"max_batch_write_items"`
	MaxTransactionOperations *int `json:"max_transaction_operations"`
	MaxNodeBytes             *int `json:"max_node_bytes"`
}

func main() {
	if len(os.Args) != 2 {
		fail("usage: verifycompat <compatibility.json>")
	}
	contents, err := os.ReadFile(os.Args[1])
	if err != nil {
		fail("read manifest: %v", err)
	}
	var value manifest
	if err := json.Unmarshal(contents, &value); err != nil {
		fail("decode manifest: %v", err)
	}
	if value.ProtocolMajor != 1 {
		fail("protocol_major must be 1")
	}
	goAdapters := value.Languages["go"]
	if len(goAdapters) != len(requiredProviders) {
		fail("Go must contain exactly seven providers, got %d", len(goAdapters))
	}
	actual := make([]string, 0, len(goAdapters))
	for provider := range goAdapters {
		actual = append(actual, provider)
	}
	sort.Strings(actual)
	for index, provider := range requiredProviders {
		if actual[index] != provider {
			fail("Go provider set mismatch: got %v", actual)
		}
		validate(provider, goAdapters[provider])
	}
	for language, adapters := range value.Languages {
		for provider, entry := range adapters {
			if entry.Status == "unsupported" && entry.Reason == "" {
				fail("%s/%s unsupported entry needs a reason", language, provider)
			}
		}
	}
	fmt.Printf("compatibility manifest valid: protocol 1, %d Go providers\n", len(goAdapters))
}

func validate(provider string, entry adapter) {
	if entry.Status != "supported" || entry.Module == "" || entry.SDKModule == "" || entry.SDKVersion == "" {
		fail("Go/%s must be supported with module and SDK metadata", provider)
	}
	if entry.ProtocolMajor != 1 || entry.SchemaVersion != 1 {
		fail("Go/%s must use protocol and schema version 1", provider)
	}
	if !goVersion.MatchString(entry.MinimumGo) {
		fail("Go/%s has invalid minimum_go %q", provider, entry.MinimumGo)
	}
	if entry.Capabilities.ReadParallelism < 1 {
		fail("Go/%s read_parallelism must be positive", provider)
	}
	if entry.Capabilities.AtomicNodesAndHint && !entry.Capabilities.Hints {
		fail("Go/%s atomic_nodes_and_hint requires hints", provider)
	}
	if len(entry.Evidence) == 0 {
		fail("Go/%s needs at least one evidence command", provider)
	}
	for name, limit := range map[string]*int{
		"max_batch_read_items":       entry.Limits.MaxBatchReadItems,
		"max_batch_write_items":      entry.Limits.MaxBatchWriteItems,
		"max_transaction_operations": entry.Limits.MaxTransactionOperations,
		"max_node_bytes":             entry.Limits.MaxNodeBytes,
	} {
		if limit != nil && *limit < 1 {
			fail("Go/%s %s must be positive", provider, name)
		}
	}
}

func fail(format string, args ...any) {
	fmt.Fprintf(os.Stderr, format+"\n", args...)
	os.Exit(1)
}
