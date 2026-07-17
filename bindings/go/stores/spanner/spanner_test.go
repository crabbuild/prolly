package spanner

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"os"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	prolly "build.crab/prolly-go"
	"build.crab/prolly-go/storetest"
	gspanner "cloud.google.com/go/spanner"
	database "cloud.google.com/go/spanner/admin/database/apiv1"
	databasepb "cloud.google.com/go/spanner/admin/database/apiv1/databasepb"
	instance "cloud.google.com/go/spanner/admin/instance/apiv1"
	instancepb "cloud.google.com/go/spanner/admin/instance/apiv1/instancepb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestDDLMatchesRust(t *testing.T) {
	ddl := strings.Join(DDLStatements, ";\n")
	for _, fragment := range []string{
		"CREATE TABLE ProllyNodes", "Cid BYTES(32) NOT NULL", "Node BYTES(MAX) NOT NULL", "PRIMARY KEY (Cid)",
		"CREATE TABLE ProllyHints", "Namespace BYTES(MAX) NOT NULL", "HintKey BYTES(MAX) NOT NULL", "Value BYTES(MAX) NOT NULL", "PRIMARY KEY (Namespace, HintKey)",
		"CREATE TABLE ProllyRoots", "Name BYTES(MAX) NOT NULL", "Manifest BYTES(MAX) NOT NULL", "PRIMARY KEY (Name)",
	} {
		if !strings.Contains(ddl, fragment) {
			t.Fatalf("DDL missing %q", fragment)
		}
	}
}

func TestNewDefaults(t *testing.T) {
	store := New(nil, Options{})
	_, err := store.Descriptor(context.Background())
	if err == nil {
		t.Fatal("nil SDK client should be rejected")
	}
	var _ prolly.RemoteStore = store
}

func TestSpannerSDKContractConformance(t *testing.T) {
	store := NewWithClient(newMemoryClient(), Options{})
	storetest.RunWithStore(t, prolly.RemoteStore(store))
}

func TestSpannerEmulatorConformance(t *testing.T) {
	if os.Getenv("SPANNER_EMULATOR_HOST") == "" {
		t.Skip("SPANNER_EMULATOR_HOST is not set")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	projectID, instanceID := "prolly-test", "prolly-test"
	instanceAdmin, err := instance.NewInstanceAdminClient(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer instanceAdmin.Close()
	instanceName := fmt.Sprintf("projects/%s/instances/%s", projectID, instanceID)
	operation, err := instanceAdmin.CreateInstance(ctx, &instancepb.CreateInstanceRequest{
		Parent: "projects/" + projectID, InstanceId: instanceID,
		Instance: &instancepb.Instance{Config: "projects/" + projectID + "/instanceConfigs/emulator-config", DisplayName: instanceID, NodeCount: 1},
	})
	if err == nil {
		if _, err := operation.Wait(ctx); err != nil {
			t.Fatal(err)
		}
	} else if status.Code(err) != codes.AlreadyExists {
		t.Fatal(err)
	}
	databaseAdmin, err := database.NewDatabaseAdminClient(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer databaseAdmin.Close()
	databaseID := fmt.Sprintf("prolly_go_%d", time.Now().UnixNano())
	databaseName := instanceName + "/databases/" + databaseID
	databaseOperation, err := databaseAdmin.CreateDatabase(ctx, &databasepb.CreateDatabaseRequest{
		Parent: instanceName, CreateStatement: "CREATE DATABASE `" + databaseID + "`", ExtraStatements: append([]string(nil), DDLStatements...),
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := databaseOperation.Wait(ctx); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_ = databaseAdmin.DropDatabase(context.Background(), &databasepb.DropDatabaseRequest{Database: databaseName})
	})
	client, err := gspanner.NewClient(ctx, databaseName)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(client.Close)
	storetest.RunWithStore(t, prolly.RemoteStore(New(client, Options{})))
}

func TestSpannerErrorClassification(t *testing.T) {
	for _, test := range []struct {
		code      codes.Code
		retryable bool
	}{
		{code: codes.Aborted, retryable: true},
		{code: codes.Unavailable, retryable: true},
		{code: codes.ResourceExhausted, retryable: true},
		{code: codes.FailedPrecondition, retryable: false},
	} {
		err := spannerError("test", status.Error(test.code, "failure"))
		var storeErr *prolly.StoreError
		if !errors.As(err, &storeErr) || storeErr.Retryable != test.retryable || storeErr.ProviderCode != test.code.String() {
			t.Fatalf("code %s: error = %#v", test.code, err)
		}
	}
}

func TestConcurrentRootCASAppliesExactlyOnce(t *testing.T) {
	store := NewWithClient(newMemoryClient(), Options{})
	results := make(chan prolly.RootCASResult, 2)
	errorsChannel := make(chan error, 2)
	var wait sync.WaitGroup
	for _, value := range [][]byte{[]byte("one"), []byte("two")} {
		wait.Add(1)
		go func(value []byte) {
			defer wait.Done()
			result, err := store.CompareAndSwapRootManifest(context.Background(), []byte("root"), prolly.MissingBytes(), prolly.PresentBytes(value))
			results <- result
			errorsChannel <- err
		}(value)
	}
	wait.Wait()
	close(results)
	close(errorsChannel)
	for err := range errorsChannel {
		if err != nil {
			t.Fatal(err)
		}
	}
	applied := 0
	for result := range results {
		if result.Applied {
			applied++
		}
	}
	if applied != 1 {
		t.Fatalf("applied = %d", applied)
	}
}

type memoryState struct {
	nodes map[string][]byte
	hints map[string][]byte
	roots map[string][]byte
}

type memoryClient struct {
	mu    sync.Mutex
	state memoryState
}

func newMemoryClient() *memoryClient {
	return &memoryClient{state: emptyState()}
}

func emptyState() memoryState {
	return memoryState{nodes: make(map[string][]byte), hints: make(map[string][]byte), roots: make(map[string][]byte)}
}

func (c *memoryClient) GetNode(ctx context.Context, key []byte) (prolly.OptionalBytes, error) {
	if err := ctx.Err(); err != nil {
		return prolly.OptionalBytes{}, err
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	return optional(c.state.nodes[string(key)]), nil
}

func (c *memoryClient) GetHint(ctx context.Context, namespace, key []byte) (prolly.OptionalBytes, error) {
	if err := ctx.Err(); err != nil {
		return prolly.OptionalBytes{}, err
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	return optional(c.state.hints[hintMapKey(namespace, key)]), nil
}

func (c *memoryClient) GetRoot(ctx context.Context, name []byte) (prolly.OptionalBytes, error) {
	if err := ctx.Err(); err != nil {
		return prolly.OptionalBytes{}, err
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	return optional(c.state.roots[string(name)]), nil
}

func (c *memoryClient) ListNodeCIDs(ctx context.Context) ([][]byte, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	result := make([][]byte, 0, len(c.state.nodes))
	for key := range c.state.nodes {
		result = append(result, []byte(key))
	}
	sort.Slice(result, func(i, j int) bool { return bytes.Compare(result[i], result[j]) < 0 })
	return result, nil
}

func (c *memoryClient) ListRoots(ctx context.Context) ([]RootRecord, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	result := make([]RootRecord, 0, len(c.state.roots))
	for name, manifest := range c.state.roots {
		result = append(result, RootRecord{Name: []byte(name), Manifest: clone(manifest)})
	}
	sort.Slice(result, func(i, j int) bool { return bytes.Compare(result[i].Name, result[j].Name) < 0 })
	return result, nil
}

func (c *memoryClient) Apply(ctx context.Context, mutations []Mutation) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	state := cloneState(c.state)
	applyMutations(&state, mutations)
	c.state = state
	return nil
}

func (c *memoryClient) ReadWrite(ctx context.Context, function func(context.Context, Transaction) error) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	state := cloneState(c.state)
	if err := function(ctx, &memoryTransaction{state: &state}); err != nil {
		return err
	}
	c.state = state
	return nil
}

type memoryTransaction struct{ state *memoryState }

func (t *memoryTransaction) GetRoot(ctx context.Context, name []byte) (prolly.OptionalBytes, error) {
	if err := ctx.Err(); err != nil {
		return prolly.OptionalBytes{}, err
	}
	return optional(t.state.roots[string(name)]), nil
}

func (t *memoryTransaction) Buffer(mutations []Mutation) error {
	applyMutations(t.state, mutations)
	return nil
}

func applyMutations(state *memoryState, mutations []Mutation) {
	for _, mutation := range mutations {
		switch mutation.Kind {
		case MutationUpsertNode:
			state.nodes[string(mutation.Key)] = clone(mutation.Value)
		case MutationDeleteNode:
			delete(state.nodes, string(mutation.Key))
		case MutationUpsertHint:
			state.hints[hintMapKey(mutation.Namespace, mutation.Key)] = clone(mutation.Value)
		case MutationUpsertRoot:
			state.roots[string(mutation.Key)] = clone(mutation.Value)
		case MutationDeleteRoot:
			delete(state.roots, string(mutation.Key))
		}
	}
}

func cloneState(state memoryState) memoryState {
	result := emptyState()
	for key, value := range state.nodes {
		result.nodes[key] = clone(value)
	}
	for key, value := range state.hints {
		result.hints[key] = clone(value)
	}
	for key, value := range state.roots {
		result.roots[key] = clone(value)
	}
	return result
}

func optional(value []byte) prolly.OptionalBytes {
	if value == nil {
		return prolly.MissingBytes()
	}
	return prolly.PresentBytes(value)
}

func hintMapKey(namespace, key []byte) string {
	var length [8]byte
	binary.BigEndian.PutUint64(length[:], uint64(len(namespace)))
	return string(length[:]) + string(namespace) + string(key)
}
