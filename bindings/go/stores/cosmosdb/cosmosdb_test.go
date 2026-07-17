package cosmosdb

import (
	"context"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"os"
	"strconv"
	"sync"
	"testing"
	"time"

	prolly "build.crab/prolly-go"
	"build.crab/prolly-go/storetest"
	"github.com/Azure/azure-sdk-for-go/sdk/azcore"
	"github.com/Azure/azure-sdk-for-go/sdk/data/azcosmos"
)

func TestDocumentLayoutMatchesRust(t *testing.T) {
	document := newDocument("tenant-a", "node", []byte("node:abc"), []byte("value"))
	encoded, err := json.Marshal(document)
	if err != nil {
		t.Fatal(err)
	}
	var fields map[string]string
	if err := json.Unmarshal(encoded, &fields); err != nil {
		t.Fatal(err)
	}
	expected := map[string]string{
		"id": "k" + hex.EncodeToString([]byte("node:abc")), "kind": "tenant-a",
		"family": "node", "key": hex.EncodeToString([]byte("node:abc")),
		"value": base64.StdEncoding.EncodeToString([]byte("value")),
	}
	if !mapsEqual(fields, expected) {
		t.Fatalf("document = %#v", fields)
	}
}

func TestTransactionLimitIsPreflighted(t *testing.T) {
	store := New(nil, Options{})
	nodes := make([]prolly.NodeMutation, 101)
	for index := range nodes {
		nodes[index] = prolly.UpsertNode([]byte{byte(index)}, []byte("value"))
	}
	_, err := store.CommitTransaction(context.Background(), nodes, nil, nil)
	var storeErr *prolly.StoreError
	if err == nil || !errors.As(err, &storeErr) || storeErr.Code != "limit_exceeded" {
		t.Fatalf("error = %#v", err)
	}
}

func TestAzureErrorClassification(t *testing.T) {
	for _, test := range []struct {
		status    int
		retryable bool
	}{
		{status: http.StatusTooManyRequests, retryable: true},
		{status: http.StatusServiceUnavailable, retryable: true},
		{status: http.StatusPreconditionFailed, retryable: false},
	} {
		err := cosmosError("test", &azcore.ResponseError{StatusCode: test.status, ErrorCode: http.StatusText(test.status)})
		var storeErr *prolly.StoreError
		if !errors.As(err, &storeErr) || storeErr.Retryable != test.retryable || storeErr.ProviderCode == "" {
			t.Fatalf("status %d: error = %#v", test.status, err)
		}
	}
}

func TestCosmosSDKContractConformance(t *testing.T) {
	client := newMemoryClient()
	store := NewWithClient(client, Options{PartitionKey: "test", KeyPrefix: []byte("prolly:test:")})
	storetest.RunWithStore(t, prolly.RemoteStore(store))
	if len(client.matchedETags) == 0 {
		t.Fatal("root CAS and transactions never propagated an ETag")
	}
}

func TestCosmosLiveConformance(t *testing.T) {
	endpoint := os.Getenv("PROLLY_COSMOS_ENDPOINT")
	key := os.Getenv("PROLLY_COSMOS_KEY")
	databaseID := os.Getenv("PROLLY_COSMOS_DATABASE")
	if endpoint == "" || key == "" || databaseID == "" {
		t.Skip("PROLLY_COSMOS_ENDPOINT, PROLLY_COSMOS_KEY, and PROLLY_COSMOS_DATABASE are required")
	}
	credential, err := azcosmos.NewKeyCredential(key)
	if err != nil {
		t.Fatal(err)
	}
	client, err := azcosmos.NewClientWithKey(endpoint, credential, nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(client.Close)
	containerID := fmt.Sprintf("prolly-go-%d", time.Now().UnixNano())
	container, err := EnsureDatabaseAndContainer(context.Background(), client, databaseID, containerID)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _, _ = container.Delete(context.Background(), nil) })
	storetest.RunWithStore(t, prolly.RemoteStore(New(container, Options{PartitionKey: "prolly-test"})))
}

func mapsEqual(left, right map[string]string) bool {
	if len(left) != len(right) {
		return false
	}
	for key, value := range left {
		if right[key] != value {
			return false
		}
	}
	return true
}

type memoryRecord struct {
	value []byte
	etag  string
}

type memoryClient struct {
	mu           sync.Mutex
	items        map[string]memoryRecord
	nextETag     int
	matchedETags []string
}

func newMemoryClient() *memoryClient {
	return &memoryClient{items: make(map[string]memoryRecord), nextETag: 1}
}

func (c *memoryClient) Read(ctx context.Context, partition, id string) (Item, error) {
	if err := ctx.Err(); err != nil {
		return Item{}, err
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	record, ok := c.items[partition+"\x00"+id]
	if !ok {
		return Item{}, responseError(404)
	}
	return Item{Value: clone(record.value), ETag: record.etag}, nil
}

func (c *memoryClient) Create(ctx context.Context, partition string, value []byte) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	id, err := itemID(value)
	if err != nil {
		return err
	}
	key := partition + "\x00" + id
	if _, exists := c.items[key]; exists {
		return responseError(409)
	}
	c.items[key] = c.record(value)
	return nil
}

func (c *memoryClient) Upsert(ctx context.Context, partition string, value []byte) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	id, err := itemID(value)
	if err != nil {
		return err
	}
	c.items[partition+"\x00"+id] = c.record(value)
	return nil
}

func (c *memoryClient) Replace(ctx context.Context, partition, id string, value []byte, etag string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	key := partition + "\x00" + id
	current, exists := c.items[key]
	if !exists {
		return responseError(404)
	}
	if etag != "" {
		c.matchedETags = append(c.matchedETags, etag)
		if current.etag != etag {
			return responseError(412)
		}
	}
	c.items[key] = c.record(value)
	return nil
}

func (c *memoryClient) Delete(ctx context.Context, partition, id, etag string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	key := partition + "\x00" + id
	current, exists := c.items[key]
	if !exists {
		return responseError(404)
	}
	if etag != "" {
		c.matchedETags = append(c.matchedETags, etag)
		if current.etag != etag {
			return responseError(412)
		}
	}
	delete(c.items, key)
	return nil
}

func (c *memoryClient) QueryFamily(ctx context.Context, partition, family string) ([][]byte, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	var result [][]byte
	for key, record := range c.items {
		if len(key) < len(partition)+1 || key[:len(partition)+1] != partition+"\x00" {
			continue
		}
		var document cosmosDocument
		if err := json.Unmarshal(record.value, &document); err != nil {
			return nil, err
		}
		if document.Family == family {
			result = append(result, clone(record.value))
		}
	}
	return result, nil
}

func (c *memoryClient) ExecuteBatch(ctx context.Context, partition string, operations []BatchOperation) (BatchResponse, error) {
	if err := ctx.Err(); err != nil {
		return BatchResponse{}, err
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	working := make(map[string]memoryRecord, len(c.items))
	for key, record := range c.items {
		working[key] = memoryRecord{value: clone(record.value), etag: record.etag}
	}
	results := make([]BatchResult, len(operations))
	for index, operation := range operations {
		status := c.applyOperation(working, partition, operation)
		results[index] = BatchResult{StatusCode: status}
		if status < 200 || status >= 300 {
			for remaining := index + 1; remaining < len(results); remaining++ {
				results[remaining] = BatchResult{StatusCode: 424}
			}
			return BatchResponse{Results: results}, nil
		}
	}
	c.items = working
	return BatchResponse{Success: true, Results: results}, nil
}

func (c *memoryClient) applyOperation(items map[string]memoryRecord, partition string, operation BatchOperation) int {
	id := operation.ID
	if id == "" {
		var err error
		id, err = itemID(operation.Value)
		if err != nil {
			return 400
		}
	}
	key := partition + "\x00" + id
	current, exists := items[key]
	switch operation.Kind {
	case "create":
		if exists {
			return 409
		}
		items[key] = c.record(operation.Value)
		return 201
	case "upsert":
		items[key] = c.record(operation.Value)
		return 200
	case "replace":
		if !exists {
			return 404
		}
		if operation.ETag != "" && current.etag != operation.ETag {
			return 412
		}
		if operation.ETag != "" {
			c.matchedETags = append(c.matchedETags, operation.ETag)
		}
		items[key] = c.record(operation.Value)
		return 200
	case "delete":
		if !exists {
			return 404
		}
		if operation.ETag != "" && current.etag != operation.ETag {
			return 412
		}
		if operation.ETag != "" {
			c.matchedETags = append(c.matchedETags, operation.ETag)
		}
		delete(items, key)
		return 204
	default:
		return 400
	}
}

func (c *memoryClient) record(value []byte) memoryRecord {
	etag := strconv.Itoa(c.nextETag)
	c.nextETag++
	return memoryRecord{value: clone(value), etag: etag}
}

func itemID(value []byte) (string, error) {
	var header struct {
		ID string `json:"id"`
	}
	if err := json.Unmarshal(value, &header); err != nil {
		return "", err
	}
	return header.ID, nil
}

func responseError(status int) error {
	return &azcore.ResponseError{StatusCode: status, ErrorCode: http.StatusText(status)}
}
