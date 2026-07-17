package cosmosdb

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"sort"
	"strings"

	prolly "build.crab/prolly-go"
	"github.com/Azure/azure-sdk-for-go/sdk/azcore"
	"github.com/Azure/azure-sdk-for-go/sdk/data/azcosmos"
)

const transactionLimit = 100

var (
	nodeFamily = []byte("node:")
	rootFamily = []byte("root:")
	hintFamily = []byte("hint:")
)

// ItemClient is the narrow Cosmos item surface consumed by Store. Use New for
// the official SDK client or NewWithClient for controlled tests and wrappers.
type ItemClient interface {
	Read(context.Context, string, string) (Item, error)
	Create(context.Context, string, []byte) error
	Upsert(context.Context, string, []byte) error
	Replace(context.Context, string, string, []byte, string) error
	Delete(context.Context, string, string, string) error
	QueryFamily(context.Context, string, string) ([][]byte, error)
	ExecuteBatch(context.Context, string, []BatchOperation) (BatchResponse, error)
}

type Item struct {
	Value []byte
	ETag  string
}

type BatchOperation struct {
	Kind  string
	ID    string
	Value []byte
	ETag  string
}

type BatchResult struct {
	StatusCode int
}

type BatchResponse struct {
	Success bool
	Results []BatchResult
}

type Options struct {
	AdapterName     string
	KeyPrefix       []byte
	PartitionKey    string
	ReadParallelism uint32
}

type Store struct {
	client  ItemClient
	options Options
}

func New(container *azcosmos.ContainerClient, options Options) *Store {
	if container == nil {
		return NewWithClient(nil, options)
	}
	return NewWithClient(&sdkItemClient{container: container}, options)
}

func NewWithClient(client ItemClient, options Options) *Store {
	if strings.TrimSpace(options.AdapterName) == "" {
		options.AdapterName = "cosmosdb-v1"
	}
	if options.KeyPrefix == nil {
		options.KeyPrefix = []byte("prolly:")
	}
	options.KeyPrefix = clone(options.KeyPrefix)
	if options.PartitionKey == "" {
		options.PartitionKey = "prolly"
	}
	if options.ReadParallelism == 0 {
		options.ReadParallelism = 16
	}
	return &Store{client: client, options: options}
}

func (s *Store) Descriptor(ctx context.Context) (prolly.StoreDescriptor, error) {
	if err := s.ready(ctx); err != nil {
		return prolly.StoreDescriptor{}, err
	}
	limit := uint32(transactionLimit)
	return prolly.StoreDescriptor{
		ProtocolMajor: 1, AdapterName: s.options.AdapterName, Provider: "cosmosdb", SchemaVersion: 1,
		Capabilities: prolly.StoreCapabilities{
			NativeBatchReads: false, AtomicBatchWrites: false, NodeScan: true, Hints: true,
			AtomicNodesAndHint: false, RootScan: true, RootCompareAndSwap: true,
			Transactions: true, ReadParallelism: s.options.ReadParallelism,
		},
		Limits: prolly.StoreLimits{MaxTransactionOperations: &limit},
	}, nil
}

func (s *Store) GetNode(ctx context.Context, key []byte) (prolly.OptionalBytes, error) {
	return s.get(ctx, s.nodeKey(key), "get_node")
}

func (s *Store) PutNode(ctx context.Context, key, value []byte) error {
	return s.upsert(ctx, "node", s.nodeKey(key), value, "put_node")
}

func (s *Store) DeleteNode(ctx context.Context, key []byte) error {
	return s.delete(ctx, s.nodeKey(key), "", true, "delete_node")
}

func (s *Store) BatchNodes(ctx context.Context, mutations []prolly.NodeMutation) error {
	for _, mutation := range mutations {
		var err error
		if mutation.Value.Present {
			err = s.PutNode(ctx, mutation.Key, mutation.Value.Value)
		} else {
			err = s.DeleteNode(ctx, mutation.Key)
		}
		if err != nil {
			return err
		}
	}
	return nil
}

func (s *Store) BatchGetNodesOrdered(ctx context.Context, keys [][]byte) ([]prolly.OptionalBytes, error) {
	result := make([]prolly.OptionalBytes, len(keys))
	for index, key := range keys {
		value, err := s.GetNode(ctx, key)
		if err != nil {
			return nil, err
		}
		result[index] = value
	}
	return result, nil
}

func (s *Store) ListNodeCIDs(ctx context.Context) ([][]byte, error) {
	documents, err := s.queryFamily(ctx, "node")
	if err != nil {
		return nil, err
	}
	prefix := s.familyPrefix(nodeFamily)
	result := make([][]byte, 0, len(documents))
	for _, document := range documents {
		key, decodeErr := document.logicalKey()
		if decodeErr != nil {
			return nil, decodeErr
		}
		if bytes.HasPrefix(key, prefix) && len(key) == len(prefix)+32 {
			result = append(result, clone(key[len(prefix):]))
		}
	}
	sort.Slice(result, func(i, j int) bool { return bytes.Compare(result[i], result[j]) < 0 })
	return result, nil
}

func (s *Store) GetHint(ctx context.Context, namespace, key []byte) (prolly.OptionalBytes, error) {
	return s.get(ctx, s.hintKey(namespace, key), "get_hint")
}

func (s *Store) PutHint(ctx context.Context, namespace, key, value []byte) error {
	return s.upsert(ctx, "hint", s.hintKey(namespace, key), value, "put_hint")
}

func (s *Store) BatchPutNodesWithHint(ctx context.Context, nodes []prolly.NodeEntry, namespace, key, value []byte) error {
	for _, node := range nodes {
		if err := s.PutNode(ctx, node.Key, node.Value); err != nil {
			return err
		}
	}
	return s.PutHint(ctx, namespace, key, value)
}

func (s *Store) GetRootManifest(ctx context.Context, name []byte) (prolly.OptionalBytes, error) {
	return s.get(ctx, s.rootKey(name), "get_root")
}

func (s *Store) PutRootManifest(ctx context.Context, name, manifest []byte) error {
	return s.upsert(ctx, "root", s.rootKey(name), manifest, "put_root")
}

func (s *Store) DeleteRootManifest(ctx context.Context, name []byte) error {
	return s.delete(ctx, s.rootKey(name), "", true, "delete_root")
}

func (s *Store) CompareAndSwapRootManifest(ctx context.Context, name []byte, expected, replacement prolly.OptionalBytes) (prolly.RootCASResult, error) {
	if err := s.ready(ctx); err != nil {
		return prolly.RootCASResult{}, err
	}
	logicalKey := s.rootKey(name)
	id := documentID(logicalKey)
	if !expected.Present {
		if !replacement.Present {
			current, err := s.get(ctx, logicalKey, "root_cas_read")
			return prolly.RootCASResult{Applied: !current.Present, Current: current}, err
		}
		encoded, err := json.Marshal(newDocument(s.options.PartitionKey, "root", logicalKey, replacement.Value))
		if err != nil {
			return prolly.RootCASResult{}, invalidResult("marshal Cosmos document", err)
		}
		err = s.client.Create(ctx, s.options.PartitionKey, encoded)
		if err == nil {
			return prolly.RootCASResult{Applied: true, Current: replacement.Clone()}, nil
		}
		if !isConflict(err) {
			return prolly.RootCASResult{}, cosmosError("root_cas_create", err)
		}
		current, readErr := s.get(ctx, logicalKey, "root_cas_read")
		return prolly.RootCASResult{Current: current}, readErr
	}
	current, err := s.readDocument(ctx, logicalKey)
	if err != nil {
		if isNotFound(err) {
			return prolly.RootCASResult{Current: prolly.MissingBytes()}, nil
		}
		return prolly.RootCASResult{}, cosmosError("root_cas_read", err)
	}
	currentValue, err := current.document.valueBytes()
	if err != nil {
		return prolly.RootCASResult{}, err
	}
	if !bytes.Equal(currentValue, expected.Value) {
		return prolly.RootCASResult{Current: prolly.PresentBytes(currentValue)}, nil
	}
	if replacement.Present {
		encoded, marshalErr := json.Marshal(newDocument(s.options.PartitionKey, "root", logicalKey, replacement.Value))
		if marshalErr != nil {
			return prolly.RootCASResult{}, invalidResult("marshal Cosmos document", marshalErr)
		}
		err = s.client.Replace(ctx, s.options.PartitionKey, id, encoded, current.etag)
	} else {
		err = s.client.Delete(ctx, s.options.PartitionKey, id, current.etag)
	}
	if err == nil {
		return prolly.RootCASResult{Applied: true, Current: replacement.Clone()}, nil
	}
	if !isConflict(err) {
		return prolly.RootCASResult{}, cosmosError("root_cas_write", err)
	}
	latest, readErr := s.get(ctx, logicalKey, "root_cas_read")
	return prolly.RootCASResult{Current: latest}, readErr
}

func (s *Store) ListRootManifests(ctx context.Context) ([]prolly.NamedStoreRoot, error) {
	documents, err := s.queryFamily(ctx, "root")
	if err != nil {
		return nil, err
	}
	prefix := s.familyPrefix(rootFamily)
	result := make([]prolly.NamedStoreRoot, 0, len(documents))
	for _, document := range documents {
		key, keyErr := document.logicalKey()
		value, valueErr := document.valueBytes()
		if keyErr != nil {
			return nil, keyErr
		}
		if valueErr != nil {
			return nil, valueErr
		}
		if bytes.HasPrefix(key, prefix) {
			result = append(result, prolly.NamedStoreRoot{Name: clone(key[len(prefix):]), Manifest: value})
		}
	}
	sort.Slice(result, func(i, j int) bool { return bytes.Compare(result[i].Name, result[j].Name) < 0 })
	return result, nil
}

func (s *Store) CommitTransaction(ctx context.Context, nodes []prolly.NodeMutation, conditions []prolly.RootCondition, roots []prolly.RootWrite) (prolly.StoreTransactionResult, error) {
	if len(nodes)+len(roots) > transactionLimit {
		return prolly.StoreTransactionResult{}, limitError(len(nodes) + len(roots))
	}
	if err := s.ready(ctx); err != nil {
		return prolly.StoreTransactionResult{}, err
	}
	conditionsByName := make(map[string]prolly.RootCondition, len(conditions))
	writtenRoots := make(map[string]struct{}, len(roots))
	for _, condition := range conditions {
		conditionsByName[string(condition.Name)] = condition
	}
	for _, root := range roots {
		writtenRoots[string(root.Name)] = struct{}{}
	}
	operations := make([]BatchOperation, 0, len(nodes)+len(roots)+len(conditions)*2)
	for _, condition := range conditions {
		if _, written := writtenRoots[string(condition.Name)]; written {
			continue
		}
		conditionOps, conflict, err := s.conditionOperations(ctx, condition)
		if err != nil {
			return prolly.StoreTransactionResult{}, err
		}
		if conflict != nil {
			return prolly.StoreTransactionResult{Conflict: conflict}, nil
		}
		operations = append(operations, conditionOps...)
	}
	for _, root := range roots {
		rootOps, conflict, err := s.rootWriteOperations(ctx, root, conditionsByName)
		if err != nil {
			return prolly.StoreTransactionResult{}, err
		}
		if conflict != nil {
			return prolly.StoreTransactionResult{Conflict: conflict}, nil
		}
		operations = append(operations, rootOps...)
	}
	for _, node := range nodes {
		logicalKey := s.nodeKey(node.Key)
		if node.Value.Present {
			encoded, err := json.Marshal(newDocument(s.options.PartitionKey, "node", logicalKey, node.Value.Value))
			if err != nil {
				return prolly.StoreTransactionResult{}, invalidResult("marshal Cosmos document", err)
			}
			operations = append(operations, BatchOperation{Kind: "upsert", Value: encoded})
		} else {
			current, err := s.readDocument(ctx, logicalKey)
			if err != nil {
				if isNotFound(err) {
					continue
				}
				return prolly.StoreTransactionResult{}, cosmosError("transaction_read_node", err)
			}
			operations = append(operations, BatchOperation{Kind: "delete", ID: documentID(logicalKey), ETag: current.etag})
		}
	}
	if len(operations) > transactionLimit {
		return prolly.StoreTransactionResult{}, limitError(len(operations))
	}
	if len(operations) == 0 {
		return prolly.StoreTransactionResult{Applied: true}, nil
	}
	response, err := s.client.ExecuteBatch(ctx, s.options.PartitionKey, operations)
	if err != nil {
		return prolly.StoreTransactionResult{}, cosmosError("transaction", err)
	}
	if response.Success {
		return prolly.StoreTransactionResult{Applied: true}, nil
	}
	for _, condition := range conditions {
		current, readErr := s.GetRootManifest(ctx, condition.Name)
		if readErr != nil {
			return prolly.StoreTransactionResult{}, readErr
		}
		if !optionalEqual(current, condition.Expected) {
			return prolly.StoreTransactionResult{Conflict: conflictFor(condition, current)}, nil
		}
	}
	return prolly.StoreTransactionResult{}, &prolly.StoreError{Code: "provider_error", Message: fmt.Sprintf("Cosmos DB transaction failed: %#v", response.Results)}
}

func (s *Store) conditionOperations(ctx context.Context, condition prolly.RootCondition) ([]BatchOperation, *prolly.StoreTransactionConflict, error) {
	logicalKey := s.rootKey(condition.Name)
	current, err := s.readDocument(ctx, logicalKey)
	if !condition.Expected.Present {
		if err == nil {
			value, decodeErr := current.document.valueBytes()
			if decodeErr != nil {
				return nil, nil, decodeErr
			}
			return nil, conflictFor(condition, prolly.PresentBytes(value)), nil
		}
		if !isNotFound(err) {
			return nil, nil, cosmosError("transaction_condition", err)
		}
		placeholder, _ := json.Marshal(newDocument(s.options.PartitionKey, "root", logicalKey, []byte{}))
		return []BatchOperation{{Kind: "create", Value: placeholder}, {Kind: "delete", ID: documentID(logicalKey)}}, nil, nil
	}
	if err != nil {
		if isNotFound(err) {
			return nil, conflictFor(condition, prolly.MissingBytes()), nil
		}
		return nil, nil, cosmosError("transaction_condition", err)
	}
	value, decodeErr := current.document.valueBytes()
	if decodeErr != nil {
		return nil, nil, decodeErr
	}
	if !bytes.Equal(value, condition.Expected.Value) {
		return nil, conflictFor(condition, prolly.PresentBytes(value)), nil
	}
	encoded, _ := json.Marshal(current.document)
	return []BatchOperation{{Kind: "replace", ID: documentID(logicalKey), Value: encoded, ETag: current.etag}}, nil, nil
}

func (s *Store) rootWriteOperations(ctx context.Context, root prolly.RootWrite, conditions map[string]prolly.RootCondition) ([]BatchOperation, *prolly.StoreTransactionConflict, error) {
	condition, conditioned := conditions[string(root.Name)]
	logicalKey := s.rootKey(root.Name)
	id := documentID(logicalKey)
	if !conditioned {
		if root.Replacement.Present {
			encoded, _ := json.Marshal(newDocument(s.options.PartitionKey, "root", logicalKey, root.Replacement.Value))
			return []BatchOperation{{Kind: "upsert", Value: encoded}}, nil, nil
		}
		current, err := s.readDocument(ctx, logicalKey)
		if isNotFound(err) {
			return nil, nil, nil
		}
		if err != nil {
			return nil, nil, cosmosError("transaction_read_root", err)
		}
		return []BatchOperation{{Kind: "delete", ID: id, ETag: current.etag}}, nil, nil
	}
	if !condition.Expected.Present {
		current, err := s.readDocument(ctx, logicalKey)
		if err == nil {
			value, decodeErr := current.document.valueBytes()
			if decodeErr != nil {
				return nil, nil, decodeErr
			}
			return nil, conflictFor(condition, prolly.PresentBytes(value)), nil
		}
		if !isNotFound(err) {
			return nil, nil, cosmosError("transaction_condition", err)
		}
		if root.Replacement.Present {
			encoded, _ := json.Marshal(newDocument(s.options.PartitionKey, "root", logicalKey, root.Replacement.Value))
			return []BatchOperation{{Kind: "create", Value: encoded}}, nil, nil
		}
		placeholder, _ := json.Marshal(newDocument(s.options.PartitionKey, "root", logicalKey, []byte{}))
		return []BatchOperation{{Kind: "create", Value: placeholder}, {Kind: "delete", ID: id}}, nil, nil
	}
	current, err := s.readDocument(ctx, logicalKey)
	if err != nil {
		if isNotFound(err) {
			return nil, conflictFor(condition, prolly.MissingBytes()), nil
		}
		return nil, nil, cosmosError("transaction_condition", err)
	}
	value, decodeErr := current.document.valueBytes()
	if decodeErr != nil {
		return nil, nil, decodeErr
	}
	if !bytes.Equal(value, condition.Expected.Value) {
		return nil, conflictFor(condition, prolly.PresentBytes(value)), nil
	}
	if root.Replacement.Present {
		encoded, _ := json.Marshal(newDocument(s.options.PartitionKey, "root", logicalKey, root.Replacement.Value))
		return []BatchOperation{{Kind: "replace", ID: id, Value: encoded, ETag: current.etag}}, nil, nil
	}
	return []BatchOperation{{Kind: "delete", ID: id, ETag: current.etag}}, nil, nil
}

func (s *Store) get(ctx context.Context, logicalKey []byte, operation string) (prolly.OptionalBytes, error) {
	current, err := s.readDocument(ctx, logicalKey)
	if err != nil {
		if isNotFound(err) {
			return prolly.MissingBytes(), nil
		}
		return prolly.OptionalBytes{}, cosmosError(operation, err)
	}
	value, err := current.document.valueBytes()
	if err != nil {
		return prolly.OptionalBytes{}, err
	}
	return prolly.PresentBytes(value), nil
}

type readDocument struct {
	document cosmosDocument
	etag     string
}

func (s *Store) readDocument(ctx context.Context, logicalKey []byte) (readDocument, error) {
	if err := s.ready(ctx); err != nil {
		return readDocument{}, err
	}
	item, err := s.client.Read(ctx, s.options.PartitionKey, documentID(logicalKey))
	if err != nil {
		return readDocument{}, err
	}
	var document cosmosDocument
	if err := json.Unmarshal(item.Value, &document); err != nil {
		return readDocument{}, invalidResult("decode Cosmos document", err)
	}
	if document.ID != documentID(logicalKey) || document.Kind != s.options.PartitionKey {
		return readDocument{}, &prolly.StoreError{Code: "invalid_result", Message: "Cosmos document identity does not match requested key"}
	}
	return readDocument{document: document, etag: item.ETag}, nil
}

func (s *Store) upsert(ctx context.Context, family string, logicalKey, value []byte, operation string) error {
	if err := s.ready(ctx); err != nil {
		return err
	}
	encoded, err := json.Marshal(newDocument(s.options.PartitionKey, family, logicalKey, value))
	if err != nil {
		return invalidResult("marshal Cosmos document", err)
	}
	return cosmosError(operation, s.client.Upsert(ctx, s.options.PartitionKey, encoded))
}

func (s *Store) delete(ctx context.Context, logicalKey []byte, etag string, ignoreMissing bool, operation string) error {
	if err := s.ready(ctx); err != nil {
		return err
	}
	err := s.client.Delete(ctx, s.options.PartitionKey, documentID(logicalKey), etag)
	if ignoreMissing && isNotFound(err) {
		return nil
	}
	return cosmosError(operation, err)
}

func (s *Store) queryFamily(ctx context.Context, family string) ([]cosmosDocument, error) {
	if err := s.ready(ctx); err != nil {
		return nil, err
	}
	items, err := s.client.QueryFamily(ctx, s.options.PartitionKey, family)
	if err != nil {
		return nil, cosmosError("query", err)
	}
	result := make([]cosmosDocument, 0, len(items))
	for _, item := range items {
		var document cosmosDocument
		if err := json.Unmarshal(item, &document); err != nil {
			return nil, invalidResult("decode Cosmos query document", err)
		}
		if document.Kind == s.options.PartitionKey && document.Family == family {
			result = append(result, document)
		}
	}
	return result, nil
}

func (s *Store) ready(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if s == nil || s.client == nil {
		return &prolly.StoreError{Code: "invalid_configuration", Message: "Cosmos DB container client is nil"}
	}
	if s.options.PartitionKey == "" {
		return &prolly.StoreError{Code: "invalid_configuration", Message: "Cosmos DB partition key value is empty"}
	}
	return nil
}

func (s *Store) nodeKey(key []byte) []byte         { return s.familyKey(nodeFamily, key) }
func (s *Store) rootKey(key []byte) []byte         { return s.familyKey(rootFamily, key) }
func (s *Store) familyPrefix(family []byte) []byte { return s.familyKey(family, nil) }
func (s *Store) familyKey(family, suffix []byte) []byte {
	result := make([]byte, 0, len(s.options.KeyPrefix)+len(family)+len(suffix))
	result = append(result, s.options.KeyPrefix...)
	result = append(result, family...)
	return append(result, suffix...)
}
func (s *Store) hintKey(namespace, key []byte) []byte {
	result := s.familyPrefix(hintFamily)
	var length [8]byte
	binary.BigEndian.PutUint64(length[:], uint64(len(namespace)))
	result = append(result, length[:]...)
	result = append(result, namespace...)
	return append(result, key...)
}

type cosmosDocument struct {
	ID     string `json:"id"`
	Kind   string `json:"kind"`
	Family string `json:"family"`
	Key    string `json:"key"`
	Value  string `json:"value"`
}

func newDocument(partitionKey, family string, logicalKey, value []byte) cosmosDocument {
	return cosmosDocument{ID: documentID(logicalKey), Kind: partitionKey, Family: family, Key: hex.EncodeToString(logicalKey), Value: base64.StdEncoding.EncodeToString(value)}
}
func documentID(logicalKey []byte) string { return "k" + hex.EncodeToString(logicalKey) }
func (d cosmosDocument) logicalKey() ([]byte, error) {
	value, err := hex.DecodeString(d.Key)
	if err != nil {
		return nil, invalidResult("decode Cosmos document key", err)
	}
	return value, nil
}
func (d cosmosDocument) valueBytes() ([]byte, error) {
	value, err := base64.StdEncoding.DecodeString(d.Value)
	if err != nil {
		return nil, invalidResult("decode Cosmos document value", err)
	}
	return value, nil
}

type sdkItemClient struct{ container *azcosmos.ContainerClient }

func (c *sdkItemClient) Read(ctx context.Context, partition, id string) (Item, error) {
	response, err := c.container.ReadItem(ctx, azcosmos.NewPartitionKeyString(partition), id, nil)
	return Item{Value: clone(response.Value), ETag: string(response.ETag)}, err
}
func (c *sdkItemClient) Create(ctx context.Context, partition string, value []byte) error {
	_, err := c.container.CreateItem(ctx, azcosmos.NewPartitionKeyString(partition), value, nil)
	return err
}
func (c *sdkItemClient) Upsert(ctx context.Context, partition string, value []byte) error {
	_, err := c.container.UpsertItem(ctx, azcosmos.NewPartitionKeyString(partition), value, nil)
	return err
}
func (c *sdkItemClient) Replace(ctx context.Context, partition, id string, value []byte, etag string) error {
	match := azcore.ETag(etag)
	_, err := c.container.ReplaceItem(ctx, azcosmos.NewPartitionKeyString(partition), id, value, &azcosmos.ItemOptions{IfMatchEtag: &match})
	return err
}
func (c *sdkItemClient) Delete(ctx context.Context, partition, id, etag string) error {
	options := &azcosmos.ItemOptions{}
	if etag != "" {
		match := azcore.ETag(etag)
		options.IfMatchEtag = &match
	}
	_, err := c.container.DeleteItem(ctx, azcosmos.NewPartitionKeyString(partition), id, options)
	return err
}
func (c *sdkItemClient) QueryFamily(ctx context.Context, partition, family string) ([][]byte, error) {
	query := "SELECT * FROM c WHERE c.kind = @kind AND c.family = @family"
	pager := c.container.NewQueryItemsPager(query, azcosmos.NewPartitionKeyString(partition), &azcosmos.QueryOptions{
		PageSizeHint:    100,
		QueryParameters: []azcosmos.QueryParameter{{Name: "@kind", Value: partition}, {Name: "@family", Value: family}},
	})
	var result [][]byte
	for pager.More() {
		page, err := pager.NextPage(ctx)
		if err != nil {
			return nil, err
		}
		for _, item := range page.Items {
			result = append(result, clone(item))
		}
	}
	return result, nil
}
func (c *sdkItemClient) ExecuteBatch(ctx context.Context, partition string, operations []BatchOperation) (BatchResponse, error) {
	batch := c.container.NewTransactionalBatch(azcosmos.NewPartitionKeyString(partition))
	for _, operation := range operations {
		var options *azcosmos.TransactionalBatchItemOptions
		if operation.ETag != "" {
			match := azcore.ETag(operation.ETag)
			options = &azcosmos.TransactionalBatchItemOptions{IfMatchETag: &match}
		}
		switch operation.Kind {
		case "create":
			batch.CreateItem(operation.Value, options)
		case "upsert":
			batch.UpsertItem(operation.Value, options)
		case "replace":
			batch.ReplaceItem(operation.ID, operation.Value, options)
		case "delete":
			batch.DeleteItem(operation.ID, options)
		default:
			return BatchResponse{}, &prolly.StoreError{Code: "invalid_argument", Message: "unknown Cosmos batch operation " + operation.Kind}
		}
	}
	response, err := c.container.ExecuteTransactionalBatch(ctx, batch, nil)
	result := BatchResponse{Success: response.Success, Results: make([]BatchResult, len(response.OperationResults))}
	for index, operation := range response.OperationResults {
		result.Results[index] = BatchResult{StatusCode: int(operation.StatusCode)}
	}
	return result, err
}

func isNotFound(err error) bool { return responseStatus(err) == http.StatusNotFound }
func isConflict(err error) bool {
	status := responseStatus(err)
	return status == http.StatusConflict || status == http.StatusPreconditionFailed || status == http.StatusNotFound
}
func responseStatus(err error) int {
	var responseErr *azcore.ResponseError
	if errors.As(err, &responseErr) {
		return responseErr.StatusCode
	}
	return 0
}
func cosmosError(operation string, err error) error {
	if err == nil {
		return nil
	}
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return err
	}
	var responseErr *azcore.ResponseError
	providerCode := ""
	status := 0
	if errors.As(err, &responseErr) {
		providerCode, status = responseErr.ErrorCode, responseErr.StatusCode
	}
	retryable := status == http.StatusRequestTimeout || status == http.StatusTooManyRequests || status >= 500
	return &prolly.StoreError{Code: "provider_error", Message: operation + ": " + err.Error(), Retryable: retryable, ProviderCode: providerCode, Cause: err}
}
func invalidResult(operation string, err error) error {
	return &prolly.StoreError{Code: "invalid_result", Message: operation + ": " + err.Error(), Cause: err}
}
func limitError(operations int) error {
	return &prolly.StoreError{Code: "limit_exceeded", Message: fmt.Sprintf("Cosmos DB transaction has %d operations, exceeding the %d operation limit", operations, transactionLimit)}
}
func conflictFor(condition prolly.RootCondition, current prolly.OptionalBytes) *prolly.StoreTransactionConflict {
	return &prolly.StoreTransactionConflict{Name: clone(condition.Name), Expected: condition.Expected.Clone(), Current: current}
}
func optionalEqual(left, right prolly.OptionalBytes) bool {
	return left.Present == right.Present && (!left.Present || bytes.Equal(left.Value, right.Value))
}
func clone(value []byte) []byte {
	if value == nil {
		return nil
	}
	result := make([]byte, len(value))
	copy(result, value)
	return result
}
