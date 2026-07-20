package spanner

import (
	"bytes"
	"context"
	"errors"
	"sort"
	"strings"

	prolly "build.crab/prolly-go"
	gspanner "cloud.google.com/go/spanner"
	"google.golang.org/api/iterator"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const (
	nodesTable = "ProllyNodes"
	hintsTable = "ProllyHints"
	rootsTable = "ProllyRoots"
)

type MutationKind uint8

const (
	MutationUpsertNode MutationKind = iota + 1
	MutationDeleteNode
	MutationUpsertHint
	MutationUpsertRoot
	MutationDeleteRoot
)

type Mutation struct {
	Kind      MutationKind
	Key       []byte
	Namespace []byte
	Value     []byte
}

type RootRecord struct {
	Name     []byte
	Manifest []byte
}

// Transaction is the narrow read/write transaction surface Store requires.
type Transaction interface {
	GetRoot(context.Context, []byte) (prolly.OptionalBytes, error)
	Buffer([]Mutation) error
}

// Client is the narrow provider surface wrapped around *spanner.Client.
type Client interface {
	GetNode(context.Context, []byte) (prolly.OptionalBytes, error)
	GetHint(context.Context, []byte, []byte) (prolly.OptionalBytes, error)
	GetRoot(context.Context, []byte) (prolly.OptionalBytes, error)
	ListNodeCIDs(context.Context) ([][]byte, error)
	ListRoots(context.Context) ([]RootRecord, error)
	Apply(context.Context, []Mutation) error
	ReadWrite(context.Context, func(context.Context, Transaction) error) error
}

type Options struct {
	AdapterName     string
	ReadParallelism uint32
}

type Store struct {
	client  Client
	options Options
}

func New(client *gspanner.Client, options Options) *Store {
	if client == nil {
		return NewWithClient(nil, options)
	}
	return NewWithClient(&sdkClient{client: client}, options)
}

func NewWithClient(client Client, options Options) *Store {
	if strings.TrimSpace(options.AdapterName) == "" {
		options.AdapterName = "spanner-v1"
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
	return prolly.StoreDescriptor{
		ProtocolMajor: prolly.StoreProtocolMajor, AdapterName: s.options.AdapterName, Provider: "spanner", SchemaVersion: 1,
		Capabilities: prolly.StoreCapabilities{
			NativeBatchReads: false, AtomicBatchWrites: true, NodeScan: true, Hints: true,
			AtomicNodesAndHint: true, RootScan: true, RootCompareAndSwap: true,
			Transactions: true, ReadParallelism: s.options.ReadParallelism,
		},
	}, nil
}

func (s *Store) GetNode(ctx context.Context, key []byte) (prolly.OptionalBytes, error) {
	if err := s.ready(ctx); err != nil {
		return prolly.OptionalBytes{}, err
	}
	value, err := s.client.GetNode(ctx, key)
	return value, spannerError("get_node", err)
}

func (s *Store) PutNode(ctx context.Context, key, value []byte) error {
	return s.apply(ctx, []Mutation{{Kind: MutationUpsertNode, Key: clone(key), Value: clone(value)}}, "put_node")
}

func (s *Store) DeleteNode(ctx context.Context, key []byte) error {
	return s.apply(ctx, []Mutation{{Kind: MutationDeleteNode, Key: clone(key)}}, "delete_node")
}

func (s *Store) BatchNodes(ctx context.Context, mutations []prolly.NodeMutation) error {
	providerMutations := make([]Mutation, len(mutations))
	for index, mutation := range mutations {
		if mutation.Value.Present {
			providerMutations[index] = Mutation{Kind: MutationUpsertNode, Key: clone(mutation.Key), Value: clone(mutation.Value.Value)}
		} else {
			providerMutations[index] = Mutation{Kind: MutationDeleteNode, Key: clone(mutation.Key)}
		}
	}
	return s.apply(ctx, providerMutations, "batch_nodes")
}

func (s *Store) PublishNodes(ctx context.Context, publication prolly.NodePublication) error {
	return prolly.PublishNodesWithGeneralPath(ctx, s, publication)
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
	if err := s.ready(ctx); err != nil {
		return nil, err
	}
	keys, err := s.client.ListNodeCIDs(ctx)
	if err != nil {
		return nil, spannerError("list_nodes", err)
	}
	result := make([][]byte, 0, len(keys))
	for _, key := range keys {
		if len(key) == 32 {
			result = append(result, clone(key))
		}
	}
	sort.Slice(result, func(i, j int) bool { return bytes.Compare(result[i], result[j]) < 0 })
	return result, nil
}

func (s *Store) GetHint(ctx context.Context, namespace, key []byte) (prolly.OptionalBytes, error) {
	if err := s.ready(ctx); err != nil {
		return prolly.OptionalBytes{}, err
	}
	value, err := s.client.GetHint(ctx, namespace, key)
	return value, spannerError("get_hint", err)
}

func (s *Store) PutHint(ctx context.Context, namespace, key, value []byte) error {
	return s.apply(ctx, []Mutation{{Kind: MutationUpsertHint, Namespace: clone(namespace), Key: clone(key), Value: clone(value)}}, "put_hint")
}

func (s *Store) BatchPutNodesWithHint(ctx context.Context, nodes []prolly.NodeEntry, namespace, key, value []byte) error {
	mutations := make([]Mutation, 0, len(nodes)+1)
	for _, node := range nodes {
		mutations = append(mutations, Mutation{Kind: MutationUpsertNode, Key: clone(node.Key), Value: clone(node.Value)})
	}
	mutations = append(mutations, Mutation{Kind: MutationUpsertHint, Namespace: clone(namespace), Key: clone(key), Value: clone(value)})
	return s.apply(ctx, mutations, "batch_nodes_hint")
}

func (s *Store) GetRootManifest(ctx context.Context, name []byte) (prolly.OptionalBytes, error) {
	if err := s.ready(ctx); err != nil {
		return prolly.OptionalBytes{}, err
	}
	value, err := s.client.GetRoot(ctx, name)
	return value, spannerError("get_root", err)
}

func (s *Store) PutRootManifest(ctx context.Context, name, manifest []byte) error {
	return s.apply(ctx, []Mutation{{Kind: MutationUpsertRoot, Key: clone(name), Value: clone(manifest)}}, "put_root")
}

func (s *Store) DeleteRootManifest(ctx context.Context, name []byte) error {
	return s.apply(ctx, []Mutation{{Kind: MutationDeleteRoot, Key: clone(name)}}, "delete_root")
}

func (s *Store) CompareAndSwapRootManifest(ctx context.Context, name []byte, expected, replacement prolly.OptionalBytes) (result prolly.RootCASResult, err error) {
	if err := s.ready(ctx); err != nil {
		return result, err
	}
	err = s.client.ReadWrite(ctx, func(txCtx context.Context, tx Transaction) error {
		current, readErr := tx.GetRoot(txCtx, name)
		if readErr != nil {
			return readErr
		}
		if !optionalEqual(current, expected) {
			result = prolly.RootCASResult{Current: current}
			return nil
		}
		mutation := Mutation{Kind: MutationDeleteRoot, Key: clone(name)}
		if replacement.Present {
			mutation.Kind, mutation.Value = MutationUpsertRoot, clone(replacement.Value)
		}
		if bufferErr := tx.Buffer([]Mutation{mutation}); bufferErr != nil {
			return bufferErr
		}
		result = prolly.RootCASResult{Applied: true, Current: replacement.Clone()}
		return nil
	})
	return result, spannerError("root_cas", err)
}

func (s *Store) ListRootManifests(ctx context.Context) ([]prolly.NamedStoreRoot, error) {
	if err := s.ready(ctx); err != nil {
		return nil, err
	}
	records, err := s.client.ListRoots(ctx)
	if err != nil {
		return nil, spannerError("list_roots", err)
	}
	result := make([]prolly.NamedStoreRoot, len(records))
	for index, record := range records {
		result[index] = prolly.NamedStoreRoot{Name: clone(record.Name), Manifest: clone(record.Manifest)}
	}
	sort.Slice(result, func(i, j int) bool { return bytes.Compare(result[i].Name, result[j].Name) < 0 })
	return result, nil
}

func (s *Store) CommitTransaction(ctx context.Context, nodes []prolly.NodeMutation, conditions []prolly.RootCondition, roots []prolly.RootWrite) (result prolly.StoreTransactionResult, err error) {
	if err := s.ready(ctx); err != nil {
		return result, err
	}
	err = s.client.ReadWrite(ctx, func(txCtx context.Context, tx Transaction) error {
		for _, condition := range conditions {
			current, readErr := tx.GetRoot(txCtx, condition.Name)
			if readErr != nil {
				return readErr
			}
			if !optionalEqual(current, condition.Expected) {
				result = prolly.StoreTransactionResult{Conflict: &prolly.StoreTransactionConflict{Name: clone(condition.Name), Expected: condition.Expected.Clone(), Current: current}}
				return nil
			}
		}
		mutations := make([]Mutation, 0, len(nodes)+len(roots))
		for _, node := range nodes {
			mutation := Mutation{Kind: MutationDeleteNode, Key: clone(node.Key)}
			if node.Value.Present {
				mutation.Kind, mutation.Value = MutationUpsertNode, clone(node.Value.Value)
			}
			mutations = append(mutations, mutation)
		}
		for _, root := range roots {
			mutation := Mutation{Kind: MutationDeleteRoot, Key: clone(root.Name)}
			if root.Replacement.Present {
				mutation.Kind, mutation.Value = MutationUpsertRoot, clone(root.Replacement.Value)
			}
			mutations = append(mutations, mutation)
		}
		if len(mutations) != 0 {
			if err := tx.Buffer(mutations); err != nil {
				return err
			}
		}
		result = prolly.StoreTransactionResult{Applied: true}
		return nil
	})
	return result, spannerError("transaction", err)
}

func (s *Store) apply(ctx context.Context, mutations []Mutation, operation string) error {
	if err := s.ready(ctx); err != nil {
		return err
	}
	if len(mutations) == 0 {
		return nil
	}
	return spannerError(operation, s.client.Apply(ctx, mutations))
}

func (s *Store) ready(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if s == nil || s.client == nil {
		return &prolly.StoreError{Code: "invalid_configuration", Message: "Spanner client is nil"}
	}
	return nil
}

type sdkClient struct{ client *gspanner.Client }

func (c *sdkClient) GetNode(ctx context.Context, key []byte) (prolly.OptionalBytes, error) {
	return readSDKValue(ctx, c.client.Single(), nodesTable, gspanner.Key{clone(key)}, "Node")
}
func (c *sdkClient) GetHint(ctx context.Context, namespace, key []byte) (prolly.OptionalBytes, error) {
	return readSDKValue(ctx, c.client.Single(), hintsTable, gspanner.Key{clone(namespace), clone(key)}, "Value")
}
func (c *sdkClient) GetRoot(ctx context.Context, name []byte) (prolly.OptionalBytes, error) {
	return readSDKValue(ctx, c.client.Single(), rootsTable, gspanner.Key{clone(name)}, "Manifest")
}
func (c *sdkClient) ListNodeCIDs(ctx context.Context) ([][]byte, error) {
	rows := c.client.Single().Query(ctx, gspanner.Statement{SQL: "SELECT Cid FROM ProllyNodes ORDER BY Cid"})
	defer rows.Stop()
	var result [][]byte
	for {
		row, err := rows.Next()
		if err == iterator.Done {
			return result, nil
		}
		if err != nil {
			return nil, err
		}
		var key []byte
		if err := row.ColumnByName("Cid", &key); err != nil {
			return nil, err
		}
		result = append(result, clone(key))
	}
}
func (c *sdkClient) ListRoots(ctx context.Context) ([]RootRecord, error) {
	rows := c.client.Single().Query(ctx, gspanner.Statement{SQL: "SELECT Name, Manifest FROM ProllyRoots ORDER BY Name"})
	defer rows.Stop()
	var result []RootRecord
	for {
		row, err := rows.Next()
		if err == iterator.Done {
			return result, nil
		}
		if err != nil {
			return nil, err
		}
		var name, manifest []byte
		if err := row.Columns(&name, &manifest); err != nil {
			return nil, err
		}
		result = append(result, RootRecord{Name: clone(name), Manifest: clone(manifest)})
	}
}
func (c *sdkClient) Apply(ctx context.Context, mutations []Mutation) error {
	_, err := c.client.Apply(ctx, sdkMutations(mutations))
	return err
}
func (c *sdkClient) ReadWrite(ctx context.Context, function func(context.Context, Transaction) error) error {
	_, err := c.client.ReadWriteTransaction(ctx, func(txCtx context.Context, tx *gspanner.ReadWriteTransaction) error {
		return function(txCtx, &sdkTransaction{tx: tx})
	})
	return err
}

type sdkTransaction struct {
	tx *gspanner.ReadWriteTransaction
}

func (t *sdkTransaction) GetRoot(ctx context.Context, name []byte) (prolly.OptionalBytes, error) {
	return readSDKValue(ctx, t.tx, rootsTable, gspanner.Key{clone(name)}, "Manifest")
}
func (t *sdkTransaction) Buffer(mutations []Mutation) error {
	return t.tx.BufferWrite(sdkMutations(mutations))
}

type rowReader interface {
	ReadRow(context.Context, string, gspanner.Key, []string) (*gspanner.Row, error)
}

func readSDKValue(ctx context.Context, reader rowReader, table string, key gspanner.Key, column string) (prolly.OptionalBytes, error) {
	row, err := reader.ReadRow(ctx, table, key, []string{column})
	if errors.Is(err, gspanner.ErrRowNotFound) {
		return prolly.MissingBytes(), nil
	}
	if err != nil {
		return prolly.OptionalBytes{}, err
	}
	var value []byte
	if err := row.Column(0, &value); err != nil {
		return prolly.OptionalBytes{}, err
	}
	return prolly.PresentBytes(value), nil
}

func sdkMutations(mutations []Mutation) []*gspanner.Mutation {
	result := make([]*gspanner.Mutation, 0, len(mutations))
	for _, mutation := range mutations {
		switch mutation.Kind {
		case MutationUpsertNode:
			result = append(result, gspanner.InsertOrUpdate(nodesTable, []string{"Cid", "Node"}, []any{clone(mutation.Key), clone(mutation.Value)}))
		case MutationDeleteNode:
			result = append(result, gspanner.Delete(nodesTable, gspanner.Key{clone(mutation.Key)}))
		case MutationUpsertHint:
			result = append(result, gspanner.InsertOrUpdate(hintsTable, []string{"Namespace", "HintKey", "Value"}, []any{clone(mutation.Namespace), clone(mutation.Key), clone(mutation.Value)}))
		case MutationUpsertRoot:
			result = append(result, gspanner.InsertOrUpdate(rootsTable, []string{"Name", "Manifest"}, []any{clone(mutation.Key), clone(mutation.Value)}))
		case MutationDeleteRoot:
			result = append(result, gspanner.Delete(rootsTable, gspanner.Key{clone(mutation.Key)}))
		}
	}
	return result
}

func spannerError(operation string, err error) error {
	if err == nil {
		return nil
	}
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return err
	}
	code := status.Code(err)
	retryable := code == codes.Aborted || code == codes.Unavailable || code == codes.ResourceExhausted || code == codes.DeadlineExceeded
	return &prolly.StoreError{Code: "provider_error", Message: operation + ": " + err.Error(), Retryable: retryable, ProviderCode: code.String(), Cause: err}
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
