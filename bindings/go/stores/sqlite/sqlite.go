package sqlite

import (
	"bytes"
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"sync/atomic"

	prolly "build.crab/prolly-go"
	_ "modernc.org/sqlite"
)

type Options struct {
	AdapterName     string
	ReadParallelism uint32
}

type Store struct {
	db      *sql.DB
	options Options
	owned   bool
	closed  atomic.Bool
}

func New(db *sql.DB, options Options) *Store {
	return &Store{db: db, options: normalizeOptions(options)}
}

func Open(dataSourceName string, options Options) (*Store, error) {
	db, err := sql.Open("sqlite", dataSourceName)
	if err != nil {
		return nil, storeError("open", err)
	}
	store := New(db, options)
	store.owned = true
	return store, nil
}

func normalizeOptions(options Options) Options {
	if strings.TrimSpace(options.AdapterName) == "" {
		options.AdapterName = "sqlite-v1"
	}
	if options.ReadParallelism == 0 {
		options.ReadParallelism = 4
	}
	return options
}

func (s *Store) InitializeSchema(ctx context.Context) error {
	if err := s.ready(ctx); err != nil {
		return err
	}
	if _, err := s.db.ExecContext(ctx, createSchemaSQL); err != nil {
		return storeError("initialize_schema", err)
	}
	return nil
}

func (s *Store) Close() error {
	if s == nil || s.closed.Swap(true) || !s.owned || s.db == nil {
		return nil
	}
	return s.db.Close()
}

func (s *Store) Descriptor(ctx context.Context) (prolly.StoreDescriptor, error) {
	if err := s.ready(ctx); err != nil {
		return prolly.StoreDescriptor{}, err
	}
	return prolly.StoreDescriptor{
		ProtocolMajor: prolly.StoreProtocolMajor,
		AdapterName:   s.options.AdapterName,
		Provider:      "sqlite",
		SchemaVersion: 1,
		Capabilities: prolly.StoreCapabilities{
			NativeBatchReads:   true,
			AtomicBatchWrites:  true,
			NodeScan:           true,
			Hints:              true,
			AtomicNodesAndHint: true,
			RootScan:           true,
			RootCompareAndSwap: true,
			Transactions:       true,
			ReadParallelism:    s.options.ReadParallelism,
		},
	}, nil
}

func (s *Store) GetNode(ctx context.Context, key []byte) (prolly.OptionalBytes, error) {
	if err := s.ready(ctx); err != nil {
		return prolly.OptionalBytes{}, err
	}
	return queryOptional(ctx, s.db, selectNodeSQL, key)
}

func (s *Store) PutNode(ctx context.Context, key, value []byte) error {
	if err := s.ready(ctx); err != nil {
		return err
	}
	_, err := s.db.ExecContext(ctx, upsertNodeSQL, key, value)
	return storeError("put_node", err)
}

func (s *Store) DeleteNode(ctx context.Context, key []byte) error {
	if err := s.ready(ctx); err != nil {
		return err
	}
	_, err := s.db.ExecContext(ctx, deleteNodeSQL, key)
	return storeError("delete_node", err)
}

func (s *Store) BatchNodes(ctx context.Context, mutations []prolly.NodeMutation) error {
	if err := s.ready(ctx); err != nil {
		return err
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return storeError("batch_nodes_begin", err)
	}
	defer tx.Rollback()
	if err := applyNodeMutations(ctx, tx, mutations); err != nil {
		return err
	}
	return storeError("batch_nodes_commit", tx.Commit())
}

func (s *Store) PublishNodes(ctx context.Context, publication prolly.NodePublication) error {
	return prolly.PublishNodesWithGeneralPath(ctx, s, publication)
}

func (s *Store) BatchGetNodesOrdered(ctx context.Context, keys [][]byte) ([]prolly.OptionalBytes, error) {
	if err := s.ready(ctx); err != nil {
		return nil, err
	}
	result := make([]prolly.OptionalBytes, len(keys))
	var err error
	for index, key := range keys {
		result[index], err = queryOptional(ctx, s.db, selectNodeSQL, key)
		if err != nil {
			return nil, err
		}
	}
	return result, nil
}

func (s *Store) ListNodeCIDs(ctx context.Context) ([][]byte, error) {
	if err := s.ready(ctx); err != nil {
		return nil, err
	}
	rows, err := s.db.QueryContext(ctx, `SELECT cid FROM prolly_nodes ORDER BY cid`)
	if err != nil {
		return nil, storeError("list_node_cids", err)
	}
	defer rows.Close()
	var result [][]byte
	for rows.Next() {
		var key []byte
		if err := rows.Scan(&key); err != nil {
			return nil, storeError("list_node_cids_scan", err)
		}
		result = append(result, clone(key))
	}
	return result, storeError("list_node_cids_rows", rows.Err())
}

func (s *Store) GetHint(ctx context.Context, namespace, key []byte) (prolly.OptionalBytes, error) {
	if err := s.ready(ctx); err != nil {
		return prolly.OptionalBytes{}, err
	}
	return queryOptional(ctx, s.db, selectHintSQL, namespace, key)
}

func (s *Store) PutHint(ctx context.Context, namespace, key, value []byte) error {
	if err := s.ready(ctx); err != nil {
		return err
	}
	_, err := s.db.ExecContext(ctx, upsertHintSQL, namespace, key, value)
	return storeError("put_hint", err)
}

func (s *Store) BatchPutNodesWithHint(ctx context.Context, nodes []prolly.NodeEntry, namespace, key, value []byte) error {
	if err := s.ready(ctx); err != nil {
		return err
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return storeError("batch_nodes_hint_begin", err)
	}
	defer tx.Rollback()
	for _, node := range nodes {
		if _, err := tx.ExecContext(ctx, upsertNodeSQL, node.Key, node.Value); err != nil {
			return storeError("batch_nodes_hint_node", err)
		}
	}
	if _, err := tx.ExecContext(ctx, upsertHintSQL, namespace, key, value); err != nil {
		return storeError("batch_nodes_hint_hint", err)
	}
	return storeError("batch_nodes_hint_commit", tx.Commit())
}

func (s *Store) GetRootManifest(ctx context.Context, name []byte) (prolly.OptionalBytes, error) {
	if err := s.ready(ctx); err != nil {
		return prolly.OptionalBytes{}, err
	}
	return queryOptional(ctx, s.db, selectRootSQL, name)
}

func (s *Store) PutRootManifest(ctx context.Context, name, manifest []byte) error {
	if err := s.ready(ctx); err != nil {
		return err
	}
	_, err := s.db.ExecContext(ctx, upsertRootSQL, name, manifest)
	return storeError("put_root", err)
}

func (s *Store) DeleteRootManifest(ctx context.Context, name []byte) error {
	if err := s.ready(ctx); err != nil {
		return err
	}
	_, err := s.db.ExecContext(ctx, deleteRootSQL, name)
	return storeError("delete_root", err)
}

func (s *Store) CompareAndSwapRootManifest(ctx context.Context, name []byte, expected, replacement prolly.OptionalBytes) (prolly.RootCASResult, error) {
	if err := s.ready(ctx); err != nil {
		return prolly.RootCASResult{}, err
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return prolly.RootCASResult{}, storeError("cas_begin", err)
	}
	defer tx.Rollback()
	current, err := queryOptional(ctx, tx, selectRootSQL, name)
	if err != nil {
		return prolly.RootCASResult{}, err
	}
	if !optionalEqual(current, expected) {
		return prolly.RootCASResult{Current: current}, nil
	}
	if err := writeOptionalRoot(ctx, tx, name, replacement); err != nil {
		return prolly.RootCASResult{}, err
	}
	if err := tx.Commit(); err != nil {
		return prolly.RootCASResult{}, storeError("cas_commit", err)
	}
	return prolly.RootCASResult{Applied: true, Current: replacement.Clone()}, nil
}

func (s *Store) ListRootManifests(ctx context.Context) ([]prolly.NamedStoreRoot, error) {
	if err := s.ready(ctx); err != nil {
		return nil, err
	}
	rows, err := s.db.QueryContext(ctx, `SELECT name, manifest FROM prolly_roots ORDER BY name`)
	if err != nil {
		return nil, storeError("list_roots", err)
	}
	defer rows.Close()
	var result []prolly.NamedStoreRoot
	for rows.Next() {
		var name, manifest []byte
		if err := rows.Scan(&name, &manifest); err != nil {
			return nil, storeError("list_roots_scan", err)
		}
		result = append(result, prolly.NamedStoreRoot{Name: clone(name), Manifest: clone(manifest)})
	}
	return result, storeError("list_roots_rows", rows.Err())
}

func (s *Store) CommitTransaction(ctx context.Context, nodes []prolly.NodeMutation, conditions []prolly.RootCondition, roots []prolly.RootWrite) (prolly.StoreTransactionResult, error) {
	if err := s.ready(ctx); err != nil {
		return prolly.StoreTransactionResult{}, err
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return prolly.StoreTransactionResult{}, storeError("transaction_begin", err)
	}
	defer tx.Rollback()
	for _, condition := range conditions {
		current, err := queryOptional(ctx, tx, selectRootSQL, condition.Name)
		if err != nil {
			return prolly.StoreTransactionResult{}, err
		}
		if !optionalEqual(current, condition.Expected) {
			return prolly.StoreTransactionResult{Conflict: &prolly.StoreTransactionConflict{
				Name: clone(condition.Name), Expected: condition.Expected.Clone(), Current: current,
			}}, nil
		}
	}
	if err := applyNodeMutations(ctx, tx, nodes); err != nil {
		return prolly.StoreTransactionResult{}, err
	}
	for _, root := range roots {
		if err := writeOptionalRoot(ctx, tx, root.Name, root.Replacement); err != nil {
			return prolly.StoreTransactionResult{}, err
		}
	}
	if err := tx.Commit(); err != nil {
		return prolly.StoreTransactionResult{}, storeError("transaction_commit", err)
	}
	return prolly.StoreTransactionResult{Applied: true}, nil
}

type queryer interface {
	QueryRowContext(context.Context, string, ...any) *sql.Row
}

type execer interface {
	ExecContext(context.Context, string, ...any) (sql.Result, error)
}

func queryOptional(ctx context.Context, queryer queryer, query string, args ...any) (prolly.OptionalBytes, error) {
	var value []byte
	err := queryer.QueryRowContext(ctx, query, args...).Scan(&value)
	if errors.Is(err, sql.ErrNoRows) {
		return prolly.MissingBytes(), nil
	}
	if err != nil {
		return prolly.OptionalBytes{}, storeError("query", err)
	}
	return prolly.PresentBytes(value), nil
}

func applyNodeMutations(ctx context.Context, tx *sql.Tx, mutations []prolly.NodeMutation) error {
	for _, mutation := range mutations {
		var err error
		if mutation.Value.Present {
			_, err = tx.ExecContext(ctx, upsertNodeSQL, mutation.Key, mutation.Value.Value)
		} else {
			_, err = tx.ExecContext(ctx, deleteNodeSQL, mutation.Key)
		}
		if err != nil {
			return storeError("apply_node_mutation", err)
		}
	}
	return nil
}

func writeOptionalRoot(ctx context.Context, target execer, name []byte, replacement prolly.OptionalBytes) error {
	var err error
	if replacement.Present {
		_, err = target.ExecContext(ctx, upsertRootSQL, name, replacement.Value)
	} else {
		_, err = target.ExecContext(ctx, deleteRootSQL, name)
	}
	return storeError("write_root", err)
}

func optionalEqual(left, right prolly.OptionalBytes) bool {
	return left.Present == right.Present && (!left.Present || bytes.Equal(left.Value, right.Value))
}

func clone(value []byte) []byte { return append([]byte(nil), value...) }

func (s *Store) ready(ctx context.Context) error {
	if ctx != nil {
		if err := ctx.Err(); err != nil {
			return err
		}
	}
	if s == nil || s.db == nil {
		return &prolly.StoreError{Code: "invalid_store", Message: "SQLite store has no database"}
	}
	if s.closed.Load() {
		return &prolly.StoreError{Code: "closed", Message: "SQLite store is closed"}
	}
	return nil
}

func storeError(operation string, err error) error {
	if err == nil {
		return nil
	}
	retryable := false
	lower := strings.ToLower(err.Error())
	if strings.Contains(lower, "busy") || strings.Contains(lower, "locked") {
		retryable = true
	}
	return &prolly.StoreError{
		Code:      "sqlite",
		Message:   fmt.Sprintf("%s: %v", operation, err),
		Retryable: retryable,
		Cause:     err,
	}
}

var _ prolly.RemoteStore = (*Store)(nil)
