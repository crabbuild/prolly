package mysql

import (
	"bytes"
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"sync/atomic"

	prolly "build.crab/prolly-go"
	_ "github.com/go-sql-driver/mysql"
	mysqldriver "github.com/go-sql-driver/mysql"
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
	if strings.TrimSpace(options.AdapterName) == "" {
		options.AdapterName = "mysql-v1"
	}
	if options.ReadParallelism == 0 {
		options.ReadParallelism = 16
	}
	return &Store{db: db, options: options}
}

func Open(dataSourceName string, options Options) (*Store, error) {
	db, err := sql.Open("mysql", dataSourceName)
	if err != nil {
		return nil, mysqlError("open", err)
	}
	store := New(db, options)
	store.owned = true
	return store, nil
}

func (s *Store) InitializeSchema(ctx context.Context) error {
	if err := s.ready(ctx); err != nil {
		return err
	}
	for _, statement := range strings.Split(Schema, ";") {
		if strings.TrimSpace(statement) == "" {
			continue
		}
		if _, err := s.db.ExecContext(ctx, statement); err != nil {
			return mysqlError("initialize_schema", err)
		}
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
		ProtocolMajor: prolly.StoreProtocolMajor, AdapterName: s.options.AdapterName, Provider: "mysql", SchemaVersion: 1,
		Capabilities: prolly.StoreCapabilities{
			NativeBatchReads: true, AtomicBatchWrites: true, NodeScan: true, Hints: true,
			AtomicNodesAndHint: true, RootScan: true, RootCompareAndSwap: true,
			Transactions: true, ReadParallelism: s.options.ReadParallelism,
		},
	}, nil
}

func (s *Store) GetNode(ctx context.Context, key []byte) (prolly.OptionalBytes, error) {
	if err := s.ready(ctx); err != nil {
		return prolly.OptionalBytes{}, err
	}
	return queryOptional(ctx, s.db, selectNode, key)
}
func (s *Store) PutNode(ctx context.Context, key, value []byte) error {
	if err := s.ready(ctx); err != nil {
		return err
	}
	_, err := s.db.ExecContext(ctx, upsertNode, key, value)
	return mysqlError("put_node", err)
}
func (s *Store) DeleteNode(ctx context.Context, key []byte) error {
	if err := s.ready(ctx); err != nil {
		return err
	}
	_, err := s.db.ExecContext(ctx, deleteNode, key)
	return mysqlError("delete_node", err)
}
func (s *Store) BatchNodes(ctx context.Context, mutations []prolly.NodeMutation) error {
	if err := s.ready(ctx); err != nil {
		return err
	}
	return s.withTx(ctx, "batch_nodes", func(tx *sql.Tx) error { return applyNodes(ctx, tx, mutations) })
}
func (s *Store) PublishNodes(ctx context.Context, publication prolly.NodePublication) error {
	return prolly.PublishNodesWithGeneralPath(ctx, s, publication)
}

func (s *Store) BatchGetNodesOrdered(ctx context.Context, keys [][]byte) ([]prolly.OptionalBytes, error) {
	if err := s.ready(ctx); err != nil {
		return nil, err
	}
	result := make([]prolly.OptionalBytes, len(keys))
	for index, key := range keys {
		value, err := queryOptional(ctx, s.db, selectNode, key)
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
	rows, err := s.db.QueryContext(ctx, `SELECT cid FROM prolly_nodes ORDER BY cid`)
	if err != nil {
		return nil, mysqlError("list_nodes", err)
	}
	defer rows.Close()
	var result [][]byte
	for rows.Next() {
		var value []byte
		if err := rows.Scan(&value); err != nil {
			return nil, mysqlError("list_nodes_scan", err)
		}
		result = append(result, clone(value))
	}
	return result, mysqlError("list_nodes_rows", rows.Err())
}

func (s *Store) GetHint(ctx context.Context, namespace, key []byte) (prolly.OptionalBytes, error) {
	if err := s.ready(ctx); err != nil {
		return prolly.OptionalBytes{}, err
	}
	return queryOptional(ctx, s.db, selectHint, namespace, key)
}
func (s *Store) PutHint(ctx context.Context, namespace, key, value []byte) error {
	if err := s.ready(ctx); err != nil {
		return err
	}
	_, err := s.db.ExecContext(ctx, upsertHint, namespace, key, value)
	return mysqlError("put_hint", err)
}
func (s *Store) BatchPutNodesWithHint(ctx context.Context, nodes []prolly.NodeEntry, namespace, key, value []byte) error {
	if err := s.ready(ctx); err != nil {
		return err
	}
	return s.withTx(ctx, "batch_nodes_hint", func(tx *sql.Tx) error {
		for _, node := range nodes {
			if _, err := tx.ExecContext(ctx, upsertNode, node.Key, node.Value); err != nil {
				return mysqlError("batch_node", err)
			}
		}
		_, err := tx.ExecContext(ctx, upsertHint, namespace, key, value)
		return mysqlError("batch_hint", err)
	})
}

func (s *Store) GetRootManifest(ctx context.Context, name []byte) (prolly.OptionalBytes, error) {
	if err := s.ready(ctx); err != nil {
		return prolly.OptionalBytes{}, err
	}
	return queryOptional(ctx, s.db, selectRoot, name)
}
func (s *Store) PutRootManifest(ctx context.Context, name, manifest []byte) error {
	if err := s.ready(ctx); err != nil {
		return err
	}
	_, err := s.db.ExecContext(ctx, upsertRoot, name, manifest)
	return mysqlError("put_root", err)
}
func (s *Store) DeleteRootManifest(ctx context.Context, name []byte) error {
	if err := s.ready(ctx); err != nil {
		return err
	}
	_, err := s.db.ExecContext(ctx, deleteRoot, name)
	return mysqlError("delete_root", err)
}
func (s *Store) CompareAndSwapRootManifest(ctx context.Context, name []byte, expected, replacement prolly.OptionalBytes) (prolly.RootCASResult, error) {
	if err := s.ready(ctx); err != nil {
		return prolly.RootCASResult{}, err
	}
	var result prolly.RootCASResult
	err := s.withTx(ctx, "root_cas", func(tx *sql.Tx) error {
		current, err := queryOptional(ctx, tx, selectRootForUpdate, name)
		if err != nil {
			return err
		}
		if !optionalEqual(current, expected) {
			result.Current = current
			return nil
		}
		if err := writeRoot(ctx, tx, name, replacement); err != nil {
			return err
		}
		result = prolly.RootCASResult{Applied: true, Current: replacement.Clone()}
		return nil
	})
	return result, err
}
func (s *Store) ListRootManifests(ctx context.Context) ([]prolly.NamedStoreRoot, error) {
	if err := s.ready(ctx); err != nil {
		return nil, err
	}
	rows, err := s.db.QueryContext(ctx, `SELECT name, manifest FROM prolly_roots ORDER BY name`)
	if err != nil {
		return nil, mysqlError("list_roots", err)
	}
	defer rows.Close()
	var result []prolly.NamedStoreRoot
	for rows.Next() {
		var name, manifest []byte
		if err := rows.Scan(&name, &manifest); err != nil {
			return nil, mysqlError("list_roots_scan", err)
		}
		result = append(result, prolly.NamedStoreRoot{Name: clone(name), Manifest: clone(manifest)})
	}
	return result, mysqlError("list_roots_rows", rows.Err())
}

func (s *Store) CommitTransaction(ctx context.Context, nodes []prolly.NodeMutation, conditions []prolly.RootCondition, roots []prolly.RootWrite) (prolly.StoreTransactionResult, error) {
	if err := s.ready(ctx); err != nil {
		return prolly.StoreTransactionResult{}, err
	}
	var result prolly.StoreTransactionResult
	err := s.withTx(ctx, "transaction", func(tx *sql.Tx) error {
		for _, condition := range conditions {
			current, err := queryOptional(ctx, tx, selectRootForUpdate, condition.Name)
			if err != nil {
				return err
			}
			if !optionalEqual(current, condition.Expected) {
				result.Conflict = &prolly.StoreTransactionConflict{Name: clone(condition.Name), Expected: condition.Expected.Clone(), Current: current}
				return nil
			}
		}
		if err := applyNodes(ctx, tx, nodes); err != nil {
			return err
		}
		for _, root := range roots {
			if err := writeRoot(ctx, tx, root.Name, root.Replacement); err != nil {
				return err
			}
		}
		result.Applied = true
		return nil
	})
	return result, err
}

type queryer interface {
	QueryRowContext(context.Context, string, ...any) *sql.Row
}
type execer interface {
	ExecContext(context.Context, string, ...any) (sql.Result, error)
}

func queryOptional(ctx context.Context, target queryer, query string, args ...any) (prolly.OptionalBytes, error) {
	var value []byte
	err := target.QueryRowContext(ctx, query, args...).Scan(&value)
	if errors.Is(err, sql.ErrNoRows) {
		return prolly.MissingBytes(), nil
	}
	if err != nil {
		return prolly.OptionalBytes{}, mysqlError("query", err)
	}
	return prolly.PresentBytes(value), nil
}
func applyNodes(ctx context.Context, tx *sql.Tx, mutations []prolly.NodeMutation) error {
	for _, mutation := range mutations {
		var err error
		if mutation.Value.Present {
			_, err = tx.ExecContext(ctx, upsertNode, mutation.Key, mutation.Value.Value)
		} else {
			_, err = tx.ExecContext(ctx, deleteNode, mutation.Key)
		}
		if err != nil {
			return mysqlError("node_mutation", err)
		}
	}
	return nil
}
func writeRoot(ctx context.Context, target execer, name []byte, value prolly.OptionalBytes) error {
	var err error
	if value.Present {
		_, err = target.ExecContext(ctx, upsertRoot, name, value.Value)
	} else {
		_, err = target.ExecContext(ctx, deleteRoot, name)
	}
	return mysqlError("write_root", err)
}
func (s *Store) withTx(ctx context.Context, operation string, call func(*sql.Tx) error) error {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return mysqlError(operation+"_begin", err)
	}
	defer tx.Rollback()
	if err := call(tx); err != nil {
		return err
	}
	return mysqlError(operation+"_commit", tx.Commit())
}
func optionalEqual(left, right prolly.OptionalBytes) bool {
	return left.Present == right.Present && (!left.Present || bytes.Equal(left.Value, right.Value))
}
func clone(value []byte) []byte { return append([]byte(nil), value...) }
func (s *Store) ready(ctx context.Context) error {
	if ctx != nil && ctx.Err() != nil {
		return ctx.Err()
	}
	if s == nil || s.db == nil {
		return &prolly.StoreError{Code: "invalid_store", Message: "MySQL database is nil"}
	}
	if s.closed.Load() {
		return &prolly.StoreError{Code: "closed", Message: "MySQL store is closed"}
	}
	return nil
}
func mysqlError(operation string, err error) error {
	if err == nil {
		return nil
	}
	retryable, providerCode := false, ""
	var driverErr *mysqldriver.MySQLError
	if errors.As(err, &driverErr) {
		providerCode = fmt.Sprint(driverErr.Number)
		switch driverErr.Number {
		case 1040, 1205, 1213, 2006, 2013:
			retryable = true
		}
	}
	return &prolly.StoreError{Code: "mysql", Message: fmt.Sprintf("%s: %v", operation, err), Retryable: retryable, ProviderCode: providerCode, Cause: err}
}

var _ prolly.RemoteStore = (*Store)(nil)
