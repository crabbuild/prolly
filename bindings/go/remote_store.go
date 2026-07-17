package prolly

import (
	"context"
	"errors"
	"fmt"
	"strings"
)

const StoreProtocolMajor uint32 = 1

var ErrInvalidStoreDescriptor = errors.New("invalid remote store descriptor")

type StoreCapabilities struct {
	NativeBatchReads   bool
	AtomicBatchWrites  bool
	NodeScan           bool
	Hints              bool
	AtomicNodesAndHint bool
	RootScan           bool
	RootCompareAndSwap bool
	Transactions       bool
	ReadParallelism    uint32
}

type StoreLimits struct {
	MaxBatchReadItems        *uint32
	MaxBatchWriteItems       *uint32
	MaxTransactionOperations *uint32
	MaxNodeBytes             *uint64
}

type StoreDescriptor struct {
	ProtocolMajor uint32
	AdapterName   string
	Provider      string
	SchemaVersion uint32
	Capabilities  StoreCapabilities
	Limits        StoreLimits
}

func (d StoreDescriptor) Validate() error {
	invalid := func(format string, args ...any) error {
		return fmt.Errorf("%w: %s", ErrInvalidStoreDescriptor, fmt.Sprintf(format, args...))
	}
	if d.ProtocolMajor != StoreProtocolMajor {
		return invalid("protocol major must be %d, got %d", StoreProtocolMajor, d.ProtocolMajor)
	}
	if strings.TrimSpace(d.AdapterName) == "" {
		return invalid("adapter name must not be empty")
	}
	if strings.TrimSpace(d.Provider) == "" {
		return invalid("provider must not be empty")
	}
	if d.SchemaVersion == 0 {
		return invalid("schema version must be at least 1")
	}
	if d.Capabilities.ReadParallelism == 0 {
		return invalid("read parallelism must be at least 1")
	}
	if d.Capabilities.AtomicNodesAndHint && !d.Capabilities.Hints {
		return invalid("atomic nodes and hint requires hints support")
	}
	if d.Limits.MaxBatchReadItems != nil && *d.Limits.MaxBatchReadItems == 0 {
		return invalid("max batch read items must be at least 1 when present")
	}
	if d.Limits.MaxBatchWriteItems != nil && *d.Limits.MaxBatchWriteItems == 0 {
		return invalid("max batch write items must be at least 1 when present")
	}
	if d.Limits.MaxTransactionOperations != nil && *d.Limits.MaxTransactionOperations == 0 {
		return invalid("max transaction operations must be at least 1 when present")
	}
	if d.Limits.MaxNodeBytes != nil && *d.Limits.MaxNodeBytes == 0 {
		return invalid("max node bytes must be at least 1 when present")
	}
	return nil
}

type OptionalBytes struct {
	Value   []byte
	Present bool
}

func MissingBytes() OptionalBytes {
	return OptionalBytes{}
}

func PresentBytes(value []byte) OptionalBytes {
	return OptionalBytes{Value: cloneRemoteBytes(value), Present: true}
}

func (v OptionalBytes) Clone() OptionalBytes {
	return OptionalBytes{Value: cloneRemoteBytes(v.Value), Present: v.Present}
}

type NodeMutation struct {
	Key   []byte
	Value OptionalBytes
}

func UpsertNode(key, value []byte) NodeMutation {
	return NodeMutation{Key: cloneRemoteBytes(key), Value: PresentBytes(value)}
}

func DeleteNode(key []byte) NodeMutation {
	return NodeMutation{Key: cloneRemoteBytes(key), Value: MissingBytes()}
}

type NodeEntry struct {
	Key   []byte
	Value []byte
}

type NamedStoreRoot struct {
	Name     []byte
	Manifest []byte
}

type RootCASResult struct {
	Applied bool
	Current OptionalBytes
}

type RootCondition struct {
	Name     []byte
	Expected OptionalBytes
}

type RootWrite struct {
	Name        []byte
	Replacement OptionalBytes
}

type StoreTransactionConflict struct {
	Name     []byte
	Expected OptionalBytes
	Current  OptionalBytes
}

type StoreTransactionResult struct {
	Applied  bool
	Conflict *StoreTransactionConflict
}

type StoreError struct {
	Code         string
	Message      string
	Retryable    bool
	ProviderCode string
	Cause        error
}

func (e *StoreError) Error() string {
	if e == nil {
		return "<nil>"
	}
	message := e.Code + ": " + e.Message
	if e.ProviderCode != "" {
		message += " (provider code " + e.ProviderCode + ")"
	}
	return message
}

func (e *StoreError) Unwrap() error {
	if e == nil {
		return nil
	}
	return e.Cause
}

type RemoteStore interface {
	Descriptor(context.Context) (StoreDescriptor, error)
	GetNode(context.Context, []byte) (OptionalBytes, error)
	PutNode(context.Context, []byte, []byte) error
	DeleteNode(context.Context, []byte) error
	BatchNodes(context.Context, []NodeMutation) error
	BatchGetNodesOrdered(context.Context, [][]byte) ([]OptionalBytes, error)
	ListNodeCIDs(context.Context) ([][]byte, error)
	GetHint(context.Context, []byte, []byte) (OptionalBytes, error)
	PutHint(context.Context, []byte, []byte, []byte) error
	BatchPutNodesWithHint(context.Context, []NodeEntry, []byte, []byte, []byte) error
	GetRootManifest(context.Context, []byte) (OptionalBytes, error)
	PutRootManifest(context.Context, []byte, []byte) error
	DeleteRootManifest(context.Context, []byte) error
	CompareAndSwapRootManifest(context.Context, []byte, OptionalBytes, OptionalBytes) (RootCASResult, error)
	ListRootManifests(context.Context) ([]NamedStoreRoot, error)
	CommitTransaction(context.Context, []NodeMutation, []RootCondition, []RootWrite) (StoreTransactionResult, error)
}

func cloneRemoteBytes(value []byte) []byte {
	if value == nil {
		return []byte{}
	}
	return append([]byte(nil), value...)
}
