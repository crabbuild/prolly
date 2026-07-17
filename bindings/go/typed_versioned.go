package prolly

import (
	"bytes"
	"encoding/json"
	"errors"
	"unicode/utf8"
)

// KeyCodec maps application keys to ordered map bytes.
type KeyCodec[K any] interface {
	EncodeKey(K) ([]byte, error)
	DecodeKey([]byte) (K, error)
}

// ValueCodec maps application values to stored bytes.
type ValueCodec[V any] interface {
	Encode(V) ([]byte, error)
	Decode([]byte) (V, error)
}

type StringKeyCodec struct{}

func (StringKeyCodec) EncodeKey(key string) ([]byte, error) { return []byte(key), nil }
func (StringKeyCodec) DecodeKey(value []byte) (string, error) {
	if !utf8.Valid(value) {
		return "", errors.New("typed map key is not valid UTF-8")
	}
	return string(value), nil
}

type BytesKeyCodec struct{}

func (BytesKeyCodec) EncodeKey(key []byte) ([]byte, error)   { return bytes.Clone(key), nil }
func (BytesKeyCodec) DecodeKey(value []byte) ([]byte, error) { return bytes.Clone(value), nil }

type BytesValueCodec struct{}

func (BytesValueCodec) Encode(value []byte) ([]byte, error) { return bytes.Clone(value), nil }
func (BytesValueCodec) Decode(value []byte) ([]byte, error) { return bytes.Clone(value), nil }

// JSONValueCodec uses compact JSON bytes, matching Rust's JsonCodec semantics.
type JSONValueCodec[V any] struct{}

func (JSONValueCodec[V]) Encode(value V) ([]byte, error) { return json.Marshal(value) }
func (JSONValueCodec[V]) Decode(value []byte) (V, error) {
	var decoded V
	err := json.Unmarshal(value, &decoded)
	return decoded, err
}

type TypedEntry[K, V any] struct {
	Key   K
	Value V
}

type TypedMigrationResult struct {
	Update          MapUpdate
	ScannedValues   int
	RewrittenValues int
}

// TypedVersionedMap is an idiomatic typed facade over VersionedMap.
type TypedVersionedMap[K, V any] struct {
	raw        *VersionedMap
	keyCodec   KeyCodec[K]
	valueCodec ValueCodec[V]
}

func NewTypedVersionedMap[K, V any](
	raw *VersionedMap,
	keyCodec KeyCodec[K],
	valueCodec ValueCodec[V],
) *TypedVersionedMap[K, V] {
	return &TypedVersionedMap[K, V]{raw: raw, keyCodec: keyCodec, valueCodec: valueCodec}
}

func (m *TypedVersionedMap[K, V]) Raw() *VersionedMap { return m.raw }

func (m *TypedVersionedMap[K, V]) Get(key K) (V, bool, error) {
	var zero V
	encoded, err := m.keyCodec.EncodeKey(key)
	if err != nil {
		return zero, false, err
	}
	value, found, err := m.raw.Get(encoded)
	if err != nil || !found {
		return zero, found, err
	}
	decoded, err := m.valueCodec.Decode(value)
	return decoded, err == nil, err
}

func (m *TypedVersionedMap[K, V]) GetAt(version []byte, key K) (V, bool, error) {
	var zero V
	encoded, err := m.keyCodec.EncodeKey(key)
	if err != nil {
		return zero, false, err
	}
	value, found, err := m.raw.GetAt(bytes.Clone(version), encoded)
	if err != nil || !found {
		return zero, found, err
	}
	decoded, err := m.valueCodec.Decode(value)
	return decoded, err == nil, err
}

func (m *TypedVersionedMap[K, V]) Entries() ([]TypedEntry[K, V], error) {
	entries, err := m.raw.Range(nil, nil)
	if err != nil {
		return nil, err
	}
	result := make([]TypedEntry[K, V], 0, len(entries))
	for _, entry := range entries {
		key, err := m.keyCodec.DecodeKey(entry.Key)
		if err != nil {
			return nil, err
		}
		value, err := m.valueCodec.Decode(entry.Value)
		if err != nil {
			return nil, err
		}
		result = append(result, TypedEntry[K, V]{Key: key, Value: value})
	}
	return result, nil
}

func (m *TypedVersionedMap[K, V]) Put(key K, value V) (MapVersion, error) {
	encodedKey, err := m.keyCodec.EncodeKey(key)
	if err != nil {
		return MapVersion{}, err
	}
	encodedValue, err := m.valueCodec.Encode(value)
	if err != nil {
		return MapVersion{}, err
	}
	return m.raw.Put(encodedKey, encodedValue)
}

func (m *TypedVersionedMap[K, V]) PutIf(
	expected []byte, key K, value V,
) (MapUpdate, error) {
	encodedKey, err := m.keyCodec.EncodeKey(key)
	if err != nil {
		return MapUpdate{}, err
	}
	encodedValue, err := m.valueCodec.Encode(value)
	if err != nil {
		return MapUpdate{}, err
	}
	return m.raw.PutIf(bytes.Clone(expected), encodedKey, encodedValue)
}

func (m *TypedVersionedMap[K, V]) Delete(key K) (MapVersion, error) {
	encoded, err := m.keyCodec.EncodeKey(key)
	if err != nil {
		return MapVersion{}, err
	}
	return m.raw.Delete(encoded)
}

// MigrateTypedVersionedMap is the generic Go equivalent of Rust's generic
// TypedVersionedMap::migrate_from method; Go methods cannot add type parameters.
func MigrateTypedVersionedMap[Old, K, V any](
	m *TypedVersionedMap[K, V],
	expected []byte,
	sourceCodec ValueCodec[Old],
	migrate func(Old) (V, error),
) (TypedMigrationResult, error) {
	entries, err := m.raw.RangeAt(bytes.Clone(expected), nil, nil)
	if err != nil {
		return TypedMigrationResult{}, err
	}
	mutations := make([]Mutation, 0, len(entries))
	for _, entry := range entries {
		old, err := sourceCodec.Decode(entry.Value)
		if err != nil {
			return TypedMigrationResult{}, err
		}
		value, err := migrate(old)
		if err != nil {
			return TypedMigrationResult{}, err
		}
		encoded, err := m.valueCodec.Encode(value)
		if err != nil {
			return TypedMigrationResult{}, err
		}
		mutations = append(mutations, UpsertMutation(entry.Key, encoded))
	}
	update, err := m.raw.ApplyIf(bytes.Clone(expected), mutations)
	if err != nil {
		return TypedMigrationResult{}, err
	}
	return TypedMigrationResult{
		Update: update, ScannedValues: len(entries), RewrittenValues: len(entries),
	}, nil
}
