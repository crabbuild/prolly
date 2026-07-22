package main

import (
	"bytes"
	"context"
	"errors"
	"fmt"

	"github.com/dolthub/dolt/go/store/hash"
	"github.com/dolthub/dolt/go/store/prolly"
	"github.com/dolthub/dolt/go/store/prolly/tree"
	"github.com/dolthub/dolt/go/store/val"
)

type mapCodec struct {
	ns                     tree.NodeStore
	keyDesc, valueDesc     *val.TupleDesc
	keyBuilder, valBuilder *val.TupleBuilder
}

func newMapCodec(ns tree.NodeStore) *mapCodec {
	keyDesc := val.NewTupleDescriptor(val.Type{Enc: val.ByteStringEnc, Nullable: false})
	valueDesc := val.NewTupleDescriptor(val.Type{Enc: val.ByteStringEnc, Nullable: false})
	return &mapCodec{
		ns: ns, keyDesc: keyDesc, valueDesc: valueDesc,
		keyBuilder: val.NewTupleBuilder(keyDesc, ns),
		valBuilder: val.NewTupleBuilder(valueDesc, ns),
	}
}

func (c *mapCodec) keyTuple(ctx context.Context, id int) (val.Tuple, error) {
	c.keyBuilder.PutByteString(0, key(id))
	return c.keyBuilder.Build(ctx, c.ns.Pool())
}

func (c *mapCodec) valueTuple(ctx context.Context, id int, generation byte) (val.Tuple, error) {
	c.valBuilder.PutByteString(0, value(id, generation))
	return c.valBuilder.Build(ctx, c.ns.Pool())
}

func (c *mapCodec) tuples(ctx context.Context, records int) ([]val.Tuple, error) {
	tuples := make([]val.Tuple, 0, records*2)
	for id := 0; id < records; id++ {
		keyTuple, err := c.keyTuple(ctx, id)
		if err != nil {
			return nil, err
		}
		valueTuple, err := c.valueTuple(ctx, id, 0)
		if err != nil {
			return nil, err
		}
		tuples = append(tuples, keyTuple, valueTuple)
	}
	return tuples, nil
}

func (c *mapCodec) assertValue(ctx context.Context, m prolly.Map, id int, generation byte) error {
	keyTuple, err := c.keyTuple(ctx, id)
	if err != nil {
		return err
	}
	var observed []byte
	err = m.Get(ctx, keyTuple, func(_, tuple val.Tuple) error {
		if tuple == nil {
			return fmt.Errorf("record %d is missing", id)
		}
		field, ok := c.valueDesc.GetBytes(0, tuple)
		if !ok {
			return fmt.Errorf("record %d has no value field", id)
		}
		observed = append([]byte(nil), field...)
		return nil
	})
	if err != nil {
		return err
	}
	if !bytes.Equal(observed, value(id, generation)) {
		return fmt.Errorf("record %d returned the wrong value", id)
	}
	return nil
}

func loadMap(ctx context.Context, store *sqliteChunkStore) (prolly.Map, *mapCodec, error) {
	ns := tree.NewNodeStore(store)
	codec := newMapCodec(ns)
	root, err := store.Root(ctx)
	if err != nil {
		return prolly.Map{}, nil, err
	}
	if root.IsEmpty() {
		return prolly.Map{}, nil, errors.New("fixture root is missing")
	}
	node, err := ns.Read(ctx, root)
	if err != nil {
		return prolly.Map{}, nil, err
	}
	return prolly.NewMap(node, ns, codec.keyDesc, codec.valueDesc), codec, nil
}

func publishMap(ctx context.Context, store *sqliteChunkStore, m prolly.Map, last hash.Hash) error {
	ok, err := store.Commit(ctx, m.HashOf(), last)
	if err != nil {
		return err
	}
	if !ok {
		return fmt.Errorf("stale root: expected %s", last)
	}
	return nil
}

func applyBatch(ctx context.Context, base prolly.Map, codec *mapCodec, ids []int, generation byte) (prolly.Map, error) {
	mutable := base.Mutate()
	for _, id := range ids {
		keyTuple, err := codec.keyTuple(ctx, id)
		if err != nil {
			return prolly.Map{}, err
		}
		valueTuple, err := codec.valueTuple(ctx, id, generation)
		if err != nil {
			return prolly.Map{}, err
		}
		if err := mutable.Put(ctx, keyTuple, valueTuple); err != nil {
			return prolly.Map{}, err
		}
	}
	return mutable.Map(ctx)
}
