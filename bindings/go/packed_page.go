package prolly

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"math"
	"sync/atomic"
)

var ErrViewExpired = errors.New("packed page view escaped its callback scope")

type viewScope struct{ expired atomic.Bool }

type ScopedBytes struct {
	data  []byte
	scope *viewScope
}

func (v ScopedBytes) Bytes() []byte {
	if v.scope == nil || v.scope.expired.Load() {
		return nil
	}
	return v.data
}

func (v ScopedBytes) Copy() ([]byte, error) {
	if v.scope == nil || v.scope.expired.Load() {
		return nil, ErrViewExpired
	}
	return append([]byte(nil), v.data...), nil
}

type IndexQuery struct {
	Kind    uint32
	Start   []byte
	End     []byte
	Reverse bool
}

func ExactIndex(term []byte) IndexQuery {
	return IndexQuery{Kind: 1, Start: append([]byte(nil), term...)}
}
func PrefixIndex(prefix []byte) IndexQuery {
	return IndexQuery{Kind: 2, Start: append([]byte(nil), prefix...)}
}
func RangeIndex(start, end []byte) IndexQuery {
	return IndexQuery{Kind: 3, Start: append([]byte(nil), start...), End: append([]byte(nil), end...)}
}

type IndexMatchView struct {
	Term       ScopedBytes
	PrimaryKey ScopedBytes
	Projection *ScopedBytes
}

type packedHeader struct {
	kind       uint16
	count      int
	tableBytes int
	arenaBytes int
}

func parsePackedHeader(page []byte, expectedKind uint16, width int) (packedHeader, error) {
	if len(page) < 28 || string(page[:4]) != "PRPG" {
		return packedHeader{}, errors.New("invalid packed page header")
	}
	version := binary.LittleEndian.Uint16(page[4:6])
	kind := binary.LittleEndian.Uint16(page[6:8])
	if version != 2 || kind != expectedKind {
		return packedHeader{}, fmt.Errorf("unexpected packed page version/kind %d/%d", version, kind)
	}
	count := int(binary.LittleEndian.Uint32(page[12:16]))
	table := int(binary.LittleEndian.Uint32(page[16:20]))
	arena64 := binary.LittleEndian.Uint64(page[20:28])
	if arena64 > uint64(len(page)) {
		return packedHeader{}, errors.New("packed page arena exceeds host size")
	}
	arena := int(arena64)
	if table != count*width || 28+table+arena != len(page) {
		return packedHeader{}, errors.New("invalid packed page bounds")
	}
	return packedHeader{kind, count, table, arena}, nil
}

func scopedArenaField(page []byte, arenaStart, arenaBytes, offset, length int, scope *viewScope) (ScopedBytes, error) {
	if offset < 0 || length < 0 || offset > arenaBytes || length > arenaBytes-offset {
		return ScopedBytes{}, errors.New("packed page field exceeds arena")
	}
	return ScopedBytes{data: page[arenaStart+offset : arenaStart+offset+length], scope: scope}, nil
}

func decodeIndexViews(page []byte, scope *viewScope) ([]IndexMatchView, error) {
	header, err := parsePackedHeader(page, 5, 36)
	if err != nil {
		return nil, err
	}
	arenaStart := 28 + header.tableBytes
	rows := make([]IndexMatchView, 0, header.count)
	for index := 0; index < header.count; index++ {
		base := 28 + index*36
		flags := binary.LittleEndian.Uint32(page[base : base+4])
		term, err := scopedArenaField(page, arenaStart, header.arenaBytes, int(binary.LittleEndian.Uint32(page[base+4:base+8])), int(binary.LittleEndian.Uint32(page[base+8:base+12])), scope)
		if err != nil {
			return nil, err
		}
		key, err := scopedArenaField(page, arenaStart, header.arenaBytes, int(binary.LittleEndian.Uint32(page[base+12:base+16])), int(binary.LittleEndian.Uint32(page[base+16:base+20])), scope)
		if err != nil {
			return nil, err
		}
		var projection *ScopedBytes
		if flags&1 != 0 {
			field, err := scopedArenaField(page, arenaStart, header.arenaBytes, int(binary.LittleEndian.Uint32(page[base+20:base+24])), int(binary.LittleEndian.Uint32(page[base+24:base+28])), scope)
			if err != nil {
				return nil, err
			}
			projection = &field
		}
		rows = append(rows, IndexMatchView{term, key, projection})
	}
	return rows, nil
}

func (i *SecondaryIndex) QueryView(ctx context.Context, query IndexQuery, visit func(IndexMatchView) bool) error {
	if ctx == nil {
		ctx = context.Background()
	}
	if visit == nil {
		return errors.New("nil index view visitor")
	}
	_, fast, unlock, err := i.withHandle()
	if err != nil {
		return err
	}
	defer unlock()
	if err := ctx.Err(); err != nil {
		return err
	}
	cursor, err := ffiOpenIndexCursor(fast, query)
	if err != nil {
		return err
	}
	defer cursor.Close()
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		page, err := cursor.Next(256)
		if err != nil {
			return err
		}
		scope := &viewScope{}
		rows, decodeErr := decodeIndexViews(page.data, scope)
		if decodeErr != nil {
			scope.expired.Store(true)
			page.Close()
			return decodeErr
		}
		keepGoing := true
		for _, row := range rows {
			if !visit(row) {
				keepGoing = false
				break
			}
		}
		scope.expired.Store(true)
		terminal := page.terminal
		page.Close()
		if !keepGoing || terminal {
			return nil
		}
	}
}

type NeighborView struct {
	Key      ScopedBytes
	Value    *ScopedBytes
	Proof    *ScopedBytes
	Distance float64
	Rank     uint32
}

func decodeNeighborViews(page []byte, scope *viewScope) ([]NeighborView, error) {
	header, err := parsePackedHeader(page, 7, 40)
	if err != nil {
		return nil, err
	}
	arenaStart := 28 + header.tableBytes
	rows := make([]NeighborView, 0, header.count)
	for index := 0; index < header.count; index++ {
		base := 28 + index*40
		flags := binary.LittleEndian.Uint32(page[base : base+4])
		key, err := scopedArenaField(page, arenaStart, header.arenaBytes, int(binary.LittleEndian.Uint32(page[base+4:base+8])), int(binary.LittleEndian.Uint32(page[base+8:base+12])), scope)
		if err != nil {
			return nil, err
		}
		var value, proof *ScopedBytes
		if flags&1 != 0 {
			field, err := scopedArenaField(page, arenaStart, header.arenaBytes, int(binary.LittleEndian.Uint32(page[base+24:base+28])), int(binary.LittleEndian.Uint32(page[base+28:base+32])), scope)
			if err != nil {
				return nil, err
			}
			value = &field
		}
		if flags&2 != 0 {
			field, err := scopedArenaField(page, arenaStart, header.arenaBytes, int(binary.LittleEndian.Uint32(page[base+32:base+36])), int(binary.LittleEndian.Uint32(page[base+36:base+40])), scope)
			if err != nil {
				return nil, err
			}
			proof = &field
		}
		rows = append(rows, NeighborView{key, value, proof, math.Float64frombits(binary.LittleEndian.Uint64(page[base+12 : base+20])), binary.LittleEndian.Uint32(page[base+20 : base+24])})
	}
	return rows, nil
}
