package prolly

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"math"
	"sync"
)

var ErrViewExpired = errors.New("packed page view escaped its callback scope")

type viewScope struct {
	mu    sync.RWMutex
	alive bool
}

func newViewScope() *viewScope { return &viewScope{alive: true} }

func (s *viewScope) close() {
	s.mu.Lock()
	s.alive = false
	s.mu.Unlock()
}

type ScopedBytes struct {
	data  []byte
	scope *viewScope
}

// Bytes returns an owned copy, or nil after the callback scope expires.
// Use Len, At, Equal, Compare, or WriteTo to inspect the live view without a copy.
func (v ScopedBytes) Bytes() []byte {
	value, _ := v.Copy()
	return value
}

func (v ScopedBytes) Copy() ([]byte, error) {
	var result []byte
	err := v.withData(func(data []byte) { result = append([]byte(nil), data...) })
	return result, err
}

func (v ScopedBytes) withData(read func([]byte)) error {
	if v.scope == nil {
		return ErrViewExpired
	}
	v.scope.mu.RLock()
	defer v.scope.mu.RUnlock()
	if !v.scope.alive {
		return ErrViewExpired
	}
	read(v.data)
	return nil
}

func (v ScopedBytes) mustWithData(read func([]byte)) {
	if err := v.withData(read); err != nil {
		panic(err)
	}
}

func (v ScopedBytes) Len() int {
	length := 0
	v.mustWithData(func(data []byte) { length = len(data) })
	return length
}

func (v ScopedBytes) At(index int) byte {
	var value byte
	v.mustWithData(func(data []byte) { value = data[index] })
	return value
}

func (v ScopedBytes) AppendTo(destination []byte) []byte {
	v.mustWithData(func(data []byte) { destination = append(destination, data...) })
	return destination
}

func (v ScopedBytes) Equal(other []byte) bool {
	equal := false
	v.mustWithData(func(data []byte) { equal = bytes.Equal(data, other) })
	return equal
}

func (v ScopedBytes) Compare(other []byte) int {
	comparison := 0
	v.mustWithData(func(data []byte) { comparison = bytes.Compare(data, other) })
	return comparison
}

func (v ScopedBytes) String() string {
	var result string
	v.mustWithData(func(data []byte) { result = string(data) })
	return result
}

// WriteTo writes the view synchronously without an intermediate copy. As
// required by io.Writer, the writer must not retain the supplied byte slice.
func (v ScopedBytes) WriteTo(writer io.Writer) (int64, error) {
	if writer == nil {
		return 0, errors.New("nil scoped-bytes writer")
	}
	var written int
	var writeErr error
	if err := v.withData(func(data []byte) {
		written, writeErr = writer.Write(data)
		if writeErr == nil && written != len(data) {
			writeErr = io.ErrShortWrite
		}
	}); err != nil {
		return 0, err
	}
	return int64(written), writeErr
}

func (v ScopedBytes) copyTo(destination []byte) int {
	written := 0
	v.mustWithData(func(data []byte) { written = copy(destination, data) })
	return written
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
		scope := newViewScope()
		rows, decodeErr := decodeIndexViews(page.data, scope)
		if decodeErr != nil {
			scope.close()
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
		scope.close()
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
