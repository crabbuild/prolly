package prolly

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
)

// ScanOutcome reports how many records were delivered to a visitor. Stopped is
// true when the visitor returned false; the stopping record is included in
// Visited.
type ScanOutcome struct {
	Visited uint64
	Stopped bool
}

type EntryVisitor func(Entry) bool

// EntryView is backed by a native packed page and is valid only for the
// duration of the ScanRangeView callback. Copy Key or Value to retain it.
type EntryView struct {
	Key   []byte
	Value []byte
}

type EntryViewVisitor func(EntryView) bool
type DiffVisitor func(Diff) bool
type ConflictVisitor func(Conflict) bool

// Go crosses the native boundary in bounded pages. This keeps memory bounded
// and never lets Rust retain a Go pointer, while the native page traversal uses
// the packed read path. Records passed to visitors are owned Go values.
const bindingScanPageSize uint64 = 1024

const (
	fastScanPageRecords    = uint32(4096)
	fastScanPageArenaBytes = uint64(4 * 1024 * 1024)
	fastPageHeaderBytes    = 28
	fastPageEntryBytes     = 16
)

func visitEntry(outcome *ScanOutcome, entry Entry, visitor EntryVisitor) bool {
	outcome.Visited++
	if visitor(entry) {
		return true
	}
	outcome.Stopped = true
	return false
}

func visitDiff(outcome *ScanOutcome, diff Diff, visitor DiffVisitor) bool {
	outcome.Visited++
	if visitor(diff) {
		return true
	}
	outcome.Stopped = true
	return false
}

func visitConflict(outcome *ScanOutcome, conflict Conflict, visitor ConflictVisitor) bool {
	outcome.Visited++
	if visitor(conflict) {
		return true
	}
	outcome.Stopped = true
	return false
}

// ScanRangeView visits [start, end) with callback-scoped views into immutable
// native page memory. The callback must copy bytes it needs after returning.
func (s *ReadSession) ScanRangeView(start []byte, end []byte, visitor EntryViewVisitor) (ScanOutcome, error) {
	var outcome ScanOutcome
	if visitor == nil {
		return outcome, errors.New("nil entry view visitor")
	}
	if s == nil {
		return outcome, errors.New("prolly read session is closed")
	}
	s.mu.RLock()
	if s.closed.Load() || s.fast == 0 {
		s.mu.RUnlock()
		return outcome, errors.New("prolly read session is closed")
	}
	scan, err := s.openScanLocked(start, end)
	s.mu.RUnlock()
	if err != nil {
		return outcome, err
	}
	defer scan.close()

	var after []byte
	hasAfter := false
	for {
		s.mu.RLock()
		if s.closed.Load() || s.fast == 0 {
			s.mu.RUnlock()
			return outcome, errors.New("prolly read session was closed during scan")
		}
		page, err := s.scanNextLocked(&scan, fastScanPageRecords, fastScanPageArenaBytes)
		s.mu.RUnlock()
		if err != nil {
			return outcome, err
		}
		terminal := page.terminal
		var visited uint32
		var stopped bool
		var last []byte
		func() {
			defer page.close()
			visited, stopped, last, err = visitPackedEntryPage(&page, after, hasAfter, visitor)
		}()
		outcome.Visited += uint64(visited)
		if err != nil {
			return outcome, err
		}
		if stopped {
			outcome.Stopped = true
			return outcome, nil
		}
		if terminal {
			return outcome, nil
		}
		if len(last) == 0 && visited == 0 {
			return outcome, errors.New("non-terminal packed scan page made no progress")
		}
		after = append(after[:0], last...)
		hasAfter = true
	}
}

func visitPackedEntryPage(
	page *fastScanPage,
	after []byte,
	hasAfter bool,
	visitor EntryViewVisitor,
) (visited uint32, stopped bool, last []byte, err error) {
	data := page.bytes
	if len(data) < fastPageHeaderBytes || !bytes.Equal(data[:4], []byte("PRPG")) {
		return 0, false, nil, errors.New("malformed packed scan page header")
	}
	version := binary.LittleEndian.Uint16(data[4:6])
	kind := binary.LittleEndian.Uint16(data[6:8])
	flags := binary.LittleEndian.Uint32(data[8:12])
	records := binary.LittleEndian.Uint32(data[12:16])
	tableBytes := binary.LittleEndian.Uint32(data[16:20])
	arenaBytes := binary.LittleEndian.Uint64(data[20:28])
	if version != 1 || kind != 1 || records != page.records ||
		uint64(tableBytes) < uint64(records)*fastPageEntryBytes ||
		uint64(tableBytes)%fastPageEntryBytes != 0 ||
		(flags&1 != 0) != page.terminal {
		return 0, false, nil, errors.New("inconsistent packed scan page metadata")
	}
	total := uint64(fastPageHeaderBytes) + uint64(tableBytes) + arenaBytes
	if total != uint64(len(data)) {
		return 0, false, nil, errors.New("malformed packed scan page length")
	}
	arena := data[fastPageHeaderBytes+int(tableBytes):]
	var previous []byte
	for index := uint32(0); index < records; index++ {
		base := fastPageHeaderBytes + int(index)*fastPageEntryBytes
		keyOffset := binary.LittleEndian.Uint32(data[base : base+4])
		keyLength := binary.LittleEndian.Uint32(data[base+4 : base+8])
		valueOffset := binary.LittleEndian.Uint32(data[base+8 : base+12])
		valueLength := binary.LittleEndian.Uint32(data[base+12 : base+16])
		key, ok := packedRange(arena, keyOffset, keyLength)
		if !ok {
			return visited, false, nil, fmt.Errorf("packed scan key %d is out of range", index)
		}
		value, ok := packedRange(arena, valueOffset, valueLength)
		if !ok {
			return visited, false, nil, fmt.Errorf("packed scan value %d is out of range", index)
		}
		if (hasAfter && index == 0 && bytes.Compare(key, after) <= 0) ||
			(previous != nil && bytes.Compare(previous, key) >= 0) {
			return visited, false, nil, errors.New("packed scan page keys are not strictly ordered")
		}
		previous = key
		visited++
		if !visitor(EntryView{Key: key, Value: value}) {
			return visited, true, nil, nil
		}
	}
	if records != 0 {
		last = append([]byte(nil), previous...)
	}
	return visited, false, last, nil
}

func packedRange(arena []byte, offset uint32, length uint32) ([]byte, bool) {
	start := uint64(offset)
	size := uint64(length)
	if start > uint64(len(arena)) || size > uint64(len(arena))-start {
		return nil, false
	}
	return arena[int(start):int(start+size)], true
}

// ScanRange visits [start, end) with owned Go entries.
func (s *ReadSession) ScanRange(start []byte, end []byte, visitor EntryVisitor) (ScanOutcome, error) {
	if visitor == nil {
		return ScanOutcome{}, errors.New("nil entry visitor")
	}
	return s.ScanRangeView(start, end, func(view EntryView) bool {
		bytes := make([]byte, len(view.Key)+len(view.Value))
		keyEnd := copy(bytes, view.Key)
		copy(bytes[keyEnd:], view.Value)
		return visitor(Entry{
			Key:   bytes[:keyEnd:keyEnd],
			Value: bytes[keyEnd:],
		})
	})
}

// ScanRange visits [start, end) in ascending key order.
func (e *Engine) ScanRange(tree Tree, start []byte, end []byte, visitor EntryVisitor) (ScanOutcome, error) {
	if visitor == nil {
		return ScanOutcome{}, errors.New("nil entry visitor")
	}
	session, err := e.Read(tree)
	if err != nil {
		return ScanOutcome{}, err
	}
	defer session.Close()
	return session.ScanRange(start, end, visitor)
}

// ScanPrefix visits entries under prefix in ascending key order.
func (e *Engine) ScanPrefix(tree Tree, prefix []byte, visitor EntryVisitor) (ScanOutcome, error) {
	var outcome ScanOutcome
	if visitor == nil {
		return outcome, errors.New("nil entry visitor")
	}
	var cursor *RangeCursor
	for {
		page, err := e.PrefixPage(tree, prefix, cursor, bindingScanPageSize)
		if err != nil {
			return outcome, err
		}
		for _, entry := range page.Entries {
			if !visitEntry(&outcome, entry, visitor) {
				return outcome, nil
			}
		}
		if page.NextCursor == nil {
			return outcome, nil
		}
		cursor = page.NextCursor
	}
}

// ScanRangeReverse visits [start, end) in descending key order.
func (e *Engine) ScanRangeReverse(tree Tree, start []byte, end []byte, visitor EntryVisitor) (ScanOutcome, error) {
	var outcome ScanOutcome
	if visitor == nil {
		return outcome, errors.New("nil entry visitor")
	}
	var cursor *ReverseCursor
	if end != nil {
		cursor = &ReverseCursor{BeforeKey: append([]byte(nil), end...)}
	}
	for {
		page, err := e.ReversePage(tree, cursor, start, bindingScanPageSize)
		if err != nil {
			return outcome, err
		}
		for _, entry := range page.Entries {
			if !visitEntry(&outcome, entry, visitor) {
				return outcome, nil
			}
		}
		if page.NextCursor == nil {
			return outcome, nil
		}
		cursor = page.NextCursor
	}
}

// ScanPrefixReverse visits entries under prefix in descending key order.
func (e *Engine) ScanPrefixReverse(tree Tree, prefix []byte, visitor EntryVisitor) (ScanOutcome, error) {
	var outcome ScanOutcome
	if visitor == nil {
		return outcome, errors.New("nil entry visitor")
	}
	var cursor *ReverseCursor
	for {
		page, err := e.PrefixReversePage(tree, prefix, cursor, bindingScanPageSize)
		if err != nil {
			return outcome, err
		}
		for _, entry := range page.Entries {
			if !visitEntry(&outcome, entry, visitor) {
				return outcome, nil
			}
		}
		if page.NextCursor == nil {
			return outcome, nil
		}
		cursor = page.NextCursor
	}
}

// ScanDiff visits structural differences in ascending key order.
func (e *Engine) ScanDiff(base Tree, other Tree, visitor DiffVisitor) (ScanOutcome, error) {
	return e.ScanRangeDiff(base, other, nil, nil, visitor)
}

// ScanRangeDiff visits differences in [start, end). Pages before start are
// skipped without retaining them. This is bounded-memory; native callback ABI
// users can avoid the skipped prefix entirely.
func (e *Engine) ScanRangeDiff(base Tree, other Tree, start []byte, end []byte, visitor DiffVisitor) (ScanOutcome, error) {
	var outcome ScanOutcome
	if visitor == nil {
		return outcome, errors.New("nil diff visitor")
	}
	var cursor *RangeCursor
	for {
		page, err := e.DiffPage(base, other, cursor, end, bindingScanPageSize)
		if err != nil {
			return outcome, err
		}
		for _, diff := range page.Diffs {
			if start != nil && bytes.Compare(diff.Key, start) < 0 {
				continue
			}
			if !visitDiff(&outcome, diff, visitor) {
				return outcome, nil
			}
		}
		if page.NextCursor == nil {
			return outcome, nil
		}
		cursor = page.NextCursor
	}
}

// ScanConflicts visits genuine three-way conflicts in ascending key order.
func (e *Engine) ScanConflicts(base Tree, left Tree, right Tree, visitor ConflictVisitor) (ScanOutcome, error) {
	var outcome ScanOutcome
	if visitor == nil {
		return outcome, errors.New("nil conflict visitor")
	}
	var cursor *RangeCursor
	for {
		page, err := e.ConflictPage(base, left, right, cursor, bindingScanPageSize)
		if err != nil {
			return outcome, err
		}
		for _, conflict := range page.Conflicts {
			if !visitConflict(&outcome, conflict, visitor) {
				return outcome, nil
			}
		}
		if page.NextCursor == nil {
			return outcome, nil
		}
		cursor = page.NextCursor
	}
}
