package prolly

import (
	"bytes"
	"errors"
)

// ScanOutcome reports how many records were delivered to a visitor. Stopped is
// true when the visitor returned false; the stopping record is included in
// Visited.
type ScanOutcome struct {
	Visited uint64
	Stopped bool
}

type EntryVisitor func(Entry) bool
type DiffVisitor func(Diff) bool
type ConflictVisitor func(Conflict) bool

// Go crosses the native boundary in bounded pages. This keeps memory bounded
// and never lets Rust retain a Go pointer, while the native page traversal uses
// the packed read path. Records passed to visitors are owned Go values.
const bindingScanPageSize uint64 = 1024

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

// ScanRange visits [start, end) in ascending key order.
func (e *Engine) ScanRange(tree Tree, start []byte, end []byte, visitor EntryVisitor) (ScanOutcome, error) {
	var outcome ScanOutcome
	if visitor == nil {
		return outcome, errors.New("nil entry visitor")
	}
	first, err := e.LowerBound(tree, start)
	if err != nil || first == nil {
		return outcome, err
	}
	if end != nil && bytes.Compare(first.Key, end) >= 0 {
		return outcome, nil
	}
	if !visitEntry(&outcome, *first, visitor) {
		return outcome, nil
	}
	cursor := &RangeCursor{AfterKey: append([]byte(nil), first.Key...)}
	for cursor != nil {
		page, err := e.RangePage(tree, cursor, end, bindingScanPageSize)
		if err != nil {
			return outcome, err
		}
		for _, entry := range page.Entries {
			if !visitEntry(&outcome, entry, visitor) {
				return outcome, nil
			}
		}
		cursor = page.NextCursor
	}
	return outcome, nil
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
