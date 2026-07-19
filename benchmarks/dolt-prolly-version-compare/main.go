package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"runtime"
	"time"

	"github.com/dolthub/dolt/go/store/chunks"
	"github.com/dolthub/dolt/go/store/prolly"
	"github.com/dolthub/dolt/go/store/prolly/message"
	"github.com/dolthub/dolt/go/store/prolly/tree"
	"github.com/dolthub/dolt/go/store/val"
)

const csvHeader = "implementation,revision,contract_version,records,density,locality,operation,relationship,operations,elapsed_ns,ns_per_op,ops_per_sec,workload_digest,result_digest,result_count,base_count,target_count,conflict_count,validated"

type arguments struct {
	records  uint64
	density  uint64
	locality locality
}

type measurement struct {
	operation      string
	relationship   string
	operations     uint64
	elapsed        time.Duration
	workloadDigest uint64
	resultDigest   uint64
	resultCount    uint64
	baseCount      uint64
	targetCount    uint64
	conflictCount  uint64
}

type mapEnvironment struct {
	ctx       context.Context
	ns        tree.NodeStore
	keyDesc   *val.TupleDesc
	valueDesc *val.TupleDesc
}

type slicePatchIter struct {
	patches []tree.Patch
	index   int
}

func (s *slicePatchIter) NextPatch(context.Context) (tree.Patch, error) {
	if s.index >= len(s.patches) {
		return tree.Patch{}, nil
	}
	result := s.patches[s.index]
	s.index++
	return result, nil
}

func (s *slicePatchIter) Close() error { return nil }

func main() {
	args, err := parseArgs(os.Args[1:])
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(2)
	}
	rows, err := run(args)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	revision := os.Getenv("BENCH_REVISION")
	if revision == "" {
		revision = "unknown"
	}
	fmt.Println(csvHeader)
	for _, row := range rows {
		emit(revision, args, row)
	}
}

func parseArgs(argv []string) (arguments, error) {
	flags := flag.NewFlagSet("prolly-version-compare", flag.ContinueOnError)
	records := flags.Uint64("records", 0, "base record count")
	density := flags.Uint64("density", 0, "0, 1, or 30")
	localityName := flags.String("locality", "", "none, append, random, or clustered")
	if err := flags.Parse(argv); err != nil {
		return arguments{}, err
	}
	selected := locality(*localityName)
	if *records < clusterSize || *records%clusterSize != 0 {
		return arguments{}, fmt.Errorf("records must be a multiple of %d", clusterSize)
	}
	if *density != 0 && *density != 1 && *density != 30 {
		return arguments{}, errors.New("density must be 0, 1, or 30")
	}
	if selected != localityNone && selected != localityAppend && selected != localityRandom && selected != localityClustered {
		return arguments{}, fmt.Errorf("invalid locality %q", selected)
	}
	if (*density == 0) != (selected == localityNone) {
		return arguments{}, errors.New("0 density requires none locality and non-zero density requires a real locality")
	}
	return arguments{records: *records, density: *density, locality: selected}, nil
}

func run(args arguments) ([]measurement, error) {
	storage := &chunks.TestStorage{}
	env := mapEnvironment{
		ctx:       context.Background(),
		ns:        tree.NewNodeStore(storage.NewView()),
		keyDesc:   val.NewTupleDescriptor(val.Type{Enc: val.ByteStringEnc, Nullable: false}),
		valueDesc: val.NewTupleDescriptor(val.Type{Enc: val.ByteStringEnc, Nullable: false}),
	}
	base, err := env.buildMap(prolly.Map{}, baseMutations(args.records), true)
	if err != nil {
		return nil, fmt.Errorf("build base: %w", err)
	}
	baseCount, _, err := env.mapSummary(base)
	if err != nil || baseCount != args.records {
		return nil, fmt.Errorf("validate base count %d: %w", baseCount, err)
	}

	edits := changeCount(args.records, args.density)
	leftMutations := branchMutations(args.records, args.density, args.locality, 0, 1)
	left, err := env.buildMap(base, leftMutations, false)
	if err != nil {
		return nil, fmt.Errorf("build left: %w", err)
	}
	leftCount, leftDigest, err := env.mapSummary(left)
	if err != nil {
		return nil, err
	}
	startKey, endKey := rangeBounds(args.records, args.density, args.locality, leftMutations)
	startTuple, err := env.tuple(env.keyDesc, startKey)
	if err != nil {
		return nil, err
	}
	endTuple, err := env.tuple(env.keyDesc, endKey)
	if err != nil {
		return nil, err
	}
	compareWorkload := workloadDigest(args.records, "compare", leftMutations)
	expectedCount, expectedDiffDigest, err := env.diffSummary(base, left)
	if err != nil || expectedCount != edits {
		return nil, fmt.Errorf("validation diff count %d want %d: %w", expectedCount, edits, err)
	}

	rows := make([]measurement, 0, 7)
	started := time.Now()
	diffCount, diffDigest, err := env.diffSummary(base, left)
	elapsed := time.Since(started)
	if err != nil || diffCount != expectedCount || diffDigest != expectedDiffDigest {
		return nil, fmt.Errorf("timed diff mismatch: %w", err)
	}
	rows = append(rows, measurement{"full_diff", "compare", max(diffCount, 1), elapsed, compareWorkload, diffDigest, diffCount, baseCount, leftCount, 0})

	expectedRangeCount, expectedRangeDigest, err := env.rangeDiffSummary(base, left, startTuple, endTuple)
	if err != nil {
		return nil, err
	}
	started = time.Now()
	rangeCount, rangeDigest, err := env.rangeDiffSummary(base, left, startTuple, endTuple)
	elapsed = time.Since(started)
	if err != nil || rangeCount != expectedRangeCount || rangeDigest != expectedRangeDigest {
		return nil, fmt.Errorf("timed range diff mismatch: %w", err)
	}
	rows = append(rows, measurement{"range_diff", "compare", max(rangeCount, 1), elapsed, compareWorkload, rangeDigest, rangeCount, baseCount, leftCount, 0})

	started = time.Now()
	patches, err := env.generatePatches(base, left)
	elapsed = time.Since(started)
	if err != nil {
		return nil, err
	}
	rows = append(rows, measurement{"patch_generate", "compare", max(expectedCount, 1), elapsed, compareWorkload, expectedDiffDigest, uint64(len(patches)), baseCount, leftCount, 0})

	patchIter := &slicePatchIter{patches: patches}
	serializer := message.NewProllyMapSerializer(env.valueDesc, env.ns.Pool())
	started = time.Now()
	patchedRoot, err := tree.ApplyPatches[val.Tuple](env.ctx, env.ns, base.Node(), env.keyDesc, serializer, patchIter)
	elapsed = time.Since(started)
	if err != nil {
		return nil, fmt.Errorf("apply patches: %w", err)
	}
	patched := prolly.NewMap(patchedRoot, env.ns, env.keyDesc, env.valueDesc)
	patchedCount, patchedDigest, err := env.mapSummary(patched)
	if err != nil || patchedCount != leftCount || patchedDigest != leftDigest {
		return nil, fmt.Errorf("patched map mismatch: %w", err)
	}
	rows = append(rows, measurement{"patch_apply", "compare", max(expectedCount, 1), elapsed, compareWorkload, patchedDigest, patchedCount, baseCount, leftCount, 0})

	if args.density == 0 {
		row, err := env.measureMerge(base, base, base, "noop", compareWorkload, 0, 1)
		if err != nil {
			return nil, err
		}
		return append(rows, row), nil
	}

	rightMutations := branchMutations(args.records, args.density, args.locality, edits, 2)
	right, err := env.buildMap(base, rightMutations, false)
	if err != nil {
		return nil, err
	}
	row, err := env.measureMerge(base, left, right, "disjoint", workloadDigest(args.records, "disjoint", leftMutations, rightMutations), 0, edits*2)
	if err != nil {
		return nil, err
	}
	rows = append(rows, row)

	row, err = env.measureMerge(base, left, left, "convergent", workloadDigest(args.records, "convergent", leftMutations, leftMutations), 0, edits)
	if err != nil {
		return nil, err
	}
	rows = append(rows, row)

	conflictMutations := conflictingMutations(leftMutations)
	rightConflict, err := env.buildMap(base, conflictMutations, false)
	if err != nil {
		return nil, err
	}
	row, err = env.measureMerge(base, left, rightConflict, "conflict", workloadDigest(args.records, "conflict", leftMutations, conflictMutations), edits, edits)
	if err != nil {
		return nil, err
	}
	return append(rows, row), nil
}

func (env mapEnvironment) buildMap(base prolly.Map, mutations []logicalMutation, empty bool) (prolly.Map, error) {
	if empty {
		var err error
		base, err = prolly.NewMapFromTuples(env.ctx, env.ns, env.keyDesc, env.valueDesc)
		if err != nil {
			return prolly.Map{}, err
		}
	}
	mutable := base.Mutate()
	for _, mutation := range mutations {
		key, err := env.tuple(env.keyDesc, mutation.key)
		if err != nil {
			return prolly.Map{}, err
		}
		var value val.Tuple
		if !mutation.delete {
			value, err = env.tuple(env.valueDesc, mutation.value)
			if err != nil {
				return prolly.Map{}, err
			}
		}
		if err := mutable.Put(env.ctx, key, value); err != nil {
			return prolly.Map{}, err
		}
	}
	return mutable.Map(env.ctx)
}

func (env mapEnvironment) tuple(desc *val.TupleDesc, logical []byte) (val.Tuple, error) {
	builder := val.NewTupleBuilder(desc, env.ns)
	builder.PutByteString(0, logical)
	return builder.Build(env.ctx, env.ns.Pool())
}

func (env mapEnvironment) mapSummary(m prolly.Map) (uint64, uint64, error) {
	iter, err := m.IterAll(env.ctx)
	if err != nil {
		return 0, 0, err
	}
	count, digest := uint64(0), fnvOffset
	for {
		keyTuple, valueTuple, err := iter.Next(env.ctx)
		if errors.Is(err, io.EOF) {
			return count, digestUint64(digest, count), nil
		}
		if err != nil {
			return 0, 0, err
		}
		key, keyOK := env.keyDesc.GetBytes(0, keyTuple)
		value, valueOK := env.valueDesc.GetBytes(0, valueTuple)
		if !keyOK || !valueOK {
			return 0, 0, errors.New("map contains NULL key or value")
		}
		digest = digestEntry(digest, key, value)
		count++
	}
}

func (env mapEnvironment) diffSummary(from, to prolly.Map) (uint64, uint64, error) {
	return env.consumeDiff(func(cb tree.DiffFn) error { return prolly.DiffMaps(env.ctx, from, to, false, cb) })
}

func (env mapEnvironment) rangeDiffSummary(from, to prolly.Map, start, stop val.Tuple) (uint64, uint64, error) {
	return env.consumeDiff(func(cb tree.DiffFn) error { return prolly.DiffMapsKeyRange(env.ctx, from, to, start, stop, cb) })
}

func (env mapEnvironment) consumeDiff(run func(tree.DiffFn) error) (uint64, uint64, error) {
	count, digest := uint64(0), fnvOffset
	err := run(func(_ context.Context, diff tree.Diff) error {
		key, ok := env.keyDesc.GetBytes(0, val.Tuple(diff.Key))
		if !ok {
			return errors.New("diff key is NULL")
		}
		switch diff.Type {
		case tree.AddedDiff:
			value, ok := env.valueDesc.GetBytes(0, val.Tuple(diff.To))
			if !ok {
				return errors.New("added value is NULL")
			}
			digest = digestBytes(digest, []byte{1})
			digest = digestBytes(digest, key)
			digest = digestBytes(digest, value)
		case tree.RemovedDiff:
			value, ok := env.valueDesc.GetBytes(0, val.Tuple(diff.From))
			if !ok {
				return errors.New("removed value is NULL")
			}
			digest = digestBytes(digest, []byte{2})
			digest = digestBytes(digest, key)
			digest = digestBytes(digest, value)
		case tree.ModifiedDiff:
			oldValue, oldOK := env.valueDesc.GetBytes(0, val.Tuple(diff.From))
			newValue, newOK := env.valueDesc.GetBytes(0, val.Tuple(diff.To))
			if !oldOK || !newOK {
				return errors.New("modified value is NULL")
			}
			digest = digestBytes(digest, []byte{3})
			digest = digestBytes(digest, key)
			digest = digestBytes(digest, oldValue)
			digest = digestBytes(digest, newValue)
		default:
			return errors.New("unexpected diff type")
		}
		count++
		return nil
	})
	if errors.Is(err, io.EOF) {
		err = nil
	}
	return count, digestUint64(digest, count), err
}

func (env mapEnvironment) generatePatches(from, to prolly.Map) ([]tree.Patch, error) {
	generator, err := tree.PatchGeneratorFromRoots[val.Tuple](env.ctx, env.ns, env.ns, from.Node(), to.Node(), env.keyDesc)
	if err != nil {
		return nil, err
	}
	var patches []tree.Patch
	for {
		patch, _, more, err := generator.Next(env.ctx)
		if err != nil {
			return nil, err
		}
		if !more {
			return patches, nil
		}
		patches = append(patches, patch)
	}
}

func (env mapEnvironment) measureMerge(base, left, right prolly.Map, relationship string, workload uint64, expectedConflicts, operations uint64) (measurement, error) {
	conflicts := uint64(0)
	resolver := func(leftDiff, _ tree.Diff) (tree.Diff, bool) {
		conflicts++
		return leftDiff, true
	}
	started := time.Now()
	merged, _, err := prolly.MergeMaps(env.ctx, left, right, base, resolver)
	elapsed := time.Since(started)
	if err != nil {
		return measurement{}, err
	}
	if conflicts != expectedConflicts {
		return measurement{}, fmt.Errorf("merge conflicts %d want %d", conflicts, expectedConflicts)
	}
	count, digest, err := env.mapSummary(merged)
	if err != nil {
		return measurement{}, err
	}
	if relationship == "convergent" || relationship == "conflict" {
		leftCount, leftDigest, err := env.mapSummary(left)
		if err != nil || count != leftCount || digest != leftDigest {
			return measurement{}, fmt.Errorf("prefer-left merge mismatch: %w", err)
		}
	}
	baseCount, _, err := env.mapSummary(base)
	if err != nil {
		return measurement{}, err
	}
	name := "merge_" + relationship
	return measurement{name, relationship, max(operations, 1), elapsed, workload, digest, count, baseCount, count, conflicts}, nil
}

func emit(revision string, args arguments, row measurement) {
	operations := max(row.operations, 1)
	elapsedNS := uint64(row.elapsed.Nanoseconds())
	nsPerOp := float64(elapsedNS) / float64(operations)
	opsPerSec := float64(0)
	if elapsedNS != 0 {
		opsPerSec = float64(operations) * 1e9 / float64(elapsedNS)
	}
	fmt.Printf("dolt-go,%s,%s,%d,%d,%s,%s,%s,%d,%d,%.3f,%.3f,%016x,%016x,%d,%d,%d,%d,true\n",
		revision, contractVersion, args.records, args.density, args.locality, row.operation, row.relationship,
		operations, elapsedNS, nsPerOp, opsPerSec, row.workloadDigest, row.resultDigest,
		row.resultCount, row.baseCount, row.targetCount, row.conflictCount)
	runtime.KeepAlive(row.resultDigest)
}
