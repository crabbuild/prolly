package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"os"
)

type command struct {
	kind, output, revision, operation, pattern string
	records, repetition, changes, readSamples  int
}

func main() {
	cmd, err := parseCommand(os.Args[1:])
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(2)
	}
	var row protocolRow
	if cmd.kind == "fixture" {
		row, err = buildFixture(context.Background(), fixtureSpec{output: cmd.output, records: cmd.records, repetition: cmd.repetition, revision: cmd.revision})
	} else {
		op, parseErr := parseOperation(cmd.operation)
		if parseErr != nil {
			fmt.Fprintln(os.Stderr, parseErr)
			os.Exit(2)
		}
		selectedPattern, parseErr := parsePattern(cmd.pattern)
		if parseErr != nil {
			fmt.Fprintln(os.Stderr, parseErr)
			os.Exit(2)
		}
		cache := cacheNA
		if op == opGetCold {
			cache = cacheCold
		}
		if op == opGetWarm {
			cache = cacheWarm
		}
		row, err = runCell(context.Background(), cellSpec{output: cmd.output, records: cmd.records, repetition: cmd.repetition, revision: cmd.revision, operation: op, pattern: selectedPattern, cacheState: cache, changes: cmd.changes, readSamples: cmd.readSamples})
	}
	encoder := json.NewEncoder(os.Stdout)
	encoder.SetEscapeHTML(false)
	if err != nil {
		row.Validated = false
		row.Error = err.Error()
		_ = encoder.Encode(row)
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	if err := encoder.Encode(row); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func parseCommand(args []string) (command, error) {
	if len(args) == 0 || args[0] != "fixture" && args[0] != "cell" {
		return command{}, fmt.Errorf("usage: prolly-sqlite-compare fixture|cell [options]")
	}
	cmd := command{kind: args[0]}
	flags := flag.NewFlagSet(cmd.kind, flag.ContinueOnError)
	flags.SetOutput(io.Discard)
	flags.StringVar(&cmd.output, "output", "", "fixture output root")
	flags.StringVar(&cmd.revision, "revision", "", "implementation revision")
	flags.IntVar(&cmd.records, "records", 0, "record count")
	flags.IntVar(&cmd.repetition, "repetition", 0, "repetition")
	if cmd.kind == "cell" {
		flags.StringVar(&cmd.operation, "operation", "", "operation")
		flags.StringVar(&cmd.pattern, "pattern", "", "pattern")
		flags.IntVar(&cmd.changes, "changes", 0, "change count")
		flags.IntVar(&cmd.readSamples, "read-samples", 0, "read samples")
	}
	if err := flags.Parse(args[1:]); err != nil {
		return command{}, err
	}
	if flags.NArg() != 0 {
		return command{}, fmt.Errorf("unexpected arguments: %v", flags.Args())
	}
	if cmd.output == "" || cmd.revision == "" || cmd.records <= 0 || cmd.repetition <= 0 {
		return command{}, fmt.Errorf("output, revision, records, and repetition are required and positive")
	}
	if cmd.kind == "cell" {
		op, err := parseOperation(cmd.operation)
		if err != nil {
			return command{}, err
		}
		if _, err := parsePattern(cmd.pattern); err != nil {
			return command{}, err
		}
		if cmd.changes <= 0 || cmd.changes > cmd.records || cmd.readSamples <= 0 || cmd.readSamples > cmd.records {
			return command{}, fmt.Errorf("changes and read samples must be positive and not exceed records")
		}
		if op == opMerge && cmd.changes%2 != 0 {
			return command{}, fmt.Errorf("merge changes must be even")
		}
	}
	return cmd, nil
}
