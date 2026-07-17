package storetest_test

import (
	"context"
	"testing"

	prolly "build.crab/prolly-go"
	"build.crab/prolly-go/storetest"
)

func TestFakeConformance(t *testing.T) {
	storetest.Run(t, func(context.Context, *testing.T) prolly.RemoteStore {
		return storetest.NewFakeStore(storetest.AllCapabilities())
	})
}

func TestFakeConformanceSharedStore(t *testing.T) {
	storetest.RunWithStore(t, storetest.NewFakeStore(storetest.AllCapabilities()))
}
