package prolly

import (
	"errors"
	"testing"
)

func validRemoteStoreDescriptor() StoreDescriptor {
	return StoreDescriptor{
		ProtocolMajor: 1,
		AdapterName:   "test-adapter",
		Provider:      "test",
		SchemaVersion: 1,
		Capabilities: StoreCapabilities{
			Hints:              true,
			AtomicNodesAndHint: true,
			ReadParallelism:    4,
			RootCompareAndSwap: true,
			Transactions:       true,
			NativeBatchReads:   true,
			AtomicBatchWrites:  true,
			NodeScan:           true,
			RootScan:           true,
		},
	}
}

func TestStoreDescriptorValidate(t *testing.T) {
	descriptor := validRemoteStoreDescriptor()
	if err := descriptor.Validate(); err != nil {
		t.Fatalf("valid descriptor: %v", err)
	}

	descriptor.ProtocolMajor = 2
	if err := descriptor.Validate(); !errors.Is(err, ErrInvalidStoreDescriptor) {
		t.Fatalf("wrong protocol error = %v", err)
	}

	descriptor = validRemoteStoreDescriptor()
	descriptor.Capabilities.ReadParallelism = 0
	if err := descriptor.Validate(); !errors.Is(err, ErrInvalidStoreDescriptor) {
		t.Fatalf("zero read parallelism error = %v", err)
	}
}

func TestStoreDescriptorRejectsInconsistentCapabilitiesAndLimits(t *testing.T) {
	descriptor := validRemoteStoreDescriptor()
	descriptor.Capabilities.Hints = false
	if err := descriptor.Validate(); !errors.Is(err, ErrInvalidStoreDescriptor) {
		t.Fatalf("atomic hint without hints error = %v", err)
	}

	descriptor = validRemoteStoreDescriptor()
	zero := uint32(0)
	descriptor.Limits.MaxBatchReadItems = &zero
	if err := descriptor.Validate(); !errors.Is(err, ErrInvalidStoreDescriptor) {
		t.Fatalf("zero limit error = %v", err)
	}
}

func TestOptionalBytesPreservesMissingAndEmpty(t *testing.T) {
	missing := MissingBytes()
	empty := PresentBytes(nil)
	if missing.Present {
		t.Fatal("missing bytes reported present")
	}
	if !empty.Present || len(empty.Value) != 0 {
		t.Fatalf("empty bytes = %#v", empty)
	}
}

func TestStoreErrorSupportsErrorsIsAndRetryMetadata(t *testing.T) {
	cause := errors.New("socket closed")
	err := &StoreError{
		Code:         "unavailable",
		Message:      "provider unavailable",
		Retryable:    true,
		ProviderCode: "503",
		Cause:        cause,
	}
	if !errors.Is(err, cause) {
		t.Fatal("store error did not unwrap cause")
	}
	if got := err.Error(); got != "unavailable: provider unavailable (provider code 503)" {
		t.Fatalf("Error() = %q", got)
	}
}
