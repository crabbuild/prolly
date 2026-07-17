package prolly

/*
#include <stdint.h>
#include <stdlib.h>
#include <string.h>

#ifndef PROLLY_GO_CALLBACK_TYPES
#define PROLLY_GO_CALLBACK_TYPES
typedef struct RustBuffer {
	uint64_t capacity;
	uint64_t len;
	uint8_t *data;
} RustBuffer;

typedef struct RustCallStatus {
	int8_t code;
	RustBuffer error_buf;
} RustCallStatus;
#endif

extern RustBuffer ffi_prolly_bindings_rustbuffer_alloc(uint64_t size, RustCallStatus *out_err);
extern void ffi_prolly_bindings_rustbuffer_free(RustBuffer buf, RustCallStatus *out_err);
*/
import "C"

import (
	"bytes"
	"sync"
	"sync/atomic"
	"unsafe"
)

var (
	goIndexExtractorNext atomic.Uint64
	goIndexExtractorMu   sync.Mutex
	goIndexExtractors    = map[uint64]IndexExtractor{}

	goProximityVisitorNext atomic.Uint64
	goProximityVisitorMu   sync.Mutex
	goProximityVisitors    = map[uint64]func(ProximityRecord) bool{}
)

func registerGoProximityRecordVisitor(visitor func(ProximityRecord) bool) uint64 {
	handle := goProximityVisitorNext.Add(2) - 1
	goProximityVisitorMu.Lock()
	goProximityVisitors[handle] = visitor
	goProximityVisitorMu.Unlock()
	return handle
}

func cloneGoProximityRecordVisitor(handle uint64) uint64 {
	goProximityVisitorMu.Lock()
	defer goProximityVisitorMu.Unlock()
	visitor := goProximityVisitors[handle]
	if visitor == nil {
		return 0
	}
	clone := goProximityVisitorNext.Add(2) - 1
	goProximityVisitors[clone] = visitor
	return clone
}

func removeGoProximityRecordVisitor(handle uint64) {
	goProximityVisitorMu.Lock()
	delete(goProximityVisitors, handle)
	goProximityVisitorMu.Unlock()
}

func getGoProximityRecordVisitor(handle uint64) func(ProximityRecord) bool {
	goProximityVisitorMu.Lock()
	defer goProximityVisitorMu.Unlock()
	return goProximityVisitors[handle]
}

func registerGoIndexExtractor(extractor IndexExtractor) uint64 {
	handle := goIndexExtractorNext.Add(2) - 1
	goIndexExtractorMu.Lock()
	goIndexExtractors[handle] = extractor
	goIndexExtractorMu.Unlock()
	return handle
}

func cloneGoIndexExtractor(handle uint64) uint64 {
	goIndexExtractorMu.Lock()
	defer goIndexExtractorMu.Unlock()
	extractor := goIndexExtractors[handle]
	if extractor == nil {
		return 0
	}
	clone := goIndexExtractorNext.Add(2) - 1
	goIndexExtractors[clone] = extractor
	return clone
}

func removeGoIndexExtractor(handle uint64) {
	goIndexExtractorMu.Lock()
	delete(goIndexExtractors, handle)
	goIndexExtractorMu.Unlock()
}

func getGoIndexExtractor(handle uint64) IndexExtractor {
	goIndexExtractorMu.Lock()
	defer goIndexExtractorMu.Unlock()
	return goIndexExtractors[handle]
}

//export prolly_go_index_extractor_free
func prolly_go_index_extractor_free(handle C.uint64_t) { removeGoIndexExtractor(uint64(handle)) }

//export prolly_go_index_extractor_clone
func prolly_go_index_extractor_clone(handle C.uint64_t) C.uint64_t {
	return C.uint64_t(cloneGoIndexExtractor(uint64(handle)))
}

//export prolly_go_index_extractor_extract
func prolly_go_index_extractor_extract(handle C.uint64_t, key C.RustBuffer, value C.RustBuffer, outReturn *C.RustBuffer, outStatus *C.RustCallStatus) {
	resetPortableCallbackStatus(outStatus)
	keyBytes, err := takePortableCallbackByteArray(key)
	if err != nil {
		writePortableCallbackPanic(outStatus, err.Error())
		return
	}
	valueBytes, err := takePortableCallbackByteArray(value)
	if err != nil {
		writePortableCallbackPanic(outStatus, err.Error())
		return
	}
	extractor := getGoIndexExtractor(uint64(handle))
	if extractor == nil {
		writePortableCallbackPanic(outStatus, "secondary index extractor was released")
		return
	}
	entries, err := extractor.Extract(keyBytes, valueBytes)
	if err != nil {
		writePortableCallbackPanic(outStatus, err.Error())
		return
	}
	var encoded bytes.Buffer
	writeI32(&encoded, int32(len(entries)))
	for _, entry := range entries {
		encodeByteArrayInto(&encoded, entry.Term)
		encodeOptionalByteArrayInto(&encoded, entry.Projection)
	}
	if outReturn != nil {
		*outReturn = portableCallbackBuffer(encoded.Bytes())
	}
}

//export prolly_go_proximity_record_visitor_free
func prolly_go_proximity_record_visitor_free(handle C.uint64_t) {
	removeGoProximityRecordVisitor(uint64(handle))
}

//export prolly_go_proximity_record_visitor_clone
func prolly_go_proximity_record_visitor_clone(handle C.uint64_t) C.uint64_t {
	return C.uint64_t(cloneGoProximityRecordVisitor(uint64(handle)))
}

//export prolly_go_proximity_record_visitor_visit
func prolly_go_proximity_record_visitor_visit(handle C.uint64_t, record C.RustBuffer, outReturn *C.int8_t, outStatus *C.RustCallStatus) {
	resetPortableCallbackStatus(outStatus)
	defer func() {
		if recovered := recover(); recovered != nil {
			writePortableCallbackPanic(outStatus, "panic in proximity record visitor")
			if outReturn != nil {
				*outReturn = 0
			}
		}
	}()
	raw := portableCallbackRawBuffer(record)
	decoded, err := decodeProximityRecordBytes(raw)
	if err != nil {
		writePortableCallbackPanic(outStatus, err.Error())
		return
	}
	visitor := getGoProximityRecordVisitor(uint64(handle))
	if visitor == nil {
		writePortableCallbackPanic(outStatus, "proximity record visitor was released")
		return
	}
	if outReturn != nil && visitor(decoded) {
		*outReturn = 1
	}
}

func portableCallbackRawBuffer(buf C.RustBuffer) []byte {
	var raw []byte
	if buf.data != nil && buf.len != 0 {
		raw = C.GoBytes(unsafe.Pointer(buf.data), C.int(buf.len))
	}
	var status C.RustCallStatus
	C.ffi_prolly_bindings_rustbuffer_free(buf, &status)
	return raw
}

func resetPortableCallbackStatus(status *C.RustCallStatus) {
	if status == nil {
		return
	}
	status.code = 0
	status.error_buf = C.RustBuffer{}
}

func writePortableCallbackPanic(status *C.RustCallStatus, message string) {
	if status == nil {
		return
	}
	status.code = 2
	status.error_buf = portableCallbackBuffer([]byte(message))
}

func takePortableCallbackByteArray(buf C.RustBuffer) ([]byte, error) {
	var raw []byte
	if buf.data != nil && buf.len != 0 {
		raw = C.GoBytes(unsafe.Pointer(buf.data), C.int(buf.len))
	}
	var status C.RustCallStatus
	C.ffi_prolly_bindings_rustbuffer_free(buf, &status)
	return decodeRequiredByteArray(raw)
}

func portableCallbackBuffer(data []byte) C.RustBuffer {
	var status C.RustCallStatus
	buf := C.ffi_prolly_bindings_rustbuffer_alloc(C.uint64_t(len(data)), &status)
	if status.code != 0 {
		return C.RustBuffer{}
	}
	if len(data) != 0 {
		C.memcpy(unsafe.Pointer(buf.data), unsafe.Pointer(&data[0]), C.size_t(len(data)))
	}
	buf.len = C.uint64_t(len(data))
	return buf
}
