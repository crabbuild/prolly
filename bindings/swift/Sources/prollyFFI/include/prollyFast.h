#ifndef PROLLY_FAST_H
#define PROLLY_FAST_H

#include <stddef.h>
#include <stdint.h>

typedef struct ProllyFastPageResult {
    int32_t status;
    uint8_t terminal;
    uint8_t reserved[3];
    uint32_t record_count;
    uint64_t lease_handle;
    const uint8_t *data_ptr;
    uint64_t data_len;
} ProllyFastPageResult;

typedef struct ProllyFastScanOpenResult {
    int32_t status;
    uint32_t reserved;
    uint64_t scan_handle;
} ProllyFastScanOpenResult;

typedef struct ProllyFastValueLeaseResult {
    int32_t status;
    uint8_t found;
    uint8_t reserved[3];
    uint64_t lease_handle;
    const uint8_t *data_ptr;
    uint64_t data_len;
} ProllyFastValueLeaseResult;

ProllyFastPageResult prolly_fast_proximity_search(
    uint64_t map_handle,
    const float *query_ptr,
    size_t dimensions,
    uint32_t k,
    uint64_t max_arena_bytes
);

ProllyFastScanOpenResult prolly_fast_read_session_scan_open(
    uint64_t session_handle,
    const uint8_t *start_ptr,
    size_t start_len,
    const uint8_t *end_ptr,
    size_t end_len,
    uint8_t has_end
);

ProllyFastPageResult prolly_fast_read_session_scan_next(
    uint64_t session_handle,
    uint64_t scan_handle,
    uint32_t max_records,
    uint64_t max_arena_bytes
);

void prolly_fast_scan_close(uint64_t scan_handle);

void prolly_fast_page_release(uint64_t lease_handle);

ProllyFastValueLeaseResult prolly_fast_read_session_get_lease(
    uint64_t session_handle,
    const uint8_t *key_ptr,
    size_t key_len
);

ProllyFastValueLeaseResult prolly_fast_proximity_get_lease(
    uint64_t map_handle,
    const uint8_t *key_ptr,
    size_t key_len
);

ProllyFastValueLeaseResult prolly_fast_indexed_get_lease(
    uint64_t map_handle,
    const uint8_t *key_ptr,
    size_t key_len
);

ProllyFastPageResult prolly_fast_proximity_scan_range_page(
    uint64_t map_handle,
    const uint8_t *start_ptr,
    size_t start_len,
    const uint8_t *end_ptr,
    size_t end_len,
    uint8_t has_end,
    const uint8_t *after_ptr,
    size_t after_len,
    uint8_t has_after,
    uint32_t max_records,
    uint64_t max_arena_bytes
);

void prolly_fast_value_release(uint64_t lease_handle);

#endif
