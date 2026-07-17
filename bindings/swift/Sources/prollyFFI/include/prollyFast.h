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

ProllyFastPageResult prolly_fast_proximity_search(
    uint64_t map_handle,
    const float *query_ptr,
    size_t dimensions,
    uint32_t k,
    uint64_t max_arena_bytes
);

void prolly_fast_page_release(uint64_t lease_handle);

#endif
