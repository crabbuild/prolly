//! Narrow native transport for operations where UniFFI record serialization
//! dominates the Rust work.
//!
//! UniFFI still owns object construction, lifetime, portable APIs, and rich
//! errors. Handwritten native adapters may use these functions only while
//! holding a live `ProllyReadSession` object.

use std::collections::HashMap;
use std::panic::{catch_unwind, AssertUnwindSafe};
use std::ptr;
use std::slice;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, OnceLock, Weak};

use super::{
    BindingProximityMap, BindingRangeScanSession, BindingSecondaryIndexSnapshot, ProllyReadSession,
    ProximitySearchRequestRecord,
};
use crate::domain::handle::{HandleKind, HandleRegistry, ResourceHandle};
use crate::domain::page::{PackedPage, PackedPageBuilder, PackedPageKind, PageLimits};

pub const FAST_ABI_VERSION: u32 = 1;
pub const FAST_CAP_GET_INTO: u64 = 1 << 0;
pub const FAST_CAP_SCAN_PAGE: u64 = 1 << 1;
pub const FAST_CAP_RETAINED_SCAN: u64 = 1 << 2;
pub const FAST_CAP_GET_MANY_PAGE: u64 = 1 << 3;
pub const FAST_CAP_VALUE_LEASE: u64 = 1 << 4;
pub const FAST_CAP_INDEX_CURSOR: u64 = 1 << 5;
pub const FAST_CAP_PROXIMITY_SEARCH: u64 = 1 << 6;

pub const FAST_STATUS_OK: i32 = 0;
pub const FAST_STATUS_BUFFER_TOO_SMALL: i32 = 1;
pub const FAST_STATUS_INVALID_ARGUMENT: i32 = 2;
pub const FAST_STATUS_READ_ERROR: i32 = 3;
pub const FAST_STATUS_PANIC: i32 = 4;

/// Result returned entirely in registers/by value; no Rust allocation crosses
/// the boundary on a successful common-size point read.
#[repr(C)]
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct FastCopyResult {
    pub status: i32,
    pub found: u8,
    pub reserved: [u8; 3],
    pub written: u64,
    pub required: u64,
}

#[repr(C)]
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct FastPageResult {
    pub status: i32,
    pub terminal: u8,
    pub reserved: [u8; 3],
    pub record_count: u32,
    pub lease_handle: u64,
    pub data_ptr: *const u8,
    pub data_len: u64,
}

#[repr(C)]
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct FastScanOpenResult {
    pub status: i32,
    pub reserved: u32,
    pub scan_handle: u64,
}

#[repr(C)]
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct FastValueLeaseResult {
    pub status: i32,
    pub found: u8,
    pub reserved: [u8; 3],
    pub lease_handle: u64,
    pub data_ptr: *const u8,
    pub data_len: u64,
}

struct FastScanHandle {
    owner_session: u64,
    scan: Mutex<BindingRangeScanSession>,
}

struct FastPageLease {
    bytes: Box<[u8]>,
}

enum FastIndexQuery {
    Exact(Vec<u8>),
    Prefix(Vec<u8>),
    Range(Vec<u8>, Option<Vec<u8>>),
}

struct FastIndexCursorState {
    cursor: Option<Vec<u8>>,
    terminal: bool,
}

struct FastIndexCursorHandle {
    owner_snapshot: u64,
    query: FastIndexQuery,
    reverse: bool,
    state: Mutex<FastIndexCursorState>,
}

static NEXT_SESSION_HANDLE: AtomicU64 = AtomicU64::new(1);
static NEXT_SCAN_HANDLE: AtomicU64 = AtomicU64::new(1);
static NEXT_VALUE_HANDLE: AtomicU64 = AtomicU64::new(1);
static NEXT_INDEX_SNAPSHOT_HANDLE: AtomicU64 = AtomicU64::new(1);
static NEXT_INDEX_CURSOR_HANDLE: AtomicU64 = AtomicU64::new(1);
static NEXT_PROXIMITY_HANDLE: AtomicU64 = AtomicU64::new(1);
static SESSION_HANDLES: OnceLock<Mutex<HashMap<u64, Weak<ProllyReadSession>>>> = OnceLock::new();
static SCAN_HANDLES: OnceLock<Mutex<HashMap<u64, Arc<FastScanHandle>>>> = OnceLock::new();
static VALUE_HANDLES: OnceLock<Mutex<HashMap<u64, Arc<prolly::OwnedValueLease>>>> = OnceLock::new();
static INDEX_SNAPSHOT_HANDLES: OnceLock<Mutex<HashMap<u64, Weak<BindingSecondaryIndexSnapshot>>>> =
    OnceLock::new();
static INDEX_CURSOR_HANDLES: OnceLock<Mutex<HashMap<u64, Arc<FastIndexCursorHandle>>>> =
    OnceLock::new();
static PROXIMITY_HANDLES: OnceLock<Mutex<HashMap<u64, Weak<BindingProximityMap>>>> =
    OnceLock::new();
static RESOURCE_HANDLES: OnceLock<HandleRegistry> = OnceLock::new();

fn session_handles() -> &'static Mutex<HashMap<u64, Weak<ProllyReadSession>>> {
    SESSION_HANDLES.get_or_init(|| Mutex::new(HashMap::new()))
}

fn scan_handles() -> &'static Mutex<HashMap<u64, Arc<FastScanHandle>>> {
    SCAN_HANDLES.get_or_init(|| Mutex::new(HashMap::new()))
}

fn resource_handles() -> &'static HandleRegistry {
    RESOURCE_HANDLES.get_or_init(HandleRegistry::new)
}

fn value_handles() -> &'static Mutex<HashMap<u64, Arc<prolly::OwnedValueLease>>> {
    VALUE_HANDLES.get_or_init(|| Mutex::new(HashMap::new()))
}

fn index_snapshot_handles() -> &'static Mutex<HashMap<u64, Weak<BindingSecondaryIndexSnapshot>>> {
    INDEX_SNAPSHOT_HANDLES.get_or_init(|| Mutex::new(HashMap::new()))
}

fn index_cursor_handles() -> &'static Mutex<HashMap<u64, Arc<FastIndexCursorHandle>>> {
    INDEX_CURSOR_HANDLES.get_or_init(|| Mutex::new(HashMap::new()))
}

pub(crate) fn register_index_snapshot(snapshot: &Arc<BindingSecondaryIndexSnapshot>) -> u64 {
    let mut handles = index_snapshot_handles()
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    loop {
        let handle = next_nonzero_handle(&NEXT_INDEX_SNAPSHOT_HANDLE);
        if let std::collections::hash_map::Entry::Vacant(entry) = handles.entry(handle) {
            entry.insert(Arc::downgrade(snapshot));
            return handle;
        }
    }
}

pub(crate) fn unregister_index_snapshot(handle: u64) {
    if handle == 0 {
        return;
    }
    index_snapshot_handles()
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .remove(&handle);
    index_cursor_handles()
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .retain(|_, cursor| cursor.owner_snapshot != handle);
}

fn index_snapshot_from_handle(handle: u64) -> Option<Arc<BindingSecondaryIndexSnapshot>> {
    let mut handles = index_snapshot_handles()
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    let snapshot = handles.get(&handle).and_then(Weak::upgrade);
    if snapshot.is_none() {
        handles.remove(&handle);
    }
    snapshot
}

fn register_index_cursor(cursor: FastIndexCursorHandle) -> u64 {
    let mut handles = index_cursor_handles()
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    loop {
        let handle = next_nonzero_handle(&NEXT_INDEX_CURSOR_HANDLE);
        if let std::collections::hash_map::Entry::Vacant(entry) = handles.entry(handle) {
            entry.insert(Arc::new(cursor));
            return handle;
        }
    }
}

fn index_cursor_from_handle(handle: u64) -> Option<Arc<FastIndexCursorHandle>> {
    index_cursor_handles()
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .get(&handle)
        .cloned()
}

fn proximity_handles() -> &'static Mutex<HashMap<u64, Weak<BindingProximityMap>>> {
    PROXIMITY_HANDLES.get_or_init(|| Mutex::new(HashMap::new()))
}

pub(crate) fn register_proximity_map(map: &Arc<BindingProximityMap>) -> u64 {
    let mut handles = proximity_handles()
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    loop {
        let handle = next_nonzero_handle(&NEXT_PROXIMITY_HANDLE);
        if let std::collections::hash_map::Entry::Vacant(entry) = handles.entry(handle) {
            entry.insert(Arc::downgrade(map));
            return handle;
        }
    }
}

pub(crate) fn unregister_proximity_map(handle: u64) {
    if handle != 0 {
        proximity_handles()
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .remove(&handle);
    }
}

fn proximity_from_handle(handle: u64) -> Option<Arc<BindingProximityMap>> {
    let mut handles = proximity_handles()
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    let map = handles.get(&handle).and_then(Weak::upgrade);
    if map.is_none() {
        handles.remove(&handle);
    }
    map
}

fn next_nonzero_handle(counter: &AtomicU64) -> u64 {
    loop {
        let handle = counter.fetch_add(1, Ordering::Relaxed);
        if handle != 0 {
            return handle;
        }
    }
}

pub(crate) fn register_read_session(session: &Arc<ProllyReadSession>) -> u64 {
    let mut handles = session_handles()
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    loop {
        let handle = next_nonzero_handle(&NEXT_SESSION_HANDLE);
        if let std::collections::hash_map::Entry::Vacant(entry) = handles.entry(handle) {
            entry.insert(Arc::downgrade(session));
            return handle;
        }
    }
}

pub(crate) fn unregister_read_session(handle: u64) {
    if handle == 0 {
        return;
    }
    session_handles()
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .remove(&handle);
    scan_handles()
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .retain(|_, scan| scan.owner_session != handle);
}

fn session_from_handle(handle: u64) -> Option<Arc<ProllyReadSession>> {
    if handle == 0 {
        return None;
    }
    let mut handles = session_handles()
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    let session = handles.get(&handle).and_then(Weak::upgrade);
    if session.is_none() {
        handles.remove(&handle);
    }
    session
}

fn register_scan(scan: FastScanHandle) -> u64 {
    let mut handles = scan_handles()
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    loop {
        let handle = next_nonzero_handle(&NEXT_SCAN_HANDLE);
        if let std::collections::hash_map::Entry::Vacant(entry) = handles.entry(handle) {
            entry.insert(Arc::new(scan));
            return handle;
        }
    }
}

fn scan_from_handle(handle: u64) -> Option<Arc<FastScanHandle>> {
    scan_handles()
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .get(&handle)
        .cloned()
}

fn register_page(bytes: Box<[u8]>) -> (u64, *const u8, u64) {
    let page = PackedPage::parse(&bytes, PageLimits::default())
        .expect("internal packed page encoder produced an invalid page");
    let _validated_header = (
        page.version(),
        page.kind(),
        page.record_count(),
        page.terminal(),
    );
    let lease = FastPageLease { bytes };
    let data_ptr = lease.bytes.as_ptr();
    let data_len = lease.bytes.len() as u64;
    let handle = resource_handles().insert(HandleKind::Page, lease);
    (handle.raw(), data_ptr, data_len)
}

fn register_value(lease: prolly::OwnedValueLease) -> Result<(u64, *const u8, u64), prolly::Error> {
    let lease = Arc::new(lease);
    let bytes = lease.as_bytes()?;
    let data_ptr = bytes.as_ptr();
    let data_len = bytes.len() as u64;
    let mut handles = value_handles()
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    loop {
        let handle = next_nonzero_handle(&NEXT_VALUE_HANDLE);
        if let std::collections::hash_map::Entry::Vacant(entry) = handles.entry(handle) {
            entry.insert(lease);
            return Ok((handle, data_ptr, data_len));
        }
    }
}

#[derive(Clone, Copy)]
struct EntryOffsets {
    key_offset: u32,
    key_len: u32,
    value_offset: u32,
    value_len: u32,
}

#[derive(Clone, Copy)]
struct GetManyOffsets {
    found: u32,
    value_offset: u32,
    value_len: u32,
}

const PAGE_MAGIC: &[u8; 4] = b"PRPG";
const PAGE_VERSION: u16 = 1;
const PAGE_KIND_ENTRY: u16 = 1;
const PAGE_KIND_GET_MANY: u16 = 2;
const PAGE_FLAG_TERMINAL: u32 = 1;
const PAGE_HEADER_BYTES: usize = 28;
const PAGE_ENTRY_BYTES: usize = 16;
const PAGE_GET_MANY_BYTES: usize = 12;
const MAX_PAGE_RECORDS: u32 = 65_536;
const MAX_PAGE_ARENA_BYTES: u64 = 64 * 1024 * 1024;
const MAX_MULTI_GET_KEYS: u32 = 65_536;
const MAX_MULTI_GET_INPUT_BYTES: usize = 128 * 1024 * 1024;
const MAX_BOUND_BYTES: usize = 64 * 1024 * 1024;

struct PackedEntryPageBuilder {
    bytes: Vec<u8>,
    table_bytes: usize,
    record_count: u32,
}

impl PackedEntryPageBuilder {
    fn new(max_records: u32, max_arena_bytes: u64) -> Result<Self, &'static str> {
        if max_records == 0 || max_records > MAX_PAGE_RECORDS {
            return Err("packed page record limit is invalid");
        }
        if max_arena_bytes == 0 || max_arena_bytes > MAX_PAGE_ARENA_BYTES {
            return Err("packed page arena limit is invalid");
        }
        let table_bytes = max_records as usize * PAGE_ENTRY_BYTES;
        let arena_capacity = usize::try_from(max_arena_bytes)
            .map_err(|_| "packed page arena exceeds address space")?;
        let capacity = PAGE_HEADER_BYTES
            .checked_add(table_bytes)
            .and_then(|bytes| bytes.checked_add(arena_capacity))
            .ok_or("packed page capacity overflows address space")?;
        let mut bytes = Vec::with_capacity(capacity);
        bytes.resize(PAGE_HEADER_BYTES + table_bytes, 0);
        Ok(Self {
            bytes,
            table_bytes,
            record_count: 0,
        })
    }

    fn push(&mut self, key: &[u8], value: &[u8]) -> Result<(), &'static str> {
        let entry_bytes = key
            .len()
            .checked_add(value.len())
            .ok_or("packed page entry size overflows address space")?;
        let arena_bytes = self
            .arena_len()
            .checked_add(entry_bytes)
            .ok_or("packed page arena overflows address space")?;
        if arena_bytes > MAX_PAGE_ARENA_BYTES as usize {
            return Err("packed page arena exceeds the 64 MiB transport limit");
        }
        let arena_start = PAGE_HEADER_BYTES + self.table_bytes;
        let key_offset = u32::try_from(self.bytes.len() - arena_start)
            .map_err(|_| "packed page arena exceeds u32")?;
        let key_len = u32::try_from(key.len()).map_err(|_| "packed page key exceeds u32")?;
        self.bytes.extend_from_slice(key);
        let value_offset = u32::try_from(self.bytes.len() - arena_start)
            .map_err(|_| "packed page arena exceeds u32")?;
        let value_len = u32::try_from(value.len()).map_err(|_| "packed page value exceeds u32")?;
        self.bytes.extend_from_slice(value);

        let record = PAGE_HEADER_BYTES + self.record_count as usize * PAGE_ENTRY_BYTES;
        self.bytes[record..record + 4].copy_from_slice(&key_offset.to_le_bytes());
        self.bytes[record + 4..record + 8].copy_from_slice(&key_len.to_le_bytes());
        self.bytes[record + 8..record + 12].copy_from_slice(&value_offset.to_le_bytes());
        self.bytes[record + 12..record + 16].copy_from_slice(&value_len.to_le_bytes());
        self.record_count += 1;
        Ok(())
    }

    fn arena_len(&self) -> usize {
        self.bytes.len() - PAGE_HEADER_BYTES - self.table_bytes
    }

    fn finish(mut self, terminal: bool) -> Box<[u8]> {
        let arena_len = self.arena_len() as u64;
        self.bytes[..4].copy_from_slice(PAGE_MAGIC);
        self.bytes[4..6].copy_from_slice(&PAGE_VERSION.to_le_bytes());
        self.bytes[6..8].copy_from_slice(&PAGE_KIND_ENTRY.to_le_bytes());
        self.bytes[8..12]
            .copy_from_slice(&(if terminal { PAGE_FLAG_TERMINAL } else { 0 }).to_le_bytes());
        self.bytes[12..16].copy_from_slice(&self.record_count.to_le_bytes());
        self.bytes[16..20].copy_from_slice(&(self.table_bytes as u32).to_le_bytes());
        self.bytes[20..28].copy_from_slice(&arena_len.to_le_bytes());
        self.bytes.into_boxed_slice()
    }
}

impl FastCopyResult {
    fn status(status: i32) -> Self {
        Self {
            status,
            ..Self::default()
        }
    }
}

#[no_mangle]
pub extern "C" fn prolly_fast_abi_version() -> u32 {
    FAST_ABI_VERSION
}

#[no_mangle]
pub extern "C" fn prolly_fast_abi_capabilities() -> u64 {
    FAST_CAP_GET_INTO
        | FAST_CAP_SCAN_PAGE
        | FAST_CAP_RETAINED_SCAN
        | FAST_CAP_GET_MANY_PAGE
        | FAST_CAP_VALUE_LEASE
        | FAST_CAP_INDEX_CURSOR
        | FAST_CAP_PROXIMITY_SEARCH
}

unsafe fn input_slice<'a>(ptr: *const u8, len: usize) -> Option<&'a [u8]> {
    if len == 0 {
        return Some(&[]);
    }
    if ptr.is_null() {
        return None;
    }
    // SAFETY: the caller promises a readable input range for this synchronous
    // call. Rust never retains this slice.
    Some(unsafe { slice::from_raw_parts(ptr, len) })
}

/// Copy one point-read result directly into caller-owned memory.
///
/// A missing key returns `status=OK, found=0`. If the output is too small,
/// `required` reports the exact value size and no output bytes are written.
#[no_mangle]
pub unsafe extern "C" fn prolly_fast_read_session_get_into(
    session_handle: u64,
    key_ptr: *const u8,
    key_len: usize,
    out_ptr: *mut u8,
    out_capacity: usize,
) -> FastCopyResult {
    let Some(session) = session_from_handle(session_handle) else {
        return FastCopyResult::status(FAST_STATUS_INVALID_ARGUMENT);
    };
    if key_len > MAX_BOUND_BYTES {
        session.set_fast_error("point-read key exceeds the fast ABI limit");
        return FastCopyResult::status(FAST_STATUS_INVALID_ARGUMENT);
    }
    let Some(key) = (unsafe { input_slice(key_ptr, key_len) }) else {
        session.set_fast_error("non-empty key has a null pointer");
        return FastCopyResult::status(FAST_STATUS_INVALID_ARGUMENT);
    };
    if out_capacity != 0 && out_ptr.is_null() {
        session.set_fast_error("non-empty output capacity has a null pointer");
        return FastCopyResult::status(FAST_STATUS_INVALID_ARGUMENT);
    }

    session.clear_fast_error();
    match catch_unwind(AssertUnwindSafe(|| {
        let mut required = 0usize;
        let found = session.get_with(key, |value| {
            required = value.len();
            if value.len() <= out_capacity && !value.is_empty() {
                // SAFETY: output capacity was validated above and the regions
                // cannot overlap: the value is retained Rust node memory while
                // the destination is caller-owned output memory.
                unsafe { ptr::copy_nonoverlapping(value.as_ptr(), out_ptr, value.len()) };
            }
        });

        match found {
            Ok(None) => FastCopyResult {
                status: FAST_STATUS_OK,
                found: 0,
                ..FastCopyResult::default()
            },
            Ok(Some(())) if required > out_capacity => FastCopyResult {
                status: FAST_STATUS_BUFFER_TOO_SMALL,
                found: 1,
                required: required as u64,
                ..FastCopyResult::default()
            },
            Ok(Some(())) => FastCopyResult {
                status: FAST_STATUS_OK,
                found: 1,
                written: required as u64,
                required: required as u64,
                ..FastCopyResult::default()
            },
            Err(error) => {
                session.set_fast_error(error.to_string());
                FastCopyResult::status(FAST_STATUS_READ_ERROR)
            }
        }
    })) {
        Ok(result) => result,
        Err(_) => {
            session.set_fast_error("panic in fast point-read transport");
            FastCopyResult::status(FAST_STATUS_PANIC)
        }
    }
}

/// Retain the immutable packed leaf containing a value and return a direct
/// view. The pointer remains valid until `prolly_fast_value_release` is called.
#[no_mangle]
pub unsafe extern "C" fn prolly_fast_read_session_get_lease(
    session_handle: u64,
    key_ptr: *const u8,
    key_len: usize,
) -> FastValueLeaseResult {
    let Some(session) = session_from_handle(session_handle) else {
        return FastValueLeaseResult {
            status: FAST_STATUS_INVALID_ARGUMENT,
            ..FastValueLeaseResult::default()
        };
    };
    if key_len > MAX_BOUND_BYTES {
        session.set_fast_error("point-read key exceeds the fast ABI limit");
        return FastValueLeaseResult {
            status: FAST_STATUS_INVALID_ARGUMENT,
            ..FastValueLeaseResult::default()
        };
    }
    let Some(key) = (unsafe { input_slice(key_ptr, key_len) }) else {
        session.set_fast_error("non-empty key has a null pointer");
        return FastValueLeaseResult {
            status: FAST_STATUS_INVALID_ARGUMENT,
            ..FastValueLeaseResult::default()
        };
    };

    session.clear_fast_error();
    match catch_unwind(AssertUnwindSafe(|| match session.inner.get_lease(key) {
        Ok(None) => FastValueLeaseResult {
            status: FAST_STATUS_OK,
            found: 0,
            ..FastValueLeaseResult::default()
        },
        Ok(Some(lease)) => match register_value(lease) {
            Ok((lease_handle, data_ptr, data_len)) => FastValueLeaseResult {
                status: FAST_STATUS_OK,
                found: 1,
                lease_handle,
                data_ptr,
                data_len,
                ..FastValueLeaseResult::default()
            },
            Err(error) => {
                session.set_fast_error(error.to_string());
                FastValueLeaseResult {
                    status: FAST_STATUS_READ_ERROR,
                    ..FastValueLeaseResult::default()
                }
            }
        },
        Err(error) => {
            session.set_fast_error(error.to_string());
            FastValueLeaseResult {
                status: FAST_STATUS_READ_ERROR,
                ..FastValueLeaseResult::default()
            }
        }
    })) {
        Ok(result) => result,
        Err(_) => {
            session.set_fast_error("panic in fast point-read lease transport");
            FastValueLeaseResult {
                status: FAST_STATUS_PANIC,
                ..FastValueLeaseResult::default()
            }
        }
    }
}

/// Batch point reads from a little-endian offset table plus key arena and
/// return one packed result page in caller order.
#[no_mangle]
pub unsafe extern "C" fn prolly_fast_read_session_get_many_page(
    session_handle: u64,
    input_ptr: *const u8,
    input_len: usize,
    key_count: u32,
) -> FastPageResult {
    let Some(session) = session_from_handle(session_handle) else {
        return FastPageResult {
            status: FAST_STATUS_INVALID_ARGUMENT,
            ..FastPageResult::default()
        };
    };
    if key_count > MAX_MULTI_GET_KEYS || input_len > MAX_MULTI_GET_INPUT_BYTES {
        session.set_fast_error("multi-get input exceeds the fast ABI limit");
        return FastPageResult {
            status: FAST_STATUS_INVALID_ARGUMENT,
            ..FastPageResult::default()
        };
    }
    let Some(input) = (unsafe { input_slice(input_ptr, input_len) }) else {
        session.set_fast_error("non-empty multi-get input has a null pointer");
        return FastPageResult {
            status: FAST_STATUS_INVALID_ARGUMENT,
            ..FastPageResult::default()
        };
    };
    let Some(table_bytes) = (key_count as usize)
        .checked_add(1)
        .and_then(|count| count.checked_mul(4))
    else {
        session.set_fast_error("multi-get offset table overflows address space");
        return FastPageResult {
            status: FAST_STATUS_INVALID_ARGUMENT,
            ..FastPageResult::default()
        };
    };
    if input.len() < table_bytes {
        session.set_fast_error("multi-get input is shorter than its offset table");
        return FastPageResult {
            status: FAST_STATUS_INVALID_ARGUMENT,
            ..FastPageResult::default()
        };
    }
    let key_arena = &input[table_bytes..];
    let mut previous = 0usize;
    let mut keys = Vec::with_capacity(key_count as usize);
    for index in 0..=key_count as usize {
        let base = index * 4;
        let offset = u32::from_le_bytes(input[base..base + 4].try_into().expect("four bytes"));
        let offset = offset as usize;
        if offset < previous || offset > key_arena.len() || (index == 0 && offset != 0) {
            session.set_fast_error("multi-get offsets are not monotonic within the key arena");
            return FastPageResult {
                status: FAST_STATUS_INVALID_ARGUMENT,
                ..FastPageResult::default()
            };
        }
        if index != 0 {
            keys.push(&key_arena[previous..offset]);
        }
        previous = offset;
    }
    if previous != key_arena.len() {
        session.set_fast_error("multi-get final offset does not consume the key arena");
        return FastPageResult {
            status: FAST_STATUS_INVALID_ARGUMENT,
            ..FastPageResult::default()
        };
    }

    session.clear_fast_error();
    match catch_unwind(AssertUnwindSafe(|| {
        let mut records = vec![
            GetManyOffsets {
                found: 0,
                value_offset: 0,
                value_len: 0,
            };
            keys.len()
        ];
        let mut arena = Vec::new();
        let mut build_error = None;
        let read = session.inner.get_many_with(&keys, |position, _, value| {
            if build_error.is_some() {
                return;
            }
            let Some(value) = value else {
                return;
            };
            let Some(next_arena_len) = arena.len().checked_add(value.len()) else {
                build_error = Some("multi-get result arena overflows address space");
                return;
            };
            if next_arena_len > MAX_PAGE_ARENA_BYTES as usize {
                build_error = Some("multi-get result arena exceeds the 64 MiB transport limit");
                return;
            }
            let Ok(value_offset) = u32::try_from(arena.len()) else {
                build_error = Some("multi-get result arena exceeds u32");
                return;
            };
            let Ok(value_len) = u32::try_from(value.len()) else {
                build_error = Some("multi-get value exceeds u32");
                return;
            };
            arena.extend_from_slice(value);
            records[position] = GetManyOffsets {
                found: 1,
                value_offset,
                value_len,
            };
        });
        if let Err(error) = read {
            session.set_fast_error(error.to_string());
            return FastPageResult {
                status: FAST_STATUS_READ_ERROR,
                ..FastPageResult::default()
            };
        }
        if let Some(error) = build_error {
            session.set_fast_error(error);
            return FastPageResult {
                status: FAST_STATUS_INVALID_ARGUMENT,
                ..FastPageResult::default()
            };
        }
        if arena.len() > u32::MAX as usize {
            session.set_fast_error("multi-get result arena exceeds u32");
            return FastPageResult {
                status: FAST_STATUS_INVALID_ARGUMENT,
                ..FastPageResult::default()
            };
        }
        let bytes = encode_get_many_page(&records, &arena);
        let (lease_handle, data_ptr, data_len) = register_page(bytes);
        FastPageResult {
            status: FAST_STATUS_OK,
            terminal: 1,
            record_count: records.len() as u32,
            lease_handle,
            data_ptr,
            data_len,
            ..FastPageResult::default()
        }
    })) {
        Ok(result) => result,
        Err(_) => {
            session.set_fast_error("panic in fast multi-get transport");
            FastPageResult {
                status: FAST_STATUS_PANIC,
                ..FastPageResult::default()
            }
        }
    }
}

/// Copy the most recent fast-path error for this session into caller-owned
/// storage. Error retrieval is cold-path only.
#[no_mangle]
pub unsafe extern "C" fn prolly_fast_read_session_last_error_into(
    session_handle: u64,
    out_ptr: *mut u8,
    out_capacity: usize,
) -> FastCopyResult {
    let Some(session) = session_from_handle(session_handle) else {
        return FastCopyResult::status(FAST_STATUS_INVALID_ARGUMENT);
    };
    if out_capacity != 0 && out_ptr.is_null() {
        return FastCopyResult::status(FAST_STATUS_INVALID_ARGUMENT);
    }
    let error = session.fast_error();
    let bytes = error.as_bytes();
    if bytes.len() > out_capacity {
        return FastCopyResult {
            status: FAST_STATUS_BUFFER_TOO_SMALL,
            required: bytes.len() as u64,
            ..FastCopyResult::default()
        };
    }
    if !bytes.is_empty() {
        // SAFETY: output capacity and non-nullness were checked above.
        unsafe { ptr::copy_nonoverlapping(bytes.as_ptr(), out_ptr, bytes.len()) };
    }
    FastCopyResult {
        status: FAST_STATUS_OK,
        written: bytes.len() as u64,
        required: bytes.len() as u64,
        ..FastCopyResult::default()
    }
}

fn append_entry(
    records: &mut Vec<EntryOffsets>,
    arena: &mut Vec<u8>,
    key: &[u8],
    value: &[u8],
) -> Result<(), &'static str> {
    let key_offset = u32::try_from(arena.len()).map_err(|_| "packed page arena exceeds u32")?;
    let key_len = u32::try_from(key.len()).map_err(|_| "packed page key exceeds u32")?;
    arena.extend_from_slice(key);
    let value_offset = u32::try_from(arena.len()).map_err(|_| "packed page arena exceeds u32")?;
    let value_len = u32::try_from(value.len()).map_err(|_| "packed page value exceeds u32")?;
    arena.extend_from_slice(value);
    records.push(EntryOffsets {
        key_offset,
        key_len,
        value_offset,
        value_len,
    });
    Ok(())
}

fn encode_entry_page(records: &[EntryOffsets], arena: &[u8], terminal: bool) -> Box<[u8]> {
    let table_bytes = records.len() * PAGE_ENTRY_BYTES;
    let mut page = Vec::with_capacity(PAGE_HEADER_BYTES + table_bytes + arena.len());
    page.extend_from_slice(PAGE_MAGIC);
    page.extend_from_slice(&PAGE_VERSION.to_le_bytes());
    page.extend_from_slice(&PAGE_KIND_ENTRY.to_le_bytes());
    page.extend_from_slice(&(if terminal { PAGE_FLAG_TERMINAL } else { 0 }).to_le_bytes());
    page.extend_from_slice(&(records.len() as u32).to_le_bytes());
    page.extend_from_slice(&(table_bytes as u32).to_le_bytes());
    page.extend_from_slice(&(arena.len() as u64).to_le_bytes());
    for record in records {
        page.extend_from_slice(&record.key_offset.to_le_bytes());
        page.extend_from_slice(&record.key_len.to_le_bytes());
        page.extend_from_slice(&record.value_offset.to_le_bytes());
        page.extend_from_slice(&record.value_len.to_le_bytes());
    }
    page.extend_from_slice(arena);
    page.into_boxed_slice()
}

fn encode_get_many_page(records: &[GetManyOffsets], arena: &[u8]) -> Box<[u8]> {
    let table_bytes = records.len() * PAGE_GET_MANY_BYTES;
    let mut page = Vec::with_capacity(PAGE_HEADER_BYTES + table_bytes + arena.len());
    page.extend_from_slice(PAGE_MAGIC);
    page.extend_from_slice(&PAGE_VERSION.to_le_bytes());
    page.extend_from_slice(&PAGE_KIND_GET_MANY.to_le_bytes());
    page.extend_from_slice(&PAGE_FLAG_TERMINAL.to_le_bytes());
    page.extend_from_slice(&(records.len() as u32).to_le_bytes());
    page.extend_from_slice(&(table_bytes as u32).to_le_bytes());
    page.extend_from_slice(&(arena.len() as u64).to_le_bytes());
    for record in records {
        page.extend_from_slice(&record.found.to_le_bytes());
        page.extend_from_slice(&record.value_offset.to_le_bytes());
        page.extend_from_slice(&record.value_len.to_le_bytes());
    }
    page.extend_from_slice(arena);
    page.into_boxed_slice()
}

/// Build one validated packed entry page. The returned pointer is owned by the
/// lease and remains valid until `prolly_fast_page_release` is called.
#[no_mangle]
pub unsafe extern "C" fn prolly_fast_read_session_scan_page(
    session_handle: u64,
    start_ptr: *const u8,
    start_len: usize,
    end_ptr: *const u8,
    end_len: usize,
    has_end: u8,
    after_ptr: *const u8,
    after_len: usize,
    has_after: u8,
    max_records: u32,
    max_arena_bytes: u64,
) -> FastPageResult {
    let Some(session) = session_from_handle(session_handle) else {
        return FastPageResult {
            status: FAST_STATUS_INVALID_ARGUMENT,
            ..FastPageResult::default()
        };
    };
    if start_len > MAX_BOUND_BYTES
        || end_len > MAX_BOUND_BYTES
        || after_len > MAX_BOUND_BYTES
        || has_end > 1
        || has_after > 1
    {
        session.set_fast_error("scan bounds or presence flags exceed the fast ABI limit");
        return FastPageResult {
            status: FAST_STATUS_INVALID_ARGUMENT,
            ..FastPageResult::default()
        };
    }
    let Some(start) = (unsafe { input_slice(start_ptr, start_len) }) else {
        session.set_fast_error("non-empty scan start has a null pointer");
        return FastPageResult {
            status: FAST_STATUS_INVALID_ARGUMENT,
            ..FastPageResult::default()
        };
    };
    let end = if has_end == 0 {
        None
    } else {
        let Some(end) = (unsafe { input_slice(end_ptr, end_len) }) else {
            session.set_fast_error("non-empty scan end has a null pointer");
            return FastPageResult {
                status: FAST_STATUS_INVALID_ARGUMENT,
                ..FastPageResult::default()
            };
        };
        Some(end)
    };
    let after = if has_after == 0 {
        None
    } else {
        let Some(after) = (unsafe { input_slice(after_ptr, after_len) }) else {
            session.set_fast_error("non-empty scan cursor has a null pointer");
            return FastPageResult {
                status: FAST_STATUS_INVALID_ARGUMENT,
                ..FastPageResult::default()
            };
        };
        Some(after)
    };
    if after.is_some_and(|after| after < start) {
        session.set_fast_error("scan cursor precedes the requested start bound");
        return FastPageResult {
            status: FAST_STATUS_INVALID_ARGUMENT,
            ..FastPageResult::default()
        };
    }
    if max_records == 0
        || max_records > MAX_PAGE_RECORDS
        || max_arena_bytes == 0
        || max_arena_bytes > MAX_PAGE_ARENA_BYTES
    {
        session.set_fast_error("scan page limits exceed the fast ABI maximum");
        return FastPageResult {
            status: FAST_STATUS_INVALID_ARGUMENT,
            ..FastPageResult::default()
        };
    }

    session.clear_fast_error();
    match catch_unwind(AssertUnwindSafe(|| {
        let seek = after.unwrap_or(start);
        let mut records = Vec::with_capacity(max_records as usize);
        let initial_arena_capacity = usize::try_from(max_arena_bytes)
            .unwrap_or(usize::MAX)
            .min(1024 * 1024);
        let mut arena = Vec::with_capacity(initial_arena_capacity);
        let mut build_error = None;
        let outcome = session.inner.scan_range_until(seek, end, |entry| {
            if after.is_some_and(|after| entry.key() == after) {
                return std::ops::ControlFlow::Continue(());
            }
            let Some(next_bytes) = entry.key().len().checked_add(entry.value().len()) else {
                build_error = Some("packed page entry size overflows address space");
                return std::ops::ControlFlow::Break(());
            };
            if next_bytes > MAX_PAGE_ARENA_BYTES as usize {
                build_error = Some("packed page entry exceeds the 64 MiB transport limit");
                return std::ops::ControlFlow::Break(());
            }
            let byte_limit = usize::try_from(max_arena_bytes).unwrap_or(usize::MAX);
            if records.len() >= max_records as usize
                || (!records.is_empty() && arena.len().saturating_add(next_bytes) > byte_limit)
            {
                return std::ops::ControlFlow::Break(());
            }
            if let Err(error) = append_entry(&mut records, &mut arena, entry.key(), entry.value()) {
                build_error = Some(error);
                return std::ops::ControlFlow::Break(());
            }
            std::ops::ControlFlow::Continue(())
        });

        let outcome = match outcome {
            Ok(outcome) => outcome,
            Err(error) => {
                session.set_fast_error(error.to_string());
                return FastPageResult {
                    status: FAST_STATUS_READ_ERROR,
                    ..FastPageResult::default()
                };
            }
        };
        if let Some(error) = build_error {
            session.set_fast_error(error);
            return FastPageResult {
                status: FAST_STATUS_INVALID_ARGUMENT,
                ..FastPageResult::default()
            };
        }

        let terminal = outcome.break_value.is_none();
        let bytes = encode_entry_page(&records, &arena, terminal);
        let record_count = records.len() as u32;
        let (lease_handle, data_ptr, data_len) = register_page(bytes);
        FastPageResult {
            status: FAST_STATUS_OK,
            terminal: u8::from(terminal),
            record_count,
            lease_handle,
            data_ptr,
            data_len,
            ..FastPageResult::default()
        }
    })) {
        Ok(result) => result,
        Err(_) => {
            session.set_fast_error("panic in fast scan-page transport");
            FastPageResult {
                status: FAST_STATUS_PANIC,
                ..FastPageResult::default()
            }
        }
    }
}

/// Open a retained native traversal. It seeks once and owns its traversal
/// stack until close.
#[no_mangle]
pub unsafe extern "C" fn prolly_fast_read_session_scan_open(
    session_handle: u64,
    start_ptr: *const u8,
    start_len: usize,
    end_ptr: *const u8,
    end_len: usize,
    has_end: u8,
) -> FastScanOpenResult {
    let Some(session) = session_from_handle(session_handle) else {
        return FastScanOpenResult {
            status: FAST_STATUS_INVALID_ARGUMENT,
            ..FastScanOpenResult::default()
        };
    };
    if start_len > MAX_BOUND_BYTES || end_len > MAX_BOUND_BYTES || has_end > 1 {
        session.set_fast_error("scan bounds or presence flags exceed the fast ABI limit");
        return FastScanOpenResult {
            status: FAST_STATUS_INVALID_ARGUMENT,
            ..FastScanOpenResult::default()
        };
    }
    let Some(start) = (unsafe { input_slice(start_ptr, start_len) }) else {
        session.set_fast_error("non-empty scan start has a null pointer");
        return FastScanOpenResult {
            status: FAST_STATUS_INVALID_ARGUMENT,
            ..FastScanOpenResult::default()
        };
    };
    let end = if has_end == 0 {
        None
    } else {
        let Some(end) = (unsafe { input_slice(end_ptr, end_len) }) else {
            session.set_fast_error("non-empty scan end has a null pointer");
            return FastScanOpenResult {
                status: FAST_STATUS_INVALID_ARGUMENT,
                ..FastScanOpenResult::default()
            };
        };
        Some(end)
    };

    session.clear_fast_error();
    match catch_unwind(AssertUnwindSafe(|| {
        match session.inner.open_range_scan(start, end) {
            Ok(scan) => {
                let handle = register_scan(FastScanHandle {
                    owner_session: session_handle,
                    scan: Mutex::new(scan),
                });
                FastScanOpenResult {
                    status: FAST_STATUS_OK,
                    scan_handle: handle,
                    ..FastScanOpenResult::default()
                }
            }
            Err(error) => {
                session.set_fast_error(error.to_string());
                FastScanOpenResult {
                    status: FAST_STATUS_READ_ERROR,
                    ..FastScanOpenResult::default()
                }
            }
        }
    })) {
        Ok(result) => result,
        Err(_) => {
            session.set_fast_error("panic opening retained scan transport");
            FastScanOpenResult {
                status: FAST_STATUS_PANIC,
                ..FastScanOpenResult::default()
            }
        }
    }
}

/// Continue a retained scan and return one packed page.
#[no_mangle]
pub unsafe extern "C" fn prolly_fast_read_session_scan_next(
    session_handle: u64,
    scan_handle: u64,
    max_records: u32,
    max_arena_bytes: u64,
) -> FastPageResult {
    let Some(session) = session_from_handle(session_handle) else {
        return FastPageResult {
            status: FAST_STATUS_INVALID_ARGUMENT,
            ..FastPageResult::default()
        };
    };
    if scan_handle == 0
        || max_records == 0
        || max_records > MAX_PAGE_RECORDS
        || max_arena_bytes == 0
        || max_arena_bytes > MAX_PAGE_ARENA_BYTES
    {
        session.set_fast_error("retained scan handle and limits must be non-zero");
        return FastPageResult {
            status: FAST_STATUS_INVALID_ARGUMENT,
            ..FastPageResult::default()
        };
    }
    let Some(scan_handle) = scan_from_handle(scan_handle) else {
        session.set_fast_error("retained scan handle is invalid or closed");
        return FastPageResult {
            status: FAST_STATUS_INVALID_ARGUMENT,
            ..FastPageResult::default()
        };
    };
    if scan_handle.owner_session != session_handle {
        session.set_fast_error("retained scan belongs to a different read session");
        return FastPageResult {
            status: FAST_STATUS_INVALID_ARGUMENT,
            ..FastPageResult::default()
        };
    }

    session.clear_fast_error();
    match catch_unwind(AssertUnwindSafe(|| {
        let Ok(mut scan) = scan_handle.scan.lock() else {
            session.set_fast_error("retained scan lock poisoned");
            return FastPageResult {
                status: FAST_STATUS_READ_ERROR,
                ..FastPageResult::default()
            };
        };
        let mut builder = match PackedEntryPageBuilder::new(max_records, max_arena_bytes) {
            Ok(builder) => builder,
            Err(error) => {
                session.set_fast_error(error);
                return FastPageResult {
                    status: FAST_STATUS_INVALID_ARGUMENT,
                    ..FastPageResult::default()
                };
            }
        };
        let mut build_error = None;
        let outcome = scan.next_until(|entry| {
            let byte_limit = usize::try_from(max_arena_bytes).unwrap_or(usize::MAX);
            if let Err(error) = builder.push(entry.key(), entry.value()) {
                build_error = Some(error);
                return std::ops::ControlFlow::Break(());
            }
            if builder.record_count >= max_records || builder.arena_len() >= byte_limit {
                std::ops::ControlFlow::Break(())
            } else {
                std::ops::ControlFlow::Continue(())
            }
        });
        let outcome = match outcome {
            Ok(outcome) => outcome,
            Err(error) => {
                session.set_fast_error(error.to_string());
                return FastPageResult {
                    status: FAST_STATUS_READ_ERROR,
                    ..FastPageResult::default()
                };
            }
        };
        if let Some(error) = build_error {
            session.set_fast_error(error);
            return FastPageResult {
                status: FAST_STATUS_INVALID_ARGUMENT,
                ..FastPageResult::default()
            };
        }

        let terminal = outcome.break_value.is_none();
        let record_count = builder.record_count;
        let bytes = builder.finish(terminal);
        let (lease_handle, data_ptr, data_len) = register_page(bytes);
        FastPageResult {
            status: FAST_STATUS_OK,
            terminal: u8::from(terminal),
            record_count,
            lease_handle,
            data_ptr,
            data_len,
            ..FastPageResult::default()
        }
    })) {
        Ok(result) => result,
        Err(_) => {
            session.set_fast_error("panic continuing retained scan transport");
            FastPageResult {
                status: FAST_STATUS_PANIC,
                ..FastPageResult::default()
            }
        }
    }
}

/// Open a retained, snapshot-bound secondary-index cursor. Query kinds are
/// `1=exact`, `2=prefix`, and `3=range`; `reverse` selects descending order.
#[no_mangle]
pub unsafe extern "C" fn prolly_fast_index_cursor_open(
    snapshot_handle: u64,
    query_kind: u32,
    start_ptr: *const u8,
    start_len: usize,
    end_ptr: *const u8,
    end_len: usize,
    has_end: u8,
    reverse: u8,
) -> FastScanOpenResult {
    if index_snapshot_from_handle(snapshot_handle).is_none()
        || start_len > MAX_BOUND_BYTES
        || end_len > MAX_BOUND_BYTES
        || has_end > 1
        || reverse > 1
        || !(1..=3).contains(&query_kind)
        || (query_kind != 3 && has_end != 0)
    {
        return FastScanOpenResult {
            status: FAST_STATUS_INVALID_ARGUMENT,
            ..FastScanOpenResult::default()
        };
    }
    let Some(start) = (unsafe { input_slice(start_ptr, start_len) }) else {
        return FastScanOpenResult {
            status: FAST_STATUS_INVALID_ARGUMENT,
            ..FastScanOpenResult::default()
        };
    };
    let end = if has_end == 0 {
        None
    } else {
        let Some(end) = (unsafe { input_slice(end_ptr, end_len) }) else {
            return FastScanOpenResult {
                status: FAST_STATUS_INVALID_ARGUMENT,
                ..FastScanOpenResult::default()
            };
        };
        Some(end.to_vec())
    };
    let query = match query_kind {
        1 => FastIndexQuery::Exact(start.to_vec()),
        2 => FastIndexQuery::Prefix(start.to_vec()),
        3 => FastIndexQuery::Range(start.to_vec(), end),
        _ => unreachable!("query kind validated above"),
    };
    let handle = register_index_cursor(FastIndexCursorHandle {
        owner_snapshot: snapshot_handle,
        query,
        reverse: reverse != 0,
        state: Mutex::new(FastIndexCursorState {
            cursor: None,
            terminal: false,
        }),
    });
    FastScanOpenResult {
        status: FAST_STATUS_OK,
        scan_handle: handle,
        ..FastScanOpenResult::default()
    }
}

/// Continue a retained secondary-index cursor and return one leased PRPG v2
/// index-match page. The last record carries the stable continuation cursor.
#[no_mangle]
pub unsafe extern "C" fn prolly_fast_index_cursor_next(
    snapshot_handle: u64,
    cursor_handle: u64,
    max_records: u32,
    max_arena_bytes: u64,
) -> FastPageResult {
    let Some(snapshot) = index_snapshot_from_handle(snapshot_handle) else {
        return FastPageResult {
            status: FAST_STATUS_INVALID_ARGUMENT,
            ..FastPageResult::default()
        };
    };
    let Some(cursor) = index_cursor_from_handle(cursor_handle) else {
        return FastPageResult {
            status: FAST_STATUS_INVALID_ARGUMENT,
            ..FastPageResult::default()
        };
    };
    if cursor.owner_snapshot != snapshot_handle
        || max_records == 0
        || max_records > MAX_PAGE_RECORDS
        || max_arena_bytes == 0
        || max_arena_bytes > MAX_PAGE_ARENA_BYTES
    {
        return FastPageResult {
            status: FAST_STATUS_INVALID_ARGUMENT,
            ..FastPageResult::default()
        };
    }

    match catch_unwind(AssertUnwindSafe(|| {
        let Ok(mut state) = cursor.state.lock() else {
            return FastPageResult {
                status: FAST_STATUS_READ_ERROR,
                ..FastPageResult::default()
            };
        };
        if state.terminal {
            let bytes = match PackedPageBuilder::with_limits(
                PackedPageKind::IndexMatch,
                PageLimits {
                    max_records,
                    max_arena_bytes,
                },
            )
            .finish(true)
            {
                Ok(bytes) => bytes,
                Err(_) => {
                    return FastPageResult {
                        status: FAST_STATUS_READ_ERROR,
                        ..FastPageResult::default()
                    };
                }
            };
            let (lease_handle, data_ptr, data_len) = register_page(bytes);
            return FastPageResult {
                status: FAST_STATUS_OK,
                terminal: 1,
                lease_handle,
                data_ptr,
                data_len,
                ..FastPageResult::default()
            };
        }

        let current = state.cursor.clone();
        let page = match (&cursor.query, cursor.reverse) {
            (FastIndexQuery::Exact(term), false) => {
                snapshot.exact_page(term.clone(), current, max_records as u64)
            }
            (FastIndexQuery::Exact(term), true) => {
                snapshot.exact_reverse_page(term.clone(), current, max_records as u64)
            }
            (FastIndexQuery::Prefix(prefix), false) => {
                snapshot.prefix_page(prefix.clone(), current, max_records as u64)
            }
            (FastIndexQuery::Prefix(prefix), true) => {
                snapshot.prefix_reverse_page(prefix.clone(), current, max_records as u64)
            }
            (FastIndexQuery::Range(start, end), false) => {
                snapshot.range_page(start.clone(), end.clone(), current, max_records as u64)
            }
            (FastIndexQuery::Range(start, end), true) => {
                snapshot.range_reverse_page(start.clone(), end.clone(), current, max_records as u64)
            }
        };
        let Ok(page) = page else {
            return FastPageResult {
                status: FAST_STATUS_READ_ERROR,
                ..FastPageResult::default()
            };
        };
        let terminal = page.next_cursor.is_none();
        let record_count = page.matches.len() as u32;
        let mut builder = PackedPageBuilder::with_limits(
            PackedPageKind::IndexMatch,
            PageLimits {
                max_records,
                max_arena_bytes,
            },
        );
        for (position, matched) in page.matches.iter().enumerate() {
            let continuation = (position + 1 == page.matches.len())
                .then_some(page.next_cursor.as_deref())
                .flatten();
            builder = match builder.push_index_match(
                &matched.term,
                &matched.primary_key,
                matched.projection.as_deref(),
                continuation,
            ) {
                Ok(builder) => builder,
                Err(_) => {
                    return FastPageResult {
                        status: FAST_STATUS_INVALID_ARGUMENT,
                        ..FastPageResult::default()
                    };
                }
            };
        }
        let bytes = match builder.finish(terminal) {
            Ok(bytes) => bytes,
            Err(_) => {
                return FastPageResult {
                    status: FAST_STATUS_READ_ERROR,
                    ..FastPageResult::default()
                };
            }
        };
        state.cursor = page.next_cursor;
        state.terminal = terminal;
        let (lease_handle, data_ptr, data_len) = register_page(bytes);
        FastPageResult {
            status: FAST_STATUS_OK,
            terminal: u8::from(terminal),
            record_count,
            lease_handle,
            data_ptr,
            data_len,
            ..FastPageResult::default()
        }
    })) {
        Ok(result) => result,
        Err(_) => FastPageResult {
            status: FAST_STATUS_PANIC,
            ..FastPageResult::default()
        },
    }
}

#[no_mangle]
pub unsafe extern "C" fn prolly_fast_index_cursor_close(cursor_handle: u64) {
    if cursor_handle == 0 {
        return;
    }
    index_cursor_handles()
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .remove(&cursor_handle);
}

/// Execute an exact native proximity search and return one leased PRPG v2
/// neighbor page. Distances retain the Rust API's full `f64` precision.
#[no_mangle]
pub unsafe extern "C" fn prolly_fast_proximity_search(
    map_handle: u64,
    query_ptr: *const f32,
    dimensions: usize,
    k: u32,
    max_arena_bytes: u64,
) -> FastPageResult {
    let Some(map) = proximity_from_handle(map_handle) else {
        return FastPageResult {
            status: FAST_STATUS_INVALID_ARGUMENT,
            ..FastPageResult::default()
        };
    };
    if dimensions == 0
        || dimensions > MAX_BOUND_BYTES / std::mem::size_of::<f32>()
        || query_ptr.is_null()
        || k == 0
        || k > MAX_PAGE_RECORDS
        || max_arena_bytes == 0
        || max_arena_bytes > MAX_PAGE_ARENA_BYTES
    {
        return FastPageResult {
            status: FAST_STATUS_INVALID_ARGUMENT,
            ..FastPageResult::default()
        };
    }
    // SAFETY: the caller supplies an aligned, readable f32 range that remains
    // live for this synchronous call; Rust copies it before returning.
    let query = unsafe { slice::from_raw_parts(query_ptr, dimensions) }.to_vec();
    match catch_unwind(AssertUnwindSafe(|| {
        let result = match map.search(ProximitySearchRequestRecord::exact(query, k as u64)) {
            Ok(result) => result,
            Err(_) => {
                return FastPageResult {
                    status: FAST_STATUS_READ_ERROR,
                    ..FastPageResult::default()
                };
            }
        };
        let record_count = result.neighbors.len() as u32;
        let mut builder = PackedPageBuilder::with_limits(
            PackedPageKind::ProximityNeighbor,
            PageLimits {
                max_records: k,
                max_arena_bytes,
            },
        );
        for (rank, neighbor) in result.neighbors.iter().enumerate() {
            builder = match builder.push_neighbor(
                &neighbor.key,
                neighbor.distance,
                rank as u32,
                Some(&neighbor.value),
                None,
            ) {
                Ok(builder) => builder,
                Err(_) => {
                    return FastPageResult {
                        status: FAST_STATUS_INVALID_ARGUMENT,
                        ..FastPageResult::default()
                    };
                }
            };
        }
        let bytes = match builder.finish(true) {
            Ok(bytes) => bytes,
            Err(_) => {
                return FastPageResult {
                    status: FAST_STATUS_READ_ERROR,
                    ..FastPageResult::default()
                };
            }
        };
        let (lease_handle, data_ptr, data_len) = register_page(bytes);
        FastPageResult {
            status: FAST_STATUS_OK,
            terminal: 1,
            record_count,
            lease_handle,
            data_ptr,
            data_len,
            ..FastPageResult::default()
        }
    })) {
        Ok(result) => result,
        Err(_) => FastPageResult {
            status: FAST_STATUS_PANIC,
            ..FastPageResult::default()
        },
    }
}

#[no_mangle]
pub unsafe extern "C" fn prolly_fast_scan_close(scan_handle: u64) {
    if scan_handle == 0 {
        return;
    }
    scan_handles()
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .remove(&scan_handle);
}

#[no_mangle]
pub unsafe extern "C" fn prolly_fast_page_release(lease_handle: u64) {
    if lease_handle == 0 {
        return;
    }
    let Ok(handle) = ResourceHandle::from_raw(lease_handle) else {
        return;
    };
    if handle.kind() != HandleKind::Page {
        return;
    }
    let _ = resource_handles().close(handle);
}

#[no_mangle]
pub unsafe extern "C" fn prolly_fast_value_release(lease_handle: u64) {
    if lease_handle == 0 {
        return;
    }
    value_handles()
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .remove(&lease_handle);
}

#[cfg(test)]
mod tests {
    use super::super::{default_config, ProllyEngine};
    use super::*;

    struct FastIndexExtractor;

    impl crate::SecondaryIndexExtractorCallback for FastIndexExtractor {
        fn extract(
            &self,
            _primary_key: Vec<u8>,
            _source_value: Vec<u8>,
        ) -> Result<Vec<crate::IndexEntryRecord>, crate::ProllyBindingError> {
            Ok(vec![crate::IndexEntryRecord {
                term: b"red".to_vec(),
                projection: Some(b"Ada".to_vec()),
            }])
        }
    }

    #[test]
    fn indexed_cursor_returns_generation_checked_packed_pages() {
        let engine = Arc::new(ProllyEngine::memory(default_config()).unwrap());
        let registry = Arc::new(crate::BindingIndexRegistry::new());
        registry
            .register(
                b"by_team".to_vec(),
                1,
                "tests.fast-index/v1".to_string(),
                crate::IndexProjectionRecord::Include,
                None,
                Arc::new(FastIndexExtractor),
            )
            .unwrap();
        let map = engine.indexed_map(b"users".to_vec(), registry).unwrap();
        map.ensure_index(b"by_team".to_vec()).unwrap();
        map.put(b"u1".to_vec(), b"source".to_vec()).unwrap();
        map.put(b"u2".to_vec(), b"source-2".to_vec()).unwrap();
        let index = map.snapshot().unwrap().index(b"by_team".to_vec()).unwrap();
        let snapshot_handle = index.fast_handle();
        let opened = unsafe {
            prolly_fast_index_cursor_open(
                snapshot_handle,
                1,
                b"red".as_ptr(),
                3,
                ptr::null(),
                0,
                0,
                0,
            )
        };
        assert_eq!(opened.status, FAST_STATUS_OK);
        let page =
            unsafe { prolly_fast_index_cursor_next(snapshot_handle, opened.scan_handle, 1, 4096) };
        assert_eq!(page.status, FAST_STATUS_OK);
        assert_eq!(page.record_count, 1);
        let packed = PackedPage::parse(
            unsafe { slice::from_raw_parts(page.data_ptr, page.data_len as usize) },
            PageLimits::default(),
        )
        .unwrap();
        let matched = packed.index_match(0).unwrap();
        assert_eq!(matched.term, b"red");
        assert_eq!(matched.primary_key, b"u1");
        assert!(matched.cursor.is_some());
        assert_eq!(page.terminal, 0);
        let second =
            unsafe { prolly_fast_index_cursor_next(snapshot_handle, opened.scan_handle, 1, 4096) };
        assert_eq!(second.status, FAST_STATUS_OK);
        assert_eq!(second.record_count, 1);
        assert_eq!(second.terminal, 1);
        unsafe {
            prolly_fast_page_release(page.lease_handle);
            prolly_fast_page_release(second.lease_handle);
            prolly_fast_index_cursor_close(opened.scan_handle);
        }
        let stale =
            unsafe { prolly_fast_index_cursor_next(snapshot_handle, opened.scan_handle, 16, 4096) };
        assert_eq!(stale.status, FAST_STATUS_INVALID_ARGUMENT);
    }

    #[test]
    fn proximity_search_returns_one_leased_f64_neighbor_page() {
        let engine = Arc::new(ProllyEngine::memory(default_config()).unwrap());
        let map = engine
            .build_proximity_map(
                crate::ProximityConfigRecord::new(2),
                vec![crate::ProximityRecordRecord {
                    key: b"a".to_vec(),
                    vector: vec![0.0, 0.0],
                    value: b"alpha".to_vec(),
                }],
                None,
            )
            .unwrap();
        let query = [0.25f32, 0.25];
        let page = unsafe {
            prolly_fast_proximity_search(map.fast_handle(), query.as_ptr(), query.len(), 1, 4096)
        };
        assert_eq!(page.status, FAST_STATUS_OK);
        assert_eq!(page.record_count, 1);
        let packed = PackedPage::parse(
            unsafe { slice::from_raw_parts(page.data_ptr, page.data_len as usize) },
            PageLimits::default(),
        )
        .unwrap();
        let neighbor = packed.neighbor(0).unwrap();
        assert_eq!(neighbor.key, b"a");
        assert_eq!(neighbor.value, Some(b"alpha".as_slice()));
        assert_eq!(neighbor.distance, 0.125);
        unsafe { prolly_fast_page_release(page.lease_handle) };
    }

    #[test]
    fn opaque_handles_reject_stale_values_and_release_idempotently() {
        let engine = ProllyEngine::memory(default_config()).unwrap();
        let tree = engine.create();
        let tree = engine.put(tree, b"a".to_vec(), b"one".to_vec()).unwrap();
        let tree = engine.put(tree, b"b".to_vec(), Vec::new()).unwrap();
        let session = engine.read_session(tree.clone()).unwrap();
        let handle = session.fast_handle();
        assert_ne!(handle, 0);

        let mut output = [0u8; 16];
        let copied = unsafe {
            prolly_fast_read_session_get_into(
                handle,
                b"a".as_ptr(),
                1,
                output.as_mut_ptr(),
                output.len(),
            )
        };
        assert_eq!(copied.status, FAST_STATUS_OK);
        assert_eq!(copied.found, 1);
        assert_eq!(&output[..copied.written as usize], b"one");

        let value = unsafe { prolly_fast_read_session_get_lease(handle, b"a".as_ptr(), 1) };
        assert_eq!(value.status, FAST_STATUS_OK);
        assert_eq!(value.found, 1);
        assert_ne!(value.lease_handle, 0);
        assert_eq!(
            unsafe { slice::from_raw_parts(value.data_ptr, value.data_len as usize) },
            b"one"
        );
        unsafe {
            prolly_fast_value_release(value.lease_handle);
            prolly_fast_value_release(value.lease_handle);
        }

        let opened = unsafe {
            prolly_fast_read_session_scan_open(handle, ptr::null(), 0, ptr::null(), 0, 0)
        };
        assert_eq!(opened.status, FAST_STATUS_OK);
        let excessive_page = unsafe {
            prolly_fast_read_session_scan_page(
                handle,
                ptr::null(),
                0,
                ptr::null(),
                0,
                0,
                ptr::null(),
                0,
                0,
                u32::MAX,
                1024,
            )
        };
        assert_eq!(excessive_page.status, FAST_STATUS_INVALID_ARGUMENT);

        let other_session = engine.read_session(tree).unwrap();
        let wrong_owner = unsafe {
            prolly_fast_read_session_scan_next(
                other_session.fast_handle(),
                opened.scan_handle,
                1,
                1024,
            )
        };
        assert_eq!(wrong_owner.status, FAST_STATUS_INVALID_ARGUMENT);

        let page =
            unsafe { prolly_fast_read_session_scan_next(handle, opened.scan_handle, 1, 1024) };
        assert_eq!(page.status, FAST_STATUS_OK);
        assert_eq!(page.record_count, 1);
        unsafe {
            prolly_fast_page_release(page.lease_handle);
            prolly_fast_page_release(page.lease_handle);
            prolly_fast_scan_close(opened.scan_handle);
            prolly_fast_scan_close(opened.scan_handle);
        }
        let closed_scan =
            unsafe { prolly_fast_read_session_scan_next(handle, opened.scan_handle, 1, 1024) };
        assert_eq!(closed_scan.status, FAST_STATUS_INVALID_ARGUMENT);

        let malformed = [0u8, 0, 0, 0, 2, 0, 0, 0, 1, 0, 0, 0];
        let malformed_result = unsafe {
            prolly_fast_read_session_get_many_page(handle, malformed.as_ptr(), malformed.len(), 2)
        };
        assert_eq!(malformed_result.status, FAST_STATUS_INVALID_ARGUMENT);

        let orphaned_scan = unsafe {
            prolly_fast_read_session_scan_open(handle, ptr::null(), 0, ptr::null(), 0, 0)
        };
        assert_eq!(orphaned_scan.status, FAST_STATUS_OK);
        drop(session);
        assert!(scan_from_handle(orphaned_scan.scan_handle).is_none());
        let stale = unsafe {
            prolly_fast_read_session_get_into(
                handle,
                b"a".as_ptr(),
                1,
                output.as_mut_ptr(),
                output.len(),
            )
        };
        assert_eq!(stale.status, FAST_STATUS_INVALID_ARGUMENT);

        let arbitrary = unsafe {
            prolly_fast_read_session_get_into(
                u64::MAX,
                b"a".as_ptr(),
                1,
                output.as_mut_ptr(),
                output.len(),
            )
        };
        assert_eq!(arbitrary.status, FAST_STATUS_INVALID_ARGUMENT);
    }
}
