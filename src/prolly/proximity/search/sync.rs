//! Synchronous native execution is implemented by `ProximityMap::search`.
//!
//! This module is intentionally a boundary for the async executor and derived
//! backends; authoritative traversal state lives in `engine`.
