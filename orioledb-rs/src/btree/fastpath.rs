// B-tree fast-path lookup (skip direct page search on hot pages).
//
// Ported from `include/btree/fastpath.h` and `src/btree/fastpath.c`.
//
// Copyright (c) 2021-2026, Oriole DB Inc.
// Copyright (c) 2025-2026, Supabase Inc.

use super::btree::BTreeKeyType;

/// Result of a fast-path search attempt.
///
/// Mirrors `OBTreeFastPathFindResult` in `include/btree/fastpath.h`.
#[repr(i32)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum OBTreeFastPathFindResult {
    /// Fast-path found the target item.
    Found = 0,
    /// Fast-path determined the item is not present.
    NotFound = 1,
    /// Fast-path could not determine — caller must search normally.
    Inconclusive = 2,
}

extern "C" {
    /// Hint the fast-path that a downlink at `context` may be skippable.
    pub fn can_fastpath_find_downlink(
        context: *mut std::ffi::c_void, // OBTreeFindPageContext*
        page: *mut u8,
    );

    /// Attempt a fast-path search for `key` in a chunk.
    pub fn fastpath_find_chunk(
        page_ptr: *mut u8,
        key: *mut std::ffi::c_void,
        key_type: BTreeKeyType,
        desc: *mut std::ffi::c_void, // BTreeDescr*
        locator: *mut std::ffi::c_void, // BTreePageItemLocator*
    ) -> OBTreeFastPathFindResult;

    /// Attempt a fast-path search for a downlink in a chunk.
    pub fn fastpath_find_downlink(
        page_ptr: *mut u8,
        key: *mut std::ffi::c_void,
        key_type: BTreeKeyType,
        desc: *mut std::ffi::c_void,
        locator: *mut std::ffi::c_void,
    ) -> OBTreeFastPathFindResult;
}
