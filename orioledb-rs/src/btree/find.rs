// B-tree page-find operations (traverse to a leaf given a key).
//
// Ported from `include/btree/find.h` and `src/btree/find.c`.
//
// Copyright (c) 2021-2026, Oriole DB Inc.
// Copyright (c) 2025-2026, Supabase Inc.

use super::btree::{BTreeDescr, BTreeKeyType, OTuple};
use crate::utils::page_pool::OInMemoryBlkno;

/// Result of a `find_page` call.
///
/// Mirrors `OFindPageResult` in `include/btree/find.h`.
#[repr(i32)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum OFindPageResult {
    Found = 0,
    NotFound = 1,
    Concurrent = 2,
}

extern "C" {
    /// Binary-search `p` for `key`, returning the locator for the match (or insert position).
    pub fn btree_page_search(
        desc: *mut BTreeDescr,
        p: *mut u8,
        key: *mut std::ffi::c_void,
        key_type: BTreeKeyType,
        locator: *mut std::ffi::c_void, // BTreePageItemLocator*
        hikey_locator: *mut std::ffi::c_void,
    ) -> bool;

    /// Initialise a find-page context for subsequent `find_page` calls.
    pub fn init_page_find_context(
        context: *mut std::ffi::c_void,
        desc: *mut BTreeDescr,
        csn: crate::CommitSeqNo,
        flags: u32,
    );

    /// Descend from the root to the leaf that would contain `key`.
    pub fn find_page(
        context: *mut std::ffi::c_void,
        key: *mut std::ffi::c_void,
        key_type: BTreeKeyType,
        target_level: u32,
    ) -> OFindPageResult;

    /// Re-find the page after a concurrent modification.
    pub fn refind_page(
        context: *mut std::ffi::c_void,
        key: *mut std::ffi::c_void,
        key_type: BTreeKeyType,
        target_level: u32,
        blkno: OInMemoryBlkno,
        page_change_count: u32,
    ) -> OFindPageResult;

    pub fn find_right_page(
        context: *mut std::ffi::c_void,
        hikey: *mut std::ffi::c_void, // OFixedKey*
    ) -> bool;

    pub fn find_left_page(
        context: *mut std::ffi::c_void,
        hikey: *mut std::ffi::c_void,
    ) -> bool;

    pub fn btree_find_context_lokey(context: *mut std::ffi::c_void) -> OTuple;
    pub fn btree_find_context_has_lokey(context: *mut std::ffi::c_void) -> bool;

    /// Switch a find-page context from modify mode to read mode.
    pub fn btree_find_context_from_modify_to_read(
        context: *mut std::ffi::c_void,
        key: *mut std::ffi::c_void,
        key_type: BTreeKeyType,
    );
}
