// B-tree insert operations.
//
// Ported from `include/btree/insert.h` and `src/btree/insert.c`.
//
// Copyright (c) 2021-2026, Oriole DB Inc.
// Copyright (c) 2025-2026, Supabase Inc.

use super::btree::OTuple;
use crate::utils::page_pool::OInMemoryBlkno;

extern "C" {
    /// Fix an in-progress split on `descr` and unlock the pages.
    pub fn o_btree_split_fix_and_unlock(
        descr: *mut std::ffi::c_void, // BTreeDescr*
        right_blkno: OInMemoryBlkno,
        context: *mut std::ffi::c_void,
    );

    /// Fix an in-progress split for the right page only, then unlock.
    pub fn o_btree_split_fix_for_right_page_and_unlock(
        desc: *mut std::ffi::c_void,
        right_blkno: OInMemoryBlkno,
        context: *mut std::ffi::c_void,
    );

    /// Insert `tuple` into the leaf page identified by `context`.
    pub fn o_btree_insert_tuple_to_leaf(
        context: *mut std::ffi::c_void, // OBTreeFindPageContext*
        tuple: OTuple,
        insert_kind: i32,
        undo_location: u64,
        callback_info: *mut std::ffi::c_void,
    );

    /// Return `true` when the split of `left_blkno` is still in progress.
    pub fn o_btree_split_is_incomplete(
        left_blkno: OInMemoryBlkno,
        right_blkno: OInMemoryBlkno,
    ) -> bool;

    /// Insert the next item during a multi-insert batch.
    ///
    /// Returns the number of items consumed.
    pub fn o_btree_multi_insert_item(
        ctx: *mut std::ffi::c_void,
        items: *mut std::ffi::c_void,
        nitems: i32,
    ) -> i32;
}
