// B-tree iterator — forward and backward tuple iteration.
//
// Ported from `include/btree/iterator.h` and `src/btree/iterator.c`.
//
// Copyright (c) 2021-2026, Oriole DB Inc.
// Copyright (c) 2025-2026, Supabase Inc.

use pgrx::pg_sys::{CommitSeqNo, MemoryContext};
use super::btree::{BTreeDescr, BTreeKeyType, OTuple};

/// Opaque B-tree iterator handle (allocated by the C layer).
pub enum BTreeIterator {}

extern "C" {
    /// Find a single tuple matching `key` of type `key_type`.
    ///
    /// Returns `OTuple::NULL` when not found.
    pub fn o_btree_find_tuple_by_key(
        desc: *mut BTreeDescr,
        key: *mut std::ffi::c_void,
        key_type: BTreeKeyType,
        csn: CommitSeqNo,
        primary_tuple: *mut OTuple,
        mctx: MemoryContext,
    ) -> OTuple;

    /// Start an iteration from `key`.
    ///
    /// Returns the first matching tuple; continue with `o_btree_find_tuples_continue`.
    pub fn o_btree_find_tuples_start(
        desc: *mut BTreeDescr,
        key: *mut std::ffi::c_void,
        key_type: BTreeKeyType,
        csn: CommitSeqNo,
        it: *mut *mut BTreeIterator,
        mctx: MemoryContext,
    ) -> OTuple;

    /// Advance the iterator returned by `o_btree_find_tuples_start`.
    pub fn o_btree_find_tuples_continue(
        it: *mut BTreeIterator,
        end_key: *mut std::ffi::c_void,
        end_key_type: BTreeKeyType,
        csn: CommitSeqNo,
    ) -> OTuple;

    /// Release the iterator created by `o_btree_find_tuples_start`.
    pub fn o_btree_find_tuples_finish(it: *mut BTreeIterator);

    /// Create a re-usable B-tree iterator starting at `key`.
    pub fn o_btree_iterator_create(
        desc: *mut BTreeDescr,
        key: *mut std::ffi::c_void,
        key_type: BTreeKeyType,
        csn: CommitSeqNo,
        scan_dir: i32,
    ) -> *mut BTreeIterator;

    /// Advance the iterator to the next key that satisfies `key` of `key_type`.
    pub fn o_btree_iterator_advance(
        it: *mut BTreeIterator,
        key: *mut std::ffi::c_void,
        key_type: BTreeKeyType,
    );

    /// Set a tuple context on the iterator (for memory lifetime management).
    pub fn o_btree_iterator_set_tuple_ctx(it: *mut BTreeIterator, mctx: MemoryContext);

    /// Install a callback to be called for each tuple returned by the iterator.
    pub fn o_btree_iterator_set_callback(
        it: *mut BTreeIterator,
        callback: Option<unsafe extern "C" fn(*mut std::ffi::c_void, OTuple) -> bool>,
        arg: *mut std::ffi::c_void,
    );

    /// Fetch the next tuple from the iterator.
    pub fn o_btree_iterator_fetch(
        it: *mut BTreeIterator,
        csn: *mut CommitSeqNo,
        end: *mut std::ffi::c_void,
        end_key_type: BTreeKeyType,
        scan_dir: i32,
        tuple_mctx: MemoryContext,
    ) -> OTuple;

    /// Iterate without visibility filtering (raw scan for maintenance operations).
    pub fn btree_iterate_raw(
        it: *mut BTreeIterator,
        end: *mut std::ffi::c_void,
        end_key_type: BTreeKeyType,
        deleted: *mut bool,
        csn: *mut CommitSeqNo,
        xact_info: *mut u64,
    ) -> OTuple;

    /// Iterate all tuples including deleted ones (for checkpoint/compaction).
    pub fn btree_iterate_all(
        it: *mut BTreeIterator,
        end: *mut std::ffi::c_void,
        end_key_type: BTreeKeyType,
        deleted: *mut bool,
        csn: *mut CommitSeqNo,
        xact_info: *mut u64,
    ) -> OTuple;

    /// Free the iterator and release all held resources.
    pub fn btree_iterator_free(it: *mut BTreeIterator);

    /// Find a tuple by key, invoking `cb` for each candidate.
    pub fn o_btree_find_tuple_by_key_cb(
        desc: *mut BTreeDescr,
        key: *mut std::ffi::c_void,
        key_type: BTreeKeyType,
        csn: CommitSeqNo,
        cb: Option<unsafe extern "C" fn(*mut std::ffi::c_void, OTuple, CommitSeqNo) -> bool>,
        cb_arg: *mut std::ffi::c_void,
        mctx: MemoryContext,
    ) -> OTuple;

    /// Find the visible version of a tuple on `p` at the given `undo_loc`.
    pub fn o_find_tuple_version(
        desc: *mut BTreeDescr,
        p: *mut u8,
        locator: *mut std::ffi::c_void,
        undo_loc: u64,
        csn: CommitSeqNo,
        mctx: MemoryContext,
        xact_info: *mut u64,
    ) -> OTuple;
}
