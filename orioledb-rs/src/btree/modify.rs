// B-tree modify operations (insert, update, delete, lock).
//
// Ported from `include/btree/modify.h` and `src/btree/modify.c`.
//
// Copyright (c) 2021-2026, Oriole DB Inc.
// Copyright (c) 2025-2026, Supabase Inc.

use super::btree::{BTreeDescr, BTreeKeyType, OBTreeModifyResult, OTuple};
use crate::transam::oxid::OXid;

extern "C" {
    /// Null callback info (no modify callbacks installed).
    pub static mut nullCallbackInfo: std::ffi::c_void;

    /// Insert `tuple` into `desc`, bypassing MVCC (used for system trees).
    ///
    /// Returns `true` on success.
    pub fn o_btree_autonomous_insert(desc: *mut BTreeDescr, tuple: OTuple) -> bool;

    /// Delete the tuple matching `key` from `desc` autonomously.
    pub fn o_btree_autonomous_delete(
        desc: *mut BTreeDescr,
        key: OTuple,
        key_type: BTreeKeyType,
        oxid: OXid,
    ) -> bool;

    /// The main B-tree modify entry point (insert / update / delete / lock).
    pub fn o_btree_modify(
        desc: *mut BTreeDescr,
        modify_type: i32,
        tuple: OTuple,
        tuple_key_type: BTreeKeyType,
        key: *mut std::ffi::c_void,
        key_type: BTreeKeyType,
        oxid: OXid,
        csn: crate::CommitSeqNo,
        lock_mode: i32,
        callback_info: *mut std::ffi::c_void,
    ) -> OBTreeModifyResult;

    /// Delete a tuple that has been moved to another partition.
    pub fn o_btree_delete_moved_partitions(
        desc: *mut BTreeDescr,
        key: OTuple,
        key_type: BTreeKeyType,
        oxid: OXid,
        csn: crate::CommitSeqNo,
    ) -> OBTreeModifyResult;

    /// Delete a tuple whose primary key has changed.
    pub fn o_btree_delete_pk_changed(
        desc: *mut BTreeDescr,
        key: OTuple,
        key_type: BTreeKeyType,
        oxid: OXid,
        csn: crate::CommitSeqNo,
    ) -> OBTreeModifyResult;

    /// Insert `tuple` while checking for unique-constraint violations.
    pub fn o_btree_insert_unique(
        desc: *mut BTreeDescr,
        tuple: OTuple,
        key: *mut std::ffi::c_void,
        key_type: BTreeKeyType,
        oxid: OXid,
        csn: crate::CommitSeqNo,
        callback_info: *mut std::ffi::c_void,
    ) -> OBTreeModifyResult;
}
