/*-------------------------------------------------------------------------
 *
 * operations.rs
 *		Declarations and FFI signatures of table-level operations in OrioleDB.
 *
 * Copyright (c) 2021-2026, Oriole DB Inc.
 * Copyright (c) 2025-2026, Supabase Inc.
 *
 *-------------------------------------------------------------------------
 */

use pgrx::pg_sys::{self, Relation, CommandId, IndexUniqueCheck, ItemPointer};
use crate::{OXid, CommitSeqNo};
use crate::tableam::descr::{OTableDescr, OIndexDescr};
use crate::tableam::key_range::OBTreeKeyBound;

#[repr(C)]
pub struct OTableModifyResult {
    pub success: bool,
    pub action: std::ffi::c_int, // BTreeOperationType
    pub failedIxNum: std::ffi::c_int, // OIndexNumber
    pub oldTuple: *mut pg_sys::TupleTableSlot,
}

#[repr(C)]
pub struct InsertOnConflictCallbackArg {
    pub desc: *mut OTableDescr,
    pub scanSlot: *mut pg_sys::TupleTableSlot,
    pub newSlot: *mut std::ffi::c_void, // OTableSlot
    pub conflictOxid: OXid,
    pub oxid: OXid,
    pub csn: CommitSeqNo,
    pub tupUndoLocation: u64, // UndoLocation
    pub conflictIxNum: std::ffi::c_int,
    pub copyPrimaryOxid: bool,
    pub lockMode: std::ffi::c_int, // RowLockMode
}

#[repr(C)]
pub struct OModifyCallbackArg {
    pub scanSlot: *mut pg_sys::TupleTableSlot,
    pub tmpSlot: *mut pg_sys::TupleTableSlot,
    pub descr: *mut OTableDescr,
    pub newSlot: *mut std::ffi::c_void,
    pub oxid: OXid,
    pub csn: CommitSeqNo,
    pub tup_undo_location: u64,
    pub deleted: std::ffi::c_int, // BTreeLeafTupleDeletedStatus
    pub modifyCid: CommandId,
    pub tupleCid: CommandId,
    pub modified: bool,
    pub selfModified: bool,
    pub changingPart: bool,
    pub keyAttrs: *mut pg_sys::Bitmapset,
    pub options: std::ffi::c_int,
}

#[repr(C)]
pub struct OLockCallbackArg {
    pub rel: Relation,
    pub scanSlot: *mut pg_sys::TupleTableSlot,
    pub descr: *mut OTableDescr,
    pub oxid: OXid,
    pub csn: CommitSeqNo,
    pub waitPolicy: pg_sys::LockWaitPolicy,
    pub tupUndoLocation: u64,
    pub deleted: std::ffi::c_int,
    pub modifyCid: CommandId,
    pub tupleCid: CommandId,
    pub wouldBlock: bool,
    pub modified: bool,
    pub selfModified: bool,
}

#[no_mangle]
pub unsafe extern "C" fn o_tbl_insert(
    descr: *mut OTableDescr,
    relation: Relation,
    slot: *mut pg_sys::TupleTableSlot,
    oxid: OXid,
    csn: CommitSeqNo,
) -> *mut pg_sys::TupleTableSlot {
    extern "C" {
        fn o_tbl_insert_c(
            descr: *mut OTableDescr,
            relation: Relation,
            slot: *mut pg_sys::TupleTableSlot,
            oxid: OXid,
            csn: CommitSeqNo,
        ) -> *mut pg_sys::TupleTableSlot;
    }
    o_tbl_insert_c(descr, relation, slot, oxid, csn)
}

#[no_mangle]
pub unsafe extern "C" fn o_tbl_multi_insert(
    descr: *mut OTableDescr,
    relation: Relation,
    slots: *mut *mut pg_sys::TupleTableSlot,
    ntuples: std::ffi::c_int,
    oxid: OXid,
    csn: CommitSeqNo,
) {
    extern "C" {
        fn o_tbl_multi_insert_c(
            descr: *mut OTableDescr,
            relation: Relation,
            slots: *mut *mut pg_sys::TupleTableSlot,
            ntuples: std::ffi::c_int,
            oxid: OXid,
            csn: CommitSeqNo,
        );
    }
    o_tbl_multi_insert_c(descr, relation, slots, ntuples, oxid, csn);
}

#[no_mangle]
pub unsafe extern "C" fn o_tbl_insert_with_arbiter(
    rel: Relation,
    descr: *mut OTableDescr,
    slot: *mut pg_sys::TupleTableSlot,
    arbiterIndexes: *mut pg_sys::List,
    cid: CommandId,
    lockmode: pg_sys::LockTupleMode,
    lockedSlot: *mut pg_sys::TupleTableSlot,
    estate: *mut pg_sys::EState,
    resultRelInfo: *mut pg_sys::ResultRelInfo,
) -> *mut pg_sys::TupleTableSlot {
    extern "C" {
        fn o_tbl_insert_with_arbiter_c(
            rel: Relation,
            descr: *mut OTableDescr,
            slot: *mut pg_sys::TupleTableSlot,
            arbiterIndexes: *mut pg_sys::List,
            cid: CommandId,
            lockmode: pg_sys::LockTupleMode,
            lockedSlot: *mut pg_sys::TupleTableSlot,
            estate: *mut pg_sys::EState,
            resultRelInfo: *mut pg_sys::ResultRelInfo,
        ) -> *mut pg_sys::TupleTableSlot;
    }
    o_tbl_insert_with_arbiter_c(rel, descr, slot, arbiterIndexes, cid, lockmode, lockedSlot, estate, resultRelInfo)
}

#[no_mangle]
pub unsafe extern "C" fn o_tbl_index_insert(
    descr: *mut OTableDescr,
    id: *mut OIndexDescr,
    own_tup: *mut pg_sys::OTuple,
    slot: *mut pg_sys::TupleTableSlot,
    oxid: OXid,
    csn: CommitSeqNo,
    callbackInfo: *mut std::ffi::c_void, // BTreeModifyCallbackInfo
    checkUnique: IndexUniqueCheck,
) -> OTableModifyResult {
    // Note: in operations.h, o_tbl_index_insert returns OBTreeModifyResult.
    // We mock/forward via C interface.
    extern "C" {
        fn o_tbl_index_insert_c(
            descr: *mut OTableDescr,
            id: *mut OIndexDescr,
            own_tup: *mut pg_sys::OTuple,
            slot: *mut pg_sys::TupleTableSlot,
            oxid: OXid,
            csn: CommitSeqNo,
            callbackInfo: *mut std::ffi::c_void,
            checkUnique: IndexUniqueCheck,
        ) -> OTableModifyResult;
    }
    o_tbl_index_insert_c(descr, id, own_tup, slot, oxid, csn, callbackInfo, checkUnique)
}

#[no_mangle]
pub unsafe extern "C" fn o_tbl_lock(
    descr: *mut OTableDescr,
    pkey: *mut OBTreeKeyBound,
    mode: pg_sys::LockTupleMode,
    oxid: OXid,
    larg: *mut OLockCallbackArg,
    hint: *mut pg_sys::BTreeLocationHint,
) -> OTableModifyResult {
    extern "C" {
        fn o_tbl_lock_c(
            descr: *mut OTableDescr,
            pkey: *mut OBTreeKeyBound,
            mode: pg_sys::LockTupleMode,
            oxid: OXid,
            larg: *mut OLockCallbackArg,
            hint: *mut pg_sys::BTreeLocationHint,
        ) -> OTableModifyResult;
    }
    o_tbl_lock_c(descr, pkey, mode, oxid, larg, hint)
}

#[no_mangle]
pub unsafe extern "C" fn o_tbl_update(
    descr: *mut OTableDescr,
    slot: *mut pg_sys::TupleTableSlot,
    oldPkey: *mut OBTreeKeyBound,
    rel: Relation,
    oxid: OXid,
    csn: CommitSeqNo,
    hint: *mut pg_sys::BTreeLocationHint,
    arg: *mut OModifyCallbackArg,
    bridge_ctid: ItemPointer,
) -> OTableModifyResult {
    extern "C" {
        fn o_tbl_update_c(
            descr: *mut OTableDescr,
            slot: *mut pg_sys::TupleTableSlot,
            oldPkey: *mut OBTreeKeyBound,
            rel: Relation,
            oxid: OXid,
            csn: CommitSeqNo,
            hint: *mut pg_sys::BTreeLocationHint,
            arg: *mut OModifyCallbackArg,
            bridge_ctid: ItemPointer,
        ) -> OTableModifyResult;
    }
    o_tbl_update_c(descr, slot, oldPkey, rel, oxid, csn, hint, arg, bridge_ctid)
}

#[no_mangle]
pub unsafe extern "C" fn o_update_secondary_index(
    id: *mut OIndexDescr,
    ix_num: std::ffi::c_int,
    new_valid: bool,
    old_valid: bool,
    newSlot: *mut pg_sys::TupleTableSlot,
    new_ix_tup: pg_sys::OTuple,
    oldSlot: *mut pg_sys::TupleTableSlot,
    oxid: OXid,
    csn: CommitSeqNo,
    checkUnique: IndexUniqueCheck,
) -> OTableModifyResult {
    extern "C" {
        fn o_update_secondary_index_c(
            id: *mut OIndexDescr,
            ix_num: std::ffi::c_int,
            new_valid: bool,
            old_valid: bool,
            newSlot: *mut pg_sys::TupleTableSlot,
            new_ix_tup: pg_sys::OTuple,
            oldSlot: *mut pg_sys::TupleTableSlot,
            oxid: OXid,
            csn: CommitSeqNo,
            checkUnique: IndexUniqueCheck,
        ) -> OTableModifyResult;
    }
    o_update_secondary_index_c(id, ix_num, new_valid, old_valid, newSlot, new_ix_tup, oldSlot, oxid, csn, checkUnique)
}

#[no_mangle]
pub unsafe extern "C" fn o_tbl_delete(
    rel: Relation,
    descr: *mut OTableDescr,
    primary_key: *mut OBTreeKeyBound,
    oxid: OXid,
    csn: CommitSeqNo,
    hint: *mut pg_sys::BTreeLocationHint,
    arg: *mut OModifyCallbackArg,
) -> OTableModifyResult {
    extern "C" {
        fn o_tbl_delete_c(
            rel: Relation,
            descr: *mut OTableDescr,
            primary_key: *mut OBTreeKeyBound,
            oxid: OXid,
            csn: CommitSeqNo,
            hint: *mut pg_sys::BTreeLocationHint,
            arg: *mut OModifyCallbackArg,
        ) -> OTableModifyResult;
    }
    o_tbl_delete_c(rel, descr, primary_key, oxid, csn, hint, arg)
}

#[no_mangle]
pub unsafe extern "C" fn o_tbl_index_delete(
    id: *mut OIndexDescr,
    ix_num: std::ffi::c_int,
    slot: *mut pg_sys::TupleTableSlot,
    oxid: OXid,
    csn: CommitSeqNo,
) -> OTableModifyResult {
    extern "C" {
        fn o_tbl_index_delete_c(
            id: *mut OIndexDescr,
            ix_num: std::ffi::c_int,
            slot: *mut pg_sys::TupleTableSlot,
            oxid: OXid,
            csn: CommitSeqNo,
        ) -> OTableModifyResult;
    }
    o_tbl_index_delete_c(id, ix_num, slot, oxid, csn)
}

#[no_mangle]
pub unsafe extern "C" fn o_check_tbl_update_mres(
    mres: OTableModifyResult,
    descr: *mut OTableDescr,
    rel: Relation,
    slot: *mut pg_sys::TupleTableSlot,
) {
    extern "C" {
        fn o_check_tbl_update_mres_c(
            mres: OTableModifyResult,
            descr: *mut OTableDescr,
            rel: Relation,
            slot: *mut pg_sys::TupleTableSlot,
        );
    }
    o_check_tbl_update_mres_c(mres, descr, rel, slot);
}

#[no_mangle]
pub unsafe extern "C" fn o_check_tbl_delete_mres(
    mres: OTableModifyResult,
    descr: *mut OTableDescr,
    rel: Relation,
) {
    extern "C" {
        fn o_check_tbl_delete_mres_c(
            mres: OTableModifyResult,
            descr: *mut OTableDescr,
            rel: Relation,
        );
    }
    o_check_tbl_delete_mres_c(mres, descr, rel);
}

#[no_mangle]
pub unsafe extern "C" fn set_pending_sk_marker(descr: *mut OTableDescr, pkUndoLoc: u64) {
    extern "C" {
        fn set_pending_sk_marker_c(descr: *mut OTableDescr, pkUndoLoc: u64);
    }
    set_pending_sk_marker_c(descr, pkUndoLoc);
}

#[no_mangle]
pub unsafe extern "C" fn fire_sk_modify_pending_stopevent(descr: *mut OTableDescr) {
    extern "C" {
        fn fire_sk_modify_pending_stopevent_c(descr: *mut OTableDescr);
    }
    fire_sk_modify_pending_stopevent_c(descr);
}

#[no_mangle]
pub unsafe extern "C" fn clear_pending_sk_marker() {
    extern "C" {
        fn clear_pending_sk_marker_c();
    }
    clear_pending_sk_marker_c();
}

#[no_mangle]
pub unsafe extern "C" fn o_is_index_predicate_satisfied(
    idx: *mut OIndexDescr,
    slot: *mut pg_sys::TupleTableSlot,
    econtext: *mut pg_sys::ExprContext,
) -> bool {
    extern "C" {
        fn o_is_index_predicate_satisfied_c(
            idx: *mut OIndexDescr,
            slot: *mut pg_sys::TupleTableSlot,
            econtext: *mut pg_sys::ExprContext,
        ) -> bool;
    }
    o_is_index_predicate_satisfied_c(idx, slot, econtext)
}

#[no_mangle]
pub unsafe extern "C" fn o_truncate_table(oids: pg_sys::ORelOids, missingOK: bool) {
    extern "C" {
        fn o_truncate_table_c(oids: pg_sys::ORelOids, missingOK: bool);
    }
    o_truncate_table_c(oids, missingOK);
}

#[no_mangle]
pub unsafe extern "C" fn o_apply_new_bridge_index_ctid(
    descr: *mut OTableDescr,
    relation: Relation,
    slot: *mut pg_sys::TupleTableSlot,
    csn: CommitSeqNo,
    increment_bridge_ctid: bool,
) {
    extern "C" {
        fn o_apply_new_bridge_index_ctid_c(
            descr: *mut OTableDescr,
            relation: Relation,
            slot: *mut pg_sys::TupleTableSlot,
            csn: CommitSeqNo,
            increment_bridge_ctid: bool,
        );
    }
    o_apply_new_bridge_index_ctid_c(descr, relation, slot, csn, increment_bridge_ctid);
}
