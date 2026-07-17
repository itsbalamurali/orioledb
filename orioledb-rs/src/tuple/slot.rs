/*-------------------------------------------------------------------------
 *
 * slot.rs
 * 		Declarations and routines for orioledb tuple slot implementation.
 *
 * Copyright (c) 2021-2026, Oriole DB Inc.
 * Copyright (c) 2025-2026, Supabase Inc.
 *
 * IDENTIFICATION
 *	  contrib/orioledb/src/tuple/slot.rs
 *
 *-------------------------------------------------------------------------
 */

use std::ffi::{c_char, c_int, c_void};
use pgrx::pg_sys::{
    Datum, TupleTableSlot, TupleTableSlotOps, TupleDesc, CommitSeqNo,
    StringInfo, Tuplesortstate, Bitmapset, ItemPointer, Oid, ExprState, bytea,
};
use crate::tuple::format::{OTuple, OTupleReaderState, BridgeData};
use crate::tuple::sort::{OIndexDescr, BTreeLocationHint};
use crate::tuple::toast::OTableDescr;

#[repr(C)]
pub struct OTableSlot {
    pub base: TupleTableSlot,
    pub data: *mut c_char,
    pub to_toast: *mut c_char,
    pub vfree: *mut bool,
    pub detoasted: *mut Datum,
    pub tuple: OTuple,
    pub descr: *mut OTableDescr,
    pub rowid: *mut bytea,
    pub csn: CommitSeqNo,
    pub ixnum: c_int,
    pub leafTuple: bool,
    pub bridgeChanged: bool,
    pub version: u32,
    pub state: OTupleReaderState,
    pub hint: BTreeLocationHint,
    pub bridge_ctid: pgrx::pg_sys::ItemPointerData,
}

extern "C" {
    pub static TTSOpsOrioleDB: TupleTableSlotOps;

    pub fn tts_orioledb_detoast(slot: *mut TupleTableSlot);
    pub fn tts_orioledb_store_tuple(
        slot: *mut TupleTableSlot,
        tuple: OTuple,
        descr: *mut OTableDescr,
        csn: CommitSeqNo,
        ixnum: c_int,
        shouldfree: bool,
        hint: *mut BTreeLocationHint,
    );
    pub fn tts_orioledb_store_non_leaf_tuple(
        slot: *mut TupleTableSlot,
        tuple: OTuple,
        descr: *mut OTableDescr,
        csn: CommitSeqNo,
        ixnum: c_int,
        shouldfree: bool,
        hint: *mut BTreeLocationHint,
    );
    pub fn tts_orioledb_make_secondary_tuple(
        slot: *mut TupleTableSlot,
        idx: *mut OIndexDescr,
        leaf: bool,
    ) -> OTuple;
    pub fn tts_orioledb_fill_key_bound(
        slot: *mut TupleTableSlot,
        idx: *mut OIndexDescr,
        bound: *mut std::ffi::c_void, // OBTreeKeyBound
    );
    pub fn tss_orioledb_print_idx_key(
        slot: *mut TupleTableSlot,
        id: *mut OIndexDescr,
    ) -> *mut c_char;
    pub fn appendStringInfoIndexKey(
        str_: StringInfo,
        slot: *mut TupleTableSlot,
        id: *mut OIndexDescr,
    );
    pub fn tts_orioledb_toast(slot: *mut TupleTableSlot, descr: *mut OTableDescr);
    pub fn tts_orioledb_form_tuple(slot: *mut TupleTableSlot, descr: *mut OTableDescr) -> OTuple;
    pub fn tts_orioledb_form_orphan_tuple(slot: *mut TupleTableSlot, descr: *mut OTableDescr) -> OTuple;
    pub fn tts_orioledb_insert_toast_values(
        slot: *mut TupleTableSlot,
        descr: *mut OTableDescr,
        oxid: u32,
        csn: CommitSeqNo,
    ) -> bool;
    pub fn tts_orioledb_toast_sort_add(
        slot: *mut TupleTableSlot,
        descr: *mut OTableDescr,
        sortstate: *mut Tuplesortstate,
    );
    pub fn tts_orioledb_remove_toast_values(
        slot: *mut TupleTableSlot,
        descr: *mut OTableDescr,
        oxid: u32,
        csn: CommitSeqNo,
    ) -> bool;
    pub fn tts_orioledb_update_toast_values(
        oldSlot: *mut TupleTableSlot,
        newSlot: *mut TupleTableSlot,
        descr: *mut OTableDescr,
        oxid: u32,
        csn: CommitSeqNo,
    ) -> bool;
    pub fn tts_orioledb_modified(
        oldSlot: *mut TupleTableSlot,
        newSlot: *mut TupleTableSlot,
        attrs: *mut Bitmapset,
    ) -> bool;
    pub fn tts_orioledb_set_ctid(slot: *mut TupleTableSlot, iptr: ItemPointer);
    pub fn o_get_tbl_att(
        slot: *mut TupleTableSlot,
        attnum: c_int,
        primaryIsCtid: bool,
        isnull: *mut bool,
        typid: *mut Oid,
        decompress: bool,
    ) -> Datum;
    pub fn o_get_idx_expr_att(
        slot: *mut TupleTableSlot,
        idx: *mut OIndexDescr,
        exp_state: *mut ExprState,
        isnull: *mut bool,
    ) -> Datum;
}
