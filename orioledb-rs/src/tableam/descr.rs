//! descr.rs
//!
//! Copyright (c) 2021-2026, Oriole DB Inc.
//! Copyright (c) 2025-2026, Supabase Inc.
//!
//! IDENTIFICATION
//!   contrib/orioledb/orioledb-rs/src/tableam/descr.rs

use pgrx::pg_sys::{self, Oid};
use crate::OXid;
use crate::tableam::key_range::{OIndexField, INDEX_MAX_KEYS, OComparator};

#[repr(C)]
#[derive(Debug, Clone, Copy)]
pub struct BTreeRootInfo {
    pub rootPageBlkno: pg_sys::BlockNumber,
    pub metaPageBlkno: pg_sys::BlockNumber,
    pub rootPageChangeCount: u32,
}

#[repr(C)]
#[derive(Debug, Clone, Copy)]
pub struct SeqBufDescPrivate {
    pub file: std::ffi::c_int,
}

#[repr(C)]
#[derive(Debug, Clone, Copy)]
pub struct BTreeS3PartsInfo {
    pub part_num: u32,
    pub part_size: u64,
}

#[repr(C)]
pub struct BTreeDescr {
    pub rootInfo: BTreeRootInfo,
    pub arg: *mut std::ffi::c_void,
    pub smgr: [u8; 8],
    pub oids: pg_sys::ORelOids,
    pub tablespace: Oid,
    pub type_: std::ffi::c_int,
    pub ppool: *mut std::ffi::c_void,
    pub compress: std::ffi::c_int,
    pub fillfactor: u8,
    pub undoType: std::ffi::c_int,
    pub storageType: std::ffi::c_int,
    pub freeBuf: SeqBufDescPrivate,
    pub nextChkp: [SeqBufDescPrivate; 2],
    pub tmpBuf: [SeqBufDescPrivate; 2],
    pub buildPartsInfo: [BTreeS3PartsInfo; 2],
    pub createOxid: OXid,
    pub ops: *mut std::ffi::c_void,
    pub localFreeExtents: *mut std::ffi::c_void,
}

#[repr(C)]
pub struct OTableDescr {
    pub oids: pg_sys::ORelOids,
    pub version: u32,
    pub refcnt: std::ffi::c_int,
    pub tupdesc: pg_sys::TupleDesc,
    pub oldTuple: *mut pg_sys::TupleTableSlot,
    pub newTuple: *mut pg_sys::TupleTableSlot,
    pub indices: *mut *mut OIndexDescr,
    pub bridge: *mut OIndexDescr,
    pub toast: *mut OIndexDescr,
    pub toastable: *mut pg_sys::AttrNumber,
    pub ntoastable: std::ffi::c_int,
    pub nIndices: std::ffi::c_int,
    pub nUniqueIndices: std::ffi::c_int,
    pub tablespace: Oid,
    pub noInvalidation: bool,
}

#[repr(C)]
pub struct OIndexDescr {
    pub oids: pg_sys::ORelOids,
    pub tableOids: pg_sys::ORelOids,
    pub version: u32,
    pub refcnt: std::ffi::c_int,
    pub valid: bool,
    pub desc: BTreeDescr,
    pub name: pg_sys::NameData,
    pub index_mctx: pg_sys::MemoryContext,
    pub expressions: *mut pg_sys::List,
    pub predicate: *mut pg_sys::List,
    pub predicate_str: *mut std::ffi::c_char,
    pub expressions_state: *mut pg_sys::List,
    pub predicate_state: *mut pg_sys::ExprState,
    pub econtext: *mut pg_sys::ExprContext,
    pub nonLeafTupdesc: pg_sys::TupleDesc,
    pub nonLeafSpec: [u8; 32],
    pub leafTupdesc: pg_sys::TupleDesc,
    pub leafSpec: [u8; 32],
    pub unique: bool,
    pub immediate: bool,
    pub nulls_not_distinct: bool,
    pub nUniqueFields: std::ffi::c_int,
    pub primaryIsCtid: bool,
    pub bridging: bool,
    pub fillfactor: u8,
    pub nFields: std::ffi::c_int,
    pub nKeyFields: std::ffi::c_int,
    pub nIncludedFields: std::ffi::c_int,
    pub fields: *mut OIndexField,
    pub nPrimaryFields: std::ffi::c_int,
    pub primaryFieldsAttnums: [pg_sys::AttrNumber; INDEX_MAX_KEYS],
    pub compress: std::ffi::c_int,
    pub tableAttnums: *mut pg_sys::AttrNumber,
    pub maxTableAttnum: std::ffi::c_int,
    pub pk_tbl_field_map: *mut std::ffi::c_void,
    pub pk_comparators: *mut *mut OComparator,
    pub itupdesc: pg_sys::TupleDesc,
    pub index_slot: *mut pg_sys::TupleTableSlot,
    pub old_leaf_slot: *mut pg_sys::TupleTableSlot,
    pub new_leaf_slot: *mut pg_sys::TupleTableSlot,
    pub duplicates: *mut pg_sys::List,
}

#[repr(C)]
pub struct EvictedTreeData {
    pub key: [u8; 16], // SharedRootInfoKey
    pub file_header: [u8; 64], // CheckpointFileHeader
    pub maxLocation: [u8; 32], // S3TaskLocation
    pub freeBuf: [u8; 16], // EvictedSeqBufData
    pub nextChkp: [u8; 16],
    pub tmpBuf: [u8; 16],
    pub dirtyFlag1: bool,
    pub dirtyFlag2: bool,
    pub punchHolesChkpNum: u32,
}

#[no_mangle]
pub unsafe extern "C" fn o_fetch_table_descr(oids: pg_sys::ORelOids) -> *mut OTableDescr {
    extern "C" {
        fn o_fetch_table_descr_c(oids: pg_sys::ORelOids) -> *mut OTableDescr;
    }
    o_fetch_table_descr_c(oids)
}

#[no_mangle]
pub unsafe extern "C" fn o_fetch_index_descr(
    oids: pg_sys::ORelOids,
    index_type: std::ffi::c_int,
    lock: bool,
    nested: *mut bool,
) -> *mut OIndexDescr {
    extern "C" {
        fn o_fetch_index_descr_c(
            oids: pg_sys::ORelOids,
            index_type: std::ffi::c_int,
            lock: bool,
            nested: *mut bool,
        ) -> *mut OIndexDescr;
    }
    o_fetch_index_descr_c(oids, index_type, lock, nested)
}

#[no_mangle]
pub unsafe extern "C" fn recreate_table_descr_by_oids(oids: pg_sys::ORelOids) {
    extern "C" {
        fn recreate_table_descr_by_oids_c(oids: pg_sys::ORelOids);
    }
    recreate_table_descr_by_oids_c(oids);
}

#[no_mangle]
pub unsafe extern "C" fn o_fill_tmp_table_descr(descr: *mut OTableDescr, o_table: *mut std::ffi::c_void) {
    extern "C" {
        fn o_fill_tmp_table_descr_c(descr: *mut OTableDescr, o_table: *mut std::ffi::c_void);
    }
    o_fill_tmp_table_descr_c(descr, o_table);
}

#[no_mangle]
pub unsafe extern "C" fn o_free_tmp_table_descr(descr: *mut OTableDescr) {
    extern "C" {
        fn o_free_tmp_table_descr_c(descr: *mut OTableDescr);
    }
    o_free_tmp_table_descr_c(descr);
}
