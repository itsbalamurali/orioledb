/*-------------------------------------------------------------------------
 *
 * tree.rs
 *		Implementation of BTree interface for OrioleDB tables.
 *
 * Copyright (c) 2021-2026, Oriole DB Inc.
 * Copyright (c) 2025-2026, Supabase Inc.
 *
 *-------------------------------------------------------------------------
 */

use pgrx::pg_sys::{self, Datum, Oid};
use crate::tableam::descr::{BTreeDescr, OIndexDescr};
use crate::tableam::key_range::{OBTreeKeyBound, OBTreeValueBound, OIndexField};

// C enum types represented as u32
pub type OCompress = std::ffi::c_int;
pub type OIndexType = std::ffi::c_int;
pub type BTreeKeyType = std::ffi::c_int;

#[no_mangle]
pub unsafe extern "C" fn index_btree_desc_init(
    desc: *mut BTreeDescr,
    compress: OCompress,
    fillfactor: std::ffi::c_int,
    oids: pg_sys::ORelOids,
    index_type: OIndexType,
    persistence: std::ffi::c_char,
    tablespace: Oid,
    createOxid: pg_sys::OXid,
    arg: *mut std::ffi::c_void,
) {
    // Ported from tree.c
    let desc = &mut *desc;
    desc.oids = oids;
    desc.tablespace = tablespace;
    desc.arg = arg;
    desc.compress = compress;
    if fillfactor >= 10 && fillfactor <= 100 {
        desc.fillfactor = fillfactor as u8;
    } else {
        desc.fillfactor = 90; // BTREE_DEFAULT_FILLFACTOR
    }
    desc.type_ = index_type;
    desc.rootInfo.rootPageBlkno = pg_sys::OInvalidInMemoryBlkno;
    desc.rootInfo.metaPageBlkno = pg_sys::OInvalidInMemoryBlkno;
    desc.rootInfo.rootPageChangeCount = 0;
    
    // Call C backend init functions if needed via FFI
    extern "C" {
        fn btree_init_smgr(desc: *mut BTreeDescr);
        fn get_ppool(pool: std::ffi::c_int) -> *mut std::ffi::c_void;
    }
    btree_init_smgr(desc);
    
    desc.freeBuf.file = -1;
    desc.nextChkp[0].file = -1;
    desc.nextChkp[1].file = -1;
    desc.tmpBuf[0].file = -1;
    desc.tmpBuf[1].file = -1;
    desc.ppool = get_ppool(0); // OPagePoolMain
    
    if persistence == pg_sys::RELPERSISTENCE_TEMP as std::ffi::c_char {
        extern "C" {
            static local_ppool: std::ffi::c_void;
        }
        desc.ppool = &local_ppool as *const std::ffi::c_void as *mut std::ffi::c_void;
        desc.storageType = 2; // BTreeStorageTemporary
    } else if persistence == pg_sys::RELPERSISTENCE_UNLOGGED as std::ffi::c_char {
        desc.storageType = 1; // BTreeStorageUnlogged
    } else {
        desc.storageType = 0; // BTreeStoragePersistence
    }
    desc.undoType = 0; // UndoLogRegular
    desc.createOxid = createOxid;
    desc.localFreeExtents = std::ptr::null_mut();
}

#[no_mangle]
pub unsafe extern "C" fn o_hash_iptr(idx: *mut OIndexDescr, iptr: pg_sys::ItemPointer) -> u32 {
    extern "C" {
        fn o_hash_iptr_c(idx: *mut OIndexDescr, iptr: pg_sys::ItemPointer) -> u32;
    }
    o_hash_iptr_c(idx, iptr)
}

#[no_mangle]
pub unsafe extern "C" fn o_fill_key_bound(
    id: *mut OIndexDescr,
    tuple: pg_sys::OTuple,
    keyType: BTreeKeyType,
    bound: *mut OBTreeKeyBound,
) {
    extern "C" {
        fn o_fill_key_bound_c(id: *mut OIndexDescr, tuple: pg_sys::OTuple, keyType: BTreeKeyType, bound: *mut OBTreeKeyBound);
    }
    o_fill_key_bound_c(id, tuple, keyType, bound);
}

#[no_mangle]
pub unsafe extern "C" fn o_fill_bridge_index_key_bound(
    secondary: *mut BTreeDescr,
    tuple: pg_sys::OTuple,
    bound: *mut OBTreeKeyBound,
) {
    extern "C" {
        fn o_fill_bridge_index_key_bound_c(secondary: *mut BTreeDescr, tuple: pg_sys::OTuple, bound: *mut OBTreeKeyBound);
    }
    o_fill_bridge_index_key_bound_c(secondary, tuple, bound);
}

#[no_mangle]
pub unsafe extern "C" fn o_fill_pindex_tuple_key_bound(
    desc: *mut BTreeDescr,
    tup: pg_sys::OTuple,
    bound: *mut OBTreeKeyBound,
) {
    extern "C" {
        fn o_fill_pindex_tuple_key_bound_c(desc: *mut BTreeDescr, tup: pg_sys::OTuple, bound: *mut OBTreeKeyBound);
    }
    o_fill_pindex_tuple_key_bound_c(desc, tup, bound);
}

#[no_mangle]
pub unsafe extern "C" fn o_idx_cmp(
    desc: *mut BTreeDescr,
    p1: *mut std::ffi::c_void,
    keyType1: BTreeKeyType,
    p2: *mut std::ffi::c_void,
    keyType2: BTreeKeyType,
) -> std::ffi::c_int {
    extern "C" {
        fn o_idx_cmp_c(desc: *mut BTreeDescr, p1: *mut std::ffi::c_void, keyType1: BTreeKeyType, p2: *mut std::ffi::c_void, keyType2: BTreeKeyType) -> std::ffi::c_int;
    }
    o_idx_cmp_c(desc, p1, keyType1, p2, keyType2)
}

#[no_mangle]
pub unsafe extern "C" fn o_idx_cmp_range_key_to_value(
    sk1: *mut OBTreeValueBound,
    field: *mut OIndexField,
    value: Datum,
    isnull: bool,
) -> std::ffi::c_int {
    extern "C" {
        fn o_idx_cmp_range_key_to_value_c(sk1: *mut OBTreeValueBound, field: *mut OIndexField, value: Datum, isnull: bool) -> std::ffi::c_int;
    }
    o_idx_cmp_range_key_to_value_c(sk1, field, value, isnull)
}
