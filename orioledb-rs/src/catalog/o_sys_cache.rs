// Catalog module: o_sys_cache.
//
// Ported from `include/catalog/o_sys_cache.h` and `src/catalog/o_sys_cache.c`.
//
// Copyright (c) 2021-2026, Oriole DB Inc.
// Copyright (c) 2025-2026, Supabase Inc.

use pgrx::pg_sys::{self, Oid, Datum, XLogRecPtr, MemoryContext, HTAB};

#[repr(C)]
#[derive(Copy, Clone)]
pub struct OSysCacheKeyCommon {
    pub datoid: Oid,
    pub lsn: XLogRecPtr,
    pub deleted: bool,
    pub dataLength: std::ffi::c_int,
}

#[repr(C)]
#[derive(Copy, Clone)]
pub struct OSysCacheKey1 {
    pub common: OSysCacheKeyCommon,
    pub keys: [Datum; 1],
}

#[repr(C)]
#[derive(Copy, Clone)]
pub struct OSysCacheKey2 {
    pub common: OSysCacheKeyCommon,
    pub keys: [Datum; 2],
}

#[repr(C)]
#[derive(Copy, Clone)]
pub struct OSysCacheKey3 {
    pub common: OSysCacheKeyCommon,
    pub keys: [Datum; 3],
}

#[repr(C)]
#[derive(Copy, Clone)]
pub struct OSysCacheKey4 {
    pub common: OSysCacheKeyCommon,
    pub keys: [Datum; 4],
}

#[repr(C)]
pub struct OSysCacheKey {
    pub common: OSysCacheKeyCommon,
    pub keys: [Datum; 0],
}

#[repr(C)]
pub struct OSysCacheFuncs {
    pub free_entry: Option<unsafe extern "C-unwind" fn(entry: *mut std::ffi::c_void)>,
    pub fill_entry: Option<unsafe extern "C-unwind" fn(entry_ptr: *mut *mut std::ffi::c_void, key: *mut OSysCacheKey, arg: *mut std::ffi::c_void)>,
    pub toast_serialize_entry: Option<unsafe extern "C-unwind" fn(entry: *mut std::ffi::c_void, len: *mut std::ffi::c_int) -> *mut std::ffi::c_void>,
    pub toast_deserialize_entry: Option<unsafe extern "C-unwind" fn(mcxt: MemoryContext, data: *mut std::ffi::c_void, length: usize) -> *mut std::ffi::c_void>,
}

pub type O_CCHashFN = Option<unsafe extern "C-unwind" fn(key: *mut OSysCacheKey, att_num: std::ffi::c_int) -> u32>;

#[repr(C)]
pub struct OSysCache {
    pub sys_tree_num: std::ffi::c_int,
    pub is_toast: bool,
    pub cc_indexoid: Oid,
    pub cacheId: std::ffi::c_int,
    pub nkeys: std::ffi::c_int,
    pub keytypes: [Oid; 4], // CATCACHE_MAXKEYS is 4
    pub data_len: std::ffi::c_int,
    pub mcxt: MemoryContext,
    pub fast_cache: *mut HTAB,
    pub cc_hashfunc: [O_CCHashFN; 4],
    pub last_fast_cache_key: u32,
    pub last_fast_cache_entry: *mut std::ffi::c_void,
    pub funcs: *mut OSysCacheFuncs,
}

extern "C-unwind" {
    pub fn o_create_sys_cache(
        sys_tree_num: std::ffi::c_int,
        is_toast: bool,
        cc_indexoid: Oid,
        cacheId: std::ffi::c_int,
        nkeys: std::ffi::c_int,
        keytypes: *mut Oid,
        data_len: std::ffi::c_int,
        fastcache: bool,
        mcxt: MemoryContext,
        funcs: *mut OSysCacheFuncs,
    ) -> *mut OSysCache;

    pub fn o_sys_cache_search(
        cache: *mut OSysCache,
        nkeys_arg: std::ffi::c_int,
        key: *mut OSysCacheKey,
    ) -> *mut std::ffi::c_void;

    pub fn o_sys_cache_delete(
        cache: *mut OSysCache,
        key: *mut OSysCacheKey,
    ) -> bool;

    pub fn o_sys_cache_update_if_needed(
        cache: *mut OSysCache,
        key: *mut OSysCacheKey,
        arg: *mut std::ffi::c_void,
    );

    pub fn o_sys_cache_add_if_needed(
        cache: *mut OSysCache,
        key: *mut OSysCacheKey,
        insert_lsn: XLogRecPtr,
        arg: *mut std::ffi::c_void,
    );

    pub fn o_sys_cache_set_datoid_lsn(
        cur_lsn: *mut XLogRecPtr,
        datoid: *mut Oid,
    );

    pub fn o_sys_cache_key_print(
        desc: *mut crate::btree::btree::BTreeDescr,
        buf: pg_sys::StringInfo,
        tup: crate::btree::btree::OTuple,
        arg: *mut std::ffi::c_void,
    );
}
