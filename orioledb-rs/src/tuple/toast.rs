//! toast.rs
//!
//! Copyright (c) 2021-2026, Oriole DB Inc.
//! Copyright (c) 2025-2026, Supabase Inc.
//!
//! IDENTIFICATION
//!   contrib/orioledb/orioledb-rs/src/tuple/toast.rs

use std::ffi::{c_char, c_int, c_void};
use pgrx::pg_sys::{Datum, TupleDesc, Tuplesortstate, Oid, Size, Pointer};
use crate::btree::btree::BTreeDescr;
use crate::tuple::format::{
    OTuple, OTupleFixedFormatSpec, OToastValue, ORelOids, BridgeData,
    o_fastgetattr, o_fastgetattr_ptr, o_tuple_size, o_form_tuple,
    SizeOfOTupleHeader, TupleDescAttr, maxalign, BITMAPLEN, att_isnull,
    VARATT_IS_EXTERNAL, VARATT_IS_COMPRESSED, VARATT_IS_EXTERNAL_EXPANDED,
    VARSIZE_ANY, VARSIZE_ANY_EXHDR, VARSIZE, varattrib_1b,
};
use crate::tuple::sort::OIndexDescr;

pub const ATTN_POS: i32 = 1;
pub const CHUNKN_POS: i32 = 2;
pub const DATA_POS: i32 = 3;

pub const TOAST_LEAF_FIELDS_NUM: i32 = 3;
pub const TOAST_NON_LEAF_FIELDS_NUM: i32 = 2;

#[repr(C)]
#[derive(Copy, Clone, Debug)]
pub struct OToastKey {
    pub pk_tuple: OTuple,
    pub chunknum: u32,
    pub attnum: u16,
}

pub type OTupleXactInfo = u64;

#[repr(C)]
#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub enum TupleFetchCallbackResult {
    Next = 0,
    Match = 1,
    NotMatch = 2,
}

pub type TupleFetchCallback = unsafe extern "C" fn(
    tuple: OTuple,
    xactInfo: OTupleXactInfo,
    arg: *mut c_void,
) -> TupleFetchCallbackResult;

#[repr(C)]
#[derive(Copy, Clone)]
pub struct ToastAPI {
    pub getBTreeDesc: unsafe extern "C" fn(arg: *mut c_void) -> *mut BTreeDescr,
    pub getBTreeVersion: Option<unsafe extern "C" fn(arg: *mut c_void) -> u32>,
    pub getBaseBTreeVersion: Option<unsafe extern "C" fn(arg: *mut c_void) -> u32>,
    pub getKeySize: Option<unsafe extern "C" fn(arg: *mut c_void) -> u32>,
    pub getMaxChunkSize: unsafe extern "C" fn(key: *mut c_void, arg: *mut c_void) -> u32,
    pub updateKey: unsafe extern "C" fn(key: *mut c_void, chunknum: u32, arg: *mut c_void),
    pub getNextKey: unsafe extern "C" fn(key: *mut c_void, arg: *mut c_void) -> *mut c_void,
    pub createTuple: unsafe extern "C" fn(
        key: *mut c_void,
        data: *mut c_char,
        offset: u32,
        chunknum: u32,
        length: c_int,
        arg: *mut c_void,
    ) -> OTuple,
    pub createKey: unsafe extern "C" fn(
        key: *mut c_void,
        chunknum: u32,
        arg: *mut c_void,
    ) -> OTuple,
    pub getTupleData: unsafe extern "C" fn(tuple: OTuple, arg: *mut c_void) -> *mut c_char,
    pub getTupleChunknum: unsafe extern "C" fn(tuple: OTuple, arg: *mut c_void) -> u32,
    pub getTupleDataSize: unsafe extern "C" fn(tuple: OTuple, arg: *mut c_void) -> u32,
    pub deleteLogFullTuple: bool,
    pub fetchCallback: Option<TupleFetchCallback>,
}

#[repr(C)]
pub struct OTableToastArg {
    pub pk: *mut OIndexDescr,
    pub toast: *mut OIndexDescr,
    pub version: u32,
}

#[repr(C)]
pub struct OTableDescr {
    pub oids: ORelOids,
    pub version: u32,
    // Add toast descriptor which is also OIndexDescr
    pub refcnt: c_int,
    pub valid: bool,
    pub indices: *mut *mut OIndexDescr,
    pub nindices: c_int,
    pub tupdesc: TupleDesc,
    pub toast: *mut OIndexDescr,
    pub bridge: *mut OIndexDescr,
    pub ntoastable: c_int,
    pub toastable: *mut pgrx::pg_sys::AttrNumber,
    pub has_primary: bool,
    // We only access these fields of OTableDescr in slot/toast.
}

#[repr(C)]
#[derive(Copy, Clone, Debug)]
pub struct OToastExternal {
    pub datoid: Oid,
    pub relid: Oid,
    pub relnode: Oid,
    pub attnum: pgrx::pg_sys::AttrNumber,
    pub toasted_size: i32,
    pub raw_size: i32,
    pub csn: u64,
    pub formatFlags: u8,
    pub data_size: i32,
}

pub const O_TOAST_EXTERNAL_SZ: usize = std::mem::size_of::<OToastExternal>();

pub const fn maxalign_down(len: usize) -> usize {
    len & !7
}

pub const O_BTREE_MAX_TUPLE_SIZE: usize = maxalign_down((8192 - 24) / 3 - 2 - 8);

pub unsafe fn VARATT_IS_EXTERNAL_ORIOLEDB(ptr: *const c_char) -> bool {
    if !VARATT_IS_EXTERNAL(ptr) {
        return false;
    }
    let va = &*(ptr as *const varattrib_1b_toast);
    va.va_tag == pgrx::pg_sys::VartagType_VARTAG_ORIOLEDB as u8
}

pub unsafe fn o_get_raw_size(value: Datum) -> i32 {
    let ptr = value.value() as *const c_char;
    if VARATT_IS_EXTERNAL_ORIOLEDB(ptr) {
        let mut ote = std::mem::MaybeUninit::<OToastExternal>::uninit();
        std::ptr::copy_nonoverlapping(
            pgrx::pg_sys::VARATT_EXTERNAL_GET_POINTER(value.value() as *mut pgrx::pg_sys::varlena) as *const u8,
            ote.as_mut_ptr() as *mut u8,
            O_TOAST_EXTERNAL_SZ,
        );
        let ote = ote.assume_init();
        ote.raw_size
    } else if pgrx::pg_sys::VARATT_IS_EXTERNAL(value.value() as *mut pgrx::pg_sys::varlena) {
        (pgrx::pg_sys::toast_raw_datum_size(value) - 4) as i32
    } else if VARATT_IS_COMPRESSED(ptr) {
        let attr = value.value() as *const pgrx::pg_sys::varlena;
        let rawsize_ptr = (attr as *const u8).add(4) as *const i32;
        *rawsize_ptr
    } else {
        VARSIZE_ANY_EXHDR(value) as i32
    }
}

pub unsafe fn o_get_src_size(value: Datum) -> i32 {
    let ptr = value.value() as *const c_char;
    if VARATT_IS_EXTERNAL_ORIOLEDB(ptr) {
        let mut ote = std::mem::MaybeUninit::<OToastExternal>::uninit();
        std::ptr::copy_nonoverlapping(
            pgrx::pg_sys::VARATT_EXTERNAL_GET_POINTER(value.value() as *mut pgrx::pg_sys::varlena) as *const u8,
            ote.as_mut_ptr() as *mut u8,
            O_TOAST_EXTERNAL_SZ,
        );
        let ote = ote.assume_init();
        ote.toasted_size
    } else if pgrx::pg_sys::VARATT_IS_EXTERNAL_ONDISK(value.value() as *mut pgrx::pg_sys::varlena) {
        (pgrx::pg_sys::toast_datum_size(value) + 4) as i32
    } else if pgrx::pg_sys::VARATT_IS_EXTERNAL(value.value() as *mut pgrx::pg_sys::varlena) {
        pgrx::pg_sys::toast_datum_size(value) as i32
    } else {
        VARSIZE_ANY(ptr) as i32
    }
}

unsafe extern "C" fn tableGetBTreeDesc(arg: *mut c_void) -> *mut BTreeDescr {
    let toast_arg = arg as *mut OTableToastArg;
    let toast = (*toast_arg).toast;
    std::ptr::addr_of_mut!((*toast).desc)
}

unsafe extern "C" fn tableGetBTreeVersion(arg: *mut c_void) -> u32 {
    let toast_arg = arg as *mut OTableToastArg;
    (*(*toast_arg).toast).version
}

unsafe extern "C" fn tableGetBaseBTreeVersion(arg: *mut c_void) -> u32 {
    let toast_arg = arg as *mut OTableToastArg;
    (*toast_arg).version
}

unsafe extern "C" fn tableGetMaxChunkSize(key: *mut c_void, arg: *mut c_void) -> u32 {
    let tkey = key as *mut OToastKey;
    let toast = (*(arg as *mut OTableToastArg)).toast;
    let primary = (*(arg as *mut OTableToastArg)).pk;
    let mut values = [Datum::from(0); 16 + 3];
    let mut isnull = [false; 16 + 3];

    let natts = (*(*primary).nonLeafTupdesc).natts;
    for i in 0..natts {
        let attnum = i + 1;
        values[i as usize] = o_fastgetattr((*tkey).pk_tuple, attnum, (*primary).nonLeafTupdesc, &(*primary).nonLeafSpec, &mut isnull[i as usize]);
    }
    values[natts as usize] = Datum::from(0);
    values[(natts + 1) as usize] = Datum::from(0);

    let mut data = pgrx::pg_sys::varlena {
        vl_len_: [0; 4],
        vl_dat: pgrx::pg_sys::__IncompleteArrayField::new(),
    };
    // SET_VARSIZE(&data, 4)
    data.vl_len_[0] = 4;
    values[(natts + 2) as usize] = Datum::from(&data as *const pgrx::pg_sys::varlena as usize);

    let min_tuple_size = crate::tuple::format::o_new_tuple_size(
        (*toast).leafTupdesc,
        &mut (*toast).leafSpec,
        std::ptr::null_mut(),
        std::ptr::null_mut(),
        1,
        values.as_mut_ptr(),
        isnull.as_mut_ptr(),
        std::ptr::null_mut(),
    );

    maxalign_down(O_BTREE_MAX_TUPLE_SIZE * 3 - maxalign(min_tuple_size)) as u32 / 3 - min_tuple_size as u32 - 2
}

unsafe extern "C" fn tableUpdateKey(key: *mut c_void, chunknum: u32, _arg: *mut c_void) {
    let tkey = key as *mut OToastKey;
    (*tkey).chunknum = chunknum;
}

unsafe extern "C" fn tableGetNextKey(key: *mut c_void, _arg: *mut c_void) -> *mut c_void {
    let tkey = key as *mut OToastKey;
    (*tkey).chunknum += 1;
    key
}

unsafe extern "C" fn tableCreateTuple(
    key: *mut c_void,
    data: *mut c_char,
    _offset: u32,
    chunknum: u32,
    length: c_int,
    arg: *mut c_void,
) -> OTuple {
    let tkey = *(key as *mut OToastKey);
    let mut tkey_mod = tkey;
    tkey_mod.chunknum = chunknum;
    let toast_arg = arg as *mut OTableToastArg;
    o_create_toast_tuple(tkey_mod, data, length as usize, toast_arg)
}

unsafe extern "C" fn tableCreateKey(
    key: *mut c_void,
    chunknum: u32,
    arg: *mut c_void,
) -> OTuple {
    let tkey = *(key as *mut OToastKey);
    let mut tkey_mod = tkey;
    tkey_mod.chunknum = chunknum;
    let toast_arg = arg as *mut OTableToastArg;
    o_create_toast_key(tkey_mod, toast_arg)
}

unsafe extern "C" fn tableGetTupleData(tuple: OTuple, arg: *mut c_void) -> *mut c_char {
    let toast_arg = arg as *mut OTableToastArg;
    let toast = (*toast_arg).toast;
    let pk_natts = (*(*toast_arg).pk).nonLeafTupdesc.as_ref().unwrap().natts;
    let mut isnull = false;
    o_fastgetattr_ptr(tuple, pk_natts + DATA_POS, (*toast).leafTupdesc, &(*toast).leafSpec)
}

unsafe extern "C" fn tableGetTupleChunknum(tuple: OTuple, arg: *mut c_void) -> u32 {
    let toast_arg = arg as *mut OTableToastArg;
    let toast = (*toast_arg).toast;
    let pk_natts = (*(*toast_arg).pk).nonLeafTupdesc.as_ref().unwrap().natts;
    let mut isnull = false;
    o_fastgetattr(tuple, pk_natts + CHUNKN_POS, (*toast).leafTupdesc, &(*toast).leafSpec, &mut isnull).value() as u32
}

unsafe extern "C" fn tableGetTupleDataSize(tuple: OTuple, arg: *mut c_void) -> u32 {
    let toast_arg = arg as *mut OTableToastArg;
    let toast = (*toast_arg).toast;
    let pk_natts = (*(*toast_arg).pk).nonLeafTupdesc.as_ref().unwrap().natts;
    let data_ptr = tableGetTupleData(tuple, arg);
    VARSIZE_ANY(data_ptr) as u32
}

unsafe extern "C" fn tableVersionCallback(
    _tuple: OTuple,
    _xact_info: OTupleXactInfo,
    _arg: *mut c_void,
) -> TupleFetchCallbackResult {
    TupleFetchCallbackResult::Match
}

#[no_mangle]
pub static mut tableToastAPI: ToastAPI = ToastAPI {
    getBTreeDesc: tableGetBTreeDesc,
    getBTreeVersion: Some(tableGetBTreeVersion),
    getBaseBTreeVersion: Some(tableGetBaseBTreeVersion),
    getKeySize: None,
    getMaxChunkSize: tableGetMaxChunkSize,
    updateKey: tableUpdateKey,
    getNextKey: tableGetNextKey,
    createTuple: tableCreateTuple,
    createKey: tableCreateKey,
    getTupleData: tableGetTupleData,
    getTupleChunknum: tableGetTupleChunknum,
    getTupleDataSize: tableGetTupleDataSize,
    deleteLogFullTuple: false,
    fetchCallback: Some(tableVersionCallback),
};

unsafe fn o_create_toast_tuple(
    tkey: OToastKey,
    data: *mut c_char,
    data_length: usize,
    arg: *mut OTableToastArg,
) -> OTuple {
    let primary = (*arg).pk;
    let toast = (*arg).toast;
    let mut values = [Datum::from(0); 16 + 3];
    let mut isnull = [false; 16 + 3];

    let natts = (*(*primary).nonLeafTupdesc).natts;
    for i in 0..natts {
        let attnum = i + 1;
        values[i as usize] = o_fastgetattr(tkey.pk_tuple, attnum, (*primary).nonLeafTupdesc, &(*primary).nonLeafSpec, &mut isnull[i as usize]);
    }
    values[natts as usize] = Datum::from(tkey.attnum as usize);
    values[(natts + 1) as usize] = Datum::from(tkey.chunknum as usize);
    values[(natts + 2) as usize] = Datum::from(data as usize);

    o_form_tuple((*toast).leafTupdesc, &mut (*toast).leafSpec, 1, values.as_mut_ptr(), isnull.as_mut_ptr(), std::ptr::null_mut())
}

unsafe fn o_create_toast_key(
    tkey: OToastKey,
    arg: *mut OTableToastArg,
) -> OTuple {
    let primary = (*arg).pk;
    let toast = (*arg).toast;
    let mut values = [Datum::from(0); 16 + 2];
    let mut isnull = [false; 16 + 2];

    let natts = (*(*primary).nonLeafTupdesc).natts;
    for i in 0..natts {
        let attnum = i + 1;
        values[i as usize] = o_fastgetattr(tkey.pk_tuple, attnum, (*primary).nonLeafTupdesc, &(*primary).nonLeafSpec, &mut isnull[i as usize]);
    }
    values[natts as usize] = Datum::from(tkey.attnum as usize);
    values[(natts + 1) as usize] = Datum::from(tkey.chunknum as usize);

    o_form_tuple((*toast).nonLeafTupdesc, &mut (*toast).nonLeafSpec, 0, values.as_mut_ptr(), isnull.as_mut_ptr(), std::ptr::null_mut())
}

extern "C" {
    pub fn generic_toast_insert(api: *mut ToastAPI, key: *mut c_void, data: *mut c_char, data_size: Size, oxid: u32, csn: u64, arg: *mut c_void) -> bool;
    pub fn generic_toast_update(api: *mut ToastAPI, key: *mut c_void, data: *mut c_char, data_size: Size, oxid: u32, csn: u64, arg: *mut c_void) -> bool;
    pub fn generic_toast_delete(api: *mut ToastAPI, key: *mut c_void, oxid: u32, csn: u64, arg: *mut c_void) -> bool;
    pub fn generic_toast_get_any(api: *mut ToastAPI, key: *mut c_void, data_size: *mut Size, snapshot: *mut std::ffi::c_void, arg: *mut c_void) -> Pointer;
    pub fn generic_toast_get_any_with_key(api: *mut ToastAPI, key: *mut c_void, data_size: *mut Size, snapshot: *mut std::ffi::c_void, arg: *mut c_void, found_key: *mut Pointer) -> Pointer;
    pub fn generic_toast_get_any_with_callback(api: *mut ToastAPI, key: Pointer, data_size: *mut Size, snapshot: *mut std::ffi::c_void, arg: *mut c_void, fetch_callback: TupleFetchCallback, callback_arg: *mut c_void) -> Pointer;
    pub fn o_toast_insert(descr: *mut OTableDescr, pk: OTuple, attn: u16, data: Pointer, data_size: Size, oxid: u32, csn: u64) -> bool;
    pub fn o_toast_sort_add(descr: *mut OTableDescr, pk: OTuple, attn: u16, data: Pointer, data_size: Size, sortstate: *mut Tuplesortstate);
    pub fn o_toast_delete(descr: *mut OTableDescr, pk: OTuple, attn: u16, oxid: u32, csn: u64) -> bool;
    pub fn o_toast_cmp(desc: *mut BTreeDescr, p1: *mut c_void, k1: std::ffi::c_int, p2: *mut c_void, k2: std::ffi::c_int) -> c_int;
    pub fn o_toast_needs_undo(desc: *mut BTreeDescr, action: std::ffi::c_int, oldTuple: OTuple, oldXactInfo: OTupleXactInfo, oldDeleted: bool, newTuple: OTuple, newOxid: u32) -> bool;
    pub fn o_get_raw_value(value: Datum, free: *mut bool) -> Datum;
    pub fn o_get_src_value(value: Datum, free: *mut bool) -> Datum;
    pub fn o_toast_equal(primary: *mut BTreeDescr, left: Datum, right: Datum) -> bool;
}
