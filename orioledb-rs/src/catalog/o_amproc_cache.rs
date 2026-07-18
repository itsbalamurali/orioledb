// Catalog module: o_amproc_cache.
//
// Ported from `include/catalog/o_sys_cache.h` and `src/catalog/o_amproc_cache.c`.
//
// Copyright (c) 2021-2026, Oriole DB Inc.
// Copyright (c) 2025-2026, Supabase Inc.

use crate::catalog::o_sys_cache::{
    self, OSysCache, OSysCacheFuncs, OSysCacheKey, OSysCacheKey4, OSysCacheKeyCommon,
};
use pgrx::pg_sys::{self, Datum, Oid, XLogRecPtr, MemoryContext, TupleDesc, HeapTuple};
use std::ffi::{c_char, c_void, CString};

pub static mut amproc_cache: *mut OSysCache = std::ptr::null_mut();

#[repr(C)]
pub struct OAmProc {
    pub key: OSysCacheKey4,
    pub amproc: pg_sys::regproc,
}

unsafe fn get_struct(tup: pg_sys::HeapTuple) -> *mut c_char {
    ((*tup).t_data as *mut c_char).add((*(*tup).t_data).t_hoff as usize)
}

#[no_mangle]
pub unsafe extern "C-unwind" fn o_amproc_cache_init(fastcache: bool, mcxt: MemoryContext) {
    static mut AMPROC_CACHE_FUNCS: OSysCacheFuncs = OSysCacheFuncs {
        free_entry: Some(o_amproc_cache_free_entry),
        fill_entry: Some(o_amproc_cache_fill_entry),
        toast_serialize_entry: None,
        toast_deserialize_entry: None,
    };

    let mut keytypes = [
        pg_sys::OIDOID,
        pg_sys::OIDOID,
        pg_sys::OIDOID,
        pg_sys::INT2OID,
    ];

    amproc_cache = o_sys_cache::o_create_sys_cache(
        16, // SYS_TREES_AMPROC_CACHE
        false,
        Oid::from(pg_sys::AccessMethodProcedureIndexId),
        5, // AMPROCNUM
        4,
        keytypes.as_mut_ptr(),
        0,
        fastcache,
        mcxt,
        std::ptr::addr_of_mut!(AMPROC_CACHE_FUNCS),
    );
}

#[no_mangle]
pub unsafe extern "C-unwind" fn o_amproc_cache_search(
    datoid: Oid,
    arg1: Datum,
    arg2: Datum,
    arg3: Datum,
    arg4: Datum,
    search_lsn: XLogRecPtr,
    nkeys_arg: std::ffi::c_int,
) -> *mut OAmProc {
    let mut key = OSysCacheKey4 {
        common: OSysCacheKeyCommon {
            datoid,
            lsn: search_lsn,
            deleted: false,
            dataLength: 0,
        },
        keys: [arg1, arg2, arg3, arg4],
    };
    o_sys_cache::o_sys_cache_search(
        amproc_cache,
        nkeys_arg,
        std::ptr::addr_of_mut!(key) as *mut OSysCacheKey,
    ) as *mut OAmProc
}

#[no_mangle]
pub unsafe extern "C-unwind" fn o_amproc_cache_delete(
    datoid: Oid,
    arg1: Datum,
    arg2: Datum,
    arg3: Datum,
    arg4: Datum,
) -> bool {
    let mut key = OSysCacheKey4 {
        common: OSysCacheKeyCommon {
            datoid,
            lsn: 0,
            deleted: false,
            dataLength: 0,
        },
        keys: [arg1, arg2, arg3, arg4],
    };
    o_sys_cache::o_sys_cache_delete(
        amproc_cache,
        std::ptr::addr_of_mut!(key) as *mut OSysCacheKey,
    )
}

#[no_mangle]
pub unsafe extern "C-unwind" fn o_amproc_cache_update_if_needed(
    datoid: Oid,
    arg1: Datum,
    arg2: Datum,
    arg3: Datum,
    arg4: Datum,
    arg: *mut c_void,
) {
    let mut key = OSysCacheKey4 {
        common: OSysCacheKeyCommon {
            datoid,
            lsn: 0,
            deleted: false,
            dataLength: 0,
        },
        keys: [arg1, arg2, arg3, arg4],
    };
    o_sys_cache::o_sys_cache_update_if_needed(
        amproc_cache,
        std::ptr::addr_of_mut!(key) as *mut OSysCacheKey,
        arg,
    );
}

#[no_mangle]
pub unsafe extern "C-unwind" fn o_amproc_cache_add_if_needed(
    datoid: Oid,
    arg1: Datum,
    arg2: Datum,
    arg3: Datum,
    arg4: Datum,
    insert_lsn: XLogRecPtr,
    arg: *mut c_void,
) {
    let mut key = OSysCacheKey4 {
        common: OSysCacheKeyCommon {
            datoid,
            lsn: insert_lsn,
            deleted: false,
            dataLength: 0,
        },
        keys: [arg1, arg2, arg3, arg4],
    };
    o_sys_cache::o_sys_cache_add_if_needed(
        amproc_cache,
        std::ptr::addr_of_mut!(key) as *mut OSysCacheKey,
        insert_lsn,
        arg,
    );
}

unsafe extern "C-unwind" fn o_amproc_cache_fill_entry(
    entry_ptr: *mut *mut c_void,
    key: *mut OSysCacheKey,
    _arg: *mut c_void,
) {
    let keys = std::slice::from_raw_parts((*key).keys.as_ptr(), 4);
    let amprocfamily = keys[0];
    let amproclefttype = keys[1];
    let amprocrighttype = keys[2];
    let amprocnum = keys[3];

    let amproctup = pg_sys::SearchSysCache4(
        5, // AMPROCNUM
        amprocfamily,
        amproclefttype,
        amprocrighttype,
        amprocnum,
    );
    if amproctup.is_null() {
        pg_sys::ereport!(
            pgrx::PgLogLevel::ERROR,
            pg_sys::errcodes::PgSqlErrorCode::ERRCODE_INTERNAL_ERROR,
            format!(
                "cache lookup failed for amproc ({} {} {} {})",
                amprocfamily.value(),
                amproclefttype.value(),
                amprocrighttype.value(),
                amprocnum.value() as i16
            )
        );
    }

    let amprocform = get_struct(amproctup) as *mut pg_sys::FormData_pg_amproc;

    let prev_context = pg_sys::MemoryContextSwitchTo((*amproc_cache).mcxt);

    let mut o_amproc = *entry_ptr as *mut OAmProc;
    if !o_amproc.is_null() {
        panic!("Assert failed: o_amproc is not null");
    } else {
        o_amproc = pg_sys::palloc0(std::mem::size_of::<OAmProc>()) as *mut OAmProc;
        *entry_ptr = o_amproc as *mut c_void;
    }

    (*o_amproc).amproc = (*amprocform).amproc;

    pg_sys::MemoryContextSwitchTo(prev_context);
    pg_sys::ReleaseSysCache(amproctup);
}

unsafe extern "C-unwind" fn o_amproc_cache_free_entry(entry: *mut c_void) {
    pg_sys::pfree(entry);
}

#[no_mangle]
pub unsafe extern "C-unwind" fn o_amproc_cache_search_htup(
    tupdesc: TupleDesc,
    amprocfamily: Oid,
    amproclefttype: Oid,
    amprocrighttype: Oid,
    amprocnum: i16,
) -> HeapTuple {
    let mut cur_lsn: XLogRecPtr = 0;
    let mut datoid: Oid = Oid::from(0);
    let mut result: HeapTuple = std::ptr::null_mut();
    let mut values = [pg_sys::Datum::from(0); 6]; // Natts_pg_amproc is 6
    let mut nulls = [false; 6];

    o_sys_cache::o_sys_cache_set_datoid_lsn(&mut cur_lsn, &mut datoid);

    let o_amproc = o_amproc_cache_search(
        datoid,
        Datum::from(amprocfamily),
        Datum::from(amproclefttype),
        Datum::from(amprocrighttype),
        Datum::from(amprocnum),
        cur_lsn,
        (*amproc_cache).nkeys,
    );

    if !o_amproc.is_null() {
        values[5] = Datum::from((*o_amproc).amproc);
        result = pg_sys::heap_form_tuple(tupdesc, values.as_mut_ptr(), nulls.as_mut_ptr());
    }

    result
}

#[no_mangle]
pub unsafe extern "C-unwind" fn o_amproc_cache_tup_print(
    desc: *mut crate::btree::btree::BTreeDescr,
    buf: pg_sys::StringInfo,
    tup: crate::btree::btree::OTuple,
    arg: *mut c_void,
) {
    let o_amproc = tup.data as *mut OAmProc;

    pg_sys::appendStringInfoString(buf, b"(\0".as_ptr() as *const c_char);
    o_sys_cache::o_sys_cache_key_print(desc, buf, tup, arg);
    
    let s = format!(", amproc: {})\0", (*o_amproc).amproc.to_u32());
    let c_str = CString::new(s).unwrap();
    pg_sys::appendStringInfoString(buf, c_str.as_ptr());
}
