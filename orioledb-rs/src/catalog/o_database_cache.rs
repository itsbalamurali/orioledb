// Catalog module: o_database_cache.
//
// Ported from `include/catalog/o_sys_cache.h` and `src/catalog/o_database_cache.c`.
//
// Copyright (c) 2021-2026, Oriole DB Inc.
// Copyright (c) 2025-2026, Supabase Inc.

use crate::catalog::o_sys_cache::{
    self, OSysCache, OSysCacheFuncs, OSysCacheKey, OSysCacheKey1, OSysCacheKeyCommon,
};
use pgrx::pg_sys::{self, Datum, Oid, XLogRecPtr, MemoryContext};
use std::ffi::{c_char, c_void};

pub static mut database_cache: *mut OSysCache = std::ptr::null_mut();

#[repr(C)]
pub struct ODatabase {
    pub key: OSysCacheKey1,
    pub data_version: u16,
    pub encoding: i32,
    pub datlocprovider: std::ffi::c_char,
    pub datlocale: *mut std::ffi::c_char,
    pub daticurules: *mut std::ffi::c_char,
    pub datcollate: *mut std::ffi::c_char,
    pub datctype: *mut std::ffi::c_char,
}

unsafe fn get_struct(tup: pg_sys::HeapTuple) -> *mut c_char {
    ((*tup).t_data as *mut c_char).add((*(*tup).t_data).t_hoff as usize)
}

extern "C" {
    pub fn o_serialize_string(s: *const c_char, str_info: pg_sys::StringInfo);
    pub fn o_deserialize_string(ptr: *mut *mut c_char) -> *mut c_char;
}

#[no_mangle]
pub unsafe extern "C-unwind" fn o_database_cache_init(fastcache: bool, mcxt: MemoryContext) {
    static mut DATABASE_CACHE_FUNCS: OSysCacheFuncs = OSysCacheFuncs {
        free_entry: Some(o_database_cache_free_entry),
        fill_entry: Some(o_database_cache_fill_entry),
        toast_serialize_entry: Some(o_database_cache_serialize_entry),
        toast_deserialize_entry: Some(o_database_cache_deserialize_entry),
    };

    let mut keytypes = [pg_sys::OIDOID];

    database_cache = o_sys_cache::o_create_sys_cache(
        13, // SYS_TREES_DATABASE_CACHE
        true,
        Oid::from(pg_sys::DatabaseOidIndexId),
        pg_sys::DATABASEOID,
        1,
        keytypes.as_mut_ptr(),
        0,
        fastcache,
        mcxt,
        std::ptr::addr_of_mut!(DATABASE_CACHE_FUNCS),
    );
}

#[no_mangle]
pub unsafe extern "C-unwind" fn o_database_cache_search(
    datoid: Oid,
    arg1: Datum,
    search_lsn: XLogRecPtr,
    nkeys_arg: std::ffi::c_int,
) -> *mut ODatabase {
    let mut key = OSysCacheKey1 {
        common: OSysCacheKeyCommon {
            datoid,
            lsn: search_lsn,
            deleted: false,
            dataLength: 0,
        },
        keys: [arg1],
    };
    o_sys_cache::o_sys_cache_search(
        database_cache,
        nkeys_arg,
        std::ptr::addr_of_mut!(key) as *mut OSysCacheKey,
    ) as *mut ODatabase
}

#[no_mangle]
pub unsafe extern "C-unwind" fn o_database_cache_delete(
    datoid: Oid,
    arg1: Datum,
) -> bool {
    let mut key = OSysCacheKey1 {
        common: OSysCacheKeyCommon {
            datoid,
            lsn: 0,
            deleted: false,
            dataLength: 0,
        },
        keys: [arg1],
    };
    o_sys_cache::o_sys_cache_delete(
        database_cache,
        std::ptr::addr_of_mut!(key) as *mut OSysCacheKey,
    )
}

#[no_mangle]
pub unsafe extern "C-unwind" fn o_database_cache_update_if_needed(
    datoid: Oid,
    arg1: Datum,
    arg: *mut c_void,
) {
    let mut key = OSysCacheKey1 {
        common: OSysCacheKeyCommon {
            datoid,
            lsn: 0,
            deleted: false,
            dataLength: 0,
        },
        keys: [arg1],
    };
    o_sys_cache::o_sys_cache_update_if_needed(
        database_cache,
        std::ptr::addr_of_mut!(key) as *mut OSysCacheKey,
        arg,
    );
}

#[no_mangle]
pub unsafe extern "C-unwind" fn o_database_cache_add_if_needed(
    datoid: Oid,
    arg1: Datum,
    insert_lsn: XLogRecPtr,
    arg: *mut c_void,
) {
    let mut key = OSysCacheKey1 {
        common: OSysCacheKeyCommon {
            datoid,
            lsn: insert_lsn,
            deleted: false,
            dataLength: 0,
        },
        keys: [arg1],
    };
    o_sys_cache::o_sys_cache_add_if_needed(
        database_cache,
        std::ptr::addr_of_mut!(key) as *mut OSysCacheKey,
        insert_lsn,
        arg,
    );
}

unsafe extern "C-unwind" fn o_database_cache_fill_entry(
    entry_ptr: *mut *mut c_void,
    key: *mut OSysCacheKey,
    _arg: *mut c_void,
) {
    let key_ptr = key as *mut OSysCacheKey1;
    let dboid = Oid::from(Datum::from((*key_ptr).keys[0]));

    let databasetup = pg_sys::SearchSysCache1(pg_sys::DATABASEOID as i32, (*key_ptr).keys[0]);
    if databasetup.is_null() {
        pg_sys::ereport!(
            pgrx::PgLogLevel::ERROR,
            pg_sys::errcodes::PgSqlErrorCode::ERRCODE_INTERNAL_ERROR,
            format!("cache lookup failed for database ({})", dboid.to_u32())
        );
    }

    let dbform = get_struct(databasetup) as *mut pg_sys::FormData_pg_database;

    let prev_context = pg_sys::MemoryContextSwitchTo((*database_cache).mcxt);

    let mut o_database = *entry_ptr as *mut ODatabase;
    if !o_database.is_null() {
        panic!("Assert failed: o_database is not null");
    } else {
        o_database = pg_sys::palloc0(std::mem::size_of::<ODatabase>()) as *mut ODatabase;
        *entry_ptr = o_database as *mut c_void;
    }

    (*o_database).data_version = pg_sys::ORIOLEDB_SYS_TREE_VERSION as u16;
    (*o_database).encoding = (*dbform).encoding;
    (*o_database).datlocprovider = (*dbform).datlocprovider;

    let mut is_null = false;

    let datum_collate = pg_sys::SysCacheGetAttr(
        pg_sys::DATABASEOID as i32,
        databasetup,
        pg_sys::Anum_pg_database_datcollate as i32,
        std::ptr::addr_of_mut!(is_null),
    );
    (*o_database).datcollate = if !is_null {
        pg_sys::TextDatumGetCString(datum_collate)
    } else {
        std::ptr::null_mut()
    };

    let datum_locale = pg_sys::SysCacheGetAttr(
        pg_sys::DATABASEOID as i32,
        databasetup,
        pg_sys::Anum_pg_database_datlocale as i32,
        std::ptr::addr_of_mut!(is_null),
    );
    (*o_database).datlocale = if !is_null {
        pg_sys::TextDatumGetCString(datum_locale)
    } else {
        std::ptr::null_mut()
    };

    let datum_icurules = pg_sys::SysCacheGetAttr(
        pg_sys::DATABASEOID as i32,
        databasetup,
        pg_sys::Anum_pg_database_daticurules as i32,
        std::ptr::addr_of_mut!(is_null),
    );
    (*o_database).daticurules = if !is_null {
        pg_sys::TextDatumGetCString(datum_icurules)
    } else {
        std::ptr::null_mut()
    };

    let datum_ctype = pg_sys::SysCacheGetAttr(
        pg_sys::DATABASEOID as i32,
        databasetup,
        pg_sys::Anum_pg_database_datctype as i32,
        std::ptr::addr_of_mut!(is_null),
    );
    (*o_database).datctype = if !is_null {
        pg_sys::TextDatumGetCString(datum_ctype)
    } else {
        std::ptr::null_mut()
    };

    pg_sys::MemoryContextSwitchTo(prev_context);
    pg_sys::ReleaseSysCache(databasetup);
}

unsafe extern "C-unwind" fn o_database_cache_free_entry(entry: *mut c_void) {
    pg_sys::pfree(entry);
}

#[no_mangle]
pub unsafe extern "C-unwind" fn o_database_cache_get_database_encoding() -> i32 {
    let mut cur_lsn: XLogRecPtr = 0;
    let template1_dboid = Oid::from(1);
    o_sys_cache::o_sys_cache_set_datoid_lsn(&mut cur_lsn, std::ptr::null_mut());
    let o_database = o_database_cache_search(
        template1_dboid,
        Datum::from(template1_dboid),
        cur_lsn,
        (*database_cache).nkeys,
    );
    if !o_database.is_null() {
        (*o_database).encoding
    } else {
        pg_sys::PG_SQL_ASCII as i32
    }
}

#[no_mangle]
pub unsafe extern "C-unwind" fn o_database_cache_set_database_encoding() {
    let encoding = o_database_cache_get_database_encoding();
    pg_sys::SetDatabaseEncoding(encoding);
}

#[no_mangle]
pub unsafe extern "C-unwind" fn o_database_cache_set_default_locale_provider() {
    // No-op under unpatched PostgreSQL 18
}

#[no_mangle]
pub unsafe extern "C-unwind" fn o_database_cache_set_lc_collate() {
    let mut cur_lsn: XLogRecPtr = 0;
    let template1_dboid = Oid::from(1);
    o_sys_cache::o_sys_cache_set_datoid_lsn(&mut cur_lsn, std::ptr::null_mut());
    let o_database = o_database_cache_search(
        template1_dboid,
        Datum::from(template1_dboid),
        cur_lsn,
        (*database_cache).nkeys,
    );
    if !o_database.is_null() && !(*o_database).datcollate.is_null() {
        if pg_sys::pg_perm_setlocale(pg_sys::LC_COLLATE as i32, (*o_database).datcollate).is_null() {
            pg_sys::ereport!(
                pgrx::PgLogLevel::FATAL,
                pg_sys::errcodes::PgSqlErrorCode::ERRCODE_INTERNAL_ERROR,
                "database locale is incompatible with operating system",
                format!("The database was initialized with LC_COLLATE \"{:?}\", which is not recognized by setlocale().", std::ffi::CStr::from_ptr((*o_database).datcollate))
            );
        }
    }
}

unsafe extern "C-unwind" fn o_database_cache_serialize_entry(
    entry: *mut c_void,
    len: *mut std::ffi::c_int,
) -> *mut std::ffi::c_char {
    let o_database = entry as *mut ODatabase;

    if (*o_database).data_version != pg_sys::ORIOLEDB_SYS_TREE_VERSION as u16 {
        pg_sys::elog(
            pg_sys::FATAL as i32,
            b"ORIOLEDB_SYS_TREE_VERSION %u of OrioleDB cluster is not among supported for conversion from %u\0"
                .as_ptr() as *const c_char,
            (*o_database).data_version as std::ffi::c_uint,
            pg_sys::ORIOLEDB_SYS_TREE_VERSION as std::ffi::c_uint,
        );
    }

    let mut str_info = std::mem::zeroed::<pg_sys::StringInfoData>();
    pg_sys::initStringInfo(std::ptr::addr_of_mut!(str_info));

    let offset_provider = memoffset::offset_of!(ODatabase, datlocprovider);
    pg_sys::appendBinaryStringInfo(
        std::ptr::addr_of_mut!(str_info),
        o_database as *mut c_char,
        offset_provider as std::ffi::c_int,
    );

    let offset_locale = memoffset::offset_of!(ODatabase, datlocale);
    pg_sys::appendBinaryStringInfo(
        std::ptr::addr_of_mut!(str_info),
        (o_database as *mut c_char).add(offset_provider),
        (offset_locale - offset_provider) as std::ffi::c_int,
    );

    o_serialize_string((*o_database).datlocale, std::ptr::addr_of_mut!(str_info));
    o_serialize_string((*o_database).daticurules, std::ptr::addr_of_mut!(str_info));
    o_serialize_string((*o_database).datcollate, std::ptr::addr_of_mut!(str_info));
    o_serialize_string((*o_database).datctype, std::ptr::addr_of_mut!(str_info));

    *len = str_info.len;
    str_info.data
}

unsafe extern "C-unwind" fn o_database_cache_deserialize_entry(
    _mcxt: pg_sys::MemoryContext,
    data: *mut std::ffi::c_char,
    length: pg_sys::Size,
) -> *mut std::ffi::c_char {
    let mut ptr = data;
    let o_database = pg_sys::palloc0(std::mem::size_of::<ODatabase>()) as *mut ODatabase;

    let offset_provider = memoffset::offset_of!(ODatabase, datlocprovider);
    assert!((ptr as usize - data as usize) + offset_provider <= length as usize);
    std::ptr::copy_nonoverlapping(ptr, o_database as *mut std::ffi::c_char, offset_provider);
    ptr = ptr.add(offset_provider);

    if (*o_database).data_version != pg_sys::ORIOLEDB_SYS_TREE_VERSION as u16 {
        pg_sys::elog(
            pg_sys::FATAL as i32,
            b"ORIOLEDB_SYS_TREE_VERSION %u of OrioleDB cluster is not among supported for conversion to %u\0"
                .as_ptr() as *const c_char,
            (*o_database).data_version as std::ffi::c_uint,
            pg_sys::ORIOLEDB_SYS_TREE_VERSION as std::ffi::c_uint,
        );
    }

    let offset_locale = memoffset::offset_of!(ODatabase, datlocale);
    let len = offset_locale - offset_provider;
    assert!((ptr as usize - data as usize) + len <= length as usize);
    std::ptr::copy_nonoverlapping(ptr, (o_database as *mut std::ffi::c_char).add(offset_provider), len);
    ptr = ptr.add(len);

    let mut ptr_ptr = ptr;
    (*o_database).datlocale = o_deserialize_string(std::ptr::addr_of_mut!(ptr_ptr));
    (*o_database).daticurules = o_deserialize_string(std::ptr::addr_of_mut!(ptr_ptr));

    if (ptr_ptr as usize - data as usize) != length as usize {
        (*o_database).datcollate = o_deserialize_string(std::ptr::addr_of_mut!(ptr_ptr));
    }

    if (ptr_ptr as usize - data as usize) != length as usize {
        (*o_database).datctype = o_deserialize_string(std::ptr::addr_of_mut!(ptr_ptr));
    }

    o_database as *mut std::ffi::c_char
}
