// Catalog module: o_tablespace_cache.
//
// Ported from `include/catalog/o_tablespace_cache.h` and `src/catalog/o_tablespace_cache.c`.
//
// Copyright (c) 2021-2026, Oriole DB Inc.
// Copyright (c) 2025-2026, Supabase Inc.

use std::ffi::{c_char, c_void, CStr, CString};
use pgrx::pg_sys::{self, Oid};

const ORIOLEDB_DATA_DIR: &str = "orioledb_data";

#[cfg(feature = "pg18")]
const PG_MAJOR_VERSION: &str = "18";

// Fallback/default if none is defined
#[cfg(not(any(feature = "pg15", feature = "pg16", feature = "pg17", feature = "pg18", feature = "pg19")))]
const PG_MAJOR_VERSION: &str = "18";

unsafe fn get_tablespace_version_directory() -> String {
    format!("PG_{}_{}", PG_MAJOR_VERSION, pg_sys::CATALOG_VERSION_NO)
}

extern "C-unwind" {
    pub fn pg_tablespace_location(fcinfo: pg_sys::FunctionCallInfo) -> pg_sys::Datum;
}

#[no_mangle]
pub unsafe extern "C-unwind" fn o_get_prefixes_for_tablespace(
    datoid: Oid,
    mut tablespace: Oid,
    prefix: *mut *mut c_char,
    db_prefix: *mut *mut c_char,
) {
    static mut PATHBUF: [c_char; 1024] = [0; 1024];

    if tablespace.to_u32() == 0 {
        tablespace = Oid::from(pg_sys::DEFAULTTABLESPACE_OID);
    }

    let path_datum = pg_sys::DirectFunctionCall1Coll(
        Some(pg_tablespace_location),
        pg_sys::InvalidOid,
        pg_sys::Datum::from(tablespace),
    );

    let path = pg_sys::pg_detoast_datum_packed(path_datum.value() as *mut pg_sys::varlena) as *mut pg_sys::text;
    let path_str_ptr = pg_sys::text_to_cstring(path);
    let path_str = CStr::from_ptr(path_str_ptr);

    if path_str.to_bytes().is_empty() {
        let dir_cstr = CString::new(ORIOLEDB_DATA_DIR).unwrap();
        libc::snprintf(
            PATHBUF.as_mut_ptr(),
            PATHBUF.len(),
            b"%s\0".as_ptr() as *const c_char,
            dir_cstr.as_ptr(),
        );
    } else {
        let ts_dir = get_tablespace_version_directory();
        let ts_dir_cstr = CString::new(ts_dir).unwrap();
        let dir_cstr = CString::new(ORIOLEDB_DATA_DIR).unwrap();
        libc::snprintf(
            PATHBUF.as_mut_ptr(),
            PATHBUF.len(),
            b"%s/%s/%s\0".as_ptr() as *const c_char,
            path_str_ptr,
            ts_dir_cstr.as_ptr(),
            dir_cstr.as_ptr(),
        );
    }

    pg_sys::pfree(path_str_ptr as *mut c_void);
    pg_sys::pfree(path as *mut c_void);

    if !prefix.is_null() {
        *prefix = PATHBUF.as_mut_ptr();
    }
    if !db_prefix.is_null() {
        let pathbuf_str = CStr::from_ptr(PATHBUF.as_ptr()).to_string_lossy();
        let db_path = format!("{}/{}", pathbuf_str, datoid.to_u32());
        let db_path_cstr = CString::new(db_path).unwrap();
        *db_prefix = pg_sys::pstrdup(db_path_cstr.as_ptr());
    }
}
