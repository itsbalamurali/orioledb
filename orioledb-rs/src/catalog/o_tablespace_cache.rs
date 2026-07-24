//! Compute data directory prefixes for a given tablespace.
//!
//! Mirrors `src/catalog/o_tablespace_cache.c`. Provides utilities for
//! determining the on-disk location of OrioleDB data files based on a
//! tablespace OID.

use std::ffi::CStr;

/// Name of the OrioleDB data directory within a tablespace.
const ORIOLEDB_DATA_DIR: &str = "orioledb_data";

/// Version directory suffix for tablespaces (empty — cppcheck silence).
const TABLESPACE_VERSION_DIRECTORY: &str = "";

/// Compute the data directory path and database-specific path for a given tablespace.
///
/// If `tablespace` is `0` (invalid/`InvalidOid`), defaults to PostgreSQL's
/// default tablespace (`DEFAULTTABLESPACE_OID`). This covers system trees and
/// trees whose tablespace has not been set yet.
///
/// # Returns
/// A tuple `(prefix, db_prefix)` where:
/// - `prefix` is the data directory path for the tablespace
/// - `db_prefix` is the database-specific path (`prefix/datoid`)
///
/// # API Note
/// The original C API uses output parameters (`char **prefix`, `char **db_prefix`)
/// with a mixed static/allocated buffer pattern. This Rust implementation returns
/// `(String, String)` instead, which is more idiomatic and avoids the manual
/// memory management of the C version.
pub fn o_get_prefixes_for_tablespace(datoid: u32, tablespace: u32) -> (String, String) {
    // Treat InvalidOid as the default tablespace.
    let tablespace = if tablespace == 0 {
        pgrx::pg_sys::DEFAULTTABLESPACE_OID
    } else {
        tablespace
    };

    // Get tablespace location via pg_tablespace_location().
    let location_datum = unsafe {
        pgrx::pg_sys::DirectFunctionCall1(
            pgrx::pg_sys::pg_tablespace_location,
            pgrx::pg_sys::ObjectIdGetDatum(tablespace),
        )
    };
    let location_text = unsafe { pgrx::pg_sys::DatumGetTextP(location_datum) };
    let cstr_ptr = unsafe { pgrx::pg_sys::text_to_cstring(location_text) };
    let location = unsafe { CStr::from_ptr(cstr_ptr).to_string_lossy().into_owned() };

    // Free the C-allocated strings (matches C: pfree(path_str); pfree(path);)
    unsafe {
        pgrx::pg_sys::pfree(cstr_ptr as *mut _);
        pgrx::pg_sys::pfree(location_text as *mut _);
    }

    // Build prefix path.
    // When location is empty, the prefix is just ORIOLEDB_DATA_DIR.
    // Otherwise: location/"/"TABLESPACE_VERSION_DIRECTORY"/"ORIOLODB_DATA_DIR
    // (TABLESPACE_VERSION_DIRECTORY is empty, producing a // which the FS handles).
    let prefix = if location.is_empty() {
        ORIOLEDB_DATA_DIR.to_string()
    } else {
        format!(
            "{}/{}/{}",
            location, TABLESPACE_VERSION_DIRECTORY, ORIOLEDB_DATA_DIR
        )
    };

    // Build database-specific prefix.
    let db_prefix = format!("{}/{}", prefix, datoid);

    (prefix, db_prefix)
}
