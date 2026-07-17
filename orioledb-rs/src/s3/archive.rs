// S3 WAL archive module integration.
//
// Ported from `src/s3/archive.c`.
//
// Copyright (c) 2024-2026, Oriole DB Inc.
// Copyright (c) 2025-2026, Supabase Inc.

use pgrx::pg_sys::Datum;

extern "C" {
    /// PostgreSQL archive module callback: `_PG_archive_module_init`.
    ///
    /// Returns the `ArchiveModuleCallbacks` struct for this module.
    pub fn _PG_archive_module_init() -> *const std::ffi::c_void;
}

/// Entry point for the S3 archive module.
///
/// Declared `#[no_mangle]` so it can be found by PostgreSQL's dynamic loader.
#[no_mangle]
pub extern "C" fn pg_archive_module_init() -> *const std::ffi::c_void {
    unsafe { _PG_archive_module_init() }
}
