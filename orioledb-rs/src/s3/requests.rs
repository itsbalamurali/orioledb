// S3 HTTP request helpers.
//
// Ported from `include/s3/requests.h` and `src/s3/requests.c`.
//
// Copyright (c) 2024-2026, Oriole DB Inc.
// Copyright (c) 2025-2026, Supabase Inc.

use pgrx::pg_sys::StringInfoData;

// ---------------------------------------------------------------------------
// HTTP response codes used by the S3 request layer.
// ---------------------------------------------------------------------------

pub const S3_RESPONSE_OK: i64 = 200;
pub const S3_RESPONSE_NOT_FOUND: i64 = 404;
pub const S3_RESPONSE_CONDITION_CONFLICT: i64 = 409;
pub const S3_RESPONSE_CONDITION_FAILED: i64 = 412;

extern "C" {
    /// Upload a local file to S3.
    ///
    /// When `if_none_match` is `true`, the operation is conditional (ETag precondition).
    /// Returns the HTTP response code.
    pub fn s3_put_file(
        objectname: *mut std::ffi::c_char,
        filename: *mut std::ffi::c_char,
        if_none_match: bool,
    ) -> i64;

    /// Download an S3 object to a local file.
    pub fn s3_get_file(objectname: *mut std::ffi::c_char, filename: *mut std::ffi::c_char);

    /// Create an empty S3 "directory" marker object.
    pub fn s3_put_empty_dir(objectname: *mut std::ffi::c_char);

    /// Upload one part of a multipart S3 file.
    pub fn s3_put_file_part(
        objectname: *mut std::ffi::c_char,
        filename: *mut std::ffi::c_char,
        partnum: i32,
    ) -> i64;

    /// Download one part of a multipart S3 file.
    pub fn s3_get_file_part(
        objectname: *mut std::ffi::c_char,
        filename: *mut std::ffi::c_char,
        partnum: i32,
    );

    /// Upload an in-memory buffer as an S3 object.
    ///
    /// `data_checksum` is an optional pre-computed SHA-256 hex string; pass
    /// `NULL` to let the function compute it.  When `if_none_match` is `true`
    /// the upload is conditional.
    pub fn s3_put_object_with_contents(
        objectname: *mut std::ffi::c_char,
        data: *mut u8,
        data_size: u64,
        data_checksum: *mut std::ffi::c_char,
        if_none_match: bool,
    ) -> i64;

    /// Download an S3 object into a `StringInfo` buffer.
    ///
    /// When `missing_ok` is `true`, a 404 response is not an error.
    pub fn s3_get_object(
        objectname: *mut std::ffi::c_char,
        str: *mut StringInfoData,
        missing_ok: bool,
    ) -> i64;

    /// Delete an S3 object.
    pub fn s3_delete_object(objectname: *mut std::ffi::c_char);

    /// Read a local file into a palloc-allocated buffer.
    ///
    /// Sets `*size` to the number of bytes read.
    pub fn read_file(filename: *const std::ffi::c_char, size: *mut u64) -> *mut u8;
}
