// S3 control and lock file management.
//
// Ported from `include/s3/control.h` and `src/s3/control.c`.
//
// Copyright (c) 2024-2026, Oriole DB Inc.
// Copyright (c) 2025-2026, Supabase Inc.

extern "C" {
    /// Check the S3 control file for compatibility.
    ///
    /// On mismatch, sets `*errmsgp` and `*errdetailp` to static C strings
    /// describing the problem and returns `false`.
    pub fn s3_check_control(
        errmsgp: *mut *const std::ffi::c_char,
        errdetailp: *mut *const std::ffi::c_char,
    ) -> bool;

    /// Write the S3 lock file so that no other instance starts concurrently.
    pub fn s3_put_lock_file();

    /// Remove the S3 lock file on clean shutdown.
    pub fn s3_delete_lock_file();
}
