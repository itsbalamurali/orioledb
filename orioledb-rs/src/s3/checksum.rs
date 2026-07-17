// S3 data-file checksum management.
//
// Ported from `include/s3/checksum.h` and `src/s3/checksum.c`.
//
// Copyright (c) 2024-2026, Oriole DB Inc.
// Copyright (c) 2025-2026, Supabase Inc.

use std::ffi::c_char;

/// Length of a SHA-256 hex digest string (64 hex chars + NUL terminator).
pub const O_SHA256_DIGEST_STRING_LENGTH: usize = 65;

/// Maximum path length reused from PostgreSQL's `MAXPGPATH`.
pub const MAXPGPATH: usize = 1024;

/// Per-file checksum record persisted across checkpoints.
///
/// Mirrors `S3FileChecksum` in `include/s3/checksum.h`.
#[repr(C)]
pub struct S3FileChecksum {
    pub filename: [c_char; MAXPGPATH],
    pub checksum: [c_char; O_SHA256_DIGEST_STRING_LENGTH],
    /// `true` if the checksum changed since the last checkpoint.
    pub changed: bool,
    pub checkpoint_number: u32,
}

/// Transient state used while computing checksums during a checkpoint pass.
///
/// Mirrors `S3ChecksumState` in `include/s3/checksum.h`.
#[repr(C)]
pub struct S3ChecksumState {
    /// Hash-table keyed by filename (managed by palloc/pfree).
    pub hash_table: *mut std::ffi::c_void,
    pub checkpoint_number: u32,
    /// Buffer of `S3FileChecksum` entries (shared with caller).
    pub file_checksums: *mut S3FileChecksum,
    pub file_checksums_max_len: u32,
    pub file_checksums_len: u32,
}

extern "C" {
    /// Create a new `S3ChecksumState`, optionally loading existing checksums
    /// from `filename`.
    pub fn makeS3ChecksumState(
        checkpoint_number: u32,
        file_checksums: *mut S3FileChecksum,
        file_checksums_max_len: u32,
        filename: *const c_char,
    ) -> *mut S3ChecksumState;

    /// Release all memory associated with `state`.
    pub fn freeS3ChecksumState(state: *mut S3ChecksumState);

    /// Persist the current checksum map to `filename`.
    pub fn flushS3ChecksumState(state: *mut S3ChecksumState, filename: *const c_char);

    /// Look up or compute the SHA-256 checksum for the given file data.
    ///
    /// Returns a pointer into `state`'s buffer — do not free it separately.
    pub fn getS3FileChecksum(
        state: *mut S3ChecksumState,
        filename: *const c_char,
        data: *mut u8,
        size: u64,
    ) -> *mut S3FileChecksum;
}
