// S3 data-file header management (part loading/eviction state).
//
// Ported from `include/s3/headers.h` and `src/s3/headers.c`.
//
// Copyright (c) 2024-2026, Oriole DB Inc.
// Copyright (c) 2025-2026, Supabase Inc.

use crate::utils::seq_buf::OIndexKey;

// ---------------------------------------------------------------------------
// Types
// ---------------------------------------------------------------------------

/// Tag identifying a single on-disk data file part.
///
/// Two tags represent the same file when `datoid`, `relnode`, `tablespace`,
/// `checkpoint_num`, and `seg_num` are all equal.  `reloid` and `ix_type` are
/// tree-level metadata and are **not** part of the equality check.
///
/// Mirrors `S3HeaderTag` in `include/s3/headers.h`.
#[repr(C)]
#[derive(Clone, Copy)]
pub struct S3HeaderTag {
    pub key: OIndexKey,
    pub checkpoint_num: u32,
    pub seg_num: i32,
}

impl S3HeaderTag {
    /// Return `true` if `self` and `other` refer to the same on-disk file.
    pub fn is_same_file(&self, other: &S3HeaderTag) -> bool {
        self.key.oids.datoid == other.key.oids.datoid
            && self.key.oids.relnode == other.key.oids.relnode
            && self.key.tablespace == other.key.tablespace
            && self.checkpoint_num == other.checkpoint_num
            && self.seg_num == other.seg_num
    }
}

/// Loading/eviction state of a single file part.
///
/// Mirrors `S3PartStatus` in `include/s3/headers.h`.
#[repr(i32)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum S3PartStatus {
    NotLoaded = 0,
    Loading = 1,
    Loaded = 2,
    Evicting = 3,
}

// ---------------------------------------------------------------------------
// Extern C declarations
// ---------------------------------------------------------------------------

extern "C" {
    pub static mut s3_headers_buffers_size: i32;

    pub fn s3_headers_shmem_needs() -> usize;
    pub fn s3_headers_shmem_init(buf: *mut u8, found: bool);

    /// Increase the count of currently loaded parts by `inc`.
    pub fn s3_headers_increase_loaded_parts(inc: u64);

    /// Return a generation counter used to detect concurrent evictions.
    pub fn s3_header_get_load_id(tag: S3HeaderTag) -> u32;

    /// Attempt to lock part `index` for this `tag`.
    ///
    /// Returns `false` if the part is already locked; sets `*load_id` on success.
    pub fn s3_header_lock_part(tag: S3HeaderTag, index: i32, load_id: *mut u32) -> bool;

    /// Mark part `index` as loading; returns the previous status.
    pub fn s3_header_mark_part_loading(tag: S3HeaderTag, index: i32) -> S3PartStatus;

    /// Mark part `index` as fully loaded.
    pub fn s3_header_mark_part_loaded(tag: S3HeaderTag, index: i32);

    /// Release the part lock; when `set_dirty` is `true`, the header page is
    /// marked dirty so it will be flushed.
    pub fn s3_header_unlock_part(tag: S3HeaderTag, index: i32, set_dirty: bool);

    /// Atomically claim part `index` for a pending S3 write.
    ///
    /// Returns `false` when the part is already scheduled.
    pub fn s3_header_mark_part_scheduled_for_write(tag: S3HeaderTag, index: i32) -> bool;

    pub fn s3_header_mark_part_writing(tag: S3HeaderTag, index: i32);
    pub fn s3_header_mark_part_written(tag: S3HeaderTag, index: i32);
    pub fn s3_header_mark_part_not_written(tag: S3HeaderTag, index: i32);

    /// Flush all dirty S3 header pages to disk.
    pub fn s3_headers_sync();

    /// Rollback any in-progress S3 header writes after an error.
    pub fn s3_headers_error_cleanup();

    /// Run one eviction cycle, freeing the least-recently-used loaded parts.
    pub fn s3_headers_try_eviction_cycle();
}
