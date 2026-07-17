// Buffered file-access layer (OBuffers).
//
// Ported from `include/utils/o_buffers.h` and `src/utils/o_buffers.c`.
//
// Copyright (c) 2021-2026, Oriole DB Inc.
// Copyright (c) 2025-2026, Supabase Inc.

use std::ffi::c_char;

/// Maximum number of file "tags" a single `OBuffersDesc` can manage.
pub const O_BUFFERS_MAX_TAGS: usize = 4;

// ---------------------------------------------------------------------------
// Opaque handles (sizes unknown to Rust — only used via pointers)
// ---------------------------------------------------------------------------

/// Shared-memory metadata page for an OBuffers group.
pub enum OBuffersMeta {}

/// A single buffer group (one shared-memory page pool slot).
pub enum OBuffersGroup {}

// ---------------------------------------------------------------------------
// Descriptor
// ---------------------------------------------------------------------------

/// Descriptor for an OBuffers file-backed buffer pool.
///
/// The `filename_template` and `*_tranche_name` fields must point to static
/// C strings; they are owned by the C code.
///
/// Mirrors `OBuffersDesc` in `include/utils/o_buffers.h`.
#[repr(C)]
pub struct OBuffersDesc {
    /// Maximum size of a single underlying file (bytes).
    pub single_file_size: u64,
    /// Printf-style filename templates, one per tag.
    pub filename_template: [*const c_char; O_BUFFERS_MAX_TAGS],
    /// LWLock tranche names.
    pub group_ctl_tranche_name: *const c_char,
    pub buffer_ctl_tranche_name: *const c_char,
    pub buffers_count: u32,

    // Fields initialised by the C layer — do not set from Rust.
    pub groups_count: u32,
    pub meta_page_blkno: *mut OBuffersMeta,
    pub groups: *mut OBuffersGroup,
    pub cur_file: i32,
    pub cur_file_name: [c_char; 1024], // MAXPGPATH
    pub cur_file_tag: u32,
    pub cur_file_num: u64,
}

// ---------------------------------------------------------------------------
// Extern C declarations
// ---------------------------------------------------------------------------

extern "C" {
    pub fn o_buffers_shmem_needs(desc: *mut OBuffersDesc) -> usize;
    pub fn o_buffers_shmem_init(desc: *mut OBuffersDesc, buf: *mut std::ffi::c_void, found: bool);

    /// Read `size` bytes from position `offset` in file `tag` into `buf`.
    ///
    /// Returns `false` (and does nothing) when `if_exists` is `true` and the
    /// file does not yet exist.
    pub fn o_buffers_read(
        desc: *mut OBuffersDesc,
        buf: *mut u8,
        tag: u32,
        offset: i64,
        size: i64,
        if_exists: bool,
    ) -> bool;

    /// Write `size` bytes from `buf` at position `offset` in file `tag`.
    ///
    /// When `mark_clean` is `true`, the written pages are marked clean after
    /// the write (useful for checkpoint-style flushes).
    pub fn o_buffers_write(
        desc: *mut OBuffersDesc,
        buf: *mut u8,
        tag: u32,
        offset: i64,
        size: i64,
        if_exists: bool,
        mark_clean: bool,
    ) -> bool;

    /// Write a full page directly to the file, bypassing the buffer pool.
    pub fn o_buffers_write_page_direct(
        desc: *mut OBuffersDesc,
        data: *mut c_char,
        tag: u32,
        offset: i64,
    );

    /// Sync a range of pages in `tag` from `from_offset` to `to_offset`.
    pub fn o_buffers_sync(
        desc: *mut OBuffersDesc,
        tag: u32,
        from_offset: i64,
        to_offset: i64,
        wait_event_info: u32,
    );

    /// Unlink backing blocks in the given file range.
    pub fn o_buffers_unlink_blocks_range(
        desc: *mut OBuffersDesc,
        tag: u32,
        first_block_number: i64,
        last_block_number: i64,
    );

    /// Unlink blocks that are no longer retained by any checkpoint or xact.
    pub fn unlink_unretained_o_buffers(
        desc: *mut OBuffersDesc,
        tag: u32,
        items_per_block: i64,
        cleanup_start: i64,
        cleanup_end: i64,
        chkp_retain_start: i64,
        chkp_retain_end: i64,
        transaction_retain_start: i64,
    );
}
