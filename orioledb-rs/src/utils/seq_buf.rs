// Sequential buffer utilities for OrioleDB.
//
// Ported from `include/utils/seq_buf.h` and `src/utils/seq_buf.c`.
//
// Copyright (c) 2021-2026, Oriole DB Inc.
// Copyright (c) 2025-2026, Supabase Inc.

use pgrx::pg_sys::Oid;
use std::sync::atomic::AtomicU64;

// ---------------------------------------------------------------------------
// Types (mirroring `include/utils/seq_buf.h`)
// ---------------------------------------------------------------------------

/// Validity state of the previous page in a sequential buffer.
#[repr(C)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SeqBufPrevPageState {
    Done = 0,
    InProgress = 1,
    Error = 2,
}

/// Combined OID triple identifying an OrioleDB index.
///
/// Also used as the key for the sequential-buffer files.
#[repr(C)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ORelOids {
    pub datoid: Oid,
    pub reloid: Oid,
    pub relnode: Oid,
}

impl ORelOids {
    /// Return `true` when all three OIDs are non-zero / valid.
    pub fn is_valid(self) -> bool {
        self.datoid.to_u32() != 0 && self.reloid.to_u32() != 0 && self.relnode.to_u32() != 0
    }
}

/// Key that uniquely identifies a sequential buffer on disk.
#[repr(C)]
#[derive(Clone, Copy, Debug)]
pub struct OIndexKey {
    pub oids: ORelOids,
    pub tablespace: Oid,
}

/// Tag for a sequential buffer file (key + sequence number + type char).
#[repr(C)]
#[derive(Clone, Copy, Debug)]
pub struct SeqBufTag {
    pub key: OIndexKey,
    pub num: u32,
    pub tag_type: std::ffi::c_char,
}

impl SeqBufTag {
    pub fn equals(&self, other: &SeqBufTag) -> bool {
        self.key.oids.datoid == other.key.oids.datoid
            && self.key.oids.relnode == other.key.oids.relnode
            && self.num == other.num
            && self.tag_type == other.tag_type
    }
}

/// Shared-memory descriptor for a sequential buffer.
///
/// Mirrors `SeqBufDescShared` in `include/utils/seq_buf.h`.
#[repr(C)]
pub struct SeqBufDescShared {
    /// Spinlock protecting the fields below.
    pub lock: u8,
    /// Pair of in-memory buffer pages.
    pub pages: [u32; 2],
    pub location: i32,
    /// Current page in use from the above pair.
    pub cur_page_num: i32,
    /// File page currently loaded.
    pub file_page_num: u32,
    /// How many unread bytes remain in the file.
    pub free_bytes_num: i64,
    pub evict_offset: i64,
    pub tag: SeqBufTag,
    pub prev_page_state: SeqBufPrevPageState,
}

/// Backend-private sequential buffer descriptor.
///
/// Mirrors `SeqBufDescPrivate` in `include/utils/seq_buf.h`.
#[repr(C)]
pub struct SeqBufDescPrivate {
    pub shared: *mut SeqBufDescShared,
    pub file: i32,
    pub tag: SeqBufTag,
    pub write: bool,
}

/// Data saved when a sequential buffer is evicted to disk.
///
/// Mirrors `EvictedSeqBufData` in `include/utils/seq_buf.h`.
#[repr(C)]
pub struct EvictedSeqBufData {
    pub offset: i64,
    pub tag: SeqBufTag,
}

/// Result of a `seq_buf_try_replace` operation.
#[repr(C)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SeqBufReplaceResult {
    Success = 0,
    Already = 1,
    Error = 2,
}

/// Packed on-disk extent: 16-bit length and 48-bit offset.
#[repr(C)]
#[derive(Clone, Copy, Debug)]
pub struct FileExtent {
    /// Packed `len:16 | off:48` — use accessor methods.
    raw: u64,
}

impl FileExtent {
    const LEN_MASK: u64 = 0xFFFF_0000_0000_0000;
    const OFF_MASK: u64 = 0x0000_FFFF_FFFF_FFFF;

    pub fn len(self) -> u16 {
        ((self.raw & Self::LEN_MASK) >> 48) as u16
    }

    pub fn off(self) -> u64 {
        self.raw & Self::OFF_MASK
    }

    pub fn is_valid(self) -> bool {
        self.len() != 0 && self.off() < Self::OFF_MASK
    }
}

// ---------------------------------------------------------------------------
// Extern C declarations
// ---------------------------------------------------------------------------

extern "C" {
    pub fn init_seq_buf(
        seq_buf_private: *mut SeqBufDescPrivate,
        shared: *mut SeqBufDescShared,
        tag: *mut SeqBufTag,
        write: bool,
        init_shared: bool,
        skip_len: i32,
        evicted: *mut EvictedSeqBufData,
    ) -> bool;

    pub fn seq_buf_write_u32(seq_buf_private: *mut SeqBufDescPrivate, offset: u32) -> bool;
    pub fn seq_buf_read_u32(seq_buf_private: *mut SeqBufDescPrivate, ptr: *mut u32) -> bool;
    pub fn seq_buf_write_file_extent(
        seq_buf_private: *mut SeqBufDescPrivate,
        extent: FileExtent,
    ) -> bool;
    pub fn seq_buf_read_file_extent(
        seq_buf_private: *mut SeqBufDescPrivate,
        extent: *mut FileExtent,
    ) -> bool;
    pub fn seq_buf_finalize(seq_buf_private: *mut SeqBufDescPrivate) -> u64;
    pub fn seq_buf_snapshot_pending_data(
        seq_buf_private: *mut SeqBufDescPrivate,
        buf: *mut std::ffi::c_char,
    ) -> usize;
    pub fn seq_buf_max_pending_data_size() -> usize;
    pub fn get_seq_buf_filename(tag: *mut SeqBufTag) -> *mut std::ffi::c_char;
    pub fn seq_buf_get_offset(seq_buf_private: *mut SeqBufDescPrivate) -> u64;
    pub fn seq_buf_try_replace(
        seq_buf_private: *mut SeqBufDescPrivate,
        tag: *mut SeqBufTag,
        size: *mut AtomicU64,
        data_size: usize,
    ) -> SeqBufReplaceResult;
    pub fn seq_buf_file_exist(tag: *mut SeqBufTag) -> bool;
    pub fn seq_buf_remove_file(tag: *mut SeqBufTag) -> bool;
    pub fn seq_buf_close_file(seq_buf_private: *mut SeqBufDescPrivate);
}
