// Rewind worker implementation.
//
// Ported from `include/rewind/rewind.h` and `src/rewind/rewind.c`.
//
// Copyright (c) 2021-2026, Oriole DB Inc.
// Copyright (c) 2025-2026, Supabase Inc.

use pgrx::pg_sys::{Datum, FullTransactionId, TimestampTz, TransactionId};
use std::sync::atomic::AtomicU64;

use crate::transam::oxid::OXid;
use crate::transam::undo::{UndoLocation, UNDO_LOGS_COUNT};

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

pub const REWIND_FILE_SIZE: u64 = 0x100_0000;
pub const REWIND_BUFFERS_TAG: u32 = 0;

pub const EMPTY_ITEM_TAG: u8 = 0;
pub const REWIND_ITEM_TAG: u8 = 1;
pub const SUBXIDS_ITEM_TAG: u8 = 2;

/// Number of sub-transaction IDs that fit in a single `SubxidsItem`.
pub const SUBXIDS_PER_ITEM: usize = 25;

pub const PG_CTL_CMD_LEN: usize = 8;

/// Size of the on-disk rewind disk buffer (items per page).
pub const REWIND_DISK_BUFFER_LENGTH: usize = 8192 / std::mem::size_of::<RewindItem>();

// ---------------------------------------------------------------------------
// Types
// ---------------------------------------------------------------------------

/// A single entry in the rewind ring buffer.
///
/// Mirrors `RewindItem` in `include/rewind/rewind.h`.
#[repr(C)]
pub struct RewindItem {
    pub tag: u8,
    pub nsubxids: i32,
    pub oxid: OXid,
    /// Regular PostgreSQL transaction ID (if any).
    pub xid: TransactionId,
    pub on_commit_undo_location: [u64; UNDO_LOGS_COUNT],
    pub undo_location: [u64; UNDO_LOGS_COUNT],
    pub min_retain_location: [u64; UNDO_LOGS_COUNT],
    pub oldest_considered_running_xid: FullTransactionId,
    pub run_xmin: OXid,
    pub timestamp: TimestampTz,
}

/// Packed sub-transaction ID item stored in the rewind ring buffer.
///
/// Must be the same size as `RewindItem` so they can be cast to each other.
/// Mirrors `SubxidsItem` in `include/rewind/rewind.h`.
#[repr(C)]
pub struct SubxidsItem {
    pub tag: u8,
    pub nsubxids: i32,
    /// OXid — redundant, kept for debugging.
    pub oxid: OXid,
    pub subxids: [TransactionId; SUBXIDS_PER_ITEM],
}

/// Shared-memory control block for the rewind ring buffer.
#[repr(C)]
pub struct RewindMeta {
    /// Next adding position available for concurrent add process.
    pub add_pos_reserved: AtomicU64,
    /// First position that is not yet added (may not be evicted or read yet).
    pub add_pos_filled_upto: AtomicU64,
    /// Current read position.
    pub read_pos: AtomicU64,
    /// Total capacity of the ring buffer (number of `RewindItem` slots).
    pub capacity: u64,
}

// ---------------------------------------------------------------------------
// Extern C declarations
// ---------------------------------------------------------------------------

extern "C" {
    pub fn orioledb_vacuum_horizon_hook() -> TransactionId;
    pub fn register_rewind_worker();
    pub fn is_rewind_worker() -> bool;
    pub fn rewind_worker_main(arg: Datum);
    pub fn rewind_shmem_needs() -> usize;
    pub fn rewind_init_shmem(buf: *mut u8, found: bool);
    pub fn checkpoint_write_rewind_xids();
    pub fn add_to_rewind_buffer(
        oxid: OXid,
        xid: TransactionId,
        nsubxids: i32,
        subxids: *mut TransactionId,
    );
    pub fn save_precommit_xid_subxids();
    pub fn get_precommit_xid_subxids(
        nsubxids: *mut i32,
        subxids: *mut *mut TransactionId,
    ) -> TransactionId;
    pub fn reset_precommit_xid_subxids();
    pub fn get_rewind_run_xmin() -> OXid;
}

// ---------------------------------------------------------------------------
// Safe wrappers
// ---------------------------------------------------------------------------

/// Register the rewind background worker with PostgreSQL.
///
/// # Safety
/// Must be called during `_PG_init` while shared preload libraries are being processed.
pub unsafe fn register() {
    register_rewind_worker();
}

/// Return the current rewind run-xmin, if the rewind feature is enabled.
///
/// # Safety
/// Reads from shared memory; must be called from a backend context.
pub unsafe fn run_xmin() -> OXid {
    get_rewind_run_xmin()
}
