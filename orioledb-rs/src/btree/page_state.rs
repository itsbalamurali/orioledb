// B-tree page state management (locking, usage counts).
//
// Ported from `include/btree/page_state.h` and `src/btree/page_state.c`.
//
// Copyright (c) 2021-2026, Oriole DB Inc.
// Copyright (c) 2025-2026, Supabase Inc.

use crate::utils::page_pool::OInMemoryBlkno;

// ---------------------------------------------------------------------------
// Page-state bit-field constants
// ---------------------------------------------------------------------------

pub const PAGE_STATE_LOCKED_FLAG: u64 = 0x0000_0000_0004_0000;
pub const PAGE_STATE_NO_READ_FLAG: u64 = 0x0000_0000_0008_0000;
pub const PAGE_STATE_CHANGE_COUNT_ONE: u64 = 0x0000_0000_0010_0000;
pub const PAGE_STATE_CHANGE_COUNT_MASK: u64 = 0x000F_FFFF_FF00_0000;
pub const PAGE_STATE_CHANGE_NON_WAITERS_MASK: u64 = 0x000F_FFFF_FFFC_0000;
pub const PAGE_STATE_CHANGE_USAGE_COUNT_MASK: u64 = 0x00F0_0000_0000_0000;
pub const PAGE_STATE_CHANGE_USAGE_COUNT_ONE: u64 = 0x0010_0000_0000_0000;
pub const PAGE_STATE_CHANGE_USAGE_COUNT_SHIFT: u32 = 52;
pub const PAGE_STATE_LIST_TAIL_MASK: u64 = 0x0000_0000_0003_FFFF;
pub const PAGE_STATE_INVALID_PROCNO: u64 = PAGE_STATE_LIST_TAIL_MASK;

/// Maximum items that fit in a single B-tree page chunk.
pub const BTREE_PAGE_MAX_CHUNK_ITEMS: usize = 64;
/// Maximum items involved in a split (two chunks).
pub const BTREE_PAGE_MAX_SPLIT_ITEMS: usize = 2 * BTREE_PAGE_MAX_CHUNK_ITEMS;

// ---------------------------------------------------------------------------
// Page-state helpers (mirror the C macros as inline Rust functions)
// ---------------------------------------------------------------------------

pub fn page_state_is_locked(state: u64) -> bool {
    state & PAGE_STATE_LOCKED_FLAG != 0
}

pub fn page_state_lock(state: u64) -> u64 {
    state | PAGE_STATE_LOCKED_FLAG
}

pub fn page_state_block_read(state: u64) -> u64 {
    state | PAGE_STATE_LOCKED_FLAG | PAGE_STATE_NO_READ_FLAG
}

pub fn page_state_read_is_blocked(state: u64) -> bool {
    state & PAGE_STATE_NO_READ_FLAG != 0
}

pub fn page_state_get_usage_count(state: u64) -> u64 {
    (state & PAGE_STATE_CHANGE_USAGE_COUNT_MASK) >> PAGE_STATE_CHANGE_USAGE_COUNT_SHIFT
}

pub fn page_state_set_usage_count(state: u64, usage_count: u64) -> u64 {
    (state & !PAGE_STATE_CHANGE_USAGE_COUNT_MASK)
        | (usage_count << PAGE_STATE_CHANGE_USAGE_COUNT_SHIFT)
}

// ---------------------------------------------------------------------------
// Enum: locking result when locking a page alongside a tuple
// ---------------------------------------------------------------------------

/// Result of `lock_page_with_tuple`.
///
/// Mirrors `OLockPageWithTupleResult` in `include/btree/page_state.h`.
#[repr(i32)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum OLockPageWithTupleResult {
    /// Page locked successfully; the target tuple was found.
    Found = 0,
    /// Page locked successfully; the target tuple was not found.
    NotFound = 1,
    /// The lock attempt was skipped (page concurrently modified).
    Skipped = 2,
}

// ---------------------------------------------------------------------------
// Extern C declarations
// ---------------------------------------------------------------------------

extern "C" {
    pub fn page_state_shmem_needs() -> usize;
    pub fn page_state_shmem_init(buf: *mut u8, found: bool);
    pub fn have_locked_pages() -> bool;

    /// Return the number of waiters holding tuple-lock pins on the split
    /// range, filling `procnums` with their process numbers.
    pub fn get_waiters_with_tuples(
        desc: *mut std::ffi::c_void, // BTreeDescr*
        blkno: OInMemoryBlkno,
        procnums: *mut i32,
    ) -> i32;

    /// Signal all waiters in `procnums` that their tuples have been inserted.
    pub fn mark_waiter_tuples_inserted(procnums: *mut i32, n: i32);

    pub fn lock_page(blkno: OInMemoryBlkno);

    pub fn lock_page_with_tuple(
        desc: *mut std::ffi::c_void,
        blkno: OInMemoryBlkno,
        tuple: *const u8,
        tuple_len: i32,
    ) -> OLockPageWithTupleResult;

    pub fn relock_page(blkno: OInMemoryBlkno);
    pub fn try_lock_page(blkno: OInMemoryBlkno) -> bool;
    pub fn delare_page_as_locked(blkno: OInMemoryBlkno);
    pub fn page_is_locked(blkno: OInMemoryBlkno) -> bool;
    pub fn page_block_reads(blkno: OInMemoryBlkno);
    pub fn unlock_page(blkno: OInMemoryBlkno);
    pub fn unlock_page_after_split(blkno: OInMemoryBlkno);
    pub fn release_all_page_locks();
    pub fn page_wait_for_read_enable(blkno: OInMemoryBlkno);

    pub fn btree_register_inprogress_split(right_blkno: OInMemoryBlkno);
    pub fn btree_unregister_inprogress_split(right_blkno: OInMemoryBlkno);
    pub fn btree_mark_incomplete_splits();
    pub fn btree_split_mark_finished(right_blkno: OInMemoryBlkno, use_lock: bool, in_recovery: bool);
}
