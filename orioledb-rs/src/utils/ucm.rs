// Usage Count Map (UCM) — page replacement policy helper.
//
// Ported from `include/utils/ucm.h` and `src/utils/ucm.c`.
//
// Copyright (c) 2021-2026, Oriole DB Inc.
// Copyright (c) 2025-2026, Supabase Inc.

use crate::utils::page_pool::OInMemoryBlkno;
use std::sync::atomic::AtomicU32;

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

pub const UCM_INVALID_LEVEL: u32 = 0xF;
pub const UCM_USAGE_LEVELS: u32 = 0x7;
/// Level assigned to free (evictable) pages.
pub const UCM_FREE_PAGES_LEVEL: u32 = 0x7;
/// Total number of levels (usage + free).
pub const UCM_LEVELS: u32 = 0x8;

// ---------------------------------------------------------------------------
// Type
// ---------------------------------------------------------------------------

/// Usage-count map — shared-memory data structure for page-replacement.
///
/// Mirrors `UsageCountMap` in `include/utils/ucm.h`.
#[repr(C)]
pub struct UsageCountMap {
    pub epoch: *mut AtomicU32,
    pub ucm: *mut AtomicU32,
    pub offset: OInMemoryBlkno,
    pub size: OInMemoryBlkno,
    pub total: i32,
    pub non_leaf: i32,
    pub root_factor: i32,
    pub usage_counter: u32,
}

// ---------------------------------------------------------------------------
// Extern C declarations
// ---------------------------------------------------------------------------

extern "C" {
    pub static mut skip_ucm: bool;

    pub fn estimate_ucm_space(
        map: *mut UsageCountMap,
        offset: OInMemoryBlkno,
        size: OInMemoryBlkno,
    ) -> usize;
    pub fn init_ucm(map: *mut UsageCountMap, ptr: *mut u8, found: bool);
    pub fn ucm_inc(map: *mut UsageCountMap, blkno: OInMemoryBlkno, prev: i32, next: i32);
    pub fn page_inc_usage_count(map: *mut UsageCountMap, blkno: OInMemoryBlkno);
    pub fn page_change_usage_count(map: *mut UsageCountMap, blkno: OInMemoryBlkno, usage_count: u32);
    pub fn ucm_check_map(map: *mut UsageCountMap) -> bool;
    pub fn ucm_epoch_needs_shift(map: *mut UsageCountMap) -> bool;
    pub fn ucm_epoch_shift(map: *mut UsageCountMap);
    pub fn ucm_next_blkno(
        map: *mut UsageCountMap,
        init_blkno: OInMemoryBlkno,
        mask_src: u32,
    ) -> OInMemoryBlkno;
    pub fn ucm_occupy_free_page(map: *mut UsageCountMap) -> OInMemoryBlkno;
}
