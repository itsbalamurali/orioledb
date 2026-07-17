// Page pool implementation for OrioleDB.
//
// Ported from `include/utils/page_pool.h` and `src/utils/page_pool.c`.
//
// Copyright (c) 2021-2026, Oriole DB Inc.
// Copyright (c) 2025-2026, Supabase Inc.

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

/// Minimum number of in-memory pages required for the page pool.
pub const PPOOL_MIN_SIZE: usize = 1024;

/// Reserve slot indices within a page pool.
pub const PPOOL_RESERVE_META: usize = 0;
pub const PPOOL_RESERVE_INSERT: usize = 1;
pub const PPOOL_RESERVE_FIND: usize = 2;
pub const PPOOL_RESERVE_SHARED_INFO_INSERT: usize = 3;
pub const PPOOL_RESERVE_COUNT: usize = 4;

// ---------------------------------------------------------------------------
// Opaque handles
// ---------------------------------------------------------------------------

/// Shared page pool (opaque — only used via raw pointers from C).
pub enum PagePool {}

/// Backend-local page pool (opaque).
pub enum LocalPagePool {}

// ---------------------------------------------------------------------------
// Type aliases
// ---------------------------------------------------------------------------

/// An in-memory block number within an OrioleDB page pool.
pub type OInMemoryBlkno = u32;

/// Sentinel value for an invalid in-memory block number.
pub const INVALID_O_IN_MEMORY_BLKNO: OInMemoryBlkno = u32::MAX;

/// Return `true` when `blkno` is a valid in-memory block number.
pub fn o_in_memory_blkno_is_valid(blkno: OInMemoryBlkno) -> bool {
    blkno != INVALID_O_IN_MEMORY_BLKNO
}

// ---------------------------------------------------------------------------
// Extern C declarations
// ---------------------------------------------------------------------------

extern "C" {
    pub fn ppool_shmem_needs() -> usize;
    pub fn ppool_shmem_init(buf: *mut u8, found: bool);

    pub fn ppool_get_page(pool: *mut PagePool, blkno: OInMemoryBlkno) -> *mut u8;

    pub fn ppool_get_num_pages(pool: *mut PagePool) -> u64;

    pub fn ppool_try_reserve_pages(
        pool: *mut PagePool,
        kind: usize,
        amount: u32,
    ) -> bool;

    pub fn ppool_release_reserve(pool: *mut PagePool, kind: usize);

    pub fn ppool_release_all_reserves(pool: *mut PagePool);

    pub fn get_ppool_by_type(pool_type: u32) -> *mut PagePool;

    pub fn local_ppool_alloc(local_pool: *mut LocalPagePool) -> OInMemoryBlkno;
    pub fn local_ppool_free(local_pool: *mut LocalPagePool, blkno: OInMemoryBlkno);
    pub fn local_ppool_create() -> *mut LocalPagePool;
    pub fn local_ppool_destroy(local_pool: *mut LocalPagePool);
}
