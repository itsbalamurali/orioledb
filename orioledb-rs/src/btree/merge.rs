// B-tree page merge — combine under-full sibling pages.
//
// Ported from `include/btree/merge.h` and `src/btree/merge.c`.
//
// Copyright (c) 2021-2026, Oriole DB Inc.
// Copyright (c) 2025-2026, Supabase Inc.

use crate::utils::page_pool::OInMemoryBlkno;

extern "C" {
    /// Attempt to merge the page at `blkno` with its right sibling.
    ///
    /// Returns `true` if the merge succeeded and the pages were unlocked.
    pub fn btree_try_merge_pages(
        desc: *mut std::ffi::c_void, // BTreeDescr*
        blkno: OInMemoryBlkno,
        context: *mut std::ffi::c_void,
        has_right_neighbour: *mut bool,
    ) -> bool;

    /// Attempt to merge and unconditionally unlock `blkno`.
    pub fn btree_try_merge_and_unlock(
        desc: *mut std::ffi::c_void,
        blkno: OInMemoryBlkno,
        nested: bool,
    ) -> bool;

    /// Return `true` when page `p` is sparse enough to be a merge candidate.
    pub fn is_page_too_sparse(desc: *mut std::ffi::c_void, p: *mut u8) -> bool;
}
