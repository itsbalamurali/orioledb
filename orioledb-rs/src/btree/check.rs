// B-tree consistency checker.
//
// Ported from `include/btree/check.h` and `src/btree/check.c`.
//
// Copyright (c) 2021-2026, Oriole DB Inc.
// Copyright (c) 2025-2026, Supabase Inc.

use super::btree::BTreeDescr;

extern "C" {
    /// Run a consistency check on `desc`.
    ///
    /// When `force_file_check` is `true`, also verify on-disk pages.
    pub fn check_btree(desc: *mut BTreeDescr, force_file_check: bool, force_check: bool) -> bool;

    /// Verify that compressed pages can be re-decompressed correctly.
    pub fn check_btree_compression(desc: *mut BTreeDescr, deep: bool);
}
