// B-tree index build routines.
//
// Ported from `include/btree/build.h` and `src/btree/build.c`.
//
// Copyright (c) 2021-2026, Oriole DB Inc.
// Copyright (c) 2025-2026, Supabase Inc.

use pgrx::pg_sys::{IndexInfo, Relation};

extern "C" {
    /// Build a B-tree index from an existing heap relation.
    pub fn o_index_build(
        table: Relation,
        index: Relation,
        index_info: *mut IndexInfo,
        parallel: bool,
    );
}
