// B-tree page split (leaf and non-leaf splits, redistribution).
//
// Ported from `include/btree/split.h` and `src/btree/split.c`.
//
// Copyright (c) 2021-2026, Oriole DB Inc.
// Copyright (c) 2025-2026, Supabase Inc.

// TODO: port split decision, page redistribution, and parent update.
// Tracked as part of the orioledb -> orioledb-rs port.

/// Placeholder marker so the module is non-empty and documents intent.
pub const SPLIT_PORT_PENDING: bool = true;
