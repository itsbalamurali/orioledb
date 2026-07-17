// B-tree undo handling (per-page undo chains, rollback of tree edits).
//
// Ported from `include/btree/undo.h` and `src/btree/undo.c`.
//
// Copyright (c) 2021-2026, Oriole DB Inc.
// Copyright (c) 2025-2026, Supabase Inc.

// TODO: port per-page undo application and tree-edit rollback.
// Tracked as part of the orioledb -> orioledb-rs port.

/// Placeholder marker so the module is non-empty and documents intent.
pub const UNDO_PORT_PENDING: bool = true;
