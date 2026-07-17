// B-tree scan (sequential and key-range scans over pages).
//
// Ported from `include/btree/scan.h` and `src/btree/scan.c`.
//
// Copyright (c) 2021-2026, Oriole DB Inc.
// Copyright (c) 2025-2026, Supabase Inc.

// TODO: port scan state, cursor advancement, and qual evaluation.
// Tracked as part of the orioledb -> orioledb-rs port.

/// Placeholder marker so the module is non-empty and documents intent.
pub const SCAN_PORT_PENDING: bool = true;
