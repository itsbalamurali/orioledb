// B-tree page contents (tuple storage, header, leaf/non-leaf data).
//
// Ported from `include/btree/page_contents.h` and `src/btree/page_contents.c`.
//
// Copyright (c) 2021-2026, Oriole DB Inc.
// Copyright (c) 2025-2026, Supabase Inc.

// TODO: port page header accessors, key/tuple extraction, and row image helpers.
// Tracked as part of the orioledb -> orioledb-rs port.

/// Placeholder marker so the module is non-empty and documents intent.
pub const PAGE_CONTENTS_PORT_PENDING: bool = true;
