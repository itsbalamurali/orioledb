/*-------------------------------------------------------------------------
 *
 * logical.rs
 *		Support for logical decoding of OrioleDB tables.
 *
 * Copyright (c) 2024-2026, Oriole DB Inc.
 * Copyright (c) 2025-2026, Supabase Inc.
 *
 * IDENTIFICATION
 *	  orioledb-rs/src/recovery/logical.rs
 *
 *-------------------------------------------------------------------------
 */

use pgrx::pg_sys;

extern "C" {
    pub fn orioledb_decode(
        ctx: *mut pg_sys::LogicalDecodingContext,
        buf: *mut pg_sys::XLogRecordBuffer,
    );
}
