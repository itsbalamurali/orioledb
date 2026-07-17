//! logical.rs
//!
//! Copyright (c) 2021-2026, Oriole DB Inc.
//! Copyright (c) 2025-2026, Supabase Inc.
//!
//! IDENTIFICATION
//!   contrib/orioledb/orioledb-rs/src/recovery/logical.rs

use pgrx::pg_sys;

extern "C" {
    pub fn orioledb_decode(
        ctx: *mut pg_sys::LogicalDecodingContext,
        buf: *mut pg_sys::XLogRecordBuffer,
    );
}
