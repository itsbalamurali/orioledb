/*-------------------------------------------------------------------------
 *
 * mod.rs
 *		Recovery module declarations.
 *
 * Copyright (c) 2021-2026, Oriole DB Inc.
 * Copyright (c) 2025-2026, Supabase Inc.
 *
 * IDENTIFICATION
 *	  orioledb-rs/src/recovery/mod.rs
 *
 *-------------------------------------------------------------------------
 */

pub mod worker;
pub mod recovery;
pub mod logical;
pub mod wal;
pub mod wal_reader;
