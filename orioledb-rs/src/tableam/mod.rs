/*-------------------------------------------------------------------------
 *
 * mod.rs
 *		Table Access Method modules declaration for OrioleDB.
 *
 * Copyright (c) 2021-2026, Oriole DB Inc.
 * Copyright (c) 2025-2026, Supabase Inc.
 *
 *-------------------------------------------------------------------------
 */

pub mod bitmap_scan;
pub mod descr;
pub mod func;
pub mod handler;
pub mod index_scan;
pub mod key_bitmap;
pub mod key_range;
pub mod operations;
pub mod radix_selftest;
pub mod scan;
pub mod tree;
pub mod vacuum;
