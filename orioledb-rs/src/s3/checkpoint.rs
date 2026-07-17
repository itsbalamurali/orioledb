// S3 checkpoint integration.
//
// Ported from `include/s3/checkpoint.h` and `src/s3/checkpoint.c`.
//
// Copyright (c) 2024-2026, Oriole DB Inc.
// Copyright (c) 2025-2026, Supabase Inc.

use super::queue::S3TaskLocation;

extern "C" {
    /// Execute a full S3 backup for the given checkpoint.
    ///
    /// `flags`       — PostgreSQL checkpoint flags bitmask.
    /// `max_location`— Wait until all S3 writes up to this location are done.
    pub fn s3_perform_backup(flags: i32, max_location: S3TaskLocation);
}
