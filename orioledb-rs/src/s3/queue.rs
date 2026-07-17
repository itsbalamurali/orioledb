// S3 task queue.
//
// Ported from `include/s3/queue.h` and `src/s3/queue.c`.
//
// Copyright (c) 2024-2026, Oriole DB Inc.
// Copyright (c) 2025-2026, Supabase Inc.

/// Sentinel value for an invalid / unset S3 task location.
pub const INVALID_S3_TASK_LOCATION: S3TaskLocation = u64::MAX;

/// Opaque position in the S3 work queue.
pub type S3TaskLocation = u64;

extern "C" {
    pub fn s3_queue_shmem_needs() -> usize;
    pub fn s3_queue_init_shmem(ptr: *mut u8, found: bool);
    pub fn s3_queue_get_insert_location() -> S3TaskLocation;
    pub fn s3_queue_put_task(data: *mut u8, len: u32) -> S3TaskLocation;
    pub fn s3_queue_try_pick_task() -> S3TaskLocation;
    pub fn s3_queue_get_task(task_location: S3TaskLocation) -> *mut u8;
    pub fn s3_queue_erase_task(task_location: S3TaskLocation);
    pub fn s3_queue_wait_for_location(location: S3TaskLocation);
}
