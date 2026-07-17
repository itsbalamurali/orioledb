// S3 background worker for asynchronous uploads and downloads.
//
// Ported from `include/s3/worker.h` and `src/s3/worker.c`.
//
// Copyright (c) 2024-2026, Oriole DB Inc.
// Copyright (c) 2025-2026, Supabase Inc.

use pgrx::pg_sys::Datum;
use std::ffi::c_char;

use super::queue::S3TaskLocation;
use crate::transam::undo::UndoLogType;
use crate::utils::seq_buf::OIndexKey;

/// Path to the file-checksum database kept alongside S3 data.
pub const FILE_CHECKSUMS_FILENAME: &str = "orioledb_data/file_checksums";

/// Task type discriminant stored inside an S3 queue entry.
#[repr(u32)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum S3TaskType {
    WriteFilePart = 0,
    WriteFile = 1,
    WriteWalFile = 2,
    WriteUndoFile = 3,
    WriteEmptyDir = 4,
    WriteRootFile = 5,
    WritePgFile = 6,
    DownlinkLoad = 7,
}

/// Inline S3 task — used to peek at the type before dispatching.
#[repr(C)]
pub struct S3TaskHeader {
    pub task_type: S3TaskType,
    pub chkp_num: u32,
    /// For file-part tasks: `OIndexKey` key.
    pub key: OIndexKey,
    pub seg_num: i32,
    pub part_num: i32,
}

extern "C" {
    pub fn s3_workers_shmem_needs() -> usize;
    pub fn s3_workers_init_shmem(ptr: *mut u8, found: bool);

    /// Register the `num`-th S3 background worker.
    pub fn register_s3worker(num: i32);

    /// Called at the start of a checkpoint to prepare S3 worker state.
    pub fn s3_workers_checkpoint_init();

    /// Called at the end of a checkpoint to finalise S3 worker state.
    pub fn s3_workers_checkpoint_finish();

    /// Entry point for S3 background worker processes (called by PostgreSQL).
    pub fn s3worker_main(arg: Datum);

    /// Schedule an upload of a whole file to S3.
    ///
    /// When `delete` is `true`, the local file is removed after uploading.
    pub fn s3_schedule_file_write(
        chkp_num: u32,
        filename: *mut c_char,
        delete: bool,
    ) -> S3TaskLocation;

    /// Schedule an upload of an empty-directory marker to S3.
    pub fn s3_schedule_empty_dir_write(
        chkp_num: u32,
        dirname: *mut c_char,
    ) -> S3TaskLocation;

    /// Schedule an upload of one data-file part to S3.
    pub fn s3_schedule_file_part_write(
        chkp_num: u32,
        key: OIndexKey,
        seg_num: i32,
        part_num: i32,
    ) -> S3TaskLocation;

    /// Schedule an upload of a WAL segment to S3.
    pub fn s3_schedule_wal_file_write(filename: *mut c_char) -> S3TaskLocation;

    /// Schedule an upload of an undo file to S3.
    pub fn s3_schedule_undo_file_write(undo_type: UndoLogType, file_num: u64) -> S3TaskLocation;

    /// Schedule an on-demand download of a B-tree downlink.
    pub fn s3_schedule_downlink_load(
        desc: *mut std::ffi::c_void, // BTreeDescr*
        downlink: u64,
    ) -> S3TaskLocation;

    /// Schedule an upload of a root-level metadata file.
    pub fn s3_schedule_root_file_write(filename: *mut c_char, delete: bool) -> S3TaskLocation;

    /// Schedule an upload of a PostgreSQL-level (non-BTree) file.
    pub fn s3_schedule_pg_file_write(chkp_num: u32, filename: *mut c_char) -> S3TaskLocation;

    /// Synchronously download a single data-file part from S3.
    pub fn s3_load_file_part(chkp_num: u32, key: OIndexKey, seg_num: i32, part_num: i32);

    /// Synchronously download the checkpoint map file for `key` from S3.
    pub fn s3_load_map_file(chkp_num: u32, key: OIndexKey);
}
