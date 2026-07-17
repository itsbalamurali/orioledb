// Control file for OrioleDB checkpoints.
//
// Ported from `include/checkpoint/control.h` and `src/checkpoint/control.c`.
//
// Copyright (c) 2024-2026, Oriole DB Inc.
// Copyright (c) 2025-2026, Supabase Inc.

use pgrx::pg_sys::XLogRecPtr;
use crate::CommitSeqNo;

use crate::transam::oxid::OXid;
use crate::transam::undo::{UndoLocation, UNDO_LOGS_COUNT};

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

/// Path to the OrioleDB control file.
pub const CONTROL_FILENAME: &str = "orioledb_data/control";

/// Binary format version stored in `CheckpointControl::control_file_version`.
pub const ORIOLEDB_CHECKPOINT_CONTROL_VERSION: u32 = 1;

/// Physical size of the control file on disk (kept constant across versions).
pub const CHECKPOINT_CONTROL_FILE_SIZE: usize = 8192;

/// Number of undo logs covered by `CheckpointControl::undo_info`.
pub const NUM_CHECKPOINTABLE_UNDO_LOGS: usize = 2;

// ---------------------------------------------------------------------------
// Types
// ---------------------------------------------------------------------------

/// Undo-log information saved per-checkpoint.
///
/// Mirrors `CheckpointUndoInfo` in `include/checkpoint/control.h`.
#[repr(C)]
#[derive(Clone, Copy, Debug)]
pub struct CheckpointUndoInfo {
    pub last_undo_location: UndoLocation,
    pub checkpoint_retain_start_location: UndoLocation,
    pub checkpoint_retain_end_location: UndoLocation,
}

/// Full checkpoint control block persisted to `orioledb_data/control`.
///
/// **IMPORTANT:** The `crc` field must always remain last; the layout between
/// `control_file_version` and `crc` may change between versions.
///
/// Mirrors `CheckpointControl` in `include/checkpoint/control.h`.
#[repr(C)]
#[derive(Clone, Copy)]
pub struct CheckpointControl {
    pub control_identifier: u64,
    pub last_checkpoint_number: u32,
    pub control_file_version: u32,
    pub last_csn: CommitSeqNo,
    pub last_xid: OXid,
    pub last_undo_location: UndoLocation,
    pub toast_consistent_ptr: XLogRecPtr,
    pub replay_start_ptr: XLogRecPtr,
    pub sys_trees_start_ptr: XLogRecPtr,
    pub mmap_data_length: u64,
    pub undo_info: [CheckpointUndoInfo; NUM_CHECKPOINTABLE_UNDO_LOGS],
    pub checkpoint_retain_start_location: UndoLocation,
    pub checkpoint_retain_end_location: UndoLocation,
    pub checkpoint_retain_xmin: OXid,
    pub checkpoint_retain_xmax: OXid,
    pub binary_version: u32,
    pub s3_mode: bool,
    /// CRC of all preceding fields — must be last.
    pub crc: u32,
}

// ---------------------------------------------------------------------------
// Extern C declarations
// ---------------------------------------------------------------------------

extern "C" {
    /// Read and validate the control file into `control`.
    ///
    /// Returns `false` when the file does not exist.
    pub fn get_checkpoint_control_data(control: *mut CheckpointControl) -> bool;

    /// Validate a control block read from disk, reporting errors via `ereport`.
    pub fn check_checkpoint_control(control: *mut CheckpointControl);

    /// Persist `control` to `orioledb_data/control`.
    pub fn write_checkpoint_control(control: *mut CheckpointControl);
}
