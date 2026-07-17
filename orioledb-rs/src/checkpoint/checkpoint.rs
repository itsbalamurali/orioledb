// Checkpoint execution engine.
//
// Ported from `include/checkpoint/checkpoint.h` and `src/checkpoint/checkpoint.c`.
//
// Copyright (c) 2021-2026, Oriole DB Inc.
// Copyright (c) 2025-2026, Supabase Inc.

use pgrx::pg_sys::{Oid, XLogRecPtr};

use crate::transam::oxid::OXid;
use crate::transam::undo::{UndoLocation, UndoLogType, UndoStackLocations};
use crate::utils::page_pool::OInMemoryBlkno;
use crate::utils::seq_buf::ORelOids;
use crate::rewind::rewind::RewindItem;

/// Format string for per-checkpoint XID files.
pub const XID_FILENAME_FORMAT: &str = "orioledb_data/%u.xid";

// ---------------------------------------------------------------------------
// Types
// ---------------------------------------------------------------------------

/// Discriminant for records inside an XID file.
///
/// Mirrors `XidRecKind` in `include/checkpoint/checkpoint.h`.
#[repr(i32)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum XidRecKind {
    /// Row-level undo record for regular undo log.
    UndoRegular = 0,
    /// Row-level undo record for regular page-level undo log.
    UndoRegularPageLevel = 1,
    /// Row-level undo record for system undo log.
    UndoSystem = 2,
    /// Rewind variant of the regular undo log record.
    RewindUndoRegular = 3,
    /// Rewind variant of the regular page-level undo log record.
    RewindUndoRegularPageLevel = 4,
    /// Rewind variant of the system undo log record.
    RewindUndoSystem = 5,
    /// Transaction with a pending secondary-index fixup at checkpoint time.
    PendingSkFixup = 6,
}

/// A single record written to the XID file during a checkpoint.
///
/// Mirrors `XidFileRec` in `include/checkpoint/checkpoint.h`.
#[repr(C)]
#[derive(Clone, Copy)]
pub struct XidFileRec {
    pub oxid: OXid,
    pub kind: XidRecKind,
    pub undo_location: UndoStackLocations,
    pub retain_location: UndoLocation,
}

/// Opaque handle for checkpoint shared-memory state (concrete layout lives in C).
pub enum CheckpointState {}

// ---------------------------------------------------------------------------
// Extern C declarations
// ---------------------------------------------------------------------------

extern "C" {
    pub static mut checkpoint_state: *mut CheckpointState;

    pub fn checkpoint_shmem_size() -> usize;
    pub fn checkpoint_shmem_init(ptr: *mut u8, found: bool);

    /// Return the most recent checkpoint number for a given relation.
    pub fn o_get_latest_chkp_num(
        datoid: Oid,
        relnode: Oid,
        max_chkp_num: u32,
        found: *mut bool,
    ) -> u32;

    /// Record that `chkp_num` is the latest checkpoint for a relation.
    pub fn o_update_latest_chkp_num(datoid: Oid, relnode: Oid, chkp_num: u32);

    /// Remove the latest-checkpoint-number entry for a relation.
    pub fn o_delete_chkp_num(datoid: Oid, relnode: Oid);

    /// Execute a full OrioleDB checkpoint at WAL position `redo_pos`.
    pub fn o_perform_checkpoint(redo_pos: XLogRecPtr, flags: i32);

    /// Post-checkpoint cleanup hook (called by bgwriter after PostgreSQL finishes).
    pub fn o_after_checkpoint_cleanup_hook(checkpoint_redo: XLogRecPtr, flags: i32);

    /// Return `true` if `blkno` is currently being checkpointed.
    pub fn page_is_under_checkpoint(
        desc: *mut std::ffi::c_void, // BTreeDescr*
        blkno: OInMemoryBlkno,
        including_hikey_blkno: bool,
    ) -> bool;

    /// Return `true` if any page of `desc`'s tree is being checkpointed.
    pub fn tree_is_under_checkpoint(desc: *mut std::ffi::c_void) -> bool;

    /// Return the checkpoint number that covers `blkno` in `desc`.
    pub fn get_checkpoint_number(
        desc: *mut std::ffi::c_void,
        blkno: OInMemoryBlkno,
        checkpoint_number: *mut u32,
        copy_blkno: *mut bool,
    ) -> bool;

    /// Return the current checkpoint number for a relation.
    pub fn get_cur_checkpoint_number(
        oids: *mut ORelOids,
        index_type: u32,
        checkpoint_concurrent: *mut bool,
    ) -> u32;

    /// Return `true` when free extents from `chkp_num` can still be reused.
    pub fn can_use_checkpoint_extents(desc: *mut std::ffi::c_void, chkp_num: u32) -> bool;

    /// Free a disk extent produced by checkpoint `chkp_num`.
    pub fn free_extent_for_checkpoint(
        desc: *mut std::ffi::c_void,
        extent: *mut crate::utils::seq_buf::FileExtent,
        chkp_num: u32,
    );

    /// Adjust this backend's autonomous-level hint in the checkpoint state.
    pub fn backend_set_autonomous_level(state: *mut CheckpointState, level: u32);

    /// Return `true` when table data files exist for the given OIDs/tablespace.
    pub fn tbl_data_exists(oids: *mut ORelOids, tablespace: Oid) -> bool;

    /// Initialise an evictable (non-checkpointable) B-tree descriptor.
    pub fn evictable_tree_init(
        desc: *mut std::ffi::c_void,
        init_shmem: bool,
        was_evicted: *mut bool,
    );

    /// Initialise a checkpointable B-tree descriptor.
    pub fn checkpointable_tree_init(
        desc: *mut std::ffi::c_void,
        init_shmem: bool,
        was_evicted: *mut bool,
    );

    /// Release resources held by a checkpointable B-tree descriptor.
    pub fn checkpointable_tree_free(desc: *mut std::ffi::c_void);

    /// Signal that a system-tree modification is starting.
    pub fn systrees_modify_start();

    /// Signal that a system-tree modification has finished.
    pub fn systrees_modify_end(any_wal: bool);

    /// Undo-callback for system-tree modifications during recovery.
    pub fn systrees_lock_callback(
        undo_type: UndoLogType,
        location: UndoLocation,
        base_item: *mut std::ffi::c_void, // UndoStackItem*
        oxid: OXid,
        stage: u32,
        change_counts_valid: bool,
    );

    /// Append an `XidFileRec` to the shared XID queue.
    pub fn write_to_xids_queue(rec: *mut XidFileRec);

    /// Append a `RewindItem` to the checkpoint's rewind log.
    pub fn checkpoint_write_rewind_item(rewind_item: *mut RewindItem);
}
