// Transaction ID (OXid) management for OrioleDB.
//
// Ported from `include/transam/oxid.h` and `src/transam/oxid.c`.
//
// Copyright (c) 2021-2026, Oriole DB Inc.
// Copyright (c) 2025-2026, Supabase Inc.

#![allow(non_snake_case, non_camel_case_types)]

use pgrx::pg_sys::{
    CommitSeqNo, CommandId, Oid, SubTransactionId, TransactionId, XLogRecPtr,
};
use std::ffi::c_void;

/// Orioledb transaction identifier (64-bit extended XID).
pub type OXid = u64;

/// Commit sequence number — same underlying type as CommitSeqNo.
pub type Csn = CommitSeqNo;

/// Per-slot OXid map item (CSN + commit WAL pointer), stored in shared memory.
#[repr(C)]
pub struct OXidMapItem {
    /// Atomically-updated commit sequence number for this OXid slot.
    pub csn: std::sync::atomic::AtomicU64,
    /// WAL LSN at which this transaction committed.
    pub commit_ptr: std::sync::atomic::AtomicU64,
}

/// Global XID metadata stored in shared memory.
///
/// Mirrors `XidMeta` in `include/transam/oxid.h`.
#[repr(C)]
pub struct XidMeta {
    pub next_xid: std::sync::atomic::AtomicU64,
    pub last_xid_when_updated_global_xmin: std::sync::atomic::AtomicU64,
    pub run_xmin: std::sync::atomic::AtomicU64,
    pub global_xmin: std::sync::atomic::AtomicU64,

    pub write_in_progress_xmin: std::sync::atomic::AtomicU64,
    pub written_xmin: std::sync::atomic::AtomicU64,
    pub checkpoint_retain_xmin: std::sync::atomic::AtomicU64,
    pub checkpoint_retain_xmax: std::sync::atomic::AtomicU64,
    pub cleaned_xmin: std::sync::atomic::AtomicU64,
    pub cleaned_checkpoint_xmin: std::sync::atomic::AtomicU64,
    pub cleaned_checkpoint_xmax: std::sync::atomic::AtomicU64,
}

/// Logical decoding XID context.
///
/// Mirrors `LogicalXidCtx` in `include/transam/oxid.h`.
#[repr(C)]
pub struct LogicalXidCtx {
    /// 32-bit PostgreSQL transaction ID used during logical decoding.
    pub xid: TransactionId,
    /// True if the current logical XID was allocated after heap XID was set.
    pub use_heap: bool,
}

/// OrioleDB snapshot — CSN-based visibility information.
///
/// Mirrors `OSnapshot` in `include/transam/oxid.h`.
#[repr(C)]
pub struct OSnapshot {
    pub csn: CommitSeqNo,
    pub xlogptr: XLogRecPtr,
    pub xmin: XLogRecPtr,
    pub cid: CommandId,
}

/// Context for fetching a table descriptor at a specific catalog version.
///
/// Mirrors `OTableFetchContext` in `include/transam/oxid.h`.
#[repr(C)]
pub struct OTableFetchContext {
    pub snapshot: *mut OSnapshot,
    pub version: u32,
}

impl OTableFetchContext {
    pub fn new(snapshot: *mut OSnapshot, version: u32) -> Self {
        Self { snapshot, version }
    }
}

/// How OrioleDB handles a `SERIALIZABLE` isolation request.
///
/// Mirrors `OSerializableMode` in `include/transam/oxid.h`.
#[repr(C)]
pub enum OSerializableMode {
    /// Coarse `ExclusiveLock` per touched relation (default).
    TableLock = 0,
    /// Reject with `ERRCODE_FEATURE_NOT_SUPPORTED`.
    Error = 1,
    /// Silently downgrade to `REPEATABLE READ`.
    RepeatableRead = 2,
}

// ---------------------------------------------------------------------------
// Extern declarations — resolved at link time from the shared pgrx objects.
// ---------------------------------------------------------------------------

extern "C" {
    pub static mut xid_meta: *mut XidMeta;
    pub static mut o_in_progress_snapshot: OSnapshot;
    pub static mut o_non_deleted_snapshot: OSnapshot;
    pub static mut orioledb_serializable_mode: std::ffi::c_int;

    pub fn oxid_subxact_callback(
        event: u32,
        my_subid: SubTransactionId,
        parent_subid: SubTransactionId,
        arg: *mut c_void,
    );

    pub fn oxid_shmem_needs() -> usize;
    pub fn oxid_init_shmem(ptr: *mut u8, found: bool);
    pub fn wait_for_oxid(oxid: OXid, error_ok: bool) -> bool;
    pub fn oxid_notify(oxid: OXid);
    pub fn oxid_notify_all();
    pub fn advance_oxids(new_xid: OXid);
    pub fn get_current_oxid() -> OXid;
    pub fn assign_subtransaction_logical_xid();
    pub fn set_oxid_csn(oxid: OXid, csn: CommitSeqNo);
    pub fn set_oxid_xlog_ptr(oxid: OXid, ptr: XLogRecPtr);
    pub fn set_current_oxid(oxid: OXid);
    pub fn set_current_logical_xid(ctx: *mut LogicalXidCtx);
    pub fn parallel_worker_set_oxid();
    pub fn reset_current_oxid();
    pub fn get_current_oxid_if_any() -> OXid;
    pub fn get_current_logical_xid() -> TransactionId;
    pub fn get_current_logical_xid_ctx(output: *mut LogicalXidCtx);
    pub fn current_oxid_precommit();
    pub fn current_oxid_xlog_precommit();
    pub fn current_oxid_commit(csn: CommitSeqNo);
    pub fn current_oxid_clear_committing();
    pub fn current_oxid_abort();
    pub fn oxid_get_csn(oxid: OXid, get_raw_csn: bool) -> CommitSeqNo;
    pub fn oxid_get_xlog_ptr(oxid: OXid) -> XLogRecPtr;
    pub fn oxid_match_snapshot(
        oxid: OXid,
        snapshot: *mut OSnapshot,
        out_csn: *mut CommitSeqNo,
        out_ptr: *mut XLogRecPtr,
    );
    pub fn fill_current_oxid_osnapshot(oxid: *mut OXid, snapshot: *mut OSnapshot);
    pub fn fill_current_oxid_osnapshot_no_check(oxid: *mut OXid, snapshot: *mut OSnapshot);
    pub fn oxid_get_procnum(oxid: OXid) -> std::ffi::c_int;
    pub fn xid_is_finished(xid: OXid) -> bool;
    pub fn xid_is_finished_for_everybody(xid: OXid) -> bool;
    pub fn fsync_xidmap_range(xmin: OXid, xmax: OXid, wait_event_info: u32);
    pub fn clear_rewind_oxid(oxid: OXid);
    pub fn csn_is_retained_for_rewind(csn: CommitSeqNo) -> bool;
}

/// Return the current OrioleDB transaction identifier, if any.
///
/// # Safety
/// Calls PostgreSQL C internals; must be called from a backend context.
pub unsafe fn current_oxid_if_any() -> Option<OXid> {
    // InvalidOXid is represented as 0 by convention.
    let oxid = get_current_oxid_if_any();
    if oxid == 0 {
        None
    } else {
        Some(oxid)
    }
}

/// Check whether an `OXid` has completed (committed or aborted).
///
/// # Safety
/// Calls PostgreSQL C internals.
pub unsafe fn oxid_finished(xid: OXid) -> bool {
    xid_is_finished(xid)
}

/// Bit masks for packing lock mode and OXid into `OTupleXactInfo` (u64).
pub mod xact_info {
    pub const LOCK_ONLY_BIT: u64 = 0x1000_0000_0000_0000;
    pub const LOCK_MODE_MASK: u64 = 0x0C00_0000_0000_0000;
    pub const LOCK_OXID_MASK: u64 = 0x03FF_FFFF_FFFF_FFFF;
    pub const LOCK_MODE_SHIFT: u32 = 58;

    pub fn is_lock_only(xact_info: u64) -> bool {
        xact_info & LOCK_ONLY_BIT != 0
    }

    pub fn get_oxid(xact_info: u64) -> u64 {
        xact_info & LOCK_OXID_MASK
    }

    pub fn get_lock_mode(xact_info: u64) -> u64 {
        (xact_info & LOCK_MODE_MASK) >> LOCK_MODE_SHIFT
    }

    pub fn oxid_get_xact_info(oxid: u64, lock_mode: u64, lock_only: bool) -> u64 {
        oxid | (lock_mode << LOCK_MODE_SHIFT) | if lock_only { LOCK_ONLY_BIT } else { 0 }
    }
}

/// Row-level lock strength.
///
/// Mirrors `RowLockMode` in `include/btree/btree.h`.
#[repr(C)]
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub enum RowLockMode {
    KeyShare = 0,
    Share = 1,
    NoKeyUpdate = 2,
    Update = 3,
}

impl RowLockMode {
    /// Return `true` when two lock modes conflict with each other.
    pub fn conflicts_with(self, other: RowLockMode) -> bool {
        (self as u32) + (other as u32) >= 3
    }
}
