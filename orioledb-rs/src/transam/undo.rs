// Undo log management for OrioleDB.
//
// Ported from `include/transam/undo.h` and `src/transam/undo.c`.
//
// Copyright (c) 2021-2026, Oriole DB Inc.
// Copyright (c) 2025-2026, Supabase Inc.

#![allow(non_snake_case)]

use pgrx::pg_sys::XLogRecPtr;
use std::sync::atomic::AtomicU64;

use super::oxid::OXid;

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

/// Invalid undo log location sentinel.
pub const INVALID_UNDO_LOCATION: UndoLocation = 0x2000_0000_0000_0000;
/// Maximum valid undo log location.
pub const MAX_UNDO_LOCATION: UndoLocation = 0x1FFF_FFFF_FFFF_FFFE;
/// Bit-mask for extracting the value portion of an UndoLocation.
pub const UNDO_LOCATION_VALUE_MASK: UndoLocation = 0x1FFF_FFFF_FFFF_FFFF;

pub const ORIOLEDB_UNDO_DATA_ROW_FILENAME_TEMPLATE: &str = "orioledb_undo/%02X%08Xrow";
pub const ORIOLEDB_UNDO_DATA_PAGE_FILENAME_TEMPLATE: &str = "orioledb_undo/%02X%08Xpage";
pub const ORIOLEDB_UNDO_SYSTEM_FILENAME_TEMPLATE: &str = "orioledb_undo/%02X%08Xsystem";
/// Maximum size of a single on-disk undo file.
pub const UNDO_FILE_SIZE: u64 = 0x400_0000;

// ---------------------------------------------------------------------------
// Types
// ---------------------------------------------------------------------------

/// 64-bit undo log position.
pub type UndoLocation = u64;

/// Undo log category.
///
/// Mirrors `UndoLogType` in `include/orioledb.h`.
#[repr(i32)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum UndoLogType {
    /// Sentinel — no undo log.
    None = -1,
    /// Row-level undo for user data modifications.
    Regular = 0,
    /// Page-level undo for user data modifications.
    RegularPageLevel = 1,
    /// Undo for system tree modifications.
    System = 2,
}

/// Total number of active undo log types.
pub const UNDO_LOGS_COUNT: usize = 3;

impl UndoLogType {
    /// Return the page-level undo type corresponding to this type.
    pub fn page_level(self) -> UndoLogType {
        if self == UndoLogType::Regular {
            UndoLogType::RegularPageLevel
        } else {
            self
        }
    }

    pub fn as_index(self) -> usize {
        match self {
            UndoLogType::Regular => 0,
            UndoLogType::RegularPageLevel => 1,
            UndoLogType::System => 2,
            UndoLogType::None => panic!("UndoLogType::None has no index"),
        }
    }
}

/// Check whether an undo location is valid.
pub fn undo_location_is_valid(loc: UndoLocation) -> bool {
    loc & INVALID_UNDO_LOCATION == 0
}

/// Extract the raw value from an undo location.
pub fn undo_location_get_value(loc: UndoLocation) -> UndoLocation {
    loc & UNDO_LOCATION_VALUE_MASK
}

/// Shared-memory metadata for a single undo log.
///
/// Mirrors `UndoMeta` in `include/transam/undo.h`.
#[repr(C)]
pub struct UndoMeta {
    pub last_used_location: AtomicU64,
    pub advance_reserved_location: AtomicU64,
    pub min_proc_reserved_location: AtomicU64,
    pub min_proc_transaction_retain_location: AtomicU64,
    pub min_proc_retain_location: AtomicU64,
    pub min_rewind_retain_location: AtomicU64,
    pub write_in_progress_location: AtomicU64,
    pub written_location: AtomicU64,
    pub cleaned_location: AtomicU64,
    pub last_used_undo_location_when_updated_min_location: AtomicU64,
    pub checkpoint_retain_start_location: AtomicU64,
    pub checkpoint_retain_end_location: AtomicU64,
}

/// Per-process undo stack locations for a single undo log type.
///
/// Mirrors `UndoStackSharedLocations` in `include/transam/undo.h`.
#[repr(C)]
pub struct UndoStackSharedLocations {
    pub location: AtomicU64,
    pub branch_location: AtomicU64,
    pub subxact_location: AtomicU64,
    pub on_commit_location: AtomicU64,
}

/// Per-process undo retain locations.
///
/// Mirrors `UndoRetainSharedLocations` in `include/transam/undo.h`.
#[repr(C)]
pub struct UndoRetainSharedLocations {
    pub location: AtomicU64,
    pub transaction_retain_location: AtomicU64,
}

/// Private undo stack locations (backend-local copy).
///
/// Mirrors `UndoStackLocations` in `include/transam/undo.h`.
#[repr(C)]
#[derive(Copy, Clone)]
pub struct UndoStackLocations {
    pub location: UndoLocation,
    pub branch_location: UndoLocation,
    pub subxact_location: UndoLocation,
    pub on_commit_location: UndoLocation,
}

/// Pending-truncate metadata stored in shared memory.
///
/// Mirrors `PendingTruncatesMeta` in `include/transam/undo.h`.
#[repr(C)]
pub struct PendingTruncatesMeta {
    pub has_retained_undo_location: [bool; UNDO_LOGS_COUNT],
}

// ---------------------------------------------------------------------------
// Extern C declarations
// ---------------------------------------------------------------------------

extern "C" {
    pub static mut oxid_needs_wal_flush: bool;
    pub static mut cur_retain_undo_locations: [UndoLocation; UNDO_LOGS_COUNT];
    pub static mut pending_truncates_meta: *mut PendingTruncatesMeta;

    pub fn undo_shmem_needs() -> usize;
    pub fn undo_shmem_init(buf: *mut u8, found: bool);
    pub fn get_undo_meta_by_type(undo_type: UndoLogType) -> *mut UndoMeta;
    pub fn update_min_undo_locations(
        undo_type: UndoLogType,
        update_written: bool,
        update_cleaned: bool,
    );
    pub fn evict_undo_to_disk(
        undo_type: UndoLogType,
        target_undo_location: UndoLocation,
        min_proc_reserved_location: UndoLocation,
        wait: bool,
    );
    pub fn reserve_undo_size_extended(
        undo_type: UndoLogType,
        size: usize,
        wait_for_undo_location: bool,
    ) -> bool;
    pub fn steal_reserved_undo_size(undo_type: UndoLogType, size: usize);
    pub fn giveup_reserved_undo_size(undo_type: UndoLogType);
    pub fn fsync_undo_range(
        undo_type: UndoLogType,
        from_loc: UndoLocation,
        to_loc: UndoLocation,
        wait_event_info: u32,
    );
    pub fn get_undo_record(
        undo_type: UndoLogType,
        undo_location: *mut UndoLocation,
        size: usize,
    ) -> *mut u8;
    pub fn get_undo_record_unreserved(
        undo_type: UndoLogType,
        undo_location: *mut UndoLocation,
        size: usize,
    ) -> *mut u8;
    pub fn get_reserved_undo_size(undo_type: UndoLogType) -> usize;
    pub fn release_undo_size(undo_type: UndoLogType);
    pub fn release_reserved_undo_location(undo_type: UndoLogType);
    pub fn add_new_undo_stack_item(undo_type: UndoLogType, location: UndoLocation);
    pub fn get_subxact_undo_location(undo_type: UndoLogType) -> UndoLocation;
    pub fn read_shared_undo_locations(
        to: *mut UndoStackLocations,
        from: *mut UndoStackSharedLocations,
    );
    pub fn get_cur_undo_locations(locations: *mut UndoStackLocations, undo_type: UndoLogType);
    pub fn set_cur_undo_locations(undo_type: UndoLogType, locations: *mut UndoStackLocations);
    pub fn reset_cur_undo_locations();
    pub fn orioledb_get_xidless_commit_lsn(wrote_xlog: *mut bool) -> XLogRecPtr;
    pub fn undo_xact_callback(event: u32, arg: *mut std::ffi::c_void);
    pub fn have_current_undo(undo_type: UndoLogType) -> bool;
    pub fn apply_undo_branches(undo_type: UndoLogType, oxid: OXid);
    pub fn apply_undo_stack(
        undo_type: UndoLogType,
        oxid: OXid,
        is_abort: bool,
        is_recovery: bool,
    );
    pub fn precommit_undo_stack(undo_type: UndoLogType, oxid: OXid, is_local: bool);
    pub fn on_commit_undo_stack(undo_type: UndoLogType, oxid: OXid, abort_xact: bool);
    pub fn free_retained_undo_location(undo_type: UndoLogType);
    pub fn undo_read(
        undo_type: UndoLogType,
        location: UndoLocation,
        size: usize,
        buf: *mut u8,
    );
    pub fn undo_read_if_exists(
        undo_type: UndoLogType,
        location: UndoLocation,
        size: usize,
        buf: *mut u8,
    ) -> bool;
    pub fn undo_write(
        undo_type: UndoLogType,
        location: UndoLocation,
        size: usize,
        buf: *const u8,
    );
    pub fn undo_write_if_exists(
        undo_type: UndoLogType,
        location: UndoLocation,
        size: usize,
        buf: *const u8,
    ) -> bool;
    pub fn check_pending_truncates();
}
