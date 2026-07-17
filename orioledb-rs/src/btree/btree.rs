// Core B-tree types, descriptors, and operations.
//
// Ported from `include/btree/btree.h` and `src/btree/btree.c`.
//
// Copyright (c) 2021-2026, Oriole DB Inc.
// Copyright (c) 2025-2026, Supabase Inc.

use pgrx::pg_sys::{Jsonb, JsonbParseState, JsonbValue, Oid};

use crate::transam::oxid::OXid;
use crate::transam::undo::UndoLogType;
use crate::utils::page_pool::{OInMemoryBlkno, PagePool};
use crate::utils::seq_buf::{OIndexKey, SeqBufDescPrivate};

// ---------------------------------------------------------------------------
// Type aliases
// ---------------------------------------------------------------------------

pub type OTupleXactInfo = u64;
pub type OIndexNumber = u16;
pub type OCompress = i32;

pub const PRIMARY_INDEX_NUMBER: OIndexNumber = 0;
pub const BRIDGE_INDEX_NUMBER: OIndexNumber = 0xFFFD;
pub const TOAST_INDEX_NUMBER: OIndexNumber = 0xFFFE;
pub const INVALID_INDEX_NUMBER: OIndexNumber = 0xFFFF;

pub const MAX_NUM_DIRTY_PARTS: usize = 4;

// ---------------------------------------------------------------------------
// Enums
// ---------------------------------------------------------------------------

/// How keys are interpreted during B-tree traversal and modification.
///
/// Mirrors `BTreeKeyType` in `include/btree/btree.h`.
#[repr(i32)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum BTreeKeyType {
    /// Complete leaf tuple.
    LeafTuple = 0,
    /// Non-leaf (internal) navigation key.
    NonLeafKey = 1,
    /// Search boundary for range scans.
    Bound = 2,
    /// Lower bound for unique-constraint checking.
    UniqueLowerBound = 3,
    /// Upper bound for unique-constraint checking.
    UniqueUpperBound = 4,
    /// Request the leftmost item/page (no comparison performed).
    None = 5,
    /// High key of a page (upper bound of all keys on the page).
    PageHiKey = 6,
    /// Request the rightmost item/page (no comparison performed).
    Rightmost = 7,
}

impl BTreeKeyType {
    pub fn is_bound(self) -> bool {
        matches!(
            self,
            BTreeKeyType::Bound
                | BTreeKeyType::UniqueLowerBound
                | BTreeKeyType::UniqueUpperBound
        )
    }
}

/// Persistence model of a B-tree.
#[repr(i32)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum BTreeStorageType {
    /// In-memory only; no eviction or checkpoint support.
    InMemory = 0,
    /// Can evict to disk but has no checkpoint.
    Temporary = 1,
    /// Like `Persistence` but without WAL for data modifications.
    Unlogged = 2,
    /// Full checkpoint + eviction support.
    Persistence = 3,
}

/// The DML operation that triggered a B-tree callback.
#[repr(i32)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum BTreeOperationType {
    Insert = 0,
    Lock = 1,
    Update = 2,
    Delete = 3,
}

/// Deletion status of a leaf tuple.
#[repr(i32)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum BTreeLeafTupleDeletedStatus {
    NonDeleted = 0,
    Deleted = 1,
    MovedPartitions = 2,
    PkChanged = 3,
}

/// What a modify-callback should do with the tuple it found.
#[repr(i32)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum OBTreeModifyCallbackAction {
    DoNothing = 1,
    Update = 2,
    Delete = 3,
    Lock = 4,
    Undo = 5,
}

/// What a wait-callback should do when it finds a conflicting XID.
#[repr(i32)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum OBTreeWaitCallbackAction {
    XidNoWait = 1,
    XidWait = 2,
    XidExit = 3,
}

/// Result of a B-tree modify operation.
#[repr(i32)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum OBTreeModifyResult {
    Inserted = 1,
    Updated = 2,
    Deleted = 3,
    Locked = 4,
    Found = 5,
    NotFound = 6,
}

// ---------------------------------------------------------------------------
// Core structs
// ---------------------------------------------------------------------------

/// An OrioleDB tuple: a (data pointer, format-flags) pair.
///
/// Mirrors `OTuple` in `include/btree/btree.h`.
#[repr(C)]
#[derive(Clone, Copy)]
pub struct OTuple {
    pub data: *mut u8,
    pub format_flags: u8,
}

impl OTuple {
    pub const NULL: OTuple = OTuple {
        data: std::ptr::null_mut(),
        format_flags: 0,
    };

    pub fn is_null(self) -> bool {
        self.data.is_null()
    }
}

/// Root-page location hint (block number + change count).
///
/// Mirrors `BTreeRootInfo` in `include/btree/btree.h`.
#[repr(C)]
#[derive(Clone, Copy, Debug)]
pub struct BTreeRootInfo {
    pub root_page_blkno: OInMemoryBlkno,
    pub root_page_change_count: u32,
    pub meta_page_blkno: OInMemoryBlkno,
}

/// Pending S3 dirty-part entries for a single data-file.
///
/// Mirrors `BTreeS3PartsInfo` in `include/btree/btree.h`.
#[repr(C)]
#[derive(Clone, Copy, Debug)]
pub struct BTreeS3PartsInfo {
    pub dirty_parts: [DirtyPart; MAX_NUM_DIRTY_PARTS],
    pub write_max_location: u64,
}

/// A single dirty part entry.
#[repr(C)]
#[derive(Clone, Copy, Debug)]
pub struct DirtyPart {
    pub chkp_num: u32,
    pub seg_num: i32,
    pub part_num: i32,
}

/// Backend-local free-extent list for temporary trees.
///
/// Mirrors `BTreeLocalFreeExtents` in `include/btree/btree.h`.
#[repr(C)]
pub struct BTreeLocalFreeExtents {
    pub items: *mut crate::utils::seq_buf::FileExtent,
    pub size: i32,
    pub capacity: i32,
}

/// The set of vtable operations for a B-tree.
///
/// Mirrors `BTreeOps` in `include/btree/btree.h`.
///
/// All function pointers are optional except `len`, `cmp`, and `hash`.
#[repr(C)]
pub struct BTreeOps {
    pub len: unsafe extern "C" fn(
        desc: *mut BTreeDescr,
        tuple: OTuple,
        length_type: i32,
    ) -> i32,
    pub tuple_make_key: unsafe extern "C" fn(
        desc: *mut BTreeDescr,
        tuple: OTuple,
        data: *mut u8,
        keep_version: bool,
        allocated: *mut bool,
    ) -> OTuple,
    pub key_to_jsonb: unsafe extern "C" fn(
        desc: *mut BTreeDescr,
        key: OTuple,
        state: *mut *mut JsonbParseState,
    ) -> *mut JsonbValue,
    pub needs_undo: Option<
        unsafe extern "C" fn(
            desc: *mut BTreeDescr,
            action: BTreeOperationType,
            old_tuple: OTuple,
            old_xact_info: OTupleXactInfo,
            old_deleted: bool,
            new_tuple: OTuple,
            new_oxid: OXid,
        ) -> bool,
    >,
    pub hash: unsafe extern "C" fn(
        desc: *mut BTreeDescr,
        tuple: OTuple,
        tuple_type: BTreeKeyType,
    ) -> u32,
    pub unique_hash: unsafe extern "C" fn(desc: *mut BTreeDescr, tuple: OTuple) -> u32,
    pub cmp: unsafe extern "C" fn(
        desc: *mut BTreeDescr,
        p1: *mut std::ffi::c_void,
        k1: BTreeKeyType,
        p2: *mut std::ffi::c_void,
        k2: BTreeKeyType,
    ) -> i32,
}

/// The main B-tree descriptor — one per index, kept in backend memory.
///
/// Mirrors `BTreeDescr` in `include/btree/btree.h`.
#[repr(C)]
pub struct BTreeDescr {
    pub root_info: BTreeRootInfo,
    pub arg: *mut std::ffi::c_void,
    /// Storage manager handle (opaque union in C).
    pub smgr: [u8; std::mem::size_of::<usize>() * 2],
    pub oids: OIndexKey,
    pub tablespace: Oid,
    /// Index type (oIndexPrimary, oIndexUnique, etc.).
    pub index_type: i32,
    pub ppool: *mut PagePool,
    pub compress: OCompress,
    pub fillfactor: u8,
    pub undo_type: UndoLogType,
    pub storage_type: BTreeStorageType,
    pub free_buf: SeqBufDescPrivate,
    pub next_chkp: [SeqBufDescPrivate; 2],
    pub tmp_buf: [SeqBufDescPrivate; 2],
    pub build_parts_info: [BTreeS3PartsInfo; 2],
    pub create_oxid: OXid,
    pub ops: *mut BTreeOps,
    pub local_free_extents: *mut BTreeLocalFreeExtents,
}

/// Inline location hint for a B-tree leaf tuple.
#[repr(C)]
#[derive(Clone, Copy, Debug)]
pub struct BTreeLocationHint {
    pub blkno: OInMemoryBlkno,
    pub page_change_count: u32,
}

// ---------------------------------------------------------------------------
// Extern C declarations
// ---------------------------------------------------------------------------

extern "C" {
    pub static mut unique_locks: *mut u8; // LWLockPadded*
    pub static mut num_unique_locks: i32;

    pub fn o_btree_check_size_of_tuple(len: i32, relation_name: *mut i8, index: bool);
    pub fn o_btree_init_unique_lwlocks();
    pub fn o_btree_init(descr: *mut BTreeDescr);
    pub fn o_btree_cleanup_pages(
        root: OInMemoryBlkno,
        meta_page_blkno: OInMemoryBlkno,
        root_page_change_count: u32,
    );
    pub fn btree_ctid_get_and_inc(desc: *mut BTreeDescr) -> pgrx::pg_sys::ItemPointerData;
    pub fn btree_bridge_ctid_get_and_inc(
        desc: *mut BTreeDescr,
        overflow: *mut bool,
    ) -> pgrx::pg_sys::ItemPointerData;
    pub fn btree_ctid_update_if_needed(desc: *mut BTreeDescr, ctid: pgrx::pg_sys::ItemPointerData);
    pub fn btree_desc_stopevent_params_internal(
        desc: *mut BTreeDescr,
        state: *mut *mut JsonbParseState,
    );
    pub fn btree_page_stopevent_params(desc: *mut BTreeDescr, p: *mut u8) -> *mut Jsonb;
    pub fn btree_downlink_stopevent_params(
        desc: *mut BTreeDescr,
        p: *mut u8,
        loc: *mut std::ffi::c_void, // BTreePageItemLocator*
    ) -> *mut Jsonb;
    pub fn o_new_rowid(
        primary: *mut std::ffi::c_void, // OIndexDescr*
        slot: *mut pgrx::pg_sys::TupleTableSlot,
        rowid_values: *mut pgrx::pg_sys::Datum,
        rowid_isnull: *mut bool,
        tuple_csn: crate::CommitSeqNo,
        hint: *mut BTreeLocationHint,
    ) -> *mut pgrx::pg_sys::varlena;
}

// ---------------------------------------------------------------------------
// Safe inline wrappers
// ---------------------------------------------------------------------------

/// Call the B-tree length function.
///
/// # Safety
/// `desc` and its vtable must be valid.
pub unsafe fn o_btree_len(desc: *mut BTreeDescr, tuple: OTuple, length_type: i32) -> i32 {
    ((*(*desc).ops).len)(desc, tuple, length_type)
}

/// Call the B-tree comparison function.
///
/// # Safety
/// `desc` and its vtable must be valid.
pub unsafe fn o_btree_cmp(
    desc: *mut BTreeDescr,
    p1: *mut std::ffi::c_void,
    k1: BTreeKeyType,
    p2: *mut std::ffi::c_void,
    k2: BTreeKeyType,
) -> i32 {
    ((*(*desc).ops).cmp)(desc, p1, k1, p2, k2)
}

/// Return `true` when the tree descriptor has valid OIDs.
///
/// Mirrors the `TREE_HAS_OIDS` macro.
pub fn tree_has_oids(desc: &BTreeDescr) -> bool {
    desc.oids.oids.is_valid()
}
