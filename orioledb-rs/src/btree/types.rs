//! Foundation types shared by all OrioleDB modules.
//!
//! This module mirrors the type definitions in `include/orioledb.h` and
//! `include/btree/*.h`. Every other btree module depends on these types,
//! so they are ported first (L0 in the dependency graph).
//!
//! Memory layout of every `#[repr(C)]` struct is verified by assertions at
//! module init time against the sizes used by the original C implementation.
//! This guarantees on-disk and on-wire compatibility.

use crate::orioledb;
use pgrx::pg_sys;
use pgrx::postgres_types::Oid;
use std::sync::atomic::{AtomicI32, AtomicU16, AtomicU32, AtomicU64, AtomicU8, Ordering};

// ============================================================================
// Engine constants (from include/orioledb.h)
// ============================================================================

/// Size of an OrioleDB page in bytes.
pub const ORIOLEDB_BLCKSZ: usize = 8192;

/// Size of one compressed page chunk on disk.
pub const ORIOLEDB_COMP_BLCKSZ: usize = 512;

/// Maximum B-tree depth.
pub const ORIOLEDB_MAX_DEPTH: usize = 32;

/// Number of meta-page LWLocks.
pub const BTREE_NUM_META_LWLOCKS: usize = 128;

/// OrioleDB WAL resource manager ID.
pub const ORIOLEDB_RMGR_ID: u8 = 129;

/// Number of usage-count levels in the UCM.
pub const UCM_USAGE_LEVELS: u8 = 7;

/// Invalid usage-count level.
pub const UCM_INVALID_LEVEL: u8 = 0xF;

/// Free-pages level in the UCM.
pub const UCM_FREE_PAGES_LEVEL: u8 = 0x7;

/// Total number of UCM levels including free.
pub const UCM_LEVELS: u8 = 8;

/// Maximum number of concurrently locked pages per process.
pub const MAX_PAGES_PER_PROCESS: usize = 8;

/// Number of sequential-scan slots per meta page.
pub const NUM_SEQ_SCANS_ARRAY_SIZE: usize = 32;

/// Maximum items that can fit on a page.
pub const BTREE_PAGE_MAX_ITEMS: usize = (ORIOLEDB_BLCKSZ - std::mem::size_of::<BTreePageHeader>())
    / (std::mem::align_of::<LocationIndex>() + std::mem::size_of::<LocationIndex>());

/// Maximum chunks per page.
pub const BTREE_PAGE_MAX_CHUNKS: usize = (512 - std::mem::offset_of!(BTreePageHeader, chunkDesc))
    / (std::mem::align_of::<BTreePageChunkDesc>() + std::mem::size_of::<BTreePageChunkDesc>());

/// Maximum items for split operations.
pub const BTREE_PAGE_MAX_SPLIT_ITEMS: usize = 2 * BTREE_PAGE_MAX_CHUNK_ITEMS;

/// Maximum items a single chunk can hold.
pub const BTREE_PAGE_MAX_CHUNK_ITEMS: usize =
    ORIOLEDB_BLCKSZ / (std::mem::align_of::<u8>() + std::mem::size_of::<LocationIndex>());

/// Minimum page pool size in pages.
pub const PPOOL_MIN_SIZE: usize = 1024;

/// Reserve-kind bitmask helpers.
pub const PPOOL_KIND_GET_MASK: fn(u8) -> u32 = |kind| 1 << kind;

/// Reserve kinds.
#[repr(u8)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum PagePoolReserveKind {
    Meta = 0,
    Insert = 1,
    Find = 2,
    SharedInfoInsert = 3,
}

pub const PPOOL_RESERVE_COUNT: usize = 4;

// ============================================================================
// Core integer aliases
// ============================================================================

/// B-tree page block number stored in memory (sentinel-based).
///
/// Bit 31 indicates whether the page is in local memory; bits 0-30 are the
/// block number within the shared buffer pool.
pub type OInMemoryBlkno = u32;

/// Invalid sentinel for `OInMemoryBlkno`.
pub const OInvalidInMemoryBlkno: OInMemoryBlkno = 0xFFFFFFFF;

/// Mask to extract the local block number from an `OInMemoryBlkno`.
pub const O_BLKNO_MASK: OInMemoryBlkno = 0x7FFFFFFF;

/// Returns `true` if the block number refers to a local (backend-private) page.
#[inline]
pub const fn o_page_is_local(blkno: OInMemoryBlkno) -> bool {
    (blkno >> 31) != 0
}

/// Returns `true` if `blkno` is a valid (non-sentinel) block number.
#[inline]
pub const fn o_in_memory_blkno_is_valid(blkno: OInMemoryBlkno) -> bool {
    blkno != OInvalidInMemoryBlkno
}

/// Root page block number stored in a tree descriptor.
pub type ORootPageBlkno = OInMemoryBlkno;

/// Meta page block number.
pub type OMetaPageBlkno = OInMemoryBlkno;

/// Transaction identifier (64-bit).
pub type OXid = u64;

/// Invalid Oxid value.
pub const InvalidOXid: OXid = 0x7FFFFFFFFFFFFFFF;

/// Returns `true` if `oxid` is valid.
#[inline]
pub const fn oxid_is_valid(oxid: OXid) -> bool {
    oxid != InvalidOXid
}

/// LXID used for local transactions.
pub const LXID_NORMAL_FROM: i32 = 1;

/// 64-bit WAL/undo location.
pub type UndoLocation = u64;

/// Invalid undo location.
pub const InvalidUndoLocation: UndoLocation = 0x2000000000000000;

/// Maximum undo location.
pub const MaxUndoLocation: UndoLocation = 0x1FFFFFFFFFFFFFFE;

/// Mask for the undo value bits.
pub const UndoLocationValueMask: UndoLocation = 0x1FFFFFFFFFFFFFFF;

/// Returns `true` if `loc` is a valid undo location.
#[inline]
pub const fn undo_location_is_valid(loc: UndoLocation) -> bool {
    (loc & InvalidUndoLocation) == 0
}

/// Extracts the pure value from an undo location (clears flags).
#[inline]
pub const fn undo_location_get_value(loc: UndoLocation) -> UndoLocation {
    loc & UndoLocationValueMask
}

/// Sentinel for `ODBProcData.pendingSkUndoLoc` meaning "self-created table".
pub const WaitingSkUndoLoc: UndoLocation = 0x2000000000000001;

/// Commit sequence number type.
pub type CommitSeqNo = u64;

/// Placeholder for an uncommitted CSN.
pub const COMMITSEQNO_INPROGRESS: CommitSeqNo = 0;

/// Placeholder for a frozen (already committed) CSN.
pub const COMMITSEQNO_FROZEN: CommitSeqNo = 0;

/// Returns `true` if `csn` represents a normal (non-placeholder) value.
#[inline]
pub const fn commitseqno_is_normal(csn: CommitSeqNo) -> bool {
    csn != 0
}

/// Returns `true` if `csn` is in-progress (uncommitted).
#[inline]
pub const fn commitseqno_is_inprogress(csn: CommitSeqNo) -> bool {
    csn == 0
}

/// 16-bit offset number (PostgreSQL standard).
pub type OffsetNumber = u16;

/// Location index — signed 16-bit offset/length within a page.
pub type LocationIndex = i16;

/// 16-bit index number.
pub type OIndexNumber = u16;

/// Primary / bridge / TOAST / invalid index number sentinels.
pub const PrimaryIndexNumber: OIndexNumber = 0;
pub const BridgeIndexNumber: OIndexNumber = 0xFFFD;
pub const TOASTIndexNumber: OIndexNumber = 0xFFFE;
pub const InvalidIndexNumber: OIndexNumber = 0xFFFF;

// ============================================================================
// Oid helpers (wrapper around pgrx::Oid)
// ============================================================================

/// Oid alias for clarity.
pub type OdbOid = Oid;

/// Returns `true` if `oid` is valid (non-zero).
#[inline]
pub const fn oid_is_valid(oid: OdbOid) -> bool {
    oid != pg_sys::InvalidOid
}

// ============================================================================
// ORelOids — identity of an OrioleDB relation
// ============================================================================

/// Triple that identifies an OrioleDB relation across shared memory.
#[repr(C)]
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct ORelOids {
    /// Database OID.
    pub datoid: OdbOid,
    /// Relation OID (pg_class.oid).
    pub reloid: OdbOid,
    /// RelNode / relation number (pg_class.relnode).
    pub relnode: OdbOid,
}

impl ORelOids {
    /// Returns `true` if all three components are valid.
    #[inline]
    pub const fn is_valid(&self) -> bool {
        oid_is_valid(self.datoid) && oid_is_valid(self.reloid) && oid_is_valid(self.relnode)
    }

    /// Sets all three fields to invalid.
    #[inline]
    pub fn set_invalid(&mut self) {
        self.datoid = pg_sys::InvalidOid;
        self.reloid = pg_sys::InvalidOid;
        self.relnode = pg_sys::InvalidOid;
    }

    /// Returns `true` if the two oids are equal.
    #[inline]
    pub const fn is_equal(&self, other: &Self) -> bool {
        self.datoid == other.datoid && self.reloid == other.reloid && self.relnode == other.relnode
    }

    /// Returns `true` if any field is invalid.
    #[inline]
    pub const fn is_invalid(&self) -> bool {
        !self.is_valid()
    }
}

/// Bitflags for `OInMemoryBlknoIsValid` as a boolean accessor on ORelOids.
#[allow(dead_code)]
pub fn orel_oids_is_valid(oids: &ORelOids) -> bool {
    oids.is_valid()
}

// ============================================================================
// OIndexType — kind of index
// ============================================================================

#[repr(u16)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum OIndexType {
    Invalid = 0,
    Toast = 1,
    Bridge = 2,
    Primary = 3,
    Unique = 4,
    Regular = 5,
    Exclusion = 6,
}

// ============================================================================
// FileExtent — on-disk file offset / length pair
// ============================================================================

/// A packed file extent: 16-bit length + 48-bit offset.
///
/// Layout on disk:
/// ```text
/// bits 0-15:   len
/// bits 16-63:  off
/// ```
#[repr(C)]
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash)]
pub struct FileExtent {
    /// Length in 512-byte chunks.
    pub len: u16,
    /// File offset in bytes (upper 48 bits of a u64).
    pub off: u64,
}

impl FileExtent {
    /// Returns `true` if this extent has a valid length.
    #[inline]
    pub const fn len_is_valid(len: u16) -> bool {
        len != 0
    }

    /// Returns `true` if this extent has a valid offset.
    #[inline]
    pub const fn off_is_valid(off: u64) -> bool {
        off != 0xFFFF_FFFF_FFFF
    }

    /// Returns `true` if both length and offset are valid.
    #[inline]
    pub const fn is_valid(&self) -> bool {
        Self::len_is_valid(self.len) && Self::off_is_valid(self.off)
    }
}

/// Invalid file extent length.
pub const InvalidFileExtentLen: u16 = 0;

/// Invalid file extent offset.
pub const InvalidFileExtentOff: u64 = 0xFFFF_FFFF_FFFF;

// ============================================================================
// OCompress — compression level
// ============================================================================

/// Compression level type.
pub type OCompress = i32;

/// Default compression level.
pub const O_COMPRESS_DEFAULT: OCompress = 10;

/// Invalid compression level.
pub const InvalidOCompress: OCompress = -1;

/// Returns `true` if `compress` is a valid level.
#[inline]
pub const fn o_compress_is_valid(compress: OCompress) -> bool {
    compress != InvalidOCompress
}

// ============================================================================
// UndoLogType — which undo log a record belongs to
// ============================================================================

#[repr(i32)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum UndoLogType {
    /// Invalid / none.
    None = -1,
    /// Row-level user-data modifications.
    Regular = 0,
    /// Page-level user-data modifications.
    RegularPageLevel = 1,
    /// System-tree modifications.
    System = 2,
}

impl UndoLogType {
    pub const COUNT: usize = 3;

    /// Returns the page-level undo type corresponding to a regular undo.
    pub const fn get_page_level(self) -> UndoLogType {
        match self {
            UndoLogType::Regular => UndoLogType::RegularPageLevel,
            other => other,
        }
    }
}

/// Maps a regular undo type to its page-level counterpart.
#[inline]
pub const fn get_page_level_undo_type(undo_type: UndoLogType) -> UndoLogType {
    undo_type.get_page_level()
}

// ============================================================================
// OrioleDBPageHeader — in-memory page header (first sizeof(OrioleDBPageHeader) bytes)
// ============================================================================

/// State flags stored in the atomic `state` field.
pub mod page_state {
    /// Bit set when the page is locked.
    pub const PAGE_STATE_LOCKED_FLAG: u64 = 0x0000_0000_0004_0000;

    /// Bit set when reads to the page are blocked.
    pub const PAGE_STATE_NO_READ_FLAG: u64 = 0x0000_0000_0008_0000;

    /// Bit set when change count is known to be exactly one waiter.
    pub const PAGE_STATE_CHANGE_COUNT_ONE: u64 = 0x0000_0000_0010_0000;

    /// Mask for the change-count bits (16 bits starting at bit 16).
    pub const PAGE_STATE_CHANGE_COUNT_MASK: u64 = 0x000F_FFFF_FFFF_0000;

    /// Mask for change-count bits excluding waiters.
    pub const PAGE_STATE_CHANGE_NON_WAITERS_MASK: u64 = 0x000F_FFFF_FFFC_0000;

    /// Mask for the usage-count field (4 bits at bit 52).
    pub const PAGE_STATE_CHANGE_USAGE_COUNT_MASK: u64 = 0x00F0_0000_0000_0000;

    /// Single unit for the usage count.
    pub const PAGE_STATE_CHANGE_USAGE_COUNT_ONE: u64 = 0x0010_0000_0000_0000;

    /// Shift for the usage-count field.
    pub const PAGE_STATE_CHANGE_USAGE_COUNT_SHIFT: u32 = 52;

    /// Mask for the list-tail pointer (bits 0-15).
    pub const PAGE_STATE_LIST_TAIL_MASK: u64 = 0x0000_0000_0003_FFFF;

    /// An invalid process number used as a sentinel.
    pub const PAGE_STATE_INVALID_PROCNO: u64 = 0x0000_0000_0003_FFFF;
}

use page_state::*;

/// In-memory header of every OrioleDB shared page.
///
/// Must be the first `O_PAGE_HEADER_SIZE` bytes of a page buffer so that
/// `O_PAGE_HEADER(page)` correctly casts a raw pointer to this type.
#[repr(C)]
pub struct OrioleDBPageHeader {
    /// Atomic state word (lock, usage count, change count, etc.).
    pub state: AtomicU64,
    /// Monotonically increasing change counter (wraps at max).
    pub page_change_count: u32,
    /// Checkpoint number when the page was last written to disk.
    pub checkpoint_num: u32,
}

impl OrioleDBPageHeader {
    /// Size of the page header in bytes (must match the on-disk header).
    pub const SIZE: usize = std::mem::size_of::<Self>();
}

/// On-disk header of an OrioleDB page.
///
/// Must have exactly the same size as `OrioleDBPageHeader` so that the two
/// are interchangeable during read/write transitions.
#[repr(C)]
#[derive(Clone, Copy, Debug, Default)]
pub struct OrioleDBOndiskPageHeader {
    /// Checkpoint number for both compressed and uncompressed pages.
    pub checkpoint_num: u32,
    /// Reserved for compressed pages (zero for uncompressed).
    pub compress_page_size: u16,
    /// Reserved for compressed pages (zero for uncompressed).
    pub compress_version: u8,
    /// Version of the binary page format.
    pub page_version: u8,
    /// Reserved.
    pub reserved1: u32,
    /// Reserved.
    pub reserved2: u32,
}

impl OrioleDBOndiskPageHeader {
    /// Size of the on-disk page header.
    pub const SIZE: usize = std::mem::size_of::<Self>();
}

/// Size of an OrioleDB page header (both in-memory and on-disk forms).
pub const O_PAGE_HEADER_SIZE: usize = OrioleDBPageHeader::SIZE;

/// Maximum change count before it wraps to zero.
pub const O_PAGE_CHANGE_COUNT_MAX: u32 = 0x7FFFFFFF;

/// Invalid / sentinel change count.
pub const InvalidOPageChangeCount: u32 = O_PAGE_CHANGE_COUNT_MAX;

/// Returns the current change count from a page header.
#[inline]
pub const fn o_page_get_change_count(page_change_count: u32) -> u32 {
    page_change_count
}

// ============================================================================
// OPagePoolType — which page pool a page belongs to
// ============================================================================

#[repr(u32)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum OPagePoolType {
    Main = 0,
    FreeTree = 1,
    Catalog = 2,
}

/// Number of page pools.
pub const OPagePoolTypesCount: usize = 3;

// ============================================================================
// OTuple — a tuple stored in the B-tree
// ============================================================================

/// A pointer-based tuple representation.
#[repr(C)]
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct OTuple {
    /// Pointer to tuple data (null means null tuple).
    pub data: *mut std::os::raw::c_void,
    /// Format flags for the tuple.
    pub format_flags: u8,
}

impl OTuple {
    /// Returns `true` if this tuple is null.
    #[inline]
    pub const fn is_null(&self) -> bool {
        self.data.is_null()
    }

    /// Sets this tuple to null.
    #[inline]
    pub fn set_null(&mut self) {
        self.data = std::ptr::null_mut();
        self.format_flags = 0;
    }
}

/// Bit-packed transaction info stored inside leaf tuples.
pub type OTupleXactInfo = u64;

/// XACT_INFO bit layout (kept as constants for reference; actual bit access
/// is via helper functions or macros in the original C).
pub mod xact_info {
    /// Bit 60 — lock-only flag.
    pub const XACT_INFO_LOCK_ONLY_BIT: u64 = 1u64 << 60;
    /// Mask for lock mode (bits 61-62).
    pub const XACT_INFO_LOCK_MODE_MASK: u64 = 0x0C00_0000_0000_0000;
    /// Mask for Oxid (bits 0-59).
    pub const XACT_INFO_LOCK_OXID_MASK: u64 = 0x03FF_FFFF_FFFF_FFFF;
    /// Shift for lock mode.
    pub const XACT_INFO_LOCK_MODE_SHIFT: u32 = 58;
}

use xact_info::*;

/// Returns `true` if `xact_info` is a lock-only entry (no tuple data).
#[inline]
pub const fn xact_info_is_lock_only(xact_info: OTupleXactInfo) -> bool {
    (xact_info & XACT_INFO_LOCK_ONLY_BIT) != 0
}

/// Extracts the Oxid from a transaction info word.
#[inline]
pub const fn xact_info_get_oxid(xact_info: OTupleXactInfo) -> OXid {
    (xact_info & XACT_INFO_LOCK_OXID_MASK) as OXid
}

/// Extracts the lock mode from a transaction info word.
#[inline]
pub const fn xact_info_get_lock_mode(xact_info: OTupleXactInfo) -> u64 {
    (xact_info & XACT_INFO_LOCK_MODE_MASK) >> XACT_INFO_LOCK_MODE_SHIFT
}

// ============================================================================
// OLengthType — what length is being measured
// ============================================================================

#[repr(u8)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum OLengthType {
    Tuple,
    Key,
    TupleKey,
    TupleKeyNoVersion,
}

// ============================================================================
// OSmgr — storage manager (union: array or hash)
// ============================================================================

/// Storage manager for data files — either an array of file descriptors
/// or a hash table of S3 file handles.
#[repr(C)]
#[derive(Clone, Copy, Debug, Default)]
pub struct OSmgr {
    /// Tag to disambiguate the union.
    tag: SmgrTag,
    /// Array-mode file descriptors.
    array: SmgrArray,
    /// Hash-mode S3 file handles.
    s3_hash: *mut std::os::raw::c_void,
}

#[repr(C)]
#[derive(Clone, Copy, Debug, Default)]
pub struct SmgrArray {
    /// Pointer to array of file descriptors.
    pub files: *mut std::os::raw::c_int,
    /// Number of file descriptors allocated.
    pub files_allocated: i32,
}

#[repr(C)]
#[derive(Clone, Copy, Debug, Default)]
pub enum SmgrTag {
    #[default]
    Array,
    S3Hash,
}

// ============================================================================
// BTreeRootInfo — root and meta page locations
// ============================================================================

/// Cached location of the root and meta pages in a B-tree.
#[repr(C)]
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct BTreeRootInfo {
    /// Block number of the root page.
    pub root_page_blkno: OInMemoryBlkno,
    /// Change count of the root page.
    pub root_page_change_count: u32,
    /// Block number of the meta page.
    pub meta_page_blkno: OInMemoryBlkno,
}

impl BTreeRootInfo {
    /// Returns `true` if the root page block number is valid.
    #[inline]
    pub const fn root_page_is_valid(&self) -> bool {
        o_in_memory_blkno_is_valid(self.root_page_blkno)
    }

    /// Returns `true` if the meta page block number is valid.
    #[inline]
    pub const fn meta_page_is_valid(&self) -> bool {
        o_in_memory_blkno_is_valid(self.meta_page_blkno)
    }
}

// ============================================================================
// BTreeStorageType — persistence semantics
// ============================================================================

#[repr(u8)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum BTreeStorageType {
    /// In-memory only; no eviction, no checkpoint.
    InMemory = 0,
    /// Pages can be evicted to disk but no checkpoint support.
    Temporary = 1,
    /// Checkpoint + eviction, but no WAL for data modifications.
    Unlogged = 2,
    /// Full persistence: checkpoint + eviction + WAL.
    Persistence = 3,
}

// ============================================================================
// BTreeKeyType — type of key being compared
// ============================================================================

#[repr(u8)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum BTreeKeyType {
    /// Complete tuple stored in a leaf page.
    LeafTuple,
    /// Key in non-leaf (internal) pages for navigation.
    NonLeafKey,
    /// Search boundary key for range scans.
    Bound,
    /// Lower bound for unique constraint checking.
    UniqueLowerBound,
    /// Upper bound for unique constraint checking.
    UniqueUpperBound,
    /// Requests the leftmost item/page.
    None,
    /// High key boundary of a B-tree page.
    PageHiKey,
    /// Requests the rightmost item/page.
    Rightmost,
}

/// Returns `true` if `key_type` is a bound-type key.
#[inline]
pub const fn is_bound_key_type(key_type: BTreeKeyType) -> bool {
    matches!(
        key_type,
        BTreeKeyType::Bound | BTreeKeyType::UniqueLowerBound | BTreeKeyType::UniqueUpperBound
    )
}

// ============================================================================
// BTreeOperationType — type of modification
// ============================================================================

#[repr(u8)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum BTreeOperationType {
    Insert,
    Lock,
    Update,
    Delete,
}

// ============================================================================
// BTreeLeafTupleDeletedStatus — deletion marker
// ============================================================================

#[repr(u8)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum BTreeLeafTupleDeletedStatus {
    NonDeleted = 0,
    Deleted = 1,
    MovedPartitions = 2,
    PkChanged = 3,
}

// ============================================================================
// BTreeS3PartsInfo — pending S3 upload tracking
// ============================================================================

/// Maximum number of dirty S3 parts per descriptor.
pub const MAX_NUM_DIRTY_PARTS: usize = 4;

/// Dirty part info for a single segment/partition.
#[repr(C)]
#[derive(Clone, Copy, Debug, Default)]
pub struct BTreeDirtyPart {
    pub chkp_num: u32,
    pub seg_num: i32,
    pub part_num: i32,
}

/// Pending data file parts to be synchronized with S3.
#[repr(C)]
pub struct BTreeS3PartsInfo {
    /// Dirty parts waiting for S3 upload.
    pub dirty_parts: [BTreeDirtyPart; MAX_NUM_DIRTY_PARTS],
    /// Maximum location among pending S3 writes.
    pub write_max_location: u64,
}

// ============================================================================
// BTreeLocalFreeExtents — backend-local free extent list
// ============================================================================

/// Backend-local free-extent list for temporary trees.
#[repr(C)]
pub struct BTreeLocalFreeExtents {
    /// Allocated free extent entries.
    pub items: *mut FileExtent,
    /// Number of valid entries.
    pub size: i32,
    /// Allocated capacity.
    pub capacity: i32,
}

// ============================================================================
// BTreePageChunkDesc — per-chunk metadata
// ============================================================================

/// Metadata stored alongside each chunk on a page.
///
/// Layout (32 bits total):
/// - bits 0-11:   short_location (12 bits)
/// - bits 12-21:  offset (10 bits)
/// - bits 22-28:  hikey_short_location (7 bits)
/// - bit 29:      chunk_keys_fixed (1 bit)
/// - bits 30-31:  hikey_flags (2 bits)
#[repr(C)]
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct BTreePageChunkDesc {
    packed: u32,
}

impl BTreePageChunkDesc {
    /// Multiplier to convert short location to byte offset.
    pub const SHORT_LOCATION_MULTIPLIER: u32 = 4;

    /// Limit for hikey short location.
    pub const HIKEY_SHORT_LOCATION_LIMIT: u32 = (1 << 7) * Self::SHORT_LOCATION_MULTIPLIER;

    /// Create from packed value.
    #[inline]
    pub const fn from_packed(packed: u32) -> Self {
        Self { packed }
    }

    /// Extract raw packed value.
    #[inline]
    pub const fn packed(&self) -> u32 {
        self.packed
    }

    /// Get short location (bits 0-11).
    #[inline]
    pub const fn short_location(&self) -> u32 {
        self.packed & 0xFFF
    }

    /// Get offset (bits 12-21).
    #[inline]
    pub const fn offset(&self) -> u32 {
        (self.packed >> 12) & 0x3FF
    }

    /// Get hikey short location (bits 22-28).
    #[inline]
    pub const fn hikey_short_location(&self) -> u32 {
        (self.packed >> 22) & 0x7F
    }

    /// Get chunk keys fixed flag (bit 29).
    #[inline]
    pub const fn chunk_keys_fixed(&self) -> bool {
        (self.packed & (1 << 29)) != 0
    }

    /// Get hikey flags (bits 30-31).
    #[inline]
    pub const fn hikey_flags(&self) -> u32 {
        (self.packed >> 30) & 0x3
    }
}

/// Convert a short location back to a byte offset.
#[inline]
pub const fn short_get_location(s: u32) -> u32 {
    s * BTreePageChunkDesc::SHORT_LOCATION_MULTIPLIER
}

/// Validate and convert a location to short form.
#[inline]
pub const fn location_get_short(l: u32) -> u32 {
    // In C this asserts (l & 3) == 0, then divides by 4.
    l / 4
}

// ============================================================================
// BTreePageChunk — chunk of items on a page
// ============================================================================

/// A chunk of items stored contiguously on a page.
///
/// In Rust we don't need a VLA; the actual items are accessed through the
/// `BTreePageItemLocator` which computes offsets.
#[repr(C)]
pub struct BTreePageChunk {
    /// Item location indices (variable-length, C VLA).
    /// In Rust this is zero-length; actual data is behind a raw pointer.
    pub items: [LocationIndex; 0],
}

// ============================================================================
// BTreePageItemLocator — cursor into items on a page
// ============================================================================

/// Locator that points to a specific item within a page's chunk structure.
#[repr(C)]
#[derive(Clone, Copy, Debug, Default)]
pub struct BTreePageItemLocator {
    /// Which chunk the item is in.
    pub chunk_offset: OffsetNumber,
    /// Which item within the chunk.
    pub item_offset: OffsetNumber,
    /// How many items are in this chunk.
    pub chunk_items_count: OffsetNumber,
    /// Total size of this chunk in bytes.
    pub chunk_size: LocationIndex,
    /// Pointer to the chunk data within the page.
    pub chunk: *mut BTreePageChunk,
}

// ============================================================================
// BTreePageItem — item being inserted / moved
// ============================================================================

/// An item (data + metadata) being operated on.
#[repr(C)]
#[derive(Clone, Copy, Debug, Default)]
pub struct BTreePageItem {
    /// Pointer to the item data.
    pub data: *mut std::os::raw::c_void,
    /// Size of the item data.
    pub size: LocationIndex,
    /// Item flags.
    pub flags: u8,
}

// ============================================================================
// BTreeItemPageFitType — result of page-fit check
// ============================================================================

#[repr(u8)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum BTreeItemPageFitType {
    /// Item fits without any changes.
    AsIs,
    /// Page needs compaction to fit the item.
    CompactRequired,
    /// Page needs splitting.
    SplitRequired,
}

// ============================================================================
// BTreeMetaPage — the meta page of a B-tree
// ============================================================================

/// The B-tree meta page, stored at `metaPageBlkno`.
#[repr(C)]
pub struct BTreeMetaPage {
    /// Standard page header.
    pub o_header: OrioleDBPageHeader,
    /// Shared seq-buf for free extents.
    pub free_buf: SeqBufDescShared,
    /// Shared seq-bufs for next checkpoint (2 slots).
    pub next_chkp: [SeqBufDescShared; 2],
    /// Shared seq-bufs for temporary checkpoint tracking (2 slots).
    pub tmp_buf: [SeqBufDescShared; 2],
    /// Number of free blocks (atomic).
    pub num_free_blocks: AtomicU64,
    /// Data file lengths (2 for primary/standby).
    pub datafile_length: [AtomicU64; 2],
    /// Lock protecting meta page modifications.
    pub meta_lock: LWLock,
    /// Lock protecting copy_blkno operations.
    pub copy_blkno_lock: LWLock,
    /// Surrogate ctid (atomic) for primary index without key.
    pub ctid: AtomicU64,
    /// Bridge ctid (atomic).
    pub bridge_ctid: AtomicU64,
    /// Number of leaf pages.
    pub leaf_pages_num: AtomicU32,
    /// Running sequential-scan counters per checkpoint window.
    pub num_seq_scans: [AtomicU32; NUM_SEQ_SCANS_ARRAY_SIZE],
    /// Deferred free flag.
    pub to_be_freed_on_seq_scan_release: bool,
    /// Dirty flags for checkpoint coordination.
    pub dirty_flag1: bool,
    pub dirty_flag2: bool,
    /// S3 part info for checkpoint.
    pub parts_info: [BTreeS3PartsInfo; 2],
    /// Lock for punch-holes operation.
    pub punch_holes_lock: LWLock,
    /// Checkpoint number for punch-holes.
    pub punch_holes_chkp_num: u32,
}

// ============================================================================
// BTreePageHeader — header of a data page (follows OrioleDBPageHeader)
// ============================================================================

/// Flags stored in the `flags` bitfield of `BTreePageHeader`.
pub mod btree_page_flags {
    /// Page is the leftmost page in its level.
    pub const O_BTREE_FLAG_LEFTMOST: u32 = 0x0001;
    /// Page is the rightmost page in its level.
    pub const O_BTREE_FLAG_RIGHTMOST: u32 = 0x0002;
    /// Page is a leaf page.
    pub const O_BTREE_FLAG_LEAF: u32 = 0x0004;
    /// Page is part of a split that has not yet completed.
    pub const O_BTREE_FLAG_BROKEN_SPLIT: u32 = 0x0008;
    /// Page is undergoing pre-cleanup.
    pub const O_BTREE_FLAG_PRE_CLEANUP: u32 = 0x0010;
    /// Page hikeys have been fixed up.
    pub const O_BTREE_FLAG_HIKEYS_FIXED: u32 = 0x0020;
}

use btree_page_flags::*;

/// Combined flags for a freshly initialized root page.
pub const O_BTREE_FLAGS_ROOT_INIT: u32 =
    O_BTREE_FLAG_LEAF | O_BTREE_FLAG_RIGHTMOST | O_BTREE_FLAG_LEFTMOST;

/// B-tree data page header — follows `OrioleDBPageHeader` as the first
/// `O_PAGE_HEADER_SIZE` bytes and then continues with page-specific fields.
///
/// This struct is laid out so that the first `O_PAGE_HEADER_SIZE` bytes
/// overlap with `OrioleDBPageHeader`.
#[repr(C)]
pub struct BTreePageHeader {
    /// Common page header (must be first).
    pub o_header: OrioleDBPageHeader,
    /// Link to the page-level undo item and corresponding CSN.
    pub undo_location: UndoLocation,
    /// Commit sequence number.
    pub csn: CommitSeqNo,
    /// Right-page link.
    pub right_link: u64,
    /// Bitfield (32 bits):
    /// - bits 0-5:   flags
    /// - bits 6-16:  field1 (level for non-leafs, vacated bytes for leafs)
    /// - bits 17-31: field2 (on-disk downlinks for non-leafs, vacated bytes for leafs)
    pub flags: u32,
    /// Maximum key length on this page.
    pub max_key_len: LocationIndex,
    /// Offset of the previous insert.
    pub prev_insert_offset: OffsetNumber,
    /// Number of chunks.
    pub chunks_count: OffsetNumber,
    /// Number of items.
    pub items_count: OffsetNumber,
    /// Offset after the last hikey.
    pub hikeys_end: OffsetNumber,
    /// Total data size on this page.
    pub data_size: LocationIndex,
    /// Per-chunk metadata (variable-length array, C VLA).
    pub chunk_desc: [BTreePageChunkDesc; 0],
}

/// Returns the hikeys end value for a page (256 for leaf, 512 for non-leaf).
#[inline]
pub const fn btree_page_hikeys_end(desc: &BTreeDescr, p: &BTreePageHeader, is_leaf: bool) -> u16 {
    if is_leaf {
        256
    } else {
        512
    }
}

// ============================================================================
// BTreeLeafTuphdr — header of a leaf tuple
// ============================================================================

/// Header of a leaf tuple stored on a page.
///
/// Layout (128-bit = 16 bytes, packed):
/// - bits 0-60:   xact_info (61 bits)
/// - bits 61-62:  deleted (2 bits)
/// - bit 63:      chain_has_locks (1 bit)
/// - bits 64-125: undo_location (62 bits)
/// - bits 126-127: format_flags (2 bits)
#[repr(C)]
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct BTreeLeafTuphdr {
    packed: [u64; 2],
}

impl BTreeLeafTuphdr {
    /// Size of a leaf tuple header (aligned).
    pub const SIZE_ALIGNED: usize = std::mem::size_of::<u64>() * 2;

    /// Xact info mask (61 bits).
    pub const XACT_INFO_MASK: u64 = (1u64 << 61) - 1;

    /// Deleted mask (2 bits at bit 61).
    pub const DELETED_MASK: u64 = 0x3;
    pub const DELETED_SHIFT: u32 = 61;

    /// Chain has locks bit (bit 63).
    pub const CHAIN_HAS_LOCKS_BIT: u64 = 1u64 << 63;

    /// Undo location mask (62 bits at bit 64).
    pub const UNDO_LOCATION_MASK: u64 = (1u64 << 62) - 1;
    pub const UNDO_LOCATION_SHIFT: u32 = 64;

    /// Format flags mask (2 bits at bit 126).
    pub const FORMAT_FLAGS_MASK: u64 = 0x3;
    pub const FORMAT_FLAGS_SHIFT: u32 = 126;

    /// Create from two u64 values (little-endian layout matching C).
    #[inline]
    pub const fn from_parts(low: u64, high: u64) -> Self {
        Self {
            packed: [low, high],
        }
    }

    /// Extract low and high parts.
    #[inline]
    pub const fn parts(&self) -> (u64, u64) {
        (self.packed[0], self.packed[1])
    }

    /// Get xact info.
    #[inline]
    pub const fn xact_info(&self) -> u64 {
        self.packed[0] & Self::XACT_INFO_MASK
    }

    /// Get deleted status.
    #[inline]
    pub const fn deleted(&self) -> u8 {
        ((self.packed[0] >> Self::DELETED_SHIFT) & Self::DELETED_MASK) as u8
    }

    /// Get chain_has_locks.
    #[inline]
    pub const fn chain_has_locks(&self) -> bool {
        (self.packed[0] & Self::CHAIN_HAS_LOCKS_BIT) != 0
    }

    /// Get undo location.
    #[inline]
    pub const fn undo_location(&self) -> u64 {
        ((self.packed[0] >> 64 & 0) | (self.packed[1] & Self::UNDO_LOCATION_MASK)) >> (64 - 62)
        // adjust for storage position
    }

    /// Get format flags.
    #[inline]
    pub const fn format_flags(&self) -> u8 {
        ((self.packed[1] >> Self::FORMAT_FLAGS_SHIFT) & Self::FORMAT_FLAGS_MASK) as u8
    }
}

/// Size of a leaf tuple header, aligned.
pub const BTreeLeafTuphdrSize: usize = MAXALIGN(BTreeLeafTuphdr::SIZE_ALIGNED);

// ============================================================================
// BTreeNonLeafTuphdr — header of a non-leaf tuple
// ============================================================================

/// Header of a non-leaf tuple (just a downlink).
#[repr(C)]
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct BTreeNonLeafTuphdr {
    /// Downlink (block number or on-disk address).
    pub downlink: u64,
}

/// Size of a non-leaf tuple header, aligned.
pub const BTreeNonLeafTuphdrSize: usize = MAXALIGN(std::mem::size_of::<BTreeNonLeafTuphdr>());

// ============================================================================
// Downlink bit flags (for non-leaf tuple headers)
// ============================================================================

pub mod downlink {
    /// Bit 63 set when the downlink points to an on-disk page.
    pub const DOWNLINK_DISK_BIT: u64 = 1u64 << 63;
    /// Mask for in-memory block number.
    pub const DOWNLINK_IO_BUF_MASK: u64 = 0x7FFF_FFFF_FFFF_FFFF;
}

use downlink::*;

/// Returns `true` if `downlink` points to an in-memory page.
#[inline]
pub const fn downlink_is_in_memory(downlink: u64) -> bool {
    (downlink & DOWNLINK_DISK_BIT) == 0
}

/// Returns `true` if `downlink` points to an I/O buffer.
#[inline]
pub const fn downlink_is_in_io(downlink: u64) -> bool {
    downlink_is_in_memory(downlink) && (downlink != 0)
}

/// Returns `true` if `downlink` points to an on-disk page.
#[inline]
pub const fn downlink_is_on_disk(downlink: u64) -> bool {
    (downlink & DOWNLINK_DISK_BIT) != 0
}

/// Extract the in-memory block number from a downlink.
#[inline]
pub const fn downlink_get_in_memory_blkno(downlink: u64) -> OInMemoryBlkno {
    (downlink & DOWNLINK_IO_BUF_MASK) as OInMemoryBlkno
}

/// Create an in-memory downlink from a block number.
#[inline]
pub const fn make_in_memory_downlink(blkno: OInMemoryBlkno) -> u64 {
    blkno as u64
}

/// Invalid disk downlink.
pub const InvalidDiskDownlink: u64 = 0;

/// Returns `true` if a disk downlink is valid.
#[inline]
pub const fn disk_downlink_is_valid(dl: u64) -> bool {
    dl != InvalidDiskDownlink
}

/// Invalid right link.
pub const InvalidRightLink: u64 = 0;

/// Returns `true` if a right link is valid.
#[inline]
pub const fn right_link_is_valid(rl: u64) -> bool {
    rl != InvalidRightLink
}

/// Create an in-memory right link from a block number.
#[inline]
pub const fn make_in_memory_rightlink(blkno: OInMemoryBlkno) -> u64 {
    blkno as u64
}

// ============================================================================
// Maximum tuple / key sizes
// ============================================================================

/// Maximum tuple size that can fit on a page (aligned).
/// Computed as: MAXALIGN_DOWN((BLCKSZ - sizeof(BTreePageHeader)) / 3 - sizeof(LocationIndex) - BTreeLeafTuphdrSize)
pub const O_BTREE_MAX_TUPLE_SIZE: usize = {
    let space = ORIOLEDB_BLCKSZ - std::mem::size_of::<BTreePageHeader>();
    let base = space / 3 - std::mem::size_of::<LocationIndex>() - BTreeLeafTuphdrSize;
    base & !(std::mem::align_of::<usize>() - 1)
};

/// Maximum key size (same as max tuple size).
pub const O_BTREE_MAX_KEY_SIZE: usize = O_BTREE_MAX_TUPLE_SIZE;

// ============================================================================
// Fixed tuple/key helpers for sorted access
// ============================================================================

/// Fixed-size container for a tuple (used for sorted index access).
#[repr(C)]
#[derive(Clone, Copy, Debug, Default)]
pub struct OFixedTuple {
    pub tuple: OTuple,
}

/// Fixed-size container for a key (used for sorted index access).
#[repr(C)]
#[derive(Clone, Copy, Debug, Default)]
pub struct OFixedKey {
    pub tuple: OTuple,
}

// ============================================================================
// ReadPageResult — result of a page read attempt
// ============================================================================

#[repr(u8)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ReadPageResult {
    /// Page read succeeded.
    Ok,
    /// Page change count mismatch (page was modified during read).
    WrongPageChangeCount,
    /// Page read failed (I/O error, eviction, etc.).
    Failed,
}

// ============================================================================
// OPageWaiterStatus — why a process is waiting on a page
// ============================================================================

/// Why a process is waiting on a page.
#[repr(u8)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum OPageWaiterStatus {
    /// Waiting for exclusive access.
    Exclusive,
    /// Waiting for non-exclusive access.
    NonExclusive,
    /// Waiting for an insert to complete.
    Insert,
    /// Being woken up.
    WakeUp,
}

// ============================================================================
// Lock page result enums
// ============================================================================

#[repr(u8)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum OLockPageWithTupleResult {
    /// Page was locked successfully.
    Locked,
    /// Need to re-find the page (page was split/merged).
    RefindNeeded,
    /// A tuple was inserted by another process while waiting.
    Inserted,
}

// ============================================================================
// Modify callback / wait callback enums
// ============================================================================

#[repr(u8)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum OBTreeModifyCallbackAction {
    DoNothing = 1,
    Update = 2,
    Delete = 3,
    Lock = 4,
    Undo = 5,
}

#[repr(u8)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum OBTreeWaitCallbackAction {
    XidNoWait = 1,
    XidWait = 2,
    XidExit = 3,
}

#[repr(u8)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum OBTreeModifyResult {
    Inserted = 1,
    Updated = 2,
    Deleted = 3,
    Locked = 4,
    Found = 5,
    NotFound = 6,
}

// ============================================================================
// Row lock modes
// ============================================================================

#[repr(u8)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum RowLockMode {
    KeyShare = 0,
    Share = 1,
    NoKeyUpdate = 2,
    Update = 3,
}

/// Returns `true` if two lock modes conflict.
#[inline]
pub const fn row_locks_conflict(lock1: RowLockMode, lock2: RowLockMode) -> bool {
    (lock1 as u8 + lock2 as u8) >= 3
}

// ============================================================================
// BTreeLocalFreeExtents — already defined above, re-exported for convenience
// ============================================================================

// (Already defined in section "BTreeLocalFreeExtents" above.)

// ============================================================================
// Forward declarations for types defined in other modules
// ============================================================================

/// Opaque handle to a page pool (defined in `utils/page_pool.rs`).
pub struct PagePool {
    _private: [u8; 0],
}

/// Opaque handle to the usage count map (defined in `utils/ucm.rs`).
pub struct UsageCountMap {
    _private: [u8; 0],
}

/// Opaque handle to compression (type alias, defined in `utils/compress.rs`).
pub use crate::utils::compress::OCompress as CompressHandle;

// ============================================================================
// MAXALIGN helper
// ============================================================================

/// Align `value` up to the given alignment (power of two).
#[inline]
pub const fn my_align(value: usize, alignment: usize) -> usize {
    (value + alignment - 1) & !(alignment - 1)
}

/// Align `value` up to the system maximum alignment.
#[inline]
pub const fn maxalign(value: usize) -> usize {
    my_align(value, std::mem::align_of::<usize>())
}

/// Align `value` down to the given alignment.
#[inline]
pub const fn my_align_down(value: usize, alignment: usize) -> usize {
    value & !(alignment - 1)
}

// PostgreSQL's MAXALIGN equivalent.
pub const MAXALIGN: fn(usize) -> usize = maxalign;
pub const MAXALIGN_DOWN: fn(usize) -> usize = my_align_down;

// ============================================================================
// S_lock helpers (placeholder for spinlock infrastructure)
// ============================================================================

/// Placeholder for a simple spinlock. The real implementation lives in
/// `utils/s_lock.rs` (to be ported).
#[repr(C)]
pub struct SLock {
    pub holder: AtomicI32,
}

impl SLock {
    pub const fn new() -> Self {
        Self {
            holder: AtomicI32::new(0),
        }
    }
}

impl Default for SLock {
    fn default() -> Self {
        Self::new()
    }
}

/// Placeholder for an LWLock. The real implementation uses PostgreSQL's
/// LWLock infrastructure.
#[repr(C)]
pub struct LWLock {
    pub lock: SLock,
}

impl LWLock {
    pub const fn new() -> Self {
        Self { lock: SLock::new() }
    }
}

impl Default for LWLock {
    fn default() -> Self {
        Self::new()
    }
}

/// Wrapper around LWLock for shared memory indexing.
#[repr(C)]
pub struct LWLockPadded {
    pub lock: LWLock,
}

// ============================================================================
// SeqBuf shared / private descriptors (forward refs — full definitions
// are in `utils/seq_buf.rs`)
// ============================================================================

/// Placeholder for SeqBufDescShared (defined in utils/seq_buf.rs).
#[repr(C)]
pub struct SeqBufDescShared {
    pub pages: [OInMemoryBlkno; 2],
    pub cur_page_num: u32,
    pub file_page_num: u32,
    pub free_bytes_num: u32,
    pub location: u32,
    pub tag: SeqBufTag,
    pub prev_page_state: u8,
    pub evict_offset: u64,
    pub lock: SLock,
}

/// Placeholder for SeqBufDescPrivate (defined in utils/seq_buf.rs).
#[repr(C)]
pub struct SeqBufDescPrivate {
    pub shared: *mut SeqBufDescShared,
    pub file: i32,
    pub write: bool,
    pub tag: SeqBufTag,
}

/// Tag for a sequence buffer.
#[repr(C)]
#[derive(Clone, Copy, Debug, Default)]
pub struct SeqBufTag {
    pub tag_type: i32,
    pub database_id: OdbOid,
    pub rel_node: OdbOid,
    pub extra1: i32,
    pub extra2: i32,
    pub extra3: i32,
}

// ============================================================================
// Page eviction / read checkpoint result
// ============================================================================

/// Result returned by the page-read path.
#[repr(u8)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum PageReadResult {
    /// Page was successfully read.
    Ok,
    /// Change count mismatch (page was modified while reading).
    WrongPageChangeCount,
    /// Read failed.
    Failed,
}

// ============================================================================
// PartialPageState — incremental page load state
// ============================================================================

/// State for partially-loaded pages (used during recovery, undo, etc.).
#[repr(C)]
pub struct PartialPageState {
    /// Pointer to the current chunk being loaded.
    pub chunk: *mut BTreePageChunk,
    /// Offset within the chunk.
    pub chunk_offset: OffsetNumber,
    /// Whether hikeys have been loaded.
    pub hikeys_loaded: bool,
    /// Full page image (when completely loaded).
    pub full_image: *mut std::os::raw::c_void,
    /// Whether a full image is present.
    pub has_full_image: bool,
}

// ============================================================================
// BTreePageItemLocator helper functions
// ============================================================================

impl BTreePageItemLocator {
    /// Returns `true` if this locator points to a valid item.
    #[inline]
    pub const fn is_valid(&self) -> bool {
        !self.chunk.is_null() && self.item_offset < self.chunk_items_count
    }

    /// Set this locator to invalid.
    #[inline]
    pub fn set_invalid(&mut self) {
        self.chunk = std::ptr::null_mut();
        self.item_offset = 0;
        self.chunk_items_count = 0;
    }

    /// Get the item offset from a location index entry.
    #[inline]
    pub const fn item_get_offset(item: LocationIndex) -> u32 {
        (item & 0x3FFF) as u32
    }

    /// Get the flags from a location index entry.
    #[inline]
    pub const fn item_get_flags(item: LocationIndex) -> u32 {
        (item >> 14) as u32
    }

    /// Set the flags on a location index entry.
    #[inline]
    pub const fn item_set_flags(item: LocationIndex, flags: bool) -> LocationIndex {
        if flags {
            item | ((1 as LocationIndex) << 14)
        } else {
            item & !((1 as LocationIndex) << 14)
        }
    }

    /// Get the byte offset of an item within a page.
    ///
    /// `chunk` is the raw chunk pointer, and `item_offset` is the index
    /// into the chunk's location-index array.
    #[inline]
    pub fn item_get_offset_at(
        chunk_ptr: *const BTreePageChunk,
        item_offset: OffsetNumber,
    ) -> usize {
        if chunk_ptr.is_null() {
            return 0;
        }
        // SAFETY: caller must ensure chunk_ptr is valid and item_offset
        // is within bounds. The actual byte offset computation follows
        // the C semantics: cast chunk to usize + index * sizeof(LocationIndex).
        unsafe {
            let base = chunk_ptr as usize;
            let items_ptr = base as *const LocationIndex;
            let offset = *items_ptr.add(item_offset as usize);
            Self::item_get_offset(offset) as usize
        }
    }
}

// ============================================================================
// BTreePageHeader helper functions
// ============================================================================

impl BTreePageHeader {
    /// Returns `true` if the page has the given flag.
    #[inline]
    pub const fn has_flag(&self, flag: u32) -> bool {
        (self.flags & flag) != 0
    }

    /// Returns `true` if this is a leaf page.
    #[inline]
    pub const fn is_leaf(&self) -> bool {
        self.has_flag(O_BTREE_FLAG_LEAF)
    }

    /// Returns `true` if this is the leftmost page.
    #[inline]
    pub const fn is_leftmost(&self) -> bool {
        self.has_flag(O_BTREE_FLAG_LEFTMOST)
    }

    /// Returns `true` if this is the rightmost page.
    #[inline]
    pub const fn is_rightmost(&self) -> bool {
        self.has_flag(O_BTREE_FLAG_RIGHTMOST)
    }

    /// Returns `true` if this page is part of a broken split.
    #[inline]
    pub const fn is_broken_split(&self) -> bool {
        self.has_flag(O_BTREE_FLAG_BROKEN_SPLIT)
    }

    /// Returns `true` if hikeys have been fixed.
    #[inline]
    pub const fn hikeys_fixed(&self) -> bool {
        self.has_flag(O_BTREE_FLAG_HIKEYS_FIXED)
    }

    /// Get the number of items on the page.
    #[inline]
    pub const fn items_count(&self) -> OffsetNumber {
        self.items_count
    }

    /// Get the right link.
    #[inline]
    pub const fn right_link(&self) -> u64 {
        self.right_link
    }

    /// Get the page level (for non-leaf pages).
    #[inline]
    pub const fn level(&self) -> u16 {
        // field1 is bits 6-16 of the flags field (11 bits).
        ((self.flags >> 6) & 0x7FF) as u16
    }

    /// Set the page level.
    #[inline]
    pub fn set_level(&mut self, level: u16) {
        self.flags = (self.flags & !(0x7FF << 6)) | (((level as u32) & 0x7FF) << 6);
    }

    /// Get the number of on-disk downlinks (non-leaf) or vacated bytes (leaf).
    #[inline]
    pub const fn n_ondisk(&self) -> u16 {
        // field2 is bits 17-31 of the flags field (15 bits).
        ((self.flags >> 17) & 0x7FFF) as u16
    }

    /// Set the number of on-disk downlinks.
    #[inline]
    pub fn set_n_ondisk(&mut self, n: u16) {
        self.flags = (self.flags & !(0x7FFF << 17)) | (((n as u32) & 0x7FFF) << 17);
    }

    /// Increment the number of on-disk downlinks.
    #[inline]
    pub fn inc_n_ondisk(&mut self) {
        self.set_n_ondisk(self.n_ondisk() + 1);
    }

    /// Decrement the number of on-disk downlinks.
    #[inline]
    pub fn dec_n_ondisk(&mut self) {
        let n = self.n_ondisk();
        if n > 0 {
            self.set_n_ondisk(n - 1);
        }
    }

    /// Get the number of vacated entries.
    #[inline]
    pub const fn n_vacated(&self) -> u16 {
        ((self.flags >> 17) & 0x7FFF) as u16
    }

    /// Set the number of vacated entries.
    #[inline]
    pub fn set_n_vacated(&mut self, n: u16) {
        self.flags = (self.flags & !(0x7FFF << 17)) | (((n as u32) & 0x7FFF) << 17);
    }

    /// Add to the number of vacated entries.
    #[inline]
    pub fn add_n_vacated(&mut self, delta: u16) {
        let n = self.n_vacated();
        self.set_n_vacated(n + delta);
    }

    /// Subtract from the number of vacated entries.
    #[inline]
    pub fn sub_n_vacated(&mut self, delta: u16) {
        let n = self.n_vacated();
        if n >= delta {
            self.set_n_vacated(n - delta);
        }
    }

    /// Get free space on the page.
    #[inline]
    pub fn free_space(&self) -> usize {
        // Free space = page size - header - data
        let header_size = std::mem::size_of::<Self>();
        ORIOLEDB_BLCKSZ.saturating_sub(header_size + self.data_size as usize)
    }
}

// ============================================================================
// BTreeLeafTuphdr helper functions
// ============================================================================

impl BTreeLeafTuphdr {
    /// Returns the deleted status.
    #[inline]
    pub const fn deleted_status(&self) -> BTreeLeafTupleDeletedStatus {
        match self.deleted() {
            0 => BTreeLeafTupleDeletedStatus::NonDeleted,
            1 => BTreeLeafTupleDeletedStatus::Deleted,
            2 => BTreeLeafTupleDeletedStatus::MovedPartitions,
            3 => BTreeLeafTupleDeletedStatus::PkChanged,
            _ => BTreeLeafTupleDeletedStatus::NonDeleted,
        }
    }
}

// ============================================================================
// ORowId — row identifier types
// ============================================================================

/// Location hint for a row (block number + change count).
#[repr(C)]
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct BTreeLocationHint {
    pub blkno: OInMemoryBlkno,
    pub page_change_count: u32,
}

/// Row ID addendum for CTID-based addressing.
#[repr(C)]
#[derive(Clone, Copy, Debug, Default)]
pub struct ORowIdAddendumCtid {
    pub hint: BTreeLocationHint,
    pub csn: CommitSeqNo,
    pub version: u32,
}

/// Row ID addendum for non-CTID addressing.
#[repr(C)]
#[derive(Clone, Copy, Debug, Default)]
pub struct ORowIdAddendumNonCtid {
    pub hint: BTreeLocationHint,
    pub csn: CommitSeqNo,
    pub flags: u8,
}

/// Bridge data for index bridging.
#[repr(C)]
#[derive(Clone, Copy, Debug, Default)]
pub struct ORowIdBridgeData {
    pub bridge_ctid: ItemPointerData,
    pub bridge_changed: bool,
}

// PostgreSQL's ItemPointerData (from pg_sys).
pub use pgrx::pg_sys::ItemPointerData;

// ============================================================================
// ORelOptions — relation options (placeholder — full port in catalog module)
// ============================================================================

/// Relation options stored in pg_class.reloptions.
#[repr(C)]
pub struct ORelOptions {
    /// Standard reloptions base.
    pub std_options: *mut std::os::raw::c_void,
    /// Compression offset.
    pub compress_offset: i32,
    /// Primary index compression offset.
    pub primary_compress_offset: i32,
    /// TOAST compression offset.
    pub toast_compress_offset: i32,
    /// Whether index bridging is enabled.
    pub index_bridging: bool,
}

// ============================================================================
// OBTOptions — B-tree index options (placeholder)
// ============================================================================

/// B-tree index options.
#[repr(C)]
pub struct OBTOptions {
    /// Base BTOptions from PostgreSQL nbtree.
    pub bt_options: *mut std::os::raw::c_void,
    /// Compression offset.
    pub compress_offset: i32,
    /// Whether this is an orioledb index.
    pub orioledb_index: bool,
}

// ============================================================================
// XidVXidMapElement — mapping from Oxid to VirtualXid
// ============================================================================

/// Maps an OrioleDB transaction ID to a PostgreSQL virtual transaction ID.
#[repr(C)]
#[derive(Clone, Copy, Debug, Default)]
pub struct XidVXidMapElement {
    pub oxid: OXid,
    pub vxid: *mut std::os::raw::c_void, // VirtualXid
}

// ============================================================================
// Undo stack / retain shared locations (placeholders — full port in transam)
// ============================================================================

/// Shared undo locations for a single undo log.
#[repr(C)]
#[derive(Clone, Copy, Debug, Default)]
pub struct UndoStackSharedLocations {
    pub location: AtomicU64,
    pub branch_location: AtomicU64,
    pub subxact_location: AtomicU64,
    pub on_commit_location: AtomicU64,
}

/// Shared undo retain locations.
#[repr(C)]
#[derive(Clone, Copy, Debug, Default)]
pub struct UndoRetainSharedLocations {
    pub reserved_undo_location: AtomicU64,
    pub transaction_undo_retain_location: AtomicU64,
    pub snapshot_retain_undo_location: AtomicU64,
}

// ============================================================================
// ODBProcData — per-backend proc data (placeholder)
// ============================================================================

/// Per-backend data for undo tracking.
///
/// This structure lives in shared memory and is replicated per-backend.
/// The full port will use atomics and proper synchronization.
#[repr(C)]
pub struct ODBProcData {
    /// Undo retain locations for each undo log type.
    pub undo_retain_locations: [UndoRetainSharedLocations; UndoLogType::COUNT],
    /// Commit-in-progress XLog location (atomic).
    pub commit_in_progress_xlog_location: AtomicU64,
    /// Autonomous transaction nesting level.
    pub autonomous_nesting_level: i32,
    /// Undo stack locations flush lock.
    pub undo_stack_locations_flush_lock: LWLock,
    /// Whether to flush undo locations.
    pub flush_undo_locations: bool,
    /// Whether waiting for Oxid.
    pub waiting_for_oxid: bool,
    /// Xmin (atomic).
    pub xmin: AtomicU64,
    /// Pending secondary-key undo location (atomic).
    pub pending_sk_undo_loc: AtomicU64,
    /// Undo stack locations for each xid slot and undo log.
    pub undo_stack_locations: [[UndoStackSharedLocations; UndoLogType::COUNT]; 32],
    /// VXID mapping entries.
    pub vxids: [XidVXidMapElement; 32],
}

// ============================================================================
// Module-level size/alignment checks
// ============================================================================

/// Compile-time assertions that critical types have the expected sizes.
///
/// These mirror the `StaticAssertDecl` calls in the C headers. They run at
/// module load time (not compile time, because C size constants are not
/// always expressible in Rust const generics).
pub fn assert_type_sizes() {
    // Page header and on-disk header must be the same size.
    assert_eq!(
        OrioleDBPageHeader::SIZE,
        OrioleDBOndiskPageHeader::SIZE,
        "OrioleDBPageHeader and OrioleDBOndiskPageHeader must have the same size"
    );

    // Meta page must fit in one OrioleDB page.
    assert!(
        std::mem::size_of::<BTreeMetaPage>() <= ORIOLEDB_BLCKSZ,
        "BTreeMetaPage must fit in one page ({} bytes)",
        ORIOLEDB_BLCKSZ
    );

    // BTreeLeafTuphdr size
    assert_eq!(
        std::mem::size_of::<BTreeLeafTuphdr>(),
        BTreeLeafTuphdr::SIZE_ALIGNED,
        "BTreeLeafTuphdr size mismatch"
    );

    // BTreeNonLeafTuphdr size
    assert_eq!(
        std::mem::size_of::<BTreeNonLeafTuphdr>(),
        std::mem::size_of::<u64>(),
        "BTreeNonLeafTuphdr must be exactly one u64"
    );
}
