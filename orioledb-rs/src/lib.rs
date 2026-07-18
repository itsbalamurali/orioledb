use std::os::raw::{c_char, c_int, c_void};
use pgrx::pg_sys::{self, Datum, MemoryContext, Relation, TupleDesc, Tuplestorestate, XLogRecPtr};
use pgrx::prelude::*;

::pgrx::pg_module_magic!(name, version);

pub mod btree;
pub mod catalog;
// pub mod checkpoint;
// pub mod indexam;
// pub mod recovery;
// pub mod rewind;
// pub mod s3;
// pub mod tableam;
pub mod transam;
// pub mod tuple;
pub mod utils;
// pub mod workers;

pub type Oid = pg_sys::Oid;
pub type CommitSeqNo = u64;
pub type OXid = u64;

// ---------------------------------------------------------------------------
// Constants & Custom Types
// ---------------------------------------------------------------------------
pub const O_SERIALIZABLE_TABLE_LOCK: c_int = 0;
pub const O_SERIALIZABLE_ERROR: c_int = 1;
pub const O_SERIALIZABLE_REPEATABLE_READ: c_int = 2;

// PostgreSQL error codes (MAKE_SQLSTATE macro results)
pub const ERRCODE_INTERNAL_ERROR: i32 = pg_sys::errcodes::PgSqlErrorCode::ERRCODE_INTERNAL_ERROR as i32;
pub const ERRCODE_CONFIG_FILE_ERROR: i32 = pg_sys::errcodes::PgSqlErrorCode::ERRCODE_CONFIG_FILE_ERROR as i32;
pub const ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE: i32 = pg_sys::errcodes::PgSqlErrorCode::ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE as i32;

#[repr(C)]
#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub struct ORelOids {
    pub datoid: Oid,
    pub reloid: Oid,
    pub relnode: Oid,
}

#[repr(C)]
#[derive(Copy, Clone, Debug)]
pub struct FileExtent {
    pub val: u64,
}

impl FileExtent {
    pub fn len(&self) -> u16 {
        (self.val & 0xFFFF) as u16
    }
    pub fn set_len(&mut self, len: u16) {
        self.val = (self.val & !0xFFFF) | (len as u64);
    }
    pub fn off(&self) -> u64 {
        self.val >> 16
    }
    pub fn set_off(&mut self, off: u64) {
        self.val = (self.val & 0xFFFF) | ((off & 0xFFFFFFFFFFFF) << 16);
    }
}

pub type OInMemoryBlkno = u32;

#[repr(C)]
#[derive(Clone, Copy)]
pub struct PagePool {
    pub ops: *const PagePoolOps,
}

#[repr(C)]
pub struct PagePoolOps {
    pub alloc_page: Option<unsafe extern "C-unwind" fn(pool: *mut PagePool, pageReserveKind: c_int) -> u32>,
    pub alloc_metapage: Option<unsafe extern "C-unwind" fn(pool: *mut PagePool) -> u32>,
    pub free_page: Option<unsafe extern "C-unwind" fn(pool: *mut PagePool, blkno: u32, haveLock: bool)>,
    pub reserve_pages: Option<unsafe extern "C-unwind" fn(pool: *mut PagePool, pageReserveKind: c_int, count: c_int)>,
    pub release_reserved: Option<unsafe extern "C-unwind" fn(pool: *mut PagePool, kind_mask: u32)>,
    pub free_pages_count: Option<unsafe extern "C-unwind" fn(pool: *mut PagePool) -> u32>,
    pub dirty_pages_count: Option<unsafe extern "C-unwind" fn(pool: *mut PagePool) -> u32>,
    pub run_maintenance: Option<unsafe extern "C-unwind" fn(pool: *mut PagePool, evict: bool, shutdown_requested: *mut c_int) -> bool>,
    pub size: Option<unsafe extern "C-unwind" fn(pool: *mut PagePool) -> u32>,
    pub ucm_inc_usage: Option<unsafe extern "C-unwind" fn(pool: *mut PagePool, blkno: u32)>,
    pub ucm_init: Option<unsafe extern "C-unwind" fn(pool: *mut PagePool, blkno: u32)>,
}

#[repr(C)]
#[derive(Clone, Copy)]
pub struct UsageCountMap {
    pub epoch: *mut pg_sys::pg_atomic_uint32,
    pub ucm: *mut pg_sys::pg_atomic_uint32,
    pub offset: OInMemoryBlkno,
    pub size: OInMemoryBlkno,
    pub total: c_int,
    pub nonLeaf: c_int,
    pub rootFactor: c_int,
    pub usageCounter: u32,
}

#[repr(C)]
#[derive(Clone, Copy)]
pub struct OPagePool {
    pub base: PagePool,
    pub availablePagesCount: *mut pg_sys::pg_atomic_uint64,
    pub dirtyPagesCount: *mut pg_sys::pg_atomic_uint32,
    pub pageEvictCount: *mut pg_sys::pg_atomic_uint64,
    pub location: OInMemoryBlkno,
    pub offset: OInMemoryBlkno,
    pub size: OInMemoryBlkno,
    pub ucm: UsageCountMap,
    pub ucmShmemSize: usize,
    pub prngSeed: pg_sys::pg_prng_state,
}

#[repr(C)]
#[derive(Clone, Copy)]
pub struct LocalPagePoolStruct {
    pub base: PagePool,
}

#[repr(C, align(8))]
#[derive(Clone, Copy)]
pub struct OFixedShmemKey {
    pub fixedData: [c_char; 2688],
    pub formatFlags: u8,
    pub notNull: bool,
    pub len: c_int,
}

#[repr(C)]
#[derive(Clone, Copy)]
pub struct CheckpointPageInfo {
    pub bound: c_int,
    pub nextkeyType: c_int,
    pub image: [c_char; 8192],
    pub hikey: OFixedShmemKey,
    pub nextkey: OFixedShmemKey,
    pub lokey: OFixedShmemKey,
    pub blkno: OInMemoryBlkno,
    pub hikeyBlkno: OInMemoryBlkno,
    pub offset: u16,
    pub leftmost: bool,
    pub autonomous: bool,
    pub autonomousTupleExist: bool,
    pub autonomousLeftmost: bool,
}

#[repr(C)]
pub struct CheckpointState {
    pub controlIdentifier: u64,
    pub changecount: u32,
    pub lastCheckpointNumber: u32,
    pub treeType: c_int,
    pub datoid: Oid,
    pub reloid: Oid,
    pub relnode: Oid,
    pub tablespace: Oid,
    pub completed: bool,
    pub curKeyType: c_int,
    pub curKeyValue: OFixedShmemKey,
    pub stack: [CheckpointPageInfo; 32],
    pub pid: c_int,
    pub dirtyPagesEstimate: f64,
    pub pagesWritten: u64,
    pub oTablesMetaTrancheId: c_int,
    pub oTablesMetaLock: pg_sys::LWLock,
    pub oSysTreesTrancheId: c_int,
    pub oSysTreesLock: pg_sys::LWLock,
    pub oSharedRootInfoInsertTrancheId: c_int,
    pub oSharedRootInfoInsertLocks: [pg_sys::LWLock; 128],
    pub checkpointerLatch: *mut pg_sys::Latch,
    pub autonomousLevel: pg_sys::pg_atomic_uint32,
    pub replayStartPtr: pg_sys::XLogRecPtr,
    pub controlReplayStartPtr: pg_sys::XLogRecPtr,
    pub sysTreesStartPtr: pg_sys::XLogRecPtr,
    pub controlSysTreesStartPtr: pg_sys::XLogRecPtr,
    pub toastConsistentPtr: pg_sys::XLogRecPtr,
    pub controlToastConsistentPtr: pg_sys::XLogRecPtr,
    pub mmapDataLength: pg_sys::pg_atomic_uint64,
    pub xidQueueCheckpointNum: u32,
    pub oXidQueueTrancheId: c_int,
    pub oXidQueueLock: pg_sys::LWLock,
    pub oXidQueueFlushTrancheId: c_int,
    pub oXidQueueFlushLock: pg_sys::LWLock,
    pub copyBlknoTrancheId: c_int,
    pub oMetaTrancheId: c_int,
    pub punchHolesTrancheId: c_int,
    pub xidRecLastPos: pg_sys::pg_atomic_uint64,
    pub xidRecFlushPos: pg_sys::pg_atomic_uint64,
}

#[repr(C)]
#[derive(Copy, Clone)]
pub struct VirtualTransactionId {
    pub proc_number: c_int,
    pub lxid: u32,
}

#[repr(C)]
#[derive(Copy, Clone)]
pub struct XidVXidMapElement {
    pub oxid: u64,
    pub vxid: VirtualTransactionId,
}

#[repr(C)]
#[derive(Clone, Copy)]
pub struct UndoStackSharedLocations {
    pub location: pg_sys::pg_atomic_uint64,
    pub branchLocation: pg_sys::pg_atomic_uint64,
    pub subxactLocation: pg_sys::pg_atomic_uint64,
    pub onCommitLocation: pg_sys::pg_atomic_uint64,
}

#[repr(C)]
#[derive(Clone, Copy)]
pub struct UndoRetainSharedLocations {
    pub reservedUndoLocation: pg_sys::pg_atomic_uint64,
    pub transactionUndoRetainLocation: pg_sys::pg_atomic_uint64,
    pub snapshotRetainUndoLocation: pg_sys::pg_atomic_uint64,
}

#[repr(C)]
pub struct ODBProcData {
    pub undoRetainLocations: [UndoRetainSharedLocations; 3],
    pub commitInProgressXlogLocation: pg_sys::pg_atomic_uint64,
    pub autonomousNestingLevel: c_int,
    pub undoStackLocationsFlushLock: pg_sys::LWLock,
    pub flushUndoLocations: bool,
    pub waitingForOxid: bool,
    pub xmin: pg_sys::pg_atomic_uint64,
    pub pendingSkUndoLoc: pg_sys::pg_atomic_uint64,
    pub undoStackLocations: [[UndoStackSharedLocations; 3]; 32],
    pub vxids: [XidVXidMapElement; 32],
}

#[repr(C)]
pub struct UndoMeta {
    pub lastUsedLocation: pg_sys::pg_atomic_uint64,
    pub advanceReservedLocation: pg_sys::pg_atomic_uint64,
    pub writeInProgressLocation: pg_sys::pg_atomic_uint64,
    pub writtenLocation: pg_sys::pg_atomic_uint64,
    pub lastUsedUndoLocationWhenUpdatedMinLocation: pg_sys::pg_atomic_uint64,
    pub minProcTransactionRetainLocation: pg_sys::pg_atomic_uint64,
    pub minProcRetainLocation: pg_sys::pg_atomic_uint64,
    pub minRewindRetainLocation: pg_sys::pg_atomic_uint64,
    pub minProcReservedLocation: pg_sys::pg_atomic_uint64,
    pub checkpointRetainStartLocation: pg_sys::pg_atomic_uint64,
    pub checkpointRetainEndLocation: pg_sys::pg_atomic_uint64,
    pub cleanedLocation: pg_sys::pg_atomic_uint64,
    pub cleanedCheckpointStartLocation: pg_sys::pg_atomic_uint64,
    pub cleanedCheckpointEndLocation: pg_sys::pg_atomic_uint64,
    pub minUndoLocationsMutex: pg_sys::slock_t,
    pub minUndoLocationsChangeCount: u32,
    pub sysXidUndoLocationChangeCount: u32,
    pub writeInProgressChangeCount: u32,
    pub undoWriteTrancheId: c_int,
    pub undoWriteLock: pg_sys::LWLock,
    pub undoStackLocationsFlushLockTrancheId: c_int,
}

#[repr(C)]
pub struct OrioleDBPageDesc {
    pub oids: ORelOids,
    pub ionum: c_int,
    pub fileExtent: FileExtent,
    pub flags_and_type: u32,
    pub leftBlkno: OInMemoryBlkno,
}

impl OrioleDBPageDesc {
    pub fn flags(&self) -> u32 {
        self.flags_and_type & 0xF
    }
    pub fn set_flags(&mut self, flags: u32) {
        self.flags_and_type = (self.flags_and_type & !0xF) | (flags & 0xF);
    }
    pub fn r#type(&self) -> u32 {
        self.flags_and_type >> 4
    }
    pub fn set_type(&mut self, r#type: u32) {
        self.flags_and_type = (self.flags_and_type & 0xF) | (r#type << 4);
    }
}

#[repr(C)]
pub struct OXidMapItem {
    pub csn: pg_sys::pg_atomic_uint64,
    pub commitPtr: pg_sys::pg_atomic_uint64,
}

#[repr(C)]
#[derive(Clone, Copy)]
pub struct RewindItem {
    pub tag: u8,
    pub nsubxids: c_int,
    pub oxid: u64,
    pub xid: pg_sys::TransactionId,
    pub onCommitUndoLocation: [u64; 3],
    pub undoLocation: [u64; 3],
    pub minRetainLocation: [u64; 3],
    pub oldestConsideredRunningXid: pg_sys::FullTransactionId,
    pub runXmin: u64,
    pub timestamp: pg_sys::TimestampTz,
}

struct ShmemItem {
    shmem_size: unsafe extern "C-unwind" fn() -> usize,
    shmem_init: unsafe extern "C-unwind" fn(ptr: *mut c_void, found: bool),
}

// ---------------------------------------------------------------------------
// GUC & Global Variables
// ---------------------------------------------------------------------------
#[cfg(any(feature = "pg17", feature = "pg18", feature = "pg19"))]
use pg_sys::GucContext::{PGC_POSTMASTER, PGC_USERSET, PGC_SUSET, PGC_SIGHUP};
#[cfg(not(any(feature = "pg17", feature = "pg18", feature = "pg19")))]
use pg_sys::{
    GucContext_PGC_POSTMASTER as PGC_POSTMASTER,
    GucContext_PGC_USERSET as PGC_USERSET,
    GucContext_PGC_SUSET as PGC_SUSET,
    GucContext_PGC_SIGHUP as PGC_SIGHUP,
};

#[no_mangle]
pub static mut debug_disable_pools_limit: bool = false;
#[no_mangle]
pub static mut shared_segment: *mut c_void = std::ptr::null_mut();
#[no_mangle]
pub static mut shared_segment_initialized: bool = false;
#[no_mangle]
pub static mut free_tree_buffers_guc: c_int = 0;
#[no_mangle]
pub static mut free_tree_buffers_count: usize = 0;
#[no_mangle]
pub static mut catalog_buffers_guc: c_int = 0;
#[no_mangle]
pub static mut catalog_buffers_count: usize = 0;
#[no_mangle]
pub static mut main_buffers_offset: usize = 0;
#[no_mangle]
pub static mut o_shared_buffers: *mut c_void = std::ptr::null_mut();
#[no_mangle]
pub static mut page_descs: *mut OrioleDBPageDesc = std::ptr::null_mut();
#[no_mangle]
pub static mut local_ppool_pages: *mut *mut c_void = std::ptr::null_mut();
#[no_mangle]
pub static mut local_ppool_page_descs: *mut OrioleDBPageDesc = std::ptr::null_mut();
#[no_mangle]
pub static mut orioledb_serializable_mode: c_int = O_SERIALIZABLE_TABLE_LOCK;
#[no_mangle]
pub static mut orioledb_debug_disable_multi_insert: bool = false;
#[no_mangle]
pub static mut main_buffers_guc: c_int = 0;
#[no_mangle]
pub static mut undo_buffers_guc: c_int = 0;
#[no_mangle]
pub static mut xid_buffers_guc: c_int = 0;
#[no_mangle]
pub static mut rewind_buffers_guc: c_int = 0;
#[no_mangle]
pub static mut temp_buffers_guc: c_int = 0;
#[no_mangle]
pub static mut max_procs: c_int = 0;
#[no_mangle]
pub static mut orioledb_buffers_size: usize = 0;
#[no_mangle]
pub static mut orioledb_buffers_count: usize = 0;
#[no_mangle]
pub static mut orioledb_temp_buffers_count: usize = 0;
#[no_mangle]
pub static mut page_descs_size: usize = 0;
#[no_mangle]
pub static mut undo_circular_buffer_size: usize = 0;
#[no_mangle]
pub static mut undo_buffers_count: u32 = 0;
#[no_mangle]
pub static mut regular_block_undo_circular_buffer_fraction: f64 = 0.45;
#[no_mangle]
pub static mut system_undo_circular_buffer_fraction: f64 = 0.10;
#[no_mangle]
pub static mut xid_circular_buffer_size: usize = 0;
#[no_mangle]
pub static mut xid_buffers_count: u32 = 0;
#[no_mangle]
pub static mut rewind_circular_buffer_size: usize = 0;
#[no_mangle]
pub static mut rewind_buffers_count: u32 = 0;
#[no_mangle]
pub static mut remove_old_checkpoint_files: bool = true;
#[no_mangle]
pub static mut skip_unmodified_trees: bool = true;
#[no_mangle]
pub static mut debug_disable_bgwriter: bool = false;
#[no_mangle]
pub static mut use_mmap: bool = false;
#[no_mangle]
pub static mut use_device: bool = false;
#[no_mangle]
pub static mut orioledb_use_sparse_files: bool = false;
#[no_mangle]
pub static mut device_filename: *mut c_char = std::ptr::null_mut();
#[no_mangle]
pub static mut mmap_data: *mut c_void = std::ptr::null_mut();
#[no_mangle]
pub static mut device_fd: c_int = -1;
#[no_mangle]
pub static mut device_length_guc: c_int = 0;
#[no_mangle]
pub static mut device_length: usize = 0;
#[no_mangle]
pub static mut o_checkpoint_completion_ratio: f64 = 0.5;
#[no_mangle]
pub static mut bgwriter_num_workers: c_int = 1;
#[no_mangle]
pub static mut max_io_concurrency: c_int = 0;
#[no_mangle]
pub static mut oProcData: *mut ODBProcData = std::ptr::null_mut();
#[no_mangle]
pub static mut default_compress: c_int = -1;
#[no_mangle]
pub static mut default_primary_compress: c_int = -1;
#[no_mangle]
pub static mut default_toast_compress: c_int = -1;
#[no_mangle]
pub static mut orioledb_table_description_compress: bool = false;
#[no_mangle]
pub static mut max_bridge_ctid_string: *mut c_char = std::ptr::null_mut();
#[no_mangle]
pub static mut max_bridge_ctid_blkno: pg_sys::BlockNumber = pg_sys::InvalidBlockNumber;
#[no_mangle]
pub static mut orioledb_s3_mode: bool = false;
#[no_mangle]
pub static mut s3_num_workers: c_int = 3;
#[no_mangle]
pub static mut s3_desired_size: c_int = 10000;
#[no_mangle]
pub static mut s3_queue_size_guc: c_int = 1024;
#[no_mangle]
pub static mut s3_host: *mut c_char = std::ptr::null_mut();
#[no_mangle]
pub static mut s3_use_https: bool = true;
#[no_mangle]
pub static mut s3_region: *mut c_char = std::ptr::null_mut();
#[no_mangle]
pub static mut s3_prefix: *mut c_char = std::ptr::null_mut();
#[no_mangle]
pub static mut s3_accesskey: *mut c_char = std::ptr::null_mut();
#[no_mangle]
pub static mut s3_secretkey: *mut c_char = std::ptr::null_mut();
#[no_mangle]
pub static mut s3_cainfo: *mut c_char = std::ptr::null_mut();
#[no_mangle]
pub static mut enable_rewind: bool = false;
#[no_mangle]
pub static mut rewind_max_time: c_int = 0;
#[no_mangle]
pub static mut rewind_max_transactions: c_int = 0;
#[no_mangle]
pub static mut logical_xid_buffers_guc: c_int = 64;
#[no_mangle]
pub static mut orioledb_strict_mode: bool = false;
#[no_mangle]
pub static mut replay_until_lsn: pg_sys::XLogRecPtr = 0;
#[no_mangle]
pub static mut replay_until_lsn_string: *mut c_char = std::ptr::null_mut();
#[no_mangle]
pub static mut min_read_page_checkpoint: u32 = u32::MAX;
#[no_mangle]
pub static mut max_read_page_checkpoint: u32 = 0;
#[no_mangle]
pub static mut btree_insert_context: MemoryContext = std::ptr::null_mut();
#[no_mangle]
pub static mut btree_seqscan_context: MemoryContext = std::ptr::null_mut();

#[no_mangle]
pub static mut page_pools: [OPagePool; 3] = unsafe { std::mem::zeroed() };
#[no_mangle]
pub static mut page_pools_size: [usize; 3] = [0; 3];
#[no_mangle]
pub static mut local_ppool: LocalPagePoolStruct = LocalPagePoolStruct {
    base: PagePool { ops: std::ptr::null() },
};

// Hook type aliases for PG18 (these are no longer in pg_sys by these names)
type base_init_startup_hook_type = Option<unsafe extern "C-unwind" fn()>;
type database_size_hook_type = Option<unsafe extern "C-unwind" fn(dbOid: pg_sys::Oid) -> i64>;
type AcceptInvalidationMessagesHookType = Option<unsafe extern "C-unwind" fn()>;
type CheckPoint_hook_type = Option<unsafe extern "C-unwind" fn(redo_pos: pg_sys::XLogRecPtr, flags: c_int)>;

static mut prev_shmem_startup_hook: pg_sys::shmem_startup_hook_type = None;
static mut prev_shmem_request_hook: Option<unsafe extern "C-unwind" fn()> = None;
static mut prev_base_init_startup_hook: base_init_startup_hook_type = None;
static mut prev_get_relation_info_hook: pg_sys::get_relation_info_hook_type = None;
#[no_mangle]
pub static mut prev_database_size_hook: database_size_hook_type = None;
static mut prev_AcceptInvalidationMessagesHook: AcceptInvalidationMessagesHookType = None;

#[cfg(not(any(feature = "pg18", feature = "pg19")))]
static mut prev_skip_tree_height_hook: Option<unsafe extern "C-unwind" fn(indexRelation: Relation) -> bool> = None;

#[no_mangle]
pub static mut next_CheckPoint_hook: CheckPoint_hook_type = None;

static mut serializable_mode_options: [pg_sys::config_enum_entry; 4] = [
    pg_sys::config_enum_entry {
        name: b"table_lock\0".as_ptr() as *const c_char,
        val: O_SERIALIZABLE_TABLE_LOCK,
        hidden: false,
    },
    pg_sys::config_enum_entry {
        name: b"error\0".as_ptr() as *const c_char,
        val: O_SERIALIZABLE_ERROR,
        hidden: false,
    },
    pg_sys::config_enum_entry {
        name: b"repeatable_read\0".as_ptr() as *const c_char,
        val: O_SERIALIZABLE_REPEATABLE_READ,
        hidden: false,
    },
    pg_sys::config_enum_entry {
        name: std::ptr::null(),
        val: 0,
        hidden: false,
    },
];

#[no_mangle]
pub static mut checkpoint_state: *mut CheckpointState = std::ptr::null_mut();

// ---------------------------------------------------------------------------
// Extern Declarations
// ---------------------------------------------------------------------------
extern "C-unwind" {
    pub fn checkpoint_shmem_size() -> usize;
    pub fn checkpoint_shmem_init(ptr: *mut c_void, found: bool);
    pub fn undo_shmem_size() -> usize;
    pub fn undo_shmem_init(ptr: *mut c_void, found: bool);
    pub fn s3_queue_shmem_size() -> usize;
    pub fn s3_queue_shmem_init(ptr: *mut c_void, found: bool);
    pub fn s3_headers_shmem_size() -> usize;
    pub fn s3_headers_shmem_init(ptr: *mut c_void, found: bool);

    pub fn rewind_shmem_size() -> usize;
    pub fn rewind_shmem_init(ptr: *mut c_void, found: bool);
    pub fn logical_xid_shmem_size() -> usize;
    pub fn logical_xid_shmem_init(ptr: *mut c_void, found: bool);

    pub fn request_btree_io_lwlocks();
    pub fn init_btree_io_lwlocks();
    pub fn o_btree_init_unique_lwlocks();
    pub fn o_btree_load_shmem(desc: *mut c_void);
    pub fn btree_io_error_cleanup();
    pub fn btree_mark_incomplete_splits();

    pub fn recovery_cleanup_old_files(last_chkp: u32, is_tmp: bool);
    pub fn o_perform_checkpoint(redo_pos: pg_sys::XLogRecPtr, flags: c_int);
    pub fn o_after_checkpoint_cleanup_hook(checkpoint_redo: pg_sys::XLogRecPtr, flags: c_int);

    pub fn undo_xact_callback(event: pg_sys::XactEvent::Type, arg: *mut c_void);
    pub fn undo_subxact_callback(event: pg_sys::SubXactEvent::Type, mySubid: pg_sys::SubTransactionId, parentSubid: pg_sys::SubTransactionId, arg: *mut c_void);
    pub fn undo_snapshot_register_hook(snapshot: *mut c_void);
    pub fn undo_snapshot_deregister_hook(snapshot: *mut c_void);
    pub fn release_undo_size(undo_type: c_int);

    pub fn orioledb_enable_rewind_check_hook(newval: *mut bool, extra: *mut *mut c_void, source: pg_sys::GucSource::Type) -> bool;
    pub fn orioledb_replay_until_lsn_check_hook(newval: *mut *mut c_char, extra: *mut *mut c_void, source: pg_sys::GucSource::Type) -> bool;
    pub fn orioledb_replay_until_lsn_assign_hook(newval: *const c_char, extra: *mut c_void);

    pub fn o_ppool_estimate_space(pool: *mut c_void, offset: usize, count: usize, debug: bool) -> usize;
    pub fn o_ppool_shmem_init(pool: *mut c_void, ptr: *mut c_void, found: bool);
    pub fn ppool_dirty_pages_count(pool: *mut c_void) -> usize;
    pub fn local_ppool_init(pool: *mut c_void);

    pub fn is_orioledb_rel(rel: pg_sys::Relation) -> bool;
    pub fn relation_get_descr(rel: pg_sys::Relation) -> *mut c_void;

    pub fn o_sys_caches_init();
    pub fn o_reset_syscache_hooks();
    pub fn o_replay_saved_inval_messages();
    pub fn o_invalidate_descrs(arg1: Oid, arg2: Oid, arg3: Oid);

    pub static mut o_scan_methods: pg_sys::CustomScanMethods;
    pub fn register_bgwriter(num: c_int);
    pub fn register_rewind_worker();

    pub fn s3_put_lock_file();
    pub fn s3_delete_lock_file();
    pub fn s3_check_control(errmsg: *mut *const c_char, errdetail: *mut *const c_char) -> bool;
    pub fn register_s3worker(num: c_int);
    pub fn s3_headers_error_cleanup();

    pub fn o_detoast(varlena: *mut pg_sys::varlena) -> *mut pg_sys::varlena;
    pub fn register_o_detoast_func(func: unsafe extern "C-unwind" fn(*mut pg_sys::varlena) -> *mut pg_sys::varlena);

    pub fn o_tableam_descr_init();
    pub fn o_compress_init();
    pub fn o_compress_max_lvl() -> c_int;

    pub fn wait_for_oxid(oxid: u64, wait: bool) -> bool;
    pub fn o_recovery_shutdown_hook(code: c_int, arg: pg_sys::Datum);


    pub static mut old_set_rel_pathlist_hook: pg_sys::set_rel_pathlist_hook_type;
    pub fn orioledb_set_rel_pathlist_hook(root: *mut pg_sys::PlannerInfo, rel: *mut pg_sys::RelOptInfo, rti: pg_sys::Index, rte: *mut pg_sys::RangeTblEntry);
    pub fn orioledb_set_plain_rel_pathlist_hook(root: *mut pg_sys::PlannerInfo, rel: *mut pg_sys::RelOptInfo, rti: pg_sys::Index, rte: *mut pg_sys::RangeTblEntry);

    pub fn orioledb_get_running_transactions_extension(extension: *mut c_void);
    pub fn orioledb_wait_snapshot(extension: *mut c_void);

    pub fn o_newlocale_from_collation() -> bool;
    pub fn o_keybitmap_pk_mode(primary: *mut c_void, arg2: *mut c_void) -> c_int;

    pub fn orioledb_setup_ddl_hooks();
    pub fn o_reset_syscache_hooks_on_error();
    pub fn o_ddl_cleanup();

    pub fn orioledb_reset_xmin_hook();

    pub fn get_undo_meta_by_type(undo_type: c_int) -> *mut UndoMeta;
    pub fn ucm_check_map(map: *mut UsageCountMap) -> bool;

    pub fn orioledb_snapshot_hook(snapshot: pg_sys::Snapshot);
    pub fn orioledb_calculate_database_size(dbOid: Oid) -> i64;
    pub fn orioledb_recovery_stops_before_hook(record: *mut pg_sys::XLogReaderState) -> bool;
    pub fn orioledb_vacuum_horizon_hook(relation: pg_sys::Relation) -> pg_sys::TransactionId;
    pub fn o_xact_redo_hook(record: *mut pg_sys::XLogReaderState);
    pub fn orioledb_indexam_routine_hook(relation: pg_sys::Relation, amroutine: *mut pg_sys::IndexAmRoutine);
    pub fn recovery_get_effective_replay_ptr() -> pg_sys::XLogRecPtr;
    pub fn orioledb_get_xidless_commit_lsn(backendId: c_int, lsn: *mut pg_sys::XLogRecPtr) -> bool;
    pub fn orioledb_skip_tree_height_hook(indexRelation: Relation) -> bool;
    pub fn orioledb_error_cleanup_hook();
    pub fn o_page_desc_init(desc: *mut OrioleDBPageDesc);
    pub fn orioledb_memsize() -> usize;
    pub fn orioledb_check_shmem();
    pub fn check_debug_max_bridge_ctid(newval: *mut *mut c_char, extra: *mut *mut c_void, source: pg_sys::GucSource::Type) -> bool;
    pub fn assign_debug_max_bridge_ctid(newval: *const c_char, extra: *mut c_void);
}

extern "C-unwind" {
    pub fn o_recovery_start_hook();
    pub fn o_recovery_finish_hook(is_cleanup: bool);
    pub fn orioledb_redo(record: *mut pg_sys::XLogReaderState);
    pub fn orioledb_rm_desc(buf: pg_sys::StringInfo, record: *mut pg_sys::XLogReaderState);
    pub fn orioledb_rm_identify(info: u8) -> *const c_char;
    pub fn orioledb_decode(record: *mut c_void);

    pub fn orioledb_AcceptInvalidationMessagesHook();
    pub fn orioledb_usercache_hook(arg: Datum, arg1: Oid, arg2: Oid, arg3: Oid);
    pub fn orioledb_get_relation_info_hook(
        root: *mut pg_sys::PlannerInfo,
        relationObjectId: Oid,
        inhparent: bool,
        rel: *mut pg_sys::RelOptInfo,
    );
    pub fn CacheRegisterUsercacheCallback(callback: Option<unsafe extern "C-unwind" fn(arg: Datum, arg1: Oid, arg2: Oid, arg3: Oid)>, arg: Datum);
    pub fn o_recovery_cleanup();
}

// OrioleDB-patched PostgreSQL hook globals (not in standard pgrx bindings)
extern "C" {
    pub static mut CheckPoint_hook: CheckPoint_hook_type;
    pub static mut AcceptInvalidationMessagesHook: AcceptInvalidationMessagesHookType;
    pub static mut database_size_hook: database_size_hook_type;
    pub static mut base_init_startup_hook: base_init_startup_hook_type;
    pub static mut AddinShmemInitLock: pg_sys::LWLock;
    pub static mut autovacuum_worker_slots: c_int;
    pub static mut after_checkpoint_cleanup_hook: Option<unsafe extern "C-unwind" fn(checkpoint_redo: pg_sys::XLogRecPtr, flags: c_int)>;
    pub static mut CustomErrorCleanupHook: Option<unsafe extern "C-unwind" fn()>;

    pub static mut set_plain_rel_pathlist_hook: pg_sys::set_rel_pathlist_hook_type;
    pub static mut get_xidless_commit_lsn_hook: Option<unsafe extern "C-unwind" fn(backendId: c_int, lsn: *mut pg_sys::XLogRecPtr) -> bool>;
    pub static mut RedoShutdownHook: Option<unsafe extern "C-unwind" fn(code: c_int, arg: pg_sys::Datum)>;
    pub static mut snapshot_hook: Option<unsafe extern "C-unwind" fn(snapshot: pg_sys::Snapshot)>;
    pub static mut snapshot_register_hook: Option<unsafe extern "C-unwind" fn(snapshot: *mut c_void)>;
    pub static mut snapshot_deregister_hook: Option<unsafe extern "C-unwind" fn(snapshot: *mut c_void)>;
    pub static mut reset_xmin_hook: Option<unsafe extern "C-unwind" fn()>;

    pub static mut xact_redo_hook: Option<unsafe extern "C-unwind" fn(record: *mut pg_sys::XLogReaderState)>;
    pub static mut pg_newlocale_from_collation_hook: Option<unsafe extern "C-unwind" fn() -> bool>;
    pub static mut IndexAMRoutineHook: Option<unsafe extern "C-unwind" fn(relation: pg_sys::Relation, amroutine: *mut pg_sys::IndexAmRoutine)>;
    pub static mut getRunningTransactionsExtension: Option<unsafe extern "C-unwind" fn(extension: *mut c_void)>;
    pub static mut waitSnapshotHook: Option<unsafe extern "C-unwind" fn(extension: *mut c_void)>;
    pub static mut GetReplayXlogPtrHook: Option<unsafe extern "C-unwind" fn() -> pg_sys::XLogRecPtr>;
    pub static mut RecoveryStopsBeforeHook: Option<unsafe extern "C-unwind" fn(record: *mut pg_sys::XLogReaderState) -> bool>;
    pub static mut VacuumHorizonHook: Option<unsafe extern "C-unwind" fn(relation: pg_sys::Relation) -> pg_sys::TransactionId>;
}

// ---------------------------------------------------------------------------
// Shmem & Hook Wrappers
// ---------------------------------------------------------------------------
unsafe extern "C-unwind" fn checkpoint_shmem_size_wrapper() -> usize { checkpoint_shmem_size() }
unsafe extern "C-unwind" fn checkpoint_shmem_init_wrapper(ptr: *mut c_void, found: bool) { checkpoint_shmem_init(ptr, found); }

unsafe extern "C-unwind" fn undo_shmem_size_wrapper() -> usize { undo_shmem_size() }
unsafe extern "C-unwind" fn undo_shmem_init_wrapper(ptr: *mut c_void, found: bool) { undo_shmem_init(ptr, found); }

unsafe extern "C-unwind" fn s3_queue_shmem_size_wrapper() -> usize { s3_queue_shmem_size() }
unsafe extern "C-unwind" fn s3_queue_shmem_init_wrapper(ptr: *mut c_void, found: bool) { s3_queue_shmem_init(ptr, found); }

unsafe extern "C-unwind" fn s3_headers_shmem_size_wrapper() -> usize { s3_headers_shmem_size() }
unsafe extern "C-unwind" fn s3_headers_shmem_init_wrapper(ptr: *mut c_void, found: bool) { s3_headers_shmem_init(ptr, found); }

unsafe extern "C-unwind" fn stopevents_shmem_size_wrapper() -> usize { crate::utils::stopevent::StopEventShmemSize() }
unsafe extern "C-unwind" fn stopevents_shmem_init_wrapper(ptr: *mut c_void, found: bool) { crate::utils::stopevent::StopEventShmemInit(ptr, found); }

unsafe extern "C-unwind" fn rewind_shmem_size_wrapper() -> usize { rewind_shmem_size() }
unsafe extern "C-unwind" fn rewind_shmem_init_wrapper(ptr: *mut c_void, found: bool) { rewind_shmem_init(ptr, found); }

unsafe extern "C-unwind" fn logical_xid_shmem_size_wrapper() -> usize { logical_xid_shmem_size() }
unsafe extern "C-unwind" fn logical_xid_shmem_init_wrapper(ptr: *mut c_void, found: bool) { logical_xid_shmem_init(ptr, found); }

unsafe extern "C-unwind" fn o_proc_shmem_needs() -> usize {
    (max_procs as usize) * std::mem::size_of::<ODBProcData>()
}

unsafe extern "C-unwind" fn o_proc_shmem_init(ptr: *mut c_void, found: bool) {
    oProcData = ptr as *mut ODBProcData;
    if !found {
        let procs = std::slice::from_raw_parts_mut(oProcData, max_procs as usize);
        for item in procs {
            for j in 0..3 {
                pg_sys::pg_atomic_init_u64(&mut item.undoRetainLocations[j].reservedUndoLocation, u64::MAX);
                pg_sys::pg_atomic_init_u64(&mut item.undoRetainLocations[j].snapshotRetainUndoLocation, u64::MAX);
                pg_sys::pg_atomic_init_u64(&mut item.undoRetainLocations[j].transactionUndoRetainLocation, u64::MAX);
            }
            pg_sys::pg_atomic_init_u64(&mut item.commitInProgressXlogLocation, u64::MAX);
            pg_sys::pg_atomic_init_u64(&mut item.xmin, u64::MAX);
            pg_sys::pg_atomic_init_u64(&mut item.pendingSkUndoLoc, u64::MAX);
            item.autonomousNestingLevel = 0;
            std::ptr::write_bytes(item.vxids.as_mut_ptr(), 0, item.vxids.len());
            
            let undo_meta = get_undo_meta_by_type(0);
            let tranche_id = (*undo_meta).undoStackLocationsFlushLockTrancheId;
            pg_sys::LWLockInitialize(&mut item.undoStackLocationsFlushLock, tranche_id);
            item.flushUndoLocations = false;
            for j in 0..32 {
                for k in 0..3 {
                    pg_sys::pg_atomic_init_u64(&mut item.undoStackLocations[j][k].location, u64::MAX);
                    pg_sys::pg_atomic_init_u64(&mut item.undoStackLocations[j][k].branchLocation, u64::MAX);
                    pg_sys::pg_atomic_init_u64(&mut item.undoStackLocations[j][k].subxactLocation, u64::MAX);
                    pg_sys::pg_atomic_init_u64(&mut item.undoStackLocations[j][k].onCommitLocation, u64::MAX);
                }
                item.vxids[j].oxid = u64::MAX;
            }
        }
    }
}

unsafe extern "C-unwind" fn ppools_shmem_needs() -> usize {
    let mut size: usize = 0;
    for i in 0..3 {
        size = size.checked_add(page_pools_size[i]).unwrap();
    }
    size = size.checked_add(orioledb_buffers_size).unwrap();
    size = size.checked_add(page_descs_size).unwrap();
    size
}

unsafe extern "C-unwind" fn ppools_shmem_init(mut ptr: *mut c_void, found: bool) {
    let mut page_pools_ptr: [*mut c_void; 3] = [std::ptr::null_mut(); 3];
    for i in 0..3 {
        page_pools_ptr[i] = ptr;
        ptr = ptr.add(page_pools_size[i]);
    }
    o_shared_buffers = ptr;
    ptr = ptr.add(orioledb_buffers_size);
    page_descs = ptr as *mut OrioleDBPageDesc;

    for i in 0..3 {
        o_ppool_shmem_init(&mut page_pools[i] as *mut OPagePool as *mut c_void, page_pools_ptr[i], found);
    }

    if !found {
        for i in 0..orioledb_buffers_count {
            let p = o_shared_buffers.add(i * 8192) as *mut pg_sys::pg_atomic_uint64;
            pg_sys::pg_atomic_init_u64(p, 0x1FFF);
            let page_change_count_ptr = (p as *mut u8).add(8) as *mut u32;
            *page_change_count_ptr = 0;
        }

        let desc_count = page_descs_size / std::mem::size_of::<OrioleDBPageDesc>();
        for i in 0..desc_count {
            o_page_desc_init(page_descs.add(i));
        }
    }
}

unsafe extern "C-unwind" fn orioledb_shmem_request() {
    if let Some(prev) = prev_shmem_request_hook {
        prev();
    }
    pg_sys::RequestAddinShmemSpace(orioledb_memsize());
    request_btree_io_lwlocks();
    pg_sys::RequestNamedLWLockTranche(b"orioledb_unique_locks\0".as_ptr() as *const c_char, max_procs * 4);
}

unsafe extern "C-unwind" fn orioledb_shmem_startup() {
    if let Some(prev) = prev_shmem_startup_hook {
        prev();
    }
    shared_segment = std::ptr::null_mut();
    pg_sys::LWLockAcquire(std::ptr::addr_of_mut!(AddinShmemInitLock), pg_sys::LWLockMode::LW_EXCLUSIVE);
    
    let mut found = false;
    shared_segment = pg_sys::ShmemInitStruct(
        b"orioledb_engine\0".as_ptr() as *const c_char,
        orioledb_memsize(),
        &mut found,
    );
    let mut ptr = shared_segment;
    for item in shmemItems.iter() {
        (item.shmem_init)(ptr, found);
        let aligned_size = ((item.shmem_size)() + 63) & !63;
        ptr = ptr.add(aligned_size);
    }
    
    init_btree_io_lwlocks();
    o_btree_init_unique_lwlocks();
    pg_sys::before_shmem_exit(Some(orioledb_on_shmem_exit), pg_sys::Datum::from(0));
    pg_sys::LWLockRelease(std::ptr::addr_of_mut!(AddinShmemInitLock));
    
    shared_segment_initialized = true;
}

unsafe extern "C-unwind" fn orioledb_on_shmem_exit(_code: c_int, _arg: Datum) {
    if !pg_sys::MyProc.is_null() {
        let procno = pg_sys::MyProcNumber;
        if procno >= 0 && procno < max_procs {
            pg_sys::pg_atomic_write_u64(&mut (*oProcData.add(procno as usize)).xmin, u64::MAX);
        }
    }
    if orioledb_s3_mode {
        s3_delete_lock_file();
    }
}

unsafe extern "C-unwind" fn o_base_init_startup_hook() {
    if pg_sys::MyBackendType == pg_sys::BackendType::B_STARTUP {
        if remove_old_checkpoint_files {
            pgrx::ereport!(
                PgLogLevel::LOG,
                pg_sys::errcodes::PgSqlErrorCode::ERRCODE_SUCCESSFUL_COMPLETION,
                format!("Cleanup of old files at startup. Checkpoint {}", (*checkpoint_state).lastCheckpointNumber)
            );
            recovery_cleanup_old_files((*checkpoint_state).lastCheckpointNumber, true);
            recovery_cleanup_old_files((*checkpoint_state).lastCheckpointNumber, false);
        }
    }
    if let Some(prev) = prev_base_init_startup_hook {
        prev();
    }
}

static mut shmemItems: [ShmemItem; 9] = [
    ShmemItem { shmem_size: o_proc_shmem_needs, shmem_init: o_proc_shmem_init },
    ShmemItem { shmem_size: ppools_shmem_needs, shmem_init: ppools_shmem_init },
    ShmemItem { shmem_size: checkpoint_shmem_size_wrapper, shmem_init: checkpoint_shmem_init_wrapper },
    ShmemItem { shmem_size: undo_shmem_size_wrapper, shmem_init: undo_shmem_init_wrapper },
    ShmemItem { shmem_size: s3_queue_shmem_size_wrapper, shmem_init: s3_queue_shmem_init_wrapper },
    ShmemItem { shmem_size: s3_headers_shmem_size_wrapper, shmem_init: s3_headers_shmem_init_wrapper },
    ShmemItem { shmem_size: stopevents_shmem_size_wrapper, shmem_init: stopevents_shmem_init_wrapper },
    ShmemItem { shmem_size: rewind_shmem_size_wrapper, shmem_init: rewind_shmem_init_wrapper },
    ShmemItem { shmem_size: logical_xid_shmem_size_wrapper, shmem_init: logical_xid_shmem_init_wrapper },
];

// ---------------------------------------------------------------------------
// _PG_init Entrypoint
// ---------------------------------------------------------------------------
#[no_mangle]
pub unsafe extern "C-unwind" fn _PG_init() {
    if !pg_sys::process_shared_preload_libraries_in_progress {
        return;
    }
    
    let _ = std::fs::create_dir_all("orioledb_data");
    let _ = std::fs::create_dir_all("orioledb_undo");
    let _ = std::fs::create_dir_all("orioledb_data/1");
    
    #[cfg(feature = "pg18")]
    {
        max_procs = pg_sys::MaxConnections 
            + autovacuum_worker_slots 
            + 1 
            + pg_sys::max_worker_processes 
            + pg_sys::max_wal_senders 
            + pg_sys::NUM_SPECIAL_WORKER_PROCS as i32
            + pg_sys::NUM_AUXILIARY_PROCS as i32;
    }
    #[cfg(feature = "pg17")]
    {
        max_procs = pg_sys::MaxConnections 
            + pg_sys::autovacuum_max_workers 
            + 1 
            + pg_sys::max_worker_processes 
            + pg_sys::max_wal_senders 
            + pg_sys::NUM_SPECIAL_WORKER_PROCS as i32
            + pg_sys::NUM_AUXILIARY_PROCS as i32;
    }
    #[cfg(not(any(feature = "pg17", feature = "pg18")))]
    {
        max_procs = pg_sys::MaxConnections 
            + pg_sys::autovacuum_max_workers 
            + 2 
            + pg_sys::max_worker_processes 
            + pg_sys::max_wal_senders 
            + pg_sys::NUM_AUXILIARY_PROCS as i32;
    }

    let min_pool_size = std::cmp::max(1024, max_procs * 4) as c_int;

    pg_sys::DefineCustomBoolVariable(
        b"orioledb.debug_disable_pools_limit\0".as_ptr() as *const c_char,
        b"Disables pools minimal limit for debug.\0".as_ptr() as *const c_char,
        std::ptr::null(),
        &mut debug_disable_pools_limit,
        false,
        PGC_POSTMASTER,
        0,
        None,
        None,
        None,
    );

    pg_sys::DefineCustomEnumVariable(
        b"orioledb.serializable\0".as_ptr() as *const c_char,
        b"How OrioleDB handles SERIALIZABLE isolation.\0".as_ptr() as *const c_char,
        b"table_lock acquires a coarse ExclusiveLock per touched relation; error rejects SERIALIZABLE transactions; repeatable_read silently downgrades them to REPEATABLE READ.\0".as_ptr() as *const c_char,
        &mut orioledb_serializable_mode,
        O_SERIALIZABLE_TABLE_LOCK,
        serializable_mode_options.as_mut_ptr(),
        PGC_USERSET,
        0,
        None,
        None,
        None,
    );

    pg_sys::DefineCustomBoolVariable(
        b"orioledb.debug_disable_multi_insert\0".as_ptr() as *const c_char,
        b"Disable the batched same-leaf primary insert path.\0".as_ptr() as *const c_char,
        b"Debug switch. When on, orioledb_multi_insert falls back to per-row o_tbl_insert instead of draining adjacent ordered keys into the same primary leaf under one lwlock.\0".as_ptr() as *const c_char,
        &mut orioledb_debug_disable_multi_insert,
        false,
        PGC_USERSET,
        0,
        None,
        None,
        None,
    );

    pg_sys::DefineCustomIntVariable(
        b"orioledb.main_buffers\0".as_ptr() as *const c_char,
        b"Size of orioledb engine shared buffers for main data.\0".as_ptr() as *const c_char,
        std::ptr::null(),
        &mut main_buffers_guc,
        std::cmp::max(8192, min_pool_size),
        if debug_disable_pools_limit { 1 } else { min_pool_size },
        c_int::MAX,
        PGC_POSTMASTER,
        pg_sys::GUC_UNIT_BLOCKS as i32,
        None,
        None,
        None,
    );

    pg_sys::DefineCustomIntVariable(
        b"orioledb.free_tree_buffers\0".as_ptr() as *const c_char,
        b"Size of orioledb engine shared buffers for free extents BTrees.\0".as_ptr() as *const c_char,
        std::ptr::null(),
        &mut free_tree_buffers_guc,
        min_pool_size,
        if debug_disable_pools_limit { 1 } else { min_pool_size },
        c_int::MAX,
        PGC_POSTMASTER,
        pg_sys::GUC_UNIT_BLOCKS as i32,
        None,
        None,
        None,
    );

    pg_sys::DefineCustomIntVariable(
        b"orioledb.catalog_buffers\0".as_ptr() as *const c_char,
        b"Size of orioledb engine shared buffers for catalog BTrees.\0".as_ptr() as *const c_char,
        std::ptr::null(),
        &mut catalog_buffers_guc,
        min_pool_size,
        if debug_disable_pools_limit { 1 } else { min_pool_size },
        c_int::MAX,
        PGC_POSTMASTER,
        pg_sys::GUC_UNIT_BLOCKS as i32,
        None,
        None,
        None,
    );

    pg_sys::DefineCustomIntVariable(
        b"orioledb.undo_buffers\0".as_ptr() as *const c_char,
        b"Size of orioledb engine undo log buffers.\0".as_ptr() as *const c_char,
        b"Each undo type's circular buffer is at least max_procs * 2 * O_MAX_UNDO_RECORD_SIZE bytes, so the actual buffer at startup may be larger than what this GUC requested when max_procs is high.\0".as_ptr() as *const c_char,
        &mut undo_buffers_guc,
        std::cmp::max(128, 16 * max_procs),
        16 * max_procs,
        c_int::MAX,
        PGC_POSTMASTER,
        pg_sys::GUC_UNIT_BLOCKS as i32,
        None,
        None,
        None,
    );

    pg_sys::DefineCustomIntVariable(
        b"orioledb.temp_buffers\0".as_ptr() as *const c_char,
        b"Size of orioledb engine buffers for temporary tables.\0".as_ptr() as *const c_char,
        std::ptr::null(),
        &mut temp_buffers_guc,
        8192 * 8,
        if debug_disable_pools_limit { 1 } else { 8192 },
        c_int::MAX,
        PGC_POSTMASTER,
        pg_sys::GUC_UNIT_BLOCKS as i32,
        None,
        None,
        None,
    );

    pg_sys::DefineCustomRealVariable(
        b"orioledb.regular_block_undo_circular_buffer_fraction\0".as_ptr() as *const c_char,
        b"Fraction of circular buffer for block-level undo of regular tables.\0".as_ptr() as *const c_char,
        std::ptr::null(),
        &mut regular_block_undo_circular_buffer_fraction,
        0.45,
        0.05,
        0.95,
        PGC_POSTMASTER,
        0,
        None,
        None,
        None,
    );

    pg_sys::DefineCustomRealVariable(
        b"orioledb.system_undo_circular_buffer_fraction\0".as_ptr() as *const c_char,
        b"Fraction of circular buffer for undo of system trees.\0".as_ptr() as *const c_char,
        std::ptr::null(),
        &mut system_undo_circular_buffer_fraction,
        0.10,
        0.05,
        0.95,
        PGC_POSTMASTER,
        0,
        None,
        None,
        None,
    );

    pg_sys::DefineCustomIntVariable(
        b"orioledb.xid_buffers\0".as_ptr() as *const c_char,
        b"Size of orioledb engine xid buffers.\0".as_ptr() as *const c_char,
        std::ptr::null(),
        &mut xid_buffers_guc,
        128,
        128,
        c_int::MAX,
        PGC_POSTMASTER,
        pg_sys::GUC_UNIT_BLOCKS as i32,
        None,
        None,
        None,
    );

    pg_sys::DefineCustomIntVariable(
        b"orioledb.rewind_buffers\0".as_ptr() as *const c_char,
        b"Size of orioledb engine rewind buffers.\0".as_ptr() as *const c_char,
        std::ptr::null(),
        &mut rewind_buffers_guc,
        128,
        6,
        c_int::MAX,
        PGC_POSTMASTER,
        pg_sys::GUC_UNIT_BLOCKS as i32,
        None,
        None,
        None,
    );

    pg_sys::DefineCustomBoolVariable(
        b"orioledb.remove_old_checkpoint_files\0".as_ptr() as *const c_char,
        b"Remove temporary *.tmp and *.map files after checkpoint.\0".as_ptr() as *const c_char,
        std::ptr::null(),
        &mut remove_old_checkpoint_files,
        true,
        PGC_POSTMASTER,
        0,
        None,
        None,
        None,
    );

    pg_sys::DefineCustomBoolVariable(
        b"orioledb.skip_unmodified_trees\0".as_ptr() as *const c_char,
        b"Skip reading of unmodified trees during checkpointing.\0".as_ptr() as *const c_char,
        std::ptr::null(),
        &mut skip_unmodified_trees,
        true,
        PGC_POSTMASTER,
        0,
        None,
        None,
        None,
    );

    pg_sys::DefineCustomBoolVariable(
        b"orioledb.debug_disable_bgwriter\0".as_ptr() as *const c_char,
        b"Disables bgwriter for debug.\0".as_ptr() as *const c_char,
        std::ptr::null(),
        &mut debug_disable_bgwriter,
        false,
        PGC_POSTMASTER,
        0,
        None,
        None,
        None,
    );

    pg_sys::DefineCustomIntVariable(
        b"orioledb.recovery_queue_size\0".as_ptr() as *const c_char,
        b"Size of orioledb recovery queue per worker.\0".as_ptr() as *const c_char,
        std::ptr::null(),
        &mut s3_queue_size_guc,
        1024,
        512,
        2147483647,
        PGC_POSTMASTER,
        pg_sys::GUC_UNIT_KB as i32,
        None,
        None,
        None,
    );

    pg_sys::DefineCustomIntVariable(
        b"orioledb.recovery_pool_size\0".as_ptr() as *const c_char,
        b"Sets the number of recovery workers.\0".as_ptr() as *const c_char,
        std::ptr::null(),
        &mut rewind_max_time,
        3,
        1,
        128,
        PGC_POSTMASTER,
        0,
        None,
        None,
        None,
    );

    pg_sys::DefineCustomIntVariable(
        b"orioledb.recovery_idx_pool_size\0".as_ptr() as *const c_char,
        b"Sets the number of recovery index build workers.\0".as_ptr() as *const c_char,
        std::ptr::null(),
        &mut rewind_max_transactions,
        3,
        1,
        128,
        PGC_POSTMASTER,
        0,
        None,
        None,
        None,
    );

    pg_sys::DefineCustomIntVariable(
        b"orioledb.logical_xid_buffers\0".as_ptr() as *const c_char,
        b"Size of shared memory buffers for subtransaction logical XIDs.\0".as_ptr() as *const c_char,
        std::ptr::null(),
        &mut logical_xid_buffers_guc,
        64,
        1,
        1024,
        PGC_POSTMASTER,
        pg_sys::GUC_UNIT_BLOCKS as i32,
        None,
        None,
        None,
    );

    pg_sys::DefineCustomRealVariable(
        b"orioledb.checkpoint_completion_ratio\0".as_ptr() as *const c_char,
        b"ratio of orioledb checkpoint to postgres checkpoint.\0".as_ptr() as *const c_char,
        std::ptr::null(),
        &mut o_checkpoint_completion_ratio,
        0.5,
        0.0,
        1.0,
        PGC_POSTMASTER,
        0,
        None,
        None,
        None,
    );

    pg_sys::DefineCustomIntVariable(
        b"orioledb.bgwriter_num_workers\0".as_ptr() as *const c_char,
        b"Number of background writers.\0".as_ptr() as *const c_char,
        std::ptr::null(),
        &mut bgwriter_num_workers,
        1,
        1,
        pg_sys::MaxBackends,
        PGC_POSTMASTER,
        0,
        None,
        None,
        None,
    );

    pg_sys::DefineCustomIntVariable(
        b"orioledb.max_io_concurrency\0".as_ptr() as *const c_char,
        b"Number of maximum concurrent IO operations.\0".as_ptr() as *const c_char,
        std::ptr::null(),
        &mut max_io_concurrency,
        0,
        0,
        c_int::MAX,
        PGC_POSTMASTER,
        0,
        None,
        None,
        None,
    );

    pg_sys::DefineCustomBoolVariable(
        b"orioledb.use_mmap\0".as_ptr() as *const c_char,
        b"Store data in the mmap'ed file.\0".as_ptr() as *const c_char,
        std::ptr::null(),
        &mut use_mmap,
        false,
        PGC_POSTMASTER,
        0,
        None,
        None,
        None,
    );

    pg_sys::DefineCustomStringVariable(
        b"orioledb.device_filename\0".as_ptr() as *const c_char,
        b"Data file for mmap.\0".as_ptr() as *const c_char,
        std::ptr::null(),
        &mut device_filename,
        std::ptr::null(),
        PGC_POSTMASTER,
        0,
        None,
        None,
        None,
    );

    pg_sys::DefineCustomIntVariable(
        b"orioledb.device_length\0".as_ptr() as *const c_char,
        b"Size of mmap.\0".as_ptr() as *const c_char,
        std::ptr::null(),
        &mut device_length_guc,
        0,
        0,
        c_int::MAX,
        PGC_POSTMASTER,
        pg_sys::GUC_UNIT_BLOCKS as i32,
        None,
        None,
        None,
    );

    pg_sys::DefineCustomIntVariable(
        b"orioledb.default_compress\0".as_ptr() as *const c_char,
        b"Default compression level.\0".as_ptr() as *const c_char,
        std::ptr::null(),
        &mut default_compress,
        -1,
        -1,
        o_compress_max_lvl(),
        PGC_USERSET,
        0,
        None,
        None,
        None,
    );

    pg_sys::DefineCustomIntVariable(
        b"orioledb.default_primary_compress\0".as_ptr() as *const c_char,
        b"Default compression level of primary index.\0".as_ptr() as *const c_char,
        std::ptr::null(),
        &mut default_primary_compress,
        -1,
        -1,
        o_compress_max_lvl(),
        PGC_USERSET,
        0,
        None,
        None,
        None,
    );

    pg_sys::DefineCustomIntVariable(
        b"orioledb.default_toast_compress\0".as_ptr() as *const c_char,
        b"Default compression level of TOAST.\0".as_ptr() as *const c_char,
        std::ptr::null(),
        &mut default_toast_compress,
        -1,
        -1,
        o_compress_max_lvl(),
        PGC_USERSET,
        0,
        None,
        None,
        None,
    );

    pg_sys::DefineCustomBoolVariable(
        b"orioledb.table_description_compress\0".as_ptr() as *const c_char,
        b"Display compression column in orioledb_table_description\0".as_ptr() as *const c_char,
        std::ptr::null(),
        &mut orioledb_table_description_compress,
        false,
        PGC_USERSET,
        0,
        None,
        None,
        None,
    );

    pg_sys::DefineCustomBoolVariable(
        b"orioledb.use_sparse_files\0".as_ptr() as *const c_char,
        b"Punch sparse file holes for free blocks\0".as_ptr() as *const c_char,
        std::ptr::null(),
        &mut orioledb_use_sparse_files,
        false,
        PGC_POSTMASTER,
        0,
        None,
        None,
        None,
    );

    pg_sys::DefineCustomStringVariable(
        b"orioledb.debug_max_bridge_ctid_blkno\0".as_ptr() as *const c_char,
        b"Sets maximum value for bridge ctid for its overflow testing\0".as_ptr() as *const c_char,
        std::ptr::null(),
        &mut max_bridge_ctid_string,
        b"\0".as_ptr() as *const c_char,
        PGC_POSTMASTER,
        0,
        Some(check_debug_max_bridge_ctid),
        Some(assign_debug_max_bridge_ctid),
        None,
    );

    pg_sys::DefineCustomBoolVariable(
        b"orioledb.s3_mode\0".as_ptr() as *const c_char,
        b"The OrioleDB function mode on top of S3 storage\0".as_ptr() as *const c_char,
        std::ptr::null(),
        &mut orioledb_s3_mode,
        false,
        PGC_POSTMASTER,
        0,
        None,
        None,
        None,
    );

    pg_sys::DefineCustomIntVariable(
        b"orioledb.s3_queue_size\0".as_ptr() as *const c_char,
        b"The size of queue for S3 tasks\0".as_ptr() as *const c_char,
        std::ptr::null(),
        &mut s3_queue_size_guc,
        1024,
        128,
        2147483647,
        PGC_POSTMASTER,
        pg_sys::GUC_UNIT_KB as i32,
        None,
        None,
        None,
    );

    pg_sys::DefineCustomIntVariable(
        b"orioledb.s3_num_workers\0".as_ptr() as *const c_char,
        b"The number of workers to make S3 requests\0".as_ptr() as *const c_char,
        std::ptr::null(),
        &mut s3_num_workers,
        3,
        1,
        pg_sys::MaxBackends,
        PGC_POSTMASTER,
        pg_sys::GUC_UNIT_KB as i32,
        None,
        None,
        None,
    );

    pg_sys::DefineCustomIntVariable(
        b"orioledb.s3_desired_size\0".as_ptr() as *const c_char,
        b"The desired size of local OrioleDB data.\0".as_ptr() as *const c_char,
        std::ptr::null(),
        &mut s3_desired_size,
        10000,
        1,
        c_int::MAX,
        PGC_SIGHUP,
        pg_sys::GUC_UNIT_MB as i32,
        None,
        None,
        None,
    );

    pg_sys::DefineCustomStringVariable(
        b"orioledb.s3_host\0".as_ptr() as *const c_char,
        b"S3 host\0".as_ptr() as *const c_char,
        std::ptr::null(),
        &mut s3_host,
        std::ptr::null(),
        PGC_POSTMASTER,
        0,
        None,
        None,
        None,
    );

    pg_sys::DefineCustomStringVariable(
        b"orioledb.s3_region\0".as_ptr() as *const c_char,
        b"S3 region\0".as_ptr() as *const c_char,
        std::ptr::null(),
        &mut s3_region,
        std::ptr::null(),
        PGC_POSTMASTER,
        0,
        None,
        None,
        None,
    );

    pg_sys::DefineCustomStringVariable(
        b"orioledb.s3_prefix\0".as_ptr() as *const c_char,
        b"Prefix to prepend to S3 object name\0".as_ptr() as *const c_char,
        std::ptr::null(),
        &mut s3_prefix,
        std::ptr::null(),
        PGC_POSTMASTER,
        0,
        None,
        None,
        None,
    );

    pg_sys::DefineCustomBoolVariable(
        b"orioledb.s3_use_https\0".as_ptr() as *const c_char,
        b"Use https for S3 connections (or http otherwise)\0".as_ptr() as *const c_char,
        std::ptr::null(),
        &mut s3_use_https,
        true,
        PGC_POSTMASTER,
        0,
        None,
        None,
        None,
    );

    pg_sys::DefineCustomStringVariable(
        b"orioledb.s3_accesskey\0".as_ptr() as *const c_char,
        b"S3 access key\0".as_ptr() as *const c_char,
        std::ptr::null(),
        &mut s3_accesskey,
        std::ptr::null(),
        PGC_POSTMASTER,
        0,
        None,
        None,
        None,
    );

    pg_sys::DefineCustomStringVariable(
        b"orioledb.s3_secretkey\0".as_ptr() as *const c_char,
        b"S3 secret key\0".as_ptr() as *const c_char,
        std::ptr::null(),
        &mut s3_secretkey,
        std::ptr::null(),
        PGC_POSTMASTER,
        0,
        None,
        None,
        None,
    );

    pg_sys::DefineCustomStringVariable(
        b"orioledb.s3_cainfo\0".as_ptr() as *const c_char,
        b"S3 CApath or CAfile path used to validate the peer certificate. For tests only!\0".as_ptr() as *const c_char,
        std::ptr::null(),
        &mut s3_cainfo,
        std::ptr::null(),
        PGC_POSTMASTER,
        0,
        None,
        None,
        None,
    );

    pg_sys::DefineCustomBoolVariable(
        b"orioledb.enable_rewind\0".as_ptr() as *const c_char,
        b"Enable rewind for OrioleDB tables\0".as_ptr() as *const c_char,
        std::ptr::null(),
        &mut enable_rewind,
        false,
        PGC_POSTMASTER,
        0,
        Some(orioledb_enable_rewind_check_hook),
        None,
        None,
    );

    pg_sys::DefineCustomIntVariable(
        b"orioledb.rewind_max_time\0".as_ptr() as *const c_char,
        b"Sets the maximum time to hold information for OrioleDB rewind.\0".as_ptr() as *const c_char,
        std::ptr::null(),
        &mut rewind_max_time,
        500,
        1,
        86400,
        PGC_POSTMASTER,
        pg_sys::GUC_UNIT_S as i32,
        None,
        None,
        None,
    );

    pg_sys::DefineCustomIntVariable(
        b"orioledb.rewind_max_transactions\0".as_ptr() as *const c_char,
        b"Maximum number of xacts retained for orioledb rewind.\0".as_ptr() as *const c_char,
        std::ptr::null(),
        &mut rewind_max_transactions,
        84600,
        1,
        c_int::MAX,
        PGC_POSTMASTER,
        0,
        None,
        None,
        None,
    );

    pg_sys::DefineCustomBoolVariable(
        b"orioledb.strict_mode\0".as_ptr() as *const c_char,
        b"Always throw an explicit error when a feature is not supported.\0".as_ptr() as *const c_char,
        std::ptr::null(),
        &mut orioledb_strict_mode,
        false,
        PGC_POSTMASTER,
        0,
        None,
        None,
        None,
    );

    pg_sys::DefineCustomStringVariable(
        b"orioledb.replay_until_lsn\0".as_ptr() as *const c_char,
        b"Sets the LSN of the write-ahead log location up to which OrioleDB recovery will proceed.\0".as_ptr() as *const c_char,
        b"Danger: use only as a last resort\0".as_ptr() as *const c_char,
        &mut replay_until_lsn_string,
        b"\0".as_ptr() as *const c_char,
        PGC_POSTMASTER,
        0,
        Some(orioledb_replay_until_lsn_check_hook),
        Some(orioledb_replay_until_lsn_assign_hook),
        None,
    );

    if orioledb_s3_mode {
        if s3_host.is_null() || s3_region.is_null() || s3_accesskey.is_null() || s3_secretkey.is_null() {
            pgrx::ereport!(
                PgLogLevel::ERROR,
                pg_sys::errcodes::PgSqlErrorCode::ERRCODE_CONFIG_FILE_ERROR,
                "missing options for S3 connection",
                "For OrioleDB S3 mode you need to specify orioledb.s3_host, orioledb.s3_region, orioledb.s3_accesskey and orioledb.s3_secretkey."
            );
        }
    }

    let blcksz = pg_sys::BLCKSZ as usize;
    let orioledb_blcksz = 8192;
    let main_buffers_count = (main_buffers_guc as usize * blcksz) / orioledb_blcksz;
    free_tree_buffers_count = (free_tree_buffers_guc as usize * blcksz) / orioledb_blcksz;
    catalog_buffers_count = (catalog_buffers_guc as usize * blcksz) / orioledb_blcksz;
    orioledb_temp_buffers_count = (temp_buffers_guc as usize * blcksz) / orioledb_blcksz;
    main_buffers_offset = free_tree_buffers_count + catalog_buffers_count;
    orioledb_buffers_count = main_buffers_count + free_tree_buffers_count + catalog_buffers_count;
    orioledb_buffers_size = orioledb_buffers_count * orioledb_blcksz;

    let mut undo_size = (undo_buffers_guc as usize * blcksz) / 2;
    undo_size /= orioledb_blcksz;
    undo_buffers_count = undo_size as u32;
    undo_circular_buffer_size = undo_size * orioledb_blcksz;

    let mut xid_size = (xid_buffers_guc as usize * blcksz) / 2;
    xid_size /= orioledb_blcksz;
    xid_buffers_count = xid_size as u32;
    xid_circular_buffer_size = xid_size * orioledb_blcksz / std::mem::size_of::<OXidMapItem>();

    if enable_rewind {
        let mut rewind_size = (rewind_buffers_guc as usize * blcksz) / 2;
        rewind_size /= orioledb_blcksz;
        rewind_buffers_count = rewind_size as u32;
        rewind_circular_buffer_size = rewind_size * orioledb_blcksz / std::mem::size_of::<RewindItem>();
    }

    let page_descs_raw_size = orioledb_buffers_count * std::mem::size_of::<OrioleDBPageDesc>();
    page_descs_size = (page_descs_raw_size + 63) & !63;

    #[cfg(any(feature = "pg13", feature = "pg14"))]
    pg_sys::EmitWarningsOnPlaceholders(b"pg_stat_statements\0".as_ptr() as *const c_char);
    #[cfg(not(any(feature = "pg13", feature = "pg14")))]
    pg_sys::MarkGUCPrefixReserved(b"pg_stat_statements\0".as_ptr() as *const c_char);

    page_pools_size[1] = (o_ppool_estimate_space(&mut page_pools[1] as *mut OPagePool as *mut c_void, 0, free_tree_buffers_count, debug_disable_pools_limit) + 63) & !63;
    page_pools_size[2] = (o_ppool_estimate_space(&mut page_pools[2] as *mut OPagePool as *mut c_void, free_tree_buffers_count, catalog_buffers_count, debug_disable_pools_limit) + 63) & !63;
    page_pools_size[0] = (o_ppool_estimate_space(&mut page_pools[0] as *mut OPagePool as *mut c_void, main_buffers_offset, main_buffers_count, debug_disable_pools_limit) + 63) & !63;

    local_ppool_init(&mut local_ppool as *mut LocalPagePoolStruct as *mut c_void);

    if !device_filename.is_null() {
        device_fd = pg_sys::BasicOpenFile(device_filename, libc::O_RDWR as i32);
        device_length = device_length_guc as usize * blcksz;
        if device_fd < 0 {
            let filename = std::ffi::CStr::from_ptr(device_filename).to_string_lossy();
            pgrx::ereport!(
                PgLogLevel::LOG,
                pg_sys::errcodes::PgSqlErrorCode::ERRCODE_SUCCESSFUL_COMPLETION,
                format!("can't open device file {}", filename)
            );
        } else if use_mmap {
            mmap_data = libc::mmap(
                std::ptr::null_mut(),
                device_length,
                libc::PROT_READ | libc::PROT_WRITE,
                libc::MAP_FILE | libc::MAP_SHARED,
                device_fd,
                0,
            );
            if mmap_data == libc::MAP_FAILED {
                mmap_data = std::ptr::null_mut();
                let filename = std::ffi::CStr::from_ptr(device_filename).to_string_lossy();
                pgrx::ereport!(
                    PgLogLevel::LOG,
                    pg_sys::errcodes::PgSqlErrorCode::ERRCODE_SUCCESSFUL_COMPLETION,
                    format!("can't map device file {}", filename)
                );
            }
        }
        if device_fd >= 0 {
            use_device = true;
        }
        if mmap_data.is_null() {
            use_mmap = false;
        }
    } else {
        use_mmap = false;
        use_device = false;
    }

    for i in 0..bgwriter_num_workers {
        register_bgwriter(i);
    }
    if enable_rewind {
        register_rewind_worker();
    }
    if orioledb_s3_mode {
        let mut check_errmsg: *const c_char = std::ptr::null();
        let mut check_errdetail: *const c_char = std::ptr::null();
        s3_put_lock_file();
        if !s3_check_control(&mut check_errmsg, &mut check_errdetail) {
            s3_delete_lock_file();
            let errmsg = if check_errmsg.is_null() {
                "S3 control check failed".to_string()
            } else {
                std::ffi::CStr::from_ptr(check_errmsg).to_string_lossy().into_owned()
            };
            let errdetail = if check_errdetail.is_null() {
                "".to_string()
            } else {
                std::ffi::CStr::from_ptr(check_errdetail).to_string_lossy().into_owned()
            };
            pgrx::ereport!(
                PgLogLevel::ERROR,
                pg_sys::errcodes::PgSqlErrorCode::ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE,
                errmsg,
                errdetail
            );
        }
    }
    if orioledb_s3_mode {
        for i in 0..s3_num_workers {
            register_s3worker(i);
        }
    }

    register_o_detoast_func(o_detoast);
    o_tableam_descr_init();
    o_compress_init();
    o_sys_caches_init();
    
    pg_sys::RegisterCustomScanMethods(&mut o_scan_methods);

    btree_insert_context = pg_sys::AllocSetContextCreateInternal(
        pg_sys::TopMemoryContext,
        b"orioledb B-tree insert context\0".as_ptr() as *const c_char,
        pg_sys::ALLOCSET_DEFAULT_MINSIZE as usize,
        pg_sys::ALLOCSET_DEFAULT_INITSIZE as usize,
        pg_sys::ALLOCSET_DEFAULT_MAXSIZE as usize,
    );

    btree_seqscan_context = pg_sys::AllocSetContextCreateInternal(
        pg_sys::TopMemoryContext,
        b"orioledb B-tree sequential scans context\0".as_ptr() as *const c_char,
        pg_sys::ALLOCSET_DEFAULT_MINSIZE as usize,
        pg_sys::ALLOCSET_DEFAULT_INITSIZE as usize,
        pg_sys::ALLOCSET_DEFAULT_MAXSIZE as usize,
    );

    // Setup hooks
    prev_shmem_request_hook = pg_sys::shmem_request_hook;
    pg_sys::shmem_request_hook = Some(orioledb_shmem_request);
    
    prev_shmem_startup_hook = pg_sys::shmem_startup_hook;
    pg_sys::shmem_startup_hook = Some(orioledb_shmem_startup);
    
    next_CheckPoint_hook = CheckPoint_hook;
    CheckPoint_hook = Some(o_perform_checkpoint);
    
    old_set_rel_pathlist_hook = pg_sys::set_rel_pathlist_hook;
    pg_sys::set_rel_pathlist_hook = Some(orioledb_set_rel_pathlist_hook);
    
    set_plain_rel_pathlist_hook = Some(orioledb_set_plain_rel_pathlist_hook);

    prev_AcceptInvalidationMessagesHook = AcceptInvalidationMessagesHook;
    AcceptInvalidationMessagesHook = Some(orioledb_AcceptInvalidationMessagesHook);
    
    pg_sys::RegisterXactCallback(Some(undo_xact_callback), std::ptr::null_mut());
    pg_sys::RegisterSubXactCallback(Some(undo_subxact_callback), std::ptr::null_mut());
    
    get_xidless_commit_lsn_hook = Some(orioledb_get_xidless_commit_lsn);
    CacheRegisterUsercacheCallback(Some(orioledb_usercache_hook), pg_sys::Datum::from(0));
    
    after_checkpoint_cleanup_hook = Some(o_after_checkpoint_cleanup_hook);

    pg_sys::RegisterCustomRmgr(142, &mut rmgr);
    
    RedoShutdownHook = Some(o_recovery_shutdown_hook);
    snapshot_hook = Some(orioledb_snapshot_hook);
    CustomErrorCleanupHook = Some(orioledb_error_cleanup_hook);
    snapshot_register_hook = Some(undo_snapshot_register_hook);
    snapshot_deregister_hook = Some(undo_snapshot_deregister_hook);
    reset_xmin_hook = Some(orioledb_reset_xmin_hook);
    
    prev_get_relation_info_hook = pg_sys::get_relation_info_hook;
    pg_sys::get_relation_info_hook = Some(orioledb_get_relation_info_hook);

    #[cfg(not(any(feature = "pg18", feature = "pg19")))]
    {
        prev_skip_tree_height_hook = pg_sys::skip_tree_height_hook;
        pg_sys::skip_tree_height_hook = Some(orioledb_skip_tree_height_hook);
    }
    
    xact_redo_hook = Some(o_xact_redo_hook);
    pg_newlocale_from_collation_hook = Some(o_newlocale_from_collation);
    
    prev_base_init_startup_hook = base_init_startup_hook;
    base_init_startup_hook = Some(o_base_init_startup_hook);
    
    IndexAMRoutineHook = Some(orioledb_indexam_routine_hook);
    getRunningTransactionsExtension = Some(orioledb_get_running_transactions_extension);
    waitSnapshotHook = Some(orioledb_wait_snapshot);
    GetReplayXlogPtrHook = Some(recovery_get_effective_replay_ptr);
    
    prev_database_size_hook = database_size_hook;
    database_size_hook = Some(orioledb_calculate_database_size);
    
    RecoveryStopsBeforeHook = Some(orioledb_recovery_stops_before_hook);

    if enable_rewind {
        VacuumHorizonHook = Some(orioledb_vacuum_horizon_hook);
    }
    
    orioledb_setup_ddl_hooks();
    crate::utils::stopevent::stopevents_make_cxt();
}

static mut rmgr: pg_sys::RmgrData = pg_sys::RmgrData {
    rm_name: b"OrioleDB resource manager\0".as_ptr() as *const c_char,
    rm_startup: Some(o_recovery_start_hook),
    rm_cleanup: Some(o_recovery_cleanup),
    rm_redo: Some(orioledb_redo),
    rm_desc: Some(orioledb_rm_desc),
    rm_identify: Some(orioledb_rm_identify),
    rm_mask: None,
    rm_decode: unsafe {
        std::mem::transmute::<
            unsafe extern "C-unwind" fn(*mut c_void),
            Option<unsafe extern "C-unwind" fn(*mut pg_sys::LogicalDecodingContext, *mut pg_sys::XLogRecordBuffer)>,
        >(orioledb_decode as unsafe extern "C-unwind" fn(*mut c_void))
    },
};

#[pg_extern]
fn hello_orioledb() -> &'static str {
    "Hello, orioledb"
}

#[pg_extern]
fn orioledb_version() -> &'static str {
    "OrioleDB beta 16"
}

#[pg_extern]
fn orioledb_commit_hash() -> &'static str {
    "unknown"
}

#[pg_extern]
unsafe fn orioledb_parallel_debug_start() {
    pg_sys::debug_parallel_query = pg_sys::DebugParallelMode::DEBUG_PARALLEL_REGRESS as i32;
}

#[pg_extern]
unsafe fn orioledb_parallel_debug_stop() {
    pg_sys::debug_parallel_query = pg_sys::DebugParallelMode::DEBUG_PARALLEL_OFF as i32;
}

#[pg_extern]
unsafe fn orioledb_ucm_check() -> bool {
    let mut result = true;
    for i in 0..3 {
        if result {
            result = ucm_check_map(&mut page_pools[i].ucm);
        }
    }
    result
}

#[pg_extern]
unsafe fn orioledb_page_stats() -> TableIterator<'static, (
    name!(pool_type, String),
    name!(allocated_pages, i64),
    name!(free_pages, i64),
    name!(dirty_pages, i64),
    name!(total_pages, i64),
)> {
    orioledb_check_shmem();
    let mut results = Vec::new();
    for i in 0..3 {
        let total_num_pages = page_pools[i].size as i64;
        let pool_name = match i {
            0 => "main".to_string(),
            1 => "free_tree".to_string(),
            2 => "catalog".to_string(),
            _ => "unknown".to_string(),
        };
        let free_pages_fn = (*page_pools[i].base.ops).free_pages_count.unwrap();
        let num_free_pages = free_pages_fn(&mut page_pools[i].base as *mut PagePool) as i64;
        let dirty_pages_fn = (*page_pools[i].base.ops).dirty_pages_count.unwrap();
        let dirty_pages = dirty_pages_fn(&mut page_pools[i].base as *mut PagePool) as i64;
        
        results.push((
            pool_name,
            total_num_pages - num_free_pages,
            num_free_pages,
            dirty_pages,
            total_num_pages,
        ));
    }
    TableIterator::new(results)
}

#[pg_extern]
unsafe fn orioledb_print_pool_pages(
    pool_type: default!(c_int, 0)
) -> TableIterator<'static, (
    name!(blkno, i64),
    name!(relation, String),
    name!(datoid, i64),
    name!(reloid, i64),
    name!(relnode, i64),
    name!(r#type, String),
    name!(usage_count, i64),
)> {
    orioledb_check_shmem();
    let mut results = Vec::new();
    let (start_blkno, end_blkno) = match pool_type {
        1 => (0, page_pools[1].size),
        2 => (free_tree_buffers_count as u32, (free_tree_buffers_count as u32) + page_pools[2].size),
        0 => (main_buffers_offset as u32, (main_buffers_offset as u32) + page_pools[0].size),
        _ => (0, 0),
    };
    for blkno in start_blkno..end_blkno {
        let page_desc = &*page_descs.add(blkno as usize);
        let header = (o_shared_buffers.add((blkno as usize) * 8192)) as *mut pg_sys::PageHeaderData;
        
        let relation_name = if page_desc.oids.datoid.to_u32() == 1 {
            "sys tree".to_string()
        } else if page_desc.oids.datoid.to_u32() != 0 && page_desc.oids.reloid.to_u32() != 0 && page_desc.oids.relnode.to_u32() != 0 {
            let rel = pg_sys::try_relation_open(page_desc.oids.reloid, pg_sys::AccessShareLock as i32);
            if !rel.is_null() {
                let name = std::ffi::CStr::from_ptr((*(*rel).rd_rel).relname.data.as_ptr() as *const c_char).to_string_lossy().into_owned();
                pg_sys::relation_close(rel, pg_sys::AccessShareLock as i32);
                name
            } else {
                "unknown".to_string()
            }
        } else if page_desc.r#type() == 0 {
            "seq buffer".to_string()
        } else {
            "unknown".to_string()
        };
        
        let type_name = match page_desc.r#type() {
            0 => "invalid".to_string(),
            1 => "toast".to_string(),
            2 => "primary".to_string(),
            3 => "unique".to_string(),
            4 => "regular".to_string(),
            5 => "bridge".to_string(),
            6 => "exclusion".to_string(),
            _ => "unknown".to_string(),
        };
        
        let state = pg_sys::pg_atomic_read_u64(header as *mut pg_sys::pg_atomic_uint64);
        let usage_count = ((state >> 10) & 0x7) as i64;
        results.push((
            blkno as i64,
            relation_name,
            page_desc.oids.datoid.to_u32() as i64,
            page_desc.oids.reloid.to_u32() as i64,
            page_desc.oids.relnode.to_u32() as i64,
            type_name,
            usage_count,
        ));
    }
    TableIterator::new(results)
}

#[cfg(any(test, feature = "pg_test"))]
#[pg_schema]
mod tests {
    use pgrx::prelude::*;

    #[pg_test]
    fn test_hello_orioledb() {
        assert_eq!("Hello, orioledb", crate::hello_orioledb());
    }
}

#[cfg(feature = "pg_bench")]
#[pg_schema]
mod benches {
    use pgrx::prelude::*;
    use pgrx_bench::{Bencher, black_box};

    #[pg_bench]
    fn bench_hello_orioledb(b: &mut Bencher) {
        b.iter(|| {
            black_box(crate::hello_orioledb());
        });
    }
}

/// This module is required by `cargo pgrx test` invocations.
/// It must be visible at the root of your extension crate.
#[cfg(test)]
pub mod pg_test {
    pub fn setup(_options: Vec<&str>) {
        // perform one-off initialization when the pg_test framework starts
    }

    #[must_use]
    pub fn postgresql_conf_options() -> Vec<&'static str> {
        vec![]
    }
}
