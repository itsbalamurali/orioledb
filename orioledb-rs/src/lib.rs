//! Main module of the OrioleDB extension.
//!
//! This is the crate root. It wires the engine's module tree, registers the
//! PostgreSQL extension entry point (`_PG_init`), defines all OrioleDB custom
//! GUCs, sets up shared memory, installs the various hooks the engine plugs
//! into, and exposes the SQL-callable helper functions.
//!
//! The structure mirrors the original C implementation in `src/orioledb.c`,
//! but it is written idiomatically in Rust on top of `pgrx`. Because OrioleDB
//! is a PostgreSQL extension, a small amount of `extern "C"` and `unsafe`
//! FFI remains unavoidable; it is isolated behind safe wrappers here.
//!
//! Copyright (c) 2021-2026, Oriole DB Inc.
//! Copyright (c) 2025-2026, Supabase Inc.

// ---------------------------------------------------------------------------
// Module tree
// ---------------------------------------------------------------------------

pub mod btree;
pub mod catalog;
pub mod checkpoint;
pub mod indexam;
pub mod recovery;
pub mod rewind;
pub mod s3;
pub mod tableam;
pub mod transam;
pub mod tuple;
pub mod utils;
pub mod workers;

use pgrx::pg_sys;

// ---------------------------------------------------------------------------
// Engine constants (mirrored from `include/orioledb.h` and friends)
// ---------------------------------------------------------------------------

/// OrioleDB page size in bytes (distinct from PostgreSQL's `BLCKSZ`).
const ORIOLEDB_BLCKSZ: usize = 8192;

/// Minimum page-pool size, in pages.
const PAGE_POOL_MIN_SIZE: usize = 1024;

/// Minimum page-pool size, in `BLCKSZ` blocks.
const PAGE_POOL_MIN_SIZE_BLCKS: usize = PAGE_POOL_MIN_SIZE * ORIOLEDB_BLCKSZ / 8192;

/// Size of one xid-map item, in bytes (`OXidMapItem`).
const OXID_MAP_ITEM_SIZE: usize = std::mem::size_of::<u64>() * 2;

/// Size of one rewind item, in bytes (`RewindItem`).
const REWIND_ITEM_SIZE: usize = 32;

// Opaque types owned by their ported modules. They are named here only so the
// buffer-size arithmetic reads like the original C; the real definitions land
// with the `transam` and `rewind` ports.
#[repr(C)]
pub struct OXidMapItem {
    _priv: [u8; OXID_MAP_ITEM_SIZE],
}

#[repr(C)]
pub struct RewindItem {
    _priv: [u8; REWIND_ITEM_SIZE],
}

// ---------------------------------------------------------------------------
// Engine-wide shared state
// ---------------------------------------------------------------------------
//
// These variables mirror the global state of the original C implementation.
// PostgreSQL extensions are initialized once at shared-library load time, so a
// handful of `static` items are required. Where possible they are plain `const`
// or non-`mut` values; the few mutable ones are owned by `_PG_init` and the
// shared-memory lifecycle, never by concurrent backends in an unsynchronized way.

/// Build-time commit hash, embedded by the build script via `COMMIT_HASH`.
const COMMIT_HASH: &str = env!("COMMIT_HASH");

/// Human-readable OrioleDB release string.
const ORIOLEDB_VERSION: &str = "OrioleDB beta 16";

/// Number of OrioleDB page pools (free tree, catalog, main).
const PAGE_POOL_TYPES_COUNT: usize = 3;

/// Guards running `_PG_init` only once.
static mut PG_INIT_DONE: bool = false;

/// Whether the shared segment has been initialized at startup.
static mut SHARED_SEGMENT_INITIALIZED: bool = false;

/// Root pointer to the engine's shared memory segment.
static mut SHARED_SEGMENT: *mut std::os::raw::c_void = std::ptr::null_mut();

/// Size of the whole engine shared-memory region, in bytes.
static mut ORIOLEDB_BUFFERS_SIZE: usize = 0;

/// Number of OrioleDB pages across all pools.
static mut ORIOLEDB_BUFFERS_COUNT: usize = 0;

/// Number of temporary (local) pages.
static mut ORIOLEDB_TEMP_BUFFERS_COUNT: usize = 0;

/// Offset of the main pool within the global page range.
static mut MAIN_BUFFERS_OFFSET: usize = 0;

/// Number of free-tree pages.
static mut FREE_TREE_BUFFERS_COUNT: usize = 0;

/// Number of catalog pages.
static mut CATALOG_BUFFERS_COUNT: usize = 0;

/// Maximum number of backends and auxiliary processes.
static mut MAX_PROCS: i32 = 0;

/// When true, the minimal pool size limits are disabled (debug only).
static mut DEBUG_DISABLE_POOLS_LIMIT: bool = false;

/// Serializable-isolation handling mode (maps to the C `orioledb_serializable_mode`).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SerializableMode {
    /// Acquire a coarse `ExclusiveLock` per touched relation.
    TableLock = 0,
    /// Reject `SERIALIZABLE` transactions outright.
    Error = 1,
    /// Silently downgrade `SERIALIZABLE` to `REPEATABLE READ`.
    RepeatableRead = 2,
}

/// Current serializable handling mode.
static mut ORIOLEDB_SERIALIZABLE_MODE: i32 = SerializableMode::TableLock as i32;

/// Disable the batched same-leaf primary insert path (debug only).
static mut ORIOLEDB_DEBUG_DISABLE_MULTI_INSERT: bool = false;

/// Undo circular buffer size, in bytes.
static mut UNDO_CIRCULAR_BUFFER_SIZE: usize = 0;

/// Number of undo circular buffer pages.
static mut UNDO_BUFFERS_COUNT: u32 = 0;

/// Fraction of the circular buffer used for block-level undo of regular tables.
static mut REGULAR_BLOCK_UNDO_CIRCULAR_BUFFER_FRACTION: f64 = 0.0;

/// Fraction of the circular buffer used for undo of system trees.
static mut SYSTEM_UNDO_CIRCULAR_BUFFER_FRACTION: f64 = 0.0;

/// Xid circular buffer size, in bytes.
static mut XID_CIRCULAR_BUFFER_SIZE: usize = 0;

/// Number of xid circular buffer pages.
static mut XID_BUFFERS_COUNT: u32 = 0;

/// Rewind circular buffer size, in bytes.
static mut REWIND_CIRCULAR_BUFFER_SIZE: usize = 0;

/// Number of rewind circular buffer pages.
static mut REWIND_BUFFERS_COUNT: u32 = 0;

/// Whether old checkpoint files are removed after a checkpoint.
static mut REMOVE_OLD_CHECKPOINT_FILES: bool = true;

/// Whether unmodified trees are skipped during checkpointing.
static mut SKIP_UNMODIFIED_TREES: bool = true;

/// Disable the background writer (debug only).
static mut DEBUG_DISABLE_BGWRITER: bool = false;

/// Store data in an `mmap(2)`-ed file.
static mut USE_MMAP: bool = false;

/// Use a raw block device for storage.
static mut USE_DEVICE: bool = false;

/// Punch sparse-file holes for free blocks.
static mut ORIOLEDB_USE_SPARSE_FILES: bool = false;

/// Path to the device file used for `mmap`/`use_device`.
static mut DEVICE_FILENAME: *mut std::os::raw::c_char = std::ptr::null_mut();

/// Base address of the `mmap`-ed device region.
static mut MMAP_DATA: *mut std::os::raw::c_void = std::ptr::null_mut();

/// File descriptor of the open device file.
static mut DEVICE_FD: i32 = 0;

/// Requested device length, in `BLCKSZ` blocks.
static mut DEVICE_LENGTH_GUC: i32 = 0;

/// Effective device length, in bytes.
static mut DEVICE_LENGTH: usize = 0;

/// Ratio of the OrioleDB checkpoint duration to the PostgreSQL checkpoint.
static mut O_CHECKPOINT_COMPLETION_RATIO: f64 = 0.0;

/// Number of background writer workers.
static mut BGWRITER_NUM_WORKERS: i32 = 1;

/// Maximum number of concurrent IO operations.
static mut MAX_IO_CONCURRENCY: i32 = 0;

/// Default compression level for all indexes.
static mut DEFAULT_COMPRESS: i32 = -1;

/// Default compression level for primary indexes.
static mut DEFAULT_PRIMARY_COMPRESS: i32 = -1;

/// Default compression level for TOAST indexes.
static mut DEFAULT_TOAST_COMPRESS: i32 = -1;

/// Show the compression column in `orioledb_table_description`.
static mut ORIOLEDB_TABLE_DESCRIPTION_COMPRESS: bool = false;

/// Maximum bridge ctid block number (overflow testing only).
static mut MAX_BRIDGE_CTID_STRING: *mut std::os::raw::c_char = std::ptr::null_mut();

/// Parsed maximum bridge ctid block number.
static mut MAX_BRIDGE_CTID_BLKNO: u32 = 0;

/// Whether OrioleDB runs on top of S3 storage.
pub(crate) static mut ORIOLEDB_S3_MODE: bool = false;

/// Number of S3 request workers.
static mut S3_NUM_WORKERS: i32 = 3;

/// Desired local data size for S3 mode, in MB.
static mut S3_DESIRED_SIZE: i32 = 10000;

/// S3 task queue size, in KB.
static mut S3_QUEUE_SIZE_GUC: i32 = 0;

/// S3 meta-information buffer size, in KB.
static mut S3_HEADERS_BUFFERS_SIZE: i32 = 0;

/// S3 host.
static mut S3_HOST: *mut std::os::raw::c_char = std::ptr::null_mut();

/// Use HTTPS (rather than HTTP) for S3 connections.
static mut S3_USE_HTTPS: bool = true;

/// S3 region.
static mut S3_REGION: *mut std::os::raw::c_char = std::ptr::null_mut();

/// Prefix prepended to S3 object names.
static mut S3_PREFIX: *mut std::os::raw::c_char = std::ptr::null_mut();

/// S3 access key.
static mut S3_ACCESSKEY: *mut std::os::raw::c_char = std::ptr::null_mut();

/// S3 secret key.
static mut S3_SECRETKEY: *mut std::os::raw::c_char = std::ptr::null_mut();

/// S3 CA path/file used to validate the peer certificate (tests only).
static mut S3_CAINFO: *mut std::os::raw::c_char = std::ptr::null_mut();

/// Whether rewind is enabled.
static mut ENABLE_REWIND: bool = false;

/// Maximum time (seconds) to retain information for rewind.
static mut REWIND_MAX_TIME: i32 = 0;

/// Maximum number of transactions retained for rewind.
static mut REWIND_MAX_TRANSACTIONS: i32 = 0;

/// Size of shared-memory buffers for subtransaction logical XIDs, in blocks.
static mut LOGICAL_XID_BUFFERS_GUC: i32 = 64;

/// Throw an explicit error whenever an unsupported feature is used.
static mut ORIOLEDB_STRICT_MODE: bool = false;

/// LSN up to which recovery proceeds (debug / last-resort only).
static mut REPLAY_UNTIL_LSN: u64 = 0;

/// Raw string value of `orioledb.replay_until_lsn`.
static mut REPLAY_UNTIL_LSN_STRING: *mut std::os::raw::c_char = std::ptr::null_mut();

/// Minimum read-page checkpoint number (page eviction/read test only).
static mut MIN_READ_PAGE_CHECKPOINT: u32 = u32::MAX;

/// Maximum read-page checkpoint number (page eviction/read test only).
static mut MAX_READ_PAGE_CHECKPOINT: u32 = 0;

// ---------------------------------------------------------------------------
// Extension entry point
// ---------------------------------------------------------------------------

pgrx::pg_magic!(version: pg_sys::PG_VERSION_NUM);

/// Extension library load handler, equivalent to the C `_PG_init`.
///
/// Registers every OrioleDB custom GUC, computes the shared-memory layout,
/// registers background and S3 workers, wires the table/index access methods,
/// and installs all the PostgreSQL hooks the engine relies on.
#[pgrx::pg_guard]
fn _PG_init() {
    // Only run once, and only as a shared preload library.
    unsafe {
        if PG_INIT_DONE {
            return;
        }
        PG_INIT_DONE = true;
    }

    if unsafe { pg_sys::process_shared_preload_libraries_in_progress } == false {
        return;
    }

    orioledb_init();
}

/// Top-level initialization performed from `_PG_init`, mirroring the original
/// C `_PG_init` body.
fn orioledb_init() {
    unsafe {
        // Ensure the on-disk directories exist before anything touches them.
        o_verify_dir_exists_or_create(
            c"data".as_ptr() as *mut _,
            std::ptr::null_mut(),
            std::ptr::null_mut(),
        );
        o_verify_dir_exists_or_create(
            c"undo".as_ptr() as *mut _,
            std::ptr::null_mut(),
            std::ptr::null_mut(),
        );
        o_verify_dir_exists_or_create(
            c"data/1".as_ptr() as *mut _,
            std::ptr::null_mut(),
            std::ptr::null_mut(),
        );
    }

    // Compute the number of backends and the minimum pool size.
    //
    // See PostgreSQL's `InitializeMaxBackends()` and `InitProcGlobal()`.
    #[cfg(feature = "pg18")]
    let procs = pg_sys::MaxConnections
        + pg_sys::autovacuum_worker_slots
        + 1
        + pg_sys::max_worker_processes
        + pg_sys::max_wal_senders
        + pg_sys::NUM_SPECIAL_WORKER_PROCS
        + pg_sys::NUM_AUXILIARY_PROCS;
    #[cfg(not(feature = "pg18"))]
    let procs = pg_sys::MaxConnections
        + pg_sys::autovacuum_max_workers
        + 2
        + pg_sys::max_worker_processes
        + pg_sys::max_wal_senders
        + pg_sys::NUM_AUXILIARY_PROCS;

    unsafe {
        MAX_PROCS = procs as i32;
    }

    let min_pool_size = (PAGE_POOL_MIN_SIZE_BLCKS).max(unsafe { MAX_PROCS } as usize * 4);

    register_gucs(min_pool_size);

    // S3 mode requires connection options up front.
    unsafe {
        if ORIOLEDB_S3_MODE != false
            && (S3_HOST.is_null()
                || S3_REGION.is_null()
                || S3_ACCESSKEY.is_null()
                || S3_SECRETKEY.is_null())
        {
            pgrx::error!(
                "missing options for S3 connection: specify orioledb.s3_host, \
                 orioledb.s3_region, orioledb.s3_accesskey and orioledb.s3_secretkey"
            );
        }
    }

    // Translate the GUC block counts (expressed in `BLCKSZ` blocks) into
    // OrioleDB page counts.
    let main_buffers_count =
        unsafe { pg_sys::main_buffers_guc as usize * pg_sys::BLCKSZ as usize } / ORIOLEDB_BLCKSZ;
    let free_tree_buffers_count =
        unsafe { pg_sys::free_tree_buffers_guc as usize * pg_sys::BLCKSZ as usize }
            / ORIOLEDB_BLCKSZ;
    let catalog_buffers_count =
        unsafe { pg_sys::catalog_buffers_guc as usize * pg_sys::BLCKSZ as usize } / ORIOLEDB_BLCKSZ;
    let temp_buffers_count =
        unsafe { pg_sys::temp_buffers_guc as usize * pg_sys::BLCKSZ as usize } / ORIOLEDB_BLCKSZ;

    unsafe {
        MAIN_BUFFERS_OFFSET = free_tree_buffers_count + catalog_buffers_count;
        ORIOLEDB_BUFFERS_COUNT =
            main_buffers_count + free_tree_buffers_count + catalog_buffers_count;
        ORIOLEDB_BUFFERS_SIZE = ORIOLEDB_BUFFERS_COUNT * ORIOLEDB_BLCKSZ;
        ORIOLEDB_TEMP_BUFFERS_COUNT = temp_buffers_count;

        let mut undo = (pg_sys::undo_buffers_guc as usize * pg_sys::BLCKSZ as usize) / 2;
        undo /= ORIOLEDB_BLCKSZ;
        UNDO_BUFFERS_COUNT = undo as u32;
        UNDO_CIRCULAR_BUFFER_SIZE = undo * ORIOLEDB_BLCKSZ;

        let mut xid = (pg_sys::xid_buffers_guc as usize * pg_sys::BLCKSZ as usize) / 2;
        xid /= ORIOLEDB_BLCKSZ;
        XID_BUFFERS_COUNT = xid as u32;
        XID_CIRCULAR_BUFFER_SIZE = xid * ORIOLEDB_BLCKSZ / std::mem::size_of::<OXidMapItem>();

        if ENABLE_REWIND {
            let mut rewind = (pg_sys::rewind_buffers_guc as usize * pg_sys::BLCKSZ as usize) / 2;
            rewind /= ORIOLEDB_BLCKSZ;
            REWIND_BUFFERS_COUNT = rewind as u32;
            REWIND_CIRCULAR_BUFFER_SIZE =
                rewind * ORIOLEDB_BLCKSZ / std::mem::size_of::<RewindItem>();
        }
    }

    // The rest of `_PG_init` (page-pool sizing, device mapping, worker
    // registration, AM wiring, hook installation) is performed by the engine
    // modules as they are ported. Those calls are collected in
    // `engine_postguc_init()` so the wiring stays faithful to the C source.
    engine_postguc_init();
}

// ---------------------------------------------------------------------------
// Custom GUC registration
// ---------------------------------------------------------------------------
//
// Every GUC mirrors a `DefineCustom*Variable` call from the original C
// `_PG_init`. The pgrx GUC registry is the safe wrapper we build on top of.

const GUC_UNIT_BLOCKS: i32 = 1 << 0;
const GUC_UNIT_KB: i32 = 1 << 1;
const GUC_UNIT_MB: i32 = 1 << 2;
const GUC_UNIT_S: i32 = 1 << 3;

/// Register all OrioleDB GUCs. `min_pool_size` is the computed floor for the
/// buffer-size GUCs.
fn register_gucs(min_pool_size: usize) {
    let postmaster = pgrx::guc::GucContext::Postmaster;
    let userset = pgrx::guc::GucContext::User;
    let suset = pgrx::guc::GucContext::Superuser;

    unsafe {
        pgrx::guc::GucRegistry::define_bool_guc(
            "orioledb.debug_disable_pools_limit",
            "Disables pools minimal limit for debug.",
            None,
            pgrx::guc::GucSetting::<bool>::new(DEBUG_DISABLE_POOLS_LIMIT),
            postmaster,
            pgrx::guc::GucFlags::default(),
        );

        pgrx::guc::GucRegistry::define_enum_guc(
            "orioledb.serializable",
            "How OrioleDB handles SERIALIZABLE isolation.",
            Some(
                "table_lock acquires a coarse ExclusiveLock per touched relation; \
                 error rejects SERIALIZABLE transactions; repeatable_read silently \
                 downgrades them to REPEATABLE READ.",
            ),
            pgrx::guc::GucSetting::<i32>::new(ORIOLEDB_SERIALIZABLE_MODE),
            &[
                pgrx::guc::GucEnum::new("table_lock", SerializableMode::TableLock as i32),
                pgrx::guc::GucEnum::new("error", SerializableMode::Error as i32),
                pgrx::guc::GucEnum::new("repeatable_read", SerializableMode::RepeatableRead as i32),
            ],
            userset,
            pgrx::guc::GucFlags::default(),
        );

        pgrx::guc::GucRegistry::define_bool_guc(
            "orioledb.debug_disable_multi_insert",
            "Disable the batched same-leaf primary insert path.",
            Some(
                "Debug switch. When on, orioledb_multi_insert falls back to per-row \
                 o_tbl_insert instead of draining adjacent ordered keys into the same \
                 primary leaf under one lwlock.",
            ),
            pgrx::guc::GucSetting::<bool>::new(ORIOLEDB_DEBUG_DISABLE_MULTI_INSERT),
            userset,
            pgrx::guc::GucFlags::default(),
        );

        pgrx::guc::GucRegistry::define_int_guc(
            "orioledb.main_buffers",
            "Size of orioledb engine shared buffers for main data.",
            None,
            pgrx::guc::GucSetting::<i32>::new(pg_sys::main_buffers_guc),
            (8192usize.max(min_pool_size)) as i32,
            if DEBUG_DISABLE_POOLS_LIMIT {
                1
            } else {
                min_pool_size as i32
            },
            i32::MAX,
            postmaster,
            GUC_UNIT_BLOCKS as i32,
        );

        pgrx::guc::GucRegistry::define_int_guc(
            "orioledb.free_tree_buffers",
            "Size of orioledb engine shared buffers for free extents BTrees.",
            None,
            pgrx::guc::GucSetting::<i32>::new(pg_sys::free_tree_buffers_guc),
            min_pool_size as i32,
            if DEBUG_DISABLE_POOLS_LIMIT {
                1
            } else {
                min_pool_size as i32
            },
            i32::MAX,
            postmaster,
            GUC_UNIT_BLOCKS as i32,
        );

        pgrx::guc::GucRegistry::define_int_guc(
            "orioledb.catalog_buffers",
            "Size of orioledb engine shared buffers for catalog BTrees.",
            None,
            pgrx::guc::GucSetting::<i32>::new(pg_sys::catalog_buffers_guc),
            min_pool_size as i32,
            if DEBUG_DISABLE_POOLS_LIMIT {
                1
            } else {
                min_pool_size as i32
            },
            i32::MAX,
            postmaster,
            GUC_UNIT_BLOCKS as i32,
        );

        pgrx::guc::GucRegistry::define_int_guc(
            "orioledb.undo_buffers",
            "Size of orioledb engine undo log buffers.",
            Some(
                "Each undo type's circular buffer is at least max_procs * 2 * \
                 O_MAX_UNDO_RECORD_SIZE bytes, so the actual buffer at startup may \
                 be larger than what this GUC requested when max_procs is high.",
            ),
            pgrx::guc::GucSetting::<i32>::new(pg_sys::undo_buffers_guc),
            (128usize.max(16 * MAX_PROCS as usize)) as i32,
            (16 * MAX_PROCS as usize) as i32,
            i32::MAX,
            postmaster,
            GUC_UNIT_BLOCKS as i32,
        );

        pgrx::guc::GucRegistry::define_int_guc(
            "orioledb.temp_buffers",
            "Size of orioledb engine buffers for temporary tables.",
            None,
            pgrx::guc::GucSetting::<i32>::new(pg_sys::temp_buffers_guc),
            (1024usize) as i32,
            if DEBUG_DISABLE_POOLS_LIMIT { 1 } else { 1024 },
            i32::MAX,
            postmaster,
            GUC_UNIT_BLOCKS as i32,
        );

        pgrx::guc::GucRegistry::define_real_guc(
            "orioledb.regular_block_undo_circular_buffer_fraction",
            "Fraction of circular buffer for block-level undo of regular tables.",
            None,
            pgrx::guc::GucSetting::<f64>::new(REGULAR_BLOCK_UNDO_CIRCULAR_BUFFER_FRACTION),
            0.45,
            0.05,
            0.95,
            postmaster,
            0,
        );

        pgrx::guc::GucRegistry::define_real_guc(
            "orioledb.system_undo_circular_buffer_fraction",
            "Fraction of circular buffer for undo of system trees.",
            None,
            pgrx::guc::GucSetting::<f64>::new(SYSTEM_UNDO_CIRCULAR_BUFFER_FRACTION),
            0.10,
            0.05,
            0.95,
            postmaster,
            0,
        );

        pgrx::guc::GucRegistry::define_int_guc(
            "orioledb.xid_buffers",
            "Size of orioledb engine xid buffers.",
            None,
            pgrx::guc::GucSetting::<i32>::new(pg_sys::xid_buffers_guc),
            128,
            128,
            i32::MAX,
            postmaster,
            GUC_UNIT_BLOCKS as i32,
        );

        pgrx::guc::GucRegistry::define_int_guc(
            "orioledb.rewind_buffers",
            "Size of orioledb engine rewind buffers.",
            None,
            pgrx::guc::GucSetting::<i32>::new(pg_sys::rewind_buffers_guc),
            128,
            6,
            i32::MAX,
            postmaster,
            GUC_UNIT_BLOCKS as i32,
        );

        pgrx::guc::GucRegistry::define_bool_guc(
            "orioledb.enable_stopevents",
            "Enable stop events.",
            None,
            pgrx::guc::GucSetting::<bool>::new(pg_sys::enable_stopevents),
            suset,
            pgrx::guc::GucFlags::default(),
        );

        pgrx::guc::GucRegistry::define_bool_guc(
            "orioledb.trace_stopevents",
            "Trace all the stop events to the system log.",
            None,
            pgrx::guc::GucSetting::<bool>::new(pg_sys::trace_stopevents),
            suset,
            pgrx::guc::GucFlags::default(),
        );

        pgrx::guc::GucRegistry::define_bool_guc(
            "orioledb.remove_old_checkpoint_files",
            "Remove temporary *.tmp and *.map files after checkpoint.",
            None,
            pgrx::guc::GucSetting::<bool>::new(REMOVE_OLD_CHECKPOINT_FILES),
            postmaster,
            pgrx::guc::GucFlags::default(),
        );

        pgrx::guc::GucRegistry::define_bool_guc(
            "orioledb.skip_unmodified_trees",
            "Skip reading of unmodified trees during checkpointing.",
            None,
            pgrx::guc::GucSetting::<bool>::new(SKIP_UNMODIFIED_TREES),
            postmaster,
            pgrx::guc::GucFlags::default(),
        );

        pgrx::guc::GucRegistry::define_bool_guc(
            "orioledb.debug_disable_bgwriter",
            "Disables bgwriter for debug.",
            None,
            pgrx::guc::GucSetting::<bool>::new(DEBUG_DISABLE_BGWRITER),
            postmaster,
            pgrx::guc::GucFlags::default(),
        );

        pgrx::guc::GucRegistry::define_int_guc(
            "orioledb.recovery_queue_size",
            "Size of orioledb recovery queue per worker.",
            None,
            pgrx::guc::GucSetting::<i32>::new(pg_sys::recovery_queue_size_guc),
            1024,
            512,
            1024 * 1024,
            postmaster,
            GUC_UNIT_KB as i32,
        );

        pgrx::guc::GucRegistry::define_int_guc(
            "orioledb.recovery_pool_size",
            "Sets the number of recovery workers.",
            None,
            pgrx::guc::GucSetting::<i32>::new(pg_sys::recovery_pool_size_guc),
            3,
            1,
            128,
            postmaster,
            0,
        );

        pgrx::guc::GucRegistry::define_int_guc(
            "orioledb.recovery_idx_pool_size",
            "Sets the number of recovery index build workers.",
            None,
            pgrx::guc::GucSetting::<i32>::new(pg_sys::recovery_idx_pool_size_guc),
            3,
            1,
            128,
            postmaster,
            0,
        );

        pgrx::guc::GucRegistry::define_int_guc(
            "orioledb.logical_xid_buffers",
            "Size of shared memory buffers for subtransaction logical XIDs.",
            None,
            pgrx::guc::GucSetting::<i32>::new(LOGICAL_XID_BUFFERS_GUC),
            64,
            1,
            1024,
            postmaster,
            GUC_UNIT_BLOCKS as i32,
        );

        pgrx::guc::GucRegistry::define_int_guc(
            "orioledb.debug_checkpoint_timeout",
            "Sets the maximum time between automatic WAL checkpoints.",
            None,
            pgrx::guc::GucSetting::<i32>::new(pg_sys::CheckPointTimeout),
            pg_sys::CheckPointTimeout,
            1,
            86400,
            postmaster,
            GUC_UNIT_S as i32,
        );

        pgrx::guc::GucRegistry::define_real_guc(
            "orioledb.checkpoint_completion_ratio",
            "Ratio of orioledb checkpoint to postgres checkpoint.",
            None,
            pgrx::guc::GucSetting::<f64>::new(O_CHECKPOINT_COMPLETION_RATIO),
            0.5,
            0.0,
            1.0,
            postmaster,
            0,
        );

        pgrx::guc::GucRegistry::define_int_guc(
            "orioledb.bgwriter_num_workers",
            "Number of background writers.",
            None,
            pgrx::guc::GucSetting::<i32>::new(BGWRITER_NUM_WORKERS),
            1,
            1,
            pg_sys::MAX_BACKENDS,
            postmaster,
            0,
        );

        pgrx::guc::GucRegistry::define_int_guc(
            "orioledb.max_io_concurrency",
            "Number of maximum concurrent IO operations.",
            None,
            pgrx::guc::GucSetting::<i32>::new(MAX_IO_CONCURRENCY),
            0,
            0,
            i32::MAX,
            postmaster,
            0,
        );

        pgrx::guc::GucRegistry::define_bool_guc(
            "orioledb.use_mmap",
            "Store data in the mmap'ed file.",
            None,
            pgrx::guc::GucSetting::<bool>::new(USE_MMAP),
            postmaster,
            pgrx::guc::GucFlags::default(),
        );

        pgrx::guc::GucRegistry::define_string_guc(
            "orioledb.device_filename",
            "Data file for mmap.",
            None,
            pgrx::guc::GucSetting::<Option<&std::ffi::CStr>>::new(None),
            postmaster,
            pgrx::guc::GucFlags::default(),
        );

        pgrx::guc::GucRegistry::define_int_guc(
            "orioledb.device_length",
            "Size of mmap.",
            None,
            pgrx::guc::GucSetting::<i32>::new(DEVICE_LENGTH_GUC),
            0,
            0,
            i32::MAX,
            postmaster,
            GUC_UNIT_BLOCKS as i32,
        );

        let max_lvl = crate::utils::compress::o_compress_max_lvl();

        pgrx::guc::GucRegistry::define_int_guc(
            "orioledb.default_compress",
            "Default compression level.",
            None,
            pgrx::guc::GucSetting::<i32>::new(DEFAULT_COMPRESS),
            -1,
            -1,
            max_lvl,
            userset,
            0,
        );

        pgrx::guc::GucRegistry::define_int_guc(
            "orioledb.default_primary_compress",
            "Default compression level of primary index.",
            None,
            pgrx::guc::GucSetting::<i32>::new(DEFAULT_PRIMARY_COMPRESS),
            -1,
            -1,
            max_lvl,
            userset,
            0,
        );

        pgrx::guc::GucRegistry::define_int_guc(
            "orioledb.default_toast_compress",
            "Default compression level of TOAST.",
            None,
            pgrx::guc::GucSetting::<i32>::new(DEFAULT_TOAST_COMPRESS),
            -1,
            -1,
            max_lvl,
            userset,
            0,
        );

        pgrx::guc::GucRegistry::define_bool_guc(
            "orioledb.table_description_compress",
            "Display compression column in orioledb_table_description.",
            None,
            pgrx::guc::GucSetting::<bool>::new(ORIOLEDB_TABLE_DESCRIPTION_COMPRESS),
            userset,
            pgrx::guc::GucFlags::default(),
        );

        pgrx::guc::GucRegistry::define_bool_guc(
            "orioledb.use_sparse_files",
            "Punch sparse file holes for free blocks.",
            None,
            pgrx::guc::GucSetting::<bool>::new(ORIOLEDB_USE_SPARSE_FILES),
            postmaster,
            pgrx::guc::GucFlags::default(),
        );

        pgrx::guc::GucRegistry::define_string_guc(
            "orioledb.debug_max_bridge_ctid_blkno",
            "Sets maximum value for bridge ctid for its overflow testing.",
            None,
            pgrx::guc::GucSetting::<Option<&std::ffi::CStr>>::new(None),
            postmaster,
            pgrx::guc::GucFlags::default(),
        );

        pgrx::guc::GucRegistry::define_bool_guc(
            "orioledb.s3_mode",
            "The OrioleDB function mode on top of S3 storage.",
            None,
            pgrx::guc::GucSetting::<bool>::new(ORIOLEDB_S3_MODE),
            postmaster,
            pgrx::guc::GucFlags::default(),
        );

        pgrx::guc::GucRegistry::define_int_guc(
            "orioledb.s3_queue_size",
            "The size of queue for S3 tasks.",
            None,
            pgrx::guc::GucSetting::<i32>::new(S3_QUEUE_SIZE_GUC),
            1024,
            128,
            1024 * 1024,
            postmaster,
            GUC_UNIT_KB as i32,
        );

        pgrx::guc::GucRegistry::define_int_guc(
            "orioledb.s3_headers_buffers",
            "The size of buffers for S3 meta-information.",
            None,
            pgrx::guc::GucSetting::<i32>::new(S3_HEADERS_BUFFERS_SIZE),
            1024,
            128,
            1024 * 1024,
            postmaster,
            GUC_UNIT_KB as i32,
        );

        pgrx::guc::GucRegistry::define_int_guc(
            "orioledb.s3_num_workers",
            "The number of workers to make S3 requests.",
            None,
            pgrx::guc::GucSetting::<i32>::new(S3_NUM_WORKERS),
            3,
            1,
            pg_sys::MAX_BACKENDS,
            postmaster,
            GUC_UNIT_KB as i32,
        );

        pgrx::guc::GucRegistry::define_int_guc(
            "orioledb.s3_desired_size",
            "The desired size of local OrioleDB data.",
            None,
            pgrx::guc::GucSetting::<i32>::new(S3_DESIRED_SIZE),
            10000,
            1,
            i32::MAX,
            pgrx::guc::GucContext::Sighup,
            GUC_UNIT_MB as i32,
        );

        pgrx::guc::GucRegistry::define_string_guc(
            "orioledb.s3_host",
            "S3 host.",
            None,
            pgrx::guc::GucSetting::<Option<&std::ffi::CStr>>::new(None),
            postmaster,
            pgrx::guc::GucFlags::default(),
        );

        pgrx::guc::GucRegistry::define_string_guc(
            "orioledb.s3_region",
            "S3 region.",
            None,
            pgrx::guc::GucSetting::<Option<&std::ffi::CStr>>::new(None),
            postmaster,
            pgrx::guc::GucFlags::default(),
        );

        pgrx::guc::GucRegistry::define_string_guc(
            "orioledb.s3_prefix",
            "Prefix to prepend to S3 object name.",
            None,
            pgrx::guc::GucSetting::<Option<&std::ffi::CStr>>::new(None),
            postmaster,
            pgrx::guc::GucFlags::default(),
        );

        pgrx::guc::GucRegistry::define_bool_guc(
            "orioledb.s3_use_https",
            "Use https for S3 connections (or http otherwise).",
            None,
            pgrx::guc::GucSetting::<bool>::new(S3_USE_HTTPS),
            postmaster,
            pgrx::guc::GucFlags::default(),
        );

        pgrx::guc::GucRegistry::define_string_guc(
            "orioledb.s3_accesskey",
            "S3 access key.",
            None,
            pgrx::guc::GucSetting::<Option<&std::ffi::CStr>>::new(None),
            postmaster,
            pgrx::guc::GucFlags::default(),
        );

        pgrx::guc::GucRegistry::define_string_guc(
            "orioledb.s3_secretkey",
            "S3 secret key.",
            None,
            pgrx::guc::GucSetting::<Option<&std::ffi::CStr>>::new(None),
            postmaster,
            pgrx::guc::GucFlags::default(),
        );

        pgrx::guc::GucRegistry::define_string_guc(
            "orioledb.s3_cainfo",
            "S3 CApath or CAfile path used to validate the peer certificate. For tests only!",
            None,
            pgrx::guc::GucSetting::<Option<&std::ffi::CStr>>::new(None),
            postmaster,
            pgrx::guc::GucFlags::default(),
        );

        pgrx::guc::GucRegistry::define_bool_guc(
            "orioledb.enable_rewind",
            "Enable rewind for OrioleDB tables.",
            None,
            pgrx::guc::GucSetting::<bool>::new(ENABLE_REWIND),
            postmaster,
            pgrx::guc::GucFlags::default(),
        );

        pgrx::guc::GucRegistry::define_int_guc(
            "orioledb.rewind_max_time",
            "Sets the maximum time to hold information for OrioleDB rewind.",
            None,
            pgrx::guc::GucSetting::<i32>::new(REWIND_MAX_TIME),
            500,
            1,
            86400,
            postmaster,
            GUC_UNIT_S as i32,
        );

        pgrx::guc::GucRegistry::define_int_guc(
            "orioledb.rewind_max_transactions",
            "Maximum number of xacts (Orioledb + heap) retained for orioledb rewind.",
            None,
            pgrx::guc::GucSetting::<i32>::new(REWIND_MAX_TRANSACTIONS),
            84600,
            1,
            i32::MAX,
            postmaster,
            0,
        );

        pgrx::guc::GucRegistry::define_bool_guc(
            "orioledb.strict_mode",
            "Always throw an explicit error when a feature is not supported.",
            None,
            pgrx::guc::GucSetting::<bool>::new(ORIOLEDB_STRICT_MODE),
            postmaster,
            pgrx::guc::GucFlags::default(),
        );

        pgrx::guc::GucRegistry::define_string_guc(
            "orioledb.replay_until_lsn",
            "Sets the LSN of the write-ahead log location up to which OrioleDB recovery will proceed.",
            Some("Danger: use only as a last resort."),
            pgrx::guc::GucSetting::<Option<&std::ffi::CStr>>::new(None),
            postmaster,
            pgrx::guc::GucFlags::default(),
        );
    }
}

// ---------------------------------------------------------------------------
// Post-GUC initialization: workers, AMs, hooks
// ---------------------------------------------------------------------------
//
// Mirrors the tail of the C `_PG_init`: once the GUCs are registered and the
// shared-memory sizes computed, the engine registers its background workers,
// wires the table/index access methods, and installs the PostgreSQL hooks.
// Each call targets a function in its ported module.

fn engine_postguc_init() {
    unsafe {
        // Register background writers.
        for i in 0..BGWRITER_NUM_WORKERS {
            crate::workers::bgwriter::register_bgwriter(i);
        }

        if ENABLE_REWIND {
            crate::rewind::rewind::register_rewind_worker();
        }

        if ORIOLEDB_S3_MODE {
            crate::s3::control::s3_put_lock_file();
            let (ok, errmsg, errdetail) = crate::s3::control::s3_check_control();
            if !ok {
                crate::s3::control::s3_delete_lock_file();
                pgrx::error!("{}: {}", errmsg, errdetail);
            }
            for i in 0..S3_NUM_WORKERS {
                crate::s3::worker::register_s3worker(i);
            }
        }

        // Wire the table and index access methods and scan support.
        crate::tableam::func::register_o_detoast_func(crate::tuple::toast::o_detoast);
        crate::tableam::descr::o_tableam_descr_init();
        crate::utils::compress::o_compress_init();
        crate::catalog::o_sys_cache::o_sys_caches_init();
        crate::tableam::handler::register_custom_scan_methods();

        // Install PostgreSQL hooks, chaining to any previously-installed hooks.
        crate::btree::io::orioledb_shmem_request();
        crate::checkpoint::checkpoint::o_perform_checkpoint_hook();
        crate::recovery::recovery::orioledb_snapshot_hook();
        crate::recovery::recovery::orioledb_reset_xmin_hook();
        crate::recovery::recovery::recovery_get_effective_replay_ptr();
        crate::transam::undo::undo_xact_callback();
        crate::transam::undo::undo_subxact_callback();
        crate::transam::undo::undo_snapshot_register_hook();
        crate::transam::undo::undo_snapshot_deregister_hook();
        crate::recovery::recovery::o_recovery_finish_hook();
        crate::recovery::recovery::orioledb_recovery_stops_before_hook();
        crate::recovery::wal::orioledb_redo();
        crate::recovery::recovery::orioledb_decode();
        crate::recovery::recovery::o_recovery_start_hook();
        crate::catalog::o_sys_cache::o_invalidate_descrs();
        crate::catalog::o_sys_cache::o_replay_saved_inval_messages();
        crate::tableam::handler::orioledb_set_rel_pathlist_hook();
        crate::tableam::handler::orioledb_set_plain_rel_pathlist_hook();
        crate::tableam::handler::orioledb_get_relation_info_hook();
        crate::transam::oxid::orioledb_get_running_transactions_extension();
        crate::transam::oxid::orioledb_wait_snapshot();
        crate::transam::oxid::o_xact_redo_hook();
        crate::transam::oxid::o_newlocale_from_collation();
        crate::checkpoint::checkpoint::o_after_checkpoint_cleanup_hook();
        crate::checkpoint::checkpoint::orioledb_calculate_database_size();
        crate::catalog::ddl::orioledb_setup_ddl_hooks();
        crate::utils::stopevent::stopevents_make_cxt();
    }
}

// ---------------------------------------------------------------------------
// Shared-memory lifecycle
// ---------------------------------------------------------------------------

/// Estimate the total size of OrioleDB's shared-memory region by summing the
/// individual sub-allocations declared in the shmem manifest.
fn orioledb_memsize() -> usize {
    let mut size: usize = 0;
    for item in shmem_manifest() {
        size = size.saturating_add(item.size());
    }
    size
}

/// Per-submodule shared-memory need/initialize pair.
struct ShmemItem {
    name: &'static str,
    size: fn() -> usize,
    init: fn(*mut std::os::raw::c_void, found: bool),
}

/// The ordered list of shared-memory consumers. `checkpoint` must be
/// initialized before `recovery` (see `recovery_shmem_init`).
fn shmem_manifest() -> &'static [ShmemItem] {
    &[
        ShmemItem {
            name: "btree_io",
            size: crate::btree::io::btree_io_shmem_needs,
            init: crate::btree::io::btree_io_shmem_init,
        },
        ShmemItem {
            name: "page_state",
            size: crate::btree::page_state::page_state_shmem_needs,
            init: crate::btree::page_state::page_state_shmem_init,
        },
        ShmemItem {
            name: "oxid",
            size: crate::transam::oxid::oxid_shmem_needs,
            init: crate::transam::oxid::oxid_init_shmem,
        },
        ShmemItem {
            name: "sys_trees",
            size: crate::catalog::sys_trees::sys_trees_shmem_needs,
            init: crate::catalog::sys_trees::sys_trees_shmem_init,
        },
        ShmemItem {
            name: "stopevent",
            size: crate::utils::stopevent::StopEventShmemSize,
            init: crate::utils::stopevent::StopEventShmemInit,
        },
        ShmemItem {
            name: "undo",
            size: crate::transam::undo::undo_shmem_needs,
            init: crate::transam::undo::undo_shmem_init,
        },
        ShmemItem {
            name: "checkpoint",
            size: crate::checkpoint::checkpoint::checkpoint_shmem_size,
            init: crate::checkpoint::checkpoint::checkpoint_shmem_init,
        },
        ShmemItem {
            name: "recovery",
            size: crate::recovery::recovery::recovery_shmem_needs,
            init: crate::recovery::recovery::recovery_shmem_init,
        },
        ShmemItem {
            name: "proc",
            size: o_proc_shmem_needs,
            init: o_proc_shmem_init,
        },
        ShmemItem {
            name: "ppools",
            size: ppools_shmem_needs,
            init: ppools_shmem_init,
        },
        ShmemItem {
            name: "btree_scan",
            size: crate::btree::scan::btree_scan_shmem_needs,
            init: crate::btree::scan::btree_scan_init_shmem,
        },
        ShmemItem {
            name: "s3_queue",
            size: crate::s3::queue::s3_queue_shmem_needs,
            init: crate::s3::queue::s3_queue_init_shmem,
        },
        ShmemItem {
            name: "s3_workers",
            size: crate::s3::worker::s3_workers_shmem_needs,
            init: crate::s3::worker::s3_workers_init_shmem,
        },
        ShmemItem {
            name: "s3_headers",
            size: crate::s3::headers::s3_headers_shmem_needs,
            init: crate::s3::headers::s3_headers_shmem_init,
        },
        ShmemItem {
            name: "rewind",
            size: crate::rewind::rewind::rewind_shmem_needs,
            init: crate::rewind::rewind::rewind_init_shmem,
        },
    ]
}

fn o_proc_shmem_needs() -> usize {
    MAX_PROCS as usize * std::mem::size_of::<crate::workers::bgwriter::ODBProcData>()
}

fn o_proc_shmem_init(_ptr: *mut std::os::raw::c_void, _found: bool) {
    // Initializes the per-process shared data array. The real work lands with
    // the `workers` port; the layout matches the C `o_proc_shmem_init`.
}

fn ppools_shmem_needs() -> usize {
    let mut size: usize = 0;
    for s in page_pools_size() {
        size = size.saturating_add(*s);
    }
    size.saturating_add(ORIOLEDB_BUFFERS_SIZE)
        .saturating_add(page_descs_size())
}

fn ppools_shmem_init(_ptr: *mut std::os::raw::c_void, _found: bool) {
    // Initializes the page pools, shared buffer array, and page descriptors.
    // The real implementation lands with the `utils::page_pool` port.
}

/// Size of the page-descriptor array, cache-line aligned.
fn page_descs_size() -> usize {
    crate::utils::page_pool::cacheline_align(
        ORIOLEDB_BUFFERS_COUNT * std::mem::size_of::<crate::btree::io::OrioleDBPageDesc>(),
    )
}

/// Per-pool shared-memory size estimates.
fn page_pools_size() -> &'static [usize; PAGE_POOL_TYPES_COUNT] {
    &PAGE_POOLS_SIZE
}

static mut PAGE_POOLS_SIZE: [usize; PAGE_POOL_TYPES_COUNT] = [0; PAGE_POOL_TYPES_COUNT];

/// Request shared memory and lwlocks from PostgreSQL.
fn orioledb_shmem_request() {
    unsafe {
        pgrx::pg_sys::RequestAddinShmemSpace(orioledb_memsize());
        crate::btree::io::request_btree_io_lwlocks();
        pgrx::pg_sys::RequestNamedLWLockTranche(
            c"orioledb_unique_locks".as_ptr(),
            MAX_PROCS as i32 * 4,
        );
    }
}

/// Initialize OrioleDB's shared memory at database-instance start or restart.
fn orioledb_shmem_startup() {
    unsafe {
        let found = std::ptr::null_mut();
        let mut ptr = std::ptr::null_mut();

        pgrx::pg_sys::LWLockAcquire(
            pgrx::pg_sys::AddinShmemInitLock,
            pgrx::pg_sys::LWLockMode::LW_EXCLUSIVE,
        );

        SHARED_SEGMENT = pgrx::pg_sys::ShmemInitStruct(
            c"orioledb_enigne".as_ptr(),
            orioledb_memsize() as u64,
            &mut found,
        );
        ptr = SHARED_SEGMENT;

        for item in shmem_manifest() {
            (item.init)(ptr, found != std::ptr::null_mut());
            ptr = ptr.add(crate::utils::page_pool::cacheline_align((item.size)()));
        }

        crate::btree::io::init_btree_io_lwlocks();
        crate::btree::io::o_btree_init_unique_lwlocks();

        pgrx::pg_sys::before_shmem_exit(
            Some(orioledb_on_shmem_exit),
            pgrx::pg_sys::Datum::from(0u64),
        );

        pgrx::pg_sys::LWLockRelease(pgrx::pg_sys::AddinShmemInitLock);

        SHARED_SEGMENT_INITIALIZED = true;
    }
}

/// Clean up per-process shared state at backend exit.
unsafe extern "C" fn orioledb_on_shmem_exit(_code: i32, _arg: pgrx::pg_sys::Datum) {
    if ORIOLEDB_S3_MODE {
        crate::s3::control::s3_delete_lock_file();
    }
}

/// Returns the shared-memory-backed page pool of the given type.
pub fn get_ppool(
    kind: crate::utils::page_pool::OPagePoolType,
) -> &'static mut crate::utils::page_pool::PagePool {
    &mut PAGE_POOLS[kind as usize]
}

/// Returns the page pool that owns the given in-memory block number.
pub fn get_ppool_by_blkno(
    blkno: crate::btree::io::OInMemoryBlkno,
) -> &'static mut crate::utils::page_pool::PagePool {
    if crate::btree::io::o_page_is_local(blkno) {
        return &mut LOCAL_PPOOL;
    }
    if blkno >= unsafe { MAIN_BUFFERS_OFFSET } {
        &mut PAGE_POOLS[crate::utils::page_pool::OPagePoolType::Main as usize]
    } else if blkno < unsafe { FREE_TREE_BUFFERS_COUNT } {
        &mut PAGE_POOLS[crate::utils::page_pool::OPagePoolType::FreeTree as usize]
    } else {
        &mut PAGE_POOLS[crate::utils::page_pool::OPagePoolType::Catalog as usize]
    }
}

static mut PAGE_POOLS: [crate::utils::page_pool::PagePool; PAGE_POOL_TYPES_COUNT] =
    unsafe { std::mem::MaybeUninit::zeroed().assume_init() };

static mut LOCAL_PPOOL: crate::utils::page_pool::LocalPagePool =
    unsafe { std::mem::MaybeUninit::zeroed().assume_init() };

/// Initialize a single page descriptor to its empty/invalid state.
pub fn o_page_desc_init(desc: &mut crate::btree::io::OrioleDBPageDesc) {
    desc.file_extent.len = crate::btree::io::INVALID_FILE_EXTENT_LEN;
    desc.file_extent.off = crate::btree::io::INVALID_FILE_EXTENT_OFF;
    desc.oids = crate::btree::io::ORelOids::invalid();
    desc.ionum = -1;
    desc.type_ = 0;
    desc.flags = 0;
}

/// Assert that shared memory has been initialized.
pub fn orioledb_check_shmem() {
    if unsafe { !SHARED_SEGMENT_INITIALIZED } {
        pgrx::error!("orioledb must be loaded via shared_preload_libraries");
    }
}

// ---------------------------------------------------------------------------
// Directory helpers
// ---------------------------------------------------------------------------

/// Test whether a directory exists.
///
/// Returns `0` if it does not exist, `1` if it exists, and `-1` if there
/// was an error accessing it (the `errno` reflects the error).
fn o_check_dir(dir: &std::ffi::CStr) -> i32 {
    let d = unsafe { libc::opendir(dir.as_ptr() as *mut _) };
    if d.is_null() {
        return if unsafe { *libc::__errno_location() } == libc::ENOENT {
            0
        } else {
            -1
        };
    }
    if unsafe { libc::closedir(d) } != 0 {
        return -1;
    }
    1
}

/// Verify that `dirname` exists, creating it (recursively) if it does not.
///
/// `created` and `found` are set when the caller asks for that information.
/// Mirrors the C `o_verify_dir_exists_or_create`.
fn o_verify_dir_exists_or_create(
    dirname: &std::ffi::CStr,
    created: Option<&mut bool>,
    found: Option<&mut bool>,
) {
    match o_check_dir(dirname) {
        0 => {
            if unsafe { libc::pg_mkdir_p(dirname.as_ptr() as *mut _, 0o700) } == -1 {
                if unsafe { *libc::__errno_location() } == libc::EEXIST {
                    if let Some(f) = found {
                        *f = true;
                    }
                    return;
                }
                let err =
                    unsafe { std::ffi::CStr::from_ptr(libc::strerror(*libc::__errno_location())) };
                pgrx::error!(
                    "could not access directory \"{}\": {}",
                    dirname.to_str().unwrap_or("?"),
                    err.to_str().unwrap_or("?")
                );
            }
            if let Some(c) = created {
                *c = true;
            }
        }
        1 => {
            if let Some(f) = found {
                *f = true;
            }
        }
        -1 => {
            let err =
                unsafe { std::ffi::CStr::from_ptr(libc::strerror(*libc::__errno_location())) };
            pgrx::error!(
                "could not access directory \"{}\": {}",
                dirname.to_str().unwrap_or("?"),
                err.to_str().unwrap_or("?")
            );
        }
        _ => unreachable!(),
    }
}

// ---------------------------------------------------------------------------
// SQL-callable functions
// ---------------------------------------------------------------------------

#[pgrx::pg_extern]
fn orioledb_page_stats() -> pgrx::datum::TableIterator<'static, (String, i64, i64, i64, i64)> {
    orioledb_check_shmem();
    let mut rows = Vec::new();
    for pool in PAGE_POOLS.iter() {
        let total = pool.size as i64;
        let free = pool.free_pages_count() as i64;
        let dirty = pool.dirty_pages_count() as i64;
        let name = match pool.kind {
            crate::utils::page_pool::OPagePoolType::Main => "main",
            crate::utils::page_pool::OPagePoolType::FreeTree => "free_tree",
            crate::utils::page_pool::OPagePoolType::Catalog => "catalog",
        }
        .to_string();
        rows.push((name, total - free, free, dirty, total));
    }
    pgrx::datum::TableIterator::new(rows)
}

#[pgrx::pg_extern]
fn orioledb_print_pool_pages(
    ppool_arg: default!(i32, 0),
) -> pgrx::datum::TableIterator<'static, (i64, String, i64, i64, i64, String, i64)> {
    orioledb_check_shmem();
    let kind = match crate::utils::page_pool::OPagePoolType::try_from(ppool_arg) {
        Ok(k) => k,
        Err(_) => pgrx::error!("invalid page pool type: {}", ppool_arg),
    };
    let (start, end) = match kind {
        crate::utils::page_pool::OPagePoolType::FreeTree => (0, PAGE_POOLS[0].size),
        crate::utils::page_pool::OPagePoolType::Catalog => {
            let s = unsafe { FREE_TREE_BUFFERS_COUNT } as u64;
            (s, s + PAGE_POOLS[1].size)
        }
        crate::utils::page_pool::OPagePoolType::Main => {
            let s = unsafe { MAIN_BUFFERS_OFFSET } as u64;
            (s, s + PAGE_POOLS[2].size)
        }
    };
    let mut rows = Vec::new();
    for blkno in start..end {
        let desc = crate::btree::io::page_desc(blkno);
        let header = crate::btree::io::page_header(blkno);
        let relname = if crate::btree::io::is_sys_tree_oids(desc.oids) {
            "sys tree".to_string()
        } else if desc.oids.is_valid() {
            crate::catalog::o_sys_cache::relation_name(desc.oids.reloid)
                .unwrap_or_else(|| "unknown".to_string())
        } else if desc.type_ == crate::btree::io::OIndexType::Invalid {
            "seq buffer".to_string()
        } else {
            "unknown".to_string()
        };
        let type_name = match desc.type_ {
            crate::btree::io::OIndexType::Invalid => "invalid",
            crate::btree::io::OIndexType::Toast => "toast",
            crate::btree::io::OIndexType::Bridge => "bridge",
            crate::btree::io::OIndexType::Primary => "primary",
            crate::btree::io::OIndexType::Unique => "unique",
            crate::btree::io::OIndexType::Regular => "regular",
            crate::btree::io::OIndexType::Exclusion => "exclusion",
        }
        .to_string();
        let usage = crate::btree::page_state::o_page_state_get_usage_count(header.state);
        rows.push((
            blkno as i64,
            relname,
            desc.oids.datoid as i64,
            desc.oids.reloid as i64,
            desc.oids.relnode as i64,
            type_name,
            usage as i64,
        ));
    }
    pgrx::datum::TableIterator::new(rows)
}

#[pgrx::pg_extern]
fn orioledb_version() -> &'static str {
    ORIOLEDB_VERSION
}

#[pgrx::pg_extern]
fn orioledb_commit_hash() -> &'static str {
    COMMIT_HASH
}

#[pgrx::pg_extern]
fn orioledb_ucm_check() -> bool {
    PAGE_POOLS
        .iter()
        .all(|p| crate::utils::ucm::ucm_check_map(&p.ucm))
}

/// SQL entry point that switches OrioleDB into its parallel-query debug
/// regression mode.
///
/// The underlying `debug_parallel_query` global is only compiled into builds
/// configured with the parallel-debug build option; for the standard build this
/// is a documented no-op that records intent for later wiring.
#[pgrx::pg_extern]
fn orioledb_parallel_debug_start() {
    unsafe { DEBUG_PARALLEL_QUERY = DEBUG_PARALLEL_REGRESS };
}

/// SQL entry point that switches OrioleDB out of parallel-query debug mode.
#[pgrx::pg_extern]
fn orioledb_parallel_debug_stop() {
    unsafe { DEBUG_PARALLEL_QUERY = DEBUG_PARALLEL_OFF };
}

/// Parallel-query debug mode selector (only meaningful in parallel-debug builds).
static mut DEBUG_PARALLEL_QUERY: i32 = 0;

/// Parallel-query debug value used by the regression tests.
const DEBUG_PARALLEL_REGRESS: i32 = 1;

/// Parallel-query debug value used in normal operation.
const DEBUG_PARALLEL_OFF: i32 = 0;

// ---------------------------------------------------------------------------
// Hook implementations
// ---------------------------------------------------------------------------

/// Chained onto PostgreSQL's `AcceptInvalidationMessages` hook: replay any
/// inval messages OrioleDB saved while it had no listener.
fn orioledb_accept_invalidation_messages_hook() {
    crate::recovery::recovery::o_replay_saved_inval_messages();
}

/// User-cache invalidation hook: drop cached descriptors for the given oids.
fn orioledb_usercache_hook(
    arg1: pgrx::pg_sys::Oid,
    arg2: pgrx::pg_sys::Oid,
    arg3: pgrx::pg_sys::Oid,
) {
    crate::catalog::o_sys_cache::o_invalidate_descrs(arg1, arg2, arg3);
}

/// Send a shared-invalidation message for the given relation oids.
pub fn o_invalidate_oids(oids: crate::btree::io::ORelOids) {
    crate::catalog::o_sys_cache::send_inval_for_oids(oids);
}

/// Planner `get_relation_info` hook: fill in OrioleDB-specific information
/// (tree height, page count, bitmap-scan availability) for each index.
fn orioledb_get_relation_info_hook(
    root: &mut pgrx::pg_sys::PlannerInfo,
    relation_object_id: pgrx::pg_sys::Oid,
    inhparent: bool,
    rel: &mut pgrx::pg_sys::RelOptInfo,
) {
    let relation =
        unsafe { pgrx::pg_sys::table_open(relation_object_id, pgrx::pg_sys::AccessShareLock) };
    if crate::tableam::tree::is_orioledb_rel(relation) {
        if unsafe { (*relation).rd_rel.as_ref() }
            .map(|r| r.relhasindex)
            .unwrap_or(false)
        {
            let descr = crate::tableam::descr::relation_get_descr(relation);
            if let Some(descr) = descr {
                let primary = descr.primary();
                for info in rel.indexlist_iter() {
                    let index = unsafe {
                        pgrx::pg_sys::index_open(info.indexoid, pgrx::pg_sys::AccessShareLock)
                    };
                    let options = unsafe { (*index).rd_options };
                    unsafe { (*info).amcanparallel = false };
                    let has_bitmap =
                        crate::tableam::key_bitmap::o_keybitmap_pk_mode(primary, std::ptr::null())
                            != crate::tableam::key_bitmap::O_KEYBITMAP_NONE;
                    unsafe { (*info).amhasgetbitmap = has_bitmap };
                    let is_orioledb_index = unsafe { (*index).rd_rel.as_ref() }
                        .map(|r| r.relam == crate::indexam::handler::BTREE_AM_OID)
                        .unwrap_or(false)
                        && !(options.is_null()
                            || !crate::indexam::handler::o_index_options_is_orioledb(options));
                    if !is_orioledb_index {
                        unsafe { pgrx::pg_sys::index_close(index, pgrx::pg_sys::AccessShareLock) };
                        continue;
                    }
                    unsafe { pgrx::pg_sys::index_close(index, pgrx::pg_sys::AccessShareLock) };
                    let index_descr = descr.index_by_reloid(info.indexoid);
                    crate::btree::io::o_btree_load_shmem(&index_descr.desc);
                    let root_page = crate::btree::io::get_in_memory_page(
                        index_descr.desc.root_info.root_page_blkno,
                    );
                    unsafe {
                        (*info).tree_height =
                            crate::btree::page_contents::page_get_level(root_page);
                        (*info).pages = crate::btree::btree::tree_num_leaf_pages(&index_descr.desc);
                    }
                }
            }
        }
    }
    unsafe { pgrx::pg_sys::table_close(relation, pgrx::pg_sys::NoLock) };
}
