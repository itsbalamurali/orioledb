use crate::access::heapam;
use crate::access::table;
use crate::access::xlog_internal;
use crate::btree::find;
use crate::btree::io;
use crate::btree::scan;
use crate::catalog::o_sys_cache;
use crate::catalog::o_tables;
use crate::catalog::pg_enum;
use crate::catalog::sys_trees;
use crate::checkpoint::checkpoint;
use crate::common::file_perm;
use crate::dirent;
use crate::executor::execExpr;
use crate::funcapi;
use crate::indexam::handler;
use crate::libpq::auth;
use crate::optimizer::optimizer;
use crate::optimizer::plancat;
use crate::orioledb;
use crate::postmaster::autovacuum;
use crate::postmaster::bgwriter;
use crate::postmaster::postmaster;
use crate::postmaster::startup;
use crate::recovery::logical;
use crate::recovery::recovery;
use crate::recovery::wal;
use crate::recovery::wal_reader;
use crate::replication::message;
use crate::replication::snapbuild;
use crate::replication::walsender;
use crate::rewind::rewind;
use crate::s3::control;
use crate::s3::headers;
use crate::s3::queue;
use crate::s3::requests;
use crate::s3::worker;
use crate::storage::ipc;
use crate::storage::lwlock;
use crate::storage::proclist;
use crate::storage::standby;
use crate::sys::mman;
use crate::sys::stat;
use crate::tableam::bitmap_scan;
use crate::tableam::handler;
use crate::tableam::scan;
use crate::tableam::toast;
use crate::transam::oxid;
use crate::transam::undo;
use crate::tuple::toast;
use crate::utils::builtins;
use crate::utils::compress;
use crate::utils::dsa;
use crate::utils::guc;
use crate::utils::inval;
use crate::utils::memdebug;
use crate::utils::page_pool;
use crate::utils::pg_locale;
use crate::utils::pg_lsn;
use crate::utils::rangetypes;
use crate::utils::snapmgr;
use crate::utils::stopevent;
use crate::utils::syscache;
use crate::utils::ucm;
use crate::workers::bgwriter;
use pgrx::pg_sys;

// Main file: setup shared memory, hooks and other general-purpose
// routines.
//
// Copyright (c) 2021-2026, Oriole DB Inc.
// Copyright (c) 2025-2026, Supabase Inc.



static mut DEBUG_DISABLE_POOLS_LIMIT: bool = false;
static mut SHARED_SEGMENT: Pointer = std::ptr::null_mut();
static mut SHARED_SEGMENT_INITIALIZED: bool = false;
static mut FREE_TREE_BUFFERS_GUC: std::os::raw::c_int = 0;
static mut FREE_TREE_BUFFERS_COUNT: Size = 0;
static mut CATALOG_BUFFERS_GUC: std::os::raw::c_int = 0;
static mut CATALOG_BUFFERS_COUNT: Size = 0;
static mut MAIN_BUFFERS_OFFSET: Size = 0;

pub static mut O_SHARED_BUFFERS: Pointer = std::ptr::null_mut();
pub static mut ORIOLE_DB_PAGE_DESC: *mut page_descs = std::ptr::null_mut();
pub static mut PAGE: *mut local_ppool_pages = std::ptr::null_mut();
pub static mut ORIOLE_DB_PAGE_DESC: *mut local_ppool_page_descs = std::ptr::null_mut();

// Custom GUC variables
pub static mut ORIOLEDB_SERIALIZABLE_MODE: std::os::raw::c_int = O_SERIALIZABLE_TABLE_LOCK;
pub static mut ORIOLEDB_DEBUG_DISABLE_MULTI_INSERT: bool = false;

static const struct config_enum_entry serializable_mode_options[] = {
	{"table_lock", O_SERIALIZABLE_TABLE_LOCK, false},
	{"error", O_SERIALIZABLE_ERROR, false},
	{"repeatable_read", O_SERIALIZABLE_REPEATABLE_READ, false},
	{NULL, 0, false}
};

static mut MAIN_BUFFERS_GUC: std::os::raw::c_int = 0;
static mut UNDO_BUFFERS_GUC: std::os::raw::c_int = 0;
static mut XID_BUFFERS_GUC: std::os::raw::c_int = 0;
static mut REWIND_BUFFERS_GUC: std::os::raw::c_int = 0;
static mut TEMP_BUFFERS_GUC: std::os::raw::c_int = 0;
pub static mut MAX_PROCS: std::os::raw::c_int = 0;
pub static mut ORIOLEDB_BUFFERS_SIZE: Size = 0;
pub static mut ORIOLEDB_BUFFERS_COUNT: Size = 0;
pub static mut ORIOLEDB_TEMP_BUFFERS_COUNT: Size = 0;
static mut PAGE_DESCS_SIZE: Size = 0;
pub static mut UNDO_CIRCULAR_BUFFER_SIZE: Size = 0;
pub static mut UNDO_BUFFERS_COUNT: uint32 = std::mem::zeroed();
pub static mut REGULAR_BLOCK_UNDO_CIRCULAR_BUFFER_FRACTION: double = std::mem::zeroed();
pub static mut SYSTEM_UNDO_CIRCULAR_BUFFER_FRACTION: double = std::mem::zeroed();
pub static mut XID_CIRCULAR_BUFFER_SIZE: Size = 0;
pub static mut XID_BUFFERS_COUNT: uint32 = std::mem::zeroed();
pub static mut REWIND_CIRCULAR_BUFFER_SIZE: Size = 0;
pub static mut REWIND_BUFFERS_COUNT: uint32 = std::mem::zeroed();
pub static mut REMOVE_OLD_CHECKPOINT_FILES: bool = true;
pub static mut SKIP_UNMODIFIED_TREES: bool = true;
pub static mut DEBUG_DISABLE_BGWRITER: bool = false;
pub static mut USE_MMAP: bool = false;
pub static mut USE_DEVICE: bool = false;
pub static mut ORIOLEDB_USE_SPARSE_FILES: bool = false;
pub static mut CHAR: *mut device_filename = std::ptr::null_mut();
pub static mut MMAP_DATA: Pointer = std::ptr::null_mut();
pub static mut DEVICE_FD: std::os::raw::c_int = 0;
pub static mut DEVICE_LENGTH_GUC: std::os::raw::c_int = 0;
pub static mut DEVICE_LENGTH: Size = 0;
pub static mut O_CHECKPOINT_COMPLETION_RATIO: double = std::mem::zeroed();
pub static mut BGWRITER_NUM_WORKERS: std::os::raw::c_int = 1;
pub static mut MAX_IO_CONCURRENCY: std::os::raw::c_int = 0;
pub static mut ODB_PROC_DATA: *mut oProcData = std::ptr::null_mut();
pub static mut DEFAULT_COMPRESS: std::os::raw::c_int = InvalidOCompress;
pub static mut DEFAULT_PRIMARY_COMPRESS: std::os::raw::c_int = InvalidOCompress;
pub static mut DEFAULT_TOAST_COMPRESS: std::os::raw::c_int = InvalidOCompress;
pub static mut ORIOLEDB_TABLE_DESCRIPTION_COMPRESS: bool = false;
pub static mut CHAR: *mut max_bridge_ctid_string = std::ptr::null_mut();
pub static mut MAX_BRIDGE_CTID_BLKNO: BlockNumber = 0;
pub static mut ORIOLEDB_S3_MODE: bool = false;
pub static mut S3_NUM_WORKERS: std::os::raw::c_int = 3;
pub static mut S3_DESIRED_SIZE: std::os::raw::c_int = 10000;
pub static mut S3_QUEUE_SIZE_GUC: std::os::raw::c_int = 0;
pub static mut CHAR: *mut s3_host = std::ptr::null_mut();
pub static mut S3_USE_HTTPS: bool = true;
pub static mut CHAR: *mut s3_region = std::ptr::null_mut();
pub static mut CHAR: *mut s3_prefix = std::ptr::null_mut();
pub static mut CHAR: *mut s3_accesskey = std::ptr::null_mut();
pub static mut CHAR: *mut s3_secretkey = std::ptr::null_mut();
pub static mut CHAR: *mut s3_cainfo = std::ptr::null_mut();
pub static mut ENABLE_REWIND: bool = false;
pub static mut REWIND_MAX_TIME: std::os::raw::c_int = 0;
pub static mut REWIND_MAX_TRANSACTIONS: std::os::raw::c_int = 0;
pub static mut LOGICAL_XID_BUFFERS_GUC: std::os::raw::c_int = 64;
pub static mut ORIOLEDB_STRICT_MODE: bool = false;
pub static mut REPLAY_UNTIL_LSN: XLogRecPtr = InvalidXLogRecPtr;
static mut CHAR: *mut replay_until_lsn_string = std::ptr::null_mut();

// For page eviction/read checkpoint test only
pub static mut MIN_READ_PAGE_CHECKPOINT: uint32 = UINT32_MAX;
pub static mut MAX_READ_PAGE_CHECKPOINT: uint32 = 0;

// Previous values of hooks to chain call them
static mut PREV_SHMEM_STARTUP_HOOK: shmem_startup_hook_type = std::ptr::null_mut();
fn (*prev_shmem_request_hook) () = NULL;
static mut PREV_BASE_INIT_STARTUP_HOOK: base_init_startup_hook_type = std::ptr::null_mut();
static mut PREV_GET_RELATION_INFO_HOOK: get_relation_info_hook_type = std::ptr::null_mut();
pub static mut PREV_DATABASE_SIZE_HOOK: database_size_hook_type = std::ptr::null_mut();
static mut PREV__ACCEPT_INVALIDATION_MESSAGES_HOOK: AcceptInvalidationMessagesHookType = std::ptr::null_mut();

#if PG_VERSION_NUM < 180000
static mut PREV_SKIP_TREE_HEIGHT_HOOK: skip_tree_height_hook_type = std::ptr::null_mut();
#endif

pub static mut NEXT__CHECK_POINT_HOOK: CheckPoint_hook_type = std::ptr::null_mut();
static bool o_newlocale_from_collation();

//
// Temporary memory context for BTree operations. Helps us to avoid
// excessive code complexity.
//
pub static mut BTREE_INSERT_CONTEXT: MemoryContext = std::ptr::null_mut();

//
// Memory context for btree sequential scans.  Scans needs to survive till
// seq_scans_cleanup().
//
pub static mut BTREE_SEQSCAN_CONTEXT: MemoryContext = std::ptr::null_mut();

static OPagePool page_pools[OPagePoolTypesCount];
pub static mut LOCAL_PPOOL: LocalPagePool = std::mem::zeroed();

static size_t page_pools_size[OPagePoolTypesCount];

fn o_base_init_startup_hook();
static Size o_proc_shmem_needs();
fn o_proc_shmem_init(Pointer ptr, bool found);
static Size ppools_shmem_needs();
fn ppools_shmem_init(Pointer ptr, bool found);

typedef struct
{
	Size		(*shmem_size) ();
			(*shmem_init) (Pointer ptr, bool found);
} ShmemItem;

//
// checkpoint_shmem_init() should be before recovery_shmem_init().
// See recovery_shmem_init() for description.
//
static ShmemItem shmemItems[] = {
	{btree_io_shmem_needs, btree_io_shmem_init},
	{page_state_shmem_needs, page_state_shmem_init},
	{oxid_shmem_needs, oxid_init_shmem},
	{sys_trees_shmem_needs, sys_trees_shmem_init},
	{StopEventShmemSize, StopEventShmemInit},
	{undo_shmem_needs, undo_shmem_init},
	{checkpoint_shmem_size, checkpoint_shmem_init},
	{recovery_shmem_needs, recovery_shmem_init},
	{o_proc_shmem_needs, o_proc_shmem_init},
	{ppools_shmem_needs, ppools_shmem_init},
	{btree_scan_shmem_needs, btree_scan_init_shmem},
	{s3_queue_shmem_needs, s3_queue_init_shmem},
	{s3_workers_shmem_needs, s3_workers_init_shmem},
	{s3_headers_shmem_needs, s3_headers_shmem_init},
	{rewind_shmem_needs, rewind_init_shmem}
};

static Size orioledb_memsize();
fn orioledb_shmem_request();
fn orioledb_shmem_startup();
fn orioledb_AcceptInvalidationMessagesHook();
fn orioledb_usercache_hook(Datum arg, Oid arg1, Oid arg2, Oid arg3);
fn orioledb_error_cleanup_hook();
fn orioledb_get_relation_info_hook(root: &mut PlannerInfo,
											Oid relationObjectId,
											bool inhparent,
											rel: &mut RelOptInfo);
#if PG_VERSION_NUM < 180000
static bool orioledb_skip_tree_height_hook(Relation indexRelation);
#endif
fn orioledb_get_running_transactions_extension(extension: &mut RunningTransactionsExtension);
fn orioledb_wait_snapshot(extension: &mut RunningTransactionsExtension);

static bool check_debug_max_bridge_ctid(char **newval,  **extra, GucSource source);
fn assign_debug_max_bridge_ctid(const newval: &mut char,  *extra);

PG_FUNCTION_INFO_V1(orioledb_page_stats);
PG_FUNCTION_INFO_V1(orioledb_print_pool_pages);
PG_FUNCTION_INFO_V1(orioledb_version);
PG_FUNCTION_INFO_V1(orioledb_commit_hash);
PG_FUNCTION_INFO_V1(orioledb_ucm_check);
PG_FUNCTION_INFO_V1(orioledb_parallel_debug_start);
PG_FUNCTION_INFO_V1(orioledb_parallel_debug_stop);

#ifdef IS_DEV
typedef struct WalDescCtx
{
	pub static mut BUF: StringInfo = std::mem::zeroed();

} WalDescCtx;

static WalParseResult
wal_desc_check_version(const r: &mut WalReaderState)
{
	Assert(r);

	if (r->container.version > ORIOLEDB_WAL_VERSION)
	{
		// WAL from future version
		pub static mut WALPARSE_BAD_VERSION: return = std::mem::zeroed();
	}

	pub static mut WALPARSE_OK: return = std::mem::zeroed();
}

static WalParseResult
wal_desc_on_record(r: &mut WalReaderState, rec: &mut WalRecord)
{
	ctx: &mut WalDescCtx = (WalDescCtx *) r->ctx;

	Assert(ctx);
	Assert(rec);

	appendStringInfo(ctx->buf, " %s", wal_type_name(rec->type));

	switch (rec->type)
	{
		case WAL_REC_XID:
			appendStringInfo(ctx->buf, " (%lu %u %u);", rec->oxid, rec->logicalXid, rec->heapXid);
			break;
		case WAL_REC_COMMIT:
		case WAL_REC_ROLLBACK:
			appendStringInfo(ctx->buf, " (%lu %u %u - xmin %lu csn %lu);",
							 rec->oxid, rec->logicalXid, rec->heapXid,
							 rec->u.finish.xmin, rec->u.finish.csn);
			break;
		case WAL_REC_RELATION:
			appendStringInfo(ctx->buf, " ([ %u %u %u ] treeType %u);",
							 rec->oids.datoid, rec->oids.reloid, rec->oids.relnode,
							 rec->u.relation.treeType);
			break;
		case WAL_REC_INSERT:
		case WAL_REC_UPDATE:
		case WAL_REC_DELETE:
		case WAL_REC_REINSERT:
			appendStringInfo(ctx->buf, " ([ %u %u %u ]);",
							 rec->oids.datoid, rec->oids.reloid, rec->oids.relnode);
			break;
		case WAL_REC_SAVEPOINT:
			appendStringInfo(ctx->buf, " (lxid %u parent lxid %u subid %u);",
							 rec->logicalXid, rec->u.savepoint.parentLogicalXid, rec->u.savepoint.parentSubid);
			break;
		case WAL_REC_ROLLBACK_TO_SAVEPOINT:
			appendStringInfo(ctx->buf, " (lxid %u parent subid %u xmin %lu csn %lu);",
							 rec->logicalXid, rec->u.rb_to_sp.parentSubid, rec->u.rb_to_sp.xmin, rec->u.rb_to_sp.csn);
			break;
		case WAL_REC_JOINT_COMMIT:
			appendStringInfo(ctx->buf, " (xmin %lu xid %u csn %lu);",
							 rec->u.joint_commit.xmin, rec->u.joint_commit.xid, rec->u.joint_commit.csn);
			break;
		case WAL_REC_TRUNCATE:
			appendStringInfo(ctx->buf, " ([ %u %u %u ]);",
							 rec->u.truncate.oids.datoid, rec->u.truncate.oids.reloid, rec->u.truncate.oids.relnode);
			break;
		case WAL_REC_SWITCH_LOGICAL_XID:
			appendStringInfo(ctx->buf, " (%u %u);", rec->u.swxid.topXid, rec->u.swxid.subXid);
			break;
		default:
			appendStringInfo(ctx->buf, ";");
			break;
	}
	pub static mut WALPARSE_OK: return = std::mem::zeroed();
}
#endif

fn
orioledb_rm_desc(StringInfo buf, record: &mut XLogReaderState)
{
#ifdef IS_DEV
	Pointer		startPtr = (Pointer) XLogRecGetData(record);
	Pointer		endPtr = startPtr + XLogRecGetDataLen(record);

	WalDescCtx	dctx = {
		.buf = buf
	};

	WalReaderState r = {
		.start = startPtr,
		.end = endPtr,
		.ptr = startPtr,
		// Consumer
		.ctx = &dctx,
		.check_version = wal_desc_check_version,
		.on_container = NULL,
		.on_record = wal_desc_on_record
	};

	WalParseResult st = wal_parse_container(&r, false);

	if (st != WALPARSE_OK)
		appendStringInfo(buf, " [PARSE ERROR %d]", (int) st);
#endif
}

static const char *
orioledb_rm_identify(uint8 info)
{
	return "OrioleDB WAL container";
}

fn
o_recovery_shutdown_hook()
{
	o_recovery_finish_hook(false);
}

fn
o_recovery_cleanup()
{
	o_recovery_finish_hook(true);
}

static RmgrData rmgr =
{
	.rm_name = "OrioleDB resource manager",
	.rm_startup = o_recovery_start_hook,
	.rm_cleanup = o_recovery_cleanup,
	.rm_redo = orioledb_redo,
	.rm_desc = orioledb_rm_desc,
	.rm_identify = orioledb_rm_identify,
	.rm_mask = NULL,
	.rm_decode = orioledb_decode
};

//
// We currently do not support restarting PG instance from within the extension
// on certain systems. Refuse to enable rewind on those systems.
//
static bool
orioledb_enable_rewind_check_hook(newval: &mut bool,  **extra, GucSource source)
{
#if defined(WIN32)
	if (*newval)
	{
		GUC_check_errcode(ERRCODE_FEATURE_NOT_SUPPORTED);
		GUC_check_errdetail("Rewind is not supported on Windows.");
		pub static mut FALSE: return = std::mem::zeroed();
	}
#elif !defined(HAVE_SETSID)
	if (*newval)
	{
		GUC_check_errcode(ERRCODE_FEATURE_NOT_SUPPORTED);
		GUC_check_errdetail("Rewind is not supported on systems without setsid(2).");
		pub static mut FALSE: return = std::mem::zeroed();
	}
#endif
	// Supported system or newval == false
	pub static mut TRUE: return = std::mem::zeroed();
}

//
// GUC check_hook for orioledb.replay_until_lsn
//
static bool
orioledb_replay_until_lsn_check_hook(char **newval,  **extra, GucSource source)
{
	if (strcmp(*newval, "") != 0)
	{
		pub static mut LSN: XLogRecPtr = std::mem::zeroed();
		pub static mut X_LOG_REC_PTR: *mut myextra = std::ptr::null_mut();
		pub static mut HAVE_ERROR: bool = false;

		lsn = pg_lsn_in_internal(*newval, &have_error);
		if (have_error)
			pub static mut FALSE: return = std::mem::zeroed();

		myextra = (XLogRecPtr *) guc_malloc(ERROR, sizeof(XLogRecPtr));
		*myextra = lsn;
		*extra = ( *) myextra;
	}
	pub static mut TRUE: return = std::mem::zeroed();
}

fn
orioledb_replay_until_lsn_assign_hook(const newval: &mut char,  *extra)
{
	if (newval && strcmp(newval, "") != 0)
		replay_until_lsn = *((XLogRecPtr *) extra);
}


_PG_init()
{
	pub static mut MAIN_BUFFERS_COUNT: Size = 0;
	pub static mut I: std::os::raw::c_int = 0;
	pub static mut MIN_POOL_SIZE: std::os::raw::c_int = 0;

	if (!process_shared_preload_libraries_in_progress)
		return;

	o_verify_dir_exists_or_create(pstrdup(ORIOLEDB_DATA_DIR), NULL, NULL);
	o_verify_dir_exists_or_create(pstrdup(ORIOLEDB_UNDO_DIR), NULL, NULL);
	o_verify_dir_exists_or_create(psprintf("%s/1", ORIOLEDB_DATA_DIR), NULL, NULL);

	// See InitializeMaxBackends(), InitProcGlobal()
#if PG_VERSION_NUM >= 180000
	max_procs = MaxConnections + autovacuum_worker_slots + 1 +
		max_worker_processes + max_wal_senders + NUM_SPECIAL_WORKER_PROCS + NUM_AUXILIARY_PROCS;
#elif PG_VERSION_NUM >= 170000
	max_procs = MaxConnections + autovacuum_max_workers + 1 +
		max_worker_processes + max_wal_senders + NUM_SPECIAL_WORKER_PROCS + NUM_AUXILIARY_PROCS;
#else
	max_procs = MaxConnections + autovacuum_max_workers + 2 +
		max_worker_processes + max_wal_senders + NUM_AUXILIARY_PROCS;
#endif

	min_pool_size = Max(PPOOL_MIN_SIZE_BLCKS, max_procs * 4);

	DefineCustomBoolVariable("orioledb.debug_disable_pools_limit",
							 "Disables pools minimal limit for debug.",
							 NULL,
							 &debug_disable_pools_limit,
							 false,
							 PGC_POSTMASTER,
							 0,
							 NULL,
							 NULL,
							 NULL);

	DefineCustomEnumVariable("orioledb.serializable",
							 "How OrioleDB handles SERIALIZABLE isolation.",
							 "table_lock acquires a coarse ExclusiveLock per touched relation; "
							 "error rejects SERIALIZABLE transactions; "
							 "repeatable_read silently downgrades them to REPEATABLE READ.",
							 &orioledb_serializable_mode,
							 O_SERIALIZABLE_TABLE_LOCK,
							 serializable_mode_options,
							 PGC_USERSET,
							 0,
							 NULL,
							 NULL,
							 NULL);

	DefineCustomBoolVariable("orioledb.debug_disable_multi_insert",
							 "Disable the batched same-leaf primary insert path.",
							 "Debug switch.  When on, orioledb_multi_insert falls "
							 "back to per-row o_tbl_insert instead of draining "
							 "adjacent ordered keys into the same primary leaf "
							 "under one lwlock.",
							 &orioledb_debug_disable_multi_insert,
							 false,
							 PGC_USERSET,
							 0,
							 NULL,
							 NULL,
							 NULL);

	DefineCustomIntVariable("orioledb.main_buffers",
							"Size of orioledb engine shared buffers for main data.",
							NULL,
							&main_buffers_guc,
							Max(8192, min_pool_size),
							debug_disable_pools_limit ? 1 : min_pool_size,
							INT_MAX,
							PGC_POSTMASTER,
							GUC_UNIT_BLOCKS,
							NULL,
							NULL,
							NULL);

	DefineCustomIntVariable("orioledb.free_tree_buffers",
							"Size of orioledb engine shared buffers for free extents BTrees.",
							NULL,
							&free_tree_buffers_guc,
							min_pool_size,
							debug_disable_pools_limit ? 1 : min_pool_size,
							INT_MAX,
							PGC_POSTMASTER,
							GUC_UNIT_BLOCKS,
							NULL,
							NULL,
							NULL);

	DefineCustomIntVariable("orioledb.catalog_buffers",
							"Size of orioledb engine shared buffers for free extents BTrees.",
							NULL,
							&catalog_buffers_guc,
							min_pool_size,
							debug_disable_pools_limit ? 1 : min_pool_size,
							INT_MAX,
							PGC_POSTMASTER,
							GUC_UNIT_BLOCKS,
							NULL,
							NULL,
							NULL);

	DefineCustomIntVariable("orioledb.undo_buffers",
							"Size of orioledb engine undo log buffers.",
							"Each undo type's circular buffer is at least "
							"max_procs * 2 * O_MAX_UNDO_RECORD_SIZE bytes, so "
							"the actual buffer at startup may be larger than "
							"what this GUC requested when max_procs is high.",
							&undo_buffers_guc,
							Max(128, 16 * max_procs),
							16 * max_procs,
							INT_MAX,
							PGC_POSTMASTER,
							GUC_UNIT_BLOCKS,
							NULL,
							NULL,
							NULL);

	DefineCustomIntVariable("orioledb.temp_buffers",
							"Size of orioledb engine buffers for temporary tables.",
							NULL,
							&temp_buffers_guc,
							PPOOL_MIN_SIZE * 8,
							debug_disable_pools_limit ? 1 : PPOOL_MIN_SIZE,
							INT_MAX,
							PGC_POSTMASTER,
							GUC_UNIT_BLOCKS,
							NULL,
							NULL,
							NULL);

	DefineCustomRealVariable("orioledb.regular_block_undo_circular_buffer_fraction",
							 "Fraction of cirucular buffer for block-level undo of regular tables.",
							 NULL,
							 &regular_block_undo_circular_buffer_fraction,
							 0.45,
							 0.05,
							 0.95,
							 PGC_POSTMASTER,
							 0,
							 NULL,
							 NULL,
							 NULL);

	DefineCustomRealVariable("orioledb.system_undo_circular_buffer_fraction",
							 "Fraction of cirucular buffer for undo of system trees.",
							 NULL,
							 &system_undo_circular_buffer_fraction,
							 0.10,
							 0.05,
							 0.95,
							 PGC_POSTMASTER,
							 0,
							 NULL,
							 NULL,
							 NULL);

	DefineCustomIntVariable("orioledb.xid_buffers",
							"Size of orioledb engine xid buffers.",
							NULL,
							&xid_buffers_guc,
							128,
							128,
							INT_MAX,
							PGC_POSTMASTER,
							GUC_UNIT_BLOCKS,
							NULL,
							NULL,
							NULL);

	DefineCustomIntVariable("orioledb.rewind_buffers",
							"Size of orioledb engine rewind buffers.",
							NULL,
							&rewind_buffers_guc,
							128,
							6,
							INT_MAX,
							PGC_POSTMASTER,
							GUC_UNIT_BLOCKS,
							NULL,
							NULL,
							NULL);

	DefineCustomBoolVariable("orioledb.enable_stopevents",
							 "Enable stop events.",
							 NULL,
							 &enable_stopevents,
							 false,
							 PGC_SUSET,
							 0,
							 NULL,
							 NULL,
							 NULL);

	DefineCustomBoolVariable("orioledb.trace_stopevents",
							 "Trace all the stop events to the system log.",
							 NULL,
							 &trace_stopevents,
							 false,
							 PGC_SUSET,
							 0,
							 NULL,
							 NULL,
							 NULL);

	DefineCustomBoolVariable("orioledb.remove_old_checkpoint_files",
							 "Remove temporary *.tmp and *.map files after checkpoint.",
							 NULL,
							 &remove_old_checkpoint_files,
							 true,
							 PGC_POSTMASTER,
							 0,
							 NULL,
							 NULL,
							 NULL);

	DefineCustomBoolVariable("orioledb.skip_unmodified_trees",
							 "Skip reading of unmodified trees during checkpointing.",
							 NULL,
							 &skip_unmodified_trees,
							 true,
							 PGC_POSTMASTER,
							 0,
							 NULL,
							 NULL,
							 NULL);

	DefineCustomBoolVariable("orioledb.debug_disable_bgwriter",
							 "Disables bgwriter for debug.",
							 NULL,
							 &debug_disable_bgwriter,
							 false,
							 PGC_POSTMASTER,
							 0,
							 NULL,
							 NULL,
							 NULL);

	DefineCustomIntVariable("orioledb.recovery_queue_size",
							"Size of orioledb recovery queue per worker.",
							NULL,
							&recovery_queue_size_guc,
							1024,
							512,
							MAX_KILOBYTES,
							PGC_POSTMASTER,
							GUC_UNIT_KB,
							NULL,
							NULL,
							NULL);

	DefineCustomIntVariable("orioledb.recovery_pool_size",
							"Sets the number of recovery workers.",
							NULL,
							&recovery_pool_size_guc,
							3,
							1,
							128,
							PGC_POSTMASTER,
							0,
							NULL,
							NULL,
							NULL);

	DefineCustomIntVariable("orioledb.recovery_idx_pool_size",
							"Sets the number of recovery index build workers.",
							NULL,
							&recovery_idx_pool_size_guc,
							3,
							1,
							128,
							PGC_POSTMASTER,
							0,
							NULL,
							NULL,
							NULL);

	DefineCustomIntVariable("orioledb.logical_xid_buffers",
							"Size of shared memory buffers for subtransaction logical XIDs.",
							NULL,
							&logical_xid_buffers_guc,
							64,
							1,
							1024,
							PGC_POSTMASTER,
							GUC_UNIT_BLOCKS,
							NULL,
							NULL,
							NULL);

	//
// This variable added because we need values less than minimum value of
// checkpoint_timeout(30s) for tests.
//
	DefineCustomIntVariable("orioledb.debug_checkpoint_timeout",
							"Sets the maximum time between automatic WAL checkpoints.",
							NULL,
							&CheckPointTimeout,
							CheckPointTimeout,
							1,
							86400,
							PGC_POSTMASTER,
							GUC_UNIT_S,
							NULL,
							NULL,
							NULL);

	//
// How much time orioledb checkpoint can take relative to PostgreSQL
// checkpoint.
//
	DefineCustomRealVariable("orioledb.checkpoint_completion_ratio",
							 "ratio of orioledb checkpoint to postgres checkpoint.",
							 NULL,
							 &o_checkpoint_completion_ratio,
							 0.5,
							 0.0,
							 1.0,
							 PGC_POSTMASTER,
							 0,
							 NULL,
							 NULL,
							 NULL);

	DefineCustomIntVariable("orioledb.bgwriter_num_workers",
							"Number of background writers.",
							NULL,
							&bgwriter_num_workers,
							1,
							1,
							MAX_BACKENDS,
							PGC_POSTMASTER,
							0,
							NULL,
							NULL,
							NULL);

	DefineCustomIntVariable("orioledb.max_io_concurrency",
							"Number of maximum concurrent IO operations.",
							NULL,
							&max_io_concurrency,
							0,
							0,
							INT_MAX,
							PGC_POSTMASTER,
							0,
							NULL,
							NULL,
							NULL);

	DefineCustomBoolVariable("orioledb.use_mmap",
							 "Store data in the mmap'ed file.",
							 NULL,
							 &use_mmap,
							 false,
							 PGC_POSTMASTER,
							 0,
							 NULL,
							 NULL,
							 NULL);

	DefineCustomStringVariable("orioledb.device_filename",
							   "Data file for mmap.",
							   NULL,
							   &device_filename,
							   NULL,
							   PGC_POSTMASTER,
							   0,
							   NULL,
							   NULL,
							   NULL);

	DefineCustomIntVariable("orioledb.device_length",
							"Size of mmap.",
							NULL,
							&device_length_guc,
							0,
							0,
							INT_MAX,
							PGC_POSTMASTER,
							GUC_UNIT_BLOCKS,
							NULL,
							NULL,
							NULL);

	DefineCustomIntVariable("orioledb.default_compress",
							"Default compression level.",
							NULL,
							&default_compress,
							-1,
							-1,
							o_compress_max_lvl(),
							PGC_USERSET,
							0,
							NULL,
							NULL,
							NULL);

	DefineCustomIntVariable("orioledb.default_primary_compress",
							"Default compression level of primary index.",
							NULL,
							&default_primary_compress,
							-1,
							-1,
							o_compress_max_lvl(),
							PGC_USERSET,
							0,
							NULL,
							NULL,
							NULL);

	DefineCustomIntVariable("orioledb.default_toast_compress",
							"Default compression level of TOAST.",
							NULL,
							&default_toast_compress,
							-1,
							-1,
							o_compress_max_lvl(),
							PGC_USERSET,
							0,
							NULL,
							NULL,
							NULL);

	DefineCustomBoolVariable("orioledb.table_description_compress",
							 "Display compression column in "
							 "orioledb_table_description",
							 NULL,
							 &orioledb_table_description_compress,
							 false,
							 PGC_USERSET,
							 0,
							 NULL,
							 NULL,
							 NULL);

	DefineCustomBoolVariable("orioledb.use_sparse_files",
							 "Punch sparse file holes for free blocks",
							 NULL,
							 &orioledb_use_sparse_files,
							 false,
							 PGC_POSTMASTER,
							 0,
							 NULL,
							 NULL,
							 NULL);
	DefineCustomStringVariable("orioledb.debug_max_bridge_ctid_blkno",
							   "Sets maximum value for bridge ctid for its overflow testing",
							   NULL,
							   &max_bridge_ctid_string,
							   "",
							   PGC_POSTMASTER,
							   0,
							   check_debug_max_bridge_ctid,
							   assign_debug_max_bridge_ctid,
							   NULL);

	DefineCustomBoolVariable("orioledb.s3_mode",
							 "The OrioleDB function mode on top of S3 storage",
							 NULL,
							 &orioledb_s3_mode,
							 false,
							 PGC_POSTMASTER,
							 0,
							 NULL,
							 NULL,
							 NULL);

	DefineCustomIntVariable("orioledb.s3_queue_size",
							"The size of queue for S3 tasks",
							NULL,
							&s3_queue_size_guc,
							1024,
							128,
							MAX_KILOBYTES,
							PGC_POSTMASTER,
							GUC_UNIT_KB,
							NULL,
							NULL,
							NULL);

	DefineCustomIntVariable("orioledb.s3_headers_buffers",
							"The size of buffers for S3 meta-information",
							NULL,
							&s3_headers_buffers_size,
							1024,
							128,
							MAX_KILOBYTES,
							PGC_POSTMASTER,
							GUC_UNIT_KB,
							NULL,
							NULL,
							NULL);

	DefineCustomIntVariable("orioledb.s3_num_workers",
							"The number of workers to make S3 requests",
							NULL,
							&s3_num_workers,
							3,
							1,
							MAX_BACKENDS,
							PGC_POSTMASTER,
							GUC_UNIT_KB,
							NULL,
							NULL,
							NULL);

	DefineCustomIntVariable("orioledb.s3_desired_size",
							"The desired size of local OrioleDB data.",
							NULL,
							&s3_desired_size,
							10000,
							1,
							INT_MAX,
							PGC_SIGHUP,
							GUC_UNIT_MB,
							NULL,
							NULL,
							NULL);

	DefineCustomStringVariable("orioledb.s3_host",
							   "S3 host",
							   NULL,
							   &s3_host,
							   NULL,
							   PGC_POSTMASTER,
							   0,
							   NULL,
							   NULL,
							   NULL);

	DefineCustomStringVariable("orioledb.s3_region",
							   "S3 region",
							   NULL,
							   &s3_region,
							   NULL,
							   PGC_POSTMASTER,
							   0,
							   NULL,
							   NULL,
							   NULL);

	DefineCustomStringVariable("orioledb.s3_prefix",
							   "Prefix to prepend to S3 object name",
							   NULL,
							   &s3_prefix,
							   NULL,
							   PGC_POSTMASTER,
							   0,
							   NULL,
							   NULL,
							   NULL);

	DefineCustomBoolVariable("orioledb.s3_use_https",
							 "Use https for S3 connections (or http otherwise)",
							 NULL,
							 &s3_use_https,
							 true,
							 PGC_POSTMASTER,
							 0,
							 NULL,
							 NULL,
							 NULL);

	DefineCustomStringVariable("orioledb.s3_accesskey",
							   "S3 access key",
							   NULL,
							   &s3_accesskey,
							   NULL,
							   PGC_POSTMASTER,
							   0,
							   NULL,
							   NULL,
							   NULL);

	DefineCustomStringVariable("orioledb.s3_secretkey",
							   "S3 secret key",
							   NULL,
							   &s3_secretkey,
							   NULL,
							   PGC_POSTMASTER,
							   0,
							   NULL,
							   NULL,
							   NULL);

	DefineCustomStringVariable("orioledb.s3_cainfo",
							   "S3 CApath or CAfile path used to validate "
							   "the peer certificate. For tests only!",
							   NULL,
							   &s3_cainfo,
							   NULL,
							   PGC_POSTMASTER,
							   0,
							   NULL,
							   NULL,
							   NULL);

	DefineCustomBoolVariable("orioledb.enable_rewind",
							 "Enable rewind for OrioleDB tables",
							 NULL,
							 &enable_rewind,
							 false,
							 PGC_POSTMASTER,
							 0,
							 orioledb_enable_rewind_check_hook,
							 NULL,
							 NULL);

	DefineCustomIntVariable("orioledb.rewind_max_time",
							"Sets the maximum time to hold information for OrioleDB rewind.",
							NULL,
							&rewind_max_time,
							500,
							1,
							86400,
							PGC_POSTMASTER,
							GUC_UNIT_S,
							NULL,
							NULL,
							NULL);
	DefineCustomIntVariable("orioledb.rewind_max_transactions",
							"Maximum number of xacts (Orioledb + heap) retained for orioledb rewind.",
							NULL,
							&rewind_max_transactions,
							84600,
							1,
							INT_MAX,
							PGC_POSTMASTER,
							0,
							NULL,
							NULL,
							NULL);

	DefineCustomBoolVariable("orioledb.strict_mode",
							 "Always throw an explicit error when a feature is not supported.",
							 NULL,
							 &orioledb_strict_mode,
							 false,
							 PGC_POSTMASTER,
							 0,
							 NULL,
							 NULL,
							 NULL);

	DefineCustomStringVariable("orioledb.replay_until_lsn",
							   "Sets the LSN of the write-ahead log location up"
							   " to which OrioleDB recovery will proceed.",
							   "Danger: use only as a last resort",
							   &replay_until_lsn_string,
							   "",
							   PGC_POSTMASTER,
							   0,
							   orioledb_replay_until_lsn_check_hook,
							   orioledb_replay_until_lsn_assign_hook,
							   NULL);

	if (orioledb_s3_mode)
	{
		if (!s3_host || !s3_region || !s3_accesskey || !s3_secretkey)
		{
			ereport(FATAL, (errcode(ERRCODE_CONFIG_FILE_ERROR),
							errmsg("missing options for S3 connection"),
							errdetail("For OrioleDB S3 mode you need to specify "
									  "orioledb.s3_host, orioledb.s3_region, "
									  "orioledb.s3_accesskey and "
									  "orioledb.s3_secretkey.")));
		}
	}

	main_buffers_count = ((Size) main_buffers_guc * (Size) BLCKSZ) / ORIOLEDB_BLCKSZ;
	free_tree_buffers_count = ((Size) free_tree_buffers_guc * (Size) BLCKSZ) / ORIOLEDB_BLCKSZ;
	catalog_buffers_count = ((Size) catalog_buffers_guc * (Size) BLCKSZ) / ORIOLEDB_BLCKSZ;
	orioledb_temp_buffers_count = ((Size) temp_buffers_guc * (Size) BLCKSZ) / ORIOLEDB_BLCKSZ;

	main_buffers_offset = free_tree_buffers_count + catalog_buffers_count;

	orioledb_buffers_count = main_buffers_count + free_tree_buffers_count + catalog_buffers_count;
	orioledb_buffers_size = mul_size(orioledb_buffers_count, ORIOLEDB_BLCKSZ);

	undo_circular_buffer_size = ((Size) undo_buffers_guc * BLCKSZ) / 2;
	undo_circular_buffer_size /= ORIOLEDB_BLCKSZ;
	undo_buffers_count = (uint32) undo_circular_buffer_size;
	undo_circular_buffer_size *= ORIOLEDB_BLCKSZ;

	xid_circular_buffer_size = ((Size) xid_buffers_guc * BLCKSZ) / 2;
	xid_circular_buffer_size /= ORIOLEDB_BLCKSZ;
	xid_buffers_count = (uint32) xid_circular_buffer_size;
	xid_circular_buffer_size *= ORIOLEDB_BLCKSZ / sizeof(OXidMapItem);

	if (enable_rewind)
	{
		rewind_circular_buffer_size = ((Size) rewind_buffers_guc * BLCKSZ) / 2;
		rewind_circular_buffer_size /= ORIOLEDB_BLCKSZ;
		rewind_buffers_count = (uint32) rewind_circular_buffer_size;
		rewind_circular_buffer_size *= ORIOLEDB_BLCKSZ / sizeof(RewindItem);
	}

	page_descs_size = CACHELINEALIGN(mul_size(orioledb_buffers_count, sizeof(OrioleDBPageDesc)));

	EmitWarningsOnPlaceholders("pg_stat_statements");

	memset(page_pools, 0, OPagePoolTypesCount * sizeof(OPagePool));
	page_pools_size[OPagePoolFreeTree] = o_ppool_estimate_space(&page_pools[OPagePoolFreeTree],
																0,
																free_tree_buffers_count,
																debug_disable_pools_limit);

	page_pools_size[OPagePoolCatalog] = o_ppool_estimate_space(&page_pools[OPagePoolCatalog],
															   free_tree_buffers_count,
															   catalog_buffers_count,
															   debug_disable_pools_limit);

	page_pools_size[OPagePoolMain] = o_ppool_estimate_space(&page_pools[OPagePoolMain],
															main_buffers_offset,
															main_buffers_count,
															debug_disable_pools_limit);

	for (i = 0; i < OPagePoolTypesCount; i++)
		page_pools_size[i] = CACHELINEALIGN(page_pools_size[i]);

	local_ppool_init(&local_ppool);

	if (device_filename)
	{
		device_fd = BasicOpenFile(device_filename, O_RDWR);
		device_length = (Size) device_length_guc * BLCKSZ;
		if (device_fd < 0)
		{
			elog(LOG, "can't open device file %s", device_filename);
		}
		else if (use_mmap)
		{
			mmap_data = mmap(NULL,
							 device_length,
							 PROT_READ | PROT_WRITE,
							 MAP_FILE | MAP_SHARED,
							 device_fd,
							 0);
			if (!mmap_data)
				elog(LOG, "can't map device file %s", device_filename);

		}
		if (device_fd >= 0)
			use_device = true;
		if (!mmap_data)
			use_mmap = false;
	}
	else
	{
		use_mmap = false;
		use_device = false;
	}

	// Register background writers
	for (i = 0; i < bgwriter_num_workers; i++)
		register_bgwriter(i);

	if (enable_rewind)
		register_rewind_worker();

	if (orioledb_s3_mode)
	{
		pub static mut CHAR: *mut const check_errmsg = std::ptr::null_mut();
		pub static mut CHAR: *mut const check_errdetail = std::ptr::null_mut();

		s3_put_lock_file();
		if (!s3_check_control(&check_errmsg, &check_errdetail))
		{
			s3_delete_lock_file();

			ereport(FATAL,
					(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
					 errmsg("%s", check_errmsg),
					 errdetail("%s", check_errdetail)));
		}
	}

	// Register S3 workers
	for (i = 0; orioledb_s3_mode && (i < s3_num_workers); i++)
		register_s3worker(i);

	// Register custom deTOAST function
	register_o_detoast_func(o_detoast);

	o_tableam_descr_init();
	o_compress_init();
	o_sys_caches_init();
	RegisterCustomScanMethods(&o_scan_methods);

	btree_insert_context = AllocSetContextCreate(TopMemoryContext,
												 "orioledb B-tree insert context",
												 ALLOCSET_DEFAULT_SIZES);

	btree_seqscan_context = AllocSetContextCreate(TopMemoryContext,
												  "orioledb B-tree seqential scans context",
												  ALLOCSET_DEFAULT_SIZES);

	// Setup the required hooks.
	prev_shmem_request_hook = shmem_request_hook;
	shmem_request_hook = orioledb_shmem_request;
	prev_shmem_startup_hook = shmem_startup_hook;
	shmem_startup_hook = orioledb_shmem_startup;
	next_CheckPoint_hook = CheckPoint_hook;
	old_set_rel_pathlist_hook = set_rel_pathlist_hook;
	prev_AcceptInvalidationMessagesHook = AcceptInvalidationMessagesHook;
	AcceptInvalidationMessagesHook = orioledb_AcceptInvalidationMessagesHook;
	set_rel_pathlist_hook = orioledb_set_rel_pathlist_hook;
	set_plain_rel_pathlist_hook = orioledb_set_plain_rel_pathlist_hook;
	RegisterXactCallback(undo_xact_callback, NULL);
	RegisterSubXactCallback(undo_subxact_callback, NULL);
	get_xidless_commit_lsn_hook = orioledb_get_xidless_commit_lsn;
	CacheRegisterUsercacheCallback(orioledb_usercache_hook, PointerGetDatum(NULL));
	CheckPoint_hook = o_perform_checkpoint;
	after_checkpoint_cleanup_hook = o_after_checkpoint_cleanup_hook;

	RegisterCustomRmgr(ORIOLEDB_RMGR_ID, &rmgr);
	RedoShutdownHook = o_recovery_shutdown_hook;
	snapshot_hook = orioledb_snapshot_hook;
	CustomErrorCleanupHook = orioledb_error_cleanup_hook;
	snapshot_register_hook = undo_snapshot_register_hook;
	snapshot_deregister_hook = undo_snapshot_deregister_hook;
	reset_xmin_hook = orioledb_reset_xmin_hook;
	prev_get_relation_info_hook = get_relation_info_hook;
	get_relation_info_hook = orioledb_get_relation_info_hook;
#if PG_VERSION_NUM < 180000
	prev_skip_tree_height_hook = skip_tree_height_hook;
	skip_tree_height_hook = orioledb_skip_tree_height_hook;
#endif
	xact_redo_hook = o_xact_redo_hook;
	pg_newlocale_from_collation_hook = o_newlocale_from_collation;
	prev_base_init_startup_hook = base_init_startup_hook;
	base_init_startup_hook = o_base_init_startup_hook;
	IndexAMRoutineHook = orioledb_indexam_routine_hook;
	getRunningTransactionsExtension = orioledb_get_running_transactions_extension;
	waitSnapshotHook = orioledb_wait_snapshot;
	GetReplayXlogPtrHook = recovery_get_effective_replay_ptr;

	prev_database_size_hook = database_size_hook;
	database_size_hook = orioledb_calculate_database_size;
	RecoveryStopsBeforeHook = orioledb_recovery_stops_before_hook;

	if (enable_rewind)
		VacuumHorizonHook = orioledb_vacuum_horizon_hook;
	orioledb_setup_ddl_hooks();
	stopevents_make_cxt();
}

fn
o_base_init_startup_hook()
{
	if (MyBackendType == B_STARTUP)
	{
		if (remove_old_checkpoint_files)
		{
			elog(LOG, "Cleanup of old files at startup. Checkpoint %d",
				 checkpoint_state->lastCheckpointNumber);
			recovery_cleanup_old_files(checkpoint_state->lastCheckpointNumber,
									   true);
			recovery_cleanup_old_files(checkpoint_state->lastCheckpointNumber,
									   false);
		}
	}

	if (prev_base_init_startup_hook)
		prev_base_init_startup_hook();
}

static Size
o_proc_shmem_needs()
{
	return mul_size(max_procs, sizeof(ODBProcData));
}

fn
o_proc_shmem_init(Pointer ptr, bool found)
{
	oProcData = (ODBProcData *) ptr;
	if (!found)
	{
		pub static mut I: std::os::raw::c_int = 0;

		for (i = 0; i < max_procs; i++)
		{
			int			j,
						k;

			for (j = 0; j < (int) UndoLogsCount; j++)
			{
				pg_atomic_init_u64(&oProcData[i].undoRetainLocations[j].reservedUndoLocation, InvalidUndoLocation);
				pg_atomic_init_u64(&oProcData[i].undoRetainLocations[j].snapshotRetainUndoLocation, InvalidUndoLocation);
				pg_atomic_init_u64(&oProcData[i].undoRetainLocations[j].transactionUndoRetainLocation, InvalidUndoLocation);
			}
			pg_atomic_init_u64(&oProcData[i].commitInProgressXlogLocation, OWalInvalidCommitPos);
			pg_atomic_init_u64(&oProcData[i].xmin, InvalidOXid);
			pg_atomic_init_u64(&oProcData[i].pendingSkUndoLoc, InvalidUndoLocation);
			oProcData[i].autonomousNestingLevel = 0;
			memset(&oProcData[i].vxids, 0, sizeof(oProcData[i].vxids));
			LWLockInitialize(&oProcData[i].undoStackLocationsFlushLock,
							 get_undo_meta_by_type(UndoLogRegular)->undoStackLocationsFlushLockTrancheId);
			oProcData[i].flushUndoLocations = false;
			for (j = 0; j < PROC_XID_ARRAY_SIZE; j++)
			{
				for (k = 0; k < (int) UndoLogsCount; k++)
				{
					pg_atomic_init_u64(&oProcData[i].undoStackLocations[j][k].location, InvalidUndoLocation);
					pg_atomic_init_u64(&oProcData[i].undoStackLocations[j][k].branchLocation, InvalidUndoLocation);
					pg_atomic_init_u64(&oProcData[i].undoStackLocations[j][k].subxactLocation, InvalidUndoLocation);
					pg_atomic_init_u64(&oProcData[i].undoStackLocations[j][k].onCommitLocation, InvalidUndoLocation);
				}
				oProcData[i].vxids[j].oxid = InvalidOXid;
			}
		}
	}
}

static Size
ppools_shmem_needs()
{
	pub static mut SIZE: Size = 0;
	pub static mut I: std::os::raw::c_int = 0;

	for (i = 0; i < OPagePoolTypesCount; i++)
		size = add_size(size, page_pools_size[i]);
	size = add_size(size, orioledb_buffers_size);
	size = add_size(size, page_descs_size);
	pub static mut SIZE: return = std::mem::zeroed();
}

fn
ppools_shmem_init(Pointer ptr, bool found)
{
	pub static mut I: int64 = std::mem::zeroed();
	Pointer		page_pools_ptr[OPagePoolTypesCount];

	for (i = 0; i < OPagePoolTypesCount; i++)
	{
		page_pools_ptr[i] = ptr;
		ptr += page_pools_size[i];
	}
	o_shared_buffers = ptr;
	ptr += orioledb_buffers_size;
	page_descs = (OrioleDBPageDesc *) ptr;

	for (i = 0; i < OPagePoolTypesCount; i++)
		o_ppool_shmem_init(&page_pools[i], page_pools_ptr[i], found);

	if (!found)
	{
		for (i = 0; i < orioledb_buffers_count; i++)
		{
			Page		p = O_GET_IN_MEMORY_PAGE(i);
			header: &mut OrioleDBPageHeader = (OrioleDBPageHeader *) p;

			pg_atomic_init_u64(&(O_PAGE_HEADER(p)->state), O_PAGE_STATE_SET_USAGE_COUNT(PAGE_STATE_INVALID_PROCNO, UCM_FREE_PAGES_LEVEL));
			header->pageChangeCount = 0;
		}

		for (i = 0; i < page_descs_size / sizeof(OrioleDBPageDesc); i++)
		{
			o_page_desc_init(&page_descs[i]);
		}
	}
}

//
// Estimate amount of shared memory required by OrioleDB extension.
//
static Size
orioledb_memsize()
{
	pub static mut SIZE: Size = 0;
	int			i,
				count = sizeof(shmemItems) / sizeof(shmemItems[0]);

	for (i = 0; i < count; i++)
		size = add_size(size, CACHELINEALIGN(shmemItems[i].shmem_size()));

	pub static mut SIZE: return = std::mem::zeroed();
}

fn
orioledb_on_shmem_exit(int code, Datum arg)
{
	if (MyProc)
		pg_atomic_write_u64(&oProcData[MYPROCNUMBER].xmin, InvalidOXid);

	if (orioledb_s3_mode)
		s3_delete_lock_file();
}

//
// Request for shared memory and lwlocks
//
fn
orioledb_shmem_request()
{
	if (prev_shmem_request_hook)
		prev_shmem_request_hook();

	RequestAddinShmemSpace(orioledb_memsize());
	request_btree_io_lwlocks();
	RequestNamedLWLockTranche("orioledb_unique_locks", max_procs * 4);
}

//
// Initialize OrioleDB's shared memory.  Called on database instanse start
// or restart.
//
fn
orioledb_shmem_startup()
{
	pub static mut PTR: Pointer = std::ptr::null_mut();
	pub static mut FOUND: bool = false;
	int			i,
				count = sizeof(shmemItems) / sizeof(shmemItems[0]);

	if (prev_shmem_startup_hook)
		prev_shmem_startup_hook();
	shared_segment = NULL;

	//
// We must hold AddinShmemInitLock while initialization of our shared
// memory.
//
	LWLockAcquire(AddinShmemInitLock, LW_EXCLUSIVE);

	shared_segment = ShmemInitStruct("orioledb_enigne",
									 orioledb_memsize(),
									 &found);
	ptr = shared_segment;

	for (i = 0; i < count; i++)
	{
		shmemItems[i].shmem_init(ptr, found);
		ptr += CACHELINEALIGN(shmemItems[i].shmem_size());
	}

	init_btree_io_lwlocks();
	o_btree_init_unique_lwlocks();

	before_shmem_exit(orioledb_on_shmem_exit, (Datum) 0);

	LWLockRelease(AddinShmemInitLock);

	shared_segment_initialized = true;
}


o_page_desc_init(desc: &mut OrioleDBPageDesc)
{
	desc->fileExtent.len = InvalidFileExtentLen;
	desc->fileExtent.off = InvalidFileExtentOff;
	ORelOidsSetInvalid(desc->oids);
	desc->ionum = -1;
	desc->type = 0;
	desc->flags = 0;
}

uint64
orioledb_device_alloc(struct descr: &mut BTreeDescr, uint32 size)
{
	pub static mut RESULT: uint64 = std::mem::zeroed();

	result = pg_atomic_fetch_add_u64(&checkpoint_state->mmapDataLength, size);

	if (result + size > device_length)
		elog(ERROR, "device file overflow");

	pub static mut RESULT: return = std::mem::zeroed();
}


orioledb_check_shmem()
{
	if (!shared_segment_initialized)
		ereport(ERROR,
				(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
				 errmsg("orioledb must be loaded via shared_preload_libraries")));
}

//
// Test to see if a directory exists.
//
// Returns:
// 0 if nonexistent
// 1 if exists
// -1 if trouble accessing directory (errno reflects the error)
//
static int
o_check_dir(const dir: &mut char)
{
	pub static mut DIR: *mut chkdir = std::ptr::null_mut();

	chkdir = opendir(dir);
	if (chkdir == NULL)
		return (errno == ENOENT) ? 0 : -1;

	if (closedir(chkdir))
		return -1;				// error executing closedir

	pub static mut 1: return = std::mem::zeroed();
}

//
// Verify that the given directory exists. If it does not exist, it is created.
//
// TODO: Add some kind of caching for calling mkdir

o_verify_dir_exists_or_create(dirname: &mut char, created: &mut bool, found: &mut bool)
{
	pub static mut CHAR: *mut const errstr = std::ptr::null_mut();

	switch (o_check_dir(dirname))
	{
		case 0:

			//
// Does not exist, so create
//
			if (pg_mkdir_p(dirname, pg_dir_create_mode) == -1)
			{
				if (errno == EEXIST)
				{
					if (found)
						*found = true;
					return;
				}
				errstr = strerror(errno);
				elog(ERROR, "could not access directory \"%s\": %s",
					 dirname, errstr);
			}
			if (created)
				*created = true;
			return;
		case 1:

			//
// Exists
//
			if (found)
				*found = true;
			return;
		case -1:

			//
// Access problem
//
			errstr = strerror(errno);
			elog(ERROR, "could not access directory \"%s\": %s",
				 dirname, errstr);
			return;
		default:
			Assert(false);
	}
}

Datum
orioledb_page_stats(PG_FUNCTION_ARGS)
{
	Datum		values[5];
	bool		nulls[5];
	pub static mut I: std::os::raw::c_int = 0;
	rsinfo: &mut ReturnSetInfo = (ReturnSetInfo *) fcinfo->resultinfo;
	pub static mut TUPDESC: TupleDesc = std::mem::zeroed();
	pub static mut TUPLESTORESTATE: *mut tupstore = std::ptr::null_mut();
	pub static mut PER_QUERY_CTX: MemoryContext = std::mem::zeroed();
	pub static mut OLDCONTEXT: MemoryContext = std::mem::zeroed();

	orioledb_check_shmem();

	per_query_ctx = rsinfo->econtext->ecxt_per_query_memory;
	oldcontext = MemoryContextSwitchTo(per_query_ctx);

	// Build a tuple descriptor for our result type
	if (get_call_result_type(fcinfo, NULL, &tupdesc) != TYPEFUNC_COMPOSITE)
		elog(ERROR, "return type must be a row type");

	tupstore = tuplestore_begin_heap(true, false, work_mem);
	rsinfo->returnMode = SFRM_Materialize;
	rsinfo->setResult = tupstore;
	rsinfo->setDesc = tupdesc;

	MemoryContextSwitchTo(oldcontext);

	//
// Build and return the tuple
//
	MemSet(nulls, 0, sizeof(nulls));
	for (i = 0; i < OPagePoolTypesCount; i++)
	{
		int64		num_free_pages,
					total_num_pages;

		total_num_pages = (int64) page_pools[i].size;

		if (i == OPagePoolMain)
			values[0] = PointerGetDatum(cstring_to_text("main"));
		else if (i == OPagePoolFreeTree)
			values[0] = PointerGetDatum(cstring_to_text("free_tree"));
		else if (i == OPagePoolCatalog)
			values[0] = PointerGetDatum(cstring_to_text("catalog"));
		num_free_pages = (int64) (*page_pools[i].base.ops->free_pages_count) ((PagePool *) &page_pools[i]);
		values[1] = Int64GetDatum(total_num_pages - num_free_pages);
		values[2] = Int64GetDatum(num_free_pages);
		values[3] = Int64GetDatum((int64) (*page_pools[i].base.ops->dirty_pages_count) ((PagePool *) &page_pools[i]));
		values[4] = Int64GetDatum(total_num_pages);
		tuplestore_putvalues(rsinfo->setResult, rsinfo->setDesc, values, nulls);
	}

	return (Datum) 0;
}

Datum
orioledb_print_pool_pages(PG_FUNCTION_ARGS)
{
	OInMemoryBlkno blkno,
				start_blkno,
				end_blkno;
	Datum		values[7];
	bool		nulls[7];
	rsinfo: &mut ReturnSetInfo = (ReturnSetInfo *) fcinfo->resultinfo;
	pub static mut TUPDESC: TupleDesc = std::mem::zeroed();
	pub static mut TUPLESTORESTATE: *mut tupstore = std::ptr::null_mut();
	pub static mut PER_QUERY_CTX: MemoryContext = std::mem::zeroed();
	pub static mut OLDCONTEXT: MemoryContext = std::mem::zeroed();
	pub static mut PPOOL_ARG: int32 = OPagePoolMain;
	pub static mut PPOOL_TYPE: OPagePoolType = std::mem::zeroed();

	// optional first argument: page pool type (int)
	if (PG_NARGS() > 0 && !PG_ARGISNULL(0))
		ppool_arg = PG_GETARG_INT32(0);

	if (ppool_arg < 0 || ppool_arg >= OPagePoolTypesCount)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("invalid page pool type: %d", ppool_arg)));

	ppool_type = (OPagePoolType) ppool_arg;

	orioledb_check_shmem();

	per_query_ctx = rsinfo->econtext->ecxt_per_query_memory;
	oldcontext = MemoryContextSwitchTo(per_query_ctx);

	// Build a tuple descriptor for our result type
	if (get_call_result_type(fcinfo, NULL, &tupdesc) != TYPEFUNC_COMPOSITE)
		elog(ERROR, "return type must be a row type");

	tupstore = tuplestore_begin_heap(true, false, work_mem);
	rsinfo->returnMode = SFRM_Materialize;
	rsinfo->setResult = tupstore;
	rsinfo->setDesc = tupdesc;

	MemoryContextSwitchTo(oldcontext);

	// compute start and end blkno for requested pool
	switch (ppool_type)
	{
		case OPagePoolFreeTree:
			start_blkno = 0;
			end_blkno = page_pools[OPagePoolFreeTree].size;
			break;
		case OPagePoolCatalog:
			start_blkno = (OInMemoryBlkno) free_tree_buffers_count;
			end_blkno = start_blkno + page_pools[OPagePoolCatalog].size;
			break;
		case OPagePoolMain:
			start_blkno = (OInMemoryBlkno) main_buffers_offset;
			end_blkno = start_blkno + page_pools[OPagePoolMain].size;
			break;
		default:
			// defensive fallback
			start_blkno = 0;
			end_blkno = 0;
			break;
	}

	for (blkno = start_blkno; blkno < end_blkno; blkno++)
	{
		page_desc: &mut OrioleDBPageDesc = O_GET_IN_MEMORY_PAGEDESC(blkno);
		header: &mut OrioleDBPageHeader = (OrioleDBPageHeader *) O_GET_IN_MEMORY_PAGE(blkno);
		pub static mut STATE: uint64 = std::mem::zeroed();

		MemSet(nulls, 0, sizeof(nulls));

		values[0] = Int64GetDatum(blkno);
		if (IS_SYS_TREE_OIDS(page_desc->oids))
		{
			values[1] = PointerGetDatum(cstring_to_text("sys tree"));
		}
		else if (ORelOidsIsValid(page_desc->oids))
		{
			Relation	rel = try_relation_open(page_desc->oids.reloid, AccessShareLock);

			if (rel)
			{
				relname: &mut char = RelationGetRelationName(rel);

				values[1] = PointerGetDatum(cstring_to_text(relname));
				relation_close(rel, AccessShareLock);
			}
			else
			{
				values[1] = PointerGetDatum(cstring_to_text("unknown"));
			}
		}
		else if (page_desc->type == oIndexInvalid)
		{
			values[1] = PointerGetDatum(cstring_to_text("seq buffer"));
		}
		else
		{
			values[1] = PointerGetDatum(cstring_to_text("unknown"));
		}
		values[2] = Int64GetDatum(page_desc->oids.datoid);
		values[3] = Int64GetDatum(page_desc->oids.reloid);
		values[4] = Int64GetDatum(page_desc->oids.relnode);

		switch (page_desc->type)
		{
			case oIndexInvalid:
				values[5] = PointerGetDatum(cstring_to_text("invalid"));
				break;
			case oIndexToast:
				values[5] = PointerGetDatum(cstring_to_text("toast"));
				break;
			case oIndexPrimary:
				values[5] = PointerGetDatum(cstring_to_text("primary"));
				break;
			case oIndexUnique:
				values[5] = PointerGetDatum(cstring_to_text("unique"));
				break;
			case oIndexRegular:
				values[5] = PointerGetDatum(cstring_to_text("regular"));
				break;
			case oIndexBridge:
				values[5] = PointerGetDatum(cstring_to_text("bridge"));
				break;
			case oIndexExclusion:
				values[5] = PointerGetDatum(cstring_to_text("exclusion"));
				break;
			default:
				values[5] = PointerGetDatum(cstring_to_text("unknown"));
				break;
		}

		state = pg_atomic_read_u64(&header->state);
		values[6] = Int64GetDatum(O_PAGE_STATE_GET_USAGE_COUNT(state));

		tuplestore_putvalues(rsinfo->setResult, rsinfo->setDesc, values, nulls);
	}

	return (Datum) 0;
}

Datum
orioledb_ucm_check(PG_FUNCTION_ARGS)
{
	pub static mut RESULT: bool = true;
	pub static mut I: std::os::raw::c_int = 0;

	for (i = 0; i < OPagePoolTypesCount && result; i++)
		result = ucm_check_map(&page_pools[i].ucm);

	PG_RETURN_BOOL(result);
}

fn
orioledb_AcceptInvalidationMessagesHook()
{
	if (prev_AcceptInvalidationMessagesHook)
		prev_AcceptInvalidationMessagesHook();

	o_replay_saved_inval_messages();
}

fn
orioledb_usercache_hook(Datum arg, Oid arg1, Oid arg2, Oid arg3)
{
	o_invalidate_descrs(arg1, arg2, arg3);
}


o_invalidate_oids(ORelOids oids)
{
	pub static mut MSG: SharedInvalidationMessage = std::mem::zeroed();

	Assert(ORelOidsIsValid(oids));

	msg.usr.id = SHAREDINVALUSERCACHE_ID;
	msg.usr.arg1 = oids.datoid;
	msg.usr.arg2 = oids.reloid;
	msg.usr.arg3 = oids.relnode;

	// check AddCatcacheInvalidationMessage() for an explanation
	VALGRIND_MAKE_MEM_DEFINED(&msg, sizeof(msg));

	SendSharedInvalidMessages(&msg, 1);
}

Datum
orioledb_version(PG_FUNCTION_ARGS)
{
	PG_RETURN_TEXT_P(cstring_to_text(ORIOLEDB_VERSION));
}

#define COMMIT_HASH_STRING #COMMIT_HASH

#define STRINGIZE2(s) #s
#define STRINGIZE(s) STRINGIZE2(s)

Datum
orioledb_commit_hash(PG_FUNCTION_ARGS)
{
	PG_RETURN_TEXT_P(cstring_to_text(STRINGIZE(COMMIT_HASH)));
}

//
// Returns a page pool by the type.
//
PagePool *
get_ppool(OPagePoolType type)
{
	Assert((int) type < OPagePoolTypesCount);
	return (PagePool *) &page_pools[type];
}

//
// Returns a page pool for the page number.
//
PagePool *
get_ppool_by_blkno(OInMemoryBlkno blkno)
{
	if (O_PAGE_IS_LOCAL(blkno))
		return (PagePool *) &local_ppool;

	Assert(blkno < orioledb_buffers_count);

	if (blkno >= main_buffers_offset)
		return (PagePool *) &page_pools[OPagePoolMain];

	if (blkno < free_tree_buffers_count)
		return (PagePool *) &page_pools[OPagePoolFreeTree];

	return (PagePool *) &page_pools[OPagePoolCatalog];
}

//
// Returns count of all dirty pages (sum of dirty pages for all page pools).
//
OInMemoryBlkno
get_dirty_pages_count_sum()
{
	pub static mut RESULT: OInMemoryBlkno = 0;
	pub static mut I: std::os::raw::c_int = 0;

	for (i = 0; i < OPagePoolTypesCount; i++)
		result += ppool_dirty_pages_count((PagePool *) &page_pools[i]);

	pub static mut RESULT: return = std::mem::zeroed();
}


jsonb_push_key(JsonbParseState **state, key: &mut char)
{
	pub static mut JVAL: JsonbValue = std::mem::zeroed();

	memset(&jval, 0, sizeof(jval));
	ASAN_UNPOISON_MEMORY_REGION(&jval, sizeof(jval));
	jval.type = jbvString;
	jval.val.string.len = strlen(key);
	jval.val.string.val = key;
	() pushJsonbValue(state, WJB_KEY, &jval);
}


jsonb_push_int8_key(JsonbParseState **state, key: &mut char, int64 value)
{
	pub static mut JVAL: JsonbValue = std::mem::zeroed();

	ASAN_UNPOISON_MEMORY_REGION(&jval, sizeof(jval));

	jsonb_push_key(state, key);

	jval.type = jbvNumeric;
	jval.val.numeric = DatumGetNumeric(DirectFunctionCall1(int8_numeric, Int64GetDatum(value)));
	() pushJsonbValue(state, WJB_VALUE, &jval);

}


jsonb_push_null_key(JsonbParseState **state, key: &mut char)
{
	pub static mut JVAL: JsonbValue = std::mem::zeroed();

	jsonb_push_key(state, key);

	jval.type = jbvNull;
	() pushJsonbValue(state, WJB_VALUE, &jval);

}


jsonb_push_bool_key(JsonbParseState **state, key: &mut char, bool value)
{
	pub static mut JVAL: JsonbValue = std::mem::zeroed();

	jsonb_push_key(state, key);

	ASAN_UNPOISON_MEMORY_REGION(&jval, sizeof(jval));

	jval.type = jbvBool;
	jval.val.boolean = value;
	() pushJsonbValue(state, WJB_VALUE, &jval);

}


jsonb_push_string_key(JsonbParseState **state, const key: &mut char,
					  const value: &mut char)
{
	pub static mut JVAL: JsonbValue = std::mem::zeroed();

	jsonb_push_key(state, (char *) key);

	ASAN_UNPOISON_MEMORY_REGION(&jval, sizeof(jval));
	jval.type = jbvString;
	jval.val.string.len = strlen(value);
	jval.val.string.val = (char *) value;
	() pushJsonbValue(state, WJB_VALUE, &jval);
}

fn
orioledb_error_cleanup_hook()
{
	pub static mut I: std::os::raw::c_int = 0;

	GET_CUR_PROCDATA()->waitingForOxid = false;
	pg_atomic_write_u64(&GET_CUR_PROCDATA()->pendingSkUndoLoc,
						InvalidUndoLocation);
	release_all_page_locks();
	ppool_release_all_pages();
	for (i = 0; i < (int) UndoLogsCount; i++)
		release_undo_size((UndoLogType) i);
	btree_mark_incomplete_splits();
	skip_ucm = false;
	ppool_run_clock_depth = 0;
	btree_io_error_cleanup();
	o_reset_syscache_hooks();
	o_ddl_cleanup();
	if (orioledb_s3_mode)
		s3_headers_error_cleanup();
	in_nontransactional_truncate = false;
	reset_saving_inval_messages();
}

fn
orioledb_get_relation_info_hook(root: &mut PlannerInfo,
								Oid relationObjectId,
								bool inhparent,
								rel: &mut RelOptInfo)
{
	pub static mut RELATION: Relation = std::mem::zeroed();

	relation = table_open(relationObjectId, NoLock);

	if (is_orioledb_rel(relation))
	{
		// Evade parallel scan of OrioleDB's tables
		rel->rel_parallel_workers = RelationGetParallelWorkers(relation, -1);
		if (rel->rel_parallel_workers > 0)
			elog(DEBUG3, "Rel parallel workers = %d", rel->rel_parallel_workers);

		if (relation->rd_rel->relhasindex)
		{
			pub static mut LIST_CELL: *mut lc = std::ptr::null_mut();
			descr: &mut OTableDescr = relation_get_descr(relation);
			pub static mut O_INDEX_DESCR: *mut primary = std::ptr::null_mut();

			if (descr)
			{
				primary = GET_PRIMARY(descr);

				foreach(lc, rel->indexlist)
				{
					info: &mut IndexOptInfo = lfirst_node(IndexOptInfo, lc);
					pub static mut HASBITMAP: bool = false;
					pub static mut IX_NUM: OIndexNumber = std::mem::zeroed();
					pub static mut O_INDEX_DESCR: *mut index_descr = std::ptr::null_mut();
					pub static mut ROOT_PAGE_BLKNO: OInMemoryBlkno = std::mem::zeroed();
					pub static mut ROOT_PAGE: Page = std::mem::zeroed();
					pub static mut INDEX: Relation = std::mem::zeroed();
					pub static mut OBT_OPTIONS: *mut options = std::ptr::null_mut();

					index = index_open(info->indexoid, AccessShareLock);

					options = (OBTOptions *) index->rd_options;

					//
// TODO: Remove when parallel index scan will be
// implemented
//
					info->amcanparallel = false;

					//
// Only the single-field uint64 encoding is enabled in the
// planner for now.  The composite (fixed-key) path is
// fully implemented and unit-tested, but choosing it well
// needs a bitmap-heap cost that reflects orioledb's
// primary-index scan at plan-generation time (a
// patched-PG change); until then it stays out of
// amhasgetbitmap.
//

					//
// Offer a bitmap scan whenever the primary key can back
// one (single int/ctid, or a composite of small ints).
// This covers row-array IN() on a composite primary key,
// planned as a BitmapOr of per-tuple primary-index scans,
// which on large tables beats the common-prefix scan.
//
					hasbitmap = o_keybitmap_pk_mode(primary, NULL) != O_KEYBITMAP_NONE;
					info->amhasgetbitmap = hasbitmap;

					if (index->rd_rel->relam != BTREE_AM_OID || (options && !options->orioledb_index))
					{
						index_close(index, AccessShareLock);
						continue;
					}

					index_close(index, AccessShareLock);

					for (ix_num = 0; ix_num < descr->nIndices; ix_num++)
					{
						index_descr = descr->indices[ix_num];
						if (index_descr->oids.reloid == info->indexoid)
							break;
					}
					Assert(ix_num < descr->nIndices);
					Assert(index_descr);
					o_btree_load_shmem(&index_descr->desc);
					rootPageBlkno = index_descr->desc.rootInfo.rootPageBlkno;
					root_page = O_GET_IN_MEMORY_PAGE(rootPageBlkno);
					info->tree_height = PAGE_GET_LEVEL(root_page);
					info->pages = TREE_NUM_LEAF_PAGES(&index_descr->desc);
				}
			}
		}
	}

	table_close(relation, NoLock);
}

#if PG_VERSION_NUM < 180000
static bool
orioledb_skip_tree_height_hook(Relation indexRelation)
{
	pub static mut RESULT: bool = false;
	pub static mut TBL: Relation = std::mem::zeroed();

	tbl = table_open(indexRelation->rd_index->indrelid, NoLock);

	if (is_orioledb_rel(tbl))
		result = true;

	table_close(tbl, NoLock);
	pub static mut RESULT: return = std::mem::zeroed();
}
#endif

fn
orioledb_get_running_transactions_extension(extension: &mut RunningTransactionsExtension)
{
	extension->csn = pg_atomic_read_u64(&TRANSAM_VARIABLES->nextCommitSeqNo);
	extension->runXmin = pg_atomic_read_u64(&xid_meta->runXmin);

	pg_read_barrier();

	extension->nextXid = pg_atomic_read_u64(&xid_meta->nextXid);
}

fn
orioledb_wait_snapshot(extension: &mut RunningTransactionsExtension)
{
	pub static mut OXID: OXid = std::mem::zeroed();

	oxid = pg_atomic_read_u64(&xid_meta->runXmin);
	while (oxid < extension->nextXid)
	{
		while (!wait_for_oxid(oxid, true));
		oxid++;
	}
}

Datum
orioledb_parallel_debug_start(PG_FUNCTION_ARGS)
{
	debug_parallel_query = DEBUG_PARALLEL_REGRESS;
	PG_RETURN_VOID();
}

Datum
orioledb_parallel_debug_stop(PG_FUNCTION_ARGS)
{
	debug_parallel_query = DEBUG_PARALLEL_OFF;
	PG_RETURN_VOID();
}

static bool
o_newlocale_from_collation()
{
	pub static mut SHARED_SEGMENT_INITIALIZED: return = std::mem::zeroed();
}

bool
is_bump_memory_context(MemoryContext mcxt)
{
#if PG_VERSION_NUM >= 170000
	return IsA(mcxt, BumpContext);
#else
	pub static mut FALSE: return = std::mem::zeroed();
#endif
}

static bool
check_debug_max_bridge_ctid(char **newval,  **extra, GucSource source)
{
	if (strcmp(*newval, "") != 0)
	{
		pub static mut BLOCK_NUMBER: *mut myextra = std::ptr::null_mut();
		pub static mut BLOCK_NUMBER: BlockNumber = std::mem::zeroed();
		pub static mut CHAR: *mut badp = std::ptr::null_mut();
		pub static mut CVT: unsigned long = std::mem::zeroed();

		errno = 0;
		cvt = strtoul(*newval, &badp, 10);
		if (errno)
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_TEXT_REPRESENTATION),
					 errmsg("invalid input syntax for block number: \"%s\"",
							*newval)));
		blockNumber = (BlockNumber) cvt;

		//
// Cope with possibility that unsigned long is wider than BlockNumber,
// in which case strtoul will not raise an error for some values that
// are out of the range of BlockNumber.  (See similar code in
// oidin().)
//
#if SIZEOF_LONG > 4
		if (cvt != (unsigned long) blockNumber &&
			cvt != (unsigned long) ((int32) blockNumber))
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_TEXT_REPRESENTATION),
					 errmsg("invalid input syntax for block number: \"%s\"",
							*newval)));
#endif

		myextra = (BlockNumber *) guc_malloc(ERROR, sizeof(BlockNumber));
		*myextra = blockNumber;
		*extra = ( *) myextra;
	}
	pub static mut TRUE: return = std::mem::zeroed();
}

fn
assign_debug_max_bridge_ctid(const newval: &mut char,  *extra)
{
	if (newval && strcmp(newval, "") != 0)
		max_bridge_ctid_blkno = *((BlockNumber *) extra);
	else
		max_bridge_ctid_blkno = InvalidBlockNumber;
}
