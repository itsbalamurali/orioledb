use crate::access::hash;
use crate::access::xlog_internal;
use crate::access::xlogrecovery;
use crate::access::xlogutils;
use crate::btree::btree;
use crate::btree::io;
use crate::btree::modify;
use crate::btree::page_chunks;
use crate::btree::undo;
use crate::catalog::free_extents;
use crate::catalog::indices;
use crate::catalog::o_indices;
use crate::catalog::o_sys_cache;
use crate::catalog::pg_database;
use crate::checkpoint::checkpoint;
use crate::fcntl;
use crate::lib::ilist;
use crate::lib::pairingheap;
use crate::orioledb;
use crate::pgstat;
use crate::postmaster::postmaster;
use crate::postmaster::startup;
use crate::recovery::internal;
use crate::recovery::recovery;
use crate::recovery::wal;
use crate::recovery::wal_reader;
use crate::replication::message;
use crate::replication::walreceiver;
use crate::storage::copydir;
use crate::storage::ipc;
use crate::storage::lmgr;
use crate::storage::shm_mq;
use crate::storage::standby;
use crate::sys::stat;
use crate::tableam::descr;
use crate::tableam::operations;
use crate::tableam::tree;
use crate::transam::oxid;
use crate::transam::undo;
use crate::tuple::slot;
use crate::unistd;
use crate::utils::dsa;
use crate::utils::elog;
use crate::utils::inval;
use crate::utils::memdebug;
use crate::utils::memutils;
use crate::utils::page_pool;
use crate::utils::stopevent;
use crate::utils::syscache;
use crate::utils::typcache;
use crate::workers::interrupt;
use pgrx::pg_sys::ItemPointerData;
use pgrx::pg_sys;

// -------------------------------------------------------------------------
//
// recovery.c
// General routines for orioledb recovery.
//
// Copyright (c) 2021-2026, Oriole DB Inc.
// Copyright (c) 2025-2026, Supabase Inc.
//
// IDENTIFICATION
// contrib/orioledb/src/recovery/recovery.c
//
// -------------------------------------------------------------------------
//

static mut PG_ATOMIC_UINT64: *mut recovery_main_retain_ptr = std::ptr::null_mut();
static mut PG_ATOMIC_UINT64: *mut recovery_index_next_pos = std::ptr::null_mut();
static OBTreeModifyCallbackAction recovery_delete_primary_callback(descr: &mut BTreeDescr,
																   OTuple tup, newtup: &mut OTuple,
																   OXid oxid, OTupleXactInfo xactInfo,
																   UndoLocation location, lock_mode: &mut RowLockMode,
																   hint: &mut BTreeLocationHint,
																    *arg);
static OBTreeModifyCallbackAction recovery_delete_deleted_primary_callback(descr: &mut BTreeDescr,
																		   OTuple tup, newtup: &mut OTuple,
																		   OXid oxid, OTupleXactInfo xactInfo,
																		   BTreeLeafTupleDeletedStatus deleted,
																		   UndoLocation location, lock_mode: &mut RowLockMode,
																		   hint: &mut BTreeLocationHint,
																		    *arg);
static OBTreeModifyCallbackAction recovery_delete_deleted_overwrite_callback(descr: &mut BTreeDescr,
																			 OTuple tup, newtup: &mut OTuple,
																			 OXid oxid, OTupleXactInfo xactInfo,
																			 BTreeLeafTupleDeletedStatus deleted,
																			 UndoLocation location, lock_mode: &mut RowLockMode,
																			 hint: &mut BTreeLocationHint,
																			  *arg);
fn worker_send_msg(int worker_id, Pointer msg, uint64 msg_size);
fn worker_queue_flush(int worker_id);
fn recovery_send_leader_oids(ORelOids oids, OIndexNumber ix_num,
									  uint32 o_table_version,
									  ORelOids old_oids, uint32 old_o_table_version,
									  bool isrebuild);
fn workers_send_finish(bool send_to_idx_pool);
static XLogRecPtr recovery_get_current_ptr();

//
// Recovery worker state in pool.
//
typedef struct
{
	// Pointer to the worker queue
	pub static mut SHM_MQ_HANDLE: *mut queue = std::ptr::null_mut();
	char		queue_buf[RECOVERY_QUEUE_BUF_SIZE];
	pub static mut QUEUE_BUF_LEN: std::os::raw::c_int = 0;
	// Current oids
	pub static mut OIDS: ORelOids = std::mem::zeroed();
	// Current oxid
	pub static mut OXID: OXid = std::mem::zeroed();
	// Current index type
	pub static mut TYPE: OIndexType = std::mem::zeroed();
	// Handle for the worker
	pub static mut BACKGROUND_WORKER_HANDLE: *mut handle = std::ptr::null_mut();
} RecoveryWorkerState;

static mut RECOVERY_WORKER_STATE: *mut workers_pool = std::ptr::null_mut();

typedef struct
{
	ORelOids	oids;			// hash table key
	pub static mut POSITION: uint64 = std::mem::zeroed();
} RecoveryIdxBuildQueueState;

//
// Recovery transaction state.
//
typedef struct
{
	OXid		oxid;			// hash table key

	TransactionId xid;			// builtin transaction identifier for joint
// commit

	pub static mut NEEDS_WAL_FLUSH: bool = false;
	UndoLocation retain_locs[(int) UndoLogsCount];
	UndoStackLocations undo_stacks[(int) UndoLogsCount];
	pub static mut CHECKPOINT_UNDO_STACKS: dlist_head = std::mem::zeroed();
	pub static mut CSN: CommitSeqNo = std::mem::zeroed();
	pub static mut PTR: XLogRecPtr = std::mem::zeroed();

	pub static mut IN_FINISHED_LIST: bool = false;
	pub static mut IN_JOINT_COMMIT_LIST: bool = false;
	bool		in_retain_undo_heaps[(int) UndoLogsCount];
	pub static mut NEEDS_FEEDBACK: bool = false;

	pub static mut JOINT_COMMIT_LIST_NODE: dlist_node = std::mem::zeroed();
	pub static mut FINISHED_LIST_NODE: dlist_node = std::mem::zeroed();
	pairingheap_node retain_undo_ph_nodes[(int) UndoLogsCount];
	pub static mut XMIN_PH_NODE: pairingheap_node = std::mem::zeroed();

	// is any system tree modified by oxid
	pub static mut SYSTREE_MODIFIED: bool = false;
	// is typecache invalidation needed after this transaction
	pub static mut INVALIDATE_TYPCACHE: bool = false;
	// is oTablesMetaLock held by transaction
	pub static mut O_TABLES_META_LOCKED: bool = false;
	// is provided by checkpoint xids file
	pub static mut CHECKPOINT_XID: bool = false;
	// is started from wal stream
	pub static mut WAL_XID: bool = false;
	// usage map
	pub static mut BOOL: *mut used_by = std::ptr::null_mut();
} RecoveryXidState;

#define RetainUndoNodeGetRecoveryXidState(node, undoType) \
	((RecoveryXidState *) ((Pointer) (node) - \
		offsetof(RecoveryXidState, retain_undo_ph_nodes) - \
		sizeof(pairingheap_node) * (int) (undoType)))

typedef struct
{
	pub static mut KIND: XidRecKind = std::mem::zeroed();
	pub static mut UNDO_STACK: UndoStackLocations = std::mem::zeroed();
	pub static mut NODE: dlist_node = std::mem::zeroed();
} CheckpointUndoStack;

#define WORKER_UNDO_TEMP_FILE (ORIOLEDB_DATA_DIR"/recovery_worker_%d.undotmp")

typedef struct WorkerUndoTempHeader
{
	pub static mut WORKER_ID: std::os::raw::c_int = 0;
	pub static mut NUM_TRANSACTIONS: uint32 = std::mem::zeroed();
} WorkerUndoTempHeader;

typedef struct WorkerUndoTempEntry
{
	pub static mut OXID: OXid = std::mem::zeroed();
	pub static mut CSN: CommitSeqNo = std::mem::zeroed();
	UndoStackLocations undoStacks[UndoLogsCount];
	UndoLocation undoRetainLocs[UndoLogsCount];
	pub static mut NUM_CHECKPOINT_STACKS: uint32 = std::mem::zeroed();
	// How many checkpoint stacks follow
} WorkerUndoTempEntry;

typedef struct WorkerUndoTempCheckpointStack
{
	pub static mut KIND: XidRecKind = std::mem::zeroed();
	pub static mut UNDO_STACK: UndoStackLocations = std::mem::zeroed();
} WorkerUndoTempCheckpointStack;

PG_FUNCTION_INFO_V1(orioledb_recovery_synchronized);

//
// Comparator for undo retain min-heap.
//
// See pairingheap.c/pairingheap_comparator description.
//
static int
retain_undo_pairingheap_cmp(const a: &mut pairingheap_node,
							const b: &mut pairingheap_node,
							 *arg)
{
	int			num = *((int *) arg);
	const l: &mut RecoveryXidState = RetainUndoNodeGetRecoveryXidState(a, num);
	const r: &mut RecoveryXidState = RetainUndoNodeGetRecoveryXidState(b, num);

	if (l->retain_locs[num] < r->retain_locs[num])
		pub static mut 1: return = std::mem::zeroed();
	else if (l->retain_locs[num] > r->retain_locs[num])
		return -1;
	else
		pub static mut 0: return = std::mem::zeroed();
}

//
// Comparator for xmin min-heap.
//
// See pairingheap.c/pairingheap_comparator description.
//
static int
xmin_pairingheap_cmp(const a: &mut pairingheap_node,
					 const b: &mut pairingheap_node,
					  *arg)
{
	const l: &mut RecoveryXidState = pairingheap_const_container(RecoveryXidState, xmin_ph_node, a);
	const r: &mut RecoveryXidState = pairingheap_const_container(RecoveryXidState, xmin_ph_node, b);

	if (l->oxid < r->oxid)
		pub static mut 1: return = std::mem::zeroed();
	else if (l->oxid > r->oxid)
		return -1;
	else
		pub static mut 0: return = std::mem::zeroed();
}

// Current recovery transaction state.
static mut RECOVERY_XID_STATE: *mut cur_recovery_xid_state = std::ptr::null_mut();

// Recovery transaction hash for the current process.
static mut HTAB: *mut recovery_xid_state_hash = std::ptr::null_mut();

static mut HTAB: *mut idxbuild_oids_hash = std::ptr::null_mut();

// Queues of undo retain locations
static retain_undo_queues: &mut pairingheap[(int) UndoLogsCount] =
{
	NULL
};
static int	retain_undo_queue_numbers[(int) UndoLogsCount];

// Queue of xmin's
static mut PAIRINGHEAP: *mut xmin_queue = std::ptr::null_mut();

//
// List of locally finished transaction, which aren't yet knows as finished
// for every recovery process.
//
static mut FINISHED_LIST: dlist_head = std::mem::zeroed();

//
// List of transactions waiting for joint commit with builtin transaction.
//
static mut JOINT_COMMIT_LIST: dlist_head = std::mem::zeroed();

// orioledb checkpoint number from which we start recovery
static mut STARTUP_CHKP_NUM: uint32 = std::mem::zeroed();

// is recovery main process has error
static mut UNEXPECTED_WORKER_DETACH: bool = false;

//
// True if current process is a recovery process (worker or master).
//
static mut IAM_RECOVERY: bool = false;

//
// In-flight oxids that recovery_finish() aborted in memory.  These were left
// COMMITSEQNO_INPROGRESS at end-of-redo with no COMMIT/ROLLBACK on the wire,
// so a streaming standby cannot resolve them on its own.  Captured here for
// the after-checkpoint hook to flush as WAL_REC_ROLLBACK once
// LocalSetXLogInsertAllowed() has run (issue #876).
//
typedef struct
{
	pub static mut OXID: OXid = std::mem::zeroed();
	pub static mut XID: TransactionId = std::mem::zeroed();
} RecoveryFinishAbortedOxid;

static mut RECOVERY_FINISH_ABORTED_OXID: *mut recovery_finish_aborted_oxids = std::ptr::null_mut();
static mut RECOVERY_FINISH_ABORTED_COUNT: std::os::raw::c_int = 0;
static mut RECOVERY_FINISH_ABORTED_CAPACITY: std::os::raw::c_int = 0;

//
// Current orioledb transaction recovery id
//
pub static mut RECOVERY_OXID: OXid = InvalidOXid;

//
// Full size of a recovery queue.
//
pub static mut RECOVERY_QUEUE_DATA_SIZE: uint64 = 0;

//
// The pointer to a first recovery queue.
//
pub static mut RECOVERY_FIRST_QUEUE: Pointer = std::ptr::null_mut();

//
// GUC value, number of recovery workers.
//
pub static mut RECOVERY_POOL_SIZE_GUC: std::os::raw::c_int = 0;
pub static mut RECOVERY_IDX_POOL_SIZE_GUC: std::os::raw::c_int = 0;

//
// GUC value, size of a single recovery queue in KB.
//
pub static mut RECOVERY_QUEUE_SIZE_GUC: std::os::raw::c_int = 0;

//
// Are TOAST trees consistent with primary indices.
//
pub static mut TOAST_CONSISTENT: bool = false;

//
// Pending PK->SK fix-ups, populated from XidRecPendingSkFixup records read
// out of the xid file at recovery start.  The list is drained once the
// recovery hits the toast-consistent boundary, at which point every entry
// is turned into synthesised secondary-index modify records.  See
// record_pending_sk_fixup() / apply_pending_sk_fixups().
//
typedef struct PendingSkFixup
{
	pub static mut OXID: OXid = std::mem::zeroed();
	pub static mut UNDO_LOCATION: UndoLocation = std::mem::zeroed();
	pub static mut PENDING_SK_FIXUP: *mut struct next = std::ptr::null_mut();
} PendingSkFixup;

static mut PENDING_SK_FIXUP: *mut pending_sk_fixups_head = std::ptr::null_mut();

fn
record_pending_sk_fixup(OXid oxid, UndoLocation undoLocation)
{
	pub static mut PENDING_SK_FIXUP: *mut entry = std::ptr::null_mut();

	entry = (PendingSkFixup *) MemoryContextAlloc(TopMemoryContext,
												  sizeof(*entry));
	entry->oxid = oxid;
	entry->undoLocation = undoLocation;
	entry->next = pending_sk_fixups_head;
	pending_sk_fixups_head = entry;
}

//
// Apply one PendingSkFixup entry: read the PK undo record back, locate
// the current PK tuple, and for every secondary index whose key differs
// between the pre-image and the post-image, dispatch a synthesised
// DELETE old / INSERT new pair through the recovery_workers' modify
// path.  Mirrors apply_tbl_update()'s per-SK logic.
//
fn
apply_one_pending_sk_fixup(entry: &mut PendingSkFixup)
{
	pub static mut TUPHDR_LOC: UndoLocation = entry->undoLocation;
	pub static mut ITEM_LOC: UndoLocation = std::mem::zeroed();
	BTreeModifyUndoStackItem item = {0};
	pub static mut OLD_TUPLE_SIZE: LocationIndex = std::mem::zeroed();
	pub static mut O_TABLE_DESCR: *mut descr = std::ptr::null_mut();
	pub static mut O_INDEX_DESCR: *mut primary = std::ptr::null_mut();
	pub static mut PK_KEY: OBTreeKeyBound = std::mem::zeroed();
	pub static mut OLD_TUPLE: OTuple = std::mem::zeroed();
	pub static mut NEW_TUPLE: OTuple = std::mem::zeroed();
	pub static mut FIND_RESULT: OFindPageResult = std::mem::zeroed();
	pub static mut CONTEXT: OBTreeFindPageContext = std::mem::zeroed();
	pub static mut PAGE_LOC: BTreePageItemLocator = std::mem::zeroed();
	pub static mut PK_PAGE: Page = std::mem::zeroed();
	pub static mut PK_ON_PAGE: OTuple = std::mem::zeroed();
	pub static mut NEW_TUPLE_LEN: LocationIndex = std::mem::zeroed();
	pub static mut TUPLE_TABLE_SLOT: *mut newSlot = std::ptr::null_mut();
	pub static mut TUPLE_TABLE_SLOT: *mut oldSlot = std::ptr::null_mut();
	pub static mut I: std::os::raw::c_int = 0;

	if (!UndoLocationIsValid(tuphdrLoc))
		return;

	//
// The marker we recorded is the location of the BTreeLeafTuphdr field
// inside the BTreeModifyUndoStackItem (that's what make_undo_record()
// returns); back up to the start of the item.
//
	itemLoc = tuphdrLoc - offsetof(BTreeModifyUndoStackItem, tuphdr);

	if (!UNDO_REC_EXISTS(UndoLogRegular, itemLoc))
	{
		// recycled in the meantime; nothing we can do
		elog(DEBUG2,
			 "pending-SK fix-up: undo record at %X/%X recycled, skipping",
			 (uint32) (itemLoc >> 32), (uint32) itemLoc);
		return;
	}

	undo_read(UndoLogRegular, itemLoc, sizeof(item), (Pointer) &item);
	if (item.header.type != ModifyUndoItemType)
		return;

	//
// Only PK modifications carry SK-side obligations; tuple inserts /
// updates / deletes are the only relevant actions.
//
	if (item.action != BTreeOperationUpdate &&
		item.action != BTreeOperationInsert &&
		item.action != BTreeOperationDelete)
		return;

	//
// item.oids is the PK index's relation OIDs (make_undo_record stores
// desc->oids).  Resolve the table descr through the index descr's
// tableOids back-pointer.
//
	{
		pub static mut O_INDEX_DESCR: *mut indexDescr = std::ptr::null_mut();

		indexDescr = o_fetch_index_descr(item.oids, oIndexPrimary,
										 false, NULL);
		if (indexDescr == NULL)
			return;
		descr = o_fetch_table_descr(indexDescr->tableOids);
	}
	if (descr == NULL || descr->nIndices < 2)
		return;
	primary = GET_PRIMARY(descr);

	// The undo entry stores the pre-image tuple right after the header.
	oldTupleSize = item.header.itemSize - sizeof(BTreeModifyUndoStackItem);
	if (oldTupleSize == 0)
		return;
	oldTuple.formatFlags = item.tuphdr.formatFlags;
	oldTuple.data = palloc(oldTupleSize);
	undo_read(UndoLogRegular,
			  itemLoc + sizeof(BTreeModifyUndoStackItem),
			  oldTupleSize,
			  oldTuple.data);

	o_btree_load_shmem(&primary->desc);
	O_TUPLE_SET_NULL(newTuple);

	//
// The DELETE undo record stores only the PK key (make_undo_record
// extracts the key portion when action == BTreeOperationDelete with
// is_tuple=true), which is not enough to derive secondary-index keys.
// Look the row up on the PK page: at this point in recovery the leaf
// still carries the full tuple data even though tuphdr->deleted is set,
// since neither vacuum nor page compaction has reclaimed it yet.
//
// For INSERT/UPDATE the on-page PK row is the post-image we want to feed
// into the SK loop as newSlot.
//
// For DELETE the on-page row is the row to be removed; we copy it back
// into oldTuple so the SK loop can derive the SK key from full attrs.
//
	if (item.action == BTreeOperationDelete)
	{
		pub static mut KEY_TUPLE: OTuple = std::mem::zeroed();

		// oldTuple currently holds just the PK key.  Build a key bound.
		keyTuple = oldTuple;
		o_fill_key_bound(primary, keyTuple, BTreeKeyNonLeafKey, &pkKey);
	}
	else
	{
		o_fill_key_bound(primary, oldTuple, BTreeKeyLeafTuple, &pkKey);
	}

	init_page_find_context(&context, &primary->desc,
						   COMMITSEQNO_INPROGRESS,
						   BTREE_PAGE_FIND_MODIFY);
	findResult = find_page(&context, &pkKey, BTreeKeyBound, 0);
	if (findResult != OFindPageResultSuccess)
	{
		pfree(oldTuple.data);
		return;
	}

	pkPage = O_GET_IN_MEMORY_PAGE(context.items[context.index].blkno);
	pageLoc = context.items[context.index].locator;

	if (!BTREE_PAGE_LOCATOR_IS_VALID(pkPage, &pageLoc))
	{
		unlock_page(context.items[context.index].blkno);
		pfree(oldTuple.data);
		return;
	}

	BTREE_PAGE_READ_TUPLE(pkOnPage, pkPage, &pageLoc);

	// Materialise a private copy before releasing the page lock.
	newTupleLen = o_btree_len(&primary->desc, pkOnPage, OTupleLength);

	if (item.action == BTreeOperationDelete)
	{
		//
// Swap the key-only oldTuple for the full leaf tuple read off the
// page; we need full column attrs to derive the SK key.
//
		pfree(oldTuple.data);
		oldTuple.formatFlags = pkOnPage.formatFlags;
		oldTuple.data = palloc(newTupleLen);
		memcpy(oldTuple.data, pkOnPage.data, newTupleLen);
	}
	else
	{
		newTuple.formatFlags = pkOnPage.formatFlags;
		newTuple.data = palloc(newTupleLen);
		memcpy(newTuple.data, pkOnPage.data, newTupleLen);
	}

	unlock_page(context.items[context.index].blkno);

	newSlot = descr->newTuple;
	oldSlot = descr->oldTuple;

	//
// INSERT's undo record carries only the PK key (not a full leaf tuple),
// so feeding it into oldSlot would expose junk to anything that reads
// past the key columns.  Only populate oldSlot when the action actually
// has a pre-image -- UPDATE (full tuple from undo) or DELETE (full tuple
// we just read off the PK page).
//
	if (!O_TUPLE_IS_NULL(newTuple))
		tts_orioledb_store_tuple(newSlot, newTuple, descr,
								 COMMITSEQNO_INPROGRESS, PrimaryIndexNumber,
								 true, NULL);
	if (item.action != BTreeOperationInsert)
		tts_orioledb_store_tuple(oldSlot, oldTuple, descr,
								 COMMITSEQNO_INPROGRESS, PrimaryIndexNumber,
								 true, NULL);

	//
// For each secondary index, DELETE the pre-image entry and INSERT the
// post-image entry when they differ.  Same shape as apply_tbl_update() so
// workers' overwrite callbacks make these idempotent against any later
// WAL records.
//
	for (i = 1; i < descr->nIndices; i++)
	{
		pub static mut O_INDEX_DESCR: *mut sk = descr->indices[i];
		OBTreeKeyBound oldSkKey,
					newSkKey;
		pub static mut CB_INFO: BTreeModifyCallbackInfo = nullCallbackInfo;
		pub static mut NULL_TUP: OTuple = std::mem::zeroed();
		pub static mut NEED_DELETE: bool = false;
		pub static mut NEED_INSERT: bool = false;

		O_TUPLE_SET_NULL(nullTup);

		if (item.action == BTreeOperationInsert)
		{
			tts_orioledb_fill_key_bound(newSlot, sk, &newSkKey);
			needInsert = true;
		}
		else if (item.action == BTreeOperationDelete)
		{
			tts_orioledb_fill_key_bound(oldSlot, sk, &oldSkKey);
			needDelete = true;
		}
		else					// UPDATE
		{
			pub static mut CMP: std::os::raw::c_int = 0;

			tts_orioledb_fill_key_bound(oldSlot, sk, &oldSkKey);
			tts_orioledb_fill_key_bound(newSlot, sk, &newSkKey);
			cmp = o_btree_cmp(&sk->desc,
							  (Pointer) &oldSkKey, BTreeKeyBound,
							  (Pointer) &newSkKey, BTreeKeyBound);
			if (cmp != 0)
			{
				needDelete = true;
				needInsert = true;
			}
		}

		o_btree_load_shmem(&sk->desc);

		if (needDelete &&
			o_is_index_predicate_satisfied(sk, oldSlot, sk->econtext))
		{
			cbInfo.modifyCallback = recovery_delete_overwrite_callback;
			() o_btree_modify(&sk->desc, BTreeOperationDelete,
								  nullTup, BTreeKeyNone,
								  (Pointer) &oldSkKey, BTreeKeyBound,
								  entry->oxid, COMMITSEQNO_INPROGRESS,
								  RowLockUpdate, NULL, &cbInfo);
		}

		if (needInsert &&
			o_is_index_predicate_satisfied(sk, newSlot, sk->econtext))
		{
			pub static mut NEW_SK_TUP: OTuple = std::mem::zeroed();

			newSkTup = tts_orioledb_make_secondary_tuple(newSlot, sk, true);
			if (o_btree_len(&sk->desc, newSkTup, OTupleLength)
				<= O_BTREE_MAX_TUPLE_SIZE)
			{
				cbInfo.modifyCallback = recovery_insert_overwrite_callback;
				cbInfo.modifyDeletedCallback = recovery_insert_deleted_overwrite_callback;
				() o_btree_modify(&sk->desc, BTreeOperationInsert,
									  newSkTup, BTreeKeyLeafTuple,
									  (Pointer) &newSkKey, BTreeKeyBound,
									  entry->oxid, COMMITSEQNO_INPROGRESS,
									  RowLockUpdate, NULL, &cbInfo);
			}
			pfree(newSkTup.data);
		}
	}

	//
// Both slots took ownership of their tuple data via shouldFree=true, so
// ExecClearTuple is responsible for the pfree -- don't free the local
// OTuple structs again.  For INSERT we never stored oldTuple in the slot,
// so its undo-read buffer is still ours to release.
//
	ExecClearTuple(newSlot);
	ExecClearTuple(oldSlot);
	if (item.action == BTreeOperationInsert)
		pfree(oldTuple.data);
}

//
// Turn every PendingSkFixup record gathered by record_pending_sk_fixup()
// into synthesised secondary-index modifications.  Called once, by the
// master recovery process, at the moment WAL replay first crosses the
// toast-consistent boundary, so the SK trees catch up to PK before any
// post-boundary WAL records get applied.
//
fn
apply_pending_sk_fixups()
{
	pub static mut PENDING_SK_FIXUP: *mut entry = pending_sk_fixups_head;
	pub static mut SAVED_OXID: OXid = recovery_oxid;

	while (entry != NULL)
	{
		pub static mut PENDING_SK_FIXUP: *mut next = entry->next;

		recovery_switch_to_oxid(entry->oxid, -1);
		set_oxid_csn(entry->oxid, COMMITSEQNO_INPROGRESS);

		apply_one_pending_sk_fixup(entry);

		pfree(entry);
		entry = next;
	}

	pending_sk_fixups_head = NULL;
	if (OXidIsValid(saved_oxid))
		recovery_switch_to_oxid(saved_oxid, -1);
	else
		recovery_oxid = InvalidOXid;
}

//
// Checkpoint requests for flushing undo positions and their completion.
//
pub static mut RECOVERY_UNDO_LOC_FLUSH: *mut recovery_undo_loc_flush = std::ptr::null_mut();

//
// The last xmin we received from primary.
//
static mut RECOVERY_XMIN: OXid = InvalidOXid;

//
// Number of successfully finished recovery workers.
//
pub static mut PG_ATOMIC_UINT32: *mut worker_finish_count = std::ptr::null_mut();
pub static mut PG_ATOMIC_UINT32: *mut idx_worker_finish_count = std::ptr::null_mut();
pub static mut PG_ATOMIC_UINT32: *mut worker_ptrs_changes = std::ptr::null_mut();
pub static mut RECOVERY_WORKER_PTRS: *mut worker_ptrs = std::ptr::null_mut();
pub static mut PG_ATOMIC_UINT64: *mut recovery_ptr = std::ptr::null_mut();
static mut PG_ATOMIC_UINT64: *mut recovery_main_retain_ptr = std::ptr::null_mut();
pub static mut PG_ATOMIC_UINT64: *mut recovery_finished_list_ptr = std::ptr::null_mut();
pub static mut BOOL: *mut recovery_single_process = std::ptr::null_mut();
pub static mut BOOL: *mut was_in_recovery = std::ptr::null_mut();
pub static mut PG_ATOMIC_UINT32: *mut after_recovery_cleaned = std::ptr::null_mut();

static mut PG_ATOMIC_UINT64: *mut recovery_index_next_pos = std::ptr::null_mut();
pub static mut PG_ATOMIC_UINT64: *mut recovery_index_completed_pos = std::ptr::null_mut();
pub static mut CONDITION_VARIABLE: *mut recovery_index_cv = std::ptr::null_mut();

// TransactionId for system trees modification for using in recovery
pub static mut RECOVERY_HEAP_TRANSACTION_ID: TransactionId = InvalidTransactionId;

fn delay_rels_queued_for_idxbuild(ORelOids oids);
fn delay_if_queued_for_idxbuild();
fn update_run_xmin();
fn free_run_xmin();
static bool need_flush_undo_pos(int worker_id);
fn flush_current_undo_stack();
fn o_handle_startup_proc_interrupts_hook();
fn abort_recovery(workers_pool: &mut RecoveryWorkerState, bool send_to_idx_pool);

static bool replay_container(Pointer startPtr, Pointer endPtr,
							 bool single, XLogRecPtr xlogRecPtr,
							 XLogRecPtr xlogRecEndPtr);

fn worker_send_modify(int worker_id, desc: &mut BTreeDescr,
							   RecoveryMsgType recType,
							   OTuple tuple, int tuple_len);
fn workers_send_oxid_finish(XLogRecPtr ptr, bool needsFeedback,
									 bool commit);
fn workers_send_savepoint(SubTransactionId parentSubId);
fn workers_send_rollback_to_savepoint(XLogRecPtr ptr,
											   SubTransactionId parentSubId);
fn workers_synchronize(XLogRecPtr ptr, bool send_synchronize);
fn workers_notify_toast_consistent();
fn worker_wait_shutdown(worker: &mut RecoveryWorkerState);

static inline bool apply_sys_tree_modify_record(int sys_tree_num, uint16 type,
												OTuple tup,
												OXid oxid, CommitSeqNo csn);
static inline  spread_idx_modify(desc: &mut BTreeDescr,
									 RecoveryMsgType recType,
									 OTuple rec);

static inline RecoveryMsgType recovery_msg_from_wal_record(WalRecordType rec_type);
fn recovery_send_init(int worker_num);

//
// Returns full size of the shared memory needed to recovery.
//
Size
recovery_shmem_needs()
{
	pub static mut SIZE: Size = 0;

	size = add_size(size, mul_size(CACHELINEALIGN((Size) recovery_queue_size_guc * 1024),
								   recovery_pool_size_guc + recovery_idx_pool_size_guc));
	size = add_size(size, CACHELINEALIGN(sizeof(bool)));
	size = add_size(size, CACHELINEALIGN(sizeof(pg_atomic_uint32)));
	size = add_size(size, CACHELINEALIGN(sizeof(pg_atomic_uint32)));
	size = add_size(size, CACHELINEALIGN(sizeof(pg_atomic_uint32)));
	size = add_size(size, CACHELINEALIGN(sizeof(RecoveryUndoLocFlush)));
	size = add_size(size, CACHELINEALIGN(mul_size(sizeof(RecoveryWorkerPtrs),
												  recovery_pool_size_guc + recovery_idx_pool_size_guc)));
	size = add_size(size, CACHELINEALIGN(mul_size(sizeof(pg_atomic_uint64), 3)));
	size = add_size(size, CACHELINEALIGN(sizeof(bool)));
	size = add_size(size, CACHELINEALIGN(sizeof(pg_atomic_uint32)));
	size = add_size(size, CACHELINEALIGN(sizeof(pg_atomic_uint64)));
	size = add_size(size, CACHELINEALIGN(sizeof(pg_atomic_uint64)));
	size = add_size(size, CACHELINEALIGN(sizeof(ConditionVariable)));

	pub static mut SIZE: return = std::mem::zeroed();
}

//
// Initializes recovery shared memory.
//
// Must be called after checkpoint_shmem_init() because it initializes
// startupCommitSeqNo.
//

recovery_shmem_init(Pointer ptr, bool found)
{
	recovery_queue_data_size = (Size) recovery_queue_size_guc * 1024;

	recovery_first_queue = ptr;
	ptr += mul_size(CACHELINEALIGN(recovery_queue_data_size),
					recovery_pool_size_guc + recovery_idx_pool_size_guc);

	recovery_single_process = (bool *) ptr;
	ptr += CACHELINEALIGN(sizeof(bool));

	worker_finish_count = (pg_atomic_uint32 *) ptr;
	ptr += CACHELINEALIGN(sizeof(pg_atomic_uint32));

	idx_worker_finish_count = (pg_atomic_uint32 *) ptr;
	ptr += CACHELINEALIGN(sizeof(pg_atomic_uint32));

	worker_ptrs_changes = (pg_atomic_uint32 *) ptr;
	ptr += CACHELINEALIGN(sizeof(pg_atomic_uint32));

	recovery_undo_loc_flush = (RecoveryUndoLocFlush *) ptr;
	ptr += CACHELINEALIGN(sizeof(RecoveryUndoLocFlush));

	worker_ptrs = (RecoveryWorkerPtrs *) ptr;
	ptr += CACHELINEALIGN(mul_size(sizeof(RecoveryWorkerPtrs), recovery_pool_size_guc + recovery_idx_pool_size_guc));

	recovery_ptr = (pg_atomic_uint64 *) ptr;
	recovery_main_retain_ptr = recovery_ptr + 1;
	recovery_finished_list_ptr = recovery_ptr + 2;

	ptr += CACHELINEALIGN(mul_size(sizeof(pg_atomic_uint64), 3));

	was_in_recovery = (bool *) ptr;
	ptr += CACHELINEALIGN(sizeof(bool));

	after_recovery_cleaned = (pg_atomic_uint32 *) ptr;
	ptr += CACHELINEALIGN(sizeof(pg_atomic_uint32));

	recovery_index_next_pos = (pg_atomic_uint64 *) ptr;
	ptr += CACHELINEALIGN(sizeof(pg_atomic_uint64));

	recovery_index_completed_pos = (pg_atomic_uint64 *) ptr;
	ptr += CACHELINEALIGN(sizeof(pg_atomic_uint64));

	recovery_index_cv = (ConditionVariable *) ptr;
	ptr += CACHELINEALIGN(sizeof(ConditionVariable));

	if (!found)
	{
		pub static mut I: std::os::raw::c_int = 0;

		recovery_undo_loc_flush->finishRequestCheckpointNumber = 0;
		recovery_undo_loc_flush->immediateRequestCheckpointNumber = 0;
		recovery_undo_loc_flush->completedCheckpointNumber = UINT32_MAX;
		recovery_undo_loc_flush->recoveryMainCompletedCheckpointNumber = 0;
		SpinLockInit(&recovery_undo_loc_flush->exitLock);

		pg_atomic_init_u32(worker_finish_count, 0);
		pg_atomic_init_u32(idx_worker_finish_count, 0);
		pg_atomic_init_u32(worker_ptrs_changes, 0);

		for (i = 0; i < recovery_pool_size_guc + recovery_idx_pool_size_guc; i++)
		{
			shm_mq_create(GET_WORKER_QUEUE(i), recovery_queue_data_size);
			pg_atomic_init_u64(&worker_ptrs[i].commitPtr, InvalidXLogRecPtr);
			pg_atomic_init_u64(&worker_ptrs[i].retainPtr, InvalidXLogRecPtr);
			worker_ptrs[i].flushedUndoLocCompletedCheckpointNumber = 0;
			pg_atomic_init_flag(&worker_ptrs[i].hasTempFile);
		}
		pg_atomic_init_u64(recovery_ptr, InvalidXLogRecPtr);
		pg_atomic_init_u64(recovery_main_retain_ptr, InvalidXLogRecPtr);
		pg_atomic_init_u64(recovery_finished_list_ptr, InvalidXLogRecPtr);

		*was_in_recovery = false;
		pg_atomic_init_u32(after_recovery_cleaned, 0);

		pg_atomic_init_u64(recovery_index_next_pos, 0);
		pg_atomic_init_u64(recovery_index_completed_pos, 0);
		ConditionVariableInit(recovery_index_cv);
	}
}

fn
undo_stack_locations_set_invalid(location: &mut UndoStackLocations)
{
	location->location = InvalidUndoLocation;
	location->subxactLocation = InvalidUndoLocation;
	location->branchLocation = InvalidUndoLocation;
	location->onCommitLocation = InvalidUndoLocation;
}

//
// Read information about undo locations of in-progress transactions.
//
fn
read_xids(int checkpointnum, bool recovery_single, int worker_id)
{
	xidFilename: &mut char = psprintf(XID_FILENAME_FORMAT, checkpointnum);
	pub static mut XID_FILE: File = std::mem::zeroed();
	pub static mut OFFSET: off_t = 0;
	uint32		count = 0,
				i;

	xidFile = PathNameOpenFile(xidFilename, O_RDONLY | PG_BINARY);
	if (xidFile < 0)
		ereport(FATAL, (errcode_for_file_access(),
						errmsg("could not open xid file %s: %m", xidFilename)));

	if (OFileRead(xidFile, (Pointer) &count,
				  sizeof(count), offset,
				  WAIT_EVENT_SLRU_READ) != sizeof(count))
		ereport(FATAL, (errcode_for_file_access(),
						errmsg("could not read xid record from file %s: %m", xidFilename)));
	offset += sizeof(count);

	for (i = 0; i < count; i++)
	{
		pub static mut RECOVERY_XID_STATE: *mut state = std::ptr::null_mut();
		XidFileRec	xidRec = {0};
		pub static mut FOUND: bool = false;

		if (OFileRead(xidFile, (Pointer) &xidRec,
					  sizeof(xidRec), offset,
					  WAIT_EVENT_SLRU_READ) != sizeof(xidRec))
			ereport(FATAL, (errcode_for_file_access(),
							errmsg("could not read xid record from file %s: %m", xidFilename)));

		advance_oxids(xidRec.oxid);
		state = (RecoveryXidState *) hash_search(recovery_xid_state_hash,
												 &xidRec.oxid,
												 HASH_ENTER,
												 &found);

		if (!found)
		{
			pub static mut J: std::os::raw::c_int = 0;

			state->xid = InvalidTransactionId;
			state->needs_wal_flush = false;
			for (j = 0; j < (int) UndoLogsCount; j++)
				state->retain_locs[j] = InvalidUndoLocation;	// undo locations are
// held by checkpoint
			state->csn = COMMITSEQNO_INPROGRESS;
			state->ptr = InvalidXLogRecPtr;
			state->in_finished_list = false;
			state->in_joint_commit_list = false;
			state->needs_feedback = false;
			for (j = 0; j < (int) UndoLogsCount; j++)
				state->in_retain_undo_heaps[j] = false;
			memset(state->undo_stacks, 0, sizeof(state->undo_stacks));
			for (j = 0; j < (int) UndoLogsCount; j++)
				undo_stack_locations_set_invalid(&state->undo_stacks[j]);
			dlist_init(&state->checkpoint_undo_stacks);
			if (worker_id < 0)
				pairingheap_add(xmin_queue, &state->xmin_ph_node);

			state->systree_modified = false;
			state->invalidate_typcache = false;
			state->o_tables_meta_locked = false;
			state->checkpoint_xid = true;
			state->wal_xid = false;
			if (!recovery_single && worker_id < 0)
				state->used_by = palloc0((recovery_pool_size_guc + recovery_idx_pool_size_guc) * sizeof(bool));
			else
				state->used_by = NULL;
		}
		if (worker_id < 0)
		{
			curProcData: &mut ODBProcData = GET_CUR_PROCDATA();
			pub static mut CHECKPOINT_UNDO_STACK: *mut stack = std::ptr::null_mut();
			pub static mut RETAIN_UNDO_LOCATION: UndoLocation = std::mem::zeroed();
			pub static mut KIND: XidRecKind = xidRec.kind;

			if (kind == XidRecPendingSkFixup)
			{
				//
// Stash the pending PK->SK fix-up; it will be turned into
// synthesised secondary-index modify records once the
// recovery hits the toast-consistent boundary.
//
				record_pending_sk_fixup(xidRec.oxid, xidRec.undoLocation.location);
				offset += sizeof(xidRec);
				continue;
			}

			stack = (CheckpointUndoStack *) MemoryContextAlloc(TopMemoryContext,
															   sizeof(CheckpointUndoStack));
			stack->kind = kind;
			stack->undoStack = xidRec.undoLocation;
			dlist_push_tail(&state->checkpoint_undo_stacks, &stack->node);
			set_oxid_csn(xidRec.oxid, COMMITSEQNO_INPROGRESS);

			//
// We will probably need to retain this till the next checkpoint.
//
			retainUndoLocation = xidRec.retainLocation;
			if ((int) kind < (int) UndoLogsCount &&
				retainUndoLocation < state->retain_locs[(UndoLogType) kind])
			{
				UndoLogType undoType = (UndoLogType) kind;
				undoMeta: &mut UndoMeta = get_undo_meta_by_type(undoType);

				if (state->in_retain_undo_heaps[undoType])
					pairingheap_remove(retain_undo_queues[undoType], &state->retain_undo_ph_nodes[undoType]);
				state->retain_locs[undoType] = retainUndoLocation;
				pairingheap_add(retain_undo_queues[undoType], &state->retain_undo_ph_nodes[undoType]);
				state->in_retain_undo_heaps[undoType] = true;

				if (state->retain_locs[undoType] < pg_atomic_read_u64(&curProcData->undoRetainLocations[undoType].transactionUndoRetainLocation))
					pg_atomic_write_u64(&curProcData->undoRetainLocations[undoType].transactionUndoRetainLocation, state->retain_locs[undoType]);

				if (state->retain_locs[undoType] < pg_atomic_read_u64(&undoMeta->minProcRetainLocation))
					pg_atomic_write_u64(&undoMeta->minProcRetainLocation, state->retain_locs[undoType]);

				if (state->retain_locs[undoType] < pg_atomic_read_u64(&undoMeta->minProcTransactionRetainLocation))
					pg_atomic_write_u64(&undoMeta->minProcTransactionRetainLocation, state->retain_locs[undoType]);
			}
		}

		offset += sizeof(xidRec);
	}

	if (worker_id < 0)
		update_run_xmin();
	FileClose(xidFile);
	pfree(xidFilename);
}

//
// Apply undo records "hidden" in undo branches.
//
// These records are intended to be already aborted.  But checkpointer could
// "see" tuples which still reference those records.  This routine is du
//
fn
apply_xids_branches()
{
	pub static mut RECOVERY_XID_STATE: *mut state = std::ptr::null_mut();
	pub static mut HASH_SEQ: HASH_SEQ_STATUS = std::mem::zeroed();

	hash_seq_init(&hash_seq, recovery_xid_state_hash);
	while ((state = (RecoveryXidState *) hash_seq_search(&hash_seq)) != NULL)
	{
		pub static mut ITER: dlist_iter = std::mem::zeroed();

		oxid_needs_wal_flush = state->needs_wal_flush;
		recovery_oxid = state->oxid;

		dlist_foreach(iter, &state->checkpoint_undo_stacks)
		{
			stack: &mut CheckpointUndoStack = dlist_container(CheckpointUndoStack,
														 node,
														 iter.cur);

			if ((int) stack->kind < (int) UndoLogsCount)
			{
				UndoLogType undoType = (UndoLogType) stack->kind;

				set_cur_undo_locations(undoType, stack->undoStack);
				apply_undo_branches(undoType, recovery_oxid);
			}
			else
			{
				pub static mut PG_USED_FOR_ASSERTS_ONLY: uint64		location = std::mem::zeroed();

				Assert(!UndoLocationIsValid(stack->undoStack.location));
				Assert(!UndoLocationIsValid(stack->undoStack.branchLocation));
				Assert(!UndoLocationIsValid(stack->undoStack.subxactLocation));
				location = walk_undo_range_with_buf((UndoLogType) ((int) stack->kind - XID_REC_REWIND_TYPES_OFFSET),
													stack->undoStack.onCommitLocation,
													InvalidUndoLocation, recovery_oxid,
													OUndoCallbackStageCommit, NULL, true);
				// NB rewindItem->oxid is not used in recovery
				Assert(!UndoLocationIsValid(location));
			}
		}
	}

	oxid_needs_wal_flush = false;
	recovery_oxid = InvalidOXid;
	reset_cur_undo_locations();
	cur_recovery_xid_state = NULL;
}


idx_workers_shutdown()
{
	pub static mut I: std::os::raw::c_int = 0;

	workers_send_finish(true);
	for (i = index_build_first_worker; i <= index_build_last_worker; i++)
	{
		worker_wait_shutdown(&workers_pool[i]);
	}

	if (pg_atomic_read_u32(idx_worker_finish_count) != index_build_workers)
		elog(ERROR, "orioledb recovery idx worker died.");
}


o_recovery_start_hook()
{
	pub static mut RECOVERY_WORKER_STATE: *mut state = std::ptr::null_mut();
	pub static mut I: std::os::raw::c_int = 0;
	pub static mut RECOVERY_SINGLE: bool = false;

	before_shmem_exit(recovery_on_proc_exit, (Datum) -1);
	recovery_single = *recovery_single_process = IsFatalError();
	if (recovery_single)
	{
		elog(LOG, "orioledb recovery after fatal error started.  Unable to make multiprocess recovery.");
	}
	else
	{
		elog(LOG, "orioledb recovery started.");
	}

	startup_chkp_num = checkpoint_state->lastCheckpointNumber;
	recovery_cleanup_old_files(startup_chkp_num, true);

	if (!recovery_single)
	{
		pub static mut FINISH: std::os::raw::c_int = recovery_idx_pool_size_guc ? index_build_leader : recovery_last_worker;

		workers_pool = palloc0(sizeof(RecoveryWorkerState) * (finish + 1));

		for (i = recovery_first_worker; i <= finish; i++)
		{
			state = &workers_pool[i];
			shm_mq_set_sender(GET_WORKER_QUEUE(i), MyProc);
			state->type = oIndexInvalid;
			ORelOidsSetInvalid(state->oids);
			state->oxid = InvalidOXid;

			workers_pool[i].handle = recovery_worker_register(i);
			if (workers_pool[i].handle == NULL)
			{
				//
// Not enough slots for background workers.
//
				for (i--; i >= 0; i--)
					TerminateBackgroundWorker(workers_pool[i].handle);

				recovery_single = *recovery_single_process = true;
				finish = -1;

				ereport(WARNING,
						(errcode(ERRCODE_CONFIGURATION_LIMIT_EXCEEDED),
						 errmsg("unable to start recovery workers"),
						 errdetail("You must increase max_worker_processes value or decrease orioledb.recovery_pool_size value.  Fallback to recovery in single-process mode.")));

				break;
			}
			state->queue = shm_mq_attach(GET_WORKER_QUEUE(i), NULL, workers_pool[i].handle);
			state->queue_buf_len = 0;
		}
		for (i = recovery_first_worker; i <= finish; i++)
		{
			if (shm_mq_wait_for_attach(workers_pool[i].queue) != SHM_MQ_SUCCESS)
				elog(ERROR, "unable to attach recovery workers to shm queue");
			recovery_send_init(i);
		}
	}

// if (enable_stopevents)
// {
// wait_for_stopevent_enabled(STOPEVENT_RECOVERY_START);
// STOPEVENT(STOPEVENT_RECOVERY_START, NULL);
// }

	recovery_undo_loc_flush->completedCheckpointNumber = 0;

	pg_write_barrier();

	recovery_init(-1);

	if (checkpoint_state->lastCheckpointNumber > 0)
		apply_xids_branches();
}


orioledb_redo(record: &mut XLogReaderState)
{
	Pointer		msg_start = (Pointer) XLogRecGetData(record);
	int			msg_len = XLogRecGetDataLen(record);
	pub static mut RECOVERY_SINGLE: bool = false;

	Assert((XLogRecGetInfo(record) & ~XLR_INFO_MASK) == ORIOLEDB_XLOG_CONTAINER);
	recovery_single = *recovery_single_process;

	if (unlikely(XLogRecPtrIsValid(replay_until_lsn)))
	{
		// Scoped to the lifetime of the Startup process.
		static mut NEEDS_INIT: bool = true;
		static mut IS_STOP_LSN_ACTIVE: bool = true;
		static mut SKIP_ALL_FUTURE_RECORDS: bool = false;

		// Short circuit: once the flag is set no further work is required
		if (skip_all_future_records)
		{
			elog(DEBUG4, "OrioleDB recovery skips WAL container [%X/%X-%X/%X]",
				 LSN_FORMAT_ARGS(record->ReadRecPtr),
				 LSN_FORMAT_ARGS(record->ReadRecPtr + msg_len));
			return;
		}

		// On the first pass we perform all the necessary sanity checks
		if (unlikely(needs_init))
		{
			needs_init = false;

			if (replay_until_lsn <= checkpoint_state->replayStartPtr)
			{
				is_stop_lsn_active = false;
				ereport(WARNING,
						(errmsg("value for orioledb.replay_until_lsn (%X/%X) "
								"is in the past",
								LSN_FORMAT_ARGS(replay_until_lsn)),
						 errdetail("The last checkpoint redo LSN is %X/%X. The "
								   "orioledb.replay_until_lsn setting will be "
								   "ignored.",
								   LSN_FORMAT_ARGS(checkpoint_state->replayStartPtr)),
						 errhint("Unset the orioledb.replay_until_lsn "
								 "parameter to prevent this warning.")));
			}
		}

		if (unlikely(is_stop_lsn_active &&
					 record->ReadRecPtr >= replay_until_lsn))
		{
			ereport(WARNING,
					(errmsg("OrioleDB recovery has reached LSN %X/%X. "
							"All future OrioleDB transactions will not be "
							"replayed",
							LSN_FORMAT_ARGS(record->ReadRecPtr)),
					 errdetail("orioledb.replay_until_lsn is %X/%X",
							   LSN_FORMAT_ARGS(replay_until_lsn)),
					 errhint("Unset the orioledb.replay_until_lsn parameter to"
							 " prevent warnings on subsequent startups.")));
			skip_all_future_records = true;
			return;
		}

	}

	if (record->ReadRecPtr >= checkpoint_state->controlToastConsistentPtr && !toast_consistent)
	{
		//
// Before running the PK->SK fix-up pass we need every pre-toast WAL
// record to have been applied to PK by the workers, otherwise the PK
// state we inspect below is stale.  Drain the worker queues up to the
// current point, then process the pending fix-ups, then notify
// workers to start applying records on the post-toast (whole-table)
// path.
//
		if (!recovery_single)
			workers_synchronize(record->ReadRecPtr, true);

		apply_pending_sk_fixups();

		toast_consistent = true;
		if (!recovery_single)
			workers_notify_toast_consistent();
	}

	if (record->ReadRecPtr >= checkpoint_state->controlReplayStartPtr)
	{
		if (!replay_container(msg_start, msg_start + msg_len, recovery_single,
							  record->ReadRecPtr, record->EndRecPtr))
		{
			abort_recovery(workers_pool, false);
			elog(ERROR, "orioledb recovery worker failed to replay WAL container.");
		}
	}

	if (unexpected_worker_detach)
	{
		abort_recovery(workers_pool, false);
		elog(ERROR, "orioledb recovery worker detached unexpectedly.");
	}
}


o_recovery_finish_hook(bool cleanup)
{
	pub static mut RECOVERY_WORKER_STATE: *mut state = std::ptr::null_mut();
	int			i,
				num_workers = recovery_idx_pool_size_guc ? recovery_pool_size_guc + 1 : recovery_pool_size_guc;
	pub static mut RECOVERY_SINGLE: bool = false;

	recovery_single = *recovery_single_process;

	if (!recovery_single)
	{
		workers_send_finish(false);
		for (i = 0; i < num_workers; i++)
		{
			worker_wait_shutdown(&workers_pool[i]);
		}
	}

	update_proc_retain_undo_location(-1);
	recovery_finish(-1);

	if (!recovery_single)
	{
		for (i = 0; i < num_workers; i++)
		{
			state = &workers_pool[i];
			shm_mq_detach(state->queue);
		}
		pfree(workers_pool);
	}

	// Release all the locks.  All of them are acquired at statement-level.
	LockReleaseCurrentOwner(NULL, 0);

	//
// No sense to check recovery_internal_error state, because shm_mq_sendv()
// can return SHM_MQ_DETACHED even if finish message was successfully
// sent.
//
	if (!recovery_single && pg_atomic_read_u32(worker_finish_count) != num_workers)
	{
		elog(ERROR, "orioledb recovery worker died.");
	}

	if (cleanup && remove_old_checkpoint_files)
		recovery_cleanup_old_files(startup_chkp_num, false);

	elog(LOG, "orioledb recovery finished.");
	recovery_undo_loc_flush->completedCheckpointNumber = UINT32_MAX;
}

static XLogRecPtr
get_workers_commit_ptr()
{
	static mut PREV_PTR: CommitSeqNo = InvalidXLogRecPtr;
	static mut PREV_CHANGES: uint64 = UINT64_MAX;
	pub static mut OLD_CHANGES: uint64 = std::mem::zeroed();

	// fast check - nothing changed
	old_changes = pg_atomic_read_u32(worker_ptrs_changes);
	if (old_changes == prev_changes)
		pub static mut PREV_PTR: return = std::mem::zeroed();

	pg_read_barrier();

	// we need to find a new ptr
	while (true)
	{
		pub static mut MIN_PTR: XLogRecPtr = std::mem::zeroed();
		pub static mut NEW_CHANGES: uint64 = std::mem::zeroed();
		pub static mut I: std::os::raw::c_int = 0;

		min_ptr = pg_atomic_read_u64(&worker_ptrs[0].commitPtr);
		for (i = 1; i < recovery_pool_size_guc; i++)
			min_ptr = Min(min_ptr, pg_atomic_read_u64(&worker_ptrs[i].commitPtr));

		pg_read_barrier();

		new_changes = pg_atomic_read_u32(worker_ptrs_changes);
		if (old_changes != new_changes)
		{
			old_changes = new_changes;
			pg_read_barrier();
			continue;
		}

		prev_changes = new_changes;
		prev_ptr = min_ptr;
		pub static mut PREV_PTR: return = std::mem::zeroed();
	}
}

//
// Returns minimum ptr which is already reached by all recovery workers.
//
static XLogRecPtr
recovery_get_current_ptr()
{
	Assert(RecoveryInProgress());

	// fast check - single process recovery
	if (*recovery_single_process)
		return pg_atomic_read_u64(recovery_ptr);

	return get_workers_commit_ptr();
}

XLogRecPtr
recovery_get_effective_replay_ptr()
{
	XLogRecPtr	ptr,
				finishedPtr;

	if (!RecoveryInProgress() || *recovery_single_process)
		pub static mut INVALID_X_LOG_REC_PTR: return = std::mem::zeroed();

	ptr = pg_atomic_read_u64(recovery_ptr);
	finishedPtr = pg_atomic_read_u64(recovery_finished_list_ptr);
	if (ptr == finishedPtr)
		pub static mut INVALID_X_LOG_REC_PTR: return = std::mem::zeroed();
	else
		pub static mut FINISHED_PTR: return = std::mem::zeroed();
}

static WalParseResult
recovery_check_version(const r: &mut WalReaderState)
{
	Assert(r);

	if (r->container.version > ORIOLEDB_WAL_VERSION)
	{
		// Unexpected new WAL record which we cannot read
		elog(PANIC, "cannot read WAL record of version %u newer than supported %u",
			 r->container.version, ORIOLEDB_WAL_VERSION);

		pub static mut WALPARSE_BAD_VERSION: return = std::mem::zeroed();
	}

	//
// If the WAL record is too old just return false and decide not to stop
// applying WAL records further.
//
	else if (r->container.version < ORIOLEDB_CONTAINER_FLAGS_WAL_VERSION)
		pub static mut WALPARSE_BAD_VERSION: return = std::mem::zeroed();

	pub static mut WALPARSE_OK: return = std::mem::zeroed();
}

static WalParseResult
recovery_on_container(r: &mut WalReaderState)
{
	if (r->container.flags & WAL_CONTAINER_HAS_XACT_INFO)
		pub static mut WALPARSE_STOP: return = std::mem::zeroed();

	return WALPARSE_EOF;		// Stop parser
}

static WalParseResult
recovery_on_record(r: &mut WalReaderState, rec: &mut WalRecord)
{
	return WALPARSE_EOF;		// Stop parser
}

//
// This function is called by the RecoveryStopsHook. It decides whether we want
// to stop applying WAL records.
//
// Returns true if we are stopping, false otherwise.
//
bool
orioledb_recovery_stops_before_hook(record: &mut XLogReaderState,
									recordXid: &mut TransactionId,
									recordXtime: &mut TimestampTz)
{
	Pointer		startPtr = (Pointer) XLogRecGetData(record);
	Pointer		endPtr = startPtr + XLogRecGetDataLen(record);

	pub static mut ST: WalParseResult = std::mem::zeroed();

	WalReaderState r = {
		.start = startPtr,
		.end = endPtr,
		.ptr = startPtr,
		.container = {0},
		.ctx = NULL,
		.check_version = recovery_check_version,
		.on_container = recovery_on_container,
		.on_record = recovery_on_record
	};

	// Currently we consider ony recovery_target_time
	if (recoveryTarget != RECOVERY_TARGET_TIME)
		pub static mut FALSE: return = std::mem::zeroed();

	// If for some reason data is empty just exit
	if (XLogRecGetDataLen(record) == 0)
		pub static mut FALSE: return = std::mem::zeroed();

	st = wal_parse_container(&r, true);

	if (st == WALPARSE_STOP)	// WAL_CONTAINER_HAS_XACT_INFO is present
	{
		Assert(r.container.flags & WAL_CONTAINER_HAS_XACT_INFO);

		*recordXid = r.container.xact_info.xid;
		*recordXtime = r.container.xact_info.xactTime;

		if (recoveryTargetInclusive)
			return r.container.xact_info.xactTime > recoveryTargetTime;
		else
			return r.container.xact_info.xactTime >= recoveryTargetTime;
	}

	pub static mut FALSE: return = std::mem::zeroed();
}

static XLogRecPtr
recovery_get_retain_ptr()
{
	// fast check - single process recovery
	if (*recovery_single_process)
	{
		return pg_atomic_read_u64(recovery_ptr);
	}

	// we need to find a new ptr
	while (true)
	{
		pub static mut RESULT: XLogRecPtr = std::mem::zeroed();
		pub static mut I: std::os::raw::c_int = 0;

		result = pg_atomic_read_u64(recovery_main_retain_ptr);
		for (i = 0; i < recovery_pool_size_guc; i++)
			result = Min(result, pg_atomic_read_u64(&worker_ptrs[i].retainPtr));

		pub static mut RESULT: return = std::mem::zeroed();
	}
}

//
// Returns true if current process is recovery process.
//
bool
is_recovery_process()
{
	pub static mut IAM_RECOVERY: return = std::mem::zeroed();
}

CommitSeqNo
recovery_map_oxid_csn(OXid oxid, found: &mut bool)
{
	pub static mut RECOVERY_XID_STATE: *mut state = std::ptr::null_mut();

	state = hash_search(recovery_xid_state_hash, &oxid, HASH_FIND, found);
	if (*found)
	{
		if (!state->wal_xid)
			pub static mut COMMITSEQNO_ABORTED: return = std::mem::zeroed();
		return state->csn;
	}
	pub static mut 0: return = std::mem::zeroed();
}

//
// Initializes a new recovery process, recovery transaction support.
//

recovery_init(int worker_id)
{
	pub static mut CTL: HASHCTL = std::mem::zeroed();
	pub static mut RECOVERY_WORKER_STATE: *mut state = std::ptr::null_mut();
	pub static mut I: std::os::raw::c_int = 0;

	MemSet(&ctl, 0, sizeof(ctl));
	ctl.keysize = sizeof(OXid);
	ctl.entrysize = sizeof(RecoveryXidState);
	ctl.hcxt = TopMemoryContext;
	recovery_xid_state_hash = hash_create("orioledb recovery xid state hash",
										  16, &ctl,
										  HASH_ELEM | HASH_BLOBS | HASH_CONTEXT);
	iam_recovery = true;
	for (i = 0; i < (int) UndoLogsCount; i++)
	{
		retain_undo_queue_numbers[i] = i;
		retain_undo_queues[i] = pairingheap_allocate(retain_undo_pairingheap_cmp,
													 &retain_undo_queue_numbers[i]);
	}

	//
// Only the recovery leader maintains the runXmin horizon via xmin_queue;
// recovery workers never touch it, so leave it NULL for them.
//
	if (worker_id < 0)
		xmin_queue = pairingheap_allocate(xmin_pairingheap_cmp, NULL);
	dlist_init(&finished_list);
	dlist_init(&joint_commit_list);
	CurTransactionContext = AllocSetContextCreate(TopMemoryContext,
												  "orioledb recovery current transaction context",
												  ALLOCSET_DEFAULT_SIZES);
	TopTransactionContext = AllocSetContextCreate(TopMemoryContext,
												  "orioledb recovery top transaction context",
												  ALLOCSET_DEFAULT_SIZES);
	RelationCacheInitialize();	// needed for OTableDescr invalidation
	InitCatalogCache();

	o_set_syscache_hooks();

	//
// Seed recovery_xmin with the checkpoint-era before: &mut floor* read_xids()
// runs its first update_run_xmin().  read_xids() pushes runXmin to
// nextXid when the on-disk xids file is empty -- which it routinely is on
// a streaming standby whose master had only long-running oxids with
// modify records buffered in the master backend's private local_wal
// (never reaching the wire, hence absent from the standby's recovery xid
// hash and its own restartpoint's xids file).  With recovery_xmin left at
// InvalidOXid (effectively unbounded), update_run_xmin's Min(...) cap is
// ineffective and runXmin / globalXmin sail past the real master floor; a
// later WAL_REC_XID(X) + WAL_REC_ROLLBACK(X) then drags globalXmin
// backwards.
//
// Pinning recovery_xmin to checkpointRetainXmin keeps the floor honest
// until a WAL commit/rollback record explicitly bumps it (see the Max()
// in WAL_REC_COMMIT/ROLLBACK / WAL_REC_JOINT_COMMIT).
//
	if (worker_id < 0)
		recovery_xmin = pg_atomic_read_u64(&xid_meta->checkpointRetainXmin);

	if (checkpoint_state->lastCheckpointNumber > 0)
		read_xids(checkpoint_state->lastCheckpointNumber,
				  *recovery_single_process,
				  worker_id);

	if (worker_id < 0)
	{
		pub static mut RELOID_CTL: HASHCTL = std::mem::zeroed();

		MemSet(&reloid_ctl, 0, sizeof(reloid_ctl));
		reloid_ctl.keysize = sizeof(ORelOids);
		reloid_ctl.entrysize = sizeof(RecoveryIdxBuildQueueState);
		reloid_ctl.hcxt = TopMemoryContext;
		idxbuild_oids_hash = hash_create("orioledb recovery index build queue relations hash",
										 16, &reloid_ctl,
										 HASH_ELEM | HASH_BLOBS | HASH_CONTEXT);
	}

	if (worker_id == index_build_leader)
	{
		workers_pool = palloc0(sizeof(RecoveryWorkerState) * (recovery_idx_pool_size_guc + recovery_pool_size_guc));

		for (i = index_build_first_worker; i <= index_build_last_worker; i++)
		{
			state = &workers_pool[i];
			shm_mq_set_sender(GET_WORKER_QUEUE(i), MyProc);
			state->type = oIndexInvalid;
			ORelOidsSetInvalid(state->oids);
			state->oxid = InvalidOXid;

			workers_pool[i].handle = recovery_worker_register(i);

			if (workers_pool[i].handle == NULL)
			{
				//
// Not enough slots for background workers.
//
				for (i--; i >= index_build_first_worker; i--)
					TerminateBackgroundWorker(workers_pool[i].handle);

				recovery_idx_pool_size_guc = 1;

				ereport(WARNING,
						(errcode(ERRCODE_CONFIGURATION_LIMIT_EXCEEDED),
						 errmsg("unable to start recovery workers"),
						 errdetail("You must increase max_worker_processes value or decrease orioledb.recovery_idx_pool_size value. Fallback to index build in single-process mode.")));
			}
			state->queue = shm_mq_attach(GET_WORKER_QUEUE(i), NULL, workers_pool[i].handle);
			state->queue_buf_len = 0;
		}

		for (i = index_build_first_worker; i <= index_build_last_worker; i++)
		{
			if (shm_mq_wait_for_attach(workers_pool[i].queue) != SHM_MQ_SUCCESS)
				elog(ERROR, "unable to attach recovery workers to shm queue");
			recovery_send_init(i);
		}
	}

	HandleStartupProcInterrupts_hook = o_handle_startup_proc_interrupts_hook;
}

fn
walk_checkpoint_stacks(recovery_xid_state: &mut RecoveryXidState, CommitSeqNo csn,
					   SubTransactionId parentSubid,
					   bool flushUndoPos)
{
	pub static mut MITER: dlist_mutable_iter = std::mem::zeroed();

	oxid_needs_wal_flush = recovery_xid_state->needs_wal_flush;
	recovery_oxid = recovery_xid_state->oxid;

	dlist_foreach_modify(miter, &recovery_xid_state->checkpoint_undo_stacks)
	{
		stack: &mut CheckpointUndoStack = dlist_container(CheckpointUndoStack,
													 node,
													 miter.cur);

		if ((int) stack->kind < (int) UndoLogsCount)
		{
			UndoLogType undoType = (UndoLogType) stack->kind;

			set_cur_undo_locations(undoType, stack->undoStack);
			if (flushUndoPos)
				flush_current_undo_stack();
			if (COMMITSEQNO_IS_ABORTED(csn))
			{
				if (parentSubid == InvalidSubTransactionId)
					apply_undo_stack(undoType, recovery_oxid,
									 NULL, false);
				else
					rollback_to_savepoint(undoType, UndoStackHead,
										  parentSubid, false);
			}
			else
			{
				precommit_undo_stack(undoType, recovery_oxid, false);
				on_commit_undo_stack(undoType, recovery_oxid, false);
			}
		}
		dlist_delete(miter.cur);
		pfree(stack);
	}
}

//
// Finishes a recovery process, close all recovery transactions.
//

recovery_finish(int worker_id)
{
	bool		flush_undo_pos = need_flush_undo_pos(worker_id);
	pub static mut RECOVERY_XID_STATE: *mut cur_state = std::ptr::null_mut();
	pub static mut HASH_SEQ: HASH_SEQ_STATUS = std::mem::zeroed();
	pub static mut I: std::os::raw::c_int = 0;

	delay_if_queued_for_idxbuild();

	if (cur_recovery_xid_state)
	{
		cur_recovery_xid_state->needs_wal_flush = oxid_needs_wal_flush;
		for (i = 0; i < (int) UndoLogsCount; i++)
		{
			get_cur_undo_locations(&cur_recovery_xid_state->undo_stacks[i],
								   (UndoLogType) i);
			cur_recovery_xid_state->retain_locs[i] = curRetainUndoLocations[i];
		}
		cur_recovery_xid_state = NULL;
	}

	hash_seq_init(&hash_seq, recovery_xid_state_hash);
	while ((cur_state = (RecoveryXidState *) hash_seq_search(&hash_seq)) != NULL)
	{
		if (cur_state->o_tables_meta_locked)
		{
			o_tables_meta_unlock_no_wal();
			cur_state->o_tables_meta_locked = false;
		}

		if (COMMITSEQNO_IS_INPROGRESS(cur_state->csn))
		{
			oxid_needs_wal_flush = cur_state->needs_wal_flush;
			recovery_oxid = cur_state->oxid;
			for (i = 0; i < (int) UndoLogsCount; i++)
				set_cur_undo_locations((UndoLogType) i, cur_state->undo_stacks[i]);
			if (flush_undo_pos)
				flush_current_undo_stack();
			for (i = 0; i < (int) UndoLogsCount; i++)
				apply_undo_stack((UndoLogType) i, recovery_oxid, NULL, true);
			walk_checkpoint_stacks(cur_state, COMMITSEQNO_ABORTED,
								   InvalidSubTransactionId,
								   flush_undo_pos);

			//
// Remember this oxid so the after-checkpoint hook can emit a
// WAL_REC_ROLLBACK for it once XLog inserts are allowed. Workers
// don't write WAL: only the main recovery process does. See issue
// #876.
//
			if (worker_id < 0)
			{
				if (cur_state->oxid >= recovery_xmin)
				{
					MemoryContext oldcxt = MemoryContextSwitchTo(TopMemoryContext);

					if (recovery_finish_aborted_count == recovery_finish_aborted_capacity)
					{
						int			new_cap = recovery_finish_aborted_capacity == 0
							? 16 : recovery_finish_aborted_capacity * 2;

						if (recovery_finish_aborted_oxids == NULL)
							recovery_finish_aborted_oxids =
								palloc(new_cap * sizeof(RecoveryFinishAbortedOxid));
						else
							recovery_finish_aborted_oxids =
								repalloc(recovery_finish_aborted_oxids,
										 new_cap * sizeof(RecoveryFinishAbortedOxid));
						recovery_finish_aborted_capacity = new_cap;
					}
					recovery_finish_aborted_oxids[recovery_finish_aborted_count].oxid =
						cur_state->oxid;
					recovery_finish_aborted_oxids[recovery_finish_aborted_count].xid =
						cur_state->xid;
					recovery_finish_aborted_count++;
					MemoryContextSwitchTo(oldcxt);
				}
				else
				{
					Assert(!cur_state->wal_xid);
				}
			}
		}
		if (cur_state->in_finished_list && COMMITSEQNO_IS_COMMITTED(cur_state->csn) && worker_id < 0)
		{
			set_oxid_csn(cur_state->oxid, COMMITSEQNO_COMMITTING);
			cur_state->csn = pg_atomic_fetch_add_u64(&TRANSAM_VARIABLES->nextCommitSeqNo, 1);
			set_oxid_csn(cur_state->oxid, cur_state->csn);
		}
		if (cur_state->used_by)
			pfree(cur_state->used_by);
	}
	HandleStartupProcInterrupts_hook = NULL;
	hash_destroy(recovery_xid_state_hash);
	recovery_xid_state_hash = NULL;

	if (worker_id < 0)
	{
		hash_destroy(idxbuild_oids_hash);
		idxbuild_oids_hash = NULL;
	}
	for (i = 0; i < (int) UndoLogsCount; i++)
	{
		release_undo_size((UndoLogType) i);
		free_retained_undo_location((UndoLogType) i);
		pairingheap_free(retain_undo_queues[i]);
	}
	if (worker_id < 0)
		pairingheap_free(xmin_queue);

	//
// Do NOT advance runXmin here.  Recovery has just aborted in-flight oxids
// in memory; if recovery_finish_aborted_oxids is non-empty, the
// after-checkpoint hook will emit a WAL_REC_ROLLBACK for each, stamping
// the record's xmin with the current runXmin.  If we advanced runXmin to
// nextXid here, the post-recovery checkpoint that runs between
// recovery_finish() and o_emit_recovery_finish_rollbacks() would persist
// the advanced horizon as control.checkpointRetainXmin, and the standby
// would replay that checkpoint before seeing the ROLLBACK records that
// justify it.  After the standby's globalXmin slid forward, the ROLLBACK
// records would then drag it back across slots already stamped FROZEN,
// breaking oxid_get_csn()'s fast-path (orioledb/orioledb#889).
// free_run_xmin() is deferred to o_emit_recovery_finish_rollbacks() so
// the WAL records and the runXmin advance are atomic with respect to
// checkpoint observers.
//
	if (worker_id >= 0)
		pg_atomic_write_u64(&worker_ptrs[worker_id].retainPtr,
							pg_atomic_read_u64(&worker_ptrs[worker_id].commitPtr));
	else
		pg_atomic_write_u64(recovery_main_retain_ptr,
							pg_atomic_read_u64(recovery_ptr));

	oxid_needs_wal_flush = false;
	recovery_oxid = InvalidOXid;
	reset_cur_undo_locations();
	MemoryContextDelete(CurTransactionContext);
	MemoryContextDelete(TopTransactionContext);
	TopTransactionContext = NULL;
	CurTransactionContext = NULL;
	iam_recovery = false;

	o_unset_syscache_hooks();
}

//
// Emit a WAL_REC_ROLLBACK for every oxid that recovery_finish() aborted in
// memory.  Called from the after_checkpoint_cleanup_hook at end of recovery,
// once LocalSetXLogInsertAllowed() has run so XLogInsert is permitted.
//
// Without this, streaming standbys that eagerly applied the in-flight txn's
// modify records hold the oxid INPROGRESS forever, and any later replayed
// modify targeting the same row spins in o_btree_modify_handle_conflicts
// (issue #876).
//

o_emit_recovery_finish_rollbacks()
{
	pub static mut I: std::os::raw::c_int = 0;

	for (i = 0; i < recovery_finish_aborted_count; i++)
	{
		elog(LOG, "orioledb: emitting WAL_REC_ROLLBACK for in-flight oxid " UINT64_FORMAT " aborted by recovery_finish",
			 recovery_finish_aborted_oxids[i].oxid);
		wal_emit_recovery_finish_rollback(recovery_finish_aborted_oxids[i].oxid,
										  recovery_finish_aborted_oxids[i].xid);
	}

	if (recovery_finish_aborted_oxids != NULL)
	{
		pfree(recovery_finish_aborted_oxids);
		recovery_finish_aborted_oxids = NULL;
		recovery_finish_aborted_count = 0;
		recovery_finish_aborted_capacity = 0;
	}

	//
// Now that every WAL_REC_ROLLBACK has been stamped with the
// checkpoint-era runXmin, it is safe to lift the horizon to nextXid.
// Deferred from recovery_finish() so the post-recovery checkpoint (which
// sits between the two phases) observes the original floor and standbys
// never see a checkpointRetainXmin that gets out from under the ROLLBACK
// records that explain it (orioledb/orioledb#889).
//
	free_run_xmin();
}

//
// Switches recovery process to other orioledb transaction.
//

recovery_switch_to_oxid(OXid oxid, int worker_id)
{
	pub static mut I: std::os::raw::c_int = 0;

	if (recovery_oxid != oxid)
	{
		pub static mut RECOVERY_XID_STATE: *mut cur_state = cur_recovery_xid_state;
		pub static mut FOUND: bool = false;

		if (cur_state)
		{
			cur_state->needs_wal_flush = oxid_needs_wal_flush;
			for (i = 0; i < (int) UndoLogsCount; i++)
			{
				get_cur_undo_locations(&cur_state->undo_stacks[i],
									   (UndoLogType) i);

				if (!UndoLocationIsValid(cur_state->retain_locs[i]) &&
					UndoLocationIsValid(curRetainUndoLocations[i]))
				{
					cur_state->retain_locs[i] = curRetainUndoLocations[i];
					Assert(!cur_recovery_xid_state->in_retain_undo_heaps[i]);
					cur_state->in_retain_undo_heaps[i] = true;
					pairingheap_add(retain_undo_queues[i], &cur_state->retain_undo_ph_nodes[i]);
				}
			}
		}

		recovery_oxid = oxid;
		cur_state = (RecoveryXidState *) hash_search(recovery_xid_state_hash,
													 &oxid,
													 HASH_ENTER,
													 &found);
		cur_state->wal_xid = true;

		if (found)
		{
			oxid_needs_wal_flush = cur_state->needs_wal_flush;
			for (i = 0; i < (int) UndoLogsCount; i++)
			{
				set_cur_undo_locations((UndoLogType) i, cur_state->undo_stacks[i]);
				curRetainUndoLocations[i] = cur_state->retain_locs[i];
			}
		}
		else
		{
			cur_state->xid = InvalidTransactionId;
			for (i = 0; i < (int) UndoLogsCount; i++)
			{
				cur_state->retain_locs[i] = InvalidUndoLocation;
				cur_state->in_retain_undo_heaps[i] = false;
			}
			cur_state->csn = COMMITSEQNO_INPROGRESS;
			cur_state->ptr = InvalidXLogRecPtr;
			cur_state->needs_wal_flush = false;
			cur_state->in_finished_list = false;
			cur_state->in_joint_commit_list = false;
			cur_state->needs_feedback = false;

			//
// undo_stacks might be copied into a temp file, so initialize it
// with InvalidUndoLocation.
//
			memset(cur_state->undo_stacks, 0, sizeof(cur_state->undo_stacks));
			for (i = 0; i < (int) UndoLogsCount; i++)
				undo_stack_locations_set_invalid(&cur_state->undo_stacks[i]);

			dlist_init(&cur_state->checkpoint_undo_stacks);
			oxid_needs_wal_flush = false;
			reset_cur_undo_locations();
			for (i = 0; i < (int) UndoLogsCount; i++)
				curRetainUndoLocations[i] = InvalidUndoLocation;
			if (worker_id < 0)
				pairingheap_add(xmin_queue, &cur_state->xmin_ph_node);
			cur_state->systree_modified = false;
			cur_state->invalidate_typcache = false;
			cur_state->o_tables_meta_locked = false;
			cur_state->checkpoint_xid = false;
			if (worker_id < 0 && !*recovery_single_process)
				cur_state->used_by = palloc0((recovery_pool_size_guc + recovery_idx_pool_size_guc) *
											 sizeof(bool));
			else
				cur_state->used_by = NULL;
		}

		cur_recovery_xid_state = cur_state;
		update_proc_retain_undo_location(worker_id);
	}
}

//
// Delete recovery xid item if it's already deleted from both retain undo
// location heap and finished list.
//
fn
check_delete_xid_state(state: &mut RecoveryXidState, int worker_id)
{
	pub static mut I: std::os::raw::c_int = 0;
	pub static mut IN_RETAIN_HEAPS: bool = false;

	for (i = 0; i < (int) UndoLogsCount; i++)
		if (state->in_retain_undo_heaps[i])
			in_retain_heaps = true;

	if (!in_retain_heaps &&
		!state->in_finished_list &&
		!state->in_joint_commit_list)
	{
		pub static mut OXID: OXid = state->oxid;
		pub static mut FOUND: bool = false;

		if (state->used_by)
			pfree(state->used_by);
		if (worker_id < 0)
		{
			pairingheap_remove(xmin_queue, &state->xmin_ph_node);
			update_run_xmin();
		}
		hash_search(recovery_xid_state_hash, &oxid, HASH_REMOVE, &found);
		Assert(found);
	}
}

static bool
need_flush_undo_pos(int worker_id)
{
	if (worker_id < 0)
	{
		return recovery_undo_loc_flush->recoveryMainCompletedCheckpointNumber <
			recovery_undo_loc_flush->finishRequestCheckpointNumber;
	}
	else
	{
		return worker_ptrs[worker_id].flushedUndoLocCompletedCheckpointNumber <
			recovery_undo_loc_flush->finishRequestCheckpointNumber;
	}
}

fn
flush_current_undo_stack()
{
	pub static mut REC: XidFileRec = std::mem::zeroed();
	pub static mut I: std::os::raw::c_int = 0;

	rec.oxid = recovery_oxid;
	for (i = 0; i < (int) UndoLogsCount; i++)
	{
		rec.kind = (XidRecKind) i;
		get_cur_undo_locations(&rec.undoLocation, (UndoLogType) i);
		rec.retainLocation = curRetainUndoLocations[i];
		write_to_xids_queue(&rec);
	}
}

//
// Finishes the current recovery transaction for the current recovery process.
//

recovery_finish_current_oxid(CommitSeqNo csn, XLogRecPtr ptr,
							 int worker_id, bool sync)
{
	pub static mut OXID: OXid = recovery_oxid;
	bool		flush_undo_pos = need_flush_undo_pos(worker_id);
	pub static mut I: std::os::raw::c_int = 0;

	Assert(cur_recovery_xid_state != NULL);

	delay_if_queued_for_idxbuild();

	if (!COMMITSEQNO_IS_ABORTED(csn) && sync)
	{
		Assert(worker_id < 0);
		set_oxid_csn(oxid, COMMITSEQNO_COMMITTING);
		if (flush_undo_pos)
			flush_current_undo_stack();
		for (i = 0; i < (int) UndoLogsCount; i++)
			precommit_undo_stack((UndoLogType) i, oxid, true);
		for (i = 0; i < (int) UndoLogsCount; i++)
			on_commit_undo_stack((UndoLogType) i, oxid, true);
		walk_checkpoint_stacks(cur_recovery_xid_state, csn,
							   InvalidSubTransactionId, flush_undo_pos);
		csn = pg_atomic_fetch_add_u64(&TRANSAM_VARIABLES->nextCommitSeqNo, 1);
		set_oxid_csn(oxid, csn);
		set_oxid_xlog_ptr(oxid, XLOG_PTR_ALIGN(ptr));
	}
	else if (!COMMITSEQNO_IS_ABORTED(csn) && !sync)
	{
		if (flush_undo_pos)
			flush_current_undo_stack();
		for (i = 0; i < (int) UndoLogsCount; i++)
			precommit_undo_stack((UndoLogType) i, oxid, true);
		for (i = 0; i < (int) UndoLogsCount; i++)
			on_commit_undo_stack((UndoLogType) i, oxid, true);
		walk_checkpoint_stacks(cur_recovery_xid_state, csn,
							   InvalidSubTransactionId, flush_undo_pos);
		cur_recovery_xid_state->in_finished_list = true;
		dlist_push_tail(&finished_list,
						&cur_recovery_xid_state->finished_list_node);
	}
	else
	{
		if (flush_undo_pos)
			flush_current_undo_stack();
		for (i = 0; i < (int) UndoLogsCount; i++)
			apply_undo_stack((UndoLogType) i, oxid, NULL, true);
		walk_checkpoint_stacks(cur_recovery_xid_state, csn,
							   InvalidSubTransactionId, flush_undo_pos);
		if (worker_id < 0)
		{
			if (sync)
			{
				set_oxid_csn(oxid, COMMITSEQNO_ABORTED);
				set_oxid_xlog_ptr(oxid, InvalidXLogRecPtr);
			}
			else
			{
				//
// Postpone transaction abort until it will be aborted by all
// the workers.  Otherwise, workers can consider it as
// committed due to runXmin.
//
				cur_recovery_xid_state->in_finished_list = true;
				dlist_push_tail(&finished_list,
								&cur_recovery_xid_state->finished_list_node);
			}
		}
	}

	cur_recovery_xid_state->csn = csn;
	cur_recovery_xid_state->ptr = ptr;

	if (cur_recovery_xid_state->o_tables_meta_locked)
	{
		o_tables_meta_unlock_no_wal();
		cur_recovery_xid_state->o_tables_meta_locked = false;
	}

	oxid_needs_wal_flush = false;
	reset_cur_undo_locations();
	recovery_oxid = InvalidOXid;

	for (i = 0; i < (int) UndoLogsCount; i++)
	{
		if (!UndoLocationIsValid(cur_recovery_xid_state->retain_locs[i]) &&
			UndoLocationIsValid(curRetainUndoLocations[i]))
		{
			cur_recovery_xid_state->retain_locs[i] = curRetainUndoLocations[i];
			pairingheap_add(retain_undo_queues[i],
							&cur_recovery_xid_state->retain_undo_ph_nodes[i]);
			cur_recovery_xid_state->in_retain_undo_heaps[i] = true;
		}
		curRetainUndoLocations[i] = InvalidUndoLocation;
	}

	for (i = 0; i < (int) UndoLogsCount; i++)
		release_undo_size((UndoLogType) i);
	check_delete_xid_state(cur_recovery_xid_state, worker_id);

	cur_recovery_xid_state = NULL;

	update_proc_retain_undo_location(worker_id);
}

fn
checkpoint_rollback_to_savepoint(SubTransactionId parentSubid)
{
	pub static mut I: std::os::raw::c_int = 0;

	for (i = 0; i < (int) UndoLogsCount; i++)
		get_cur_undo_locations(&cur_recovery_xid_state->undo_stacks[i],
							   (UndoLogType) i);
	walk_checkpoint_stacks(cur_recovery_xid_state, COMMITSEQNO_ABORTED,
						   parentSubid, false);
	for (i = 0; i < (int) UndoLogsCount; i++)
		set_cur_undo_locations((UndoLogType) i,
							   cur_recovery_xid_state->undo_stacks[i]);
}


recovery_savepoint(SubTransactionId parentSubid, int worker_id)
{
	if (worker_id == -1)
		checkpoint_rollback_to_savepoint(parentSubid);

	add_subxact_undo_item(parentSubid);
}


recovery_rollback_to_savepoint(SubTransactionId parentSubid, int worker_id)
{
	pub static mut I: std::os::raw::c_int = 0;

	for (i = 0; i < (int) UndoLogsCount; i++)
		rollback_to_savepoint((UndoLogType) i, UndoStackTail,
							  parentSubid, true);

	if (worker_id == -1)
		checkpoint_rollback_to_savepoint(parentSubid);
}

OBTreeModifyCallbackAction
recovery_insert_primary_callback(descr: &mut BTreeDescr,
								 OTuple tup, newtup: &mut OTuple, OXid oxid,
								 OTupleXactInfo xactInfo,
								 UndoLocation location, lock_mode: &mut RowLockMode,
								 hint: &mut BTreeLocationHint,  *arg)
{
	if (XACT_INFO_OXID_EQ(xactInfo, oxid) &&
		o_tuple_get_version(tup) >= o_tuple_get_version(*newtup))
		pub static mut OB_TREE_CALLBACK_ACTION_UNDO: return = std::mem::zeroed();
	pub static mut OB_TREE_CALLBACK_ACTION_UPDATE: return = std::mem::zeroed();
}

static OBTreeModifyCallbackAction
recovery_delete_primary_callback(descr: &mut BTreeDescr,
								 OTuple tup, newtup: &mut OTuple, OXid oxid,
								 OTupleXactInfo xactInfo,
								 UndoLocation location,
								 lock_mode: &mut RowLockMode,
								 hint: &mut BTreeLocationHint,  *arg)
{
	key: &mut OTuple = (OTuple *) arg;

	if (XACT_INFO_OXID_EQ(xactInfo, oxid) &&
		o_tuple_get_version(tup) > o_tuple_get_version(*key))
		pub static mut OB_TREE_CALLBACK_ACTION_UNDO: return = std::mem::zeroed();

	pub static mut OB_TREE_CALLBACK_ACTION_DELETE: return = std::mem::zeroed();
}

OBTreeModifyCallbackAction
recovery_insert_overwrite_callback(descr: &mut BTreeDescr,
								   OTuple tup, newtup: &mut OTuple, OXid oxid,
								   OTupleXactInfo xactInfo,
								   UndoLocation location,
								   lock_mode: &mut RowLockMode,
								   hint: &mut BTreeLocationHint,  *arg)
{
	if (XACT_INFO_OXID_EQ(xactInfo, oxid))
		pub static mut OB_TREE_CALLBACK_ACTION_UNDO: return = std::mem::zeroed();

	pub static mut OB_TREE_CALLBACK_ACTION_UPDATE: return = std::mem::zeroed();
}

OBTreeModifyCallbackAction
recovery_delete_overwrite_callback(descr: &mut BTreeDescr,
								   OTuple tup, newtup: &mut OTuple, OXid oxid,
								   OTupleXactInfo xactInfo,
								   UndoLocation location,
								   lock_mode: &mut RowLockMode,
								   hint: &mut BTreeLocationHint,  *arg)
{
	if (XACT_INFO_OXID_EQ(xactInfo, oxid))
		pub static mut OB_TREE_CALLBACK_ACTION_UNDO: return = std::mem::zeroed();

	pub static mut OB_TREE_CALLBACK_ACTION_DELETE: return = std::mem::zeroed();
}

static OBTreeModifyCallbackAction
recovery_insert_systree_callback(descr: &mut BTreeDescr,
								 OTuple tup, newtup: &mut OTuple, OXid oxid,
								 OTupleXactInfo xactInfo,
								 UndoLocation location, lock_mode: &mut RowLockMode,
								 hint: &mut BTreeLocationHint,  *arg)
{
	pub static mut OB_TREE_CALLBACK_ACTION_UPDATE: return = std::mem::zeroed();
}

OBTreeModifyCallbackAction
recovery_insert_deleted_primary_callback(descr: &mut BTreeDescr,
										 OTuple tup, newtup: &mut OTuple, OXid oxid,
										 OTupleXactInfo xactInfo,
										 BTreeLeafTupleDeletedStatus deleted,
										 UndoLocation location, lock_mode: &mut RowLockMode,
										 hint: &mut BTreeLocationHint,  *arg)
{
	if (XACT_INFO_OXID_EQ(xactInfo, oxid) &&
		o_tuple_get_version(tup) >= o_tuple_get_version(*newtup))
		pub static mut OB_TREE_CALLBACK_ACTION_UNDO: return = std::mem::zeroed();
	pub static mut OB_TREE_CALLBACK_ACTION_UPDATE: return = std::mem::zeroed();
}

static OBTreeModifyCallbackAction
recovery_delete_deleted_primary_callback(descr: &mut BTreeDescr,
										 OTuple tup, newtup: &mut OTuple, OXid oxid,
										 OTupleXactInfo xactInfo,
										 BTreeLeafTupleDeletedStatus deleted,
										 UndoLocation location,
										 lock_mode: &mut RowLockMode,
										 hint: &mut BTreeLocationHint,  *arg)
{
	key: &mut OTuple = (OTuple *) arg;

	if (XACT_INFO_OXID_EQ(xactInfo, oxid) &&
		o_tuple_get_version(tup) > o_tuple_get_version(*key))
		pub static mut OB_TREE_CALLBACK_ACTION_UNDO: return = std::mem::zeroed();

	pub static mut OB_TREE_CALLBACK_ACTION_DELETE: return = std::mem::zeroed();
}

OBTreeModifyCallbackAction
recovery_insert_deleted_overwrite_callback(descr: &mut BTreeDescr,
										   OTuple tup, newtup: &mut OTuple, OXid oxid,
										   OTupleXactInfo xactInfo,
										   BTreeLeafTupleDeletedStatus deleted,
										   UndoLocation location,
										   lock_mode: &mut RowLockMode,
										   hint: &mut BTreeLocationHint,  *arg)
{
	if (XACT_INFO_OXID_EQ(xactInfo, oxid))
		pub static mut OB_TREE_CALLBACK_ACTION_UNDO: return = std::mem::zeroed();

	pub static mut OB_TREE_CALLBACK_ACTION_UPDATE: return = std::mem::zeroed();
}

static OBTreeModifyCallbackAction
recovery_delete_deleted_overwrite_callback(descr: &mut BTreeDescr,
										   OTuple tup, newtup: &mut OTuple, OXid oxid,
										   OTupleXactInfo xactInfo,
										   BTreeLeafTupleDeletedStatus deleted,
										   UndoLocation location,
										   lock_mode: &mut RowLockMode,
										   hint: &mut BTreeLocationHint,  *arg)
{
	if (XACT_INFO_OXID_EQ(xactInfo, oxid))
		pub static mut OB_TREE_CALLBACK_ACTION_UNDO: return = std::mem::zeroed();

	pub static mut OB_TREE_CALLBACK_ACTION_DELETE: return = std::mem::zeroed();
}

static OBTreeModifyCallbackAction
recovery_insert_deleted_systree_callback(descr: &mut BTreeDescr,
										 OTuple tup, newtup: &mut OTuple, OXid oxid,
										 OTupleXactInfo xactInfo,
										 BTreeLeafTupleDeletedStatus deleted,
										 UndoLocation location, lock_mode: &mut RowLockMode,
										 hint: &mut BTreeLocationHint,  *arg)
{
	pub static mut OB_TREE_CALLBACK_ACTION_UPDATE: return = std::mem::zeroed();
}

//
// Applies modify recovery record to the BTree.
//
bool
apply_btree_modify_record(tree: &mut BTreeDescr, RecoveryMsgType type,
						  OTuple ptr, OXid oxid, CommitSeqNo csn)
{
	pub static mut MODIFY_RESULT: OBTreeModifyResult = std::mem::zeroed();
	pub static mut CALLBACK_INFO: BTreeModifyCallbackInfo = nullCallbackInfo;
	pub static mut RESULT: bool = false;

	callbackInfo.arg = &ptr;

	if (IS_SYS_TREE_OIDS(tree->oids))
	{
		if (type == RecoveryMsgTypeInsert || type == RecoveryMsgTypeUpdate)
		{
			callbackInfo.modifyCallback = recovery_insert_systree_callback;
			callbackInfo.modifyDeletedCallback = recovery_insert_deleted_systree_callback;
		}
	}
	else if (tree->type == oIndexPrimary || tree->type == oIndexToast || tree->type == oIndexBridge)
	{
		if (type == RecoveryMsgTypeInsert || type == RecoveryMsgTypeUpdate)
		{
			callbackInfo.modifyCallback = recovery_insert_primary_callback;
			callbackInfo.modifyDeletedCallback = recovery_insert_deleted_primary_callback;
		}
		else if (type == RecoveryMsgTypeDelete)
		{
			callbackInfo.modifyCallback = recovery_delete_primary_callback;
			callbackInfo.modifyDeletedCallback = recovery_delete_deleted_primary_callback;
		}
	}
	else
	{
		if (type == RecoveryMsgTypeInsert || type == RecoveryMsgTypeUpdate)
		{
			callbackInfo.modifyCallback = recovery_insert_overwrite_callback;
			callbackInfo.modifyDeletedCallback = recovery_insert_deleted_overwrite_callback;
		}
		else if (type == RecoveryMsgTypeDelete)
		{
			callbackInfo.modifyCallback = recovery_delete_overwrite_callback;
			callbackInfo.modifyDeletedCallback = recovery_delete_deleted_overwrite_callback;
		}
	}

	switch (type)
	{
		case RecoveryMsgTypeInsert:
			modifyResult = o_btree_modify(tree, BTreeOperationInsert,
										  ptr, BTreeKeyLeafTuple,
										  NULL, BTreeKeyNone,
										  oxid, csn, RowLockUpdate,
										  NULL, &callbackInfo);
			result = modifyResult == OBTreeModifyResultInserted || modifyResult == OBTreeModifyResultUpdated;
			break;
		case RecoveryMsgTypeUpdate:
			result = o_btree_modify(tree, BTreeOperationInsert,
									ptr, BTreeKeyLeafTuple,
									NULL, BTreeKeyNone,
									oxid, csn, RowLockNoKeyUpdate,
									NULL, &callbackInfo) == OBTreeModifyResultUpdated;
			break;
		case RecoveryMsgTypeDelete:
			result = o_btree_modify(tree, BTreeOperationDelete,
									ptr, BTreeKeyNonLeafKey,
									NULL, BTreeKeyNone, oxid, csn, RowLockUpdate,
									NULL, &callbackInfo) == OBTreeModifyResultDeleted;
			break;
		default:
			Assert(false);
			elog(ERROR, "Wrong recovery record type %d", type);
	}

	pub static mut RESULT: return = std::mem::zeroed();
}


replay_erase_bridge_item(bridge: &mut OIndexDescr, ItemPointer iptr)
{
	pub static mut CONTEXT: OBTreeFindPageContext = std::mem::zeroed();
	pub static mut BOUND: OBTreeKeyBound = std::mem::zeroed();
	pub static mut O_BTREE_PAGE_FIND_ITEM: *mut item = std::ptr::null_mut();
	pub static mut PG_USED_FOR_ASSERTS_ONLY: OFindPageResult findResult = std::mem::zeroed();
	pub static mut TUPLE: OTuple = std::mem::zeroed();
	pub static mut P: Page = std::mem::zeroed();

	bound.nkeys = 1;
	bound.n_row_keys = 0;
	bound.row_keys = NULL;
	bound.keys[0].type = TIDOID;
	bound.keys[0].flags = O_VALUE_BOUND_PLAIN_VALUE;
	bound.keys[0].comparator = bridge->fields[0].comparator;
	bound.keys[0].exclusion_fn = NULL;
	bound.keys[0].value = ItemPointerGetDatum(iptr);

	o_btree_load_shmem(&bridge->desc);
	init_page_find_context(&context, &bridge->desc,
						   COMMITSEQNO_INPROGRESS,
						   BTREE_PAGE_FIND_MODIFY);

	findResult = find_page(&context, &bound, BTreeKeyBound, 0);
	Assert(findResult == OFindPageResultSuccess);

	item = &context.items[context.index];
	p = O_GET_IN_MEMORY_PAGE(item->blkno);

	if (!BTREE_PAGE_LOCATOR_IS_VALID(p, &item->locator))
	{
		unlock_page(context.items[context.index].blkno);
		return;
	}

	BTREE_PAGE_READ_TUPLE(tuple, p, &item->locator);

	if (o_btree_cmp(&bridge->desc,
					&bound, BTreeKeyBound,
					&tuple, BTreeKeyLeafTuple) != 0)
	{
		unlock_page(context.items[context.index].blkno);
		return;
	}

	START_CRIT_SECTION();
	page_block_reads(item->blkno);
	page_locator_delete_item(p, &item->locator);
	MARK_DIRTY(&bridge->desc, item->blkno);
	END_CRIT_SECTION();
	unlock_page(context.items[context.index].blkno);
}

// Insert WAL record always stores one tuple, not a key.
OTuple
recovery_rec_insert(desc: &mut BTreeDescr, OTuple tuple, allocated: &mut bool, size: &mut int)
{
	*allocated = false;
	*size = o_btree_len(desc, tuple, OTupleLength);
	pub static mut TUPLE: return = std::mem::zeroed();
}

//
// Update WAL record always stores tuples, not keys. For REPLICA_IDENTITY_FULL
// new and old tuples, otherwise only new tuple.
//
OTuple
recovery_rec_update(desc: &mut BTreeDescr, OTuple tuple, allocated: &mut bool, size: &mut int)
{
	*allocated = false;
	*size = o_btree_len(desc, tuple, OTupleLength);
	pub static mut TUPLE: return = std::mem::zeroed();
}

//
// This function should be used for WAL recording for real tables that can be logically replicated.
// For them it depends on replica identity what should be contained in wal record: key or full tuple.
//
OTuple
recovery_rec_delete(desc: &mut BTreeDescr, OTuple tuple, allocated: &mut bool, size: &mut int, char relreplident)
{
	pub static mut KEY: OTuple = std::mem::zeroed();

	if (relreplident == REPLICA_IDENTITY_FULL)
	{
		*allocated = false;
		*size = o_btree_len(desc, tuple, OTupleLength);
		pub static mut TUPLE: return = std::mem::zeroed();
	}
	else
	{
		key = o_btree_tuple_make_key(desc, tuple, NULL, true, allocated);
		*size = o_btree_len(desc, key, OKeyLength);
		pub static mut KEY: return = std::mem::zeroed();
	}
}

//
// This function could be used only for system trees and bridge indices, that could not be logically
// replicated and can't have replica identity.
//
OTuple
recovery_rec_delete_key(desc: &mut BTreeDescr, OTuple key, allocated: &mut bool, size: &mut int)
{
	*allocated = false;
	*size = o_btree_len(desc, key, OKeyLength);
	pub static mut KEY: return = std::mem::zeroed();
}

//
// Debug method checks is recovery main process and recovery workers
// transactions is synchronized.
//
Datum
orioledb_recovery_synchronized(PG_FUNCTION_ARGS)
{
	XLogRecPtr	ptr = pg_atomic_read_u64(recovery_ptr);

	if (!ptr)
		PG_RETURN_BOOL(true);

	if (ptr != recovery_get_current_ptr())
		PG_RETURN_BOOL(false);

	if (ptr != recovery_get_retain_ptr())
		PG_RETURN_BOOL(false);

	WakeupRecovery();

	if (ptr != pg_atomic_read_u64(recovery_finished_list_ptr))
		PG_RETURN_BOOL(false);

	PG_RETURN_BOOL(true);
}

//
// Recompute and publish xid_meta->runXmin from the recovery leader's in-flight
// oxids.
//
// Must be called only by the recovery leader (worker_id < 0).  It reads and
// mutates the leader-only xmin_queue / retain_undo_queues and is the single
// writer of runXmin during recovery; recovery workers never touch these
// structures, so calling it from a worker would corrupt the horizon.
//
// This is invoked after every transaction finishes and after every
// recovery_xmin shift, so the published horizon is kept continuously
// up to date.  There is deliberately no full "scan the whole queue" pass:
// the queue is a pairing heap with no cheap ordered traversal, so we only
// ever inspect its top and advance the horizon as far as the current
// recovery_xmin allows.
//
fn
update_run_xmin()
{
	pub static mut XMIN: OXid = std::mem::zeroed();
	pub static mut I: std::os::raw::c_int = 0;
	pub static mut FOUND: bool = false;

	// Leader-only: xmin_queue is allocated only when worker_id < 0.
	Assert(xmin_queue != NULL);

	//
// Drain any fast-path-aborted oxids off the top of xmin_queue.  An entry
// that is in xmin_queue because the checkpoint's xids file named it
// (state->checkpoint_xid) but for which no WAL_REC_XID ever streamed
// (!state->wal_xid), and whose oxid lies below recovery_xmin, can only be
// a wal_rollback() fast-path abort on the master: the abort wrote no WAL
// record and a later WAL_REC_COMMIT/ROLLBACK has since carried the
// master's post-abort runXmin past its oxid.  Mark it ABORTED in shmem so
// visibility checks see a settled txn rather than the
// COMMITSEQNO_INPROGRESS that read_xids() stamped, apply any
// checkpoint_undo_stacks the master captured (lock-only undo can be
// present even though the abort took the no-WAL fast path), and drop it
// from the heap so it stops pinning runXmin.
//
	while (!pairingheap_is_empty(xmin_queue))
	{
		pub static mut RECOVERY_XID_STATE: *mut state = std::ptr::null_mut();

		state = pairingheap_container(RecoveryXidState, xmin_ph_node,
									  pairingheap_first(xmin_queue));

		//
// Only oxids strictly below recovery_xmin may be settled here: an
// oxid below recovery_xmin is guaranteed finished on the primary
// (recovery_xmin tracks the primary's advanced runXmin) and no
// further WAL will arrive for it.  The heap top is the smallest oxid,
// so once it reaches recovery_xmin there is nothing left to drain --
// stop.  We never scan deeper into the heap: it is a pairing heap
// with no cheap ordered traversal, and since update_run_xmin() runs
// after every transaction finish and recovery_xmin shift, draining
// just the eligible prefix each time keeps runXmin continuously up to
// date.
//
		if (state->oxid >= recovery_xmin)
			break;
		if (!state->checkpoint_xid || state->wal_xid)
			break;

		set_oxid_csn(state->oxid, COMMITSEQNO_ABORTED);

		//
// walk_checkpoint_stacks() clobbers recovery_oxid /
// curUndoLocations[] / oxid_needs_wal_flush, but update_run_xmin()
// can be re-entered from inside apply_wal_record() (via the
// o_handle_startup_proc_interrupts_hook ->
// update_proc_retain_undo_location -> check_delete_xid_state path)
// while another oxid is being applied -- so save those globals around
// the call and put them back.
//
		{
			pub static mut SAVED_RECOVERY_OXID: OXid = recovery_oxid;
			pub static mut SAVED_OXID_NEEDS_WAL_FLUSH: bool = oxid_needs_wal_flush;
			UndoStackLocations saved_undo_locations[(int) UndoLogsCount];
			pub static mut J: std::os::raw::c_int = 0;

			for (j = 0; j < (int) UndoLogsCount; j++)
				get_cur_undo_locations(&saved_undo_locations[j],
									   (UndoLogType) j);

			walk_checkpoint_stacks(state, COMMITSEQNO_ABORTED,
								   InvalidSubTransactionId, false);

			for (j = 0; j < (int) UndoLogsCount; j++)
				set_cur_undo_locations((UndoLogType) j,
									   saved_undo_locations[j]);
			recovery_oxid = saved_recovery_oxid;
			oxid_needs_wal_flush = saved_oxid_needs_wal_flush;
		}

		//
// The entry is a pure checkpoint-only oxid (checkpoint_xid &&
// !wal_xid) that has now been settled as ABORTED;
// checkpoint_undo_stacks is empty (walk_checkpoint_stacks emptied
// it), and in_finished_list / in_joint_commit_list are necessarily
// false (those flags are only raised by WAL_REC_COMMIT /
// WAL_REC_ROLLBACK / WAL_REC_JOINT_COMMIT processing, which never
// touched this oxid).  Tear the entry down fully so nothing --
// recovery_finish(), update_proc_retain_undo_location(), or anything
// else iterating the hash -- has to consider it again.
//
		state->csn = COMMITSEQNO_ABORTED;
		for (i = 0; i < (int) UndoLogsCount; i++)
		{
			if (state->in_retain_undo_heaps[i])
			{
				pairingheap_remove(retain_undo_queues[i],
								   &state->retain_undo_ph_nodes[i]);
				state->in_retain_undo_heaps[i] = false;
			}
		}
		pairingheap_remove(xmin_queue, &state->xmin_ph_node);
		if (state->used_by)
			pfree(state->used_by);
		hash_search(recovery_xid_state_hash, &state->oxid, HASH_REMOVE, &found);
		Assert(found);
	}

	if (!pairingheap_is_empty(xmin_queue))
	{
		pub static mut RECOVERY_XID_STATE: *mut state = std::ptr::null_mut();

		state = pairingheap_container(RecoveryXidState, xmin_ph_node,
									  pairingheap_first(xmin_queue));
		xmin = state->oxid;
	}
	else
	{
		xmin = pg_atomic_read_u64(&xid_meta->nextXid);
	}
	xmin = Min(xmin, recovery_xmin);
	pg_atomic_write_u64(&xid_meta->runXmin, xmin);

	//
// globalXmin must move monotonically forward.  The pre-existing "write
// down if xmin < globalXmin" branch existed to publish the checkpoint-era
// floor on the first read_xids() call, but checkpoint_shmem_init() now
// seeds globalXmin from control.checkpointRetainXmin -- the same floor --
// so any later downward move would be a regression we must never publish.
// Make monotonicity an explicit invariant instead.
//
	Assert(xmin >= pg_atomic_read_u64(&xid_meta->globalXmin));
}

fn
free_run_xmin()
{
	pub static mut XMIN: OXid = std::mem::zeroed();

	xmin = pg_atomic_read_u64(&xid_meta->nextXid);
	pg_atomic_write_u64(&xid_meta->runXmin, xmin);

	//
// globalXmin is the actual horizon, including any live read-only sessions
// that survive a promote -- their oProcData[].xmin can sit well below
// nextXid.  Pulling globalXmin down to nextXid here would publish a
// horizon higher than the real floor and break MVCC for those sessions.
// Leave globalXmin alone; advance_global_xmin() will bring it forward
// (only upward) once proc xmins clear.
//
	Assert(xmin >= pg_atomic_read_u64(&xid_meta->globalXmin));
}

//
// Update process transactionUndoRetainLocation according to the state of
// retain_undo_queues[undoType].  Removes finished transactions from the top
// of the heap when appropriate.
//
static bool
update_retain_location_with_heap(UndoLogType undoType, int worker_id,
								 XLogRecPtr recoveryPtr)
{
	curProcData: &mut ODBProcData = GET_CUR_PROCDATA();
	pub static mut RECOVERY_XID_STATE: *mut state = std::ptr::null_mut();

	if (pairingheap_is_empty(retain_undo_queues[undoType]))
		pub static mut FALSE: return = std::mem::zeroed();

	state = RetainUndoNodeGetRecoveryXidState(pairingheap_first(retain_undo_queues[undoType]), undoType);

	if (state->retain_locs[undoType] > pg_atomic_read_u64(&curProcData->undoRetainLocations[undoType].transactionUndoRetainLocation))
		pg_atomic_write_u64(&curProcData->undoRetainLocations[undoType].transactionUndoRetainLocation, state->retain_locs[undoType]);
	if (state->csn == COMMITSEQNO_ABORTED ||
		(COMMITSEQNO_IS_NORMAL(state->csn) && !state->in_finished_list && state->ptr <= recoveryPtr))
	{
		Assert(state->in_retain_undo_heaps[undoType]);
		pairingheap_remove(retain_undo_queues[undoType], &state->retain_undo_ph_nodes[undoType]);
		state->in_retain_undo_heaps[undoType] = false;
		check_delete_xid_state(state, worker_id);
		pub static mut TRUE: return = std::mem::zeroed();
	}
	else
	{
		pub static mut FALSE: return = std::mem::zeroed();
	}
}

//
// Updates advanceReservedLocation for a recovery process. Searches min
// transactionUndoRetainLocation for active transactions.
//

update_proc_retain_undo_location(int worker_id)
{
	XLogRecPtr	recoveryPtr = InvalidXLogRecPtr,
				listPtr;
	pub static mut RECOVERY_XID_STATE: *mut state = std::ptr::null_mut();
	pub static mut MITER: dlist_mutable_iter = std::mem::zeroed();
	pub static mut I: std::os::raw::c_int = 0;
	pub static mut ALL_RETAIN_QUEUES_EMPTY: bool = true;
	pub static mut NEEDS_FEEDBACK: bool = false;

	if (cur_recovery_xid_state != NULL)
	{
		//
// Update current recovery Xid state with retain undo locations if
// needed.
//
		for (i = 0; i < (int) UndoLogsCount; i++)
		{
			if (!UndoLocationIsValid(cur_recovery_xid_state->retain_locs[i]) &&
				UndoLocationIsValid(curRetainUndoLocations[i]))
			{
				cur_recovery_xid_state->retain_locs[i] = curRetainUndoLocations[i];
				Assert(!cur_recovery_xid_state->in_retain_undo_heaps[i]);
				cur_recovery_xid_state->in_retain_undo_heaps[i] = true;
				pairingheap_add(retain_undo_queues[i],
								&cur_recovery_xid_state->retain_undo_ph_nodes[i]);
			}
		}
	}

	if (worker_id < 0)
		listPtr = recoveryPtr = recovery_get_current_ptr();
	else
		listPtr = pg_atomic_read_u64(recovery_finished_list_ptr);

	dlist_foreach_modify(miter, &finished_list)
	{
		state = dlist_container(RecoveryXidState, finished_list_node, miter.cur);
		if (state->ptr > listPtr)
			break;

		if (worker_id < 0)
		{
			if (!COMMITSEQNO_IS_ABORTED(state->csn))
			{
				set_oxid_csn(state->oxid, COMMITSEQNO_COMMITTING);
				state->csn = pg_atomic_fetch_add_u64(&TRANSAM_VARIABLES->nextCommitSeqNo, 1);
				set_oxid_csn(state->oxid, state->csn);
				set_oxid_xlog_ptr(state->oxid, XLOG_PTR_ALIGN(state->ptr));
			}
			else
			{
				set_oxid_csn(state->oxid, COMMITSEQNO_ABORTED);
				set_oxid_xlog_ptr(state->oxid, InvalidXLogRecPtr);
			}
			if (state->needs_feedback)
				needsFeedback = true;
		}
		dlist_delete(miter.cur);
		state->in_finished_list = false;
		check_delete_xid_state(state, worker_id);
	}
	if (worker_id < 0)
	{
		pg_atomic_write_u64(recovery_finished_list_ptr, recoveryPtr);

		//
// If at least one transaction required feedback to the primary, wake
// up WAL receiver to provide it.
//
		if (needsFeedback)
			WalRcvForceReply();
	}

	//
// Remove transactions, visible for all, from the retain queue.
//
	for (i = 0; i < (int) UndoLogsCount; i++)
	{
		if (pairingheap_is_empty(retain_undo_queues[i]))
			free_retained_undo_location(i);
		else
			allRetainQueuesEmpty = false;
	}

	if (allRetainQueuesEmpty)
	{
		if (worker_id >= 0)
			pg_atomic_write_u64(&worker_ptrs[worker_id].retainPtr,
								pg_atomic_read_u64(&worker_ptrs[worker_id].commitPtr));
		else
			pg_atomic_write_u64(recovery_main_retain_ptr,
								pg_atomic_read_u64(recovery_ptr));
		return;
	}

	if (XLogRecPtrIsInvalid(recoveryPtr))
		recoveryPtr = recovery_get_current_ptr();

	while (true)
	{
		pub static mut REMOVED: bool = false;

		allRetainQueuesEmpty = true;
		for (i = 0; i < (int) UndoLogsCount; i++)
		{
			if (pairingheap_is_empty(retain_undo_queues[i]))
				free_retained_undo_location(i);
			else
				allRetainQueuesEmpty = false;
		}

		if (allRetainQueuesEmpty)
		{
			if (worker_id >= 0)
				pg_atomic_write_u64(&worker_ptrs[worker_id].retainPtr,
									pg_atomic_read_u64(&worker_ptrs[worker_id].commitPtr));
			else
				pg_atomic_write_u64(recovery_main_retain_ptr,
									pg_atomic_read_u64(recovery_ptr));
			return;
		}

		for (i = 0; i < (int) UndoLogsCount; i++)
		{
			pub static mut RESULT: bool = false;

			result = update_retain_location_with_heap(i, worker_id, recoveryPtr);
			removed = removed || result;
		}

		if (!removed)
			break;

	}
	if (worker_id >= 0)
		pg_atomic_write_u64(&worker_ptrs[worker_id].retainPtr, recoveryPtr);
	else
		pg_atomic_write_u64(recovery_main_retain_ptr, recoveryPtr);
}

fn
recovery_write_to_xids_queue(int worker_id, uint32 requestNumber)
{
	pub static mut HASH_SEQ: HASH_SEQ_STATUS = std::mem::zeroed();
	pub static mut RECOVERY_XID_STATE: *mut state = std::ptr::null_mut();
	pub static mut I: std::os::raw::c_int = 0;

	if (cur_recovery_xid_state)
	{
		pub static mut RECOVERY_XID_STATE: *mut cur_state = cur_recovery_xid_state;

		for (i = 0; i < (int) UndoLogsCount; i++)
		{
			get_cur_undo_locations(&cur_state->undo_stacks[i],
								   (UndoLogType) i);
			if (!UndoLocationIsValid(cur_state->retain_locs[i]) &&
				UndoLocationIsValid(curRetainUndoLocations[i]))
			{
				cur_state->retain_locs[i] = curRetainUndoLocations[i];
				Assert(!cur_state->in_retain_undo_heaps[i]);
				cur_state->in_retain_undo_heaps[i] = true;
				pairingheap_add(retain_undo_queues[i], &cur_state->retain_undo_ph_nodes[i]);
			}
		}
	}

	hash_seq_init(&hash_seq, recovery_xid_state_hash);
	while ((state = (RecoveryXidState *) hash_seq_search(&hash_seq)) != NULL)
	{
		pub static mut REC: XidFileRec = std::mem::zeroed();
		pub static mut ITER: dlist_iter = std::mem::zeroed();

		if (!COMMITSEQNO_IS_INPROGRESS(state->csn))
			continue;

		rec.oxid = state->oxid;
		for (i = 0; i < (int) UndoLogsCount; i++)
		{
			rec.kind = (XidRecKind) i;
			rec.undoLocation = state->undo_stacks[i];
			rec.retainLocation = state->retain_locs[i];
			write_to_xids_queue(&rec);
		}

		dlist_foreach(iter, &state->checkpoint_undo_stacks)
		{
			stack: &mut CheckpointUndoStack = dlist_container(CheckpointUndoStack,
														 node,
														 iter.cur);

			rec.kind = stack->kind;
			rec.undoLocation = stack->undoStack;
			rec.retainLocation = ((int) stack->kind < (int) UndoLogsCount)
				? state->retain_locs[(UndoLogType) stack->kind]
				pub static mut INVALID_UNDO_LOCATION: : = std::mem::zeroed();
			write_to_xids_queue(&rec);
		}
	}

	if (worker_id < 0)
		recovery_undo_loc_flush->recoveryMainCompletedCheckpointNumber = requestNumber;
	else
		worker_ptrs[worker_id].flushedUndoLocCompletedCheckpointNumber = requestNumber;
}

fn
update_undo_loc_flush_completed_number(bool single)
{
	pub static mut COMPLETED_NUMBER: uint32 = std::mem::zeroed();

	completedNumber = recovery_undo_loc_flush->recoveryMainCompletedCheckpointNumber;
	if (!single)
	{
		pub static mut I: std::os::raw::c_int = 0;

		for (i = 0; i < recovery_pool_size_guc; i++)
			completedNumber = Min(completedNumber, worker_ptrs[i].flushedUndoLocCompletedCheckpointNumber);
	}
	recovery_undo_loc_flush->completedCheckpointNumber = completedNumber;
}

//
// Handles immediate undo positions flush request from checkpointer.
//

update_recovery_undo_loc_flush(bool single, int worker_id)
{
	uint32		myCompletedNumber,
				requestNumber;

	requestNumber = recovery_undo_loc_flush->immediateRequestCheckpointNumber;
	if (recovery_undo_loc_flush->completedCheckpointNumber >= requestNumber)
		return;

	if (worker_id < 0)
		myCompletedNumber = recovery_undo_loc_flush->recoveryMainCompletedCheckpointNumber;
	else
		myCompletedNumber = worker_ptrs[worker_id].flushedUndoLocCompletedCheckpointNumber;

	//
// Process immediate request if any.
//
	if (myCompletedNumber < requestNumber)
		recovery_write_to_xids_queue(worker_id, requestNumber);

	if (worker_id >= 0)
		return;

	update_undo_loc_flush_completed_number(single);
}

//
// Save the recovery worker state to the temporary file.
//
fn
save_state_to_file(int worker_id)
{
	pub static mut CHAR: *mut filename = std::ptr::null_mut();
	pub static mut TEMP_FILE: File = std::mem::zeroed();
	pub static mut HEADER: WorkerUndoTempHeader = std::mem::zeroed();
	pub static mut HASH_SEQ: HASH_SEQ_STATUS = std::mem::zeroed();
	pub static mut RECOVERY_XID_STATE: *mut state = std::ptr::null_mut();
	off_t		offset = sizeof(header);

	// Create worker-specific temp file
	filename = psprintf(WORKER_UNDO_TEMP_FILE, worker_id);
	tempFile = PathNameOpenFile(filename, O_WRONLY | O_CREAT | O_TRUNC | PG_BINARY);
	if (tempFile < 0)
		ereport(FATAL,
				(errcode_for_file_access(),
				 errmsg("could not open file %s: %m", filename)));

	// Write header
	memset(&header, 0, sizeof(header));
	header.worker_id = worker_id;
	header.num_transactions = hash_get_num_entries(recovery_xid_state_hash);
	if (OFileWrite(tempFile, (char *) &header, sizeof(header), 0,
				   WAIT_EVENT_DATA_FILE_WRITE) != sizeof(header))
		ereport(FATAL,
				(errcode_for_file_access(),
				 errmsg("could not write recovery worker undo header to file \"%s\": %m",
						filename)));

	// Update current undo locations if needed
	if (cur_recovery_xid_state != NULL)
	{
		for (int i = 0; i < UndoLogsCount; i++)
		{
			get_cur_undo_locations(&cur_recovery_xid_state->undo_stacks[i],
								   (UndoLogType) i);
			if (!UndoLocationIsValid(cur_recovery_xid_state->retain_locs[i]) &&
				UndoLocationIsValid(curRetainUndoLocations[i]))
			{
				Assert(!cur_recovery_xid_state->in_retain_undo_heaps[i]);
				cur_recovery_xid_state->retain_locs[i] = curRetainUndoLocations[i];
				pairingheap_add(retain_undo_queues[i], &cur_recovery_xid_state->retain_undo_ph_nodes[i]);
				cur_recovery_xid_state->in_retain_undo_heaps[i] = true;
			}
		}
	}

	// Write all transaction states
	hash_seq_init(&hash_seq, recovery_xid_state_hash);
	while ((state = hash_seq_search(&hash_seq)) != NULL)
	{
		pub static mut ENTRY: WorkerUndoTempEntry = std::mem::zeroed();
		pub static mut ITER: dlist_iter = std::mem::zeroed();

		// Save complete transaction state
		memset(&entry, 0, sizeof(entry));
		entry.oxid = state->oxid;
		entry.csn = state->csn;
		entry.numCheckpointStacks = 0;

		dlist_foreach(iter, &state->checkpoint_undo_stacks)
			entry.numCheckpointStacks++;

		memcpy(entry.undoStacks, state->undo_stacks, sizeof(entry.undoStacks));
		memcpy(entry.undoRetainLocs, state->retain_locs, sizeof(entry.undoRetainLocs));

		if (OFileWrite(tempFile, (char *) &entry, sizeof(entry), offset,
					   WAIT_EVENT_DATA_FILE_WRITE) != sizeof(entry))
			ereport(FATAL,
					(errcode_for_file_access(),
					 errmsg("could not write file \"%s\": %m",
							filename)));
		offset += sizeof(entry);

		// Also write checkpoint stacks
		dlist_foreach(iter, &state->checkpoint_undo_stacks)
		{
			stack: &mut CheckpointUndoStack = dlist_container(CheckpointUndoStack,
														 node,
														 iter.cur);
			pub static mut TEMP_STACK: WorkerUndoTempCheckpointStack = std::mem::zeroed();

			memset(&tempStack, 0, sizeof(tempStack));
			tempStack.kind = stack->kind;
			tempStack.undoStack = stack->undoStack;

			if (OFileWrite(tempFile, (char *) &tempStack, sizeof(tempStack),
						   offset, WAIT_EVENT_DATA_FILE_WRITE) != sizeof(tempStack))
				ereport(FATAL,
						(errcode_for_file_access(),
						 errmsg("could not write file \"%s\": %m", filename)));

			offset += sizeof(tempStack);
		}
	}

	if (FileSync(tempFile, WAIT_EVENT_DATA_FILE_WRITE) < 0)
		ereport(FATAL,
				(errcode_for_file_access(),
				 errmsg("could not sync file \"%s\": %m", filename)));

	FileClose(tempFile);
	pfree(filename);
}

//
// Read the recovery worker state from the temporary file.
//

recovery_load_state_from_file(int worker_id, uint32 chkpnum, bool shutdown)
{
	filename: &mut char = psprintf(WORKER_UNDO_TEMP_FILE, worker_id);
	File		tempFile = PathNameOpenFile(filename, O_RDONLY | PG_BINARY);
	pub static mut HEADER: WorkerUndoTempHeader = std::mem::zeroed();
	pub static mut OFFSET: off_t = std::mem::zeroed();

	if (tempFile < 0)
	{
		if (errno != ENOENT)
			ereport(FATAL,
					(errcode_for_file_access(),
					 errmsg("could not open file \"%s\": %m", filename)));
		return;
	}

	// Read header
	if (OFileRead(tempFile, (char *) &header, sizeof(header), 0,
				  WAIT_EVENT_DATA_FILE_READ) != sizeof(header))
		ereport(FATAL,
				(errcode_for_file_access(),
				 errmsg("could not read recovery worker undo header from file \"%s\": %m",
						filename)));

	offset = sizeof(header);

	// Process each transaction entry
	for (int i = 0; i < header.num_transactions; i++)
	{
		pub static mut ENTRY: WorkerUndoTempEntry = std::mem::zeroed();

		// Read main entry
		if (OFileRead(tempFile, (char *) &entry, sizeof(entry), offset,
					  WAIT_EVENT_DATA_FILE_READ) != sizeof(entry))
			ereport(FATAL,
					(errcode_for_file_access(),
					 errmsg("could not read file \"%s\": %m",
							filename)));
		offset += sizeof(entry);

		// Skip if transaction is finished
		if (!COMMITSEQNO_IS_INPROGRESS(entry.csn))
		{
			// Still need to skip checkpoint stack entries
			offset += entry.numCheckpointStacks * sizeof(CheckpointUndoStack);
			continue;
		}

		// Process main undo stacks (Regular, PageLevel, System)
		for (int j = 0; j < UndoLogsCount; j++)
		{
			pub static mut REC: XidFileRec = std::mem::zeroed();

			rec.oxid = entry.oxid;
			rec.kind = (XidRecKind) j;
			rec.undoLocation = entry.undoStacks[j];
			rec.retainLocation = entry.undoRetainLocs[j];
			write_to_xids_queue(&rec);
		}

		// Process checkpoint stacks
		for (int j = 0; j < entry.numCheckpointStacks; j++)
		{
			pub static mut REC: XidFileRec = std::mem::zeroed();
			pub static mut STACK: WorkerUndoTempCheckpointStack = std::mem::zeroed();

			if (OFileRead(tempFile, (char *) &stack, sizeof(stack), offset,
						  WAIT_EVENT_DATA_FILE_READ) != sizeof(stack))
				ereport(FATAL,
						(errcode_for_file_access(),
						 errmsg("could not read file \"%s\": %m",
								filename)));
			offset += sizeof(stack);

			// Write checkpoint stack to XID queue
			rec.oxid = entry.oxid;
			rec.kind = stack.kind;
			rec.undoLocation = stack.undoStack;
			rec.retainLocation = ((int) stack.kind < (int) UndoLogsCount)
				? entry.undoRetainLocs[(UndoLogType) stack.kind]
				pub static mut INVALID_UNDO_LOCATION: : = std::mem::zeroed();
			write_to_xids_queue(&rec);
		}
	}

	FileClose(tempFile);

	//
// We no longer need the temporary file if it's shutdown (last)
// checkpoint. So, cleanup.
//
	if (shutdown)
		unlink(filename);
	pfree(filename);
}


recovery_on_proc_exit(int code, Datum arg)
{
	int			worker_id = (int) arg;

	if (!recovery_xid_state_hash)
		return;

	//
// The startup process (worker_id < 0) is not a recovery worker and
// doesn't use worker_ptrs[].  save_state_to_file() and hasTempFile are
// only meaningful for actual recovery workers whose state needs to be
// picked up by the checkpointer.
//
	if (worker_id < 0)
		return;

	elog(LOG, "recovery on exit: %d", worker_id);

	save_state_to_file(worker_id);

	// Mark worker as having saved state and exited
	pg_atomic_test_set_flag(&worker_ptrs[worker_id].hasTempFile);
}

fn
o_handle_startup_proc_interrupts_hook()
{
	if (is_recovery_in_progress())
		update_proc_retain_undo_location(-1);

	update_recovery_undo_loc_flush(*recovery_single_process, -1);
}

fn
abort_recovery(workers_pool: &mut RecoveryWorkerState, bool send_to_idx_pool)
{
	pub static mut I: std::os::raw::c_int = 0;
	int			start,
				finish;

	if (send_to_idx_pool)
	{
		Assert(recovery_idx_pool_size_guc);
		start = index_build_first_worker;
		finish = index_build_last_worker;
	}
	else
	{
		start = 0;
		finish = recovery_idx_pool_size_guc ? index_build_leader : recovery_last_worker;
	}

	for (i = start; i <= finish; i++)
	{
		if (workers_pool[i].queue != NULL)
			shm_mq_detach(workers_pool[i].queue);

		if (workers_pool[i].handle != NULL)
		{
			TerminateBackgroundWorker(workers_pool[i].handle);
			worker_wait_shutdown(&workers_pool[i]);
		}
	}

	elog(LOG, "orioledb recovery finished: abort recovery.");
}

//
// WaitForBackgroundWorkerShutdown() does not work in this context. We need
// an analog.
//
fn
worker_wait_shutdown(worker: &mut RecoveryWorkerState)
{
	pub static mut STATUS: BgwHandleStatus = std::mem::zeroed();
	pub static mut NOT_USED: pid_t = std::mem::zeroed();

	Assert(worker != NULL);
	Assert(worker->handle != NULL);

	while (true)
	{
		CHECK_FOR_INTERRUPTS();

		status = GetBackgroundWorkerPid(worker->handle, &not_used);

		if (status == BGWH_POSTMASTER_DIED)
			break;
		else if (status == BGWH_STOPPED)
			break;

		pg_usleep(200);
	}
}

fn
cleanup_tablespace_old_files(path: &mut char, uint32 chkp_num, bool before_recovery)
{
	dir: &mut DIR,
			   *dbDir;
	struct file: &mut dirent,
			   *dbFile;
	pub static mut CHAR: *mut filename = std::ptr::null_mut();
	char		ext[5];

	dir = opendir(path);
	if (dir == NULL)
		return;

	while (errno = 0, (file = readdir(dir)) != NULL)
	{
		pub static mut DB_OID: Oid = std::mem::zeroed();
		pub static mut CHAR: *mut dbDirName = std::ptr::null_mut();
		pub static mut FSYNC_DB_DIR: bool = false;

		if (sscanf(file->d_name, "%u", &dbOid) != 1)
			continue;

		dbDirName = psprintf("%s/%u", path, dbOid);

		dbDir = opendir(dbDirName);
		if (dbDir == NULL)
		{
			pfree(dbDirName);
			continue;
		}

		while (errno = 0, (dbFile = readdir(dbDir)) != NULL)
		{
			uint32		file_reloid,
						file_chkp,
						file_segno;
			pub static mut CLEANUP: bool = false;

			if (orioledb_s3_mode &&
				(sscanf(dbFile->d_name, "%10u-%10u",
						&file_reloid, &file_chkp) == 2 ||
				 sscanf(dbFile->d_name, "%10u.%10u-%10u",
						&file_reloid, &file_segno, &file_chkp) == 3) &&
				file_chkp > chkp_num)
			{
				cleanup = true;
			}

			if (sscanf(dbFile->d_name, "%10u-%10u.%4s",
					   &file_reloid, &file_chkp, ext) == 3)
			{
				if (before_recovery)
				{
					// ---
// Before recovery we should cleanup:
//
// 1. *.map and *.tmp files which were not created by
// checkpointer.
// 2. All free extents tree files.
//
// Otherwise:
//
// 1. In some cases wrong *.map files will be created.
// (if size of old *.map or *.tmp file is more than will
// be created by checkpointer).
//
					if (!strcmp(ext, "tmp"))
					{
						cleanup = (file_chkp > chkp_num);
					}
					else if (!strcmp(ext, "map"))
					{
						pub static mut MY_CHKP_NUM: uint32 = std::mem::zeroed();
						pub static mut FOUND: bool = false;

						my_chkp_num = o_get_latest_chkp_num(dbOid, file_reloid,
															chkp_num, &found);

						cleanup = (file_chkp > my_chkp_num);

						if (!found && file_chkp == chkp_num)
							o_update_latest_chkp_num(dbOid, file_reloid,
													 file_chkp);
					}

					if (!cleanup)
					{
						ORelOids	oids = {dbOid, file_reloid, file_reloid};

						cleanup = IS_SYS_TREE_OIDS(oids) && sys_tree_get_storage_type(oids.relnode) == BTreeStorageTemporary;
					}
				}
				else
				{
					//
// After recovery we should cleanup old *.tmp and *.map
// files.
//
					if (!strcmp(ext, "tmp"))
					{
						cleanup = (file_chkp <= chkp_num);
					}
					else if (!strcmp(ext, "map"))
					{
						pub static mut MY_CHKP_NUM: uint32 = std::mem::zeroed();

						my_chkp_num = o_get_latest_chkp_num(dbOid, file_reloid,
															chkp_num, NULL);

						cleanup = (file_chkp < my_chkp_num);
					}
				}
			}
			else if (before_recovery &&
					 sscanf(dbFile->d_name, "%10u", &file_reloid) == 1)
			{
				//
// Removes free extents tree data files.
//
				ORelOids	oids = {dbOid, file_reloid, file_reloid};

				cleanup = IS_SYS_TREE_OIDS(oids) && sys_tree_get_storage_type(oids.relnode) == BTreeStorageTemporary;
			}

			if (cleanup)
			{
				filename = psprintf("%s/%u/%s", path, dbOid, dbFile->d_name);

				if (unlink(filename) < 0)
				{
					ereport(FATAL,
							(errcode_for_file_access(),
							 errmsg("could not remove file \"%s\": %m",
									filename)));
				}
				fsyncDbDir = true;
			}
		}
		closedir(dbDir);
		if (fsyncDbDir)
			fsync_fname_ext(dbDirName, true, false, FATAL);
		pfree(dbDirName);
	}

	if (errno != 0)
	{
		ereport(ERROR, (errcode_for_file_access(),
						errmsg("unable to clean up temporary files: %m")));
	}
	closedir(dir);
}


recovery_cleanup_old_files(uint32 chkp_num, bool before_recovery)
{
	pub static mut DIR: *mut dir = std::ptr::null_mut();
	char		path[MAXPGPATH];
	char		targetpath[MAXPGPATH];
	pub static mut DIRENT: *mut struct file = std::ptr::null_mut();

#define PG_TBLSPC "pg_tblspc"

	if (!before_recovery && chkp_num == 0)
		return;

	path[0] = '\0';
	strlcat(path, ORIOLEDB_DATA_DIR, MAXPGPATH);
	cleanup_tablespace_old_files(path, chkp_num, before_recovery);

	dir = opendir(PG_TBLSPC);
	while (errno = 0, (file = readdir(dir)) != NULL)
	{
		pub static mut ST: struct stat = std::mem::zeroed();
		pub static mut RLLEN: std::os::raw::c_int = 0;

		// Skip special stuff
		if (strcmp(file->d_name, ".") == 0 || strcmp(file->d_name, "..") == 0)
			continue;

		path[0] = '\0';
		pg_snprintf(path, MAXPGPATH,
					PG_TBLSPC "/%s/" TABLESPACE_VERSION_DIRECTORY,
					file->d_name);
		if (lstat(path, &st) < 0)
		{
			ereport(ERROR,
					(errcode_for_file_access(),
					 errmsg("could not stat file \"%s\": %m",
							file->d_name)));
		}

		if (!S_ISLNK(st.st_mode))
		{
			strlcat(path, "/" ORIOLEDB_DATA_DIR, MAXPGPATH);
			cleanup_tablespace_old_files(path, chkp_num, before_recovery);
		}
		else
		{
			rllen = readlink(path, targetpath, sizeof(targetpath));
			if (rllen < 0)
				ereport(ERROR,
						(errcode_for_file_access(),
						 errmsg("could not read symbolic link \"%s\": %m",
								path)));
			if (rllen >= sizeof(targetpath))
				ereport(ERROR,
						(errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),
						 errmsg("symbolic link \"%s\" target is too long",
								path)));
			targetpath[rllen] = '\0';

			path[0] = '\0';
			pg_snprintf(path, MAXPGPATH,
						"%s/" ORIOLEDB_DATA_DIR,
						targetpath);
			cleanup_tablespace_old_files(path, chkp_num, before_recovery);
		}
	}
	closedir(dir);
#undef PG_TBLSPC
}

static OIndexKey *
o_indices_get_trees(Pointer tuple, tableOids: &mut ORelOids)
{
	pub static mut CHUNK: OIndexChunk = std::mem::zeroed();
	pub static mut O_INDEX_KEY: *mut trees = std::ptr::null_mut();

	memcpy(&chunk, tuple, offsetof(OIndexChunk, data));

	if (chunk.key.chunknum != 0)
		pub static mut NULL: return = std::mem::zeroed();

	//
// The serialized OIndex blob starts at OIndex.tableOids (the key fields
// indexOids/indexType/indexVersion are stored in OIndexChunkKey).
// tablespace must lie after tableOids in the struct so the subtraction
// below is positive and the field lands in the first chunk.
//
	StaticAssertStmt(offsetof(OIndex, tablespace) > offsetof(OIndex, tableOids),
					 "OIndex.tablespace must follow OIndex.tableOids");
	Assert(chunk.dataLength >= sizeof(*tableOids));
	memcpy(tableOids, tuple + offsetof(OIndexChunk, data), sizeof(*tableOids));
	trees = (OIndexKey *) MemoryContextAlloc(CurTransactionContext, sizeof(OIndexKey));
	trees->oids = chunk.key.oids;
	memcpy(&trees->tablespace, tuple + offsetof(OIndexChunk, data) + (offsetof(OIndex, tablespace) - offsetof(OIndex, tableOids)), sizeof(Oid));

	pub static mut TREES: return = std::mem::zeroed();
}

fn
clean_workers_oids()
{
	pub static mut I: std::os::raw::c_int = 0;

	for (i = 0; i < recovery_pool_size_guc; i++)
	{
		pub static mut RECOVERY_WORKER_STATE: *mut state = &workers_pool[i];

		ORelOidsSetInvalid(state->oids);
		state->type = oIndexInvalid;
	}
}

fn
recovery_send_leader_oids(ORelOids oids, OIndexNumber ix_num,
						  uint32 o_table_version,
						  ORelOids old_oids, uint32 old_o_table_version,	// Non-zero only for
// rebuild
						  bool isrebuild)
{
	pub static mut MSG: RecoveryMsgLeaderIdxBuild = std::mem::zeroed();
	pub static mut RECOVERY_IDX_BUILD_QUEUE_STATE: *mut state = std::ptr::null_mut();

	Assert(!(*recovery_single_process));
	Assert(ORelOidsIsValid(oids));

	memset(&msg, 0, sizeof(msg));

	msg.header.type = RecoveryMsgTypeLeaderParallelIndexBuild;
	msg.oids = oids;
	msg.old_oids = old_oids;
	msg.ix_num = ix_num;
	msg.o_table_version = o_table_version;
	msg.old_o_table_version = old_o_table_version;

	Assert(o_tables_get_extended(oids, build_fetch_context(&o_non_deleted_snapshot, o_table_version)) != NULL);

	// Remember oids of index build added to a queue in a hash table
	state = (RecoveryIdxBuildQueueState *) hash_search(idxbuild_oids_hash,
													   &oids,
													   HASH_ENTER,
													   NULL);

	state->position = pg_atomic_add_fetch_u64(recovery_index_next_pos, 1);
	msg.isrebuild = isrebuild;
	msg.oxid = recovery_oxid;
	msg.current_position = state->position;

	worker_send_msg(index_build_leader, (Pointer) &msg, sizeof(msg));
	worker_queue_flush(index_build_leader);
}


recovery_send_worker_oids(dsm_handle seg_handle)
{
	pub static mut MSG: RecoveryMsgWorkerIdxBuild = std::mem::zeroed();

	Assert(!(*recovery_single_process));

	msg.header.type = RecoveryMsgTypeWorkerParallelIndexBuild;
	msg.oxid = recovery_oxid;
	msg.seg_handle = seg_handle;

	for (int i = index_build_first_worker; i <= index_build_last_worker; i++)
	{
		worker_send_msg(i, (Pointer) &msg, sizeof(msg));
		worker_queue_flush(i);
	}
}

fn
recovery_send_init(int worker_num)
{
	pub static mut MSG: RecoveryMsgEmpty = std::mem::zeroed();

	Assert(!(*recovery_single_process));

	msg.header.type = RecoveryMsgTypeInit;

	worker_send_msg(worker_num, (Pointer) &msg, sizeof(msg));
	worker_queue_flush(worker_num);
}

fn
handle_o_tables_meta_unlock(ORelOids oids, Oid oldRelnode)
{
	if (!cur_recovery_xid_state->o_tables_meta_locked)
	{
		//
// It might happen that we didn't replay WAL_REC_O_TABLES_META_LOCK
// wal record.  That means we've finished index build before
// checkpoint of a tree was actually started.
//
		return;
	}

	if (ORelOidsIsValid(oids))
		recreate_table_descr_by_oids(oids);

	if (reachedConsistency && ORelOidsIsValid(oids))
	{
		pub static mut O_TABLE: *mut new_o_table = std::ptr::null_mut();
		pub static mut O_TABLE: *mut old_o_table = std::ptr::null_mut();
		pub static mut O_TABLE_DESCR: *mut old_descr = std::ptr::null_mut();
		pub static mut IX_NUM: OIndexNumber = std::mem::zeroed();
		pub static mut NINDICES: uint16 = std::mem::zeroed();
		pub static mut CHANGED_TABLESPACE: bool = false;

		new_o_table = o_tables_get(oids);
		Assert(new_o_table);

		if (!OidIsValid(oldRelnode))
		{
			pub static mut VERSION: uint32 = std::mem::zeroed();

			// new_o_table->version may be 0
			if (new_o_table->version == O_TABLE_INVALID_VERSION || new_o_table->version == 0)
			{
				version = O_TABLE_INVALID_VERSION;
			}
			else
			{
				version = new_o_table->version - 1;
			}

			old_o_table = o_tables_get_extended(oids, build_fetch_context(&o_non_deleted_snapshot, version));
		}
		else
		{
			ORelOids	oldOids = {oids.datoid, oids.reloid, oldRelnode};

			old_o_table = o_tables_get(oldOids);
		}
		Assert(old_o_table);

		nindices = Max(old_o_table->nindices, new_o_table->nindices);
		for (ix_num = 0; ix_num < nindices - 1; ix_num++)
		{
			if (!ORelOidsIsEqual(old_o_table->indices[ix_num].oids,
								 new_o_table->indices[ix_num].oids))
				break;
		}

		if (new_o_table->tablespace != old_o_table->tablespace)
			changed_tablespace = true;
		if (new_o_table->nindices > old_o_table->nindices)
		{
			pub static mut TMP_DESCR: OTableDescr = std::mem::zeroed();

			o_fill_tmp_table_descr(&tmp_descr, new_o_table);
			if (new_o_table->indices[ix_num].type == oIndexPrimary)
			{
				if (tbl_data_exists(&old_o_table->oids, old_o_table->tablespace))
				{
					old_descr = o_fetch_table_descr(old_o_table->oids);
					if (!changed_tablespace)
						rebuild_indices_insert_placeholders(&tmp_descr);
					o_tables_meta_unlock_no_wal();

					Assert(is_recovery_in_progress());

					//
// In main recovery worker send message to main index
// creation worker in dedicated recovery workers pool and
// exit
//
					if (!*recovery_single_process)
					{
						Assert(new_o_table->nindices == nindices);
						// Send recovery message to become a leader
						recovery_send_leader_oids(oids, InvalidIndexNumber,
												  new_o_table->version,
												  old_o_table->oids,
												  old_o_table->version,
												  true);
					}
					else
						rebuild_indices(old_o_table, old_descr,
										new_o_table, &tmp_descr, false, NULL);
				}
				else
				{
					o_tables_meta_unlock_no_wal();
				}
			}
			else
			{
				if (!changed_tablespace)
					o_insert_shared_root_placeholder(new_o_table->indices[ix_num].oids.datoid,
													 new_o_table->indices[ix_num].oids.relnode);
				o_tables_meta_unlock_no_wal();

				Assert(is_recovery_in_progress());

				//
// In main recovery worker send message to main index creation
// worker in dedicated recovery workers pool and exit
//
				if (!*recovery_single_process)
				{
					pub static mut OLD_OIDS: ORelOids = std::mem::zeroed();

					Assert(new_o_table->nindices == nindices);
					// Send recovery message to become a leader
					ORelOidsSetInvalid(old_oids);
					if (changed_tablespace)
						old_oids.relnode = old_o_table->oids.relnode;
					recovery_send_leader_oids(oids, ix_num, new_o_table->version,
											  old_oids, 0, false);
				}
				else
					build_secondary_index(changed_tablespace ?
										  old_o_table->oids.relnode :
										  InvalidOid,
										  new_o_table, &tmp_descr, ix_num,
										  false, NULL);
			}
			o_free_tmp_table_descr(&tmp_descr);
		}
		else if (new_o_table->nindices < old_o_table->nindices)
		{
			if (old_o_table->indices[ix_num].type == oIndexPrimary)
			{
				pub static mut TMP_DESCR: OTableDescr = std::mem::zeroed();

				o_fill_tmp_table_descr(&tmp_descr, new_o_table);
				if (tbl_data_exists(&old_o_table->indices[ix_num].oids, old_o_table->indices[ix_num].tablespace))
				{
					old_descr = o_fetch_table_descr(old_o_table->oids);
					if (!changed_tablespace)
						rebuild_indices_insert_placeholders(&tmp_descr);
					o_tables_meta_unlock_no_wal();

					//
// In main recovery worker send message to main index
// creation worker in dedicated recovery workers pool and
// exit
//
					if (!*recovery_single_process)
					{
						// Send recovery message to become a leader
						recovery_send_leader_oids(oids, InvalidIndexNumber,
												  new_o_table->version,
												  old_o_table->oids,
												  old_o_table->version,
												  true);
					}
					else
						rebuild_indices(old_o_table, old_descr,
										new_o_table, &tmp_descr, false, NULL);
				}
				else
				{
					o_tables_meta_unlock_no_wal();
				}
				o_free_tmp_table_descr(&tmp_descr);
			}
			else
			{
				o_tables_meta_unlock_no_wal();
			}
		}
		else if (ORelOidsIsValid(new_o_table->bridge_oids) !=
				 ORelOidsIsValid(old_o_table->bridge_oids))
		{
			//
// Bridge index was added or dropped.  The table data is stored
// under new OIDs, so we need a full rebuild.
//
// On a replica, the intermediate table created by
// recreate_o_table() has no data because rebuild_indices() writes
// directly without WAL.  Check primary index oids first, then
// fall back to table oids.
//
			pub static mut TMP_DESCR: OTableDescr = std::mem::zeroed();
			pub static mut SRC_OIDS: ORelOids = std::mem::zeroed();
			pub static mut SRC_TABLESPACE: Oid = std::mem::zeroed();

			srcOids = old_o_table->has_primary ?
				old_o_table->indices[PrimaryIndexNumber].oids :
				old_o_table->oids;
			srcTablespace = old_o_table->has_primary ?
				old_o_table->indices[PrimaryIndexNumber].tablespace :
				old_o_table->tablespace;

			//
// o_fill_tmp_table_descr() already initializes shared root info
// for the new trees via o_btree_try_use_shmem(), so we must not
// call rebuild_indices_insert_placeholders() afterwards.
//
			o_fill_tmp_table_descr(&tmp_descr, new_o_table);
			if (tbl_data_exists(&srcOids, srcTablespace))
			{
				old_descr = o_fetch_table_descr(old_o_table->oids);
				o_tables_meta_unlock_no_wal();

				if (!*recovery_single_process)
				{
					recovery_send_leader_oids(oids, InvalidIndexNumber,
											  new_o_table->version,
											  old_o_table->oids,
											  old_o_table->version,
											  true);
				}
				else
					rebuild_indices(old_o_table, old_descr,
									new_o_table, &tmp_descr, false, NULL);
			}
			else
			{
				o_tables_meta_unlock_no_wal();
			}
			o_free_tmp_table_descr(&tmp_descr);
		}
		else
		{
			pub static mut TMP_DESCR: OTableDescr = std::mem::zeroed();

			o_fill_tmp_table_descr(&tmp_descr, new_o_table);

			o_tables_meta_unlock_no_wal();
			if (ix_num < nindices && old_o_table->indices[ix_num].type != oIndexPrimary &&
				old_o_table->indices[ix_num].oids.reloid == new_o_table->indices[ix_num].oids.reloid &&
				old_o_table->indices[ix_num].oids.relnode != new_o_table->indices[ix_num].oids.relnode)
			{
				Assert(is_recovery_in_progress());

				//
// In main recovery worker send message to main index creation
// worker in dedicated recovery workers pool and exit
//
				if (!*recovery_single_process)
				{
					pub static mut INVALID_OIDS: ORelOids = std::mem::zeroed();

					// Send recovery message to become a leader
					ORelOidsSetInvalid(invalid_oids);
					recovery_send_leader_oids(oids, ix_num, new_o_table->version,
											  invalid_oids, 0, false);
				}
				else
					build_secondary_index(changed_tablespace ?
										  old_o_table->oids.relnode :
										  InvalidOid,
										  new_o_table, &tmp_descr, ix_num,
										  false, NULL);
			}
			o_free_tmp_table_descr(&tmp_descr);
		}

		pfree(old_o_table);
		pfree(new_o_table);
	}
	else
	{
		o_tables_meta_unlock_no_wal();
	}

	cur_recovery_xid_state->o_tables_meta_locked = false;
}

fn
handle_movedb(Oid dbOid, Oid src_tblspcoid, Oid dst_tblspcoid)
{
	pub static mut CHAR: *mut src_dbpath = std::ptr::null_mut();
	pub static mut CHAR: *mut dst_dbpath = std::ptr::null_mut();
	pub static mut CHAR: *mut dst_prefix = std::ptr::null_mut();
	pub static mut CHAR: *mut dst_prefix_copy = std::ptr::null_mut();
	pub static mut DIR: *mut dstdir = std::ptr::null_mut();
	pub static mut DIRENT: *mut struct xlde = std::ptr::null_mut();
	pub static mut LIST: *mut evicted = NIL;
	pub static mut LIST_CELL: *mut lc = std::ptr::null_mut();

	//
// Prepare stage of moving: check destination directory.
//
	o_get_prefixes_for_tablespace(dbOid, src_tblspcoid, NULL, &src_dbpath);
	o_get_prefixes_for_tablespace(dbOid, dst_tblspcoid, &dst_prefix, &dst_dbpath);

	dstdir = AllocateDir(dst_dbpath);
	if (dstdir != NULL)
	{
		while ((xlde = ReadDir(dstdir, dst_dbpath)) != NULL)
		{
			if (strcmp(xlde->d_name, ".") == 0 ||
				strcmp(xlde->d_name, "..") == 0)
				continue;

			ereport(ERROR,
					(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
					 errmsg("some relations of database \"%d\" are already in tablespace \"%d\"",
							dbOid, dst_tblspcoid),
					 errhint("You must move them back to the database's default tablespace before using this command.")));
		}

		FreeDir(dstdir);

		//
// The directory exists but is empty. We must remove it before using
// the copydir function.
//
		if (rmdir(dst_dbpath) != 0)
			elog(ERROR, "could not remove directory \"%s\": %m",
				 dst_dbpath);
	}

	if (InHotStandby)
	{
		//
// Lock database while we resolve conflicts to ensure that
// InitPostgres() cannot fully re-execute concurrently. This avoids
// backends re-connecting automatically to same database, which can
// happen in some cases.
//
// This will lock out walsenders trying to connect to db-specific
// slots for logical decoding too, so it's safe for us to drop slots.
//
		LockSharedObjectForSession(DatabaseRelationId, dbOid, 0, AccessExclusiveLock);
		ResolveRecoveryConflictWithDatabase(dbOid);
	}

	//
// Evict all relation related to the moved database. As soon as all
// backends are terminated and LockSharedObjectForSession is acquired, no
// new pages will appear in page pool till the and of moving database.
//
	dst_prefix_copy = pstrdup(dst_prefix);
	o_tables_evict(dbOid, &evicted);

	o_verify_dir_exists_or_create(dst_prefix_copy, NULL, NULL);
	copydir(src_dbpath, dst_dbpath, false);

	//
// Change tablespace in evicted meta data as well.
//
	foreach(lc, evicted)
	{
		pub static mut EVICTED_TREE_DATA: *mut evicted_data = std::ptr::null_mut();
		Oid			relnode = lfirst_oid(lc);

		evicted_data = read_evicted_data(dbOid, relnode, true);

		if (evicted_data != NULL)
		{
			evicted_data->freeBuf.tag.key.tablespace = dst_tblspcoid;
			evicted_data->nextChkp.tag.key.tablespace = dst_tblspcoid;
			evicted_data->tmpBuf.tag.key.tablespace = dst_tblspcoid;
			insert_evicted_data(evicted_data);
		}
	}

	list_free(evicted);
	rmtree(src_dbpath, true);

	if (InHotStandby)
	{
		//
// Release locks prior to commit. XXX There is a race condition here
// that may allow backends to reconnect, but the window for this is
// small because the gap between here and commit is mostly fairly
// small and it is unlikely that people will be dropping databases
// that we are trying to connect to anyway.
//
		UnlockSharedObjectForSession(DatabaseRelationId, dbOid, 0, AccessExclusiveLock);
	}
	pfree(dst_prefix_copy);
	pfree(src_dbpath);
	pfree(dst_dbpath);
}

fn
invalidate_typcache()
{
	pub static mut MSG: SharedInvalidationMessage = std::mem::zeroed();

	msg.cc.id = TYPEOID;
	msg.cc.dbId = InvalidOid;
	msg.cc.hashValue = 0;

	//
// check AddCatcacheInvalidationMessage() for an explanation
//
	VALGRIND_MAKE_MEM_DEFINED(&msg, sizeof(msg));

	SendSharedInvalidMessages(&msg, 1);
}

static WalParseResult
replay_check_version(const r: &mut WalReaderState)
{
	Assert(r);

	if (r->container.version > ORIOLEDB_WAL_VERSION)
	{
		// WAL from future version
		pub static mut WALPARSE_BAD_VERSION: return = std::mem::zeroed();
	}

	recoveryHeapTransactionId = InvalidTransactionId;

	pub static mut WALPARSE_OK: return = std::mem::zeroed();
}

static WalParseResult
replay_on_container(r: &mut WalReaderState)
{
	if (r->container.flags & WAL_CONTAINER_HAS_XACT_INFO)
	{
		//
// Store PG xid from WAL_CONTAINER_XACT_INFO to build
// SYS_TREES_CATALOG_XID_UNDO_LOCATION mapping in recovery for
// following logical decoding
//
		recoveryHeapTransactionId = r->container.xact_info.xid;
	}

	pub static mut WALPARSE_OK: return = std::mem::zeroed();
}

typedef struct
{
	// Input params
	pub static mut SINGLE: bool = false;
	pub static mut XLOG_REC_PTR: XLogRecPtr = std::mem::zeroed();
	pub static mut XLOG_REC_END_PTR: XLogRecPtr = std::mem::zeroed();

	// Replay state params
	pub static mut SYS_TREE_NUM: std::os::raw::c_int = 0;
	pub static mut O_TABLE_DESCR: *mut descr = std::ptr::null_mut();
	pub static mut O_INDEX_DESCR: *mut indexDescr = std::ptr::null_mut();

} ReplayWalDescCtx;

static WalParseResult
replay_on_record(r: &mut WalReaderState, rec: &mut WalRecord)
{
	ctx: &mut ReplayWalDescCtx = (ReplayWalDescCtx *) r->ctx;

	Assert(rec);

	elog(DEBUG4, "[%s] GET RTYPE %d `%s`", __func__, rec->type, wal_type_name(rec->type));

	//
// Stall hook for the
// test_checkpoint_abort_snapshot_resurrects_aborted_oxid). Blocking the
// recovery leader here stops WAL dispatch to the recovery workers while
// the walreceiver keeps writing the incoming stream to disk, so a backlog
// accumulates.  When released, the leader bursts through the backlog
// (dispatch is cheap) far ahead of the workers (apply is not), so
// get_workers_commit_ptr() stays low and the finished_list drain cannot
// remove a resurrected in-flight oxid before the deferred
// WAL_REC_ROLLBACK re-finds it (found=1) on the leader.  The test only
// needs to park on the next record after arming, so no params are pushed;
// arm match-all with pg_stopevent_set('replay_on_record', 'true').
// Runtime-gated by orioledb.enable_stopevents (off in production).
//
	STOPEVENT(STOPEVENT_REPLAY_ON_RECORD, NULL);

	switch (rec->type)
	{
		case WAL_REC_XID:
			advance_oxids(rec->oxid);
			recovery_switch_to_oxid(rec->oxid, -1);
			break;

		case WAL_REC_SWITCH_LOGICAL_XID:
			// Ignore
			break;

		case WAL_REC_COMMIT:
		case WAL_REC_ROLLBACK:
			{
				bool		commit,
							sync = false,
							needsFeedback;

				pub static mut XLOG_PTR: XLogRecPtr = ctx->xlogRecPtr + rec->offset;

				recovery_xmin = Max(recovery_xmin, rec->u.finish.xmin);
				update_run_xmin();

				Assert(ctx->sys_tree_num <= 0 || sys_tree_supports_transactions(ctx->sys_tree_num));

				commit = (rec->type == WAL_REC_COMMIT);

				Assert(rec->oxid != InvalidOXid);
				Assert(cur_recovery_xid_state != NULL);

				if (!ctx->single)
				{
					workers_send_oxid_finish(ctx->xlogRecEndPtr,
											 cur_recovery_xid_state->needs_feedback,
											 commit);
					if (cur_recovery_xid_state->systree_modified || cur_recovery_xid_state->checkpoint_xid)
					{
						sync = true;
						workers_synchronize(xlogPtr, false);
						if (cur_recovery_xid_state->invalidate_typcache)
							invalidate_typcache();
					}
				}
				else
				{
					sync = true;
					pg_atomic_write_u64(recovery_ptr, xlogPtr);
					if (cur_recovery_xid_state->invalidate_typcache)
						invalidate_typcache();

				}

				needsFeedback = ctx->single && cur_recovery_xid_state->needs_feedback;

				recovery_finish_current_oxid(commit ? COMMITSEQNO_MAX_NORMAL - 1 : COMMITSEQNO_ABORTED,
											 xlogPtr, -1, sync);
				elog(DEBUG1, "OrioleDB recovery %s transaction with oxid=" UINT64_FORMAT ". "
					 "Next WAL record starts at LSN %X/%X",
					 commit ? "committed" : "aborted", rec->oxid,
					 LSN_FORMAT_ARGS(ctx->xlogRecEndPtr));
				rec->oxid = InvalidOXid;

				if (needsFeedback)
					WalRcvForceReply();

				break;
			}

		case WAL_REC_JOINT_COMMIT:
			cur_recovery_xid_state->xid = rec->u.joint_commit.xid;
			elog(DEBUG1, "OrioleDB recovery committed transaction (xid, oxid)="
				 "(%u, " UINT64_FORMAT "). Next WAL record starts at LSN %X/%X",
				 cur_recovery_xid_state->xid, rec->oxid,
				 LSN_FORMAT_ARGS(ctx->xlogRecEndPtr));

			recovery_xmin = Max(recovery_xmin, rec->u.joint_commit.xmin);
			update_run_xmin();
			if (!cur_recovery_xid_state->in_joint_commit_list)
			{
				dlist_push_tail(&joint_commit_list,
								&cur_recovery_xid_state->joint_commit_list_node);
				cur_recovery_xid_state->in_joint_commit_list = true;
			}
			break;

		case WAL_REC_REPLAY_FEEDBACK:
			cur_recovery_xid_state->needs_feedback = true;
			break;

		case WAL_REC_RELATION:
			{
				pub static mut IX_TYPE: OIndexType = rec->u.relation.treeType;

				rec->relreplident = REPLICA_IDENTITY_DEFAULT;

				if (IS_SYS_TREE_OIDS(rec->oids))
					ctx->sys_tree_num = rec->oids.relnode;
				else
					ctx->sys_tree_num = -1;

				if (ctx->sys_tree_num > 0)
				{
					ctx->descr = NULL;
					ctx->indexDescr = NULL;
					Assert(sys_tree_get_storage_type(ctx->sys_tree_num) == BTreeStoragePersistence);
				}
				else if (ix_type == oIndexInvalid)
				{
					ctx->descr = o_fetch_table_descr(rec->oids);
					ctx->indexDescr = ctx->descr ? GET_PRIMARY(ctx->descr) : NULL;
				}
				else
				{
					Assert(ix_type == oIndexToast || ix_type == oIndexBridge);
					ctx->descr = NULL;
					ctx->indexDescr = o_fetch_index_descr(rec->oids, ix_type, false, NULL);
				}

				if (ctx->sys_tree_num == -1 && (ctx->descr || ctx->indexDescr))
				{
					pub static mut CHAR: *mut prefix = std::ptr::null_mut();
					pub static mut CHAR: *mut db_prefix = std::ptr::null_mut();
					pub static mut OIDS: ORelOids = std::mem::zeroed();
					pub static mut TABLESPACE: Oid = std::mem::zeroed();

					if (ctx->descr)
					{
						oids = GET_PRIMARY(ctx->descr)->desc.oids;
						tablespace = GET_PRIMARY(ctx->descr)->desc.tablespace;
					}
					else
					{
						Assert(ctx->indexDescr);
						oids = ctx->indexDescr->oids;
						tablespace = ctx->indexDescr->desc.tablespace;
					}
					o_get_prefixes_for_tablespace(oids.datoid, tablespace,
												  &prefix, &db_prefix);
					o_verify_dir_exists_or_create(prefix, NULL, NULL);
					o_verify_dir_exists_or_create(db_prefix, NULL, NULL);
					pfree(db_prefix);
				}

				break;
			}

		case WAL_REC_RELREPLIDENT:
			// Unused yet
			break;

		case WAL_REC_O_TABLES_META_LOCK:
			Assert(!cur_recovery_xid_state->o_tables_meta_locked);
			o_tables_meta_lock_no_wal();
			cur_recovery_xid_state->o_tables_meta_locked = true;
			elog(DEBUG3, "[%s] META_LOCK for [ %u %u %u ] ctx->sys_tree_num %d", __func__,
				 rec->oids.datoid, rec->oids.reloid, rec->oids.relnode, ctx->sys_tree_num);
			break;

		case WAL_REC_DATABASE_COPY:
			handle_movedb(rec->u.dbcopy.datOid, rec->u.dbcopy.src_tblspc, rec->u.dbcopy.dst_tblspc);
			break;

		case WAL_REC_O_TABLES_META_UNLOCK:
			{
				pub static mut XLOG_PTR: XLogRecPtr = ctx->xlogRecPtr + rec->offset;

				elog(DEBUG3, "[%s] META_UNLOCK for [ %u %u %u; old: %u ] ctx->sys_tree_num %d", __func__,
					 rec->u.unlock.oids.datoid, rec->u.unlock.oids.reloid, rec->u.unlock.oids.relnode, rec->u.unlock.oldRelnode, ctx->sys_tree_num);

				if (!ctx->single)
					workers_synchronize(xlogPtr, true);

				Assert(cur_recovery_xid_state->o_tables_meta_locked);
				handle_o_tables_meta_unlock(rec->u.unlock.oids, rec->u.unlock.oldRelnode);

				if (!ctx->single)
					workers_synchronize(xlogPtr + 1, true);

				if (!ctx->single)
					clean_workers_oids();

				break;
			}

		case WAL_REC_TRUNCATE:
			{
				pub static mut XLOG_PTR: XLogRecPtr = ctx->xlogRecPtr + rec->offset;

				if (!ctx->single)
					workers_synchronize(xlogPtr, true);

				o_truncate_table(rec->u.truncate.oids, true);

				AcceptInvalidationMessages();
				if (!ctx->single)
					clean_workers_oids();

				break;
			}

		case WAL_REC_SAVEPOINT:
			recovery_savepoint(rec->u.savepoint.parentSubid, -1);
			if (!ctx->single)
				workers_send_savepoint(rec->u.savepoint.parentSubid);
			break;

		case WAL_REC_ROLLBACK_TO_SAVEPOINT:
			{
				pub static mut XLOG_PTR: XLogRecPtr = ctx->xlogRecPtr + rec->offset;

				if (!ctx->single)
				{
					workers_send_rollback_to_savepoint(xlogPtr, rec->u.rb_to_sp.parentSubid);
					workers_synchronize(xlogPtr, false);
				}
				recovery_rollback_to_savepoint(rec->u.rb_to_sp.parentSubid, -1);
				break;
			}

		case WAL_REC_BRIDGE_ERASE:
			{
				if (ctx->indexDescr == NULL)
				{
					// nothing to do here
					pub static mut WALPARSE_OK: return = std::mem::zeroed();
				}

				if (ctx->single)
				{
					replay_erase_bridge_item(ctx->indexDescr, &rec->u.bridge_erase.iptr);
				}
				else
				{
					pub static mut HASH: uint32 = std::mem::zeroed();
					pub static mut TUPLE: OTuple = std::mem::zeroed();

					hash = o_hash_iptr(ctx->indexDescr, &rec->u.bridge_erase.iptr);
					tuple.formatFlags = 0;
					tuple.data = (Pointer) &rec->u.bridge_erase.iptr;
					worker_send_modify(GET_WORKER_ID(hash), &ctx->indexDescr->desc,
									   RecoveryMsgTypeBridgeErase, tuple, sizeof(ItemPointerData));
				}
				break;
			}

		case WAL_REC_INSERT:
		case WAL_REC_UPDATE:
		case WAL_REC_DELETE:
		case WAL_REC_REINSERT:
			{
				pub static mut SUCCESS: bool = false;
				OFixedTuple tuple1,
							tuple2;
				pub static mut XLOG_PTR: XLogRecPtr = ctx->xlogRecPtr + rec->offset;
				uint16		type = recovery_msg_from_wal_record(rec->type);
				Pointer		sys_tree_oids_ptr = rec->data + sizeof(uint8) + sizeof(OffsetNumber);

				Assert(rec->oxid != InvalidOXid);

				build_fixed_tuples(rec, &tuple1, &tuple2);

				if (ctx->sys_tree_num > 0 && ctx->xlogRecPtr >= checkpoint_state->controlSysTreesStartPtr)
				{
					Assert(sys_tree_supports_transactions(ctx->sys_tree_num));
					recovery_switch_to_oxid(rec->oxid, -1);

					cur_recovery_xid_state->systree_modified = true;
					if (IS_TYPCACHE_SYSTREE(ctx->sys_tree_num))
						cur_recovery_xid_state->invalidate_typcache = true;
					if (ctx->sys_tree_num == SYS_TREES_O_TABLES)
						Assert(cur_recovery_xid_state->o_tables_meta_locked);

					if (!ctx->single)
						workers_synchronize(xlogPtr, true);

					success = apply_sys_tree_modify_record(ctx->sys_tree_num, type,
														   tuple1.tuple, rec->oxid,
														   COMMITSEQNO_INPROGRESS);

					if (ctx->sys_tree_num == SYS_TREES_O_INDICES && success)
					{
						pub static mut O_INDEX_KEY: *mut trees = std::ptr::null_mut();
						pub static mut TMP_OIDS: ORelOids = std::mem::zeroed();

						if (type == RecoveryMsgTypeDelete)
						{
							trees = o_indices_get_trees(sys_tree_oids_ptr, &tmp_oids);
							if (trees)
								add_undo_drop_relnode(tmp_oids, trees, 1);
						}
						else if (type == RecoveryMsgTypeInsert)
						{
							trees = o_indices_get_trees(sys_tree_oids_ptr, &tmp_oids);
							if (trees)
							{
								pub static mut CHAR: *mut prefix = std::ptr::null_mut();
								pub static mut CHAR: *mut db_prefix = std::ptr::null_mut();

								//
// Ensure the per-tablespace and per-database
// orioledb data directories exist.  On the
// primary these are created in indices.c
// before the first btree file is opened.  On
// the replica we must do it here when
// replaying the SYS_TREES_O_INDICES INSERT
// that accompanies CREATE TABLE / CREATE
// INDEX.
//
								o_get_prefixes_for_tablespace(trees->oids.datoid,
															  trees->tablespace,
															  &prefix, &db_prefix);
								o_verify_dir_exists_or_create(prefix, NULL, NULL);
								o_verify_dir_exists_or_create(db_prefix, NULL, NULL);
								pfree(db_prefix);
								add_undo_create_relnode(tmp_oids, trees, 1, true);
							}
						}
					}
				}

				if (ctx->sys_tree_num > 0 || ctx->indexDescr == NULL)
				{
					// nothing to do here
					break;
				}

				if (ctx->indexDescr->desc.type == oIndexBridge)
				{
					elog(DEBUG3, "WAL change for bridge index");
				}

				Assert(!O_TUPLE_IS_NULL(tuple1.tuple));

				// Reinsert is processed as DELETE + INSERT
				if (rec->type == WAL_REC_REINSERT)
				{
					Assert(type == RecoveryMsgTypeReinsert);
					Assert(!O_TUPLE_IS_NULL(tuple2.tuple));

					if (rec->relreplident == REPLICA_IDENTITY_FULL)
					{
						pub static mut ALLOCATED: bool = false;

						//
// tuple2 (old tuple) representation is full tuple,
// not a key. We need to rewrite it with a key.
//
						tuple2.tuple = o_btree_tuple_make_key(&(GET_PRIMARY(ctx->descr))->desc, tuple2.tuple, tuple2.tuple.data, true, &allocated);
						Assert(!allocated);
					}

					if (ctx->single)
					{
						recovery_switch_to_oxid(rec->oxid, -1);
						apply_modify_record(ctx->descr, ctx->indexDescr, RecoveryMsgTypeDelete, tuple2.tuple);
						apply_modify_record(ctx->descr, ctx->indexDescr, RecoveryMsgTypeInsert, tuple1.tuple);
					}
					else
					{
						spread_idx_modify(&ctx->indexDescr->desc, RecoveryMsgTypeDelete, tuple2.tuple);
						spread_idx_modify(&ctx->indexDescr->desc, RecoveryMsgTypeInsert, tuple1.tuple);
					}
				}
				else			// WAL_REC_INSERT, WAL_REC_UPDATE or
// WAL_REC_DELETE
				{
					if (rec->relreplident == REPLICA_IDENTITY_FULL)
					{
						if (rec->type == WAL_REC_DELETE)
						{
							pub static mut ALLOCATED: bool = false;

							//
// tuple1 representation is full tuple, not a key.
// We need to rewrite it with a key.
//
							tuple1.tuple = o_btree_tuple_make_key(&(GET_PRIMARY(ctx->descr))->desc, tuple1.tuple, tuple1.tuple.data, true, &allocated);
							Assert(!allocated);
							Assert(O_TUPLE_IS_NULL(tuple2.tuple));
						}
						else if (rec->type == WAL_REC_UPDATE)
						{
							//
// tuple2 from WAL record could be safely ignored
// (it's needed only for logical decoding).
//
							Assert(!O_TUPLE_IS_NULL(tuple2.tuple));
						}
						else
						{
							Assert(O_TUPLE_IS_NULL(tuple2.tuple));
						}
					}
					else
					{
						Assert(O_TUPLE_IS_NULL(tuple2.tuple));
					}

					if (ctx->single)
					{
						recovery_switch_to_oxid(rec->oxid, -1);
						apply_modify_record(ctx->descr, ctx->indexDescr, type, tuple1.tuple);
					}
					else
					{
						spread_idx_modify(&ctx->indexDescr->desc, type, tuple1.tuple);
					}
				}

				break;
			}

		default:
			break;
	}

	pub static mut WALPARSE_OK: return = std::mem::zeroed();
}

//
// Replays a single orioledb WAL container.
//
static bool
replay_container(Pointer startPtr, Pointer endPtr,
				 bool single, XLogRecPtr xlogRecPtr, XLogRecPtr xlogRecEndPtr)
{
	ReplayWalDescCtx dctx = {
		.single = single,
		.xlogRecPtr = xlogRecPtr,
		.xlogRecEndPtr = xlogRecEndPtr,
		.sys_tree_num = -1,
		.descr = NULL,
		.indexDescr = NULL
	};

	WalReaderState r = {
		.start = startPtr,
		.end = endPtr,
		.ptr = startPtr,
		.container = {0},
		.ctx = &dctx,
		.check_version = replay_check_version,
		.on_container = replay_on_container,
		.on_record = replay_on_record
	};

	WalParseResult st = wal_parse_container(&r, true);

	if (st != WALPARSE_OK)
		pub static mut FALSE: return = std::mem::zeroed();

	update_recovery_undo_loc_flush(single, -1);
	pub static mut TRUE: return = std::mem::zeroed();
}

//
// Hook for replaying builtin commit record.  Performs joint commit.
//

o_xact_redo_hook(TransactionId xid, XLogRecPtr lsn, bool commit)
{
	pub static mut MITER: dlist_mutable_iter = std::mem::zeroed();
	pub static mut SINGLE: bool = *recovery_single_process;

	dlist_foreach_modify(miter, &joint_commit_list)
	{
		pub static mut RECOVERY_XID_STATE: *mut state = std::ptr::null_mut();
		pub static mut SYNC: bool = false;

		state = dlist_container(RecoveryXidState, joint_commit_list_node, miter.cur);
		Assert(state->in_joint_commit_list);

		if (state->xid != xid)
			continue;

		recovery_switch_to_oxid(state->oxid, -1);

		Assert(cur_recovery_xid_state != NULL);
		if (!single)
		{
			workers_send_oxid_finish(lsn,
									 cur_recovery_xid_state->needs_feedback,
									 commit);
			if (cur_recovery_xid_state->systree_modified ||
				cur_recovery_xid_state->checkpoint_xid)
			{
				sync = true;
				workers_synchronize(lsn, false);
				if (cur_recovery_xid_state->invalidate_typcache)
					invalidate_typcache();
			}
		}
		else
		{
			sync = true;
			pg_atomic_write_u64(recovery_ptr, lsn);
			if (cur_recovery_xid_state->invalidate_typcache)
				invalidate_typcache();
		}

		dlist_delete_from_thoroughly(&joint_commit_list, miter.cur);
		state->in_joint_commit_list = false;

		recovery_finish_current_oxid(commit ? COMMITSEQNO_MAX_NORMAL - 1 : COMMITSEQNO_ABORTED,
									 lsn, -1, sync);
		break;
	}
}

//
// Sends the message to a worker.
//
fn
worker_send_msg(int worker_id, Pointer msg, uint64 msg_size)
{
	pub static mut RECOVERY_WORKER_STATE: *mut state = &workers_pool[worker_id];

	Assert(workers_pool);
	Assert(state);
	Assert(state->handle);
	if ((RECOVERY_QUEUE_BUF_SIZE - state->queue_buf_len) < msg_size)
		worker_queue_flush(worker_id);

	memcpy(state->queue_buf + state->queue_buf_len, msg, msg_size);
	state->queue_buf_len += msg_size;
}

fn
delay_if_queued_for_idxbuild()
{
	while (idxbuild_oids_hash)
	{
		pub static mut HASH_SEQ: HASH_SEQ_STATUS = std::mem::zeroed();
		pub static mut RECOVERY_IDX_BUILD_QUEUE_STATE: *mut cur = std::ptr::null_mut();

		//
// This function might be called by a startup process and by a
// recovery worker, therefore check in which worker we are.
//
		if (AmStartupProcess())
#if PG_VERSION_NUM >= 180000
			ProcessStartupProcInterrupts();
#else
			HandleStartupProcInterrupts();
#endif
		else
			o_worker_handle_interrupts();

		// Remove hash entries for completed indexes
		hash_seq_init(&hash_seq, idxbuild_oids_hash);
		while ((cur = (RecoveryIdxBuildQueueState *) hash_seq_search(&hash_seq)) != NULL)
		{
			if (cur->position <= pg_atomic_read_u64(recovery_index_completed_pos))
				hash_search(idxbuild_oids_hash, &cur->oids, HASH_REMOVE, NULL);
		}

		// All completed ?
		if (hash_get_num_entries(idxbuild_oids_hash) == 0)
			break;

		//
// We wait on a condition variable that will wake us as soon as the
// pause ends, but we use a timeout so we can check the
// HandleStartupProcInterrupts() periodically too.
//
		ConditionVariableTimedSleep(recovery_index_cv, 1000,
									WAIT_EVENT_PARALLEL_CREATE_INDEX_SCAN);
	}
	ConditionVariableCancelSleep();
}

fn
delay_rels_queued_for_idxbuild(ORelOids oids)
{
	pub static mut RECOVERY_IDX_BUILD_QUEUE_STATE: *mut hash_elem = std::ptr::null_mut();
	pub static mut FOUND: bool = false;

	//
// Delay modify requests if indexes for the relation are requested to be
// build but haven't been built yet
//
	while (true)
	{
		//
// This function might be called by a startup process and by a
// recovery worker, therefore check in which worker we are.
//
		if (AmStartupProcess())
#if PG_VERSION_NUM >= 180000
			ProcessStartupProcInterrupts();
#else
			HandleStartupProcInterrupts();
#endif
		else
			o_worker_handle_interrupts();

		hash_elem = (RecoveryIdxBuildQueueState *) hash_search(idxbuild_oids_hash,
															   &oids,
															   HASH_FIND,
															   &found);
		if (!found)
		{
			ConditionVariableBroadcast(recovery_index_cv);
			break;
		}

		if (hash_elem->position <= pg_atomic_read_u64(recovery_index_completed_pos))
		{
			// Remove completed index build and repeat hash search
			hash_elem = (RecoveryIdxBuildQueueState *) hash_search(idxbuild_oids_hash,
																   &oids,
																   HASH_REMOVE,
																   &found);
		}
		else
		{
			//
// We wait on a condition variable that will wake us as soon as
// the pause ends, but we use a timeout so we can check the
// HandleStartupProcInterrupts() periodically too.
//
			ConditionVariableTimedSleep(recovery_index_cv, 1000,
										WAIT_EVENT_PARALLEL_CREATE_INDEX_SCAN);
		}
	}
	ConditionVariableCancelSleep();
}

//
// Sends modify message to a worker.
//
fn
worker_send_modify(int worker_id, desc: &mut BTreeDescr,
				   RecoveryMsgType recType,
				   OTuple tuple, int tuple_len)
{
	pub static mut RECOVERY_MSG_HEADER: *mut header = std::ptr::null_mut();
	pub static mut RECOVERY_WORKER_STATE: *mut state = &workers_pool[worker_id];
	pub static mut DATA: Pointer = std::ptr::null_mut();
	pub static mut MAX_MSG_SIZE: std::os::raw::c_int = 0;
	pub static mut OIDS: ORelOids = std::mem::zeroed();
	pub static mut TYPE: OIndexType = std::mem::zeroed();

	if (!IS_SYS_TREE_OIDS(desc->oids))
	{
		if (desc->type == oIndexPrimary)
		{
			id: &mut OIndexDescr = (OIndexDescr *) desc->arg;

			oids = id->tableOids;
			type = oIndexInvalid;
		}
		else
		{
			Assert(desc->type == oIndexToast || desc->type == oIndexBridge);
			oids = desc->oids;
			type = desc->type;
		}
	}
	else
	{
		oids = desc->oids;
		type = oIndexPrimary;
		Assert(desc->type == oIndexPrimary);
	}

	delay_rels_queued_for_idxbuild(oids);

	max_msg_size = MAXALIGN(sizeof(RecoveryMsgHeader) + sizeof(OXid)
							+ sizeof(ORelOids) + 1
							+ sizeof(int) + 1) + MAXALIGN(tuple_len);

	Assert(recType == RecoveryMsgTypeInsert ||
		   recType == RecoveryMsgTypeUpdate ||
		   recType == RecoveryMsgTypeDelete ||
		   recType == RecoveryMsgTypeBridgeErase);

	if (RECOVERY_QUEUE_BUF_SIZE - state->queue_buf_len < max_msg_size)
		worker_queue_flush(worker_id);

	data = state->queue_buf + state->queue_buf_len;
	header = (RecoveryMsgHeader *) data;
	header->type = recType;
	data += sizeof(RecoveryMsgHeader);
	state->queue_buf_len += sizeof(RecoveryMsgHeader);

	Assert(cur_recovery_xid_state || recType == RecoveryMsgTypeBridgeErase);
	if (recType != RecoveryMsgTypeBridgeErase &&
		state->oxid != cur_recovery_xid_state->oxid)
	{
		memcpy(data, &cur_recovery_xid_state->oxid, sizeof(OXid));
		data += sizeof(OXid);
		state->queue_buf_len += sizeof(OXid);
		header->type |= RECOVERY_MODIFY_OXID;
		state->oxid = cur_recovery_xid_state->oxid;
		cur_recovery_xid_state->used_by[worker_id] = true;
	}

	if (!ORelOidsIsEqual(state->oids, oids) || state->type != type)
	{
		memcpy(data, &oids, sizeof(ORelOids));
		data += sizeof(ORelOids);
		*data = type;
		data++;
		state->queue_buf_len += sizeof(ORelOids) + 1;
		header->type |= RECOVERY_MODIFY_OIDS;
		state->oids = oids;
		state->type = type;
	}

	if (recType != RecoveryMsgTypeBridgeErase)
	{
		memcpy(data, &tuple_len, sizeof(int));
		data += sizeof(int);
		memcpy(data, &tuple.formatFlags, 1);

		state->queue_buf_len += sizeof(int) + 1;
		state->queue_buf_len = MAXALIGN(state->queue_buf_len);
		data = state->queue_buf + state->queue_buf_len;

		memcpy(data, tuple.data, tuple_len);
		state->queue_buf_len += tuple_len;
		state->queue_buf_len = MAXALIGN(state->queue_buf_len);
	}
	else
	{
		memcpy(data, tuple.data, sizeof(ItemPointerData));
		state->queue_buf_len += sizeof(ItemPointerData);
		state->queue_buf_len = MAXALIGN(state->queue_buf_len);
	}
	Assert(state->queue_buf_len <= RECOVERY_QUEUE_BUF_SIZE);
}

//
// Sends recovery finish message to all workers in the pool.
//
fn
workers_send_finish(bool send_to_idx_pool)
{
	pub static mut FINISH_MSG: RecoveryMsgEmpty = std::mem::zeroed();
	pub static mut RECOVERY_WORKER_STATE: *mut state = std::ptr::null_mut();
	pub static mut I: std::os::raw::c_int = 0;
	int			start,
				finish;

	if (send_to_idx_pool)
	{
		Assert(recovery_idx_pool_size_guc);
		start = index_build_first_worker;
		finish = index_build_last_worker;
	}
	else
	{
		start = 0;
		finish = recovery_idx_pool_size_guc ? index_build_leader : recovery_last_worker;
	}

	for (i = start; i <= finish; i++)
	{
		state = &workers_pool[i];

		finish_msg.header.type = RecoveryMsgTypeFinished;
		if (RECOVERY_QUEUE_BUF_SIZE - state->queue_buf_len < sizeof(RecoveryMsgEmpty))
			worker_queue_flush(i);

		memcpy(state->queue_buf + state->queue_buf_len, &finish_msg, sizeof(RecoveryMsgEmpty));
		state->queue_buf_len += sizeof(RecoveryMsgEmpty);
		worker_queue_flush(i);
	}
}

//
// Sends savepoint message to workers with active the oxid in the pool.
//
fn
workers_send_savepoint(SubTransactionId parentSubId)
{
	pub static mut RECOVERY_WORKER_STATE: *mut state = std::ptr::null_mut();
	pub static mut MSG: RecoveryMsgSavepoint = std::mem::zeroed();
	pub static mut I: std::os::raw::c_int = 0;

	Assert(cur_recovery_xid_state);

	msg.header.type = RecoveryMsgTypeSavepoint;
	msg.oxid = cur_recovery_xid_state->oxid;
	msg.parentSubId = parentSubId;

	for (i = 0; i < recovery_pool_size_guc; i++)
	{
		if (cur_recovery_xid_state->used_by[i])
		{
			state = &workers_pool[i];
			state->oxid = InvalidOXid;

			worker_send_msg(i, (Pointer) &msg, sizeof(msg));

			if (EnableHotStandby)
			{
				// we need to apply recovery records as fast as we can
				worker_queue_flush(i);
			}
		}
	}
}

//
// Sends rollback to savepoint message to workers with active the oxid in the pool.
//
fn
workers_send_rollback_to_savepoint(XLogRecPtr ptr,
								   SubTransactionId parentSubId)
{
	pub static mut RECOVERY_WORKER_STATE: *mut state = std::ptr::null_mut();
	pub static mut MSG: RecoveryMsgRollbackToSavepoint = std::mem::zeroed();
	pub static mut I: std::os::raw::c_int = 0;

	Assert(cur_recovery_xid_state);

	msg.header.type = RecoveryMsgTypeRollbackToSavepointt;
	msg.oxid = cur_recovery_xid_state->oxid;
	msg.ptr = ptr;
	msg.parentSubId = parentSubId;

	for (i = 0; i < recovery_pool_size_guc; i++)
	{
		if (cur_recovery_xid_state->used_by[i])
		{
			state = &workers_pool[i];
			state->oxid = InvalidOXid;

			worker_send_msg(i, (Pointer) &msg, sizeof(msg));

			if (EnableHotStandby)
			{
				// we need to apply recovery records as fast as we can
				worker_queue_flush(i);
			}
		}
	}
	pg_atomic_write_u64(recovery_ptr, ptr);
}

//
// Sends commit or rollback message to workers with active the oxid in the pool.
//
fn
workers_send_oxid_finish(XLogRecPtr ptr, bool needsFeedback, bool commit)
{
	pub static mut RECOVERY_WORKER_STATE: *mut state = std::ptr::null_mut();
	pub static mut OXID_PTR_RECORD: RecoveryMsgOXidPtr = std::mem::zeroed();
	pub static mut I: std::os::raw::c_int = 0;

	oxid_ptr_record.header.type = commit ? RecoveryMsgTypeCommit : RecoveryMsgTypeRollback;
	oxid_ptr_record.oxid = cur_recovery_xid_state->oxid;
	oxid_ptr_record.ptr = ptr;
	oxid_ptr_record.needsFeedback = needsFeedback;

	for (i = 0; i < recovery_pool_size_guc; i++)
	{
		//
// Notify workers who participated in the current transaction.  For
// transactions that participated in the checkpoint xids file, we
// notify them by phone because all the works read the xids file and
// need to update their local hashes.
//
		if (cur_recovery_xid_state->used_by[i] ||
			cur_recovery_xid_state->checkpoint_xid)
		{
			state = &workers_pool[i];

			//
// Unconditionally reset cached oxid.  The worker will call
// recovery_switch_to_oxid() when processing this message,
// changing its recovery_oxid regardless of what was cached. We
// must invalidate our cache to match, so that the next modify
// message always sends the oxid explicitly.
//
			state->oxid = InvalidOXid;

			worker_send_msg(i, (Pointer) &oxid_ptr_record, sizeof(oxid_ptr_record));

			if (EnableHotStandby)
			{
				// we need to apply recovery records as fast as we can
				worker_queue_flush(i);
			}
		}
	}
	pg_atomic_write_u64(recovery_ptr, ptr);
}

//
// Synchronize execution with workers.
//
// Actually used only before delete a relnode. We can hold a list of relnodes
// used by workers and synchronize only with needed workers. But we assume that
// it does not happen too often and we can use this simple solution.
//
fn
workers_synchronize(XLogRecPtr ptr, bool send_synchronize)
{
	pub static mut I: std::os::raw::c_int = 0;

	if (send_synchronize)
	{
		pub static mut SYNC_MSG: RecoveryMsgPtr = std::mem::zeroed();

		sync_msg.header.type = RecoveryMsgTypeSynchronize;
		sync_msg.ptr = ptr;
		for (i = 0; i < recovery_pool_size_guc; i++)
		{
			worker_send_msg(i, (Pointer) &sync_msg, sizeof(sync_msg));
			worker_queue_flush(i);
		}
		pg_atomic_write_u64(recovery_ptr, ptr);
	}

	for (i = 0; i < recovery_pool_size_guc && !unexpected_worker_detach; i++)
	{
		pub static mut J: std::os::raw::c_int = 0;

		while (pg_atomic_read_u64(&worker_ptrs[i].commitPtr) < ptr &&
			   workers_pool[i].queue)
		{
			pub static mut STATUS: BgwHandleStatus = std::mem::zeroed();
			pub static mut PID: pid_t = std::mem::zeroed();

			pg_usleep(10);

			if (j % 100 == 0)
			{
				status = GetBackgroundWorkerPid(workers_pool[i].handle, &pid);
				if (status != BGWH_STARTED && status != BGWH_NOT_YET_STARTED)
				{
					unexpected_worker_detach = true;
					break;
				}
			}
			j++;
		}
	}
}

//
// Notify workers that toast reached consistent state.
//
fn
workers_notify_toast_consistent()
{
	pub static mut MSG: RecoveryMsgEmpty = std::mem::zeroed();
	pub static mut I: std::os::raw::c_int = 0;

	msg.header.type = RecoveryMsgTypeToastConsistent;

	for (i = 0; i < recovery_pool_size_guc; i++)
	{
		worker_send_msg(i, (Pointer) &msg, sizeof(msg));
		worker_queue_flush(i);
	}
}

//
// Flushes a queue buffer to the queue.
//
fn
worker_queue_flush(int worker_id)
{
	pub static mut RECOVERY_WORKER_STATE: *mut state = &workers_pool[worker_id];
	pub static mut RESULT: shm_mq_result = std::mem::zeroed();

	result = shm_mq_send(state->queue, state->queue_buf_len, state->queue_buf, false, true);
	state->queue_buf_len = 0;
	Assert(result != SHM_MQ_WOULD_BLOCK);
	if (result == SHM_MQ_DETACHED)
	{
		unexpected_worker_detach = true;
		return;
	}
	Assert(result == SHM_MQ_SUCCESS);
}

//
// Applies recovery record to o_tables.
//
// We do it by master process to avoid concurrent issues such as:
//
// Worker can not fetch table description because another worker does not
// commit transaction yet.
//
static bool
apply_sys_tree_modify_record(int sys_tree_num, uint16 type, OTuple tup,
							 OXid oxid, CommitSeqNo csn)
{
	pub static mut RESULT: bool = false;

	result = apply_btree_modify_record(get_sys_tree(sys_tree_num),
									   type, tup, oxid, csn);

	pub static mut RESULT: return = std::mem::zeroed();
}

//
// Spreads the index modify recovery record to the recovery workers pool.
//
// Tuples with a same key will be processed by a same worker. This approach
// helps to apply recovery records for tuples in the right order.
//
static inline 
spread_idx_modify(desc: &mut BTreeDescr, RecoveryMsgType recType, OTuple rec)
{
	pub static mut PG_USED_FOR_ASSERTS_ONLY: OTuple		key = std::mem::zeroed();
	pub static mut HASH: uint32 = std::mem::zeroed();
	int			key_len,
				tup_len;
	pub static mut PG_USED_FOR_ASSERTS_ONLY: bool		key_pfree = std::mem::zeroed();

	switch (recType)
	{
		case RecoveryMsgTypeInsert:
		case RecoveryMsgTypeUpdate:
			tup_len = o_btree_len(desc, rec, OTupleLength);
			hash = o_btree_hash(desc, rec, BTreeKeyLeafTuple);
#ifdef USE_ASSERT_CHECKING
			key = o_btree_tuple_make_key(desc, rec, NULL, true, &key_pfree);
			Assert(hash == o_btree_hash(desc, key, BTreeKeyNonLeafKey));
			if (key_pfree)
				pfree(key.data);
#endif
			worker_send_modify(GET_WORKER_ID(hash), desc,
							   recType, rec, tup_len);
			break;
		case RecoveryMsgTypeDelete:
			key_len = o_btree_len(desc, rec, OKeyLength);
			hash = o_btree_hash(desc, rec, BTreeKeyNonLeafKey);
			worker_send_modify(GET_WORKER_ID(hash), desc, recType,
							   rec, key_len);
			break;
		default:
			Assert(false);
	}
}

//
// Converts wal record type to recovery message type.
//
static inline RecoveryMsgType
recovery_msg_from_wal_record(WalRecordType rec_type)
{
	switch (rec_type)
	{
		case WAL_REC_INSERT:
			pub static mut RECOVERY_MSG_TYPE_INSERT: return = std::mem::zeroed();
		case WAL_REC_DELETE:
			pub static mut RECOVERY_MSG_TYPE_DELETE: return = std::mem::zeroed();
		case WAL_REC_UPDATE:
			pub static mut RECOVERY_MSG_TYPE_UPDATE: return = std::mem::zeroed();
		case WAL_REC_BRIDGE_ERASE:
			pub static mut RECOVERY_MSG_TYPE_BRIDGE_ERASE: return = std::mem::zeroed();
		case WAL_REC_REINSERT:

			//
// Temporary one for convenience. Splits down to
// RecoveryMsgTypeInsert + RecoveryMsgTypeDelete
//
			pub static mut RECOVERY_MSG_TYPE_REINSERT: return = std::mem::zeroed();
		default:
			Assert(false);
			elog(ERROR, "Wrong WAL record modify type %d", rec_type);
	}
	return (uint16) 0;			// keep compiler quiet
}

static bool
is_process_running(pid_t pid)
{
	if (kill(pid, 0) == 0)
		pub static mut TRUE: return = std::mem::zeroed();

	if (errno == ESRCH)
		pub static mut FALSE: return = std::mem::zeroed();
	else if (errno == EPERM)
		pub static mut TRUE: return = std::mem::zeroed();
	else
		pub static mut FALSE: return = std::mem::zeroed();
}

//
// Check from non-recovery process that recovery workers are finished.
//
bool
check_recovery_workers_finished()
{
	pub static mut FINISH: std::os::raw::c_int = recovery_idx_pool_size_guc ? index_build_leader : recovery_last_worker;
	pub static mut I: std::os::raw::c_int = 0;

	for (i = recovery_first_worker; i <= finish; i++)
	{
		mq: &mut shm_mq = GET_WORKER_QUEUE(i);
		receiver: &mut PGPROC = shm_mq_get_receiver(mq);

		if (receiver && receiver->pid > 0 && is_process_running(receiver->pid))
		{
			pub static mut FALSE: return = std::mem::zeroed();
		}
	}
	pub static mut TRUE: return = std::mem::zeroed();
}