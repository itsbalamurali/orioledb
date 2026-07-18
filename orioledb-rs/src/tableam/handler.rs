use crate::access::heapam;
use crate::access::heaptoast;
use crate::access::multixact;
use crate::access::reloptions;
use crate::access::tableam;
use crate::btree::btree;
use crate::btree::io;
use crate::btree::iterator;
use crate::btree::scan;
use crate::btree::undo;
use crate::catalog::heap;
use crate::catalog::index;
use crate::catalog::indices;
use crate::catalog::namespace;
use crate::catalog::o_indices;
use crate::catalog::o_sys_cache;
use crate::catalog::o_tables;
use crate::catalog::pg_am;
use crate::catalog::pg_collation;
use crate::catalog::storage;
use crate::catalog::storage_xlog;
use crate::commands::progress;
use crate::commands::vacuum;
use crate::common::relpath;
use crate::funcapi;
use crate::math;
use crate::nodes::execnodes;
use crate::optimizer::optimizer;
use crate::optimizer::plancat;
use crate::orioledb;
use crate::parser::parse_coerce;
use crate::parser::parse_relation;
use crate::parser::parse_type;
use crate::parser::parsetree;
use crate::pgstat;
use crate::recovery::wal;
use crate::replication::origin;
use crate::storage::bufmgr;
use crate::sys::stat;
use crate::tableam::descr;
use crate::tableam::handler;
use crate::tableam::operations;
use crate::tableam::tree;
use crate::tableam::vacuum;
use crate::tcop::utility;
use crate::transam::oxid;
use crate::tuple::slot;
use crate::utils::backend_progress;
use crate::utils::builtins;
use crate::utils::compress;
use crate::utils::datum;
use crate::utils::fmgroids;
use crate::utils::lsyscache;
use crate::utils::rel;
use crate::utils::sampling;
use crate::utils::stopevent;
use crate::utils::syscache;
use pgrx::pg_sys;

// -------------------------------------------------------------------------
//
// handler.c
// Implementation of table access method handler
//
// Copyright (c) 2021-2026, Oriole DB Inc.
// Copyright (c) 2025-2026, Supabase Inc.
//
// IDENTIFICATION
// contrib/orioledb/src/tableam/handler.c
//
// -------------------------------------------------------------------------
//

static Size orioledb_parallelscan_estimate(Relation rel);
static Size orioledb_parallelscan_initialize(Relation rel, ParallelTableScanDesc pscan);
fn orioledb_parallelscan_reinitialize(Relation rel, ParallelTableScanDesc pscan);

pub static mut IN_NONTRANSACTIONAL_TRUNCATE: bool = false;

typedef struct OScanDescData
{
	TableScanDescData rs_base;	// AM independent part of the descriptor
	pub static mut B_TREE_SEQ_SCAN: *mut scan = std::ptr::null_mut();
	pub static mut O_SNAPSHOT: OSnapshot = std::mem::zeroed();
	pub static mut IPTR: ItemPointerData = std::mem::zeroed();
} OScanDescData;
pub static mut O_SCAN_DESC_DATA: *mut typedef OScanDesc = std::ptr::null_mut();

//
// Operation with indices. It does not update TOAST BTree. Implementations
// are in tableam_handler.c.
//
fn get_keys_from_rowid(primary: &mut OIndexDescr, Datum pkDatum, key: &mut OBTreeKeyBound,
								hint: &mut BTreeLocationHint, csn: &mut CommitSeqNo,
								version: &mut uint32, bridge_ctid: &mut ItemPointer);
fn rowid_set_csn(id: &mut OIndexDescr, Datum pkDatum, CommitSeqNo csn);

// ------------------------------------------------------------------------
// SQL functions
// ------------------------------------------------------------------------
//
Datum		orioledb_tableam_handler(PG_FUNCTION_ARGS);

PG_FUNCTION_INFO_V1(orioledb_tableam_handler);

// ------------------------------------------------------------------------
// Slot related callbacks for heap AM
// ------------------------------------------------------------------------
//

static const TupleTableSlotOps *
orioledb_slot_callbacks(Relation relation)
{
	// TODO: Create own TupleTableSlotOps
	return &TTSOpsOrioleDB;
}

// ------------------------------------------------------------------------
// Index Scan Callbacks for orioledb AM
// ------------------------------------------------------------------------
//

//
// Descriptor for fetches from orioledb table.
//
typedef struct OrioledbIndexFetchData
{
	IndexFetchTableData xs_base;	// AM independent part of the descriptor
	pub static mut BRIDGED_TUPLE: bool = false;
} OrioledbIndexFetchData;

//
// Returns NULL to prevent index scan from inside of standard_planner
// for greater and lower where clauses.
//
static IndexFetchTableData *
orioledb_index_fetch_begin(Relation rel, Relation indexRel)
{
	o_scan: &mut OrioledbIndexFetchData = palloc0(sizeof(OrioledbIndexFetchData));
	options: &mut OBTOptions = (OBTOptions *) indexRel->rd_options;

	o_serializable_lock_relation(RelationGetRelid(rel));

	o_scan->bridged_tuple = (indexRel->rd_rel->relam != BTREE_AM_OID) ||
		(options && !options->orioledb_index);

	o_scan->xs_base.rel = rel;

	return &o_scan->xs_base;
}

fn
orioledb_index_fetch_reset(scan: &mut IndexFetchTableData)
{
}

fn
orioledb_index_fetch_end(scan: &mut IndexFetchTableData)
{
	o_scan: &mut OrioledbIndexFetchData = (OrioledbIndexFetchData *) scan;

	orioledb_index_fetch_reset(scan);

	pfree(o_scan);
}

static bool
orioledb_index_fetch_tuple(struct scan: &mut IndexFetchTableData,
						   Datum tupleid,
						   bool is_rowid,
						   Snapshot snapshot,
						   slot: &mut TupleTableSlot,
						   call_again: &mut bool, all_dead: &mut bool)
{
	o_scan: &mut OrioledbIndexFetchData = (OrioledbIndexFetchData *) scan;
	pub static mut O_TABLE_DESCR: *mut descr = std::ptr::null_mut();
	pub static mut PKEY: OBTreeKeyBound = std::mem::zeroed();
	pub static mut TUPLE: OTuple = std::mem::zeroed();
	BTreeLocationHint hint = {OInvalidInMemoryBlkno, 0};
	pub static mut CSN: CommitSeqNo = std::mem::zeroed();
	pub static mut O_SNAPSHOT: OSnapshot = std::mem::zeroed();
	pub static mut TUPLE_CSN: CommitSeqNo = std::mem::zeroed();
	pub static mut VERSION: uint32 = std::mem::zeroed();

	Assert(slot->tts_ops == &TTSOpsOrioleDB);

	*call_again = false;
	if (all_dead)
		*all_dead = false;

	descr = relation_get_descr(scan->rel);
	Assert(descr != NULL);

	if (o_scan->bridged_tuple)
	{
		pub static mut BRIDGE_BOUND: OBTreeKeyBound = std::mem::zeroed();
		pub static mut BRIDGE_TUP: OTuple = std::mem::zeroed();

		if (is_rowid)
		{
			pub static mut BYTEA: *mut rowid = std::ptr::null_mut();
			pub static mut P: Pointer = std::ptr::null_mut();
			pub static mut O_ROW_ID_BRIDGE_DATA: *mut bridgeData = std::ptr::null_mut();

			Assert(GET_PRIMARY(descr)->bridging);
			rowid = DatumGetByteaP(tupleid);
			p = (Pointer) rowid + MAXALIGN(VARHDRSZ);
			if (!GET_PRIMARY(descr)->primaryIsCtid)
			{
				p += MAXALIGN(sizeof(ORowIdAddendumNonCtid));
			}
			else
			{
				p += MAXALIGN(sizeof(ORowIdAddendumCtid));
				p += MAXALIGN(sizeof(ItemPointerData));
			}
			bridgeData = (ORowIdBridgeData *) p;
			tupleid = ItemPointerGetDatum(&bridgeData->bridgeCtid);
		}

		bridge_bound.nkeys = 1;
		bridge_bound.n_row_keys = 0;
		bridge_bound.row_keys = NULL;
		bridge_bound.keys[0].value = tupleid;
		bridge_bound.keys[0].type = TIDOID;
		bridge_bound.keys[0].flags = O_VALUE_BOUND_PLAIN_VALUE;
		bridge_bound.keys[0].comparator = NULL;
		bridge_bound.keys[0].exclusion_fn = NULL;
		csn = COMMITSEQNO_INPROGRESS;

		bridge_tup = o_btree_find_tuple_by_key(&descr->bridge->desc,
											   (Pointer) &bridge_bound, BTreeKeyBound,
											   &o_in_progress_snapshot, &tupleCsn,
											   slot->tts_mcxt, NULL);
		if (O_TUPLE_IS_NULL(bridge_tup))
			pub static mut FALSE: return = std::mem::zeroed();

		o_fill_pindex_tuple_key_bound(&descr->bridge->desc, bridge_tup, &pkey);
	}
	else
		get_keys_from_rowid(GET_PRIMARY(descr), tupleid, &pkey, &hint,
							&csn, &version, NULL);
	O_LOAD_SNAPSHOT_CSN(&oSnapshot, csn);

	tuple = o_btree_find_tuple_by_key(&GET_PRIMARY(descr)->desc,
									  (Pointer) &pkey,
									  BTreeKeyBound,
									  &oSnapshot, &tupleCsn,
									  slot->tts_mcxt,
									  &hint);

	if (O_TUPLE_IS_NULL(tuple))
		pub static mut FALSE: return = std::mem::zeroed();

	tts_orioledb_store_tuple(slot, tuple, descr, tupleCsn,
							 PrimaryIndexNumber, true, &hint);
	slot->tts_tableOid = descr->oids.reloid;

	// FIXME?
	if (snapshot->snapshot_type == SNAPSHOT_DIRTY)
		snapshot->xmin = snapshot->xmax = InvalidTransactionId;

	pub static mut TRUE: return = std::mem::zeroed();
}

// ------------------------------------------------------------------------
// Callbacks for non-modifying operations on individual tuples for heap AM
// ------------------------------------------------------------------------
//

static TupleFetchCallbackResult
fetch_row_version_callback(OTuple tuple, OXid tupOxid, oSnapshot: &mut OSnapshot,
						    *arg, bool oxidIsFinished)
{
	uint32		version = *((uint32 *) arg);

	if (oxidIsFinished)
		pub static mut O_TUPLE_FETCH_NEXT: return = std::mem::zeroed();

	if (!(COMMITSEQNO_IS_INPROGRESS(oSnapshot->csn) &&
		  tupOxid == get_current_oxid_if_any()))
		pub static mut O_TUPLE_FETCH_NEXT: return = std::mem::zeroed();

	if (o_tuple_get_version(tuple) <= version)
		pub static mut O_TUPLE_FETCH_MATCH: return = std::mem::zeroed();
	else
		pub static mut O_TUPLE_FETCH_NEXT: return = std::mem::zeroed();
}

//
// Fetches last committed row version for given tupleid.
//
static bool
orioledb_fetch_row_version(Relation relation,
						   Datum tupleid,
						   Snapshot snapshot,
						   slot: &mut TupleTableSlot)
{
	pub static mut PKEY: OBTreeKeyBound = std::mem::zeroed();
	pub static mut O_TABLE_DESCR: *mut descr = std::ptr::null_mut();
	pub static mut TUPLE: OTuple = std::mem::zeroed();
	pub static mut HINT: BTreeLocationHint = std::mem::zeroed();
	pub static mut CSN: CommitSeqNo = std::mem::zeroed();
	pub static mut O_SNAPSHOT: OSnapshot = std::mem::zeroed();
	pub static mut TUPLE_CSN: CommitSeqNo = std::mem::zeroed();
	pub static mut VERSION: uint32 = std::mem::zeroed();
	pub static mut DELETED: bool = false;

	descr = relation_get_descr(relation);
	descr->noInvalidation = true;
	Assert(descr != NULL);

	get_keys_from_rowid(GET_PRIMARY(descr), tupleid, &pkey, &hint,
						&csn, &version, NULL);
	O_LOAD_SNAPSHOT_CSN(&oSnapshot, csn);

	tuple = o_btree_find_tuple_by_key_cb(&GET_PRIMARY(descr)->desc,
										 (Pointer) &pkey,
										 BTreeKeyBound,
										 &oSnapshot, &tupleCsn,
										 slot->tts_mcxt,
										 &hint,
										 &deleted,
										 fetch_row_version_callback,
										 &version);
	descr->noInvalidation = false;

	if (deleted && COMMITSEQNO_IS_INPROGRESS(tupleCsn) && snapshot != SnapshotAny)
		pub static mut TRUE: return = std::mem::zeroed();

	if (O_TUPLE_IS_NULL(tuple))
		pub static mut FALSE: return = std::mem::zeroed();

	tts_orioledb_store_tuple(slot, tuple, descr, tupleCsn,
							 PrimaryIndexNumber, true, &hint);
	slot->tts_tableOid = RelationGetRelid(relation);

	pub static mut TRUE: return = std::mem::zeroed();
}

static bool
orioledb_tuple_tid_valid(TableScanDesc scan, ItemPointer tid)
{
	ereport(ERROR,
			(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
			 errmsg("orioledb does not support TID scan"),
			 errhint("Use a primary key scan instead.")));
	pub static mut FALSE: return = std::mem::zeroed();
}

fn
orioledb_set_tidrange(TableScanDesc sscan, ItemPointer mintid, ItemPointer maxtid)
{
	ereport(ERROR,
			(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
			 errmsg("orioledb does not support TID range scan")));
}

// Just to be safe. Normally this function should not be called before orioledb_set_tidrange.
static bool
orioledb_getnextslot_tidrange(TableScanDesc sscan, ScanDirection direction,
							  slot: &mut TupleTableSlot)
{
	ereport(ERROR,
			(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
			 errmsg("orioledb does not support TID range scan")));
	pub static mut FALSE: return = std::mem::zeroed();
}

static bool
orioledb_tuple_satisfies_snapshot(Relation rel, slot: &mut TupleTableSlot,
								  Snapshot snapshot)
{
	pub static mut PKEY: OBTreeKeyBound = std::mem::zeroed();
	pub static mut O_TABLE_DESCR: *mut descr = std::ptr::null_mut();
	pub static mut TUPLE: OTuple = std::mem::zeroed();
	pub static mut TUPLE_CSN: CommitSeqNo = std::mem::zeroed();
	BTreeLocationHint hint = {OInvalidInMemoryBlkno, 0};

	if (snapshot != SnapshotSelf)
		pub static mut TRUE: return = std::mem::zeroed();

	descr = relation_get_descr(rel);
	Assert(descr != NULL);

	tts_orioledb_fill_key_bound(slot, GET_PRIMARY(descr), &pkey);

	tuple = o_btree_find_tuple_by_key(&GET_PRIMARY(descr)->desc,
									  (Pointer) &pkey,
									  BTreeKeyBound,
									  &o_in_progress_snapshot,
									  &tupleCsn,
									  slot->tts_mcxt,
									  &hint);

	if (O_TUPLE_IS_NULL(tuple))
		pub static mut FALSE: return = std::mem::zeroed();

	tts_orioledb_store_tuple(slot, tuple, descr, tupleCsn,
							 PrimaryIndexNumber, true, &hint);
	slot->tts_tableOid = RelationGetRelid(rel);

	pub static mut TRUE: return = std::mem::zeroed();

}

#if PG_VERSION_NUM >= 180000
// OrioleDB doesn't store xmin in tuples, just return false
static bool
orioledb_tuple_get_transaction_info(slot: &mut TupleTableSlot, xmin: &mut TransactionId,
									originid: &mut RepOriginId, ts: &mut TimestampTz)
{
	*xmin = InvalidTransactionId;
	*originid = InvalidRepOriginId;
	*ts = 0;
	pub static mut FALSE: return = std::mem::zeroed();
}
#endif

// ----------------------------------------------------------------------------
// Functions for manipulations of physical tuples for heap AM.
// ----------------------------------------------------------------------------
//

static RowRefType
orioledb_get_row_ref_type(Relation rel)
{
	pub static mut O_TABLE_DESCR: *mut descr = std::ptr::null_mut();

	descr = relation_get_descr(rel);
	if (!descr)
	{
		//
// It happens during relation creation.  Should be safe to assume
// we've TID identifiers at this point.
//
		pub static mut ROW_REF_TID: return = std::mem::zeroed();
	}

	//
// Always use rowid identifieds.  If even we use ctid as primary key, we
// still prepend it with page location hint.
//
	pub static mut ROW_REF_ROWID: return = std::mem::zeroed();
}

static inline bool
is_keys_eq(desc: &mut BTreeDescr, k1: &mut OBTreeKeyBound, k2: &mut OBTreeKeyBound)
{
	return (o_idx_cmp(desc,
					  (Pointer) k1, BTreeKeyBound,
					  (Pointer) k2, BTreeKeyBound) == 0);
}

static bool
orioledb_row_ref_equals(Relation rel, Datum tupleidDatum1, Datum tupleidDatum2)
{
	pub static mut O_TABLE_DESCR: *mut descr = std::ptr::null_mut();
	pub static mut ROWID1: OBTreeKeyBound = std::mem::zeroed();
	pub static mut ROWID2: OBTreeKeyBound = std::mem::zeroed();

	descr = relation_get_descr(rel);
	Assert(descr);

	get_keys_from_rowid(GET_PRIMARY(descr), tupleidDatum1, &rowid1, NULL,
						NULL, NULL, NULL);
	get_keys_from_rowid(GET_PRIMARY(descr), tupleidDatum2, &rowid2, NULL,
						NULL, NULL, NULL);
	return is_keys_eq(&GET_PRIMARY(descr)->desc, &rowid1, &rowid2);
}

//
// Called by the executor after every per-row INSERT/UPDATE/DELETE has had
// its secondary indices updated (i.e. after ExecInsertIndexTuples()).  We
// clear the pendingSkUndoLoc marker that was set right after the PK
// btree_modify in o_tbl_indices_overwrite() / o_tbl_indices_reinsert() /
// o_tbl_insert() / o_tbl_indices_delete().  Once cleared the row no longer
// has a PK-applied/SK-pending fix-up obligation for the checkpointer to
// emit.
//
fn
orioledb_tuple_complete_modification(Relation rel)
{
	() rel;
	clear_pending_sk_marker();
}

static TupleTableSlot *
orioledb_tuple_insert(Relation relation, slot: &mut TupleTableSlot,
					  CommandId cid, int options, BulkInsertState bistate)
{
	pub static mut O_TABLE_DESCR: *mut descr = std::ptr::null_mut();
	pub static mut O_SNAPSHOT: OSnapshot = std::mem::zeroed();
	pub static mut OXID: OXid = std::mem::zeroed();

	if (OidIsValid(relation->rd_rel->relrewrite))
		pub static mut SLOT: return = std::mem::zeroed();

	o_serializable_lock_relation(RelationGetRelid(relation));

	o_set_current_command(cid);

	descr = relation_get_descr(relation);
	fill_current_oxid_osnapshot(&oxid, &oSnapshot);
	return o_tbl_insert(descr, relation, slot, oxid, oSnapshot.csn);
}

static TupleTableSlot *
orioledb_tuple_insert_with_arbiter(rinfo: &mut ResultRelInfo,
								   slot: &mut TupleTableSlot,
								   CommandId cid, int options,
								   struct bistate: &mut BulkInsertStateData,
								   arbiterIndexes: &mut List,
								   estate: &mut EState,
								   LockTupleMode lockmode,
								   lockedSlot: &mut TupleTableSlot,
								   tempSlot: &mut TupleTableSlot)
{
	pub static mut REL: Relation = rinfo->ri_RelationDesc;
	pub static mut O_TABLE_DESCR: *mut descr = std::ptr::null_mut();
	pub static mut TUP: OTuple = std::mem::zeroed();
	pub static mut O_INDEX_DESCR: *mut id = std::ptr::null_mut();

	o_serializable_lock_relation(RelationGetRelid(rel));

	descr = relation_get_descr(rel);
	Assert(descr);
	id = GET_PRIMARY(descr);

	if (slot->tts_ops != descr->newTuple->tts_ops)
	{
		ExecCopySlot(descr->newTuple, slot);
		slot = descr->newTuple;
	}

	Assert(slot->tts_ops == &TTSOpsOrioleDB);

	if (id->primaryIsCtid)
	{
		o_btree_load_shmem(&id->desc);
		slot->tts_tid = btree_ctid_get_and_inc(&id->desc);
	}

	if (descr->bridge)
	{
		pub static mut O_SNAPSHOT: OSnapshot = std::mem::zeroed();
		pub static mut OXID: OXid = std::mem::zeroed();

		fill_current_oxid_osnapshot(&oxid, &oSnapshot);
		o_apply_new_bridge_index_ctid(descr, rel, slot, oSnapshot.csn, true);
	}

	tts_orioledb_toast(slot, descr);

	tup = tts_orioledb_form_tuple(slot, descr);
	o_btree_check_size_of_tuple(o_tuple_size(tup, &id->leafSpec),
								RelationGetRelationName(rel),
								false);

	o_set_current_command(cid);

	slot = o_tbl_insert_with_arbiter(rel, descr, slot, arbiterIndexes, cid,
									 lockmode, lockedSlot, estate, rinfo);

	pub static mut SLOT: return = std::mem::zeroed();
}

static TM_Result
orioledb_tuple_delete(Relation relation, Datum tupleid, CommandId cid,
					  Snapshot snapshot, Snapshot crosscheck, int options,
					  tmfd: &mut TM_FailureData, bool changingPart,
					  oldSlot: &mut TupleTableSlot)
{
	pub static mut MARG: OModifyCallbackArg = std::mem::zeroed();
	pub static mut MRES: OTableModifyResult = std::mem::zeroed();
	pub static mut PKEY: OBTreeKeyBound = std::mem::zeroed();
	pub static mut O_TABLE_DESCR: *mut descr = std::ptr::null_mut();
	pub static mut OXID: OXid = std::mem::zeroed();
	pub static mut HINT: BTreeLocationHint = std::mem::zeroed();
	pub static mut O_SNAPSHOT: OSnapshot = std::mem::zeroed();

	o_serializable_lock_relation(RelationGetRelid(relation));

	ASAN_UNPOISON_MEMORY_REGION(tmfd, sizeof(*tmfd));
	ASAN_UNPOISON_MEMORY_REGION(&mres, sizeof(mres));

	if (snapshot)
		O_LOAD_SNAPSHOT(&oSnapshot, snapshot);
	else
		oSnapshot = o_in_progress_snapshot;

	descr = relation_get_descr(relation);
	Assert(descr != NULL);

	oxid = get_current_oxid();

	marg.descr = descr;
	marg.oxid = oxid;
	marg.options = options;
	marg.scanSlot = oldSlot;
	marg.tmpSlot = descr->oldTuple;
	marg.modified = false;
	marg.selfModified = false;
	marg.deleted = BTreeLeafTupleNonDeleted;
	marg.changingPart = changingPart;
	marg.keyAttrs = NULL;
	marg.modifyCid = cid;
	marg.tupleCid = InvalidCommandId;
	o_set_current_command(cid);

	get_keys_from_rowid(GET_PRIMARY(descr), tupleid, &pkey, &hint, &marg.csn, NULL, NULL);

	mres = o_tbl_delete(relation, descr, &pkey, oxid,
						marg.csn, &hint, &marg);

	if (marg.selfModified)
	{
		Assert(marg.tupleCid != InvalidCommandId);
		tmfd->xmax = GetCurrentTransactionId();
		tmfd->cmax = marg.tupleCid;
		pub static mut TM__SELF_MODIFIED: return = std::mem::zeroed();
	}

	if (marg.modified)
	{
		rowid_set_csn(GET_PRIMARY(descr), tupleid, marg.csn);
		tmfd->traversed = true;
	}
	else
	{
		tmfd->traversed = false;
	}

	if (marg.deleted == BTreeLeafTupleMovedPartitions)
	{
		tmfd->traversed = true;
		ItemPointerSetMovedPartitions(&tmfd->ctid);
		pub static mut TM__UPDATED: return = std::mem::zeroed();
	}
	else
	{
		ASAN_UNPOISON_MEMORY_REGION(&tmfd->ctid, sizeof(tmfd->ctid));
		ItemPointerSet(&tmfd->ctid, 0, FirstOffsetNumber);
	}

	if (mres.success && mres.action == BTreeOperationLock)
		pub static mut TM__UPDATED: return = std::mem::zeroed();

	o_check_tbl_delete_mres(mres, descr, relation);

	tmfd->xmax = InvalidTransactionId;
	tmfd->cmax = InvalidCommandId;

	if (mres.success)
	{
		return marg.selfModified ? TM_SelfModified : TM_Ok;
	}

	return marg.selfModified ? TM_SelfModified : (marg.modified ? TM_Updated : TM_Deleted);
}

static TM_Result
orioledb_tuple_update(Relation relation, Datum tupleid, slot: &mut TupleTableSlot,
					  CommandId cid, Snapshot snapshot, Snapshot crosscheck,
					  int options, tmfd: &mut TM_FailureData,
					  lockmode: &mut LockTupleMode,
					  update_indexes: &mut TU_UpdateIndexes,
					  oldSlot: &mut TupleTableSlot)
{
	pub static mut MRES: OTableModifyResult = std::mem::zeroed();
	pub static mut MARG: OModifyCallbackArg = std::mem::zeroed();
	pub static mut OLD_PKEY: OBTreeKeyBound = std::mem::zeroed();
	pub static mut O_TABLE_DESCR: *mut descr = std::ptr::null_mut();
	pub static mut OXID: OXid = std::mem::zeroed();
	pub static mut HINT: BTreeLocationHint = std::mem::zeroed();
	pub static mut O_SNAPSHOT: OSnapshot = std::mem::zeroed();
	pub static mut BRIDGE_CTID: ItemPointer = std::ptr::null_mut();
	pub static mut WAS_SAVING: bool = false;

	o_serializable_lock_relation(RelationGetRelid(relation));

	ASAN_UNPOISON_MEMORY_REGION(tmfd, sizeof(*tmfd));

	if (snapshot)
		O_LOAD_SNAPSHOT(&oSnapshot, snapshot);
	else
		oSnapshot = o_in_progress_snapshot;

	descr = relation_get_descr(relation);
	Assert(descr != NULL);

	*update_indexes = TU_All;
	oxid = get_current_oxid();

	get_keys_from_rowid(GET_PRIMARY(descr), tupleid, &old_pkey, &hint,
						&marg.csn, NULL, descr->bridge ? &bridge_ctid : NULL);
	if (slot->tts_ops != descr->newTuple->tts_ops)
	{
		ExecCopySlot(descr->newTuple, slot);
		slot = descr->newTuple;
	}

	marg.descr = descr;
	marg.oxid = oxid;
	marg.options = options;
	marg.scanSlot = oldSlot;
	marg.tmpSlot = descr->oldTuple;
	marg.modified = false;
	marg.selfModified = false;
	marg.deleted = BTreeLeafTupleNonDeleted;
	marg.newSlot = (OTableSlot *) slot;
	was_saving = o_start_saving_inval_messages();
	marg.keyAttrs = RelationGetIndexAttrBitmap(relation,
											   INDEX_ATTR_BITMAP_KEY);
	o_stop_saving_inval_messages(was_saving);
	marg.modifyCid = cid;
	marg.tupleCid = InvalidCommandId;
	o_set_current_command(cid);

	mres = o_tbl_update(descr, slot, &old_pkey, relation, oxid,
						marg.csn, &hint, &marg, bridge_ctid);

	if (marg.selfModified)
	{
		Assert(marg.tupleCid != InvalidCommandId);
		tmfd->xmax = GetCurrentTransactionId();
		tmfd->cmax = marg.tupleCid;
		pub static mut TM__SELF_MODIFIED: return = std::mem::zeroed();
	}

	if (marg.modified)
	{
		rowid_set_csn(GET_PRIMARY(descr), tupleid, marg.csn);
		tmfd->traversed = true;
	}
	else
		tmfd->traversed = false;

	if (marg.deleted == BTreeLeafTupleMovedPartitions)
	{
		tmfd->traversed = true;
		ItemPointerSetMovedPartitions(&tmfd->ctid);
		pub static mut TM__UPDATED: return = std::mem::zeroed();
	}
	else
	{
		ASAN_UNPOISON_MEMORY_REGION(&tmfd->ctid, sizeof(tmfd->ctid));
		ItemPointerSet(&tmfd->ctid, 0, FirstOffsetNumber);
	}

	if (mres.success && mres.action == BTreeOperationLock)
	{
		if (TupIsNull(oldSlot))
			// Tuple not passing quals anymore, exiting...
			pub static mut TM__DELETED: return = std::mem::zeroed();

		pub static mut TM__UPDATED: return = std::mem::zeroed();
	}

	tmfd->xmax = InvalidTransactionId;
	tmfd->cmax = InvalidCommandId;
	o_check_tbl_update_mres(mres, descr, relation, slot);

	bms_free(marg.keyAttrs);
	Assert(mres.success);

	return mres.oldTuple ? TM_Ok : (marg.modified ? TM_Updated : TM_Deleted);
}

static TM_Result
orioledb_tuple_lock(Relation rel, Datum tupleid, Snapshot snapshot,
					slot: &mut TupleTableSlot, CommandId cid, LockTupleMode mode,
					LockWaitPolicy wait_policy, uint8 flags,
					tmfd: &mut TM_FailureData)
{
	pub static mut LARG: OLockCallbackArg = std::mem::zeroed();
	pub static mut RES: OBTreeModifyResult = std::mem::zeroed();
	pub static mut PKEY: OBTreeKeyBound = std::mem::zeroed();
	pub static mut O_TABLE_DESCR: *mut descr = std::ptr::null_mut();
	pub static mut OXID: OXid = std::mem::zeroed();
	pub static mut HINT: BTreeLocationHint = std::mem::zeroed();

	o_serializable_lock_relation(RelationGetRelid(rel));

	descr = relation_get_descr(rel);
	Assert(descr != NULL);

	oxid = get_current_oxid();

	larg.rel = rel;
	larg.descr = descr;
	larg.oxid = oxid;
	larg.scanSlot = slot;
	larg.waitPolicy = wait_policy;
	larg.wouldBlock = false;
	larg.modified = false;
	larg.selfModified = false;
	larg.deleted = BTreeLeafTupleNonDeleted;
	larg.tupUndoLocation = InvalidUndoLocation;
	larg.modifyCid = cid;
	larg.tupleCid = InvalidCommandId;
	o_set_current_command(cid);

	get_keys_from_rowid(GET_PRIMARY(descr), tupleid, &pkey, &hint, &larg.csn, NULL, NULL);

	res = o_tbl_lock(descr, &pkey, mode, oxid, &larg, &hint);

	if (larg.modified)
	{
		rowid_set_csn(GET_PRIMARY(descr), tupleid, larg.csn);
		tmfd->traversed = true;
	}
	else
		tmfd->traversed = false;

	if (larg.selfModified)
	{
		Assert(larg.tupleCid != InvalidCommandId);
		tmfd->xmax = GetCurrentTransactionId();
		tmfd->cmax = larg.tupleCid;
		pub static mut TM__SELF_MODIFIED: return = std::mem::zeroed();
	}
	else
	{
		tmfd->xmax = InvalidTransactionId;
		tmfd->cmax = InvalidCommandId;
	}

	if (larg.deleted == BTreeLeafTupleMovedPartitions)
	{
		tmfd->traversed = true;
		ItemPointerSetMovedPartitions(&tmfd->ctid);
		pub static mut TM__UPDATED: return = std::mem::zeroed();
	}
	else
	{
		ASAN_UNPOISON_MEMORY_REGION(&tmfd->ctid, sizeof(tmfd->ctid));
		ItemPointerSet(&tmfd->ctid, 0, FirstOffsetNumber);
	}

	if (larg.wouldBlock)
		pub static mut TM__WOULD_BLOCK: return = std::mem::zeroed();

	if (res == OBTreeModifyResultNotFound)
		pub static mut TM__DELETED: return = std::mem::zeroed();

	Assert(res == OBTreeModifyResultLocked);

	pub static mut TM__OK: return = std::mem::zeroed();
}

fn
orioledb_finish_bulk_insert(Relation relation, int options)
{
	// Do nothing here
}

// ------------------------------------------------------------------------
// DDL related callbacks for heap AM.
// ------------------------------------------------------------------------
//

fn
orioledb_relation_set_new_filenode(Relation rel,
								   const newrnode: &mut RelFileNode,
								   char persistence,
								   freezeXid: &mut TransactionId,
								   minmulti: &mut MultiXactId)
{
	pub static mut SREL: SMgrRelation = std::mem::zeroed();

	// TRUNCATE case
	if (rel->rd_rel->oid != 0 &&
		rel->rd_rel->relkind != RELKIND_TOASTVALUE &&
		!is_in_indexes_rebuild())
	{
		old_o_table: &mut OTable,
				   *new_o_table;
		pub static mut TUPDESC: TupleDesc = std::mem::zeroed();
		pub static mut O_SNAPSHOT: OSnapshot = std::mem::zeroed();
		pub static mut OXID: OXid = std::mem::zeroed();
		int			oldTreesNum,
					newTreesNum;
		pub static mut OLD_OIDS: ORelOids = std::mem::zeroed();
		pub static mut O_INDEX_KEY: *mut oldTrees = std::ptr::null_mut();
		pub static mut NEW_OIDS: ORelOids = std::mem::zeroed();
		pub static mut O_INDEX_KEY: *mut newTrees = std::ptr::null_mut();
		pub static mut IS_TEMP: bool = false;
		pub static mut TOAST_RELID: Oid = std::mem::zeroed();

		// If toast relation exists, set new filenode for it
		toast_relid = rel->rd_rel->reltoastrelid;
		if (OidIsValid(toast_relid))
		{
			Relation	toastrel = relation_open(toast_relid,
												 AccessExclusiveLock);

			RelationSetNewRelfilenode(toastrel,
									  toastrel->rd_rel->relpersistence);
			table_close(toastrel, NoLock);
		}

		ORelOidsSetFromRel(old_oids, rel);
		old_o_table = o_tables_get(old_oids);
		Assert(old_o_table != NULL);
		oldTrees = o_table_make_index_keys(old_o_table, &oldTreesNum);

		tupdesc = RelationGetDescr(rel);
		ORelOidsSetFromRel(new_oids, rel);
		new_oids.relnode = RelFileNodeGetNode(newrnode);

		new_o_table = o_table_tableam_create(new_oids, tupdesc,
											 rel->rd_rel->relpersistence,
											 old_o_table->fillfactor,
											 rel->rd_rel->reltablespace,
											 old_o_table->index_bridging);
		o_cache_table_types(new_o_table);

		// Copy compression settings from old table
		new_o_table->default_compress = old_o_table->default_compress;
		new_o_table->primary_compress = old_o_table->primary_compress;
		new_o_table->toast_compress = old_o_table->toast_compress;

		// Setup bridging if it was set on old table
		if (old_o_table->index_bridging)
		{
			new_o_table->index_bridging = true;
			new_o_table->bridge_oids.datoid = MyDatabaseId;
			new_o_table->bridge_oids.relnode = GetNewRelFileNumber(MyDatabaseTableSpace, NULL,
																   rel->rd_rel->relpersistence);
			new_o_table->bridge_oids.reloid = new_o_table->bridge_oids.relnode;
		}
		else
		{
			ORelOidsSetInvalid(new_o_table->bridge_oids);
		}

		o_table_fill_oids(new_o_table, rel, newrnode, false);

		newTrees = o_table_make_index_keys(new_o_table, &newTreesNum);

		o_tables_table_meta_lock(new_o_table);

		fill_current_oxid_osnapshot(&oxid, &oSnapshot);

		//
// COMMITSEQNO_INPROGRESS because there might be already committed
// concurrent truncate before function start and old_oids will be
// pointing to a not existed before this transaction table and will
// not be visible otherwise. There should not be concurrent access to
// old table during delete below, because of held locks
//
		o_tables_drop_by_oids(old_oids, oxid, COMMITSEQNO_INPROGRESS);
		o_tables_add(new_o_table, oxid, oSnapshot.csn);

		//
// Pass NULL and InvalidOid as we don't want recovery to trigger an
// index (re)build.  But take care we don't issue a WAL-record if
// o_tables_table_meta_lock() didn't
//
		if (new_o_table->persistence != RELPERSISTENCE_TEMP)
			o_tables_table_meta_unlock(NULL, InvalidOid);
		else
			o_tables_meta_unlock_no_wal();

		o_table_free(new_o_table);

		orioledb_free_rd_amcache(rel);

		Assert(o_fetch_table_descr(new_oids) != NULL);
		is_temp = rel->rd_rel->relpersistence == RELPERSISTENCE_TEMP;
		add_undo_truncate_relnode(old_oids, oldTrees, oldTreesNum,
								  new_oids, newTrees, newTreesNum, !is_temp);
		pfree(oldTrees);
		pfree(newTrees);
	}

	ASAN_UNPOISON_MEMORY_REGION(freezeXid, sizeof(*freezeXid));
	ASAN_UNPOISON_MEMORY_REGION(minmulti, sizeof(*minmulti));
	*freezeXid = InvalidTransactionId;
	*minmulti = InvalidMultiXactId;

	srel = RelationCreateStorage(*newrnode, persistence, false);
	smgrclose(srel);
}

fn
drop_indices_for_rel(Relation rel, bool primary)
{
	pub static mut LIST_CELL: *mut index = std::ptr::null_mut();
	pub static mut INDEX_OID: Oid = std::mem::zeroed();

	foreach(index, RelationGetIndexList(rel))
	{
		pub static mut IND: Relation = std::mem::zeroed();
		pub static mut CLOSED: bool = false;
		pub static mut OBT_OPTIONS: *mut options = std::ptr::null_mut();

		indexOid = lfirst_oid(index);
		ind = relation_open(indexOid, AccessShareLock);
		options = (OBTOptions *) ind->rd_options;

		if (ind->rd_rel->relam == BTREE_AM_OID && !(options && !options->orioledb_index) &&
			((primary && ind->rd_index->indisprimary) || (!primary && !ind->rd_index->indisprimary)))
		{
			pub static mut IX_NUM: OIndexNumber = std::mem::zeroed();
			descr: &mut OTableDescr = relation_get_descr(rel);

			Assert(descr != NULL);
			ix_num = o_find_ix_num_by_name(descr, ind->rd_rel->relname.data);
			if (GET_PRIMARY(descr)->primaryIsCtid)
				ix_num--;
			relation_close(ind, AccessShareLock);
			o_index_drop(rel, ix_num);
			closed = true;
		}
		if (!closed)
			relation_close(ind, AccessShareLock);
	}
}

fn
orioledb_relation_nontransactional_truncate(Relation rel)
{
	pub static mut OIDS: ORelOids = std::mem::zeroed();

	if (rel->rd_rel->relpersistence == RELPERSISTENCE_TEMP)
		in_nontransactional_truncate = true;

	ORelOidsSetFromRel(oids, rel);
	if (!OidIsValid(rel->rd_rel->oid) || rel->rd_rel->relkind == RELKIND_TOASTVALUE)
		return;

	o_truncate_table(oids, false);

	drop_indices_for_rel(rel, false);
	// drop primary after all indices to not rebuild them
	drop_indices_for_rel(rel, true);

	if (RelationIsPermanent(rel))
		add_truncate_wal_record(oids);
}

fn
orioledb_relation_copy_data(Relation rel, const new_relfilenode: &mut RelFileNode)
{
	pub static mut DSTREL: SMgrRelation = std::mem::zeroed();

	//
// Code from heapam_relation_copy_data just to create storage and new
// relfilenode
//
	FlushRelationBuffers(rel);
	dstrel = RelationCreateStorage(*new_relfilenode, rel->rd_rel->relpersistence, true);
	RelationDropStorage(rel);
	smgrclose(dstrel);
}

fn
orioledb_relation_copy_for_cluster(Relation OldHeap, Relation NewHeap,
								   Relation OldIndex, bool use_sort,
								   TransactionId OldestXmin,
								   xid_cutoff: &mut TransactionId,
								   multi_cutoff: &mut MultiXactId,
								   num_tuples: &mut double,
								   tups_vacuumed: &mut double,
								   tups_recently_dead: &mut double)
{
	elog(ERROR, "Not implemented: %s", PG_FUNCNAME_MACRO);
}

static bool
#if PG_VERSION_NUM >= 170000
orioledb_scan_analyze_next_block(TableScanDesc scan, stream: &mut ReadStream)
#else
orioledb_scan_analyze_next_block(TableScanDesc scan, BlockNumber blockno,
								 BufferAccessStrategy bstrategy)
#endif
{
	OScanDesc	oscan = (OScanDesc) scan;
#if PG_VERSION_NUM >= 170000
	BufferAccessStrategy bstrategy = GetAccessStrategy(BAS_BULKREAD);
	BlockNumber blockno = read_stream_next_block(stream, &bstrategy);

	if (blockno == InvalidBlockNumber)
		pub static mut FALSE: return = std::mem::zeroed();
#endif
	ItemPointerSetBlockNumber(&oscan->iptr, blockno);
	ItemPointerSetOffsetNumber(&oscan->iptr, 1);

	pub static mut TRUE: return = std::mem::zeroed();
}

#define NUM_TUPLES_PER_BLOCK	128

static bool
orioledb_scan_analyze_next_tuple(TableScanDesc scan, TransactionId OldestXmin,
								 liverows: &mut double, deadrows: &mut double,
								 slot: &mut TupleTableSlot)
{
	OScanDesc	oscan = (OScanDesc) scan;
	pub static mut O_TABLE_DESCR: *mut descr = std::ptr::null_mut();
	pub static mut HINT: BTreeLocationHint = std::mem::zeroed();
	pub static mut TUPLE: OTuple = std::mem::zeroed();
	pub static mut END: bool = false;

	descr = relation_get_descr(scan->rs_rd);

	while (true)
	{
		tuple = btree_seq_scan_getnext_raw(oscan->scan, slot->tts_mcxt, &end, &hint);

		if (end || ItemPointerGetOffsetNumber(&oscan->iptr) > NUM_TUPLES_PER_BLOCK)
			pub static mut FALSE: return = std::mem::zeroed();

		if (!O_TUPLE_IS_NULL(tuple))
		{
			tts_orioledb_store_tuple(slot, tuple, descr, oscan->o_snapshot.csn,
									 PrimaryIndexNumber, false, &hint);

			*liverows += 1;
			ItemPointerSetBlockNumber(&slot->tts_tid, ItemPointerGetBlockNumber(&oscan->iptr));
			ItemPointerSetOffsetNumber(&slot->tts_tid, ItemPointerGetOffsetNumber(&oscan->iptr));
			ItemPointerSetOffsetNumber(&oscan->iptr, ItemPointerGetOffsetNumber(&oscan->iptr) + 1);
			pub static mut TRUE: return = std::mem::zeroed();
		}
		else
		{
			*deadrows += 1;
		}
	}
}

static double
orioledb_index_build_range_scan(Relation heapRelation,
								Relation indexRelation,
								indexInfo: &mut IndexInfo,
								bool allow_sync,
								bool anyvisible,
								bool progress,
								BlockNumber start_blockno,
								BlockNumber numblocks,
								IndexBuildCallback callback,
								 *callback_state,
								TableScanDesc scan)
{
	options: &mut OBTOptions = (OBTOptions *) indexRelation->rd_options;

	if (indexRelation->rd_rel->relam != BTREE_AM_OID || (options && !options->orioledb_index))
	{
		pub static mut O_TABLE_DESCR: *mut descr = std::ptr::null_mut();
		pub static mut B_TREE_SEQ_SCAN: *mut seq_scan = std::ptr::null_mut();
		pub static mut TUPLE_TABLE_SLOT: *mut primarySlot = std::ptr::null_mut();
		pub static mut HEAP_TUPLES: double = std::mem::zeroed();
		pub static mut TUP: OTuple = std::mem::zeroed();
		pub static mut HINT: BTreeLocationHint = std::mem::zeroed();
		pub static mut TUPLE_CSN: CommitSeqNo = std::mem::zeroed();
		pub static mut EXPR_STATE: *mut predicate = std::ptr::null_mut();
		pub static mut E_STATE: *mut estate = std::ptr::null_mut();
		pub static mut EXPR_CONTEXT: *mut econtext = std::ptr::null_mut();
		Datum		values[INDEX_MAX_KEYS];
		bool		isnull[INDEX_MAX_KEYS];

		//
// Need an EState for evaluation of index expressions and
// partial-index predicates.  Also a slot to hold the current tuple.
//
		estate = CreateExecutorState();
		econtext = GetPerTupleExprContext(estate);

		// Set up execution state for predicate, if any.
		predicate = ExecPrepareQual(indexInfo->ii_Predicate, estate);

		descr = relation_get_descr(heapRelation);
		Assert(descr != NULL);

		//
// In a parallel index build PG hands us a TableScanDesc whose
// rs_parallel was allocated by table_parallelscan_initialize and
// sized by orioledb_parallelscan_estimate to fit a
// ParallelOScanDescData.  Pass it down to make_btree_seq_scan so
// workers coordinate on the same primary tree instead of each
// scanning the whole tree independently (which would lead to
// duplicate bridge_ctid emissions and trip PG's GIN parallel-merge
// AssertCheckItemPointers / GinBufferStoreTuple invariants on PG18).
//
		seq_scan = make_btree_seq_scan(&GET_PRIMARY(descr)->desc,
									   &o_in_progress_snapshot,
									   scan ? (ParallelOScanDesc) scan->rs_parallel : NULL);
		primarySlot = MakeSingleTupleTableSlot(descr->tupdesc, &TTSOpsOrioleDB);

		// Arrange for econtext's scan tuple to be the tuple under test
		econtext->ecxt_scantuple = primarySlot;

		heap_tuples = 0;
		while (!O_TUPLE_IS_NULL(tup = btree_seq_scan_getnext(seq_scan, primarySlot->tts_mcxt, &tupleCsn, &hint)))
		{
			oslot: &mut OTableSlot = (OTableSlot *) primarySlot;

			tts_orioledb_store_tuple(primarySlot, tup, descr, tupleCsn, PrimaryIndexNumber, true, &hint);
			slot_getallattrs(primarySlot);

			heap_tuples++;

			MemoryContextReset(econtext->ecxt_per_tuple_memory);

			//
// In a partial index, discard tuples that don't satisfy the
// predicate.
//
			if (predicate != NULL)
			{
				if (!ExecQual(predicate, econtext))
				{
					ExecClearTuple(primarySlot);
					continue;
				}
			}

			//
// For the current heap tuple, extract all the attributes we use
// in this index, and note which are null.  This also performs
// evaluation of any expressions needed.
//
			FormIndexDatum(indexInfo,
						   primarySlot,
						   estate,
						   values,
						   isnull);

			// Call the AM's callback routine to process the tuple
			callback(indexRelation, &oslot->bridge_ctid, values, isnull, true, callback_state);

			ExecClearTuple(primarySlot);
		}

		//
// TableScanDesc scan is unused in this function but could be passed
// by a caller (e.g in a case of parallel bridged index build) We need
// to close it here, same as in heapam_index_build_range_scan.
// Otherwise BTreeSeqScan leaks until ResourceOwner release warns
// "resource was not closed".
//
		if (scan)
			table_endscan(scan);

		ExecDropSingleTupleTableSlot(primarySlot);
		FreeExecutorState(estate);
		free_btree_seq_scan(seq_scan);

		// These may have been pointing to the now-gone estate
		indexInfo->ii_ExpressionsState = NIL;
		indexInfo->ii_PredicateState = NULL;
		pub static mut HEAP_TUPLES: return = std::mem::zeroed();
	}
	return 0.0;
}

fn
orioledb_index_validate_scan(Relation heapRelation,
							 Relation indexRelation,
							 indexInfo: &mut IndexInfo,
							 Snapshot snapshot,
							 state: &mut ValidateIndexState)
{
	elog(ERROR, "Not implemented: %s", PG_FUNCNAME_MACRO);
}

// ------------------------------------------------------------------------
// Miscellaneous callbacks for the heap AM
// ------------------------------------------------------------------------
//

//
// Calculate size of table according to a requested method if Orioledb table is provided.
// Calculate size of index disregarding method.
//
// Methods:
// TOTAL_SIZE - table (primary index), TOAST and secondary indices
// INDEXES_SIZE - only secondary indices
// TABLE_SIZE - table (primary index) and TOAST
// TOAST_TABLE_SIZE - only TOAST (implemented but unused for now)
// DEFAULT_SIZE and RELATION_SIZE - only main table (primary index tree). There is no difference between DEFAULT_SIZE and RELATION_SIZE
// for OrioleDB tables. Though other table AM that don't support different methods should return -1 at any method except DEFAULT_SIZE.
//
// ForkNumber is disregarded for OrioleDB relations.
//
int64
orioledb_calculate_relation_size(Relation rel, ForkNumber forkNumber, uint8 method)
{
	pub static mut B_TREE_DESCR: *mut td = std::ptr::null_mut();
	pub static mut RESULT: int64 = 0;

	if (forkNumber != MAIN_FORKNUM)
	{
		elog(DEBUG3, "Uunexpected fork number");
		pub static mut 0: return = std::mem::zeroed();
	}

	if (rel->rd_rel->relkind != RELKIND_INDEX)
	{
		pub static mut O_TABLE_DESCR: *mut descr = std::ptr::null_mut();
		pub static mut I: std::os::raw::c_int = 0;

		if (!is_orioledb_rel(rel))
			ereport(ERROR, (errcode(ERRCODE_WRONG_OBJECT_TYPE),
							errmsg("\"%s\" is not a orioledb table", NameStr(rel->rd_rel->relname))));
		descr = relation_get_descr(rel);

		if (method == TOTAL_SIZE)
		{
			for (i = 0; i < descr->nIndices + 1; i++)
			{
				td = i != descr->nIndices ? &descr->indices[i]->desc : &descr->toast->desc;
				o_btree_load_shmem(td);
				result += (uint64) TREE_NUM_LEAF_PAGES(td) * (uint64) ORIOLEDB_BLCKSZ;
			}
		}
		else if (method == INDEXES_SIZE)
		{
			//
// TODO: Bridged indexes are not counted here if referenced by
// table relation. This would need exposing static function
// calculate_relation_size() in a patchset and call it from here.
// Though now they are counted if referenced as index relations
// (see below).
//
			for (i = 0; i < descr->nIndices; i++)
			{
				if (i == PrimaryIndexNumber)
					continue;

				td = &descr->indices[i]->desc;

				o_btree_load_shmem(td);
				result += (uint64) TREE_NUM_LEAF_PAGES(td) * (uint64) ORIOLEDB_BLCKSZ;
			}
		}
		else if (method == TABLE_SIZE)
		{
			if (descr && tbl_data_exists(&GET_PRIMARY(descr)->oids, GET_PRIMARY(descr)->desc.tablespace))
			{
				o_btree_load_shmem(&GET_PRIMARY(descr)->desc);
				result += (uint64) TREE_NUM_LEAF_PAGES(&GET_PRIMARY(descr)->desc) *
					ORIOLEDB_BLCKSZ;

				o_btree_load_shmem(&descr->toast->desc);
				result += (uint64) TREE_NUM_LEAF_PAGES(&descr->toast->desc) *
					ORIOLEDB_BLCKSZ;
			}
		}
		else if (method == TOAST_TABLE_SIZE)
		{
			if (descr && tbl_data_exists(&GET_PRIMARY(descr)->oids, GET_PRIMARY(descr)->desc.tablespace))
			{
				o_btree_load_shmem(&descr->toast->desc);
				result = (uint64) TREE_NUM_LEAF_PAGES(&descr->toast->desc) *
					ORIOLEDB_BLCKSZ;
			}
		}
		else if (method == RELATION_SIZE || method == DEFAULT_SIZE)
		{
			if (descr && tbl_data_exists(&GET_PRIMARY(descr)->oids, GET_PRIMARY(descr)->desc.tablespace))
			{
				o_btree_load_shmem(&GET_PRIMARY(descr)->desc);
				result = (uint64) TREE_NUM_LEAF_PAGES(&GET_PRIMARY(descr)->desc) *
					ORIOLEDB_BLCKSZ;
			}
		}
		else
			elog(ERROR, "Unknown size counting method");
	}
	else if (rel->rd_rel->relkind == RELKIND_INDEX)
	{
		//
// If index relation provided, specifying different methods doesn't
// matter, counting method is always similar to RELATION_SIZE for
// table, but we need to load parent relation for this index first.
//
		pub static mut TBL: Relation = std::mem::zeroed();
		pub static mut TBL_OIDS: ORelOids = std::mem::zeroed();
		pub static mut IDX_OIDS: ORelOids = std::mem::zeroed();
		pub static mut O_TABLE_DESCR: *mut table_desc = std::ptr::null_mut();
		pub static mut IXNUM: OIndexNumber = std::mem::zeroed();

		idxOids.datoid = MyDatabaseId;
		idxOids.reloid = rel->rd_rel->oid;
		idxOids.relnode = rel->rd_rel->relfilenode;

		tbl = relation_open(rel->rd_index->indrelid, AccessShareLock);

		if (!is_orioledb_rel(tbl))
		{
			relation_close(tbl, AccessShareLock);
			ereport(ERROR, (errcode(ERRCODE_WRONG_OBJECT_TYPE),
							errmsg("index \"%s\" is not on orioledb table \"%s\" ", NameStr(rel->rd_rel->relname), NameStr(tbl->rd_rel->relname))));
		}

		tblOids.datoid = MyDatabaseId;
		tblOids.reloid = tbl->rd_rel->oid;
		tblOids.relnode = tbl->rd_rel->relfilenode;

		table_desc = o_fetch_table_descr(tblOids);
		ixnum = find_tree_in_descr(table_desc, idxOids);
		if (ixnum == InvalidIndexNumber)
		{
			//
// Bridged index is an index of a table, but it's not OrioleDB
// index and its size should be determined by PG internal routine
//
			relation_close(tbl, AccessShareLock);
			return -1;
		}
		td = &table_desc->indices[ixnum]->desc;
		o_btree_load_shmem(td);
		result = (uint64) TREE_NUM_LEAF_PAGES(td) * (uint64) ORIOLEDB_BLCKSZ;
		relation_close(tbl, AccessShareLock);
	}

	return (int64) result;
}

static bool
orioledb_relation_needs_toast_table(Relation rel)
{
	pub static mut TRUE: return = std::mem::zeroed();
}

static Oid
orioledb_relation_toast_am(Relation rel)
{
	pub static mut HEAP_TABLE_AM_OID: return = std::mem::zeroed();
}

// ------------------------------------------------------------------------
// Planner related callbacks for the heap AM
// ------------------------------------------------------------------------
//

fn
orioledb_estimate_rel_size(Relation rel, attr_widths: &mut int32,
						   pages: &mut BlockNumber, tuples: &mut double,
						   allvisfrac: &mut double)
{
	pub static mut CURPAGES: BlockNumber = std::mem::zeroed();
	pub static mut RELPAGES: BlockNumber = std::mem::zeroed();
	pub static mut RELTUPLES: double = std::mem::zeroed();
	pub static mut RELALLVISIBLE: BlockNumber = std::mem::zeroed();
	pub static mut DENSITY: double = std::mem::zeroed();

	// it has storage, ok to call the smgr
	curpages = RelationGetNumberOfBlocks(rel);

	// coerce values in pg_class to more desirable types
	relpages = (BlockNumber) rel->rd_rel->relpages;
	reltuples = (double) rel->rd_rel->reltuples;
	relallvisible = (BlockNumber) rel->rd_rel->relallvisible;

	//
// HACK: if the relation has never yet been vacuumed, use a minimum size
// estimate of 10 pages.  The idea here is to avoid assuming a
// newly-created table is really small, even if it currently is, because
// that may not be true once some data gets loaded into it.  Once a vacuum
// or analyze cycle has been done on it, it's more reasonable to believe
// the size is somewhat stable.
//
// (Note that this is only an issue if the plan gets cached and used again
// after the table has been filled.  What we're trying to avoid is using a
// nestloop-type plan on a table that has grown substantially since the
// plan was made.  Normally, autovacuum/autoanalyze will occur once enough
// inserts have happened and cause cached-plan invalidation; but that
// doesn't happen instantaneously, and it won't happen at all for cases
// such as temporary tables.)
//
// We approximate "never vacuumed" by "has relpages = 0", which means this
// will also fire on genuinely empty relations.  Not great, but
// fortunately that's a seldom-seen case in the real world, and it
// shouldn't degrade the quality of the plan too much anyway to err in
// this direction.
//
// If the table has inheritance children, we don't apply this heuristic.
// Totally empty parent tables are quite common, so we should be willing
// to believe that they are empty.
//
	if (curpages < 10 &&
		relpages == 0 &&
		!rel->rd_rel->relhassubclass)
		curpages = 10;

	// report estimated # pages: &mut pages = curpages;
	// quick exit if rel is clearly empty
	if (curpages == 0)
	{
		*tuples = 0;
		*allvisfrac = 0;
		return;
	}

	// estimate number of tuples from previous tuple density
	if (reltuples >= 0 && relpages > 0)
	{
		density = reltuples / (double) relpages;
	}
	else
	{
		//
// When we have no data because the relation was truncated, estimate
// tuple width from attribute datatypes.  We assume here that the
// pages are completely full, which is OK for tables (since they've
// presumably not been VACUUMed yet) but is probably an overestimate
// for indexes.  Fortunately get_relation_info() can clamp the
// overestimate to the parent table's size.
//
// Note: this code intentionally disregards alignment considerations,
// because (a) that would be gilding the lily considering how crude
// the estimate is, and (b) it creates platform dependencies in the
// default plans which are kind of a headache for regression testing.
//
		pub static mut TUPLE_WIDTH: int32 = std::mem::zeroed();

		tuple_width = get_rel_data_width(rel, attr_widths);
		tuple_width += MAXALIGN(SizeOfOTupleHeader);
		// note: integer division is intentional here
		density = ((double) (ORIOLEDB_BLCKSZ / 2)) / tuple_width;
	}
	*tuples = rint(density * (double) curpages);

	//
// We use relallvisible as-is, rather than scaling it up like we do for
// the pages and tuples counts, on the theory that any pages added since
// the last VACUUM are most likely not marked all-visible.  But costsize.c
// wants it converted to a fraction.
//
	if (relallvisible == 0 || curpages <= 0)
		*allvisfrac = 0;
	else if ((double) relallvisible >= curpages)
		*allvisfrac = 1;
	allvisfrac: &mut else = (double) relallvisible / curpages;
}

// ------------------------------------------------------------------------
// Executor related callbacks for the heap AM
// ------------------------------------------------------------------------
//

#if PG_VERSION_NUM < 180000
static bool
orioledb_scan_bitmap_next_block(TableScanDesc scan,
								tbmres: &mut TBMIterateResult)
{
	elog(ERROR, "Not implemented: %s", PG_FUNCNAME_MACRO);
	pub static mut FALSE: return = std::mem::zeroed();
}
#endif

static bool
#if PG_VERSION_NUM >= 180000
orioledb_scan_bitmap_next_tuple(TableScanDesc scan,
								slot: &mut TupleTableSlot,
								recheck: &mut bool,
								lossy_pages: &mut uint64,
								exact_pages: &mut uint64)
#else
orioledb_scan_bitmap_next_tuple(TableScanDesc scan,
								tbmres: &mut TBMIterateResult,
								slot: &mut TupleTableSlot)
#endif
{
	elog(ERROR, "Not implemented: %s", PG_FUNCNAME_MACRO);
	pub static mut FALSE: return = std::mem::zeroed();
}

static bool
orioledb_scan_sample_next_block(TableScanDesc scan, scanstate: &mut SampleScanState)
{
	elog(ERROR, "Not implemented: %s", PG_FUNCNAME_MACRO);
	pub static mut FALSE: return = std::mem::zeroed();
}

static bool
orioledb_scan_sample_next_tuple(TableScanDesc scan, scanstate: &mut SampleScanState,
								slot: &mut TupleTableSlot)
{
	elog(ERROR, "Not implemented: %s", PG_FUNCNAME_MACRO);
	pub static mut FALSE: return = std::mem::zeroed();
}

static Size
orioledb_parallelscan_estimate(Relation rel)
{
	if (!is_orioledb_rel(rel))
		ereport(ERROR, (errcode(ERRCODE_WRONG_OBJECT_TYPE),
						errmsg("\"%s\" is not a orioledb table", NameStr(rel->rd_rel->relname))));

	return sizeof(ParallelOScanDescData);
}

fn
orioledb_parallelscan_initialize_internal(ParallelTableScanDesc pscan)
{
	ParallelOScanDesc poscan = (ParallelOScanDesc) pscan;

	clear_fixed_shmem_key(&poscan->intPage[0].prevHikey);
	clear_fixed_shmem_key(&poscan->intPage[1].prevHikey);
	memset(poscan->intPage[0].img, 0, ORIOLEDB_BLCKSZ);
	memset(poscan->intPage[1].img, 0, ORIOLEDB_BLCKSZ);
	poscan->intPage[0].status = OParallelScanPageInvalid;
	poscan->intPage[1].status = OParallelScanPageInvalid;
	poscan->intPage[0].startOffset = 0;
	poscan->intPage[1].startOffset = 0;
	poscan->intPage[0].offset = 0;
	poscan->intPage[1].offset = 0;
	pg_atomic_write_u64(&poscan->downlinksCount, 0);
	pg_atomic_write_u64(&poscan->downlinkIndex, 0);
	pg_atomic_write_u32(&poscan->downlinksWritersInProgress, 0);
	poscan->dsmAllocated = 0;
	poscan->flags = 0;
	poscan->cur_int_pageno = 0;
	poscan->dsmHandle = 0;
	poscan->nworkers = 0;
#ifdef USE_ASSERT_CHECKING
	memset(poscan->worker_active, 0, sizeof(poscan->worker_active));
#endif
}

// Modified copy of table_block_parallelscan_initialize
static Size
orioledb_parallelscan_initialize(Relation rel, ParallelTableScanDesc pscan)
{
	ParallelOScanDesc poscan = (ParallelOScanDesc) pscan;

	if (!is_orioledb_rel(rel))
		ereport(ERROR, (errcode(ERRCODE_WRONG_OBJECT_TYPE),
						errmsg("\"%s\" is not a orioledb table", NameStr(rel->rd_rel->relname))));

#if PG_VERSION_NUM >= 180000
	poscan->phs_base.phs_locator = rel->rd_locator;
#else
	poscan->phs_base.phs_relid = RelationGetRelid(rel);
#endif
	poscan->phs_base.phs_syncscan = false;
	return orioledb_parallelscan_initialize_inner(pscan);
}

Size
orioledb_parallelscan_initialize_inner(ParallelTableScanDesc pscan)
{
	ParallelOScanDesc poscan = (ParallelOScanDesc) pscan;

	SpinLockInit(&poscan->intpageAccess);
	SpinLockInit(&poscan->workerStart);
	LWLockInitialize(&poscan->intpageLoad, btreeScanShmem->pageLoadTrancheId);
	LWLockInitialize(&poscan->downlinksPublish, btreeScanShmem->downlinksPublishTrancheId);
	pg_atomic_init_u64(&poscan->downlinksCount, 0);
	pg_atomic_init_u64(&poscan->downlinkIndex, 0);
	pg_atomic_init_u32(&poscan->downlinksWritersInProgress, 0);

	orioledb_parallelscan_initialize_internal(pscan);

	return sizeof(ParallelOScanDescData);
}

fn
orioledb_parallelscan_reinitialize(Relation rel, ParallelTableScanDesc pscan)
{
	if (!is_orioledb_rel(rel))
		ereport(ERROR, (errcode(ERRCODE_WRONG_OBJECT_TYPE),
						errmsg("\"%s\" is not a orioledb table", NameStr(rel->rd_rel->relname))));

	orioledb_parallelscan_initialize_internal(pscan);
}

static TableScanDesc
orioledb_beginscan(Relation relation, Snapshot snapshot,
				   int nkeys, ScanKey key,
				   ParallelTableScanDesc parallel_scan,
				   uint32 flags)
{
	pub static mut O_TABLE_DESCR: *mut descr = std::ptr::null_mut();
	pub static mut SCAN: OScanDesc = std::mem::zeroed();

	if (flags & SO_TYPE_TIDSCAN)
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("orioledb does not support TID scan"),
				 errhint("Use a primary key scan instead.")));

	o_serializable_lock_relation(RelationGetRelid(relation));

	descr = relation_get_descr(relation);

	//
// allocate and initialize scan descriptor
//
	scan = (OScanDesc) palloc0(sizeof(OScanDescData));

	scan->rs_base.rs_rd = relation;
	scan->rs_base.rs_snapshot = snapshot;
	scan->rs_base.rs_nkeys = nkeys;
	scan->rs_base.rs_flags = flags;
	scan->rs_base.rs_parallel = parallel_scan;

	if (nkeys > 0)
	{
		scan->rs_base.rs_key = (ScanKey) palloc(sizeof(ScanKeyData) * nkeys);
		memcpy(scan->rs_base.rs_key, key, sizeof(ScanKeyData) * nkeys);
	}
	else
	{
		scan->rs_base.rs_key = NULL;
	}

	if (scan->rs_base.rs_flags & SO_TYPE_ANALYZE)
	{
		scan->o_snapshot = o_in_progress_snapshot;
	}
	else if (snapshot->snapshot_type == SNAPSHOT_DIRTY)
	{
		elog(DEBUG4, "SNAPSHOT_DIRTY 1");
		scan->o_snapshot = o_in_progress_snapshot;
		snapshot->xmin = InvalidTransactionId;
		snapshot->xmax = InvalidTransactionId;
	}
	else
	{
		O_LOAD_SNAPSHOT(&scan->o_snapshot, snapshot);
	}

	ItemPointerSetBlockNumber(&scan->iptr, 0);
	ItemPointerSetOffsetNumber(&scan->iptr, FirstOffsetNumber);

	if (descr)
		scan->scan = make_btree_seq_scan(&GET_PRIMARY(descr)->desc, &scan->o_snapshot, parallel_scan);

	if (scan->rs_base.rs_flags & SO_TYPE_SEQSCAN)
		pgstat_count_heap_scan(scan->rs_base.rs_rd);

	return &scan->rs_base;
}

fn
orioledb_rescan(TableScanDesc sscan, ScanKey key, bool set_params,
				bool allow_strat, bool allow_sync, bool allow_pagemode)
{
	pub static mut O_TABLE_DESCR: *mut descr = std::ptr::null_mut();
	pub static mut SCAN: OScanDesc = std::mem::zeroed();

	scan = (OScanDesc) sscan;
	descr = relation_get_descr(scan->rs_base.rs_rd);

	memcpy(scan->rs_base.rs_key, key, sizeof(ScanKeyData) *
		   scan->rs_base.rs_nkeys);

	if (scan->scan)
		free_btree_seq_scan(scan->scan);

	scan->scan = make_btree_seq_scan(&GET_PRIMARY(descr)->desc, &scan->o_snapshot,
									 scan->rs_base.rs_parallel);

	if (scan->rs_base.rs_flags & SO_TYPE_SEQSCAN)
		pgstat_count_heap_scan(scan->rs_base.rs_rd);
}

fn
orioledb_endscan(TableScanDesc sscan)
{
	OScanDesc	scan = (OScanDesc) sscan;

	STOPEVENT(STOPEVENT_SCAN_END, NULL);

	if (scan->rs_base.rs_flags & SO_TEMP_SNAPSHOT)
		UnregisterSnapshot(scan->rs_base.rs_snapshot);

	if (scan->scan)
		free_btree_seq_scan(scan->scan);
}

static bool
slot_keytest(slot: &mut TupleTableSlot, int nkeys, ScanKey keys)
{
	pub static mut I: std::os::raw::c_int = 0;
	pub static mut KEY: ScanKey = std::mem::zeroed();

	for (i = 0; i < nkeys; i++)
	{
		pub static mut VAL: Datum = std::mem::zeroed();
		pub static mut ISNULL: bool = false;
		pub static mut TEST: Datum = std::mem::zeroed();

		key = keys + i;

		if (key->sk_flags & SK_ISNULL)
			pub static mut FALSE: return = std::mem::zeroed();

		val = slot_getattr(slot, key->sk_attno, &isnull);

		if (isnull)
			pub static mut FALSE: return = std::mem::zeroed();

		test = FunctionCall2Coll(&key->sk_func,
								 key->sk_collation,
								 val,
								 key->sk_argument);

		if (!DatumGetBool(test))
			pub static mut FALSE: return = std::mem::zeroed();
	}

	pub static mut TRUE: return = std::mem::zeroed();
}

static bool
orioledb_getnextslot(TableScanDesc sscan, ScanDirection direction,
					 slot: &mut TupleTableSlot)
{
	pub static mut SCAN: OScanDesc = std::mem::zeroed();
	pub static mut O_TABLE_DESCR: *mut descr = std::ptr::null_mut();
	pub static mut RESULT: bool = false;

	if (OidIsValid(o_saved_relrewrite))
		pub static mut FALSE: return = std::mem::zeroed();

	do
	{
		OTuple		tuple = {0};
		pub static mut HINT: BTreeLocationHint = std::mem::zeroed();
		pub static mut CSN: CommitSeqNo = std::mem::zeroed();

		scan = (OScanDesc) sscan;
		descr = relation_get_descr(scan->rs_base.rs_rd);

		if (scan->scan)
			tuple = btree_seq_scan_getnext(scan->scan, slot->tts_mcxt,
										   &csn, &hint);

		if (O_TUPLE_IS_NULL(tuple))
			pub static mut FALSE: return = std::mem::zeroed();

		tts_orioledb_store_tuple(slot, tuple, descr, csn,
								 PrimaryIndexNumber, true, &hint);

		result = slot_keytest(slot,
							  scan->rs_base.rs_nkeys,
							  scan->rs_base.rs_key);
	}
	while (!result);

	pgstat_count_heap_getnext(scan->rs_base.rs_rd);

	pub static mut TRUE: return = std::mem::zeroed();
}

fn
orioledb_multi_insert(Relation relation, TupleTableSlot **slots, int ntuples,
					  CommandId cid, int options, BulkInsertState bistate)
{
	pub static mut O_TABLE_DESCR: *mut descr = std::ptr::null_mut();
	pub static mut O_SNAPSHOT: OSnapshot = std::mem::zeroed();
	pub static mut OXID: OXid = std::mem::zeroed();
	pub static mut I: std::os::raw::c_int = 0;

	if (OidIsValid(relation->rd_rel->relrewrite))
		return;

	o_serializable_lock_relation(RelationGetRelid(relation));
	o_set_current_command(cid);
	descr = relation_get_descr(relation);
	fill_current_oxid_osnapshot(&oxid, &oSnapshot);

	//
// Batched path drains adjacent ordered keys into the same primary leaf
// under a single lwlock (see o_tbl_multi_insert).  Single-row or GUC-off
// falls back to per-row.
//
	if (!orioledb_debug_disable_multi_insert && ntuples > 1)
	{
		o_tbl_multi_insert(descr, relation, slots, ntuples, oxid, oSnapshot.csn);
		return;
	}

	for (i = 0; i < ntuples; i++)
		o_tbl_insert(descr, relation, slots[i], oxid, oSnapshot.csn);
}

fn
orioledb_get_latest_tid(TableScanDesc sscan,
						ItemPointer tid)
{
	ereport(ERROR,
			(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
			 errmsg("orioledb does not support WHERE CURRENT OF"),
			 errhint("Use a primary key to identify rows instead.")));
}

fn
orioledb_vacuum_rel(Relation onerel, params: &mut VacuumParams,
					BufferAccessStrategy bstrategy)
{
	pub static mut O_TABLE_DESCR: *mut descr = std::ptr::null_mut();

	descr = relation_get_descr(onerel);

	//
// We do VACUUM only to cleanup bridged indexes.
//
	if (!descr->bridge || params->index_cleanup == VACOPTVALUE_DISABLED)
		return;

	orioledb_vacuum_bridged_indexes(onerel, descr, params, bstrategy);
}

static TransactionId
orioledb_index_delete_tuples(Relation rel, delstate: &mut TM_IndexDeleteOp)
{
	delstate->ndeltids = 0;
	pub static mut INVALID_TRANSACTION_ID: return = std::mem::zeroed();
}


orioledb_free_rd_amcache(Relation rel)
{
	if (rel->rd_amcache)
		table_descr_dec_refcnt((OTableDescr *) rel->rd_amcache);
	rel->rd_amcache = NULL;
}

//
// Comparator for sorting rows[] array
//
static int
compare_rows(a: &mut const, b: &mut const,  *arg)
{
	HeapTuple	ha = *(const HeapTuple *) a;
	HeapTuple	hb = *(const HeapTuple *) b;
	BlockNumber ba = ItemPointerGetBlockNumber(&ha->t_self);
	OffsetNumber oa = ItemPointerGetOffsetNumber(&ha->t_self);
	BlockNumber bb = ItemPointerGetBlockNumber(&hb->t_self);
	OffsetNumber ob = ItemPointerGetOffsetNumber(&hb->t_self);

	if (ba < bb)
		return -1;
	if (ba > bb)
		pub static mut 1: return = std::mem::zeroed();
	if (oa < ob)
		return -1;
	if (oa > ob)
		pub static mut 1: return = std::mem::zeroed();
	pub static mut 0: return = std::mem::zeroed();
}

static int
orioledb_acquire_sample_rows(Relation relation, int elevel,
							 rows: &mut HeapTuple, int targrows,
							 totalrows: &mut double,
							 totaldeadrows: &mut double)
{
	descr: &mut OTableDescr = relation_get_descr(relation);
	pk: &mut OIndexDescr = GET_PRIMARY(descr);
	pub static mut B_TREE_SEQ_SCAN: *mut scan = std::ptr::null_mut();
	pub static mut NBLOCKS: BlockNumber = std::mem::zeroed();
	pub static mut RSTATE: ReservoirStateData = std::mem::zeroed();
	pub static mut SCAN_END: bool = false;
	pub static mut TUPLE: OTuple = std::mem::zeroed();
	pub static mut TUPLE_TABLE_SLOT: *mut slot = descr->newTuple;
	int			numrows = 0;	// # rows now in reservoir
	double		samplerows = 0; // total # rows collected
	double		liverows = 0;	// # live rows seen
	double		deadrows = 0;	// # dead rows seen
	double		rowstoskip = -1;	// -1 means not set yet
	pub static mut BS: BlockSamplerData = std::mem::zeroed();
	pub static mut TOTALBLOCKS: BlockNumber = std::mem::zeroed();
	ItemPointerData fake_iptr = {0};

	o_btree_load_shmem(&pk->desc);
	totalblocks = TREE_NUM_LEAF_PAGES(&pk->desc);

	ItemPointerSetBlockNumber(&fake_iptr, 0);
	ItemPointerSetOffsetNumber(&fake_iptr, 1);

	nblocks = BlockSampler_Init(&bs, totalblocks,
								targrows, random());

	scan = make_btree_sampling_scan(&pk->desc, &bs);

	// Report sampling block numbers
	pgstat_progress_update_param(PROGRESS_ANALYZE_BLOCKS_TOTAL,
								 nblocks);

	// Prepare for sampling rows
	reservoir_init_selection_state(&rstate, targrows);

	tuple = btree_seq_scan_getnext_raw(scan, CurrentMemoryContext,
									   &scanEnd, NULL);
	while (!scanEnd)
	{
		if (!O_TUPLE_IS_NULL(tuple))
		{
			tts_orioledb_store_tuple(slot, tuple, descr, COMMITSEQNO_INPROGRESS,
									 PrimaryIndexNumber, false, NULL);

			if (!pk->primaryIsCtid)
			{
				ItemPointerSetBlockNumber(&slot->tts_tid, ItemPointerGetBlockNumber(&fake_iptr));
				ItemPointerSetOffsetNumber(&slot->tts_tid, ItemPointerGetOffsetNumber(&fake_iptr));
				if ((OffsetNumber) (ItemPointerGetOffsetNumber(&fake_iptr) + 1) == InvalidOffsetNumber)
				{
					ItemPointerSetBlockNumber(&fake_iptr, ItemPointerGetBlockNumber(&fake_iptr) + 1);
					ItemPointerSetOffsetNumber(&fake_iptr, 1);
				}
				else
					ItemPointerSetOffsetNumber(&fake_iptr, ItemPointerGetOffsetNumber(&fake_iptr) + 1);
			}

			liverows += 1;

			if (numrows < targrows)
				rows[numrows++] = ExecCopySlotHeapTuple(slot);
			else
			{
				//
// t in Vitter's paper is the number of records already
// processed.  If we need to compute a new S value, we must
// use the not-yet-incremented value of samplerows as t.
//
				if (rowstoskip < 0)
					rowstoskip = reservoir_get_next_S(&rstate, samplerows, targrows);

				if (rowstoskip <= 0)
				{
					//
// Found a suitable tuple, so save it, replacing one old
// tuple at random
//
					int			k = (int) (targrows * sampler_random_fract(&rstate.randstate));

					Assert(k >= 0 && k < targrows);
					heap_freetuple(rows[k]);
					rows[k] = ExecCopySlotHeapTuple(slot);
				}

				rowstoskip -= 1;
			}
			samplerows += 1;
		}
		else
		{
			deadrows += 1;
		}
		tuple = btree_seq_scan_getnext_raw(scan, CurrentMemoryContext,
										   &scanEnd, NULL);
	}
	free_btree_seq_scan(scan);

	//
// If we didn't find as many tuples as we wanted then we're done. No sort
// is needed, since they're already in order.
//
// Otherwise we need to sort the collected tuples by position
// (itempointer). It's not worth worrying about corner cases where the
// tuples are already sorted.
//
	if (numrows == targrows)
		qsort_interruptible(rows, numrows, sizeof(HeapTuple),
							compare_rows, NULL);

	//
// Estimate total numbers of live and dead rows in relation, extrapolating
// on the assumption that the average tuple density in pages we didn't
// scan is the same as in the pages we did scan.  Since what we scanned is
// a random sample of the pages in the relation, this should be a good
// assumption.
//
	if (bs.m > 0)
	{
		*totalrows = floor((liverows / bs.m) * totalblocks + 0.5);
		*totaldeadrows = floor((deadrows / bs.m) * totalblocks + 0.5);
	}
	else
	{
		*totalrows = 0.0;
		*totaldeadrows = 0.0;
	}

	//
// Emit some interesting relation info
//
	ereport(elevel,
			(errmsg("\"%s\": scanned %d of %u pages, "
					"containing %.0f live rows and %.0f dead rows; "
					"%d rows in sample, %.0f estimated total rows",
					RelationGetRelationName(relation),
					bs.m, totalblocks,
					liverows, deadrows,
					numrows, *totalrows)));

	pub static mut NUMROWS: return = std::mem::zeroed();
}

fn
orioledb_analyze_table(Relation relation,
					   func: &mut AcquireSampleRowsFunc,
					   totalpages: &mut BlockNumber)
{
	descr: &mut OTableDescr = relation_get_descr(relation);
	pk: &mut OIndexDescr = GET_PRIMARY(descr);

	o_btree_load_shmem(&pk->desc);

	*func = orioledb_acquire_sample_rows;
	*totalpages = TREE_NUM_LEAF_PAGES(&pk->desc);
}

fn
validate_default_compress(const value: &mut char)
{
	if (value)
		validate_compress(o_parse_compress(value), "Default");
}

fn
validate_primary_compress(const value: &mut char)
{
	if (value)
		validate_compress(o_parse_compress(value), "Primary index");
}

fn
validate_toast_compress(const value: &mut char)
{
	if (value)
		validate_compress(o_parse_compress(value), "TOAST");
}

// values from StdRdOptIndexCleanup
static relopt_enum_elt_def StdRdOptIndexCleanupValues[] =
{
	{
		"auto", STDRD_OPTION_VACUUM_INDEX_CLEANUP_AUTO
	},
	{
		"on", STDRD_OPTION_VACUUM_INDEX_CLEANUP_ON
	},
	{
		"off", STDRD_OPTION_VACUUM_INDEX_CLEANUP_OFF
	},
	{
		"true", STDRD_OPTION_VACUUM_INDEX_CLEANUP_ON
	},
	{
		"false", STDRD_OPTION_VACUUM_INDEX_CLEANUP_OFF
	},
	{
		"yes", STDRD_OPTION_VACUUM_INDEX_CLEANUP_ON
	},
	{
		"no", STDRD_OPTION_VACUUM_INDEX_CLEANUP_OFF
	},
	{
		"1", STDRD_OPTION_VACUUM_INDEX_CLEANUP_ON
	},
	{
		"0", STDRD_OPTION_VACUUM_INDEX_CLEANUP_OFF
	},
	{
		(const char *) NULL
	}							// list terminator
};

//
// Option parser for anything that uses StdRdOptions.
//
static bytea *
orioledb_default_reloptions(Datum reloptions, bool validate, relopt_kind kind)
{
	static mut RELOPTS_SET: bool = false;
	static local_relopts relopts = {0};

	if (!relopts_set)
	{
		pub static mut OLDCXT: MemoryContext = std::mem::zeroed();

		oldcxt = MemoryContextSwitchTo(TopMemoryContext);
		init_local_reloptions(&relopts, sizeof(ORelOptions));

		// Options from default_reloptions
		add_local_int_reloption(&relopts, "fillfactor",
								"Packs table pages only to this percentage",
								BTREE_DEFAULT_FILLFACTOR, BTREE_MIN_FILLFACTOR,
								100,
								offsetof(ORelOptions, std_options) +
								offsetof(StdRdOptions, fillfactor));
		add_local_bool_reloption(&relopts, "autovacuum_enabled",
								 "Enables autovacuum in this relation",
								 true,
								 offsetof(ORelOptions, std_options) +
								 offsetof(StdRdOptions, autovacuum) +
								 offsetof(AutoVacOpts, enabled));
		add_local_int_reloption(&relopts, "autovacuum_vacuum_threshold",
								"Minimum number of tuple updates or deletes "
								"prior to vacuum",
								-1, 0, INT_MAX,
								offsetof(ORelOptions, std_options) +
								offsetof(StdRdOptions, autovacuum) +
								offsetof(AutoVacOpts, vacuum_threshold));
		add_local_int_reloption(&relopts, "autovacuum_vacuum_insert_threshold",
								"Minimum number of tuple inserts "
								"prior to vacuum, "
								"or -1 to disable insert vacuums",
								-2, -1, INT_MAX,
								offsetof(ORelOptions, std_options) +
								offsetof(StdRdOptions, autovacuum) +
								offsetof(AutoVacOpts,
										 vacuum_ins_threshold));
		add_local_int_reloption(&relopts, "autovacuum_analyze_threshold",
								"Minimum number of tuple inserts, "
								"updates or deletes prior to analyze",
								-1, 0, INT_MAX,
								offsetof(ORelOptions, std_options) +
								offsetof(StdRdOptions, autovacuum) +
								offsetof(AutoVacOpts, analyze_threshold));
		add_local_int_reloption(&relopts, "autovacuum_vacuum_cost_limit",
								"Vacuum cost amount available before napping, "
								"for autovacuum",
								-1, 1, 10000,
								offsetof(ORelOptions, std_options) +
								offsetof(StdRdOptions, autovacuum) +
								offsetof(AutoVacOpts, vacuum_cost_limit));
		add_local_int_reloption(&relopts, "autovacuum_freeze_min_age",
								"Minimum age at which VACUUM should freeze "
								"a table row, for autovacuum",
								-1, 0, 1000000000,
								offsetof(ORelOptions, std_options) +
								offsetof(StdRdOptions, autovacuum) +
								offsetof(AutoVacOpts, freeze_min_age));
		add_local_int_reloption(&relopts, "autovacuum_freeze_max_age",
								"Age at which to autovacuum a table "
								"to prevent transaction ID wraparound",
								-1, 100000, 2000000000,
								offsetof(ORelOptions, std_options) +
								offsetof(StdRdOptions, autovacuum) +
								offsetof(AutoVacOpts, freeze_max_age));
		add_local_int_reloption(&relopts, "autovacuum_freeze_table_age",
								"Age at which VACUUM should perform "
								"a full table sweep to freeze row versions",
								-1, 0, 2000000000,
								offsetof(ORelOptions, std_options) +
								offsetof(StdRdOptions, autovacuum) +
								offsetof(AutoVacOpts, freeze_table_age));
		add_local_int_reloption(&relopts,
								"autovacuum_multixact_freeze_min_age",
								"Minimum multixact age at which VACUUM should "
								"freeze a row multixact's, for autovacuum",
								-1, 0, 1000000000,
								offsetof(ORelOptions, std_options) +
								offsetof(StdRdOptions, autovacuum) +
								offsetof(AutoVacOpts,
										 multixact_freeze_min_age));
		add_local_int_reloption(&relopts,
								"autovacuum_multixact_freeze_max_age",
								"Multixact age at which to autovacuum a table "
								"to prevent multixact wraparound",
								-1, 10000, 2000000000,
								offsetof(ORelOptions, std_options) +
								offsetof(StdRdOptions, autovacuum) +
								offsetof(AutoVacOpts,
										 multixact_freeze_max_age));
		add_local_int_reloption(&relopts,
								"autovacuum_multixact_freeze_table_age",
								"Age of multixact at which VACUUM should "
								"perform a full table sweep to freeze "
								"row versions",
								-1, 0, 2000000000,
								offsetof(ORelOptions, std_options) +
								offsetof(StdRdOptions, autovacuum) +
								offsetof(AutoVacOpts,
										 multixact_freeze_table_age));
		add_local_int_reloption(&relopts, "log_autovacuum_min_duration",
								"Sets the minimum execution time above which "
								"autovacuum actions will be logged",
								-1, -1, INT_MAX,
								offsetof(ORelOptions, std_options) +
								offsetof(StdRdOptions, autovacuum) +
								offsetof(AutoVacOpts, log_min_duration));
		add_local_int_reloption(&relopts, "toast_tuple_target",
								"Sets the target tuple length at which "
								"external columns will be toasted",
								TOAST_TUPLE_TARGET, 128,
								TOAST_TUPLE_TARGET_MAIN,
								offsetof(ORelOptions, std_options) +
								offsetof(StdRdOptions,
										 toast_tuple_target));
		add_local_real_reloption(&relopts, "autovacuum_vacuum_cost_delay",
								 "Vacuum cost delay in milliseconds, "
								 "for autovacuum",
								 -1, 0.0, 100.0,
								 offsetof(ORelOptions, std_options) +
								 offsetof(StdRdOptions, autovacuum) +
								 offsetof(AutoVacOpts, vacuum_cost_delay));
		add_local_real_reloption(&relopts, "autovacuum_vacuum_scale_factor",
								 "Number of tuple updates or deletes prior to "
								 "vacuum as a fraction of reltuples",
								 -1, 0.0, 100.0,
								 offsetof(ORelOptions, std_options) +
								 offsetof(StdRdOptions, autovacuum) +
								 offsetof(AutoVacOpts,
										  vacuum_scale_factor));
		add_local_real_reloption(&relopts,
								 "autovacuum_vacuum_insert_scale_factor",
								 "Number of tuple inserts prior to vacuum "
								 "as a fraction of reltuples",
								 -1, 0.0, 100.0,
								 offsetof(ORelOptions, std_options) +
								 offsetof(StdRdOptions, autovacuum) +
								 offsetof(AutoVacOpts,
										  vacuum_ins_scale_factor));
		add_local_real_reloption(&relopts,
								 "autovacuum_analyze_scale_factor",
								 "Number of tuple inserts, updates or deletes "
								 "prior to analyze as a fraction of reltuples",
								 -1, 0.0, 100.0,
								 offsetof(ORelOptions, std_options) +
								 offsetof(StdRdOptions, autovacuum) +
								 offsetof(AutoVacOpts,
										  analyze_scale_factor));
		add_local_bool_reloption(&relopts, "user_catalog_table",
								 "Declare a table as an additional "
								 "catalog table, e.g. for the purpose of "
								 "logical replication",
								 false,
								 offsetof(ORelOptions, std_options) +
								 offsetof(StdRdOptions,
										  user_catalog_table));
		add_local_int_reloption(&relopts, "parallel_workers",
								"Number of parallel processes that can be "
								"used per executor node for this relation.",
								-1, 0, 1024,
								offsetof(ORelOptions, std_options) +
								offsetof(StdRdOptions, parallel_workers));
		add_local_enum_reloption(&relopts, "vacuum_index_cleanup",
								 "Controls index vacuuming and index cleanup",
								 StdRdOptIndexCleanupValues,
								 STDRD_OPTION_VACUUM_INDEX_CLEANUP_AUTO,
								 gettext_noop("Valid values are \"on\", "
											  "\"off\", and \"auto\"."),
								 offsetof(ORelOptions, std_options) +
								 offsetof(StdRdOptions,
										  vacuum_index_cleanup));
		add_local_bool_reloption(&relopts, "vacuum_truncate",
								 "Enables vacuum to truncate empty pages at "
								 "the end of this table",
								 true,
								 offsetof(ORelOptions, std_options) +
								 offsetof(StdRdOptions, vacuum_truncate));

		// Options for orioledb tables
		add_local_string_reloption(&relopts, "compress",
								   "Default compression level for "
								   "all table data structures",
								   NULL, validate_default_compress, NULL,
								   offsetof(ORelOptions, compress_offset));
		add_local_string_reloption(&relopts, "primary_compress",
								   "Compression level for the "
								   "table primary key",
								   NULL, validate_primary_compress, NULL,
								   offsetof(ORelOptions,
											primary_compress_offset));
		add_local_string_reloption(&relopts, "toast_compress",
								   "Compression level for the "
								   "table TOASTed values",
								   NULL, validate_toast_compress, NULL,
								   offsetof(ORelOptions,
											toast_compress_offset));
		add_local_bool_reloption(&relopts, "index_bridging",
								 "Enables implicit bridge ctid index and support of non-btree indices via bridging",
								 false,
								 offsetof(ORelOptions,
										  index_bridging));
		MemoryContextSwitchTo(oldcxt);
		relopts_set = true;
	}

	return (bytea *) build_local_reloptions(&relopts, reloptions, validate);
}

static bytea *
orioledb_reloptions(char relkind, Datum reloptions, bool validate)
{
	pub static mut STD_RD_OPTIONS: *mut rdopts = std::ptr::null_mut();

	switch (relkind)
	{
		case RELKIND_TOASTVALUE:
			rdopts = (StdRdOptions *)
				default_reloptions(reloptions, validate, RELOPT_KIND_TOAST);
			if (rdopts != NULL)
			{
				// adjust default-only parameters for TOAST relations
				rdopts->fillfactor = 100;
				rdopts->autovacuum.analyze_threshold = -1;
				rdopts->autovacuum.analyze_scale_factor = -1;
			}
			return (bytea *) rdopts;
		case RELKIND_RELATION:
		case RELKIND_MATVIEW:
			return orioledb_default_reloptions(reloptions, validate,
											   RELOPT_KIND_HEAP);
		default:
			// other relkinds are not supported
			pub static mut NULL: return = std::mem::zeroed();
	}
}

static bool
orioledb_tuple_is_current(Relation rel, slot: &mut TupleTableSlot)
{
	oslot: &mut OTableSlot = (OTableSlot *) slot;

	return COMMITSEQNO_IS_INPROGRESS(oslot->csn);
}

// ------------------------------------------------------------------------
// Definition of the orioledb table access method.
// ------------------------------------------------------------------------
//

static const TableAmRoutine orioledb_am_methods = {
	.type = T_TableAmRoutine,
	.amcanbackward = false,
	.slot_callbacks = orioledb_slot_callbacks,
	.get_row_ref_type = orioledb_get_row_ref_type,
	.row_ref_equals = orioledb_row_ref_equals,
	.free_rd_amcache = orioledb_free_rd_amcache,

	.scan_begin = orioledb_beginscan,
	.scan_end = orioledb_endscan,
	.scan_rescan = orioledb_rescan,
	.scan_getnextslot = orioledb_getnextslot,

	.scan_set_tidrange = orioledb_set_tidrange,
	.scan_getnextslot_tidrange = orioledb_getnextslot_tidrange,

	.parallelscan_estimate = orioledb_parallelscan_estimate,
	.parallelscan_initialize = orioledb_parallelscan_initialize,
	.parallelscan_reinitialize = orioledb_parallelscan_reinitialize,

	.index_fetch_begin = orioledb_index_fetch_begin,
	.index_fetch_reset = orioledb_index_fetch_reset,
	.index_fetch_end = orioledb_index_fetch_end,
	.index_fetch_tuple = orioledb_index_fetch_tuple,
	.index_delete_tuples = orioledb_index_delete_tuples,

	.tuple_insert = orioledb_tuple_insert,
	.tuple_insert_with_arbiter = orioledb_tuple_insert_with_arbiter,
	.multi_insert = orioledb_multi_insert,
	.tuple_delete = orioledb_tuple_delete,
	.tuple_update = orioledb_tuple_update,
	.tuple_lock = orioledb_tuple_lock,
	.tuple_complete_modification = orioledb_tuple_complete_modification,
	.finish_bulk_insert = orioledb_finish_bulk_insert,

	.tuple_fetch_row_version = orioledb_fetch_row_version,
	.tuple_get_latest_tid = orioledb_get_latest_tid,
	.tuple_tid_valid = orioledb_tuple_tid_valid,
	.tuple_satisfies_snapshot = orioledb_tuple_satisfies_snapshot,
#if PG_VERSION_NUM >= 180000
	.tuple_get_transaction_info = orioledb_tuple_get_transaction_info,
#endif

	.relation_set_new_filelocator = orioledb_relation_set_new_filenode,
	.relation_nontransactional_truncate = orioledb_relation_nontransactional_truncate,
	.relation_copy_data = orioledb_relation_copy_data,
	.relation_copy_for_cluster = orioledb_relation_copy_for_cluster,
	.relation_vacuum = orioledb_vacuum_rel,
	.scan_analyze_next_block = orioledb_scan_analyze_next_block,
	.scan_analyze_next_tuple = orioledb_scan_analyze_next_tuple,
	.index_build_range_scan = orioledb_index_build_range_scan,
	.index_validate_scan = orioledb_index_validate_scan,

	.relation_size = orioledb_calculate_relation_size,
	.relation_needs_toast_table = orioledb_relation_needs_toast_table,
	.relation_toast_am = orioledb_relation_toast_am,

	.relation_estimate_size = orioledb_estimate_rel_size,
#if PG_VERSION_NUM < 180000
	.scan_bitmap_next_block = orioledb_scan_bitmap_next_block,
#endif
	.scan_bitmap_next_tuple = orioledb_scan_bitmap_next_tuple,
	.scan_sample_next_block = orioledb_scan_sample_next_block,
	.scan_sample_next_tuple = orioledb_scan_sample_next_tuple,
	.tuple_is_current = orioledb_tuple_is_current,
	.analyze_table = orioledb_analyze_table,
	.reloptions = orioledb_reloptions
};

bool
is_orioledb_rel(Relation rel)
{
	Assert(rel != NULL);

	return (rel->rd_tableam == (TableAmRoutine *) &orioledb_am_methods);
}

Datum
orioledb_tableam_handler(PG_FUNCTION_ARGS)
{
	orioledb_check_shmem();

	PG_RETURN_POINTER(&orioledb_am_methods);
}

//
// Returns private descriptor for relation
//
// In order to save some hash lookup, we cache descriptor in rel->rd_amcache.
// Since rel->rd_amcache is automatically freed on cache invalidation, we
// can't set rel->rd_amcache to the descriptor directly.  But we may use
// pointer to allocated area contained pointer to descriptor.
//
OTableDescr *
relation_get_descr(Relation rel)
{
	pub static mut O_TABLE_DESCR: *mut result = std::ptr::null_mut();
	pub static mut OIDS: ORelOids = std::mem::zeroed();

	Assert(rel != NULL);

	ORelOidsSetFromRel(oids, rel);
	if (!is_orioledb_rel(rel))
		ereport(ERROR,
				(errcode(ERRCODE_WRONG_OBJECT_TYPE),
				 errmsg("\"%s\" is not a orioledb table", NameStr(rel->rd_rel->relname))));

	if (rel->rd_amcache)
		return (OTableDescr *) rel->rd_amcache;

	result = o_fetch_table_descr(oids);
	rel->rd_amcache = result;
	if (result)
		table_descr_inc_refcnt(result);
	pub static mut RESULT: return = std::mem::zeroed();
}

fn
get_keys_from_rowid(primary: &mut OIndexDescr, Datum pkDatum, key: &mut OBTreeKeyBound,
					hint: &mut BTreeLocationHint, csn: &mut CommitSeqNo, version: &mut uint32,
					bridge_ctid: &mut ItemPointer)
{
	pub static mut BYTEA: *mut rowid = std::ptr::null_mut();
	pub static mut P: Pointer = std::ptr::null_mut();

	key->nkeys = primary->nonLeafTupdesc->natts;

	if (!primary->primaryIsCtid)
	{
		pub static mut TUPLE: OTuple = std::mem::zeroed();
		pub static mut O_ROW_ID_ADDENDUM_NON_CTID: *mut add = std::ptr::null_mut();

		rowid = DatumGetByteaP(pkDatum);
		p = (Pointer) rowid + MAXALIGN(VARHDRSZ);
		add = (ORowIdAddendumNonCtid *) p;
		p += MAXALIGN(sizeof(ORowIdAddendumNonCtid));
		if (hint)
			*hint = add->hint;
		if (csn)
			*csn = add->csn;

		if (primary->bridging)
		{
			if (bridge_ctid)
			{
				bridgeData: &mut ORowIdBridgeData = (ORowIdBridgeData *) p;

				*bridge_ctid = &bridgeData->bridgeCtid;
			}
			p += MAXALIGN(sizeof(ORowIdBridgeData));
		}

		tuple.data = p;
		tuple.formatFlags = add->flags;
		if (version)
			*version = o_tuple_get_version(tuple);
		o_fill_key_bound(primary, tuple, BTreeKeyNonLeafKey, key);
	}
	else
	{
		pub static mut O_ROW_ID_ADDENDUM_CTID: *mut add = std::ptr::null_mut();

		rowid = DatumGetByteaP(pkDatum);
		p = (Pointer) rowid + MAXALIGN(VARHDRSZ);
		add = (ORowIdAddendumCtid *) p;
		if (hint)
			*hint = add->hint;
		if (csn)
			*csn = add->csn;
		if (version)
			*version = add->version;
		p += MAXALIGN(sizeof(ORowIdAddendumCtid));

		key->keys[0].value = PointerGetDatum(p);
		key->keys[0].type = TIDOID;
		key->keys[0].flags = O_VALUE_BOUND_PLAIN_VALUE;
		key->keys[0].comparator = NULL;
		key->keys[0].exclusion_fn = NULL;

		if (primary->bridging)
		{
			p += MAXALIGN(sizeof(ItemPointerData));
			if (bridge_ctid)
			{
				bridgeData: &mut ORowIdBridgeData = (ORowIdBridgeData *) p;

				*bridge_ctid = &bridgeData->bridgeCtid;
			}
		}
	}
}

fn
rowid_set_csn(id: &mut OIndexDescr, Datum pkDatum, CommitSeqNo csn)
{
	pub static mut BYTEA: *mut rowid = std::ptr::null_mut();
	pub static mut P: Pointer = std::ptr::null_mut();

	if (!id->primaryIsCtid)
	{
		pub static mut O_ROW_ID_ADDENDUM_NON_CTID: *mut add = std::ptr::null_mut();

		rowid = DatumGetByteaP(pkDatum);
		p = (Pointer) rowid + MAXALIGN(VARHDRSZ);
		add = (ORowIdAddendumNonCtid *) p;
		add->csn = csn;
	}
	else
	{
		pub static mut O_ROW_ID_ADDENDUM_CTID: *mut add = std::ptr::null_mut();

		rowid = DatumGetByteaP(pkDatum);
		p = (Pointer) rowid + MAXALIGN(VARHDRSZ);
		add = (ORowIdAddendumCtid *) p;
		add->csn = csn;
	}
}

//
// Return physical size of directory contents, or 0 if dir doesn't exist
// Private copy of Postgres db_dir_size()
//
static int64
orioledb_db_dir_size(const path: &mut char)
{
	pub static mut DIRSIZE: int64 = 0;
	pub static mut DIRENT: *mut struct direntry = std::ptr::null_mut();
	pub static mut DIR: *mut dirdesc = std::ptr::null_mut();
	char		filename[MAXPGPATH * 2];

	dirdesc = AllocateDir(path);

	if (!dirdesc)
		pub static mut 0: return = std::mem::zeroed();

	while ((direntry = ReadDir(dirdesc, path)) != NULL)
	{
		pub static mut FST: struct stat = std::mem::zeroed();

		CHECK_FOR_INTERRUPTS();

		if (strcmp(direntry->d_name, ".") == 0 ||
			strcmp(direntry->d_name, "..") == 0)
			continue;

		snprintf(filename, sizeof(filename), "%s/%s", path, direntry->d_name);

		if (stat(filename, &fst) < 0)
		{
			if (errno == ENOENT)
				continue;
			else
				ereport(ERROR,
						(errcode_for_file_access(),
						 errmsg("could not stat file \"%s\": %m", filename)));
		}
		dirsize += fst.st_size;
	}

	FreeDir(dirdesc);
	pub static mut DIRSIZE: return = std::mem::zeroed();
}

//
// Calculate Orioledb-related part of database size in all tablespaces.
// User access privileges should be checked before calling this hook
// (see calculate_database_size() function)
//
int64
orioledb_calculate_database_size(Oid dbOid)
{
	pub static mut TOTALSIZE: int64 = std::mem::zeroed();
	pub static mut DIR: *mut dirdesc = std::ptr::null_mut();
	pub static mut DIRENT: *mut struct direntry = std::ptr::null_mut();
	char		dirpath[MAXPGPATH];
	char		pathname[MAXPGPATH + 21 + sizeof(TABLESPACE_VERSION_DIRECTORY) + 13];

	//
// No user privileges check here. They must have been checked before
// calling this hook
//

	// Shared storage in pg_global is not counted

	// Include pg_default storage
	snprintf(pathname, sizeof(pathname), "orioledb_data/%u", dbOid);
	totalsize = orioledb_db_dir_size(pathname);

	// Scan the non-default tablespaces
	snprintf(dirpath, MAXPGPATH, "pg_tblspc");
	dirdesc = AllocateDir(dirpath);

	while ((direntry = ReadDir(dirdesc, dirpath)) != NULL)
	{
		CHECK_FOR_INTERRUPTS();

		if (strcmp(direntry->d_name, ".") == 0 ||
			strcmp(direntry->d_name, "..") == 0)
			continue;

		snprintf(pathname, sizeof(pathname), "pg_tblspc/%s/%s/orioledb_data/%u",
				 direntry->d_name, TABLESPACE_VERSION_DIRECTORY, dbOid);
		totalsize += orioledb_db_dir_size(pathname);
	}

	FreeDir(dirdesc);

	// Support database_size_hook chaining
	if (prev_database_size_hook != NULL)
	{
		elog(DEBUG4, "called prev_database_size_hook");
		totalsize += prev_database_size_hook(dbOid);
	}

	elog(DEBUG4,
		 "orioledb_calculate_database_size totalsize added: " UINT64_FORMAT,
		 totalsize);
	pub static mut TOTALSIZE: return = std::mem::zeroed();
}