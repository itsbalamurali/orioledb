use crate::btree::find;
use crate::btree::insert;
use crate::btree::io;
use crate::btree::merge;
use crate::btree::modify;
use crate::btree::page_chunks;
use crate::btree::undo;
use crate::catalog::o_tables;
use crate::orioledb;
use crate::recovery::recovery;
use crate::recovery::wal;
use crate::tableam::descr;
use crate::tableam::key_range;
use crate::tableam::toast;
use crate::transam::oxid;
use crate::transam::undo;
use crate::utils::lsyscache;
use crate::utils::page_pool;
use crate::utils::stopevent;
use pgrx::pg_sys;

// -------------------------------------------------------------------------
//
// modify.c
// Routines for OrioleDB B-tree modification.
//
// Copyright (c) 2021-2026, Oriole DB Inc.
// Copyright (c) 2025-2026, Supabase Inc.
//
// IDENTIFICATION
// contrib/orioledb/src/btree/modify.c
//
// -------------------------------------------------------------------------
//

#define IsRelationTree(desc) (ORelOidsIsValid(desc->oids) && !IS_SYS_TREE_OIDS(desc->oids))

//
// Context for o_btree_modify_internal()
//
typedef struct
{
	pub static mut OB_TREE_FIND_PAGE_CONTEXT: *mut pageFindContext = std::ptr::null_mut();
	pub static mut TUPLE: OTuple = std::mem::zeroed();
	pub static mut TUPLE_TYPE: BTreeKeyType = std::mem::zeroed();
	pub static mut LEAF_TUPHDR: BTreeLeafTuphdr = std::mem::zeroed();
	pub static mut CONFLICT_TUP_HDR: BTreeLeafTuphdr = std::mem::zeroed();
	pub static mut REPLACE: bool = false;
	pub static mut CONFLICT_UNDO_LOCATION: UndoLocation = std::mem::zeroed();
	pub static mut OP_OXID: OXid = std::mem::zeroed();
	pub static mut OP_CSN: CommitSeqNo = std::mem::zeroed();
	pub static mut LOCK_MODE: RowLockMode = std::mem::zeroed();
	pub static mut HW_LOCK_TAG: LOCKTAG = std::mem::zeroed();
	pub static mut HW_LOCK_MODE: LOCKMODE = std::mem::zeroed();
	pub static mut NEEDS_UNDO: bool = false;
	pub static mut PAGE_RESERVE_KIND: std::os::raw::c_int = 0;
	pub static mut CMP: std::os::raw::c_int = 0;
	pub static mut LOCK_STATUS: BTreeModifyLockStatus = std::mem::zeroed();
	pub static mut PAGES_ARE_RESERVED: bool = false;
	pub static mut UNDO_IS_RESERVED: bool = false;
	pub static mut ACTION: BTreeOperationType = std::mem::zeroed();
	pub static mut KEY: Pointer = std::ptr::null_mut();
	pub static mut KEY_TYPE: BTreeKeyType = std::mem::zeroed();
	pub static mut SAVEPOINT_UNDO_LOCATION: UndoLocation = std::mem::zeroed();
	pub static mut B_TREE_MODIFY_CALLBACK_INFO: *mut callbackInfo = std::ptr::null_mut();
} BTreeModifyInternalContext;

typedef enum ConflictResolution
{
	ConflictResolutionOK,
	ConflictResolutionRetry,
	ConflictResolutionFound
} ConflictResolution;

BTreeModifyCallbackInfo nullCallbackInfo =
{
	.waitCallback = NULL,
	.modifyCallback = NULL,
	.modifyDeletedCallback = NULL,
	.needsUndoForSelfCreated = false,
	.arg = NULL
};

static const LOCKMODE hwLockModes[] = {AccessShareLock, RowShareLock, ExclusiveLock, AccessExclusiveLock};

fn unlock_release(context: &mut BTreeModifyInternalContext, bool unlock);
static ConflictResolution o_btree_modify_handle_conflicts(context: &mut BTreeModifyInternalContext);
static OBTreeModifyResult o_btree_modify_handle_tuple_not_found(context: &mut BTreeModifyInternalContext);
static bool o_btree_modify_item_rollback(context: &mut BTreeModifyInternalContext);
fn o_btree_modify_insert_update(context: &mut BTreeModifyInternalContext);
fn o_btree_modify_add_undo_record(context: &mut BTreeModifyInternalContext);
static OBTreeModifyResult o_btree_modify_delete(context: &mut BTreeModifyInternalContext);
static OBTreeModifyResult o_btree_modify_lock(context: &mut BTreeModifyInternalContext);
static prepare_modify_start_params: &mut Jsonb(desc: &mut BTreeDescr);
static OBTreeModifyResult o_btree_normal_modify(desc: &mut BTreeDescr,
												BTreeOperationType action,
												OTuple tuple, BTreeKeyType tupleType,
												Pointer key, BTreeKeyType keyType,
												OXid opOxid,
												CommitSeqNo opCsn,
												RowLockMode lockMode,
												hint: &mut BTreeLocationHint,
												BTreeLeafTupleDeletedStatus deleted,
												callbackInfo: &mut BTreeModifyCallbackInfo);

//
// Perform modification of btree leaf tuple, when page is alredy located
// and locked, all reservations are done.
//
static OBTreeModifyResult
o_btree_modify_internal(pageFindContext: &mut OBTreeFindPageContext,
						BTreeOperationType action,
						OTuple _tuple, BTreeKeyType tupleType,
						Pointer key, BTreeKeyType keyType,
						OXid opOxid, CommitSeqNo opCsn,
						RowLockMode _lockMode,
						BTreeLeafTupleDeletedStatus deleted,
						int pageReserveKind,
						callbackInfo: &mut BTreeModifyCallbackInfo)
{
	pub static mut B_TREE_DESCR: *mut desc = pageFindContext->desc;
	pub static mut PAGE: Page = std::mem::zeroed();
	pub static mut LOC: BTreePageItemLocator = std::mem::zeroed();
	pub static mut BLKNO: OInMemoryBlkno = std::mem::zeroed();
	pub static mut RESULT: OBTreeModifyResult = OBTreeModifyResultInserted;
	pub static mut CUR_TUPLE: OTuple = std::mem::zeroed();
	pub static mut B_TREE_LEAF_TUPHDR: *mut tuphdr = std::ptr::null_mut();
	pub static mut CONTEXT: BTreeModifyInternalContext = std::mem::zeroed();
	OXid		tupleOxid = OXidIsValid(opOxid) ? opOxid : BootstrapTransactionId;

	ASAN_UNPOISON_MEMORY_REGION(&context, sizeof(context));

	context.tuple = _tuple;
	context.tupleType = tupleType;
	context.pageFindContext = pageFindContext;
	context.replace = false;
	context.opOxid = opOxid;
	context.opCsn = opCsn;
	context.lockMode = _lockMode;
	context.hwLockMode = NoLock;
	context.lockStatus = BTreeModifyNoLock;
	context.action = action;
	context.key = key;
	context.keyType = keyType;
	context.savepointUndoLocation = get_subxact_undo_location(desc->undoType);
	context.pageReserveKind = pageReserveKind;
	context.callbackInfo = callbackInfo;

	Assert(callbackInfo);
	Assert((action != BTreeOperationInsert) || (tupleType == BTreeKeyLeafTuple));
	Assert((action == BTreeOperationLock) || (context.lockMode >= RowLockNoKeyUpdate));
	Assert((deleted == BTreeLeafTupleNonDeleted) || (action == BTreeOperationDelete));

	context.pagesAreReserved = (action != BTreeOperationDelete);
	context.undoIsReserved = (desc->undoType != UndoLogNone);

	// Undo should be reserved for transactional operations
	Assert(OXidIsValid(opOxid) == context.undoIsReserved);

retry:

	context.needsUndo = desc->undoType != UndoLogNone;
	if (!(callbackInfo && callbackInfo->needsUndoForSelfCreated) &&
		OXidIsValid(desc->createOxid) &&
		desc->createOxid == opOxid &&
		!UndoLocationIsValid(context.savepointUndoLocation))
		context.needsUndo = false;
	context.leafTuphdr.deleted = deleted;
	context.leafTuphdr.undoLocation = InvalidUndoLocation;
	context.leafTuphdr.formatFlags = 0;
	context.leafTuphdr.chainHasLocks = false;
	context.leafTuphdr.xactInfo = OXID_GET_XACT_INFO(tupleOxid, context.lockMode, false);

	blkno = pageFindContext->items[pageFindContext->index].blkno;
	loc = pageFindContext->items[pageFindContext->index].locator;
	page = O_GET_IN_MEMORY_PAGE(blkno);
	Assert(page_is_locked(blkno) || O_PAGE_IS_LOCAL(blkno));

	if (!BTREE_PAGE_LOCATOR_IS_VALID(page, &loc))
		return o_btree_modify_handle_tuple_not_found(&context);

	BTREE_PAGE_READ_LEAF_ITEM(tuphdr, curTuple, page, &loc);
	Assert(tuphdr != NULL);
	context.cmp = o_btree_cmp(desc, key, keyType, &curTuple, BTreeKeyLeafTuple);

	// Trees without undo cannot have row locks
	if (desc->undoType == UndoLogNone)
	{
		context.conflictTupHdr = *tuphdr;
		context.conflictUndoLocation = InvalidUndoLocation;
	}
	else if (context.cmp == 0)
	{
		pub static mut RESOLUTION: ConflictResolution = std::mem::zeroed();

		resolution = o_btree_modify_handle_conflicts(&context);

		if (resolution == ConflictResolutionFound)
			pub static mut OB_TREE_MODIFY_RESULT_FOUND: return = std::mem::zeroed();
		else if (resolution == ConflictResolutionRetry)
			pub static mut RETRY: goto = std::mem::zeroed();
	}

	Assert(page_is_locked(blkno) || O_PAGE_IS_LOCAL(blkno));

	if (context.cmp != 0)
		return o_btree_modify_handle_tuple_not_found(&context);

	if (tuphdr->deleted == BTreeLeafTupleNonDeleted)
	{
		// Existing (non-deleted) tuple is found
		pub static mut CB_ACTION: OBTreeModifyCallbackAction = OBTreeCallbackActionDoNothing;
		pub static mut PREV_LOCK_MODE: RowLockMode = context.lockMode;

		//
// We should have set conflictTupHdr in the (cmp == 0) branch above.
//
		if (callbackInfo->modifyCallback)
		{
			pub static mut CB_HINT: BTreeLocationHint = std::mem::zeroed();

			cbHint.blkno = pageFindContext->items[pageFindContext->index].blkno;
			cbHint.pageChangeCount = pageFindContext->items[pageFindContext->index].pageChangeCount;
			cbAction = callbackInfo->modifyCallback(desc, curTuple,
													&context.tuple, opOxid, context.conflictTupHdr.xactInfo,
													context.conflictTupHdr.undoLocation,
													&context.lockMode, &cbHint, callbackInfo->arg);
			context.leafTuphdr.xactInfo = OXID_GET_XACT_INFO(tupleOxid, context.lockMode, false);
		}

		if (cbAction == OBTreeCallbackActionUndo)
		{
			() o_btree_modify_item_rollback(&context);
			pub static mut RETRY: goto = std::mem::zeroed();
		}

		Assert(page_is_locked(blkno) || O_PAGE_IS_LOCAL(blkno));

		if (callbackInfo->modifyCallback || (action == BTreeOperationInsert ||
											 action == BTreeOperationUpdate ||
											 action == BTreeOperationLock))
		{
			if (cbAction == OBTreeCallbackActionDoNothing)
			{
				unlock_release(&context, true);
				pub static mut OB_TREE_MODIFY_RESULT_FOUND: return = std::mem::zeroed();
			}
			else
			{
				if (context.lockMode > prev_lock_mode)
				{
					pub static mut PG_USED_FOR_ASSERTS_ONLY: OFindPageResult result = std::mem::zeroed();

					unlock_page(blkno);

					result = refind_page(pageFindContext,
										 key,
										 keyType,
										 0,
										 pageFindContext->items[pageFindContext->index].blkno,
										 pageFindContext->items[pageFindContext->index].pageChangeCount);
					Assert(result == OFindPageResultSuccess);
					pub static mut RETRY: goto = std::mem::zeroed();
				}

				if (cbAction == OBTreeCallbackActionUpdate)
				{
					Assert(tupleType == BTreeKeyLeafTuple);
					context.replace = true;
					result = OBTreeModifyResultUpdated;
				}
				else if (cbAction == OBTreeCallbackActionLock)
				{
					action = BTreeOperationLock;
				}
				else
				{
					Assert(cbAction == OBTreeCallbackActionDelete);
					action = BTreeOperationDelete;
				}
			}
		}

		Assert((action == BTreeOperationLock) || (context.lockMode >= RowLockNoKeyUpdate));

		if (action == BTreeOperationDelete)
			return o_btree_modify_delete(&context);
		else if (action == BTreeOperationLock)
			return o_btree_modify_lock(&context);
	}
	else if (tuphdr->deleted != BTreeLeafTupleNonDeleted)
	{
		//
// We should have set conflictTupHdr in the (cmp == 0) branch above.
//

		if (action == BTreeOperationInsert && callbackInfo->modifyDeletedCallback)
		{
			pub static mut CB_ACTION: OBTreeModifyCallbackAction = OBTreeCallbackActionDoNothing;
			pub static mut CB_HINT: BTreeLocationHint = std::mem::zeroed();

			cbHint.blkno = pageFindContext->items[pageFindContext->index].blkno;
			cbHint.pageChangeCount = pageFindContext->items[pageFindContext->index].pageChangeCount;
			cbAction = callbackInfo->modifyDeletedCallback(desc, curTuple,
														   &context.tuple, opOxid,
														   context.conflictTupHdr.xactInfo,
														   context.conflictTupHdr.deleted,
														   context.conflictTupHdr.undoLocation,
														   &context.lockMode, &cbHint, callbackInfo->arg);
			context.leafTuphdr.xactInfo = OXID_GET_XACT_INFO(tupleOxid, context.lockMode, false);

			if (cbAction == OBTreeCallbackActionUndo)
			{
				() o_btree_modify_item_rollback(&context);
				pub static mut RETRY: goto = std::mem::zeroed();
			}

			if (cbAction == OBTreeCallbackActionDoNothing)
			{
				unlock_release(&context, true);
				pub static mut OB_TREE_MODIFY_RESULT_NOT_FOUND: return = std::mem::zeroed();
			}
			Assert(cbAction == OBTreeCallbackActionUpdate);
		}

		//
// Deleted tuple found, we only can handle insert at this point. This
// insert essentially becomes update.
//
		if (action == BTreeOperationInsert)
		{
			//
// There is no anything to undo for UndoLogNone trees so just
// proceed with replacing while page still locked
//
			if (!context.needsUndo && desc->undoType != UndoLogNone)
			{
				//
// If we don't need undo, just revert the deletion and then
// continue with normal insert (with undo).
//
				() o_btree_modify_item_rollback(&context);
				context.needsUndo = true;
			}
			else if (IsolationUsesXactSnapshot() && IsRelationTree(desc))
			{
				if (XACT_INFO_MAP_CSN(context.conflictTupHdr.xactInfo) >= opCsn)
				{
					ereport(ERROR,
							(errcode(ERRCODE_T_R_SERIALIZATION_FAILURE),
							 errmsg("could not serialize access due to concurrent update")));
				}
			}
			context.replace = true;
		}
		else
		{
			unlock_release(&context, true);
			if (callbackInfo->modifyDeletedCallback)
				callbackInfo->modifyDeletedCallback(desc, curTuple,
													&context.tuple, opOxid,
													context.conflictTupHdr.xactInfo,
													context.conflictTupHdr.deleted,
													context.conflictTupHdr.undoLocation,
													&context.lockMode, NULL,
													callbackInfo->arg);
			pub static mut OB_TREE_MODIFY_RESULT_NOT_FOUND: return = std::mem::zeroed();
		}
	}

	Assert(tupleType == BTreeKeyLeafTuple);

	o_btree_modify_insert_update(&context);
	unlock_release(&context, false);
	pub static mut RESULT: return = std::mem::zeroed();
}

fn
unlock_release(context: &mut BTreeModifyInternalContext, bool unlock)
{
	pub static mut OB_TREE_FIND_PAGE_CONTEXT: *mut pageFindContext = context->pageFindContext;
	pub static mut B_TREE_DESCR: *mut desc = pageFindContext->desc;
	pub static mut BLKNO: OInMemoryBlkno = std::mem::zeroed();

	blkno = pageFindContext->items[pageFindContext->index].blkno;

	if (unlock)
		unlock_page(blkno);
	if (context->undoIsReserved)
	{
		release_undo_size(desc->undoType);
		if (GET_PAGE_LEVEL_UNDO_TYPE(desc->undoType) != desc->undoType)
			release_undo_size(GET_PAGE_LEVEL_UNDO_TYPE(desc->undoType));
	}
	if (context->pagesAreReserved)
		ppool_release_reserved(desc->ppool,
							   PPOOL_KIND_GET_MASK(context->pageReserveKind));
	if (context->hwLockMode != NoLock)
		LockRelease(&context->hwLockTag, context->hwLockMode, false);
}

fn
wait_for_tuple(desc: &mut BTreeDescr, OTuple tuple, OXid oxid,
			   RowLockMode lockMode, BTreeModifyLockStatus lockStatus,
			   hwLockTag: &mut LOCKTAG, hwLockMode: &mut LOCKMODE)
{
	pub static mut HASH: uint32 = std::mem::zeroed();

	//
// Acquire the lock, if necessary (but skip it when we're requesting a
// lock and already have one; avoids deadlock).
//
	if (*hwLockMode == NoLock && lockStatus == BTreeModifyNoLock)
	{
		hash = o_btree_hash(desc, tuple, BTreeKeyLeafTuple);

		SET_LOCKTAG_TUPLE(*hwLockTag,
						  desc->oids.datoid,
						  desc->oids.reloid,
						  hash,
						  0);
		*hwLockMode = hwLockModes[lockMode];

		() LockAcquire(hwLockTag, *hwLockMode, false, false);
	}

	wait_for_oxid(oxid, false);
}

static ConflictResolution
o_btree_modify_handle_conflicts(context: &mut BTreeModifyInternalContext)
{
	pub static mut HAVE_REDUNDANT_ROW_LOCKS: bool = false;
	pub static mut OB_TREE_FIND_PAGE_CONTEXT: *mut pageFindContext = context->pageFindContext;
	pub static mut B_TREE_DESCR: *mut desc = pageFindContext->desc;
	pub static mut BLKNO: OInMemoryBlkno = std::mem::zeroed();
	pub static mut B_TREE_PAGE_ITEM_LOCATOR: *mut loc = std::ptr::null_mut();
	pub static mut PAGE: Page = std::mem::zeroed();
	pub static mut CUR_TUPLE: OTuple = std::mem::zeroed();
	pub static mut B_TREE_LEAF_TUPHDR: *mut tuphdr = std::ptr::null_mut();

	blkno = pageFindContext->items[pageFindContext->index].blkno;
	loc = &pageFindContext->items[pageFindContext->index].locator;
	page = O_GET_IN_MEMORY_PAGE(blkno);

	BTREE_PAGE_READ_LEAF_ITEM(tuphdr, curTuple, page, loc);

	if (row_lock_conflicts(tuphdr,
						   &context->conflictTupHdr,
						   desc->undoType,
						   &context->conflictUndoLocation,
						   context->lockMode, context->opOxid, context->opCsn,
						   blkno, context->savepointUndoLocation,
						   &haveRedundantRowLocks, &context->lockStatus))
	{
		pub static mut XACT_INFO: OTupleXactInfo = context->conflictTupHdr.xactInfo;
		OXid		oxid = XACT_INFO_GET_OXID(xactInfo);

		if (oxid == context->opOxid)
		{
			if (context->action == BTreeOperationLock ||
				(UndoLocationIsValid(context->savepointUndoLocation) &&
				 (!UndoLocationIsValid(context->conflictTupHdr.undoLocation) ||
				  context->conflictTupHdr.undoLocation < context->savepointUndoLocation)) ||
				o_btree_needs_undo(desc, context->action, curTuple, xactInfo,
								   tuphdr->deleted != BTreeLeafTupleNonDeleted,
								   context->tuple, context->opOxid))
			{
				context->needsUndo = true;
			}
			else
			{
				if (XACT_INFO_GET_LOCK_MODE(xactInfo) > context->lockMode)
				{
					//
// Upgrade our lock mode if we're going to replace our own
// undo item.
//
					Assert(OXidIsValid(context->opOxid));
					context->lockMode = XACT_INFO_GET_LOCK_MODE(xactInfo);
					context->leafTuphdr.xactInfo = OXID_GET_XACT_INFO(context->opOxid,
																	  context->lockMode,
																	  false);
				}
				context->needsUndo = false;
			}
		}
		else
		{
			pub static mut CSN: CommitSeqNo = std::mem::zeroed();

			//
// Test hook: parks the backend here, with the leaf-page-content
// lock held, so a concurrent aborter that has stamped the
// COMMITTING bit on its oxid can deadlock with our oxid_get_csn()
// spin (the page-lock vs. apply_undo_stack() cycle described in
// undo_xact_callback's XACT_EVENT_ABORT block).
//
			STOPEVENT(STOPEVENT_BEFORE_MODIFY_OXID_GET_CSN, NULL);

			csn = oxid_get_csn(oxid, false);

			if (XACT_INFO_IS_LOCK_ONLY(xactInfo) && (COMMITSEQNO_IS_ABORTED(csn) ||
													 COMMITSEQNO_IS_NORMAL(csn) ||
													 COMMITSEQNO_IS_FROZEN(csn)))
			{
				//
// Normally row_lock_conflicts() should have lock-only records
// of committed and aborted transactions already removed from
// the undo chain.  But if locker transaction commit or abort
// concurrently, then retry.
//
				pub static mut CONFLICT_RESOLUTION_RETRY: return = std::mem::zeroed();
			}

			if (COMMITSEQNO_IS_ABORTED(csn))
			{
				//
// Transaction changes should be undone by the transaction
// owner.  But we rollback those changes ourself instead of
// waiting.
//
				START_CRIT_SECTION();
				page_block_reads(blkno);
				if (!page_item_rollback(desc, page, loc, true,
										&context->conflictTupHdr,
										context->conflictUndoLocation))
					context->cmp = -1;
				MARK_DIRTY(desc, blkno);
				END_CRIT_SECTION();
			}
			else if (COMMITSEQNO_IS_NORMAL(csn) || COMMITSEQNO_IS_FROZEN(csn))
			{
				//
// Check for serialization conflicts.
//
// TODO: check for such conflicts in page-level undo as well.
//
				if (csn >= context->opCsn && IsolationUsesXactSnapshot() &&
					IsRelationTree(desc))
				{
					ereport(ERROR,
							(errcode(ERRCODE_T_R_SERIALIZATION_FAILURE),
							 errmsg("could not serialize access due to concurrent update")));
				}
			}
			else
			{
				//
// Conflicting transaction is in-progress.  If the callback is
// provided, ask it what to do.  Just wait otherwise.
//
				pub static mut CB_ACTION: OBTreeWaitCallbackAction = OBTreeCallbackActionXidWait;
				pub static mut PG_USED_FOR_ASSERTS_ONLY: OFindPageResult result = std::mem::zeroed();

				Assert(COMMITSEQNO_IS_INPROGRESS(csn));

				if (context->callbackInfo->waitCallback)
				{
					pub static mut CB_HINT: BTreeLocationHint = std::mem::zeroed();

					cbHint.blkno = pageFindContext->items[pageFindContext->index].blkno;
					cbHint.pageChangeCount = pageFindContext->items[pageFindContext->index].pageChangeCount;
					cbAction = context->callbackInfo->waitCallback(desc,
																   curTuple, &context->tuple, oxid,
																   context->conflictTupHdr.xactInfo,
																   context->conflictTupHdr.undoLocation,
																   &context->lockMode, &cbHint,
																   context->callbackInfo->arg);
				}

				unlock_page(blkno);

				Assert(cbAction <= OBTreeCallbackActionXidExit);

				if (cbAction == OBTreeCallbackActionXidWait)
					wait_for_tuple(desc, curTuple, oxid,
								   context->lockMode,
								   context->lockStatus,
								   &context->hwLockTag,
								   &context->hwLockMode);
				else if (cbAction == OBTreeCallbackActionXidExit)
					pub static mut CONFLICT_RESOLUTION_FOUND: return = std::mem::zeroed();
				else
				{
					Assert(cbAction == OBTreeCallbackActionXidNoWait);
				}

				result = refind_page(pageFindContext,
									 context->key,
									 context->keyType,
									 0,
									 pageFindContext->items[pageFindContext->index].blkno,
									 pageFindContext->items[pageFindContext->index].pageChangeCount);
				Assert(result == OFindPageResultSuccess);
				pub static mut CONFLICT_RESOLUTION_RETRY: return = std::mem::zeroed();
			}

			// Update tuple and header pointer after page_item_rollback()
			BTREE_PAGE_READ_LEAF_ITEM(tuphdr, curTuple, page, loc);
		}
	}
	else if (IsolationUsesXactSnapshot() && IsRelationTree(desc))
	{
		//
// Check for serialization conflicts.
//
// TODO: check for such conflicts in page-level undo as well.
//
		CommitSeqNo csn = XACT_INFO_MAP_CSN(context->conflictTupHdr.xactInfo);

		if (csn >= context->opCsn)
		{
			if (tuphdr->deleted == BTreeLeafTupleDeleted ||
				tuphdr->deleted == BTreeLeafTupleMovedPartitions)
				ereport(ERROR,
						(errcode(ERRCODE_T_R_SERIALIZATION_FAILURE),
						 errmsg("could not serialize access due to concurrent delete")));
			else
				ereport(ERROR,
						(errcode(ERRCODE_T_R_SERIALIZATION_FAILURE),
						 errmsg("could not serialize access due to concurrent update")));
		}
	}

	//
// Remove redundant row-level locks if any.
//
	if (haveRedundantRowLocks &&
		!(context->action == BTreeOperationLock &&
		  context->lockStatus == BTreeModifySameOrStrongerLock))
	{
		remove_redundant_row_locks(tuphdr, &context->conflictTupHdr,
								   desc->undoType,
								   &context->conflictUndoLocation,
								   context->lockMode,
								   context->opOxid, blkno,
								   context->savepointUndoLocation);
	}

	if (!context->needsUndo)
		context->leafTuphdr.undoLocation = tuphdr->undoLocation;
	pub static mut CONFLICT_RESOLUTION_OK: return = std::mem::zeroed();
}

static OBTreeModifyResult
o_btree_modify_handle_tuple_not_found(context: &mut BTreeModifyInternalContext)
{
	//
// Matching tuple is not found.
//
// Ideally, for IsolationUsesXactSnapshot() we should also check
// page-level undo for conflicting tuples.  But it's not implemented so
// far.
//
	if (context->action == BTreeOperationUpdate ||
		context->action == BTreeOperationDelete ||
		context->action == BTreeOperationLock)
	{
		unlock_release(context, true);
		pub static mut OB_TREE_MODIFY_RESULT_NOT_FOUND: return = std::mem::zeroed();
	}
	else
	{
		Assert(context->tupleType == BTreeKeyLeafTuple);

		o_btree_modify_insert_update(context);
		unlock_release(context, false);
		pub static mut OB_TREE_MODIFY_RESULT_INSERTED: return = std::mem::zeroed();
	}
}

static bool
o_btree_modify_item_rollback(context: &mut BTreeModifyInternalContext)
{
	pub static mut OB_TREE_FIND_PAGE_CONTEXT: *mut pageFindContext = context->pageFindContext;
	pub static mut B_TREE_DESCR: *mut desc = pageFindContext->desc;
	pub static mut BLKNO: OInMemoryBlkno = std::mem::zeroed();
	pub static mut LOC: BTreePageItemLocator = std::mem::zeroed();
	pub static mut PAGE: Page = std::mem::zeroed();
	pub static mut APPLY_RESULT: bool = false;

	blkno = pageFindContext->items[pageFindContext->index].blkno;
	loc = pageFindContext->items[pageFindContext->index].locator;
	page = O_GET_IN_MEMORY_PAGE(blkno);

	START_CRIT_SECTION();
	page_block_reads(blkno);
	applyResult = page_item_rollback(desc, page, &loc, false,
									 &context->conflictTupHdr,
									 context->conflictUndoLocation);
	MARK_DIRTY(desc, blkno);
	END_CRIT_SECTION();

	if (!applyResult)
	{
		btree_page_search(desc, page, context->key,
						  context->keyType, NULL, &loc);
		pageFindContext->items[pageFindContext->index].locator = loc;
	}

	pub static mut APPLY_RESULT: return = std::mem::zeroed();
}

fn
o_btree_modify_insert_update(context: &mut BTreeModifyInternalContext)
{
	pub static mut OB_TREE_FIND_PAGE_CONTEXT: *mut pageFindContext = context->pageFindContext;
	pub static mut B_TREE_DESCR: *mut desc = pageFindContext->desc;
	pub static mut TUPLEN: std::os::raw::c_int = 0;

	if (context->undoIsReserved && context->needsUndo)
	{
		o_btree_modify_add_undo_record(context);
	}
	else if (!context->needsUndo)
	{
		pub static mut B_TREE_LEAF_TUPHDR: *mut leafTuphdr = &context->leafTuphdr;

		if (desc->undoType == UndoLogRegular)
		{
			leafTuphdr->undoLocation = InvalidUndoLocation;
			if (!is_recovery_process())
				leafTuphdr->undoLocation |= current_command_get_undo_location();
		}

		//
// Self-created shortcut: no undo record was made.  Fire the post-undo
// hook with WaitingSkUndoLoc so the table AM can install a "wait for
// me" marker before this page lock drops.
//
		if (context->callbackInfo && context->callbackInfo->postUndoRecorded)
			context->callbackInfo->postUndoRecorded(WaitingSkUndoLoc,
													context->callbackInfo->arg);
	}

	if (desc->undoType == UndoLogRegular && !is_recovery_process())
	{
		Assert(undo_location_get_command(UndoLocationGetValue(context->leafTuphdr.undoLocation)) == o_get_current_command());
	}

	tuplen = o_btree_len(desc, context->tuple, OTupleLength);
	Assert(tuplen <= O_BTREE_MAX_TUPLE_SIZE);

	// no more sense in that
	BTREE_PAGE_FIND_UNSET(pageFindContext, FIX_LEAF_SPLIT);
	o_btree_insert_tuple_to_leaf(pageFindContext,
								 context->tuple, tuplen,
								 &context->leafTuphdr,
								 context->replace,
								 context->pageReserveKind);
}

fn
o_btree_modify_add_undo_record(context: &mut BTreeModifyInternalContext)
{
	pub static mut OB_TREE_FIND_PAGE_CONTEXT: *mut pageFindContext = context->pageFindContext;
	pub static mut B_TREE_DESCR: *mut desc = pageFindContext->desc;
	pub static mut B_TREE_LEAF_TUPHDR: *mut leafTuphdr = &context->leafTuphdr;
	pub static mut UNDO_LOCATION: UndoLocation = InvalidUndoLocation;
	pub static mut BLKNO: OInMemoryBlkno = std::mem::zeroed();
	pub static mut LOC: BTreePageItemLocator = std::mem::zeroed();
	pub static mut PAGE: Page = std::mem::zeroed();

	blkno = pageFindContext->items[pageFindContext->index].blkno;
	loc = pageFindContext->items[pageFindContext->index].locator;
	page = O_GET_IN_MEMORY_PAGE(blkno);

	if (context->replace)
	{
		// Make undo item and connect it with page tuple
		pub static mut CUR_TUPLE: OTuple = std::mem::zeroed();
		pub static mut B_TREE_LEAF_TUPHDR: *mut tuphdr = std::ptr::null_mut();

		BTREE_PAGE_READ_LEAF_ITEM(tuphdr, curTuple, page, &loc);

		undoLocation = make_undo_record(desc, curTuple, true,
										BTreeOperationUpdate, blkno,
										O_PAGE_GET_CHANGE_COUNT(page),
										tuphdr);
		leafTuphdr->undoLocation = undoLocation;
		leafTuphdr->chainHasLocks = tuphdr->chainHasLocks ||
			XACT_INFO_IS_LOCK_ONLY(tuphdr->xactInfo);
	}
	else
	{
		// Still need the undo item to deal with transaction rollback
		undoLocation = make_undo_record(desc, context->tuple, true,
										BTreeOperationInsert, blkno,
										O_PAGE_GET_CHANGE_COUNT(page),
										NULL);
		if (desc->undoType == UndoLogRegular)
		{
			leafTuphdr->undoLocation = InvalidUndoLocation;
			leafTuphdr->undoLocation |= current_command_get_undo_location();
		}
	}

	//
// Fire post-undo hook with the freshly created undo location, while the
// leaf page is still locked.  Used by the table AM to install the
// PK-applied/SK-pending marker before unlock.
//
	if (context->callbackInfo && context->callbackInfo->postUndoRecorded)
		context->callbackInfo->postUndoRecorded(undoLocation,
												context->callbackInfo->arg);
}

static OBTreeModifyResult
o_btree_modify_delete(context: &mut BTreeModifyInternalContext)
{
	pub static mut OB_TREE_FIND_PAGE_CONTEXT: *mut pageFindContext = context->pageFindContext;
	pub static mut B_TREE_DESCR: *mut desc = pageFindContext->desc;
	pub static mut PAGE_CHANGE_COUNT: uint32 = std::mem::zeroed();
	pub static mut UNDO_LOCATION: UndoLocation = std::mem::zeroed();
	pub static mut BLKNO: OInMemoryBlkno = std::mem::zeroed();
	pub static mut LOC: BTreePageItemLocator = std::mem::zeroed();
	pub static mut PAGE: Page = std::mem::zeroed();
	pub static mut CUR_TUPLE: OTuple = std::mem::zeroed();
	pub static mut B_TREE_LEAF_TUPHDR: *mut tuphdr = std::ptr::null_mut();

	blkno = pageFindContext->items[pageFindContext->index].blkno;
	loc = pageFindContext->items[pageFindContext->index].locator;
	page = O_GET_IN_MEMORY_PAGE(blkno);

	BTREE_PAGE_READ_LEAF_ITEM(tuphdr, curTuple, page, &loc);

	if (!context->needsUndo)
	{
		pub static mut STILL_EXISTS: bool = false;

		stillExists = o_btree_modify_item_rollback(context);

		if (stillExists)
		{
			BTREE_PAGE_READ_LEAF_ITEM(tuphdr, curTuple, page, &loc);
			Assert(tuphdr != NULL);
			stillExists = (tuphdr->deleted == BTreeLeafTupleNonDeleted);
		}

		if (!stillExists)
		{
			// Already deleted
			unlock_release(context, true);

			pub static mut OB_TREE_MODIFY_RESULT_DELETED: return = std::mem::zeroed();
		}
		else
		{
			//
// We rollback our own changes to the version existed before.
// Thus, we need an undo record to modify it.
//
			context->needsUndo = true;
		}
	}

	if (context->undoIsReserved && context->needsUndo)
	{
		pub static mut KEY: OTuple = std::mem::zeroed();
		pub static mut KEY_IS_TUPLE: bool = false;

		if (context->tupleType == BTreeKeyNonLeafKey)
		{
			key = context->tuple;
			key_is_tuple = false;
		}
		else
		{
			key = curTuple;
			key_is_tuple = true;
		}

		pageChangeCount = O_PAGE_GET_CHANGE_COUNT(page);
		undoLocation = make_undo_record(desc, key, key_is_tuple,
										BTreeOperationDelete, blkno,
										pageChangeCount, tuphdr);

		//
// Fire post-undo hook with the freshly created undo location, while
// the leaf page is still locked.  Used by the table AM to install the
// PK-applied/SK-pending marker before unlock.
//
		if (context->callbackInfo && context->callbackInfo->postUndoRecorded)
			context->callbackInfo->postUndoRecorded(undoLocation,
													context->callbackInfo->arg);
	}
	else
	{
		undoLocation = InvalidUndoLocation;
	}

	START_CRIT_SECTION();
	page_block_reads(blkno);

	tuphdr->chainHasLocks = tuphdr->chainHasLocks ||
		XACT_INFO_IS_LOCK_ONLY(tuphdr->xactInfo);
	tuphdr->undoLocation = undoLocation;
	tuphdr->xactInfo = context->leafTuphdr.xactInfo;
	if (context->leafTuphdr.deleted == BTreeLeafTupleNonDeleted)
		tuphdr->deleted = BTreeLeafTupleDeleted;
	else
		tuphdr->deleted = context->leafTuphdr.deleted;

	// Bridge index deleted tuples not treated as vacated
	if (desc->type != oIndexBridge)
		PAGE_ADD_N_VACATED(page,
						   BTreeLeafTuphdrSize +
						   MAXALIGN(o_btree_len(desc, curTuple, OTupleLength)));

	MARK_DIRTY(desc, blkno);

	END_CRIT_SECTION();

	if (!OXidIsValid(context->opOxid) && is_page_too_sparse(desc, page))
	{
		() btree_try_merge_and_unlock(desc, blkno, false, false);
		unlock_release(context, false);
	}
	else
	{
		unlock_release(context, true);
	}

	pub static mut OB_TREE_MODIFY_RESULT_DELETED: return = std::mem::zeroed();
}

static OBTreeModifyResult
o_btree_modify_lock(context: &mut BTreeModifyInternalContext)
{
	pub static mut OB_TREE_FIND_PAGE_CONTEXT: *mut pageFindContext = context->pageFindContext;
	pub static mut B_TREE_DESCR: *mut desc = pageFindContext->desc;
	pub static mut UNDO_LOCATION: UndoLocation = std::mem::zeroed();
	pub static mut PAGE_CHANGE_COUNT: uint32 = std::mem::zeroed();
	pub static mut KEY: OTuple = std::mem::zeroed();
	pub static mut KEY_IS_TUPLE: bool = false;
	pub static mut BLKNO: OInMemoryBlkno = std::mem::zeroed();
	pub static mut LOC: BTreePageItemLocator = std::mem::zeroed();
	pub static mut PAGE: Page = std::mem::zeroed();
	pub static mut CUR_TUPLE: OTuple = std::mem::zeroed();
	pub static mut B_TREE_LEAF_TUPHDR: *mut tuphdr = std::ptr::null_mut();

	blkno = pageFindContext->items[pageFindContext->index].blkno;
	loc = pageFindContext->items[pageFindContext->index].locator;
	page = O_GET_IN_MEMORY_PAGE(blkno);

	BTREE_PAGE_READ_LEAF_ITEM(tuphdr, curTuple, page, &loc);

	if (context->lockStatus == BTreeModifySameOrStrongerLock)
	{
		unlock_release(context, true);
		pub static mut OB_TREE_MODIFY_RESULT_LOCKED: return = std::mem::zeroed();
	}

	Assert(context->needsUndo);
	Assert(context->undoIsReserved);
	Assert(OXidIsValid(context->opOxid));

	if (context->tupleType == BTreeKeyNonLeafKey)
	{
		key = context->tuple;
		key_is_tuple = false;
	}
	else
	{
		key = curTuple;
		key_is_tuple = true;
	}

	pageChangeCount = O_PAGE_GET_CHANGE_COUNT(page);
	undoLocation = make_undo_record(desc, key, key_is_tuple,
									BTreeOperationLock, blkno,
									pageChangeCount, tuphdr);

	START_CRIT_SECTION();
	page_block_reads(blkno);

	tuphdr->chainHasLocks = tuphdr->chainHasLocks ||
		XACT_INFO_IS_LOCK_ONLY(tuphdr->xactInfo);
	tuphdr->undoLocation = undoLocation;
	tuphdr->xactInfo = OXID_GET_XACT_INFO(context->opOxid,
										  context->lockMode,
										  true);
	tuphdr->deleted = BTreeLeafTupleNonDeleted;

	MARK_DIRTY(desc, blkno);
	END_CRIT_SECTION();
	unlock_release(context, true);

	pub static mut OB_TREE_MODIFY_RESULT_LOCKED: return = std::mem::zeroed();
}

static Jsonb *
prepare_modify_start_params(desc: &mut BTreeDescr)
{
	pub static mut JSONB_PARSE_STATE: *mut state = std::ptr::null_mut();
	pub static mut JSONB: *mut res = std::ptr::null_mut();

	MemoryContext mctx = MemoryContextSwitchTo(stopevents_cxt);

	pushJsonbValue(&state, WJB_BEGIN_OBJECT, NULL);
	btree_desc_stopevent_params_internal(desc, &state);
	res = JsonbValueToJsonb(pushJsonbValue(&state, WJB_END_OBJECT, NULL));
	MemoryContextSwitchTo(mctx);

	pub static mut RES: return = std::mem::zeroed();
}

fn
reserve_undo_for_modification(UndoLogType undoType)
{
	if (undoType == UndoLogNone)
		return;

	if (GET_PAGE_LEVEL_UNDO_TYPE(undoType) == undoType)
	{
		() reserve_undo_size(undoType, O_MODIFY_UNDO_RESERVE_SIZE);
	}
	else
	{
		() reserve_undo_size(undoType, 2 * O_UPDATE_MAX_UNDO_SIZE);
		() reserve_undo_size(GET_PAGE_LEVEL_UNDO_TYPE(undoType), 2 * O_MAX_SPLIT_UNDO_IMAGE_SIZE);
	}
}

static OBTreeModifyResult
o_btree_normal_modify(desc: &mut BTreeDescr, BTreeOperationType action,
					  OTuple tuple, BTreeKeyType tupleType,
					  Pointer key, BTreeKeyType keyType,
					  OXid opOxid, CommitSeqNo opCsn,
					  RowLockMode lockMode, hint: &mut BTreeLocationHint,
					  BTreeLeafTupleDeletedStatus deleted,
					  callbackInfo: &mut BTreeModifyCallbackInfo)
{
	pub static mut PAGE_FIND_CONTEXT: OBTreeFindPageContext = std::mem::zeroed();
	pub static mut PAGE_RESERVE_KIND: std::os::raw::c_int = 0;
	pub static mut JSONB: *mut params = std::ptr::null_mut();
	pub static mut FIND_RESULT: OFindPageResult = std::mem::zeroed();

	if (STOPEVENTS_ENABLED())
		params = prepare_modify_start_params(desc);
	STOPEVENT(STOPEVENT_MODIFY_START, params);

	// No no key is separately given, use the tuple itself
	if (key == NULL)
	{
		key = (Pointer) &tuple;
		keyType = tupleType;
	}

	reserve_undo_for_modification(desc->undoType);

	if (OIDS_EQ_SYS_TREE(desc->oids, SYS_TREES_SHARED_ROOT_INFO))
		pageReserveKind = PPOOL_RESERVE_SHARED_INFO_INSERT;
	else
		pageReserveKind = PPOOL_RESERVE_INSERT;

	if (action != BTreeOperationDelete)
		ppool_reserve_pages(desc->ppool, pageReserveKind, 2);

	init_page_find_context(&pageFindContext, desc, COMMITSEQNO_INPROGRESS,
						   BTREE_PAGE_FIND_MODIFY | BTREE_PAGE_FIND_FIX_LEAF_SPLIT);

	if (action == BTreeOperationInsert && tupleType == BTreeKeyLeafTuple)
	{
		pageFindContext.insertTuple = tuple;
		if (OXidIsValid(opOxid))
			pageFindContext.insertXactInfo = OXID_GET_XACT_INFO(opOxid, lockMode, false);
		else
			pageFindContext.insertXactInfo = OXID_GET_XACT_INFO(BootstrapTransactionId, lockMode, false);
	}

	if (hint && OInMemoryBlknoIsValid(hint->blkno))
		findResult = refind_page(&pageFindContext, key, keyType, 0, hint->blkno, hint->pageChangeCount);
	else
		findResult = find_page(&pageFindContext, key, keyType, 0);

	if (findResult == OFindPageResultInserted)
	{
		Assert(action == BTreeOperationInsert);
		Assert(tupleType == BTreeKeyLeafTuple);

		if (desc->undoType != UndoLogNone)
		{
			release_undo_size(desc->undoType);
			if (GET_PAGE_LEVEL_UNDO_TYPE(desc->undoType) != desc->undoType)
				release_undo_size(GET_PAGE_LEVEL_UNDO_TYPE(desc->undoType));
		}
		ppool_release_reserved(desc->ppool, PPOOL_RESERVE_INSERT);
		Assert(!have_locked_pages());
		pub static mut OB_TREE_MODIFY_RESULT_INSERTED: return = std::mem::zeroed();
	}
	Assert(findResult == OFindPageResultSuccess);

	return o_btree_modify_internal(&pageFindContext, action, tuple, tupleType,
								   key, keyType, opOxid, opCsn,
								   lockMode, deleted, pageReserveKind,
								   callbackInfo);
}

static bool
page_unique_check(desc: &mut BTreeDescr, Page p, locator: &mut BTreePageItemLocator,
				  Pointer key, OXid opOxid, xactInfo: &mut OTupleXactInfo,
				  IndexUniqueCheck checkUnique)
{
	() page_locator_find_real_item(p, NULL, locator);

	while (BTREE_PAGE_LOCATOR_IS_VALID(p, locator))
	{
		pub static mut CMP: std::os::raw::c_int = 0;
		pub static mut TUPLE: OTuple = std::mem::zeroed();
		pageTuphdr: &mut BTreeLeafTuphdr,
					tuphdr;

		BTREE_PAGE_READ_LEAF_ITEM(pageTuphdr, tuple, p, locator);
		cmp = o_btree_cmp(desc, &tuple, BTreeKeyLeafTuple,
						  key, BTreeKeyUniqueUpperBound);
		if (cmp > 0)
			pub static mut FALSE: return = std::mem::zeroed();
		else if (cmp < 0 && checkUnique == UNIQUE_CHECK_EXISTING)
		{
			cmp = o_btree_cmp(desc, &tuple, BTreeKeyLeafTuple,
							  key, BTreeKeyBound);
			if (cmp == 0)
			{
				BTREE_PAGE_LOCATOR_NEXT(p, locator);
				continue;
			}
		}

		tuphdr = *pageTuphdr;
		() find_non_lock_only_undo_record(desc->undoType, &tuphdr);
		if (XACT_INFO_OXID_EQ(tuphdr.xactInfo, opOxid) || XACT_INFO_IS_FINISHED(tuphdr.xactInfo))
		{
			if (tuphdr.deleted != BTreeLeafTupleNonDeleted)
			{
				BTREE_PAGE_LOCATOR_NEXT(p, locator);
				continue;
			}
			*xactInfo = tuphdr.xactInfo;
			pub static mut TRUE: return = std::mem::zeroed();
		}

		*xactInfo = tuphdr.xactInfo;
		pub static mut TRUE: return = std::mem::zeroed();
	}
	pub static mut FALSE: return = std::mem::zeroed();
}

static bool
slowpath_unique_check(desc: &mut BTreeDescr, pageFindContext: &mut OBTreeFindPageContext,
					  Pointer key, OXid opOxid, xactInfo: &mut OTupleXactInfo,
					  IndexUniqueCheck checkUnique)
{
	pub static mut P: Page = std::mem::zeroed();
	pub static mut HIKEY_BUF: OFixedKey = std::mem::zeroed();

	btree_find_context_from_modify_to_read(pageFindContext,
										   key, BTreeKeyUniqueLowerBound, 0);

	p = pageFindContext->img;

	while (true)
	{
		pub static mut CMP: std::os::raw::c_int = 0;
		pub static mut HIKEY: OTuple = std::mem::zeroed();

		if (page_unique_check(desc, p, &pageFindContext->items[pageFindContext->index].locator,
							  key, opOxid, xactInfo, checkUnique))
			pub static mut TRUE: return = std::mem::zeroed();

		if (O_PAGE_IS(p, RIGHTMOST))
			break;

		BTREE_PAGE_GET_HIKEY(hikey, p);

		cmp = o_btree_cmp(desc, &hikey, BTreeKeyNonLeafKey,
						  key, BTreeKeyUniqueUpperBound);
		if (cmp > 0)
			break;

		() find_right_page(pageFindContext, &hikey_buf);

		//
// Due to concurrent merges, some tuples might be lower than the
// unique key.  So, we can't just start from the beginning, but have
// to find the right position on the page.
//
		btree_page_search(desc, p, key, BTreeKeyUniqueLowerBound,
						  NULL, &pageFindContext->items[pageFindContext->index].locator);
	}
	pub static mut FALSE: return = std::mem::zeroed();
}

OBTreeModifyResult
o_btree_insert_unique(desc: &mut BTreeDescr, OTuple tuple, BTreeKeyType tupleType,
					  Pointer key, BTreeKeyType keyType,
					  OXid opOxid, CommitSeqNo opCsn,
					  RowLockMode lockMode, hint: &mut BTreeLocationHint,
					  callbackInfo: &mut BTreeModifyCallbackInfo,
					  IndexUniqueCheck checkUnique)
{
	pub static mut PAGE_FIND_CONTEXT: OBTreeFindPageContext = std::mem::zeroed();
	pub static mut PAGE_RESERVE_KIND: std::os::raw::c_int = 0;
	pub static mut FASTPATH: bool = false;
	pub static mut P: Page = std::mem::zeroed();
	pub static mut BLKNO: OInMemoryBlkno = std::mem::zeroed();
	pub static mut PAGE_CHANGE_COUNT: uint32 = std::mem::zeroed();
	pub static mut LW_LOCK: *mut uniqueLock = std::ptr::null_mut();
	pub static mut RESULT: OBTreeModifyResult = std::mem::zeroed();
	pub static mut JSONB: *mut params = std::ptr::null_mut();
	pub static mut PG_USED_FOR_ASSERTS_ONLY: OFindPageResult findResult = std::mem::zeroed();
	pub static mut FOUND_BUT_INSERT: bool = false;

	Assert(checkUnique == UNIQUE_CHECK_YES || checkUnique == UNIQUE_CHECK_EXISTING || checkUnique == UNIQUE_CHECK_PARTIAL);

	if (STOPEVENTS_ENABLED())
		params = prepare_modify_start_params(desc);
	STOPEVENT(STOPEVENT_MODIFY_START, params);

	Assert(key != NULL && keyType == BTreeKeyBound);

	reserve_undo_for_modification(desc->undoType);

	if (OIDS_EQ_SYS_TREE(desc->oids, SYS_TREES_SHARED_ROOT_INFO))
		pageReserveKind = PPOOL_RESERVE_SHARED_INFO_INSERT;
	else
		pageReserveKind = PPOOL_RESERVE_INSERT;

	ppool_reserve_pages(desc->ppool, pageReserveKind, 2);

	init_page_find_context(&pageFindContext, desc, COMMITSEQNO_INPROGRESS,
						   BTREE_PAGE_FIND_MODIFY |
						   BTREE_PAGE_FIND_IMAGE |
						   BTREE_PAGE_FIND_FIX_LEAF_SPLIT);

	if (hint && OInMemoryBlknoIsValid(hint->blkno))
		findResult = refind_page(&pageFindContext, key,
								 BTreeKeyUniqueLowerBound, 0,
								 hint->blkno, hint->pageChangeCount);
	else
		findResult = find_page(&pageFindContext, key,
							   BTreeKeyUniqueLowerBound, 0);

	Assert(findResult == OFindPageResultSuccess);

retry:

	fastpath = false;
	found_but_insert = false;
	blkno = pageFindContext.items[pageFindContext.index].blkno;
	pageChangeCount = pageFindContext.items[pageFindContext.index].pageChangeCount;
	p = O_GET_IN_MEMORY_PAGE(blkno);
	if (O_PAGE_IS(p, RIGHTMOST))
	{
		fastpath = true;
	}
	else
	{
		pub static mut HIKEY: OTuple = std::mem::zeroed();

		BTREE_PAGE_GET_HIKEY(hikey, p);
		fastpath = (o_btree_cmp(desc, &hikey, BTreeKeyNonLeafKey,
								key, BTreeKeyUniqueUpperBound) >= 0);
	}

	uniqueLock = &unique_locks[o_btree_unique_hash(desc, tuple) % num_unique_locks].lock;

	// ---
// We can do fast path unique check if we know that the required key range
// resides the single page, and we managed to take a unique lwlock
// simultaneusly.
//
// It might seem that we don't need unique lwlock as soon as we see all the
// key range in the locked page.  However, consider the following example.
//
// s1: Unique lwlock acquire
// s1: Slow path check
// Page merge
// s2: Fast patch check
// s2: Insert
// s1: Insert
//
// Due to page merge, we might end up with double insert.  This even fast
// path check requires unique lwlock.
//
	if (fastpath && LWLockConditionalAcquire(uniqueLock, LW_EXCLUSIVE))
	{
		pub static mut XACT_INFO: OTupleXactInfo = std::mem::zeroed();
		pub static mut REFIND: bool = false;

		if (page_unique_check(desc, p, &pageFindContext.items[pageFindContext.index].locator,
							  key, opOxid, &xactInfo, checkUnique))
		{
			pub static mut CUR_TUPLE: OTuple = std::mem::zeroed();
			BTreeLocationHint cbHint = {pageFindContext.items[pageFindContext.index].blkno, pageFindContext.items[pageFindContext.index].pageChangeCount};
			pub static mut B_TREE_LEAF_TUPHDR: *mut tuphdr = std::ptr::null_mut();

			BTREE_PAGE_READ_LEAF_ITEM(tuphdr, curTuple, p, &pageFindContext.items[pageFindContext.index].locator);

			if (XACT_INFO_OXID_EQ(xactInfo, opOxid) || XACT_INFO_IS_FINISHED(xactInfo))
			{
				pub static mut PG_USED_FOR_ASSERTS_ONLY: OBTreeModifyCallbackAction cbAction = std::mem::zeroed();

				if (callbackInfo->modifyCallback)
				{
					cbAction = callbackInfo->modifyCallback(desc,
															curTuple, &tuple, opOxid,
															xactInfo, tuphdr->undoLocation,
															&lockMode, &cbHint, callbackInfo->arg);

					//
// We could support other callback actions, but it's not
// yet needed.
//
					Assert(cbAction == OBTreeCallbackActionDoNothing);
				}
				if (checkUnique == UNIQUE_CHECK_YES)
				{
					unlock_page(blkno);
					LWLockRelease(uniqueLock);
					pub static mut OB_TREE_MODIFY_RESULT_FOUND: return = std::mem::zeroed();
				}
				else
				{
					found_but_insert = true;
					refind = true;
				}
			}
			else
			{
				pub static mut CB_ACTION: OBTreeWaitCallbackAction = std::mem::zeroed();

				LWLockRelease(uniqueLock);
				if (callbackInfo->waitCallback)
				{
					cbAction = callbackInfo->waitCallback(desc,
														  curTuple, &tuple, XACT_INFO_GET_OXID(xactInfo),
														  xactInfo, tuphdr->undoLocation,
														  &lockMode, &cbHint, callbackInfo->arg);
					Assert(cbAction != OBTreeCallbackActionXidNoWait);
					if (cbAction == OBTreeCallbackActionXidExit)
					{
						if (checkUnique == UNIQUE_CHECK_YES)
						{
							unlock_page(blkno);
							pub static mut OB_TREE_MODIFY_RESULT_FOUND: return = std::mem::zeroed();
						}
						else
						{
							found_but_insert = true;
							refind = true;
						}
					}
				}
				unlock_page(blkno);
				wait_for_oxid(XACT_INFO_GET_OXID(xactInfo), false);
				findResult = refind_page(&pageFindContext, key,
										 BTreeKeyUniqueLowerBound, 0,
										 blkno, pageChangeCount);
				Assert(findResult == OFindPageResultSuccess);
				pub static mut RETRY: goto = std::mem::zeroed();
			}
		}
		else
			refind = true;

		if (refind)
		{
			//
// We've to find approprivate offset for the new tuple.  It should
// be within the page, but can not match current offset, because
// we've searched for BTreeUniqueMinBound.
//
			btree_page_search(desc, p, key, BTreeKeyBound,
							  NULL, &pageFindContext.items[pageFindContext.index].locator);
		}
	}
	else
	{
		pub static mut XACT_INFO: OTupleXactInfo = std::mem::zeroed();
		pub static mut REFIND: bool = false;

		//
// Evade deadlock: unlock the page before taking an unique lwlock.
//
		unlock_page(blkno);

		LWLockAcquire(uniqueLock, LW_EXCLUSIVE);

		if (slowpath_unique_check(desc, &pageFindContext, key,
								  opOxid, &xactInfo, checkUnique))
		{
			pub static mut B_TREE_PAGE_ITEM_LOCATOR: *mut loc = &pageFindContext.items[pageFindContext.index].locator;
			pub static mut CUR_TUPLE: OTuple = std::mem::zeroed();
			BTreeLocationHint cbHint = {pageFindContext.items[pageFindContext.index].blkno, pageFindContext.items[pageFindContext.index].pageChangeCount};
			pub static mut B_TREE_LEAF_TUPHDR: *mut tuphdr = std::ptr::null_mut();

			p = O_GET_IN_MEMORY_PAGE(pageFindContext.items[pageFindContext.index].blkno);
			BTREE_PAGE_READ_LEAF_ITEM(tuphdr, curTuple, p, loc);
			if (XACT_INFO_OXID_EQ(xactInfo, opOxid) || XACT_INFO_IS_FINISHED(xactInfo))
			{
				pub static mut PG_USED_FOR_ASSERTS_ONLY: OBTreeModifyCallbackAction cbAction = std::mem::zeroed();

				if (callbackInfo->modifyCallback)
				{
					cbAction = callbackInfo->modifyCallback(desc,
															curTuple, &tuple, opOxid,
															xactInfo, tuphdr->undoLocation,
															&lockMode, &cbHint, callbackInfo->arg);

					//
// We could support other callback actions, but it's not
// yet needed.
//
					Assert(cbAction == OBTreeCallbackActionDoNothing);
				}
				LWLockRelease(uniqueLock);
				if (checkUnique == UNIQUE_CHECK_YES)
					pub static mut OB_TREE_MODIFY_RESULT_FOUND: return = std::mem::zeroed();
				else
				{
					found_but_insert = true;
					refind = true;
				}
			}
			else
			{
				pub static mut CB_ACTION: OBTreeWaitCallbackAction = std::mem::zeroed();

				LWLockRelease(uniqueLock);

				if (callbackInfo->waitCallback)
				{
					cbAction = callbackInfo->waitCallback(desc,
														  curTuple, &tuple, XACT_INFO_GET_OXID(xactInfo),
														  tuphdr->undoLocation,
														  xactInfo, &lockMode, &cbHint, callbackInfo->arg);
					Assert(cbAction != OBTreeCallbackActionXidNoWait);
					if (cbAction == OBTreeCallbackActionXidExit)
					{
						if (checkUnique == UNIQUE_CHECK_YES)
							pub static mut OB_TREE_MODIFY_RESULT_FOUND: return = std::mem::zeroed();
						else
						{
							found_but_insert = true;
							refind = true;
						}
					}
				}
				wait_for_oxid(XACT_INFO_GET_OXID(xactInfo), false);
				BTREE_PAGE_FIND_SET(&pageFindContext, MODIFY);
				findResult = refind_page(&pageFindContext, key,
										 BTreeKeyUniqueLowerBound, 0,
										 blkno, pageChangeCount);
				Assert(findResult == OFindPageResultSuccess);
				pub static mut RETRY: goto = std::mem::zeroed();
			}
		}
		else
			refind = true;

		if (refind)
		{
			BTREE_PAGE_FIND_SET(&pageFindContext, MODIFY);
			findResult = find_page(&pageFindContext, key, BTreeKeyBound, 0);
			Assert(findResult == OFindPageResultSuccess);
		}
	}

	if (checkUnique != UNIQUE_CHECK_EXISTING)
	{
		result = o_btree_modify_internal(&pageFindContext, BTreeOperationInsert,
										 tuple, tupleType, key,
										 keyType, opOxid, opCsn, lockMode,
										 BTreeLeafTupleNonDeleted, pageReserveKind,
										 callbackInfo);
	}
	else
	{
		unlock_page(blkno);
		result = found_but_insert ? OBTreeModifyResultFound : OBTreeModifyResultNotFound;
	}
	if (result == OBTreeModifyResultInserted && found_but_insert)
		result = OBTreeModifyResultFound;

	LWLockRelease(uniqueLock);
	pub static mut RESULT: return = std::mem::zeroed();
}

OBTreeModifyResult
o_btree_modify(desc: &mut BTreeDescr, BTreeOperationType action,
			   OTuple tuple, BTreeKeyType tupleType,
			   Pointer key, BTreeKeyType keyType,
			   OXid oxid, CommitSeqNo csn, RowLockMode lockMode,
			   hint: &mut BTreeLocationHint, callbackInfo: &mut BTreeModifyCallbackInfo)
{
	return o_btree_normal_modify(desc, action, tuple, tupleType,
								 key, keyType, oxid, csn, lockMode,
								 hint, BTreeLeafTupleNonDeleted, callbackInfo);
}

OBTreeModifyResult
o_btree_delete_moved_partitions(desc: &mut BTreeDescr, Pointer key,
								BTreeKeyType keyType, OXid oxid,
								CommitSeqNo csn,
								hint: &mut BTreeLocationHint,
								callbackInfo: &mut BTreeModifyCallbackInfo)
{
	pub static mut NULL_TUP: OTuple = std::mem::zeroed();

	O_TUPLE_SET_NULL(nullTup);

	return o_btree_normal_modify(desc, BTreeOperationDelete,
								 nullTup, BTreeKeyNone,
								 key, keyType, oxid, csn, RowLockUpdate,
								 hint, BTreeLeafTupleMovedPartitions,
								 callbackInfo);
}

OBTreeModifyResult
o_btree_delete_pk_changed(desc: &mut BTreeDescr, Pointer key,
						  BTreeKeyType keyType, OXid oxid,
						  CommitSeqNo csn,
						  hint: &mut BTreeLocationHint,
						  callbackInfo: &mut BTreeModifyCallbackInfo)
{
	pub static mut NULL_TUP: OTuple = std::mem::zeroed();

	O_TUPLE_SET_NULL(nullTup);

	return o_btree_normal_modify(desc, BTreeOperationDelete,
								 nullTup, BTreeKeyNone,
								 key, keyType, oxid, csn, RowLockUpdate,
								 hint, BTreeLeafTuplePKChanged,
								 callbackInfo);
}

bool
o_btree_autonomous_insert(desc: &mut BTreeDescr, OTuple tuple)
{
	pub static mut STATE: OAutonomousTxState = std::mem::zeroed();
	pub static mut RESULT: OBTreeModifyResult = std::mem::zeroed();

	if (desc->undoType != UndoLogNone)
	{
		start_autonomous_transaction(&state);
		PG_TRY();
		{
			result = o_btree_normal_modify(desc, BTreeOperationInsert,
										   tuple, BTreeKeyLeafTuple,
										   NULL, BTreeKeyNone,
										   get_current_oxid(),
										   COMMITSEQNO_INPROGRESS,
										   RowLockUpdate,
										   NULL, BTreeLeafTupleNonDeleted,
										   &nullCallbackInfo);
			// no version is necessary here for system trees other than OTable
			if (result == OBTreeModifyResultInserted)
				o_wal_insert(desc, tuple, REPLICA_IDENTITY_DEFAULT, O_TABLE_INVALID_VERSION);
		}
		PG_CATCH();
		{
			abort_autonomous_transaction(&state);
			PG_RE_THROW();
		}
		PG_END_TRY();
		finish_autonomous_transaction(&state);
	}
	else
	{
		result = o_btree_normal_modify(desc, BTreeOperationInsert,
									   tuple, BTreeKeyLeafTuple,
									   NULL, BTreeKeyNone,
									   InvalidOXid,
									   COMMITSEQNO_INPROGRESS,
									   RowLockUpdate,
									   NULL, BTreeLeafTupleNonDeleted,
									   &nullCallbackInfo);
	}

	return (result == OBTreeModifyResultInserted);
}

bool
o_btree_autonomous_delete(desc: &mut BTreeDescr, OTuple key, BTreeKeyType keyType,
						  hint: &mut BTreeLocationHint)
{
	pub static mut STATE: OAutonomousTxState = std::mem::zeroed();
	pub static mut RESULT: OBTreeModifyResult = std::mem::zeroed();

	Assert(keyType == BTreeKeyLeafTuple || keyType == BTreeKeyNonLeafKey);

	if (desc->undoType != UndoLogNone)
	{
		start_autonomous_transaction(&state);
		PG_TRY();
		{
			result = o_btree_normal_modify(desc, BTreeOperationDelete,
										   key, keyType,
										   NULL, BTreeKeyNone,
										   get_current_oxid(), COMMITSEQNO_INPROGRESS,
										   RowLockUpdate,
										   hint, BTreeLeafTupleNonDeleted,
										   &nullCallbackInfo);
			Assert(IS_SYS_TREE_OIDS(desc->oids));
			// no version is necessary here for system trees other than OTable
			if (result == OBTreeModifyResultDeleted)
			{
				if (keyType == BTreeKeyLeafTuple)
					o_wal_delete(desc, key, REPLICA_IDENTITY_DEFAULT, O_TABLE_INVALID_VERSION);
				else if (keyType == BTreeKeyNonLeafKey)
					o_wal_delete_key(desc, key, false, O_TABLE_INVALID_VERSION);
			}
		}
		PG_CATCH();
		{
			abort_autonomous_transaction(&state);
			PG_RE_THROW();
		}
		PG_END_TRY();
		finish_autonomous_transaction(&state);
	}
	else
	{
		result = o_btree_normal_modify(desc, BTreeOperationDelete,
									   key, keyType,
									   NULL, BTreeKeyNone,
									   InvalidOXid, COMMITSEQNO_INPROGRESS,
									   RowLockUpdate,
									   hint, BTreeLeafTupleNonDeleted,
									   &nullCallbackInfo);
	}

	return (result == OBTreeModifyResultDeleted);
}