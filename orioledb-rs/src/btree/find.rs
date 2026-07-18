use crate::access::transam;
use crate::btree::fastpath;
use crate::btree::find;
use crate::btree::insert;
use crate::btree::io;
use crate::btree::page_chunks;
use crate::orioledb;
use crate::tableam::descr;
use crate::utils::stopevent;
use pgrx::pg_sys;

// -------------------------------------------------------------------------
//
// find.c
// Routines for finding appropriate page in B-tree.
//
// Copyright (c) 2021-2026, Oriole DB Inc.
// Copyright (c) 2025-2026, Supabase Inc.
//
// IDENTIFICATION
// contrib/orioledb/src/btree/find.c
//
// -------------------------------------------------------------------------
//

typedef struct
{
	pub static mut OB_TREE_FIND_PAGE_CONTEXT: *mut context = std::ptr::null_mut();
		   *key;
	pub static mut KEY_TYPE: BTreeKeyType = std::mem::zeroed();
	pub static mut PAGE_PTR: Page = std::mem::zeroed();
	pub static mut TARGET_LEVEL: std::os::raw::c_int = 0;
	pub static mut BLKNO: OInMemoryBlkno = std::mem::zeroed();
	pub static mut PAGE_CHANGE_COUNT: uint32 = std::mem::zeroed();
	pub static mut PARTIAL_PAGE_STATE: *mut partial = std::ptr::null_mut();
	pub static mut HAVE_LOCK: bool = false;
	pub static mut INSERTED: bool = false;
	pub static mut TRY_LOCK_FAILED: bool = false;
} OBTreeFindPageInternalContext;

static bool follow_rightlink(intCxt: &mut OBTreeFindPageInternalContext);
fn step_upward_level(intCxt: &mut OBTreeFindPageInternalContext);
static bool btree_find_read_page(context: &mut OBTreeFindPageContext,
								 OInMemoryBlkno blkno, uint32 pageChangeCount,
								 bool parent,  *key, BTreeKeyType keyType,
								 partial: &mut PartialPageState,
								 bool loadHikeysChunk);
static ReadPageResult btree_find_try_read_page(context: &mut OBTreeFindPageContext,
											   OInMemoryBlkno blkno,
											   uint32 pageChangeCount, bool parent,
											    *key, BTreeKeyType keyType,
											   partial: &mut PartialPageState,
											   bool loadHikeysChunk);

static OffsetNumber btree_page_binary_search_chunks(desc: &mut BTreeDescr, Page p,
													Pointer key,
													BTreeKeyType keyType);
fn btree_page_search_items(desc: &mut BTreeDescr, Page p, Pointer key,
									BTreeKeyType keyType,
									locator: &mut BTreePageItemLocator);
fn refresh_parent_img_chunk(intCxt: &mut OBTreeFindPageInternalContext);
static bool convert_fastpath_parent_to_img(context: &mut OBTreeFindPageContext,
										   locator: &mut BTreePageItemLocator);

//
// A parent locator that find_left_page()/find_right_page() will navigate must
// point into context->parentImg (or be NULL).  Assert that at every place we
// record or advance such a locator, so a stray shared-page/img pointer is
// caught at its producer rather than only when the sibling step consumes it.
//
#define ASSERT_PARENT_LOCATOR_LOCAL(context, loc) \
	Assert((loc).chunk == NULL || \
		   ((Pointer) (loc).chunk >= (context)->parentImg && \
			(Pointer) (loc).chunk < (context)->parentImg + ORIOLEDB_BLCKSZ))

//
// Initialize B-tree page find context.
//

init_page_find_context(context: &mut OBTreeFindPageContext, desc: &mut BTreeDescr,
					   CommitSeqNo csn, uint16 flags)
{
	ASAN_UNPOISON_MEMORY_REGION(context, sizeof(*context));
	context->partial.isPartial = false;
	context->desc = desc;
	context->csn = csn;
	context->index = 0;
	context->flags = flags;
	context->imgUndoLoc = InvalidUndoLocation;
	context->img = NULL;
	context->parentImg = NULL;
	O_TUPLE_SET_NULL(context->insertTuple);
	O_TUPLE_SET_NULL(context->lokey.tuple);
}

static OBTreeFastPathFindResult
page_find_downlink(intCxt: &mut OBTreeFindPageInternalContext,
				   meta: &mut FastpathFindDownlinkMeta,
				   int level,
				   bool fastPathDownlink,
				   loc: &mut BTreePageItemLocator,
				   BTreeNonLeafTuphdr **tuphdr)
{
	pub static mut OB_TREE_FIND_PAGE_CONTEXT: *mut context = intCxt->context;
	pub static mut B_TREE_DESCR: *mut desc = context->desc;
		   *key = intCxt->key;
	pub static mut KEY_TYPE: BTreeKeyType = intCxt->keyType;
	pub static mut ITEM_FOUND: bool = true;

	if (fastPathDownlink)
	{
		pub static mut RESULT: OBTreeFastPathFindResult = std::mem::zeroed();

		result = fastpath_find_downlink(intCxt->pagePtr, intCxt->blkno,
										meta, loc, tuphdr);

		if (result != OBTreeFastPathFindSlowpath)
			pub static mut RESULT: return = std::mem::zeroed();
	}

	if (intCxt->partial &&
		intCxt->partial->isPartial &&
		!intCxt->partial->hikeysChunkIsLoaded)
	{
		if (!partial_load_hikeys_chunk(intCxt->partial, intCxt->pagePtr))
			pub static mut OB_TREE_FAST_PATH_FIND_RETRY: return = std::mem::zeroed();
	}

	//
// BTreeKeyNone requests leftmost page.  Otherwise, consider following the
// rightlink.
//
	if (keyType != BTreeKeyNone)
	{
		if (follow_rightlink(intCxt))
		{
			if (intCxt->tryLockFailed)
				pub static mut OB_TREE_FAST_PATH_FIND_FAILURE: return = std::mem::zeroed();
			if (intCxt->inserted)
				pub static mut OB_TREE_FAST_PATH_FIND_FAILURE: return = std::mem::zeroed();
			Assert(context->index > 0);
			Assert(!intCxt->haveLock);
			step_upward_level(intCxt);
			pub static mut OB_TREE_FAST_PATH_FIND_RETRY: return = std::mem::zeroed();
		}
	}

	//
// Choose the appropriate downlink for further search.
//
	if (keyType == BTreeKeyRightmost)
		BTREE_PAGE_LOCATOR_LAST(intCxt->pagePtr, loc);
	else if (keyType == BTreeKeyNone)
		BTREE_PAGE_LOCATOR_FIRST(intCxt->pagePtr, loc);
	else
	{
		Assert(key);
		// Have to do the binary search otherwise
		itemFound = btree_page_search(desc, intCxt->pagePtr, key, keyType,
									  intCxt->partial, loc);
		if (itemFound)
		{
			BTREE_PAGE_LOCATOR_PREV(intCxt->pagePtr, loc);
			if (intCxt->partial)
				itemFound = partial_load_chunk(intCxt->partial,
											   intCxt->pagePtr,
											   loc->chunkOffset,
											   NULL);
		}
	}

	if (intCxt->partial)
	{
		if (!itemFound || !partial_load_chunk(intCxt->partial,
											  intCxt->pagePtr,
											  loc->chunkOffset,
											  NULL))
		{
			Assert(!intCxt->haveLock);
			if (BTREE_PAGE_FIND_IS(context, TRY_LOCK))
				pub static mut OB_TREE_FAST_PATH_FIND_FAILURE: return = std::mem::zeroed();
			pub static mut OB_TREE_FAST_PATH_FIND_RETRY: return = std::mem::zeroed();
		}

		if (BTREE_PAGE_FIND_IS(context, IMAGE) &&
			level == intCxt->targetLevel + 1 &&
			BTREE_PAGE_FIND_IS(context, KEEP_LOKEY))
		{
			//
// We may need to load another one tuple for a backward iteration.
//
			if (loc->itemOffset == 0 && loc->chunkOffset > 0 &&
				!partial_load_chunk(intCxt->partial, intCxt->pagePtr,
									loc->chunkOffset - 1, NULL))
			{
				Assert(!intCxt->haveLock);
				pub static mut OB_TREE_FAST_PATH_FIND_RETRY: return = std::mem::zeroed();
			}
		}
	}

	*tuphdr = (BTreeNonLeafTuphdr *) BTREE_PAGE_LOCATOR_GET_ITEM(intCxt->pagePtr, loc);

	pub static mut OB_TREE_FAST_PATH_FIND_OK: return = std::mem::zeroed();
}

static OBTreeFastPathFindResult
page_find_item(intCxt: &mut OBTreeFindPageInternalContext,
			   meta: &mut FastpathFindDownlinkMeta,
			   int level,
			   bool fastpath,
			   loc: &mut BTreePageItemLocator,
			   BTreeNonLeafTuphdr **tuphdr)
{
	pub static mut OB_TREE_FIND_PAGE_CONTEXT: *mut context = intCxt->context;
	pub static mut B_TREE_DESCR: *mut desc = context->desc;
		   *key = intCxt->key;
	pub static mut KEY_TYPE: BTreeKeyType = intCxt->keyType;
	pub static mut ITEM_FOUND: bool = true;

	if (fastpath && intCxt->partial->isPartial)
	{
		pub static mut RESULT: OBTreeFastPathFindResult = std::mem::zeroed();
		pub static mut CHUNK_INDEX: std::os::raw::c_int = 0;

		Assert(!BTREE_PAGE_FIND_IS(context, MODIFY));

		result = fastpath_find_chunk(intCxt->pagePtr,
									 intCxt->blkno,
									 meta,
									 &chunkIndex);

		if (result == OBTreeFastPathFindOK &&
			!partial_load_chunk(intCxt->partial,
								intCxt->pagePtr,
								chunkIndex,
								loc))
			result = OBTreeFastPathFindRetry;

		if (result == OBTreeFastPathFindOK)
		{
			if (keyType == BTreeKeyRightmost)
			{
				loc->itemOffset = loc->chunkItemsCount - 1;
			}
			else if (keyType == BTreeKeyNone)
			{
				loc->itemOffset = 0;
			}
			else
			{
				btree_page_search_items(desc, intCxt->pagePtr,
										key, keyType, loc);
			}

			if (page_locator_find_real_item(intCxt->pagePtr,
											intCxt->partial,
											loc))
			{
				if (level > 0)
					*tuphdr = (BTreeNonLeafTuphdr *) BTREE_PAGE_LOCATOR_GET_ITEM(intCxt->pagePtr, loc);

				pub static mut OB_TREE_FAST_PATH_FIND_OK: return = std::mem::zeroed();
			}
			else
			{
				result = OBTreeFastPathFindRetry;
			}
		}

		if (result == OBTreeFastPathFindRetry)
		{
			//
// Can not read partial page, it happens if the pages was
// concurrently changed. But it should not happen under the
// lock_page().
//
			Assert(!intCxt->haveLock);
			if (BTREE_PAGE_FIND_IS(context, TRY_LOCK))
				pub static mut OB_TREE_FAST_PATH_FIND_FAILURE: return = std::mem::zeroed();
			pub static mut OB_TREE_FAST_PATH_FIND_RETRY: return = std::mem::zeroed();
		}
		else if (result == OBTreeFastPathFindFailure)
		{
			pub static mut OB_TREE_FAST_PATH_FIND_FAILURE: return = std::mem::zeroed();
		}
		Assert(result == OBTreeFastPathFindSlowpath);
	}

	if (intCxt->partial &&
		intCxt->partial->isPartial &&
		!intCxt->partial->hikeysChunkIsLoaded)
	{
		if (!partial_load_hikeys_chunk(intCxt->partial, intCxt->pagePtr))
			pub static mut OB_TREE_FAST_PATH_FIND_RETRY: return = std::mem::zeroed();
	}

	//
// BTreeKeyNone requests leftmost page.  Otherwise, consider following the
// rightlink.
//
	if (keyType != BTreeKeyNone)
	{
		if (follow_rightlink(intCxt))
		{
			if (intCxt->tryLockFailed)
				pub static mut OB_TREE_FAST_PATH_FIND_FAILURE: return = std::mem::zeroed();
			Assert(context->index > 0);
			Assert(!intCxt->haveLock);
			step_upward_level(intCxt);
			pub static mut OB_TREE_FAST_PATH_FIND_RETRY: return = std::mem::zeroed();
		}
	}

	//
// Choose the appropriate downlink for further search.
//
	if (keyType == BTreeKeyRightmost)
		BTREE_PAGE_LOCATOR_LAST(intCxt->pagePtr, loc);
	else if (keyType == BTreeKeyNone)
		BTREE_PAGE_LOCATOR_FIRST(intCxt->pagePtr, loc);
	else
	{
		Assert(key);
		// Have to do the binary search otherwise
		itemFound = btree_page_search(desc, intCxt->pagePtr,
									  key, keyType,
									  intCxt->partial, loc);
		if (itemFound && !BTREE_PAGE_FIND_IS(context, MODIFY))
			itemFound = page_locator_find_real_item(intCxt->pagePtr,
													intCxt->partial,
													loc);
	}

	if (intCxt->partial &&
		(!itemFound || !partial_load_chunk(intCxt->partial,
										   intCxt->pagePtr,
										   loc->chunkOffset,
										   NULL)))
	{
		//
// Can not read partial page, it happens if the pages was concurrently
// changed. But it should not happen under the lock_page().
//
		Assert(!intCxt->haveLock);
		if (BTREE_PAGE_FIND_IS(context, TRY_LOCK))
			pub static mut OB_TREE_FAST_PATH_FIND_FAILURE: return = std::mem::zeroed();
		pub static mut OB_TREE_FAST_PATH_FIND_RETRY: return = std::mem::zeroed();
	}

	if (level > 0)
		*tuphdr = (BTreeNonLeafTuphdr *) BTREE_PAGE_LOCATOR_GET_ITEM(intCxt->pagePtr, loc);

	pub static mut OB_TREE_FAST_PATH_FIND_OK: return = std::mem::zeroed();
}

//
// Refresh context->parentImg from the locked shared-memory page held
// by `intCxt` and rebind the current locator's chunk onto parentImg.
// Used by find_page at level == targetLevel + 1 in IMAGE mode when
// intCxt->pagePtr is the real shared-memory page (not parentImg):
// without this rebind, the iterator's later find_right_page /
// find_left_page would navigate through a chunk pointer into a page
// the descent has already unlocked.
//
// Only the page header (with hikeys) and the chunk that the locator
// currently references are copied; the partial state is set up so
// other chunks can be loaded on demand by partial_load_chunk if
// find_right_page / find_left_page later visit them.  The standard
// consistency check in partial_load_chunk then falls through to a
// find_page re-descent if the source has been concurrently mutated.
//
fn
refresh_parent_img_chunk(intCxt: &mut OBTreeFindPageInternalContext)
{
	pub static mut OB_TREE_FIND_PAGE_CONTEXT: *mut context = intCxt->context;
	pub static mut SRC: Pointer = intCxt->pagePtr;
	pub static mut B_TREE_PAGE_ITEM_LOCATOR: *mut locator = &context->items[context->index].locator;
	hdr: &mut BTreePageHeader = (BTreePageHeader *) src;
	pub static mut CHUNK_OFFSET: OffsetNumber = locator->chunkOffset;
	pub static mut CHUNK_BEGIN: LocationIndex = std::mem::zeroed();
	pub static mut CHUNK_END: LocationIndex = std::mem::zeroed();

	chunkBegin = SHORT_GET_LOCATION(hdr->chunkDesc[chunkOffset].shortLocation);
	if (chunkOffset + 1 < hdr->chunksCount)
		chunkEnd = SHORT_GET_LOCATION(hdr->chunkDesc[chunkOffset + 1].shortLocation);
	else
		chunkEnd = hdr->dataSize;

	// Header including the hikeys chunk.
	memcpy(context->parentImg, src, hdr->hikeysEnd);
	// The single chunk that `locator` references.
	memcpy(context->parentImg + chunkBegin,
		   src + chunkBegin,
		   chunkEnd - chunkBegin);

	context->parentPartial.src = src;
	context->parentPartial.isPartial = true;
	context->parentPartial.hikeysChunkIsLoaded = true;
	memset(context->parentPartial.chunkIsLoaded, 0,
		   sizeof(context->parentPartial.chunkIsLoaded));
	context->parentPartial.chunkIsLoaded[chunkOffset] = true;

	locator->chunk =
		(BTreePageChunk *) (context->parentImg + chunkBegin);
}

//
// The fastpath downlink search positions the locator straight onto the shared
// page (it skips loading the hikeys chunk, so parentImg holds only the base
// header that the descent's partial read already copied there).  Callers that
// step to siblings (KEEP_PARENT) need the parent fully navigable in parentImg,
// so finish that partial read on top of existing: &mut the* snapshot: load the
// hikeys chunk (the chunk-descriptor array) and the chunk holding the downlink
// into parentImg through the already-set-up context->parentPartial, then rebind
// the locator onto parentImg.
//
// We deliberately do NOT re-read the page from scratch (o_btree_read_page()):
// the base header was snapshotted during the descent, and re-snapshotting would
// capture a possibly newer page version, leaving parentImg inconsistent with
// the downlink the fastpath already chose off that snapshot.  Both loads
// validate against the snapshot's change count (state bits + pageChangeCount),
// so a parent that changed or was evicted/reused under us makes them fail and
// the caller re-finds.  partial_load_chunk() positions the locator at item 0,
// so the caller's real itemOffset is restored afterwards.
//
// Returns false if the parent changed under us; the caller must re-find.
//
static bool
convert_fastpath_parent_to_img(context: &mut OBTreeFindPageContext,
							   locator: &mut BTreePageItemLocator)
{
	pub static mut CHUNK_OFFSET: OffsetNumber = locator->chunkOffset;
	pub static mut ITEM_OFFSET: OffsetNumber = locator->itemOffset;

	if (!partial_load_hikeys_chunk(&context->parentPartial, context->parentImg))
		pub static mut FALSE: return = std::mem::zeroed();

	if (!partial_load_chunk(&context->parentPartial, context->parentImg,
							chunkOffset, locator))
		pub static mut FALSE: return = std::mem::zeroed();

	locator->itemOffset = itemOffset;
	pub static mut TRUE: return = std::mem::zeroed();
}

// --
// Locate page and location within it for given key
//
// - context - context of parent pages
// - key - key/tuple for search (NULL for the leftmost page)
// - keyType - type of the key
// - targetLevel - target page targetLevel to find
//
// For better efficiency on large pages we use partial approach for page read
// from the shared memory. We have 3 alternative types of the call
// depending on context->flags:
//
// 1. BTREE_PAGE_FIND_FETCH - fetches a single tuple. It uses partial read for
// all pages.
//
// 2. BTREE_PAGE_FIND_MODIFY - find the page for modification. It uses partial read
// for all parent pages, call lock_page() on a target page and search a tuple
// on the target page in the shared memory.
//
// 3. BTREE_PAGE_FIND_IMAGE - copy a target leaf(!) to context->img. It useful
// for iteration through the page. Reads parent pages partial and then
// memcpy() a leaf page to the context.image. It holds lokey
// if BTREE_PAGE_FIND_KEEP_LOKEY is set.
//
OFindPageResult
find_page(context: &mut OBTreeFindPageContext,  *key, BTreeKeyType keyType,
		  uint16 targetLevel)
{
	pub static mut B_TREE_DESCR: *mut desc = context->desc;
	pub static mut INT_CXT: OBTreeFindPageInternalContext = std::mem::zeroed();
	pub static mut LOC: BTreePageItemLocator = std::mem::zeroed();
	bool		needLock = false,
				fetchFlag = BTREE_PAGE_FIND_IS(context, FETCH),
				modifyFlag = BTREE_PAGE_FIND_IS(context, MODIFY),
				imageFlag = BTREE_PAGE_FIND_IS(context, IMAGE),
				tryFlag = BTREE_PAGE_FIND_IS(context, TRY_LOCK),
				fixLeafFlag = BTREE_PAGE_FIND_IS(context, FIX_LEAF_SPLIT),
				noFixFlag PG_USED_FOR_ASSERTS_ONLY = BTREE_PAGE_FIND_IS(context, NO_FIX_SPLIT),
				keepLokeyFlag = BTREE_PAGE_FIND_IS(context, KEEP_LOKEY),
				keepParentFlag = BTREE_PAGE_FIND_IS(context, KEEP_PARENT),
				downlinkLocationFlag = BTREE_PAGE_FIND_IS(context, DOWNLINK_LOCATION);
	pub static mut SHMEM_IS_RELOADED: bool = false;
	pub static mut LOAD_HIKEYS: bool = false;
	pub static mut FASTPATH_META: FastpathFindDownlinkMeta = std::mem::zeroed();
	pub static mut JSONB: *mut params = std::ptr::null_mut();

	memset(&intCxt, 0, sizeof(intCxt));
	ASAN_UNPOISON_MEMORY_REGION(&intCxt, sizeof(intCxt));
	intCxt.context = context;
	intCxt.key = key;
	intCxt.keyType = keyType;
	intCxt.targetLevel = targetLevel;
	intCxt.inserted = false;
	context->parentImgDeferred = false;

	ASAN_UNPOISON_MEMORY_REGION(&fastpathMeta, sizeof(fastpathMeta));
	if (STOPEVENTS_ENABLED())
		fastpathMeta.enabled = false;
	else
		can_fastpath_find_downlink(context, key, keyType, &fastpathMeta);

	//
// See description of the function.
//
	Assert((imageFlag && (targetLevel <= ORIOLEDB_MAX_DEPTH) && !fetchFlag && !modifyFlag)
		   || (imageFlag && targetLevel == 0 && !fetchFlag && modifyFlag)
		   || (!imageFlag && fetchFlag && !modifyFlag)
		   || (!imageFlag && !fetchFlag && modifyFlag && !keepLokeyFlag));
	Assert(!(COMMITSEQNO_IS_NORMAL(context->csn) && modifyFlag));

	// resets the context before start
	if (BTREE_PAGE_FIND_IS(context, KEEP_LOKEY))
	{
		BTREE_PAGE_FIND_UNSET(context, LOKEY_EXISTS);
		BTREE_PAGE_FIND_UNSET(context, LOKEY_SIBLING);
		BTREE_PAGE_FIND_UNSET(context, LOKEY_UNDO);
	}
	context->imgUndoLoc = InvalidUndoLocation;
	context->partial.isPartial = false;
	context->parentPartial.isPartial = false;
	context->index = 0;

	if (!tryFlag)
	{
		o_btree_load_shmem(desc);
	}
	else
	{
		if (!o_btree_try_use_shmem(desc))
			pub static mut O_FIND_PAGE_RESULT_FAILURE: return = std::mem::zeroed();
	}
	Assert(ORootPageIsValid(desc) && OMetaPageIsValid(desc));

	// starts from the rootPageBlkno
	intCxt.blkno = desc->rootInfo.rootPageBlkno;
	intCxt.pageChangeCount = desc->rootInfo.rootPageChangeCount;
	while (true)
	{
		pub static mut B_TREE_NON_LEAF_TUPHDR: *mut nonLeafHdr = std::ptr::null_mut();
		pub static mut LEVEL: std::os::raw::c_int = 0;
		pub static mut PARENT_BLKNO: OInMemoryBlkno = std::mem::zeroed();
		pub static mut WRONG_CHANGE_COUNT: bool = false;
		pub static mut P: Pointer = std::ptr::null_mut();
		pub static mut FASTPATH: bool = false;

		//
// Local-pool slots are NULLed on eviction, unlike shared-pool slots
// whose shmem page stays readable (only pageChangeCount changes).  An
// IN_MEMORY downlink we just descended through may reference a slot
// the backend evicted earlier in the same call chain -- e.g. a
// reserve_page triggered during a seq scan, or a find_page invoked
// from an undo callback.  PAGE_GET_LEVEL below would segfault on the
// NULL slot, so step back to the parent and re-resolve the downlink
// (it now points to disk).  At the root there is no parent, so report
// failure.
//
		if (O_PAGE_IS_LOCAL(intCxt.blkno) &&
			local_ppool_pages[intCxt.blkno & O_BLKNO_MASK] == NULL)
		{
			if (context->index == 0)
				pub static mut O_FIND_PAGE_RESULT_FAILURE: return = std::mem::zeroed();
			step_upward_level(&intCxt);
			continue;
		}

		p = O_GET_IN_MEMORY_PAGE(intCxt.blkno);
		level = PAGE_GET_LEVEL(p);

		fastpath = fastpathMeta.enabled && !needLock;
		fastpath = fastpath && (keyType != BTreeKeyPageHiKey || level > 0);

		intCxt.partial = NULL;

		//
// The leaf's partial state is re-initialized by each page read, so it
// is safe to reset here unconditionally.  The parent's partial state
// lives in context->parentPartial and is preserved across the leaf
// read so find_left_page()/find_right_page() can navigate siblings
// via the parent.
//
		context->partial.isPartial = false;

		if (needLock || (modifyFlag && level == targetLevel))
		{
			if (tryFlag)
			{
				if (!try_lock_page(intCxt.blkno))
					pub static mut O_FIND_PAGE_RESULT_FAILURE: return = std::mem::zeroed();
				intCxt.pagePtr = p;
				intCxt.haveLock = true;
				needLock = false;
			}
			else if (!O_TUPLE_IS_NULL(context->insertTuple))
			{
				pub static mut RESULT: OLockPageWithTupleResult = std::mem::zeroed();

				result = lock_page_with_tuple(desc,
											  &intCxt.blkno,
											  &intCxt.pageChangeCount,
											  context->insertXactInfo,
											  context->insertTuple);

				if (result == OLockPageWithTupleResultLocked)
				{
					p = O_GET_IN_MEMORY_PAGE(intCxt.blkno);
					intCxt.pagePtr = p;
					intCxt.haveLock = true;
					needLock = false;
				}
				else if (result == OLockPageWithTupleResultInserted)
				{
					pub static mut O_FIND_PAGE_RESULT_INSERTED: return = std::mem::zeroed();
				}
				else
				{
					Assert(result == OLockPageWithTupleResultRefindNeeded);
					wrongChangeCount = true;
				}
			}
			else
			{
				lock_page(intCxt.blkno);
				intCxt.pagePtr = p;
				intCxt.haveLock = true;
				needLock = false;
			}
		}
		else
		{
			pub static mut USE_PARENT_IMG: bool = false;

			if (imageFlag || fetchFlag)
			{
				//
// In both BTREE_PAGE_FIND_IMAGE and BTREE_PAGE_FIND_FETCH we
// read upper non-leaf (parent) pages partially to
// context->parentImg using context->parentPartial, so
// find_left_page()/find_right_page() can navigate to sibling
// pages through the parent's downlinks.
//
// The target (leaf) page is read to context->img: in full for
// IMAGE (cheaper to iterate the whole page in memory) and
// partially for FETCH (cheaper when only part of the page is
// actually read).
//
// We consider it's OK to return page of lower targetLevel
// than required, if tree doesn't have enough height.  That's
// suitable for sequential scan (see btree_scan.c).
//
				if (level <= targetLevel)
				{
					useParentImg = false;
					if (fetchFlag)
					{
						intCxt.partial = &context->partial;
					}
					else
					{
						intCxt.partial = NULL;
						Assert(!fastpath);
					}
				}
				else
				{
					useParentImg = true;
					intCxt.partial = &context->parentPartial;
				}
			}
			else
			{
				//
// BTREE_PAGE_FIND_MODIFY: parent pages are read partially to
// context->img; the target page is locked above.
//
				useParentImg = false;
				intCxt.partial = &context->partial;
			}

			intCxt.haveLock = false;

			//
// The fastpath skips loading the hikeys chunk.  That is fine for
// a single-tuple search; a sibling-navigating caller
// (KEEP_PARENT, the iterator) loads the hikeys chunk on demand
// only when it needs it -- when stepping to a sibling, or when
// crossing a chunk boundary within a leaf (the chunk-descriptor
// array lives in the hikeys chunk).  So it does not need the
// hikeys chunk in the image here either.
//
			loadHikeys = !fastpath;

			if (tryFlag)
			{
				pub static mut RESULT: ReadPageResult = std::mem::zeroed();

				result = btree_find_try_read_page(context, intCxt.blkno,
												  intCxt.pageChangeCount,
												  useParentImg,
												  key, keyType,
												  intCxt.partial,
												  loadHikeys);
				intCxt.pagePtr = useParentImg ? context->parentImg : context->img;
				if (result == ReadPageResultWrongPageChangeCount)
				{
					wrongChangeCount = true;
				}
				else if (result == ReadPageResultFailed)
				{
					pub static mut O_FIND_PAGE_RESULT_FAILURE: return = std::mem::zeroed();
				}
			}
			else
			{
				pub static mut RESULT: bool = false;

				result = btree_find_read_page(context, intCxt.blkno,
											  intCxt.pageChangeCount,
											  useParentImg, key, keyType,
											  intCxt.partial,
											  loadHikeys);
				intCxt.pagePtr = useParentImg ? context->parentImg : context->img;
				if (!result)
				{
					if (context->index == 0)
					{
						wrongChangeCount = true;
					}
					else
					{
						step_upward_level(&intCxt);
						continue;
					}
				}
			}
		}

		// Re-try the page level has been changed
		if (!wrongChangeCount && level != PAGE_GET_LEVEL(intCxt.pagePtr))
		{
			if (intCxt.haveLock)
			{
				unlock_page(intCxt.blkno);
				intCxt.haveLock = false;
			}
			continue;
		}

		if (!wrongChangeCount && STOPEVENTS_ENABLED())
		{
			params = btree_page_stopevent_params(desc, intCxt.pagePtr);
			STOPEVENT(STOPEVENT_PAGE_READ, params);
		}

		// Handle the incorrect root situation
		if (context->index == 0 && (wrongChangeCount ||
									intCxt.pageChangeCount != O_PAGE_GET_CHANGE_COUNT(intCxt.pagePtr)))
		{
			// Release lock if needed
			if (intCxt.haveLock)
			{
				unlock_page(intCxt.blkno);
				intCxt.haveLock = false;
			}

			//
// We don't need to re-read shared memory more that once with TRY
// flag.
//
			if (tryFlag && shmemIsReloaded)
				pub static mut O_FIND_PAGE_RESULT_FAILURE: return = std::mem::zeroed();

			// Reload root information from the shared memory
			desc->rootInfo.rootPageBlkno = OInvalidInMemoryBlkno;
			desc->rootInfo.metaPageBlkno = OInvalidInMemoryBlkno;
			desc->rootInfo.rootPageChangeCount = 0;
			if (tryFlag)
			{
				if (!o_btree_try_use_shmem(desc))
					pub static mut O_FIND_PAGE_RESULT_FAILURE: return = std::mem::zeroed();
			}
			else
			{
				o_btree_load_shmem(desc);
			}
			shmemIsReloaded = true;

			// Initiate another attempt
			intCxt.blkno = desc->rootInfo.rootPageBlkno;
			intCxt.pageChangeCount = desc->rootInfo.rootPageChangeCount;
			p = O_GET_IN_MEMORY_PAGE(intCxt.blkno);
			continue;
		}

		if (context->index > 0 && (wrongChangeCount ||
								   intCxt.pageChangeCount != O_PAGE_GET_CHANGE_COUNT(intCxt.pagePtr)))
		{
			//
// It's not the expected page, try to refind it.
//
			step_upward_level(&intCxt);
			continue;
		}

		if (level > targetLevel || (downlinkLocationFlag && level > 0))
		{
			pub static mut RESULT: OBTreeFastPathFindResult = std::mem::zeroed();

			result = page_find_downlink(&intCxt, &fastpathMeta, level,
										fastpath, &loc, &nonLeafHdr);

			Assert(result != OBTreeFastPathFindSlowpath);

			if (result == OBTreeFastPathFindFailure)
				pub static mut O_FIND_PAGE_RESULT_FAILURE: return = std::mem::zeroed();
			else if (result == OBTreeFastPathFindRetry)
				continue;
			p = O_GET_IN_MEMORY_PAGE(intCxt.blkno);
		}
		else
		{
			pub static mut RESULT: OBTreeFastPathFindResult = std::mem::zeroed();

			result = page_find_item(&intCxt, &fastpathMeta, level,
									fastpath, &loc, &nonLeafHdr);

			if (result == OBTreeFastPathFindFailure)
				pub static mut O_FIND_PAGE_RESULT_FAILURE: return = std::mem::zeroed();
			else if (result == OBTreeFastPathFindRetry)
			{
				if (intCxt.inserted)
					pub static mut O_FIND_PAGE_RESULT_INSERTED: return = std::mem::zeroed();
				continue;
			}
			p = O_GET_IN_MEMORY_PAGE(intCxt.blkno);
		}

		if (STOPEVENTS_ENABLED())
		{
			params = btree_page_stopevent_params(desc, intCxt.pagePtr);
			STOPEVENT(STOPEVENT_AFTER_FIND_DOWNLINK, params);
		}

		// Place new item to the context
		Assert(context->index < ORIOLEDB_MAX_DEPTH);

		context->items[context->index].locator = loc;
		context->items[context->index].blkno = intCxt.blkno;

		//
// The immediate parent's downlink may have been located via the
// fastpath, leaving the locator pointing into the shared page (and
// parentImg not populated).  A sibling-navigating caller
// (KEEP_PARENT, e.g. the iterator) needs the parent in parentImg.
//
// A backward scan (KEEP_LOKEY) reads the parent's lokey from
// parentImg just below, so materialize it now; if the parent changed
// under us, re-read it from the top of the loop.  A forward scan
// touches the parent only when it steps right, so defer the copy to
// find_right_page() -- a scan that never crosses a parent boundary
// then pays nothing, and the on-demand copy is fresher than one
// carried across many iterator steps.  (A slowpath parent read
// already filled parentImg, so nothing to do there.)
//
		if (level == targetLevel + 1)
		{
			if (fastpath && keepParentFlag && !keepLokeyFlag)
				context->parentImgDeferred = true;
			else
			{
				context->parentImgDeferred = false;
				if (fastpath && keepParentFlag &&
					!convert_fastpath_parent_to_img(context,
													&context->items[context->index].locator))
					continue;
			}
		}

		context->items[context->index].pageChangeCount = O_PAGE_GET_CHANGE_COUNT(intCxt.pagePtr);

		//
// Save the lokey if needed.
//
// For levels above the immediate parent the located downlink is the
// propagated lokey of the leftmost descent below; keep it in
// context->lokey (LOKEY_EXISTS), which btree_find_context_lokey()
// returns when the target page's own downlink is the parent's first
// one.
//
// For the immediate parent of leaf: &mut a* target (level == 1, i.e. the
// leaf iterator's targetLevel == 0) the located downlink is the leaf
// page's own lokey; stash it in the dedicated, stable
// context->leafLokey so btree_find_context_lokey() can return it
// without re-reading the parent image -- which is unreliable in FETCH
// mode, where parentImg is partial and may be reclaimed under
// page-pool pressure during iteration.
//
// When targetLevel > 0 (e.g. the sequential scan descends to
// targetLevel == 1 with KEEP_LOKEY and reads context->lokey
// directly), the immediate parent's downlink is still the target
// page's own lokey, but its consumer expects it in context->lokey,
// exactly as the pre-FETCH-iterator code produced it.  So only divert
// to leafLokey for the leaf case (targetLevel == 0); otherwise fall
// through to context->lokey.
//
		if (keepLokeyFlag && level > targetLevel)
		{
			pub static mut LOKEY: OTuple = std::mem::zeroed();

			//
// A FETCH-mode descent locates the downlink via the fastpath,
// which does not materialize parentImg
// (can_fastpath_find_downlink() only enables the fastpath in
// FETCH mode).  The offset check and the lokey read below both
// touch parentImg -- the offset needs the hikeys chunk (chunk
// descriptors) and the read needs the chunk holding `loc` -- so
// materialize them.  This is a no-op when the parent was read
// whole (IMAGE/MODIFY disable the fastpath) or the chunk is
// already loaded; a lost race re-descends from the top.
//
			if (context->parentPartial.isPartial &&
				intCxt.pagePtr == context->parentImg &&
				!convert_fastpath_parent_to_img(context, &loc))
				continue;

			if (BTREE_PAGE_LOCATOR_GET_OFFSET(intCxt.pagePtr, &loc) > 0)
			{
				Assert(nonLeafHdr);

				BTREE_PAGE_READ_INTERNAL_TUPLE(lokey, intCxt.pagePtr, &loc);

				if (level == targetLevel + 1 && targetLevel == 0)
					copy_fixed_key(context->desc, &context->leafLokey, lokey);
				else
				{
					copy_fixed_key(context->desc, &context->lokey, lokey);
					BTREE_PAGE_FIND_SET(context, LOKEY_EXISTS);
					BTREE_PAGE_FIND_UNSET(context, LOKEY_SIBLING);
					BTREE_PAGE_FIND_UNSET(context, LOKEY_UNDO);
				}
			}
		}

		if (level != targetLevel && ((!imageFlag && !fetchFlag) || level > targetLevel) && !nonLeafHdr)
		{
			Assert(tryFlag);
			if (intCxt.haveLock)
			{
				unlock_page(intCxt.blkno);
				intCxt.haveLock = false;
			}
			pub static mut O_FIND_PAGE_RESULT_FAILURE: return = std::mem::zeroed();
		}

		if (level == targetLevel || ((imageFlag || fetchFlag) && level <= targetLevel))
		{
			if (intCxt.haveLock)
			{
				//
// The only way the target is reached under a page lock is the
// modify path -- needLock is set only on level > targetLevel
// and is cleared before we step down, and step_upward_level()
// clears haveLock when it unlocks. The IMAGE/FETCH callers
// expect context->img to be populated, which only happens in
// the lockless else branch above; if we ever reached here
// holding a lock without modifyFlag, that contract would be
// silently broken.
//
				Assert(modifyFlag);

				if (level == 0 && fixLeafFlag)
				{
					// called from o_btree_normal_modify()
					// try to fix incomplete split for leafs here
					pub static mut RELOCKED: bool = false;

					Assert(!noFixFlag);

					if (O_PAGE_IS(p, BROKEN_SPLIT))
					{
						o_btree_split_fix_for_right_page_and_unlock(desc, intCxt.blkno);
						intCxt.haveLock = false;
						step_upward_level(&intCxt);
						continue;
					}
					else if (relocked)
					{
						step_upward_level(&intCxt);
						continue;
					}
				}
			}

			O_TUPLE_SET_NULL(context->insertTuple);
			pub static mut O_FIND_PAGE_RESULT_SUCCESS: return = std::mem::zeroed();
		}
		else if (!nonLeafHdr)
		{
			Assert(false);		// make clang static analyzer happy
		}
		else if (DOWNLINK_IS_ON_DISK(nonLeafHdr->downlink))
		{
			if (tryFlag)
			{
				//
// Don't try to load page from write_page()
//
				if (intCxt.haveLock)
					unlock_page(intCxt.blkno);
				pub static mut O_FIND_PAGE_RESULT_FAILURE: return = std::mem::zeroed();
			}

			if (intCxt.haveLock)
			{
				load_page(context);
				intCxt.blkno = context->items[context->index].blkno;
				loc = context->items[context->index].locator;
				intCxt.pagePtr = p = O_GET_IN_MEMORY_PAGE(intCxt.blkno);
				nonLeafHdr = (BTreeNonLeafTuphdr *) BTREE_PAGE_LOCATOR_GET_ITEM(intCxt.pagePtr, &loc);

				if (level != PAGE_GET_LEVEL(p))
				{
					unlock_page(intCxt.blkno);
					intCxt.haveLock = false;
					continue;
				}

				if ((imageFlag || keepParentFlag) && level == targetLevel + 1)
				{
					//
// Just loaded the target's child into shared memory and
// refound the parent under MODIFY lock; the parent's
// downlinks differ from the pre-load partial read still
// sitting in parentImg.  Refresh parentImg and rebind the
// locator before stepping down.  Needed for any caller
// that later navigates siblings (IMAGE, or FETCH via
// KEEP_PARENT).
//
					refresh_parent_img_chunk(&intCxt);
				}
			}
			else
			{
				needLock = true;
				continue;
			}
		}
		else if (DOWNLINK_IS_IN_IO(nonLeafHdr->downlink))
		{
			int			ionum = DOWNLINK_GET_IO_LOCKNUM(nonLeafHdr->downlink);

			if (intCxt.haveLock)
			{
				unlock_page(intCxt.blkno);
				intCxt.haveLock = false;
			}
			wait_for_io_completion(ionum);
			continue;
		}
		else
		{
			//
// IN_MEMORY downlink at the parent of the target in IMAGE mode.
// If we got here under the lock (needLock = true on an earlier
// iteration) intCxt.pagePtr is the real shared-memory page, not
// parentImg, and the locator that find_right_page/find_left_page
// will later consult still has its chunk pointer bound to shared
// memory.  Refresh parentImg from the locked page and rebind the
// locator onto parentImg so subsequent reads do not race against
// concurrent writers on the unlocked shared page.
//
			if ((imageFlag || keepParentFlag) && level == targetLevel + 1 &&
				intCxt.haveLock && intCxt.pagePtr != context->parentImg)
				refresh_parent_img_chunk(&intCxt);
		}

		parentBlkno = intCxt.blkno;
		context->index++;
		intCxt.blkno = DOWNLINK_GET_IN_MEMORY_BLKNO(nonLeafHdr->downlink);
		intCxt.pageChangeCount = DOWNLINK_GET_IN_MEMORY_CHANGECOUNT(nonLeafHdr->downlink);

		if (STOPEVENTS_ENABLED())
		{
			params = btree_downlink_stopevent_params(desc, intCxt.pagePtr, &loc);
		}

		if (intCxt.haveLock)
		{
			unlock_page(parentBlkno);
			intCxt.haveLock = false;
		}

		p = O_GET_IN_MEMORY_PAGE(intCxt.blkno);
		STOPEVENT(STOPEVENT_STEP_DOWN, params);
	}
}

static bool
follow_rightlink(intCxt: &mut OBTreeFindPageInternalContext)
{
	pub static mut OB_TREE_FIND_PAGE_CONTEXT: *mut context = intCxt->context;
	pub static mut B_TREE_DESCR: *mut desc = context->desc;
	BTreeKeyType keykind = (intCxt->keyType == BTreeKeyPageHiKey ?
							BTreeKeyNonLeafKey :
							intCxt->keyType);
	int			followVal = (intCxt->keyType == BTreeKeyPageHiKey ? 1 : 0);
	pub static mut PAGE_HI_KEY: OTuple = std::mem::zeroed();

	if (!O_PAGE_IS(intCxt->pagePtr, RIGHTMOST))
		BTREE_PAGE_GET_HIKEY(pageHiKey, intCxt->pagePtr);
	while (!O_PAGE_IS(intCxt->pagePtr, RIGHTMOST) &&
		   (intCxt->keyType == BTreeKeyRightmost ||
			o_btree_cmp(desc, intCxt->key, keykind,
						&pageHiKey, BTreeKeyNonLeafKey) >= followVal))
	{
		uint64		rightlink = BTREE_PAGE_GET_RIGHTLINK(intCxt->pagePtr);

		if (!OInMemoryBlknoIsValid(RIGHTLINK_GET_BLKNO(rightlink)))
		{
			if (intCxt->haveLock)
			{
				unlock_page(intCxt->blkno);
				intCxt->haveLock = false;
			}
			pub static mut TRUE: return = std::mem::zeroed();
		}

		if (BTREE_PAGE_FIND_IS(context, KEEP_LOKEY))
		{
			copy_fixed_hikey(desc, &context->lokey, intCxt->pagePtr);
			\
				Assert(!O_TUPLE_IS_NULL(context->lokey.tuple));
			BTREE_PAGE_FIND_SET(context, LOKEY_EXISTS);
			if (PAGE_GET_LEVEL(intCxt->pagePtr) == intCxt->targetLevel)
			{
				BTREE_PAGE_FIND_SET(context, LOKEY_SIBLING);
				BTREE_PAGE_FIND_UNSET(context, LOKEY_UNDO);
			}
			else
			{
				BTREE_PAGE_FIND_UNSET(context, LOKEY_SIBLING);
				BTREE_PAGE_FIND_UNSET(context, LOKEY_UNDO);
			}
		}

		if (intCxt->haveLock)
			unlock_page(intCxt->blkno);

		intCxt->blkno = RIGHTLINK_GET_BLKNO(rightlink);

		if (intCxt->haveLock)
		{
			if (BTREE_PAGE_FIND_IS(context, TRY_LOCK))
			{
				if (!try_lock_page(intCxt->blkno))
				{
					intCxt->haveLock = false;
					intCxt->tryLockFailed = true;
					pub static mut TRUE: return = std::mem::zeroed();
				}
			}
			else if (!O_TUPLE_IS_NULL(context->insertTuple))
			{
				pub static mut RESULT: OLockPageWithTupleResult = std::mem::zeroed();

				result = lock_page_with_tuple(desc,
											  &intCxt->blkno,
											  &intCxt->pageChangeCount,
											  context->insertXactInfo,
											  context->insertTuple);

				if (result == OLockPageWithTupleResultInserted)
				{
					intCxt->haveLock = false;
					intCxt->inserted = true;
					pub static mut TRUE: return = std::mem::zeroed();
				}
				else if (result == OLockPageWithTupleResultRefindNeeded)
				{
					intCxt->haveLock = false;
					pub static mut TRUE: return = std::mem::zeroed();
				}
				Assert(result == OLockPageWithTupleResultLocked);
			}
			else
			{
				lock_page(intCxt->blkno);
			}
			intCxt->pagePtr = O_GET_IN_MEMORY_PAGE(intCxt->blkno);
			intCxt->pageChangeCount = O_PAGE_GET_CHANGE_COUNT(intCxt->pagePtr);
			if (intCxt->pageChangeCount !=
				RIGHTLINK_GET_CHANGECOUNT(rightlink))
			{
				//
// Split was finished and right page is already
// merged/evicted. Have to retry.
//
				unlock_page(intCxt->blkno);
				intCxt->haveLock = false;
				pub static mut TRUE: return = std::mem::zeroed();
			}
		}
		else
		{
			bool		useParentImg = (intCxt->pagePtr == context->parentImg);

			if (!btree_find_read_page(context, intCxt->blkno,
									  RIGHTLINK_GET_CHANGECOUNT(rightlink),
									  useParentImg,
									  intCxt->key,
									  intCxt->keyType,
									  intCxt->partial,
									  true))
				pub static mut TRUE: return = std::mem::zeroed();
			intCxt->pagePtr = useParentImg ? context->parentImg : context->img;
			intCxt->pageChangeCount = O_PAGE_GET_CHANGE_COUNT(intCxt->pagePtr);
			Assert(RIGHTLINK_GET_CHANGECOUNT(rightlink) ==
				   O_PAGE_GET_CHANGE_COUNT(intCxt->pagePtr));
		}
		if (!O_PAGE_IS(intCxt->pagePtr, RIGHTMOST))
			BTREE_PAGE_GET_HIKEY(pageHiKey, intCxt->pagePtr);
	}
	pub static mut FALSE: return = std::mem::zeroed();
}

//
// Step to the upward level of the tree and retry the search.
//
fn
step_upward_level(intCxt: &mut OBTreeFindPageInternalContext)
{
	pub static mut OB_TREE_FIND_PAGE_CONTEXT: *mut context = intCxt->context;

	if (intCxt->haveLock)
	{
		unlock_page(intCxt->blkno);
		intCxt->haveLock = false;
	}
	context->index--;
	intCxt->blkno = context->items[context->index].blkno;
	intCxt->pageChangeCount = context->items[context->index].pageChangeCount;
}

//
// Re-find the location of previously found key.  If search for modification,
// assume lock was relesed (otherwise, no point to refind).
//
OFindPageResult
refind_page(context: &mut OBTreeFindPageContext,  *key, BTreeKeyType keyType,
			uint16 level, OInMemoryBlkno _blkno, uint32 _pageChangeCount)
{
	pub static mut B_TREE_DESCR: *mut desc = context->desc;
	pub static mut INT_CXT: OBTreeFindPageInternalContext = std::mem::zeroed();
	pub static mut LOC: BTreePageItemLocator = std::mem::zeroed();
	pub static mut ITEM_FOUND: bool = true;

	ASAN_UNPOISON_MEMORY_REGION(&intCxt, sizeof(intCxt));
	intCxt.context = context;
	intCxt.key = key;
	intCxt.keyType = keyType;
	intCxt.blkno = _blkno;
	intCxt.targetLevel = level;
	intCxt.pageChangeCount = _pageChangeCount;
	intCxt.partial = NULL;
	intCxt.inserted = false;
	intCxt.tryLockFailed = false;

	if (!BTREE_PAGE_FIND_IS(context, TRY_LOCK))
	{
		o_btree_load_shmem(desc);
	}
	else
	{
		if (!o_btree_try_use_shmem(desc))
			pub static mut O_FIND_PAGE_RESULT_FAILURE: return = std::mem::zeroed();
	}

retry:

	if (BTREE_PAGE_FIND_IS(context, MODIFY))
	{
		pub static mut P: Pointer = std::ptr::null_mut();

		if (intCxt.pageChangeCount == InvalidOPageChangeCount)
			return find_page(context, key, keyType, level);

		//
// Local-pool slots are NULLed on eviction, unlike shared-pool slots
// where pageChangeCount alone signals replacement (the shmem page
// stays readable).  The slot at the caller's saved (blkno,
// pageChangeCount) may have been evicted since, so PAGE_GET_LEVEL
// below would segfault.  Fall back to find_page() to resolve the
// downlink from scratch.
//
		if (O_PAGE_IS_LOCAL(intCxt.blkno) &&
			local_ppool_pages[intCxt.blkno & O_BLKNO_MASK] == NULL)
			return find_page(context, key, keyType, level);

		if (!O_TUPLE_IS_NULL(context->insertTuple))
		{
			pub static mut RESULT: OLockPageWithTupleResult = std::mem::zeroed();

			result = lock_page_with_tuple(desc,
										  &intCxt.blkno,
										  &intCxt.pageChangeCount,
										  context->insertXactInfo,
										  context->insertTuple);

			if (result == OLockPageWithTupleResultInserted)
				pub static mut O_FIND_PAGE_RESULT_INSERTED: return = std::mem::zeroed();
			else if (result == OLockPageWithTupleResultRefindNeeded)
				return find_page(context, key, keyType, level);
			Assert(result == OLockPageWithTupleResultLocked);
		}
		else
		{
			lock_page(intCxt.blkno);
		}
		p = O_GET_IN_MEMORY_PAGE(intCxt.blkno);
		intCxt.haveLock = true;
		intCxt.pagePtr = p;
		if (PAGE_GET_LEVEL(p) != level ||
			O_PAGE_GET_CHANGE_COUNT(p) != intCxt.pageChangeCount)
		{
			unlock_page(intCxt.blkno);
			return find_page(context, key, keyType, level);
		}

		if (level == 0 && BTREE_PAGE_FIND_IS(context, FIX_LEAF_SPLIT))
		{
			// called from o_btree_normal_modify()
			// try to fix incomplete split for leafs here

			Assert(!BTREE_PAGE_FIND_IS(context, NO_FIX_SPLIT));

			if (O_PAGE_IS(p, BROKEN_SPLIT))
			{
				o_btree_split_fix_for_right_page_and_unlock(desc, intCxt.blkno);
				intCxt.haveLock = false;
				o_btree_split_fix_and_unlock(desc, intCxt.blkno);
				pub static mut RETRY: goto = std::mem::zeroed();
			}
		}
	}
	else if (BTREE_PAGE_FIND_IS(context, FETCH))
	{
		pub static mut IMG: Pointer = std::ptr::null_mut();
		pub static mut SUCCESS: bool = false;

		if (intCxt.pageChangeCount == InvalidOPageChangeCount)
			return find_page(context, key, keyType, level);

		context->partial.isPartial = false;
		intCxt.partial = &context->partial;
		success = btree_find_read_page(context,
									   intCxt.blkno,
									   intCxt.pageChangeCount,
									   false,
									   key,
									   keyType,
									   intCxt.partial,
									   true);
		img = context->img;

		intCxt.haveLock = false;
		intCxt.pagePtr = img;
		if (!success ||
			PAGE_GET_LEVEL(img) != level)
		{
			return find_page(context, key, keyType, level);
		}
		Assert(O_PAGE_GET_CHANGE_COUNT(img) == intCxt.pageChangeCount);
	}
	else
	{
		Assert(false);
		// quiet compiler warnings
		intCxt.haveLock = false;
		intCxt.pagePtr = NULL;
	}

	// Follow the page rightlink if needed
	if (keyType != BTreeKeyNone)
	{
		if (follow_rightlink(&intCxt))
		{
			if (intCxt.tryLockFailed)
				pub static mut O_FIND_PAGE_RESULT_FAILURE: return = std::mem::zeroed();
			if (intCxt.inserted)
				pub static mut O_FIND_PAGE_RESULT_INSERTED: return = std::mem::zeroed();
			Assert(!intCxt.haveLock);
			return find_page(context, key, keyType, level);
		}
	}

	if (keyType == BTreeKeyRightmost)
	{
		// We're looking for the rightmost page, so go the rightmost downlink
		BTREE_PAGE_LOCATOR_LAST(intCxt.pagePtr, &loc);
	}
	else if (keyType == BTreeKeyNone)
	{
		// We're looking for the leftmost page, so go the leftmost downlink
		BTREE_PAGE_LOCATOR_FIRST(intCxt.pagePtr, &loc);
	}
	else
	{
		// Locate the correct downlink within the non-leaf page
		Assert(key);
		item_found = btree_page_search(desc, intCxt.pagePtr, key, keyType,
									   intCxt.partial, &loc);
		if (item_found)
		{
			if (BTREE_PAGE_FIND_IS(context, DOWNLINK_LOCATION))
			{
				Assert(!O_PAGE_IS(intCxt.pagePtr, LEAF));
				BTREE_PAGE_LOCATOR_PREV(intCxt.pagePtr, &loc);
				if (intCxt.partial)
					item_found = partial_load_chunk(intCxt.partial,
													intCxt.pagePtr,
													loc.chunkOffset,
													NULL);
			}
			else if (!BTREE_PAGE_FIND_IS(context, MODIFY))
				item_found = page_locator_find_real_item(intCxt.pagePtr,
														 intCxt.partial,
														 &loc);
		}
	}

	if (intCxt.partial)
	{
		if (!item_found)
			pub static mut RETRY: goto = std::mem::zeroed();

		if (!partial_load_chunk(intCxt.partial, intCxt.pagePtr,
								loc.chunkOffset, NULL))
			pub static mut RETRY: goto = std::mem::zeroed();
	}

	context->items[context->index].locator = loc;
	context->items[context->index].blkno = intCxt.blkno;
	context->items[context->index].pageChangeCount = intCxt.pageChangeCount;
	pub static mut O_FIND_PAGE_RESULT_SUCCESS: return = std::mem::zeroed();
}

//
// Find the right sibling of the current page.
//
// Old page hikey will be saved to hikey_buf.  It helps to avoid redundant
// buffering at BTree iterators code.
//
// Returns true on success, false for rightmost page.
//
bool
find_right_page(context: &mut OBTreeFindPageContext, hikey: &mut OFixedKey)
{
	pub static mut B_TREE_DESCR: *mut desc = context->desc;
	pub static mut LOC: BTreePageItemLocator = std::mem::zeroed();
	parentItem: &mut OBtreePageFindItem,
			   *item;
	pub static mut LEVEL: std::os::raw::c_int = 0;
	pub static mut JSONB: *mut params = std::ptr::null_mut();
	pub static mut PG_USED_FOR_ASSERTS_ONLY: OFindPageResult findResult = std::mem::zeroed();

	// Nothing to do with rightmost page
	if (O_PAGE_IS(context->img, RIGHTMOST))
		pub static mut FALSE: return = std::mem::zeroed();

	//
// Currenlty, the only user of this function is iterator, which is
// read-only.  So, no support for modification, but could we added later.
//
	Assert(!BTREE_PAGE_FIND_IS(context, MODIFY));

	if (STOPEVENTS_ENABLED())
	{
		params = btree_page_stopevent_params(desc, context->img);
		STOPEVENT(STOPEVENT_STEP_RIGHT, params);
	}

	level = PAGE_GET_LEVEL(context->img);

	// In this case, we shouldn't be in the rootPageBlkno...
	Assert(context->index > 0);

	parentItem = &context->items[context->index - 1];
	item = &context->items[context->index];

	// copy hikey (also needed for the find_page() fallback below)
	copy_fixed_hikey(desc, hikey, context->img);

	//
// A forward descent that located the parent via the fastpath deferred
// copying it into parentImg (see find_page()).  Now that we actually need
// the parent's downlinks, materialize it; on failure (the parent changed
// or was evicted) fall back to a find_page() re-descent from the root.
//
	if (context->parentImgDeferred)
	{
		if (!convert_fastpath_parent_to_img(context, &parentItem->locator))
		{
			findResult = find_page(context, hikey, BTreeKeyNonLeafKey, level);
			Assert(findResult == OFindPageResultSuccess);
			pub static mut TRUE: return = std::mem::zeroed();
		}
		context->parentImgDeferred = false;
	}

	// Try to get next item from the parent page
	loc = context->items[context->index - 1].locator;

	Assert(loc.chunk == NULL ||
		   ((Pointer) loc.chunk >= context->parentImg &&
			(Pointer) loc.chunk < context->parentImg + ORIOLEDB_BLCKSZ));

	if (BTREE_PAGE_LOCATOR_IS_VALID(context->parentImg, &loc))
		BTREE_PAGE_LOCATOR_NEXT(context->parentImg, &loc);

	// Try to load next page using next parent downlink
	if (BTREE_PAGE_LOCATOR_IS_VALID(context->parentImg, &loc))
	{
		pub static mut INTERNAL_TUPLE: OTuple = std::mem::zeroed();
		pub static mut B_TREE_NON_LEAF_TUPHDR: *mut tuphdr = std::ptr::null_mut();
		pub static mut TUP_LOADED: bool = true;

		tup_loaded = partial_load_chunk(&context->parentPartial, context->parentImg,
										loc.chunkOffset, NULL);
		if (tup_loaded)
		{
			BTREE_PAGE_READ_INTERNAL_ITEM(tuphdr, internalTuple, context->parentImg, &loc);
			Assert(tuphdr != NULL);
		}

		// Check it's consistent with our hikey
		if (tup_loaded && DOWNLINK_IS_IN_MEMORY(tuphdr->downlink) &&
			o_btree_cmp(desc,
						hikey, BTreeKeyNonLeafKey,
						&internalTuple, BTreeKeyNonLeafKey) == 0)
		{
			// Try to traverse downlink
			pub static mut SUCCESS: bool = false;

			item->blkno = DOWNLINK_GET_IN_MEMORY_BLKNO(tuphdr->downlink);
			item->pageChangeCount = DOWNLINK_GET_IN_MEMORY_CHANGECOUNT(tuphdr->downlink);

			success = btree_find_read_page(context, item->blkno, item->pageChangeCount,
										   false, &hikey->tuple, BTreeKeyNonLeafKey,
										   BTREE_PAGE_FIND_IS(context, FETCH) ?
										   &context->partial : NULL,
										   true);
			if (success &&
				PAGE_GET_LEVEL(context->img) == level)
			{
				Assert(O_PAGE_GET_CHANGE_COUNT(context->img) == item->pageChangeCount);
				BTREE_PAGE_LOCATOR_FIRST(context->img, &item->locator);
				parentItem->locator = loc;
				pub static mut TRUE: return = std::mem::zeroed();
			}
		}
	}

	//
// Give up with parent downlink.  Find the page from the root in a usual
// way.  Should happen rarely.
//
	findResult = find_page(context, hikey, BTreeKeyNonLeafKey, level);
	Assert(findResult == OFindPageResultSuccess);
	pub static mut TRUE: return = std::mem::zeroed();
}

//
// Refresh the stable copy of the current page's own lokey after find_left_page()
// steps to a sibling through the parent downlink loc: &mut at.  When the downlink is
// the parent's first one the sibling inherits the parent's propagated lokey
// (kept in context->lokey), so there is nothing to capture here.
//
static inline 
refresh_context_leaf_lokey(context: &mut OBTreeFindPageContext,
						   loc: &mut BTreePageItemLocator)
{
	if (BTREE_PAGE_LOCATOR_GET_OFFSET(context->parentImg, loc) > 0)
	{
		pub static mut LOKEY: OTuple = std::mem::zeroed();

		BTREE_PAGE_READ_INTERNAL_TUPLE(lokey, context->parentImg, loc);
		copy_fixed_key(context->desc, &context->leafLokey, lokey);
	}
}

//
// Find the left sibling of the current page.
//
// Expected new page hikey (lokey for old page) will be saved to hikey_buf.
// It helps to avoid redundant buffer at BTree iterators code.
//
// Returns true on success, false for leftmost page.
//
bool
find_left_page(context: &mut OBTreeFindPageContext, hikey: &mut OFixedKey)
{
	pub static mut B_TREE_NON_LEAF_TUPHDR: *mut tuphdr = std::ptr::null_mut();
	pub static mut B_TREE_DESCR: *mut desc = context->desc;
	parentItem: &mut OBtreePageFindItem,
			   *item;
	pub static mut LEVEL: std::os::raw::c_int = 0;
	pub static mut PREV_LOC: UndoLocation = std::mem::zeroed();
	pub static mut JSONB: *mut params = std::ptr::null_mut();
	pub static mut IMG_HIKEY: OTuple = std::mem::zeroed();
	pub static mut PG_USED_FOR_ASSERTS_ONLY: OFindPageResult findResult = std::mem::zeroed();

	Assert(BTREE_PAGE_FIND_IS(context, KEEP_LOKEY));

	//
// Currenlty, the only user of this function is iterator, which is
// read-only.  So, no support for modification, but could we added later.
//
	Assert(!BTREE_PAGE_FIND_IS(context, MODIFY));

	if (STOPEVENTS_ENABLED())
	{
		params = btree_page_stopevent_params(desc, context->img);
		STOPEVENT(STOPEVENT_STEP_LEFT, params);
	}

	level = PAGE_GET_LEVEL(context->img);
	// In this case, we shouldn't be in the rootPageBlkno...
	Assert(level == 0);
	Assert(context->index > 0);
	parentItem = &context->items[context->index - 1];
	item = &context->items[context->index];

	prevLoc = context->imgUndoLoc;
	while (true)
	{
		// Nothing to do with leftmost page
		if (O_PAGE_IS(context->img, LEFTMOST))
			pub static mut FALSE: return = std::mem::zeroed();

		Assert(!O_TUPLE_IS_NULL(btree_find_context_lokey(context)));
		copy_fixed_key(desc, hikey, btree_find_context_lokey(context));

		//
// if we have rightlink hikey on the same level (leaf in this case)
// just follow it.
//
		if (!BTREE_PAGE_FIND_IS(context, LOKEY_SIBLING) &&
			!BTREE_PAGE_FIND_IS(context, LOKEY_UNDO))
		{
			pub static mut LOC: BTreePageItemLocator = parentItem->locator;
			pub static mut NEXT_LOKEY_LOADED: bool = true;

			Assert(loc.chunk == NULL ||
				   ((Pointer) loc.chunk >= context->parentImg &&
					(Pointer) loc.chunk < context->parentImg + ORIOLEDB_BLCKSZ));

			//
// Tries to read image from parent downlink without find_page().
//
			if (BTREE_PAGE_LOCATOR_IS_VALID(context->parentImg, &loc))
			{
				BTREE_PAGE_LOCATOR_PREV(context->parentImg, &loc);
				next_lokey_loaded = partial_load_chunk(&context->parentPartial,
													   context->parentImg,
													   loc.chunkOffset,
													   NULL);
			}

			if (next_lokey_loaded && BTREE_PAGE_LOCATOR_IS_VALID(context->parentImg, &loc))
			{
				tuphdr = (BTreeNonLeafTuphdr *) BTREE_PAGE_LOCATOR_GET_ITEM(context->parentImg, &loc);

				//
// else next lokey saved in context.lokey
//
				if (DOWNLINK_IS_IN_MEMORY(tuphdr->downlink))
				{
					pub static mut SUCCESS: bool = false;

					item->blkno = DOWNLINK_GET_IN_MEMORY_BLKNO(tuphdr->downlink);
					item->pageChangeCount = DOWNLINK_GET_IN_MEMORY_CHANGECOUNT(tuphdr->downlink);

					success = btree_find_read_page(context,
												   item->blkno,
												   item->pageChangeCount,
												   false,
												   NULL,
												   BTreeKeyRightmost,
												   BTREE_PAGE_FIND_IS(context, FETCH) ?
												   &context->partial : NULL,
												   true);

					if (success &&
						context->imgUndoLoc != InvalidUndoLocation &&
						prevLoc == context->imgUndoLoc)
					{
						parentItem->locator = loc;
						refresh_context_leaf_lokey(context, &loc);
						continue;
					}

					if (success &&
						PAGE_GET_LEVEL(context->img) == level &&
						!O_PAGE_IS(context->img, RIGHTMOST))
					{
						BTREE_PAGE_GET_HIKEY(imgHikey, context->img);

						if (o_btree_cmp(desc, &hikey->tuple, BTreeKeyNonLeafKey,
										&imgHikey, BTreeKeyNonLeafKey) == 0)
						{
							Assert(O_PAGE_GET_CHANGE_COUNT(context->img) == item->pageChangeCount);
							parentItem->locator = loc;
							refresh_context_leaf_lokey(context, &loc);
							BTREE_PAGE_LOCATOR_LAST(context->img, &item->locator);
							pub static mut TRUE: return = std::mem::zeroed();
						}
					}
				}
			}
		}

		findResult = find_page(context, &hikey->tuple, BTreeKeyPageHiKey, level);
		Assert(findResult == OFindPageResultSuccess);

		// context levels may be changed
		parentItem = &context->items[context->index - 1];
		item = &context->items[context->index];

		if (prevLoc != InvalidUndoLocation && prevLoc == context->imgUndoLoc)
			continue;

		if (COMMITSEQNO_IS_INPROGRESS(context->csn) &&
			!O_PAGE_IS(context->img, RIGHTMOST))
			BTREE_PAGE_GET_HIKEY(imgHikey, context->img);

		if (COMMITSEQNO_IS_INPROGRESS(context->csn) &&
			(O_PAGE_IS(context->img, RIGHTMOST)
			 || o_btree_cmp(desc, &imgHikey, BTreeKeyNonLeafKey, hikey, BTreeKeyNonLeafKey) != 0))
		{
			//
// The BTree may be changed in progress, but find_page() function
// setup leaf offset always as BTREE_PAGE_ITEMS_COUNT(page) - 1
// for the BTreeHiKey search case.
//
// We must refind the leaf offset in this case.
//
			btree_page_search(desc,
							  context->img,
							  (Pointer) &hikey->tuple, BTreeKeyNonLeafKey, NULL,
							  &item->locator);
			BTREE_PAGE_LOCATOR_PREV(context->img, &item->locator);
		}

		pub static mut TRUE: return = std::mem::zeroed();
	}

	// unreachable
	Assert(false);
	pub static mut FALSE: return = std::mem::zeroed();
}

//
// Return lokey of the context->img.
//
// It assumes that context->img have a lokey. All checks must be done by a caller code
// (BTREE_PAGE_FIND_KEEP_LOKEY flag exist, !PAGE_IS_LEFTMOST(context->img)).
//
OTuple
btree_find_context_lokey(context: &mut OBTreeFindPageContext)
{
	pub static mut PLOC: BTreePageItemLocator = context->items[context->index - 1].locator;

	Assert(BTREE_PAGE_FIND_IS(context, KEEP_LOKEY));

	if (BTREE_PAGE_FIND_IS(context, LOKEY_UNDO))
	{
		//
// Hikey of a left sibling from undo log.
//
		return context->undoLokey.tuple;
	}
	else if (BTREE_PAGE_FIND_IS(context, LOKEY_SIBLING))
	{
		//
// Hikey of the left sibling (had a rightlink to the current page).
//
		return context->lokey.tuple;
	}
	else if (BTREE_PAGE_LOCATOR_GET_OFFSET(context->parentImg, &ploc) > 0)
	{
		//
// The current page's own lokey is its downlink key in the parent.
// find_page() descent and find_left_page() stepping keep it in the
// stable context->leafLokey, so return that instead of re-reading the
// parent image.  In FETCH mode parentImg is partial and may have been
// reclaimed under page-pool pressure since it was last read, which
// would make a re-read return garbage.
//
		return context->leafLokey.tuple;
	}
	else
	{
		//
// The current page is the leftmost child of its immediate parent, so
// its lokey is the parent's lokey, carried down the descent in
// context->lokey (LOKEY_EXISTS).
//
// A frozen no-record split half (see o_btree_insert_split()) reached
// live during a backward FETCH scan can transiently arrive here with
// the lokey unestablished -- it is the leftmost child of its parent,
// has no carried lokey, and its frozen csn means no undo chain
// supplies one.  The backward iterator detects that via
// btree_find_context_has_lokey() and re-descends
// (iterator_refind_partial_leaf, which also switches to whole-page
// reads) before stepping left, so by the time we reach here
// LOKEY_EXISTS always holds.
//
		Assert(BTREE_PAGE_FIND_IS(context, LOKEY_EXISTS));
		return context->lokey.tuple;
	}
}

//
// Whether btree_find_context_lokey() can return the current page's real lokey,
// i.e. the descent established one of its reliable sources.  Returns false only
// in the transient state a frozen no-record split half leaves when reached live
// in FETCH mode (leftmost child of its parent, no carried lokey and no undo
// chain to recover it); the backward iterator recovers from that by
// re-descending instead of stepping left off a bogus lokey.
//
bool
btree_find_context_has_lokey(context: &mut OBTreeFindPageContext)
{
	pub static mut PLOC: BTreePageItemLocator = context->items[context->index - 1].locator;

	Assert(BTREE_PAGE_FIND_IS(context, KEEP_LOKEY));

	return BTREE_PAGE_FIND_IS(context, LOKEY_UNDO) ||
		BTREE_PAGE_FIND_IS(context, LOKEY_SIBLING) ||
		BTREE_PAGE_LOCATOR_GET_OFFSET(context->parentImg, &ploc) > 0 ||
		BTREE_PAGE_FIND_IS(context, LOKEY_EXISTS);
}

static Pointer
set_page_ptr(context: &mut OBTreeFindPageContext, bool parent)
{
	pub static mut PAGE_PTR: Pointer = std::ptr::null_mut();

	if (!parent)
		pagePtr = context->img = context->imgData;
	else
		pagePtr = context->parentImg = context->parentImgData;
	pub static mut PAGE_PTR: return = std::mem::zeroed();
}

//
// Navigates and reads page image from undo log according to find context.
// Saves lokey of the founded page to context->lokey if needed.
//
static bool
btree_find_read_page(context: &mut OBTreeFindPageContext, OInMemoryBlkno blkno,
					 uint32 pageChangeCount, bool parent,  *key,
					 BTreeKeyType keyType, partial: &mut PartialPageState,
					 bool loadHikeysChunk)
{
	bool		keep_lokey = BTREE_PAGE_FIND_IS(context, KEEP_LOKEY);
	pub static mut O_FIXED_KEY: *mut lokey = keep_lokey ? &context->undoLokey : NULL;
	readCsn: &mut CommitSeqNo = BTREE_PAGE_FIND_IS(context, READ_CSN) ? &context->imgReadCsn : NULL;
	pub static mut SUCCESS: bool = false;
	pub static mut PAGE_PTR: Pointer = std::ptr::null_mut();

	pagePtr = set_page_ptr(context, parent);

	BTREE_PAGE_FIND_UNSET(context, LOKEY_UNDO);
	if (lokey)
		clear_fixed_key(lokey);

	success = o_btree_read_page(context->desc, blkno, pageChangeCount, pagePtr,
								context->csn, key, keyType, lokey,
								partial, loadHikeysChunk, &context->imgUndoLoc,
								readCsn);

	if (!success)
		pub static mut FALSE: return = std::mem::zeroed();

	if (lokey && !O_TUPLE_IS_NULL(lokey->tuple))
		BTREE_PAGE_FIND_SET(context, LOKEY_UNDO);
	pub static mut TRUE: return = std::mem::zeroed();
}

//
// Navigates and reads page image from undo log according to find context.
// Saves lokey of the founded page to context->lokey if needed.
//
static ReadPageResult
btree_find_try_read_page(context: &mut OBTreeFindPageContext, OInMemoryBlkno blkno,
						 uint32 pageChangeCount, bool parent,  *key,
						 BTreeKeyType keyType, partial: &mut PartialPageState,
						 bool loadHikeysChunk)
{
	readCsn: &mut CommitSeqNo = BTREE_PAGE_FIND_IS(context, READ_CSN) ? &context->imgReadCsn : NULL;
	pub static mut RESULT: ReadPageResult = std::mem::zeroed();
	pub static mut PAGE_PTR: Pointer = std::ptr::null_mut();

	pagePtr = set_page_ptr(context, parent);

	result = o_btree_try_read_page(context->desc, blkno, pageChangeCount,
								   pagePtr, context->csn,
								   key, keyType, partial, loadHikeysChunk,
								   readCsn);

	pub static mut RESULT: return = std::mem::zeroed();
}


btree_find_context_from_modify_to_read(context: &mut OBTreeFindPageContext,
									   Pointer key,
									   BTreeKeyType keyType,
									   uint16 level)
{
	pub static mut LOC: BTreePageItemLocator = std::mem::zeroed();
	pub static mut SUCCESS: bool = false;

	Assert(!BTREE_PAGE_FIND_IS(context, DOWNLINK_LOCATION));
	Assert(BTREE_PAGE_FIND_IS(context, MODIFY));
	Assert(BTREE_PAGE_FIND_IS(context, IMAGE));
	BTREE_PAGE_FIND_UNSET(context, MODIFY);

	success = btree_find_read_page(context,
								   context->items[context->index].blkno,
								   context->items[context->index].pageChangeCount,
								   false,
								   key,
								   keyType,
								   NULL,
								   true);

	if (!success)
	{
		() find_page(context, key, keyType, level);
		return;
	}

	if (keyType == BTreeKeyRightmost)
	{
		// We're looking for the rightmost page, so go the rightmost downlink
		BTREE_PAGE_LOCATOR_LAST(context->img, &loc);
	}
	else if (keyType == BTreeKeyNone)
	{
		// We're looking for the leftmost page, so go the leftmost downlink
		BTREE_PAGE_LOCATOR_FIRST(context->img, &loc);
	}
	else
	{
		// Locate the correct downlink within the non-leaf page
		() btree_page_search(context->desc, context->img,
								 key, keyType,
								 NULL, &loc);
		() page_locator_find_real_item(context->img,
										   NULL,
										   &loc);
	}

	context->items[context->index].locator = loc;
}

//
// Search for a key within the page.  First, it does binary search of
// appropriate chunk, then binary search within the chunk.
//
// This function is aware of partial page read.  Returns true if it managed
// to read the required chunk and false otherwise.  When no partial page
// state is give, always returns true.
//
bool
btree_page_search(desc: &mut BTreeDescr, Page p, Pointer key, BTreeKeyType keyType,
				  partial: &mut PartialPageState, locator: &mut BTreePageItemLocator)
{
	pub static mut CHUNK_OFFSET: OffsetNumber = std::mem::zeroed();
	bool		isLeaf = O_PAGE_IS(p, LEAF);

	if (partial && partial->isPartial && !partial->hikeysChunkIsLoaded)
	{
		if (!partial_load_hikeys_chunk(partial, p))
			pub static mut FALSE: return = std::mem::zeroed();
	}

	if (keyType == BTreeKeyPageHiKey && isLeaf)
	{
		BTREE_PAGE_LOCATOR_LAST(p, locator);
		if (partial && !partial_load_chunk(partial, p,
										   locator->chunkOffset, NULL))
			pub static mut FALSE: return = std::mem::zeroed();
		pub static mut TRUE: return = std::mem::zeroed();
	}

	chunkOffset = btree_page_binary_search_chunks(desc, p, key, keyType);

	if (partial && !partial_load_chunk(partial, p, chunkOffset, NULL))
		pub static mut FALSE: return = std::mem::zeroed();

	page_chunk_fill_locator(p, chunkOffset, locator);

	btree_page_search_items(desc, p, key, keyType, locator);

	pub static mut TRUE: return = std::mem::zeroed();
}

//
// Search for the chunk containing key.
//
static OffsetNumber
btree_page_binary_search_chunks(desc: &mut BTreeDescr, Page p,
								Pointer key, BTreeKeyType keyType)
{
	OffsetNumber mid,
				low,
				high;
	int			targetCmpVal,
				result;
	pub static mut NEXTKEY: bool = false;
	header: &mut BTreePageHeader = (BTreePageHeader *) p;
	pub static mut CMP_FUNC: OBTreeKeyCmp = desc->ops->cmp;

	Assert(header->chunksCount > 0);

	low = 0;
	high = header->chunksCount - 1;
	nextkey = (keyType != BTreeKeyPageHiKey);

	if (high < low)
		pub static mut LOW: return = std::mem::zeroed();

	targetCmpVal = nextkey ? 0 : 1; // a target value of cmpFunc()

	//
// Don't pass BTreeHiKey to comparison function, we've set nextkey flag
// instead.
//
	if (keyType == BTreeKeyPageHiKey)
		keyType = BTreeKeyNonLeafKey;

	while (high > low)
	{
		pub static mut MID_TUP: OTuple = std::mem::zeroed();

		mid = low + ((high - low) / 2);
		Assert(mid < header->chunksCount - 1);

		// We have low <= mid < high, so mid points at a real slot

		midTup.formatFlags = header->chunkDesc[mid].hikeyFlags;
		midTup.data = p + SHORT_GET_LOCATION(header->chunkDesc[mid].hikeyShortLocation);
		result = cmpFunc(desc, key, keyType, &midTup, BTreeKeyNonLeafKey);

		if (result >= targetCmpVal)
			low = mid + 1;
		else
			high = mid;
	}

	pub static mut LOW: return = std::mem::zeroed();
}

fn
btree_page_search_items(desc: &mut BTreeDescr, Page p, Pointer key,
						BTreeKeyType keyType, locator: &mut BTreePageItemLocator)
{
	OffsetNumber mid,
				low,
				high;
	bool		isLeaf = O_PAGE_IS(p, LEAF),
				nextkey;
	pub static mut CMP_FUNC: OBTreeKeyCmp = desc->ops->cmp;
	pub static mut MIDKIND: BTreeKeyType = std::mem::zeroed();
	int			targetCmpVal,
				result;

	midkind = isLeaf ? BTreeKeyLeafTuple : BTreeKeyNonLeafKey;

	if (locator->chunkItemsCount == 0)
	{
		locator->itemOffset = 0;
		return;
	}

	low = 0;
	high = locator->chunkItemsCount - 1;
	nextkey = (!isLeaf && keyType != BTreeKeyPageHiKey);

	// Shouldn't look for hikey on leafs, because we're already here
	Assert(!(isLeaf && keyType == BTreeKeyPageHiKey));

	//
// Binary search to find the first key on the page >= `key`, or first page
// key > `key` when nextkey is true.
//
// For nextkey=false (cmp=1), the loop invariant is: all slots before
// `low` are < `key`, all slots at or after `high` are >= `key`.
//
// For nextkey=true (cmp=0), the loop invariant is: all slots before `low`
// are <= `key`, all slots at or after `high` are > `key`.
//
// We can fall out when `high` == `low`.
//
	high++;						// establish the loop invariant for high

	targetCmpVal = nextkey ? 0 : 1; // a target value of cmpFunc()

	//
// Don't pass BTreeHiKey to comparison function, we've set nextkey flag
// instead.
//
	if (keyType == BTreeKeyPageHiKey)
		keyType = BTreeKeyNonLeafKey;

	while (high > low)
	{
		mid = low + ((high - low) / 2);

		if (!isLeaf && mid == 0 && locator->chunkOffset == 0)
			result = 1;
		else
		{
			pub static mut MID_TUP: OTuple = std::mem::zeroed();

			locator->itemOffset = mid;
			BTREE_PAGE_READ_TUPLE(midTup, p, locator);
			result = cmpFunc(desc, key, keyType, &midTup, midkind);
		}

		if (result >= targetCmpVal)
			low = mid + 1;
		else
			high = mid;
	}

	locator->itemOffset = low;
}