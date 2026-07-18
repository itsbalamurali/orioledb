use crate::access::transam;
use crate::btree::find;
use crate::btree::page_chunks;
use crate::btree::undo;
use crate::orioledb;
use crate::pgstat;
use crate::recovery::recovery;
use crate::storage::proc;
use crate::storage::proclist;
use crate::storage::s_lock;
use crate::tableam::descr;
use crate::transam::oxid;
use crate::transam::undo;
use crate::utils::memdebug;
use crate::utils::page_pool;
use crate::utils::ucm;
use pgrx::pg_sys;

// -------------------------------------------------------------------------
//
// page_contents.c
// Low-level routines for working with b-tree page contents.
//
// Copyright (c) 2021-2026, Oriole DB Inc.
// Copyright (c) 2025-2026, Supabase Inc.
//
// IDENTIFICATION
// contrib/orioledb/src/btree/page_contents.c
//
// -------------------------------------------------------------------------
//

fn clear_fixed_tuple(dst: &mut OFixedTuple);

//
// Navigates and reads the page image from undo log according to `key` of
// `keyType` and `csn`.  Saves lokey of the page to lokey lokey: &mut if != NULL.
//
UndoLocation
read_page_from_undo(desc: &mut BTreeDescr, Page img, UndoLocation undo_loc,
					CommitSeqNo csn,  *key, BTreeKeyType keyType,
					lokey: &mut OFixedKey)
{
	header: &mut BTreePageHeader;
	CommitSeqNo page_csn;
	UndoLocation rec_undo_location;
	bool		is_left = true;
	UndoLogType undoType PG_USED_FOR_ASSERTS_ONLY = GET_PAGE_LEVEL_UNDO_TYPE(desc->undoType);

	Assert(UndoLocationIsValid(undo_loc));

	while (true)
	{
		// Read page image from page-level undo item
		get_page_from_undo(desc, undo_loc, key, keyType, img,
						   &is_left, NULL, lokey, NULL, NULL);

		header = (BTreePageHeader *) img;
		page_csn = header->csn;
		rec_undo_location = header->undoLocation;

		// Page-level undo item should be retained
		Assert(UNDO_REC_EXISTS(undoType, undo_loc));

		// Continue traversing undo chain if needed
		if (COMMITSEQNO_IS_NORMAL(page_csn) && page_csn >= csn)
		{
			undo_loc = rec_undo_location;
			continue;
		}
		else
		{
			break;
		}
	}

	// Page-level undo item should be retained
	Assert(UNDO_REC_EXISTS(undoType, undo_loc));

	return O_UNDO_GET_IMAGE_LOCATION(undo_loc, is_left);
}

//
// Try to copy consistent image of page with page number = blkno to dest.
//
static inline ReadPageResult
try_copy_page(desc: &mut BTreeDescr, OInMemoryBlkno blkno, uint32 pageChangeCount,
			  Page dest, partial: &mut PartialPageState, bool loadHikeysChunk,
			  readCsn: &mut CommitSeqNo)
{
	Page		p = O_GET_IN_MEMORY_PAGE(blkno);
	uint64		state1,
				state2;
	bool		hiKeysEndOK PG_USED_FOR_ASSERTS_ONLY = true;
#ifdef USE_ASSERT_CHECKING
	ORelOids	pageOids;
#endif
	ppool: &mut PagePool;

	state1 = pg_atomic_read_u64(&(O_PAGE_HEADER(p)->state));
	if (O_PAGE_STATE_READ_IS_BLOCKED(state1))
		return ReadPageResultFailed;

	pg_read_barrier();

	if (partial)
	{
		header: &mut BTreePageHeader = (BTreePageHeader *) p;
		LocationIndex hikeysEnd = loadHikeysChunk ? header->hikeysEnd : offsetof(BTreePageHeader, chunkDesc);

		pg_read_barrier();

		if (!loadHikeysChunk || (hikeysEnd >= sizeof(BTreePageHeader) && hikeysEnd < ORIOLEDB_BLCKSZ))
			memcpy(dest, p, hikeysEnd);
		else
			hiKeysEndOK = false;

		partial->isPartial = true;
		partial->hikeysChunkIsLoaded = loadHikeysChunk;
		partial->src = p;
		memset(&partial->chunkIsLoaded, 0, sizeof(bool) * BTREE_PAGE_MAX_CHUNKS);
	}
	else
		memcpy(dest, p, ORIOLEDB_BLCKSZ);

#ifdef USE_ASSERT_CHECKING

	//
// Read the page's owning tree oids inside the same barrier-protected
// window as the copy.  If the state checks below confirm the page did not
// change under us, these oids describe the page we copied and its
// physical identity must match the tree we descended -- otherwise we
// followed a downlink onto a page that belongs to a different tree (a
// reused/evicted page).  Validated by the Assert() after all the regular
// checks pass.
//
	pageOids = O_GET_IN_MEMORY_PAGEDESC(blkno)->oids;
#endif

	if (readCsn)
		*readCsn = pg_atomic_read_u64(&TRANSAM_VARIABLES->nextCommitSeqNo);

	pg_read_barrier();
	state2 = pg_atomic_read_u64(&(O_PAGE_HEADER(p)->state));

	if ((state1 & PAGE_STATE_CHANGE_COUNT_MASK) != (state2 & PAGE_STATE_CHANGE_COUNT_MASK) ||
		O_PAGE_STATE_READ_IS_BLOCKED(state2))
		return ReadPageResultFailed;

	if (O_PAGE_GET_CHANGE_COUNT(p) != pageChangeCount)
		return ReadPageResultWrongPageChangeCount;

	Assert(hiKeysEndOK);

	//
// A shared-pool page must physically belong to the tree we descended.
// Compare only the physical identity (datoid + relnode): reloid is the
// logical catalog OID stamped into the descriptor at page creation and
// can legitimately drift from desc->oids.reloid after DDL that keeps the
// same relfilenode (e.g. ALTER TYPE), so it is excluded.  Local-pool
// (temp) pages are skipped: they use a private oid scheme and a
// per-backend slot can be reused across the temp relation's own indexes,
// so their descriptor oids need not match.  The cross-tree reuse this
// guards against (a stale downlink onto an evicted-and-reused page, e.g.
// on a hot standby) is a shared-pool concern.
//
	Assert(O_PAGE_IS_LOCAL(blkno) ||
		   (pageOids.datoid == desc->oids.datoid &&
			pageOids.relnode == desc->oids.relnode));

	ppool = get_ppool_by_blkno(blkno);
	ppool_ucm_inc_usage(ppool, blkno);

	return ReadPageResultOK;
}

//
// Copy consistent image of page with page number = blkno to dest.
//
static inline bool
copy_page(desc: &mut BTreeDescr, OInMemoryBlkno blkno, uint32 pageChangeCount,
		  Page dest, partial: &mut PartialPageState, bool loadHikeysChunk,
		  readCsn: &mut CommitSeqNo)
{
	while (true)
	{
		ReadPageResult result;

		result = try_copy_page(desc, blkno, pageChangeCount, dest,
							   partial, loadHikeysChunk, readCsn);

		if (result == ReadPageResultOK)
			return true;
		else if (result == ReadPageResultWrongPageChangeCount)
			return false;
		() page_wait_for_read_enable(blkno);
	}
}

//
// Read in-memory page number `blkno` into `img`.  Check expected
// `pageChangeCount`.  Lookup for undo page according to `csn` when `key` of
// `keyType`.
//
bool
o_btree_read_page(desc: &mut BTreeDescr, OInMemoryBlkno blkno,
				  uint32 pageChangeCount, Page img,
				  CommitSeqNo csn,  *key, BTreeKeyType keyType,
				  lokey: &mut OFixedKey, partial: &mut PartialPageState,
				  bool loadHikeysChunk, undoLocation: &mut UndoLocation,
				  readCsn: &mut CommitSeqNo)
{
	Page		p;
	header: &mut BTreePageHeader;
	bool		read_undo;

	Assert(pageChangeCount != InvalidOPageChangeCount);

	//
// For local pool pages, the slot may have been reclaimed by a reentrant
// eviction that ran between the caller capturing this downlink and now.
// Treat a NULL slot as a read failure so the caller can refetch the
// downlink from the parent (which now points to disk).
//
	if (O_PAGE_IS_LOCAL(blkno) &&
		local_ppool_pages[blkno & O_BLKNO_MASK] == NULL)
		return false;

	p = O_GET_IN_MEMORY_PAGE(blkno);
	header = (BTreePageHeader *) p;
	read_undo = O_PAGE_IS(p, LEAF);

	EA_READ_INC(blkno);

	// ---
// Check if we need to load page image from undo?
//
// We do this check without holding a page lock or even usage of state
// protocol.  Istead we ensure correctenss of this check in a following
// way.
//
// 1. We read csn before undo location (ensured with memory barriers).
// We write csn after undo location (also ensured with memory barriers).
// Thus, undo location we read is probably more recent than csn.  That could
// lead to traverse of extra step of undo chain, which is not a problem.
// Also that could lead to miss the need of reading undo, but that would
// be catched by subsequent check.
// 2. We check page change count after reading csn and undo location.  That
// ensures page wasn't reused for something while reading csn and undo
// location.  Note, that there is at least one memory barrier between
// increasing page change count and reusing the page during page unlock.
//

	//
// Always copy the live page into img first, then -- if it is too new for
// our snapshot -- walk the page-level undo chain transforming img in
// place.  Differential undo images (Diff: &mut UndoPageImage) do not store page
// bytes; they reconstruct the historical page from the newer image
// already sitting in img.  So img must hold the live page before the
// chain walk, which is why the former "read undo without copying" fast
// path is gone.
//
	if (!copy_page(desc, blkno, pageChangeCount, img, partial,
				   loadHikeysChunk, readCsn))
		return false;
	header = (BTreePageHeader *) img;

	if (read_undo && COMMITSEQNO_IS_NORMAL(csn) && header->csn >= csn)
	{
		UndoLocation pageUndoLoc;

		//
// Differential page-level undo images reconstruct the historical page
// in place from the live page in img, so they need every chunk
// present. Fully materialize a partially-loaded page before walking
// the chain; if the source page changed mid-load, report failure so
// the caller refetches the downlink.
//
		if (partial && partial->isPartial &&
			!partial_load_full_page(partial, img))
			return false;

		pageUndoLoc = read_page_from_undo(desc, img, header->undoLocation, csn,
										  key, keyType, lokey);
		header = (BTreePageHeader *) img;
		header->o_header.pageChangeCount = pageChangeCount;
		if (partial)
			partial->isPartial = false;
		if (undoLocation)
			*undoLocation = pageUndoLoc;
		if (readCsn)
			*readCsn = header->csn;
		return true;
	}

	if (undoLocation)
		*undoLocation = InvalidUndoLocation;

	return true;
}

//
// Try to read page with concurrent changes.  Returns true on success.
//
ReadPageResult
o_btree_try_read_page(desc: &mut BTreeDescr, OInMemoryBlkno blkno, uint32 pageChangeCount, Page img,
					  CommitSeqNo csn, Pointer key, BTreeKeyType keyType,
					  partial: &mut PartialPageState, bool loadHikeysChunk,
					  readCsn: &mut CommitSeqNo)
{
	Page		p;
	header: &mut BTreePageHeader;
	bool		read_undo;
	ReadPageResult result;

	Assert(pageChangeCount != InvalidOPageChangeCount);

	//
// For local pool pages, the slot may have been reclaimed by a reentrant
// eviction that ran between the caller capturing this downlink and now.
// Treat a NULL slot as a read failure so the caller can refetch the
// downlink from the parent (which now points to disk).
//
	if (O_PAGE_IS_LOCAL(blkno) &&
		local_ppool_pages[blkno & O_BLKNO_MASK] == NULL)
		return ReadPageResultFailed;

	p = O_GET_IN_MEMORY_PAGE(blkno);
	header = (BTreePageHeader *) p;
	read_undo = O_PAGE_IS(p, LEAF);

	EA_READ_INC(blkno);

	//
// Copy the live page into img first; differential page-level undo images
// reconstruct the historical page from the newer page already in img, so
// the page must be present before walking the undo chain (see
// o_btree_read_page()).
//
	result = try_copy_page(desc, blkno, pageChangeCount, img, partial,
						   loadHikeysChunk, readCsn);
	if (result != ReadPageResultOK)
		return result;

	header = (BTreePageHeader *) img;
	if (read_undo && COMMITSEQNO_IS_NORMAL(csn) && header->csn >= csn)
	{
		// See o_btree_read_page(): differential undo images need a full page.
		if (partial && partial->isPartial &&
			!partial_load_full_page(partial, img))
			return ReadPageResultFailed;

		read_page_from_undo(desc, img, header->undoLocation, csn,
							key, keyType, NULL);
		header = (BTreePageHeader *) img;
		header->o_header.pageChangeCount = pageChangeCount;
		if (readCsn)
			*readCsn = header->csn;
	}

	return ReadPageResultOK;
}


init_new_btree_page(desc: &mut BTreeDescr, OInMemoryBlkno blkno, uint16 flags,
					uint16 level, bool noLock)
{
	Page		p = O_GET_IN_MEMORY_PAGE(blkno);
	page_desc: &mut OrioleDBPageDesc = O_GET_IN_MEMORY_PAGEDESC(blkno);
	header: &mut BTreePageHeader = (BTreePageHeader *) p;

	if (!noLock)
	{
		lock_page(blkno);
		page_block_reads(blkno);
	}

	page_desc->oids = desc->oids;
	page_desc->type = desc->type;
	page_desc->fileExtent.len = InvalidFileExtentLen;
	page_desc->fileExtent.off = InvalidFileExtentOff;
	header->flags = flags;
	if (flags & O_BTREE_FLAG_LEAF)
	{
		header->field1 = 0;
		PAGE_SET_N_VACATED(p, 0);
	}
	else
	{
		PAGE_SET_LEVEL(p, level);
		PAGE_SET_N_ONDISK(p, 0);
	}
	header->rightLink = InvalidRightLink;
	header->csn = COMMITSEQNO_FROZEN;
	header->undoLocation = InvalidUndoLocation;
	header->o_header.checkpointNum = 0;
	header->itemsCount = 0;
	header->prevInsertOffset = MaxOffsetNumber;
	header->maxKeyLen = 0;
	ppool_ucm_init(desc->ppool, blkno);

	memset(p + offsetof(BTreePageHeader, chunkDesc),
		   0,
		   ORIOLEDB_BLCKSZ - offsetof(BTreePageHeader, chunkDesc));
}


init_meta_page(OInMemoryBlkno blkno, uint32 leafPagesNum)
{
	Page		p = O_GET_IN_MEMORY_PAGE(blkno);
	page_desc: &mut OrioleDBPageDesc = O_GET_IN_MEMORY_PAGEDESC(blkno);
	metaPage: &mut BTreeMetaPage = (BTreeMetaPage *) p;
	int			i,
				j;

	memset(p + O_PAGE_HEADER_SIZE, 0, ORIOLEDB_BLCKSZ - O_PAGE_HEADER_SIZE);
	pg_atomic_init_u32(&metaPage->leafPagesNum, leafPagesNum);
	pg_atomic_init_u64(&metaPage->numFreeBlocks, 0);
	pg_atomic_init_u64(&metaPage->datafileLength[0], 0);
	pg_atomic_init_u64(&metaPage->datafileLength[1], 0);
	pg_atomic_init_u64(&metaPage->ctid, 0);
	pg_atomic_init_u64(&metaPage->bridge_ctid, 0);
	for (i = 0; i < NUM_SEQ_SCANS_ARRAY_SIZE; i++)
		pg_atomic_init_u32(&metaPage->numSeqScans[i], 0);

	LWLockInitialize(&metaPage->copyBlknoLock,
					 checkpoint_state->copyBlknoTrancheId);
	LWLockInitialize(&metaPage->metaLock,
					 checkpoint_state->oMetaTrancheId);
	LWLockInitialize(&metaPage->punchHolesLock,
					 checkpoint_state->punchHolesTrancheId);

	page_desc->type = oIndexInvalid;
	ORelOidsSetInvalid(page_desc->oids);
	page_desc->fileExtent.len = InvalidFileExtentLen;
	page_desc->fileExtent.off = InvalidFileExtentOff;

	for (i = 0; i < 2; i++)
	{
		metaPage->freeBuf.pages[i] = OInvalidInMemoryBlkno;
		for (j = 0; j < 2; j++)
		{
			metaPage->nextChkp[j].pages[i] = OInvalidInMemoryBlkno;
			metaPage->tmpBuf[j].pages[i] = OInvalidInMemoryBlkno;
		}

		metaPage->partsInfo[i].writeMaxLocation = 0;
		for (j = 0; j < MAX_NUM_DIRTY_PARTS; j++)
		{
			metaPage->partsInfo[i].dirtyParts[j].segNum = -1;
			metaPage->partsInfo[i].dirtyParts[j].partNum = -1;
		}
	}
	metaPage->punchHolesChkpNum = checkpoint_state->lastCheckpointNumber;
	metaPage->toBeFreedOnSeqScanRelease = false;
}

//
// Estimate vacated space in the page after item replace on the given offset.
//
LocationIndex
page_get_vacated_skip_item(desc: &mut BTreeDescr, Page p, CommitSeqNo csn,
						   LocationIndex offset)
{
	LocationIndex vacatedBytes = 0;
	BTreePageItemLocator loc;

	BTREE_PAGE_FOREACH_ITEMS(p, &loc)
	{
		header: &mut BTreeLeafTuphdr;
		OTuple		tuple;

		if (BTREE_PAGE_LOCATOR_GET_OFFSET(p, &loc) == offset)
			continue;

		BTREE_PAGE_READ_LEAF_ITEM(header, tuple, p, &loc);
		if (XACT_INFO_FINISHED_FOR_EVERYBODY(header->xactInfo))
		{
			if (header->deleted)
			{
				if (COMMITSEQNO_IS_INPROGRESS(csn) || XACT_INFO_MAP_CSN(header->xactInfo) < csn)
					vacatedBytes += BTREE_PAGE_GET_ITEM_SIZE(p, &loc);
			}
			else
			{
				LocationIndex itemCompactedSize;

				itemCompactedSize = BTreeLeafTuphdrSize + MAXALIGN(o_btree_len(desc, tuple, OTupleLength));
				vacatedBytes += BTREE_PAGE_GET_ITEM_SIZE(p, &loc) - itemCompactedSize;
			}
		}
	}

	return vacatedBytes;
}

//
// Estimate vacated space in the page.
//
LocationIndex
page_get_vacated_space(desc: &mut BTreeDescr, Page p, CommitSeqNo csn)
{
	return page_get_vacated_skip_item(desc, p, csn, -1);
}


page_cut_first_key(Page node)
{
	tuphdr: &mut BTreeNonLeafTuphdr,
				tmp;
	BTreePageItemLocator loc;

	Assert(!O_PAGE_IS(node, LEAF));
	BTREE_PAGE_LOCATOR_FIRST(node, &loc);
	Assert(BTREE_PAGE_GET_ITEM_SIZE(node, &loc) > BTreeNonLeafTuphdrSize);

	tuphdr = (BTreeNonLeafTuphdr *) BTREE_PAGE_LOCATOR_GET_ITEM(node, &loc);
	tmp = *tuphdr;

	page_locator_resize_item(node, &loc, BTreeNonLeafTuphdrSize);

	tuphdr = (BTreeNonLeafTuphdr *) BTREE_PAGE_LOCATOR_GET_ITEM(node, &loc);
	*tuphdr = tmp;
}


put_page_image(OInMemoryBlkno blkno, Page img)
{
	Page		page = O_GET_IN_MEMORY_PAGE(blkno);
	int			skipSize = offsetof(OrioleDBPageHeader, checkpointNum);

	pg_write_barrier();

	memcpy(page + skipSize,
		   (char *) img + skipSize,
		   ORIOLEDB_BLCKSZ - skipSize);
}

//
// Calculates number of vacated bytes for leaf pages and number of
// disk downlinks for non-leaf pages.
//

o_btree_page_calculate_statistics(desc: &mut BTreeDescr, Pointer p)
{
	BTreePageItemLocator loc;

	if (O_PAGE_IS(p, LEAF))
	{
		int			nVacated = 0;

		// Bridge tuples not treated as vacated
		if (desc->type == oIndexBridge)
			return;

		BTREE_PAGE_FOREACH_ITEMS(p, &loc)
		{
			tupHdr: &mut BTreeLeafTuphdr;
			OTuple		tuple;

			BTREE_PAGE_READ_LEAF_ITEM(tupHdr, tuple, p, &loc);

			if (tupHdr->deleted)
				nVacated += BTREE_PAGE_GET_ITEM_SIZE(p, &loc);
			else
				nVacated += BTREE_PAGE_GET_ITEM_SIZE(p, &loc) -
					(BTreeLeafTuphdrSize + MAXALIGN(o_btree_len(desc, tuple, OTupleLength)));
		}
		PAGE_SET_N_VACATED(p, nVacated);
	}
	else
	{
		int			nOnDisk = 0;

		BTREE_PAGE_FOREACH_ITEMS(p, &loc)
		{
			tupHdr: &mut BTreeNonLeafTuphdr = (BTreeNonLeafTuphdr *) BTREE_PAGE_LOCATOR_GET_ITEM(p, &loc);

			if (DOWNLINK_IS_ON_DISK(tupHdr->downlink))
				nOnDisk++;
		}
		PAGE_SET_N_ONDISK(p, nOnDisk);
	}
}


copy_fixed_tuple(desc: &mut BTreeDescr, dst: &mut OFixedTuple, OTuple src)
{
	int			tuplen;

	if (O_TUPLE_IS_NULL(src))
	{
		clear_fixed_tuple(dst);
		return;
	}

	tuplen = o_btree_len(desc, src, OTupleLength);
	Assert(tuplen <= sizeof(dst->fixedData));
	dst->tuple.formatFlags = src.formatFlags;
	dst->tuple.data = dst->fixedData;
	memcpy(dst->fixedData, src.data, tuplen);
	if (tuplen != MAXALIGN(tuplen))
		memset(&dst->fixedData[tuplen], 0, MAXALIGN(tuplen) - tuplen);
}

fn
copy_fixed_key_with_len(dst: &mut OFixedKey, OTuple src, int tuplen)
{
	if (O_TUPLE_IS_NULL(src))
	{
		clear_fixed_key(dst);
		return;
	}

	dst->tuple.formatFlags = src.formatFlags;
	dst->tuple.data = dst->fixedData;
	memcpy(dst->fixedData, src.data, tuplen);
	if (tuplen != MAXALIGN(tuplen))
		memset(&dst->fixedData[tuplen], 0, MAXALIGN(tuplen) - tuplen);
}


copy_fixed_key(desc: &mut BTreeDescr, dst: &mut OFixedKey, OTuple src)
{
	int			tuplen;

	if (O_TUPLE_IS_NULL(src))
	{
		clear_fixed_key(dst);
		return;
	}

	tuplen = o_btree_len(desc, src, OKeyLength);
	Assert(tuplen <= sizeof(dst->fixedData));
	copy_fixed_key_with_len(dst, src, tuplen);
}


copy_fixed_page_key(desc: &mut BTreeDescr, dst: &mut OFixedKey,
					Page p, loc: &mut BTreePageItemLocator)
{
	OTuple		src;

	BTREE_PAGE_READ_TUPLE(src, p, loc);
	copy_fixed_key(desc, dst, src);
}


copy_fixed_hikey(desc: &mut BTreeDescr, dst: &mut OFixedKey, Page p)
{
	OTuple		src;

	BTREE_PAGE_GET_HIKEY(src, p);
	copy_fixed_key(desc, dst, src);
}

fn
clear_fixed_tuple(dst: &mut OFixedTuple)
{
	dst->tuple.formatFlags = 0;
	dst->tuple.data = NULL;
}


clear_fixed_key(dst: &mut OFixedKey)
{
	dst->tuple.formatFlags = 0;
	dst->tuple.data = NULL;
}


copy_from_fixed_shmem_key(dst: &mut OFixedKey, src: &mut OFixedShmemKey)
{
	if (!src->notNull)
	{
		clear_fixed_key(dst);
		return;
	}

	memcpy(dst->fixedData, src->data.fixedData, src->len);
	dst->tuple.data = dst->fixedData;
	dst->tuple.formatFlags = src->formatFlags;
}


copy_fixed_shmem_key(desc: &mut BTreeDescr, dst: &mut OFixedShmemKey, OTuple src)
{
	if (O_TUPLE_IS_NULL(src))
	{
		clear_fixed_shmem_key(dst);
		return;
	}

	dst->len = o_btree_len(desc, src, OKeyLength);
	Assert(dst->len <= sizeof(dst->data.fixedData));
	memcpy(dst->data.fixedData, src.data, dst->len);
	dst->notNull = true;
	dst->formatFlags = src.formatFlags;
}


copy_fixed_shmem_page_key(desc: &mut BTreeDescr, dst: &mut OFixedShmemKey,
						  Page p, loc: &mut BTreePageItemLocator)
{
	OTuple		src;

	BTREE_PAGE_READ_TUPLE(src, p, loc);
	copy_fixed_shmem_key(desc, dst, src);
}


copy_fixed_shmem_hikey(desc: &mut BTreeDescr, dst: &mut OFixedShmemKey, Page p)
{
	OTuple		src;

	BTREE_PAGE_GET_HIKEY(src, p);
	copy_fixed_shmem_key(desc, dst, src);
}


clear_fixed_shmem_key(dst: &mut OFixedShmemKey)
{
	dst->notNull = false;
	dst->formatFlags = 0;
	dst->len = 0;
}

OTuple
fixed_shmem_key_get_tuple(src: &mut OFixedShmemKey)
{
	OTuple		result;

	if (src->notNull)
	{
		result.data = src->data.fixedData;
		result.formatFlags = src->formatFlags;
	}
	else
	{
		result.data = NULL;
		result.formatFlags = 0;
	}
	return result;
}

OTuple
page_get_hikey(Page p)
{
	chunkDesc: &mut BTreePageChunkDesc;
	header: &mut BTreePageHeader = (BTreePageHeader *) p;
	OTuple		result;

	Assert(!O_PAGE_IS(p, RIGHTMOST));

	chunkDesc = &header->chunkDesc[header->chunksCount - 1];

	result.formatFlags = chunkDesc->hikeyFlags;
	result.data = (Pointer) p + SHORT_GET_LOCATION(chunkDesc->hikeyShortLocation);

	return result;
}

int
page_get_hikey_size(Page p)
{
	chunkDesc: &mut BTreePageChunkDesc;
	header: &mut BTreePageHeader = (BTreePageHeader *) p;

	Assert(!O_PAGE_IS(p, RIGHTMOST));
	chunkDesc = &header->chunkDesc[header->chunksCount - 1];

	return (header->hikeysEnd - SHORT_GET_LOCATION(chunkDesc->hikeyShortLocation));
}


page_set_hikey_flags(Page p, uint8 flags)
{
	chunkDesc: &mut BTreePageChunkDesc;
	header: &mut BTreePageHeader = (BTreePageHeader *) p;

	Assert(!O_PAGE_IS(p, RIGHTMOST));
	chunkDesc = &header->chunkDesc[header->chunksCount - 1];
	chunkDesc->hikeyFlags = flags;
}

bool
page_fits_hikey(Page p, LocationIndex newHikeySize)
{
	header: &mut BTreePageHeader = (BTreePageHeader *) p;
	LocationIndex dataShift,
				hikeyLocation,
				dataLocation;

	Assert(newHikeySize = MAXALIGN(newHikeySize));
	Assert(header->chunksCount == 1);

	hikeyLocation = SHORT_GET_LOCATION(header->chunkDesc[0].hikeyShortLocation);
	dataLocation = SHORT_GET_LOCATION(header->chunkDesc[0].shortLocation);
	if (hikeyLocation + newHikeySize <= dataLocation)
		return true;

	dataShift = hikeyLocation + newHikeySize - dataLocation;
	return (header->dataSize + dataShift <= ORIOLEDB_BLCKSZ);
}


page_resize_hikey(Page p, LocationIndex newHikeySize)
{
	header: &mut BTreePageHeader = (BTreePageHeader *) p;
	LocationIndex dataShift,
				hikeyLocation,
				dataLocation;

	Assert(newHikeySize = MAXALIGN(newHikeySize));
	Assert(header->chunksCount == 1);

	hikeyLocation = SHORT_GET_LOCATION(header->chunkDesc[0].hikeyShortLocation);
	dataLocation = SHORT_GET_LOCATION(header->chunkDesc[0].shortLocation);
	if (hikeyLocation + newHikeySize <= dataLocation)
	{
		// Fits
		header->hikeysEnd = hikeyLocation + newHikeySize;
		return;
	}

	dataShift = hikeyLocation + newHikeySize - dataLocation;
	Assert(header->dataSize + dataShift <= ORIOLEDB_BLCKSZ);
	memmove((Pointer) p + dataLocation + dataShift,
			(Pointer) p + dataLocation,
			header->dataSize - dataLocation);
	header->chunkDesc[0].shortLocation += LOCATION_GET_SHORT(dataShift);
	header->hikeysEnd = hikeyLocation + newHikeySize;
	header->dataSize += dataShift;
}


btree_page_update_max_key_len(desc: &mut BTreeDescr, Page p)
{
	LocationIndex maxKeyLen;
	header: &mut BTreePageHeader = (BTreePageHeader *) p;
	BTreePageItemLocator loc;

	if (!O_PAGE_IS(p, RIGHTMOST))
		maxKeyLen = BTREE_PAGE_GET_HIKEY_SIZE(p);
	else
		maxKeyLen = 0;

	BTREE_PAGE_FOREACH_ITEMS(p, &loc)
	{
		LocationIndex keyLen;

		if (!O_PAGE_IS(p, LEAF))
		{
			keyLen = BTREE_PAGE_GET_ITEM_SIZE(p, &loc) -
				BTreeNonLeafTuphdrSize;
		}
		else
		{
			OTuple		tuple;

			BTREE_PAGE_READ_TUPLE(tuple, p, &loc);
			keyLen = o_btree_len(desc, tuple, OTupleKeyLengthNoVersion);
		}
		maxKeyLen = Max(maxKeyLen, keyLen);
	}
	header->maxKeyLen = maxKeyLen;
}