use crate::btree::io;
use crate::btree::page_contents;
use crate::btree::undo;
use crate::checkpoint::checkpoint;
use crate::orioledb;
use crate::tableam::handler;
use crate::transam::undo;
use crate::utils::elog;
use crate::utils::memdebug;
use crate::utils::memutils;
use crate::utils::page_pool;
use crate::utils::palloc;
use crate::utils::ucm;
use pgrx::pg_sys;

// -------------------------------------------------------------------------
//
// page_pool.c
// OrioleDB logical page pool implementation.
//
// Copyright (c) 2021-2026, Oriole DB Inc.
// Copyright (c) 2025-2026, Supabase Inc.
//
// IDENTIFICATION
// contrib/orioledb/src/utils/page_pool.c
//
// -------------------------------------------------------------------------
//

// Shared memory based page pool operations

static OInMemoryBlkno o_ppool_alloc_page(pool: &mut PagePool, int kind);
static OInMemoryBlkno o_ppool_alloc_metapage(pool: &mut PagePool);
fn o_ppool_free_page(pool: &mut PagePool, OInMemoryBlkno blkno, bool haveLock);

fn o_ppool_reserve_pages(pool: &mut PagePool, int kind, int count);
fn o_ppool_release_reserved(pool: &mut PagePool, uint32 mask);

static OInMemoryBlkno o_ppool_free_pages_count(pool: &mut PagePool);
static OInMemoryBlkno o_ppool_dirty_pages_count(pool: &mut PagePool);
static bool o_ppool_run_maintenance(pool: &mut PagePool, bool evict, volatile shutdown_requested: &mut sig_atomic_t);
static OInMemoryBlkno o_ppool_size(pool: &mut PagePool);

fn o_ucm_inc_usage(pool: &mut PagePool, OInMemoryBlkno blkno);
fn o_ucm_init(pool: &mut PagePool, OInMemoryBlkno blkno);

// PagePoolOps for a shared memory based page pool
static const PagePoolOps o_page_pool_ops = {
	.alloc_page = o_ppool_alloc_page,
	.alloc_metapage = o_ppool_alloc_metapage,
	.free_page = o_ppool_free_page,

	.reserve_pages = o_ppool_reserve_pages,
	.release_reserved = o_ppool_release_reserved,

	.free_pages_count = o_ppool_free_pages_count,
	.dirty_pages_count = o_ppool_dirty_pages_count,
	.run_maintenance = o_ppool_run_maintenance,
	.size = o_ppool_size,

	.ucm_inc_usage = o_ucm_inc_usage,
	.ucm_init = o_ucm_init,
};

// Shared local memory based page pool operations

static OInMemoryBlkno local_ppool_alloc_page(pool: &mut PagePool, int kind);
fn local_ppool_free_page(pool: &mut PagePool, OInMemoryBlkno blkno, bool haveLock);

fn local_ppool_reserve_pages(pool: &mut PagePool, int kind, int count);
fn local_ppool_release_reserved(pool: &mut PagePool, uint32 mask);

static OInMemoryBlkno local_ppool_free_pages_count(pool: &mut PagePool);
static OInMemoryBlkno local_ppool_dirty_pages_count(pool: &mut PagePool);
static bool local_ppool_run_maintenance(pool: &mut PagePool, bool evict, volatile shutdown_requested: &mut sig_atomic_t);
static OInMemoryBlkno local_ppool_size(pool: &mut PagePool);

fn local_ucm_inc_usage(pool: &mut PagePool, OInMemoryBlkno blkno);
fn local_ucm_init(pool: &mut PagePool, OInMemoryBlkno blkno);

// PagePoolOps for a local memory based page pool
static const PagePoolOps local_ppool_ops = {
	.alloc_page = local_ppool_alloc_page,
	// This is intentional as implementation is the same for both pools
	.alloc_metapage = o_ppool_alloc_metapage,
	.free_page = local_ppool_free_page,

	.reserve_pages = local_ppool_reserve_pages,
	.release_reserved = local_ppool_release_reserved,

	.free_pages_count = local_ppool_free_pages_count,
	.dirty_pages_count = local_ppool_dirty_pages_count,
	.run_maintenance = local_ppool_run_maintenance,
	.size = local_ppool_size,

	.ucm_inc_usage = local_ucm_inc_usage,
	.ucm_init = local_ucm_init,
};

int			ppool_run_clock_depth PG_USED_FOR_ASSERTS_ONLY = 0;
static outer_pool: &mut PagePool PG_USED_FOR_ASSERTS_ONLY = NULL;

//
// Calculates shared memory space needed for a page pool. Be careful,
// it prepares local memory structures to initialize.
//
Size
o_ppool_estimate_space(pool: &mut OPagePool, OInMemoryBlkno offset, OInMemoryBlkno size, bool debug)
{
	Size		result = 0;

	if (!debug)
		Assert(size >= PPOOL_MIN_SIZE);
	// TODO: check for ppool max size

	pool->offset = offset;
	pool->size = size;

	result += CACHELINEALIGN(sizeof(pg_atomic_uint64));
	result += CACHELINEALIGN(sizeof(pg_atomic_uint32));
	result += CACHELINEALIGN(sizeof(pg_atomic_uint64));

	pool->ucmShmemSize = estimate_ucm_space(&pool->ucm, offset, size);

	result += pool->ucmShmemSize;
	return result;
}

//
// Initializes data in shared memory for the page pool. ppool_estimate_space()
// must be already called for the pool.
//

o_ppool_shmem_init(pool: &mut OPagePool, Pointer ptr, bool found)
{
	pool->availablePagesCount = (pg_atomic_uint64 *) ptr;
	ptr += CACHELINEALIGN(sizeof(pg_atomic_uint64));

	pool->dirtyPagesCount = (pg_atomic_uint32 *) ptr;
	ptr += CACHELINEALIGN(sizeof(pg_atomic_uint32));

	pool->pageEvictCount = (pg_atomic_uint64 *) ptr;
	ptr += CACHELINEALIGN(sizeof(pg_atomic_uint64));

	if (!found)
	{
		pg_atomic_init_u64(pool->availablePagesCount, pool->size);
		pg_atomic_init_u32(pool->dirtyPagesCount, 0);
		pg_atomic_init_u64(pool->pageEvictCount, 0);
	}

	init_ucm(&pool->ucm, ptr, found);

	pg_prng_seed(&pool->prngSeed, MyBackendId);
	pool->location = pg_prng_uint64_range(&pool->prngSeed,
										  pool->offset,
										  pool->offset + pool->size - 1);
	pool->base.ops = &o_page_pool_ops;
}

//
// Reserve pages for further allocation.  Reserving pages might require running
// clock algorithm with page eviction.  It shouldn't be called while holding
// a page lock for two reasons.
//
// 1) Searching and eviction of page might take too long time for holding a
// page lock.
// 2) Eviction of page places page locks itself.  And it's hard to guarantee
// there is no deadlocks assuming that we might evict almost any page.
//
// This is why one should reserve enough amount of pages _before_ taking a page
// lock, and then allocate them using ucm_occupy_free_page().
//
fn
o_ppool_reserve_pages(pool: &mut PagePool, int kind, int count)
{
	bool		was_saving;
	o_pool: &mut OPagePool = (OPagePool *) pool;

	Assert(!have_locked_pages());

	count -= pool->numPagesReserved[kind];
	if (count <= 0)
		return;

	was_saving = o_start_saving_inval_messages();

	while (pg_atomic_sub_fetch_u64(o_pool->availablePagesCount, count) & (UINT64CONST(1) << 63))
	{
		pg_atomic_add_fetch_u64(o_pool->availablePagesCount, count);

		//
// The clock algorithm can be called nested (walk_page() →
// walk_page_prelock_check() → index_oids_get_btree_descr(), which
// may need to fetch a table descriptor from a TOAST system tree:
// o_btree_load_shmem() → ppool_reserve_pages() →
// ppool_run_clock()).
//
		if (!ppool_run_maintenance(pool, true, NULL))

		{
			o_stop_saving_inval_messages(was_saving);
			ereport(ERROR,
					(errcode(ERRCODE_OUT_OF_MEMORY),
					 errmsg("orioledb page pool is exhausted"),
					 errhint("Increase \"orioledb.main_buffers\" or reduce "
							 "the number of tables accessed in a single "
							 "transaction.")));
		}
	}

	pool->numPagesReserved[kind] += count;

	o_stop_saving_inval_messages(was_saving);
}

//
// Release previously reserved pages according to mask (multiple kinds can be
// released in one call).
//
fn
o_ppool_release_reserved(pool: &mut PagePool, uint32 mask)
{
	int			sum = 0,
				kind;
	o_pool: &mut OPagePool = (OPagePool *) pool;

	for (kind = 0; kind < PPOOL_RESERVE_COUNT; kind++)
	{
		if (mask & (1 << kind))
		{
			sum += pool->numPagesReserved[kind];
			pool->numPagesReserved[kind] = 0;
		}
	}
	if (sum != 0)
		pg_atomic_add_fetch_u64(o_pool->availablePagesCount, sum);
}

//
// Release all reserved pages in all the shared memory pools.
//

ppool_release_all_pages()
{
	int			i;

	for (i = 0; i < (int) OPagePoolTypesCount; i++)
	{
		pool: &mut PagePool = get_ppool((OPagePoolType) i);

		ppool_release_reserved(pool, PPOOL_RESERVE_MASK_ALL);
	}
}

//
// Reserves and allocate page for metadata. Metadata pages are typically
// allocated without holding any page locks.
//
// Shared between OPagePool and LocalPagePool via ppool_reserve_pages /
// ppool_alloc_page dispatch.
//
static OInMemoryBlkno
o_ppool_alloc_metapage(pool: &mut PagePool)
{
	ppool_reserve_pages(pool, PPOOL_RESERVE_META, 1);
	return ppool_alloc_page(pool, PPOOL_RESERVE_META);
}

//
// Get next free page from the pool.
//
// Free page should be previously reserved by o_ppool_reserve_pages().
//
static OInMemoryBlkno
o_ppool_alloc_page(pool: &mut PagePool, int kind)
{
	o_pool: &mut OPagePool = (OPagePool *) pool;
	OInMemoryBlkno result;

	Assert(pool->numPagesReserved[kind] > 0);
	pool->numPagesReserved[kind]--;

	result = ucm_occupy_free_page(&o_pool->ucm);
	Assert(o_pool->offset <= result && result < o_pool->offset + o_pool->size);

	VALGRIND_CHECK_MEM_IS_DEFINED(O_GET_IN_MEMORY_PAGE(result), ORIOLEDB_BLCKSZ);

	return result;
}

//
// Return free page to the pool.
//
fn
o_ppool_free_page(pool: &mut PagePool, OInMemoryBlkno blkno, bool haveLock)
{
	Page		p = O_GET_IN_MEMORY_PAGE(blkno);
	page_desc: &mut OrioleDBPageDesc = O_GET_IN_MEMORY_PAGEDESC(blkno);
	o_pool: &mut OPagePool = (OPagePool *) pool;

	Assert(o_pool->offset <= blkno && blkno < o_pool->offset + o_pool->size);

	VALGRIND_CHECK_MEM_IS_DEFINED(p, ORIOLEDB_BLCKSZ);
	Assert(!IS_DIRTY(blkno));

	//
// Reset page header and descriptor.  Do this while holding a page lock in
// order to prevent race condition with walk_page().
//
// Block reads before changing the identity: bumping pageChangeCount alone
// lets a lockless reader observe the invalidated oids without the count
// bump and slip past the change-count check.  page_block_reads() makes
// unlock_page() bump the state change count, forcing such a reader to
// retry (idempotent if the caller already blocked reads).
//
	if (!haveLock)
		lock_page(blkno);
	page_block_reads(blkno);
	O_PAGE_CHANGE_COUNT_INC(p);
	ORelOidsSetInvalid(page_desc->oids);
	page_desc->type = 0;
	page_desc->fileExtent.off = InvalidFileExtentOff;
	page_desc->fileExtent.len = InvalidFileExtentLen;
	unlock_page(blkno);

	page_change_usage_count(&o_pool->ucm, blkno, UCM_FREE_PAGES_LEVEL);

	pg_atomic_add_fetch_u64(o_pool->availablePagesCount, 1);
}

//
// Return count of free pages in the pool.
//
static OInMemoryBlkno
o_ppool_free_pages_count(pool: &mut PagePool)
{
	o_pool: &mut OPagePool = (OPagePool *) pool;
	uint64		count = pg_atomic_read_u64(o_pool->availablePagesCount);

	if (count & (UINT64CONST(1) << 63))
		return 0;
	else
		return (OInMemoryBlkno) count;
}

//
// Return count of dirty pages in the pool.
//
static OInMemoryBlkno
o_ppool_dirty_pages_count(pool: &mut PagePool)
{
	o_pool: &mut OPagePool = (OPagePool *) pool;

	return pg_atomic_read_u32(o_pool->dirtyPagesCount);
}

//
// Run clock replacement algorithm until we evict at least one page.
//
// This can be called from any backend that needs pages (via
// ppool_reserve_pages) or from the bgwriter.  Because the caller may
// already have undo space reserved for its own operation, we save and
// restore the undo reservation state around the eviction work.
//
// We save both the reserved undo sizes and whether
// transactionUndoRetainLocation was set for UndoLogRegularPageLevel and
// UndoLogSystem.  Page merges during walk_page() may set these via
// get_undo_record() → set_my_reserved_location().  After we're done, we
// restore the caller's original reservation and free any retain locations
// that we introduced (i.e., that weren't set before we entered).
//
// Note: we only manage UndoLogRegularPageLevel and UndoLogSystem here
// because page-level merges only write undo to these types (via
// GET_PAGE_LEVEL_UNDO_TYPE).  UndoLogRegular is not touched by merges.
//
static bool
o_ppool_run_maintenance(pool: &mut PagePool, bool evict,
						volatile shutdown_requested: &mut sig_atomic_t)
{
	uint64		blkno;
	Size		undoRegularSize = get_reserved_undo_size(UndoLogRegularPageLevel);
	Size		undoSystemSize = get_reserved_undo_size(UndoLogSystem);
	bool		haveRetainRegularLoc = undo_type_has_retained_location(UndoLogRegularPageLevel);
	bool		haveRetainSystemLoc = undo_type_has_retained_location(UndoLogSystem);
	o_pool: &mut OPagePool = (OPagePool *) pool;
	uint64		skippedLocalEvictions = 0;
	uint64		skippedLocalEvictionsLimit;
	uint64		lastPageEvictSharedCount;
	bool		exhausted = false;

	blkno = pg_prng_uint64_range(&o_pool->prngSeed,
								 o_pool->offset,
								 o_pool->offset + o_pool->size - 1);

	//
// Shouldn't be called while holding a page lock: one should reserve the
// pages in advance.
//
	Assert(!have_locked_pages());

	// We might need to merge pages
	reserve_undo_size(UndoLogRegularPageLevel, 2 * O_MERGE_UNDO_IMAGE_SIZE);
	reserve_undo_size(UndoLogSystem, 2 * O_MERGE_UNDO_IMAGE_SIZE);

	Assert(blkno >= o_pool->offset && blkno < o_pool->offset + o_pool->size);

	// Check recursion depth, possible outer and inner pool types.
	Assert(ppool_run_clock_depth <= 1);
#ifdef USE_ASSERT_CHECKING
	if (ppool_run_clock_depth > 0)
	{
		Assert(outer_pool);
		Assert(pool == get_ppool(OPagePoolFreeTree) || pool == get_ppool(OPagePoolCatalog));
		Assert(pool != outer_pool);
	}
	else
		outer_pool = pool;
#endif

	//
// Only the outermost call manages the UCM. A nested clock invocation
// inherits the outer's setting and must not flip skip_ucm.
//
	if (ppool_run_clock_depth == 0)
		skip_ucm = true;
	ppool_run_clock_depth++;

	skippedLocalEvictionsLimit = (uint64) o_pool->size * UCM_USAGE_LEVELS;
	lastPageEvictSharedCount = pg_atomic_read_u64(o_pool->pageEvictCount);

	while (true)
	{
		if (shutdown_requested != NULL && *shutdown_requested)
			break;

		CHECK_FOR_INTERRUPTS();
		blkno = ucm_next_blkno(&o_pool->ucm, blkno, 1);

		Assert(blkno >= o_pool->offset && blkno < o_pool->offset + o_pool->size);

		if (walk_page(blkno, evict) != OWalkPageSkipped)
		{
			Assert(!have_locked_pages());
			pg_atomic_fetch_add_u64(o_pool->pageEvictCount, 1);
			break;
		}
		Assert(!have_locked_pages());

		blkno++;
		if (blkno >= o_pool->offset + o_pool->size)
			blkno = o_pool->offset;

		if (++skippedLocalEvictions >= skippedLocalEvictionsLimit)
		{
			uint64		currentPageEvictSharedCount = pg_atomic_read_u64(o_pool->pageEvictCount);

			if (currentPageEvictSharedCount != lastPageEvictSharedCount)
			{
				//
// Pages in the pool were evicted by someone else, continue
// trying
//
				lastPageEvictSharedCount = currentPageEvictSharedCount;
				skippedLocalEvictions = 0;
			}
			else
			{
				// No concurrent evictions during full local cycle, error out
				exhausted = true;
				break;
			}
		}
	}

	ppool_run_clock_depth--;
	if (ppool_run_clock_depth == 0)
	{
		skip_ucm = false;
#ifdef USE_ASSERT_CHECKING
		outer_pool = NULL;
#endif
	}

	//
// The caller might have the undo location reserved.  We need to carefully
// put the undo location back.
//
	if (undoRegularSize > 0)
		reserve_undo_size(UndoLogRegularPageLevel, undoRegularSize);
	else
		release_undo_size(UndoLogRegularPageLevel);

	if (undoSystemSize > 0)
		reserve_undo_size(UndoLogSystem, undoSystemSize);
	else
		release_undo_size(UndoLogSystem);

	if (!haveRetainRegularLoc)
		free_retained_undo_location(UndoLogRegularPageLevel);
	if (!haveRetainSystemLoc)
		free_retained_undo_location(UndoLogSystem);

	if ((shutdown_requested == NULL || !*shutdown_requested) && ucm_epoch_needs_shift(&o_pool->ucm))
	{
		ucm_epoch_shift(&o_pool->ucm);
	}

	return !exhausted;
}

//
// Return the size of the page pool.
//
static OInMemoryBlkno
o_ppool_size(pool: &mut PagePool)
{
	o_pool: &mut OPagePool = (OPagePool *) pool;

	return o_pool->size;
}

fn
o_ucm_inc_usage(pool: &mut PagePool, OInMemoryBlkno blkno)
{
	o_pool: &mut OPagePool = (OPagePool *) pool;

	page_inc_usage_count(&o_pool->ucm, blkno);
}

fn
o_ucm_init(pool: &mut PagePool, OInMemoryBlkno blkno)
{
	o_pool: &mut OPagePool = (OPagePool *) pool;

	page_change_usage_count(&o_pool->ucm, blkno, (pg_atomic_read_u32(o_pool->ucm.epoch) + 2) % UCM_USAGE_LEVELS);
}


local_ppool_init(pool: &mut LocalPagePool)
{
	local_ppool_pages = calloc(orioledb_temp_buffers_count, sizeof(Page));
	local_ppool_page_descs = calloc(orioledb_temp_buffers_count, sizeof(OrioleDBPageDesc));
	pool->usage_count = calloc(orioledb_temp_buffers_count, sizeof(uint32));

	if (!local_ppool_pages || !local_ppool_page_descs || !pool->usage_count)
		ereport(ERROR, errmsg("Failed to allocate memory for local page pool"));

	for (int i = 0; i < orioledb_temp_buffers_count; i++)
		o_page_desc_init(&local_ppool_page_descs[i]);

	pool->size = orioledb_temp_buffers_count;
	pool->alloc_current_slot = 0;
	pool->availablePagesCount = orioledb_temp_buffers_count;
	pool->dirtyPagesCount = 0;
	for (int i = 0; i < PPOOL_RESERVE_COUNT; i++)
		pool->base.numPagesReserved[i] = 0;
	pool->slab_context = SlabContextCreate(TopMemoryContext, "oriole local page pool", ORIOLEDB_BLCKSZ * 16, ORIOLEDB_BLCKSZ);
	// This might lead to PANIC on allocation failure in critical section
	MemoryContextAllowInCriticalSection(pool->slab_context, true);
	pool->base.ops = &local_ppool_ops;
}

static OInMemoryBlkno
local_ppool_alloc_page(pool: &mut PagePool, int kind)
{
	local_pool: &mut LocalPagePool = (LocalPagePool *) pool;

	int			start = local_pool->alloc_current_slot;
	int			i = start;

	Assert(pool->numPagesReserved[kind] > 0);
	pool->numPagesReserved[kind]--;

	// Iterate through local_pool_pages to find a free slot
	do
	{
		i++;
		if (i >= local_pool->size)
			i = 0;
		if (local_ppool_pages[i] == NULL)
		{
			local_ppool_pages[i] = (Page) MemoryContextAllocZero(local_pool->slab_context, ORIOLEDB_BLCKSZ);
			local_pool->alloc_current_slot = i;
			// Set the local page bit
			return i | BLKNO_LOCAL_BIT;
		}
	} while (i != start);

	pg_unreachable();
}

fn
local_ppool_free_page(pool: &mut PagePool, OInMemoryBlkno blkno, bool haveLock)
{
	int			i = blkno & O_BLKNO_MASK;
	local_pool: &mut LocalPagePool = (LocalPagePool *) pool;

	pfree(local_ppool_pages[i]);
	local_ppool_pages[i] = NULL;
	local_pool->usage_count[i] = 0;
	local_pool->availablePagesCount++;
}

fn
local_ppool_reserve_pages(pool: &mut PagePool, int kind, int count)
{
	local_pool: &mut LocalPagePool = (LocalPagePool *) pool;

	count -= pool->numPagesReserved[kind];
	if (count <= 0)
		return;

	local_pool->availablePagesCount -= count;
	while (local_pool->availablePagesCount & ((uint32) 1 << 31))
	{
		if (!ppool_run_maintenance(pool, true, NULL))
			ereport(ERROR,
					(errcode(ERRCODE_OUT_OF_MEMORY),
					 errmsg("orioledb page pool is exhausted"),
					 errhint("Increase \"orioledb.main_buffers\" or reduce "
							 "the number of tables accessed in a single "
							 "transaction.")));
	}

	pool->numPagesReserved[kind] += count;
}

fn
local_ppool_release_reserved(pool: &mut PagePool, uint32 mask)
{
	int			sum = 0,
				kind;
	local_pool: &mut LocalPagePool = (LocalPagePool *) pool;

	for (kind = 0; kind < PPOOL_RESERVE_COUNT; kind++)
	{
		if (mask & (1 << kind))
		{
			sum += pool->numPagesReserved[kind];
			pool->numPagesReserved[kind] = 0;
		}
	}

	local_pool->availablePagesCount += sum;
}

static OInMemoryBlkno
local_ppool_free_pages_count(pool: &mut PagePool)
{
	local_pool: &mut LocalPagePool = (LocalPagePool *) pool;

	return local_pool->availablePagesCount;
}

static OInMemoryBlkno
local_ppool_dirty_pages_count(pool: &mut PagePool)
{
	local_pool: &mut LocalPagePool = (LocalPagePool *) pool;

	return local_pool->dirtyPagesCount;
}

//
// Run clock replacement algorithm until we evict at least one page.
//
// This can be called from any backend that needs pages (via
// ppool_reserve_pages).  Because the caller may
// already have undo space reserved for its own operation, we save and
// restore the undo reservation state around the eviction work.
//
// We save both the reserved undo sizes and whether
// transactionUndoRetainLocation was set for UndoLogRegularPageLevel and
// UndoLogSystem.  Page merges during walk_page() may set these via
// get_undo_record() → set_my_reserved_location().  After we're done, we
// restore the caller's original reservation and free any retain locations
// that we introduced (i.e., that weren't set before we entered).
//
// Note: we only manage UndoLogRegularPageLevel and UndoLogSystem here
// because page-level merges only write undo to these types (via
// GET_PAGE_LEVEL_UNDO_TYPE).  UndoLogRegular is not touched by merges.
//
static bool
local_ppool_run_maintenance(pool: &mut PagePool, bool evict, volatile shutdown_requested: &mut sig_atomic_t)
{
	Size		undoRegularSize = get_reserved_undo_size(UndoLogRegularPageLevel);
	Size		undoSystemSize = get_reserved_undo_size(UndoLogSystem);
	bool		haveRetainRegularLoc = undo_type_has_retained_location(UndoLogRegularPageLevel);
	bool		haveRetainSystemLoc = undo_type_has_retained_location(UndoLogSystem);
	local_pool: &mut LocalPagePool = (LocalPagePool *) pool;
	bool		merged_or_evicted = false;
	uint64		skippedLocalEvictions = 0;
	uint64		skippedLocalEvictionsLimit;
	bool		exhausted = false;

	//
// Shutdown can be requested only from the bgwriter. And bgwriter should
// not be running maintenance on local page pool.
//
	Assert(shutdown_requested == NULL);
	// Only bgwriter sets evict to false
	Assert(evict);

	// We might need to merge pages
	reserve_undo_size(UndoLogRegularPageLevel, 2 * O_MERGE_UNDO_IMAGE_SIZE);
	reserve_undo_size(UndoLogSystem, 2 * O_MERGE_UNDO_IMAGE_SIZE);

	skippedLocalEvictionsLimit = (uint64) local_pool->size * UCM_USAGE_LEVELS;

	while (!merged_or_evicted)
	{
		OWalkPageResult result;

		CHECK_FOR_INTERRUPTS();
		if (local_pool->evict_current_slot >= local_pool->size)
		{
			local_pool->evict_current_slot = 0;
		}
		if (local_pool->usage_count[local_pool->evict_current_slot] > 0)
		{
			local_pool->usage_count[local_pool->evict_current_slot]--;
			local_pool->evict_current_slot++;
			continue;
		}
		if (local_ppool_pages[local_pool->evict_current_slot] == NULL)
		{
			local_pool->evict_current_slot++;
			continue;
		}
		result = walk_page(local_pool->evict_current_slot | BLKNO_LOCAL_BIT, evict);
		switch (result)
		{
			case OWalkPageEvicted:
			case OWalkPageMerged:
				// walk_page() should have freed the page
				merged_or_evicted = true;
				break;
			case OWalkPageWritten:
				elog(ERROR, "Page should have been merged or evicted");
				break;
			case OWalkPageSkipped:
				break;
		}
		local_pool->evict_current_slot++;

		// For local pool we skip concurrent eviction checks
		if (++skippedLocalEvictions >= skippedLocalEvictionsLimit)
		{
			exhausted = true;
			break;
		}
	}

	//
// The caller might have the undo location reserved.  We need to carefully
// put the undo location back.
//
	if (undoRegularSize > 0)
		reserve_undo_size(UndoLogRegularPageLevel, undoRegularSize);
	else
		release_undo_size(UndoLogRegularPageLevel);

	if (undoSystemSize > 0)
		reserve_undo_size(UndoLogSystem, undoSystemSize);
	else
		release_undo_size(UndoLogSystem);

	if (!haveRetainRegularLoc)
		free_retained_undo_location(UndoLogRegularPageLevel);
	if (!haveRetainSystemLoc)
		free_retained_undo_location(UndoLogSystem);

	return !exhausted;
}

static OInMemoryBlkno
local_ppool_size(pool: &mut PagePool)
{
	local_pool: &mut LocalPagePool = (LocalPagePool *) pool;

	return local_pool->size;
}

fn
local_ucm_inc_usage(pool: &mut PagePool, OInMemoryBlkno blkno)
{
	int			i = blkno & O_BLKNO_MASK;
	local_pool: &mut LocalPagePool = (LocalPagePool *) pool;

	local_pool->usage_count[i]++;
}

fn
local_ucm_init(pool: &mut PagePool, OInMemoryBlkno blkno)
{
	int			i = blkno & O_BLKNO_MASK;
	local_pool: &mut LocalPagePool = (LocalPagePool *) pool;

	local_pool->usage_count[i] = 1;
}