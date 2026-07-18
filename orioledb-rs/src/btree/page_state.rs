use crate::access::transam;
use crate::btree::find;
use crate::btree::io;
use crate::btree::page_chunks;
use crate::btree::undo;
use crate::orioledb;
use crate::pgstat;
use crate::recovery::recovery;
use crate::storage::proc;
use crate::storage::proclist;
use crate::storage::s_lock;
use crate::tableam::descr;
use crate::tableam::key_range;
use crate::transam::oxid;
use crate::transam::undo;
use crate::utils::dsa;
use crate::utils::memdebug;
use crate::utils::page_pool;
use crate::utils::stopevent;
use crate::utils::ucm;
use pgrx::pg_sys::ItemPointerData;
use pgrx::pg_sys;

// -------------------------------------------------------------------------
//
// page_state.c
// OrioleDB B-tree page locking, waiting, reading etc.
//
// Copyright (c) 2021-2026, Oriole DB Inc.
// Copyright (c) 2025-2026, Supabase Inc.
//
// IDENTIFICATION
// contrib/orioledb/src/btree/page_state.c
//
// -------------------------------------------------------------------------
//

// Maximum simultaneously locked pages per process
#define MAX_PAGES_PER_PROCESS 8

//
// Enable this to recheck page stats on every unlock.
//
// #define CHECK_PAGE_STATS

typedef struct
{
	pub static mut BLKNO: OInMemoryBlkno = std::mem::zeroed();
	pub static mut STATE: uint64 = std::mem::zeroed();
} MyLockedPage;

static MyLockedPage myLockedPages[MAX_PAGES_PER_PROCESS];
static OInMemoryBlkno myInProgressSplitPages[ORIOLEDB_MAX_DEPTH * 2];
static mut NUMBER_OF_MY_LOCKED_PAGES: std::os::raw::c_int = 0;
static mut NUMBER_OF_MY_IN_PROGRESS_SPLIT_PAGES: std::os::raw::c_int = 0;

pub static mut O_PAGE_WAITER_SHMEM_STATE: *mut lockerStates = std::ptr::null_mut();

#ifdef CHECK_PAGE_STATS
fn o_check_btree_page_statistics(desc: &mut BTreeDescr, Pointer p);
#endif

#ifdef CHECK_PAGE_STRUCT
fn o_check_page_struct(desc: &mut BTreeDescr, Page p);
#endif

Size
page_state_shmem_needs()
{
	return CACHELINEALIGN(sizeof(OPageWaiterShmemState) * max_procs);
}


page_state_shmem_init(Pointer buf, bool found)
{
	pub static mut PTR: Pointer = buf;

	lockerStates = (OPageWaiterShmemState *) ptr;
}

static int
get_my_locked_page_index(OInMemoryBlkno blkno)
{
	pub static mut I: std::os::raw::c_int = 0;

	for (i = 0; i < numberOfMyLockedPages; i++)
		if (myLockedPages[i].blkno == blkno)
			pub static mut I: return = std::mem::zeroed();
	return -1;
}

fn
my_locked_page_add(OInMemoryBlkno blkno, uint64 state)
{
	Assert(get_my_locked_page_index(blkno) < 0);
	Assert(numberOfMyLockedPages < MAX_PAGES_PER_PROCESS);

	Assert(pg_atomic_read_u64(&((OrioleDBPageHeader *) O_GET_IN_MEMORY_PAGE(blkno))->state) & PAGE_STATE_LOCKED_FLAG);
	myLockedPages[numberOfMyLockedPages].blkno = blkno;
	myLockedPages[numberOfMyLockedPages++].state = state;
}

static uint64
my_locked_page_del(OInMemoryBlkno blkno)
{
	int			i = get_my_locked_page_index(blkno);
	pub static mut STATE: uint64 = std::mem::zeroed();

	Assert(i >= 0 && i < MAX_PAGES_PER_PROCESS);
	state = myLockedPages[i].state;
	myLockedPages[i] = myLockedPages[--numberOfMyLockedPages];

	pub static mut STATE: return = std::mem::zeroed();
}

static uint64
my_locked_page_get_state(OInMemoryBlkno blkno)
{
	int			i = get_my_locked_page_index(blkno);

	Assert(i >= 0 && i < MAX_PAGES_PER_PROCESS);
	return myLockedPages[i].state;
}

static uint64
lock_page_or_queue(OInMemoryBlkno blkno, uint32 pgprocnum)
{
	ppool: &mut OPagePool = (OPagePool *) get_ppool_by_blkno(blkno);
	Page		p = O_GET_IN_MEMORY_PAGE(blkno);
	header: &mut OrioleDBPageHeader = (OrioleDBPageHeader *) p;
	pub static mut STATE: uint64 = std::mem::zeroed();
	pub static mut O_PAGE_WAITER_SHMEM_STATE: *mut lockerState = &lockerStates[pgprocnum];
	pub static mut UCM_UPDATE_TRIED: bool = false;

	Assert(pgprocnum < max_procs);
	Assert(!O_PAGE_IS_LOCAL(blkno));

	state = pg_atomic_read_u64(&header->state);
	while (true)
	{
		pub static mut NEW_STATE: uint64 = std::mem::zeroed();

		if (!O_PAGE_STATE_IS_LOCKED(state))
		{
			newState = O_PAGE_STATE_LOCK(state);
		}
		else
		{
			Assert((state & PAGE_STATE_LIST_TAIL_MASK) != pgprocnum);
			lockerState->status = OPageWaitExclusive;
			lockerState->next = (state & PAGE_STATE_LIST_TAIL_MASK);
			newState = state & (~PAGE_STATE_LIST_TAIL_MASK);
			newState |= pgprocnum;
		}

		if (!ucmUpdateTried)
		{
			newState = ucm_update_state(&ppool->ucm, blkno, newState);
			ucmUpdateTried = true;
		}

		if (pg_atomic_compare_exchange_u64(&header->state, &state, newState))
		{
			ucm_after_update_state(&ppool->ucm, blkno, state, newState);
			break;
		}
	}

	pub static mut STATE: return = std::mem::zeroed();
}

typedef struct
{
	char		img[8192];
	pub static mut PARTIAL: PartialPageState = std::mem::zeroed();
	pub static mut LOAD: bool = false;
} PageImg;

typedef enum
{
	LockPageResultLocked = 1,
	LockPageResultQueued = 2,
	LockPageResultSplitDetected = 3
} LockPageResult;

static LockPageResult
lock_page_or_queue_or_split_detect(desc: &mut BTreeDescr, blkno: &mut OInMemoryBlkno,
								   pageChangeCount: &mut uint32, uint32 pgprocnum,
								   img: &mut PageImg, OTupleXactInfo xactInfo,
								   OTuple tuple, prevState: &mut uint64,
								   keySerialized: &mut bool)
{
	ppool: &mut OPagePool = (OPagePool *) get_ppool_by_blkno(*blkno);
	Page		p = O_GET_IN_MEMORY_PAGE(*blkno);
	header: &mut OrioleDBPageHeader = (OrioleDBPageHeader *) p;
	imgHeader: &mut OrioleDBPageHeader = (OrioleDBPageHeader *) img->img;
	pub static mut STATE: uint64 = std::mem::zeroed();
	pub static mut O_PAGE_WAITER_SHMEM_STATE: *mut lockerState = &lockerStates[pgprocnum];
	pub static mut UCM_UPDATE_TRIED: bool = false;

	Assert(pgprocnum < max_procs);
	Assert(!O_PAGE_IS_LOCAL(*blkno));

	state = pg_atomic_read_u64(&header->state);
	while (true)
	{
		pub static mut NEW_STATE: uint64 = std::mem::zeroed();

		if (!O_PAGE_STATE_IS_LOCKED(state))
		{
			newState = O_PAGE_STATE_LOCK(state);
		}
		else
		{
			if (!img->load ||
				(state & PAGE_STATE_CHANGE_COUNT_MASK) != (pg_atomic_read_u64(&imgHeader->state) & PAGE_STATE_CHANGE_COUNT_MASK))
			{
				if (!o_btree_read_page(desc, *blkno, *pageChangeCount, img->img,
									   COMMITSEQNO_INPROGRESS, NULL, BTreeKeyNone, NULL,
									   &img->partial, true, NULL, NULL))
				{
					pub static mut LOCK_PAGE_RESULT_SPLIT_DETECTED: return = std::mem::zeroed();
				}
				img->load = true;

				if (!O_PAGE_IS(img->img, RIGHTMOST))
				{
					pub static mut HIKEY: OTuple = std::mem::zeroed();

					BTREE_PAGE_GET_HIKEY(hikey, img->img);

					if (o_btree_cmp(desc, &tuple, BTreeKeyLeafTuple,
									&hikey, BTreeKeyNonLeafKey) >= 0)
					{
						uint64		rightlink = BTREE_PAGE_GET_RIGHTLINK(img->img);

						if (OInMemoryBlknoIsValid(RIGHTLINK_GET_BLKNO(rightlink)))
						{
							*blkno = RIGHTLINK_GET_BLKNO(rightlink);
							*pageChangeCount = RIGHTLINK_GET_CHANGECOUNT(rightlink);
							p = O_GET_IN_MEMORY_PAGE(*blkno);
							header = (OrioleDBPageHeader *) p;
							Assert(get_my_locked_page_index(*blkno) < 0);
							state = pg_atomic_read_u64(&header->state);
							continue;
						}
						else
						{
							pub static mut LOCK_PAGE_RESULT_SPLIT_DETECTED: return = std::mem::zeroed();
						}
					}
				}
			}

			if (!*keySerialized)
			{
				pub static mut TUPHDR: BTreeLeafTuphdr = std::mem::zeroed();
				pub static mut TUPLEN: std::os::raw::c_int = 0;

				tuphdr.deleted = false;
				tuphdr.undoLocation = InvalidUndoLocation;
				tuphdr.formatFlags = 0;
				tuphdr.chainHasLocks = false;
				tuphdr.xactInfo = xactInfo;

				lockerState->reloids = desc->oids;
				if (desc->undoType != UndoLogNone)
					lockerState->reservedUndoSize = get_reserved_undo_size(desc->undoType);
				else
					lockerState->reservedUndoSize = 0;
				lockerState->tupleFlags = tuple.formatFlags;
				memcpy(lockerState->tupleData.fixedData,
					   &tuphdr,
					   BTreeLeafTuphdrSize);
				tuplen = o_btree_len(desc, tuple, OTupleLength);
				memcpy(&lockerState->tupleData.fixedData[BTreeLeafTuphdrSize],
					   tuple.data,
					   tuplen);
				if (tuplen != MAXALIGN(tuplen))
					memset(&lockerState->tupleData.fixedData[BTreeLeafTuphdrSize + tuplen],
						   0, MAXALIGN(tuplen) - tuplen);
				*keySerialized = true;
			}

			Assert((state & PAGE_STATE_LIST_TAIL_MASK) != pgprocnum);
			lockerState->status = OPageWaitInsert;
			lockerState->undoLocation = InvalidUndoLocation;
			lockerState->pageChangeCount = *pageChangeCount;
			lockerState->autonomousNestingLevel = GET_CUR_PROCDATA()->autonomousNestingLevel;
			Assert(!lockerState->inserted);
			lockerState->next = (state & PAGE_STATE_LIST_TAIL_MASK);
			newState = state & (~PAGE_STATE_LIST_TAIL_MASK);
			newState |= pgprocnum;
		}

		if (!ucmUpdateTried)
		{
			newState = ucm_update_state(&ppool->ucm, *blkno, newState);
			ucmUpdateTried = true;
		}

		if (pg_atomic_compare_exchange_u64(&header->state, &state, newState))
		{
			ucm_after_update_state(&ppool->ucm, *blkno, state, newState);
			break;
		}
	}

	*prevState = state;

	if (!O_PAGE_STATE_IS_LOCKED(state))
		pub static mut LOCK_PAGE_RESULT_LOCKED: return = std::mem::zeroed();
	else
		pub static mut LOCK_PAGE_RESULT_QUEUED: return = std::mem::zeroed();
}

//
// This function finishes when page is enable to read or we managed to lock
// the page list.
//
static uint64
read_enabled_or_queue(OInMemoryBlkno blkno, uint32 pgprocnum)
{
	Page		p = O_GET_IN_MEMORY_PAGE(blkno);
	header: &mut OrioleDBPageHeader = (OrioleDBPageHeader *) p;
	pub static mut STATE: uint64 = std::mem::zeroed();
	pub static mut O_PAGE_WAITER_SHMEM_STATE: *mut lockerState = &lockerStates[pgprocnum];

	state = pg_atomic_read_u64(&header->state);
	while (true)
	{
		pub static mut NEW_STATE: uint64 = std::mem::zeroed();

		if (!O_PAGE_STATE_READ_IS_BLOCKED(state))
		{
			break;
		}
		else
		{
			Assert((state & PAGE_STATE_LIST_TAIL_MASK) != pgprocnum);
			lockerState->status = OPageWaitNonExclusive;
			lockerState->next = (state & PAGE_STATE_LIST_TAIL_MASK);
			newState = state & (~PAGE_STATE_LIST_TAIL_MASK);
			newState |= pgprocnum;
		}

		if (pg_atomic_compare_exchange_u64(&header->state, &state, newState))
			break;
	}

	pub static mut STATE: return = std::mem::zeroed();
}

static uint64
state_changed_or_queue(OInMemoryBlkno blkno, uint32 pgprocnum,
					   uint64 oldState)
{
	ppool: &mut OPagePool = (OPagePool *) get_ppool_by_blkno(blkno);
	Page		p = O_GET_IN_MEMORY_PAGE(blkno);
	header: &mut OrioleDBPageHeader = (OrioleDBPageHeader *) p;
	pub static mut STATE: uint64 = std::mem::zeroed();
	pub static mut O_PAGE_WAITER_SHMEM_STATE: *mut lockerState = &lockerStates[pgprocnum];
	pub static mut UCM_UPDATE_TRIED: bool = false;

	Assert(!O_PAGE_IS_LOCAL(blkno));

	state = pg_atomic_read_u64(&header->state);
	while (true)
	{
		pub static mut NEW_STATE: uint64 = std::mem::zeroed();

		if ((state & PAGE_STATE_CHANGE_COUNT_MASK) !=
			(oldState & PAGE_STATE_CHANGE_COUNT_MASK))
		{
			break;
		}
		else
		{
			Assert((state & PAGE_STATE_LIST_TAIL_MASK) != pgprocnum);
			lockerState->status = OPageWaitNonExclusive;
			lockerState->next = (state & PAGE_STATE_LIST_TAIL_MASK);
			newState = state & (~PAGE_STATE_LIST_TAIL_MASK);
			newState |= pgprocnum;
		}

		if (!ucmUpdateTried)
		{
			newState = ucm_update_state(&ppool->ucm, blkno, newState);
			ucmUpdateTried = true;
		}

		if (pg_atomic_compare_exchange_u64(&header->state, &state, newState))
		{
			ucm_after_update_state(&ppool->ucm, blkno, state, newState);
			break;
		}
	}

	pub static mut STATE: return = std::mem::zeroed();
}

//
// Place exclusive lock on the page.  Doesn't block readers before
// page_block_reads() is called.
//

lock_page(OInMemoryBlkno blkno)
{
	pub static mut O_PAGE_WAITER_SHMEM_STATE: *mut lockerState = &lockerStates[MYPROCNUMBER];
	pub static mut PREV_STATE: uint64 = std::mem::zeroed();
	pub static mut EXTRA_WAITS: std::os::raw::c_int = 0;

	// Local pages do not need locking
	if (O_PAGE_IS_LOCAL(blkno))
		return;

	Assert(get_my_locked_page_index(blkno) < 0);

	EA_LOCK_INC(blkno);

	while (true)
	{
		prevState = lock_page_or_queue(blkno, MYPROCNUMBER);

		if (!O_PAGE_STATE_IS_LOCKED(prevState))
			break;

		pgstat_report_wait_start(PG_WAIT_LWLOCK | LWTRANCHE_BUFFER_CONTENT);

		for (;;)
		{
			PGSemaphoreLock(MyProc->sem);
			if (lockerState->status == OPageWaitWakeUp)
				break;
			extraWaits++;
		}

		pgstat_report_wait_end();
	}

	my_locked_page_add(blkno, prevState | PAGE_STATE_LOCKED_FLAG);

	//
// Fix the process wait semaphore's count for any absorbed wakeups.
//
	while (extraWaits-- > 0)
		PGSemaphoreUnlock(MyProc->sem);
}

//
// Place exclusive lock on the page.  Doesn't block readers before
// page_block_reads() is called.
//
OLockPageWithTupleResult
lock_page_with_tuple(desc: &mut BTreeDescr,
					 blkno: &mut OInMemoryBlkno, pageChangeCount: &mut uint32,
					 OTupleXactInfo xactInfo, OTuple tuple)
{
	pub static mut PREV_STATE: uint64 = std::mem::zeroed();
	pub static mut EXTRA_WAITS: std::os::raw::c_int = 0;
	pub static mut O_PAGE_WAITER_SHMEM_STATE: *mut lockerState = &lockerStates[MYPROCNUMBER];
	pub static mut KEY_SERIALIZED: bool = false;
	pub static mut IMG: PageImg = std::mem::zeroed();

	// Local pages do not need locking
	if (O_PAGE_IS_LOCAL(*blkno))
		pub static mut O_LOCK_PAGE_WITH_TUPLE_RESULT_LOCKED: return = std::mem::zeroed();

	img.load = false;
	Assert(get_my_locked_page_index(*blkno) < 0);

	while (true)
	{
		pub static mut LOCK_RESULT: LockPageResult = std::mem::zeroed();

		lockResult = lock_page_or_queue_or_split_detect(desc, blkno,
														pageChangeCount,
														MYPROCNUMBER,
														&img, xactInfo,
														tuple, &prevState,
														&keySerialized);

		if (lockResult == LockPageResultLocked)
		{
			break;
		}
		else if (lockResult == LockPageResultSplitDetected)
		{
			pub static mut O_LOCK_PAGE_WITH_TUPLE_RESULT_REFIND_NEEDED: return = std::mem::zeroed();
		}
		Assert(lockResult == LockPageResultQueued);

		pgstat_report_wait_start(PG_WAIT_LWLOCK | LWTRANCHE_BUFFER_CONTENT);

		for (;;)
		{
			PGSemaphoreLock(MyProc->sem);
			if (lockerState->status == OPageWaitWakeUp)
				break;
			extraWaits++;
		}
		pgstat_report_wait_end();

		//
// Fix the process wait semaphore's count for any absorbed wakeups.
//
		while (extraWaits-- > 0)
			PGSemaphoreUnlock(MyProc->sem);

		if (lockerState->inserted)
		{
			pub static mut UNDO_TYPE: UndoLogType = desc->undoType;

			Assert(keySerialized);
			lockerState->inserted = false;
			if (undoType != UndoLogNone)
			{
				giveup_reserved_undo_size(undoType);
				if (UndoLocationIsValid(lockerState->undoLocation) &&
					!UndoLocationIsValid(curRetainUndoLocations[undoType]))
					curRetainUndoLocations[undoType] = lockerState->undoLocation;

				//
// The lock holder allocated the waiter's undo record on our
// behalf via make_waiter_undo_record(), and stamped its
// location into the tuphdr on the page.  When an INSERT
// doesn't queue, o_btree_modify_insert_update() registers the
// freshly allocated undo location in this backend's
// commandInfos[] via current_command_get_undo_location().
// Here that registration never happens, because we returned
// before reaching that code.  Do it now, otherwise a
// subsequent same-transaction read of the row would call
// undo_location_get_command() with a location below every
// commandInfos[i].undoLocation, tripping its lo >= 0
// assertion (or returning a bogus cid in non-assert builds).
//
				if (undoType == UndoLogRegular &&
					UndoLocationIsValid(lockerState->undoLocation) &&
					!IsParallelWorker())
					update_command_undo_location(o_get_current_command(),
												 lockerState->undoLocation);
			}

			pub static mut O_LOCK_PAGE_WITH_TUPLE_RESULT_INSERTED: return = std::mem::zeroed();
		}
	}

	EA_LOCK_INC(*blkno);

	my_locked_page_add(*blkno, prevState | PAGE_STATE_LOCKED_FLAG);

	pub static mut O_LOCK_PAGE_WITH_TUPLE_RESULT_LOCKED: return = std::mem::zeroed();
}


page_wait_for_read_enable(OInMemoryBlkno blkno)
{
	pub static mut PREV_STATE: uint32 = std::mem::zeroed();
	pub static mut EXTRA_WAITS: std::os::raw::c_int = 0;
	pub static mut O_PAGE_WAITER_SHMEM_STATE: *mut lockerState = &lockerStates[MYPROCNUMBER];

	// Local pages do not need locking
	if (O_PAGE_IS_LOCAL(blkno))
		return;

	while (true)
	{
		prevState = read_enabled_or_queue(blkno, MYPROCNUMBER);

		if (!(prevState & PAGE_STATE_NO_READ_FLAG))
			break;

		pgstat_report_wait_start(PG_WAIT_LWLOCK | LWTRANCHE_BUFFER_CONTENT);

		for (;;)
		{
			PGSemaphoreLock(MyProc->sem);
			if (lockerState->status == OPageWaitWakeUp)
				break;
			extraWaits++;
		}

		pgstat_report_wait_end();
	}

	//
// Fix the process wait semaphore's count for any absorbed wakeups.
//
	while (extraWaits-- > 0)
		PGSemaphoreUnlock(MyProc->sem);

	return;
}

static uint32
page_wait_for_changecount(OInMemoryBlkno blkno, uint32 state)
{
	Page		p = O_GET_IN_MEMORY_PAGE(blkno);
	header: &mut OrioleDBPageHeader = (OrioleDBPageHeader *) p;
	pub static mut CUR_STATE: uint64 = std::mem::zeroed();
	pub static mut EXTRA_WAITS: std::os::raw::c_int = 0;
	pub static mut O_PAGE_WAITER_SHMEM_STATE: *mut lockerState = &lockerStates[MYPROCNUMBER];

	while (true)
	{
		pub static mut EXIT_LOOP: bool = false;

		curState = state_changed_or_queue(blkno, MYPROCNUMBER, state);
		if ((curState & PAGE_STATE_CHANGE_COUNT_MASK) !=
			(state & PAGE_STATE_CHANGE_COUNT_MASK))
		{
			pub static mut CUR_STATE: return = std::mem::zeroed();
		}

		pgstat_report_wait_start(PG_WAIT_LWLOCK | LWTRANCHE_BUFFER_CONTENT);

		for (;;)
		{
			PGSemaphoreLock(MyProc->sem);
			if (lockerState->status == OPageWaitWakeUp)
			{
				curState = pg_atomic_read_u64(&header->state);
				if ((curState & PAGE_STATE_CHANGE_COUNT_MASK) !=
					(state & PAGE_STATE_CHANGE_COUNT_MASK))
					exit_loop = true;
				break;
			}
			extraWaits++;
		}
		if (exit_loop)
			break;

		pgstat_report_wait_end();
	}

	//
// Fix the process wait semaphore's count for any absorbed wakeups.
//
	while (extraWaits-- > 0)
		PGSemaphoreUnlock(MyProc->sem);

	pub static mut CUR_STATE: return = std::mem::zeroed();
}

bool
have_locked_pages()
{
	return (numberOfMyLockedPages > 0);
}

// Wait for a change of the page and lock it.

relock_page(OInMemoryBlkno blkno)
{
	pub static mut STATE: uint64 = std::mem::zeroed();

	// Local pages do not need locking
	if (O_PAGE_IS_LOCAL(blkno))
		return;

	state = my_locked_page_get_state(blkno);
	unlock_page(blkno);

	STOPEVENT(STOPEVENT_RELOCK_PAGE, NULL);

	page_wait_for_changecount(blkno, state);
	lock_page(blkno);
}

//
// Try to lock the given page from concurrent changes.  Returns true on success.
//
bool
try_lock_page(OInMemoryBlkno blkno)
{
	ppool: &mut PagePool = get_ppool_by_blkno(blkno);
	Page		p = O_GET_IN_MEMORY_PAGE(blkno);
	pub static mut STATE: uint64 = std::mem::zeroed();

	// Local pages do not need locking
	if (O_PAGE_IS_LOCAL(blkno))
		pub static mut TRUE: return = std::mem::zeroed();

	state = pg_atomic_fetch_or_u64(&(O_PAGE_HEADER(p)->state),
								   PAGE_STATE_LOCKED_FLAG);

	if (O_PAGE_STATE_IS_LOCKED(state))
		pub static mut FALSE: return = std::mem::zeroed();

	EA_LOCK_INC(blkno);
	my_locked_page_add(blkno, state | PAGE_STATE_LOCKED_FLAG);
	ppool_ucm_inc_usage(ppool, blkno);

	pub static mut TRUE: return = std::mem::zeroed();
}

//
// Declare newly created page as already locked by our process.
//
// No existing callers.
//

delare_page_as_locked(OInMemoryBlkno blkno)
{
	Page		p = O_GET_IN_MEMORY_PAGE(blkno);

	// Local pages do not need locking
	if (O_PAGE_IS_LOCAL(blkno))
		return;

	my_locked_page_add(blkno, pg_atomic_read_u64(&(O_PAGE_HEADER(p)->state)));
}

//
// Check if page is locked.
//
bool
page_is_locked(OInMemoryBlkno blkno)
{
	// Local pages do not need locking
	if (O_PAGE_IS_LOCAL(blkno))
		pub static mut FALSE: return = std::mem::zeroed();

	return (get_my_locked_page_index(blkno) >= 0);
}

//
// Block reads on locked page to prepare it for the modification.
//

page_block_reads(OInMemoryBlkno blkno)
{
	Page		p = O_GET_IN_MEMORY_PAGE(blkno);
	pub static mut STATE: uint64 = std::mem::zeroed();
	pub static mut I: std::os::raw::c_int = 0;

	if (O_PAGE_IS_LOCAL(blkno))
	{
		//
// Local pages don't go through the lock_page / unlock_page path that
// bumps the change count on modification, so a same-backend
// partial_load_chunk() would otherwise miss writes to the page
// between the descent and the iterator's later reads (parentImg
// carries partial state across find_page calls).  No concurrency, so
// a plain RMW on state is enough.
//
		hdr: &mut OrioleDBPageHeader = (OrioleDBPageHeader *) p;
		uint64		old = pg_atomic_read_u64(&hdr->state);
		uint64		newChangeCount = ((old & PAGE_STATE_CHANGE_COUNT_MASK) +
									  PAGE_STATE_CHANGE_COUNT_ONE) &
			PAGE_STATE_CHANGE_COUNT_MASK;

		pg_atomic_write_u64(&hdr->state,
							(old & ~PAGE_STATE_CHANGE_COUNT_MASK) | newChangeCount);
		return;
	}

	i = get_my_locked_page_index(blkno);

	Assert((myLockedPages[i].state & PAGE_STATE_CHANGE_NON_WAITERS_MASK) ==
		   (pg_atomic_read_u64(&(O_PAGE_HEADER(p)->state)) & PAGE_STATE_CHANGE_NON_WAITERS_MASK));

	//
// Idempotent: if reads are already blocked on this locked page, a
// repeated call is a no-op.  Callers may legitimately double up -- e.g.
// o_ppool_free_page() now blocks reads defensively on every free, which
// can stack with a caller (such as free_page()) that already blocked
// them.
//
	if (myLockedPages[i].state & PAGE_STATE_NO_READ_FLAG)
		return;

	state = pg_atomic_fetch_or_u64(&(O_PAGE_HEADER(p)->state), PAGE_STATE_NO_READ_FLAG);
	Assert((state & PAGE_STATE_LOCKED_FLAG));
	myLockedPages[i].state = state | PAGE_STATE_NO_READ_FLAG;
}

int
get_waiters_with_tuples(desc: &mut BTreeDescr,
						OInMemoryBlkno blkno,
						int result[BTREE_PAGE_MAX_SPLIT_ITEMS])
{
	Page		p = O_GET_IN_MEMORY_PAGE(blkno);
	pub static mut PGPROCNUM: uint32 = std::mem::zeroed();
	pub static mut COUNT: std::os::raw::c_int = 0;

	// Local pages do not need locking
	if (O_PAGE_IS_LOCAL(blkno))
		pub static mut 0: return = std::mem::zeroed();

	pgprocnum = pg_atomic_read_u64(&(O_PAGE_HEADER(p)->state)) & PAGE_STATE_LIST_TAIL_MASK;

	while (pgprocnum != PAGE_STATE_INVALID_PROCNO)
	{
		pub static mut O_PAGE_WAITER_SHMEM_STATE: *mut lockerState = &lockerStates[pgprocnum];

		if (lockerState->status == OPageWaitInsert &&
			lockerState->pageChangeCount == O_PAGE_HEADER(p)->pageChangeCount &&
			ORelOidsIsEqual(desc->oids, lockerState->reloids))
		{
			result[count++] = pgprocnum;
			if (count >= BTREE_PAGE_MAX_SPLIT_ITEMS)
			{
				Assert(count == BTREE_PAGE_MAX_SPLIT_ITEMS);
				break;
			}
		}

		pgprocnum = lockerState->next;
	}

	pub static mut COUNT: return = std::mem::zeroed();
}


mark_waiter_tuples_inserted(int procnums[BTREE_PAGE_MAX_SPLIT_ITEMS],
							int count)
{
	pub static mut I: std::os::raw::c_int = 0;

	Assert(count > 0);

	for (i = 0; i < count; i++)
		lockerStates[procnums[i]].inserted = true;

}

//
// Check page before unlocking.
//
fn
unlock_check_page(OInMemoryBlkno blkno)
{
	Page		p = O_GET_IN_MEMORY_PAGE(blkno);

#ifdef CHECK_PAGE_STRUCT
	if (O_GET_IN_MEMORY_PAGEDESC(blkno)->type != oIndexInvalid)
		o_check_page_struct(NULL, p);
#else
	if (O_GET_IN_MEMORY_PAGEDESC(blkno)->type != oIndexInvalid)
	{
		header: &mut BTreePageHeader = (BTreePageHeader *) p;
		pub static mut B_TREE_PAGE_CHUNK_DESC: *mut lastChunk = &header->chunkDesc[header->chunksCount - 1];

		if (SHORT_GET_LOCATION(lastChunk->shortLocation) > header->dataSize ||
			header->dataSize > ORIOLEDB_BLCKSZ)
			elog(PANIC, "broken page: (blkno: %u, p: %p, lastChunk: %u, dataSize: %u)",
				 blkno, p, SHORT_GET_LOCATION(lastChunk->shortLocation),
				 header->dataSize);
	}
#endif

#ifdef CHECK_PAGE_STATS
	{
		//
// XXX: index_oids_get_btree_descr() might expand a hash table under
// critical section.
//
		page_desc: &mut OrioleDBPageDesc = O_GET_IN_MEMORY_PAGEDESC(blkno);

		if (O_PAGE_IS(p, LEAF) && page_desc->type != oIndexInvalid)
		{
			pub static mut OIDS: ORelOids = page_desc->oids;
			pub static mut B_TREE_DESCR: *mut desc = std::ptr::null_mut();

			if (!IS_SYS_TREE_OIDS(oids))
				desc = index_oids_get_btree_descr(oids, page_desc->type);
			else
				desc = get_sys_tree_no_init(oids.reloid);
			if (desc)
				o_check_btree_page_statistics(desc, p);
		}
	}
#endif

#ifdef USE_ASSERT_CHECKING
	if (!O_PAGE_IS(p, LEAF) && OidIsValid(O_GET_IN_MEMORY_PAGEDESC(blkno)->oids.reloid))
	{
		pub static mut ON_DISK: std::os::raw::c_int = 0;
		pub static mut LOC: BTreePageItemLocator = std::mem::zeroed();

		BTREE_PAGE_FOREACH_ITEMS(p, &loc)
		{
			tuphdr: &mut BTreeNonLeafTuphdr = (BTreeNonLeafTuphdr *) BTREE_PAGE_LOCATOR_GET_ITEM(p, &loc);

			if (DOWNLINK_IS_ON_DISK(tuphdr->downlink))
				on_disk++;
		}
		Assert(on_disk == PAGE_GET_N_ONDISK(p));
	}
#endif

	VALGRIND_CHECK_MEM_IS_DEFINED(O_GET_IN_MEMORY_PAGE(blkno), ORIOLEDB_BLCKSZ);
}

//
// unlock_page_internal -- release a previously locked in‑memory page and wake
// any backends that can now proceed.
//
// The waiters are stored in a lock‑less, singly‑linked list.  The tail
// (newest waiter) PGPROC number is packed into the low bits of the 64-bit
// page‑state word.  A successful unlock therefore needs to:
// 1. Walk that list;
// 2. Move every suitable waiter (see `shouldWake`) and at most one
// exclusive waiter to a private wake list;
// 3. Patch the shared list so that the removed waiters vanish from it;
// 4. Publish a new page‑state word with the updated tail via atomic CAS;
// 5. If the CAS fails, process the newly added waiters (if any) and retry;
// 6. Finally, wake up all backends we collected on our private list.
//
// The two auxiliary variables `prevTail` and `prevTailPatch` are the key to
// the logic: if we fail the CAS, the list may already contain our previous
// patch (i.e. `prevTail->next` now points somewhere else).  We detect that
// and re‑apply the patch in the next iteration instead of trying to start
// from scratch (the latter is not possible, because we might already have
// modified the list).
//
fn
unlock_page_internal(OInMemoryBlkno blkno, bool split)
{
	Page		page = O_GET_IN_MEMORY_PAGE(blkno);
	hdr: &mut OrioleDBPageHeader = (OrioleDBPageHeader *) page;

	// Head of our private stack of waiters to wake once the page is unlocked
	pub static mut WAKE_LIST_HEAD: uint32 = PAGE_STATE_INVALID_PROCNO;

	// Bookkeeping needed when the CAS fails and we must retry
	pub static mut PREV_TAIL: uint32 = PAGE_STATE_INVALID_PROCNO;
	pub static mut PREV_TAIL_PATCH: uint32 = PAGE_STATE_INVALID_PROCNO;

	// We may wake **one** exclusive waiter per unlock attempt
	pub static mut EXCLUSIVE_ALREADY_WOKEN: bool = false;
	pub static mut STATE: uint64 = std::mem::zeroed();

	pub static mut PG_USED_FOR_ASSERTS_ONLY: int			expectedWakeCount = 0;
	pub static mut PG_USED_FOR_ASSERTS_ONLY: int			actualWakeCount = 0;

	unlock_check_page(blkno);

	state = pg_atomic_read_u64(&hdr->state);

	for (;;)
	{
		// Snapshot the tail encoded in the state word
		pub static mut TAIL: uint32 = state & PAGE_STATE_LIST_TAIL_MASK;
		pub static mut CUR: uint32 = tail;
		pub static mut PREV: uint32 = PAGE_STATE_INVALID_PROCNO;
		pub static mut NEW_STATE: uint64 = std::mem::zeroed();

		uint32		newTail = tail; // will become the new list tail

		// Remember the first exclusive waiter we may decide to wake
		pub static mut EXCLUSIVE: uint32 = PAGE_STATE_INVALID_PROCNO;
		pub static mut EXCLUSIVE_PREV: uint32 = PAGE_STATE_INVALID_PROCNO;

		// --------------------------------------------------------------
// 1. Walk the waiter list, unlinking suitable lockers on the fly
// --------------------------------------------------------------
		while (cur != prevTail) // stop before the node we patched during the
// previous (failed) iteration
		{
			pub static mut O_PAGE_WAITER_SHMEM_STATE: *mut lock = &lockerStates[cur];

			bool		shouldWake =
				lock->inserted ||
				lock->status == OPageWaitNonExclusive ||
				(split && lock->status == OPageWaitInsert);

			if (shouldWake)
			{
				pub static mut NEXT: uint32 = lock->next;

				// Unlink waiter from shared waiter list
				if (prev == PAGE_STATE_INVALID_PROCNO)
					newTail = next; // removed the first element
				else
					lockerStates[prev].next = next;

				// Push waiter onto our private wake list
				lock->next = wakeListHead;
				wakeListHead = cur;
				expectedWakeCount++;

				cur = next;
				continue;		// stay on the same `prev`
			}

			// Remember the first (oldest) exclusive waiter
			if (!exclusiveAlreadyWoken && exclusive == PAGE_STATE_INVALID_PROCNO)
			{
				exclusive = cur;
				exclusivePrev = prev;
			}

			prev = cur;
			cur = lock->next;
		}

		// ----------------------------------------------------------------
// 2. Optionally move the first exclusive waiter to the wake list
// ----------------------------------------------------------------
		if (exclusive != PAGE_STATE_INVALID_PROCNO && !exclusiveAlreadyWoken)
		{
			pub static mut O_PAGE_WAITER_SHMEM_STATE: *mut lock = &lockerStates[exclusive];

			exclusiveAlreadyWoken = true;

			if (exclusivePrev == PAGE_STATE_INVALID_PROCNO)
				newTail = lock->next;	// exclusive was the first node
			else
				lockerStates[exclusivePrev].next = lock->next;

			// push to wake list
			lock->next = wakeListHead;
			wakeListHead = exclusive;
			expectedWakeCount++;

			if (prev == exclusive)
				prev = exclusivePrev;
		}

		// ----------------------------------------------------------------
// 3. Re‑apply the patch from the previous failed CAS attempt
// ----------------------------------------------------------------
		if (prevTail != prevTailPatch)
		{
			Assert(prevTail != PAGE_STATE_INVALID_PROCNO);

			if (prev == PAGE_STATE_INVALID_PROCNO)
				newTail = prevTailPatch;	// new head is different
			else
			{
				Assert(prev != prevTailPatch);
				lockerStates[prev].next = prevTailPatch;
			}
		}

		// ----------------------------------------------------------------
// 4. Compose and try to publish the new page‑state word
// ----------------------------------------------------------------
		newState = state &
			~(PAGE_STATE_LIST_TAIL_MASK |
			  PAGE_STATE_LOCKED_FLAG |
			  PAGE_STATE_NO_READ_FLAG);

		// Bump change‑counter if reads had been blocked
		if (O_PAGE_STATE_READ_IS_BLOCKED(state))
		{
			uint64		changeCount = (newState & PAGE_STATE_CHANGE_COUNT_MASK);

			newState &= ~PAGE_STATE_CHANGE_COUNT_MASK;
			changeCount += PAGE_STATE_CHANGE_COUNT_ONE;
			changeCount &= PAGE_STATE_CHANGE_COUNT_MASK;
			newState |= changeCount;
		}

		newState |= newTail;

		if (pg_atomic_compare_exchange_u64(&hdr->state, &state, newState))
			break;				// Success!  Exit retry loop

		// ----------------------------------------------------------------
// 5. CAS failed – remember what we did and retry
// ----------------------------------------------------------------
		prevTail = tail;
		prevTailPatch = newTail;
		// `state` now holds the value returned by the failed CAS
	}

	// Cleanup the local list of locked pages
	my_locked_page_del(blkno);

	// --------------------------------------------------------------------
// 6. Waking collected waiters
// --------------------------------------------------------------------
	pg_write_barrier();			// ensure list modifications are visible

	for (uint32 procno = wakeListHead;
		 procno != PAGE_STATE_INVALID_PROCNO;)
	{
		pub static mut O_PAGE_WAITER_SHMEM_STATE: *mut lockState = &lockerStates[procno];
		pub static mut NEXT: uint32 = std::mem::zeroed();
		proc: &mut PGPROC = GetPGProcByNumber(procno);

		next = lockState->next;

		//
// Ensure memory access ordering.  The effect of statement above must
// materialize before waking up the waiter, which must see
// lockState->status == OPageWaitWakeUp and can modify
// lockState->next.
//
		pg_memory_barrier();

		lockState->status = OPageWaitWakeUp;

		//
// Also, ensure woken up waiter will see lockState->status ==
// OPageWaitWakeUp.
//
		pg_memory_barrier();

		PGSemaphoreUnlock(proc->sem);
		actualWakeCount++;

		procno = next;
	}

	Assert(actualWakeCount == expectedWakeCount);
}


unlock_page(OInMemoryBlkno blkno)
{
	// Local pages do not need locking
	if (O_PAGE_IS_LOCAL(blkno))
		return;

	unlock_page_internal(blkno, false);
}

//
// Unlock the page after page split.  Page should be locked before.
//

unlock_page_after_split(OInMemoryBlkno blkno)
{
	// Local pages do not need locking
	if (O_PAGE_IS_LOCAL(blkno))
		return;

	unlock_page_internal(blkno, true);
}

//
// Release all previously acquired page locks one-by-one.
//

release_all_page_locks()
{
	pg_write_barrier();

	while (numberOfMyLockedPages > 0)
		unlock_page(myLockedPages[0].blkno);
}

//
// Register in-progress split.  This split will be marked as incomplete on
// errer cleanup unless it's unregistered before.
//
// Must be called within critical section.
//

btree_register_inprogress_split(OInMemoryBlkno rightBlkno)
{
#ifdef USE_ASSERT_CHECKING
	pub static mut I: std::os::raw::c_int = 0;

	for (i = 0; i < numberOfMyInProgressSplitPages; i++)
		Assert(myInProgressSplitPages[i] != rightBlkno);
#endif
	Assert(CritSectionCount > 0);
	Assert((numberOfMyInProgressSplitPages + 1) <= sizeof(myInProgressSplitPages) / sizeof(myInProgressSplitPages[0]));
	myInProgressSplitPages[numberOfMyInProgressSplitPages++] = rightBlkno;
}

//
// Unregister in-progress split.
//
// Must be calles within critical section.
//

btree_unregister_inprogress_split(OInMemoryBlkno rightBlkno)
{
	pub static mut I: std::os::raw::c_int = 0;

	Assert(CritSectionCount > 0);
	Assert(numberOfMyInProgressSplitPages > 0);
	for (i = 0; i < numberOfMyInProgressSplitPages; i++)
	{
		if (myInProgressSplitPages[i] == rightBlkno)
		{
			numberOfMyInProgressSplitPages--;
			myInProgressSplitPages[i] = myInProgressSplitPages[numberOfMyInProgressSplitPages];
			return;
		}
	}
	Assert(false);
}

//
// Marks all in-progress splits as incomplete.
//

btree_mark_incomplete_splits()
{
	pub static mut I: std::os::raw::c_int = 0;

	for (i = 0; i < numberOfMyInProgressSplitPages; i++)
		btree_split_mark_finished(myInProgressSplitPages[i], true, false);
	numberOfMyInProgressSplitPages = 0;
}

//
// Marks the split as finished.
//
// It sets O_BTREE_FLAG_BROKEN_SPLIT if success = false or removes rightlink
// on the left page.
//
// It does not call modify_page if use_lock = false.
//

btree_split_mark_finished(OInMemoryBlkno rightBlkno, bool use_lock, bool success)
{
	pub static mut B_TREE_PAGE_HEADER: *mut leftHeader = std::ptr::null_mut();
	pub static mut B_TREE_PAGE_HEADER: *mut rightHeader = std::ptr::null_mut();
	rightPageDesc: &mut OrioleDBPageDesc = O_GET_IN_MEMORY_PAGEDESC(rightBlkno);
	pub static mut LEFT_BLKNO: OInMemoryBlkno = std::mem::zeroed();

	// Local pages do not need locking
	if (O_PAGE_IS_LOCAL(rightBlkno))
		use_lock = false;

	leftBlkno = rightPageDesc->leftBlkno;
	Assert(OInMemoryBlknoIsValid(leftBlkno));

	//
// Still need to lock th left page even if we're going to just set
// BROKEN_SPLIT on the right page, because we need to notify waiters in
// o_btree_split_is_incomplete().
//
	if (use_lock)
	{
		while (true)
		{
			lock_page(leftBlkno);

			if (rightPageDesc->leftBlkno == leftBlkno)
				break;

			unlock_page(leftBlkno);
			leftBlkno = rightPageDesc->leftBlkno;
			Assert(OInMemoryBlknoIsValid(leftBlkno));
		}
	}

	lock_page(rightBlkno);

	if (use_lock)
		page_block_reads(leftBlkno);
	page_block_reads(rightBlkno);

	START_CRIT_SECTION();

	leftHeader = (BTreePageHeader *) O_GET_IN_MEMORY_PAGE(leftBlkno);
	rightHeader = (BTreePageHeader *) O_GET_IN_MEMORY_PAGE(rightBlkno);

	Assert(RightLinkIsValid(leftHeader->rightLink));
	Assert(use_lock || success);

	if (success)
	{
		rightHeader->flags &= ~O_BTREE_FLAG_BROKEN_SPLIT;
		leftHeader->rightLink = InvalidRightLink;
		rightPageDesc->leftBlkno = OInvalidInMemoryBlkno;
	}
	else
	{
		Assert(!O_PAGE_IS(O_GET_IN_MEMORY_PAGE(rightBlkno), BROKEN_SPLIT));
		rightHeader->flags |= O_BTREE_FLAG_BROKEN_SPLIT;
	}

	END_CRIT_SECTION();

	unlock_page(rightBlkno);

	if (use_lock)
		unlock_page(leftBlkno);
}

#ifdef CHECK_PAGE_STRUCT

extern  log_btree(desc: &mut BTreeDescr);

//
// Check if page has a consistent structure.
//
fn
o_check_page_struct(desc: &mut BTreeDescr, Page p)
{
	header: &mut BTreePageHeader = (BTreePageHeader *) p;
	int			i,
				j,
				itemsCount;
	LocationIndex endLocation,
				chunkSize;
	pub static mut PREV_CHUNK_HIKEY: OTuple = std::mem::zeroed();

	Assert(header->dataSize <= ORIOLEDB_BLCKSZ);
	Assert(header->hikeysEnd <= header->dataSize);

	O_TUPLE_SET_NULL(prevChunkHikey);

	for (i = 0; i < header->chunksCount; i++)
	{
		pub static mut B_TREE_PAGE_CHUNK_DESC: *mut chunk = &header->chunkDesc[i];
		pub static mut B_TREE_PAGE_CHUNK: *mut chunkData = std::ptr::null_mut();
		pub static mut CHUNK_HIKEY: OTuple = std::mem::zeroed();

		if (O_PAGE_IS(p, RIGHTMOST) && i == header->chunksCount - 1)
		{
			O_TUPLE_SET_NULL(chunkHikey);
		}
		else
		{
			chunkHikey.formatFlags = header->chunkDesc[i].hikeyFlags;
			chunkHikey.data = p + SHORT_GET_LOCATION(header->chunkDesc[i].hikeyShortLocation);
		}

		if (!O_PAGE_IS(p, RIGHTMOST) || i < header->chunksCount - 1)
		{
			Assert((chunk->hikeyFlags & O_TUPLE_FLAGS_FIXED_FORMAT) || !(header->flags & O_BTREE_FLAG_HIKEYS_FIXED));
		}

		if (i > 0)
		{
			pub static mut B_TREE_PAGE_CHUNK_DESC: *mut prevChunk = &header->chunkDesc[i - 1] PG_USED_FOR_ASSERTS_ONLY;

			Assert(chunk->shortLocation >= prevChunk->shortLocation);
			Assert(chunk->offset >= prevChunk->offset);
			Assert(chunk->hikeyShortLocation > prevChunk->hikeyShortLocation);
			Assert(SHORT_GET_LOCATION(chunk->hikeyShortLocation) <= header->hikeysEnd);
			Assert(SHORT_GET_LOCATION(chunk->shortLocation) <= header->dataSize);
			Assert(chunk->offset <= header->itemsCount);
		}
		else
		{
			Assert(SHORT_GET_LOCATION(chunk->shortLocation) == header->hikeysEnd || SHORT_GET_LOCATION(chunk->shortLocation) == BTREE_PAGE_HIKEYS_END(NULL, p));
			Assert(chunk->offset == 0);
			Assert(SHORT_GET_LOCATION(chunk->hikeyShortLocation) == MAXALIGN(offsetof(BTreePageHeader, chunkDesc) + sizeof(BTreePageChunkDesc) * header->chunksCount));
		}

		if (i == header->chunksCount - 1)
		{
			if (!O_PAGE_IS(p, RIGHTMOST))
				Assert(SHORT_GET_LOCATION(chunk->hikeyShortLocation) < header->hikeysEnd);
			itemsCount = header->itemsCount - chunk->offset;
			endLocation = header->dataSize;
		}
		else
		{
			Assert(header->chunkDesc[i + 1].offset <= header->itemsCount);
			Assert(header->chunkDesc[i + 1].offset >= chunk->offset);
			itemsCount = header->chunkDesc[i + 1].offset - chunk->offset;
			endLocation = SHORT_GET_LOCATION(header->chunkDesc[i + 1].shortLocation);
			Assert(endLocation <= header->dataSize);
		}

		chunkData = (BTreePageChunk *) ((Pointer) p + SHORT_GET_LOCATION(chunk->shortLocation));
		chunkSize = endLocation - SHORT_GET_LOCATION(chunk->shortLocation);
		Assert(MAXALIGN(sizeof(LocationIndex) * itemsCount) <= chunkSize);

		for (j = 0; j < itemsCount; j++)
		{
			if (!(i == 0 && j == 0 && !O_PAGE_IS(p, LEAF)))
			{
				Assert((ITEM_GET_FLAGS(chunkData->items[j]) & O_TUPLE_FLAGS_FIXED_FORMAT) || (chunk->chunkKeysFixed == 0));
			}
			Assert(ITEM_GET_OFFSET(chunkData->items[j]) >= MAXALIGN(sizeof(LocationIndex) * itemsCount));
			Assert(ITEM_GET_OFFSET(chunkData->items[j]) <= chunkSize);
			if (j > 0)
				Assert(ITEM_GET_OFFSET(chunkData->items[j]) >= ITEM_GET_OFFSET(chunkData->items[j - 1]));
			if (j < itemsCount - 1 && O_PAGE_IS(p, LEAF) && ITEM_GET_FLAGS(chunkData->items[j]) == 0)
				Assert(ITEM_GET_OFFSET(chunkData->items[j]) < ITEM_GET_OFFSET(chunkData->items[j + 1]));
			if (desc)
			{
				pub static mut TUPLE: OTuple = std::mem::zeroed();
				pub static mut LEN: std::os::raw::c_int = 0;

				tuple.formatFlags = ITEM_GET_FLAGS(chunkData->items[j]);
				if (O_PAGE_IS(p, LEAF))
				{
					tuple.data = (Pointer) chunkData + ITEM_GET_OFFSET(chunkData->items[j]) + BTreeLeafTuphdrSize;
					len = BTreeLeafTuphdrSize + o_btree_len(desc, tuple, OTupleLength);
					if (!O_TUPLE_IS_NULL(chunkHikey))
						Assert(o_btree_cmp(desc, &tuple, BTreeKeyLeafTuple, &chunkHikey, BTreeKeyNonLeafKey) < 0);
					if (!O_TUPLE_IS_NULL(prevChunkHikey))
						Assert(o_btree_cmp(desc, &tuple, BTreeKeyLeafTuple, &prevChunkHikey, BTreeKeyNonLeafKey) >= 0);
				}
				else
				{
#ifdef ORIOLEDB_CUT_FIRST_KEY
					if (i == 0 && j == 0)
					{
						len = BTreeNonLeafTuphdrSize;
						O_TUPLE_SET_NULL(tuple);
					}
					else
#endif
					{
						tuple.data = (Pointer) chunkData + ITEM_GET_OFFSET(chunkData->items[j]) + BTreeNonLeafTuphdrSize;
						len = BTreeNonLeafTuphdrSize + o_btree_len(desc, tuple, OKeyLength);
					}
					if (!O_TUPLE_IS_NULL(chunkHikey) && !O_TUPLE_IS_NULL(tuple))
						Assert(o_btree_cmp(desc, &tuple, BTreeKeyNonLeafKey, &chunkHikey, BTreeKeyNonLeafKey) < 0);
					if (!O_TUPLE_IS_NULL(prevChunkHikey) && !O_TUPLE_IS_NULL(tuple))
						Assert(o_btree_cmp(desc, &tuple, BTreeKeyNonLeafKey, &prevChunkHikey, BTreeKeyNonLeafKey) >= 0);
				}

				if (j < itemsCount - 1)
					Assert(ITEM_GET_OFFSET(chunkData->items[j]) + len <= ITEM_GET_OFFSET(chunkData->items[j + 1]));
				else
					Assert(ITEM_GET_OFFSET(chunkData->items[j]) + len <= chunkSize);

			}
		}

		prevChunkHikey = chunkHikey;
	}

}
#endif

#ifdef CHECK_PAGE_STATS

//
// Check if precalculated number of vacated bytes for leaf pages and number
// of disk downlinks for non-leaf pages is correct.
//
fn
o_check_btree_page_statistics(desc: &mut BTreeDescr, Pointer p)
{
	if (O_PAGE_IS(p, LEAF))
	{
		pub static mut N_VACATED_BYTES: std::os::raw::c_int = 0;

		nVacatedBytes = PAGE_GET_N_VACATED(p);
		o_btree_page_calculate_statistics(desc, p);

		Assert(nVacatedBytes == PAGE_GET_N_VACATED(p));
	}
	else
	{
		pub static mut N_DISK_DOWNLINKS: std::os::raw::c_int = 0;

		nDiskDownlinks = PAGE_GET_N_ONDISK(p);
		o_btree_page_calculate_statistics(desc, p);

		Assert(nDiskDownlinks == PAGE_GET_N_ONDISK(p));
	}
}
#endif