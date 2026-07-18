use crate::access::nbtree;
use crate::btree::io;
use crate::btree::iterator;
use crate::btree::modify;
use crate::btree::undo;
use crate::catalog::free_extents;
use crate::catalog::o_indices;
use crate::catalog::o_sys_cache;
use crate::catalog::o_tables;
use crate::catalog::pg_opfamily;
use crate::catalog::sys_trees;
use crate::checkpoint::checkpoint;
use crate::common::hashfn;
use crate::executor::functions;
use crate::funcapi;
use crate::orioledb;
use crate::parser::parse_coerce;
use crate::pgstat;
use crate::recovery::recovery;
use crate::tableam::handler;
use crate::tableam::toast;
use crate::tableam::tree;
use crate::transam::undo;
use crate::tuple::slot;
use crate::utils::builtins;
use crate::utils::fmgrtab;
use crate::utils::lsyscache;
use crate::utils::memutils;
use crate::utils::page_pool;
use crate::utils::resowner;
use crate::utils::stopevent;
use crate::utils::syscache;
use pgrx::pg_sys;

// -------------------------------------------------------------------------
//
// descr.c
// Routines for handling descriptors of orioledb trees.
//
// Copyright (c) 2021-2026, Oriole DB Inc.
// Copyright (c) 2025-2026, Supabase Inc.
//
// IDENTIFICATION
// contrib/orioledb/src/tableam/descr.c
//
// -------------------------------------------------------------------------
//

fn o_invalidate_comparator_cache(Oid opfamily, Oid lefttype,
										  Oid righttype);

typedef struct
{
	pub static mut HEADER: OnCommitUndoStackItem = std::mem::zeroed();
	pub static mut OPFAMILY: Oid = std::mem::zeroed();
	pub static mut LEFTTYPE: Oid = std::mem::zeroed();
	pub static mut RIGHTTYPE: Oid = std::mem::zeroed();
} InvalidateComparatorUndoStackItem;

static get_index_descr: &mut OIndexDescr(ORelOids ixOids, OIndexType ixType,
									bool miss_ok, OTableFetchContext ctx,  *o_table_source, OTableSource source);
static bool o_table_descr_fill_indices(descr: &mut OTableDescr, table: &mut OTable, snapshot: &mut OSnapshot);
fn init_shared_root_info(pool: &mut PagePool,
								  sharedRootInfo: &mut SharedRootInfo);
fn o_invalidate_descrs_internal(Oid datoid, Oid reloid, Oid relfilenode);
static bool o_tree_init_free_extents(desc: &mut BTreeDescr);
static o_find_opclass_comparator: &mut OComparator(opclass: &mut OOpclass, Oid collation,
											  Oid exacttype);
static inline o_find_cached_comparator: &mut OComparator(key: &mut OComparatorKey);
static inline o_add_comparator_to_cache: &mut OComparator(comparator: &mut OComparator);
static bool recreate_table_descr(descr: &mut OTableDescr);
fn recreate_index_descr(descr: &mut OIndexDescr);
static o_find_exclusion_op_fn: &mut OExclusionFn(Oid exclusion_op);
static inline o_find_cached_exclusion_fn: &mut OExclusionFn(Oid exclusion_op);
static inline o_add_exclusion_fn_to_cache: &mut OExclusionFn(exclusion_fn: &mut OExclusionFn);
static o_find_hash_fn: &mut OHashFn(Oid hash_fn_oid, Oid datoid);
static inline o_find_cached_hash_fn: &mut OHashFn(key: &mut OHashFnKey);
static inline o_add_hash_fn_to_cache: &mut OHashFn(hash_fn: &mut OHashFn);

PG_FUNCTION_INFO_V1(orioledb_get_table_descrs);
PG_FUNCTION_INFO_V1(orioledb_get_index_descrs);
PG_FUNCTION_INFO_V1(orioledb_get_evicted_trees);

typedef struct DeferredDescrInvalidation
{
	pub static mut DATOID: Oid = std::mem::zeroed();
	pub static mut RELOID: Oid = std::mem::zeroed();
	pub static mut RELFILENODE: Oid = std::mem::zeroed();
} DeferredDescrInvalidation;

//
// When true, invalidation messages arriving via o_invalidate_descrs() are
// saved instead of being processed immediately.  This is set while executing
// a comparator function inside o_call_comparator(), because the comparator
// may trigger AcceptInvalidationMessages() which processes sinval messages,
// which in turn may call o_invalidate_descrs().  Processing those
// invalidations inside a comparator is unsafe (it can read catalogs while
// the caller holds page locks), so we save them and replay later.
//
static mut SAVING_INVAL_MESSAGES: bool = false;
static mut LIST: *mut saved_descr_invals = NIL;

struct OComparatorKey
{
	pub static mut OPFAMILY: Oid = std::mem::zeroed();
	pub static mut LEFTTYPE: Oid = std::mem::zeroed();
	pub static mut RIGHTTYPE: Oid = std::mem::zeroed();
	pub static mut EXACTTYPE: Oid = std::mem::zeroed();
	pub static mut COLLATION: Oid = std::mem::zeroed();
};

struct OComparator
{
	pub static mut KEY: OComparatorKey = std::mem::zeroed();
	pub static mut HAVE_SORT_SUPPORT: bool = false;

	// Filled when haveSortSupport == false
	pub static mut FINFO: FmgrInfo = std::mem::zeroed();

	// Filled when haveSortSupport == true
	pub static mut SSUP_CXT: MemoryContext = std::mem::zeroed();
		   *ssup_extra;
	int			(*ssup_comparator) (Datum x, Datum y, SortSupport ssup);
};

static mut HTAB: *mut oTableDescrHash = std::ptr::null_mut();
static mut HTAB: *mut oIndexDescrHash = std::ptr::null_mut();
static mut HTAB: *mut comparatorCache = std::ptr::null_mut();
static mut HTAB: *mut exclusionFnCache = std::ptr::null_mut();
static mut HTAB: *mut hashFnCache = std::ptr::null_mut();

//
// Backend-local hash of SharedRootInfo for trees backed by LocalPagePool
// (temp tables).  Their pages carry BLKNO_LOCAL_BIT and are meaningless to
// other backends, so they must not reach SYS_TREES_SHARED_ROOT_INFO.
//
static mut HTAB: *mut localSharedRootInfoHash = std::ptr::null_mut();
static OComparatorKey lastkey = {0};
static mut O_COMPARATOR: *mut lastcmp = std::ptr::null_mut();
static mut DESCR_CXT: MemoryContext = std::ptr::null_mut();
static mut LAST_EXCLUSION_OP: Oid = InvalidOid;
static mut O_EXCLUSION_FN: *mut last_exclusion_fn = std::ptr::null_mut();
static OHashFnKey last_hash_fn_key = {0};
static mut O_HASH_FN: *mut last_hash_fn = std::ptr::null_mut();
OHashFn		o_default_hash_fn = {.key = {.hash_fn_oid = O_DEFAULT_HASH_FN_OID}};

fn o_find_toastable_attrs(tableDescr: &mut OTableDescr);

//
// Default context for fetching table/index descriptors from system trees.
//
// Uses o_non_deleted_snapshot so that trees deleted by uncommitted
// (sub-)transactions are still accessible -- they may become visible again
// on rollback.
//
OTableFetchContext default_table_fetch_context = {.snapshot = &o_non_deleted_snapshot,.version = O_TABLE_INVALID_VERSION};

//
// Creates shared root info.  But insertion into shared cache is performed by
// table_descr_init_tree function.
//
static SharedRootInfo *
create_shared_root_info(pool: &mut PagePool, key: &mut SharedRootInfoKey)
{
	pub static mut SHARED_ROOT_INFO: *mut sharedRootInfo = std::ptr::null_mut();

	sharedRootInfo = palloc0(sizeof(SharedRootInfo));
	sharedRootInfo->key = *key;
	init_shared_root_info(pool, sharedRootInfo);
	pub static mut SHARED_ROOT_INFO: return = std::mem::zeroed();
}

static HTAB *
get_local_shared_root_info_hash()
{
	if (localSharedRootInfoHash == NULL)
	{
		pub static mut CTL: HASHCTL = std::mem::zeroed();

		MemSet(&ctl, 0, sizeof(ctl));
		ctl.keysize = sizeof(SharedRootInfoKey);
		ctl.entrysize = sizeof(SharedRootInfo);
		ctl.hcxt = TopMemoryContext;
		localSharedRootInfoHash = hash_create("OrioleDB local shared root info",
											  8, &ctl,
											  HASH_ELEM | HASH_BLOBS | HASH_CONTEXT);
	}
	pub static mut LOCAL_SHARED_ROOT_INFO_HASH: return = std::mem::zeroed();
}

static SharedRootInfo *
find_local_shared_root_info(key: &mut SharedRootInfoKey)
{
	pub static mut SHARED_ROOT_INFO: *mut entry = std::ptr::null_mut();
	pub static mut SHARED_ROOT_INFO: *mut copy = std::ptr::null_mut();
	pub static mut FOUND: bool = false;

	if (localSharedRootInfoHash == NULL)
		pub static mut NULL: return = std::mem::zeroed();

	entry = (SharedRootInfo *) hash_search(localSharedRootInfoHash, key,
										   HASH_FIND, &found);
	if (!found)
		pub static mut NULL: return = std::mem::zeroed();

	copy = (SharedRootInfo *) palloc(sizeof(SharedRootInfo));
	memcpy(copy, entry, sizeof(SharedRootInfo));
	pub static mut COPY: return = std::mem::zeroed();
}

fn
insert_local_shared_root_info(info: &mut SharedRootInfo)
{
	hash: &mut HTAB = get_local_shared_root_info_hash();
	pub static mut SHARED_ROOT_INFO: *mut entry = std::ptr::null_mut();
	pub static mut FOUND: bool = false;

	entry = (SharedRootInfo *) hash_search(hash, &info->key, HASH_ENTER, &found);
	Assert(!found);
	memcpy(entry, info, sizeof(SharedRootInfo));
}

static bool
drop_local_shared_root_info(key: &mut SharedRootInfoKey)
{
	pub static mut FOUND: bool = false;

	if (localSharedRootInfoHash == NULL)
		pub static mut FALSE: return = std::mem::zeroed();

	() hash_search(localSharedRootInfoHash, key, HASH_REMOVE, &found);
	pub static mut FOUND: return = std::mem::zeroed();
}

EvictedTreeData *
read_evicted_data(Oid datoid, Oid relnode, bool delete)
{
	pub static mut KEY: SharedRootInfoKey = std::mem::zeroed();
	pub static mut KEY_TUPLE: OTuple = std::mem::zeroed();
	pub static mut RESULT: OTuple = std::mem::zeroed();

	//
// Don't do lookup for system trees.  This is essential for initialization
// sequence.  This is correct because we don't evict root pages of system
// trees.
//
	if (datoid == SYS_TREES_DATOID)
		pub static mut NULL: return = std::mem::zeroed();

	key.datoid = datoid;
	key.relnode = relnode;
	keyTuple.formatFlags = 0;
	keyTuple.data = (Pointer) &key;

	result = o_btree_find_tuple_by_key(get_sys_tree(SYS_TREES_EVICTED_DATA),
									   &keyTuple, BTreeKeyNonLeafKey,
									   &o_in_progress_snapshot, NULL,
									   CurrentMemoryContext, NULL);
	if (O_TUPLE_IS_NULL(result))
		pub static mut NULL: return = std::mem::zeroed();

	if (delete)
	{
		pub static mut PG_USED_FOR_ASSERTS_ONLY: bool		success = std::mem::zeroed();

		success = o_btree_autonomous_delete(get_sys_tree(SYS_TREES_EVICTED_DATA),
											keyTuple, BTreeKeyNonLeafKey, NULL);
		Assert(success);
	}

	return (EvictedTreeData *) result.data;
}


insert_evicted_data(data: &mut EvictedTreeData)
{
	pub static mut TUPLE: OTuple = std::mem::zeroed();
	pub static mut PG_USED_FOR_ASSERTS_ONLY: bool		success = std::mem::zeroed();

	tuple.formatFlags = 0;
	tuple.data = (Pointer) data;

	success = o_btree_autonomous_insert(get_sys_tree(SYS_TREES_EVICTED_DATA),
										tuple);
	Assert(success);
}

Datum
orioledb_get_evicted_trees(PG_FUNCTION_ARGS)
{
	rsinfo: &mut ReturnSetInfo = (ReturnSetInfo *) fcinfo->resultinfo;
	pub static mut TUPDESC: TupleDesc = std::mem::zeroed();
	pub static mut TUPLESTORESTATE: *mut tupstore = std::ptr::null_mut();
	pub static mut PER_QUERY_CTX: MemoryContext = std::mem::zeroed();
	pub static mut OLDCONTEXT: MemoryContext = std::mem::zeroed();
	pub static mut B_TREE_ITERATOR: *mut it = std::ptr::null_mut();

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

	it = o_btree_iterator_create(get_sys_tree(SYS_TREES_EVICTED_DATA),
								 NULL, BTreeKeyNone,
								 &o_in_progress_snapshot, ForwardScanDirection);

	while (true)
	{
		pub static mut TUPLE: OTuple = std::mem::zeroed();
		pub static mut TUPLE_CSN: CommitSeqNo = std::mem::zeroed();
		Datum		values[4];
		bool		nulls[4] = {false};
		pub static mut EVICTED_TREE_DATA: *mut data = std::ptr::null_mut();

		tuple = o_btree_iterator_fetch(it, &tupleCsn, NULL,
									   BTreeKeyNone, false, NULL);
		if (O_TUPLE_IS_NULL(tuple))
			break;

		data = (EvictedTreeData *) tuple.data;
		values[0] = ObjectIdGetDatum(data->key.datoid);
		values[1] = ObjectIdGetDatum(data->key.relnode);
		values[2] = UInt64GetDatum(data->file_header.rootDownlink);
		values[3] = UInt64GetDatum(data->file_header.datafileLength);
		tuplestore_putvalues(tupstore, tupdesc, values, nulls);
	}

	btree_iterator_free(it);

	return (Datum) 0;
}

//
// OTableDescr* BTrees are created without shared memory initialization.
// Sequence buffers files, data rootInfo file are not initialized too. There are
// reasons for it:
//
// 1. Long queries may do not use all indices.
// 2. In some cases no sense to initialize BTree memory if it not exists.
//
// We can load shared memory in-place, in low-level BTree code
// but it more complicated approach. It will be harder to understand and debug.
//
// To avoid concurrency problems with eviction/cleanup table this call must be
// under AccessShareLock (See o_tables.h/o_tables_rel_lock()).
//
static bool
o_btree_load_shmem_internal(desc: &mut BTreeDescr, bool checkpoint)
{
	pub static mut KEY: SharedRootInfoKey = std::mem::zeroed();
	pub static mut SHARED_ROOT_INFO: *mut sharedRootInfo = std::ptr::null_mut();
	bool		was_evicted,
				is_compressed,
				init_extents,
				pub static mut PG_USED_FOR_ASSERTS_ONLY: inserted = std::mem::zeroed();
	pub static mut LOCK_NO: std::os::raw::c_int = 0;
	pub static mut HAS_LOCK: bool = false;
	pub static mut IS_TEMP: bool = false;

	Assert(desc != NULL);
	if (!ORelOidsIsValid(desc->oids) || IS_SYS_TREE_OIDS(desc->oids))
		pub static mut TRUE: return = std::mem::zeroed();

	// easy case: shared memory is initialized
	if (ORootPageIsValid(desc) && OMetaPageIsValid(desc))
		pub static mut TRUE: return = std::mem::zeroed();

	is_temp = (desc->storageType == BTreeStorageTemporary);

	memset(&key, 0, sizeof(SharedRootInfoKey));
	key.datoid = desc->oids.datoid;
	key.relnode = desc->oids.relnode;

	//
// evictable_tree_init() needs that.  Initialized it before we get one of
// checkpoint_state->oSharedRootInfoInsertLocks.
//
	() get_sys_tree(SYS_TREES_CHKP_NUM);

	sharedRootInfo = o_find_shared_root_info(&key);
	if (sharedRootInfo == NULL)
	{
		//
// Deletion from SYS_TREES_SHARED_ROOT_INFO comes before applying undo
// records to SYS_TREES_O_INDICES.  So, this situation is possible in
// checkpointer due to concurrent deletion.  Just give up then.
//
		if (checkpoint && tree_is_under_checkpoint(desc))
			pub static mut FALSE: return = std::mem::zeroed();

		// ---
// Reserve 8 pages:
//
// - root page
// - meta page
// - 2 for nextChkp seq bufs
// - 2 for tmp seq bufs
// - 2 for free seq bufs
//
		ppool_reserve_pages(desc->ppool, PPOOL_RESERVE_META, 8);

		//
// Temporary trees live in a backend-local hash, so the shared insert
// lock is unnecessary (and a no-op re-lookup would be too).
//
		if (!is_temp)
		{
			lockNo = tag_hash(&key, sizeof(key)) % SHARED_ROOT_INFO_INSERT_NUM_LOCKS;
			LWLockAcquire(&checkpoint_state->oSharedRootInfoInsertLocks[lockNo],
						  LW_EXCLUSIVE);
			hasLock = true;
			sharedRootInfo = o_find_shared_root_info(&key);
		}
	}

	if (sharedRootInfo && sharedRootInfo->placeholder)
	{
		if (hasLock)
		{
			LWLockRelease(&checkpoint_state->oSharedRootInfoInsertLocks[lockNo]);
			ppool_release_reserved(desc->ppool, PPOOL_RESERVE_META);
		}
		pfree(sharedRootInfo);
		pub static mut FALSE: return = std::mem::zeroed();
	}

	if (sharedRootInfo == NULL)
	{
		pub static mut SHARED_ROOT_INFO_TUPLE: OTuple = std::mem::zeroed();

		// tries to create SharedRootInfo
		sharedRootInfo = create_shared_root_info(desc->ppool, &key);
		desc->rootInfo = sharedRootInfo->rootInfo;
		Assert(desc->storageType == BTreeStoragePersistence ||
			   desc->storageType == BTreeStorageTemporary ||
			   desc->storageType == BTreeStorageUnlogged);
		if (desc->storageType == BTreeStoragePersistence ||
			desc->storageType == BTreeStorageUnlogged)
		{
			checkpointable_tree_init(desc, true, &was_evicted);
		}
		else if (desc->storageType == BTreeStorageTemporary)
		{
			evictable_tree_init(desc, true, &was_evicted);
		}
		is_compressed = OCompressIsValid(desc->compress);
		desc->rootInfo = sharedRootInfo->rootInfo;

		init_extents = false;
		if (is_compressed && !was_evicted)
		{
			init_extents = true;

			//
// We should prevent iteration through free extentents list by the
// checkpointer until free extents is not completely initialized
// yet.
//
			LWLockAcquire(&BTREE_GET_META(desc)->copyBlknoLock, LW_SHARED);
		}

		if (is_temp)
		{
			insert_local_shared_root_info(sharedRootInfo);
		}
		else
		{
			sharedRootInfoTuple.data = (Pointer) sharedRootInfo;
			sharedRootInfoTuple.formatFlags = 0;
			inserted = o_btree_autonomous_insert(get_sys_tree(SYS_TREES_SHARED_ROOT_INFO),
												 sharedRootInfoTuple);
			Assert(inserted);
		}

		if (init_extents)
		{
			//
// The loader of an index fills the free extents list.
//
			if (!o_tree_init_free_extents(desc))
			{
				LWLockRelease(&BTREE_GET_META(desc)->copyBlknoLock);
				elog(FATAL,
					 "unable to read free extents file %s",
					 get_seq_buf_filename(&desc->freeBuf.tag));
			}
			LWLockRelease(&BTREE_GET_META(desc)->copyBlknoLock);
		}
	}
	else
	{
		//
// o_btree_load_shmem() must be called only under relation locks, in
// this state BTree can not be evicted and removed from ShareDescr
// cache because AccessExclusiveLock needed for this actions.
//
		Assert(OInMemoryBlknoIsValid(sharedRootInfo->rootInfo.rootPageBlkno));
		Assert(OInMemoryBlknoIsValid(sharedRootInfo->rootInfo.metaPageBlkno));

		desc->rootInfo = sharedRootInfo->rootInfo;

		if (desc->storageType == BTreeStoragePersistence || desc->storageType == BTreeStorageUnlogged)
		{
			checkpointable_tree_init(desc, false, NULL);
		}
		else if (desc->storageType == BTreeStorageTemporary)
		{
			evictable_tree_init(desc, false, NULL);
		}

	}

	if (hasLock)
		LWLockRelease(&checkpoint_state->oSharedRootInfoInsertLocks[lockNo]);

	Assert(sharedRootInfo != NULL);
	Assert(!sharedRootInfo->placeholder);
	pfree(sharedRootInfo);
	ppool_release_reserved(desc->ppool, PPOOL_RESERVE_META);
	pub static mut TRUE: return = std::mem::zeroed();
}


o_btree_load_shmem(desc: &mut BTreeDescr)
{
	pub static mut PG_USED_FOR_ASSERTS_ONLY: bool		result = std::mem::zeroed();

	result = o_btree_load_shmem_internal(desc, false);
	Assert(result == true);
}

bool
o_btree_load_shmem_checkpoint(desc: &mut BTreeDescr)
{
	return o_btree_load_shmem_internal(desc, true);
}

//
// Returns false if BTree does not exist in shared memory.
//
// Same to o_btree_load_shmem() but it does not create a BTree in shared
// memory. Must be called under relation locks too.
//
bool
o_btree_try_use_shmem(desc: &mut BTreeDescr)
{
	Assert(ORelOidsIsValid(desc->oids));

	if (!ORootPageIsValid(desc) || !OMetaPageIsValid(desc))
	{
		pub static mut KEY: SharedRootInfoKey = std::mem::zeroed();
		pub static mut SHARED_ROOT_INFO: *mut shared = std::ptr::null_mut();

		key.datoid = desc->oids.datoid;
		key.relnode = desc->oids.relnode;

		shared = o_find_shared_root_info(&key);
		if (shared == NULL)
			pub static mut FALSE: return = std::mem::zeroed();

		if (shared->placeholder)
			pub static mut FALSE: return = std::mem::zeroed();

		Assert(OInMemoryBlknoIsValid(shared->rootInfo.rootPageBlkno));
		Assert(OInMemoryBlknoIsValid(shared->rootInfo.metaPageBlkno));

		desc->rootInfo = shared->rootInfo;

		if (desc->storageType == BTreeStoragePersistence || desc->storageType == BTreeStorageUnlogged)
		{
			checkpointable_tree_init(desc, false, NULL);
		}
		else if (desc->storageType == BTreeStorageTemporary)
		{
			evictable_tree_init(desc, false, NULL);
		}
		pfree(shared);
	}
	pub static mut TRUE: return = std::mem::zeroed();
}

//
// Appends extents from free blocks file to the free extents list.
//
static bool
o_tree_init_free_extents(desc: &mut BTreeDescr)
{
	metaPageBlkno: &mut BTreeMetaPage = BTREE_GET_META(desc);
	uint64		num_free_blocks = pg_atomic_read_u64(&metaPageBlkno->numFreeBlocks);
	pub static mut FILE: File = std::mem::zeroed();
	pub static mut CHAR: *mut filename = std::ptr::null_mut();

	Assert(OCompressIsValid(desc->compress));

	if (num_free_blocks == 0)
		pub static mut TRUE: return = std::mem::zeroed();

	filename = get_seq_buf_filename(&desc->freeBuf.tag);
	file = PathNameOpenFile(filename, O_RDONLY | PG_BINARY);
	pfree(filename);

	if (file >= 0)
	{
		pub static mut FILE_EXTENT: *mut extent = std::ptr::null_mut();
		off_t		offset = sizeof(CheckpointFileHeader),
					bytes_read,
					i;
		char		buf[ORIOLEDB_BLCKSZ];

		do
		{
			bytes_read = OFileRead(file, buf, ORIOLEDB_BLCKSZ, offset,
								   WAIT_EVENT_DATA_FILE_READ);
			offset += bytes_read;
			if (bytes_read % sizeof(FileExtent) > 0)
				break;

			for (i = 0; i < bytes_read; i += sizeof(FileExtent))
			{
				extent = (FileExtent *) (buf + i);
				if (extent->len > 1)
					pg_atomic_fetch_add_u64(&metaPageBlkno->numFreeBlocks, (uint64) extent->len - 1);

				free_extent(desc, *extent);
				num_free_blocks--;
			}
		} while (num_free_blocks > 0 && bytes_read == ORIOLEDB_BLCKSZ);
		FileClose(file);

		pub static mut NUM_FREE_BLOCKS: return = = 0;
	}

	pub static mut FALSE: return = std::mem::zeroed();
}

fn
index_descr_free(tree: &mut OIndexDescr)
{
	if (tree->old_leaf_slot)
	{
		ExecDropSingleTupleTableSlot(tree->old_leaf_slot);
		tree->old_leaf_slot = NULL;
	}
	if (tree->new_leaf_slot)
	{
		ExecDropSingleTupleTableSlot(tree->new_leaf_slot);
		tree->new_leaf_slot = NULL;
	}
	if (tree->index_slot)
	{
		ExecDropSingleTupleTableSlot(tree->index_slot);
		tree->index_slot = NULL;
	}
	if (tree->leafTupdesc)
	{
		FreeTupleDesc(tree->leafTupdesc);
		tree->leafTupdesc = NULL;
	}
	if (tree->nonLeafTupdesc)
	{
		FreeTupleDesc(tree->nonLeafTupdesc);
		tree->nonLeafTupdesc = NULL;
	}
	if (tree->itupdesc)
	{
		FreeTupleDesc(tree->itupdesc);
		tree->itupdesc = NULL;
	}
	if (tree->econtext)
	{
		FreeExprContext(tree->econtext, false);
		tree->econtext = NULL;
	}
	if (tree->index_mctx)
	{
		MemoryContextDelete(tree->index_mctx);
		tree->index_mctx = NULL;
	}
	checkpointable_tree_free(&tree->desc);
}

fn
index_descr_delete_from_hash(tree: &mut OIndexDescr)
{
	pub static mut FOUND: bool = false;

	index_descr_free(tree);

	elog(DEBUG3, "index descr hash delete index (%u, %u, %u)",
		 tree->oids.datoid,
		 tree->oids.reloid,
		 tree->oids.relnode);

	() hash_search(oIndexDescrHash, &tree->oids,
					   HASH_REMOVE, &found);
	Assert(found);
}

fn
table_descr_free(descr: &mut OTableDescr)
{
	pub static mut I: std::os::raw::c_int = 0;

	elog(DEBUG3, "index descr hash delete for (%u, %u, %u)",
		 descr->oids.datoid,
		 descr->oids.reloid,
		 descr->oids.relnode);

	if (descr->toast)
	{
		descr->toast->refcnt--;
		if (!descr->toast->valid && descr->toast->refcnt == 0)
			index_descr_delete_from_hash(descr->toast);
	}

	if (descr->indices)
	{
		for (i = 0; i < descr->nIndices; i++)
			if (descr->indices[i])
			{
				descr->indices[i]->refcnt--;
				if (!descr->indices[i]->valid && descr->indices[i]->refcnt == 0)
					index_descr_delete_from_hash(descr->indices[i]);
			}
		pfree(descr->indices);
	}

	if (descr->oldTuple)
		ExecDropSingleTupleTableSlot(descr->oldTuple);
	if (descr->newTuple)
		ExecDropSingleTupleTableSlot(descr->newTuple);
	if (descr->tupdesc)
		FreeTupleDesc(descr->tupdesc);
}


o_free_tmp_table_descr(descr: &mut OTableDescr)
{
	pub static mut I: std::os::raw::c_int = 0;

	if (descr->toast)
	{
		index_descr_free(descr->toast);
		pfree(descr->toast);
	}

	if (descr->indices)
	{
		for (i = 0; i < descr->nIndices; i++)
		{
			index_descr_free(descr->indices[i]);
			pfree(descr->indices[i]);
		}
		pfree(descr->indices);
	}

	if (descr->oldTuple)
		ExecDropSingleTupleTableSlot(descr->oldTuple);
	if (descr->newTuple)
		ExecDropSingleTupleTableSlot(descr->newTuple);
	if (descr->tupdesc)
		FreeTupleDesc(descr->tupdesc);
}

fn
table_descr_delete_from_hash(descr: &mut OTableDescr)
{
	pub static mut FOUND: bool = false;

	table_descr_free(descr);
	() hash_search(oTableDescrHash, &descr->oids,
					   HASH_REMOVE, &found);
	Assert(found);
}

fn
fill_table_descr_common_fields(descr: &mut OTableDescr, o_table: &mut OTable)
{
	pub static mut OLD_CONTEXT: MemoryContext = std::mem::zeroed();
	pub static mut REFCNT: std::os::raw::c_int = 0;

	refcnt = descr->refcnt;
	memset(descr, 0, sizeof(OTableDescr));
	old_context = MemoryContextSwitchTo(descrCxt);
	descr->refcnt = refcnt;
	descr->oids = o_table->oids;
	descr->version = o_table->version;
	descr->tablespace = o_table->tablespace;
	descr->tupdesc = o_table_tupdesc(o_table);
	descr->oldTuple = MakeSingleTupleTableSlot(descr->tupdesc,
											   &TTSOpsOrioleDB);
	descr->newTuple = MakeSingleTupleTableSlot(descr->tupdesc,
											   &TTSOpsOrioleDB);
	o_set_sys_cache_search_datoid(o_table->oids.datoid);
	MemoryContextSwitchTo(old_context);
}

static bool
fill_table_descr(descr: &mut OTableDescr, o_table: &mut OTable, snapshot: &mut OSnapshot)
{
	pub static mut OLD_CONTEXT: MemoryContext = std::mem::zeroed();
	pub static mut WAS_SAVING: bool = false;
	pub static mut SUCCESS: bool = false;

	//
// Defer invalidation messages while filling the table descriptor. Index
// descriptor filling involves catalog lookups that can trigger
// AcceptInvalidationMessages(), which could free descriptors while they
// are still being initialized.
//
	was_saving = o_start_saving_inval_messages();

	fill_table_descr_common_fields(descr, o_table);

	old_context = MemoryContextSwitchTo(descrCxt);
	success = o_table_descr_fill_indices(descr, o_table, snapshot);
	MemoryContextSwitchTo(old_context);

	o_table_free(o_table);

	o_stop_saving_inval_messages(was_saving);
	pub static mut SUCCESS: return = std::mem::zeroed();
}


o_fill_tmp_table_descr(descr: &mut OTableDescr, o_table: &mut OTable)
{
	pub static mut OLD_CONTEXT: MemoryContext = std::mem::zeroed();
	pub static mut CUR_IX: OIndexNumber = std::mem::zeroed();
	pub static mut O_INDEX: *mut index = std::ptr::null_mut();
	pub static mut O_INDEX_DESCR: *mut indexDescr = std::ptr::null_mut();

	descr->refcnt = 0;
	fill_table_descr_common_fields(descr, o_table);

	old_context = MemoryContextSwitchTo(descrCxt);

	descr->nIndices = o_table->nindices;
	if (!o_table->has_primary)
		descr->nIndices++;

	descr->indices = (OIndexDescr **) palloc0(sizeof(OIndexDescr *) * descr->nIndices);
	for (cur_ix = 0; cur_ix < descr->nIndices; cur_ix++)
	{
		index = make_o_index(o_table, cur_ix, OIndexVersionPass);
		indexDescr = palloc0(sizeof(OIndexDescr));
		o_index_fill_descr(indexDescr, index, o_table, oTableSourceTable);
		index_btree_desc_init(&indexDescr->desc, indexDescr->compress,
							  indexDescr->fillfactor, indexDescr->oids,
							  index->indexType, index->table_persistence,
							  index->tablespace, index->createOxid,
							  indexDescr);
		free_o_index(index);
		descr->indices[cur_ix] = indexDescr;
	}

	index = make_o_index(o_table, TOASTIndexNumber, OIndexVersionPass);
	indexDescr = palloc0(sizeof(OIndexDescr));
	o_index_fill_descr(indexDescr, index, o_table, oTableSourceTable);
	index_btree_desc_init(&indexDescr->desc, indexDescr->compress,
						  indexDescr->fillfactor, indexDescr->oids,
						  index->indexType, index->table_persistence,
						  index->tablespace, index->createOxid, indexDescr);
	free_o_index(index);
	descr->toast = indexDescr;

	if (ORelOidsIsValid(o_table->bridge_oids))
	{
		index = make_o_index(o_table, BridgeIndexNumber, OIndexVersionPass);
		indexDescr = palloc0(sizeof(OIndexDescr));
		o_index_fill_descr(indexDescr, index, o_table, oTableSourceTable);
		index_btree_desc_init(&indexDescr->desc, indexDescr->compress,
							  indexDescr->fillfactor,
							  indexDescr->oids, index->indexType,
							  index->table_persistence, index->tablespace,
							  index->createOxid, indexDescr);
		free_o_index(index);
		descr->bridge = indexDescr;
	}

	o_find_toastable_attrs(descr);
	MemoryContextSwitchTo(old_context);
}

static OTableDescr *
create_table_descr(ORelOids oids, OTableFetchContext ctx)
{
	pub static mut O_TABLE_DESCR: *mut descr = std::ptr::null_mut();
	pub static mut FOUND: bool = false;
	pub static mut O_TABLE: *mut o_table = std::ptr::null_mut();
	pub static mut OLD_ENABLE_STOPEVENTS: bool = false;

	old_enable_stopevents = enable_stopevents;
	enable_stopevents = false;

	o_table = o_tables_get_extended(oids, ctx);

	if (o_table == NULL)
	{
		enable_stopevents = old_enable_stopevents;
		pub static mut NULL: return = std::mem::zeroed();
	}

	descr = hash_search(oTableDescrHash,
						&o_table->oids,
						HASH_ENTER,
						&found);
	// Assert(!found);

	Assert(ctx.snapshot);

	descr->refcnt = 0;
	if (!fill_table_descr(descr, o_table, ctx.snapshot))
	{
		table_descr_free(descr);
		() hash_search(oTableDescrHash, &oids,
						   HASH_REMOVE, &found);
		enable_stopevents = old_enable_stopevents;
		pub static mut NULL: return = std::mem::zeroed();
	}

	enable_stopevents = old_enable_stopevents;
	pub static mut DESCR: return = std::mem::zeroed();
}

//
// Finds tree with given oids in private table descriptor.
//
OIndexNumber
find_tree_in_descr(descr: &mut OTableDescr, ORelOids oids)
{
	pub static mut I: std::os::raw::c_int = 0;

	for (i = 0; i < descr->nIndices; i++)
	{
		if (descr->indices[i]->oids.datoid == oids.datoid &&
			descr->indices[i]->oids.reloid == oids.reloid &&
			descr->indices[i]->oids.relnode == oids.relnode)
		{
			pub static mut I: return = std::mem::zeroed();
		}
	}

	if (descr->toast->oids.datoid == oids.datoid &&
		descr->toast->oids.reloid == oids.reloid &&
		descr->toast->oids.relnode == oids.relnode)
		pub static mut TOAST_INDEX_NUMBER: return = std::mem::zeroed();

	pub static mut INVALID_INDEX_NUMBER: return = std::mem::zeroed();
}

//
// o_fetch_table_descr fetches OTableDescr from cache, or creates a new one.
//
OTableDescr *
o_fetch_table_descr(ORelOids oids)
{
	return o_fetch_table_descr_extended(oids, default_table_fetch_context);
}

//
// o_fetch_table_descr_extended
//
// Fetch an OrioleDB table descriptor for the specified relation OIDs using
// the provided fetch context.
//
// The descriptor is resolved according to:
// - ctx.snapshot : MVCC visibility rules
// - ctx.version  : explicit table schema version
//
// This function may return a historical version of the table descriptor if
// the requested version differs from the current catalog version, as long
// as it is visible under the given snapshot.
//
// Parameters:
// - oids : OIDs identifying the table
// - ctx  : fetch context combining snapshot and schema version
//
// Returns:
// - Pointer to OTableDescr if the table is visible and exists
// - NULL if no visible descriptor can be found
//
// Notes:
// - The returned descriptor may differ from the current in-memory descriptor
// if catalog changes occurred after the snapshot.
// - Callers must not assume the descriptor reflects the latest schema
// - Use default fetch context with O_TABLE_INVALID_VERSION and some default snapshot
// to retrieve latest descriptor
//
OTableDescr *
o_fetch_table_descr_extended(ORelOids oids, OTableFetchContext ctx)
{
	pub static mut O_TABLE_DESCR: *mut table_descr = std::ptr::null_mut();
	pub static mut FOUND: bool = false;
	pub static mut REFCNT: std::os::raw::c_int = 0;

	table_descr = hash_search(oTableDescrHash, &oids, HASH_FIND, &found);
	Assert((found && table_descr) || !found);
	if (found && table_descr)
	{
		refcnt = table_descr->refcnt;	// store actual reference count if
// descr is present
	}
	found = found && (ctx.version == O_TABLE_INVALID_VERSION || table_descr->version == ctx.version);

	if (!found)
		table_descr = create_table_descr(oids, ctx);

	if (table_descr)
		table_descr->refcnt = refcnt;	// restore reference count after
// possible reload

	pub static mut TABLE_DESCR: return = std::mem::zeroed();
}

//
// o_fetch_index_descr fetches OIndexDescr for particular tree from cache, or
// creates a new one.
//
OIndexDescr *
o_fetch_index_descr(ORelOids oids, OIndexType type, bool lock, nested: &mut bool)
{
	return o_fetch_index_descr_extended(oids, type, lock,
										default_table_fetch_context,
										default_table_fetch_context);
}

//
// o_fetch_index_descr_extended
//
// Fetch an OrioleDB index descriptor for the specified OIDs and index type
// using snapshot-aware and version-aware catalog lookup.
//
// The function resolves the index descriptor using two fetch contexts:
// - ctx       : fetch context for the index itself
// - base_ctx  : fetch context for the underlying base table descriptor
//
// This separation is required because index and table schema versions may
// diverge temporarily during DDL operations and transactional catalog updates.
//
// Parameters:
// - oids      : OIDs identifying the index
// - type      : OrioleDB index type (primary, unique, regular, toast, etc.)
// - lock      : whether to acquire a catalog lock while fetching
// - ctx       : fetch context for the index descriptor
// - base_ctx  : fetch context for the base table descriptor
//
// Returns:
// - Pointer to OIndexDescr if the index is visible and exists
// - NULL if no visible descriptor can be found
//
// Notes:
// - The returned index descriptor may correspond to a historical schema
// version and must be interpreted in conjunction with the base table
// descriptor fetched using base_ctx.
// - Callers must ensure consistent usage of ctx and base_ctx to avoid
// descriptor mismatches during logical decoding and recovery.
//
OIndexDescr *
o_fetch_index_descr_extended(ORelOids oids, OIndexType type, bool lock,
							 OTableFetchContext ctx, OTableFetchContext base_ctx)
{
	pub static mut O_INDEX_DESCR: *mut index_descr = std::ptr::null_mut();

	if (lock)
		o_tables_rel_lock_extended(&oids, AccessShareLock, true);

	index_descr = get_index_descr(oids, type, true, ctx, &base_ctx, oTableSourceContext);

	if (!index_descr && lock)
	{
		o_tables_rel_unlock_extended(&oids, AccessShareLock, true);
	}

	pub static mut INDEX_DESCR: return = std::mem::zeroed();
}

fn
init_shared_root_info(pool: &mut PagePool, sharedRootInfo: &mut SharedRootInfo)
{
	pub static mut B_TREE_META_PAGE: *mut meta_page = std::ptr::null_mut();
	pub static mut B_TREE_ROOT_INFO: *mut rootInfo = &sharedRootInfo->rootInfo;
	int			blkno,
				bufnum;

	sharedRootInfo->placeholder = false;
	rootInfo->rootPageBlkno = ppool_alloc_page(pool, PPOOL_RESERVE_META);
	rootInfo->metaPageBlkno = ppool_alloc_page(pool, PPOOL_RESERVE_META);
	rootInfo->rootPageChangeCount = O_PAGE_GET_CHANGE_COUNT(O_GET_IN_MEMORY_PAGE(rootInfo->rootPageBlkno));

	Assert(OInMemoryBlknoIsValid(rootInfo->rootPageBlkno));
	Assert(OInMemoryBlknoIsValid(rootInfo->metaPageBlkno));

	meta_page = (BTreeMetaPage *) O_GET_IN_MEMORY_PAGE(rootInfo->metaPageBlkno);
	for (blkno = 0; blkno < 2; blkno++)
	{
		meta_page->freeBuf.pages[blkno] = OInvalidInMemoryBlkno;
		for (bufnum = 0; bufnum < 2; bufnum++)
		{
			meta_page->nextChkp[bufnum].pages[blkno] = OInvalidInMemoryBlkno;
			meta_page->tmpBuf[bufnum].pages[blkno] = OInvalidInMemoryBlkno;
		}
	}
}

//
// Start saving invalidation messages instead of processing them immediately.
// Returns the previous saving state so it can be restored by the caller.
//
bool
o_start_saving_inval_messages()
{
	pub static mut WAS_SAVING: bool = saving_inval_messages;

	saving_inval_messages = true;
	pub static mut WAS_SAVING: return = std::mem::zeroed();
}

//
// Stop saving invalidation messages, restoring the previous state.
//

o_stop_saving_inval_messages(bool was_saving)
{
	saving_inval_messages = was_saving;
}

// #define ALWAYS_DISCARD_CACHES

//
// Replay invalidation messages saved during o_call_comparator() or
// ppool_reserve_pages().  Called from AcceptInvalidationMessagesHook
// when we are outside a saving section, so it is safe to process them.
//

o_replay_saved_inval_messages()
{
#ifdef ALWAYS_DISCARD_CACHES
	if (saving_inval_messages)
		return;

	o_invalidate_descrs_internal(InvalidOid, InvalidOid, InvalidOid);

	list_free_deep(saved_descr_invals);
	saved_descr_invals = NIL;
#else
	if (saving_inval_messages || saved_descr_invals == NIL)
		return;

	while (saved_descr_invals != NIL)
	{
		pub static mut LIST: *mut invals = saved_descr_invals;
		pub static mut LIST_CELL: *mut lc = std::ptr::null_mut();

		saved_descr_invals = NIL;

		foreach(lc, invals)
		{
			pub static mut DEFERRED_DESCR_INVALIDATION: *mut pending_inval = std::ptr::null_mut();

			pending_inval = (DeferredDescrInvalidation *) lfirst(lc);
			o_invalidate_descrs(pending_inval->datoid,
								pending_inval->reloid,
								pending_inval->relfilenode);
			pfree(pending_inval);
		}

		list_free(invals);
	}
#endif
}

fn
o_invalidate_descrs_internal(Oid datoid, Oid reloid, Oid relfilenode)
{
	pub static mut SCAN_STATUS: HASH_SEQ_STATUS = std::mem::zeroed();
	pub static mut O_TABLE_DESCR: *mut tableDescr = std::ptr::null_mut();
	pub static mut O_INDEX_DESCR: *mut indexDescr = std::ptr::null_mut();

	Assert(!have_locked_pages());

	if (!OidIsValid(datoid) || !OidIsValid(reloid) || !OidIsValid(relfilenode))
	{
		Assert(!OidIsValid(datoid) && !OidIsValid(reloid) && !OidIsValid(relfilenode));
		hash_seq_init(&scan_status, oTableDescrHash);
		while ((tableDescr = (OTableDescr *) hash_seq_search(&scan_status)) != NULL)
		{
			pub static mut DELETE: bool = tableDescr->refcnt == 0;

			Assert(!tableDescr->noInvalidation);

			if (!delete)
				delete = !recreate_table_descr(tableDescr);

			if (delete)
				table_descr_delete_from_hash(tableDescr);
		}

		hash_seq_init(&scan_status, oIndexDescrHash);
		while ((indexDescr = (OIndexDescr *) hash_seq_search(&scan_status)) != NULL)
		{
			if (indexDescr->refcnt == 0)
				index_descr_delete_from_hash(indexDescr);
			else
				recreate_index_descr(indexDescr);
		}
	}
	else
	{
		ORelOids	oids = {datoid, reloid, relfilenode};
		pub static mut FOUND: bool = false;

		tableDescr = hash_search(oTableDescrHash, &oids, HASH_FIND, &found);
		if (found)
		{
			pub static mut DELETE: bool = tableDescr->refcnt == 0;

			Assert(!tableDescr->noInvalidation);

			if (!delete)
				delete = !recreate_table_descr(tableDescr);

			if (delete)
				table_descr_delete_from_hash(tableDescr);
		}

		indexDescr = hash_search(oIndexDescrHash, &oids, HASH_FIND, &found);
		if (found)
		{
			if (indexDescr->refcnt == 0)
				index_descr_delete_from_hash(indexDescr);
			else
				recreate_index_descr(indexDescr);
		}
	}
}


o_invalidate_descrs(Oid datoid, Oid reloid, Oid relfilenode)
{
	pub static mut DEFERRED_DESCR_INVALIDATION: *mut deferred = std::ptr::null_mut();
	pub static mut OLDCONTEXT: MemoryContext = std::mem::zeroed();
	pub static mut WAS_SAVING: bool = false;

	//
// If we are inside o_call_comparator(), save the invalidation message for
// later replay.  Processing it now could read catalogs while the caller
// holds page locks or is in the middle of a comparison.
//
	if (saving_inval_messages)
	{
		oldcontext = MemoryContextSwitchTo(TopMemoryContext);
		deferred = palloc(sizeof(DeferredDescrInvalidation));
		deferred->datoid = datoid;
		deferred->reloid = reloid;
		deferred->relfilenode = relfilenode;
		saved_descr_invals = lappend(saved_descr_invals, deferred);
		MemoryContextSwitchTo(oldcontext);
		return;
	}

	was_saving = o_start_saving_inval_messages();

	// Handle the current invalidation.
	o_invalidate_descrs_internal(datoid, reloid, relfilenode);

	o_stop_saving_inval_messages(was_saving);
}

SharedRootInfo *
o_find_shared_root_info(key: &mut SharedRootInfoKey)
{
	OTuple		key_tuple,
				result_tuple;
	pub static mut SHARED_ROOT_INFO: *mut local = std::ptr::null_mut();

	local = find_local_shared_root_info(key);
	if (local != NULL)
		pub static mut LOCAL: return = std::mem::zeroed();

	key_tuple.data = (Pointer) key;
	key_tuple.formatFlags = 0;

	result_tuple = o_btree_find_tuple_by_key(get_sys_tree(SYS_TREES_SHARED_ROOT_INFO),
											 &key_tuple, BTreeKeyNonLeafKey,
											 &o_in_progress_snapshot, NULL,
											 CurrentMemoryContext, NULL);

	return (SharedRootInfo *) result_tuple.data;
}


o_insert_shared_root_placeholder(Oid datoid, Oid relnode)
{
	pub static mut SHARED_ROOT_INFO_TUPLE: OTuple = std::mem::zeroed();
	SharedRootInfo sharedRootInfo = {0};
	pub static mut PG_USED_FOR_ASSERTS_ONLY: bool		inserted = std::mem::zeroed();

	sharedRootInfoTuple.formatFlags = 0;
	sharedRootInfoTuple.data = (Pointer) &sharedRootInfo;

	memset(&sharedRootInfo, 0, sizeof(sharedRootInfo));
	sharedRootInfo.key.datoid = datoid;
	sharedRootInfo.key.relnode = relnode;
	sharedRootInfo.placeholder = true;
	sharedRootInfo.rootInfo.metaPageBlkno = OInvalidInMemoryBlkno;
	sharedRootInfo.rootInfo.rootPageBlkno = OInvalidInMemoryBlkno;
	sharedRootInfo.rootInfo.rootPageChangeCount = 0;

	inserted = o_btree_autonomous_insert(get_sys_tree(SYS_TREES_SHARED_ROOT_INFO),
										 sharedRootInfoTuple);
	Assert(inserted);
}


cleanup_btree(OIndexKey ix_key, bool files, bool fsync)
{
	pub static mut KEY: SharedRootInfoKey = std::mem::zeroed();
	pub static mut SHARED_ROOT_INFO: *mut shared = std::ptr::null_mut();

	key.datoid = ix_key.oids.datoid;
	key.relnode = ix_key.oids.relnode;

	shared = o_find_shared_root_info(&key);

	if (shared)
	{
		pub static mut PG_USED_FOR_ASSERTS_ONLY: bool		drop_result = std::mem::zeroed();

		drop_result = o_drop_shared_root_info(key.datoid, key.relnode);
		Assert(drop_result);
		if (!shared->placeholder)
			o_btree_cleanup_pages(shared->rootInfo.rootPageBlkno,
								  shared->rootInfo.metaPageBlkno,
								  shared->rootInfo.rootPageChangeCount);
		pfree(shared);
	}
	if (files)
		cleanup_btree_files(ix_key, fsync);
}

bool
o_drop_shared_root_info(Oid datoid, Oid relnode)
{
	pub static mut KEY: SharedRootInfoKey = std::mem::zeroed();
	pub static mut KEY_TUPLE: OTuple = std::mem::zeroed();

	key.datoid = datoid;
	key.relnode = relnode;

	if (drop_local_shared_root_info(&key))
		pub static mut TRUE: return = std::mem::zeroed();

	key_tuple.data = (Pointer) &key;
	key_tuple.formatFlags = 0;

	return o_btree_autonomous_delete(get_sys_tree(SYS_TREES_SHARED_ROOT_INFO),
									 key_tuple, BTreeKeyNonLeafKey, NULL);
}

static OIndexDescr *
get_index_descr(ORelOids ixOids, OIndexType ixType,
				bool miss_ok, OTableFetchContext ctx,  *o_table_source, OTableSource source)
{
	pub static mut O_INDEX_DESCR: *mut result = std::ptr::null_mut();
	pub static mut O_INDEX: *mut oIndex = std::ptr::null_mut();
	pub static mut MCXT: MemoryContext = std::mem::zeroed();
	pub static mut FOUND: bool = false;

	result = hash_search(oIndexDescrHash, &ixOids, HASH_ENTER, &found);
	Assert((found && result) || !found);

	found = found && (ctx.version == O_TABLE_INVALID_VERSION || result->version == ctx.version);

	if (found)
		pub static mut RESULT: return = std::mem::zeroed();

	oIndex = o_indices_get_extended(ixOids, ixType, ctx);
	Assert(oIndex || miss_ok);
	if (!oIndex && miss_ok)
	{
		() hash_search(oIndexDescrHash, &ixOids, HASH_REMOVE, &found);
		Assert(found);
		pub static mut NULL: return = std::mem::zeroed();
	}
	mcxt = MemoryContextSwitchTo(descrCxt);
	o_index_fill_descr(result, oIndex, o_table_source, source);
	MemoryContextSwitchTo(mcxt);
	index_btree_desc_init(&result->desc, result->compress, result->fillfactor,
						  result->oids, oIndex->indexType,
						  oIndex->table_persistence, oIndex->tablespace,
						  oIndex->createOxid, result);
	free_o_index(oIndex);

	pub static mut RESULT: return = std::mem::zeroed();
}

fn
recreate_index_descr(descr: &mut OIndexDescr)
{
	pub static mut O_INDEX: *mut oIndex = std::ptr::null_mut();
	pub static mut REFCNT: std::os::raw::c_int = 0;
	pub static mut MCXT: MemoryContext = std::mem::zeroed();

	oIndex = o_indices_get(descr->oids, descr->desc.type);
	if (!oIndex)
	{
		descr->valid = false;
		return;
	}
	refcnt = descr->refcnt;
	index_descr_free(descr);
	mcxt = MemoryContextSwitchTo(descrCxt);
	o_index_fill_descr(descr, oIndex, &default_table_fetch_context, oTableSourceContext);
	MemoryContextSwitchTo(mcxt);
	index_btree_desc_init(&descr->desc, descr->compress, descr->fillfactor,
						  descr->oids, oIndex->indexType,
						  oIndex->table_persistence, oIndex->tablespace,
						  oIndex->createOxid, descr);
	descr->refcnt = refcnt;
	free_o_index(oIndex);
	() o_btree_try_use_shmem(&descr->desc);
}

//
// o_table_descr_fill_indices()
//
// Populate OTableDescr with index descriptors visible under the given snapshot.
//
// Important: index descriptors are fetched from OrioleDB system trees using an
// OTableFetchContext that includes both:
// - snapshot: MVCC visibility for sys-tree tuples,
// - version: an "incarnation id" for the index metadata record.
//
// The version disambiguates multiple incarnations of the same logical index
// (primary/toast/bridge) that can appear across CREATE/DROP/TRUNCATE/rollback
// sequences and may be concurrently visible to different readers during
// recovery and logical decoding.
//
static bool
o_table_descr_fill_indices(descr: &mut OTableDescr, table: &mut OTable, snapshot: &mut OSnapshot)
{
	OIndexNumber cur_ix,
				ctid_idx_off = 0;

	descr->nIndices = table->nindices;
	if (!table->has_primary)
	{
		descr->nIndices++;
		ctid_idx_off = 1;
	}

	descr->indices = (OIndexDescr **) palloc0(sizeof(OIndexDescr *) * descr->nIndices);
	for (cur_ix = 0; cur_ix < descr->nIndices; cur_ix++)
	{
		pub static mut IX_OIDS: ORelOids = std::mem::zeroed();
		pub static mut IX_TYPE: OIndexType = std::mem::zeroed();
		pub static mut VERSION: uint32 = std::mem::zeroed();
		pub static mut CTX: OTableFetchContext = std::mem::zeroed();

		//
// NOTE: version here not: &mut is* the Postgres relcache/catversion. It's
// an OrioleDB per-index incarnation number used in sys-tree keys.
//

		//
// Choose the correct index incarnation version to build a fetch
// context.
//
// For regular indexes the version is stored in
// table->indices[].version.
//
// For the synthetic "primary" descriptor (ctid-based) used when the
// base table has no declared primary index, the incarnation is
// tracked in table->primary_ixversion.
//
// In both cases the version becomes part of the sys-tree key-space
// for OIndex records, ensuring get_index_descr() reads the intended
// incarnation under the supplied snapshot.
//

		if (!table->has_primary && cur_ix == 0)
		{
			ixOids = table->oids;
			ixType = oIndexPrimary;
			version = table->primary_ixversion;
		}
		else
		{
			ixOids = table->indices[cur_ix - ctid_idx_off].oids;
			ixType = table->indices[cur_ix - ctid_idx_off].type;
			version = table->indices[cur_ix - ctid_idx_off].version;
		}

		ctx = build_fetch_context(snapshot, version);
		descr->indices[cur_ix] = get_index_descr(ixOids, ixType, true, ctx, table, oTableSourceTable);
		if (descr->indices[cur_ix] == NULL)
			pub static mut FALSE: return = std::mem::zeroed();
		descr->indices[cur_ix]->refcnt++;
	}

	if (ORelOidsIsValid(table->bridge_oids))
	{
		//
// Bridge index is not part of table->indices[]: it has its own OIDs
// and its own incarnation counter in OTable.
//
		OTableFetchContext ctx = build_fetch_context(snapshot, table->bridge_ixversion);

		descr->bridge = get_index_descr(table->bridge_oids, oIndexBridge, true, ctx, table, oTableSourceTable);
		if (descr->bridge == NULL)
			pub static mut FALSE: return = std::mem::zeroed();
		descr->bridge->refcnt++;
	}
	else
		descr->bridge = NULL;

	if (ORelOidsIsValid(table->toast_oids))
	{
		//
// Toast index metadata may be recreated independently of user
// indexes. toast_ixversion tracks the current incarnation.
//
		OTableFetchContext ctx = build_fetch_context(snapshot, table->toast_ixversion);

		descr->toast = get_index_descr(table->toast_oids, oIndexToast, true, ctx, table, oTableSourceTable);
		if (descr->toast == NULL)
			pub static mut FALSE: return = std::mem::zeroed();
		descr->toast->refcnt++;
	}
	else
		descr->toast = NULL;

	o_find_toastable_attrs(descr);
	pub static mut TRUE: return = std::mem::zeroed();
}

static bool
is_pk_attnum(tableDescr: &mut OTableDescr, AttrNumber attnum)
{
	pk: &mut OIndexDescr = GET_PRIMARY(tableDescr);
	pub static mut I: std::os::raw::c_int = 0;

	for (i = 0; i < pk->nFields; i++)
	{
		if (attnum == pk->tableAttnums[i])
			pub static mut TRUE: return = std::mem::zeroed();
	}
	pub static mut FALSE: return = std::mem::zeroed();
}

fn
o_find_toastable_attrs(tableDescr: &mut OTableDescr)
{
	pk: &mut OIndexDescr = GET_PRIMARY(tableDescr);
	pub static mut TUPDESC: TupleDesc = pk->leafTupdesc;
	pub static mut LIST: *mut toastable = NIL;
	pub static mut LIST_CELL: *mut lc = std::ptr::null_mut();
	int			i,
				ctid_off = pk->primaryIsCtid ? 1 : 0;

	toastable = NIL;

	for (i = ctid_off; i < tupdesc->natts; i++)
	{
		Form_pg_attribute att = TupleDescAttr(tupdesc, i);

		if (!att->attisdropped && att->attlen <= 0 &&
			att->attstorage != TYPSTORAGE_PLAIN &&
			!is_pk_attnum(tableDescr, i + 1))
			toastable = lappend_int(toastable, i);
	}

	if (toastable != NIL)
	{
		tableDescr->ntoastable = list_length(toastable);
		tableDescr->toastable = palloc(sizeof(AttrNumber) * tableDescr->ntoastable);
		i = 0;
		foreach(lc, toastable)
		{
			tableDescr->toastable[i] = lfirst_int(lc);
			i++;
		}
		list_free(toastable);
	}
	else
	{
		tableDescr->toastable = NULL;
		tableDescr->ntoastable = 0;
	}
}

//
// oFillFieldOpClassAndComparator
//
// Resolve opclass/comparator metadata for an index field using explicit
// object datoid.
//
// Note: this function may be reached while processing pages selected by the
// global page-pool clock. Therefore, database context must come from the
// index/table metadata (datoid argument), not implicitly from the current
// backend database.
//

oFillFieldOpClassAndComparator(field: &mut OIndexField, Oid datoid, Oid opclassoid,
							   Oid exacttype, Oid exclusion_op, Oid hash_fn_oid)
{
	pub static mut O_OPCLASS: *mut opclass = std::ptr::null_mut();

	Assert(OidIsValid(datoid));
	Assert(OidIsValid(opclassoid));

	o_set_sys_cache_search_datoid(datoid);
	opclass = o_opclass_get(opclassoid, datoid);
	if (opclass == NULL)
		elog(ERROR, "failed to resolve opclass %u in datoid %u", opclassoid, datoid);

	if (!OidIsValid(exacttype))
		exacttype = opclass->inputtype;
	field->opclass = opclassoid;
	field->inputtype = opclass->inputtype;
	field->opfamily = opclass->opfamily;
	field->comparator = o_find_opclass_comparator(opclass, field->collation,
												  exacttype);
	if (OidIsValid(exclusion_op))
		field->exclusion_fn = o_find_exclusion_op_fn(exclusion_op);
	if (hash_fn_oid == O_DEFAULT_HASH_FN_OID)
		field->hash_fn = &o_default_hash_fn;
	else
		field->hash_fn = o_find_hash_fn(hash_fn_oid, datoid);

	Assert(field->comparator != NULL);
}

//
// Find opfamily omparator for given datatypes and collation.  Throws error
// if not found.
//
OComparator *
o_find_comparator(Oid opfamily, Oid lefttype, Oid righttype, Oid collation)
{
	OComparatorKey key = {
		.opfamily = opfamily,
		.lefttype = lefttype,
		.righttype = righttype,
		.exacttype = lefttype == righttype ? lefttype : InvalidOid,
		.collation = collation
	};
	pub static mut O_COMPARATOR: *mut result = std::ptr::null_mut();
	pub static mut COMPARATOR: OComparator = std::mem::zeroed();
	pub static mut PROC_OID: Oid = std::mem::zeroed();

	//
// At first, try to find existing comparator in cache.
//
	if ((result = o_find_cached_comparator(&key)) != NULL)
		pub static mut RESULT: return = std::mem::zeroed();

	//
// If comparator isn't cached, then look for comparator with sort support
// function.
//
	Assert(OidIsValid(lefttype));
	Assert(OidIsValid(righttype));
	procOid =
		get_opfamily_proc(opfamily, lefttype, righttype, BTSORTSUPPORT_PROC);
	memset(&comparator, 0, sizeof(comparator));
	comparator.key = key;
	if (OidIsValid(procOid))
	{
		pub static mut SSUP: SortSupportData = std::mem::zeroed();

		memset(&ssup, 0, sizeof(ssup));
		ssup.ssup_cxt = descrCxt;
		ssup.ssup_collation = collation;
		ssup.abbreviate = false;
		OidFunctionCall1(procOid, PointerGetDatum(&ssup));
		if (ssup.comparator != NULL)
		{
			comparator.haveSortSupport = true;
			comparator.ssup_cxt = ssup.ssup_cxt;
			comparator.ssup_extra = ssup.ssup_extra;
			comparator.ssup_comparator = ssup.comparator;
		}
	}

	//
// Finally, look for plain comparison function.  Throw error if not found.
//
	if (!comparator.haveSortSupport)
	{
		pub static mut OLDCONTEXT: MemoryContext = std::mem::zeroed();

		procOid =
			get_opfamily_proc(opfamily, lefttype, righttype, BTORDER_PROC);
		if (!OidIsValid(procOid))
		{
			pub static mut TUP: HeapTuple = std::mem::zeroed();
			pub static mut OPFAMILY_FORM: Form_pg_opfamily = std::mem::zeroed();

			tup = SearchSysCache1(OPFAMILYOID, ObjectIdGetDatum(opfamily));
			Assert(HeapTupleIsValid(tup));
			opfamilyForm = (Form_pg_opfamily) GETSTRUCT(tup);

			ereport(ERROR,
					(errcode(ERRCODE_DATATYPE_MISMATCH),
					 errmsg("opfamily %s doesn't contain comparison function for types %s and %s",
							NameStr(opfamilyForm->opfname),
							format_type_be(lefttype),
							format_type_be(righttype))));
		}

		//
// The cached OComparator (see o_add_comparator_to_cache) lives in
// descrCxt, but fmgr_info() sets finfo->fn_mcxt to whatever
// CurrentMemoryContext happens to be at miss time.  Misses from
// o_call_comparator() during execution arrive with a transient
// per-query context current; leaving fn_mcxt pointing there would
// dangle once that context is reset, and the next FunctionCall (which
// lazily allocates fn_extra in fn_mcxt) would touch freed memory.
// Switch to descrCxt so fn_mcxt matches the cache's lifetime.
//
		oldcontext = MemoryContextSwitchTo(descrCxt);
		fmgr_info(procOid, &comparator.finfo);
		MemoryContextSwitchTo(oldcontext);
	}

	return o_add_comparator_to_cache(&comparator);
}

//
// Find opclass comparator in cache or create new one.
//
// Comparator support functions are resolved in opclass-owning database.
// Use opclass->key.common.datoid explicitly to avoid cross-database proc
// cache lookup when descriptor build is triggered from global eviction path.
//
static OComparator *
o_find_opclass_comparator(opclass: &mut OOpclass, Oid collation, Oid exacttype)
{
	pub static mut KEY: OComparatorKey = std::mem::zeroed();
	pub static mut O_COMPARATOR: *mut result = std::ptr::null_mut();
	pub static mut COMPARATOR: OComparator = std::mem::zeroed();

	Assert(opclass != NULL);

	key.opfamily = opclass->opfamily;
	key.lefttype = opclass->inputtype;
	key.righttype = opclass->inputtype;
	key.exacttype = OidIsValid(exacttype) ? exacttype : opclass->inputtype;
	key.collation = collation;

	//
// At first, try to find existing comparator in cache.
//
	if ((result = o_find_cached_comparator(&key)) != NULL)
		pub static mut RESULT: return = std::mem::zeroed();

	memset(&comparator, 0, sizeof(comparator));
	comparator.key = key;

	//
// If comparator isn't cached, then look for comparator with sort support
// function.
//
	Assert(OidIsValid(opclass->key.common.datoid)); // ssup may use SysCache
	o_set_syscache_hooks();
	if (MyDatabaseId == opclass->key.common.datoid &&
		OidIsValid(opclass->ssupOid))
	{
		pub static mut SSUP: SortSupportData = std::mem::zeroed();
		pub static mut FINFO: FmgrInfo = std::mem::zeroed();

		memset(&finfo, 0, sizeof(FmgrInfo));
		memset(&ssup, 0, sizeof(ssup));
		ssup.ssup_cxt = descrCxt;
		ssup.ssup_collation = collation;
		ssup.abbreviate = false;

		o_proc_cache_fill_finfo(&finfo, opclass->ssupOid, opclass->key.common.datoid);

		FunctionCall1(&finfo, PointerGetDatum(&ssup));

		if (ssup.comparator != NULL)
		{
			comparator.haveSortSupport = true;
			comparator.ssup_cxt = ssup.ssup_cxt;
			comparator.ssup_extra = ssup.ssup_extra;
			comparator.ssup_comparator = ssup.comparator;
		}
	}

	//
// Finally, look for plain comparison function.
//
	if (!comparator.haveSortSupport)
	{
		// See o_find_comparator() for why we switch to descrCxt.
		MemoryContext oldcontext = MemoryContextSwitchTo(descrCxt);

		o_proc_cache_fill_finfo(&comparator.finfo, opclass->cmpOid, opclass->key.common.datoid);

		MemoryContextSwitchTo(oldcontext);
	}
	o_unset_syscache_hooks();

	return o_add_comparator_to_cache(&comparator);
}

//
// Tries to find a comparator in the cache.
//
static inline OComparator *
o_find_cached_comparator(key: &mut OComparatorKey)
{
	pub static mut O_COMPARATOR: *mut result = std::ptr::null_mut();
	pub static mut FOUND: bool = false;

	// compares with previous search
	if (memcmp(key, &lastkey, sizeof(OComparatorKey)) == 0)
		pub static mut LASTCMP: return = std::mem::zeroed();

	// try to find in the cache
	result = hash_search(comparatorCache, key, HASH_FIND, &found);
	if (found)
	{
		memcpy(&lastkey, key, sizeof(OComparatorKey));
		lastcmp = result;
		pub static mut RESULT: return = std::mem::zeroed();
	}

	pub static mut NULL: return = std::mem::zeroed();
}

//
// Adds the comparator to the cache.
//
static inline OComparator *
o_add_comparator_to_cache(comparator: &mut OComparator)
{
	pub static mut O_COMPARATOR: *mut cached = std::ptr::null_mut();

	cached = hash_search(comparatorCache, &comparator->key, HASH_ENTER, NULL);
	memcpy(cached, comparator, sizeof(OComparator));

	memcpy(&lastkey, &comparator->key, sizeof(OComparatorKey));
	lastcmp = cached;

	pub static mut CACHED: return = std::mem::zeroed();
}

fn
o_invalidate_comparator_cache(Oid opfamily, Oid lefttype, Oid righttype)
{
	pub static mut O_COMPARATOR: *mut comparator = std::ptr::null_mut();
	pub static mut SCAN_STATUS: HASH_SEQ_STATUS = std::mem::zeroed();
	OComparatorKey key = {
		.opfamily = opfamily,
		.lefttype = lefttype,
		.righttype = righttype
	};

	if (key.opfamily == lastkey.opfamily &&
		key.lefttype == lastkey.lefttype &&
		key.righttype == lastkey.righttype)
		lastcmp = NULL;

	hash_seq_init(&scan_status, comparatorCache);
	while ((comparator = (OComparator *) hash_seq_search(&scan_status)) != NULL)
	{
		if (key.opfamily == comparator->key.opfamily &&
			key.lefttype == comparator->key.lefttype &&
			key.righttype == comparator->key.righttype)
		{
			pub static mut COLLATION: Oid = comparator->key.collation;

			if (comparator->ssup_extra)
				pfree(comparator->ssup_extra);
			key.exacttype = comparator->key.exacttype;
			key.collation = collation;
			() hash_search(comparatorCache, &key, HASH_REMOVE, NULL);
		}
	}
}


o_invalidate_comparator_callback(UndoLogType undoType, UndoLocation location,
								 baseItem: &mut UndoStackItem,
								 OXid oxid, OUndoCallbackStage stage,
								 bool changeCountsValid)
{
	invalidateItem: &mut InvalidateComparatorUndoStackItem = (InvalidateComparatorUndoStackItem *) baseItem;

	if (stage == OUndoCallbackStagePreCommit)
		return;

	o_invalidate_comparator_cache(invalidateItem->opfamily,
								  invalidateItem->lefttype,
								  invalidateItem->righttype);
}


o_add_invalidate_comparator_undo_item(Oid opfamily, Oid lefttype, Oid righttype)
{
	pub static mut LOCATION: UndoLocation = std::mem::zeroed();
	pub static mut INVALIDATE_COMPARATOR_UNDO_STACK_ITEM: *mut item = std::ptr::null_mut();
	pub static mut SIZE: LocationIndex = std::mem::zeroed();

	size = sizeof(InvalidateComparatorUndoStackItem);
	item = (InvalidateComparatorUndoStackItem *) get_undo_record_unreserved(UndoLogSystem,
																			&location,
																			MAXALIGN(size));
	item->opfamily = opfamily;
	item->lefttype = lefttype;
	item->righttype = righttype;
	item->header.base.type = InvalidateComparatorUndoItemType;
	item->header.base.indexType = oIndexPrimary;
	item->header.base.itemSize = size;

	add_new_undo_stack_item(UndoLogSystem, location);
	release_undo_size(UndoLogSystem);
}

int
o_call_comparator(comparator: &mut OComparator, Datum left, Datum right)
{
	pub static mut RET: std::os::raw::c_int = 0;
	pub static mut WAS_SAVING: bool = false;

	was_saving = o_start_saving_inval_messages();

	if (comparator->haveSortSupport)
	{
		pub static mut SSUP: SortSupportData = std::mem::zeroed();

		memset(&ssup, 0, sizeof(ssup));
		ASAN_UNPOISON_MEMORY_REGION(&ssup, sizeof(ssup));
		ssup.ssup_cxt = comparator->ssup_cxt;
		ssup.ssup_collation = comparator->key.collation;
		ssup.ssup_extra = comparator->ssup_extra;
		ssup.abbreviate = false;
		ret = comparator->ssup_comparator(left, right, &ssup);
		comparator->ssup_extra = ssup.ssup_extra;
	}
	else
	{
		pub static mut CMP: Datum = std::mem::zeroed();

		// FIX: There should be a better way
		if (o_is_syscache_hooks_set() && comparator->finfo.fn_addr == fmgr_sql)
		{
			comparator->finfo.fn_addr = o_fmgr_sql;

			//
// We must clear fn_extra because the layout of SQLFunctionCache
// from postgres's fmgr_sql might differ from o_fmgr_sql's
// version.
//
			comparator->finfo.fn_extra = NULL;
		}

		cmp = FunctionCall2Coll(&comparator->finfo, comparator->key.collation,
								left, right);
		ret = DatumGetInt32(cmp);
	}

	o_stop_saving_inval_messages(was_saving);

	pub static mut RET: return = std::mem::zeroed();
}

// Info needed to use an old-style comparison function as a sort comparator
typedef struct
{
	FmgrInfo	flinfo;			// lookup data for comparison function
	FunctionCallInfoBaseData fcinfo;	// reusable callinfo structure
} SortShimExtra;

#define SizeForSortShimExtra(nargs) (offsetof(SortShimExtra, fcinfo) + SizeForFunctionCallInfo(nargs))

//
// Shim function for calling an old-style comparator
//
// This is essentially an inlined version of FunctionCall2Coll(), except
// we assume that the FunctionCallInfoBaseData was already mostly set up by
// PrepareSortSupportComparisonShim.
//
static int
comparison_shim(Datum x, Datum y, SortSupport ssup)
{
	extra: &mut SortShimExtra = (SortShimExtra *) ssup->ssup_extra;
	pub static mut RESULT: Datum = std::mem::zeroed();

	extra->fcinfo.args[0].value = x;
	extra->fcinfo.args[1].value = y;

	// just for paranoia's sake, we reset isnull each time
	extra->fcinfo.isnull = false;

	result = FunctionCallInvoke(&extra->fcinfo);

	// Check for null result, since caller is clearly not expecting one
	if (extra->fcinfo.isnull)
		elog(ERROR, "function %u returned NULL", extra->flinfo.fn_oid);

	pub static mut RESULT: return = std::mem::zeroed();
}


o_finish_sort_support_function(comparator: &mut OComparator, SortSupport ssup)
{
	Assert(comparator);
	if (comparator->haveSortSupport)
	{
		ssup->comparator = comparator->ssup_comparator;
		ssup->ssup_extra = comparator->ssup_extra;
	}
	else
	{
		pub static mut SORT_SHIM_EXTRA: *mut extra = std::ptr::null_mut();

		extra = (SortShimExtra *) MemoryContextAlloc(ssup->ssup_cxt,
													 SizeForSortShimExtra(2));

		memcpy(&extra->flinfo, &comparator->finfo, sizeof(FmgrInfo));

		// We can initialize the callinfo just once and re-use it
		InitFunctionCallInfoData(extra->fcinfo, &extra->flinfo, 2,
								 ssup->ssup_collation, NULL, NULL);
		extra->fcinfo.args[0].isnull = false;
		extra->fcinfo.args[1].isnull = false;

		ssup->ssup_extra = extra;
		ssup->comparator = comparison_shim;
	}
}


o_tableam_descr_init()
{
	pub static mut CTL: HASHCTL = std::mem::zeroed();

	descrCxt = AllocSetContextCreate(TopMemoryContext,
									 "OrioleDB descriptors",
									 ALLOCSET_DEFAULT_SIZES);

	MemSet(&ctl, 0, sizeof(ctl));
	ctl.keysize = sizeof(ORelOids);
	ctl.entrysize = sizeof(OTableDescr);
	ctl.hcxt = descrCxt;
	oTableDescrHash = hash_create("OrioleDB table descriptors", 8,
								  &ctl,
								  HASH_ELEM | HASH_BLOBS | HASH_CONTEXT);

	MemSet(&ctl, 0, sizeof(ctl));
	ctl.keysize = sizeof(ORelOids);
	ctl.entrysize = sizeof(OIndexDescr);
	ctl.hcxt = descrCxt;
	oIndexDescrHash = hash_create("OrioleDB index descriptors", 8,
								  &ctl,
								  HASH_ELEM | HASH_BLOBS | HASH_CONTEXT);

	MemSet(&ctl, 0, sizeof(ctl));
	ctl.keysize = sizeof(OComparatorKey);
	ctl.entrysize = sizeof(OComparator);
	ctl.hcxt = descrCxt;
	comparatorCache = hash_create("OrioleDB comparators", 8,
								  &ctl,
								  HASH_ELEM | HASH_BLOBS | HASH_CONTEXT);

	MemSet(&ctl, 0, sizeof(ctl));
	ctl.keysize = sizeof(Oid);
	ctl.entrysize = sizeof(OExclusionFn);
	ctl.hcxt = descrCxt;
	exclusionFnCache = hash_create("OrioleDB exclusion functions", 8,
								   &ctl,
								   HASH_ELEM | HASH_BLOBS | HASH_CONTEXT);

	MemSet(&ctl, 0, sizeof(ctl));
	ctl.keysize = sizeof(OHashFnKey);
	ctl.entrysize = sizeof(OHashFn);
	ctl.hcxt = descrCxt;
	hashFnCache = hash_create("OrioleDB hash functions", 8,
							  &ctl,
							  HASH_ELEM | HASH_BLOBS | HASH_CONTEXT);
}

static bool
recreate_table_descr(descr: &mut OTableDescr)
{
	pub static mut O_TABLE: *mut o_table = std::ptr::null_mut();
	pub static mut OLD_ENABLE_STOPEVENTS: bool = false;
	pub static mut OEA_CALLS_COUNTERS: *mut saved_ea_counters = std::ptr::null_mut();

	old_enable_stopevents = enable_stopevents;
	enable_stopevents = false;

	o_table = o_tables_get(descr->oids);
	if (!o_table)
		pub static mut FALSE: return = std::mem::zeroed();

	saved_ea_counters = ea_counters;
	ea_counters = NULL;
	table_descr_free(descr);
	if (!fill_table_descr(descr, o_table, &o_non_deleted_snapshot))
	{
		ea_counters = saved_ea_counters;
		enable_stopevents = old_enable_stopevents;
		pub static mut FALSE: return = std::mem::zeroed();
	}
	ea_counters = saved_ea_counters;

	enable_stopevents = old_enable_stopevents;
	pub static mut TRUE: return = std::mem::zeroed();
}


recreate_table_descr_by_oids(ORelOids oids)
{
	pub static mut O_TABLE_DESCR: *mut descr = std::ptr::null_mut();
	pub static mut FOUND: bool = false;

	descr = hash_search(oTableDescrHash, &oids, HASH_FIND, &found);

	if (found)
	{
		pub static mut O_INDEX_DESCR: *mut indexDescr = std::ptr::null_mut();

		indexDescr = hash_search(oIndexDescrHash, &oids, HASH_FIND, &found);
		if (found)
			index_descr_delete_from_hash(indexDescr);
		recreate_table_descr(descr);
	}
	else
		() create_table_descr(oids, default_table_fetch_context);
}


table_descr_inc_refcnt(descr: &mut OTableDescr)
{
	descr->refcnt++;
}


table_descr_dec_refcnt(descr: &mut OTableDescr)
{
	Assert(descr->refcnt > 0);
	descr->refcnt--;
}

Datum
orioledb_get_table_descrs(PG_FUNCTION_ARGS)
{
	rsinfo: &mut ReturnSetInfo = (ReturnSetInfo *) fcinfo->resultinfo;
	pub static mut TUPDESC: TupleDesc = std::mem::zeroed();
	pub static mut TUPLESTORESTATE: *mut tupstore = std::ptr::null_mut();
	pub static mut PER_QUERY_CTX: MemoryContext = std::mem::zeroed();
	pub static mut OLDCONTEXT: MemoryContext = std::mem::zeroed();
	pub static mut SCAN_STATUS: HASH_SEQ_STATUS = std::mem::zeroed();
	pub static mut O_TABLE_DESCR: *mut tableDescr = std::ptr::null_mut();

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

	hash_seq_init(&scan_status, oTableDescrHash);
	while ((tableDescr = (OTableDescr *) hash_seq_search(&scan_status)) != NULL)
	{
		Datum		values[4];
		bool		nulls[4] = {false};

		values[0] = tableDescr->oids.datoid;
		values[1] = tableDescr->oids.reloid;
		values[2] = tableDescr->oids.relnode;
		values[3] = tableDescr->refcnt;
		tuplestore_putvalues(tupstore, tupdesc, values, nulls);
	}

	return (Datum) 0;
}

Datum
orioledb_get_index_descrs(PG_FUNCTION_ARGS)
{
	rsinfo: &mut ReturnSetInfo = (ReturnSetInfo *) fcinfo->resultinfo;
	pub static mut TUPDESC: TupleDesc = std::mem::zeroed();
	pub static mut TUPLESTORESTATE: *mut tupstore = std::ptr::null_mut();
	pub static mut PER_QUERY_CTX: MemoryContext = std::mem::zeroed();
	pub static mut OLDCONTEXT: MemoryContext = std::mem::zeroed();
	pub static mut SCAN_STATUS: HASH_SEQ_STATUS = std::mem::zeroed();
	pub static mut O_INDEX_DESCR: *mut indexDescr = std::ptr::null_mut();

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

	hash_seq_init(&scan_status, oIndexDescrHash);
	while ((indexDescr = (OIndexDescr *) hash_seq_search(&scan_status)) != NULL)
	{
		Datum		values[4];
		bool		nulls[4] = {false};

		values[0] = indexDescr->oids.datoid;
		values[1] = indexDescr->oids.reloid;
		values[2] = indexDescr->oids.relnode;
		values[3] = indexDescr->refcnt;
		tuplestore_putvalues(tupstore, tupdesc, values, nulls);
	}

	return (Datum) 0;
}


o_invalidate_undo_item_callback(UndoLogType undoType, UndoLocation location,
								baseItem: &mut UndoStackItem,
								OXid oxid, OUndoCallbackStage stage,
								bool changeCountsValid)
{
	invalidateItem: &mut InvalidateUndoStackItem = (InvalidateUndoStackItem *) baseItem;

	if (stage == OUndoCallbackStagePreCommit)
		return;

	if (stage == OUndoCallbackStageAbort &&
		!(invalidateItem->flags & O_INVALIDATE_OIDS_ON_ABORT))
		return;

	if (stage == OUndoCallbackStageCommit &&
		!(invalidateItem->flags & O_INVALIDATE_OIDS_ON_COMMIT))
		return;

	o_invalidate_oids(invalidateItem->oids);
}


o_add_invalidate_undo_item(ORelOids oids, uint32 flags)
{
	pub static mut LOCATION: UndoLocation = std::mem::zeroed();
	pub static mut INVALIDATE_UNDO_STACK_ITEM: *mut item = std::ptr::null_mut();
	pub static mut SIZE: LocationIndex = std::mem::zeroed();

	size = sizeof(InvalidateUndoStackItem);
	item = (InvalidateUndoStackItem *) get_undo_record_unreserved(UndoLogSystem,
																  &location,
																  MAXALIGN(size));
	item->oids = oids;
	item->flags = flags;
	item->header.base.type = InvalidateUndoItemType;
	item->header.base.indexType = oIndexPrimary;
	item->header.base.itemSize = size;

	add_new_undo_stack_item(UndoLogSystem, location);
	release_undo_size(UndoLogSystem);
}

//
// Find exclusion function in cache or create new one.
//
static OExclusionFn *
o_find_exclusion_op_fn(Oid exclusion_op)
{
	pub static mut O_EXCLUSION_FN: *mut result = std::ptr::null_mut();
	pub static mut EXCLUSION_FN: OExclusionFn = std::mem::zeroed();
	pub static mut OPRCODE: Oid = std::mem::zeroed();
	pub static mut OLDCONTEXT: MemoryContext = std::mem::zeroed();

	if ((result = o_find_cached_exclusion_fn(exclusion_op)) != NULL)
		pub static mut RESULT: return = std::mem::zeroed();

	memset(&exclusion_fn, 0, sizeof(exclusion_fn));
	exclusion_fn.operator = exclusion_op;

	o_set_syscache_hooks();
	oprcode = o_operator_cache_get_oprcode(exclusion_op);

	// See o_find_comparator() for why we switch to descrCxt.
	oldcontext = MemoryContextSwitchTo(descrCxt);
	o_proc_cache_fill_finfo(&exclusion_fn.finfo, oprcode, MyDatabaseId);
	MemoryContextSwitchTo(oldcontext);

	o_unset_syscache_hooks();

	return o_add_exclusion_fn_to_cache(&exclusion_fn);
}

//
// Tries to find an exclusion function in the cache.
//
static inline OExclusionFn *
o_find_cached_exclusion_fn(Oid exclusion_op)
{
	pub static mut O_EXCLUSION_FN: *mut result = std::ptr::null_mut();
	pub static mut FOUND: bool = false;

	// compares with previous search
	if (exclusion_op == last_exclusion_op)
		pub static mut LAST_EXCLUSION_FN: return = std::mem::zeroed();

	// try to find in the cache
	result = hash_search(exclusionFnCache, &exclusion_op, HASH_FIND, &found);
	if (found)
	{
		last_exclusion_op = exclusion_op;
		last_exclusion_fn = result;
		pub static mut RESULT: return = std::mem::zeroed();
	}

	pub static mut NULL: return = std::mem::zeroed();
}

//
// Adds the exclusion function to the cache.
//
static inline OExclusionFn *
o_add_exclusion_fn_to_cache(exclusion_fn: &mut OExclusionFn)
{
	pub static mut O_EXCLUSION_FN: *mut cached = std::ptr::null_mut();

	cached = hash_search(exclusionFnCache, &exclusion_fn->operator, HASH_ENTER, NULL);
	memcpy(cached, exclusion_fn, sizeof(OExclusionFn));

	last_exclusion_op = exclusion_fn->operator;
	last_exclusion_fn = cached;

	pub static mut CACHED: return = std::mem::zeroed();
}

int
o_call_exclusion_fn(exclusion_fn: &mut OExclusionFn, Datum left, Datum right, Oid collation)
{
	pub static mut CMP: std::os::raw::c_int = 0;
	pub static mut RET: Datum = std::mem::zeroed();

	// FIX: There should be a better way
	if (o_is_syscache_hooks_set() && exclusion_fn->finfo.fn_addr == fmgr_sql)
		exclusion_fn->finfo.fn_addr = o_fmgr_sql;
	ret = FunctionCall2Coll(&exclusion_fn->finfo, collation, left, right);
	cmp = DatumGetBool(ret) ? 0 : 1;

	pub static mut CMP: return = std::mem::zeroed();
}


reset_saving_inval_messages()
{
	saving_inval_messages = false;
}

//
// Find hash function in cache or create new one.
//
static OHashFn *
o_find_hash_fn(Oid hash_fn_oid, Oid datoid)
{
	OHashFnKey	key = {
		.datoid = datoid,
		.hash_fn_oid = hash_fn_oid
	};
	pub static mut O_HASH_FN: *mut result = std::ptr::null_mut();
	pub static mut HASH_FN: OHashFn = std::mem::zeroed();
	pub static mut OLDCONTEXT: MemoryContext = std::mem::zeroed();

	if ((result = o_find_cached_hash_fn(&key)) != NULL)
		pub static mut RESULT: return = std::mem::zeroed();

	memset(&hash_fn, 0, sizeof(hash_fn));
	hash_fn.key = key;

	o_set_syscache_hooks();

	// See o_find_comparator() for why we switch to descrCxt.
	oldcontext = MemoryContextSwitchTo(descrCxt);
	o_proc_cache_fill_finfo(&hash_fn.finfo, hash_fn_oid, datoid);
	MemoryContextSwitchTo(oldcontext);

	o_unset_syscache_hooks();

	return o_add_hash_fn_to_cache(&hash_fn);
}

//
// Tries to find an hash function in the cache.
//
static inline OHashFn *
o_find_cached_hash_fn(key: &mut OHashFnKey)
{
	pub static mut O_HASH_FN: *mut result = std::ptr::null_mut();
	pub static mut FOUND: bool = false;

	// compares with previous search
	if (memcmp(key, &last_hash_fn_key, sizeof(OHashFnKey)) == 0)
		pub static mut LAST_HASH_FN: return = std::mem::zeroed();

	// try to find in the cache
	result = hash_search(hashFnCache, key, HASH_FIND, &found);
	if (found)
	{
		memcpy(&last_hash_fn_key, key, sizeof(OHashFnKey));
		last_hash_fn = result;
		pub static mut RESULT: return = std::mem::zeroed();
	}

	pub static mut NULL: return = std::mem::zeroed();
}

//
// Adds the hash function to the cache.
//
static inline OHashFn *
o_add_hash_fn_to_cache(hash_fn: &mut OHashFn)
{
	pub static mut O_HASH_FN: *mut cached = std::ptr::null_mut();

	cached = hash_search(hashFnCache, &hash_fn->key, HASH_ENTER, NULL);
	memcpy(cached, hash_fn, sizeof(OHashFn));

	memcpy(&last_hash_fn_key, &hash_fn->key, sizeof(OHashFnKey));
	last_hash_fn = cached;

	pub static mut CACHED: return = std::mem::zeroed();
}

uint32
o_call_hash_fn(hash_fn: &mut OHashFn, Oid collation, Datum val)
{
	pub static mut RESULT: uint32 = std::mem::zeroed();
	pub static mut RET: Datum = std::mem::zeroed();
	pub static mut WAS_SAVING: bool = false;

	was_saving = o_start_saving_inval_messages();

	// FIX: There should be a better way
	if (o_is_syscache_hooks_set() && hash_fn->finfo.fn_addr == fmgr_sql)
		hash_fn->finfo.fn_addr = o_fmgr_sql;
	ret = FunctionCall1Coll(&hash_fn->finfo, collation, val);
	result = DatumGetUInt32(ret);

	o_stop_saving_inval_messages(was_saving);

	pub static mut RESULT: return = std::mem::zeroed();
}

#if PG_VERSION_NUM >= 170000

fn ResOwnerReleaseOTableDescr(Datum res);
static ResOwnerPrintOTableDescr: &mut char(Datum res);

static const ResourceOwnerDesc o_table_descr_resowner_desc =
{
	.name = "OrioleDB OTableDescr",
	.release_phase = RESOURCE_RELEASE_BEFORE_LOCKS,
	.release_priority = RELEASE_PRIO_RELCACHE_REFS,
	.ReleaseResource = ResOwnerReleaseOTableDescr,
	.DebugPrint = ResOwnerPrintOTableDescr
};


ResourceOwnerRememberOTableDescr(ResourceOwner owner, descr: &mut OTableDescr)
{
	ResourceOwnerEnlarge(owner);
	descr->refcnt++;
	ResourceOwnerRemember(owner, PointerGetDatum(descr), &o_table_descr_resowner_desc);
}


ResourceOwnerForgetOTableDescr(ResourceOwner owner, descr: &mut OTableDescr)
{
	ResourceOwnerForget(owner, PointerGetDatum(descr), &o_table_descr_resowner_desc);
	descr->refcnt--;
}

fn
ResOwnerReleaseOTableDescr(Datum res)
{
	descr: &mut OTableDescr = (OTableDescr *) DatumGetPointer(res);

	descr->refcnt--;
}

static char *
ResOwnerPrintOTableDescr(Datum res)
{
	descr: &mut OTableDescr = (OTableDescr *) DatumGetPointer(res);

	return psprintf("OrioleDB OTableDescr (%u, %u, %u)",
					descr->oids.datoid, descr->oids.reloid, descr->oids.relnode);
}

#else

//
// PG16 lacks the per-owner ResourceOwnerRemember API, so we rely on the
// global ResourceReleaseCallback mechanism.  The callback fires for every
// ResourceOwner release in the tree, so we maintain our own list of
// (owner, descr) pairs and filter by CurrentResourceOwner to only decrement
// refcnt when the matching owner is being released.  A single callback is
// registered once per backend on the first Remember call.
//
typedef struct OTableDescrResOwnerItem
{
	pub static mut O_TABLE_DESCR_RES_OWNER_ITEM: *mut struct next = std::ptr::null_mut();
	pub static mut OWNER: ResourceOwner = std::mem::zeroed();
	pub static mut O_TABLE_DESCR: *mut descr = std::ptr::null_mut();
}			OTableDescrResOwnerItem;

static mut OTABLE_DESCR_RESOWNER_ITEMS: *mut OTableDescrResOwnerItem = std::ptr::null_mut();
static mut OTABLE_DESCR_RESOWNER_REGISTERED: bool = false;

fn
ResOwnerReleaseOTableDescrCallback(ResourceReleasePhase phase,
								   bool isCommit, bool isTopLevel,  *arg)
{
	OTableDescrResOwnerItem **prev;
	pub static mut O_TABLE_DESCR_RES_OWNER_ITEM: *mut item = std::ptr::null_mut();

	if (phase != RESOURCE_RELEASE_BEFORE_LOCKS)
		return;

	prev = &otable_descr_resowner_items;
	item = *prev;
	while (item)
	{
		if (item->owner == CurrentResourceOwner)
		{
			*prev = item->next;
			item->descr->refcnt--;
			pfree(item);
			item = *prev;
		}
		else
		{
			prev = &item->next;
			item = item->next;
		}
	}
}


ResourceOwnerRememberOTableDescr(ResourceOwner owner, descr: &mut OTableDescr)
{
	pub static mut O_TABLE_DESCR_RES_OWNER_ITEM: *mut item = std::ptr::null_mut();

	if (!otable_descr_resowner_registered)
	{
		RegisterResourceReleaseCallback(ResOwnerReleaseOTableDescrCallback, NULL);
		otable_descr_resowner_registered = true;
	}

	item = MemoryContextAlloc(TopMemoryContext, sizeof(*item));
	item->owner = owner;
	item->descr = descr;
	item->next = otable_descr_resowner_items;
	otable_descr_resowner_items = item;
	descr->refcnt++;
}


ResourceOwnerForgetOTableDescr(ResourceOwner owner, descr: &mut OTableDescr)
{
	OTableDescrResOwnerItem **prev = &otable_descr_resowner_items;
	pub static mut O_TABLE_DESCR_RES_OWNER_ITEM: *mut item = *prev;

	while (item)
	{
		if (item->owner == owner && item->descr == descr)
		{
			*prev = item->next;
			pfree(item);
			descr->refcnt--;
			return;
		}
		prev = &item->next;
		item = item->next;
	}
	elog(ERROR, "OTableDescr not remembered by this ResourceOwner");
}

#endif

#if PG_VERSION_NUM >= 170000

fn ResOwnerReleaseOIndexDescr(Datum res);
static ResOwnerPrintOIndexDescr: &mut char(Datum res);

static const ResourceOwnerDesc o_index_descr_resowner_desc =
{
	.name = "OrioleDB OIndexDescr",
	.release_phase = RESOURCE_RELEASE_BEFORE_LOCKS,
	.release_priority = RELEASE_PRIO_RELCACHE_REFS,
	.ReleaseResource = ResOwnerReleaseOIndexDescr,
	.DebugPrint = ResOwnerPrintOIndexDescr
};


ResourceOwnerRememberOIndexDescr(ResourceOwner owner, descr: &mut OIndexDescr)
{
	ResourceOwnerEnlarge(owner);
	descr->refcnt++;
	ResourceOwnerRemember(owner, PointerGetDatum(descr), &o_index_descr_resowner_desc);
}


ResourceOwnerForgetOIndexDescr(ResourceOwner owner, descr: &mut OIndexDescr)
{
	ResourceOwnerForget(owner, PointerGetDatum(descr), &o_index_descr_resowner_desc);
	descr->refcnt--;
}

fn
ResOwnerReleaseOIndexDescr(Datum res)
{
	descr: &mut OIndexDescr = (OIndexDescr *) DatumGetPointer(res);

	descr->refcnt--;
}

static char *
ResOwnerPrintOIndexDescr(Datum res)
{
	descr: &mut OIndexDescr = (OIndexDescr *) DatumGetPointer(res);

	return psprintf("OrioleDB OIndexDescr (%u, %u, %u)",
					descr->oids.datoid, descr->oids.reloid, descr->oids.relnode);
}

#else

// See comment on OTableDescr's PG16 implementation above.
typedef struct OIndexDescrResOwnerItem
{
	pub static mut O_INDEX_DESCR_RES_OWNER_ITEM: *mut struct next = std::ptr::null_mut();
	pub static mut OWNER: ResourceOwner = std::mem::zeroed();
	pub static mut O_INDEX_DESCR: *mut descr = std::ptr::null_mut();
}			OIndexDescrResOwnerItem;

static mut OINDEX_DESCR_RESOWNER_ITEMS: *mut OIndexDescrResOwnerItem = std::ptr::null_mut();
static mut OINDEX_DESCR_RESOWNER_REGISTERED: bool = false;

fn
ResOwnerReleaseOIndexDescrCallback(ResourceReleasePhase phase,
								   bool isCommit, bool isTopLevel,  *arg)
{
	OIndexDescrResOwnerItem **prev;
	pub static mut O_INDEX_DESCR_RES_OWNER_ITEM: *mut item = std::ptr::null_mut();

	if (phase != RESOURCE_RELEASE_BEFORE_LOCKS)
		return;

	prev = &oindex_descr_resowner_items;
	item = *prev;
	while (item)
	{
		if (item->owner == CurrentResourceOwner)
		{
			*prev = item->next;
			item->descr->refcnt--;
			pfree(item);
			item = *prev;
		}
		else
		{
			prev = &item->next;
			item = item->next;
		}
	}
}


ResourceOwnerRememberOIndexDescr(ResourceOwner owner, descr: &mut OIndexDescr)
{
	pub static mut O_INDEX_DESCR_RES_OWNER_ITEM: *mut item = std::ptr::null_mut();

	if (!oindex_descr_resowner_registered)
	{
		RegisterResourceReleaseCallback(ResOwnerReleaseOIndexDescrCallback, NULL);
		oindex_descr_resowner_registered = true;
	}

	item = MemoryContextAlloc(TopMemoryContext, sizeof(*item));
	item->owner = owner;
	item->descr = descr;
	item->next = oindex_descr_resowner_items;
	oindex_descr_resowner_items = item;
	descr->refcnt++;
}


ResourceOwnerForgetOIndexDescr(ResourceOwner owner, descr: &mut OIndexDescr)
{
	OIndexDescrResOwnerItem **prev = &oindex_descr_resowner_items;
	pub static mut O_INDEX_DESCR_RES_OWNER_ITEM: *mut item = *prev;

	while (item)
	{
		if (item->owner == owner && item->descr == descr)
		{
			*prev = item->next;
			pfree(item);
			descr->refcnt--;
			return;
		}
		prev = &item->next;
		item = item->next;
	}
	elog(ERROR, "OIndexDescr not remembered by this ResourceOwner");
}

#endif