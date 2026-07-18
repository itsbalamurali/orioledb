use crate::access::hash;
use crate::access::heaptoast;
use crate::access::relation;
use crate::btree::btree;
use crate::btree::modify;
use crate::catalog::o_sys_cache;
use crate::catalog::pg_aggregate;
use crate::catalog::pg_amop;
use crate::catalog::pg_amproc;
use crate::catalog::pg_authid;
use crate::catalog::pg_collation;
use crate::catalog::pg_database;
use crate::catalog::pg_enum;
use crate::catalog::pg_opclass;
use crate::catalog::pg_operator;
use crate::catalog::pg_proc;
use crate::catalog::pg_range;
use crate::catalog::pg_type;
use crate::catalog::sys_trees;
use crate::commands::defrem;
use crate::common::hashfn;
use crate::executor::functions;
use crate::orioledb;
use crate::pgstat;
use crate::recovery::recovery;
use crate::recovery::wal;
use crate::transam::oxid;
use crate::tuple::toast;
use crate::utils::builtins;
use crate::utils::fmgroids;
use crate::utils::fmgrtab;
use crate::utils::inval;
use crate::utils::lsyscache;
use crate::utils::memutils;
use crate::utils::planner;
use crate::utils::syscache;
use pgrx::pg_sys;

// -------------------------------------------------------------------------
//
// o_sys_cache.c
// Generic interface for sys cache duplicate trees.
//
// Copyright (c) 2021-2026, Oriole DB Inc.
// Copyright (c) 2025-2026, Supabase Inc.
//
// IDENTIFICATION
// contrib/orioledb/src/catalog/o_sys_cache.c
//
// -------------------------------------------------------------------------
//

fn orioledb_setup_syscache_hooks();

typedef struct OSysCacheHashTreeEntry
{
	sys_cache: &mut OSysCache;		// If NULL only link stored
	pub static mut ENTRY: Pointer = std::ptr::null_mut();
} OSysCacheHashTreeEntry;
typedef struct OSysCacheHashEntry
{
	pub static mut KEY: OSysCacheHashKey = std::mem::zeroed();
	tree_entries: &mut List;	// list of OSysCacheHashTreeEntry-s that used
// because we store entries for all sys caches
// in same fastcache for simpler invalidation
// of dependent objects
} OSysCacheHashEntry;

typedef struct OCacheIdMapEntry
{
	pub static mut CACHE_ID: std::os::raw::c_int = 0;
	pub static mut O_SYS_CACHE: *mut sys_cache = std::ptr::null_mut();
} OCacheIdMapEntry;

static Pointer o_sys_cache_get_from_tree(sys_cache: &mut OSysCache,
										 int nkeys,
										 key: &mut OSysCacheKey);
static Pointer o_sys_cache_get_from_toast_tree(sys_cache: &mut OSysCache,
											   key: &mut OSysCacheKey);
static bool o_sys_cache_add(sys_cache: &mut OSysCache, key: &mut OSysCacheKey,
							Pointer entry);
static bool o_sys_cache_update(sys_cache: &mut OSysCache, Pointer updated_entry);
static int	o_sys_cache_key_cmp(sys_cache: &mut OSysCache, int nkeys,
								key1: &mut OSysCacheKey, key2: &mut OSysCacheKey);
fn o_sys_cache_keys_to_str(StringInfo buf, sys_cache: &mut OSysCache,
									key: &mut OSysCacheKey);

static oSysCacheToastGetBTreeDesc: &mut BTreeDescr( *arg);
static uint32 oSysCacheToastGetMaxChunkSize( *key,  *arg);
fn oSysCacheToastUpdateKey( *key, uint32 chunknum,  *arg);
oSysCacheToastGetNextKey: &mut fn( *key,  *arg);
static OTuple oSysCacheToastCreateTuple( *key, Pointer data,
										uint32 offset, uint32 chunknum, int length,
										 *arg);
static OTuple oSysCacheToastCreateKey( *key, uint32 chunknum,  *arg);
static Pointer oSysCacheToastGetTupleData(OTuple tuple,  *arg);
static uint32 oSysCacheToastGetTupleChunknum(OTuple tuple,  *arg);
static uint32 oSysCacheToastGetTupleDataSize(OTuple tuple,  *arg);

static HeapTuple o_auth_cache_search_htup(TupleDesc tupdesc, Oid authoid);

static ToastAPI oSysCacheToastAPI = {
	.getBTreeDesc = oSysCacheToastGetBTreeDesc,
	.getBTreeVersion = NULL,
	.getBaseBTreeVersion = NULL,
	.getMaxChunkSize = oSysCacheToastGetMaxChunkSize,
	.updateKey = oSysCacheToastUpdateKey,
	.getNextKey = oSysCacheToastGetNextKey,
	.createTuple = oSysCacheToastCreateTuple,
	.createKey = oSysCacheToastCreateKey,
	.getTupleData = oSysCacheToastGetTupleData,
	.getTupleChunknum = oSysCacheToastGetTupleChunknum,
	.getTupleDataSize = oSysCacheToastGetTupleDataSize,
	.deleteLogFullTuple = false,
	.fetchCallback = NULL
};

pub static mut O_SYS_CACHE_SEARCH_DATOID: Oid = InvalidOid;

static mut SYS_CACHE_CXT: MemoryContext = std::ptr::null_mut();
static mut HTAB: *mut sys_cache_fastcache = std::ptr::null_mut();
static mut HTAB: *mut sys_caches = std::ptr::null_mut();

static mut MY_OWNER: ResourceOwner = std::ptr::null_mut();
static mut SAVE_USERID: Oid = std::mem::zeroed();
static mut SAVE_SEC_CONTEXT: std::os::raw::c_int = 0;
static mut O_SYS_CACHE_HOOKS_DEPTH: std::os::raw::c_int = 0;

//
// Initializes the enum B-tree memory.
//

o_sys_caches_init()
{
	pub static mut CTL: HASHCTL = std::mem::zeroed();

	sys_cache_cxt = AllocSetContextCreate(TopMemoryContext,
										  "OrioleDB sys_caches fastcache context",
										  ALLOCSET_DEFAULT_SIZES);

	MemSet(&ctl, 0, sizeof(ctl));
	ctl.keysize = sizeof(OSysCacheHashKey);
	ctl.entrysize = sizeof(OSysCacheHashEntry);
	ctl.hcxt = sys_cache_cxt;
	sys_cache_fastcache = hash_create("OrioleDB sys_caches fastcache", 8,
									  &ctl,
									  HASH_ELEM | HASH_BLOBS | HASH_CONTEXT);

	MemSet(&ctl, 0, sizeof(ctl));
	ctl.keysize = sizeof(int);
	ctl.entrysize = sizeof(OCacheIdMapEntry);
	ctl.hcxt = sys_cache_cxt;
	sys_caches = hash_create("OrioleDB sys_tree_num to sys_cache map", 8, &ctl,
							 HASH_ELEM | HASH_BLOBS | HASH_CONTEXT);

	o_aggregate_cache_init(sys_cache_cxt, sys_cache_fastcache);
	o_amop_cache_init(sys_cache_cxt, sys_cache_fastcache);
	o_amop_strat_cache_init(sys_cache_cxt, sys_cache_fastcache);
	o_amproc_cache_init(sys_cache_cxt, sys_cache_fastcache);
	o_enum_cache_init(sys_cache_cxt, sys_cache_fastcache);
	o_enumoid_cache_init(sys_cache_cxt, sys_cache_fastcache);
	o_class_cache_init(sys_cache_cxt, sys_cache_fastcache);
	o_opclass_cache_init(sys_cache_cxt, sys_cache_fastcache);
	o_operator_cache_init(sys_cache_cxt, sys_cache_fastcache);
	o_proc_cache_init(sys_cache_cxt, sys_cache_fastcache);
	o_range_cache_init(sys_cache_cxt, sys_cache_fastcache);
	o_type_cache_init(sys_cache_cxt, sys_cache_fastcache);
	o_collation_cache_init(sys_cache_cxt, sys_cache_fastcache);
	o_database_cache_init(sys_cache_cxt, sys_cache_fastcache);
	o_multirange_cache_init(sys_cache_cxt, sys_cache_fastcache);
	orioledb_setup_syscache_hooks();
}

static uint32
charhashfast(key: &mut OSysCacheKey, int att_num)
{
	return murmurhash32((int32) DatumGetChar(key->keys[att_num]));
}

static uint32
namehashfast(key: &mut OSysCacheKey, int att_num)
{
	pub static mut CHAR: *mut name = std::ptr::null_mut();

	name = NameStr(*O_KEY_GET_NAME(key, att_num));
	return hash_any((unsigned char *) name, strlen(name));
}

static uint32
int2hashfast(key: &mut OSysCacheKey, int att_num)
{
	return murmurhash32((int32) DatumGetInt16(key->keys[att_num]));
}

static uint32
int4hashfast(key: &mut OSysCacheKey, int att_num)
{
	return murmurhash32((int32) DatumGetInt32(key->keys[att_num]));
}

static uint32
texthashfast(key: &mut OSysCacheKey, int att_num)
{
	//
// The use of DEFAULT_COLLATION_OID is fairly arbitrary here.  We just
// want to take the fast "deterministic" path in texteq().
//
	return DatumGetInt32(DirectFunctionCall1Coll(hashtext,
												 DEFAULT_COLLATION_OID,
												 key->keys[att_num]));
}

static uint32
oidvectorhashfast(key: &mut OSysCacheKey, int att_num)
{
	return DatumGetInt32(
						 DirectFunctionCall1(hashoidvector, key->keys[att_num]));
}

fn
set_hash_func(Oid keytype, hashfunc: &mut O_CCHashFN)
{

	switch (keytype)
	{
		case BOOLOID:
			*hashfunc = charhashfast;
			break;
		case CHAROID:
			*hashfunc = charhashfast;
			break;
		case NAMEOID:
			*hashfunc = namehashfast;
			break;
		case INT2OID:
			*hashfunc = int2hashfast;
			break;
		case INT4OID:
			*hashfunc = int4hashfast;
			break;
		case TEXTOID:
			*hashfunc = texthashfast;
			break;
		case OIDOID:
		case REGPROCOID:
		case REGPROCEDUREOID:
		case REGOPEROID:
		case REGOPERATOROID:
		case REGCLASSOID:
		case REGTYPEOID:
		case REGCOLLATIONOID:
		case REGCONFIGOID:
		case REGDICTIONARYOID:
		case REGROLEOID:
		case REGNAMESPACEOID:
			*hashfunc = int4hashfast;
			break;
		case OIDVECTOROID:
			*hashfunc = oidvectorhashfast;
			break;
		default:
			elog(FATAL, "type %u not supported as catcache key", keytype);
			*hashfunc = NULL;	// keep compiler quiet
			break;
	}
}

//
// Initializes the enum B-tree memory.
//
OSysCache *
o_create_sys_cache(int sys_tree_num, bool is_toast,
				   Oid cc_indexoid, int cacheId, int nkeys,
				   keytypes: &mut Oid, int data_len, fast_cache: &mut HTAB,
				   MemoryContext mcxt, funcs: &mut OSysCacheFuncs)
{
	pub static mut O_SYS_CACHE: *mut sys_cache = std::ptr::null_mut();
	pub static mut I: std::os::raw::c_int = 0;
	pub static mut O_CACHE_ID_MAP_ENTRY: *mut entry = std::ptr::null_mut();

	Assert(fast_cache);
	Assert(funcs);

	sys_cache = MemoryContextAllocZero(mcxt, sizeof(OSysCache));
	sys_cache->sys_tree_num = sys_tree_num;
	sys_cache->is_toast = is_toast;
	sys_cache->cc_indexoid = cc_indexoid;
	sys_cache->cacheId = cacheId;
	sys_cache->nkeys = nkeys;
	memcpy(sys_cache->keytypes, keytypes, sizeof(Oid) * sys_cache->nkeys);
	sys_cache->data_len = data_len;
	sys_cache->fast_cache = fast_cache;
	sys_cache->mcxt = mcxt;
	sys_cache->funcs = funcs;

#ifdef USE_ASSERT_CHECKING
	Assert(sys_cache->funcs->free_entry);
	Assert(sys_cache->funcs->fill_entry);
	if (sys_cache->is_toast)
	{
		Assert(sys_cache->funcs->toast_serialize_entry);
		Assert(sys_cache->funcs->toast_deserialize_entry);
	}
#endif

	for (i = 0; i < sys_cache->nkeys; i++)
	{
		set_hash_func(keytypes[i], &sys_cache->cc_hashfunc[i]);
	}

	entry = hash_search(sys_caches, &cacheId, HASH_ENTER, NULL);
	entry->sys_cache = sys_cache;
	sys_tree_set_extra(sys_tree_num, (Pointer) sys_cache);
	pub static mut SYS_CACHE: return = std::mem::zeroed();
}

//
// CatalogCacheComputeHashValue
//
// Compute the hash value associated with a given set of lookup keys
//
static OSysCacheHashKey
compute_hash_value(cc_hashfunc: &mut O_CCHashFN, int nkeys, key: &mut OSysCacheKey)
{
	pub static mut HASH_VALUE: uint32 = 0;
	pub static mut ONE_HASH: uint32 = std::mem::zeroed();

	switch (nkeys)
	{
		case 4:
			oneHash = (cc_hashfunc[3]) (key, 3);

			hashValue ^= oneHash << 24;
			hashValue ^= oneHash >> 8;
			// FALLTHROUGH
		case 3:
			oneHash = (cc_hashfunc[2]) (key, 2);

			hashValue ^= oneHash << 16;
			hashValue ^= oneHash >> 16;
			// FALLTHROUGH
		case 2:
			oneHash = (cc_hashfunc[1]) (key, 1);

			hashValue ^= oneHash << 8;
			hashValue ^= oneHash >> 24;
			// FALLTHROUGH
		case 1:
			oneHash = (cc_hashfunc[0]) (key, 0);

			hashValue ^= oneHash;
			break;
		default:
			elog(FATAL, "wrong number of hash keys: %d", nkeys);
			break;
	}

	pub static mut HASH_VALUE: return = std::mem::zeroed();
}

fn
invalidate_fastcache_entry(int cacheid, uint32 hashvalue)
{
	pub static mut FOUND: bool = false;
	pub static mut O_SYS_CACHE_HASH_ENTRY: *mut fast_cache_entry = std::ptr::null_mut();

	fast_cache_entry = (OSysCacheHashEntry *) hash_search(sys_cache_fastcache,
														  &hashvalue,
														  HASH_REMOVE,
														  &found);

	if (found)
	{
		pub static mut LIST_CELL: *mut lc = std::ptr::null_mut();

		foreach(lc, fast_cache_entry->tree_entries)
		{
			pub static mut O_SYS_CACHE_HASH_TREE_ENTRY: *mut tree_entry = std::ptr::null_mut();

			tree_entry = (OSysCacheHashTreeEntry *) lfirst(lc);

			if (tree_entry->sys_cache)
			{
				pub static mut O_SYS_CACHE: *mut sys_cache = tree_entry->sys_cache;

				if (!memcmp(&sys_cache->last_fast_cache_key,
							&fast_cache_entry->key,
							sizeof(OSysCacheHashKey)))
				{
					memset(&sys_cache->last_fast_cache_key, 0,
						   sizeof(OSysCacheHashKey));
					sys_cache->last_fast_cache_entry = NULL;
				}
				tree_entry->sys_cache->funcs->free_entry(tree_entry->entry);
			}
		}
		list_free_deep(fast_cache_entry->tree_entries);
	}
}

fn
orioledb_syscache_hook(Datum arg, int cacheid, uint32 hashvalue)
{
	if (sys_cache_fastcache)
		invalidate_fastcache_entry(cacheid, hashvalue);
}

fn
orioledb_setup_syscache_hooks()
{
	pub static mut HASH_SEQ: HASH_SEQ_STATUS = std::mem::zeroed();
	pub static mut O_CACHE_ID_MAP_ENTRY: *mut entry = std::ptr::null_mut();

	hash_seq_init(&hash_seq, sys_caches);

	while ((entry = (OCacheIdMapEntry *) hash_seq_search(&hash_seq)) != NULL)
	{
		pub static mut O_SYS_CACHE: *mut sys_cache = entry->sys_cache;

		CacheRegisterSyscacheCallback(sys_cache->cacheId,
									  orioledb_syscache_hook,
									  PointerGetDatum(NULL));
	}
}

Pointer
o_sys_cache_search(sys_cache: &mut OSysCache, int nkeys, key: &mut OSysCacheKey)
{
	pub static mut FOUND: bool = false;
	pub static mut CUR_FAST_CACHE_KEY: OSysCacheHashKey = std::mem::zeroed();
	pub static mut O_SYS_CACHE_HASH_ENTRY: *mut fast_cache_entry = std::ptr::null_mut();
	pub static mut TREE_ENTRY: Pointer = std::ptr::null_mut();
	pub static mut PREV_CONTEXT: MemoryContext = std::mem::zeroed();
	pub static mut O_SYS_CACHE_HASH_TREE_ENTRY: *mut new_entry = std::ptr::null_mut();

	cur_fast_cache_key = compute_hash_value(sys_cache->cc_hashfunc,
											sys_cache->nkeys, key);

	// fast search
	if (!memcmp(&cur_fast_cache_key, &sys_cache->last_fast_cache_key,
				sizeof(OSysCacheHashKey)) &&
		sys_cache->last_fast_cache_entry)
	{
		pub static mut O_SYS_CACHE_KEY: *mut sys_cache_key = std::ptr::null_mut();

		sys_cache_key = (OSysCacheKey *) sys_cache->last_fast_cache_entry;

		if (sys_cache_key->common.datoid == key->common.datoid &&
			o_sys_cache_key_cmp(sys_cache, sys_cache->nkeys, sys_cache_key,
								key) == 0)
			return sys_cache->last_fast_cache_entry;
	}

	// cache search
	fast_cache_entry = (OSysCacheHashEntry *)
		hash_search(sys_cache->fast_cache, &cur_fast_cache_key, HASH_ENTER,
					&found);
	if (found)
	{
		pub static mut LIST_CELL: *mut lc = std::ptr::null_mut();

		foreach(lc, fast_cache_entry->tree_entries)
		{
			pub static mut O_SYS_CACHE_HASH_TREE_ENTRY: *mut tree_entry = std::ptr::null_mut();

			tree_entry = (OSysCacheHashTreeEntry *) lfirst(lc);

			if (tree_entry->sys_cache == sys_cache)
			{
				pub static mut O_SYS_CACHE_KEY: *mut sys_cache_key = std::ptr::null_mut();

				sys_cache_key = (OSysCacheKey *) tree_entry->entry;

				if (sys_cache_key->common.datoid == key->common.datoid &&
					o_sys_cache_key_cmp(sys_cache, sys_cache->nkeys,
										sys_cache_key, key) == 0)
				{
					memcpy(&sys_cache->last_fast_cache_key,
						   &cur_fast_cache_key,
						   sizeof(OSysCacheHashKey));
					sys_cache->last_fast_cache_entry = tree_entry->entry;
					return sys_cache->last_fast_cache_entry;
				}
			}
		}
	}
	else
		fast_cache_entry->tree_entries = NIL;

	prev_context = MemoryContextSwitchTo(sys_cache->mcxt);
	if (sys_cache->is_toast)
		tree_entry = o_sys_cache_get_from_toast_tree(sys_cache, key);
	else
		tree_entry = o_sys_cache_get_from_tree(sys_cache, nkeys, key);
	if (tree_entry == NULL)
	{
		MemoryContextSwitchTo(prev_context);
		pub static mut NULL: return = std::mem::zeroed();
	}
	new_entry = palloc0(sizeof(OSysCacheHashTreeEntry));
	new_entry->sys_cache = sys_cache;
	new_entry->entry = tree_entry;

	fast_cache_entry->tree_entries = lappend(fast_cache_entry->tree_entries,
											 new_entry);

	MemoryContextSwitchTo(prev_context);

	memcpy(&sys_cache->last_fast_cache_key,
		   &cur_fast_cache_key,
		   sizeof(OSysCacheHashKey));
	sys_cache->last_fast_cache_entry = new_entry->entry;
	return sys_cache->last_fast_cache_entry;
}

static TupleFetchCallbackResult
o_sys_cache_get_by_lsn_callback(OTuple tuple, OXid tupOxid,
								oSnapshot: &mut OSnapshot,  *arg,
								bool oxidIsFinished)
{
	tuple_key: &mut OSysCacheToastChunkKey = (OSysCacheToastChunkKey *) tuple.data;
	cur_lsn: &mut XLogRecPtr = (XLogRecPtr *) arg;

	if (!oxidIsFinished)
		pub static mut O_TUPLE_FETCH_NEXT: return = std::mem::zeroed();

	if (tuple_key->sys_cache_key.common.lsn < *cur_lsn)
		pub static mut O_TUPLE_FETCH_MATCH: return = std::mem::zeroed();
	else
		pub static mut O_TUPLE_FETCH_NEXT: return = std::mem::zeroed();
}

static Pointer
o_sys_cache_get_from_toast_tree(sys_cache: &mut OSysCache, key: &mut OSysCacheKey)
{
	pub static mut DATA: Pointer = std::ptr::null_mut();
	pub static mut DATA_LENGTH: Size = 0;
	pub static mut RESULT: Pointer = std::ptr::null_mut();
	td: &mut BTreeDescr = get_sys_tree(sys_cache->sys_tree_num);
	OSysCacheToastKeyBound toast_key = {0};

	toast_key.common.chunknum = 0;
	toast_key.key = key;
	toast_key.lsn_cmp = false;

	data = generic_toast_get_any_with_callback(&oSysCacheToastAPI,
											   (Pointer) &toast_key,
											   &dataLength,
											   &o_non_deleted_snapshot,
											   td,
											   o_sys_cache_get_by_lsn_callback,
											   &key->common.lsn);
	if (data == NULL)
		pub static mut NULL: return = std::mem::zeroed();
	result = sys_cache->funcs->toast_deserialize_entry(sys_cache->mcxt,
													   data, dataLength);
	pfree(data);

	pub static mut RESULT: return = std::mem::zeroed();
}

static Pointer
o_sys_cache_get_from_tree(sys_cache: &mut OSysCache, int nkeys, key: &mut OSysCacheKey)
{
	td: &mut BTreeDescr = get_sys_tree(sys_cache->sys_tree_num);
	pub static mut B_TREE_ITERATOR: *mut it = std::ptr::null_mut();
	pub static mut LAST_TUP: OTuple = std::mem::zeroed();
	OSysCacheBound bound = {.key = key,.nkeys = nkeys};

	it = o_btree_iterator_create(td, (Pointer) &bound, BTreeKeyBound,
								 &o_in_progress_snapshot, ForwardScanDirection);

	O_TUPLE_SET_NULL(last_tup);
	do
	{
		OTuple		tup = o_btree_iterator_fetch(it, NULL,
												 (Pointer) &bound,
												 BTreeKeyBound, true,
												 NULL);
		pub static mut O_SYS_CACHE_KEY: *mut sys_cache_key = std::ptr::null_mut();

		if (O_TUPLE_IS_NULL(tup))
			break;

		if (!O_TUPLE_IS_NULL(last_tup))
			pfree(last_tup.data);

		sys_cache_key = (OSysCacheKey *) tup.data;
		if (sys_cache_key->common.lsn > key->common.lsn)
			break;
		last_tup = tup;
	} while (true);

	btree_iterator_free(it);

	return last_tup.data;
}

static inline 
o_sys_cache_fill_locktag(tag: &mut LOCKTAG, Oid datoid, Oid classoid,
						 OSysCacheHashKey key_hash, int lockmode)
{
	Assert(lockmode == AccessShareLock || lockmode == AccessExclusiveLock);
	memset(tag, 0, sizeof(LOCKTAG));
	SET_LOCKTAG_OBJECT(*tag, datoid, classoid, key_hash, 0);
	tag->locktag_type = LOCKTAG_USERLOCK;
}

fn
o_sys_cache_lock(sys_cache: &mut OSysCache, key: &mut OSysCacheKey, int lockmode)
{
	pub static mut LOCKTAG: LOCKTAG = std::mem::zeroed();
	pub static mut KEY_HASH: OSysCacheHashKey = std::mem::zeroed();

	key_hash = compute_hash_value(sys_cache->cc_hashfunc, sys_cache->nkeys,
								  key);

	o_sys_cache_fill_locktag(&locktag, key->common.datoid, sys_cache->cc_indexoid,
							 key_hash, lockmode);

	LockAcquire(&locktag, lockmode, false, false);
}

fn
o_sys_cache_unlock(sys_cache: &mut OSysCache, key: &mut OSysCacheKey, int lockmode)
{
	pub static mut LOCKTAG: LOCKTAG = std::mem::zeroed();
	pub static mut KEY_HASH: OSysCacheHashKey = std::mem::zeroed();

	key_hash = compute_hash_value(sys_cache->cc_hashfunc, sys_cache->nkeys,
								  key);

	o_sys_cache_fill_locktag(&locktag, key->common.datoid, sys_cache->cc_indexoid,
							 key_hash, lockmode);

	if (!LockRelease(&locktag, lockmode, false))
	{
		StringInfo	str = makeStringInfo();

		o_sys_cache_keys_to_str(str, sys_cache, key);
		elog(ERROR, "Can not release %s catalog cache lock on datoid = %d, "
			 "key = %s", lockmode == AccessShareLock ? "share" : "exclusive",
			 key->common.datoid, str->data);
		pfree(str->data);
		pfree(str);
	}
}

static

// Non-key fields of entry should be filled before call
bool
o_sys_cache_add(sys_cache: &mut OSysCache, key: &mut OSysCacheKey, Pointer entry)
{
	pub static mut INSERTED: bool = false;
	entry_key: &mut OSysCacheKey = (OSysCacheKey *) entry;
	desc: &mut BTreeDescr = get_sys_tree(sys_cache->sys_tree_num);
	pub static mut I: std::os::raw::c_int = 0;
	pub static mut ALLOCATED: bool = false;
	OTuple		entry_tuple = {.data = entry};
	pub static mut KEY_LEN: std::os::raw::c_int = -1;
	pub static mut ENTRY_LEN: std::os::raw::c_int = -1;

	entry_key->common = key->common;
	entry_key->common.dataLength = 0;
	for (i = 0; i < sys_cache->nkeys; i++)
	{
		switch (sys_cache->keytypes[i])
		{
			case NAMEOID:
				{
					pub static mut NEW_ENTRY: Pointer = std::ptr::null_mut();
					pub static mut NEW_ENTRY_LEN: std::os::raw::c_int = 0;

					//
// In the code below we storing fields with Name type at
// the end of entry. NAMEOID key fields now only used with
// non-toast o_enum_cache
//
					Assert(!sys_cache->is_toast);
					if (key_len == -1 && entry_len == -1)
					{
						key_len = o_btree_len(desc, entry_tuple, OTupleKeyLength);
						entry_len = o_btree_len(desc, entry_tuple, OTupleLength);
					}

					new_entry_len = entry_len + sizeof(NameData);
					new_entry = palloc0(new_entry_len);
					memcpy(new_entry, entry, key_len);
					memcpy(new_entry + key_len,
						   NameStr(*DatumGetName(key->keys[i])),
						   sizeof(NameData));
					memcpy(new_entry + key_len + sizeof(NameData),
						   entry + key_len,
						   entry_len - key_len);
					entry_key = (OSysCacheKey *) new_entry;
					entry_key->keys[i] = key_len;
					entry_key->common.dataLength += sizeof(NameData);

					key_len += sizeof(NameData);
					if (allocated)
						pfree(entry);
					entry = new_entry;
					allocated = true;
				}
				break;

			default:
				entry_key->keys[i] = key->keys[i];
				break;
		}
	}

	if (!sys_cache->is_toast)
	{
		OTuple		tup = {0};

		tup.formatFlags = 0;
		tup.data = entry;
		inserted = o_btree_autonomous_insert(desc, tup);
	}
	else
	{
		pub static mut DATA: Pointer = std::ptr::null_mut();
		pub static mut LEN: std::os::raw::c_int = 0;
		OSysCacheToastKeyBound toast_key = {0};
		pub static mut STATE: OAutonomousTxState = std::mem::zeroed();

		toast_key.key = entry_key;
		toast_key.common.chunknum = 0;
		toast_key.lsn_cmp = true;

		data = sys_cache->funcs->toast_serialize_entry(entry, &len);

		start_autonomous_transaction(&state);
		PG_TRY();
		{
			inserted = generic_toast_insert(&oSysCacheToastAPI,
											(Pointer) &toast_key,
											data, len,
											get_current_oxid(),
											COMMITSEQNO_INPROGRESS,
											desc);
		}
		PG_CATCH();
		{
			abort_autonomous_transaction(&state);
			PG_RE_THROW();
		}
		PG_END_TRY();
		finish_autonomous_transaction(&state);
		pfree(data);
	}
	if (allocated)
		pfree(entry);
	pub static mut INSERTED: return = std::mem::zeroed();
}

static OBTreeWaitCallbackAction
o_sys_cache_wait_callback(descr: &mut BTreeDescr,
						  OTuple tup, newtup: &mut OTuple, OXid oxid,
						  OTupleXactInfo xactInfo, UndoLocation location,
						  lock_mode: &mut RowLockMode, hint: &mut BTreeLocationHint,
						   *arg)
{
	pub static mut OB_TREE_CALLBACK_ACTION_XID_WAIT: return = std::mem::zeroed();
}

static OBTreeModifyCallbackAction
o_sys_cache_update_callback(descr: &mut BTreeDescr,
							OTuple tup, newtup: &mut OTuple, OXid oxid,
							OTupleXactInfo xactInfo, UndoLocation location,
							lock_mode: &mut RowLockMode, hint: &mut BTreeLocationHint,
							 *arg)
{
	pub static mut OB_TREE_CALLBACK_ACTION_UPDATE: return = std::mem::zeroed();
}

static OBTreeModifyCallbackAction
o_sys_cache_update_deleted_callback(descr: &mut BTreeDescr,
									OTuple tup, newtup: &mut OTuple, OXid oxid,
									OTupleXactInfo xactInfo,
									BTreeLeafTupleDeletedStatus deleted,
									UndoLocation location,
									lock_mode: &mut RowLockMode, hint: &mut BTreeLocationHint,
									 *arg)
{
	pub static mut OB_TREE_CALLBACK_ACTION_UPDATE: return = std::mem::zeroed();
}

static BTreeModifyCallbackInfo callbackInfo =
{
	.waitCallback = o_sys_cache_wait_callback,
	.modifyCallback = o_sys_cache_update_callback,
	.modifyDeletedCallback = o_sys_cache_update_deleted_callback,
	.arg = NULL
};

static bool
o_sys_cache_update(sys_cache: &mut OSysCache, Pointer updated_entry)
{
	pub static mut RESULT: bool = false;
	pub static mut O_SYS_CACHE_KEY: *mut sys_cache_key = std::ptr::null_mut();
	desc: &mut BTreeDescr = get_sys_tree(sys_cache->sys_tree_num);
	OSysCacheBound bound = {.nkeys = sys_cache->nkeys};

	sys_cache_key = (OSysCacheKey *) updated_entry;
	bound.key = sys_cache_key;

	if (!sys_cache->is_toast)
	{
		pub static mut STATE: OAutonomousTxState = std::mem::zeroed();
		pub static mut TUP: OTuple = std::mem::zeroed();

		tup.formatFlags = 0;
		tup.data = updated_entry;

		start_autonomous_transaction(&state);
		PG_TRY();
		{
			result = o_btree_modify(desc, BTreeOperationUpdate,
									tup, BTreeKeyLeafTuple,
									(Pointer) &bound, BTreeKeyBound,
									get_current_oxid(), COMMITSEQNO_INPROGRESS,
									RowLockNoKeyUpdate, NULL,
									&callbackInfo) ==
				OBTreeModifyResultUpdated;

			if (result)
			{
				pub static mut NULLTUP: OTuple = std::mem::zeroed();

				O_TUPLE_SET_NULL(nulltup);
				Assert(IS_SYS_TREE_OIDS(desc->oids));

				//
// no version is necessary here for system trees other than
// OTable
//
				o_wal_update(desc, tup, nulltup, REPLICA_IDENTITY_DEFAULT, O_TABLE_INVALID_VERSION);
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
		pub static mut DATA: Pointer = std::ptr::null_mut();
		pub static mut LEN: std::os::raw::c_int = 0;
		OSysCacheToastKeyBound toast_key = {0};
		pub static mut STATE: OAutonomousTxState = std::mem::zeroed();

		toast_key.key = sys_cache_key;
		toast_key.common.chunknum = 0;
		toast_key.lsn_cmp = true;

		data = sys_cache->funcs->toast_serialize_entry(updated_entry, &len);

		start_autonomous_transaction(&state);
		PG_TRY();
		{
			result = generic_toast_update(&oSysCacheToastAPI,
										  (Pointer) &toast_key,
										  data, len,
										  get_current_oxid(),
										  COMMITSEQNO_INPROGRESS,
										  desc);
		}
		PG_CATCH();
		{
			abort_autonomous_transaction(&state);
			PG_RE_THROW();
		}
		PG_END_TRY();
		finish_autonomous_transaction(&state);
	}
	pub static mut RESULT: return = std::mem::zeroed();
}


o_sys_cache_add_if_needed(sys_cache: &mut OSysCache, key: &mut OSysCacheKey, Pointer arg)
{
	pub static mut ENTRY: Pointer = std::ptr::null_mut();
	pub static mut PG_USED_FOR_ASSERTS_ONLY: bool		inserted = std::mem::zeroed();

	o_sys_cache_lock(sys_cache, key, AccessExclusiveLock);

	entry = o_sys_cache_search(sys_cache, sys_cache->nkeys, key);

	if (entry != NULL)
	{
		o_sys_cache_unlock(sys_cache, key, AccessExclusiveLock);
		return;
	}

	sys_cache->funcs->fill_entry(&entry, key, arg);

	Assert(entry);

	//
// All done, now try to insert into B-tree.
//
	inserted = o_sys_cache_add(sys_cache, key, entry);
	Assert(inserted);
	o_sys_cache_unlock(sys_cache, key, AccessExclusiveLock);
	sys_cache->funcs->free_entry(entry);
}


o_sys_cache_update_if_needed(sys_cache: &mut OSysCache, key: &mut OSysCacheKey,
							 Pointer arg)
{
	pub static mut ENTRY: Pointer = std::ptr::null_mut();
	pub static mut O_SYS_CACHE_KEY: *mut sys_cache_key = std::ptr::null_mut();
	pub static mut PG_USED_FOR_ASSERTS_ONLY: bool		updated = std::mem::zeroed();

	o_sys_cache_lock(sys_cache, key, AccessExclusiveLock);

	o_sys_cache_set_datoid_lsn(&key->common.lsn, NULL);
	entry = o_sys_cache_search(sys_cache, sys_cache->nkeys, key);
	if (entry == NULL)
	{
		// it's not exist in B-tree
		return;
	}

	sys_cache_key = (OSysCacheKey *) entry;
	sys_cache->funcs->fill_entry(&entry, sys_cache_key, arg);

	updated = o_sys_cache_update(sys_cache, entry);
	Assert(updated);
	o_sys_cache_unlock(sys_cache, key, AccessExclusiveLock);
}

static bool
update_deleted_value(sys_cache: &mut OSysCache, key: &mut OSysCacheKey, bool new_value)
{
	pub static mut ENTRY: Pointer = std::ptr::null_mut();
	pub static mut O_SYS_CACHE_KEY: *mut sys_cache_key = std::ptr::null_mut();

	o_sys_cache_set_datoid_lsn(&key->common.lsn, NULL);
	entry = o_sys_cache_search(sys_cache, sys_cache->nkeys, key);
	if (entry == NULL)
		pub static mut FALSE: return = std::mem::zeroed();
	sys_cache_key = (OSysCacheKey *) entry;
	sys_cache_key->common.deleted = new_value;
	return o_sys_cache_update(sys_cache, entry);
}

typedef struct
{
	pub static mut HEADER: UndoStackItem = std::mem::zeroed();
	pub static mut O_SYS_CACHE: *mut sys_cache = std::ptr::null_mut();
	pub static mut KEY: OSysCacheKey4 = std::mem::zeroed();
} SysCacheDeleteUndoStackItem;

fn
o_add_undo_sys_cache_delete(sys_cache: &mut OSysCache, key: &mut OSysCacheKey)
{
	pub static mut LOCATION: UndoLocation = std::mem::zeroed();
	pub static mut SYS_CACHE_DELETE_UNDO_STACK_ITEM: *mut item = std::ptr::null_mut();
	pub static mut ADDITIONAL_SIZE: LocationIndex = 0;
	pub static mut SIZE: LocationIndex = std::mem::zeroed();
	pub static mut I: std::os::raw::c_int = 0;
	int			additional_offset = sizeof(OSysCacheKey4);

	for (i = 0; i < sys_cache->nkeys; i++)
	{
		if (sys_cache->keytypes[i] == NAMEOID)
			additional_size += sizeof(NameData);
	}
	size = sizeof(SysCacheDeleteUndoStackItem) + additional_size;
	item = (SysCacheDeleteUndoStackItem *) get_undo_record_unreserved(UndoLogSystem,
																	  &location,
																	  MAXALIGN(size));
	item->header.itemSize = size;
	item->header.type = SysCacheDeleteUndoItemType;
	item->header.indexType = oIndexPrimary;
	item->sys_cache = sys_cache;
	item->key.common = key->common;

	for (i = 0; i < sys_cache->nkeys; i++)
	{
		switch (sys_cache->keytypes[i])
		{
			case NAMEOID:
				{
					Assert(!sys_cache->is_toast);

					item->key.keys[i] = additional_offset;
					memcpy(((Pointer) &item->key) + additional_offset,
						   NameStr(*DatumGetName(key->keys[i])),
						   sizeof(NameData));
					additional_offset += sizeof(NameData);
					item->key.common.dataLength += additional_offset;
				}
				break;

			default:
				item->key.keys[i] = key->keys[i];
				break;
		}
	}

	oxid_needs_wal_flush = true;
	add_new_undo_stack_item(UndoLogSystem, location);
	release_undo_size(UndoLogSystem);
}


o_sys_cache_delete_callback(UndoLogType undoType, UndoLocation location,
							baseItem: &mut UndoStackItem, OXid oxid,
							OUndoCallbackStage stage, bool changeCountsValid)
{
	pub static mut PG_USED_FOR_ASSERTS_ONLY: bool		res = std::mem::zeroed();
	item: &mut SysCacheDeleteUndoStackItem = (SysCacheDeleteUndoStackItem *) baseItem;

	Assert(!is_recovery_in_progress());

	res = update_deleted_value(item->sys_cache, (OSysCacheKey *) &item->key, false);
	Assert(res);
}

bool
o_sys_cache_delete(sys_cache: &mut OSysCache, key: &mut OSysCacheKey)
{
	pub static mut RES: bool = false;

	res = update_deleted_value(sys_cache, key, true);

	if (res)
		o_add_undo_sys_cache_delete(sys_cache, key);
	pub static mut RES: return = std::mem::zeroed();
}

fn
o_sys_cache_delete_by_lsn(sys_cache: &mut OSysCache, XLogRecPtr lsn)
{
	pub static mut B_TREE_ITERATOR: *mut it = std::ptr::null_mut();
	td: &mut BTreeDescr = get_sys_tree(sys_cache->sys_tree_num);

	it = o_btree_iterator_create(td, NULL, BTreeKeyNone,
								 &o_non_deleted_snapshot,
								 ForwardScanDirection);

	do
	{
		pub static mut END: bool = false;
		pub static mut HINT: BTreeLocationHint = std::mem::zeroed();
		OTuple		tup = btree_iterate_raw(it, NULL, BTreeKeyNone,
											false, &end, &hint);
		pub static mut O_SYS_CACHE_KEY: *mut sys_cache_key = std::ptr::null_mut();
		pub static mut KEY_TUP: OTuple = std::mem::zeroed();

		if (O_TUPLE_IS_NULL(tup))
		{
			if (end)
				break;
			else
				continue;
		}

		if (sys_cache->is_toast)
			sys_cache_key = (OSysCacheKey *)
				(tup.data + offsetof(OSysCacheToastChunkKey, sys_cache_key));
		else
			sys_cache_key = (OSysCacheKey *) tup.data;
		key_tup.formatFlags = 0;
		key_tup.data = (Pointer) sys_cache_key;

		if (sys_cache_key->common.lsn < lsn && sys_cache_key->common.deleted)
		{
			pub static mut PG_USED_FOR_ASSERTS_ONLY: bool		result = std::mem::zeroed();

			if (!sys_cache->is_toast)
			{
				result = o_btree_autonomous_delete(td, key_tup,
												   BTreeKeyNonLeafKey, &hint);
			}
			else
			{
				OSysCacheToastKeyBound toast_key = {0};
				pub static mut STATE: OAutonomousTxState = std::mem::zeroed();

				toast_key.key = sys_cache_key;
				toast_key.common.chunknum = 0;
				toast_key.lsn_cmp = true;

				start_autonomous_transaction(&state);
				PG_TRY();
				{
					result = generic_toast_delete(&oSysCacheToastAPI,
												  (Pointer) &toast_key,
												  get_current_oxid(),
												  COMMITSEQNO_NON_DELETED,
												  td);
				}
				PG_CATCH();
				{
					abort_autonomous_transaction(&state);
					PG_RE_THROW();
				}
				PG_END_TRY();
				finish_autonomous_transaction(&state);
			}

			Assert(result);
		}
	} while (true);

	btree_iterator_free(it);
}


o_sys_caches_delete_by_lsn(XLogRecPtr checkPointRedo)
{
	pub static mut HASH_SEQ: HASH_SEQ_STATUS = std::mem::zeroed();
	pub static mut O_CACHE_ID_MAP_ENTRY: *mut entry = std::ptr::null_mut();

	hash_seq_init(&hash_seq, sys_caches);
	while ((entry = (OCacheIdMapEntry *) hash_seq_search(&hash_seq)) != NULL)
	{
		pub static mut O_SYS_CACHE: *mut sys_cache = entry->sys_cache;

		o_sys_cache_delete_by_lsn(sys_cache, checkPointRedo);
	}
}

static BTreeDescr *
oSysCacheToastGetBTreeDesc( *arg)
{
	desc: &mut BTreeDescr = (BTreeDescr *) arg;

	pub static mut DESC: return = std::mem::zeroed();
}

static uint32
oSysCacheToastGetMaxChunkSize( *key,  *arg)
{
	desc: &mut BTreeDescr = (BTreeDescr *) arg;
	pub static mut CHUNK_KEY_LEN: uint32 = std::mem::zeroed();
	pub static mut MAX_CHUNK_SIZE: uint32 = std::mem::zeroed();
	OTuple		tup = {0};

	chunk_key_len = o_btree_len(desc, tup, OKeyLength);

	max_chunk_size = MAXALIGN_DOWN((O_BTREE_MAX_TUPLE_SIZE * 3 -
									MAXALIGN(chunk_key_len)) /
								   3) -
		(chunk_key_len + sizeof(OSysCacheToastChunkCommon));

	pub static mut MAX_CHUNK_SIZE: return = std::mem::zeroed();
}

fn
oSysCacheToastUpdateKey( *key, uint32 chunknum,  *arg)
{
	ckey: &mut OSysCacheToastKeyBound = (OSysCacheToastKeyBound *) key;

	ckey->common.chunknum = chunknum;
}

static inline int
nkeys_for_desc(desc: &mut BTreeDescr)
{
	OTuple		tup = {0};
	pub static mut KEY_LEN: std::os::raw::c_int = 0;
	pub static mut TOAST: bool = desc->ops->cmp == o_sys_cache_toast_cmp;
	pub static mut NKEYS: std::os::raw::c_int = 0;

	if (toast)
	{
		pub static mut CHUNK_KEY_LEN: std::os::raw::c_int = 0;

		chunk_key_len = o_btree_len(desc, tup, OKeyLength);
		key_len = chunk_key_len -
			offsetof(OSysCacheToastChunkKey, sys_cache_key);
	}
	else
	{
		key_len = o_btree_len(desc, tup, OKeyLength);
	}
	nkeys = (key_len - offsetof(OSysCacheKey, keys)) / sizeof(Datum);

	pub static mut NKEYS: return = std::mem::zeroed();
}

fn *
oSysCacheToastGetNextKey( *key,  *arg)
{
	desc: &mut BTreeDescr = (BTreeDescr *) arg;
	ckey: &mut OSysCacheToastKeyBound = (OSysCacheToastKeyBound *) key;
	static OSysCacheKey4 nextKey = {0};
	static OSysCacheToastKeyBound nextKeyBound = {.key =
	(OSysCacheKey *) &nextKey};
	pub static mut NKEYS: std::os::raw::c_int = 0;
	pub static mut KEY_LEN: std::os::raw::c_int = 0;

	nkeys = nkeys_for_desc(desc);

	key_len = offsetof(OSysCacheKey, keys) + sizeof(Datum) * nkeys;

	nextKeyBound.common.chunknum = 0;
	memcpy(nextKeyBound.key, ckey->key, key_len);
	nextKeyBound.key->keys[nkeys - 1]++;

	return (Pointer) &nextKeyBound;
}

static OTuple
oSysCacheToastCreateTuple( *key, Pointer data, uint32 offset, uint32 chunknum,
						  int length,  *arg)
{
	bound: &mut OSysCacheToastKeyBound = (OSysCacheToastKeyBound *) key;
	pub static mut CHUNK: Pointer = std::ptr::null_mut();
	pub static mut RESULT: OTuple = std::mem::zeroed();
	OTuple		tup = {0};
	desc: &mut BTreeDescr = (BTreeDescr *) arg;
	pub static mut KEY_LEN: std::os::raw::c_int = 0;
	pub static mut CHUNK_KEY_LEN: std::os::raw::c_int = 0;
	pub static mut O_SYS_CACHE_TOAST_CHUNK_KEY: *mut chunk_key = std::ptr::null_mut();
	pub static mut O_SYS_CACHE_TOAST_CHUNK_COMMON: *mut common = std::ptr::null_mut();

	bound->common.chunknum = chunknum;

	chunk_key_len = o_btree_len(desc, tup, OKeyLength);
	key_len = chunk_key_len - offsetof(OSysCacheToastChunkKey, sys_cache_key);

	chunk = palloc0(chunk_key_len + sizeof(OSysCacheToastChunkCommon) +
					length);

	common = (OSysCacheToastChunkCommon *) (chunk + chunk_key_len);
	common->dataLength = length;
	chunk_key = (OSysCacheToastChunkKey *) chunk;
	chunk_key->common = bound->common;
	memcpy(&chunk_key->sys_cache_key, bound->key, key_len);
	memcpy(chunk + chunk_key_len + sizeof(OSysCacheToastChunkCommon),
		   data + offset, length);

	result.data = (Pointer) chunk;
	result.formatFlags = 0;

	pub static mut RESULT: return = std::mem::zeroed();
}

static OTuple
oSysCacheToastCreateKey( *key, uint32 chunknum,  *arg)
{
	ckey: &mut OSysCacheToastChunkKey = (OSysCacheToastChunkKey *) key;
	pub static mut O_SYS_CACHE_TOAST_CHUNK_KEY: *mut ckey_copy = std::ptr::null_mut();
	pub static mut RESULT: OTuple = std::mem::zeroed();

	ckey_copy = (OSysCacheToastChunkKey *) palloc(sizeof(OSysCacheToastChunkKey));
	*ckey_copy = *ckey;

	result.data = (Pointer) ckey_copy;
	result.formatFlags = 0;

	pub static mut RESULT: return = std::mem::zeroed();
}

static Pointer
oSysCacheToastGetTupleData(OTuple tuple,  *arg)
{
	desc: &mut BTreeDescr = (BTreeDescr *) arg;
	pub static mut CHUNK_KEY_LEN: std::os::raw::c_int = 0;
	OTuple		tup = {0};
	pub static mut CHUNK: Pointer = tuple.data;

	chunk_key_len = o_btree_len(desc, tup, OKeyLength);

	return chunk + chunk_key_len + sizeof(OSysCacheToastChunkCommon);
}

static uint32
oSysCacheToastGetTupleChunknum(OTuple tuple,  *arg)
{
	pub static mut CHUNK: Pointer = tuple.data;
	pub static mut O_SYS_CACHE_TOAST_CHUNK_KEY: *mut chunk_key = std::ptr::null_mut();

	chunk_key = (OSysCacheToastChunkKey *) chunk;

	return chunk_key->common.chunknum;
}

static uint32
oSysCacheToastGetTupleDataSize(OTuple tuple,  *arg)
{
	pub static mut CHUNK: Pointer = tuple.data;
	pub static mut O_SYS_CACHE_TOAST_CHUNK_COMMON: *mut common = std::ptr::null_mut();
	desc: &mut BTreeDescr = (BTreeDescr *) arg;
	pub static mut CHUNK_KEY_LEN: std::os::raw::c_int = 0;
	OTuple		tup = {0};

	chunk_key_len = o_btree_len(desc, tup, OKeyLength);

	common = (OSysCacheToastChunkCommon *) (chunk + chunk_key_len);

	return common->dataLength;
}

fn
o_cache_type_opclasses(Oid datoid, Oid typoid,
					   Oid btree_opclass, Oid hash_opclass,
					   XLogRecPtr insert_lsn,
					   List **processed)
{
	if (!OidIsValid(btree_opclass))
		btree_opclass = GetDefaultOpClass(typoid, BTREE_AM_OID);
	if (OidIsValid(btree_opclass))
	{
		pub static mut SYS_LSN: XLogRecPtr = std::mem::zeroed();
		pub static mut SYS_DATOID: Oid = std::mem::zeroed();
		pub static mut BTREE_OPF: Oid = std::mem::zeroed();
		pub static mut BTREE_OPINTYPE: Oid = std::mem::zeroed();
		pub static mut SSUP_OID: Oid = std::mem::zeroed();
		pub static mut CMP_OID: Oid = std::mem::zeroed();

		btree_opf = get_opclass_family(btree_opclass);
		btree_opintype = get_opclass_input_type(btree_opclass);

		o_sys_cache_set_datoid_lsn(&sys_lsn, &sys_datoid);
		o_class_cache_add_if_needed(sys_datoid, OperatorClassRelationId, sys_lsn,
									NULL);
		ssupOid = get_opfamily_proc(btree_opf, btree_opintype, btree_opintype,
									BTSORTSUPPORT_PROC);
		if (OidIsValid(ssupOid))
			o_proc_cache_validate_add(datoid, ssupOid, InvalidOid, "sort", "field",
									  processed);
		cmpOid = get_opfamily_proc(btree_opf, btree_opintype, btree_opintype,
								   BTORDER_PROC);
		o_proc_cache_validate_add(datoid, cmpOid, InvalidOid, "comparison",
								  "field", processed);
		o_opclass_cache_add_if_needed(datoid, btree_opclass, insert_lsn, NULL);
		o_class_cache_add_if_needed(sys_datoid, AccessMethodProcedureRelationId,
									sys_lsn, NULL);
		o_amproc_cache_add_if_needed(datoid, btree_opf, btree_opintype,
									 btree_opintype, BTORDER_PROC, insert_lsn,
									 NULL);
		o_class_cache_add_if_needed(sys_datoid, AccessMethodOperatorRelationId,
									sys_lsn, NULL);

		if (get_opfamily_member(btree_opf, btree_opintype, btree_opintype,
								BTLessStrategyNumber))
			o_amop_strat_cache_add_if_needed(datoid, btree_opf, btree_opintype,
											 btree_opintype, BTLessStrategyNumber,
											 insert_lsn, NULL);
		if (get_opfamily_member(btree_opf, btree_opintype, btree_opintype,
								BTLessEqualStrategyNumber))
			o_amop_strat_cache_add_if_needed(datoid, btree_opf, btree_opintype,
											 btree_opintype,
											 BTLessEqualStrategyNumber, insert_lsn,
											 NULL);
		if (get_opfamily_member(btree_opf, btree_opintype, btree_opintype,
								BTEqualStrategyNumber))
			o_amop_strat_cache_add_if_needed(datoid, btree_opf, btree_opintype,
											 btree_opintype, BTEqualStrategyNumber,
											 insert_lsn, NULL);
	}
	if (!OidIsValid(hash_opclass))
		hash_opclass = GetDefaultOpClass(typoid, HASH_AM_OID);
	if (OidIsValid(hash_opclass))
	{
		pub static mut SYS_LSN: XLogRecPtr = std::mem::zeroed();
		pub static mut SYS_DATOID: Oid = std::mem::zeroed();
		pub static mut HASH_OPF: Oid = std::mem::zeroed();
		pub static mut HASH_OPINTYPE: Oid = std::mem::zeroed();
		pub static mut HASH_EXTENDED_PROC: Oid = InvalidOid;

		hash_opf = get_opclass_family(hash_opclass);
		hash_opintype = get_opclass_input_type(hash_opclass);

		o_sys_cache_set_datoid_lsn(&sys_lsn, &sys_datoid);
		o_class_cache_add_if_needed(sys_datoid, OperatorClassRelationId, sys_lsn,
									NULL);
		o_opclass_cache_add_if_needed(datoid, hash_opclass, insert_lsn, NULL);
		o_class_cache_add_if_needed(sys_datoid,
									AccessMethodProcedureRelationId, sys_lsn,
									NULL);
		o_amproc_cache_add_if_needed(datoid, hash_opf, hash_opintype,
									 hash_opintype, HASHSTANDARD_PROC,
									 insert_lsn, NULL);
		hash_extended_proc = get_opfamily_proc(hash_opf,
											   hash_opintype,
											   hash_opintype,
											   HASHEXTENDED_PROC);
		if (OidIsValid(hash_extended_proc))
			o_amproc_cache_add_if_needed(datoid, hash_opf, hash_opintype,
										 hash_opintype, HASHEXTENDED_PROC,
										 insert_lsn, NULL);

		o_class_cache_add_if_needed(sys_datoid, AccessMethodOperatorRelationId,
									sys_lsn, NULL);
		o_amop_strat_cache_add_if_needed(datoid, hash_opf, hash_opintype,
										 hash_opintype, HTEqualStrategyNumber,
										 insert_lsn, NULL);
	}
}


o_cache_type(Oid datoid, Oid typoid, Oid opclass, XLogRecPtr insert_lsn)
{
	pub static mut LIST: *mut processed = NIL;

	o_cache_type_safe(datoid, typoid, opclass, insert_lsn, &processed);
	list_free_deep(processed);
}


o_cache_type_safe(Oid datoid, Oid typoid, Oid opclass, XLogRecPtr insert_lsn,
				  List **processed)
{
	pub static mut TYPEFORM: Form_pg_type = std::mem::zeroed();
	pub static mut TUPLE: HeapTuple = std::ptr::null_mut();
	pub static mut LIST: *mut oids = std::ptr::null_mut();
	pub static mut OLDCXT: MemoryContext = std::mem::zeroed();

	if (!OidIsValid(opclass))
		opclass = GetDefaultOpClass(typoid, BTREE_AM_OID);

	oldcxt = MemoryContextSwitchTo(CurTransactionContext);
	// cppcheck-suppress unknownEvaluationOrder
	oids = list_make2_oid(typoid, opclass);
	Assert(processed);
	if (*processed && list_member(*processed, oids))
		return;
	else
	{
		(*processed) = lappend(*processed, oids);
	}
	MemoryContextSwitchTo(oldcxt);

	tuple = SearchSysCache1(TYPEOID, ObjectIdGetDatum(typoid));
	Assert(tuple);
	typeform = (Form_pg_type) GETSTRUCT(tuple);

	o_type_cache_add_if_needed(datoid, typoid, insert_lsn, NULL);
	o_cache_type_opclasses(datoid, typoid, opclass, InvalidOid, insert_lsn,
						   processed);
	switch (typeform->typtype)
	{
		case TYPTYPE_COMPOSITE:
			if (typeform->typtypmod == -1)
			{
				pub static mut I: std::os::raw::c_int = 0;
				pub static mut REL: Relation = std::mem::zeroed();

				o_class_cache_add_if_needed(datoid, typeform->typrelid, insert_lsn,
											NULL);
				rel = relation_open(typeform->typrelid, AccessShareLock);
				for (i = 0; i < rel->rd_att->natts; i++)
				{
					pub static mut TYPCACHE_ATTR: Form_pg_attribute = std::mem::zeroed();

					typcache_attr = TupleDescAttr(rel->rd_att, i);
					if (!typcache_attr->attisdropped)
						o_cache_type_safe(datoid, typcache_attr->atttypid,
										  InvalidOid, insert_lsn,
										  processed);
				}
				relation_close(rel, AccessShareLock);
			}
			break;
		case TYPTYPE_RANGE:
			{
				pub static mut RANGETUP: HeapTuple = std::mem::zeroed();
				pub static mut RANGEFORM: Form_pg_range = std::mem::zeroed();
				pub static mut SYS_LSN: XLogRecPtr = std::mem::zeroed();
				pub static mut SYS_DATOID: Oid = std::mem::zeroed();

				o_sys_cache_set_datoid_lsn(&sys_lsn, &sys_datoid);
				o_class_cache_add_if_needed(sys_datoid, RangeRelationId,
											sys_lsn, NULL);
				o_range_cache_add_if_needed(datoid, typoid, insert_lsn,
											NULL);
				rangetup = SearchSysCache1(RANGETYPE, ObjectIdGetDatum(typoid));
				if (!HeapTupleIsValid(rangetup))
					elog(ERROR, "cache lookup failed for range (%u)", typoid);
				rangeform = (Form_pg_range) GETSTRUCT(rangetup);
				o_cache_type_safe(datoid, rangeform->rngsubtype,
								  rangeform->rngsubopc, insert_lsn, processed);
				if (OidIsValid(rangeform->rngcollation))
					o_collation_cache_add_if_needed(datoid,
													rangeform->rngcollation,
													insert_lsn,
													NULL);
				ReleaseSysCache(rangetup);
			}
			break;
		case TYPTYPE_MULTIRANGE:
			{
				pub static mut SYS_LSN: XLogRecPtr = std::mem::zeroed();
				pub static mut SYS_DATOID: Oid = std::mem::zeroed();
				pub static mut MULTIRANGETUP: HeapTuple = std::mem::zeroed();
				pub static mut MULTIRANGEFORM: Form_pg_range = std::mem::zeroed();

				multirangetup = SearchSysCache1(RANGEMULTIRANGE,
												ObjectIdGetDatum(typoid));
				if (!HeapTupleIsValid(multirangetup))
					elog(ERROR, "cache lookup failed for multirange (%u)", typoid);
				multirangeform = (Form_pg_range) GETSTRUCT(multirangetup);
				o_sys_cache_set_datoid_lsn(&sys_lsn, &sys_datoid);
				o_class_cache_add_if_needed(sys_datoid, RangeRelationId, sys_lsn,
											NULL);
				o_multirange_cache_add_if_needed(datoid, typoid, insert_lsn, NULL);
				o_cache_type_safe(datoid, multirangeform->rngtypid,
								  multirangeform->rngsubopc, insert_lsn,
								  processed);
				ReleaseSysCache(multirangetup);
			}
			break;
		case TYPTYPE_ENUM:
			{
				pub static mut SYS_LSN: XLogRecPtr = std::mem::zeroed();
				pub static mut SYS_DATOID: Oid = std::mem::zeroed();

				o_sys_cache_set_datoid_lsn(&sys_lsn, &sys_datoid);
				o_class_cache_add_if_needed(sys_datoid, EnumRelationId, sys_lsn,
											NULL);
				o_enum_cache_add_all(datoid, typoid, insert_lsn);
			}
			break;
		case TYPTYPE_DOMAIN:
			o_cache_type_safe(datoid, typeform->typbasetype, InvalidOid,
							  insert_lsn, processed);
			break;
		default:
			if (typeform->typcategory == TYPCATEGORY_ARRAY)
			{
				o_cache_type_safe(datoid, typeform->typelem, InvalidOid,
								  insert_lsn, processed);
			}
			break;
	}
	if (tuple != NULL)
		ReleaseSysCache(tuple);
}

//
// Find a hash function for the type given its btree opclass.  Looks up the
// equality operator of the btree opclass, then searches for a hash opclass
// that uses the same equality operator, and returns the hash procedure from
// that hash opclass.  Returns InvalidOid if no matching hash procedure exists.
//
Oid
o_get_hash_proc_by_btree_opclass(Oid btreeOpclass)
{
	pub static mut BTREE_OPFAMILY: Oid = std::mem::zeroed();
	pub static mut INPUT_TYPE: Oid = std::mem::zeroed();
	pub static mut EQUAL_OP: Oid = std::mem::zeroed();
	pub static mut RESULT: RegProcedure = std::mem::zeroed();

	btreeOpfamily = get_opclass_family(btreeOpclass);
	inputType = get_opclass_input_type(btreeOpclass);
	Assert(OidIsValid(btreeOpfamily));
	equalOp = get_opfamily_member(btreeOpfamily, inputType, inputType, BTEqualStrategyNumber);
	if (!OidIsValid(equalOp))
		pub static mut INVALID_OID: return = std::mem::zeroed();

	if (get_op_hash_functions(equalOp, &result, NULL))
		pub static mut RESULT: return = std::mem::zeroed();
	else
		pub static mut INVALID_OID: return = std::mem::zeroed();
}

bool
custom_type_try_add_hash_fn_if_needed(Oid typoid, Oid opclass, List **processed)
{
	pub static mut HASHABLE: bool = true;
	pub static mut TYPEFORM: Form_pg_type = std::mem::zeroed();
	pub static mut TUPLE: HeapTuple = std::ptr::null_mut();
	pub static mut HASH_FN_OID: Oid = std::mem::zeroed();

	tuple = SearchSysCache1(TYPEOID, ObjectIdGetDatum(typoid));
	Assert(tuple);
	typeform = (Form_pg_type) GETSTRUCT(tuple);

	hash_fn_oid = o_get_hash_proc_by_btree_opclass(opclass);
	hashable = OidIsValid(hash_fn_oid);

	if (hashable)
	{
		o_validate_function_by_oid(hash_fn_oid,
								   " should be used as hash function "
								   "for type of column used in orioledb index");
		o_collect_function_by_oid(hash_fn_oid, InvalidOid, processed);
		switch (typeform->typtype)
		{
			case TYPTYPE_COMPOSITE:
				if (typeform->typtypmod == -1)
				{
					pub static mut I: std::os::raw::c_int = 0;
					pub static mut REL: Relation = std::mem::zeroed();

					rel = relation_open(typeform->typrelid, AccessShareLock);
					for (i = 0; i < rel->rd_att->natts; i++)
					{
						pub static mut TYPCACHE_ATTR: Form_pg_attribute = std::mem::zeroed();

						typcache_attr = TupleDescAttr(rel->rd_att, i);
						if (!typcache_attr->attisdropped)
						{
							typoid = typcache_attr->atttypid;
							opclass = GetDefaultOpClass(typoid, BTREE_AM_OID);
							hashable = custom_type_try_add_hash_fn_if_needed(typoid, opclass, processed);
						}

						if (!hashable)
							break;
					}
					relation_close(rel, AccessShareLock);
				}
				break;
			case TYPTYPE_RANGE:
				{
					pub static mut RANGETUP: HeapTuple = std::mem::zeroed();
					pub static mut RANGEFORM: Form_pg_range = std::mem::zeroed();

					rangetup = SearchSysCache1(RANGETYPE, ObjectIdGetDatum(typoid));
					if (!HeapTupleIsValid(rangetup))
						elog(ERROR, "cache lookup failed for range (%u)", typoid);
					rangeform = (Form_pg_range) GETSTRUCT(rangetup);

					typoid = rangeform->rngsubtype;
					opclass = rangeform->rngsubopc;
					hashable = custom_type_try_add_hash_fn_if_needed(typoid, opclass, processed);
					ReleaseSysCache(rangetup);
				}
				break;
			case TYPTYPE_MULTIRANGE:
				{
					pub static mut RANGETUP: HeapTuple = std::mem::zeroed();
					pub static mut RANGEFORM: Form_pg_range = std::mem::zeroed();

					rangetup = SearchSysCache1(RANGEMULTIRANGE,
											   ObjectIdGetDatum(typoid));
					if (!HeapTupleIsValid(rangetup))
						elog(ERROR, "cache lookup failed for range (%u)", typoid);
					rangeform = (Form_pg_range) GETSTRUCT(rangetup);

					typoid = rangeform->rngsubtype;
					opclass = rangeform->rngsubopc;
					hashable = custom_type_try_add_hash_fn_if_needed(typoid, opclass, processed);
					ReleaseSysCache(rangetup);
				}
				break;
			case TYPTYPE_ENUM:
				break;
			case TYPTYPE_DOMAIN:
				{
					typoid = typeform->typbasetype;
					opclass = GetDefaultOpClass(typoid, BTREE_AM_OID);
					hashable = custom_type_try_add_hash_fn_if_needed(typoid, opclass, processed);
				}
				break;
			default:
				if (typeform->typcategory == TYPCATEGORY_ARRAY)
				{
					typoid = typeform->typelem;
					opclass = GetDefaultOpClass(typoid, BTREE_AM_OID);
					hashable = custom_type_try_add_hash_fn_if_needed(typoid, opclass, processed);
				}
				break;
		}
	}
	if (tuple != NULL)
		ReleaseSysCache(tuple);
	pub static mut HASHABLE: return = std::mem::zeroed();
}


o_validate_composite_type(Oid typoid, Oid opclass)
{
	pub static mut TYPEFORM: Form_pg_type = std::mem::zeroed();
	pub static mut TUPLE: HeapTuple = std::ptr::null_mut();

	tuple = SearchSysCache1(TYPEOID, ObjectIdGetDatum(typoid));
	Assert(tuple);
	typeform = (Form_pg_type) GETSTRUCT(tuple);

	if (OidIsValid(opclass))
	{
		switch (typeform->typtype)
		{
			case TYPTYPE_COMPOSITE:
				if (typeform->typtypmod == -1)
				{
					pub static mut I: std::os::raw::c_int = 0;
					pub static mut REL: Relation = std::mem::zeroed();

					rel = relation_open(typeform->typrelid, AccessShareLock);
					for (i = 0; i < rel->rd_att->natts; i++)
					{
						pub static mut TYPCACHE_ATTR: Form_pg_attribute = std::mem::zeroed();

						typcache_attr = TupleDescAttr(rel->rd_att, i);
						if (!typcache_attr->attisdropped)
						{
							typoid = typcache_attr->atttypid;
							opclass = GetDefaultOpClass(typoid, BTREE_AM_OID);
							o_validate_composite_type(typoid, opclass);
						}
					}
					relation_close(rel, AccessShareLock);
				}
				break;
			case TYPTYPE_RANGE:
				{
					pub static mut RANGETUP: HeapTuple = std::mem::zeroed();
					pub static mut RANGEFORM: Form_pg_range = std::mem::zeroed();

					rangetup = SearchSysCache1(RANGETYPE, ObjectIdGetDatum(typoid));
					if (!HeapTupleIsValid(rangetup))
						elog(ERROR, "cache lookup failed for range (%u)", typoid);
					rangeform = (Form_pg_range) GETSTRUCT(rangetup);

					typoid = rangeform->rngsubtype;
					opclass = rangeform->rngsubopc;
					o_validate_composite_type(typoid, opclass);
					ReleaseSysCache(rangetup);
				}
				break;
			case TYPTYPE_MULTIRANGE:
				{
					pub static mut RANGETUP: HeapTuple = std::mem::zeroed();
					pub static mut RANGEFORM: Form_pg_range = std::mem::zeroed();

					rangetup = SearchSysCache1(RANGEMULTIRANGE,
											   ObjectIdGetDatum(typoid));
					if (!HeapTupleIsValid(rangetup))
						elog(ERROR, "cache lookup failed for range (%u)", typoid);
					rangeform = (Form_pg_range) GETSTRUCT(rangetup);

					typoid = rangeform->rngsubtype;
					opclass = rangeform->rngsubopc;
					o_validate_composite_type(typoid, opclass);
					ReleaseSysCache(rangetup);
				}
				break;
			case TYPTYPE_ENUM:
				break;
			case TYPTYPE_DOMAIN:
				{
					typoid = typeform->typbasetype;
					opclass = GetDefaultOpClass(typoid, BTREE_AM_OID);
					o_validate_composite_type(typoid, opclass);
				}
				break;
			default:
				if (typeform->typcategory == TYPCATEGORY_ARRAY)
				{
					typoid = typeform->typelem;
					opclass = GetDefaultOpClass(typoid, BTREE_AM_OID);
					o_validate_composite_type(typoid, opclass);
				}
				break;
		}
	}
	if (tuple != NULL)
		ReleaseSysCache(tuple);

	if (!OidIsValid(opclass))
		ereport(ERROR,
				(errcode(ERRCODE_UNDEFINED_FUNCTION),
				 errmsg("could not identify a comparison function for type \"%s\" used in btree index field",
						format_type_be(typoid)),
				 errdetail("not allowing it here, because any inserts in such index will break recovery for orioledb tables")));
}

//
// Inserts type elements for all fields of the o_table to the orioledb sys
// cache.
//

o_cache_index_types(o_table: &mut OTable, o_table_index: &mut OTableIndex)
{
	pub static mut CUR_FIELD: std::os::raw::c_int = 0;
	pub static mut CUR_LSN: XLogRecPtr = std::mem::zeroed();
	pub static mut EXPR_FIELD: std::os::raw::c_int = 0;

	o_sys_cache_set_datoid_lsn(&cur_lsn, NULL);
	for (cur_field = 0; cur_field < o_table_index->nfields; cur_field++)
	{
		pub static mut ATTNUM: std::os::raw::c_int = o_table_index->fields[cur_field].attnum;
		pub static mut OPCLASS: Oid = o_table_index->fields[cur_field].opclass;
		pub static mut TYPID: Oid = std::mem::zeroed();
		pub static mut LIST: *mut processed = NIL;

		if (attnum != EXPR_ATTNUM)
			typid = o_table->fields[attnum].typid;
		else
			typid = o_table_index->exprfields[expr_field++].typid;
		o_cache_type_safe(o_table->oids.datoid, typid, opclass, cur_lsn,
						  &processed);
		list_free_deep(processed);
	}

}

//
// Inserts opclasses for all fields of the o_table to the opclass B-tree.
//

o_cache_table_types(o_table: &mut OTable)
{
	pub static mut CUR_LSN: XLogRecPtr = std::mem::zeroed();
	pub static mut DATOID: Oid = std::mem::zeroed();
	pub static mut SYS_LSN: XLogRecPtr = std::mem::zeroed();
	pub static mut SYS_DATOID: Oid = std::mem::zeroed();
	pub static mut LIST: *mut processed = NIL;

	o_sys_cache_set_datoid_lsn(&sys_lsn, &sys_datoid);
	o_class_cache_add_if_needed(sys_datoid, OperatorClassRelationId, sys_lsn,
								NULL);

	o_sys_cache_set_datoid_lsn(&cur_lsn, NULL);
	datoid = o_table->oids.datoid;

	o_database_cache_add_if_needed(Template1DbOid, Template1DbOid, cur_lsn, NULL);

	//
// Inserts opclasses for TOAST index.
//
	o_cache_type(datoid, INT2OID, InvalidOid, cur_lsn);
	o_collect_function_by_oid(F_HASHINT2, InvalidOid, &processed);

	o_cache_type(datoid, INT4OID, InvalidOid, cur_lsn);
	o_collect_function_by_oid(F_HASHINT4, InvalidOid, &processed);

	//
// Inserts opclass for default index.
//
	Assert(o_table->nindices == 0);
	o_cache_type(datoid, TIDOID, InvalidOid, cur_lsn);
	o_collect_function_by_oid(F_HASHTID, InvalidOid, &processed);
	list_free_deep(processed);
}

static CatCTup *
heap_to_catctup(cache: &mut CatCache, TupleDesc cc_tupdesc, HeapTuple tuple,
				bool refcount)
{
	pub static mut CAT_C_TUP: *mut ct = std::ptr::null_mut();
	pub static mut DTP: HeapTuple = std::mem::zeroed();
	pub static mut OLDCXT: MemoryContext = std::mem::zeroed();
	pub static mut I: std::os::raw::c_int = 0;

	//
// If there are any out-of-line toasted fields in the tuple, expand them
// in-line. This saves cycles during later use of the catcache entry, and
// also protects us against the possibility of the toast tuples being
// freed before we attempt to fetch them, in case of something using a
// slightly stale catcache entry.
//
	if (HeapTupleHasExternal(tuple))
		dtp = toast_flatten_tuple(tuple, cc_tupdesc);
	else
		dtp = tuple;

	// Allocate memory for CatCTup and the cached tuple in one go
	oldcxt = MemoryContextSwitchTo(CacheMemoryContext);

	ct = (CatCTup *) palloc0(sizeof(CatCTup) + MAXIMUM_ALIGNOF + dtp->t_len);
	ct->tuple.t_len = dtp->t_len;
	ct->tuple.t_self = dtp->t_self;
	ct->tuple.t_tableOid = dtp->t_tableOid;
	ct->tuple.t_data = (HeapTupleHeader) MAXALIGN(((char *) ct) +
												  sizeof(CatCTup));
	// copy tuple contents
	memcpy((char *) ct->tuple.t_data, (const char *) dtp->t_data,
		   dtp->t_len);
	MemoryContextSwitchTo(oldcxt);

	if (dtp != tuple)
		heap_freetuple(dtp);

	// extract keys - they'll point into the tuple if not by-value
	for (i = 0; i < cache->cc_nkeys; i++)
	{
		pub static mut ATP: Datum = std::mem::zeroed();
		pub static mut ISNULL: bool = false;

		atp = heap_getattr(&ct->tuple, cache->cc_keyno[i], cc_tupdesc,
						   &isnull);
		Assert(!isnull);
		ct->keys[i] = atp;
	}

	//
// Finish initializing the CatCTup header, and add it to the cache's
// linked list and counts.
//
	ct->ct_magic = CT_MAGIC;
	ct->my_cache = cache;

	// immediately set the refcount to 1
	if (refcount)
	{
		ResourceOwnerEnlargeCatCacheRefs(CurrentResourceOwner);
		ct->refcount++;
		ResourceOwnerRememberCatCacheRef(CurrentResourceOwner, &ct->tuple);
	}
	pub static mut CT: return = std::mem::zeroed();
}

static CatCTup *
o_SearchCatCacheInternal_hook(cache: &mut CatCache, int nkeys, Datum v1, Datum v2,
							  Datum v3, Datum v4)
{
	pub static mut CAT_C_TUP: *mut result = std::ptr::null_mut();
	pub static mut TUPDESC: TupleDesc = std::ptr::null_mut();
	pub static mut HOOK_TUPLE: HeapTuple = std::ptr::null_mut();

	switch (cache->cc_indexoid)
	{
		case AggregateFnoidIndexId:
		case AccessMethodOperatorIndexId:
		case AccessMethodStrategyIndexId:
		case AccessMethodProcedureIndexId:
		case AuthIdOidIndexId:
		case CollationOidIndexId:
		case EnumOidIndexId:
		case EnumTypIdLabelIndexId:
		case OpclassOidIndexId:
		case OperatorOidIndexId:
		case ProcedureOidIndexId:
		case RangeTypidIndexId:
		case RangeMultirangeTypidIndexId:
		case TypeOidIndexId:
			if (cache->cc_tupdesc)
				tupdesc = cache->cc_tupdesc;
			else
				tupdesc = o_class_cache_search_tupdesc(cache->cc_reloid);
			break;
		default:
			break;
	}

	switch (cache->cc_indexoid)
	{
		case AggregateFnoidIndexId:
			{
				pub static mut AGGFNOID: Oid = std::mem::zeroed();

				aggfnoid = DatumGetObjectId(v1);

				Assert(tupdesc);

				hook_tuple = o_aggregate_cache_search_htup(tupdesc, aggfnoid);
			}
			break;
		case AccessMethodOperatorIndexId:
			{
				pub static mut AMOPOPR: Oid = std::mem::zeroed();
				pub static mut AMOPPURPOSE: char = std::mem::zeroed();
				pub static mut AMOPFAMILY: Oid = std::mem::zeroed();

				amopopr = DatumGetObjectId(v1);
				amoppurpose = DatumGetChar(v2);
				amopfamily = DatumGetObjectId(v3);

				Assert(tupdesc);

				hook_tuple = o_amop_cache_search_htup(tupdesc, amopopr,
													  amoppurpose, amopfamily);
			}
			break;
		case AccessMethodStrategyIndexId:
			{
				pub static mut AMOPFAMILY: Oid = std::mem::zeroed();
				pub static mut AMOPLEFTTYPE: Oid = std::mem::zeroed();
				pub static mut AMOPRIGHTTYPE: Oid = std::mem::zeroed();
				pub static mut AMOPSTRATEGY: int16 = std::mem::zeroed();

				amopfamily = DatumGetObjectId(v1);
				amoplefttype = DatumGetObjectId(v2);
				amoprighttype = DatumGetObjectId(v3);
				amopstrategy = DatumGetChar(v4);

				Assert(tupdesc);

				hook_tuple =
					o_amop_strat_cache_search_htup(tupdesc, amopfamily,
												   amoplefttype, amoprighttype,
												   amopstrategy);
			}
			break;
		case AccessMethodProcedureIndexId:
			{
				pub static mut AMPROCFAMILY: Oid = std::mem::zeroed();
				pub static mut AMPROCLEFTTYPE: Oid = std::mem::zeroed();
				pub static mut AMPROCRIGHTTYPE: Oid = std::mem::zeroed();
				pub static mut AMPROCNUM: int16 = std::mem::zeroed();

				amprocfamily = DatumGetObjectId(v1);
				amproclefttype = DatumGetObjectId(v2);
				amprocrighttype = DatumGetObjectId(v3);
				amprocnum = DatumGetChar(v4);

				Assert(tupdesc);

				hook_tuple = o_amproc_cache_search_htup(tupdesc, amprocfamily,
														amproclefttype,
														amprocrighttype,
														amprocnum);
			}
			break;
		case AuthIdOidIndexId:
			{
				pub static mut AUTHOID: Oid = std::mem::zeroed();

				authoid = DatumGetObjectId(v1);

				Assert(tupdesc);

				hook_tuple = o_auth_cache_search_htup(tupdesc, authoid);
			}
			break;
		case CollationOidIndexId:
			{
				pub static mut COLLOID: Oid = std::mem::zeroed();

				colloid = DatumGetObjectId(v1);

				Assert(tupdesc);

				hook_tuple = o_collation_cache_search_htup(tupdesc, colloid);
			}
			break;
		case EnumOidIndexId:
			{
				pub static mut ENUM_OID: Oid = std::mem::zeroed();

				enum_oid = DatumGetObjectId(v1);

				Assert(tupdesc);

				hook_tuple = o_enumoid_cache_search_htup(tupdesc, enum_oid);
			}
			break;
		case EnumTypIdLabelIndexId:
			{
				pub static mut ENUMTYPID: Oid = std::mem::zeroed();
				pub static mut ENUMLABEL: Name = std::mem::zeroed();

				enumtypid = DatumGetObjectId(v1);
				enumlabel = DatumGetName(v1);

				Assert(tupdesc);

				hook_tuple =
					o_enum_cache_search_htup(tupdesc, enumtypid, enumlabel);
			}
			break;
		case OpclassOidIndexId:
			{
				pub static mut OPCLASSOID: Oid = std::mem::zeroed();

				opclassoid = DatumGetObjectId(v1);

				Assert(tupdesc);

				hook_tuple = o_opclass_cache_search_htup(tupdesc, opclassoid);
			}
			break;
		case OperatorOidIndexId:
			{
				pub static mut OPEROID: Oid = std::mem::zeroed();

				operoid = DatumGetObjectId(v1);

				Assert(tupdesc);

				hook_tuple = o_operator_cache_search_htup(tupdesc, operoid);
			}
			break;
		case ProcedureOidIndexId:
			{
				pub static mut PROCOID: Oid = std::mem::zeroed();

				procoid = DatumGetObjectId(v1);

				Assert(tupdesc);

				hook_tuple = o_proc_cache_search_htup(tupdesc, procoid);
			}
			break;
		case RangeTypidIndexId:
			{
				pub static mut RNGTYPID: Oid = std::mem::zeroed();

				rngtypid = DatumGetObjectId(v1);

				Assert(tupdesc);

				hook_tuple = o_range_cache_search_htup(tupdesc, rngtypid);
			}
			break;
		case RangeMultirangeTypidIndexId:
			{
				pub static mut RNGMULTITYPID: Oid = std::mem::zeroed();

				rngmultitypid = DatumGetObjectId(v1);

				Assert(tupdesc);

				hook_tuple = o_multirange_cache_search_htup(tupdesc,
															rngmultitypid);
			}
			break;
		case TypeOidIndexId:
			{
				pub static mut TYPEOID: Oid = std::mem::zeroed();

				typeoid = DatumGetObjectId(v1);

				Assert(tupdesc);

				hook_tuple = o_type_cache_search_htup(tupdesc, typeoid);
			}
			break;

		default:
			break;
	}

	if (hook_tuple)
		result = heap_to_catctup(cache, tupdesc, hook_tuple, true);

	if (tupdesc && tupdesc != cache->cc_tupdesc)
		FreeTupleDesc(tupdesc);

	pub static mut RESULT: return = std::mem::zeroed();
}

static CatCList *
o_SearchCatCacheList_hook(cache: &mut CatCache, int nkeys, Datum v1, Datum v2,
						  Datum v3)
{
	pub static mut CAT_C_LIST: *mut cl = std::ptr::null_mut();

	switch (cache->cc_indexoid)
	{
		case AccessMethodOperatorIndexId:
			{
				pub static mut TUPDESC: TupleDesc = std::ptr::null_mut();
				pub static mut LIST: *mut htup_list = std::ptr::null_mut();
				pub static mut NMEMBERS: std::os::raw::c_int = 0;
				pub static mut AMOPOPR: Oid = std::mem::zeroed();
				pub static mut I: std::os::raw::c_int = 0;
				pub static mut LIST_CELL: *mut lc = std::ptr::null_mut();
				pub static mut OLDCXT: MemoryContext = std::mem::zeroed();

				if (cache->cc_tupdesc)
					tupdesc = cache->cc_tupdesc;
				else
					tupdesc = o_class_cache_search_tupdesc(cache->cc_reloid);

				Assert(nkeys == 1);
				amopopr = DatumGetObjectId(v1);

				Assert(tupdesc);

				htup_list = o_amop_cache_search_htup_list(tupdesc, amopopr);
				if (htup_list != NIL)
				{
					nmembers = list_length(htup_list);

					oldcxt = MemoryContextSwitchTo(CacheMemoryContext);
					cl = (CatCList *)
						palloc0(offsetof(CatCList, members) +
								nmembers * sizeof(CatCTup *));
					MemoryContextSwitchTo(oldcxt);

					cl->cl_magic = CL_MAGIC;
					cl->my_cache = cache;
					cl->n_members = nmembers;

					ResourceOwnerEnlargeCatCacheListRefs(CurrentResourceOwner);
					i = 0;
					foreach(lc, htup_list)
					{
						HeapTuple	ht = lfirst(lc);
						pub static mut CAT_C_TUP: *mut ct = std::ptr::null_mut();

						ct = heap_to_catctup(cache, tupdesc, ht, false);
						cl->members[i++] = ct;
						ct->c_list = cl;
					}
					Assert(i == nmembers);

					cl->refcount++;
					ResourceOwnerRememberCatCacheListRef(CurrentResourceOwner, cl);
				}

				if (tupdesc && tupdesc != cache->cc_tupdesc)
					FreeTupleDesc(tupdesc);
			}
			break;
		default:
			break;
	}

	pub static mut CL: return = std::mem::zeroed();
}

static TupleDesc
o_SysCacheGetAttr_hook(SysCache: &mut CatCache)
{
	pub static mut TUPDESC: TupleDesc = std::ptr::null_mut();

	switch (SysCache->cc_indexoid)
	{
		case AggregateFnoidIndexId:
		case AccessMethodOperatorIndexId:
		case AccessMethodProcedureIndexId:
		case AuthIdOidIndexId:
		case CollationOidIndexId:
		case OpclassOidIndexId:
		case OperatorOidIndexId:
		case ProcedureOidIndexId:
		case TypeOidIndexId:
			if (SysCache->cc_tupdesc)
				tupdesc = SysCache->cc_tupdesc;
			else
				tupdesc = o_class_cache_search_tupdesc(SysCache->cc_reloid);
			break;
		default:
			break;
	}

	pub static mut TUPDESC: return = std::mem::zeroed();
}

static uint32
o_GetCatCacheHashValue_hook(cache: &mut CatCache, int nkeys, Datum v1, Datum v2,
							Datum v3, Datum v4)
{
	OSysCacheKey4 key = {.keys = {v1, v2, v3, v4}};
	pub static mut O_CACHE_ID_MAP_ENTRY: *mut entry = std::ptr::null_mut();

	entry = hash_search(sys_caches, &cache->id, HASH_ENTER, NULL);
	Assert(entry);
	return compute_hash_value(entry->sys_cache->cc_hashfunc, nkeys,
							  (OSysCacheKey *) &key);
}

fn
o_load_typcache_tupdesc_hook(typentry: &mut TypeCacheEntry)
{
	typentry->tupDesc = o_class_cache_search_tupdesc(typentry->typrelid);
	typentry->tupDesc->tdrefcount++;
}

static int
o_sys_cache_key_cmp(sys_cache: &mut OSysCache, int nkeys, key1: &mut OSysCacheKey,
					key2: &mut OSysCacheKey)
{
	pub static mut I: std::os::raw::c_int = 0;
	pub static mut CMP: std::os::raw::c_int = 0;

	for (i = 0; i < nkeys; i++)
	{
		pub static mut KEYTYPE: Oid = sys_cache->keytypes[i];

		switch (keytype)
		{
			case NAMEOID:
				{
					pub static mut CHAR: *mut arg1 = std::ptr::null_mut();
					pub static mut CHAR: *mut arg2 = std::ptr::null_mut();

					arg1 = NameStr(*O_KEY_GET_NAME(key1, i));
					arg2 = NameStr(*O_KEY_GET_NAME(key2, i));
					cmp = strncmp(arg1, arg2, NAMEDATALEN);
				}
				break;
			default:
				cmp = key1->keys[i] - key2->keys[i];
				break;
		}
		if (cmp != 0)
			break;
	}
	pub static mut CMP: return = std::mem::zeroed();
}

static inline OSysCache *
get_o_sys_cache(int sys_tree_num)
{
	return (OSysCache *) sys_tree_get_extra(sys_tree_num);
}

int
o_sys_cache_key_length(desc: &mut BTreeDescr, OTuple tuple)
{
	pub static mut DATA: Pointer = tuple.data;
	pub static mut O_SYS_CACHE_KEY_COMMON: *mut common = std::ptr::null_mut();
	pub static mut O_SYS_CACHE: *mut sys_cache = std::ptr::null_mut();
	pub static mut KEY_LEN: std::os::raw::c_int = 0;

	sys_cache = get_o_sys_cache(desc->oids.reloid);
	key_len = offsetof(OSysCacheKey, keys) + sizeof(Datum) * sys_cache->nkeys;

	common = (OSysCacheKeyCommon *) data;

	return key_len + common->dataLength;
}

int
o_sys_cache_tup_length(desc: &mut BTreeDescr, OTuple tuple)
{
	pub static mut O_SYS_CACHE: *mut sys_cache = std::ptr::null_mut();
	pub static mut KEY_LEN: std::os::raw::c_int = 0;
	pub static mut DATA_LEN: std::os::raw::c_int = 0;

	key_len = o_sys_cache_key_length(desc, tuple);
	sys_cache = get_o_sys_cache(desc->oids.reloid);
	data_len = sys_cache->data_len;

	return key_len + data_len;
}

//
// Comparison function for non-TOAST sys cache B-tree.
//
// If none of the arguments is BTreeKeyBound it compares by both
// oid and lsn. It make possible to insert values with same oid.
// Else it compares only by oid, which is used by other operations than
// insert, to find all rows with exact oid.
// If key kind is not BTreeKeyBound it expects that OTuple passed.
//
int
o_sys_cache_cmp(desc: &mut BTreeDescr,  *p1, BTreeKeyType k1,  *p2,
				BTreeKeyType k2)
{
	pub static mut O_SYS_CACHE_KEY: *mut key1 = std::ptr::null_mut();
	pub static mut O_SYS_CACHE_KEY: *mut key2 = std::ptr::null_mut();
	pub static mut LSN_CMP: bool = true;
	pub static mut NKEYS: std::os::raw::c_int = 0;
	pub static mut CMP: std::os::raw::c_int = 0;
	pub static mut O_SYS_CACHE: *mut sys_cache = std::ptr::null_mut();

	sys_cache = get_o_sys_cache(desc->oids.reloid);
	nkeys = sys_cache->nkeys;

	if (k1 == BTreeKeyBound)
	{
		bound: &mut OSysCacheBound = (OSysCacheBound *) p1;

		key1 = bound->key;
		nkeys = bound->nkeys;
		lsn_cmp = false;
	}
	else
		key1 = (OSysCacheKey *) (((OTuple *) p1)->data);

	if (k2 == BTreeKeyBound)
	{
		bound: &mut OSysCacheBound = (OSysCacheBound *) p2;

		key2 = bound->key;
		nkeys = bound->nkeys;
		lsn_cmp = false;
	}
	else
		key2 = (OSysCacheKey *) (((OTuple *) p2)->data);

	if (key1->common.datoid != key2->common.datoid)
		return key1->common.datoid < key2->common.datoid ? -1 : 1;

	cmp = o_sys_cache_key_cmp(sys_cache, nkeys, key1, key2);
	if (cmp != 0)
		pub static mut CMP: return = std::mem::zeroed();

	if (lsn_cmp)
		if (key1->common.lsn != key2->common.lsn)
			return key1->common.lsn < key2->common.lsn ? -1 : 1;

	pub static mut 0: return = std::mem::zeroed();
}

fn
o_sys_cache_keys_to_str(StringInfo buf, sys_cache: &mut OSysCache,
						key: &mut OSysCacheKey)
{
	pub static mut I: std::os::raw::c_int = 0;

	appendStringInfo(buf, "(");
	for (i = 0; i < sys_cache->nkeys; i++)
	{
		if (i != 0)
			appendStringInfo(buf, ", ");
		switch (sys_cache->keytypes[i])
		{
			case NAMEOID:
				{
					name: &mut char = NameStr(*O_KEY_GET_NAME(key, i));

					appendStringInfo(buf, "\"%s\"", name);
				}
				break;

			default:
				appendStringInfo(buf, "%lu", key->keys[i]);
				break;
		}
	}
	appendStringInfo(buf, ")");
}

//
// Generic non-TOAST sys cache key print function for o_print_btree_pages()
//

o_sys_cache_key_print(desc: &mut BTreeDescr, StringInfo buf, OTuple key_tup,
					  Pointer arg)
{
	key: &mut OSysCacheKey = (OSysCacheKey *) key_tup.data;
	uint32		id,
				off;

	// Decode ID and offset
	id = (uint32) (key->common.lsn >> 32);
	off = (uint32) key->common.lsn;

	appendStringInfo(buf, "(%u, ", key->common.datoid);
	o_sys_cache_keys_to_str(buf, get_o_sys_cache(desc->oids.reloid), key);
	appendStringInfo(buf, ", %X/%X, %c)", id, off,
					 key->common.deleted ? 'Y' : 'N');
}

fn
o_sys_cache_keys_push_to_jsonb_state(sys_cache: &mut OSysCache,
									 key: &mut OSysCacheKey,
									 JsonbParseState **state)
{
	pub static mut I: std::os::raw::c_int = 0;

	jsonb_push_key(state, "keys");
	() pushJsonbValue(state, WJB_BEGIN_ARRAY, NULL);
	for (i = 0; i < sys_cache->nkeys; i++)
	{
		switch (sys_cache->keytypes[i])
		{
			case NAMEOID:
				{
					pub static mut JVAL: JsonbValue = std::mem::zeroed();
					pub static mut CHAR: *mut name = std::ptr::null_mut();

					name = NameStr(*O_KEY_GET_NAME(key, i));

					jval.type = jbvString;
					jval.val.string.len = strlen(name);
					jval.val.string.val = name;
					() pushJsonbValue(state, WJB_ELEM, &jval);
				}
				break;

			default:
				{
					pub static mut RES: Datum = std::mem::zeroed();
					pub static mut JVAL: JsonbValue = std::mem::zeroed();

					res = DirectFunctionCall1(int8_numeric,
											  Int64GetDatum(key->keys[i]));

					jval.type = jbvNumeric;
					jval.val.numeric = DatumGetNumeric(res);
					() pushJsonbValue(state, WJB_ELEM, &jval);
				}
				break;
		}
	}
	() pushJsonbValue(state, WJB_END_ARRAY, NULL);
}

fn
o_sys_cache_key_push_to_jsonb_state(desc: &mut BTreeDescr, key: &mut OSysCacheKey,
									JsonbParseState **state)
{
	pub static mut STR: StringInfo = std::mem::zeroed();

	jsonb_push_int8_key(state, "datoid", key->common.datoid);
	jsonb_push_int8_key(state, "lsn", key->common.lsn);
	jsonb_push_bool_key(state, "deleted", key->common.deleted);

	str = makeStringInfo();
	o_sys_cache_keys_push_to_jsonb_state(get_o_sys_cache(desc->oids.reloid),
										 key, state);
	pfree(str->data);
	pfree(str);
}

JsonbValue *
o_sys_cache_key_to_jsonb(desc: &mut BTreeDescr, OTuple tup, JsonbParseState **state)
{
	key: &mut OSysCacheKey = (OSysCacheKey *) tup.data;

	() pushJsonbValue(state, WJB_BEGIN_OBJECT, NULL);
	o_sys_cache_key_push_to_jsonb_state(desc, key, state);
	return pushJsonbValue(state, WJB_END_OBJECT, NULL);
}

int
o_sys_cache_toast_chunk_length(desc: &mut BTreeDescr, OTuple tuple)
{
	pub static mut CHUNK: Pointer = tuple.data;
	pub static mut CHUNK_KEY_LEN: std::os::raw::c_int = 0;
	OTuple		tup = {0};
	pub static mut O_SYS_CACHE_TOAST_CHUNK_COMMON: *mut common = std::ptr::null_mut();

	chunk_key_len = o_btree_len(desc, tup, OKeyLength);

	common = (OSysCacheToastChunkCommon *) (chunk + chunk_key_len);

	return chunk_key_len + sizeof(OSysCacheToastChunkCommon) +
		common->dataLength;
}

//
// Comparison function for TOAST sys cache B-tree.
//
// If key kind BTreeKeyBound it expects OSysCacheToastKeyBound.
// Otherwise it expects that OTuple passed.
// It wraps OSysCacheToastChunkKey to OTuple to pass it to o_sys_cache_cmp.
//
int
o_sys_cache_toast_cmp(desc: &mut BTreeDescr,  *p1, BTreeKeyType k1,
					   *p2, BTreeKeyType k2)
{
	uint32		chunknum1,
				chunknum2;
	pub static mut O_SYS_CACHE_KEY: *mut key1 = std::ptr::null_mut();
	pub static mut O_SYS_CACHE_KEY: *mut key2 = std::ptr::null_mut();
	OSysCacheKey4 _key = {0};
	OSysCacheBound _bound = {.key = (OSysCacheKey *) &_key};
	OTuple		key_tuple1 = {0},
				key_tuple2 = {0};
	Pointer		sys_cache_key_cmp_arg1 = NULL,
				sys_cache_key_cmp_arg2 = NULL;
	pub static mut SYS_CACHE_KEY_CMP_RESULT: std::os::raw::c_int = 0;
	pub static mut NKEYS: std::os::raw::c_int = 0;

	nkeys = nkeys_for_desc(desc);
	_bound.nkeys = nkeys;

	if (k1 == BTreeKeyBound)
	{
		kb1: &mut OSysCacheToastKeyBound = (OSysCacheToastKeyBound *) p1;

		Assert(k2 != BTreeKeyBound);
		key1 = (OSysCacheKey *) &_key;
		key1->common = kb1->key->common;
		chunknum1 = kb1->common.chunknum;
		memcpy(key1->keys, kb1->key->keys, sizeof(Datum) * nkeys);
		if (kb1->lsn_cmp)
			k1 = BTreeKeyNonLeafKey;	// make o_sys_cache_cmp to compare by
// lsn
		else
			sys_cache_key_cmp_arg1 = (Pointer) &_bound;
	}
	else
	{
		chunk_key: &mut OSysCacheToastChunkKey =
			((OSysCacheToastChunkKey *) ((OTuple *) p1)->data);

		key1 = &chunk_key->sys_cache_key;
		chunknum1 = chunk_key->common.chunknum;
	}

	if (!sys_cache_key_cmp_arg1)
	{
		key_tuple1.data = (Pointer) key1;
		sys_cache_key_cmp_arg1 = (Pointer) &key_tuple1;
	}

	if (k2 == BTreeKeyBound)
	{
		kb2: &mut OSysCacheToastKeyBound = (OSysCacheToastKeyBound *) p2;

		Assert(k1 != BTreeKeyBound);
		key2 = (OSysCacheKey *) &_key;
		key2->common = kb2->key->common;
		chunknum2 = kb2->common.chunknum;
		memcpy(key2->keys, kb2->key->keys, sizeof(Datum) * nkeys);
		if (kb2->lsn_cmp)
			k2 = BTreeKeyNonLeafKey;	// make o_sys_cache_cmp to compare by
// lsn
		else
			sys_cache_key_cmp_arg2 = (Pointer) &_bound;
	}
	else
	{
		chunk_key: &mut OSysCacheToastChunkKey =
			((OSysCacheToastChunkKey *) ((OTuple *) p2)->data);

		key2 = &chunk_key->sys_cache_key;
		chunknum2 = chunk_key->common.chunknum;
	}

	if (!sys_cache_key_cmp_arg2)
	{
		key_tuple2.data = (Pointer) key2;
		sys_cache_key_cmp_arg2 = (Pointer) &key_tuple2;
	}

	sys_cache_key_cmp_result = o_sys_cache_cmp(desc,
											   sys_cache_key_cmp_arg1, k1,
											   sys_cache_key_cmp_arg2, k2);

	if (sys_cache_key_cmp_result != 0)
		pub static mut SYS_CACHE_KEY_CMP_RESULT: return = std::mem::zeroed();

	if (chunknum1 != chunknum2)
		return chunknum1 < chunknum2 ? -1 : 1;

	pub static mut 0: return = std::mem::zeroed();
}

//
// Generic TOAST sys cache key print function for o_print_btree_pages()
//

o_sys_cache_toast_key_print(desc: &mut BTreeDescr, StringInfo buf,
							OTuple tup, Pointer arg)
{
	OTuple		key_tup = {0};
	key: &mut OSysCacheToastChunkKey = (OSysCacheToastChunkKey *) tup.data;

	appendStringInfo(buf, "(");
	key_tup.data = (Pointer) &key->sys_cache_key;
	o_sys_cache_key_print(desc, buf, key_tup, arg);
	appendStringInfo(buf, ", %u)",
					 key->common.chunknum);
}

JsonbValue *
o_sys_cache_toast_key_to_jsonb(desc: &mut BTreeDescr, OTuple tup,
							   JsonbParseState **state)
{
	key: &mut OSysCacheToastChunkKey = (OSysCacheToastChunkKey *) tup.data;

	() pushJsonbValue(state, WJB_BEGIN_OBJECT, NULL);
	o_sys_cache_key_push_to_jsonb_state(desc, &key->sys_cache_key, state);
	jsonb_push_int8_key(state, "chunknum", key->common.chunknum);
	return pushJsonbValue(state, WJB_END_OBJECT, NULL);
}

//
// A tuple print function for o_print_btree_pages()
//

o_sys_cache_toast_tup_print(desc: &mut BTreeDescr, StringInfo buf,
							OTuple tup, Pointer arg)
{
	OTuple		key_tup = {0};
	pub static mut CHUNK: Pointer = tup.data;
	pub static mut O_SYS_CACHE_TOAST_CHUNK_COMMON: *mut common = std::ptr::null_mut();
	pub static mut CHUNK_KEY_LEN: std::os::raw::c_int = 0;

	chunk_key_len = o_btree_len(desc, key_tup, OKeyLength);

	common = (OSysCacheToastChunkCommon *) (chunk + chunk_key_len);

	appendStringInfo(buf, "(");
	key_tup.data = chunk;
	o_sys_cache_toast_key_print(desc, buf, key_tup, arg);
	appendStringInfo(buf, ", %u)", common->dataLength);
}

static HeapTuple
o_auth_cache_search_htup(TupleDesc tupdesc, Oid authoid)
{
	pub static mut RESULT: HeapTuple = std::ptr::null_mut();
	Datum		values[Natts_pg_authid] = {0};
	bool		nulls[Natts_pg_authid] = {0};
	pub static mut ONAME: NameData = std::mem::zeroed();

	Assert(authoid == BOOTSTRAP_SUPERUSERID);

	values[Anum_pg_authid_oid - 1] = ObjectIdGetDatum(BOOTSTRAP_SUPERUSERID);
	namestrcpy(&oname, "");
	values[Anum_pg_authid_rolname - 1] = NameGetDatum(&oname);
	values[Anum_pg_authid_rolsuper - 1] = BoolGetDatum(true);

	nulls[Anum_pg_authid_rolpassword - 1] = true;
	nulls[Anum_pg_authid_rolvaliduntil - 1] = true;

	result = heap_form_tuple(tupdesc, values, nulls);
	pub static mut RESULT: return = std::mem::zeroed();
}

bool
o_is_syscache_hooks_set()
{
	pub static mut SEARCH_CAT_CACHE_INTERNAL_HOOK: return = = o_SearchCatCacheInternal_hook;
}


o_set_syscache_hooks()
{
	o_sys_cache_hooks_depth++;
	if (!IsTransactionState() && SearchCatCacheInternal_hook == NULL)
	{
		if (!CurrentResourceOwner)
		{
			if (!my_owner)
				my_owner = ResourceOwnerCreate(NULL, "orioledb o_fmgr_sql");
			CurrentResourceOwner = my_owner;
		}

		GetUserIdAndSecContext(&save_userid, &save_sec_context);
		SetUserIdAndSecContext(BOOTSTRAP_SUPERUSERID,
							   save_sec_context |
							   SECURITY_LOCAL_USERID_CHANGE);
		SearchCatCacheInternal_hook = o_SearchCatCacheInternal_hook;
		SearchCatCacheList_hook = o_SearchCatCacheList_hook;
		SysCacheGetAttr_hook = o_SysCacheGetAttr_hook;
		GetCatCacheHashValue_hook = o_GetCatCacheHashValue_hook;
		GetDefaultOpClass_hook = o_type_cache_default_opclass;
		load_typcache_tupdesc_hook = o_load_typcache_tupdesc_hook;
		load_enum_cache_data_hook = o_load_enum_cache_data_hook;
	}
}


o_unset_syscache_hooks()
{
	o_sys_cache_hooks_depth--;
	if (SearchCatCacheInternal_hook != NULL && o_sys_cache_hooks_depth == 0)
	{
		SearchCatCacheInternal_hook = NULL;
		SearchCatCacheList_hook = NULL;
		SysCacheGetAttr_hook = NULL;
		GetCatCacheHashValue_hook = NULL;
		GetDefaultOpClass_hook = NULL;
		load_typcache_tupdesc_hook = NULL;
		load_enum_cache_data_hook = NULL;
		SetUserIdAndSecContext(save_userid, save_sec_context);
		if (CurrentResourceOwner == my_owner)
		{
			CurrentResourceOwner = NULL;
		}
	}
}


o_reset_syscache_hooks()
{
	o_sys_cache_hooks_depth = 1;
	o_unset_syscache_hooks();
}