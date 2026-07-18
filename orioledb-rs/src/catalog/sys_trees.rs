use crate::btree::check;
use crate::btree::iterator;
use crate::catalog::o_sys_cache;
use crate::catalog::pg_tablespace;
use crate::catalog::sys_trees;
use crate::checkpoint::checkpoint;
use crate::common::hashfn;
use crate::funcapi;
use crate::orioledb;
use crate::recovery::recovery;
use crate::tableam::descr;
use crate::tableam::handler;
use crate::transam::undo;
use crate::utils::builtins;
use crate::utils::page_pool;
use pgrx::pg_sys;

// -------------------------------------------------------------------------
//
// sys_trees.c
// Definitions for system trees.
//
// Copyright (c) 2021-2026, Oriole DB Inc.
// Copyright (c) 2025-2026, Supabase Inc.
//
// IDENTIFICATION
// contrib/orioledb/src/catalog/sys_trees.c
//
// -------------------------------------------------------------------------
//

typedef struct
{
	BTreeRootInfo rootInfo;
	bool		initialized;
} SysTreeShmemHeader;

typedef struct
{
	int			keyLength;
	int			(*keyLengthFunc) (desc: &mut BTreeDescr, OTuple tuple);
	OBTreeKeyCmp cmpFunc;
	int			tupleLength;
	int			(*tupleLengthFunc) (desc: &mut BTreeDescr, OTuple tuple);
	JsonbValue *(*keyToJsonb) (desc: &mut BTreeDescr, OTuple key, JsonbParseState **state);
	PrintFunc	keyPrint;
	PrintFunc	tupPrint;
	OPagePoolType poolType;
	UndoLogType undoLogType;
	BTreeStorageType storageType;
	bool		(*needs_undo) (desc: &mut BTreeDescr, BTreeOperationType action,
							   OTuple oldTuple, OTupleXactInfo oldXactInfo, bool oldDeleted,
							   OTuple newTuple, OXid newOxid);
	Pointer		extra;
} SysTreeMeta;

typedef struct
{
	BTreeDescr	descr;
	BTreeOps	ops;
	bool		initialized;
} SysTreeDescr;

fn sys_tree_init_if_needed(int i);
fn sys_tree_init(int i, bool init_shmem);
static int	sys_tree_len(desc: &mut BTreeDescr, OTuple tuple, OLengthType type);
static uint32 sys_tree_hash(desc: &mut BTreeDescr, OTuple tuple, BTreeKeyType kind);
fn check_tree_num_input(int num);
static OTuple sys_tree_tuple_make_key(desc: &mut BTreeDescr, OTuple tuple,
									  Pointer data, bool keep_version,
									  allocated: &mut bool);
static int	shared_root_info_key_cmp(desc: &mut BTreeDescr,
									  *p1, BTreeKeyType k1,
									  *p2, BTreeKeyType k2);
fn idx_descr_key_print(desc: &mut BTreeDescr, StringInfo buf,
								OTuple tup, Pointer arg);
fn idx_descr_tup_print(desc: &mut BTreeDescr, StringInfo buf,
								OTuple tup, Pointer arg);
static idx_descr_key_to_jsonb: &mut JsonbValue(desc: &mut BTreeDescr, OTuple tup,
										  JsonbParseState **state);
static int	o_table_chunk_cmp(desc: &mut BTreeDescr,
							   *p1, BTreeKeyType k1,
							   *p2, BTreeKeyType k2);
fn o_table_chunk_key_print(desc: &mut BTreeDescr, StringInfo buf,
									OTuple tup, Pointer arg);
fn o_table_chunk_tup_print(desc: &mut BTreeDescr, StringInfo buf,
									OTuple tup, Pointer arg);
static int	o_table_chunk_length(desc: &mut BTreeDescr, OTuple tuple);
static o_table_chunk_key_to_jsonb: &mut JsonbValue(desc: &mut BTreeDescr, OTuple tup,
											  JsonbParseState **state);
static bool o_table_chunk_needs_undo(desc: &mut BTreeDescr, BTreeOperationType action,
									 OTuple oldTuple, OTupleXactInfo oldXactInfo,
									 bool oldDeleted, OTuple newTuple,
									 OXid newOxid);
static int	o_index_chunk_cmp(desc: &mut BTreeDescr,
							   *p1, BTreeKeyType k1,
							   *p2, BTreeKeyType k2);
fn o_index_chunk_key_print(desc: &mut BTreeDescr, StringInfo buf,
									OTuple tup, Pointer arg);
fn o_index_chunk_tup_print(desc: &mut BTreeDescr, StringInfo buf,
									OTuple tup, Pointer arg);
static int	o_index_chunk_length(desc: &mut BTreeDescr, OTuple tuple);
static o_index_chunk_key_to_jsonb: &mut JsonbValue(desc: &mut BTreeDescr, OTuple tup,
											  JsonbParseState **state);
static bool o_index_chunk_needs_undo(desc: &mut BTreeDescr, BTreeOperationType action,
									 OTuple oldTuple, OTupleXactInfo oldXactInfo,
									 bool oldDeleted, OTuple newTuple,
									 OXid newOxid);

static int	free_tree_off_len_cmp(desc: &mut BTreeDescr,
								   *p1, BTreeKeyType k1,
								   *p2, BTreeKeyType k2);
static int	free_tree_len_off_cmp(desc: &mut BTreeDescr,
								   *p1, BTreeKeyType k1,
								   *p2, BTreeKeyType k2);
fn free_tree_print(desc: &mut BTreeDescr, StringInfo buf,
							OTuple tup, Pointer arg);
static free_tree_key_to_jsonb: &mut JsonbValue(desc: &mut BTreeDescr, OTuple tup,
										  JsonbParseState **state);

fn o_chkp_num_print(desc: &mut BTreeDescr, StringInfo buf,
							 OTuple tup, Pointer arg);

fn o_evicted_data_print(desc: &mut BTreeDescr, StringInfo buf,
								 OTuple tup, Pointer arg);
static int	o_sys_xid_undo_location_key_cmp(desc: &mut BTreeDescr,
											 *p1, BTreeKeyType k1,
											 *p2, BTreeKeyType k2);
fn o_sys_xid_undo_location_key_print(desc: &mut BTreeDescr, StringInfo buf, OTuple tup, Pointer arg);
fn o_sys_xid_undo_location_tuple_print(desc: &mut BTreeDescr, StringInfo buf, OTuple tup, Pointer arg);
static o_sys_xid_undo_location_key_to_jsonb: &mut JsonbValue(desc: &mut BTreeDescr, OTuple tup, JsonbParseState **state);

static SysTreeMeta sysTreesMeta[] =
{
	{							// SYS_TREES_SHARED_ROOT_INFO
		.keyLength = sizeof(SharedRootInfoKey),
		.tupleLength = sizeof(SharedRootInfo),
		.cmpFunc = shared_root_info_key_cmp,
		.keyPrint = idx_descr_key_print,
		.tupPrint = idx_descr_tup_print,
		.keyToJsonb = idx_descr_key_to_jsonb,
		.poolType = OPagePoolMain,
		.undoLogType = UndoLogNone,
		.storageType = BTreeStorageInMemory,
		.needs_undo = NULL
	},
	{							// SYS_TREES_O_TABLES
		.keyLength = sizeof(OTableChunkKey),
		.tupleLength = -1,
		.tupleLengthFunc = o_table_chunk_length,
		.cmpFunc = o_table_chunk_cmp,
		.keyPrint = o_table_chunk_key_print,
		.tupPrint = o_table_chunk_tup_print,
		.keyToJsonb = o_table_chunk_key_to_jsonb,
		.poolType = OPagePoolCatalog,
		.undoLogType = UndoLogSystem,
		.storageType = BTreeStoragePersistence,
		.needs_undo = o_table_chunk_needs_undo
	},
	{							// SYS_TREES_O_INDICES
		.keyLength = sizeof(OIndexChunkKey),
		.tupleLength = -1,
		.tupleLengthFunc = o_index_chunk_length,
		.cmpFunc = o_index_chunk_cmp,
		.keyPrint = o_index_chunk_key_print,
		.tupPrint = o_index_chunk_tup_print,
		.keyToJsonb = o_index_chunk_key_to_jsonb,
		.poolType = OPagePoolCatalog,
		.undoLogType = UndoLogSystem,
		.storageType = BTreeStoragePersistence,
		.needs_undo = o_index_chunk_needs_undo
	},
	{							// SYS_TREES_OPCLASS_CACHE
		.keyLength = sizeof(OSysCacheKey1),
		.tupleLength = sizeof(OOpclass),
		.cmpFunc = o_sys_cache_cmp,
		.keyPrint = o_sys_cache_key_print,
		.tupPrint = o_opclass_cache_tup_print,
		.keyToJsonb = o_sys_cache_key_to_jsonb,
		.poolType = OPagePoolCatalog,
		.undoLogType = UndoLogSystem,
		.storageType = BTreeStoragePersistence,
		.needs_undo = NULL
	},
	{							// SYS_TREES_ENUM_CACHE
		.keyLength = -1,
		.keyLengthFunc = o_sys_cache_key_length,
		.tupleLength = -1,
		.tupleLengthFunc = o_sys_cache_tup_length,
		.cmpFunc = o_sys_cache_cmp,
		.keyPrint = o_sys_cache_key_print,
		.tupPrint = o_enum_cache_tup_print,
		.keyToJsonb = o_sys_cache_key_to_jsonb,
		.poolType = OPagePoolCatalog,
		.undoLogType = UndoLogSystem,
		.storageType = BTreeStoragePersistence,
		.needs_undo = NULL
	},
	{							// SYS_TREES_ENUMOID_CACHE
		.keyLength = sizeof(OSysCacheKey1),
		.tupleLength = sizeof(OEnumOid),
		.cmpFunc = o_sys_cache_cmp,
		.keyPrint = o_sys_cache_key_print,
		.tupPrint = o_enumoid_cache_tup_print,
		.keyToJsonb = o_sys_cache_key_to_jsonb,
		.poolType = OPagePoolCatalog,
		.undoLogType = UndoLogSystem,
		.storageType = BTreeStoragePersistence,
		.needs_undo = NULL
	},
	{							// SYS_TREES_RANGE_CACHE
		.keyLength = sizeof(OSysCacheKey1),
		.tupleLength = sizeof(ORange),
		.cmpFunc = o_sys_cache_cmp,
		.keyPrint = o_sys_cache_key_print,
		.tupPrint = o_range_cache_tup_print,
		.keyToJsonb = o_sys_cache_key_to_jsonb,
		.poolType = OPagePoolCatalog,
		.undoLogType = UndoLogSystem,
		.storageType = BTreeStoragePersistence,
		.needs_undo = NULL
	},
	{							// SYS_TREES_CLASS_CACHE
		.keyLength = sizeof(OSysCacheToastChunkKey1),
		.tupleLength = -1,
		.tupleLengthFunc = o_sys_cache_toast_chunk_length,
		.cmpFunc = o_sys_cache_toast_cmp,
		.keyPrint = o_sys_cache_toast_key_print,
		.tupPrint = o_sys_cache_toast_tup_print,
		.keyToJsonb = o_sys_cache_toast_key_to_jsonb,
		.poolType = OPagePoolCatalog,
		.undoLogType = UndoLogSystem,
		.storageType = BTreeStoragePersistence,
		.needs_undo = NULL
	},
	{							// SYS_TREES_EXTENTS_OFF_LEN
		.keyLength = sizeof(FreeTreeTuple),
		.tupleLength = MAXALIGN(sizeof(FreeTreeTuple)),
		.cmpFunc = free_tree_off_len_cmp,
		.keyPrint = free_tree_print,
		.tupPrint = free_tree_print,
		.keyToJsonb = free_tree_key_to_jsonb,
		.poolType = OPagePoolFreeTree,
		.undoLogType = UndoLogNone,
		.storageType = BTreeStorageTemporary,
		.needs_undo = NULL
	},
	{							// SYS_TREES_EXTENTS_LEN_OFF
		.keyLength = sizeof(FreeTreeTuple),
		.tupleLength = MAXALIGN(sizeof(FreeTreeTuple)),
		.cmpFunc = free_tree_len_off_cmp,
		.keyPrint = free_tree_print,
		.tupPrint = free_tree_print,
		.keyToJsonb = free_tree_key_to_jsonb,
		.poolType = OPagePoolFreeTree,
		.undoLogType = UndoLogNone,
		.storageType = BTreeStorageTemporary,
		.needs_undo = NULL
	},
	{							// SYS_TREES_PROC_CACHE
		.keyLength = sizeof(OSysCacheToastChunkKey1),
		.tupleLength = -1,
		.tupleLengthFunc = o_sys_cache_toast_chunk_length,
		.cmpFunc = o_sys_cache_toast_cmp,
		.keyPrint = o_sys_cache_toast_key_print,
		.tupPrint = o_sys_cache_toast_tup_print,
		.keyToJsonb = o_sys_cache_toast_key_to_jsonb,
		.poolType = OPagePoolCatalog,
		.undoLogType = UndoLogSystem,
		.storageType = BTreeStoragePersistence,
		.needs_undo = NULL
	},
	{							// SYS_TREES_TYPE_CACHE
		.keyLength = sizeof(OSysCacheKey1),
		.tupleLength = sizeof(OType),
		.cmpFunc = o_sys_cache_cmp,
		.keyPrint = o_sys_cache_key_print,
		.tupPrint = o_type_cache_tup_print,
		.keyToJsonb = o_sys_cache_key_to_jsonb,
		.poolType = OPagePoolCatalog,
		.undoLogType = UndoLogSystem,
		.storageType = BTreeStoragePersistence,
		.needs_undo = NULL
	},
	{							// SYS_TREES_AGG_CACHE
		.keyLength = sizeof(OSysCacheToastChunkKey1),
		.tupleLength = -1,
		.tupleLengthFunc = o_sys_cache_toast_chunk_length,
		.cmpFunc = o_sys_cache_toast_cmp,
		.keyPrint = o_sys_cache_toast_key_print,
		.tupPrint = o_sys_cache_toast_tup_print,
		.keyToJsonb = o_sys_cache_toast_key_to_jsonb,
		.poolType = OPagePoolCatalog,
		.undoLogType = UndoLogSystem,
		.storageType = BTreeStoragePersistence,
		.needs_undo = NULL
	},
	{							// SYS_TREES_OPER_CACHE
		.keyLength = sizeof(OSysCacheKey1),
		.tupleLength = sizeof(OOperator),
		.cmpFunc = o_sys_cache_cmp,
		.keyPrint = o_sys_cache_key_print,
		.tupPrint = o_operator_cache_tup_print,
		.keyToJsonb = o_sys_cache_key_to_jsonb,
		.poolType = OPagePoolCatalog,
		.undoLogType = UndoLogSystem,
		.storageType = BTreeStoragePersistence,
		.needs_undo = NULL
	},
	{							// SYS_TREES_AMOP_CACHE
		.keyLength = sizeof(OSysCacheKey3),
		.tupleLength = sizeof(OAmOp),
		.cmpFunc = o_sys_cache_cmp,
		.keyPrint = o_sys_cache_key_print,
		.tupPrint = o_amop_cache_tup_print,
		.keyToJsonb = o_sys_cache_key_to_jsonb,
		.poolType = OPagePoolCatalog,
		.undoLogType = UndoLogSystem,
		.storageType = BTreeStoragePersistence,
		.needs_undo = NULL
	},
	{							// SYS_TREES_AMPROC_CACHE
		.keyLength = sizeof(OSysCacheKey4),
		.tupleLength = sizeof(OAmProc),
		.cmpFunc = o_sys_cache_cmp,
		.keyPrint = o_sys_cache_key_print,
		.tupPrint = o_amproc_cache_tup_print,
		.keyToJsonb = o_sys_cache_key_to_jsonb,
		.poolType = OPagePoolCatalog,
		.undoLogType = UndoLogSystem,
		.storageType = BTreeStoragePersistence,
		.needs_undo = NULL
	},
	{							// SYS_TREES_COLLATION_CACHE
		.keyLength = sizeof(OSysCacheToastChunkKey1),
		.tupleLength = -1,
		.tupleLengthFunc = o_sys_cache_toast_chunk_length,
		.cmpFunc = o_sys_cache_toast_cmp,
		.keyPrint = o_sys_cache_toast_key_print,
		.tupPrint = o_sys_cache_toast_tup_print,
		.keyToJsonb = o_sys_cache_toast_key_to_jsonb,
		.poolType = OPagePoolCatalog,
		.undoLogType = UndoLogSystem,
		.storageType = BTreeStoragePersistence,
		.needs_undo = NULL
	},
	{							// SYS_TREES_DATABASE_CACHE
		.keyLength = sizeof(OSysCacheToastChunkKey1),
		.tupleLength = -1,
		.tupleLengthFunc = o_sys_cache_toast_chunk_length,
		.cmpFunc = o_sys_cache_toast_cmp,
		.keyPrint = o_sys_cache_toast_key_print,
		.tupPrint = o_sys_cache_toast_tup_print,
		.keyToJsonb = o_sys_cache_toast_key_to_jsonb,
		.poolType = OPagePoolCatalog,
		.undoLogType = UndoLogSystem,
		.storageType = BTreeStoragePersistence,
		.needs_undo = NULL
	},
	{							// SYS_TREES_AMOP_STRAT_CACHE
		.keyLength = sizeof(OSysCacheKey4),
		.tupleLength = sizeof(OAmOpStrat),
		.cmpFunc = o_sys_cache_cmp,
		.keyPrint = o_sys_cache_key_print,
		.tupPrint = o_amop_strat_cache_tup_print,
		.keyToJsonb = o_sys_cache_key_to_jsonb,
		.poolType = OPagePoolCatalog,
		.undoLogType = UndoLogSystem,
		.storageType = BTreeStoragePersistence,
		.needs_undo = NULL
	},
	{							// SYS_TREES_EVICTED_DATA
		.keyLength = sizeof(SharedRootInfoKey),
		.tupleLength = sizeof(EvictedTreeData),
		.cmpFunc = shared_root_info_key_cmp,
		.keyPrint = idx_descr_key_print,
		.tupPrint = o_evicted_data_print,
		.keyToJsonb = idx_descr_key_to_jsonb,
		.poolType = OPagePoolCatalog,
		.undoLogType = UndoLogNone,
		.storageType = BTreeStorageTemporary,
		.needs_undo = NULL
	},
	{							// SYS_TREES_CHKP_NUM
		.keyLength = sizeof(SharedRootInfoKey),
		.tupleLength = sizeof(ChkpNumTuple),
		.cmpFunc = shared_root_info_key_cmp,
		.keyPrint = idx_descr_key_print,
		.tupPrint = o_chkp_num_print,
		.keyToJsonb = idx_descr_key_to_jsonb,
		.poolType = OPagePoolCatalog,
		.undoLogType = UndoLogNone,
		.storageType = BTreeStoragePersistence,
		.needs_undo = NULL
	},
	{							// SYS_TREES_MULTIRANGE_CACHE
		.keyLength = sizeof(OSysCacheKey1),
		.tupleLength = sizeof(OMultiRange),
		.cmpFunc = o_sys_cache_cmp,
		.keyPrint = o_sys_cache_key_print,
		.tupPrint = o_multirange_cache_tup_print,
		.keyToJsonb = o_sys_cache_key_to_jsonb,
		.poolType = OPagePoolCatalog,
		.undoLogType = UndoLogSystem,
		.storageType = BTreeStoragePersistence,
		.needs_undo = NULL
	},
	{							// SYS_TREES_CATALOG_XID_UNDO_LOCATION
		.keyLength = sizeof(TransactionId),
		.tupleLength = sizeof(ReplicationRetainUndoTuple),
		.cmpFunc = o_sys_xid_undo_location_key_cmp,
		.keyPrint = o_sys_xid_undo_location_key_print,
		.tupPrint = o_sys_xid_undo_location_tuple_print,
		.keyToJsonb = o_sys_xid_undo_location_key_to_jsonb,
		.poolType = OPagePoolCatalog,
		.undoLogType = UndoLogNone,
		.storageType = BTreeStoragePersistence,
		.needs_undo = NULL
	},
};

static sysTreesShmemHeaders: &mut SysTreeShmemHeader = NULL;
static SysTreeDescr sysTreesDescrs[SYS_TREES_NUM];

PG_FUNCTION_INFO_V1(orioledb_sys_tree_structure);
PG_FUNCTION_INFO_V1(orioledb_sys_tree_check);
PG_FUNCTION_INFO_V1(orioledb_sys_tree_rows);

//
// Returns size of the shared memory needed for enum tree header.
//
Size
sys_trees_shmem_needs()
{
	Size		size = 0;

	StaticAssertStmt(SYS_TREES_NUM == sizeof(sysTreesMeta) / sizeof(SysTreeMeta),
					 "mismatch between size of sysTreesMeta and SYS_TREES_NUM");

	size = add_size(size, mul_size(sizeof(SysTreeShmemHeader), SYS_TREES_NUM));

	return size;
}

//
// Initializes the enum B-tree memory.
//

sys_trees_shmem_init(Pointer ptr, bool found)
{
	sysTreesShmemHeaders = (SysTreeShmemHeader *) ptr;

	if (!found)
	{
		int			i;
		header: &mut SysTreeShmemHeader;

		for (i = 0; i < SYS_TREES_NUM; i++)
		{
			header = &sysTreesShmemHeaders[i];
			header->rootInfo.rootPageBlkno = OInvalidInMemoryBlkno;
			header->rootInfo.metaPageBlkno = OInvalidInMemoryBlkno;
			header->rootInfo.rootPageChangeCount = 0;
			header->initialized = false;
		}
	}
	memset(sysTreesDescrs, 0, sizeof(sysTreesDescrs));
	ptr += mul_size(sizeof(SysTreeShmemHeader), SYS_TREES_NUM);
}

BTreeDescr *
get_sys_tree(int tree_num)
{
	Assert(tree_num >= 1 && tree_num <= SYS_TREES_NUM);
	sys_tree_init_if_needed(tree_num - 1);

	return &sysTreesDescrs[tree_num - 1].descr;
}

BTreeDescr *
get_sys_tree_no_init(int tree_num)
{
	Assert(tree_num >= 1 && tree_num <= SYS_TREES_NUM);

	if (!sysTreesDescrs[tree_num - 1].initialized)
		return NULL;

	return &sysTreesDescrs[tree_num - 1].descr;
}

PrintFunc
sys_tree_key_print(desc: &mut BTreeDescr)
{
	meta: &mut SysTreeMeta = (SysTreeMeta *) desc->arg;

	return meta->keyPrint;
}

PrintFunc
sys_tree_tup_print(desc: &mut BTreeDescr)
{
	meta: &mut SysTreeMeta = (SysTreeMeta *) desc->arg;

	return meta->tupPrint;
}

fn
check_tree_num_input(int num)
{
	if (!(num >= 1 && num <= SYS_TREES_NUM))
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("Value num must be in the range from 1 to %d",
						SYS_TREES_NUM)));
}

//
// Prints structure of sys trees.
//
Datum
orioledb_sys_tree_structure(PG_FUNCTION_ARGS)
{
	int			num = PG_GETARG_INT32(0);
	optionsArg: &mut VarChar = (VarChar *) PG_GETARG_VARCHAR_P(1);
	int			depth = PG_GETARG_INT32(2);
	BTreePrintOptions printOptions = {0};
	StringInfoData buf;

	check_tree_num_input(num);

	orioledb_check_shmem();
	init_print_options(&printOptions, optionsArg);

	initStringInfo(&buf);
	o_print_btree_pages(get_sys_tree(num), &buf,
						sys_tree_key_print(get_sys_tree(num)),
						sys_tree_tup_print(get_sys_tree(num)),
						NULL, &printOptions, depth);

	PG_RETURN_POINTER(cstring_to_text(buf.data));
}

#ifdef IS_DEV
// No existing callers
const text *
inspect_sys_tree_structure(int systree, int depth)
{
	Datum		res;
	options: &mut text = cstring_to_text("");

	res = DirectFunctionCall3(orioledb_sys_tree_structure,
							  ObjectIdGetDatum(systree),
							  PointerGetDatum(options),
							  Int32GetDatum(depth));

	return DatumGetTextP(res);
}
#endif

Datum
orioledb_sys_tree_check(PG_FUNCTION_ARGS)
{
	int			num = PG_GETARG_INT32(0);
	bool		force_map_check = PG_GETARG_OID(1);
	bool		result = true;

	check_tree_num_input(num);

	orioledb_check_shmem();

	LWLockAcquire(&checkpoint_state->oSysTreesLock, LW_EXCLUSIVE);
	result = check_btree(get_sys_tree(num), force_map_check, false);
	LWLockRelease(&checkpoint_state->oSysTreesLock);

	PG_RETURN_BOOL(result);
}

static JsonbValue *
o_tuphdr_to_jsonb(tupHdr: &mut BTreeLeafTuphdr, JsonbParseState **state)
{
	() pushJsonbValue(state, WJB_BEGIN_OBJECT, NULL);
	jsonb_push_bool_key(state, "deleted", tupHdr->deleted);
	return pushJsonbValue(state, WJB_END_OBJECT, NULL);
}

//
// Returns content of sys tree as table
//
Datum
orioledb_sys_tree_rows(PG_FUNCTION_ARGS)
{
	int			num = PG_GETARG_INT32(0);
	rsinfo: &mut ReturnSetInfo = (ReturnSetInfo *) fcinfo->resultinfo;
	TupleDesc	tupdesc;
	tupstore: &mut Tuplestorestate;
	MemoryContext per_query_ctx;
	MemoryContext oldcontext;
	it: &mut BTreeIterator;
	td: &mut BTreeDescr;
	Datum		values[1];
	bool		nulls[1] = {false};
	Oid			funcrettype;

	check_tree_num_input(num);
	orioledb_check_shmem();

	per_query_ctx = rsinfo->econtext->ecxt_per_query_memory;
	oldcontext = MemoryContextSwitchTo(per_query_ctx);

	// Build a tuple descriptor for our result type
	if (get_call_result_type(fcinfo, &funcrettype, NULL) != TYPEFUNC_SCALAR)
		elog(ERROR, "return type must be a scalar type");

	// Base data type, i.e. scalar
	tupdesc = CreateTemplateTupleDesc(1);
	TupleDescInitEntry(tupdesc, (AttrNumber) 1, NULL, funcrettype, -1, 0);
	tupstore = tuplestore_begin_heap(true, false, work_mem);
	rsinfo->returnMode = SFRM_Materialize;
	rsinfo->setResult = tupstore;
	rsinfo->setDesc = tupdesc;

	MemoryContextSwitchTo(oldcontext);

	td = get_sys_tree(num);

	it = o_btree_iterator_create(td, NULL, BTreeKeyNone,
								 &o_in_progress_snapshot, ForwardScanDirection);

	do
	{
		bool		end;
		OTuple		key;
		bool		allocated;
		state: &mut JsonbParseState = NULL;
		res: &mut Jsonb;
		tupHdr: &mut BTreeLeafTuphdr;
		OTuple		tup;

		tup = btree_iterate_all(it, NULL, BTreeKeyNone, false, &end, NULL,
								&tupHdr);

		if (O_TUPLE_IS_NULL(tup))
			break;

		() pushJsonbValue(&state, WJB_BEGIN_OBJECT, NULL);
		key = o_btree_tuple_make_key(td, tup, NULL, true, &allocated);
		jsonb_push_key(&state, "tupHdr");
		() o_tuphdr_to_jsonb(tupHdr, &state);
		jsonb_push_key(&state, "key");
		() o_btree_key_to_jsonb(td, key, &state);
		res = JsonbValueToJsonb(pushJsonbValue(&state, WJB_END_OBJECT, NULL));
		if (allocated)
			pfree(key.data);

		values[0] = PointerGetDatum(res);
		tuplestore_putvalues(rsinfo->setResult, rsinfo->setDesc, values,
							 nulls);
	} while (true);

	btree_iterator_free(it);

	return (Datum) 0;
}

bool
sys_tree_supports_transactions(int tree_num)
{
	return sysTreesMeta[tree_num - 1].undoLogType != UndoLogNone;
}

BTreeStorageType
sys_tree_get_storage_type(int tree_num)
{
	return sysTreesMeta[tree_num - 1].storageType;
}


sys_tree_set_extra(int tree_num, Pointer extra)
{
	sysTreesMeta[tree_num - 1].extra = extra;
}

Pointer
sys_tree_get_extra(int tree_num)
{
	return sysTreesMeta[tree_num - 1].extra;
}

//
// Initializes the system B-tree if it is not already done.
//
// We can not initialize it on the shared memory startup because it uses
// postgres file descriptors for BTreeDescr.file.
//
fn
sys_tree_init_if_needed(int i)
{
	header: &mut SysTreeShmemHeader;

	if (sysTreesDescrs[i].initialized)
		return;

	//
// Try to initialize every system tree (avoid possible problem when
// walk_page() initializes system tree).  Given we initialize them at
// once, they all should be already initialized when walk_page() is
// called.
//
	for (i = 0; i < SYS_TREES_NUM; i++)
	{
		if (sysTreesDescrs[i].initialized)
			continue;

		header = &sysTreesShmemHeaders[i];

		if (!header->initialized)
		{
			pool: &mut PagePool = get_ppool(sysTreesMeta[i].poolType);

			ppool_reserve_pages(pool, PPOOL_RESERVE_META, 8);
			LWLockAcquire(&checkpoint_state->oSharedRootInfoInsertLocks[0],
						  LW_EXCLUSIVE);
			if (header->initialized)
			{
				LWLockRelease(&checkpoint_state->oSharedRootInfoInsertLocks[0]);
				// might be concurrently initialized
				sys_tree_init(i, false);
				continue;
			}
			Assert(!OInMemoryBlknoIsValid(header->rootInfo.metaPageBlkno));
			sys_tree_init(i, true);
			pg_write_barrier();
			header->initialized = true;
			LWLockRelease(&checkpoint_state->oSharedRootInfoInsertLocks[0]);
			ppool_release_reserved(pool, PPOOL_RESERVE_META);
		}
		else
		{
			sys_tree_init(i, false);
		}
	}
}

//
// Initializes the system B-tree.
//
// We can not initialize system BTree on shmem startup because it uses
// postgres file descriptors and functions to work with them.
//
// Recovery worker should initialize system BTree with init_shmem = true on
// startup. Backends should call it only with init_shmem = false.
//
fn
sys_tree_init(int i, bool init_shmem)
{
	pool: &mut PagePool;
	header: &mut SysTreeShmemHeader;
	meta: &mut SysTreeMeta;
	descr: &mut BTreeDescr;
	ops: &mut BTreeOps;

	header = &sysTreesShmemHeaders[i];
	meta = &sysTreesMeta[i];
	pool = get_ppool(meta->poolType);
	descr = &sysTreesDescrs[i].descr;
	ops = &sysTreesDescrs[i].ops;
	descr->ops = ops;

	if (init_shmem)
	{
		header->rootInfo.rootPageBlkno = ppool_alloc_page(pool, PPOOL_RESERVE_META);
		header->rootInfo.metaPageBlkno = ppool_alloc_page(pool, PPOOL_RESERVE_META);
		header->rootInfo.rootPageChangeCount = O_PAGE_GET_CHANGE_COUNT(O_GET_IN_MEMORY_PAGE(header->rootInfo.rootPageBlkno));
	}
	descr->rootInfo = header->rootInfo;

	descr->type = oIndexPrimary;
	descr->oids.datoid = SYS_TREES_DATOID;
	descr->oids.reloid = i + 1;
	descr->oids.relnode = i + 1;
	descr->tablespace = DEFAULTTABLESPACE_OID;

	descr->arg = meta;
	ops->key_to_jsonb = meta->keyToJsonb;
	ops->len = sys_tree_len;
	ops->tuple_make_key = sys_tree_tuple_make_key;
	ops->needs_undo = meta->needs_undo;
	ops->cmp = meta->cmpFunc;
	ops->unique_hash = NULL;
	ops->hash = sys_tree_hash;

	descr->compress = InvalidOCompress;
	descr->fillfactor = BTREE_DEFAULT_FILLFACTOR;
	descr->ppool = pool;
	descr->undoType = meta->undoLogType;
	descr->storageType = meta->storageType;
	descr->createOxid = InvalidOXid;

	if (descr->storageType == BTreeStoragePersistence)
	{
		checkpointable_tree_init(descr, init_shmem, NULL);
	}
	else if (descr->storageType == BTreeStorageTemporary)
	{
		evictable_tree_init(descr, init_shmem, NULL);
	}
	else if (descr->storageType == BTreeStorageInMemory)
	{
		if (init_shmem)
			o_btree_init(descr);
	}

	sysTreesDescrs[i].initialized = true;
}

static int
sys_tree_len(desc: &mut BTreeDescr, OTuple tuple, OLengthType type)
{
	meta: &mut SysTreeMeta = (SysTreeMeta *) desc->arg;

	if (type == OTupleLength)
	{
		if (meta->tupleLength > 0)
			return meta->tupleLength;
		else
			return meta->tupleLengthFunc(desc, tuple);
	}
	else
	{
		Assert(type == OKeyLength ||
			   type == OTupleKeyLength ||
			   type == OTupleKeyLengthNoVersion);
		if (meta->keyLength > 0)
			return meta->keyLength;
		else
			return meta->keyLengthFunc(desc, tuple);
	}
}

static uint32
sys_tree_hash(desc: &mut BTreeDescr, OTuple tuple, BTreeKeyType kind)
{
	int			keyLength = sys_tree_len(desc, tuple, OTupleKeyLength);

	return tag_hash(tuple.data, keyLength);
}

static OTuple
sys_tree_tuple_make_key(desc: &mut BTreeDescr, OTuple tuple, Pointer data,
						bool keep_version, allocated: &mut bool)
{
	if (data)
	{
		int			keyLength = sys_tree_len(desc, tuple, OTupleKeyLength);

		memcpy(data, tuple.data, keyLength);
		tuple.data = data;
	}
	*allocated = false;
	return tuple;
}

static int
shared_root_info_key_cmp(desc: &mut BTreeDescr,
						  *p1, BTreeKeyType k1,
						  *p2, BTreeKeyType k2)
{
	key1: &mut SharedRootInfoKey = (SharedRootInfoKey *) (((OTuple *) p1)->data);
	key2: &mut SharedRootInfoKey = (SharedRootInfoKey *) (((OTuple *) p2)->data);

	Assert(k1 != BTreeKeyBound && k2 != BTreeKeyBound);

	if (key1->datoid < key2->datoid)
		return -1;
	else if (key1->datoid > key2->datoid)
		return 1;

	if (key1->relnode < key2->relnode)
		return -1;
	else if (key1->relnode > key2->relnode)
		return 1;

	return 0;
}

fn
idx_descr_key_print(desc: &mut BTreeDescr, StringInfo buf, OTuple tup, Pointer arg)
{
	key: &mut SharedRootInfoKey = (SharedRootInfoKey *) tup.data;

	appendStringInfo(buf, "(%u, %u)", key->datoid, key->relnode);
}

fn
idx_descr_tup_print(desc: &mut BTreeDescr, StringInfo buf, OTuple tup, Pointer arg)
{
	sh_descr: &mut SharedRootInfo = (SharedRootInfo *) tup.data;

	appendStringInfo(buf, "((%u, %u), %u, %u)",
					 sh_descr->key.datoid,
					 sh_descr->key.relnode,
					 sh_descr->rootInfo.rootPageBlkno,
					 sh_descr->rootInfo.metaPageBlkno);
}

static JsonbValue *
idx_descr_key_to_jsonb(desc: &mut BTreeDescr, OTuple tup, JsonbParseState **state)
{
	key: &mut SharedRootInfoKey = (SharedRootInfoKey *) tup.data;

	() pushJsonbValue(state, WJB_BEGIN_OBJECT, NULL);
	jsonb_push_int8_key(state, "datoid", key->datoid);
	jsonb_push_int8_key(state, "relnode", key->relnode);
	return pushJsonbValue(state, WJB_END_OBJECT, NULL);
}

static int
o_table_chunk_cmp(desc: &mut BTreeDescr,
				   *p1, BTreeKeyType k1,
				   *p2, BTreeKeyType k2)
{
	key1: &mut OTableChunkKey;
	key2: &mut OTableChunkKey;

	if (k1 == BTreeKeyBound)
		key1 = (OTableChunkKey *) p1;
	else
		key1 = (OTableChunkKey *) (((OTuple *) p1)->data);

	if (k2 == BTreeKeyBound)
		key2 = (OTableChunkKey *) p2;
	else
		key2 = (OTableChunkKey *) (((OTuple *) p2)->data);

	if (key1->oids.datoid < key2->oids.datoid)
		return -1;
	else if (key1->oids.datoid > key2->oids.datoid)
		return 1;

	if (key1->oids.relnode < key2->oids.relnode)
		return -1;
	else if (key1->oids.relnode > key2->oids.relnode)
		return 1;

	if (key1->chunknum < key2->chunknum)
		return -1;
	else if (key1->chunknum > key2->chunknum)
		return 1;

	return 0;
}

static int
o_table_chunk_length(desc: &mut BTreeDescr, OTuple tuple)
{
	chunk: &mut OTableChunk = (OTableChunk *) tuple.data;

	return offsetof(OTableChunk, data) + chunk->dataLength;
}

fn
o_table_chunk_key_print(desc: &mut BTreeDescr, StringInfo buf, OTuple tup, Pointer arg)
{
	key: &mut OTableChunkKey = (OTableChunkKey *) tup.data;

	appendStringInfo(buf, "((%u, %u, %u), %u, %u)", key->oids.datoid,
					 key->oids.relnode, key->oids.reloid, key->chunknum,
					 key->version);
}

fn
o_table_chunk_tup_print(desc: &mut BTreeDescr, StringInfo buf, OTuple tup, Pointer arg)
{
	chunk: &mut OTableChunk = (OTableChunk *) tup.data;

	appendStringInfo(buf, "(((%u, %u, %u), chunknum %u, version %u), dataLength %u)",
					 chunk->key.oids.datoid,
					 chunk->key.oids.relnode,
					 chunk->key.oids.reloid,
					 chunk->key.chunknum,
					 chunk->key.version,
					 chunk->dataLength);
}

static JsonbValue *
o_table_chunk_key_to_jsonb(desc: &mut BTreeDescr, OTuple tup, JsonbParseState **state)
{
	key: &mut OTableChunkKey = (OTableChunkKey *) tup.data;

	() pushJsonbValue(state, WJB_BEGIN_OBJECT, NULL);
	jsonb_push_int8_key(state, "datoid", key->oids.datoid);
	jsonb_push_int8_key(state, "reloid", key->oids.reloid);
	jsonb_push_int8_key(state, "relnode", key->oids.relnode);
	jsonb_push_int8_key(state, "chunknum", key->chunknum);
	jsonb_push_int8_key(state, "version", key->version);
	return pushJsonbValue(state, WJB_END_OBJECT, NULL);
}

static bool
o_table_chunk_needs_undo(desc: &mut BTreeDescr, BTreeOperationType action,
						 OTuple oldTuple, OTupleXactInfo oldXactInfo, bool oldDeleted,
						 OTuple newTuple, OXid newOxid)
{
	old_tuple_key: &mut OTableChunkKey = (OTableChunkKey *) oldTuple.data;
	new_tuple_key: &mut OTableChunkKey = (OTableChunkKey *) newTuple.data;

	if (action == BTreeOperationDelete)
		return true;

	if (!XACT_INFO_OXID_EQ(oldXactInfo, newOxid))
		return false;

	if (oldDeleted && old_tuple_key->version + 1 == new_tuple_key->version)
		return false;

	if (new_tuple_key && old_tuple_key->version >= new_tuple_key->version)
		return false;

	return true;
}

static int
o_index_chunk_cmp(desc: &mut BTreeDescr,
				   *p1, BTreeKeyType k1,
				   *p2, BTreeKeyType k2)
{
	key1: &mut OIndexChunkKey;
	key2: &mut OIndexChunkKey;

	if (k1 == BTreeKeyBound)
		key1 = (OIndexChunkKey *) p1;
	else
		key1 = (OIndexChunkKey *) (((OTuple *) p1)->data);

	if (k2 == BTreeKeyBound)
		key2 = (OIndexChunkKey *) p2;
	else
		key2 = (OIndexChunkKey *) (((OTuple *) p2)->data);

	if (key1->type != key2->type)
		return (key1->type < key2->type) ? -1 : 1;

	if (key1->oids.datoid != key2->oids.datoid)
		return (key1->oids.datoid < key2->oids.datoid) ? -1 : 1;

	if (key1->oids.relnode != key2->oids.relnode)
		return (key1->oids.relnode < key2->oids.relnode) ? -1 : 1;

	if (key1->chunknum != key2->chunknum)
		return (key1->chunknum < key2->chunknum) ? -1 : 1;

	return 0;
}

static int
o_index_chunk_length(desc: &mut BTreeDescr, OTuple tuple)
{
	chunk: &mut OIndexChunk = (OIndexChunk *) tuple.data;

	return offsetof(OIndexChunk, data) + chunk->dataLength;
}

fn
o_index_chunk_key_print(desc: &mut BTreeDescr, StringInfo buf, OTuple tup, Pointer arg)
{
	key: &mut OIndexChunkKey = (OIndexChunkKey *) tup.data;

	appendStringInfo(buf, "(%d, (%u, %u, %u), %u)", (int) key->type, key->oids.datoid, key->oids.relnode, key->oids.reloid, key->chunknum);
}

fn
o_index_chunk_tup_print(desc: &mut BTreeDescr, StringInfo buf, OTuple tup, Pointer arg)
{
	chunk: &mut OIndexChunk = (OIndexChunk *) tup.data;

	appendStringInfo(buf, "((%d, (%u, %u, %u), %u), %u)",
					 (int) chunk->key.type,
					 chunk->key.oids.datoid,
					 chunk->key.oids.relnode,
					 chunk->key.oids.reloid,
					 chunk->key.chunknum,
					 chunk->dataLength);
}

static JsonbValue *
o_index_chunk_key_to_jsonb(desc: &mut BTreeDescr, OTuple tup, JsonbParseState **state)
{
	key: &mut OIndexChunkKey = (OIndexChunkKey *) tup.data;

	() pushJsonbValue(state, WJB_BEGIN_OBJECT, NULL);
	jsonb_push_int8_key(state, "type", (int64) key->type);
	jsonb_push_int8_key(state, "datoid", key->oids.datoid);
	jsonb_push_int8_key(state, "reloid", key->oids.reloid);
	jsonb_push_int8_key(state, "relnode", key->oids.relnode);
	jsonb_push_int8_key(state, "chunknum", key->chunknum);
	return pushJsonbValue(state, WJB_END_OBJECT, NULL);
}

static bool
o_index_chunk_needs_undo(desc: &mut BTreeDescr, BTreeOperationType action,
						 OTuple oldTuple, OTupleXactInfo oldXactInfo, bool oldDeleted,
						 OTuple newTuple, OXid newOxid)
{
	return true;
}

//
// Compares oids and ix_num of FreeTreeTuples.
//
static inline int
free_tree_id_cmp(left: &mut FreeTreeTuple, right: &mut FreeTreeTuple)
{
	if (left->ixType != right->ixType)
		return left->ixType < right->ixType ? -1 : 1;
	if (left->datoid != right->datoid)
		return left->datoid < right->datoid ? -1 : 1;
	if (left->relnode != right->relnode)
		return left->relnode < right->relnode ? -1 : 1;
	return 0;
}

//
// Comparator for sort order inside a B-tree:
// 1. FreeTreeTuple.datoid
// 2. FreeTreeTuple.relnode
// 3. FreeTreeTuple.ix_num
// 4. FreeTreeTuple.extent.off
//
static int
free_tree_off_len_cmp(desc: &mut BTreeDescr,
					   *p1, BTreeKeyType k1,
					   *p2, BTreeKeyType k2)
{
	left: &mut FreeTreeTuple = (FreeTreeTuple *) ((OTuple *) p1)->data,
			   *right = (FreeTreeTuple *) ((OTuple *) p2)->data;
	int			cmp = free_tree_id_cmp(left, right);

	if (cmp != 0)
		return cmp;

	if (left->extent.offset != right->extent.offset)
		return left->extent.offset < right->extent.offset ? -1 : 1;

	return 0;
}

//
// Comparator for sort order inside a B-tree:
// 1. FreeTreeTuple.datoid
// 2. FreeTreeTuple.relnode
// 3. FreeTreeTuple.ix_num
// 4. FreeTreeTuple.extent.len
// 5. FreeTreeTuple.extent.off
//
static int
free_tree_len_off_cmp(desc: &mut BTreeDescr,
					   *p1, BTreeKeyType k1,
					   *p2, BTreeKeyType k2)
{
	left: &mut FreeTreeTuple = (FreeTreeTuple *) ((OTuple *) p1)->data,
			   *right = (FreeTreeTuple *) ((OTuple *) p2)->data;
	int			cmp = free_tree_id_cmp(left, right);

	if (cmp != 0)
		return cmp;

	if (left->extent.length != right->extent.length)
		return left->extent.length < right->extent.length ? -1 : 1;

	if (left->extent.offset != right->extent.offset)
		return left->extent.offset < right->extent.offset ? -1 : 1;

	return 0;
}

fn
free_tree_print(desc: &mut BTreeDescr, StringInfo buf, OTuple tup, Pointer arg)
{
	f_tree_tup: &mut FreeTreeTuple = (FreeTreeTuple *) tup.data;

	appendStringInfo(
					 buf, "((%u, %u, %u), " UINT64_FORMAT ", " UINT64_FORMAT ")",
					 f_tree_tup->ixType,
					 f_tree_tup->datoid,
					 f_tree_tup->relnode,
					 f_tree_tup->extent.offset,
					 f_tree_tup->extent.length);
}

static JsonbValue *
free_tree_key_to_jsonb(desc: &mut BTreeDescr, OTuple tup, JsonbParseState **state)
{
	key: &mut FreeTreeTuple = (FreeTreeTuple *) tup.data;

	() pushJsonbValue(state, WJB_BEGIN_OBJECT, NULL);
	jsonb_push_int8_key(state, "ixType", (int64) key->ixType);
	jsonb_push_int8_key(state, "datoid", key->datoid);
	jsonb_push_int8_key(state, "relnode", key->relnode);
	jsonb_push_int8_key(state, "offset", key->extent.offset);
	jsonb_push_int8_key(state, "length", key->extent.length);
	return pushJsonbValue(state, WJB_END_OBJECT, NULL);
}

fn
o_chkp_num_print(desc: &mut BTreeDescr, StringInfo buf, OTuple tup, Pointer arg)
{
	chkpNumTup: &mut ChkpNumTuple = (ChkpNumTuple *) tup.data;

	appendStringInfo(buf, "((%u, %u), %u, %u)",
					 chkpNumTup->key.datoid,
					 chkpNumTup->key.relnode,
					 chkpNumTup->checkpointNumbers[0],
					 chkpNumTup->checkpointNumbers[1]);
}

fn
o_evicted_data_print(desc: &mut BTreeDescr, StringInfo buf, OTuple tup, Pointer arg)
{
	evictedData: &mut EvictedTreeData = (EvictedTreeData *) tup.data;

	appendStringInfo(buf, "((%u, %u), %llu, %llu)",
					 evictedData->key.datoid,
					 evictedData->key.relnode,
					 (unsigned long long) evictedData->file_header.rootDownlink,
					 (unsigned long long) evictedData->file_header.datafileLength);
}

static int
o_sys_xid_undo_location_key_cmp(desc: &mut BTreeDescr,
								 *p1, BTreeKeyType k1,
								 *p2, BTreeKeyType k2)
{
	key1: &mut TransactionId = (TransactionId *) (((OTuple *) p1)->data);
	key2: &mut TransactionId = (TransactionId *) (((OTuple *) p2)->data);

	if (TransactionIdPrecedes(*key1, *key2))
		return -1;
	else if (TransactionIdPrecedes(*key2, *key1))
		return 1;

	return 0;
}

fn
o_sys_xid_undo_location_key_print(desc: &mut BTreeDescr, StringInfo buf, OTuple tup, Pointer arg)
{
	key: &mut TransactionId = (TransactionId *) tup.data;

	appendStringInfo(buf, "(%u)", *key);
}

fn
o_sys_xid_undo_location_tuple_print(desc: &mut BTreeDescr, StringInfo buf, OTuple tup, Pointer arg)
{
	tuple: &mut ReplicationRetainUndoTuple = (ReplicationRetainUndoTuple *) tup.data;

	//
// The undo location is an absolute offset into the undo log, which shifts
// with any change to undo allocation (e.g. differential page-level undo
// images consume less space).  Mask it so the structure dump stays stable
// across such layout changes; the actual value is verified separately via
// orioledb_read_sys_xid_undo_location().
//
	appendStringInfo(buf, "(%u, X)", tuple->xid);
}

static JsonbValue *
o_sys_xid_undo_location_key_to_jsonb(desc: &mut BTreeDescr, OTuple tup, JsonbParseState **state)
{
	key: &mut TransactionId = (TransactionId *) tup.data;

	() pushJsonbValue(state, WJB_BEGIN_OBJECT, NULL);
	jsonb_push_int8_key(state, "xid", (int64) *key);
	return pushJsonbValue(state, WJB_END_OBJECT, NULL);
}