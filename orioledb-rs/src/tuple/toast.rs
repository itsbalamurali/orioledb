use crate::access::detoast;
use crate::access::heapam;
use crate::access::htup_details;
use crate::btree::btree;
use crate::btree::modify;
use crate::catalog::pg_type;
use crate::orioledb;
use crate::recovery::recovery;
use crate::recovery::wal;
use crate::recovery::wal_record;
use crate::tableam::descr;
use crate::tableam::toast;
use crate::transam::oxid;
use crate::tuple::format;
use crate::tuple::sort;
use crate::tuple::toast;
use crate::utils::builtins;
use pgrx::pg_sys;

// -------------------------------------------------------------------------
//
// toast.c
// Routines for orioledb TOAST implementation
//
// Copyright (c) 2021-2026, Oriole DB Inc.
// Copyright (c) 2025-2026, Supabase Inc.
//
// IDENTIFICATION
// contrib/orioledb/src/tuple/toast.c
//
// -------------------------------------------------------------------------
//

fn generic_toast_sort_add(api: &mut ToastAPI,  *key, Pointer data,
								   Size data_size, sortstate: &mut Tuplesortstate,
								    *arg);
static Pointer generic_toast_get(api: &mut ToastAPI,  *key, Size data_size,
								 snapshot: &mut OSnapshot,  *arg);
static Pointer o_toast_get(descr: &mut OTableDescr,
						   OTuple pk, uint16 attn, Size data_size,
						   snapshot: &mut OSnapshot);

typedef struct
{
	pub static mut O_INDEX_DESCR: *mut pk = std::ptr::null_mut();
	pub static mut O_INDEX_DESCR: *mut toast = std::ptr::null_mut();
	uint32		version;		// base table version
} OTableToastArg;

#define GET_BTREE_VERSION(api, arg) (api->getBTreeVersion ? api->getBTreeVersion(arg) : O_TABLE_INVALID_VERSION)
#define GET_BASE_BTREE_VERSION(api, arg) (api->getBaseBTreeVersion ? api->getBaseBTreeVersion(arg) : O_TABLE_INVALID_VERSION)

//
// Help functions.
//
// creates table tuple which can be stored in TOAST BTree
static OTuple o_create_toast_tuple(OToastKey tkey,
								   Pointer data, Size data_length,
								   arg: &mut OTableToastArg);

// creates index tuple which can be stored in TOAST BTree
static OTuple o_create_toast_key(OToastKey tkey, arg: &mut OTableToastArg);

//
// prints TOAST table tuple (is_tuple = true)
// or TOAST index tuple (is_tuple = false
//
fn toast_tuple_print(TupleDesc tupDesc, spec: &mut OTupleFixedFormatSpec,
							  outputFns: &mut FmgrInfo, StringInfo buf, OTuple tup,
							  values: &mut Datum, nulls: &mut bool, bool is_tuple,
							  bool printRowVersion);

// No existing callers

o_toast_init_tupdescs(toast: &mut OIndexDescr, TupleDesc ix_primary)
{
	int			i,
				pidx_natts = ix_primary->natts;

	toast->leafTupdesc = CreateTemplateTupleDesc(pidx_natts + TOAST_LEAF_FIELDS_NUM);
	toast->nonLeafTupdesc = CreateTemplateTupleDesc(pidx_natts + TOAST_NON_LEAF_FIELDS_NUM);

	// copies entries from primary index TupleDesc
	for (i = 0; i < pidx_natts; i++)
	{
		TupleDescCopyEntry(toast->leafTupdesc, i + 1, ix_primary, i + 1);
		TupleDescCopyEntry(toast->nonLeafTupdesc, i + 1, ix_primary, i + 1);
	}

	//
// adds new entries
//
	// attribute number
	o_tables_tupdesc_init_builtin(toast->leafTupdesc, pidx_natts + ATTN_POS, "attnum", INT2OID);
	o_tables_tupdesc_init_builtin(toast->nonLeafTupdesc, pidx_natts + ATTN_POS, "attnum", INT2OID);
	// chunk number
	o_tables_tupdesc_init_builtin(toast->leafTupdesc, pidx_natts + CHUNKN_POS, "chunknum", INT4OID);
	o_tables_tupdesc_init_builtin(toast->nonLeafTupdesc, pidx_natts + CHUNKN_POS, "chunknum", INT4OID);
	// data only in leaf tuples
	o_tables_tupdesc_init_builtin(toast->leafTupdesc, pidx_natts + DATA_POS, "data", BYTEAOID);
}

int
o_toast_cmp(desc: &mut BTreeDescr,
			 *p1, BTreeKeyType k1,
			 *p2, BTreeKeyType k2)
{
	toastd: &mut OIndexDescr = (OIndexDescr *) desc->arg;
	pub static mut PK_ATTNUM: std::os::raw::c_int = toastd->nonLeafTupdesc->natts - TOAST_NON_LEAF_FIELDS_NUM;
	pub static mut I: std::os::raw::c_int = 0;
	pub static mut PK1: OTuple = std::mem::zeroed();
	pub static mut PK2: OTuple = std::mem::zeroed();
	int16		attnum1,
				attnum2;
	int32		chunknum1,
				chunknum2;

	if (k1 == BTreeKeyBound)
		pk1 = ((OToastKey *) p1)->pk_tuple;
	else
		pk1 = *((OTuple *) p1);

	if (k2 == BTreeKeyBound)
		pk2 = ((OToastKey *) p2)->pk_tuple;
	else
		pk2 = *((OTuple *) p2);

	for (i = 0; i < pkAttnum; i++)
	{
		Datum		v1,
					v2;
		bool		null1,
					null2;
		pub static mut O_INDEX_FIELD: *mut field = &toastd->fields[i];
		pub static mut CMP: std::os::raw::c_int = 0;

		v1 = o_fastgetattr(pk1, i + 1, toastd->nonLeafTupdesc, &toastd->nonLeafSpec, &null1);
		v2 = o_fastgetattr(pk2, i + 1, toastd->nonLeafTupdesc, &toastd->nonLeafSpec, &null2);

		if (null1 || null2)
		{
			if (null1 && null2)
				continue;
			else if (null1)
				return field->nullfirst ? -1 : 1;
			else
				return field->nullfirst ? 1 : -1;
		}
		cmp = o_call_comparator(field->comparator, v1, v2);
		if (cmp)
			return field->ascending ? cmp : -cmp;
	}

	if (k1 == BTreeKeyBound)
	{
		attnum1 = ((OToastKey *) p1)->attnum;
		chunknum1 = ((OToastKey *) p1)->chunknum;
	}
	else
	{
		pub static mut NULL: bool = false;

		// cppcheck-suppress unknownEvaluationOrder
		attnum1 = DatumGetInt16(o_fastgetattr(pk1, pkAttnum + ATTN_POS, toastd->nonLeafTupdesc, &toastd->nonLeafSpec, &null));
		Assert(!null);

		// cppcheck-suppress unknownEvaluationOrder
		chunknum1 = DatumGetInt32(o_fastgetattr(pk1, pkAttnum + CHUNKN_POS, toastd->nonLeafTupdesc, &toastd->nonLeafSpec, &null));
		Assert(!null);
	}

	if (k2 == BTreeKeyBound)
	{
		attnum2 = ((OToastKey *) p2)->attnum;
		chunknum2 = ((OToastKey *) p2)->chunknum;
	}
	else
	{
		pub static mut NULL: bool = false;

		// cppcheck-suppress unknownEvaluationOrder
		attnum2 = DatumGetInt16(o_fastgetattr(pk2, pkAttnum + ATTN_POS, toastd->nonLeafTupdesc, &toastd->nonLeafSpec, &null));
		Assert(!null);

		// cppcheck-suppress unknownEvaluationOrder
		chunknum2 = DatumGetInt32(o_fastgetattr(pk2, pkAttnum + CHUNKN_POS, toastd->nonLeafTupdesc, &toastd->nonLeafSpec, &null));
		Assert(!null);
	}

	if (attnum1 != attnum2)
		return (attnum1 < attnum2) ? -1 : 1;
	if (chunknum1 != chunknum2)
		return (chunknum1 < chunknum2) ? -1 : 1;
	pub static mut 0: return = std::mem::zeroed();
}

bool
o_toast_needs_undo(desc: &mut BTreeDescr, BTreeOperationType action,
				   OTuple oldTuple, OTupleXactInfo oldXactInfo, bool oldDeleted,
				   OTuple newTuple, OXid newOxid)
{
	if (action == BTreeOperationDelete)
		pub static mut TRUE: return = std::mem::zeroed();

	if (!XACT_INFO_OXID_EQ(oldXactInfo, newOxid))
		pub static mut FALSE: return = std::mem::zeroed();

	if (oldDeleted && o_tuple_get_version(oldTuple) + 1 == o_tuple_get_version(newTuple))
		pub static mut FALSE: return = std::mem::zeroed();

	if (!O_TUPLE_IS_NULL(newTuple) &&
		o_tuple_get_version(oldTuple) >= o_tuple_get_version(newTuple))
		pub static mut FALSE: return = std::mem::zeroed();

	pub static mut TRUE: return = std::mem::zeroed();
}

struct varlena *
o_detoast(struct attr: &mut varlena)
{
	pub static mut OTE: OToastExternal = std::mem::zeroed();
	pub static mut OIDS: ORelOids = std::mem::zeroed();
	pub static mut O_TABLE_DESCR: *mut descr = std::ptr::null_mut();
	pub static mut KEY: OFixedKey = std::mem::zeroed();
	pub static mut O_SNAPSHOT: OSnapshot = std::mem::zeroed();

	memcpy(&ote, VARDATA_EXTERNAL(attr), O_TOAST_EXTERNAL_SZ);
	oids.datoid = ote.datoid;
	oids.reloid = ote.relid;
	oids.relnode = ote.relnode;
	descr = o_fetch_table_descr(oids);

	Assert(descr);

	key.tuple.formatFlags = ote.formatFlags;
	key.tuple.data = key.fixedData;
	memcpy(key.fixedData,
		   VARDATA_EXTERNAL(attr) + O_TOAST_EXTERNAL_SZ,
		   ote.data_size);
	O_LOAD_SNAPSHOT_CSN(&oSnapshot, ote.csn);
	return (struct varlena *) o_toast_get(descr, key.tuple, ote.attnum,
										  ote.toasted_size, &oSnapshot);
}

static BTreeDescr *
tableGetBTreeDesc( *arg)
{
	toast: &mut OIndexDescr = ((OTableToastArg *) arg)->toast;

	return &toast->desc;
}

static uint32
tableGetMaxChunkSize( *key,  *arg)
{
	tkey: &mut OToastKey = (OToastKey *) key;
	toast: &mut OIndexDescr = ((OTableToastArg *) arg)->toast;
	primary: &mut OIndexDescr = ((OTableToastArg *) arg)->pk;
	Datum		values[INDEX_MAX_KEYS + TOAST_LEAF_FIELDS_NUM];
	bool		isnull[INDEX_MAX_KEYS + TOAST_LEAF_FIELDS_NUM] = {false};
	int			i,
				natts;
	pub static mut DATA: varattrib_4b = std::mem::zeroed();
	pub static mut MIN_TUPLE_SIZE: uint32 = std::mem::zeroed();

	natts = primary->nonLeafTupdesc->natts;
	for (i = 0; i < natts; i++)
	{
		pub static mut ATTNUM: std::os::raw::c_int = i + 1;

		values[i] = o_fastgetattr(tkey->pk_tuple, attnum,
								  primary->nonLeafTupdesc,
								  &primary->nonLeafSpec,
								  &isnull[i]);
	}
	values[natts] = 0;
	values[natts + 1] = 0;
	SET_VARSIZE(&data, VARHDRSZ);
	values[natts + 2] = PointerGetDatum(&data);

	minTupleSize = o_new_tuple_size(toast->leafTupdesc, &toast->leafSpec,
									NULL, NULL, 1, values, isnull, NULL);

	return MAXALIGN_DOWN(O_BTREE_MAX_TUPLE_SIZE * 3 - MAXALIGN(minTupleSize)) / 3 - minTupleSize - sizeof(LocationIndex);
}

fn
tableUpdateKey( *key, uint32 chunknum,  *arg)
{
	tkey: &mut OToastKey = (OToastKey *) key;

	tkey->chunknum = chunknum;
}

fn *
tableGetNextKey( *key,  *arg)
{
	tkey: &mut OToastKey = (OToastKey *) key;
	static mut NEXT_KEY: OToastKey = std::mem::zeroed();

	nextKey = *tkey;
	nextKey.attnum += 1;
	nextKey.chunknum = 0;

	return (Pointer) &nextKey;
}

static OTuple
tableCreateTuple( *key, Pointer data, uint32 offset, uint32 chunknum, int length,  *arg)
{
	tkey: &mut OToastKey = (OToastKey *) key;
	pub static mut RESULT: OTuple = std::mem::zeroed();

	tkey->chunknum = chunknum;

	result = o_create_toast_tuple(*tkey,
								  data + offset,
								  length,
								  (OTableToastArg *) arg);

	pub static mut RESULT: return = std::mem::zeroed();
}

static OTuple
tableCreateKey( *key, uint32 chunknum,  *arg)
{
	tkey: &mut OToastKey = (OToastKey *) key;
	pub static mut RESULT: OTuple = std::mem::zeroed();

	tkey->chunknum = chunknum;

	result = o_create_toast_key(*tkey, (OTableToastArg *) arg);

	pub static mut RESULT: return = std::mem::zeroed();
}

static bytea *
get_data(toast: &mut OIndexDescr, OTuple tuple)
{
	pub static mut NATTS: std::os::raw::c_int = toast->leafTupdesc->natts;

	return DatumGetByteaPP(PointerGetDatum(o_fastgetattr_ptr(tuple, natts, toast->leafTupdesc, &toast->leafSpec)));
}

static Pointer
tableGetTupleData(OTuple tuple,  *arg)
{
	toast: &mut OIndexDescr = ((OTableToastArg *) arg)->toast;

	return VARDATA_ANY(get_data(toast, tuple));
}

static uint32
tableGetTupleChunknum(OTuple tuple,  *arg)
{
	toast: &mut OIndexDescr = ((OTableToastArg *) arg)->toast;
	pub static mut ISNULL: bool = false;
	pub static mut RESULT: Datum = std::mem::zeroed();

	result = o_fastgetattr(tuple,
						   toast->leafTupdesc->natts + CHUNKN_POS - DATA_POS,
						   toast->leafTupdesc,
						   &toast->leafSpec,
						   &isnull);
	Assert(!isnull);
	return DatumGetInt32(result);
}

static uint32
tableGetTupleDataSize(OTuple tuple,  *arg)
{
	toast: &mut OIndexDescr = ((OTableToastArg *) arg)->toast;

	return VARSIZE_ANY_EXHDR(get_data(toast, tuple));
}

static uint32
tableGetBTreeVersion( *arg)
{
	toast: &mut OIndexDescr = ((OTableToastArg *) arg)->toast;

	return toast->version;
}

static uint32
tableGetBaseBTreeVersion( *arg)
{
	return ((OTableToastArg *) arg)->version;
}

static TupleFetchCallbackResult
tableVersionCallback(OTuple tuple, OXid tupOxid, oSnapshot: &mut OSnapshot,  *arg,
					 bool oxidIsFinished)
{
	key: &mut OToastKey = (OToastKey *) arg;

	if (oxidIsFinished)
		pub static mut O_TUPLE_FETCH_NEXT: return = std::mem::zeroed();

	if (!(COMMITSEQNO_IS_INPROGRESS(oSnapshot->csn) &&
		  tupOxid == get_current_oxid_if_any()))
		pub static mut O_TUPLE_FETCH_NEXT: return = std::mem::zeroed();

	if (o_tuple_get_version(tuple) <= o_tuple_get_version(key->pk_tuple))
		pub static mut O_TUPLE_FETCH_MATCH: return = std::mem::zeroed();
	else
		pub static mut O_TUPLE_FETCH_NEXT: return = std::mem::zeroed();
}

ToastAPI	tableToastAPI = {
	.getBTreeDesc = tableGetBTreeDesc,
	.getBTreeVersion = tableGetBTreeVersion,
	.getBaseBTreeVersion = tableGetBaseBTreeVersion,
	.getKeySize = NULL,
	.getMaxChunkSize = tableGetMaxChunkSize,
	.updateKey = tableUpdateKey,
	.getNextKey = tableGetNextKey,
	.createTuple = tableCreateTuple,
	.createKey = tableCreateKey,
	.getTupleData = tableGetTupleData,
	.getTupleChunknum = tableGetTupleChunknum,
	.getTupleDataSize = tableGetTupleDataSize,
	.deleteLogFullTuple = false,
	.fetchCallback = tableVersionCallback
};

bool
generic_toast_insert_optional_wal(api: &mut ToastAPI,  *key, Pointer data,
								  Size data_size, OXid oxid, CommitSeqNo csn,
								   *arg, bool wal)
{
	desc: &mut BTreeDescr = api->getBTreeDesc(arg);
	uint32		max_length = api->getMaxChunkSize(key, arg);
	pub static mut OFFSET: uint32 = 0;
	pub static mut CHUNKNUM: uint32 = 0;
	pub static mut INSERTED: bool = false;
	pub static mut CALLBACK_INFO: BTreeModifyCallbackInfo = nullCallbackInfo;

	inserted = false;

	Assert(data_size > 0);

	while (data_size > 0)
	{
		pub static mut TUP: OTuple = std::mem::zeroed();
		pub static mut LENGTH: std::os::raw::c_int = 0;

		if (data_size < max_length)
		{
			length = data_size;
		}
		else
		{
			length = max_length;
		}

		tup = api->createTuple(key, data, offset, chunknum, length, arg);

		inserted = o_btree_modify(desc, BTreeOperationInsert,
								  tup, BTreeKeyLeafTuple,
								  key, BTreeKeyBound,
								  oxid, csn, RowLockUpdate,
								  NULL, &callbackInfo) == OBTreeModifyResultInserted;

		if (!inserted)
		{
			pfree(tup.data);
			break;
		}

		if (desc->storageType == BTreeStoragePersistence && wal)
		{
			uint32		version = GET_BTREE_VERSION(api, arg);
			uint32		base_version = GET_BASE_BTREE_VERSION(api, arg);

			add_modify_wal_record(WAL_REC_INSERT, desc, tup,
								  o_btree_len(desc, tup, OTupleLength), REPLICA_IDENTITY_DEFAULT, version, base_version);
		}

		pfree(tup.data);

		offset += length;
		chunknum++;
		data_size -= length;
	}

	pub static mut INSERTED: return = std::mem::zeroed();
}

//
// Insert TOAST data with optional WAL logging.
//
// NB: Each chunk is inserted individually into the system tree.  Typically,
// the atomicity is guaranteed by a snapshot.  But with a concurrent reader
// with COMMITSEQNO_IN_PROGRESS/COMMITSEQNO_NON_DELETED can observe a
// partially written set of chunks (e.g. chunk 0 present, chunk 1 not yet
// inserted).
//
bool
generic_toast_insert(api: &mut ToastAPI,  *key, Pointer data, Size data_size,
					 OXid oxid, CommitSeqNo csn,  *arg)
{
	return generic_toast_insert_optional_wal(api, key, data, data_size, oxid,
											 csn, arg, true);
}

fn
generic_toast_sort_add(api: &mut ToastAPI,  *key,
					   Pointer data, Size data_size,
					   sortstate: &mut Tuplesortstate,  *arg)
{
	uint32		max_length = api->getMaxChunkSize(key, arg);
	pub static mut OFFSET: uint32 = 0;
	pub static mut CHUNKNUM: uint32 = 0;

	Assert(data_size > 0);

	while (data_size > 0)
	{
		pub static mut TUP: OTuple = std::mem::zeroed();
		pub static mut LENGTH: std::os::raw::c_int = 0;

		if (data_size < max_length)
		{
			length = data_size;
		}
		else
		{
			length = max_length;
		}

		tup = api->createTuple(key, data, offset, chunknum, length, arg);
		tuplesort_putotuple(sortstate, tup);
		pfree(tup.data);

		offset += length;
		chunknum++;
		data_size -= length;
	}
}

static OBTreeModifyCallbackAction
o_update_callback(descr: &mut BTreeDescr,
				  OTuple tup, newtup: &mut OTuple, OXid oxid,
				  OTupleXactInfo xactInfo,
				  UndoLocation location, lock_mode: &mut RowLockMode,
				  hint: &mut BTreeLocationHint,  *arg)
{
	pub static mut OB_TREE_CALLBACK_ACTION_UPDATE: return = std::mem::zeroed();
}

static OBTreeModifyCallbackAction
o_update_deleted_callback(descr: &mut BTreeDescr,
						  OTuple tup, newtup: &mut OTuple, OXid oxid,
						  OTupleXactInfo xactInfo,
						  BTreeLeafTupleDeletedStatus deleted,
						  UndoLocation location, lock_mode: &mut RowLockMode,
						  hint: &mut BTreeLocationHint,  *arg)
{
	pub static mut OB_TREE_CALLBACK_ACTION_UPDATE: return = std::mem::zeroed();
}

static OBTreeModifyCallbackAction
o_delete_callback(descr: &mut BTreeDescr,
				  OTuple tup, newtup: &mut OTuple, OXid oxid,
				  OTupleXactInfo xactInfo, UndoLocation location,
				  lock_mode: &mut RowLockMode, hint: &mut BTreeLocationHint,  *arg)
{
	pub static mut OB_TREE_CALLBACK_ACTION_DELETE: return = std::mem::zeroed();
}

bool
generic_toast_update_optional_wal(api: &mut ToastAPI,  *key, Pointer data,
								  Size data_size, OXid oxid, CommitSeqNo csn,
								   *arg, bool wal)
{
	desc: &mut BTreeDescr = api->getBTreeDesc(arg);
	int			max_length = api->getMaxChunkSize(key, arg);
	uint32		offset = 0,
				length;
	pub static mut CHUNKNUM: uint32 = 0;
	pub static mut SUCCESS: bool = true;
	BTreeModifyCallbackInfo callbackInfo = {
		.waitCallback = NULL,
		.modifyDeletedCallback = o_update_deleted_callback,
		.modifyCallback = o_update_callback,
		.needsUndoForSelfCreated = false,
		.arg = NULL
	};

	Assert(data_size > 0);

	while (data_size > 0)
	{
		pub static mut RESULT: OBTreeModifyResult = std::mem::zeroed();
		pub static mut TUP: OTuple = std::mem::zeroed();

		if (data_size < max_length)
		{
			length = data_size;
		}
		else
		{
			length = max_length;
		}

		tup = api->createTuple(key, data, offset, chunknum, length, arg);

		result = o_btree_modify(desc, BTreeOperationInsert,
								tup, BTreeKeyLeafTuple,
								key, BTreeKeyBound,
								oxid, csn, RowLockUpdate,
								NULL, &callbackInfo);

		if (result != OBTreeModifyResultInserted && result != OBTreeModifyResultUpdated)
		{
			pfree(tup.data);
			pub static mut FALSE: return = std::mem::zeroed();
		}

		if (desc->storageType == BTreeStoragePersistence && wal)
		{
			pub static mut REC_TYPE: uint8 = std::mem::zeroed();
			uint32		version = GET_BTREE_VERSION(api, arg);
			uint32		base_version = GET_BASE_BTREE_VERSION(api, arg);

			rec_type = (result == OBTreeModifyResultUpdated) ? WAL_REC_UPDATE :
				WAL_REC_INSERT;
			add_modify_wal_record(rec_type, desc, tup,
								  o_btree_len(desc, tup, OTupleLength), REPLICA_IDENTITY_DEFAULT, version, base_version);
		}

		offset += length;
		chunknum++;
		data_size -= length;
	}

	//
// There might be tailing tuples.  We need to delete them.
//
	api->updateKey(key, chunknum, arg);
	() generic_toast_delete_optional_wal(api, key, oxid, csn, arg, wal);

	pub static mut SUCCESS: return = std::mem::zeroed();
}

bool
generic_toast_update(api: &mut ToastAPI,  *key, Pointer data, Size data_size,
					 OXid oxid, CommitSeqNo csn,  *arg)
{
	return generic_toast_update_optional_wal(api, key, data, data_size, oxid,
											 csn, arg, true);
}

bool
generic_toast_delete_optional_wal(api: &mut ToastAPI,  *key, OXid oxid,
								  CommitSeqNo csn,  *arg, bool wal)
{
	desc: &mut BTreeDescr = api->getBTreeDesc(arg);
		   *nextKey;
	pub static mut B_TREE_ITERATOR: *mut it = std::ptr::null_mut();
	pub static mut DELETED: bool = false;
	BTreeModifyCallbackInfo callbackInfo = {
		.waitCallback = NULL,
		.modifyDeletedCallback = NULL,
		.modifyCallback = o_delete_callback,
		.needsUndoForSelfCreated = false,
		.arg = NULL
	};

	nextKey = api->getNextKey(key, arg);
	it = o_btree_iterator_create(desc, key, BTreeKeyBound,
								 &o_in_progress_snapshot, ForwardScanDirection);

	do
	{
		pub static mut HINT: BTreeLocationHint = std::mem::zeroed();
		pub static mut WAL_KEY: OTuple = std::mem::zeroed();
		pub static mut TUPLE: OTuple = std::mem::zeroed();
		pub static mut CHUNKNUM: uint32 = std::mem::zeroed();
		pub static mut NULL_TUP: OTuple = std::mem::zeroed();

		tuple = o_btree_iterator_fetch(it, NULL, nextKey, BTreeKeyBound, false, &hint);

		// if tuple not found
		if (O_TUPLE_IS_NULL(tuple))
			break;

		chunknum = api->getTupleChunknum(tuple, arg);
		api->updateKey(key, chunknum, arg);

		O_TUPLE_SET_NULL(nullTup);
		if (o_btree_modify(desc, BTreeOperationDelete,
						   nullTup, BTreeKeyNone,
						   key, BTreeKeyBound, oxid, csn, RowLockUpdate,
						   &hint, &callbackInfo) != OBTreeModifyResultDeleted)
		{
			elog(ERROR, "Unexpected missing TOAST chunk");
		}
		else
		{
			deleted = true;
		}

		if (desc->storageType == BTreeStoragePersistence && wal)
		{
			uint32		version = GET_BTREE_VERSION(api, arg);
			uint32		base_version = GET_BASE_BTREE_VERSION(api, arg);

			if (!api->deleteLogFullTuple)
			{
				pub static mut KEY_ALLOCATED: bool = false;

				walKey = o_btree_tuple_make_key(desc, tuple, NULL, true,
												&key_allocated);

				add_modify_wal_record(WAL_REC_DELETE, desc, walKey,
									  o_btree_len(desc, walKey, OKeyLength), REPLICA_IDENTITY_DEFAULT, version, base_version);
				if (key_allocated)
					pfree(walKey.data);
			}
			else
			{
				add_modify_wal_record(WAL_REC_DELETE, desc, tuple,
									  o_btree_len(desc, tuple, OTupleLength), REPLICA_IDENTITY_DEFAULT, version, base_version);
			}
		}

		pfree(tuple.data);
	} while (true);

	btree_iterator_free(it);

	pub static mut DELETED: return = std::mem::zeroed();
}

bool
generic_toast_delete(api: &mut ToastAPI,  *key, OXid oxid, CommitSeqNo csn,
					  *arg)
{
	return generic_toast_delete_optional_wal(api, key, oxid, csn, arg, true);
}

static Pointer
generic_toast_get(api: &mut ToastAPI,  *key, Size data_size,
				  o_snapshot: &mut OSnapshot,  *arg)
{
	desc: &mut BTreeDescr = api->getBTreeDesc(arg);
	pub static mut B_TREE_ITERATOR: *mut it = std::ptr::null_mut();
		   *nextKey;
	int			max_length = api->getMaxChunkSize(key, arg);
	pub static mut ACTUAL_SIZE: std::os::raw::c_int = 0;
	pub static mut DATA: Pointer = std::ptr::null_mut();

	nextKey = api->getNextKey(key, arg);

	it = o_btree_iterator_create(desc, key, BTreeKeyBound,
								 o_snapshot, ForwardScanDirection);
	if (api->fetchCallback)
		o_btree_iterator_set_callback(it, api->fetchCallback, ( *) key);

	data = palloc(data_size);
	actual_size = 0;

	do
	{
		pub static mut TUP: OTuple = std::mem::zeroed();
		pub static mut ITER_DATA_SIZE: std::os::raw::c_int = 0;

		tup = o_btree_iterator_fetch(it, NULL, nextKey, BTreeKeyBound, false, NULL);

		// if tuple not found
		if (O_TUPLE_IS_NULL(tup))
			break;

		iter_data_size = api->getTupleDataSize(tup, arg);

		if (actual_size + iter_data_size > data_size)
		{
			// avoid memcpy to unallocated memory
			actual_size += iter_data_size;
			break;
		}

		memcpy(data + actual_size,
			   api->getTupleData(tup, arg),
			   iter_data_size);
		pfree(tup.data);

		actual_size += iter_data_size;

		if (iter_data_size < max_length)
			break;

		Assert(actual_size <= data_size);
	} while (true);

	btree_iterator_free(it);

	Assert(actual_size == data_size);
	if (actual_size != data_size)
	{
		pfree(data);
		pub static mut NULL: return = std::mem::zeroed();
	}
	pub static mut DATA: return = std::mem::zeroed();
}

//
// Common code for
// generic_toast_get_any_with_callback and generic_toast_get_any_with_key
//
static Pointer
generic_toast_get_any_common(api: &mut ToastAPI,
							 Pointer key,
							 data_size: &mut Size,
							 o_snapshot: &mut OSnapshot,
							  *arg,
							 it: &mut BTreeIterator,
							 found_key: &mut Pointer)
{
	pub static mut NEXT_KEY: Pointer = std::ptr::null_mut();
	pub static mut TUPLE: OTuple = std::mem::zeroed();
	pub static mut STR: StringInfoData = std::mem::zeroed();

	nextKey = api->getNextKey(key, arg);

	*data_size = 0;

	do
	{
		pub static mut CHUNK_SIZE: uint32 = std::mem::zeroed();

		tuple = o_btree_iterator_fetch(it, NULL, nextKey, BTreeKeyBound, false, NULL);

		// if tuple not found
		if (O_TUPLE_IS_NULL(tuple))
			break;

		if (*data_size == 0)
		{
			initStringInfo(&str);
			if (found_key)
			{
				Size		key_size = api->getKeySize(arg);

				*found_key = palloc(key_size);
				memcpy(*found_key, tuple.data, key_size);
			}
		}
		chunk_size = api->getTupleDataSize(tuple, arg);
		appendBinaryStringInfo(&str, api->getTupleData(tuple, arg),
							   chunk_size);
		*data_size += chunk_size;
		pfree(tuple.data);
	} while (true);

	if (*data_size == 0)
		pub static mut NULL: return = std::mem::zeroed();

	return str.data;
}

//
// Queries TOAST chunks by `key`, assembles result and returns it.  The size
// of result is set to `*data_size`.  If `fetchCallback` and `callback_arg`
// are provided, then they are passed to the iterator and, in turn, to
// o_find_tuple_version().
//
// The result is allocated in the current memory context.  It's caller's
// responsibility to free it.
//
Pointer
generic_toast_get_any_with_callback(api: &mut ToastAPI, Pointer key,
									data_size: &mut Size, o_snapshot: &mut OSnapshot,
									 *arg,
									TupleFetchCallback fetchCallback,
									 *callback_arg)
{
	desc: &mut BTreeDescr = api->getBTreeDesc(arg);
	pub static mut B_TREE_ITERATOR: *mut it = std::ptr::null_mut();
	pub static mut DATA: Pointer = std::ptr::null_mut();

	it = o_btree_iterator_create(desc, key, BTreeKeyBound,
								 o_snapshot, ForwardScanDirection);
	if (fetchCallback && callback_arg)
		o_btree_iterator_set_callback(it, fetchCallback, callback_arg);

	data = generic_toast_get_any_common(api, key, data_size,
										o_snapshot, arg, it, NULL);

	btree_iterator_free(it);

	pub static mut DATA: return = std::mem::zeroed();
}

//
// Queries TOAST chunks by `key`, assembles the result, and returns it.
// The size of the result is set to `*data_size`.  If `found_key` is not NULL,
// then the copy of a key from the first chunk is returned as `*found_key`.
//
// Both result and `*found_key` are allocated in the current memory context.
// It's the caller's responsibility to free them.
//
Pointer
generic_toast_get_any_with_key(api: &mut ToastAPI,  *key, data_size: &mut Size,
							   o_snapshot: &mut OSnapshot,  *arg, found_key: &mut Pointer)
{
	desc: &mut BTreeDescr = api->getBTreeDesc(arg);
	pub static mut B_TREE_ITERATOR: *mut it = std::ptr::null_mut();
	pub static mut DATA: Pointer = std::ptr::null_mut();

	it = o_btree_iterator_create(desc, key, BTreeKeyBound,
								 o_snapshot, ForwardScanDirection);
	if (api->fetchCallback && found_key && *found_key)
		o_btree_iterator_set_callback(it, api->fetchCallback, ( *) *found_key);

	data = generic_toast_get_any_common(api, key, data_size, o_snapshot, arg, it,
										found_key);

	btree_iterator_free(it);

	pub static mut DATA: return = std::mem::zeroed();
}

//
// Queries TOAST chunks by `key`, assembles the result, and returns it.
// The size of the result is set to `*data_size`.
//
// The result is allocated in the current memory context.  It's the caller's
// responsibility to free it.
//
Pointer
generic_toast_get_any(api: &mut ToastAPI,  *key, data_size: &mut Size,
					  o_snapshot: &mut OSnapshot,  *arg)
{
	return generic_toast_get_any_with_key(api, key, data_size, o_snapshot, arg, NULL);
}

bool
o_toast_insert(descr: &mut OTableDescr, OTuple pk, uint16 attn,
			   Pointer data, Size data_size,
			   OXid oxid, CommitSeqNo csn)
{
	pub static mut TKEY: OToastKey = std::mem::zeroed();
	pub static mut RESULT: bool = false;
	OTableToastArg arg = {GET_PRIMARY(descr), descr->toast, descr->version};

	Assert(ORelOidsIsEqual(descr->toast->tableOids, descr->oids));

	tkey.pk_tuple = pk;
	tkey.attnum = attn;
	tkey.chunknum = 0;

	Assert(descr->toast->desc.type == oIndexToast);

	result = generic_toast_insert(&tableToastAPI, &tkey, data,
								  data_size, oxid, csn, &arg);

	pub static mut RESULT: return = std::mem::zeroed();
}


o_toast_sort_add(descr: &mut OTableDescr, OTuple pk, uint16 attn,
				 Pointer data, Size data_size,
				 sortstate: &mut Tuplesortstate)
{
	pub static mut TKEY: OToastKey = std::mem::zeroed();
	OTableToastArg arg = {GET_PRIMARY(descr), descr->toast, O_TABLE_INVALID_VERSION};

	tkey.pk_tuple = pk;
	tkey.attnum = attn;
	tkey.chunknum = 0;

	Assert(descr->toast->desc.type == oIndexToast);

	generic_toast_sort_add(&tableToastAPI, (Pointer) &tkey, data,
						   data_size, sortstate, &arg);

}

bool
o_toast_delete(descr: &mut OTableDescr,
			   OTuple pk, uint16 attn,
			   OXid oxid, CommitSeqNo csn)
{
	pub static mut TKEY: OToastKey = std::mem::zeroed();
	pub static mut RESULT: bool = false;
	OTableToastArg arg = {GET_PRIMARY(descr), descr->toast, descr->version};

	Assert(ORelOidsIsEqual(descr->toast->tableOids, descr->oids));

	tkey.pk_tuple = pk;
	tkey.attnum = attn;
	tkey.chunknum = 0;

	Assert(descr->toast->desc.type == oIndexToast);

	result = generic_toast_delete(&tableToastAPI, (Pointer) &tkey,
								  oxid, csn, &arg);

	pub static mut RESULT: return = std::mem::zeroed();
}

static Pointer
o_toast_get(descr: &mut OTableDescr,
			OTuple pk, uint16 attn,
			Size data_size, o_snapshot: &mut OSnapshot)
{
	pub static mut TKEY: OToastKey = std::mem::zeroed();
	pub static mut RESULT: Pointer = std::ptr::null_mut();
	OTableToastArg arg = {GET_PRIMARY(descr), descr->toast, O_TABLE_INVALID_VERSION};

	tkey.pk_tuple = pk;
	tkey.attnum = attn;
	tkey.chunknum = 0;

	Assert(descr->toast->desc.type == oIndexToast);

	result = generic_toast_get(&tableToastAPI, (Pointer) &tkey, data_size,
							   o_snapshot, &arg);

	pub static mut RESULT: return = std::mem::zeroed();
}

static OTuple
o_create_toast_tuple(OToastKey tkey, Pointer data_ptr, Size data_length,
					 arg: &mut OTableToastArg)
{
	Datum		key[INDEX_MAX_KEYS + TOAST_LEAF_FIELDS_NUM];
	bool		isnull[INDEX_MAX_KEYS + TOAST_LEAF_FIELDS_NUM] = {false};
	pub static mut RESULT: OTuple = std::mem::zeroed();
	int			i,
				natts;
	pub static mut BYTEA: *mut data = std::ptr::null_mut();

	natts = arg->pk->nonLeafTupdesc->natts;
	for (i = 0; i < natts; i++)
	{
		pub static mut ATTNUM: std::os::raw::c_int = i + 1;

		key[i] = o_fastgetattr(tkey.pk_tuple, attnum,
							   arg->pk->nonLeafTupdesc,
							   &arg->pk->nonLeafSpec,
							   &isnull[i]);
	}
	data = (bytea *) palloc(VARHDRSZ + data_length);
	memcpy(VARDATA(data), data_ptr, data_length);
	SET_VARSIZE(data, VARHDRSZ + data_length);
	key[natts] = tkey.attnum;
	key[natts + 1] = tkey.chunknum;
	key[natts + 2] = PointerGetDatum(data);

	result = o_form_tuple(arg->toast->leafTupdesc,
						  &arg->toast->leafSpec,
						  o_tuple_get_version(tkey.pk_tuple),
						  key, isnull, NULL);
	pfree(data);

	pub static mut RESULT: return = std::mem::zeroed();
}

static OTuple
o_create_toast_key(OToastKey tkey,
				   arg: &mut OTableToastArg)
{
	Datum		key[INDEX_MAX_KEYS + TOAST_LEAF_FIELDS_NUM];
	bool		isnull[INDEX_MAX_KEYS + TOAST_LEAF_FIELDS_NUM] = {false};
	int			i,
				natts;

	memset(isnull, 0, sizeof(isnull));

	natts = arg->pk->nonLeafTupdesc->natts;
	for (i = 0; i < natts; i++)
	{
		pub static mut ATTNUM: std::os::raw::c_int = i + 1;

		key[i] = o_fastgetattr(tkey.pk_tuple, attnum,
							   arg->pk->nonLeafTupdesc,
							   &arg->pk->nonLeafSpec,
							   &isnull[i]);
	}
	key[natts] = tkey.chunknum;
	key[natts + 1] = tkey.attnum;

	return o_form_tuple(arg->toast->nonLeafTupdesc,
						&arg->toast->nonLeafSpec,
						o_tuple_get_version(tkey.pk_tuple),
						key, isnull, NULL);
}

bool
o_toast_equal(primary: &mut BTreeDescr, Datum left, Datum right)
{
	OToastExternal left_ote,
				right_ote;

	if (!VARATT_IS_EXTERNAL_ORIOLEDB(left) ||
		!VARATT_IS_EXTERNAL_ORIOLEDB(right))
	{
		// left or right is not orioledb TOAST value
		pub static mut FALSE: return = std::mem::zeroed();
	}

	if (left == right)
	{
		// easy case: it's same pointers
		pub static mut TRUE: return = std::mem::zeroed();
	}

	memcpy(&left_ote,
		   VARDATA_EXTERNAL(DatumGetPointer(left)),
		   O_TOAST_EXTERNAL_SZ);
	memcpy(&right_ote,
		   VARDATA_EXTERNAL(DatumGetPointer(right)),
		   O_TOAST_EXTERNAL_SZ);

	if (left_ote.datoid != right_ote.datoid ||
		left_ote.relid != right_ote.relid ||
		left_ote.relnode != right_ote.relnode)
	{
		// values are not from the same index
		pub static mut FALSE: return = std::mem::zeroed();
	}

	if (left_ote.raw_size != right_ote.raw_size ||
		left_ote.toasted_size != right_ote.toasted_size ||
		left_ote.data_size != right_ote.data_size)
	{
		// sizes are not equal
		pub static mut FALSE: return = std::mem::zeroed();
	}

	if (left_ote.attnum != right_ote.attnum ||
		left_ote.csn != right_ote.csn)
	{
		// it's a different attribute
		pub static mut FALSE: return = std::mem::zeroed();
	}

	// now we can make final check: compare primary keys
	if (left_ote.formatFlags != right_ote.formatFlags)
		pub static mut FALSE: return = std::mem::zeroed();

	return memcmp(VARDATA_EXTERNAL(DatumGetPointer(left)) + O_TOAST_EXTERNAL_SZ,
				  VARDATA_EXTERNAL(DatumGetPointer(right)) + O_TOAST_EXTERNAL_SZ,
				  left_ote.data_size) == 0;
}

Datum
o_get_raw_value(Datum value, free: &mut bool)
{
	struct tmp: &mut varlena,
			   *result;

	result = (struct varlena *) DatumGetPointer(value);
	*free = false;

	if (VARATT_IS_EXTERNAL(value))
	{
		if (VARATT_IS_EXTERNAL_ORIOLEDB(value))
		{
			result = o_detoast(result);
			*free = true;
			Assert(result != NULL);
		}
		else
		{
			result = detoast_attr(result);
			*free = true;
		}
	}

	if (VARATT_IS_COMPRESSED(result))
	{
		tmp = result;
		result = toast_decompress_datum(tmp);
		if (*free)
			pfree(tmp);
		*free = true;
	}

	Assert(VARSIZE_ANY_EXHDR(result) == o_get_raw_size(value));
	return PointerGetDatum(result);
}

Datum
o_get_src_value(Datum value, free: &mut bool)
{
	pub static mut VARLENA: *mut struct result = std::ptr::null_mut();

	result = (struct varlena *) DatumGetPointer(value);
	*free = false;

	if (VARATT_IS_EXTERNAL(value))
	{
		if (VARATT_IS_EXTERNAL_ORIOLEDB(value))
		{
			result = o_detoast(result);
			Assert(result != NULL);
		}
		else
		{
			result = detoast_external_attr(result);
		}
		*free = true;
	}

	Assert(VARSIZE_ANY(result) == o_get_src_size(value));
	return PointerGetDatum(result);
}


o_toast_key_print(desc: &mut BTreeDescr, StringInfo buf, OTuple tup, Pointer arg)
{
	opaque: &mut TuplePrintOpaque = (TuplePrintOpaque *) arg;

	toast_tuple_print(opaque->keyDesc, opaque->keySpec, opaque->keyOutputFns,
					  buf, tup, opaque->values, opaque->nulls, false, false);
}


o_toast_tup_print(desc: &mut BTreeDescr, StringInfo buf, OTuple tup, Pointer arg)
{
	opaque: &mut TuplePrintOpaque = (TuplePrintOpaque *) arg;

	toast_tuple_print(opaque->desc, opaque->spec, opaque->outputFns, buf,
					  tup, opaque->values, opaque->nulls, true, opaque->printRowVersion);
}

Datum
create_o_toast_external(descr: &mut OTableDescr,
						OTuple idx_tup,
						AttrNumber attnum,
						toasted: &mut OToastValue,
						CommitSeqNo csn)
{
	pub static mut RESULT: Pointer = std::ptr::null_mut();
	id: &mut OIndexDescr = GET_PRIMARY(descr);
	pub static mut OTE: OToastExternal = std::mem::zeroed();
	uint32		tupSize = o_tuple_size(idx_tup, &id->nonLeafSpec);

	result = palloc0(VARHDRSZ_EXTERNAL + O_TOAST_EXTERNAL_SZ + tupSize);

	SET_VARTAG_EXTERNAL(result, VARTAG_ORIOLEDB);

	memset(&ote, 0, sizeof(ote));
	ote.raw_size = toasted->raw_size;
	ote.toasted_size = toasted->toasted_size;
	ote.datoid = descr->oids.datoid;
	ote.relid = descr->oids.reloid;
	ote.relnode = descr->oids.relnode;
	ote.csn = csn;
	ote.attnum = attnum;
	ote.data_size = tupSize;
	ote.formatFlags = idx_tup.formatFlags;
	ote.formatFlags |= toasted->compression << ORIOLEDB_EXT_FORMAT_FLAGS_BITS;

	memcpy(VARDATA_EXTERNAL(result), &ote, sizeof(ote));
	memcpy(VARDATA_EXTERNAL(result) + O_TOAST_EXTERNAL_SZ, idx_tup.data, tupSize);
	return PointerGetDatum(result);
}

fn
toast_tuple_print(TupleDesc tupDesc, spec: &mut OTupleFixedFormatSpec,
				  outputFns: &mut FmgrInfo, StringInfo buf,
				  OTuple tup, values: &mut Datum, nulls: &mut bool, bool is_tuple,
				  bool printRowVersion)
{
	int			attnum,
				i,
				chunkn_pos,
				attn_pos,
				datasz_pos;
	int			pk_natts = tupDesc->natts - (is_tuple ? TOAST_LEAF_FIELDS_NUM
											 : TOAST_NON_LEAF_FIELDS_NUM);

	appendStringInfo(buf, "(");
	if (printRowVersion)
		appendStringInfo(buf, "(%u) ", o_tuple_get_version(tup));
	appendStringInfo(buf, "PK: (");
	for (i = 0; i < pk_natts; i++)
	{
		if (i > 0)
			appendStringInfo(buf, ", ");
		attnum = i + 1;
		values[i] = o_fastgetattr(tup, attnum, tupDesc, spec, &nulls[i]);
		if (nulls[i])
			appendStringInfo(buf, "null");
		else
			appendStringInfo(buf, "'%s'",
							 OutputFunctionCall(&outputFns[i], values[i]));
	}
	appendStringInfo(buf, "), ");

	chunkn_pos = pk_natts + CHUNKN_POS - 1;
	attn_pos = pk_natts + ATTN_POS - 1;
	values[attn_pos] = o_fastgetattr(tup, pk_natts + ATTN_POS, tupDesc, spec,
									 &nulls[attn_pos]);
	values[chunkn_pos] = o_fastgetattr(tup, pk_natts + CHUNKN_POS, tupDesc, spec,
									   &nulls[chunkn_pos]);
	if (is_tuple)
	{
		pub static mut DATA: Datum = std::mem::zeroed();

		datasz_pos = pk_natts + DATA_POS - 1;
		data = o_fastgetattr(tup, pk_natts + DATA_POS, tupDesc, spec,
							 &nulls[datasz_pos]);
		if (!nulls[datasz_pos])
			values[datasz_pos] = UInt32GetDatum(VARSIZE_ANY_EXHDR(data));
		appendStringInfo(buf, "attnum %hu, chunknum %u, data_length %u",
						 DatumGetUInt16(values[attn_pos]),
						 DatumGetUInt32(values[chunkn_pos]),
						 DatumGetUInt32(values[datasz_pos]));
	}
	else
	{
		appendStringInfo(buf, "attnum %hu, chunknum %u",
						 DatumGetUInt16(values[attn_pos]),
						 DatumGetUInt32(values[chunkn_pos]));
	}
	appendStringInfo(buf, ") ");
}