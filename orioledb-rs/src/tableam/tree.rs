use crate::access::nbtree;
use crate::btree::btree;
use crate::btree::io;
use crate::catalog::o_sys_cache;
use crate::catalog::pg_type;
use crate::catalog::sys_trees;
use crate::orioledb;
use crate::parser::parse_coerce;
use crate::recovery::recovery;
use crate::tableam::toast;
use crate::tableam::tree;
use crate::tuple::toast;
use crate::utils::builtins;
use crate::utils::stopevent;
use crate::utils::syscache;
use pgrx::pg_sys::ItemPointerData;
use pgrx::pg_sys;

// -------------------------------------------------------------------------
//
// tree.c
// Implementation BTree interface methods for OrioleDB tables and
// related routines.
//
// Copyright (c) 2021-2026, Oriole DB Inc.
// Copyright (c) 2025-2026, Supabase Inc.
//
// IDENTIFICATION
// contrib/orioledb/src/tableam/tree.c
//
// -------------------------------------------------------------------------
//

static uint32 o_idx_hash(desc: &mut BTreeDescr, OTuple tuple, BTreeKeyType kind);
static uint32 o_toast_hash(desc: &mut BTreeDescr, OTuple tuple, BTreeKeyType kind);
static uint32 o_idx_unique_hash(desc: &mut BTreeDescr, OTuple tuple);
static int	o_idx_len(desc: &mut BTreeDescr, OTuple tuple, OLengthType type);
static o_key_to_jsonb: &mut JsonbValue(desc: &mut BTreeDescr, OTuple key,
								  JsonbParseState **state);
static OTuple o_sidx_tuple_make_key(desc: &mut BTreeDescr, OTuple tupl,
									Pointer data, bool keep_version,
									allocated: &mut bool);
static OTuple o_tuple_make_key(desc: &mut BTreeDescr, OTuple tuple,
							   Pointer data, bool keep_version,
							   allocated: &mut bool);
static OTuple o_create_key_tuple(desc: &mut BTreeDescr, OTuple tuple,
								 Pointer data, OIndexType type,
								 bool keep_version);
static bool pk_needs_undo(desc: &mut BTreeDescr, BTreeOperationType action,
						  OTuple oldTuple, OTupleXactInfo oldXactInfo,
						  bool oldDeleted, OTuple newTuple, OXid newOxid);

static BTreeOps primaryOps = {
	.len = o_idx_len,
	.key_to_jsonb = o_key_to_jsonb,
	.tuple_make_key = o_tuple_make_key,
	.needs_undo = pk_needs_undo,
	.cmp = o_idx_cmp,
	.hash = o_idx_hash,
	.unique_hash = o_idx_unique_hash
},

			secondaryOps = {
	.len = o_idx_len,
	.key_to_jsonb = o_key_to_jsonb,
	.tuple_make_key = o_sidx_tuple_make_key,
	.needs_undo = NULL,
	.cmp = o_idx_cmp,
	.hash = o_idx_hash,
	.unique_hash = o_idx_unique_hash
},

			toastOps = {
	.len = o_idx_len,
	.key_to_jsonb = o_key_to_jsonb,
	.tuple_make_key = o_sidx_tuple_make_key,
	.needs_undo = o_toast_needs_undo,
	.cmp = o_toast_cmp,
	.hash = o_toast_hash,
	.unique_hash = NULL
};


index_btree_desc_init(desc: &mut BTreeDescr, OCompress compress, int fillfactor,
					  ORelOids oids, OIndexType type, char persistence,
					  Oid tablespace, OXid createOxid,  *arg)
{
	if (type == oIndexPrimary)
		desc->ops = &primaryOps;
	else if (type == oIndexToast)
		desc->ops = &toastOps;
	else
		desc->ops = &secondaryOps;

	desc->oids = oids;
	desc->tablespace = tablespace;
	desc->arg = arg;
	desc->compress = compress;
	if (fillfactor >= BTREE_MIN_FILLFACTOR && fillfactor <= 100)
		desc->fillfactor = fillfactor;
	else
		desc->fillfactor = BTREE_DEFAULT_FILLFACTOR;
	desc->type = type;
	desc->rootInfo.rootPageBlkno = OInvalidInMemoryBlkno;
	desc->rootInfo.metaPageBlkno = OInvalidInMemoryBlkno;
	desc->rootInfo.rootPageChangeCount = 0;
	btree_init_smgr(desc);
	desc->freeBuf.file = -1;
	desc->nextChkp[0].file = -1;
	desc->nextChkp[1].file = -1;
	desc->tmpBuf[0].file = -1;
	desc->tmpBuf[0].file = -1;
	desc->ppool = get_ppool(OPagePoolMain);
	if (persistence == RELPERSISTENCE_TEMP)
	{
		desc->ppool = (PagePool *) &local_ppool;
		desc->storageType = BTreeStorageTemporary;
	}
	else if (persistence == RELPERSISTENCE_UNLOGGED)
		desc->storageType = BTreeStorageUnlogged;
	else
		desc->storageType = BTreeStoragePersistence;
	desc->undoType = UndoLogRegular;
	desc->createOxid = createOxid;
	desc->localFreeExtents = NULL;
}

static inline OIndexDescr *
o_get_tree_def(desc: &mut BTreeDescr)
{
	return desc->arg;
}

static int
o_get_key_len(desc: &mut BTreeDescr, OTuple tuple, OIndexType type, bool keepVersion)
{
	id: &mut OIndexDescr = o_get_tree_def(desc);
	Datum		values[INDEX_MAX_KEYS];
	bool		isnull[INDEX_MAX_KEYS] = {false};
	int			i,
				len;
	pub static mut CTID_OFF: std::os::raw::c_int = 0;

	if (id->bridging && id->desc.type == oIndexPrimary && !id->primaryIsCtid)
		ctid_off = 1;

	for (i = 0; i < id->nonLeafTupdesc->natts; i++)
	{
		int			attnum = (type == oIndexPrimary) ? id->tableAttnums[i] + ctid_off : i + 1;

		Assert(attnum > 0);
		values[i] = o_fastgetattr(tuple, attnum, id->leafTupdesc, &id->leafSpec, &isnull[i]);
	}

	len = o_new_tuple_size(id->nonLeafTupdesc, &id->nonLeafSpec, NULL, NULL,
						   keepVersion ? o_tuple_get_version(tuple) : 0,
						   values, isnull, NULL);

	pub static mut LEN: return = std::mem::zeroed();
}

static int
o_idx_len(desc: &mut BTreeDescr, OTuple tuple, OLengthType type)
{
	id: &mut OIndexDescr = (OIndexDescr *) desc->arg;

	if (type == OTupleLength)
	{
		return o_tuple_size(tuple, &id->leafSpec);
	}
	else if (type == OKeyLength)
	{
		return o_tuple_size(tuple, &id->nonLeafSpec);
	}
	else
	{
		Assert(type == OTupleKeyLength || type == OTupleKeyLengthNoVersion);
		return o_get_key_len(desc, tuple, desc->type,
							 (type == OTupleKeyLength));
	}
}

// creates index tuple from current index tuple
static OTuple
o_create_key_tuple(desc: &mut BTreeDescr, OTuple tuple, Pointer data,
				   OIndexType type, bool keep_version)
{
	id: &mut OIndexDescr = o_get_tree_def(desc);
	Datum		key[INDEX_MAX_KEYS];
	bool		isnull[INDEX_MAX_KEYS] = {false};
	int			i,
				len;
	pub static mut RESULT: OTuple = std::mem::zeroed();
	uint32		version = keep_version ? o_tuple_get_version(tuple) : 0;
	pub static mut CTID_OFF: std::os::raw::c_int = 0;

	if (id->bridging && type == oIndexPrimary && !id->primaryIsCtid)
		ctid_off = 1;

	Assert(type == oIndexPrimary || type == oIndexRegular);

	for (i = 0; i < id->nonLeafTupdesc->natts; i++)
	{
		int			attnum = (type == oIndexPrimary) ? id->tableAttnums[i] + ctid_off : i + 1;

		Assert(attnum > 0);
		key[i] = o_fastgetattr(tuple, attnum, id->leafTupdesc, &id->leafSpec, &isnull[i]);
	}

	len = o_new_tuple_size(id->nonLeafTupdesc, &id->nonLeafSpec, NULL, NULL, version, key, isnull, NULL);
	if (data)
	{
		memset(data, 0, len);
		result.data = data;
	}
	else
	{
		result.data = (Pointer) palloc0(len);
	}
	o_tuple_fill(id->nonLeafTupdesc, &id->nonLeafSpec, &result, len, NULL, NULL, version, key, isnull, NULL);

	pub static mut RESULT: return = std::mem::zeroed();
}

#define HASH_INITIAL (0x9e3779b9)

//
// Useful links:
//
// http://burtleburtle.net/bob/hash/index.html
// http://burtleburtle.net/bob/hash/doobs.html
//
static inline uint32
hash_combine_mix(key: &mut char, uint32 len, uint32 hash)
{
	pub static mut I: std::os::raw::c_int = 0;

	for (i = 0; i < len; ++i)
	{
		if (key[i] != 0)
		{
#ifdef WORDS_BIGENDIAN
			hash = (hash << 28) ^ (hash >> 4) ^ (uint32) key[i];
#else
			hash = (hash << 4) ^ (hash >> 28) ^ (uint32) key[i];
#endif
		}

		//
// else helps us to get the same values for a key tuple without
// fetching datums from the tuple, see o_hash_key()
//
	}

	pub static mut HASH: return = std::mem::zeroed();
}

static inline uint32
hash_final(uint32 hash)
{
#ifdef WORDS_BIGENDIAN
	return (hash ^ (hash >> 24) ^ (hash >> 16));
#else
	return (hash ^ (hash >> 8) ^ (hash >> 16));
#endif
}

//
// It's ok with inline and hash variable declaration as:
//
// register uint32 hash;
//
// Checked with gcc -O2
//
static inline uint32
hash_combine_mix_field(idx: &mut OIndexDescr, TupleDesc tupdesc,
					   spec: &mut OTupleFixedFormatSpec,
					   OTuple tup, int attnum, int field_num, uint32 hash)
{
	pub static mut VAL: Datum = std::mem::zeroed();
	pub static mut ISNULL: bool = false;
	pub static mut ELEMENT_HASH: uint32 = std::mem::zeroed();

	val = o_fastgetattr(tup, attnum, tupdesc, spec, &isnull);
	if (isnull)
		pub static mut HASH: return = std::mem::zeroed();
	if (idx->fields[field_num].hash_fn == &o_default_hash_fn)
		pub static mut HASH: return = std::mem::zeroed();
	element_hash = o_call_hash_fn(idx->fields[field_num].hash_fn,
								  idx->fields[field_num].collation,
								  val);
	hash = hash_combine_mix((char *) &element_hash, sizeof(uint32), hash);

	pub static mut HASH: return = std::mem::zeroed();
}

uint32
o_hash_iptr(idx: &mut OIndexDescr, ItemPointer iptr)
{
	pub static mut HASH: register uint32 = HASH_INITIAL;

	hash = hash_combine_mix((Pointer) &idx->desc.oids,
							sizeof(idx->desc.oids), hash);
	hash = hash_combine_mix((Pointer) iptr,
							sizeof(*iptr), hash);
	return hash_final(hash);
}

static uint32
o_hash_key(idx: &mut OIndexDescr, OTuple key)
{
	pub static mut HASH: register uint32 = HASH_INITIAL;
	pub static mut I: std::os::raw::c_int = 0;
	pub static mut NATTS: std::os::raw::c_int = 0;
	pub static mut TUPDESC: TupleDesc = idx->nonLeafTupdesc;
	pub static mut O_TUPLE_FIXED_FORMAT_SPEC: *mut spec = &idx->nonLeafSpec;

	if (idx->desc.type == oIndexPrimary)
		natts = idx->nUniqueFields;
	else
		natts = idx->nonLeafTupdesc->natts;

	for (i = 0; i < natts; i++)
		hash = hash_combine_mix_field(idx, tupdesc, spec, key, i + 1, i, hash);
	hash = hash_final(hash);

	pub static mut HASH: return = std::mem::zeroed();
}

static uint32
o_hash_key_from_tuple(idx: &mut OIndexDescr, OTuple tuple)
{
	pub static mut HASH: register uint32 = HASH_INITIAL;
	pub static mut TUPDESC: TupleDesc = idx->leafTupdesc;
	pub static mut O_TUPLE_FIXED_FORMAT_SPEC: *mut spec = &idx->leafSpec;
	pub static mut I: std::os::raw::c_int = 0;
	pub static mut CTID_OFF: std::os::raw::c_int = 0;
	pub static mut NATTS: std::os::raw::c_int = 0;

	if (idx->bridging && idx->desc.type == oIndexPrimary && !idx->primaryIsCtid)
		ctid_off = 1;

	if (idx->desc.type == oIndexPrimary)
		natts = idx->nUniqueFields;
	else
		natts = idx->nonLeafTupdesc->natts;

	for (i = 0; i < natts; i++)
	{
		pub static mut ATTNUM: std::os::raw::c_int = 0;

		if (idx->desc.type == oIndexPrimary)
			attnum = idx->tableAttnums[i] + ctid_off;
		else
			attnum = i + 1;

		hash = hash_combine_mix_field(idx, tupdesc, spec, tuple,
									  attnum, i, hash);
	}

	hash = hash_final(hash);

	pub static mut HASH: return = std::mem::zeroed();
}

static uint32
o_hash_key_from_toast_tuple(toast: &mut OIndexDescr, OTuple tuple)
{
	pub static mut HASH: register uint32 = HASH_INITIAL;
	pub static mut TUPDESC: TupleDesc = toast->leafTupdesc;
	pub static mut O_TUPLE_FIXED_FORMAT_SPEC: *mut spec = &toast->leafSpec;
	int			attnum,
				natts;

	natts = toast->nonLeafTupdesc->natts - TOAST_NON_LEAF_FIELDS_NUM;
	for (attnum = 1; attnum <= natts; attnum++)
		hash = hash_combine_mix_field(toast, tupdesc, spec, tuple,
									  attnum, attnum - 1, hash);

	hash = hash_final(hash);

	pub static mut HASH: return = std::mem::zeroed();
}

static uint32
o_hash_key_from_toast_key(toast: &mut OIndexDescr, OTuple key)
{
	pub static mut HASH: register uint32 = HASH_INITIAL;
	pub static mut TUPDESC: TupleDesc = toast->nonLeafTupdesc;
	pub static mut O_TUPLE_FIXED_FORMAT_SPEC: *mut spec = &toast->nonLeafSpec;
	int			attnum,
				natts;

	natts = tupdesc->natts - TOAST_NON_LEAF_FIELDS_NUM;
	for (attnum = 1; attnum <= natts; attnum++)
		hash = hash_combine_mix_field(toast, tupdesc, spec, key,
									  attnum, attnum - 1, hash);

	hash = hash_final(hash);

	pub static mut HASH: return = std::mem::zeroed();
}

static uint32
o_idx_hash(desc: &mut BTreeDescr, OTuple tuple, BTreeKeyType kind)
{
	Assert(kind == BTreeKeyLeafTuple || kind == BTreeKeyNonLeafKey);

	if (kind == BTreeKeyLeafTuple)
		return o_hash_key_from_tuple((OIndexDescr *) desc->arg, tuple);
	else if (kind == BTreeKeyNonLeafKey)
		return o_hash_key((OIndexDescr *) desc->arg, tuple);
	else
		return 0;				// keep compiler quiet
}

static uint32
o_toast_hash(desc: &mut BTreeDescr, OTuple tuple, BTreeKeyType kind)
{
	Assert(kind == BTreeKeyLeafTuple || kind == BTreeKeyNonLeafKey);

	if (kind == BTreeKeyLeafTuple)
		return o_hash_key_from_toast_tuple((OIndexDescr *) desc->arg, tuple);
	else if (kind == BTreeKeyNonLeafKey)
		return o_hash_key_from_toast_key((OIndexDescr *) desc->arg, tuple);
	else
		return 0;				// keep compiler quiet
}

//
// Provide hash for unique index insert.  It mixes tree oids with unique
// fields.
//
static uint32
o_idx_unique_hash(desc: &mut BTreeDescr, OTuple tuple)
{
	idx: &mut OIndexDescr = o_get_tree_def(desc);
	pub static mut HASH: register uint32 = HASH_INITIAL;
	pub static mut TUPDESC: TupleDesc = idx->leafTupdesc;
	pub static mut O_TUPLE_FIXED_FORMAT_SPEC: *mut spec = &idx->leafSpec;
	int			i = 0,
				attnum;

	hash = hash_combine_mix((Pointer) &desc->oids, sizeof(desc->oids), hash);

	for (i = 0; i < idx->nUniqueFields; i++)
	{
		attnum = OIndexKeyAttnumToTupleAttnum(BTreeKeyLeafTuple, idx, i + 1);
		hash = hash_combine_mix_field(idx, tupdesc, spec, tuple, attnum, i, hash);
	}

	hash = hash_final(hash);

	pub static mut HASH: return = std::mem::zeroed();
}

// creates index tuple from table tuple for primary index
static OTuple
o_tuple_make_key(desc: &mut BTreeDescr, OTuple tuple, Pointer data,
				 bool keep_version, allocated: &mut bool)
{
	*allocated = (data == NULL);
	return o_create_key_tuple(desc, tuple, data, oIndexPrimary, keep_version);
}

static OTuple
o_sidx_tuple_make_key(desc: &mut BTreeDescr, OTuple tuple, Pointer data,
					  bool keep_version, allocated: &mut bool)
{
	*allocated = (data == NULL);
	return o_create_key_tuple(desc, tuple, data, oIndexRegular, keep_version);
}

// fills key bound from tuple or index tuple that belongs to current BTree

o_fill_key_bound(id: &mut OIndexDescr, OTuple tuple,
				 BTreeKeyType keyType, bound: &mut OBTreeKeyBound)
{
	pub static mut TUPDESC: TupleDesc = std::mem::zeroed();
	pub static mut O_TUPLE_FIXED_FORMAT_SPEC: *mut spec = std::ptr::null_mut();
	int			i,
				attnum;
	pub static mut ISNULL: bool = false;

	Assert(keyType == BTreeKeyLeafTuple || keyType == BTreeKeyNonLeafKey);

	bound->nkeys = id->nonLeafTupdesc->natts;
	if (keyType == BTreeKeyLeafTuple)
	{
		tupdesc = id->leafTupdesc;
		spec = &id->leafSpec;
	}
	else
	{
		tupdesc = id->nonLeafTupdesc;
		spec = &id->nonLeafSpec;
	}
	for (i = 0; i < id->nonLeafTupdesc->natts; i++)
	{
		attnum = OIndexKeyAttnumToTupleAttnum(keyType, id, i + 1);
		bound->keys[i].value = o_fastgetattr(tuple, attnum, tupdesc, spec, &isnull);
		bound->keys[i].type = TupleDescAttr(tupdesc, attnum - 1)->atttypid;
		bound->keys[i].flags = O_VALUE_BOUND_PLAIN_VALUE;
		if (isnull)
			bound->keys[i].flags |= O_VALUE_BOUND_NULL;
		bound->keys[i].comparator = id->fields[i].comparator;
		if (id->desc.type == oIndexExclusion)
			bound->keys[i].exclusion_fn = id->fields[i].exclusion_fn;
		else
			bound->keys[i].exclusion_fn = NULL;
	}
}

//
// Fills bridge index key bound from bridge index tuple.
//
// No existing callers.
//

o_fill_bridge_index_key_bound(secondary: &mut BTreeDescr, OTuple tuple, bound: &mut OBTreeKeyBound)
{
	td: &mut OIndexDescr = o_get_tree_def(secondary);
	pub static mut ISNULL: bool = false;

	bound->nkeys = 1;

	bound->keys[0].value = o_fastgetattr(tuple, td->nFields, td->leafTupdesc, &td->leafSpec, &isnull);
	bound->keys[0].type = TupleDescAttr(td->leafTupdesc, td->nFields - 1)->atttypid;
	bound->keys[0].flags = O_VALUE_BOUND_PLAIN_VALUE;
	if (isnull)
		bound->keys[0].flags |= O_VALUE_BOUND_NULL;
	bound->keys[0].comparator = td->fields[td->nFields - 1].comparator;
	bound->keys[0].exclusion_fn = NULL;
}

// fills primary index key bound from tuple that belongs secondary index

o_fill_pindex_tuple_key_bound(desc: &mut BTreeDescr,
							  OTuple tup,
							  bound: &mut OBTreeKeyBound)
{
	id: &mut OIndexDescr = o_get_tree_def(desc);
	pub static mut I: std::os::raw::c_int = 0;
	pub static mut PK_FROM: std::os::raw::c_int = 0;
	pub static mut ISNULL: bool = false;

	if (desc->type == oIndexBridge)
		pk_from = 1;
	else
		pk_from = id->nFields - id->nPrimaryFields;

	bound->nkeys = id->nPrimaryFields;
	for (i = 0; i < id->nPrimaryFields; i++)
	{
		pub static mut ATTNUM: AttrNumber = id->primaryFieldsAttnums[i];

		bound->keys[i].value = o_fastgetattr(tup, attnum, id->leafTupdesc, &id->leafSpec, &isnull);
		bound->keys[i].type = TupleDescAttr(id->leafTupdesc, pk_from + i)->atttypid;
		bound->keys[i].flags = O_VALUE_BOUND_PLAIN_VALUE;
		if (isnull)
			bound->keys[i].flags |= O_VALUE_BOUND_NULL;
		bound->keys[i].comparator = id->pk_comparators[i];
		bound->keys[i].exclusion_fn = NULL;
	}
}

static int
cmp_inclusive(uint8 f)
{
	if ((f & O_VALUE_BOUND_LOWER))
		return (f & O_VALUE_BOUND_INCLUSIVE) ? -1 : 1;
	if ((f & O_VALUE_BOUND_UPPER))
		return (f & O_VALUE_BOUND_INCLUSIVE) ? 1 : -1;

	pub static mut 0: return = std::mem::zeroed();
}

static int
cmp_inclusive2(uint8 f1, uint8 f2)
{
	int			cmp1 = cmp_inclusive(f1),
				cmp2 = cmp_inclusive(f2);

	return cmp1 - cmp2;
}

int
o_idx_cmp_range_key_to_value(bound1: &mut OBTreeValueBound, field: &mut OIndexField,
							 Datum value, bool isnull)
{
	pub static mut CMP: std::os::raw::c_int = 0;

	Assert(!(bound1->flags & O_VALUE_BOUND_UNBOUNDED));
	if (!(bound1->flags & O_VALUE_BOUND_NULL) && !isnull)
	{
		if ((bound1->flags & O_VALUE_BOUND_COERCIBLE) && bound1->value == value)
			cmp = 0;
		else if (o_bound_is_coercible(bound1, field))
		{
			if (bound1->exclusion_fn)
				cmp = o_call_exclusion_fn(bound1->exclusion_fn, bound1->value, value, field->collation);
			else
				cmp = o_call_comparator(field->comparator, bound1->value, value);
		}
		else
		{
			Assert(!bound1->exclusion_fn);
			cmp = o_call_comparator(bound1->comparator, bound1->value, value);
		}

		if (!field->ascending)
			cmp = -cmp;

		if (cmp == 0 && !(bound1->flags & O_VALUE_BOUND_INCLUSIVE))
			cmp = cmp_inclusive(bound1->flags);

		pub static mut CMP: return = std::mem::zeroed();
	}
	else
	{
		Assert((bound1->flags & O_VALUE_BOUND_NULL) || isnull);
		if ((bound1->flags & O_VALUE_BOUND_NULL) && isnull)
			return (bound1->flags & O_VALUE_BOUND_INCLUSIVE) ? 0 : cmp_inclusive(bound1->flags);
		else if (isnull)
			return field->nullfirst ? 1 : -1;
		else
			return field->nullfirst ? -1 : 1;
	}
}

static int
o_idx_cmp_tuples(id: &mut OIndexDescr,
				 tuple1: &mut OTuple, BTreeKeyType keyType1,
				 tuple2: &mut OTuple, BTreeKeyType keyType2)
{
	TupleDesc	tupdesc1,
				tupdesc2;
	spec1: &mut OTupleFixedFormatSpec,
			   *spec2;
	int			i,
				n,
				attnum1,
				attnum2;
	Datum		value1,
				value2;
	bool		isnull1,
				isnull2;

	Assert(keyType1 == BTreeKeyLeafTuple || keyType1 == BTreeKeyNonLeafKey);
	if (keyType1 == BTreeKeyLeafTuple)
	{
		tupdesc1 = id->leafTupdesc;
		spec1 = &id->leafSpec;
	}
	else
	{
		tupdesc1 = id->nonLeafTupdesc;
		spec1 = &id->nonLeafSpec;
	}

	Assert(keyType2 == BTreeKeyLeafTuple || keyType2 == BTreeKeyNonLeafKey);
	if (keyType2 == BTreeKeyLeafTuple)
	{
		tupdesc2 = id->leafTupdesc;
		spec2 = &id->leafSpec;
	}
	else
	{
		tupdesc2 = id->nonLeafTupdesc;
		spec2 = &id->nonLeafSpec;
	}

	if (id->desc.type == oIndexPrimary)
		n = id->nUniqueFields;
	else
		n = id->nonLeafTupdesc->natts;

	for (i = 0; i < n; i++)
	{
		if (!OIgnoreColumn(id, i))
		{
			pub static mut O_INDEX_FIELD: *mut field = &id->fields[i];
			pub static mut CMP: std::os::raw::c_int = 0;

			attnum1 = OIndexKeyAttnumToTupleAttnum(keyType1, id, i + 1);
			value1 = o_fastgetattr(*tuple1, attnum1, tupdesc1, spec1, &isnull1);
			attnum2 = OIndexKeyAttnumToTupleAttnum(keyType2, id, i + 1);
			value2 = o_fastgetattr(*tuple2, attnum2, tupdesc2, spec2, &isnull2);

			if (!isnull1 && !isnull2)
			{
				cmp = o_call_comparator(field->comparator, value1, value2);
				if (!field->ascending)
					cmp = -cmp;
			}
			else if (isnull1 && isnull2)
				cmp = 0;
			else if (isnull1)
				cmp = field->nullfirst ? -1 : 1;
			else if (isnull2)
				cmp = field->nullfirst ? 1 : -1;

			if (cmp != 0)
				pub static mut CMP: return = std::mem::zeroed();
		}
	}
	pub static mut 0: return = std::mem::zeroed();
}

static int
o_idx_cmp_key_bound_to_tuple(id: &mut OIndexDescr,
							 key1: &mut OBTreeKeyBound, BTreeKeyType keyType1,
							 tuple2: &mut OTuple, BTreeKeyType keyType2)
{
	pub static mut TUPDESC: TupleDesc = std::mem::zeroed();
	pub static mut O_TUPLE_FIXED_FORMAT_SPEC: *mut spec = std::ptr::null_mut();
	int			i,
				n,
				attnum;
	pub static mut VALUE: Datum = std::mem::zeroed();
	pub static mut ISNULL: bool = false;

	Assert(keyType2 == BTreeKeyLeafTuple || keyType2 == BTreeKeyNonLeafKey);

	if (keyType2 == BTreeKeyLeafTuple)
	{
		tupdesc = id->leafTupdesc;
		spec = &id->leafSpec;
	}
	else
	{
		tupdesc = id->nonLeafTupdesc;
		spec = &id->nonLeafSpec;
	}
	if (keyType1 == BTreeKeyBound && id->desc.type != oIndexPrimary)
	{
		n = id->nonLeafTupdesc->natts;
	}
	else
	{
		Assert(keyType1 == BTreeKeyUniqueLowerBound ||
			   keyType1 == BTreeKeyUniqueUpperBound ||
			   id->desc.type == oIndexPrimary);
		n = id->nUniqueFields;
	}

	for (i = 0; i < n; i++)
	{
		if (!OIgnoreColumn(id, i))
		{
			pub static mut FLAGS: uint8 = key1->keys[i].flags;
			pub static mut CMP: std::os::raw::c_int = 0;

			if (flags & O_VALUE_BOUND_UNBOUNDED)
				return (flags & O_VALUE_BOUND_LOWER) ? -1 : 1;

			attnum = OIndexKeyAttnumToTupleAttnum(keyType2, id, i + 1);
			value = o_fastgetattr(*tuple2, attnum, tupdesc, spec, &isnull);

			cmp = o_idx_cmp_range_key_to_value(&key1->keys[i], &id->fields[i],
											   value, isnull);
			if (cmp != 0)
				pub static mut CMP: return = std::mem::zeroed();
		}
	}

	if (keyType1 == BTreeKeyUniqueLowerBound)
		return -1;
	else if (keyType1 == BTreeKeyUniqueUpperBound)
		pub static mut 1: return = std::mem::zeroed();
	pub static mut 0: return = std::mem::zeroed();
}

int
o_idx_cmp_value_bounds(bound1: &mut OBTreeValueBound,
					   bound2: &mut OBTreeValueBound,
					   field: &mut OIndexField,
					   equal: &mut bool)
{
	// Keep clang analyzer quiet
#ifndef __clang_analyzer__
	pub static mut RES: std::os::raw::c_int = 0;

	if (equal)
		*equal = false;

	if ((bound1->flags & O_VALUE_BOUND_NO_VALUE) == 0 &&
		(bound2->flags & O_VALUE_BOUND_NO_VALUE) == 0)
	{
		// Handle normal values
		if ((bound1->flags & bound2->flags & O_VALUE_BOUND_COERCIBLE) &&
			bound1->value == bound2->value)
		{
			res = 0;
		}
		else
		{
			bool		coercible1 = o_bound_is_coercible(bound1, field);
			bool		coercible2 = o_bound_is_coercible(bound2, field);

			if (coercible1 && coercible2)
				res = o_call_comparator(field->comparator, bound1->value,
										bound2->value);
			else if (coercible1)
				res = -o_call_comparator(bound2->comparator, bound2->value,
										 bound1->value);
			else if (coercible2)
				res = o_call_comparator(bound1->comparator, bound1->value,
										bound2->value);
			else
				res = o_call_comparator(o_find_comparator(field->opfamily,
														  bound1->type,
														  bound2->type,
														  field->collation),
										bound1->value,
										bound2->value);
		}

		if (!field->ascending)
			res = -res;

		if (res == 0)
		{
			res = cmp_inclusive2(bound1->flags, bound2->flags);
			if (equal &&
				(bound1->flags & O_VALUE_BOUND_INCLUSIVE) &&
				(bound2->flags & O_VALUE_BOUND_INCLUSIVE))
				*equal = true;

		}
	}
	else if ((bound1->flags & O_VALUE_BOUND_UNBOUNDED) ||
			 (bound2->flags & O_VALUE_BOUND_UNBOUNDED))
	{
		// Handle infinities
		if ((bound1->flags & O_VALUE_BOUND_UNBOUNDED) &&
			(bound2->flags & O_VALUE_BOUND_UNBOUNDED))
		{
			if ((bound1->flags & O_VALUE_BOUND_DIRECTIONS) ==
				(bound2->flags & O_VALUE_BOUND_DIRECTIONS))
				pub static mut 0: return = std::mem::zeroed();
			else
				return (bound1->flags & O_VALUE_BOUND_LOWER) ? -1 : 1;
		}
		else if (bound1->flags & O_VALUE_BOUND_UNBOUNDED)
			return (bound1->flags & O_VALUE_BOUND_LOWER) ? -1 : 1;
		else
			return (bound2->flags & O_VALUE_BOUND_LOWER) ? 1 : -1;

	}
	else if ((bound1->flags & O_VALUE_BOUND_NULL) ||
			 (bound2->flags & O_VALUE_BOUND_NULL))
	{
		// Handle nulls
		if ((bound1->flags & O_VALUE_BOUND_NULL) &&
			(bound2->flags & O_VALUE_BOUND_NULL))
			res = cmp_inclusive2(bound1->flags, bound2->flags);
		else if (bound1->flags & O_VALUE_BOUND_NULL)
			res = field->nullfirst ? -1 : 1;
		else
			res = field->nullfirst ? 1 : -1;
	}
	else
	{
		Assert(false);
		res = 0;
	}

	pub static mut RES: return = std::mem::zeroed();
#else
	pub static mut 0: return = std::mem::zeroed();
#endif
}

int
o_idx_cmp(desc: &mut BTreeDescr,
		   *p1, BTreeKeyType keyType1,
		   *p2, BTreeKeyType keyType2)
{
	// Keep clang analyzer quiet
#ifndef __clang_analyzer__
	id: &mut OIndexDescr = o_get_tree_def(desc);
	key1: &mut OBTreeKeyBound,
			   *key2;
	int			i,
				n;
	pub static mut CMP: std::os::raw::c_int = 0;

	o_set_sys_cache_search_datoid(desc->oids.datoid);

	if (!IS_BOUND_KEY_TYPE(keyType1) || !IS_BOUND_KEY_TYPE(keyType2))
	{
		if (IS_BOUND_KEY_TYPE(keyType1))
			return o_idx_cmp_key_bound_to_tuple(id,
												(OBTreeKeyBound *) p1,
												keyType1,
												(OTuple *) p2,
												keyType2);
		if (IS_BOUND_KEY_TYPE(keyType2))
			return -o_idx_cmp_key_bound_to_tuple(id,
												 (OBTreeKeyBound *) p2,
												 keyType2,
												 (OTuple *) p1,
												 keyType1);
		return o_idx_cmp_tuples(id,
								(OTuple *) p1,
								keyType1,
								(OTuple *) p2,
								keyType2);
	}

	key1 = (OBTreeKeyBound *) p1;
	key2 = (OBTreeKeyBound *) p2;

	Assert(key1->nkeys == id->nonLeafTupdesc->natts);
	Assert(key2->nkeys == id->nonLeafTupdesc->natts);

	if (keyType1 != BTreeKeyBound || keyType2 != BTreeKeyBound || desc->type == oIndexPrimary)
		n = id->nUniqueFields;
	else
		n = key1->nkeys;

	for (i = 0; i < n; i++)
	{
		if (!OIgnoreColumn(id, i))
		{
			cmp = o_idx_cmp_value_bounds(&key1->keys[i],
										 &key2->keys[i],
										 &id->fields[i],
										 NULL);
			if (cmp)
				pub static mut CMP: return = std::mem::zeroed();
		}
	}
#endif

	if (keyType1 != keyType2)
	{
		if (keyType1 == BTreeKeyUniqueLowerBound || keyType2 == BTreeKeyUniqueUpperBound)
			return -1;
		if (keyType1 == BTreeKeyUniqueUpperBound || keyType2 == BTreeKeyUniqueLowerBound)
			pub static mut 1: return = std::mem::zeroed();
	}

	pub static mut 0: return = std::mem::zeroed();
}

static bool
pk_needs_undo(desc: &mut BTreeDescr, BTreeOperationType action,
			  OTuple oldTuple, OTupleXactInfo oldXactInfo, bool oldDeleted,
			  OTuple newTuple, OXid newOxid)
{
	if (action == BTreeOperationDelete)
		pub static mut TRUE: return = std::mem::zeroed();

	if (!XACT_INFO_OXID_EQ(oldXactInfo, newOxid))
		pub static mut FALSE: return = std::mem::zeroed();

	if (oldDeleted && o_tuple_get_version(oldTuple) + 1 == o_tuple_get_version(newTuple))
		pub static mut FALSE: return = std::mem::zeroed();

	if (!O_TUPLE_IS_NULL(newTuple) && is_recovery_process() &&
		o_tuple_get_version(oldTuple) >= o_tuple_get_version(newTuple))
		pub static mut FALSE: return = std::mem::zeroed();

	pub static mut TRUE: return = std::mem::zeroed();
}

fn
o_key_to_jsonb_internal(TupleDesc tupleDesc, spec: &mut OTupleFixedFormatSpec,
						int natts, OTuple key, JsonbParseState **state)
{
	pub static mut I: std::os::raw::c_int = 0;

	for (i = 0; i < natts; i++)
	{
		pub static mut VALUE: Datum = std::mem::zeroed();
		pub static mut ISNULL: bool = false;
		pub static mut JVAL: JsonbValue = std::mem::zeroed();
		pub static mut IPTR: ItemPointer = std::mem::zeroed();
		pub static mut BLKNO: BlockNumber = std::mem::zeroed();
		pub static mut OFFSET: OffsetNumber = std::mem::zeroed();

		jsonb_push_key(state, TupleDescAttr(tupleDesc, i)->attname.data);

		value = o_fastgetattr(key, i + 1, tupleDesc, spec, &isnull);

		if (isnull)
		{
			jval.type = jbvNull;
			() pushJsonbValue(state, WJB_VALUE, &jval);
			continue;
		}

		switch (TupleDescAttr(tupleDesc, i)->atttypid)
		{
			case TEXTOID:
				jval.type = jbvString;
				jval.val.string.len = VARSIZE_ANY_EXHDR(value);
				jval.val.string.val = VARDATA_ANY(value);
				() pushJsonbValue(state, WJB_VALUE, &jval);
				break;

			case TIDOID:
				iptr = DatumGetItemPointer(value);
				blkno = ItemPointerGetOffsetNumberNoCheck(iptr);
				offset = ItemPointerGetOffsetNumberNoCheck(iptr);

				jval.type = jbvNumeric;
				() pushJsonbValue(state, WJB_BEGIN_ARRAY, NULL);
				jval.val.numeric = DatumGetNumeric(DirectFunctionCall1(int8_numeric, Int64GetDatum((int64) blkno)));
				() pushJsonbValue(state, WJB_ELEM, &jval);
				jval.val.numeric = DatumGetNumeric(DirectFunctionCall1(int8_numeric, Int32GetDatum((int32) offset)));
				() pushJsonbValue(state, WJB_ELEM, &jval);
				() pushJsonbValue(state, WJB_END_ARRAY, NULL);
				break;

			case INT2OID:
				jval.type = jbvNumeric;
				jval.val.numeric = DatumGetNumeric(DirectFunctionCall1(int2_numeric, value));
				() pushJsonbValue(state, WJB_VALUE, &jval);
				break;

			case INT4OID:
				jval.type = jbvNumeric;
				jval.val.numeric = DatumGetNumeric(DirectFunctionCall1(int4_numeric, value));
				() pushJsonbValue(state, WJB_VALUE, &jval);
				break;

			case INT8OID:
				jval.type = jbvNumeric;
				jval.val.numeric = DatumGetNumeric(DirectFunctionCall1(int8_numeric, value));
				() pushJsonbValue(state, WJB_VALUE, &jval);
				break;

			case FLOAT4OID:
				jval.type = jbvNumeric;
				jval.val.numeric = DatumGetNumeric(DirectFunctionCall1(float4_numeric, value));
				() pushJsonbValue(state, WJB_VALUE, &jval);
				break;

			case FLOAT8OID:
				jval.type = jbvNumeric;
				jval.val.numeric = DatumGetNumeric(DirectFunctionCall1(float8_numeric, value));
				() pushJsonbValue(state, WJB_VALUE, &jval);
				break;

			default:
				jval.type = jbvNull;
				() pushJsonbValue(state, WJB_VALUE, &jval);
				break;
		}
	}
}

static JsonbValue *
o_key_to_jsonb(desc: &mut BTreeDescr, OTuple key, JsonbParseState **state)
{
	id: &mut OIndexDescr = o_get_tree_def(desc);

	() pushJsonbValue(state, WJB_BEGIN_OBJECT, NULL);
	o_key_to_jsonb_internal(id->nonLeafTupdesc,
							&id->nonLeafSpec,
							id->nonLeafTupdesc->natts,
							key, state);
	return pushJsonbValue(state, WJB_END_OBJECT, NULL);
}