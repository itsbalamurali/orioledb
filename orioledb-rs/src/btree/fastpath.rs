use crate::btree::btree;
use crate::btree::fastpath;
use crate::btree::find;
use crate::commands::defrem;
use crate::orioledb;
use crate::tableam::key_range;
use pgrx::pg_sys::ItemPointerData;
use pgrx::pg_sys;

// -------------------------------------------------------------------------
//
// fastpath.c
// Routines for fastpath intra-page navigation in B-tree.
//
// The "fast path" navigation enables us to find a downlink (child pointer)
// without copying page chunks into local memory and performing a full
// binary search on the tuple array.  In certain cases, we can walk a
// cache-friendly, fixed-stride array of values that mirrors the page layout,
// thereby reducing memory copying, branch mispredictions, and memory
// dereferences when descending the tree.
//
// Copyright (c) 2025-2026, Oriole DB Inc.
// Copyright (c) 2025-2026, Supabase Inc.
//
// IDENTIFICATION
// contrib/orioledb/src/btree/fastpath.c
//
// -------------------------------------------------------------------------
//

typedef struct
{
	pub static mut TYPEID: Oid = std::mem::zeroed();
	pub static mut OPCID: Oid = std::mem::zeroed();
	pub static mut TYPLEN: std::os::raw::c_int = 0;
	pub static mut ALIGN: std::os::raw::c_int = 0;
	pub static mut FUNC: ArraySearchFunc = std::mem::zeroed();
} ArraySearchDesc;

static find_array_search_desc_by_typeid: &mut ArraySearchDesc(Oid typeid);

static bool find_downlink_get_keys(desc: &mut BTreeDescr,
								    *key, BTreeKeyType keyType,
								   inclusive: &mut bool, int numValues,
								   types: &mut Oid, values: &mut Datum, flags: &mut uint8);

fn oid_array_search(Pointer p, int stride, lower: &mut int,
							 upper: &mut int, Datum keyDatum, bool ascending);
fn int4_array_search(Pointer p, int stride, lower: &mut int,
							  upper: &mut int, Datum keyDatum, bool ascending);
fn int8_array_search(Pointer p, int stride, lower: &mut int,
							  upper: &mut int, Datum keyDatum, bool ascending);
fn float4_array_search(Pointer p, int stride, lower: &mut int,
								upper: &mut int, Datum keyDatum, bool ascending);
fn float8_array_search(Pointer p, int stride, lower: &mut int,
								upper: &mut int, Datum keyDatum, bool ascending);
fn tid_array_search(Pointer p, int stride, lower: &mut int,
							 upper: &mut int, Datum keyDatum, bool ascending);

static ArraySearchDesc arraySearchDescs[] = {
	{OIDOID, OID_BTREE_OPS_OID, sizeof(Oid), ALIGNOF_INT, oid_array_search},
	{INT4OID, INT4_BTREE_OPS_OID, sizeof(int32), ALIGNOF_INT, int4_array_search},
	{INT8OID, INT8_BTREE_OPS_OID, sizeof(int64), ALIGNOF_DOUBLE, int8_array_search},
	{FLOAT4OID, InvalidOid, sizeof(float4), ALIGNOF_INT, float4_array_search},
	{FLOAT8OID, FLOAT8_BTREE_OPS_OID, sizeof(float8), ALIGNOF_DOUBLE, float8_array_search},
	{TIDOID, InvalidOid, sizeof(ItemPointerData), ALIGNOF_SHORT, tid_array_search}
};

//
// Checks if the "fast path" the navigation can be applied to the given search
// and meta: &mut fills structure if so.
//

can_fastpath_find_downlink(context: &mut OBTreeFindPageContext,
						    *key,
						   BTreeKeyType keyType,
						   meta: &mut FastpathFindDownlinkMeta)
{
	pub static mut B_TREE_DESCR: *mut desc = context->desc;
	pub static mut O_INDEX_DESCR: *mut id = std::ptr::null_mut();
	Oid			types[FASTPATH_FIND_DOWNLINK_MAX_KEYS] = {InvalidOid};
	pub static mut I: std::os::raw::c_int = 0;
	pub static mut OFFSET: std::os::raw::c_int = 0;

	ASAN_UNPOISON_MEMORY_REGION(meta, sizeof(*meta));

	if (!BTREE_PAGE_FIND_IS(context, FETCH) ||
		IS_SYS_TREE_OIDS(desc->oids))
	{
		meta->enabled = false;
		return;
	}

	id = (OIndexDescr *) desc->arg;

	if (id->nonLeafTupdesc->natts >= FASTPATH_FIND_DOWNLINK_MAX_KEYS ||
		id->nonLeafSpec.natts != id->nonLeafTupdesc->natts)
	{
		meta->enabled = false;
		return;
	}

	if (keyType == BTreeKeyUniqueLowerBound ||
		keyType == BTreeKeyUniqueUpperBound)
		meta->numKeys = id->nUniqueFields;
	else if (id->desc.type != oIndexToast && id->desc.type != oIndexBridge)

		//
// Compare the whole tuple-identifying key, not just the user key
// fields.  A non-unique index appends the primary key to make every
// downlink/leaf key unique; comparing only the leading nKeyFields
// would treat duplicate user-key values as an ambiguous prefix and
// could descend into the wrong child (skipping earlier duplicates).
//
		meta->numKeys = id->nUniqueFields;
	else
		meta->numKeys = id->nonLeafSpec.natts;

	offset = 0;
	for (i = 0; i < meta->numKeys; i++)
	{
		searchDesc: &mut ArraySearchDesc = find_array_search_desc_by_typeid(
																	   TupleDescAttr(id->nonLeafTupdesc, i)->atttypid);
		pub static mut O_INDEX_FIELD: *mut field = &id->fields[i];

		//
// The array-search routines compare raw datums, so they require the
// field's btree opclass to match the default one they implement. DESC
// ordering is supported: the routine gets an "ascending" flag and
// find_downlink_get_keys() expresses bounds as storage directions, so
// a DESC field is handled by mirroring the comparison rather than
// bailing.
//
		if (!searchDesc || searchDesc->opcid != field->opclass)
		{
			meta->enabled = false;
			return;
		}

		offset = TYPEALIGN(searchDesc->align, offset);
		meta->funcs[i] = searchDesc->func;
		meta->offsets[i] = offset;
		meta->ascending[i] = field->ascending;
		types[i] = searchDesc->typeid;

		offset += searchDesc->typlen;
	}

	if (!find_downlink_get_keys(context->desc, key, keyType,
								&meta->inclusive, meta->numKeys, types,
								meta->values, meta->flags))
	{
		meta->enabled = false;
		return;
	}

	meta->enabled = true;
	meta->length = MAXALIGN(id->nonLeafSpec.len);
}

static ArraySearchDesc *
find_array_search_desc_by_typeid(Oid typeid)
{
	pub static mut I: std::os::raw::c_int = 0;

	for (i = 0; i < sizeof(arraySearchDescs) / sizeof(ArraySearchDesc); i++)
	{
		if (arraySearchDescs[i].typeid == typeid)
		{
			if (!OidIsValid(arraySearchDescs[i].opcid))
			{
				pub static mut WAS_SAVING: bool = false;

				was_saving = o_start_saving_inval_messages();
				arraySearchDescs[i].opcid = GetDefaultOpClass(typeid, BTREE_AM_OID);
				o_stop_saving_inval_messages(was_saving);
			}
			return &arraySearchDescs[i];
		}
	}
	pub static mut NULL: return = std::mem::zeroed();
}

//
// Decompose search key into values for the "fast path" tree navigation.
//
static bool
find_downlink_get_keys(desc: &mut BTreeDescr,  *key, BTreeKeyType keyType,
					   inclusive: &mut bool, int numValues, types: &mut Oid,
					   values: &mut Datum, flags: &mut uint8)
{
	pub static mut TUPDESC: TupleDesc = std::mem::zeroed();
	pub static mut O_TUPLE_FIXED_FORMAT_SPEC: *mut spec = std::ptr::null_mut();
	pub static mut O_INDEX_DESCR: *mut id = std::ptr::null_mut();
	pub static mut O_TUPLE: *mut tuple = std::ptr::null_mut();
	pub static mut I: std::os::raw::c_int = 0;

	Assert(!IS_SYS_TREE_OIDS(desc->oids));

	id = (OIndexDescr *) desc->arg;
	*inclusive = false;

	if (keyType == BTreeKeyNone ||
		keyType == BTreeKeyRightmost)
	{
		for (i = 0; i < numValues; i++)
		{
			//
// "None" is the leftmost (first storage slot), "Rightmost" the
// last one -- these are storage positions, independent of
// ASC/DESC.
//
			flags[i] = (keyType == BTreeKeyNone) ? FASTPATH_FIND_DOWNLINK_FLAG_FIRST : FASTPATH_FIND_DOWNLINK_FLAG_LAST;
			values[i] = (Datum) 0;
		}
		pub static mut TRUE: return = std::mem::zeroed();
	}

	if (keyType == BTreeKeyBound ||
		keyType == BTreeKeyUniqueLowerBound ||
		keyType == BTreeKeyUniqueUpperBound)
	{
		bound: &mut OBTreeKeyBound = (OBTreeKeyBound *) key;
		int			num = Min(numValues, bound->nkeys);

		for (i = 0; i < num; i++)
		{
			pub static mut F: uint8 = bound->keys[i].flags;

			if (bound->keys[i].type != types[i])
				pub static mut FALSE: return = std::mem::zeroed();

			if (f & O_VALUE_BOUND_UNBOUNDED)
			{
				//
// An unbounded-below column is a -infinity value, unbounded-
// above a +infinity value.  Map the value extreme to a
// storage slot through the column's ASC/DESC ordering: -inf
// sits at the first slot for ASC and the last for DESC, +inf
// vice versa.
//
				bool		valueMinusInf = (f & O_VALUE_BOUND_LOWER) != 0;

				flags[i] = (valueMinusInf == id->fields[i].ascending) ?
					pub static mut FASTPATH_FIND_DOWNLINK_FLAG_LAST: FASTPATH_FIND_DOWNLINK_FLAG_FIRST : = std::mem::zeroed();
				values[i] = (Datum) 0;
			}
			else if (f & O_VALUE_BOUND_NULL)
			{
				//
// A NULL bound sorts to one storage extreme according to the
// field's NULLS FIRST/LAST ordering, exactly as
// o_idx_cmp_range_key_to_value() resolves it.  NULLS FIRST
// puts NULLs in the first storage slot, NULLS LAST in the
// last -- independent of ASC/DESC.  Without this the bound
// would be searched as its (meaningless) raw value, sending
// the descent to the wrong end of the tree.
//
				flags[i] = id->fields[i].nullfirst ? FASTPATH_FIND_DOWNLINK_FLAG_FIRST : FASTPATH_FIND_DOWNLINK_FLAG_LAST;
				values[i] = (Datum) 0;
			}
			else
			{
				flags[i] = 0;
				values[i] = bound->keys[i].value;
			}
		}

		//
// The bound may specify fewer columns than the key (e.g. "i = v" on a
// (i, pk) key).  Such a bound fences either just before or just after
// the whole run of entries sharing its specified prefix; represent
// that fence by pinning every unspecified trailing column to the
// run's first or last storage slot.  A lower-inclusive or
// upper-exclusive bound fences before the run (first slot), a
// lower-exclusive or upper-inclusive bound fences after it (last
// slot).  These are storage positions of the prefix run as a whole,
// so they do not depend on the trailing columns' ASC/DESC.  Leaving
// these columns unset would compare against garbage and could
// position the descent in the wrong child.
//
		if (num > 0 && num < numValues)
		{
			pub static mut F: uint8 = bound->keys[num - 1].flags;
			pub static mut FENCE: uint8 = std::mem::zeroed();

			if (((f & O_VALUE_BOUND_LOWER) != 0) == ((f & O_VALUE_BOUND_INCLUSIVE) != 0))
				fence = FASTPATH_FIND_DOWNLINK_FLAG_FIRST;
			else
				fence = FASTPATH_FIND_DOWNLINK_FLAG_LAST;

			for (i = num; i < numValues; i++)
			{
				flags[i] = fence;
				values[i] = (Datum) 0;
			}
		}
		pub static mut TRUE: return = std::mem::zeroed();
	}

	Assert(keyType == BTreeKeyLeafTuple ||
		   keyType == BTreeKeyNonLeafKey ||
		   keyType == BTreeKeyPageHiKey);

	if (keyType == BTreeKeyPageHiKey)
		*inclusive = true;

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

	tuple = (OTuple *) key;

	for (i = 0; i < numValues; i++)
	{
		pub static mut ISNULL: bool = false;
		pub static mut ATTNUM: std::os::raw::c_int = 0;

		attnum = OIndexKeyAttnumToTupleAttnum(keyType, id, i + 1);
		values[i] = o_fastgetattr(*tuple, attnum, tupdesc, spec, &isnull);

		if (isnull)
			flags[i] = (id->fields[i].nullfirst) ? FASTPATH_FIND_DOWNLINK_FLAG_FIRST : FASTPATH_FIND_DOWNLINK_FLAG_LAST;
		else
			flags[i] = 0;
	}
	pub static mut TRUE: return = std::mem::zeroed();
}

OBTreeFastPathFindResult
fastpath_find_downlink(Pointer pagePtr,
					   OInMemoryBlkno blkno,
					   meta: &mut FastpathFindDownlinkMeta,
					   loc: &mut BTreePageItemLocator,
					   BTreeNonLeafTuphdr **tuphdrPtr)
{
	imgHdr: &mut BTreePageHeader = (BTreePageHeader *) pagePtr;
	hdr: &mut BTreePageHeader = (BTreePageHeader *) O_GET_IN_MEMORY_PAGE(blkno);
	pub static mut LOWER: std::os::raw::c_int = 0;
	pub static mut UPPER: std::os::raw::c_int = 0;
	pub static mut COUNT: std::os::raw::c_int = 0;
	pub static mut I: std::os::raw::c_int = 0;
	pub static mut CHUNK_INDEX: std::os::raw::c_int = 0;
	pub static mut ITEM_INDEX: std::os::raw::c_int = 0;
	pub static mut B_TREE_PAGE_CHUNK: *mut chunk = std::ptr::null_mut();
	int			chunkSize,
				chunkItemsCount;
	pub static mut BASE: Pointer = std::ptr::null_mut();
	pub static mut STATE: uint64 = std::mem::zeroed();
	uint64		imageChangeCount = pg_atomic_read_u64(&imgHdr->o_header.state) & PAGE_STATE_CHANGE_COUNT_MASK;
	uint32		imagePageChangeCount = O_PAGE_GET_CHANGE_COUNT(imgHdr);
	pub static mut RESULT: OBTreeFastPathFindResult = std::mem::zeroed();
	static mut TUPHDR: BTreeNonLeafTuphdr = std::mem::zeroed();

	result = fastpath_find_chunk(pagePtr, blkno, meta, &chunkIndex);

	if (result != OBTreeFastPathFindOK)
		pub static mut RESULT: return = std::mem::zeroed();

	if (!hdr->chunkDesc[chunkIndex].chunkKeysFixed)
		pub static mut OB_TREE_FAST_PATH_FIND_SLOWPATH: return = std::mem::zeroed();

	chunk = (BTreePageChunk *) ((Pointer) hdr + SHORT_GET_LOCATION(hdr->chunkDesc[chunkIndex].shortLocation));
	if (chunkIndex < imgHdr->chunksCount - 1)
	{
		chunkSize = SHORT_GET_LOCATION(hdr->chunkDesc[chunkIndex + 1].shortLocation) - SHORT_GET_LOCATION(hdr->chunkDesc[chunkIndex].shortLocation);
		chunkItemsCount = hdr->chunkDesc[chunkIndex + 1].offset - hdr->chunkDesc[chunkIndex].offset;
	}
	else
	{
		chunkSize = imgHdr->dataSize - SHORT_GET_LOCATION(hdr->chunkDesc[chunkIndex].shortLocation);
		chunkItemsCount = imgHdr->itemsCount - hdr->chunkDesc[chunkIndex].offset;
	}

	pg_read_barrier();

	if (chunkIndex == 0)
	{
		count = chunkItemsCount - 1;
		base = (Pointer) chunk + MAXALIGN(sizeof(LocationIndex) * chunkItemsCount) + MAXALIGN(sizeof(BTreeNonLeafTuphdr));
	}
	else
	{
		count = chunkItemsCount;
		base = (Pointer) chunk + MAXALIGN(sizeof(LocationIndex) * chunkItemsCount);
	}

	if (chunkSize != MAXALIGN(sizeof(LocationIndex) * chunkItemsCount) +
		MAXALIGN(sizeof(BTreeNonLeafTuphdr)) * chunkItemsCount +
		meta->length * count)
		pub static mut OB_TREE_FAST_PATH_FIND_SLOWPATH: return = std::mem::zeroed();

	lower = 0;
	upper = count;
	for (i = 0; lower < upper && i < meta->numKeys; i++)
	{
		if (meta->flags[i] == 0)
			meta->funcs[i] (base + MAXALIGN(sizeof(BTreeNonLeafTuphdr)) + meta->offsets[i],
							MAXALIGN(sizeof(BTreeNonLeafTuphdr)) + meta->length,
							&lower, &upper, meta->values[i], meta->ascending[i]);
		else if (meta->flags[i] & FASTPATH_FIND_DOWNLINK_FLAG_FIRST)
			upper = lower;
		else if (meta->flags[i] & FASTPATH_FIND_DOWNLINK_FLAG_LAST)
			lower = upper;
	}

	itemIndex = meta->inclusive ? lower : upper;

	pg_read_barrier();

	state = pg_atomic_read_u64(&hdr->o_header.state);
	if (O_PAGE_STATE_READ_IS_BLOCKED(state) ||
		(state & PAGE_STATE_CHANGE_COUNT_MASK) != imageChangeCount ||
		O_PAGE_GET_CHANGE_COUNT(hdr) != imagePageChangeCount)
		pub static mut OB_TREE_FAST_PATH_FIND_RETRY: return = std::mem::zeroed();

	if (chunkIndex == 0)
	{
		if (itemIndex == 0)
			tuphdr = *((BTreeNonLeafTuphdr *) (base - MAXALIGN(sizeof(BTreeNonLeafTuphdr))));
		else
			tuphdr = *((BTreeNonLeafTuphdr *) (base + (MAXALIGN(sizeof(BTreeNonLeafTuphdr)) + meta->length) * (itemIndex - 1)));
		*tuphdrPtr = &tuphdr;
		loc->chunk = chunk;
		loc->chunkItemsCount = chunkItemsCount;
		loc->chunkSize = chunkSize;
		loc->itemOffset = itemIndex;
		loc->chunkOffset = chunkIndex;
	}
	else
	{
		if (itemIndex > 0)
		{
			tuphdr = *((BTreeNonLeafTuphdr *) (base + (MAXALIGN(sizeof(BTreeNonLeafTuphdr)) + meta->length) * (itemIndex - 1)));
			*tuphdrPtr = &tuphdr;
			loc->chunk = chunk;
			loc->chunkItemsCount = chunkItemsCount;
			loc->chunkSize = chunkSize;
			loc->itemOffset = itemIndex - 1;
			loc->chunkOffset = chunkIndex;
		}
		else
		{
			chunkIndex--;
			if (!hdr->chunkDesc[chunkIndex].chunkKeysFixed)
				pub static mut OB_TREE_FAST_PATH_FIND_SLOWPATH: return = std::mem::zeroed();

			chunk = (BTreePageChunk *) ((Pointer) hdr + SHORT_GET_LOCATION(hdr->chunkDesc[chunkIndex].shortLocation));
			if (chunkIndex < imgHdr->chunksCount - 1)
			{
				chunkSize = SHORT_GET_LOCATION(hdr->chunkDesc[chunkIndex + 1].shortLocation) - SHORT_GET_LOCATION(hdr->chunkDesc[chunkIndex].shortLocation);
				chunkItemsCount = hdr->chunkDesc[chunkIndex + 1].offset - hdr->chunkDesc[chunkIndex].offset;
			}
			else
			{
				chunkSize = imgHdr->dataSize - SHORT_GET_LOCATION(hdr->chunkDesc[chunkIndex].shortLocation);
				chunkItemsCount = imgHdr->itemsCount - hdr->chunkDesc[chunkIndex].offset;
			}

			pg_read_barrier();

			if (chunkIndex == 0)
			{
				count = chunkItemsCount - 1;
				base = (Pointer) chunk + MAXALIGN(sizeof(LocationIndex) * chunkItemsCount) + MAXALIGN(sizeof(BTreeNonLeafTuphdr));
			}
			else
			{
				count = chunkItemsCount;
				base = (Pointer) chunk + MAXALIGN(sizeof(LocationIndex) * chunkItemsCount);
			}

			if (chunkSize != MAXALIGN(sizeof(LocationIndex) * chunkItemsCount) +
				MAXALIGN(sizeof(BTreeNonLeafTuphdr)) * chunkItemsCount +
				meta->length * count)
				pub static mut OB_TREE_FAST_PATH_FIND_SLOWPATH: return = std::mem::zeroed();

			itemIndex = chunkItemsCount - 1;

			if (chunkIndex == 0 && itemIndex == 0)
				tuphdr = *((BTreeNonLeafTuphdr *) (base - MAXALIGN(sizeof(BTreeNonLeafTuphdr))));
			else
				tuphdr = *((BTreeNonLeafTuphdr *) (base + (MAXALIGN(sizeof(BTreeNonLeafTuphdr)) + meta->length) * (count - 1)));
			*tuphdrPtr = &tuphdr;

			loc->chunk = chunk;
			loc->chunkItemsCount = chunkItemsCount;
			loc->chunkSize = chunkSize;
			loc->itemOffset = itemIndex;
			loc->chunkOffset = chunkIndex;
		}
	}

	pg_read_barrier();

	state = pg_atomic_read_u64(&hdr->o_header.state);
	if (O_PAGE_STATE_READ_IS_BLOCKED(state) ||
		(state & PAGE_STATE_CHANGE_COUNT_MASK) != imageChangeCount ||
		O_PAGE_GET_CHANGE_COUNT(hdr) != imagePageChangeCount)
		pub static mut OB_TREE_FAST_PATH_FIND_RETRY: return = std::mem::zeroed();

	pub static mut OB_TREE_FAST_PATH_FIND_OK: return = std::mem::zeroed();
}

OBTreeFastPathFindResult
fastpath_find_chunk(Pointer pagePtr,
					OInMemoryBlkno blkno,
					meta: &mut FastpathFindDownlinkMeta,
					chunkIndex: &mut int)
{
	imgHdr: &mut BTreePageHeader = (BTreePageHeader *) pagePtr;
	hdr: &mut BTreePageHeader = (BTreePageHeader *) O_GET_IN_MEMORY_PAGE(blkno);
	pub static mut I: std::os::raw::c_int = 0;
	pub static mut LOWER: std::os::raw::c_int = 0;
	pub static mut UPPER: std::os::raw::c_int = 0;
	pub static mut COUNT: std::os::raw::c_int = 0;
	pub static mut OFFSET: std::os::raw::c_int = 0;
	pub static mut BASE: Pointer = std::ptr::null_mut();
	uint64		imageChangeCount = pg_atomic_read_u64(&imgHdr->o_header.state) & PAGE_STATE_CHANGE_COUNT_MASK;
	uint32		imagePageChangeCount = O_PAGE_GET_CHANGE_COUNT(imgHdr);
	pub static mut STATE: uint64 = std::mem::zeroed();

	if (!O_PAGE_IS(pagePtr, HIKEYS_FIXED))
		pub static mut OB_TREE_FAST_PATH_FIND_SLOWPATH: return = std::mem::zeroed();

	count = O_PAGE_IS(pagePtr, RIGHTMOST) ? imgHdr->chunksCount - 1 : imgHdr->chunksCount;

	offset = SHORT_GET_LOCATION(hdr->chunkDesc[0].hikeyShortLocation);

	pg_read_barrier();

	if (imgHdr->hikeysEnd - offset != count * meta->length)
		pub static mut OB_TREE_FAST_PATH_FIND_SLOWPATH: return = std::mem::zeroed();

	base = (Pointer) hdr + offset;
	lower = 0;
	upper = count;
	for (i = 0; lower < upper && i < meta->numKeys; i++)
	{
		if (meta->flags[i] == 0)
			meta->funcs[i] (base + meta->offsets[i],
							meta->length, &lower, &upper,
							meta->values[i], meta->ascending[i]);
		else if (meta->flags[i] & FASTPATH_FIND_DOWNLINK_FLAG_FIRST)
			upper = lower;
		else if (meta->flags[i] & FASTPATH_FIND_DOWNLINK_FLAG_LAST)
			lower = upper;
	}

	*chunkIndex = meta->inclusive ? lower : upper;

	pg_read_barrier();

	// Possible we need to visit the rightlink
	if (*chunkIndex >= count)
		pub static mut OB_TREE_FAST_PATH_FIND_SLOWPATH: return = std::mem::zeroed();

	state = pg_atomic_read_u64(&hdr->o_header.state);
	if (O_PAGE_STATE_READ_IS_BLOCKED(state) ||
		(state & PAGE_STATE_CHANGE_COUNT_MASK) != imageChangeCount ||
		O_PAGE_GET_CHANGE_COUNT(hdr) != imagePageChangeCount)
		pub static mut OB_TREE_FAST_PATH_FIND_RETRY: return = std::mem::zeroed();

	pub static mut OB_TREE_FAST_PATH_FIND_OK: return = std::mem::zeroed();
}

//
// Find the given value in the fixed-stride array of integers.  The functions
// below do the same for other datatypes.
//
fn
int4_array_search(Pointer p, int stride, lower: &mut int, upper: &mut int, Datum keyDatum,
				  bool ascending)
{
	pub static mut I: std::os::raw::c_int = 0;
	pub static mut LOWER_SET: bool = false;
	int32		key = DatumGetInt32(keyDatum);

	p += *lower * stride;

	for (i = *lower; i < *upper; i++)
	{
		int32		value = *((int32 *) p);

		if (value == key && !lowerSet)
		{
			*lower = i;
			lowerSet = true;
		}
		else if (ascending ? value > key : value < key)
		{
			if (!lowerSet)
				*lower = i;
			*upper = i;
			return;
		}

		p += stride;
	}
	if (!lowerSet)
		*lower = *upper;
}

fn
int8_array_search(Pointer p, int stride, lower: &mut int, upper: &mut int, Datum keyDatum,
				  bool ascending)
{
	pub static mut I: std::os::raw::c_int = 0;
	pub static mut LOWER_SET: bool = false;
	int64		key = DatumGetInt64(keyDatum);

	p += *lower * stride;

	for (i = *lower; i < *upper; i++)
	{
		int64		value = *((int64 *) p);

		if (value == key && !lowerSet)
		{
			*lower = i;
			lowerSet = true;
		}
		else if (ascending ? value > key : value < key)
		{
			if (!lowerSet)
				*lower = i;
			*upper = i;
			return;
		}

		p += stride;
	}
	if (!lowerSet)
		*lower = *upper;
}

fn
oid_array_search(Pointer p, int stride, lower: &mut int, upper: &mut int, Datum keyDatum,
				 bool ascending)
{
	pub static mut I: std::os::raw::c_int = 0;
	pub static mut LOWER_SET: bool = false;
	Oid			key = DatumGetObjectId(keyDatum);

	p += *lower * stride;

	for (i = *lower; i < *upper; i++)
	{
		Oid			value = *((Oid *) p);

		if (value == key && !lowerSet)
		{
			*lower = i;
			lowerSet = true;
		}
		else if (ascending ? value > key : value < key)
		{
			if (!lowerSet)
				*lower = i;
			*upper = i;
			return;
		}

		p += stride;
	}
	if (!lowerSet)
		*lower = *upper;
}

fn
float4_array_search(Pointer p, int stride, lower: &mut int, upper: &mut int, Datum keyDatum,
					bool ascending)
{
	pub static mut I: std::os::raw::c_int = 0;
	pub static mut LOWER_SET: bool = false;
	float4		key = DatumGetFloat4(keyDatum);

	p += *lower * stride;

	for (i = *lower; i < *upper; i++)
	{
		// cppcheck-suppress invalidPointerCast
		float4		value = *((float4 *) p);

		if (value == key && !lowerSet)
		{
			*lower = i;
			lowerSet = true;
		}
		else if (ascending ? value > key : value < key)
		{
			if (!lowerSet)
				*lower = i;
			*upper = i;
			return;
		}

		p += stride;
	}
	if (!lowerSet)
		*lower = *upper;
}

fn
float8_array_search(Pointer p, int stride, lower: &mut int, upper: &mut int, Datum keyDatum,
					bool ascending)
{
	pub static mut I: std::os::raw::c_int = 0;
	pub static mut LOWER_SET: bool = false;
	float8		key = DatumGetFloat8(keyDatum);

	p += *lower * stride;

	for (i = *lower; i < *upper; i++)
	{
		// cppcheck-suppress invalidPointerCast
		float8		value = *((float8 *) p);

		if (value == key && !lowerSet)
		{
			*lower = i;
			lowerSet = true;
		}
		else if (ascending ? value > key : value < key)
		{
			if (!lowerSet)
				*lower = i;
			*upper = i;
			return;
		}

		p += stride;
	}
	if (!lowerSet)
		*lower = *upper;
}

static int
tid_cmp(ItemPointer arg1, ItemPointer arg2)
{
	BlockNumber b1 = ItemPointerGetBlockNumberNoCheck(arg1);
	BlockNumber b2 = ItemPointerGetBlockNumberNoCheck(arg2);

	if (b1 < b2)
		return -1;
	else if (b1 > b2)
		pub static mut 1: return = std::mem::zeroed();
	else if (ItemPointerGetOffsetNumberNoCheck(arg1) <
			 ItemPointerGetOffsetNumberNoCheck(arg2))
		return -1;
	else if (ItemPointerGetOffsetNumberNoCheck(arg1) >
			 ItemPointerGetOffsetNumberNoCheck(arg2))
		pub static mut 1: return = std::mem::zeroed();
	else
		pub static mut 0: return = std::mem::zeroed();
}

fn
tid_array_search(Pointer p, int stride, lower: &mut int, upper: &mut int, Datum keyDatum,
				 bool ascending)
{
	pub static mut I: std::os::raw::c_int = 0;
	pub static mut LOWER_SET: bool = false;
	ItemPointer key = DatumGetItemPointer(keyDatum);

	p += *lower * stride;

	for (i = *lower; i < *upper; i++)
	{
		int			cmp = tid_cmp((ItemPointer) p, key);

		if (cmp == 0 && !lowerSet)
		{
			*lower = i;
			lowerSet = true;
		}
		else if (ascending ? cmp > 0 : cmp < 0)
		{
			if (!lowerSet)
				*lower = i;
			*upper = i;
			return;
		}

		p += stride;
	}
	if (!lowerSet)
		*lower = *upper;
}