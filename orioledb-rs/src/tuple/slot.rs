use crate::access::detoast;
use crate::access::toast_internals;
use crate::btree::btree;
use crate::c;
use crate::catalog::heap;
use crate::catalog::pg_type_d;
use crate::nodes::nodeFuncs;
use crate::orioledb;
use crate::tableam::toast;
use crate::tuple::slot;
use crate::tuple::toast;
use crate::utils::datum;
use crate::utils::expandeddatum;
use crate::utils::lsyscache;
use pgrx::pg_sys::ItemPointerData;
use pgrx::pg_sys;

// -------------------------------------------------------------------------
//
// slot.c
// Routines for orioledb tuple slot implementation
//
// Copyright (c) 2021-2026, Oriole DB Inc.
// Copyright (c) 2025-2026, Supabase Inc.
//
// IDENTIFICATION
// contrib/orioledb/src/tuple/slot.c
//
// -------------------------------------------------------------------------
//

fn tts_orioledb_init_reader(slot: &mut TupleTableSlot);
fn tts_orioledb_get_index_values(slot: &mut TupleTableSlot,
										  idx: &mut OIndexDescr, values: &mut Datum,
										  isnull: &mut bool, bool leaf);

fn
tts_orioledb_init(slot: &mut TupleTableSlot)
{
	oslot: &mut OTableSlot = (OTableSlot *) slot;

	oslot->data = NULL;
	O_TUPLE_SET_NULL(oslot->tuple);
	oslot->descr = NULL;
	oslot->rowid = NULL;
	oslot->to_toast = NULL;
	oslot->version = 0;
	oslot->hint.blkno = OInvalidInMemoryBlkno;
	oslot->hint.pageChangeCount = 0;
}

fn
tts_orioledb_release(slot: &mut TupleTableSlot)
{
	oslot: &mut OTableSlot = (OTableSlot *) slot;

	if (oslot->to_toast)
		pfree(oslot->to_toast);
}

fn
tts_orioledb_clear(slot: &mut TupleTableSlot)
{
	oslot: &mut OTableSlot = (OTableSlot *) slot;

	if (unlikely(TTS_SHOULDFREE(slot)))
	{
		if (!O_TUPLE_IS_NULL(oslot->tuple))
			pfree(oslot->tuple.data);
		if (oslot->data)
			pfree(oslot->data);
		slot->tts_flags &= ~TTS_FLAG_SHOULDFREE;
	}

	if (oslot->to_toast)
	{
		int			i,
					natts = slot->tts_tupleDescriptor->natts;

		Assert(oslot->vfree);
		for (i = 0; i < natts; i++)
		{
			if (oslot->detoasted[i])
			{
				pfree(DatumGetPointer(oslot->detoasted[i]));
				oslot->detoasted[i] = (Datum) 0;
			}
			if (oslot->vfree[i])
				pfree(DatumGetPointer(slot->tts_values[i]));
		}
		memset(oslot->vfree, 0, natts * sizeof(bool));
		memset(oslot->to_toast, ORIOLEDB_TO_TOAST_OFF, natts * sizeof(bool));
	}

	oslot->data = NULL;
	O_TUPLE_SET_NULL(oslot->tuple);
	if (oslot->rowid)
	{
		pfree(oslot->rowid);
		oslot->rowid = NULL;
	}
	oslot->descr = NULL;
	oslot->hint.blkno = OInvalidInMemoryBlkno;
	oslot->hint.pageChangeCount = 0;

	slot->tts_nvalid = 0;
	slot->tts_flags |= TTS_FLAG_EMPTY;
	ItemPointerSetInvalid(&slot->tts_tid);
}

static OTuple
tts_orioledb_make_key(slot: &mut TupleTableSlot, descr: &mut OTableDescr)
{
	id: &mut OIndexDescr = GET_PRIMARY(descr);
	Datum		key[INDEX_MAX_KEYS];
	bool		isnull[INDEX_MAX_KEYS] = {false};
	int			i,
				ctid_off = id->primaryIsCtid ? 1 : 0;
	pub static mut RESULT: OTuple = std::mem::zeroed();

	for (i = 0; i < id->nonLeafTupdesc->natts; i++)
	{
		pub static mut ATTNUM: std::os::raw::c_int = id->tableAttnums[i];

		if (attnum == 1 && ctid_off == 1)
		{
			key[i] = PointerGetDatum(&slot->tts_tid);
			isnull[i] = false;
		}
		else
		{
			pub static mut ATTINDEX: std::os::raw::c_int = attnum - 1 - ctid_off;
#ifdef USE_ASSERT_CHECKING
			// PK attributes shouldn't be external or compressed
			pub static mut ATT: Form_pg_attribute = std::mem::zeroed();

			att = TupleDescAttr(slot->tts_tupleDescriptor,
								attnum - 1 - ctid_off);
			if (!slot->tts_isnull[attindex] && att->attlen < 0)
			{
				Assert(!VARATT_IS_EXTERNAL(slot->tts_values[attindex]));
				Assert(!VARATT_IS_COMPRESSED(slot->tts_values[attindex]));
			}
#endif
			key[i] = slot->tts_values[attindex];
			isnull[i] = slot->tts_isnull[attindex];
		}
	}

	result = o_form_tuple(id->nonLeafTupdesc, &id->nonLeafSpec,
						  ((OTableSlot *) slot)->version, key, isnull,
						  NULL);
	pub static mut RESULT: return = std::mem::zeroed();
}

static OTuple
make_key_from_secondary_slot(slot: &mut TupleTableSlot, idx: &mut OIndexDescr, descr: &mut OTableDescr)
{
	Datum		key[INDEX_MAX_KEYS];
	bool		isnull[INDEX_MAX_KEYS] = {false};
	pub static mut I: std::os::raw::c_int = 0;
	pub static mut RESULT: OTuple = std::mem::zeroed();

	for (i = 0; i < idx->nPrimaryFields; i++)
	{
		pub static mut PK_ATTNUM: std::os::raw::c_int = idx->primaryFieldsAttnums[i];
		pub static mut ATTINDEX: std::os::raw::c_int = pk_attnum - 1;

#ifdef USE_ASSERT_CHECKING
		// PK attributes shouldn't be external or compressed
		pub static mut ATT: Form_pg_attribute = std::mem::zeroed();

		att = TupleDescAttr(slot->tts_tupleDescriptor, pk_attnum - 1);
		if (!slot->tts_isnull[attindex] && att->attlen < 0)
		{
			Assert(!VARATT_IS_EXTERNAL(slot->tts_values[attindex]));
			Assert(!VARATT_IS_COMPRESSED(slot->tts_values[attindex]));
		}
#endif
		key[i] = slot->tts_values[attindex];
		isnull[i] = slot->tts_isnull[attindex];
	}

	result = o_form_tuple(GET_PRIMARY(descr)->nonLeafTupdesc, &GET_PRIMARY(descr)->nonLeafSpec,
						  ((OTableSlot *) slot)->version, key, isnull, NULL);
	pub static mut RESULT: return = std::mem::zeroed();
}

fn
alloc_to_toast_vfree_detoasted(slot: &mut TupleTableSlot)
{
	oslot: &mut OTableSlot = (OTableSlot *) slot;
	pub static mut TOTAL_NATTS: std::os::raw::c_int = slot->tts_tupleDescriptor->natts;

	Assert(!oslot->to_toast && !oslot->vfree);
	oslot->to_toast = MemoryContextAllocZero(slot->tts_mcxt,
											 MAXALIGN(sizeof(bool) * totalNatts * 2) +
											 sizeof(Datum) * totalNatts);
	oslot->vfree = (bool *) (oslot->to_toast + totalNatts);
	oslot->detoasted = (Datum *) ((Pointer) oslot->to_toast + MAXALIGN(sizeof(char) * totalNatts + sizeof(bool) * totalNatts));
}

//
// This function is designed to populate the attributes of a tuple table slot
// from an OrioleDB tuple.  It selectively retrieves attributes based on
// the provided number of attributes (__natts) and updates the slot's values
// and null flags accordingly.
//
fn
tts_orioledb_getsomeattrs(slot: &mut TupleTableSlot, int __natts)
{
	//
// Cast the generic TupleTableSlot to an OTableSlot for OrioleDB specific
// operations.
//
	oslot: &mut OTableSlot = (OTableSlot *) slot;

	// Declaration of variables used throughout the function.
	int			natts,
				attnum,
				ctid_off = 0;
	descr: &mut OTableDescr = oslot->descr;	// Descriptor for the table.
	values: &mut Datum = slot->tts_values;	// Array to store attribute
// values.
	isnull: &mut bool = slot->tts_isnull;	// Array to store null flags for
// attributes.
	bool		hastoast = false;	// Flag to indicate presence of TOASTed
// attributes.
	pub static mut O_INDEX_DESCR: *mut idx = std::ptr::null_mut();
	pub static mut INDEX_ORDER: bool = false;
	pub static mut CUR_TBL_ATTNUM: std::os::raw::c_int = 0;
	pub static mut BOOL: *mut isfilled = std::ptr::null_mut();

	//
// Early return if the requested number of attributes is already valid or
// the tuple is null.
//
	if (__natts <= slot->tts_nvalid || O_TUPLE_IS_NULL(oslot->tuple))
		return;

	// Ensure the descriptor is not NULL.
	Assert(descr);
	if (oslot->ixnum == BridgeIndexNumber)
		idx = descr->bridge;
	else
		idx = descr->indices[oslot->ixnum];

	// Determine if the attributes should be fetched in index order.
	index_order = slot->tts_tupleDescriptor->tdtypeid == RECORDOID;
	if (oslot->ixnum == PrimaryIndexNumber)
		index_order = index_order &&
			slot->tts_tupleDescriptor->natts == idx->nFields;

	//
// Ensure that if there are valid attributes, the slot is for the primary
// index.
//
	Assert(slot->tts_nvalid == 0 || oslot->ixnum == PrimaryIndexNumber);

	//
// Determine the offset of the attributes due to the possible presence of
// ctid column.
//
	if (GET_PRIMARY(descr)->primaryIsCtid && oslot->ixnum == PrimaryIndexNumber)
		ctid_off++;

	//
// Determine the offset of the attributes due to the possible presence of
// index_bridging_ctid column.
//
	if (GET_PRIMARY(descr)->bridging && oslot->ixnum == PrimaryIndexNumber)
		ctid_off++;

	//
// Determine the number of attributes to process based on the index type
// and the order of attributes.
//
	if (oslot->ixnum == PrimaryIndexNumber && oslot->leafTuple)
	{
		if (index_order)
		{
			//
// The attributes are stored in the index order.  So fetch all the
// attributes at once.
//
			natts = descr->tupdesc->natts;
		}
		else
		{
			natts = Min(__natts, descr->tupdesc->natts);
		}
	}
	else
	{
		//
// For secondary indexes, the attributes are also stored in the index
// order.  So fetch all the attributes at once.
//
		natts = oslot->state.desc->natts;
	}

	isfilled = MemoryContextAllocZero(slot->tts_mcxt, Max(natts, __natts));

	// Iterate over the attributes to populate values and null flags.
	for (attnum = slot->tts_nvalid; attnum < natts; attnum++)
	{
		pub static mut THISATT: Form_pg_attribute = std::mem::zeroed();
		pub static mut RES_ATTNUM: std::os::raw::c_int = 0;

		//
// Determine the result attribute number based on the index type and
// the order of attributes.
//
		if (oslot->ixnum == PrimaryIndexNumber)
		{
			if (index_order)
			{
				if (cur_tbl_attnum >= idx->nFields ||
					attnum != idx->pk_tbl_field_map[cur_tbl_attnum].key)
					res_attnum = -2;
				else
				{
					res_attnum = idx->pk_tbl_field_map[cur_tbl_attnum].value;
					cur_tbl_attnum++;
				}
			}
			else
			{
				//
// Map leaf position to table column position using
// tableAttnums.  For normal tables where index order matches
// table order this is identity.  For attached partitions with
// reordered columns, this correctly remaps to the right table
// column.
//
// tableAttnums values are 1-based and offset by +2 for
// ctid-based PKs or +1 otherwise (see o_index_fill_descr).
//
				res_attnum = (oslot->leafTuple) ? attnum : (idx->tableAttnums[attnum] - (idx->primaryIsCtid ? 2 : 1));
			}
		}
		else if (index_order)
		{
			if (GET_PRIMARY(descr)->primaryIsCtid && attnum == natts - 1)
				res_attnum = -1;
			else
				res_attnum = attnum;
		}
		else
		{
			Assert(false);
		}

		// Ensure the result attribute number is valid.
		Assert(res_attnum >= -2);
		if (res_attnum >= 0)
		{
			if (oslot->ixnum == BridgeIndexNumber && attnum == 0)
			{
				//
// first bridge_ctid attribute was already read in
// tts_orioledb_init_reader
//
				values[res_attnum] = PointerGetDatum(&oslot->bridge_ctid);
				isnull[res_attnum] = false;
				continue;
			}

			//
// Read the next field value and update the slot's value and null
// arrays.
//
			values[res_attnum] = o_tuple_read_next_field(&oslot->state,
														 &isnull[res_attnum]);
			isfilled[res_attnum] = true;

			// Determine the attribute metadata based on the index and order.
			if (oslot->ixnum == PrimaryIndexNumber && !index_order)
				thisatt = TupleDescAttr(slot->tts_tupleDescriptor, res_attnum);
			else
				thisatt = TupleDescAttr(idx->leafTupdesc, attnum);

			//
// Check for TOASTed attributes and adjust the number of
// attributes if necessary.
//
			if (!isnull[res_attnum] && !thisatt->attbyval && thisatt->attlen < 0)
			{
				Pointer		p = DatumGetPointer(values[res_attnum]);

				Assert(p);
				if (IS_TOAST_POINTER(p) && !VARATT_IS_EXTERNAL_ORIOLEDB(p))
				{
					hastoast = true;
					natts = Max(natts, idx->maxTableAttnum - ctid_off);
				}
			}
		}
		else if (res_attnum == -1)
		{
			if (!idx->bridging)
			{
				// Special handling for ctid attribute.
				pub static mut PG_USED_FOR_ASSERTS_ONLY: Datum		iptr_value = std::mem::zeroed();
				pub static mut IPTR_NULL: bool = false;

				iptr_value = o_tuple_read_next_field(&oslot->state,
													 &iptr_null);

				Assert(iptr_null == false);
				Assert(memcmp(&slot->tts_tid,
							  (ItemPointer) iptr_value, sizeof(ItemPointerData)) == 0);
			}
		}
		else if (res_attnum == -2)
		{
			// Handle dropped attributes by reading and ignoring the value.
			pub static mut DROPPED_NULL: bool = false;

			() o_tuple_read_next_field(&oslot->state, &dropped_null);
		}
	}

	// Process TOASTed attributes if any were found.
	if (hastoast)
	{
		pub static mut PKEY: OTuple = std::mem::zeroed();

		// Allocate memory for TOASTed attributes if not already done.
		if (!oslot->to_toast)
			alloc_to_toast_vfree_detoasted(slot);

		// Generate a primary key for the TOASTed attributes.
		if (oslot->ixnum == PrimaryIndexNumber)
			pkey = tts_orioledb_make_key(slot, descr);
		else
			pkey = make_key_from_secondary_slot(slot, idx, descr);

		// Iterate over attributes to process TOASTed values.
		for (attnum = 0; attnum < natts; attnum++)
		{
			pub static mut THISATT: Form_pg_attribute = std::mem::zeroed();

			thisatt = TupleDescAttr(slot->tts_tupleDescriptor, attnum);
			if (!isnull[attnum] && !thisatt->attbyval && thisatt->attlen < 0)
			{
				Pointer		p = DatumGetPointer(values[attnum]);

				if (IS_TOAST_POINTER(p))
				{
					// Replace TOASTed value with a detoasted version.
					MemoryContext mcxt = MemoryContextSwitchTo(slot->tts_mcxt);
					pub static mut TOAST_VALUE: OToastValue = std::mem::zeroed();

					memcpy(&toastValue, p, sizeof(toastValue));
					values[attnum] = create_o_toast_external(descr, pkey,
															 attnum + 1 + ctid_off,
															 &toastValue,
															 oslot->csn);
					isfilled[attnum] = true;
					oslot->vfree[attnum] = true;
					MemoryContextSwitchTo(mcxt);
				}
			}
		}
		// Free the primary key memory except for bump context
		if (!is_bump_memory_context(CurrentMemoryContext))
			pfree(pkey.data);
	}

	// Ensure the number of processed attributes matches the expected count.
	Assert(attnum == natts);

	{
		pub static mut FIRST_UNFILLED: std::os::raw::c_int = slot->tts_nvalid;

		for (attnum = slot->tts_nvalid; attnum < Max(natts, __natts); ++attnum)
		{
			if (isfilled[attnum])
			{
				slot_getmissingattrs(slot, first_unfilled, attnum);
				first_unfilled = attnum + 1;
			}
		}

		// Update the slot's valid attribute count.
		slot->tts_nvalid = first_unfilled;
	}

	if (!is_bump_memory_context(slot->tts_mcxt))
	{
		pfree(isfilled);
	}
}

static Datum
tts_orioledb_getsysattr(slot: &mut TupleTableSlot, int attnum, isnull: &mut bool)
{
	oslot: &mut OTableSlot = (OTableSlot *) slot;
	pub static mut FORM_DATA_PG_ATTRIBUTE: *mut const att = std::ptr::null_mut();

	ASAN_UNPOISON_MEMORY_REGION(isnull, sizeof(*isnull));

	if (attnum == RowIdAttributeNumber)
	{
		Datum		values[2 * INDEX_MAX_KEYS];
		bool		isnulls[2 * INDEX_MAX_KEYS];
		pub static mut BYTEA: *mut result = std::ptr::null_mut();
		pub static mut O_TABLE_DESCR: *mut descr = oslot->descr;
		pub static mut O_INDEX_DESCR: *mut primary = std::ptr::null_mut();
		pub static mut CTID_OFF: std::os::raw::c_int = 0;

		if (oslot->rowid)
		{
			*isnull = false;
			return datumCopy(PointerGetDatum(oslot->rowid), false, -1);
		}

		if (!descr)
		{
			*isnull = true;
			return (Datum) 0;
		}

		primary = GET_PRIMARY(descr);
		ctid_off = primary->primaryIsCtid ? 1 : 0;

		if (!primary->primaryIsCtid)
		{
			tts_orioledb_getsomeattrs(slot, primary->maxTableAttnum - ctid_off);
			tts_orioledb_get_index_values(slot, primary, values, isnulls, false);
		}
		result = o_new_rowid(primary, slot, values, isnulls,
							 oslot->csn, &oslot->hint);

		*isnull = false;
		oslot->rowid = result;
		return datumCopy(PointerGetDatum(result), false, -1);
	}

	att = SystemAttributeDefinition(attnum);
	elog(ERROR, "orioledb tuples does not have system attribute: %s",
		 att->attname.data);

	return 0;					// silence compiler warnings
}

//
// To materialize a virtual slot all the datums that aren't passed by value
// have to be copied into the slot's memory context.  To do so, compute the
// required size, and allocate enough memory to store all attributes.  That's
// good for cache hit ratio, but more importantly requires only memory
// allocation/deallocation.
//
fn
tts_orioledb_materialize(slot: &mut TupleTableSlot)
{
	oslot: &mut OTableSlot = (OTableSlot *) slot;
	pub static mut DESC: TupleDesc = slot->tts_tupleDescriptor;
	pub static mut SZ: Size = 0;
	pub static mut CHAR: *mut data = std::ptr::null_mut();

	// already materialized
	if (TTS_SHOULDFREE(slot))
		return;

	slot_getallattrs(slot);

	// compute size of memory required
	for (int natt = 0; natt < desc->natts; natt++)
	{
		Form_pg_attribute att = TupleDescAttr(desc, natt);
		pub static mut VAL: Datum = std::mem::zeroed();

		if (att->attbyval || slot->tts_isnull[natt])
			continue;

		val = slot->tts_values[natt];

		if (att->attlen == -1 &&
			VARATT_IS_EXTERNAL_EXPANDED(DatumGetPointer(val)))
		{
			//
// We want to flatten the expanded value so that the materialized
// slot doesn't depend on it.
//
			sz = att_align_nominal(sz, att->attalign);
			sz += EOH_get_flat_size(DatumGetEOHP(val));
		}
		else
		{
			sz = att_align_nominal(sz, att->attalign);
			sz = att_addlength_datum(sz, att->attlen, val);
		}
	}

	// all data is byval
	if (sz == 0)
		return;

	// allocate memory
	oslot->data = data = MemoryContextAlloc(slot->tts_mcxt, sz);
	slot->tts_flags |= TTS_FLAG_SHOULDFREE;

	// and copy all attributes into the pre-allocated space
	for (int natt = 0; natt < desc->natts; natt++)
	{
		Form_pg_attribute att = TupleDescAttr(desc, natt);
		pub static mut VAL: Datum = std::mem::zeroed();

		if (att->attbyval || slot->tts_isnull[natt])
			continue;

		val = slot->tts_values[natt];

		if (att->attlen == -1 &&
			VARATT_IS_EXTERNAL_EXPANDED(DatumGetPointer(val)))
		{
			pub static mut DATA_LENGTH: Size = 0;

			//
// We want to flatten the expanded value so that the materialized
// slot doesn't depend on it.
//
			eoh: &mut ExpandedObjectHeader = DatumGetEOHP(val);

			data = (char *) att_align_nominal(data,
											  att->attalign);
			data_length = EOH_get_flat_size(eoh);
			EOH_flatten_into(eoh, data, data_length);

			slot->tts_values[natt] = PointerGetDatum(data);
			data += data_length;
		}
		else
		{
			pub static mut DATA_LENGTH: Size = 0;

			data = (char *) att_align_nominal(data, att->attalign);
			data_length = att_addlength_datum(data_length, att->attlen, val);

			memcpy(data, DatumGetPointer(val), data_length);

			slot->tts_values[natt] = PointerGetDatum(data);
			data += data_length;
		}
	}

	if (oslot->to_toast)
	{
		memset(oslot->vfree, 0, desc->natts * sizeof(bool));
		memset(oslot->to_toast, 0, desc->natts * sizeof(char));
	}
}


tts_orioledb_detoast(slot: &mut TupleTableSlot)
{
	oslot: &mut OTableSlot = (OTableSlot *) slot;
	pub static mut TUPLE_DESC: TupleDesc = slot->tts_tupleDescriptor;
	pub static mut NATTS: std::os::raw::c_int = tupleDesc->natts;
	pub static mut I: std::os::raw::c_int = 0;

	slot_getallattrs(slot);

	for (i = 0; i < natts; i++)
	{
		Form_pg_attribute att = TupleDescAttr(tupleDesc, i);
		pub static mut TMP: Datum = std::mem::zeroed();

		if (!slot->tts_isnull[i] && att->attlen == -1 &&
			VARATT_IS_EXTENDED(slot->tts_values[i]))
		{
			pub static mut MCTX: MemoryContext = std::mem::zeroed();

			if (!oslot->vfree)
				alloc_to_toast_vfree_detoasted(slot);

			mctx = MemoryContextSwitchTo(slot->tts_mcxt);
			tmp = PointerGetDatum(PG_DETOAST_DATUM(slot->tts_values[i]));
			MemoryContextSwitchTo(mctx);
			Assert(slot->tts_values[i] != tmp);
			if (oslot->vfree[i])
				pfree(DatumGetPointer(slot->tts_values[i]));
			slot->tts_values[i] = tmp;
			oslot->vfree[i] = true;
		}
	}
}

fn
tts_orioledb_copyslot(dstslot: &mut TupleTableSlot, srcslot: &mut TupleTableSlot)
{
	pub static mut SRCDESC: TupleDesc = srcslot->tts_tupleDescriptor;
	dstoslot: &mut OTableSlot = (OTableSlot *) dstslot;

	Assert(srcdesc->natts <= dstslot->tts_tupleDescriptor->natts);

	if (srcslot->tts_ops == &TTSOpsOrioleDB &&
		(((OTableSlot *) srcslot)->descr == dstoslot->descr ||
		 ((OTableSlot *) dstslot)->descr == NULL))
	{
		srcoslot: &mut OTableSlot = (OTableSlot *) srcslot;

		tts_orioledb_clear(dstslot);
		dstoslot->version = srcoslot->version;
		if (!O_TUPLE_IS_NULL(srcoslot->tuple))
		{
			MemoryContext mctx = MemoryContextSwitchTo(dstslot->tts_mcxt);
			pub static mut TUP: OTuple = srcoslot->tuple;
			uint32		tupLen = o_tuple_size(tup, &GET_PRIMARY(srcoslot->descr)->leafSpec);

			dstoslot->tuple.data = (Pointer) palloc(tupLen);
			memcpy(dstoslot->tuple.data, srcoslot->tuple.data, tupLen);
			dstoslot->tuple.formatFlags = srcoslot->tuple.formatFlags;
			dstoslot->descr = srcoslot->descr;
			if (srcoslot->rowid)
			{
				dstoslot->rowid = (bytea *) palloc(VARSIZE_ANY(srcoslot->rowid));
				memcpy(dstoslot->rowid, srcoslot->rowid,
					   VARSIZE_ANY(srcoslot->rowid));
			}
			MemoryContextSwitchTo(mctx);
			dstslot->tts_flags &= ~TTS_FLAG_EMPTY;
			dstslot->tts_flags |= TTS_FLAG_SHOULDFREE;
			dstslot->tts_nvalid = 0;
			dstoslot->csn = srcoslot->csn;
			dstoslot->ixnum = srcoslot->ixnum;
			dstoslot->leafTuple = srcoslot->leafTuple;
			tts_orioledb_init_reader(dstslot);
			return;
		}
	}

	tts_orioledb_clear(dstslot);
	slot_getallattrs(srcslot);

	for (int natt = 0; natt < srcdesc->natts; natt++)
	{
		dstslot->tts_values[natt] = srcslot->tts_values[natt];
		dstslot->tts_isnull[natt] = srcslot->tts_isnull[natt];
	}

	dstslot->tts_nvalid = srcdesc->natts;
	dstslot->tts_flags &= ~TTS_FLAG_EMPTY;

	// make sure storage doesn't depend on external memory
	tts_orioledb_materialize(dstslot);
}

static HeapTuple
tts_orioledb_copy_heap_tuple(slot: &mut TupleTableSlot)
{
	pub static mut RESULT: HeapTuple = std::mem::zeroed();

	Assert(!TTS_EMPTY(slot));

	slot_getallattrs(slot);

	result = heap_form_tuple(slot->tts_tupleDescriptor,
							 slot->tts_values,
							 slot->tts_isnull);

	ItemPointerCopy(&slot->tts_tid, &result->t_self);

	pub static mut RESULT: return = std::mem::zeroed();
}

static MinimalTuple
#if PG_VERSION_NUM >= 180000
tts_orioledb_copy_minimal_tuple(slot: &mut TupleTableSlot, Size extra)
#else
tts_orioledb_copy_minimal_tuple(slot: &mut TupleTableSlot)
#endif
{
	Assert(!TTS_EMPTY(slot));

	slot_getallattrs(slot);

	return heap_form_minimal_tuple(slot->tts_tupleDescriptor,
								   slot->tts_values,
#if PG_VERSION_NUM >= 180000
								   slot->tts_isnull, extra);
#else
								   slot->tts_isnull);
#endif
}

fn
tts_orioledb_init_reader(slot: &mut TupleTableSlot)
{
	oslot: &mut OTableSlot = (OTableSlot *) slot;
	pub static mut O_INDEX_DESCR: *mut idx = std::ptr::null_mut();

	if (oslot->ixnum == BridgeIndexNumber)
		idx = oslot->descr->bridge;
	else
		idx = oslot->descr->indices[oslot->ixnum];

	if (oslot->leafTuple)
		o_tuple_init_reader(&oslot->state, oslot->tuple,
							idx->leafTupdesc, &idx->leafSpec);
	else
		o_tuple_init_reader(&oslot->state, oslot->tuple,
							idx->nonLeafTupdesc, &idx->nonLeafSpec);

	if (idx->primaryIsCtid)
	{
		if (oslot->ixnum == PrimaryIndexNumber && oslot->leafTuple)
		{
			pub static mut VALUE: Datum = std::mem::zeroed();
			pub static mut ISNULL: bool = false;

			value = o_tuple_read_next_field(&oslot->state, &isnull);
			slot->tts_tid = *((ItemPointer) value);
		}
		else if (!(idx->bridging && oslot->leafTuple &&
				   (oslot->ixnum == BridgeIndexNumber || oslot->ixnum == PrimaryIndexNumber)))
		{
			pub static mut IPTR: ItemPointer = std::mem::zeroed();
			pub static mut ISNULL: bool = false;

			if (oslot->leafTuple)
				iptr = o_tuple_get_last_iptr(idx->leafTupdesc, &idx->leafSpec,
											 oslot->tuple, &isnull);
			else
				iptr = o_tuple_get_last_iptr(idx->nonLeafTupdesc,
											 &idx->nonLeafSpec,
											 oslot->tuple, &isnull);
			Assert(!isnull && iptr);
			slot->tts_tid = *iptr;
		}
	}

	if (idx->bridging && oslot->leafTuple && (oslot->ixnum == BridgeIndexNumber || oslot->ixnum == PrimaryIndexNumber))
	{
		pub static mut VALUE: Datum = std::mem::zeroed();
		pub static mut ISNULL: bool = false;

		value = o_tuple_read_next_field(&oslot->state, &isnull);
		oslot->bridge_ctid = *((ItemPointer) value);
	}

	slot->tts_tableOid = oslot->descr->oids.reloid;
}

fn
tts_orioledb_store_tuple_internal(slot: &mut TupleTableSlot, OTuple tuple,
								  descr: &mut OTableDescr, CommitSeqNo csn,
								  int ixnum, bool leafTuple, bool shouldfree,
								  hint: &mut BTreeLocationHint)
{
	oslot: &mut OTableSlot = (OTableSlot *) slot;

	Assert(COMMITSEQNO_IS_NORMAL(csn) || COMMITSEQNO_IS_INPROGRESS(csn));
	Assert(slot->tts_ops == &TTSOpsOrioleDB);

	tts_orioledb_clear(slot);

	Assert(!TTS_SHOULDFREE(slot));
	Assert(TTS_EMPTY(slot));

	slot->tts_flags &= ~TTS_FLAG_EMPTY;
	slot->tts_nvalid = 0;

	oslot->tuple = tuple;
	oslot->descr = descr;
	oslot->csn = csn;
	oslot->ixnum = ixnum;
	oslot->leafTuple = leafTuple;
	oslot->version = o_tuple_get_version(tuple);

	if (hint)
		oslot->hint = *hint;

	tts_orioledb_init_reader(slot);

	if (shouldfree)
		slot->tts_flags |= TTS_FLAG_SHOULDFREE;
}


tts_orioledb_store_tuple(slot: &mut TupleTableSlot, OTuple tuple,
						 descr: &mut OTableDescr, CommitSeqNo csn,
						 int ixnum, bool shouldfree, hint: &mut BTreeLocationHint)
{
	tts_orioledb_store_tuple_internal(slot, tuple, descr, csn, ixnum, true,
									  shouldfree, hint);
}


tts_orioledb_store_non_leaf_tuple(slot: &mut TupleTableSlot, OTuple tuple,
								  descr: &mut OTableDescr, CommitSeqNo csn,
								  int ixnum, bool shouldfree,
								  hint: &mut BTreeLocationHint)
{
	tts_orioledb_store_tuple_internal(slot, tuple, descr, csn, ixnum, false,
									  shouldfree, hint);
}

Datum
o_get_tbl_att(slot: &mut TupleTableSlot, int attnum, bool primaryIsCtid,
			  isnull: &mut bool, typid: &mut Oid, bool decompress)
{
	pub static mut I: std::os::raw::c_int = 0;
	pub static mut VALUE: Datum = std::mem::zeroed();
	pub static mut ATT: Form_pg_attribute = std::mem::zeroed();
	oSlot: &mut OTableSlot = (OTableSlot *) slot;

	if (primaryIsCtid)
	{
		if (attnum == 1)
		{
			*isnull = false;
			if (typid)
				*typid = TIDOID;
			return PointerGetDatum(&slot->tts_tid);
		}
		else if (attnum == -1)
		{
			*isnull = false;
			if (typid)
				*typid = TIDOID;
			return PointerGetDatum(&oSlot->bridge_ctid);
		}
		else
		{
			i = attnum - 2;
		}
	}
	else
	{
		if (attnum == -1)
		{
			*isnull = false;
			if (typid)
				*typid = TIDOID;
			return PointerGetDatum(&oSlot->bridge_ctid);
		}
		else
			i = attnum - 1;
	}

	att = TupleDescAttr(slot->tts_tupleDescriptor, i);
	if (typid)
		*typid = att->atttypid;
	*isnull = slot->tts_isnull[i];
	value = slot->tts_values[i];

	if (!*isnull && att->attlen < 0 &&
		(VARATT_IS_EXTENDED(value) && (decompress || !VARATT_IS_COMPRESSED(value))))
	{
		if (!oSlot->to_toast)
			alloc_to_toast_vfree_detoasted(&oSlot->base);

		if (!oSlot->detoasted[i])
		{
			MemoryContext mcxt = MemoryContextSwitchTo(slot->tts_mcxt);

			oSlot->detoasted[i] = PointerGetDatum(PG_DETOAST_DATUM(value));
			MemoryContextSwitchTo(mcxt);

		}
		value = oSlot->detoasted[i];
	}
	pub static mut VALUE: return = std::mem::zeroed();
}

Datum
o_get_idx_expr_att(slot: &mut TupleTableSlot, idx: &mut OIndexDescr,
				   exp_state: &mut ExprState, isnull: &mut bool)
{
	pub static mut RESULT: Datum = std::mem::zeroed();

	idx->econtext->ecxt_scantuple = slot;

	result = ExecEvalExprSwitchContext(exp_state,
									   idx->econtext, isnull);
	pub static mut RESULT: return = std::mem::zeroed();
}

//
// Prepares values for index tuple.  Works for leaf and non-leaf tuples of
// secondary index and non-leaf tuple of primary index.
//
// Detoasts all the values and marks detoasted values in 'detoasted' array.
// If 'detoasted' array isn't given, asserts not values are toasted.
//
fn
tts_orioledb_get_index_values(slot: &mut TupleTableSlot, idx: &mut OIndexDescr,
							  values: &mut Datum, isnull: &mut bool, bool leaf)
{
	pub static mut TUPLE_DESC: TupleDesc = leaf ? idx->leafTupdesc : idx->nonLeafTupdesc;
	pub static mut NATTS: std::os::raw::c_int = tupleDesc->natts;
	pub static mut I: std::os::raw::c_int = 0;
	indexpr_item: &mut ListCell = list_head(idx->expressions_state);

	Assert(natts <= 2 * INDEX_MAX_KEYS);

	for (i = 0; i < natts; i++)
	{
		pub static mut ATTNUM: std::os::raw::c_int = idx->tableAttnums[i];

		if (attnum != EXPR_ATTNUM)
			values[i] = o_get_tbl_att(slot, attnum, idx->primaryIsCtid,
									  &isnull[i], NULL, false);
		else
		{
			values[i] = o_get_idx_expr_att(slot, idx,
										   (ExprState *) lfirst(indexpr_item),
										   &isnull[i]);
			indexpr_item = lnext(idx->expressions_state, indexpr_item);
		}
	}
}

OTuple
tts_orioledb_make_secondary_tuple(slot: &mut TupleTableSlot, idx: &mut OIndexDescr, bool leaf)
{
	Datum		values[2 * INDEX_MAX_KEYS];
	bool		isnull[2 * INDEX_MAX_KEYS];
	pub static mut TUPLE_DESC: TupleDesc = std::mem::zeroed();
	pub static mut O_TUPLE_FIXED_FORMAT_SPEC: *mut spec = std::ptr::null_mut();
	pub static mut CTID_OFF: std::os::raw::c_int = idx->primaryIsCtid ? 1 : 0;
	oslot: &mut OTableSlot = (OTableSlot *) slot;
	pub static mut BRIDGE_DATA: BridgeData = std::mem::zeroed();
	pub static mut BRIDGE_DATA: *mut bridge_data_arg = std::ptr::null_mut();

	slot_getsomeattrs(slot, idx->maxTableAttnum - ctid_off);

	tts_orioledb_get_index_values(slot, idx, values, isnull, leaf);

	if (leaf)
	{
		tupleDesc = idx->leafTupdesc;
		spec = &idx->leafSpec;
	}
	else
	{
		tupleDesc = idx->nonLeafTupdesc;
		spec = &idx->nonLeafSpec;
	}

	if (leaf && idx->bridging && idx->desc.type == oIndexBridge)
	{
		bridge_data.bridge_iptr = &oslot->bridge_ctid;
		bridge_data.is_pkey = false;
		bridge_data.attnum = 1;
		bridge_data_arg = &bridge_data;
	}

	return o_form_tuple(tupleDesc, spec, 0, values, isnull, bridge_data_arg);
}

// fills key bound from tuple or index tuple that belongs to current BTree

tts_orioledb_fill_key_bound(slot: &mut TupleTableSlot, idx: &mut OIndexDescr,
							bound: &mut OBTreeKeyBound)
{
	pub static mut I: std::os::raw::c_int = 0;
	pub static mut CTID_OFF: std::os::raw::c_int = idx->primaryIsCtid ? 1 : 0;
	indexpr_item: &mut ListCell = list_head(idx->expressions_state);

	slot_getsomeattrs(slot, idx->maxTableAttnum - ctid_off);

	bound->nkeys = idx->nonLeafTupdesc->natts;
	for (i = 0; i < bound->nkeys; i++)
	{
		pub static mut VALUE: Datum = std::mem::zeroed();
		pub static mut ISNULL: bool = false;
		pub static mut ATTNUM: std::os::raw::c_int = 0;
		pub static mut TYPID: Oid = std::mem::zeroed();

		attnum = idx->tableAttnums[i];

		if (attnum != EXPR_ATTNUM)
			value = o_get_tbl_att(slot, attnum, idx->primaryIsCtid,
								  &isnull, &typid, true);
		else
		{
			value = o_get_idx_expr_att(slot, idx,
									   (ExprState *) lfirst(indexpr_item),
									   &isnull);
			typid = TupleDescAttr(idx->nonLeafTupdesc, i)->atttypid;
			indexpr_item = lnext(idx->expressions_state, indexpr_item);
		}

		bound->keys[i].value = value;
		bound->keys[i].type = typid;
		bound->keys[i].flags = O_VALUE_BOUND_PLAIN_VALUE;
		if (isnull)
			bound->keys[i].flags |= O_VALUE_BOUND_NULL;
		bound->keys[i].comparator = idx->fields[i].comparator;
		bound->keys[i].exclusion_fn = NULL;
	}
}

//
// Appends index key stored in the tuple slot to the given string.
//

appendStringInfoIndexKey(StringInfo str, slot: &mut TupleTableSlot, id: &mut OIndexDescr)
{
	pub static mut I: std::os::raw::c_int = 0;
	indexpr_item: &mut ListCell = list_head(id->expressions_state);

	slot_getallattrs(slot);

	appendStringInfo(str, "(");
	for (i = 0; i < id->nUniqueFields; i++)
	{
		pub static mut VALUE: Datum = std::mem::zeroed();
		pub static mut ISNULL: bool = false;
		pub static mut ATTNUM: std::os::raw::c_int = id->tableAttnums[i];

		if (attnum != EXPR_ATTNUM)
			value = o_get_tbl_att(slot, attnum, id->primaryIsCtid,
								  &isnull, NULL, true);
		else
		{
			value = o_get_idx_expr_att(slot, id,
									   (ExprState *) lfirst(indexpr_item),
									   &isnull);
			indexpr_item = lnext(id->expressions_state, indexpr_item);
		}

		if (i != 0)
			appendStringInfo(str, ", ");
		if (isnull)
			appendStringInfo(str, "null");
		else
		{
			pub static mut TYPOUTPUT: Oid = std::mem::zeroed();
			pub static mut TYPISVARLENA: bool = false;
			pub static mut CHAR: *mut res = std::ptr::null_mut();

			getTypeOutputInfo(TupleDescAttr(id->nonLeafTupdesc, i)->atttypid,
							  &typoutput, &typisvarlena);
			res = OidOutputFunctionCall(typoutput, value);
			appendStringInfo(str, "%s", res);
		}
	}
	appendStringInfo(str, ")");
}

//
// Returns a string representation of the index key that is stored in the
// tuple slot.
//
char *
tss_orioledb_print_idx_key(slot: &mut TupleTableSlot, id: &mut OIndexDescr)
{
	pub static mut BUF: StringInfoData = std::mem::zeroed();

	initStringInfo(&buf);
	appendStringInfoIndexKey(&buf, slot, id);

	return buf.data;
}

//
// Returns the expected length of the tuple that will be stored in the primary
// key index.
//
static inline int
expected_tuple_len(slot: &mut TupleTableSlot, descr: &mut OTableDescr)
{
	oslot: &mut OTableSlot = (OTableSlot *) slot;
	idx: &mut OIndexDescr = GET_PRIMARY(descr);
	pub static mut TUP_SIZE: std::os::raw::c_int = 0;
	pub static mut BRIDGE_DATA: BridgeData = std::mem::zeroed();
	pub static mut BRIDGE_DATA: *mut bridge_data_arg = std::ptr::null_mut();

	if (idx->bridging)
	{
		bridge_data.bridge_iptr = &oslot->bridge_ctid;
		bridge_data.is_pkey = true;
		bridge_data.attnum = idx->primaryIsCtid ? 2 : 1;
		bridge_data_arg = &bridge_data;
	}
	tup_size = o_new_tuple_size(idx->leafTupdesc,
								&idx->leafSpec,
								idx->primaryIsCtid ? &slot->tts_tid : NULL,
								bridge_data_arg,
								oslot->version,
								slot->tts_values,
								slot->tts_isnull,
								oslot->to_toast);

	pub static mut TUP_SIZE: return = std::mem::zeroed();
}

//
// Returns true if the tuple stored in the slot fits the maximum size to be
// stored in the index.
//
static inline bool
can_be_stored_in_index(slot: &mut TupleTableSlot, descr: &mut OTableDescr)
{
	int			tup_size = expected_tuple_len(slot, descr);

	Assert(tup_size > 0);

	if (tup_size <= O_BTREE_MAX_TUPLE_SIZE)
		pub static mut TRUE: return = std::mem::zeroed();
	pub static mut FALSE: return = std::mem::zeroed();
}

//
// Apply TOAST including compression and out-of-line storage to the tuple
// stored in the slot if necessary.
//

tts_orioledb_toast(slot: &mut TupleTableSlot, descr: &mut OTableDescr)
{
	oslot: &mut OTableSlot = (OTableSlot *) slot;
	pub static mut ATT: Form_pg_attribute = std::mem::zeroed();
	int			i,
				full_size = 0,
				to_toastn,
				natts;
	pub static mut TOAST_ATTN: AttrNumber = std::mem::zeroed();
	pub static mut HAS_TOASTED: bool = false;
	pub static mut TUPDESC: TupleDesc = slot->tts_tupleDescriptor;
	pub static mut PRIMARY_IS_CTID: bool = false;
	pub static mut CTID_OFF: std::os::raw::c_int = 0;

	primaryIsCtid = GET_PRIMARY(descr)->primaryIsCtid;
	ctid_off = primaryIsCtid ? 1 : 0;

	if (GET_PRIMARY(descr)->bridging)
		ctid_off++;

	slot_getallattrs(slot);

	// temporary, pointers to TupleDesc attributes
	natts = tupdesc->natts;
	for (i = 0; i < natts; i++)
	{
		att = TupleDescAttr(tupdesc, i);
		if (att->attlen <= 0 && !slot->tts_isnull[i]
			&& (VARATT_IS_EXTERNAL_ONDISK(slot->tts_values[i]) ||
				VARATT_IS_EXTERNAL_ORIOLEDB(slot->tts_values[i])))
			has_toasted = true;
	}

	if (!has_toasted)
		full_size = expected_tuple_len(slot, descr);

	// we do not need use TOAST
	if (full_size <= O_BTREE_MAX_TUPLE_SIZE && !has_toasted)
	{
		return;
	}

	// if we there than tuple's values should be TOASTed or compressed
	if (!oslot->to_toast)
		alloc_to_toast_vfree_detoasted(slot);

	full_size = 0;
	for (i = 0; i < descr->ntoastable; i++)
		oslot->to_toast[descr->toastable[i] - ctid_off] = ORIOLEDB_TO_TOAST_ON;

	full_size = expected_tuple_len(slot, descr);

	memset(oslot->to_toast, ORIOLEDB_TO_TOAST_OFF, sizeof(bool) * natts);

	// if we can not compress tuple, we do not try do it
	if (full_size > O_BTREE_MAX_TUPLE_SIZE)
	{
		return;
	}

	//
// If we there than we must calculate which values should be compressed or
// TOASTed.
//
	to_toastn = 0;
	// to make it easy now all values must be reTOASTed
	for (i = 0; i < descr->ntoastable; i++)
	{
		toast_attn = descr->toastable[i] - ctid_off;

		if (slot->tts_isnull[toast_attn])
			continue;

		if (VARATT_IS_EXTERNAL_ONDISK(slot->tts_values[toast_attn]) ||
			VARATT_IS_EXTERNAL_ORIOLEDB(slot->tts_values[toast_attn]))
		{
			oslot->to_toast[toast_attn] = ORIOLEDB_TO_TOAST_ON;
			to_toastn++;
		}
	}

	while (to_toastn < descr->ntoastable &&
		   !can_be_stored_in_index(slot, descr))
	{
		pub static mut TMP: Datum = std::mem::zeroed();
		int			max = 0,
					max_attn = -1,
					var_size;
		pub static mut OLD_MCTX: MemoryContext = std::mem::zeroed();

		// search max unprocessed value
		for (i = 0; i < descr->ntoastable; i++)
		{
			toast_attn = descr->toastable[i] - ctid_off;
			if (!slot->tts_isnull[toast_attn] &&
				oslot->to_toast[toast_attn] == ORIOLEDB_TO_TOAST_OFF)
			{
				att = TupleDescAttr(tupdesc, toast_attn);

				Assert(att->attstorage != TYPSTORAGE_PLAIN);

				if (att->attstorage == TYPSTORAGE_MAIN &&
					VARATT_IS_COMPRESSED(slot->tts_values[toast_attn]))
					continue;

				var_size = VARSIZE_ANY(slot->tts_values[toast_attn]);
				if (var_size > max)
				{
					max = var_size;
					max_attn = toast_attn;
				}
			}
			// else we already process it or it is NULL
		}

		// we have no values which can be toasted
		if (max_attn == -1)
			break;

		att = TupleDescAttr(tupdesc, max_attn);

		//
// If the value is already compressed or can not be compressed - it
// must be toasted
//
		if (VARATT_IS_COMPRESSED(slot->tts_values[max_attn])
			|| att->attstorage == TYPSTORAGE_EXTERNAL)
		{
			oslot->to_toast[max_attn] = ORIOLEDB_TO_TOAST_ON;
			to_toastn++;
			continue;
		}

		oldMctx = MemoryContextSwitchTo(slot->tts_mcxt);
		tmp = toast_compress_datum(slot->tts_values[max_attn],
								   TOAST_PGLZ_COMPRESSION);
		MemoryContextSwitchTo(oldMctx);

		if (DatumGetPointer(tmp) != NULL)
		{
			// Suceessfully compressed, replace the value

			// free the old value
			if (oslot->vfree[max_attn])
				pfree(DatumGetPointer(slot->tts_values[max_attn]));
			// store the new value and mark to free it later
			slot->tts_values[max_attn] = tmp;
			oslot->vfree[max_attn] = true;
		}
		else if (att->attstorage != TYPSTORAGE_MAIN)
		{
			// Compression failed, try to TOAST it
			oslot->to_toast[max_attn] = ORIOLEDB_TO_TOAST_ON;
			to_toastn++;
		}
		else
		{
			//
// Compression failed for STORAGE MAIN attribute. Mark it as
// compression-tried for now; we may need to force out-of-line
// storage below if the tuple still doesn't fit.
//
			Assert(att->attstorage == TYPSTORAGE_MAIN);
			oslot->to_toast[max_attn] = ORIOLEDB_TO_TOAST_COMPRESSION_TRIED;
			to_toastn++;
		}
	}

	//
// If the tuple is still oversized after compression attempts, we need to
// force STORAGE MAIN attributes to be stored out-of-line in the TOAST
// table. Process them largest-first to minimize the number of attributes
// that need out-of-line storage.
//
	while (!can_be_stored_in_index(slot, descr))
	{
		pub static mut MAX: std::os::raw::c_int = 0;
		pub static mut MAX_ATTN: std::os::raw::c_int = -1;
		pub static mut VAR_SIZE: std::os::raw::c_int = 0;

		for (i = 0; i < descr->ntoastable; i++)
		{
			toast_attn = descr->toastable[i] - ctid_off;

			if (slot->tts_isnull[toast_attn])
				continue;

			// Skip attributes already marked for out-of-line storage
			if (oslot->to_toast[toast_attn] == ORIOLEDB_TO_TOAST_ON)
				continue;

			att = TupleDescAttr(tupdesc, toast_attn);

			// Only consider STORAGE MAIN attributes in this pass
			if (att->attstorage != TYPSTORAGE_MAIN)
				continue;

			var_size = VARSIZE_ANY(slot->tts_values[toast_attn]);
			if (var_size > max)
			{
				max = var_size;
				max_attn = toast_attn;
			}
		}

		// No more MAIN attributes to toast - nothing more we can do
		if (max_attn == -1)
			break;

		oslot->to_toast[max_attn] = ORIOLEDB_TO_TOAST_ON;
	}

	//
// Reset any remaining COMPRESSION_TRIED flags to OFF. These are MAIN
// attributes that were compressed or didn't need out-of-line storage.
//
	for (i = 0; i < descr->ntoastable; i++)
	{
		toast_attn = descr->toastable[i] - ctid_off;
		if (oslot->to_toast[toast_attn] == ORIOLEDB_TO_TOAST_COMPRESSION_TRIED)
			oslot->to_toast[toast_attn] = ORIOLEDB_TO_TOAST_OFF;
	}
}

OTuple
tts_orioledb_form_tuple(slot: &mut TupleTableSlot,
						descr: &mut OTableDescr)
{
	oslot: &mut OTableSlot = (OTableSlot *) slot;
	OTuple		tuple;			// return tuple
	pub static mut LEN: Size = 0;
	idx: &mut OIndexDescr = GET_PRIMARY(descr);
	pub static mut TUPLE_DESCRIPTOR: TupleDesc = idx->leafTupdesc;
	pub static mut O_TUPLE_FIXED_FORMAT_SPEC: *mut spec = &idx->leafSpec;
	pub static mut PRIMARY_IS_CTID: bool = idx->primaryIsCtid;
	pub static mut IPTR: ItemPointer = std::mem::zeroed();
	pub static mut BRIDGE_DATA: BridgeData = std::mem::zeroed();
	pub static mut BRIDGE_DATA: *mut bridge_data_arg = std::ptr::null_mut();

	if (!O_TUPLE_IS_NULL(oslot->tuple) && oslot->descr == descr &&
		oslot->ixnum == PrimaryIndexNumber && oslot->leafTuple)
		return oslot->tuple;

	if (idx->leafTupdesc->natts > MaxTupleAttributeNumber)
		ereport(ERROR,
				(errcode(ERRCODE_TOO_MANY_COLUMNS),
				 errmsg("number of columns (%d) exceeds limit (%d)",
						idx->leafTupdesc->natts, MaxTupleAttributeNumber)));

	if (primaryIsCtid)
		iptr = &slot->tts_tid;
	else
		iptr = NULL;

	if (idx->bridging && (idx->desc.type == oIndexPrimary || idx->desc.type == oIndexBridge))
	{
		bridge_data.bridge_iptr = &oslot->bridge_ctid;
		bridge_data.is_pkey = idx->desc.type == oIndexPrimary;
		bridge_data.attnum = idx->desc.type == oIndexBridge ? 1 : idx->primaryIsCtid ? 2 : 1;
		bridge_data_arg = &bridge_data;
	}

	len = o_new_tuple_size(tupleDescriptor, spec, iptr, bridge_data_arg,
						   0, slot->tts_values, slot->tts_isnull,
						   oslot->to_toast);

	tuple.data = (Pointer) MemoryContextAllocZero(slot->tts_mcxt, len);

	o_tuple_fill(tupleDescriptor, spec, &tuple, len,
				 iptr, bridge_data_arg, 0,
				 slot->tts_values, slot->tts_isnull, oslot->to_toast);

	oslot->tuple = tuple;
	oslot->descr = descr;
	oslot->ixnum = PrimaryIndexNumber;
	oslot->leafTuple = true;
	slot->tts_flags |= TTS_FLAG_SHOULDFREE;
	tts_orioledb_init_reader(slot);

	pub static mut TUPLE: return = std::mem::zeroed();
}

OTuple
tts_orioledb_form_orphan_tuple(slot: &mut TupleTableSlot,
							   descr: &mut OTableDescr)
{
	oslot: &mut OTableSlot = (OTableSlot *) slot;
	pub static mut TUPLE: OTuple = std::mem::zeroed();
	pub static mut LEN: Size = 0;
	idx: &mut OIndexDescr = GET_PRIMARY(descr);
	pub static mut TUPLE_DESCRIPTOR: TupleDesc = idx->leafTupdesc;
	pub static mut O_TUPLE_FIXED_FORMAT_SPEC: *mut spec = &idx->leafSpec;
	pub static mut PRIMARY_IS_CTID: bool = idx->primaryIsCtid;
	pub static mut IPTR: ItemPointer = std::mem::zeroed();
	pub static mut BRIDGE_DATA: BridgeData = std::mem::zeroed();
	pub static mut BRIDGE_DATA: *mut bridge_data_arg = std::ptr::null_mut();

	if (idx->leafTupdesc->natts > MaxTupleAttributeNumber)
		ereport(ERROR,
				(errcode(ERRCODE_TOO_MANY_COLUMNS),
				 errmsg("number of columns (%d) exceeds limit (%d)",
						idx->leafTupdesc->natts, MaxTupleAttributeNumber)));

	if (primaryIsCtid)
		iptr = &slot->tts_tid;
	else
		iptr = NULL;

	if (idx->bridging)
	{
		bridge_data.bridge_iptr = &oslot->bridge_ctid;
		bridge_data.is_pkey = true;
		bridge_data.attnum = idx->primaryIsCtid ? 2 : 1;
		bridge_data_arg = &bridge_data;
	}

	len = o_new_tuple_size(tupleDescriptor, spec,
						   iptr, bridge_data_arg, oslot->version,
						   slot->tts_values, slot->tts_isnull, oslot->to_toast);

	tuple.data = (Pointer) palloc0(len);

	o_tuple_fill(tupleDescriptor, spec, &tuple, len,
				 iptr, bridge_data_arg, oslot->version,
				 slot->tts_values, slot->tts_isnull, oslot->to_toast);

	pub static mut TUPLE: return = std::mem::zeroed();
}

bool
tts_orioledb_insert_toast_values(slot: &mut TupleTableSlot,
								 descr: &mut OTableDescr,
								 OXid oxid, CommitSeqNo csn)
{
	oslot: &mut OTableSlot = (OTableSlot *) slot;
	pub static mut TUPLE_DESC: TupleDesc = slot->tts_tupleDescriptor;
	pub static mut IDX_TUP: OTuple = std::mem::zeroed();
	pub static mut I: std::os::raw::c_int = 0;
	pub static mut RESULT: bool = true;
	int			ctid_off = GET_PRIMARY(descr)->primaryIsCtid ? 1 : 0;

	if (GET_PRIMARY(descr)->bridging)
		ctid_off++;

	if (oslot->to_toast == NULL)
		pub static mut TRUE: return = std::mem::zeroed();

	idx_tup = tts_orioledb_make_key(slot, descr);

	for (i = 0; i < tupleDesc->natts; i++)
	{
		//
// Only TOAST attributes explicitly marked ON. COMPRESSION_TRIED
// should have been reset to OFF by tts_orioledb_toast().
//
		Assert(oslot->to_toast[i] != ORIOLEDB_TO_TOAST_COMPRESSION_TRIED);

		if (oslot->to_toast[i] == ORIOLEDB_TO_TOAST_ON)
		{
			pub static mut VALUE: Datum = std::mem::zeroed();
			pub static mut P: Pointer = std::ptr::null_mut();
			pub static mut FREE: bool = false;

			value = o_get_src_value(slot->tts_values[i], &free);
			p = DatumGetPointer(value);

			result = o_toast_insert(descr,
									idx_tup, i + 1 + ctid_off, p,
									toast_datum_size(value), oxid, csn);
			if (free)
				pfree(p);
			if (!result)
				break;
		}
	}
	pfree(idx_tup.data);
	pub static mut RESULT: return = std::mem::zeroed();
}


tts_orioledb_toast_sort_add(slot: &mut TupleTableSlot,
							descr: &mut OTableDescr,
							sortstate: &mut Tuplesortstate)
{
	oslot: &mut OTableSlot = (OTableSlot *) slot;
	pub static mut TUPLE_DESC: TupleDesc = slot->tts_tupleDescriptor;
	pub static mut IDX_TUP: OTuple = std::mem::zeroed();
	pub static mut I: std::os::raw::c_int = 0;
	int			ctid_off = GET_PRIMARY(descr)->primaryIsCtid ? 1 : 0;

	if (GET_PRIMARY(descr)->bridging)
		ctid_off++;

	if (oslot->to_toast == NULL)
		return;

	idx_tup = tts_orioledb_make_key(slot, descr);

	for (i = 0; i < tupleDesc->natts; i++)
	{
		//
// Only TOAST attributes explicitly marked ON. COMPRESSION_TRIED
// should have been reset to OFF by tts_orioledb_toast().
//
		Assert(oslot->to_toast[i] != ORIOLEDB_TO_TOAST_COMPRESSION_TRIED);

		if (oslot->to_toast[i] == ORIOLEDB_TO_TOAST_ON)
		{
			pub static mut VALUE: Datum = std::mem::zeroed();
			pub static mut P: Pointer = std::ptr::null_mut();
			pub static mut FREE: bool = false;

			value = o_get_src_value(slot->tts_values[i], &free);
			p = DatumGetPointer(value);

			o_toast_sort_add(descr, idx_tup, i + 1 + ctid_off, p,
							 toast_datum_size(value), sortstate);
			if (free)
				pfree(p);
		}
	}
	pfree(idx_tup.data);
}

bool
tts_orioledb_remove_toast_values(slot: &mut TupleTableSlot,
								 descr: &mut OTableDescr,
								 OXid oxid, CommitSeqNo csn)
{
	pub static mut I: std::os::raw::c_int = 0;
	pub static mut RESULT: bool = true;
	int			ctid_off = GET_PRIMARY(descr)->primaryIsCtid ? 1 : 0;

	if (GET_PRIMARY(descr)->bridging)
		ctid_off++;

	slot_getallattrs(slot);

	for (i = 0; i < descr->ntoastable; i++)
	{
		pub static mut TOAST_ATTN: std::os::raw::c_int = 0;
		pub static mut VALUE: Datum = std::mem::zeroed();

		toast_attn = descr->toastable[i] - ctid_off;

		if (slot->tts_isnull[toast_attn])
			continue;

		value = slot->tts_values[toast_attn];
		if (VARATT_IS_EXTERNAL_ORIOLEDB(value))
		{
			pub static mut OTE: OToastExternal = std::mem::zeroed();
			pub static mut KEY: OFixedKey = std::mem::zeroed();

			memcpy(&ote, VARDATA_EXTERNAL(DatumGetPointer(value)), O_TOAST_EXTERNAL_SZ);
			key.tuple.formatFlags = ote.formatFlags;
			key.tuple.data = key.fixedData;
			memcpy(key.fixedData,
				   VARDATA_EXTERNAL(DatumGetPointer(value)) + O_TOAST_EXTERNAL_SZ,
				   ote.data_size);

			result = o_toast_delete(descr,
									key.tuple,
									toast_attn + 1 + ctid_off,
									oxid,
									csn);
			if (!result)
				break;
		}
	}
	pub static mut RESULT: return = std::mem::zeroed();
}

bool
tts_orioledb_update_toast_values(oldSlot: &mut TupleTableSlot,
								 newSlot: &mut TupleTableSlot,
								 descr: &mut OTableDescr,
								 OXid oxid, CommitSeqNo csn)
{
	newOSlot: &mut OTableSlot = (OTableSlot *) newSlot;
	pub static mut IDX_TUP: OTuple = std::mem::zeroed();
	pub static mut PG_USED_FOR_ASSERTS_ONLY: OTuple		old_idx_tup = std::mem::zeroed();
	pub static mut I: std::os::raw::c_int = 0;
	pub static mut RESULT: bool = true;
	primary: &mut OIndexDescr = GET_PRIMARY(descr);
	pub static mut CTID_OFF: std::os::raw::c_int = primary->primaryIsCtid ? 1 : 0;

	if (descr->bridge)
		ctid_off++;

	slot_getallattrs(oldSlot);

	idx_tup = tts_orioledb_make_key(newSlot, descr);

#ifdef USE_ASSERT_CHECKING
	{
		pub static mut NATTS: std::os::raw::c_int = 0;

		old_idx_tup = tts_orioledb_make_key(oldSlot, descr);
		o_tuple_set_version(&primary->nonLeafSpec, &old_idx_tup,
							o_tuple_get_version(idx_tup));

		//
// old_idx_tup and idx_tup are equal using comparator, but some
// collations could consider equal tuples of different sizes. E.g.
// when we update tuple on logical subscriber key could be equal in a
// subscriber collation, but size of old and new keys could be
// different. (see PG test src/test/subscription/012_collation.pl)
//
// So we refrain from checking size equality here.
//

		Assert(old_idx_tup.formatFlags == idx_tup.formatFlags);

		//
// Cannot use simple memcmp(old_idx_tup.data, idx_tup.data, ...)
// because of included fields and also equality of such special values
// as '0.0' and '-0.0' for float
//
		if (old_idx_tup.formatFlags & O_TUPLE_FLAGS_FIXED_FORMAT)
			natts = primary->nonLeafSpec.natts;
		else
			natts = primary->nonLeafTupdesc->natts;
		for (i = 0; i < natts; i++)
		{
			if (!OIgnoreColumn(primary, i))
			{
				pub static mut OLD_VALUE: Datum = std::mem::zeroed();
				pub static mut NEW_VALUE: Datum = std::mem::zeroed();
				pub static mut ISNULL: bool = false;
				pub static mut O_INDEX_FIELD: *mut pkfield = &primary->fields[i];
				pub static mut CMP: std::os::raw::c_int = 0;

				old_value = o_fastgetattr(old_idx_tup, i + 1,
										  primary->nonLeafTupdesc,
										  &primary->nonLeafSpec, &isnull);
				Assert(!isnull);
				new_value = o_fastgetattr(idx_tup, i + 1,
										  primary->nonLeafTupdesc,
										  &primary->nonLeafSpec, &isnull);
				Assert(!isnull);

				cmp = o_call_comparator(pkfield->comparator,
										old_value, new_value);
				Assert(cmp == 0);
			}
		}
		pfree(old_idx_tup.data);
	}
#endif

	for (i = 0; i < descr->ntoastable; i++)
	{
		pub static mut TOAST_ATTN: std::os::raw::c_int = 0;
		Datum		oldValue = 0,
					newValue = 0;
		bool		newToast = false,
					oldToast = false;
		pub static mut INSERT_NEW: bool = false;
		pub static mut DELETE_OLD: bool = false;

		toast_attn = descr->toastable[i] - ctid_off;
		if (!oldSlot->tts_isnull[toast_attn])
		{
			oldValue = oldSlot->tts_values[toast_attn];
			if (VARATT_IS_EXTERNAL_ORIOLEDB(oldValue))
				oldToast = true;
		}

		if (newOSlot->to_toast)
		{
			//
// Only TOAST attributes explicitly marked ON. COMPRESSION_TRIED
// should have been reset to OFF by tts_orioledb_toast().
//
			Assert(newOSlot->to_toast[toast_attn] != ORIOLEDB_TO_TOAST_COMPRESSION_TRIED);

			if (newOSlot->to_toast[toast_attn] == ORIOLEDB_TO_TOAST_ON)
			{
				newToast = true;
				newValue = newSlot->tts_values[toast_attn];
			}
		}

		if (!newToast && !oldToast)
			continue;

		if (newToast && !oldToast)
		{
			insertNew = true;
		}
		else if (!newToast && oldToast)
		{
			deleteOld = true;
		}
		else if (o_toast_equal(&GET_PRIMARY(descr)->desc,
							   newValue,
							   oldValue))
		{
			// if it is the same toast value than nothing to do
			continue;
		}
		else
		{
			// update value if it does not equal
			pub static mut EQUAL: bool = false;
			pub static mut RAW_SIZE: std::os::raw::c_int = 0;

			rawSize = o_get_raw_size(newValue);
			equal = (rawSize == o_get_raw_size(oldValue));
			if (equal)
			{
				pub static mut NEW_RAW_VALUE: Datum = std::mem::zeroed();
				pub static mut OLD_RAW_VALUE: Datum = std::mem::zeroed();
				pub static mut NEW_PTR: Pointer = std::ptr::null_mut();
				pub static mut OLD_PTR: Pointer = std::ptr::null_mut();
				pub static mut FREE_NEW: bool = false;
				pub static mut FREE_OLD: bool = false;

				newRawValue = o_get_raw_value(newValue, &freeNew);
				oldRawValue = o_get_raw_value(oldValue, &freeOld);
				newPtr = DatumGetPointer(newRawValue);
				oldPtr = DatumGetPointer(oldRawValue);

				Assert(VARSIZE_ANY_EXHDR(newPtr) == VARSIZE_ANY_EXHDR(oldPtr));
				Assert(VARSIZE_ANY_EXHDR(newPtr) == rawSize);
				equal = memcmp(VARDATA_ANY(oldPtr),
							   VARDATA_ANY(newPtr),
							   rawSize) == 0;
				if (freeNew)
					pfree(newPtr);
				if (freeOld)
					pfree(oldPtr);

				if (equal)
					continue;
			}

			insertNew = true;
			deleteOld = true;
		}

		if (deleteOld)
		{
			pub static mut OTE: OToastExternal = std::mem::zeroed();
			pub static mut KEY: OFixedKey = std::mem::zeroed();

			memcpy(&ote, VARDATA_EXTERNAL(DatumGetPointer(oldValue)), O_TOAST_EXTERNAL_SZ);
			key.tuple.formatFlags = ote.formatFlags;
			key.tuple.data = key.fixedData;
			memcpy(key.fixedData,
				   VARDATA_EXTERNAL(DatumGetPointer(oldValue)) + O_TOAST_EXTERNAL_SZ,
				   ote.data_size);

			result = o_toast_delete(descr,
									key.tuple,
									toast_attn + 1 + ctid_off,
									oxid,
									csn);
			if (!result)
				break;
		}

		if (insertNew)
		{
			pub static mut VALUE: Datum = std::mem::zeroed();
			pub static mut P: Pointer = std::ptr::null_mut();
			pub static mut FREE: bool = false;

			value = o_get_src_value(newValue, &free);
			p = DatumGetPointer(value);

			result = o_toast_insert(descr,
									idx_tup,
									toast_attn + 1 + ctid_off,
									p,
									toast_datum_size(value),
									oxid,
									csn);
			if (free)
				pfree(p);
			if (!result)
				break;
		}
	}

	pfree(idx_tup.data);
	pub static mut RESULT: return = std::mem::zeroed();
}

//
// tts_orioledb_modified - Check if specified attributes were modified between two tuples
//
// Compares the values of specific attributes between an old and new tuple slot
// to determine if any modifications have occurred. This is primarily used during
// UPDATE operations to distinguish between key and non-key updates.
//
// Parameters:
// oldSlot - The original tuple slot before modification
// newSlot - The new tuple slot with pending changes
// attrs   - Bitmap set indicating which attributes to check for modifications.
//
// Returns:
// true if any of the specified attributes have different values between
// the old and new slots, false if all specified attributes are unchanged.
//
bool
tts_orioledb_modified(oldSlot: &mut TupleTableSlot,
					  newSlot: &mut TupleTableSlot,
					  attrs: &mut Bitmapset)
{
	pub static mut TUPDESC: TupleDesc = oldSlot->tts_tupleDescriptor;
	int			attnum,
				maxAttr;

	maxAttr = bms_prev_member(attrs, -1) + FirstLowInvalidHeapAttributeNumber - 1;

	if (maxAttr < 0)
		pub static mut FALSE: return = std::mem::zeroed();

	slot_getsomeattrs(oldSlot, maxAttr + 1);
	slot_getsomeattrs(newSlot, maxAttr + 1);

	attnum = -1;
	while ((attnum = bms_next_member(attrs, attnum)) >= 0)
	{
		pub static mut I: std::os::raw::c_int = attnum + FirstLowInvalidHeapAttributeNumber - 1;

		if (unlikely(i < 0))
			elog(ERROR, "invalid attribute number %d", i);
		else
		{
			Form_pg_attribute att = TupleDescAttr(tupdesc, i);
			Datum		val1 = oldSlot->tts_values[i],
						val2 = newSlot->tts_values[i];
			bool		isnull1 = oldSlot->tts_isnull[i],
						isnull2 = newSlot->tts_isnull[i];

			if (isnull1 != isnull2)
				pub static mut TRUE: return = std::mem::zeroed();

			if (isnull1)
				continue;

			if (!datumIsEqual(val1, val2, att->attbyval, att->attlen))
				pub static mut TRUE: return = std::mem::zeroed();
		}
	}
	pub static mut FALSE: return = std::mem::zeroed();
}


tts_orioledb_set_ctid(slot: &mut TupleTableSlot, ItemPointer iptr)
{
	oslot: &mut OTableSlot = (OTableSlot *) slot;

	slot->tts_tid = *iptr;
	if (!O_TUPLE_IS_NULL(oslot->tuple) &&
		oslot->ixnum == PrimaryIndexNumber &&
		oslot->leafTuple)
		o_tuple_set_ctid(oslot->tuple, iptr);
}

const TupleTableSlotOps TTSOpsOrioleDB = {
	.base_slot_size = sizeof(OTableSlot),
	.init = tts_orioledb_init,
	.release = tts_orioledb_release,
	.clear = tts_orioledb_clear,
	.getsomeattrs = tts_orioledb_getsomeattrs,
	.getsysattr = tts_orioledb_getsysattr,
	.materialize = tts_orioledb_materialize,
	.copyslot = tts_orioledb_copyslot,

	//
// A virtual tuple table slot can not "own" a heap tuple or a minimal
// tuple.
//
	.get_heap_tuple = NULL,
	.get_minimal_tuple = NULL,
	.copy_heap_tuple = tts_orioledb_copy_heap_tuple,
	.copy_minimal_tuple = tts_orioledb_copy_minimal_tuple
};