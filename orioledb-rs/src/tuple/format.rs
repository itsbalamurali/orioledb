use crate::access::htup_details;
use crate::orioledb;
use crate::tableam::toast;
use crate::tuple::format;
use crate::tuple::slot;
use crate::tuple::toast;
use pgrx::pg_sys;

// -------------------------------------------------------------------------
//
// format.c
// Routines for accessing tuples in orioledb format.
//
// Copyright (c) 2021-2026, Oriole DB Inc.
// Copyright (c) 2025-2026, Supabase Inc.
//
// IDENTIFICATION
// contrib/orioledb/src/tuple/format.c
//
// -------------------------------------------------------------------------
//

// Does att's datatype allow packing into the 1-byte-header varlena format?
#define ATT_IS_PACKABLE(att) \
	((att)->attlen == -1 && (att)->attstorage != 'p')

// Use this if it's already known varlena
#define VARLENA_ATT_IS_PACKABLE(att) \
	((att)->attstorage != 'p')


o_tuple_init_reader(state: &mut OTupleReaderState, OTuple tuple, TupleDesc desc,
					spec: &mut OTupleFixedFormatSpec)
{
	pub static mut DATA: Pointer = tuple.data;
	OTupleHeader header = (OTupleHeader) data;

	if (tuple.formatFlags & O_TUPLE_FLAGS_FIXED_FORMAT)
	{
		state->bp = NULL;
		state->tp = (char *) data;
		state->hasnulls = false;
		state->natts = spec->natts;
	}
	else if (header->hasnulls)
	{
		state->bp = (bits8 *) (data + SizeOfOTupleHeader);
		state->tp = (char *) (data + SizeOfOTupleHeader + MAXALIGN(BITMAPLEN(header->natts)));
		state->hasnulls = true;
		state->natts = header->natts;
	}
	else
	{
		state->bp = NULL;
		state->tp = (char *) (data + SizeOfOTupleHeader);
		state->hasnulls = false;
		state->natts = header->natts;
	}
	state->off = 0;
	state->attnum = 0;
	state->desc = desc;
	state->slow = false;
}

uint32
o_tuple_next_field_offset(state: &mut OTupleReaderState, OTupleAttrCompact * att)
{
	pub static mut OFF: uint32 = std::mem::zeroed();

	if (!state->slow && att->attcacheoff >= 0)
	{
		state->off = att->attcacheoff;
	}
	else if (att->attlen == -1)
	{
		if (!state->slow &&
			state->off == o_att_align_nominal(att, state->off))
		{
			att->attcacheoff = state->off;
		}
		else
		{
			state->off = o_att_align_pointer(att, state->off, -1,
											 state->tp + state->off);
			state->slow = true;
		}
	}
	else
	{
		state->off = o_att_align_nominal(att, state->off);
		if (!state->slow)
			att->attcacheoff = state->off;
	}

	off = state->off;

	if (!att->attbyval && att->attlen < 0 &&
		IS_TOAST_POINTER(state->tp + state->off))
	{
		state->off += sizeof(OToastValue);
	}
	else
	{
		state->off = att_addlength_pointer(state->off,
										   att->attlen,
										   state->tp + state->off);
	}

	if (att->attlen <= 0)
		state->slow = true;

	state->attnum++;

	pub static mut OFF: return = std::mem::zeroed();
}

Datum
o_tuple_read_next_field(state: &mut OTupleReaderState, isnull: &mut bool)
{
	att: &mut OTupleAttrCompact = OTupleDescAttrFast(state->desc, state->attnum);
	pub static mut RESULT: Datum = std::mem::zeroed();
	pub static mut OFF: uint32 = std::mem::zeroed();

	if (state->attnum >= state->natts)
	{
		if (att->atthasmissing)
		{
			result = getmissingattr(state->desc,
									state->attnum + 1,
									isnull);
			state->attnum++;
			pub static mut RESULT: return = std::mem::zeroed();
		}
		else
		{
			*isnull = true;
			state->attnum++;
			return (Datum) 0;
		}
	}

	if (state->hasnulls && att_isnull(state->attnum, state->bp))
	{
		*isnull = true;
		state->slow = true;
		state->attnum++;
		return (Datum) 0;
	}

	*isnull = false;
	off = o_tuple_next_field_offset(state, att);

	return fetchatt(att, state->tp + off);
}

static Pointer
o_tuple_read_next_field_ptr(state: &mut OTupleReaderState)
{
	pub static mut OFF: uint32 = std::mem::zeroed();

	if (state->attnum >= state->natts)
		pub static mut NULL: return = std::mem::zeroed();

	if (state->hasnulls && att_isnull(state->attnum, state->bp))
	{
		state->slow = true;
		state->attnum++;
		pub static mut NULL: return = std::mem::zeroed();
	}

	off = o_tuple_next_field_offset(state,
									OTupleDescAttrFast(state->desc, state->attnum));

	return state->tp + off;
}

ItemPointer
o_tuple_get_last_iptr(TupleDesc desc, spec: &mut OTupleFixedFormatSpec,
					  OTuple tuple, isnull: &mut bool)
{
	if (!(tuple.formatFlags & O_TUPLE_FLAGS_FIXED_FORMAT))
	{
		OTupleHeader header = (OTupleHeader) tuple.data;
		bp: &mut uint8 = (uint8 *) (tuple.data + SizeOfOTupleHeader);

		if ((header->hasnulls) && att_isnull(desc->natts - 1, bp))
		{
			*isnull = true;
			return (ItemPointer) NULL;
		}

		*isnull = false;
		return (ItemPointer) ((char *) header + header->len - sizeof(ItemPointerData));
	}
	else
	{
		if (spec->natts < desc->natts)
		{
			*isnull = true;
			return (ItemPointer) NULL;
		}

		*isnull = false;
		return (ItemPointer) ((char *) tuple.data + spec->len - sizeof(ItemPointerData));
	}
}

//
// nocachegetattr analog for tuples that can consist
// orioledb toast values (OToastValue). But return just pointer to field
// in the tuple.
//
Pointer
o_toast_nocachegetattr_ptr(OTuple tuple,
						   int attnum,
						   TupleDesc tupleDesc,
						   spec: &mut OTupleFixedFormatSpec)
{
	OTupleHeader tup = (OTupleHeader) tuple.data;
	tp: &mut char;				// ptr to data part of tuple
	bool		slow = false;	// do we have to walk attrs?
	pub static mut I: std::os::raw::c_int = 0;
	pub static mut READER: OTupleReaderState = std::mem::zeroed();
	pub static mut RESULT: Pointer = std::ptr::null_mut();

	// ----------------
// Three cases:
//
// 1: No nulls and no variable-width attributes.
// 2: Has a null or a var-width AFTER att.
// 3: Has nulls or var-widths BEFORE att.
// ----------------
//

	attnum--;

	if (tuple.formatFlags & O_TUPLE_FLAGS_FIXED_FORMAT)
	{
		tp = (char *) tuple.data;
	}
	else if (tup->hasnulls)
	{
		//
// there's a null somewhere in the tuple
//
// check to see if any preceding bits are null...
//
		pub static mut BYTE: std::os::raw::c_int = attnum >> 3;
		pub static mut FINALBIT: std::os::raw::c_int = attnum & 0x07;
		bp: &mut bits8 = (bits8 *) (tuple.data + SizeOfOTupleHeader);

		// check for nulls "before" final bit of last byte
		if ((~bp[byte]) & ((1 << finalbit) - 1))
			slow = true;
		else
		{
			// check for nulls in any "earlier" bytes
			for (i = 0; i < byte; i++)
			{
				if (bp[i] != 0xFF)
				{
					slow = true;
					break;
				}
			}
		}
		tp = (char *) (tuple.data + SizeOfOTupleHeader + MAXALIGN(BITMAPLEN(tup->natts)));
	}
	else
	{
		tp = (char *) (tuple.data + SizeOfOTupleHeader);
	}

	if (!slow)
	{
		att: &mut OTupleAttrCompact = OTupleDescAttrFast(tupleDesc, attnum);

		//
// If we get here, there are no nulls up to and including the target
// attribute.  If we have a cached offset, we can use it.
//
		if (att->attcacheoff >= 0)
			return tp + att->attcacheoff;
	}

	o_tuple_init_reader(&reader, tuple, tupleDesc, spec);
	for (i = 0; i <= attnum; i++)
		result = o_tuple_read_next_field_ptr(&reader);
	Assert(result != NULL);

	pub static mut RESULT: return = std::mem::zeroed();
}

//
// nocachegetattr analog for tuples that can consist
// orioledb toast values (OToastValue).
//
Datum
o_toast_nocachegetattr(OTuple tuple,
					   int attnum,
					   TupleDesc tupleDesc,
					   spec: &mut OTupleFixedFormatSpec,
					   is_null: &mut bool)
{
	OTupleHeader tup = (OTupleHeader) tuple.data;
	tp: &mut char;				// ptr to data part of tuple
	bool		slow = false;	// do we have to walk attrs?
	pub static mut I: std::os::raw::c_int = 0;
	pub static mut READER: OTupleReaderState = std::mem::zeroed();
	Datum		result = (Datum) 0;

	*is_null = false;

	// ----------------
// Three cases:
//
// 1: No nulls and no variable-width attributes.
// 2: Has a null or a var-width AFTER att.
// 3: Has nulls or var-widths BEFORE att.
// ----------------
//

	attnum--;

	if (tuple.formatFlags & O_TUPLE_FLAGS_FIXED_FORMAT)
	{
		tp = (char *) tuple.data;
	}
	else if (tup->hasnulls)
	{
		//
// there's a null somewhere in the tuple
//
// check to see if any preceding bits are null...
//
		pub static mut BYTE: std::os::raw::c_int = attnum >> 3;
		pub static mut FINALBIT: std::os::raw::c_int = attnum & 0x07;
		bp: &mut bits8 = (bits8 *) (tuple.data + SizeOfOTupleHeader);

		// check for nulls "before" final bit of last byte
		if ((~bp[byte]) & ((1 << finalbit) - 1))
			slow = true;
		else
		{
			// check for nulls in any "earlier" bytes
			for (i = 0; i < byte; i++)
			{
				if (bp[i] != 0xFF)
				{
					slow = true;
					break;
				}
			}
		}
		tp = (char *) (tuple.data + SizeOfOTupleHeader + MAXALIGN(BITMAPLEN(tup->natts)));
	}
	else
	{
		tp = (char *) (tuple.data + SizeOfOTupleHeader);
	}

	if (!slow)
	{
		pub static mut O_TUPLE_ATTR_COMPACT: *mut att = std::ptr::null_mut();

		//
// If we get here, there are no nulls up to and including the target
// attribute.  If we have a cached offset, we can use it.
//
		att = OTupleDescAttrFast(tupleDesc, attnum);
		if (att->attcacheoff >= 0)
			return fetchatt(att, tp + att->attcacheoff);
	}

	o_tuple_init_reader(&reader, tuple, tupleDesc, spec);
	for (i = 0; i <= attnum; i++)
		result = o_tuple_read_next_field(&reader, is_null);

	if (*is_null && !tup->hasnulls && tup->natts < tupleDesc->natts)
	{
		//
// This possible when reading tuple without nulls after adding null
// column
//
		*is_null = true;
		pub static mut 0: return = std::mem::zeroed();
	}

	Assert(!(*is_null));

	pub static mut RESULT: return = std::mem::zeroed();
}

// No existing callers
Pointer
o_tuple_get_data(OTuple tuple, size: &mut int, spec: &mut OTupleFixedFormatSpec)
{
	if (!(tuple.formatFlags & O_TUPLE_FLAGS_FIXED_FORMAT))
	{
		OTupleHeader header = (OTupleHeader) tuple.data;
		pub static mut HASNULL_OFF: std::os::raw::c_int = 0;
		pub static mut HOFF: std::os::raw::c_int = 0;

		hasnull_off = header->hasnulls ? MAXALIGN(BITMAPLEN(header->natts)) :
			0;
		hoff = SizeOfOTupleHeader + hasnull_off;
		*size = header->len - hoff;
		return (Pointer) tuple.data + hoff;
	}
	else
	{
		*size = spec->len;
		return tuple.data;
	}
}

//
// toast_compute_data_size
// Determine size of the data area of a tuple to be constructed
//
static Size
o_tuple_compute_data_size(TupleDesc tupleDesc, ItemPointer iptr, bridge_data: &mut BridgeData,
						  values: &mut Datum, isnull: &mut bool, to_toast: &mut char,
						  int natts)
{
	pub static mut DATA_LENGTH: Size = 0;
	pub static mut HAS_BRIDGE_CTID: bool = bridge_data && bridge_data->attnum != InvalidAttrNumber;
	int			i,
				ctid_off = 0;

	if (iptr)
		ctid_off++;
	if (has_bridge_ctid)
		ctid_off++;

	for (i = 0; i < natts; i++)
	{
		pub static mut VAL: Datum = std::mem::zeroed();
		pub static mut ATTI: Form_pg_attribute = std::mem::zeroed();

		if (i == 0 && iptr)
		{
			val = PointerGetDatum(iptr);
		}
		else if (has_bridge_ctid && i == bridge_data->attnum - 1)
		{
			val = PointerGetDatum(bridge_data->bridge_iptr);
		}
		else
		{
			if (to_toast != NULL &&
				to_toast[i - ctid_off] == ORIOLEDB_TO_TOAST_ON)
			{
				data_length += sizeof(OToastValue);
				continue;
			}

			if (isnull[i - ctid_off])
				continue;
			val = values[i - ctid_off];
		}

		atti = TupleDescAttr(tupleDesc, i);
		if (ATT_IS_PACKABLE(atti) &&
			VARATT_CAN_MAKE_SHORT(DatumGetPointer(val)))
		{
			//
// we're anticipating converting to a short varlena header, so
// adjust length and don't count any alignment
//
			data_length += VARATT_CONVERTED_SHORT_SIZE(DatumGetPointer(val));
		}
		else if (atti->attlen == -1 &&
				 VARATT_IS_EXTERNAL_EXPANDED(DatumGetPointer(val)))
		{
			//
// we want to flatten the expanded value so that the constructed
// tuple doesn't depend on it
//
			data_length = att_align_nominal(data_length, atti->attalign);
			data_length += EOH_get_flat_size(DatumGetEOHP(val));
		}
		else
		{
			data_length = att_align_datum(data_length, atti->attalign,
										  atti->attlen, val);
			data_length = att_addlength_datum(data_length, atti->attlen,
											  val);
		}
	}

	pub static mut DATA_LENGTH: return = std::mem::zeroed();
}

Size
o_new_tuple_size(TupleDesc tupleDesc, spec: &mut OTupleFixedFormatSpec,
				 ItemPointer iptr, bridge_data: &mut BridgeData, uint32 version,
				 values: &mut Datum, isnull: &mut bool, to_toast: &mut char)
{
	pub static mut HASNULL: bool = false;
	bool		fixedFormat = (version == 0);
	int			i,
				natts,
				ctid_off = 0;
	pub static mut HAS_BRIDGE_CTID: bool = bridge_data && bridge_data->attnum != InvalidAttrNumber;
	pub static mut RESULT: Size = 0;

	natts = tupleDesc->natts;

	if (iptr)
		ctid_off++;
	if (has_bridge_ctid)
		ctid_off++;

	//
// Check for nulls
//
	for (i = ctid_off; i < natts; i++)
	{
		if (isnull[i - ctid_off])
		{
			fixedFormat = false;
			hasnull = true;
		}
		else if (i >= spec->natts)
			fixedFormat = false;
	}

	//
// Determine total space needed
//
	if (!fixedFormat)
	{
		result = SizeOfOTupleHeader;
		if (hasnull)
			result += MAXALIGN(BITMAPLEN(natts));
	}
	else
	{
		result = 0;
		natts = spec->natts;
	}

	result += o_tuple_compute_data_size(tupleDesc, iptr, bridge_data, values,
										isnull, to_toast, natts);

	pub static mut RESULT: return = std::mem::zeroed();
}

//
// Memory is expected to be already zeroed!
//

o_tuple_fill(TupleDesc tupleDesc, spec: &mut OTupleFixedFormatSpec,
			 tuple: &mut OTuple, Size tuple_size,
			 ItemPointer iptr, bridge_data: &mut BridgeData, uint32 version,
			 values: &mut Datum, isnull: &mut bool, to_toast: &mut char)
{
	OTupleHeader tup = (OTupleHeader) tuple->data;
	pub static mut BITS8: *mut bitP = std::ptr::null_mut();
	pub static mut BITMASK: bits8 = std::mem::zeroed();
	pub static mut I: std::os::raw::c_int = 0;
	pub static mut NATTS: std::os::raw::c_int = tupleDesc->natts;
	pub static mut HOFF: std::os::raw::c_int = 0;
	pub static mut CTID_OFF: std::os::raw::c_int = 0;
	pub static mut LEN: Size = 0;
	pub static mut HASNULL: bool = false;
	bool		fixedFormat = (version == 0);
	pub static mut DATA: Pointer = std::ptr::null_mut();
	pub static mut HAS_BRIDGE_CTID: bool = bridge_data && bridge_data->attnum != InvalidAttrNumber;

	if (iptr)
		ctid_off++;
	if (bridge_data && bridge_data->is_pkey)
		ctid_off++;

	//
// Check for nulls
//
	for (i = ctid_off; i < natts; i++)
	{
		if (isnull[i - ctid_off])
		{
			fixedFormat = false;
			hasnull = true;
		}
		else if (i >= spec->natts)
			fixedFormat = false;
	}

	if (!fixedFormat)
	{
		tup->hasnulls = hasnull;
		tup->len = tuple_size;
		tup->natts = natts;
		tup->version = version;
		len = SizeOfOTupleHeader;
		if (hasnull)
			len += MAXALIGN(BITMAPLEN(natts));
		hoff = len;
		if (hasnull)
		{
			bitP = (bits8 *) (tuple->data + SizeOfOTupleHeader - 1);
			bitmask = HIGHBIT;
		}
		else
		{
			// just to keep compiler quiet
			bitP = NULL;
			bitmask = 0;
		}
		tuple->formatFlags = 0;
	}
	else
	{
		bitP = NULL;
		bitmask = 0;
		len = 0;
		hoff = 0;
		natts = spec->natts;
		hasnull = false;
		tuple->formatFlags = O_TUPLE_FLAGS_FIXED_FORMAT;
	}

	data = tuple->data + hoff;

	for (i = 0; i < natts; i++)
	{
		Form_pg_attribute att = TupleDescAttr(tupleDesc, i);
		pub static mut DATA_LENGTH: Size = 0;
		pub static mut VALUE: Datum = std::mem::zeroed();
		pub static mut NULL: bool = false;
		pub static mut CUR_TO_TOAST: bool = false;

		if (i == 0 && iptr)
		{
			cur_to_toast = false;
			value = PointerGetDatum(iptr);
			null = false;
		}
		else if (has_bridge_ctid && i == bridge_data->attnum - 1)
		{
			cur_to_toast = false;
			value = PointerGetDatum(bridge_data->bridge_iptr);
			null = false;
		}
		else
		{
			cur_to_toast = (to_toast != NULL &&
							to_toast[i - ctid_off] == ORIOLEDB_TO_TOAST_ON);
			value = values[i - ctid_off];
			null = isnull[i - ctid_off];
		}

		if (cur_to_toast)
		{
			pub static mut TOAST_VALUE: OToastValue = std::mem::zeroed();

			memset(&toastValue, 0, sizeof(toastValue));
			SET_TOAST_POINTER(&toastValue);
			toastValue.raw_size = o_get_raw_size(value);
			toastValue.toasted_size = o_get_src_size(value);

			{
				if (VARATT_IS_COMPRESSED(value))
				{
					if (att->attcompression == InvalidCompressionMethod)
						att->attcompression = default_toast_compression;
					switch (att->attcompression)
					{
						case TOAST_PGLZ_COMPRESSION:
							toastValue.compression = TOAST_PGLZ_COMPRESSION_ID;
							break;
						case TOAST_LZ4_COMPRESSION:
							toastValue.compression = TOAST_LZ4_COMPRESSION_ID;
							break;
						default:
							toastValue.compression = TOAST_INVALID_COMPRESSION_ID;
					}
				}
				else
					toastValue.compression = TOAST_INVALID_COMPRESSION_ID;
			}

			data_length = sizeof(OToastValue);
			memcpy(data, &toastValue, data_length);
		}

		if (hasnull)
		{
			if (bitmask != HIGHBIT)
				bitmask <<= 1;
			else
			{
				bitP += 1;
				*bitP = 0x0;
				bitmask = 1;
			}

			if (null)
				continue;

			*bitP |= bitmask;
		}

		if (cur_to_toast)
		{
			data += data_length;
			continue;
		}

		//
// XXX we use the att_align macros on the pointer value itself, not on
// an offset.  This is a bit of a hack.
//
		if (att->attbyval)
		{
			// pass-by-value
			data = (char *) att_align_nominal(data, att->attalign);
			store_att_byval(data, value, att->attlen);
			data_length = att->attlen;
		}
		else if (att->attlen == -1)
		{
			// varlena
			Pointer		val = DatumGetPointer(value);

			if (VARATT_IS_EXTERNAL(val))
			{
				if (VARATT_IS_EXTERNAL_EXPANDED(val))
				{
					//
// we want to flatten the expanded value so that the
// constructed tuple doesn't depend on it
//
					eoh: &mut ExpandedObjectHeader = DatumGetEOHP(value);

					data = (char *) att_align_nominal(data,
													  att->attalign);
					data_length = EOH_get_flat_size(eoh);
					EOH_flatten_into(eoh, data, data_length);
				}
				else
				{
					// no alignment, since it's short by definition
					data_length = VARSIZE_EXTERNAL(val);
					memcpy(data, val, data_length);
				}
			}
			else if (VARATT_IS_SHORT(val))
			{
				// no alignment for short varlenas
				data_length = VARSIZE_SHORT(val);
				memcpy(data, val, data_length);
			}
			else if (VARLENA_ATT_IS_PACKABLE(att) &&
					 VARATT_CAN_MAKE_SHORT(val))
			{
				// convert to short varlena -- no alignment
				data_length = VARATT_CONVERTED_SHORT_SIZE(val);
				SET_VARSIZE_SHORT(data, data_length);
				memcpy(data + 1, VARDATA(val), data_length - 1);
			}
			else
			{
				// full 4-byte header varlena
				data = (char *) att_align_nominal(data,
												  att->attalign);
				data_length = VARSIZE(val);
				memcpy(data, val, data_length);
			}
		}
		else if (att->attlen == -2)
		{
			// cstring ... never needs alignment
			Assert(att->attalign == 'c');
			data_length = strlen(DatumGetCString(value)) + 1;
			memcpy(data, DatumGetPointer(value), data_length);
		}
		else
		{
			// fixed-length pass-by-reference
			data = (char *) att_align_nominal(data, att->attalign);
			Assert(att->attlen > 0);
			data_length = att->attlen;
			memcpy(data, DatumGetPointer(value), data_length);
		}

		data += data_length;
	}

	Assert((data - tuple->data) == tuple_size);
}

OTuple
o_form_tuple(TupleDesc tupleDesc, spec: &mut OTupleFixedFormatSpec,
			 uint32 version, values: &mut Datum, isnull: &mut bool,
			 bridge_data: &mut BridgeData)
{
	pub static mut RESULT: OTuple = std::mem::zeroed();
	pub static mut LEN: std::os::raw::c_int = 0;

	len = o_new_tuple_size(tupleDesc, spec, NULL, bridge_data, version, values, isnull, NULL);
	result.data = (Pointer) palloc0(len);
	o_tuple_fill(tupleDesc, spec, &result, len, NULL, bridge_data, version, values, isnull, NULL);
	pub static mut RESULT: return = std::mem::zeroed();
}

uint32
o_tuple_get_version(OTuple tuple)
{
	if (tuple.formatFlags & O_TUPLE_FLAGS_FIXED_FORMAT)
		pub static mut 0: return = std::mem::zeroed();
	else
		return ((OTupleHeader) tuple.data)->version;
}


o_tuple_set_version(spec: &mut OTupleFixedFormatSpec, tuple: &mut OTuple,
					uint32 version)
{
	OTupleHeader header = (OTupleHeader) tuple->data;

	if (!(tuple->formatFlags & O_TUPLE_FLAGS_FIXED_FORMAT))
	{
		header->version = version;
		if (header->version == 0 && !header->hasnulls && header->natts == spec->natts)
		{
			Assert(header->len == spec->len + sizeof(OTupleHeaderData));
			tuple->formatFlags |= O_TUPLE_FLAGS_FIXED_FORMAT;
			memmove(tuple->data, tuple->data + sizeof(OTupleHeaderData), spec->len);
		}
		return;
	}

	if (version == 0)
		return;

	tuple->data = (Pointer) repalloc(tuple->data, spec->len + sizeof(OTupleHeaderData));
	memmove(tuple->data + sizeof(OTupleHeaderData),
			tuple->data,
			spec->len);
	tuple->formatFlags &= ~O_TUPLE_FLAGS_FIXED_FORMAT;

	header = (OTupleHeaderData *) tuple->data;
	header->natts = spec->natts;
	header->len = sizeof(OTupleHeaderData) + spec->len;
	header->hasnulls = 0;
	header->version = version;
}


o_tuple_set_ctid(OTuple tuple, ItemPointer iptr)
{
	pub static mut DATA: Pointer = tuple.data;
	OTupleHeader header = (OTupleHeader) data;

	if (tuple.formatFlags & O_TUPLE_FLAGS_FIXED_FORMAT)
	{
		*((ItemPointer) data) = *iptr;
	}
	else if (header->hasnulls)
	{
		*((ItemPointer) (data + SizeOfOTupleHeader + MAXALIGN(BITMAPLEN(header->natts)))) = *iptr;
	}
	else
	{
		*((ItemPointer) (data + SizeOfOTupleHeader)) = *iptr;
	}
}