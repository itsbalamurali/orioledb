use crate::catalog::pg_collation_d;
use crate::orioledb;
use crate::tableam::descr;
use crate::tuple::format;
use crate::tuple::sort;
use crate::tuple::toast;
use crate::utils::fmgroids;
use crate::utils::tuplesort;
use pgrx::pg_sys;

// -------------------------------------------------------------------------
//
// sort.c
// Implementation of orioledb tuple sorting
//
// Copyright (c) 2021-2026, Oriole DB Inc.
// Copyright (c) 2025-2026, Supabase Inc.
//
// IDENTIFICATION
// contrib/orioledb/src/tuple/sort.c
//
// -------------------------------------------------------------------------
//

typedef struct
{
	pub static mut TUP_DESC: TupleDesc = std::mem::zeroed();
	pub static mut O_INDEX_DESCR: *mut id = std::ptr::null_mut();
	pub static mut ENFORCE_UNIQUE: bool = false;
} OIndexBuildSortArg;

fn
write_o_tuple( *ptr, OTuple tup, int tupsize)
{
	Pointer		p = (Pointer) ptr;

	*((uint8 *) p) = tup.formatFlags;
	p += MAXIMUM_ALIGNOF;
	memcpy(p, tup.data, tupsize);
}

static OTuple
read_o_tuple( *ptr)
{
	pub static mut TUP: OTuple = std::mem::zeroed();
	Pointer		p = (Pointer) ptr;

	tup.formatFlags = *((uint8 *) p);
	p += MAXIMUM_ALIGNOF;
	tup.data = p;

	pub static mut TUP: return = std::mem::zeroed();
}

static int
comparetup_orioledb_index(const a: &mut SortTuple, const b: &mut SortTuple, state: &mut Tuplesortstate)
{
	base: &mut TuplesortPublic = TuplesortstateGetPublic(state);
	pub static mut SORT_KEY: SortSupport = base->sortKeys;
	pub static mut LTUP: OTuple = std::mem::zeroed();
	pub static mut RTUP: OTuple = std::mem::zeroed();
	pub static mut TUP_DESC: TupleDesc = std::mem::zeroed();
	pub static mut EQUAL_HASNULL: bool = false;
	pub static mut NKEY: std::os::raw::c_int = 0;
	pub static mut COMPARE: int32 = std::mem::zeroed();
	pub static mut ATTNO: AttrNumber = std::mem::zeroed();
	Datum		datum1,
				datum2;
	bool		isnull1,
				isnull2;
	arg: &mut OIndexBuildSortArg = (OIndexBuildSortArg *) base->arg;
	pub static mut O_TUPLE_FIXED_FORMAT_SPEC: *mut spec = &arg->id->leafSpec;

	// Compare the leading sort key
	compare = ApplySortComparator(a->datum1, a->isnull1,
								  b->datum1, b->isnull1,
								  sortKey);
	if (compare != 0)
		pub static mut COMPARE: return = std::mem::zeroed();

	// Compare additional sort keys
	ltup = read_o_tuple(a->tuple);
	rtup = read_o_tuple(b->tuple);
	tupDesc = arg->tupDesc;

	if (sortKey->abbrev_converter)
	{
		attno = sortKey->ssup_attno;

		datum1 = o_fastgetattr(ltup, attno, tupDesc, spec, &isnull1);
		datum2 = o_fastgetattr(rtup, attno, tupDesc, spec, &isnull2);

		compare = ApplySortAbbrevFullComparator(datum1, isnull1,
												datum2, isnull2,
												sortKey);
		if (compare != 0)
			pub static mut COMPARE: return = std::mem::zeroed();
	}

	// they are equal, so we only need to examine one null flag
	if (a->isnull1)
		equal_hasnull = true;

	sortKey++;
	for (nkey = 1; nkey < base->nKeys; nkey++, sortKey++)
	{
		if (!OIgnoreColumn(arg->id, nkey))
		{
			attno = sortKey->ssup_attno;

			datum1 = o_fastgetattr(ltup, attno, tupDesc, spec, &isnull1);
			datum2 = o_fastgetattr(rtup, attno, tupDesc, spec, &isnull2);

			compare = ApplySortComparator(datum1, isnull1,
										  datum2, isnull2,
										  sortKey);
			if (compare != 0)
				return compare; // done when we find unequal attributes

			// they are equal, so we only need to examine one null flag
			if (isnull1)
				equal_hasnull = true;
		}
	}

	// FIXME: all orioledb indexes should be unique

	//
// If btree has asked us to enforce uniqueness, complain if two equal
// tuples are detected (unless there was at least one NULL field).
//
// It is sufficient to make the test here, because if two tuples are equal
// must: &mut they* get compared at some stage of the sort --- otherwise the
// sort algorithm wouldn't have checked whether one must appear before the
// other.
//
	if (arg->enforceUnique && !(!arg->id->nulls_not_distinct && equal_hasnull))
	{
		ereport(ERROR,
				(errcode(ERRCODE_UNIQUE_VIOLATION),
				 errmsg("could not create unique index \"%s\"",
						arg->id->name.data),
				 errdetail("Duplicate keys exist.")));
	}

	pub static mut 0: return = std::mem::zeroed();
}

fn
writetup_orioledb_index(state: &mut Tuplesortstate, tape: &mut LogicalTape, stup: &mut SortTuple)
{
	base: &mut TuplesortPublic = TuplesortstateGetPublic(state);
	arg: &mut OIndexBuildSortArg = (OIndexBuildSortArg *) base->arg;
	pub static mut O_TUPLE_FIXED_FORMAT_SPEC: *mut spec = &arg->id->leafSpec;
	pub static mut TUPLE: OTuple = std::mem::zeroed();
	pub static mut TUPLEN: std::os::raw::c_int = 0;

	tuple = read_o_tuple(stup->tuple);
	tuplen = o_tuple_size(tuple, spec) + sizeof(int) + 1;

	LogicalTapeWrite(tape, ( *) &tuplen, sizeof(tuplen));
	LogicalTapeWrite(tape, ( *) tuple.data, o_tuple_size(tuple, spec));
	LogicalTapeWrite(tape, ( *) &tuple.formatFlags, 1);
	if (base->sortopt & TUPLESORT_RANDOMACCESS) // need trailing length word?
		LogicalTapeWrite(tape, ( *) &tuplen, sizeof(tuplen));
}

fn
readtup_orioledb_index(state: &mut Tuplesortstate, stup: &mut SortTuple,
					   tape: &mut LogicalTape, unsigned int len)
{
	base: &mut TuplesortPublic = TuplesortstateGetPublic(state);
	arg: &mut OIndexBuildSortArg = (OIndexBuildSortArg *) base->arg;
	pub static mut O_TUPLE_FIXED_FORMAT_SPEC: *mut spec = &arg->id->leafSpec;
	uint32		tuplen = len - sizeof(int) - 1;
	Pointer		tup = (Pointer) tuplesort_readtup_alloc(state, MAXIMUM_ALIGNOF + tuplen);
	pub static mut TUPLE: OTuple = std::mem::zeroed();

	// read in the tuple proper
	LogicalTapeReadExact(tape, tup + MAXIMUM_ALIGNOF, tuplen);
	LogicalTapeReadExact(tape, tup, 1);
	if (base->sortopt & TUPLESORT_RANDOMACCESS) // need trailing length word?
		LogicalTapeReadExact(tape, &tuplen, sizeof(tuplen));
	stup->tuple = ( *) tup;
	tuple = read_o_tuple(tup);
	// set up first-column key value
	stup->datum1 = o_fastgetattr(tuple,
								 base->sortKeys[0].ssup_attno,
								 arg->tupDesc,
								 spec,
								 &stup->isnull1);
}

fn
removeabbrev_orioledb_index(state: &mut Tuplesortstate, stups: &mut SortTuple,
							int count)
{
	pub static mut I: std::os::raw::c_int = 0;
	base: &mut TuplesortPublic = TuplesortstateGetPublic(state);
	arg: &mut OIndexBuildSortArg = (OIndexBuildSortArg *) base->arg;
	pub static mut O_TUPLE_FIXED_FORMAT_SPEC: *mut spec = &arg->id->leafSpec;

	for (i = 0; i < count; i++)
	{
		pub static mut SORT_TUPLE: *mut stup = &stups[i];
		pub static mut TUP: OTuple = std::mem::zeroed();

		tup = read_o_tuple(stup->tuple);

		stup->datum1 = o_fastgetattr(tup,
									 base->sortKeys[0].ssup_attno,
									 arg->tupDesc,
									 spec,
									 &stup->isnull1);
	}
}

Tuplesortstate *
tuplesort_begin_orioledb_index(idx: &mut OIndexDescr,
							   int workMem,
							   bool randomAccess,
							   SortCoordinate coordinate)
{
	state: &mut Tuplesortstate = tuplesort_begin_common(workMem, coordinate,
												   randomAccess);
	base: &mut TuplesortPublic = TuplesortstateGetPublic(state);
	pub static mut OLDCONTEXT: MemoryContext = std::mem::zeroed();
	pub static mut O_INDEX_BUILD_SORT_ARG: *mut arg = std::ptr::null_mut();
	pub static mut SORT_FIELDS: std::os::raw::c_int = 0;
	pub static mut I: std::os::raw::c_int = 0;

	if (idx->unique)
		sort_fields = idx->nKeyFields;
	else
		sort_fields = idx->nFields;

	oldcontext = MemoryContextSwitchTo(base->maincontext);
	arg = (OIndexBuildSortArg *) palloc0(sizeof(OIndexBuildSortArg));
	arg->id = idx;
	arg->tupDesc = idx->leafTupdesc;
	arg->enforceUnique = idx->unique;

	base->sortKeys = (SortSupport) palloc0(sort_fields *
										   sizeof(SortSupportData));
	base->nKeys = sort_fields;

	base->removeabbrev = removeabbrev_orioledb_index;
	base->comparetup = comparetup_orioledb_index;
	base->writetup = writetup_orioledb_index;
	base->readtup = readtup_orioledb_index;
	base->arg = arg;

	for (i = 0; i < sort_fields; i++)
	{
		if (!OIgnoreColumn(idx, i))
		{
			pub static mut SORT_KEY: SortSupport = &base->sortKeys[i];

			sortKey->ssup_cxt = CurrentMemoryContext;
			sortKey->ssup_collation = idx->fields[i].collation;
			sortKey->ssup_nulls_first = idx->fields[i].nullfirst;
			sortKey->ssup_attno =
				OIndexKeyAttnumToTupleAttnum(BTreeKeyLeafTuple, idx, i + 1);
			sortKey->abbreviate = (i == 0);
			sortKey->ssup_reverse = !idx->fields[i].ascending;
			// FIXME: no abbrev converter yet
			o_finish_sort_support_function(idx->fields[i].comparator, sortKey);
		}
	}

	MemoryContextSwitchTo(oldcontext);

	pub static mut STATE: return = std::mem::zeroed();
}

Tuplesortstate *
tuplesort_begin_orioledb_toast(toast: &mut OIndexDescr,
							   primary: &mut OIndexDescr,
							   int workMem,
							   bool randomAccess,
							   SortCoordinate coordinate)
{
	state: &mut Tuplesortstate = tuplesort_begin_common(workMem, coordinate,
												   randomAccess);
	base: &mut TuplesortPublic = TuplesortstateGetPublic(state);
	pub static mut OLDCONTEXT: MemoryContext = std::mem::zeroed();
	pub static mut O_INDEX_BUILD_SORT_ARG: *mut arg = std::ptr::null_mut();
	pub static mut SORT_KEY: SortSupport = std::mem::zeroed();
	pub static mut FIELD: OIndexField = std::mem::zeroed();
	pub static mut KEY_FIELDS: std::os::raw::c_int = 0;
	pub static mut I: std::os::raw::c_int = 0;

	key_fields = primary->nKeyFields;

	oldcontext = MemoryContextSwitchTo(base->maincontext);
	arg = (OIndexBuildSortArg *) palloc0(sizeof(OIndexBuildSortArg));
	arg->id = primary;
	arg->tupDesc = toast->leafTupdesc;
	arg->enforceUnique = true;

	base->sortKeys = (SortSupport)
		palloc0((key_fields + TOAST_NON_LEAF_FIELDS_NUM) *
				sizeof(SortSupportData));
	base->nKeys = key_fields + TOAST_NON_LEAF_FIELDS_NUM;

	base->removeabbrev = removeabbrev_orioledb_index;
	base->comparetup = comparetup_orioledb_index;
	base->writetup = writetup_orioledb_index;
	base->readtup = readtup_orioledb_index;
	base->arg = arg;

	for (i = 0; i < key_fields; i++)
	{
		sortKey = &base->sortKeys[i];

		sortKey->ssup_cxt = CurrentMemoryContext;
		sortKey->ssup_collation = primary->fields[i].collation;
		sortKey->ssup_nulls_first = primary->fields[i].nullfirst;
		sortKey->ssup_attno = i + 1;
		sortKey->abbreviate = (i == 0);
		sortKey->ssup_reverse = !primary->fields[i].ascending;
		// FIXME: no abbrev converter yet
		o_finish_sort_support_function(primary->fields[i].comparator, sortKey);
	}

	field.collation = DEFAULT_COLLATION_OID;

	// ATTN_POS
	sortKey = &base->sortKeys[key_fields];
	sortKey->ssup_cxt = CurrentMemoryContext;
	sortKey->ssup_collation = DEFAULT_COLLATION_OID;
	sortKey->ssup_nulls_first = false;
	sortKey->ssup_attno = key_fields + 1;
	sortKey->abbreviate = false;
	sortKey->ssup_reverse = false;
	oFillFieldOpClassAndComparator(&field, toast->oids.datoid,
								   INT2_BTREE_OPS_OID,
								   INT2OID,
								   InvalidOid,
								   F_HASHINT2);
	o_finish_sort_support_function(field.comparator, sortKey);

	// CHUNKN_POS
	sortKey = &base->sortKeys[key_fields + 1];
	sortKey->ssup_cxt = CurrentMemoryContext;
	sortKey->ssup_collation = DEFAULT_COLLATION_OID;
	sortKey->ssup_nulls_first = false;
	sortKey->ssup_attno = key_fields + 2;
	sortKey->abbreviate = false;
	sortKey->ssup_reverse = false;
	oFillFieldOpClassAndComparator(&field, toast->oids.datoid,
								   INT4_BTREE_OPS_OID,
								   INT4OID,
								   InvalidOid,
								   F_HASHINT4);
	o_finish_sort_support_function(field.comparator, sortKey);

	MemoryContextSwitchTo(oldcontext);

	pub static mut STATE: return = std::mem::zeroed();
}

OTuple
tuplesort_getotuple(state: &mut Tuplesortstate, bool forward)
{
	MemoryContext oldcontext = MemoryContextSwitchTo(TuplesortstateGetPublic(state)->sortcontext);
	pub static mut STUP: SortTuple = std::mem::zeroed();
	pub static mut RESULT: OTuple = std::mem::zeroed();

	if (!tuplesort_gettuple_common(state, forward, &stup))
		stup.tuple = NULL;

	MemoryContextSwitchTo(oldcontext);

	if (stup.tuple)
	{
		result = read_o_tuple(stup.tuple);
	}
	else
	{
		result.data = NULL;
		result.formatFlags = 0;
	}

	pub static mut RESULT: return = std::mem::zeroed();
}


tuplesort_putotuple(state: &mut Tuplesortstate, OTuple tup)
{
	base: &mut TuplesortPublic = TuplesortstateGetPublic(state);
	arg: &mut OIndexBuildSortArg = (OIndexBuildSortArg *) base->arg;
	pub static mut O_TUPLE_FIXED_FORMAT_SPEC: *mut spec = &arg->id->leafSpec;
	MemoryContext oldcontext = MemoryContextSwitchTo(base->tuplecontext);
	pub static mut STUP: SortTuple = std::mem::zeroed();
	pub static mut TUPSIZE: std::os::raw::c_int = 0;
	pub static mut WRITTEN_TUP: OTuple = std::mem::zeroed();
#if PG_VERSION_NUM >= 170000
	pub static mut TUPLEN: Size = 0;
#endif

	//
// Copy the given tuple into memory we control, and decrease availMem.
// Then call the common code.
//
	tupsize = o_tuple_size(tup, spec);
	stup.tuple = MemoryContextAlloc(base->tuplecontext, MAXIMUM_ALIGNOF + tupsize);
	write_o_tuple(stup.tuple, tup, tupsize);
	written_tup = read_o_tuple(stup.tuple);

	stup.datum1 = o_fastgetattr(written_tup,
								base->sortKeys[0].ssup_attno,
								arg->tupDesc,
								spec,
								&stup.isnull1);
#if PG_VERSION_NUM >= 170000
	// GetMemoryChunkSpace is not supported for bump contexts
	if (TupleSortUseBumpTupleCxt(base->sortopt))
		tuplen = MAXALIGN(tupsize);
	else
		tuplen = GetMemoryChunkSpace(stup.tuple);

	tuplesort_puttuple_common(state, &stup,
							  base->sortKeys->abbrev_converter && !stup.isnull1, tuplen);
#else
	tuplesort_puttuple_common(state, &stup,
							  base->sortKeys->abbrev_converter && !stup.isnull1);
#endif
	MemoryContextSwitchTo(oldcontext);
}