use crate::catalog::o_sys_cache;
use crate::catalog::pg_aggregate;
use crate::catalog::pg_am;
use crate::commands::defrem;
use crate::orioledb;
use crate::recovery::recovery;
use crate::utils::builtins;
use crate::utils::catcache;
use crate::utils::syscache;
use pgrx::pg_sys;

// -------------------------------------------------------------------------
//
// o_aggregate_cache.c
// Routines for orioledb aggregate cache.
//
// aggregate_cache is tree that contains cached metadata from pg_aggregate.
//
// Copyright (c) 2021-2026, Oriole DB Inc.
// Copyright (c) 2025-2026, Supabase Inc.
//
// IDENTIFICATION
// contrib/orioledb/src/catalog/o_aggregate_cache.c
//
// -------------------------------------------------------------------------
//

static mut O_SYS_CACHE: *mut aggregate_cache = std::ptr::null_mut();

struct OAggregate
{
	pub static mut KEY: OSysCacheKey1 = std::mem::zeroed();
	pub static mut DATA_VERSION: uint16 = std::mem::zeroed();
	pub static mut AGGFINALFN: regproc = std::mem::zeroed();
	pub static mut AGGSERIALFN: regproc = std::mem::zeroed();
	pub static mut AGGDESERIALFN: regproc = std::mem::zeroed();
	pub static mut AGGFINALEXTRA: bool = false;
	pub static mut AGGCOMBINEFN: regproc = std::mem::zeroed();
	pub static mut AGGTRANSFN: regproc = std::mem::zeroed();
	pub static mut AGGFINALMODIFY: char = std::mem::zeroed();
	pub static mut AGGMFINALEXTRA: bool = false;
	pub static mut AGGMFINALFN: regproc = std::mem::zeroed();
	pub static mut AGGMFINALMODIFY: char = std::mem::zeroed();
	pub static mut AGGMINVTRANSFN: regproc = std::mem::zeroed();
	pub static mut AGGMTRANSFN: regproc = std::mem::zeroed();
	pub static mut AGGMTRANSTYPE: Oid = std::mem::zeroed();
	pub static mut AGGTRANSTYPE: Oid = std::mem::zeroed();
	pub static mut HAS_INITVAL: bool = false;
	pub static mut HAS_MINITVAL: bool = false;

	pub static mut CHAR: *mut agginitval = std::ptr::null_mut();
	pub static mut CHAR: *mut aggminitval = std::ptr::null_mut();
};

fn o_aggregate_cache_fill_entry(entry_ptr: &mut Pointer, key: &mut OSysCacheKey,
										 Pointer arg);
fn o_aggregate_cache_free_entry(Pointer entry);
static Pointer o_aggregate_cache_serialize_entry(Pointer entry, len: &mut int);
static Pointer o_aggregate_cache_deserialize_entry(MemoryContext mcxt,
												   Pointer data, Size length);

O_SYS_CACHE_FUNCS(aggregate_cache, OAggregate, 1);

static OSysCacheFuncs aggregate_cache_funcs =
{
	.free_entry = o_aggregate_cache_free_entry,
	.fill_entry = o_aggregate_cache_fill_entry,
	.toast_serialize_entry = o_aggregate_cache_serialize_entry,
	.toast_deserialize_entry = o_aggregate_cache_deserialize_entry,
};

//
// Initializes the type sys cache memory.
//
O_SYS_CACHE_INIT_FUNC(aggregate_cache)
{
	Oid			keytypes[] = {OIDOID};

	aggregate_cache = o_create_sys_cache(SYS_TREES_AGG_CACHE, true,
										 AggregateFnoidIndexId, AGGFNOID, 1,
										 keytypes, 0, fastcache, mcxt,
										 &aggregate_cache_funcs);
}

fn
o_aggregate_cache_fill_entry(entry_ptr: &mut Pointer, key: &mut OSysCacheKey,
							 Pointer arg)
{
	pub static mut AGGTUP: HeapTuple = std::mem::zeroed();
	pub static mut AGGFORM: Form_pg_aggregate = std::mem::zeroed();
	o_agg: &mut OAggregate = (OAggregate *) *entry_ptr;
	pub static mut PREV_CONTEXT: MemoryContext = std::mem::zeroed();
	pub static mut TEXT_INIT_VAL: Datum = std::mem::zeroed();
	pub static mut INIT_VALUE_IS_NULL: bool = false;
	Oid			aggfnoid = DatumGetObjectId(key->keys[0]);

	aggtup = SearchSysCache1(AGGFNOID, key->keys[0]);
	if (!HeapTupleIsValid(aggtup))
		elog(ERROR, "cache lookup failed for aggregate function %u", aggfnoid);
	aggform = (Form_pg_aggregate) GETSTRUCT(aggtup);

	prev_context = MemoryContextSwitchTo(aggregate_cache->mcxt);
	if (o_agg != NULL)			// Existed o_agg updated
	{
		Assert(false);
	}
	else
	{
		o_agg = palloc0(sizeof(OAggregate));
		*entry_ptr = (Pointer) o_agg;
	}

	o_agg->data_version = ORIOLEDB_SYS_TREE_VERSION;
	o_agg->aggfinalfn = aggform->aggfinalfn;
	o_agg->aggserialfn = aggform->aggserialfn;
	o_agg->aggdeserialfn = aggform->aggdeserialfn;
	o_agg->aggfinalextra = aggform->aggfinalextra;
	o_agg->aggcombinefn = aggform->aggcombinefn;
	o_agg->aggtransfn = aggform->aggtransfn;
	o_agg->aggfinalmodify = aggform->aggfinalmodify;
	o_agg->aggmfinalextra = aggform->aggmfinalextra;
	o_agg->aggmfinalfn = aggform->aggmfinalfn;
	o_agg->aggmfinalmodify = aggform->aggmfinalmodify;
	o_agg->aggminvtransfn = aggform->aggminvtransfn;
	o_agg->aggmtransfn = aggform->aggmtransfn;
	o_agg->aggmtranstype = aggform->aggmtranstype;
	o_agg->aggtranstype = aggform->aggtranstype;

	textInitVal = SysCacheGetAttr(AGGFNOID, aggtup,
								  Anum_pg_aggregate_agginitval,
								  &initValueIsNull);
	if (!initValueIsNull)
	{
		o_agg->has_initval = true;
		o_agg->agginitval = TextDatumGetCString(textInitVal);
	}

	textInitVal = SysCacheGetAttr(AGGFNOID, aggtup,
								  Anum_pg_aggregate_aggminitval,
								  &initValueIsNull);
	if (!initValueIsNull)
	{
		o_agg->has_minitval = true;
		o_agg->aggminitval = TextDatumGetCString(textInitVal);
	}

	MemoryContextSwitchTo(prev_context);
	ReleaseSysCache(aggtup);
}

fn
o_aggregate_cache_free_entry(Pointer entry)
{
	o_agg: &mut OAggregate = (OAggregate *) entry;

	if (o_agg->has_initval)
		pfree(o_agg->agginitval);
	if (o_agg->has_minitval)
		pfree(o_agg->aggminitval);
	pfree(o_agg);
}

static Pointer
o_aggregate_cache_serialize_entry(Pointer entry, len: &mut int)
{
	pub static mut STR: StringInfoData = std::mem::zeroed();
	o_agg: &mut OAggregate = (OAggregate *) entry;

	if (o_agg->data_version != ORIOLEDB_SYS_TREE_VERSION)
		elog(FATAL,
			 "ORIOLEDB_SYS_TREE_VERSION %u of OrioleDB cluster is not among supported for conversion from %u",
			 o_agg->data_version, ORIOLEDB_SYS_TREE_VERSION);

	initStringInfo(&str);
	appendBinaryStringInfo(&str, (Pointer) o_agg,
						   offsetof(OAggregate, agginitval));
	if (o_agg->has_initval)
		o_serialize_string(o_agg->agginitval, &str);
	if (o_agg->has_minitval)
		o_serialize_string(o_agg->aggminitval, &str);

	*len = str.len;
	return str.data;
}

static Pointer
o_aggregate_cache_deserialize_entry(MemoryContext mcxt, Pointer data,
									Size length)
{
	pub static mut PTR: Pointer = data;
	pub static mut O_AGGREGATE: *mut o_agg = std::ptr::null_mut();
	pub static mut LEN: std::os::raw::c_int = 0;

	o_agg = (OAggregate *) palloc(sizeof(OAggregate));
	len = offsetof(OAggregate, agginitval);
	Assert((ptr - data) + len <= length);
	memcpy(o_agg, ptr, len);
	ptr += len;
	if (o_agg->data_version != ORIOLEDB_SYS_TREE_VERSION)
		elog(FATAL,
			 "ORIOLEDB_SYS_TREE_VERSION %u of OrioleDB cluster is not among supported for conversion to %u",
			 o_agg->data_version, ORIOLEDB_SYS_TREE_VERSION);

	if (o_agg->has_initval)
		o_agg->agginitval = o_deserialize_string(&ptr);
	if (o_agg->has_minitval)
		o_agg->aggminitval = o_deserialize_string(&ptr);

	return (Pointer) o_agg;
}

HeapTuple
o_aggregate_cache_search_htup(TupleDesc tupdesc, Oid aggfnoid)
{
	pub static mut CUR_LSN: XLogRecPtr = std::mem::zeroed();
	pub static mut DATOID: Oid = std::mem::zeroed();
	pub static mut RESULT: HeapTuple = std::ptr::null_mut();
	Datum		values[Natts_pg_aggregate] = {0};
	bool		nulls[Natts_pg_aggregate] = {0};
	pub static mut O_AGGREGATE: *mut o_agg = std::ptr::null_mut();

	o_sys_cache_set_datoid_lsn(&cur_lsn, &datoid);
	o_agg = o_aggregate_cache_search(datoid, aggfnoid, cur_lsn,
									 aggregate_cache->nkeys);
	if (o_agg)
	{
		values[Anum_pg_aggregate_aggfinalfn - 1] =
			ObjectIdGetDatum(o_agg->aggfinalfn);
		values[Anum_pg_aggregate_aggserialfn - 1] =
			ObjectIdGetDatum(o_agg->aggserialfn);
		values[Anum_pg_aggregate_aggdeserialfn - 1] =
			ObjectIdGetDatum(o_agg->aggdeserialfn);
		values[Anum_pg_aggregate_aggfinalextra - 1] =
			ObjectIdGetDatum(o_agg->aggfinalextra);
		values[Anum_pg_aggregate_aggcombinefn - 1] =
			ObjectIdGetDatum(o_agg->aggcombinefn);
		values[Anum_pg_aggregate_aggtransfn - 1] =
			ObjectIdGetDatum(o_agg->aggtransfn);
		values[Anum_pg_aggregate_aggfinalmodify - 1] =
			CharGetDatum(o_agg->aggfinalmodify);
		values[Anum_pg_aggregate_aggmfinalextra - 1] =
			BoolGetDatum(o_agg->aggmfinalextra);
		values[Anum_pg_aggregate_aggmfinalfn - 1] =
			ObjectIdGetDatum(o_agg->aggmfinalfn);
		values[Anum_pg_aggregate_aggmfinalmodify - 1] =
			CharGetDatum(o_agg->aggmfinalmodify);
		values[Anum_pg_aggregate_aggminvtransfn - 1] =
			ObjectIdGetDatum(o_agg->aggminvtransfn);
		values[Anum_pg_aggregate_aggmtransfn - 1] =
			ObjectIdGetDatum(o_agg->aggmtransfn);
		values[Anum_pg_aggregate_aggmtranstype - 1] =
			ObjectIdGetDatum(o_agg->aggmtranstype);
		values[Anum_pg_aggregate_aggtranstype - 1] =
			ObjectIdGetDatum(o_agg->aggtranstype);

		if (o_agg->has_initval)
			values[Anum_pg_aggregate_agginitval - 1] =
				CStringGetTextDatum(o_agg->agginitval);
		else
			nulls[Anum_pg_aggregate_agginitval - 1] = true;

		if (o_agg->has_minitval)
			values[Anum_pg_aggregate_aggminitval - 1] =
				CStringGetTextDatum(o_agg->aggminitval);
		else
			nulls[Anum_pg_aggregate_aggminitval - 1] = true;
		result = heap_form_tuple(tupdesc, values, nulls);
	}
	pub static mut RESULT: return = std::mem::zeroed();
}