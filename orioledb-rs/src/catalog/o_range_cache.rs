use crate::access::hash;
use crate::access::htup_details;
use crate::access::xlogrecovery;
use crate::catalog::o_sys_cache;
use crate::catalog::pg_amproc;
use crate::catalog::pg_opclass;
use crate::catalog::pg_range;
use crate::catalog::pg_type;
use crate::catalog::sys_trees;
use crate::orioledb;
use crate::pgstat;
use crate::recovery::recovery;
use crate::utils::fmgrtab;
use crate::utils::lsyscache;
use crate::utils::memutils;
use crate::utils::syscache;
use pgrx::pg_sys;

// -------------------------------------------------------------------------
//
// o_range_cache.c
// Routines for orioledb range sys cache.
//
// range_cache is tree that contains cached range metadata from pg_type.
//
// Copyright (c) 2021-2026, Oriole DB Inc.
// Copyright (c) 2025-2026, Supabase Inc.
//
// IDENTIFICATION
// contrib/orioledb/src/catalog/o_range_cache.c
//
// -------------------------------------------------------------------------
//

static mut O_SYS_CACHE: *mut range_cache = std::ptr::null_mut();

fn o_range_cache_free_entry(Pointer entry);
fn o_range_cache_fill_entry(entry_ptr: &mut Pointer, key: &mut OSysCacheKey,
									 Pointer arg);

O_SYS_CACHE_FUNCS(range_cache, ORange, 1);

static OSysCacheFuncs range_cache_funcs =
{
	.free_entry = o_range_cache_free_entry,
	.fill_entry = o_range_cache_fill_entry
};

//
// Initializes the range sys cache memory.
//
O_SYS_CACHE_INIT_FUNC(range_cache)
{
	Oid			keytypes[] = {OIDOID};

	range_cache = o_create_sys_cache(SYS_TREES_RANGE_CACHE, false,
									 RangeTypidIndexId, RANGETYPE, 1, keytypes,
									 0, fastcache, mcxt, &range_cache_funcs);
}

fn
o_range_cache_fill_entry(entry_ptr: &mut Pointer, key: &mut OSysCacheKey, Pointer arg)
{
	pub static mut RANGETUP: HeapTuple = std::mem::zeroed();
	pub static mut RANGEFORM: Form_pg_range = std::mem::zeroed();
	o_range: &mut ORange = (ORange *) *entry_ptr;
	pub static mut PREV_CONTEXT: MemoryContext = std::mem::zeroed();
	pub static mut RNGTYPID: Oid = std::mem::zeroed();

	rngtypid = DatumGetObjectId(key->keys[0]);

	rangetup = SearchSysCache1(RANGETYPE, key->keys[0]);
	if (!HeapTupleIsValid(rangetup))
		elog(ERROR, "cache lookup failed for range (%u)", rngtypid);
	rangeform = (Form_pg_range) GETSTRUCT(rangetup);

	prev_context = MemoryContextSwitchTo(range_cache->mcxt);
	if (o_range == NULL)
	{
		o_range = palloc0(sizeof(ORange));
		*entry_ptr = (Pointer) o_range;
	}

	o_range->rngsubtype = rangeform->rngsubtype;
	o_range->rngsubopc = rangeform->rngsubopc;
	o_range->rngcollation = rangeform->rngcollation;

	MemoryContextSwitchTo(prev_context);
	ReleaseSysCache(rangetup);
}

fn
o_range_cache_free_entry(Pointer entry)
{
	pfree(entry);
}

//
// A tuple print function for o_print_btree_pages()
//

o_range_cache_tup_print(desc: &mut BTreeDescr, StringInfo buf,
						OTuple tup, Pointer arg)
{
	o_range: &mut ORange = (ORange *) tup.data;

	appendStringInfo(buf, "(");
	o_sys_cache_key_print(desc, buf, tup, arg);
	appendStringInfo(buf, ", rngsubtype: %u, rngcollation: %d, "
					 "rngsubopc: %u)",
					 o_range->rngsubtype, o_range->rngcollation,
					 o_range->rngsubopc);
}

HeapTuple
o_range_cache_search_htup(TupleDesc tupdesc, Oid rngtypid)
{
	pub static mut CUR_LSN: XLogRecPtr = std::mem::zeroed();
	pub static mut DATOID: Oid = std::mem::zeroed();
	pub static mut RESULT: HeapTuple = std::ptr::null_mut();
	Datum		values[Natts_pg_range] = {0};
	bool		nulls[Natts_pg_range] = {0};
	pub static mut O_RANGE: *mut o_range = std::ptr::null_mut();

	o_sys_cache_set_datoid_lsn(&cur_lsn, &datoid);
	o_range =
		o_range_cache_search(datoid, rngtypid, cur_lsn, range_cache->nkeys);
	if (o_range)
	{
		values[Anum_pg_range_rngtypid - 1] = o_range->key.keys[0];
		values[Anum_pg_range_rngcollation - 1] =
			ObjectIdGetDatum(o_range->rngcollation);
		values[Anum_pg_range_rngsubopc - 1] =
			ObjectIdGetDatum(o_range->rngsubopc);
		values[Anum_pg_range_rngsubtype - 1] =
			ObjectIdGetDatum(o_range->rngsubtype);

		result = heap_form_tuple(tupdesc, values, nulls);
	}
	pub static mut RESULT: return = std::mem::zeroed();
}

static mut O_SYS_CACHE: *mut multirange_cache = std::ptr::null_mut();

fn o_multirange_cache_free_entry(Pointer entry);
fn o_multirange_cache_fill_entry(entry_ptr: &mut Pointer,
										  key: &mut OSysCacheKey,
										  Pointer arg);

O_SYS_CACHE_FUNCS(multirange_cache, OMultiRange, 1);
static OSysCacheFuncs multirange_cache_funcs =
{
	.free_entry = o_multirange_cache_free_entry,
	.fill_entry = o_multirange_cache_fill_entry
};

O_SYS_CACHE_INIT_FUNC(multirange_cache)
{
	Oid			keytypes[] = {OIDOID};

	multirange_cache = o_create_sys_cache(SYS_TREES_MULTIRANGE_CACHE, false,
										  RangeMultirangeTypidIndexId,
										  RANGEMULTIRANGE, 1, keytypes, 0,
										  fastcache, mcxt,
										  &multirange_cache_funcs);
}

fn
o_multirange_cache_fill_entry(entry_ptr: &mut Pointer, key: &mut OSysCacheKey,
							  Pointer arg)
{
	pub static mut RANGETUP: HeapTuple = std::mem::zeroed();
	pub static mut RANGEFORM: Form_pg_range = std::mem::zeroed();
	o_multirange: &mut OMultiRange = (OMultiRange *) *entry_ptr;
	pub static mut PREV_CONTEXT: MemoryContext = std::mem::zeroed();
	pub static mut RNGTYPID: Oid = std::mem::zeroed();

	rngtypid = DatumGetObjectId(key->keys[0]);

	rangetup = SearchSysCache1(RANGEMULTIRANGE, key->keys[0]);
	if (!HeapTupleIsValid(rangetup))
		elog(ERROR, "cache lookup failed for multirange (%u)", rngtypid);
	rangeform = (Form_pg_range) GETSTRUCT(rangetup);

	prev_context = MemoryContextSwitchTo(range_cache->mcxt);
	if (o_multirange == NULL)
	{
		o_multirange = palloc0(sizeof(OMultiRange));
		*entry_ptr = (Pointer) o_multirange;
	}

	o_multirange->rngtypid = rangeform->rngtypid;

	MemoryContextSwitchTo(prev_context);
	ReleaseSysCache(rangetup);
}

fn
o_multirange_cache_free_entry(Pointer entry)
{
	pfree(entry);
}


o_multirange_cache_tup_print(desc: &mut BTreeDescr, StringInfo buf, OTuple tup,
							 Pointer arg)
{
	o_multirange: &mut OMultiRange = (OMultiRange *) tup.data;

	appendStringInfo(buf, "(");
	o_sys_cache_key_print(desc, buf, tup, arg);
	appendStringInfo(buf, ", rngtypid: %u)", o_multirange->rngtypid);
}

HeapTuple
o_multirange_cache_search_htup(TupleDesc tupdesc, Oid rngmultitypid)
{
	pub static mut CUR_LSN: XLogRecPtr = std::mem::zeroed();
	pub static mut DATOID: Oid = std::mem::zeroed();
	pub static mut RESULT: HeapTuple = std::ptr::null_mut();
	Datum		values[Natts_pg_range] = {0};
	bool		nulls[Natts_pg_range] = {0};
	pub static mut O_MULTI_RANGE: *mut o_multirange = std::ptr::null_mut();

	o_sys_cache_set_datoid_lsn(&cur_lsn, &datoid);
	o_multirange = o_multirange_cache_search(datoid, rngmultitypid, cur_lsn,
											 multirange_cache->nkeys);
	if (o_multirange)
	{
		values[Anum_pg_range_rngtypid - 1] = o_multirange->key.keys[0];
		values[Anum_pg_range_rngtypid - 1] =
			ObjectIdGetDatum(o_multirange->rngtypid);

		result = heap_form_tuple(tupdesc, values, nulls);
	}
	pub static mut RESULT: return = std::mem::zeroed();
}