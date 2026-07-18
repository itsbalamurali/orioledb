use crate::catalog::o_sys_cache;
use crate::catalog::pg_amproc;
use crate::commands::defrem;
use crate::orioledb;
use crate::recovery::recovery;
use crate::utils::builtins;
use crate::utils::catcache;
use crate::utils::syscache;
use pgrx::pg_sys;

// -------------------------------------------------------------------------
//
// o_amproc_cache.c
// Routines for orioledb amproc cache.
//
// amproc_cache is tree that contains cached metadata from pg_amproc.
//
// Copyright (c) 2021-2026, Oriole DB Inc.
// Copyright (c) 2025-2026, Supabase Inc.
//
// IDENTIFICATION
// contrib/orioledb/src/catalog/o_amproc_cache.c
//
// -------------------------------------------------------------------------
//

static mut O_SYS_CACHE: *mut amproc_cache = std::ptr::null_mut();

fn o_amproc_cache_fill_entry(entry_ptr: &mut Pointer, key: &mut OSysCacheKey,
									  Pointer arg);
fn o_amproc_cache_free_entry(Pointer entry);

O_SYS_CACHE_FUNCS(amproc_cache, OAmProc, 4);

static OSysCacheFuncs amproc_cache_funcs =
{
	.free_entry = o_amproc_cache_free_entry,
	.fill_entry = o_amproc_cache_fill_entry
};

//
// Initializes the type sys cache memory.
//
O_SYS_CACHE_INIT_FUNC(amproc_cache)
{
	Oid			keytypes[] = {OIDOID, OIDOID, OIDOID, INT2OID};

	amproc_cache = o_create_sys_cache(SYS_TREES_AMPROC_CACHE, false,
									  AccessMethodProcedureIndexId, AMPROCNUM,
									  4, keytypes, 0, fastcache, mcxt,
									  &amproc_cache_funcs);
}

fn
o_amproc_cache_fill_entry(entry_ptr: &mut Pointer, key: &mut OSysCacheKey, Pointer arg)
{
	pub static mut AMPROCTUP: HeapTuple = std::mem::zeroed();
	pub static mut AMPROCFORM: Form_pg_amproc = std::mem::zeroed();
	o_amproc: &mut OAmProc = (OAmProc *) *entry_ptr;
	pub static mut PREV_CONTEXT: MemoryContext = std::mem::zeroed();
	pub static mut AMPROCFAMILY: Oid = std::mem::zeroed();
	pub static mut AMPROCLEFTTYPE: Oid = std::mem::zeroed();
	pub static mut AMPROCRIGHTTYPE: Oid = std::mem::zeroed();
	pub static mut AMPROCNUM: int16 = std::mem::zeroed();

	amprocfamily = DatumGetObjectId(key->keys[0]);
	amproclefttype = DatumGetObjectId(key->keys[1]);
	amprocrighttype = DatumGetObjectId(key->keys[2]);
	amprocnum = DatumGetChar(key->keys[3]);

	amproctup = SearchSysCache4(AMPROCNUM, key->keys[0], key->keys[1],
								key->keys[2], key->keys[3]);
	if (!HeapTupleIsValid(amproctup))
		elog(ERROR, "cache lookup failed for amproc (%u %u %u %d)",
			 amprocfamily, amproclefttype, amprocrighttype, amprocnum);
	amprocform = (Form_pg_amproc) GETSTRUCT(amproctup);

	prev_context = MemoryContextSwitchTo(amproc_cache->mcxt);
	if (o_amproc != NULL)		// Existed o_amproc updated
	{
		Assert(false);
	}
	else
	{
		o_amproc = palloc0(sizeof(OAmProc));
		*entry_ptr = (Pointer) o_amproc;
	}

	o_amproc->amproc = amprocform->amproc;

	MemoryContextSwitchTo(prev_context);
	ReleaseSysCache(amproctup);
}

fn
o_amproc_cache_free_entry(Pointer entry)
{
	pfree(entry);
}

HeapTuple
o_amproc_cache_search_htup(TupleDesc tupdesc, Oid amprocfamily,
						   Oid amproclefttype, Oid amprocrighttype,
						   int16 amprocnum)
{
	pub static mut CUR_LSN: XLogRecPtr = std::mem::zeroed();
	pub static mut DATOID: Oid = std::mem::zeroed();
	pub static mut RESULT: HeapTuple = std::ptr::null_mut();
	Datum		values[Natts_pg_amproc] = {0};
	bool		nulls[Natts_pg_amproc] = {0};
	pub static mut O_AM_PROC: *mut o_amproc = std::ptr::null_mut();

	o_sys_cache_set_datoid_lsn(&cur_lsn, &datoid);
	o_amproc = o_amproc_cache_search(datoid, amprocfamily, amproclefttype,
									 amprocrighttype, amprocnum, cur_lsn,
									 amproc_cache->nkeys);
	if (o_amproc)
	{
		values[Anum_pg_amproc_amproc - 1] = ObjectIdGetDatum(o_amproc->amproc);

		result = heap_form_tuple(tupdesc, values, nulls);
	}
	pub static mut RESULT: return = std::mem::zeroed();
}

//
// A tuple print function for o_print_btree_pages()
//

o_amproc_cache_tup_print(desc: &mut BTreeDescr, StringInfo buf,
						 OTuple tup, Pointer arg)
{
	o_amproc: &mut OAmProc = (OAmProc *) tup.data;

	appendStringInfo(buf, "(");
	o_sys_cache_key_print(desc, buf, tup, arg);
	appendStringInfo(buf, ", amproc: %u)",
					 o_amproc->amproc);
}