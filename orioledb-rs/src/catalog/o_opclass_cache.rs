use crate::access::nbtree;
use crate::btree::iterator;
use crate::btree::modify;
use crate::catalog::o_sys_cache;
use crate::catalog::pg_am;
use crate::catalog::pg_amop;
use crate::catalog::pg_amproc;
use crate::catalog::pg_opclass;
use crate::catalog::pg_type;
use crate::checkpoint::checkpoint;
use crate::commands::defrem;
use crate::orioledb;
use crate::recovery::recovery;
use crate::recovery::wal;
use crate::utils::builtins;
use crate::utils::lsyscache;
use crate::utils::memutils;
use crate::utils::planner;
use crate::utils::stopevent;
use crate::utils::syscache;
use pgrx::pg_sys;

// -------------------------------------------------------------------------
//
// o_opclass_cache.c
// Routines for orioledb operator classes sys cache.
//
// Operator class B-tree stores data used by comparator and field initialization
// for orioledb engine tables.
//
// Copyright (c) 2021-2026, Oriole DB Inc.
// Copyright (c) 2025-2026, Supabase Inc.
//
// IDENTIFICATION
// contrib/orioledb/src/catalog/o_opclass_cache.c
//
// -------------------------------------------------------------------------
//

static mut O_SYS_CACHE: *mut opclass_cache = std::ptr::null_mut();

fn o_opclass_cache_free_entry(Pointer entry);
fn o_opclass_cache_fill_entry(entry_ptr: &mut Pointer, key: &mut OSysCacheKey,
									   Pointer arg);

O_SYS_CACHE_FUNCS(opclass_cache, OOpclass, 1);

static OSysCacheFuncs opclass_cache_funcs =
{
	.free_entry = o_opclass_cache_free_entry,
	.fill_entry = o_opclass_cache_fill_entry
};

//
// Initializes the opclass sys cache memory.
//
O_SYS_CACHE_INIT_FUNC(opclass_cache)
{
	Oid			keytypes[] = {OIDOID};

	opclass_cache = o_create_sys_cache(SYS_TREES_OPCLASS_CACHE, false,
									   OpclassOidIndexId, CLAOID, 1, keytypes,
									   0, fastcache, mcxt,
									   &opclass_cache_funcs);
}

//
// o_opclass_get
//
// Look up Oriole opclass metadata by (datoid, opclassoid).
//
// Why datoid matters:
// Oriole sys-cache entries are database-scoped. During global page-pool
// eviction, a backend may inspect index pages that belong to another
// database. In such paths, relying on MyDatabaseId can resolve metadata in
// the wrong database context, causing cache misses and descriptor failures.
//
// If datoid == InvalidOid, keep legacy behavior and infer database context
// from o_sys_cache_set_datoid_lsn(). New call sites should pass explicit
// object datoid whenever available.
//
OOpclass *
o_opclass_get(Oid opclassoid, Oid datoid)
{
	pub static mut CUR_LSN: XLogRecPtr = std::mem::zeroed();

	o_sys_cache_set_datoid_lsn(&cur_lsn, datoid == InvalidOid ? &datoid : NULL);
	return o_opclass_cache_search(datoid, opclassoid, cur_lsn,
								  opclass_cache->nkeys);
}

HeapTuple
o_opclass_cache_search_htup(TupleDesc tupdesc, Oid opclassoid)
{
	pub static mut CUR_LSN: XLogRecPtr = std::mem::zeroed();
	pub static mut DATOID: Oid = std::mem::zeroed();
	pub static mut RESULT: HeapTuple = std::ptr::null_mut();
	Datum		values[Natts_pg_opclass] = {0};
	bool		nulls[Natts_pg_opclass] = {0};
	pub static mut O_OPCLASS: *mut o_opclass = std::ptr::null_mut();
	pub static mut ONAME: NameData = std::mem::zeroed();

	o_sys_cache_set_datoid_lsn(&cur_lsn, &datoid);
	o_opclass = o_opclass_cache_search(datoid, opclassoid, cur_lsn,
									   opclass_cache->nkeys);
	if (o_opclass)
	{
		values[Anum_pg_opclass_oid - 1] = o_opclass->key.keys[0];
		namestrcpy(&oname, "");
		values[Anum_pg_opclass_opcname - 1] = NameGetDatum(&oname);
		values[Anum_pg_opclass_opcfamily - 1] =
			ObjectIdGetDatum(o_opclass->opfamily);
		values[Anum_pg_opclass_opcintype - 1] =
			ObjectIdGetDatum(o_opclass->inputtype);

		result = heap_form_tuple(tupdesc, values, nulls);
	}
	pub static mut RESULT: return = std::mem::zeroed();
}

fn
o_opclass_cache_fill_entry(entry_ptr: &mut Pointer, key: &mut OSysCacheKey, Pointer arg)
{
	pub static mut OPCLASSTUPLE: HeapTuple = std::mem::zeroed();
	pub static mut OPCLASSFORM: Form_pg_opclass = std::mem::zeroed();
	o_opclass: &mut OOpclass = (OOpclass *) *entry_ptr;
	Oid			opclassoid = DatumGetObjectId(key->keys[0]);
	pub static mut INPUTTYPE: Oid = std::mem::zeroed();

	//
// find typecache entry
//
	opclasstuple = SearchSysCache1(CLAOID, key->keys[0]);
	if (!HeapTupleIsValid(opclasstuple))
		elog(ERROR, "cache lookup failed for opclass %u", opclassoid);
	opclassform = (Form_pg_opclass) GETSTRUCT(opclasstuple);

	if (o_opclass == NULL)
	{
		o_opclass = palloc0(sizeof(OOpclass));
		*entry_ptr = (Pointer) o_opclass;
	}
	else
	{
		Assert(false);
	}

	o_opclass->opfamily = opclassform->opcfamily;
	o_opclass->inputtype = opclassform->opcintype;

	inputtype = o_opclass->inputtype;
	o_opclass->ssupOid = get_opfamily_proc(o_opclass->opfamily, inputtype,
										   inputtype, BTSORTSUPPORT_PROC);
	o_opclass->cmpOid = get_opfamily_proc(o_opclass->opfamily, inputtype,
										  inputtype, BTORDER_PROC);
	ReleaseSysCache(opclasstuple);
}

fn
o_opclass_cache_free_entry(Pointer entry)
{
	pfree(entry);
}

//
// A tuple print function for o_print_btree_pages()
//

o_opclass_cache_tup_print(desc: &mut BTreeDescr, StringInfo buf,
						  OTuple tup, Pointer arg)
{
	o_opclass: &mut OOpclass = (OOpclass *) tup.data;

	appendStringInfo(buf, "(");
	o_sys_cache_key_print(desc, buf, tup, arg);
	appendStringInfo(buf, ", opfamily: %u, inputtype: %d, "
					 "cmpOid: %u, ssupOid: %u)",
					 o_opclass->opfamily, o_opclass->inputtype,
					 o_opclass->cmpOid, o_opclass->ssupOid);
}