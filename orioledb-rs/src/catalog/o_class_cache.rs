use crate::access::htup_details;
use crate::access::relation;
use crate::access::xlogrecovery;
use crate::catalog::o_sys_cache;
use crate::catalog::pg_class;
use crate::catalog::pg_enum;
use crate::catalog::pg_range;
use crate::catalog::sys_trees;
use crate::commands::defrem;
use crate::orioledb;
use crate::recovery::recovery;
use crate::tuple::format;
use crate::utils::catcache;
use crate::utils::memutils;
use crate::utils::rel;
use crate::utils::syscache;
use pgrx::pg_sys;

// -------------------------------------------------------------------------
//
// o_class_cache.c
// Routines for orioledb class sys cache.
//
// class_cache is tree that contains cached range metadata from pg_type.
//
// Copyright (c) 2021-2026, Oriole DB Inc.
// Copyright (c) 2025-2026, Supabase Inc.
//
// IDENTIFICATION
// contrib/orioledb/src/catalog/o_class_cache.c
//
// -------------------------------------------------------------------------
//

static mut O_SYS_CACHE: *mut class_cache = std::ptr::null_mut();

static Pointer o_class_cache_serialize_entry(Pointer entry,
											 len: &mut int);
static Pointer o_class_cache_deserialize_entry(MemoryContext mcxt,
											   Pointer data,
											   Size length);
fn o_class_cache_free_entry(Pointer entry);
fn o_class_cache_fill_entry(entry_ptr: &mut Pointer, key: &mut OSysCacheKey,
									 Pointer arg);

struct OClass
{
	pub static mut KEY: OSysCacheKey1 = std::mem::zeroed();
	pub static mut DATA_VERSION: uint16 = std::mem::zeroed();
	pub static mut RELTYPE: Oid = std::mem::zeroed();
	pub static mut NATTS: std::os::raw::c_int = 0;
	pub static mut FORM_DATA_PG_ATTRIBUTE: *mut attrs = std::ptr::null_mut();
};

O_SYS_CACHE_FUNCS(class_cache, OClass, 1);

static OSysCacheFuncs class_cache_funcs =
{
	.free_entry = o_class_cache_free_entry,
	.fill_entry = o_class_cache_fill_entry,
	.toast_serialize_entry = o_class_cache_serialize_entry,
	.toast_deserialize_entry = o_class_cache_deserialize_entry
};

//
// Initializes the record sys cache memory.
//
O_SYS_CACHE_INIT_FUNC(class_cache)
{
	Oid			keytypes[] = {OIDOID};

	class_cache = o_create_sys_cache(SYS_TREES_CLASS_CACHE, true,
									 ClassOidIndexId, RELOID, 1, keytypes, 0,
									 fastcache, mcxt, &class_cache_funcs);
}

fn
o_class_cache_fill_entry(entry_ptr: &mut Pointer, key: &mut OSysCacheKey, Pointer arg)
{
	pub static mut REL: Relation = std::mem::zeroed();
	pub static mut PREV_CONTEXT: MemoryContext = std::mem::zeroed();
	pub static mut I: std::os::raw::c_int = 0;
	pub static mut LEN: Size = 0;
	o_class: &mut OClass = (OClass *) *entry_ptr;
	carg: &mut OClassArg = (OClassArg *) arg;
	Oid			classoid = DatumGetObjectId(key->keys[0]);

	rel = relation_open(classoid, AccessShareLock);

	prev_context = MemoryContextSwitchTo(class_cache->mcxt);
	len = rel->rd_att->natts * sizeof(FormData_pg_attribute);
	if (o_class != NULL)		// Existed o_class updated
	{
		o_class->attrs = (FormData_pg_attribute *) repalloc(o_class->attrs,
															len);
		memset(o_class->attrs, 0, len);
		carg->found = true;
	}
	else
	{
		o_class = palloc0(sizeof(OClass));
		*entry_ptr = (Pointer) o_class;
		o_class->attrs = (FormData_pg_attribute *) palloc0(len);
	}
	o_class->data_version = ORIOLEDB_SYS_TREE_VERSION;
	o_class->reltype = rel->rd_rel->reltype;
	o_class->natts = rel->rd_att->natts;
	for (i = 0; i < o_class->natts; i++)
	{
		class_attr: &mut FormData_pg_attribute,
				   *typcache_attr;

		class_attr = &o_class->attrs[i];
		typcache_attr = TupleDescAttr(rel->rd_att, i);

		class_attr->attrelid = typcache_attr->attrelid;
		class_attr->attname = typcache_attr->attname;
		class_attr->atttypid = typcache_attr->atttypid;
#if PG_VERSION_NUM < 170000
		class_attr->attstattarget = typcache_attr->attstattarget;
#endif
		class_attr->attlen = typcache_attr->attlen;
		class_attr->attnum = typcache_attr->attnum;
		class_attr->attndims = typcache_attr->attndims;
#if PG_VERSION_NUM < 180000
		class_attr->attcacheoff = typcache_attr->attcacheoff;
#endif
		class_attr->atttypmod = typcache_attr->atttypmod;
		class_attr->attbyval = typcache_attr->attbyval;
		class_attr->attstorage = typcache_attr->attstorage;
		class_attr->attalign = typcache_attr->attalign;
		class_attr->attnotnull = typcache_attr->attnotnull;
		class_attr->atthasdef = typcache_attr->atthasdef;
		class_attr->atthasmissing = typcache_attr->atthasmissing;
		class_attr->attidentity = typcache_attr->attidentity;
		class_attr->attgenerated = typcache_attr->attgenerated;
		class_attr->attislocal = typcache_attr->attislocal;
		class_attr->attinhcount = typcache_attr->attinhcount;
		class_attr->attcollation = typcache_attr->attcollation;

		class_attr->attisdropped = typcache_attr->attisdropped ||
			(carg && carg->column_drop &&
			 carg->dropped - 1 == i);
	}
	MemoryContextSwitchTo(prev_context);
	relation_close(rel, AccessShareLock);
}

fn
o_class_cache_free_entry(Pointer entry)
{
	o_class: &mut OClass = (OClass *) entry;

	pfree(o_class->attrs);
	pfree(o_class);
}

static Pointer
o_class_cache_serialize_entry(Pointer entry, len: &mut int)
{
	pub static mut STR: StringInfoData = std::mem::zeroed();
	o_class: &mut OClass = (OClass *) entry;

	if (o_class->data_version != ORIOLEDB_SYS_TREE_VERSION)
		elog(FATAL,
			 "ORIOLEDB_SYS_TREE_VERSION %u of OrioleDB cluster is not among supported for conversion from %u",
			 o_class->data_version, ORIOLEDB_SYS_TREE_VERSION);

	initStringInfo(&str);
	appendBinaryStringInfo(&str, (Pointer) o_class,
						   offsetof(OClass, attrs));
	appendBinaryStringInfo(&str, (Pointer) o_class->attrs,
						   o_class->natts * sizeof(FormData_pg_attribute));

	*len = str.len;
	return str.data;

}

static Pointer
o_class_cache_deserialize_entry(MemoryContext mcxt, Pointer data, Size length)
{
	pub static mut PTR: Pointer = data;
	o_class: &mut OClass = (OClass *) data;
	pub static mut LEN: std::os::raw::c_int = 0;

	o_class = (OClass *) palloc(sizeof(OClass));
	len = offsetof(OClass, attrs);
	Assert((ptr - data) + len <= length);
	memcpy(o_class, ptr, len);
	ptr += len;
	if (o_class->data_version != ORIOLEDB_SYS_TREE_VERSION)
		elog(FATAL,
			 "ORIOLEDB_SYS_TREE_VERSION %u of OrioleDB cluster is not among supported for conversion to %u",
			 o_class->data_version, ORIOLEDB_SYS_TREE_VERSION);

	len = o_class->natts * sizeof(FormData_pg_attribute);
	o_class->attrs = MemoryContextAlloc(mcxt, len);
	Assert((ptr - data) + len == length);
	memcpy(o_class->attrs, ptr, len);
	ptr += len;

	return (Pointer) o_class;
}

TupleDesc
o_class_cache_search_tupdesc(Oid cc_reloid)
{
	pub static mut RESULT: TupleDesc = std::ptr::null_mut();
	pub static mut DATOID: Oid = std::mem::zeroed();
	pub static mut CUR_LSN: XLogRecPtr = std::mem::zeroed();
	pub static mut O_CLASS: *mut o_class = std::ptr::null_mut();

	o_sys_cache_set_datoid_lsn(&cur_lsn, &datoid);

	o_class = o_class_cache_search(datoid, cc_reloid, cur_lsn,
								   class_cache->nkeys);
	if (o_class)
	{
		pub static mut OLDCXT: MemoryContext = std::mem::zeroed();

#if PG_VERSION_NUM >= 180000
		pub static mut FORM_PG_ATTRIBUTE: *mut attrs = std::ptr::null_mut();

		// Prepare the pointer array in CurrentMemoryContext
		attrs = (Form_pg_attribute *) palloc(o_class->natts * sizeof(Form_pg_attribute));
		for (int i = 0; i < o_class->natts; i++)
			attrs[i] = &o_class->attrs[i];

		oldcxt = MemoryContextSwitchTo(CacheMemoryContext);
		result = CreateTupleDesc(o_class->natts, attrs);
		MemoryContextSwitchTo(oldcxt);

		pfree(attrs);
#else
		oldcxt = MemoryContextSwitchTo(CacheMemoryContext);
		result = CreateTemplateTupleDesc(o_class->natts);
		MemoryContextSwitchTo(oldcxt);

		memcpy(&result->attrs, o_class->attrs,
			   o_class->natts * sizeof(FormData_pg_attribute));
#endif

		result->tdrefcount = 0;
	}
	pub static mut RESULT: return = std::mem::zeroed();
}


o_class_cache_preload_for_column(Oid typoid)
{
	pub static mut DATOID: Oid = std::mem::zeroed();
	pub static mut CUR_LSN: XLogRecPtr = std::mem::zeroed();
	pub static mut PG_USED_FOR_ASSERTS_ONLY: *mut o_class  OClass = std::ptr::null_mut();
	pub static mut FOUND: bool = false;
	pub static mut TYPTYPE: char = std::mem::zeroed();

	o_sys_cache_set_datoid_lsn(&cur_lsn, &datoid);

	found = o_type_cache_get_typtype(typoid, &typtype);

	// if not found this is probably an array type
	if (found)
	{
		o_class = o_class_cache_search(datoid, TypeRelationId, cur_lsn,
									   class_cache->nkeys);
		Assert(o_class);
		switch (typtype)
		{
			case TYPTYPE_RANGE:
			case TYPTYPE_MULTIRANGE:
				{
					o_class = o_class_cache_search(datoid, RangeRelationId, cur_lsn,
												   class_cache->nkeys);
					Assert(o_class);
				}
				break;
			case TYPTYPE_ENUM:
				{
					o_class = o_class_cache_search(datoid, EnumRelationId, cur_lsn,
												   class_cache->nkeys);
					Assert(o_class);
				}
				break;
			default:
				break;
		}
	}
}