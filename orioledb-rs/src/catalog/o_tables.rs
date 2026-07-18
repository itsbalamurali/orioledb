use crate::access::hash;
use crate::access::heapam;
use crate::access::transam;
use crate::btree::btree;
use crate::btree::io;
use crate::btree::undo;
use crate::catalog::heap;
use crate::catalog::namespace;
use crate::catalog::o_indices;
use crate::catalog::o_sys_cache;
use crate::catalog::o_tables;
use crate::catalog::pg_am;
use crate::catalog::pg_amop;
use crate::catalog::pg_collation;
use crate::catalog::pg_language;
use crate::catalog::pg_proc;
use crate::catalog::pg_range;
use crate::catalog::pg_tablespace_d;
use crate::catalog::pg_type;
use crate::checkpoint::checkpoint;
use crate::commands::defrem;
use crate::executor::execExpr;
use crate::executor::functions;
use crate::funcapi;
use crate::nodes::nodeFuncs;
use crate::optimizer::optimizer;
use crate::orioledb;
use crate::parser::parse_relation;
use crate::pgstat;
use crate::recovery::recovery;
use crate::recovery::wal;
use crate::tableam::operations;
use crate::transam::oxid;
use crate::tuple::toast;
use crate::utils::array;
use crate::utils::builtins;
use crate::utils::datum;
use crate::utils::elog;
use crate::utils::fmgrtab;
use crate::utils::inval;
use crate::utils::lsyscache;
use crate::utils::memutils;
use crate::utils::planner;
use crate::utils::rel;
use crate::utils::ruleutils;
use crate::utils::syscache;
use pgrx::pg_sys;

// -------------------------------------------------------------------------
//
// o_tables.c
// Routines for orioledb tables system tree.
//
// Copyright (c) 2021-2026, Oriole DB Inc.
// Copyright (c) 2025-2026, Supabase Inc.
//
// IDENTIFICATION
// contrib/orioledb/src/catalog/o_tables.c
//
// -------------------------------------------------------------------------
//

//
// Relation locks from recovery workers may conflict with PostgreSQL WAL locks
// that leads to deadlocks. We need to have own relation locks for
// checkpoint process to avoid this.
//
#define CHECKPOINT_LOCK_BIT ((uint32) 1 << (32 - 1))

PG_FUNCTION_INFO_V1(orioledb_table_description);
PG_FUNCTION_INFO_V1(orioledb_table_oids);

typedef struct
{
	pub static mut CALLBACK: OTablesCallback = std::mem::zeroed();
		   *arg;
} OTablesForeachArg;

typedef struct
{
	pub static mut OXID: OXid = std::mem::zeroed();
	pub static mut CSN: CommitSeqNo = std::mem::zeroed();
	pub static mut DATOID: Oid = std::mem::zeroed();
} OTablesDropAllArg;

typedef struct
{
	pub static mut OXID: OXid = std::mem::zeroed();
	pub static mut CSN: CommitSeqNo = std::mem::zeroed();
	pub static mut DATOID: Oid = std::mem::zeroed();
	pub static mut OLD_TABLESPACE: Oid = std::mem::zeroed();
	pub static mut NEW_TABLESPACE: Oid = std::mem::zeroed();
} OTablesMoveAllArg;

typedef struct
{
	pub static mut DATOID: Oid = std::mem::zeroed();
	pub static mut LIST: *mut evicted = std::ptr::null_mut();
} OTablesEvictDBArg;

typedef struct
{
	pub static mut OXID: OXid = std::mem::zeroed();
	pub static mut CSN: CommitSeqNo = std::mem::zeroed();
	pub static mut TYPE_OID: Oid = std::mem::zeroed();
	pub static mut TYPE_DATA: Form_pg_type = std::mem::zeroed();
} OTablesDropAllWithTypeArg;

typedef struct
{
	pub static mut TYPE: OIndexType = std::mem::zeroed();
	pub static mut OIDS: ORelOids = std::mem::zeroed();
	pub static mut IX_NUM: OIndexNumber = std::mem::zeroed();
} OTableIndexOidsKey;

typedef struct OTablesNumArg
{
	pub static mut DATOID: Oid = std::mem::zeroed();
	pub static mut RESULT: std::os::raw::c_int = 0;
} OTablesNumArg;

fn o_table_tupdesc_init_entry(TupleDesc desc, AttrNumber att_num, name: &mut char, field: &mut OTableField);
fn o_tables_foreach_callback(ORelOids oids,  *arg);
fn o_tables_drop_all_callback(ORelOids oids,  *arg);
fn o_tables_truncate_unlogged_callback(o_table: &mut OTable,  *arg);
fn o_table_oids_array_callback(ORelOids oids,  *arg);
fn o_tables_num_callback(ORelOids oids,  *arg);
static inline  o_tables_rel_fill_locktag(tag: &mut LOCKTAG, oids: &mut ORelOids, int lockmode, bool checkpoint);

static BTreeDescr *
oTablesGetBTreeDesc( *arg)
{
	desc: &mut BTreeDescr = (BTreeDescr *) arg;

	pub static mut DESC: return = std::mem::zeroed();
}

static uint32
oTablesGetKeySize( *arg)
{
	return sizeof(OTableChunkKey);
}

static uint32
oTablesGetMaxChunkSize( *key,  *arg)
{
	pub static mut MAX_CHUNK_SIZE: uint32 = std::mem::zeroed();

	max_chunk_size = MAXALIGN_DOWN((O_BTREE_MAX_TUPLE_SIZE * 3 - MAXALIGN(sizeof(OTableChunkKey))) / 3) - offsetof(OTableChunk, data);

	pub static mut MAX_CHUNK_SIZE: return = std::mem::zeroed();
}

fn
oTablesUpdateKey( *key, uint32 chunknum,  *arg)
{
	ckey: &mut OTableChunkKey = (OTableChunkKey *) key;

	ckey->chunknum = chunknum;
}

fn *
oTablesGetNextKey( *key,  *arg)
{
	ckey: &mut OTableChunkKey = (OTableChunkKey *) key;
	static mut NEXT_KEY: OTableChunkKey = std::mem::zeroed();

	nextKey = *ckey;
	nextKey.oids.relnode++;
	nextKey.chunknum = 0;

	return (Pointer) &nextKey;
}

static OTuple
oTablesCreateTuple( *key, Pointer data, uint32 offset, uint32 chunknum,
				   int length,  *arg)
{
	ckey: &mut OTableChunkKey = (OTableChunkKey *) key;
	pub static mut O_TABLE_CHUNK: *mut chunk = std::ptr::null_mut();
	pub static mut RESULT: OTuple = std::mem::zeroed();

	ckey->chunknum = chunknum;

	chunk = (OTableChunk *) palloc(offsetof(OTableChunk, data) + length);
	chunk->key = *ckey;
	chunk->dataLength = length;
	memcpy(chunk->data, data + offset, length);

	result.data = (Pointer) chunk;
	result.formatFlags = 0;

	pub static mut RESULT: return = std::mem::zeroed();
}

static OTuple
oTablesCreateKey( *key, uint32 chunknum,  *arg)
{
	ckey: &mut OTableChunkKey = (OTableChunkKey *) key;
	pub static mut O_TABLE_CHUNK_KEY: *mut ckey_copy = std::ptr::null_mut();
	pub static mut RESULT: OTuple = std::mem::zeroed();

	ckey_copy = (OTableChunkKey *) palloc(sizeof(OTableChunkKey));
	*ckey_copy = *ckey;

	result.data = (Pointer) ckey_copy;
	result.formatFlags = 0;

	pub static mut RESULT: return = std::mem::zeroed();
}

static Pointer
oTablesGetTupleData(OTuple tuple,  *arg)
{
	chunk: &mut OTableChunk = (OTableChunk *) tuple.data;

	return chunk->data;
}

static uint32
oTablesGetTupleChunknum(OTuple tuple,  *arg)
{
	chunk: &mut OTableChunk = (OTableChunk *) tuple.data;

	return chunk->key.chunknum;
}

static uint32
oTablesGetTupleDataSize(OTuple tuple,  *arg)
{
	chunk: &mut OTableChunk = (OTableChunk *) tuple.data;

	return chunk->dataLength;
}

static TupleFetchCallbackResult
oTablesFetchCallback(OTuple tuple, OXid tupOxid, oSnapshot: &mut OSnapshot,
					  *arg, bool oxidIsFinished)
{
	tupleKey: &mut OTableChunkKey = (OTableChunkKey *) tuple.data;
	boundKey: &mut OTableChunkBoundKey = (OTableChunkBoundKey *) arg;

	if (ORelOidsIsEqual(tupleKey->oids, boundKey->key.oids))
	{
		if (COMMITSEQNO_IS_INPROGRESS(oSnapshot->csn) &&
			(boundKey->key.version == O_TABLE_INVALID_VERSION ||
			 OXidIsValid(boundKey->oxid)))
		{
			if (!OXidIsValid(boundKey->oxid))
				boundKey->oxid = tupOxid;
			else if (boundKey->oxid != tupOxid)
				pub static mut O_TUPLE_FETCH_NOT_MATCH: return = std::mem::zeroed();

			if (boundKey->key.version == O_TABLE_INVALID_VERSION)
				boundKey->key.version = tupleKey->version;
			else if (boundKey->key.version != tupleKey->version)
				pub static mut O_TUPLE_FETCH_NOT_MATCH: return = std::mem::zeroed();

			if (boundKey->key.chunknum == tupleKey->chunknum)
			{
				boundKey->key.chunknum++;
				pub static mut O_TUPLE_FETCH_MATCH: return = std::mem::zeroed();
			}
			else
				pub static mut O_TUPLE_FETCH_NOT_MATCH: return = std::mem::zeroed();
		}

		if (boundKey->key.version == O_TABLE_INVALID_VERSION)
			boundKey->key.version = tupleKey->version;

		if (tupleKey->version > boundKey->key.version)
			pub static mut O_TUPLE_FETCH_NEXT: return = std::mem::zeroed();
		else if (tupleKey->version == boundKey->key.version)
			pub static mut O_TUPLE_FETCH_MATCH: return = std::mem::zeroed();
		else
			pub static mut O_TUPLE_FETCH_NOT_MATCH: return = std::mem::zeroed();
	}

	//
// Return current tuple with unmatched key to iterator immediately to
// finish the scan.
//
	pub static mut O_TUPLE_FETCH_MATCH: return = std::mem::zeroed();
}

static ToastAPI oTablesToastAPI = {
	.getBTreeDesc = oTablesGetBTreeDesc,
	.getBTreeVersion = NULL,
	.getBaseBTreeVersion = NULL,
	.getKeySize = oTablesGetKeySize,
	.getMaxChunkSize = oTablesGetMaxChunkSize,
	.updateKey = oTablesUpdateKey,
	.getNextKey = oTablesGetNextKey,
	.createTuple = oTablesCreateTuple,
	.createKey = oTablesCreateKey,
	.getTupleData = oTablesGetTupleData,
	.getTupleChunknum = oTablesGetTupleChunknum,
	.getTupleDataSize = oTablesGetTupleDataSize,
	.deleteLogFullTuple = false,
	.fetchCallback = oTablesFetchCallback
};

fn
o_tables_foreach_oids(OTablesOidsCallback callback,
					  oSnapshot: &mut OSnapshot,
					   *arg)
{
	pub static mut CHUNK_KEY: OTableChunkKey = std::mem::zeroed();
	ORelOids	oids = {0, 0, 0},
				pub static mut PG_USED_FOR_ASSERTS_ONLY: old_oids = std::mem::zeroed();
	pub static mut B_TREE_ITERATOR: *mut it = std::ptr::null_mut();
	pub static mut TUPLE: OTuple = std::mem::zeroed();
	desc: &mut BTreeDescr = get_sys_tree(SYS_TREES_O_TABLES);

	chunk_key.oids = oids;
	chunk_key.chunknum = 0;

	it = o_btree_iterator_create(desc, (Pointer) &chunk_key, BTreeKeyBound,
								 oSnapshot, ForwardScanDirection);

	tuple = o_btree_iterator_fetch(it, NULL, NULL,
								   BTreeKeyNone, false, NULL);
	old_oids = oids;
	while (!O_TUPLE_IS_NULL(tuple))
	{
		chunk: &mut OTableChunk = (OTableChunk *) tuple.data;

		oids = chunk->key.oids;
		Assert(ORelOidsIsValid(oids));
		Assert(!ORelOidsIsEqual(old_oids, oids));
		old_oids = oids;

		callback(oids, arg);

		pfree(tuple.data);
		btree_iterator_free(it);

		oids.relnode += 1;		// go to the next oid
		chunk_key.oids = oids;
		chunk_key.chunknum = 0;

		it = o_btree_iterator_create(desc, (Pointer) &chunk_key, BTreeKeyBound,
									 oSnapshot, ForwardScanDirection);
		tuple = o_btree_iterator_fetch(it, NULL, NULL,
									   BTreeKeyNone, false, NULL);
	}
	btree_iterator_free(it);
}

//
// It can be much more efficient.
//
fn
o_tables_foreach(OTablesCallback callback,
				 oSnapshot: &mut OSnapshot,
				  *arg)
{
	pub static mut FOREACH_ARG: OTablesForeachArg = std::mem::zeroed();

	foreach_arg.callback = callback;
	foreach_arg.arg = arg;

	o_tables_foreach_oids(o_tables_foreach_callback, oSnapshot, &foreach_arg);
}

static char *
o_deparse_expression(expr_str: &mut char, Oid relid)
{
	pub static mut EXPR: Datum = std::mem::zeroed();
	expr_text: &mut text = cstring_to_text(expr_str);

	expr = DirectFunctionCall2(pg_get_expr, (Datum) expr_text,
							   ObjectIdGetDatum(relid));
	return TextDatumGetCString(expr);
}


o_table_fill_index(o_table: &mut OTable, OIndexNumber ix_num, Relation index_rel)
{
	pub static mut O_TABLE_INDEX: *mut index = &o_table->indices[ix_num];
	pub static mut LIST_CELL: *mut index_expr_elem = std::ptr::null_mut();
	pub static mut IX_EXPRFIELD_NUM: std::os::raw::c_int = 0;
	pub static mut LIST_CELL: *mut lc = std::ptr::null_mut();
	MemoryContext mcxt,
				old_mcxt;
	pub static mut KEYNO: std::os::raw::c_int = 0;
	pub static mut DATUM: Datum = std::mem::zeroed();
	pub static mut OIDVECTOR: *mut indclass = std::ptr::null_mut();
	pub static mut ISNULL: bool = false;
	pub static mut I: std::os::raw::c_int = 0;

	if (index->index_mctx)
	{
		MemoryContextDelete(index->index_mctx);
		index->index_mctx = NULL;
	}
	mcxt = OGetIndexContext(index);
	old_mcxt = MemoryContextSwitchTo(mcxt);
	RelationGetIndexExpressions(index_rel);
	RelationGetIndexPredicate(index_rel);
	index_expr_elem = list_head(index_rel->rd_indexprs);
	if (index_rel->rd_indexprs != NIL)
	{
		index->nexprfields = list_length(index_rel->rd_indexprs);
		index->exprfields = palloc0(index->nexprfields * sizeof(OTableField));
	}
	index->immediate = index_rel->rd_index->indimmediate;
	index->predicate = (List *)
		expression_planner((Expr *) index_rel->rd_indpred);
	if (index->predicate)
	{
		index->predicate_str =
			o_deparse_expression(nodeToString(index->predicate),
								 o_table->oids.reloid);
	}
	o_collect_funcexpr((Node *) index->predicate);
	index->expressions = NIL;
	foreach(lc, index_rel->rd_indexprs)
	{
		e: &mut Expr = (Expr *) lfirst(lc);
		pub static mut EXPR: *mut node = std::ptr::null_mut();

		node = expression_planner(e);
		index->expressions = lappend(index->expressions, node);
	}
	o_collect_funcexpr((Node *) index->expressions);
	if (index->type == oIndexExclusion)
	{
		op_operators: &mut Oid,
				   *op_procs;
		pub static mut UINT16: *mut op_strats = std::ptr::null_mut();

		Assert(index_rel->rd_index->indisexclusion);
		RelationGetExclusionInfo(index_rel, &op_operators, &op_procs, &op_strats);

		index->exclops = palloc0(index->nkeyfields * sizeof(Oid));
		for (i = 0; i < index->nkeyfields; i++)
		{
			index->exclops[i] = index_rel->rd_exclops[i];
			o_collect_op_by_oid(index->exclops[i]);
		}
	}
	MemoryContextSwitchTo(old_mcxt);

	// Must get indclass the hard way
	datum = SysCacheGetAttr(INDEXRELID, index_rel->rd_indextuple,
							Anum_pg_index_indclass, &isnull);
	Assert(!isnull);
	indclass = (oidvector *) DatumGetPointer(datum);

	ix_exprfield_num = 0;
	for (keyno = 0; keyno < index->nfields; keyno++)
	{
		pub static mut ATTNUM: AttrNumber = index_rel->rd_index->indkey.values[keyno];
		pub static mut O_TABLE_INDEX_FIELD: *mut ix_field = std::ptr::null_mut();
		pub static mut O_TABLE_FIELD: *mut exprField = std::ptr::null_mut();

		ix_field = &index->fields[keyno];
		if (AttributeNumberIsValid(attnum))
		{
			// Field validation performed in o_validate_index_elements
			ix_field->attnum = attnum - 1;
		}
		else
		{
			// Expressional index
			pub static mut NODE: *mut indexkey = std::ptr::null_mut();
			pub static mut TUPLE: HeapTuple = std::mem::zeroed();
			pub static mut TYPE_TUP: Form_pg_type = std::mem::zeroed();
			pub static mut FIELD_TYPEID: Oid = std::mem::zeroed();

			Assert(index_rel->rd_indexprs);
			indexkey = lfirst(index_expr_elem);
			index_expr_elem = lnext(index_rel->rd_indexprs, index_expr_elem);
			exprField = &index->exprfields[ix_exprfield_num++];

			//
// Lookup the expression type in pg_type for the type length etc.
//
			field_typeid = exprType(indexkey);
			tuple = SearchSysCache1(TYPEOID, ObjectIdGetDatum(field_typeid));
			if (!HeapTupleIsValid(tuple))
				elog(ERROR, "cache lookup failed for type %u", field_typeid);
			typeTup = (Form_pg_type) GETSTRUCT(tuple);

			//
// Assign some of the attributes values. Leave the rest.
//
			namestrcpy(&(exprField->name),
					   o_deparse_expression(nodeToString(indexkey),
											o_table->oids.reloid));
			exprField->typid = field_typeid;
			exprField->typlen = typeTup->typlen;
			exprField->byval = typeTup->typbyval;
			exprField->storage = typeTup->typstorage;
			exprField->align = typeTup->typalign;
			exprField->typmod = exprTypmod(indexkey);
			exprField->collation = exprCollation(indexkey);

			orioledb_save_collation(exprField->collation);

			ReleaseSysCache(tuple);

			//
// Make sure the expression yields a type that's safe to store in
// an index.  We need this defense because we have index opclasses
// for pseudo-types such as "record", and the actually stored type
// had better be safe; eg, a named composite type is okay, an
// anonymous record type is not.  The test is the same as for
// whether a table column is of a safe type (which is why we
// needn't check for the non-expression case).
//
			CheckAttributeType("EXPR_FIELD",
							   exprField->typid, exprField->collation,
							   NIL, 0);

			ix_field->attnum = EXPR_ATTNUM;
		}

		if (keyno >= index->nkeyfields)
		{
			pub static mut O_TABLE_INDEX: *mut primary = std::ptr::null_mut();
			pub static mut PK_MEMBER: bool = false;
			pub static mut O_TABLE_INDEX_FIELD: *mut primary_field = std::ptr::null_mut();
			pub static mut PK_FIELD: std::os::raw::c_int = 0;

			if (o_table->has_primary)
			{
				primary = &o_table->indices[PrimaryIndexNumber];

				for (pk_field = 0; pk_field < primary->nfields; pk_field++)
				{
					primary_field = &primary->fields[pk_field];

					if (primary_field->attnum == ix_field->attnum)
					{
						pk_member = true;
						break;
					}
				}
			}

			if (pk_member)
			{
				ix_field->collation = primary_field->collation;
				ix_field->opclass = primary_field->opclass;
				ix_field->ordering = primary_field->ordering;
				ix_field->nullsOrdering = primary_field->nullsOrdering;
				ix_field->hash_fn_oid = primary_field->hash_fn_oid;
			}
			else
			{
				//
// Included columns have no collation, no opclass and no
// ordering options.
//
				ix_field->collation = InvalidOid;
				ix_field->opclass = InvalidOid;
				ix_field->ordering = SORTBY_DEFAULT;
				ix_field->nullsOrdering = SORTBY_NULLS_DEFAULT;
				ix_field->hash_fn_oid = InvalidOid;
			}
		}
		else
		{
			pub static mut OPT: int16 = index_rel->rd_indoption[keyno];
			pub static mut TYPID: Oid = std::mem::zeroed();
			pub static mut HASHABLE: bool = true;
			pub static mut LIST: *mut processed = NIL;

			if (AttributeNumberIsValid(attnum))
			{
				typid = o_table->fields[ix_field->attnum].typid;
			}
			else
			{
				Assert(exprField != NULL);
				Assert(OidIsValid(exprField->typid));
				typid = exprField->typid;
			}
			ix_field->collation = index_rel->rd_indcollation[keyno];
			ix_field->opclass = indclass->values[keyno];

			o_validate_composite_type(typid, ix_field->opclass);

			ix_field->ordering = SORTBY_DEFAULT;
			ix_field->nullsOrdering = SORTBY_NULLS_DEFAULT;
			if (opt & INDOPTION_DESC)
			{
				ix_field->ordering = SORTBY_DESC;
				if ((opt & INDOPTION_NULLS_FIRST) == 0)
					ix_field->nullsOrdering = SORTBY_NULLS_LAST;
			}
			else if (opt & INDOPTION_NULLS_FIRST)
			{
				ix_field->nullsOrdering = SORTBY_NULLS_FIRST;
			}

			hashable = custom_type_try_add_hash_fn_if_needed(typid,
															 ix_field->opclass,
															 &processed);
			list_free_deep(processed);

			if (hashable)
			{
				ix_field->hash_fn_oid = o_get_hash_proc_by_btree_opclass(ix_field->opclass);
			}
			else
			{
				ix_field->hash_fn_oid = O_DEFAULT_HASH_FN_OID;
				ereport(WARNING,
						(errmsg("failed to fetch the hash function for btree opclass %s", generate_opclass_name(ix_field->opclass)),
						 errdetail("Recovery might be slow due to inability to distribute values among the workers.")));
			}
		}
		orioledb_save_collation(ix_field->collation);
	}
}


o_table_resize_constr(o_table: &mut OTable)
{
	pub static mut OLDCXT: MemoryContext = std::mem::zeroed();
	pub static mut TBL_CXT: MemoryContext = std::mem::zeroed();

	tbl_cxt = OGetTableContext(o_table);
	oldcxt = MemoryContextSwitchTo(tbl_cxt);

	if (o_table->nfields > 0)
	{
		if (!o_table->missing)
			o_table->missing = palloc0(o_table->nfields * sizeof(AttrMissing));
		else
			o_table->missing = repalloc(o_table->missing,
										o_table->nfields *
										sizeof(AttrMissing));
		o_table->missing[o_table->nfields - 1].am_present = false;
		o_table->missing[o_table->nfields - 1].am_value = 0;
	}

	MemoryContextSwitchTo(oldcxt);
}

Datum
o_eval_default(o_table: &mut OTable, Relation rel, expr: &mut Node, scantuple: &mut TupleTableSlot,
			   bool byval, int16 typlen, isNull: &mut bool)
{
	pub static mut OLDCXT: MemoryContext = std::mem::zeroed();
	MemoryContext tbl_cxt = OGetTableContext(o_table);
	pub static mut NEW_VAL: Datum = std::mem::zeroed();
	pub static mut EXPR: *mut expr2 = std::ptr::null_mut();
	pub static mut PARSE_NAMESPACE_ITEM: *mut nsitem = std::ptr::null_mut();
	pub static mut PARSE_STATE: *mut pstate = std::ptr::null_mut();
	pub static mut E_STATE: *mut estate = std::ptr::null_mut();
	pub static mut EXPR_CONTEXT: *mut econtext = std::ptr::null_mut();
	pub static mut EXPR_STATE: *mut exprState = std::ptr::null_mut();
	pub static mut RESULT: Datum = 0;

	if (!expr)
	{
		*isNull = true;
		pub static mut RESULT: return = std::mem::zeroed();
	}

	pstate = make_parsestate(NULL);
	pstate->p_sourcetext = NULL;
	nsitem = addRangeTableEntryForRelation(pstate, rel, AccessShareLock,
										   NULL, false, true);
	addNSItemToQuery(pstate, nsitem, true, true, true);

	expr2 = expression_planner((Expr *) expr);

	oldcxt = MemoryContextSwitchTo(tbl_cxt);
	estate = CreateExecutorState();
	exprState = ExecPrepareExpr(expr2, estate);
	econtext = GetPerTupleExprContext(estate);

	if (scantuple)
		econtext->ecxt_scantuple = scantuple;
	new_val = ExecEvalExpr(exprState, econtext, isNull);

	FreeExecutorState(estate);
	free_parsestate(pstate);

	if (!*isNull)
		result = datumCopy(new_val, byval, typlen);
	MemoryContextSwitchTo(oldcxt);
	pub static mut RESULT: return = std::mem::zeroed();
}


o_table_fill_constr(o_table: &mut OTable, Relation rel, int fieldnum,
					old_field: &mut OTableField, field: &mut OTableField)
{
	pub static mut OLDCXT: MemoryContext = std::mem::zeroed();
	MemoryContext tbl_cxt = OGetTableContext(o_table);
	pub static mut ATTRMISS_TEMP: AttrMissing = std::mem::zeroed();
	pub static mut NODE: *mut defaultexpr = std::ptr::null_mut();
	pub static mut ATTR_MISSING: *mut attrmiss = std::ptr::null_mut();
	pub static mut MISSING_IS_NULL: bool = true;
	pub static mut HAS_DOMAIN_CONSTRAINTS: bool = false;

	if (field->hasdef || get_typtype(field->typid) == TYPTYPE_DOMAIN)
		defaultexpr = build_column_default(rel, fieldnum + 1);
	else
		defaultexpr = NULL;

	has_domain_constraints = DomainHasConstraints(field->typid);
	if (o_in_add_column &&
		!field->generated &&
		!has_domain_constraints &&
		!contain_volatile_functions((Node *) defaultexpr))
	{
		attrmiss_temp.am_value = o_eval_default(o_table, rel, defaultexpr, NULL,
												field->byval, field->typlen,
												&missingIsNull);
		attrmiss_temp.am_present = true;

		if (!old_field || (!old_field->hasmissing && !missingIsNull))
		{
			attrmiss = &attrmiss_temp;
			field->hasmissing = true;
		}
	}
	o_in_add_column = false;

	oldcxt = MemoryContextSwitchTo(tbl_cxt);

	if (attrmiss)
	{
		o_table->missing[fieldnum].am_present = field->hasmissing &&
			attrmiss->am_present;
		if (o_table->missing[fieldnum].am_present)
			o_table->missing[fieldnum].am_value = datumCopy(attrmiss->am_value,
															field->byval,
															field->typlen);
		else
			o_table->missing[fieldnum].am_value = 0;
	}
	MemoryContextSwitchTo(oldcxt);
}


orioledb_attr_to_field(field: &mut OTableField, Form_pg_attribute attr)
{
	strlcpy(NameStr(field->name), NameStr(attr->attname), NAMEDATALEN);
	field->typid = attr->atttypid;
	field->collation = attr->attcollation;
	field->typmod = attr->atttypmod;
	field->typlen = attr->attlen;
	field->ndims = attr->attndims;
	field->byval = attr->attbyval;
	field->align = attr->attalign;
	field->storage = attr->attstorage;
	field->compression = attr->attcompression;
	field->droped = attr->attisdropped;
	field->notnull = attr->attnotnull;
	field->hasmissing = attr->atthasmissing;
	field->hasdef = attr->atthasdef;
	field->generated = attr->attgenerated;
}

OTable *
o_table_tableam_create(ORelOids oids, TupleDesc tupdesc, char relpersistence,
					   uint8 fillfactor, Oid tablespace, bool bridging)
{
	pub static mut O_TABLE: *mut o_table = std::ptr::null_mut();
	pub static mut I: std::os::raw::c_int = 0;
	pub static mut HASH_OPCLASS: Oid = std::mem::zeroed();
	pub static mut HASH_OPFAMILY: Oid = std::mem::zeroed();
	pub static mut CHAR: *mut prefix = std::ptr::null_mut();
	pub static mut CHAR: *mut db_prefix = std::ptr::null_mut();

	if (tablespace == 0)
		tablespace = MyDatabaseTableSpace;
	Assert(tablespace);

	o_get_prefixes_for_tablespace(oids.datoid, tablespace, &prefix, &db_prefix);
	o_verify_dir_exists_or_create(prefix, NULL, NULL);
	o_verify_dir_exists_or_create(db_prefix, NULL, NULL);
	pfree(db_prefix);

	o_table = palloc0(sizeof(OTable));
	o_table->nfields = tupdesc->natts;
	o_table->primary_init_nfields = o_table->nfields + 1;	// + ctid field
	o_table->fields = palloc0(o_table->nfields * sizeof(OTableField));
	o_table->oids = oids;
	o_table->tablespace = tablespace;
	Assert(o_table->tablespace);
	o_table->tid_btree_ops_oid = GetDefaultOpClass(TIDOID, BTREE_AM_OID);

	hash_opclass = GetDefaultOpClass(TIDOID, HASH_AM_OID);
	hash_opfamily = get_opclass_family(hash_opclass);
	o_table->tid_hash_fn_oid = get_opfamily_proc(hash_opfamily, TIDOID, TIDOID, HASHSTANDARD_PROC);

	hash_opclass = GetDefaultOpClass(INT2OID, HASH_AM_OID);
	hash_opfamily = get_opclass_family(hash_opclass);
	o_table->int2_hash_fn_oid = get_opfamily_proc(hash_opfamily, INT2OID, INT2OID, HASHSTANDARD_PROC);

	hash_opclass = GetDefaultOpClass(INT4OID, HASH_AM_OID);
	hash_opfamily = get_opclass_family(hash_opclass);
	o_table->int4_hash_fn_oid = get_opfamily_proc(hash_opfamily, INT4OID, INT4OID, HASHSTANDARD_PROC);

	o_table->default_compress = InvalidOCompress;
	o_table->primary_compress = InvalidOCompress;
	o_table->toast_compress = InvalidOCompress;
	o_table->fillfactor = fillfactor;
	o_table->persistence = relpersistence;
	o_table->data_version = ORIOLEDB_SYS_TREE_VERSION;
	// No index incarnations yet for a freshly created table.
	o_table->toast_ixversion = O_TABLE_INVALID_VERSION; // uninitialized
	o_table->primary_ixversion = O_TABLE_INVALID_VERSION;	// uninitialized
	o_table->bridge_ixversion = O_TABLE_INVALID_VERSION;	// uninitialized
	o_table->index_bridging = bridging;

	for (i = 0; i < tupdesc->natts; i++)
	{
		pub static mut O_TABLE_FIELD: *mut field = &o_table->fields[i];

		orioledb_attr_to_field(field, TupleDescAttr(tupdesc, i));
		orioledb_save_collation(field->collation);
	}
	o_table->nindices = 0;
	o_table_resize_constr(o_table);

	pub static mut O_TABLE: return = std::mem::zeroed();
}

static OTableField builtin_fields[] =
{
	{{{0}}, INT2OID, InvalidOid, -1, 0, true, false, true, 2, 's', 'p'},
	{{{0}}, INT4OID, InvalidOid, -1, 0, true, false, true, 4, 'i', 'p'},
	{{{0}}, OIDOID, InvalidOid, -1, 0, true, false, true, 4, 'i', 'p'},
	{{{0}}, TIDOID, InvalidOid, -1, 0, false, false, true, 6, 's', 'p'},
	{{{0}}, BYTEAOID, InvalidOid, -1, 0, false, false, true, -1, 'i', 'x'}
};

OTableField *
o_tables_get_builtin_field(Oid type)
{
	pub static mut I: std::os::raw::c_int = 0;

	for (i = 0; i < sizeof(builtin_fields) / sizeof(builtin_fields[0]); i++)
	{
		if (type == builtin_fields[i].typid)
		{
			return &builtin_fields[i];
		}
	}
	Assert(false);				// shouldn't get there
	pub static mut NULL: return = std::mem::zeroed();
}

//
// We hold data of some types itself because they used inside o_tables.
//

o_tables_tupdesc_init_builtin(TupleDesc desc, AttrNumber att_num, name: &mut char, Oid type)
{
	o_table_tupdesc_init_entry(desc, att_num, name, o_tables_get_builtin_field(type));
}

//
// Returns tuple descriptor made from array
//
TupleDesc
o_table_fields_make_tupdesc(fields: &mut OTableField, int nfields)
{
	pub static mut O_TABLE_FIELD: *mut field = std::ptr::null_mut();
	TupleDesc	tupdesc = CreateTemplateTupleDesc(nfields);
	pub static mut I: std::os::raw::c_int = 0;

	for (i = 0; i < nfields; i++)
	{
		field = &fields[i];
		o_table_tupdesc_init_entry(tupdesc, i + 1, NameStr(field->name), field);
	}
	pub static mut TUPDESC: return = std::mem::zeroed();
}


o_tupdesc_load_constr(TupleDesc tupdesc, o_table: &mut OTable, descr: &mut OIndexDescr)
{
	pub static mut OLDCXT: MemoryContext = std::mem::zeroed();
	pub static mut IDX_CXT: MemoryContext = std::mem::zeroed();
	pub static mut I: std::os::raw::c_int = 0;
	pub static mut FIELDS_START: std::os::raw::c_int = 0;
	pub static mut ALL_ATTRS: std::os::raw::c_int = o_table->nfields;

	idx_cxt = OGetIndexContext(descr);
	oldcxt = MemoryContextSwitchTo(idx_cxt);
	fields_start = o_table->has_primary ? 0 : 1;

	if (o_table->index_bridging)
		fields_start++;

	all_attrs += fields_start;

	tupdesc->constr = (TupleConstr *) palloc0(sizeof(TupleConstr));
	tupdesc->constr->missing = (AttrMissing *) palloc0(all_attrs * sizeof(AttrMissing));

	if (!o_table->has_primary)
		tupdesc->constr->missing[0].am_present = false;

	for (i = 0; i < o_table->nfields; i++)
	{
		pub static mut O_TABLE_FIELD: *mut field = &o_table->fields[i];
		pub static mut ATTR_MISSING: *mut tupdesc_miss = &tupdesc->constr->missing[i + fields_start];

		tupdesc_miss->am_present = o_table->missing[i].am_present;

		if (o_table->missing[i].am_present)
		{
			tupdesc_miss->am_value =
				datumCopy(o_table->missing[i].am_value, field->byval,
						  field->typlen);
		}
	}

	if (o_table->index_bridging)
	{
		pub static mut ATTR_MISSING: *mut tupdesc_miss = &tupdesc->constr->missing[fields_start - 1];

		tupdesc_miss->am_present = false;
		tupdesc_miss->am_value = 0;
	}
	MemoryContextSwitchTo(oldcxt);
}

TupleDesc
o_table_tupdesc(o_table: &mut OTable)
{
	pub static mut TUPDESC: TupleDesc = std::mem::zeroed();

	tupdesc = o_table_fields_make_tupdesc(o_table->fields, o_table->nfields);
	if (IsTransactionState())
		tupdesc->tdtypeid = get_rel_type_id(o_table->oids.reloid);
	else if (OidIsValid(o_table->oids.reloid))

		//
// During recovery there is no active transaction, so we can't call
// get_rel_type_id().  Set tdtypeid to the reloid directly as a proxy
// to ensure it is not left as RECORDOID.  Any non-RECORDOID value is
// sufficient because the sole consumer of tdtypeid in the orioledb
// slot code (tts_orioledb_getsomeattrs) only checks whether it equals
// RECORDOID to decide if the tuple is stored in index column order.
// Table leaf tuples are always stored in table column order, so
// index_order must be false; leaving tdtypeid as RECORDOID would
// incorrectly flip index_order to true for tables where all columns
// happen to form the primary key, causing attribute-position
// scrambling and B-tree corruption on the replica.
//
		tupdesc->tdtypeid = o_table->oids.reloid;
	pub static mut TUPDESC: return = std::mem::zeroed();
}

static int
index_keys_cmp(p1: &mut const, p2: &mut const)
{
	const key1: &mut OTableIndexOidsKey = (const OTableIndexOidsKey *) p1;
	const key2: &mut OTableIndexOidsKey = (const OTableIndexOidsKey *) p2;

	if (key1->type < key2->type)
		return -1;
	else if (key1->type > key2->type)
		pub static mut 1: return = std::mem::zeroed();

	if (key1->oids.datoid < key2->oids.datoid)
		return -1;
	else if (key1->oids.datoid > key2->oids.datoid)
		pub static mut 1: return = std::mem::zeroed();

	if (key1->oids.reloid < key2->oids.reloid)
		return -1;
	else if (key1->oids.reloid > key2->oids.reloid)
		pub static mut 1: return = std::mem::zeroed();

	if (key1->oids.relnode < key2->oids.relnode)
		return -1;
	else if (key1->oids.relnode > key2->oids.relnode)
		pub static mut 1: return = std::mem::zeroed();

	pub static mut 0: return = std::mem::zeroed();
}

static OTableIndexOidsKey *
o_table_make_index_oids_keys(table: &mut OTable, num: &mut int)
{
	pub static mut O_TABLE_INDEX_OIDS_KEY: *mut keys = std::ptr::null_mut();
	pub static mut KEYS_NUM: std::os::raw::c_int = 0;
	pub static mut I: std::os::raw::c_int = 0;

	if (!table)
	{
		*num = 0;
		pub static mut NULL: return = std::mem::zeroed();
	}

	keys = (OTableIndexOidsKey *) palloc(sizeof(OTableIndexOidsKey) *
										 (table->nindices + 3));

	// ctid primary index if needed
	if (table->nindices == 0 ||
		table->indices[PrimaryIndexNumber].type != oIndexPrimary)
	{
		keys[keys_num].type = oIndexPrimary;
		keys[keys_num].ixNum = keys_num;
		keys[keys_num++].oids = table->oids;
	}

	for (i = 0; i < table->nindices; i++)
	{
		keys[keys_num].type = table->indices[i].type;
		keys[keys_num].ixNum = keys_num;
		keys[keys_num++].oids = table->indices[i].oids;
	}

	if (ORelOidsIsValid(table->bridge_oids))
	{
		keys[keys_num].type = oIndexBridge;
		keys[keys_num].ixNum = BridgeIndexNumber;
		keys[keys_num++].oids = table->bridge_oids;
	}

	if (ORelOidsIsValid(table->toast_oids))
	{
		keys[keys_num].type = oIndexToast;
		keys[keys_num].ixNum = TOASTIndexNumber;
		keys[keys_num++].oids = table->toast_oids;
	}

	qsort(keys, keys_num, sizeof(OTableIndexOidsKey), index_keys_cmp);

	*num = keys_num;
	pub static mut KEYS: return = std::mem::zeroed();
}

//
// Returns array of OIndexKey for each table index (including TOAST).
//
// Array is allocated in CurTransactionContext.
//
OIndexKey *
o_table_make_index_keys(table: &mut OTable, num: &mut int)
{
	pub static mut O_INDEX_KEY: *mut trees = std::ptr::null_mut();
	pub static mut TREES_NUM: std::os::raw::c_int = 0;
	pub static mut I: std::os::raw::c_int = 0;

	Assert(table && num);

	trees_num = table->nindices;
	trees = (OIndexKey *) palloc(sizeof(OIndexKey) * (trees_num + 3));
	for (i = 0; i < trees_num; i++)
	{
		trees[i].oids = table->indices[i].oids;
		trees[i].tablespace = table->indices[i].tablespace;
	}

	if (ORelOidsIsValid(table->bridge_oids))
	{
		trees[trees_num].oids = table->bridge_oids;
		trees[trees_num].tablespace = table->tablespace;
		trees_num++;
	}

	if (ORelOidsIsValid(table->toast_oids))
	{
		trees[trees_num].oids = table->toast_oids;
		trees[trees_num].tablespace = table->tablespace;
		trees_num++;
	}

	// ctid primary index if needed
	if (table->nindices == 0 ||
		table->indices[PrimaryIndexNumber].type != oIndexPrimary)
	{
		trees[trees_num].oids = table->oids;
		trees[trees_num].tablespace = table->tablespace;
		trees_num++;
	}

	*num = trees_num;
	pub static mut TREES: return = std::mem::zeroed();
}

//
// Updates SYS_TREES_O_INDICES.
//
fn
o_tables_oids_indexes(old_table: &mut OTable, new_table: &mut OTable,
					  OXid oxid, CommitSeqNo csn)
{
	pub static mut O_TABLE_INDEX_OIDS_KEY: *mut old_keys = std::ptr::null_mut();
	pub static mut O_TABLE_INDEX_OIDS_KEY: *mut new_keys = std::ptr::null_mut();
	int			old_keys_num = 0,
				new_keys_num = 0,
				i = 0,
				j = 0;
	pub static mut REUSE_RELNODE: bool = false;

	old_keys = o_table_make_index_oids_keys(old_table, &old_keys_num);
	new_keys = o_table_make_index_oids_keys(new_table, &new_keys_num);

	while (i < old_keys_num || j < new_keys_num)
	{
		pub static mut CMP: std::os::raw::c_int = 0;

		if (i >= old_keys_num)
		{
			cmp = 1;
		}
		else if (j >= new_keys_num)
		{
			cmp = -1;
		}
		else
		{
			cmp = index_keys_cmp(&old_keys[i], &new_keys[j]);

			if (cmp == 0)
			{
				i++;
				j++;
				continue;
			}
			else if (new_keys_num == old_keys_num &&
					 old_keys[i].oids.datoid == new_keys[j].oids.datoid &&
					 old_keys[i].oids.reloid != new_keys[j].oids.reloid &&
					 old_keys[i].oids.relnode == new_keys[j].oids.relnode)
			{
				reuse_relnode = true;
			}
		}

		if (cmp < 0)
		{
			pub static mut RESULT: bool = false;

			Assert(old_table);
			if (!reuse_relnode)
			{
				elog(DEBUG2, "o_indices del (%u, %u, %u, %u) - (%u, %u, %u)",
					 old_keys[i].type,
					 old_keys[i].oids.datoid,
					 old_keys[i].oids.reloid,
					 old_keys[i].oids.relnode,
					 old_table->oids.datoid,
					 old_table->oids.reloid,
					 old_table->oids.relnode);

				result = o_indices_del(old_table, old_keys[i].ixNum,
									   oxid, csn);
				if (!result)
					elog(ERROR, "missing entries in o_indices");
			}
			i++;
		}

		if (cmp > 0)
		{
			pub static mut PG_USED_FOR_ASSERTS_ONLY: bool		result = std::mem::zeroed();

			Assert(new_table);
			if (!reuse_relnode)
			{
				elog(DEBUG2, "o_indices add (%u, %u, %u, %u) - (%u, %u, %u)",
					 new_keys[j].type,
					 new_keys[j].oids.datoid,
					 new_keys[j].oids.reloid,
					 new_keys[j].oids.relnode,
					 new_table->oids.datoid,
					 new_table->oids.reloid,
					 new_table->oids.relnode);

				result = o_indices_add(new_table, new_keys[j].ixNum,
									   oxid, csn);
				Assert(result);
			}
			reuse_relnode = false;
			j++;
		}
	}
}

OTable *
o_tables_drop_by_oids(ORelOids oids, OXid oxid, CommitSeqNo csn)
{
	pub static mut KEY: OTableChunkKey = std::mem::zeroed();
	pub static mut O_TABLE: *mut table = std::ptr::null_mut();
	bool		result = false,
				any_wal = false;
	pub static mut B_TREE_DESCR: *mut sys_tree = std::ptr::null_mut();

	key.oids = oids;
	key.chunknum = 0;

	systrees_modify_start();
	table = o_tables_get(oids);
	Assert(table);
	if (table)
	{
		o_tables_oids_indexes(table, NULL, oxid, csn);
		sys_tree = get_sys_tree(SYS_TREES_O_TABLES);
		any_wal = table->persistence != RELPERSISTENCE_TEMP;
		result = generic_toast_delete_optional_wal(&oTablesToastAPI,
												   (Pointer) &key, oxid, csn,
												   sys_tree, any_wal);
	}
	else
	{
		ereport(ERROR,
				errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
				errmsg("[%s]: table == NULL", __func__));
	}
	systrees_modify_end(any_wal);

	if (result)
	{
		pub static mut TABLE: return = std::mem::zeroed();
	}
	else
	{
		if (table)
			o_table_free(table);
		pub static mut NULL: return = std::mem::zeroed();
	}
}


o_tables_drop_all(OXid oxid, CommitSeqNo csn, Oid database_id)
{
	pub static mut ARG: OTablesDropAllArg = std::mem::zeroed();

	arg.oxid = oxid;
	arg.csn = csn;
	arg.datoid = database_id;

	o_tables_foreach_oids(o_tables_drop_all_callback,
						  &o_non_deleted_snapshot, &arg);
}

fn
o_tables_move_all_callback(o_table: &mut OTable,  *arg)
{
	move_arg: &mut OTablesMoveAllArg = (OTablesMoveAllArg *) arg;
	pub static mut CTID_IDX_OFF: std::os::raw::c_int = o_table->has_primary ? 0 : 1;
	pub static mut TABLE_MOVED: bool = false;

	Assert(o_table);

	if (move_arg->datoid != o_table->oids.datoid)
		return;

	if (o_table->tablespace == move_arg->old_tablespace)
	{
		o_table->tablespace = move_arg->new_tablespace;
		if (!o_table->has_primary)
		{
			o_indices_update(o_table, PrimaryIndexNumber, move_arg->oxid, move_arg->csn);
			table_moved = true;
		}
		if (ORelOidsIsValid(o_table->toast_oids))
		{
			o_indices_update(o_table, TOASTIndexNumber, move_arg->oxid, move_arg->csn);
			table_moved = true;
		}
		if (ORelOidsIsValid(o_table->bridge_oids))
		{
			o_indices_update(o_table, BridgeIndexNumber, move_arg->oxid, move_arg->csn);
			table_moved = true;
		}
	}

	for (int ixnum = 0; ixnum < o_table->nindices; ixnum++)
	{
		pub static mut O_TABLE_INDEX: *mut ix_table = std::ptr::null_mut();

		ix_table = &o_table->indices[ixnum];
		if (ix_table->tablespace != move_arg->old_tablespace)
			continue;
		ix_table->tablespace = move_arg->new_tablespace;
		o_indices_update(o_table, ixnum + ctid_idx_off, move_arg->oxid, move_arg->csn);
		o_invalidate_oids(ix_table->oids);
		o_invalidate_descrs(ix_table->oids.datoid, ix_table->oids.reloid, ix_table->oids.relnode);
	}
	if (table_moved)
	{
		o_tables_update(o_table, move_arg->oxid, move_arg->csn);
	}
	o_tables_after_update(o_table, move_arg->oxid, move_arg->csn);
}


o_tables_move_all(OXid oxid, CommitSeqNo csn, Oid database_id, Oid old_tspcoid, Oid new_tspcoid)
{
	pub static mut ARG: OTablesMoveAllArg = std::mem::zeroed();

	arg.oxid = oxid;
	arg.csn = csn;
	arg.datoid = database_id;
	arg.old_tablespace = old_tspcoid;
	arg.new_tablespace = new_tspcoid;

	add_database_copy_wal_record(database_id, old_tspcoid, new_tspcoid);
	o_tables_foreach(o_tables_move_all_callback,
					 &o_non_deleted_snapshot, &arg);
}

fn
o_tables_evict_callback(o_table: &mut OTable,  *arg)
{
	args: &mut OTablesEvictDBArg = (OTablesEvictDBArg *) arg;
	pub static mut O_TABLE_DESCR: *mut descr = std::ptr::null_mut();
	pub static mut B_TREE_DESCR: *mut td = std::ptr::null_mut();

	if (args->datoid != o_table->oids.datoid)
		return;

	descr = o_fetch_table_descr(o_table->oids);

	if (!descr)
		ereport(ERROR,
				(errcode(ERRCODE_WRONG_OBJECT_TYPE),
				 errmsg("relation oid %u does not exists", o_table->oids.reloid)));

	table_descr_inc_refcnt(descr);

	for (int treen = 0; treen < descr->nIndices; treen++)
	{
		td = &descr->indices[treen]->desc;
		write_tree_pages(td, -1, true);
		args->evicted = lappend_oid(args->evicted, td->oids.relnode);
	}
	td = &descr->toast->desc;
	write_tree_pages(td, -1, true);
	args->evicted = lappend_oid(args->evicted, td->oids.relnode);

	table_descr_dec_refcnt(descr);
	o_invalidate_descrs(descr->oids.datoid, descr->oids.reloid, descr->oids.reloid);
}


o_tables_evict(Oid datoid, List **evicted)
{
	pub static mut ARG: OTablesEvictDBArg = std::mem::zeroed();

	arg.datoid = datoid;
	arg.evicted = NIL;
	o_tables_foreach(o_tables_evict_callback, &o_non_deleted_snapshot, &arg);
	*evicted = arg.evicted;
}


o_tables_truncate_all_unlogged()
{
	pub static mut ARG: OTablesDropAllArg = std::mem::zeroed();
	pub static mut OXID: OXid = std::mem::zeroed();
	pub static mut O_SNAPSHOT: OSnapshot = std::mem::zeroed();

	fill_current_oxid_osnapshot(&oxid, &oSnapshot);

	arg.oxid = oxid;
	arg.csn = oSnapshot.csn;

	o_tables_foreach(o_tables_truncate_unlogged_callback,
					 &o_non_deleted_snapshot, &arg);
}

bool
o_tables_add(table: &mut OTable, OXid oxid, CommitSeqNo csn)
{
	pub static mut KEY: OTableChunkKey = std::mem::zeroed();
	pub static mut RESULT: bool = false;
	pub static mut DATA: Pointer = std::ptr::null_mut();
	pub static mut LEN: std::os::raw::c_int = 0;
	pub static mut B_TREE_DESCR: *mut sys_tree = std::ptr::null_mut();

	key.oids = table->oids;
	key.chunknum = 0;
	key.version = 0;

	systrees_modify_start();
	o_tables_oids_indexes(NULL, table, oxid, csn);
	sys_tree = get_sys_tree(SYS_TREES_O_TABLES);
	data = serialize_o_table(table, &len);
	result = generic_toast_insert_optional_wal(&oTablesToastAPI,
											   (Pointer) &key, data, len, oxid,
											   csn, sys_tree, table->persistence != RELPERSISTENCE_TEMP);
	systrees_modify_end(table->persistence != RELPERSISTENCE_TEMP);
	pfree(data);

	pub static mut RESULT: return = std::mem::zeroed();
}

//
// Same as o_tables_get, if version not NULL find o_tables with passed version.
//
// If deserialization fails due to truncated toast data (missing chunks from a
// concurrent write), retries with exponential backoff up to
// O_DESERIALIZE_MAX_RETRIES times before reporting an error.
//
OTable *
o_tables_get_extended(ORelOids oids, OTableFetchContext ctx)
{
	pub static mut KEY: OTableChunkKey = std::mem::zeroed();
	pub static mut RETRY: std::os::raw::c_int = 0;

	key.oids = oids;
	key.chunknum = 0;
	key.version = ctx.version;

	for (retry = 0;; retry++)
	{
		pub static mut BOUND_KEY: OTableChunkBoundKey = std::mem::zeroed();
		pub static mut O_TABLE_CHUNK_BOUND_KEY: *mut found_key = std::ptr::null_mut();
		pub static mut RESULT: Pointer = std::ptr::null_mut();
		pub static mut DATA_LENGTH: Size = 0;
		pub static mut O_TABLE: *mut oTable = std::ptr::null_mut();

		boundKey.key = key;
		boundKey.oxid = InvalidOXid;
		found_key = &boundKey;
		result = generic_toast_get_any_with_key(&oTablesToastAPI,
												(Pointer) &key,
												&dataLength,
												ctx.snapshot,
												get_sys_tree(SYS_TREES_O_TABLES),
												(Pointer *) &found_key);

		if (result == NULL)
			pub static mut NULL: return = std::mem::zeroed();

		oTable = deserialize_o_table(result, dataLength);
		pfree(result);

		if (oTable != NULL)
		{
			oTable->version = found_key->key.version;
			pfree(found_key);
			pub static mut O_TABLE: return = std::mem::zeroed();
		}

		// Truncated data — concurrent chunk write in progress, retry
		pfree(found_key);

		if (retry >= O_DESERIALIZE_MAX_RETRIES ||
			!COMMITSEQNO_IS_INPROGRESS(ctx.snapshot->csn))
			ereport(ERROR,
					(errcode(ERRCODE_INTERNAL_ERROR),
					 errmsg("failed to deserialize OTable (%u, %u, %u) after %d retries",
							oids.datoid, oids.reloid, oids.relnode,
							retry + 1)));

		pg_usleep(Min(O_DESERIALIZE_RETRY_MIN_DURATION << retry, O_DESERIALIZE_RETRY_MAX_DURATION));
	}
}

//
// Find OTable by its oids
//
OTable *
o_tables_get(ORelOids oids)
{
	return o_tables_get_extended(oids, default_table_fetch_context);
}

//
// Find OTable by tree oids
//
OTable *
o_tables_get_by_tree(ORelOids oids, OIndexType type)
{
	pub static mut TABLE_OIDS: ORelOids = std::mem::zeroed();
	pub static mut RESULT: bool = false;

	// See if it's index oid first
	result = o_indices_find_table_oids(oids, type, &o_in_progress_snapshot,
									   &tableOids);
	if (!result)
		pub static mut NULL: return = std::mem::zeroed();

	return o_tables_get(tableOids);
}

// Returns number of OrioleDB tables in the database
int
o_tables_num(Oid datoid)
{
	pub static mut NUM_ARG: OTablesNumArg = std::mem::zeroed();

	num_arg.datoid = datoid;
	num_arg.result = 0;
	o_tables_foreach_oids(o_tables_num_callback, &o_non_deleted_snapshot,
						  &num_arg);

	return num_arg.result;
}


o_table_free(table: &mut OTable)
{
	pub static mut I: std::os::raw::c_int = 0;

	Assert(table != NULL);

	for (i = 0; i < table->nindices; i++)
	{
		if (table->indices[i].index_mctx)
			MemoryContextDelete(table->indices[i].index_mctx);
	}
	if (table->tbl_mctx)
		MemoryContextDelete(table->tbl_mctx);
	pfree(table);
}

bool
o_tables_update(table: &mut OTable, OXid oxid, CommitSeqNo csn)
{
	pub static mut KEY: OTableChunkKey = std::mem::zeroed();
	pub static mut O_TABLE: *mut old_table = std::ptr::null_mut();
	pub static mut RESULT: bool = false;
	pub static mut DATA: Pointer = std::ptr::null_mut();
	pub static mut LEN: std::os::raw::c_int = 0;
	pub static mut B_TREE_DESCR: *mut sys_tree = std::ptr::null_mut();

	key.oids = table->oids;
	key.chunknum = 0;
	key.version = table->version + 1;

	systrees_modify_start();
	old_table = o_tables_get(table->oids);
	o_tables_oids_indexes(old_table, table, oxid, csn);
	sys_tree = get_sys_tree(SYS_TREES_O_TABLES);
	data = serialize_o_table(table, &len);
	result = generic_toast_update_optional_wal(&oTablesToastAPI,
											   (Pointer) &key, data, len, oxid,
											   csn, sys_tree, table->persistence != RELPERSISTENCE_TEMP);
	systrees_modify_end(table->persistence != RELPERSISTENCE_TEMP);

	pfree(data);
	o_table_free(old_table);

	pub static mut RESULT: return = std::mem::zeroed();
}


o_tables_after_update(o_table: &mut OTable, OXid oxid, CommitSeqNo csn)
{
	//
// @NOTE o_indices_update(o_table, PrimaryIndexNumber, oxid, csn); moved
// out from here
//

	if (o_table->has_primary)
	{
		o_add_invalidate_undo_item(o_table->indices[PrimaryIndexNumber].oids,
								   O_INVALIDATE_OIDS_ON_ABORT);
		o_invalidate_oids(o_table->indices[PrimaryIndexNumber].oids);
	}
	o_add_invalidate_undo_item(o_table->oids,
							   O_INVALIDATE_OIDS_ON_ABORT);
	o_invalidate_oids(o_table->oids);
	if (ORelOidsIsValid(o_table->toast_oids))
	{
		o_add_invalidate_undo_item(o_table->toast_oids,
								   O_INVALIDATE_OIDS_ON_ABORT);
		o_invalidate_oids(o_table->toast_oids);
	}
}

bool
o_tables_rel_try_lock_extended(oids: &mut ORelOids, int lockmode,
							   nested: &mut bool, bool checkpoint)
{
	pub static mut LOCKTAG: LOCKTAG = std::mem::zeroed();
	pub static mut RESULT: LockAcquireResult = std::mem::zeroed();

	o_tables_rel_fill_locktag(&locktag, oids, lockmode, checkpoint);

	if (nested != NULL)
		*nested = DoLocalLockExist(&locktag);

	if (lockmode == AccessExclusiveLock &&
		locktag.locktag_lockmethodid == DEFAULT_LOCKMETHOD)
		locktag.locktag_lockmethodid = NO_LOG_LOCKMETHOD;
	result = LockAcquire(&locktag, lockmode, false, true);

	if (result != LOCKACQUIRE_NOT_AVAIL)
	{
		AcceptInvalidationMessages();
		pub static mut TRUE: return = std::mem::zeroed();
	}
	pub static mut FALSE: return = std::mem::zeroed();
}


o_tables_rel_lock_extended(oids: &mut ORelOids, int lockmode, bool checkpoint)
{
	pub static mut LOCKTAG: LOCKTAG = std::mem::zeroed();

	o_tables_rel_fill_locktag(&locktag, oids, lockmode, checkpoint);

	if (lockmode == AccessExclusiveLock && checkpoint)
		locktag.locktag_lockmethodid = NO_LOG_LOCKMETHOD;

	LockAcquire(&locktag, lockmode, false, false);
	AcceptInvalidationMessages();
}


o_tables_rel_lock_extended_no_inval(oids: &mut ORelOids, int lockmode,
									bool checkpoint)
{
	pub static mut LOCKTAG: LOCKTAG = std::mem::zeroed();

	o_tables_rel_fill_locktag(&locktag, oids, lockmode, checkpoint);

	if (lockmode == AccessExclusiveLock && checkpoint)
		locktag.locktag_lockmethodid = NO_LOG_LOCKMETHOD;

	LockAcquire(&locktag, lockmode, false, false);
}


o_tables_rel_lock_exclusive_no_inval_no_log(oids: &mut ORelOids)
{
	pub static mut LOCKTAG: LOCKTAG = std::mem::zeroed();

	o_tables_rel_fill_locktag(&locktag, oids, AccessExclusiveLock, false);
	locktag.locktag_lockmethodid = NO_LOG_LOCKMETHOD;

	LockAcquire(&locktag, AccessExclusiveLock, false, false);
}


o_tables_rel_unlock_extended(oids: &mut ORelOids, int lockmode, bool checkpoint)
{
	pub static mut LOCKTAG: LOCKTAG = std::mem::zeroed();

	o_tables_rel_fill_locktag(&locktag, oids, lockmode, checkpoint);

	if (!LockRelease(&locktag, lockmode, false))
	{
		elog(ERROR, "Can not release %s table lock on datoid = %d, "

			 "relnode = %d",
			 lockmode == AccessShareLock ? "share" : "exclusive",
			 oids->datoid, oids->relnode);
	}
}

char *
o_get_type_name(Oid typid, int32 typmod)
{
	return format_type_extended(typid,
								typmod,
								FORMAT_TYPE_TYPEMOD_GIVEN |
								FORMAT_TYPE_ALLOW_INVALID);
}

static text *
describe_table(ORelOids oids)
{
	pub static mut O_TABLE: *mut table = std::ptr::null_mut();
	StringInfoData buf,
				format,
				title;
	column_str: &mut char = "Column",
			   *type_str = "Type",
			   *collation_str = "Collation";
	int			i,
				max_column_str,
				max_type_str,
				max_collation_str;

	table = o_tables_get(oids);
	if (table == NULL)
		elog(ERROR, "unable to find orioledb table description.");

	max_column_str = strlen(column_str);
	max_type_str = strlen(type_str);
	max_collation_str = strlen(collation_str);
	for (i = 0; i < table->nfields; i++)
	{
		pub static mut O_TABLE_FIELD: *mut field = &table->fields[i];
		typename: &mut char = o_get_type_name(field->typid, field->typmod);
		colname: &mut char = get_collation_name(field->collation);

		if (max_column_str < strlen(NameStr(field->name)))
			max_column_str = strlen(NameStr(field->name));
		if (max_type_str < strlen(typename))
			max_type_str = strlen(typename);
		if (colname != NULL)
		{
			if (max_collation_str < strlen(colname))
				max_collation_str = strlen(colname);
		}
	}

	initStringInfo(&title);
	appendStringInfo(&title, "Compress = %d, Primary compress = %d, TOAST compress = %d\n",
					 table->default_compress,
					 table->primary_compress,
					 table->toast_compress);
	appendStringInfo(&title, " %%%ds | %%%ds | %%%ds | Nullable | Dropped ",
					 max_column_str,
					 max_type_str,
					 max_collation_str);
	if (orioledb_table_description_compress)
		appendStringInfo(&title, "| Compression ");
	appendStringInfo(&title, "\n");
	initStringInfo(&format);
	appendStringInfo(&format, " %%%ds | %%%ds | %%%ds | %%8s | %%7s ",
					 max_column_str,
					 max_type_str,
					 max_collation_str);
	initStringInfo(&buf);
	appendStringInfo(&buf, title.data, column_str, type_str, collation_str);

	for (i = 0; i < table->nfields; i++)
	{
		pub static mut O_TABLE_FIELD: *mut field = &table->fields[i];
		typename: &mut char = o_get_type_name(field->typid, field->typmod);
		colname: &mut char = get_collation_name(field->collation);

		appendStringInfo(&buf, format.data,
						 NameStr(field->name),
						 typename,
						 colname ? colname : "(null)",
						 field->notnull ? "false" : "true",
						 field->droped ? "true" : "false");
		if (orioledb_table_description_compress)
		{
			pub static mut CHAR: *mut const compression = "";

			if (CompressionMethodIsValid(field->compression))
				compression = GetCompressionMethodName(field->compression);
			appendStringInfo(&buf, "| %11s ", compression);
		}
		appendStringInfo(&buf, "\n");
	}

	return cstring_to_text(buf.data);
}

Datum
orioledb_table_description(PG_FUNCTION_ARGS)
{
	pub static mut OIDS: ORelOids = std::mem::zeroed();
	pub static mut REL: Relation = std::mem::zeroed();

	if (PG_NARGS() == 1)
	{
		Oid			relid = PG_GETARG_OID(0);

		rel = relation_open(relid, AccessShareLock);
		ORelOidsSetFromRel(oids, rel);
		relation_close(rel, AccessShareLock);
	}
	else if (PG_NARGS() == 3)
	{
		oids.datoid = PG_GETARG_OID(0);
		oids.reloid = PG_GETARG_OID(1);
		oids.relnode = PG_GETARG_OID(2);
	}
	else
	{
		PG_RETURN_NULL();
	}

	PG_RETURN_POINTER(describe_table(oids));
}

Datum
orioledb_table_oids(PG_FUNCTION_ARGS)
{
	rsinfo: &mut ReturnSetInfo = (ReturnSetInfo *) fcinfo->resultinfo;
	pub static mut TUPDESC: TupleDesc = std::mem::zeroed();
	pub static mut TUPLESTORESTATE: *mut tupstore = std::ptr::null_mut();
	pub static mut PER_QUERY_CTX: MemoryContext = std::mem::zeroed();
	pub static mut OLDCONTEXT: MemoryContext = std::mem::zeroed();

	per_query_ctx = rsinfo->econtext->ecxt_per_query_memory;
	oldcontext = MemoryContextSwitchTo(per_query_ctx);

	// Build a tuple descriptor for our result type
	if (get_call_result_type(fcinfo, NULL, &tupdesc) != TYPEFUNC_COMPOSITE)
		elog(ERROR, "return type must be a row type");

	tupstore = tuplestore_begin_heap(true, false, work_mem);
	rsinfo->returnMode = SFRM_Materialize;
	rsinfo->setResult = tupstore;
	rsinfo->setDesc = tupdesc;

	MemoryContextSwitchTo(oldcontext);

	o_tables_foreach_oids(o_table_oids_array_callback,
						  &o_non_deleted_snapshot, rsinfo);

	return (Datum) 0;
}

fn
o_tables_foreach_callback(ORelOids oids,  *arg)
{
	foreach_arg: &mut OTablesForeachArg = (OTablesForeachArg *) arg;
	pub static mut O_TABLE: *mut table = std::ptr::null_mut();

	Assert(ORelOidsIsValid(oids));

	table = o_tables_get(oids);
	if (table != NULL)
	{
		foreach_arg->callback(table, foreach_arg->arg);
		o_table_free(table);
	}
}

fn
o_tables_drop_all_callback(ORelOids oids,  *arg)
{
	drop_arg: &mut OTablesDropAllArg = (OTablesDropAllArg *) arg;

	if (drop_arg->datoid == oids.datoid)
	{
		pub static mut O_TABLE: *mut table = std::ptr::null_mut();

		table = o_tables_drop_by_oids(oids, drop_arg->oxid, drop_arg->csn);

		if (table)
		{
			pub static mut O_INDEX_KEY: *mut trees = std::ptr::null_mut();
			pub static mut NUM_TREES: std::os::raw::c_int = 0;

			trees = o_table_make_index_keys(table, &numTrees);
			add_undo_drop_relnode(oids, trees, numTrees);
			pfree(trees);
			o_table_free(table);
		}
	}
}

fn
o_tables_truncate_unlogged_callback(o_table: &mut OTable,  *arg)
{
	if (o_table->persistence == RELPERSISTENCE_UNLOGGED)
	{
		o_truncate_table(o_table->oids, false);
		AcceptInvalidationMessages();
	}
}

fn
o_table_oids_array_callback(ORelOids oids,  *arg)
{
	rsinfo: &mut ReturnSetInfo = (ReturnSetInfo *) arg;
	Datum		values[3];
	bool		nulls[3] = {false};

	values[0] = oids.datoid;
	values[1] = oids.reloid;
	values[2] = oids.relnode;
	tuplestore_putvalues(rsinfo->setResult, rsinfo->setDesc, values, nulls);
}

fn
o_tables_num_callback(ORelOids oids,  *arg)
{
	num_arg: &mut OTablesNumArg = (OTablesNumArg *) arg;

	if (oids.datoid == num_arg->datoid)
		num_arg->result++;
}

// No existing callers
OTableField *
o_table_field_by_name(table: &mut OTable, const name: &mut char)
{
	pub static mut I: std::os::raw::c_int = 0;

	if (name == NULL)
		pub static mut NULL: return = std::mem::zeroed();

	i = o_table_fieldnum(table, name);

	if (i < table->nfields)
		return &table->fields[i];
	else
		pub static mut NULL: return = std::mem::zeroed();
}

//
// Copy of TupleDescInitEntry() without SysCache usage.
//
fn
o_table_tupdesc_init_entry(TupleDesc desc, AttrNumber att_num, name: &mut char,
						   field: &mut OTableField)
{
	pub static mut ATT: Form_pg_attribute = std::mem::zeroed();

	//
// sanity checks
//
	Assert(PointerIsValid(desc));
	Assert(att_num >= 1);
	Assert(att_num <= desc->natts);
	Assert(field != NULL);

	//
// initialize the attribute fields
//
	att = TupleDescAttr(desc, att_num - 1);

	att->attrelid = 0;			// dummy value

	//
// Note: name can be NULL, because the planner doesn't always fill in
// valid resname values in targetlists, particularly for resjunk
// attributes. Also, do nothing if caller wants to re-use the old attname.
//
	if (name == NULL)
		MemSet(NameStr(att->attname), 0, NAMEDATALEN);
	else if (name != NameStr(att->attname))
		namestrcpy(&(att->attname), name);

#if PG_VERSION_NUM < 170000
	att->attstattarget = -1;
#endif
#if PG_VERSION_NUM < 180000
	att->attcacheoff = -1;
#endif
	att->atttypmod = field->typmod;

	att->attnum = att_num;
	att->attndims = field->ndims;

	att->attnotnull = field->notnull;
	att->atthasdef = field->hasdef;
	att->attgenerated = field->generated;
	att->atthasmissing = field->hasmissing;
	att->attidentity = '\0';
	att->attisdropped = field->droped;
	att->attislocal = true;
	att->attinhcount = 0;

	// attacl, attoptions and attfdwoptions are not present in tupledescs
	att->atttypid = field->typid;
	att->attlen = field->typlen;
	att->attbyval = field->byval;
	att->attalign = field->align;
	att->attstorage = field->storage;
	att->attcompression = field->compression;
	att->attcollation = field->collation;

#if PG_VERSION_NUM >= 180000
	populate_compact_attribute(desc, att_num - 1);
#endif
}

static inline 
o_tables_rel_fill_locktag(tag: &mut LOCKTAG, oids: &mut ORelOids, int lockmode, bool checkpoint)
{
	Oid			datoid = checkpoint ? (oids->datoid | CHECKPOINT_LOCK_BIT) : oids->datoid;

	Assert(lockmode == AccessShareLock || lockmode == AccessExclusiveLock);
	Assert(ORelOidsIsValid(*oids) && !IS_SYS_TREE_OIDS(*oids));
	memset(tag, 0, sizeof(LOCKTAG));
	SET_LOCKTAG_RELATION(*tag, datoid, oids->reloid);
	if (checkpoint)
		tag->locktag_type = LOCKTAG_USERLOCK;
}

fn
serialize_o_table_index(o_table_index: &mut OTableIndex, StringInfo str)
{
	appendBinaryStringInfo(str, (Pointer) o_table_index,
						   offsetof(OTableIndex, exprfields));
	appendBinaryStringInfo(str, (Pointer) o_table_index->exprfields,
						   o_table_index->nexprfields * sizeof(OTableField));
	o_serialize_node((Node *) o_table_index->predicate, str);
	if (o_table_index->predicate)
		o_serialize_string(o_table_index->predicate_str, str);
	o_serialize_node((Node *) o_table_index->expressions, str);
	appendBinaryStringInfo(str, (Pointer) &o_table_index->tablespace, sizeof(Oid));
	if (o_table_index->type == oIndexExclusion)
		appendBinaryStringInfo(str, (Pointer) o_table_index->exclops, sizeof(Oid) * o_table_index->nkeyfields);
	appendBinaryStringInfo(str, &o_table_index->immediate, sizeof(bool));
}

Pointer
serialize_o_table(o_table: &mut OTable, size: &mut int)
{
	pub static mut STR: StringInfoData = std::mem::zeroed();
	pub static mut I: std::os::raw::c_int = 0;

	Assert(o_table != NULL);
	if (o_table->data_version != ORIOLEDB_SYS_TREE_VERSION)
		elog(FATAL,
			 "ORIOLEDB_SYS_TREE_VERSION %u of OrioleDB cluster is not among supported for conversion from %u",
			 o_table->data_version, ORIOLEDB_SYS_TREE_VERSION);

	initStringInfo(&str);
	appendBinaryStringInfo(&str, (Pointer) o_table,
						   offsetof(OTable, indices));
	for (i = 0; i < o_table->nindices; i++)
	{
		serialize_o_table_index(&o_table->indices[i], &str);
	}
	appendBinaryStringInfo(&str, (Pointer) o_table->fields,
						   o_table->nfields * sizeof(OTableField));

	for (i = 0; i < o_table->nfields; i++)
	{
		pub static mut FIELD_SIZE: Size = 0;
		Pointer		buf,
					buf_start;

		field_size = datumEstimateSpace(o_table->missing[i].am_value,
										!o_table->missing[i].am_present,
										o_table->fields[i].byval,
										o_table->fields[i].typlen);
		appendBinaryStringInfo(&str, (Pointer) &o_table->missing[i].am_present,
							   sizeof(bool));
		buf = palloc(field_size);
		buf_start = buf;		// copied because datumSerialize moves buf ptr
		datumSerialize(o_table->missing[i].am_value,
					   !o_table->missing[i].am_present,
					   o_table->fields[i].byval,
					   o_table->fields[i].typlen,
					   &buf);
		appendBinaryStringInfo(&str, buf_start, field_size);
	}

	appendBinaryStringInfo(&str, (Pointer) &o_table->tablespace, sizeof(Oid));

	*size = str.len;
	return str.data;
}

//
// Returns false if the data is truncated (missing toast chunks).
//
static bool
deserialize_o_table_index(o_table_index: &mut OTableIndex, ptr: &mut Pointer,
						  Pointer data, Size length, uint16 data_version)
{
	pub static mut LEN: std::os::raw::c_int = 0;
	MemoryContext mcxt,
				old_mcxt;

	len = offsetof(OTableIndex, exprfields);
	if ((*ptr - data) + len > length)
		pub static mut FALSE: return = std::mem::zeroed();
	memcpy(o_table_index, *ptr, len);
	*ptr += len;

	o_table_index->index_mctx = NULL;
	mcxt = OGetIndexContext(o_table_index);
	old_mcxt = MemoryContextSwitchTo(mcxt);
	len = o_table_index->nexprfields * sizeof(OTableField);
	if ((*ptr - data) + len > length)
	{
		MemoryContextSwitchTo(old_mcxt);
		pub static mut FALSE: return = std::mem::zeroed();
	}
	o_table_index->exprfields = (OTableField *) palloc0(len);
	memcpy(o_table_index->exprfields, *ptr, len);
	*ptr += len;

	if (!o_deserialize_node_safe(ptr, data, length,
								 (Node **) &o_table_index->predicate))
	{
		MemoryContextSwitchTo(old_mcxt);
		pub static mut FALSE: return = std::mem::zeroed();
	}
	if (o_table_index->predicate)
	{
		if (!o_deserialize_string_safe(ptr, data, length,
									   &o_table_index->predicate_str))
		{
			MemoryContextSwitchTo(old_mcxt);
			pub static mut FALSE: return = std::mem::zeroed();
		}
	}
	if (!o_deserialize_node_safe(ptr, data, length,
								 (Node **) &o_table_index->expressions))
	{
		MemoryContextSwitchTo(old_mcxt);
		pub static mut FALSE: return = std::mem::zeroed();
	}

	len = sizeof(Oid);
	if ((*ptr - data) + len > length)
	{
		MemoryContextSwitchTo(old_mcxt);
		pub static mut FALSE: return = std::mem::zeroed();
	}
	memcpy(&o_table_index->tablespace, *ptr, len);
	*ptr += len;

	if (o_table_index->type == oIndexExclusion)
	{
		len = sizeof(Oid) * o_table_index->nkeyfields;
		if ((*ptr - data) + len > length)
		{
			MemoryContextSwitchTo(old_mcxt);
			pub static mut FALSE: return = std::mem::zeroed();
		}
		o_table_index->exclops = (Oid *) palloc0(len);
		memcpy(o_table_index->exclops, *ptr, len);
		*ptr += len;
	}

	len = sizeof(bool);
	if ((*ptr - data) + len > length)
	{
		MemoryContextSwitchTo(old_mcxt);
		pub static mut FALSE: return = std::mem::zeroed();
	}
	memcpy(&o_table_index->immediate, *ptr, len);
	*ptr += len;

	MemoryContextSwitchTo(old_mcxt);
	pub static mut TRUE: return = std::mem::zeroed();
}

//
// A truncation-tolerant version of datumRestore().  Returns false if there
// isn't enough data remaining in the buffer to read the full datum, so the
// caller can bail out gracefully instead of crashing.
//
static bool
datumRestoreSafe(char **start_address, isnull: &mut bool, result: &mut Datum,
				 Pointer data, Size length)
{
	pub static mut HEADER: std::os::raw::c_int = 0;
		   *d;
	pub static mut CHAR: *mut ptr = *start_address;

	// Need at least sizeof(int) for the header word.
	if ((ptr - data) + (int) sizeof(int) > length)
		pub static mut FALSE: return = std::mem::zeroed();

	memcpy(&header, ptr, sizeof(int));
	ptr += sizeof(int);

	// NULL datum.
	if (header == -2)
	{
		*isnull = true;
		*result = (Datum) 0;
		*start_address = ptr;
		pub static mut TRUE: return = std::mem::zeroed();
	}

	*isnull = false;

	// Pass-by-value datum.
	if (header == -1)
	{
		pub static mut VAL: Datum = std::mem::zeroed();

		if ((ptr - data) + (int) sizeof(Datum) > length)
			pub static mut FALSE: return = std::mem::zeroed();

		memcpy(&val, ptr, sizeof(Datum));
		ptr += sizeof(Datum);
		*result = val;
		*start_address = ptr;
		pub static mut TRUE: return = std::mem::zeroed();
	}

	// Pass-by-reference: header is the byte count.
	Assert(header > 0);
	if ((ptr - data) + header > length)
		pub static mut FALSE: return = std::mem::zeroed();

	d = palloc(header);
	memcpy(d, ptr, header);
	ptr += header;
	*result = PointerGetDatum(d);
	*start_address = ptr;
	pub static mut TRUE: return = std::mem::zeroed();
}

//
// Deserialize OTable from toast data.  Returns NULL if the data is truncated
// (e.g. due to missing toast chunks from a concurrent write race condition).
//
OTable *
deserialize_o_table(Pointer data, Size length)
{
	pub static mut PTR: Pointer = data;
	pub static mut O_TABLE: *mut o_table = std::ptr::null_mut();
	pub static mut LEN: std::os::raw::c_int = 0;
	pub static mut I: std::os::raw::c_int = 0;
	pub static mut OLDCXT: MemoryContext = std::mem::zeroed();
	pub static mut TBL_CXT: MemoryContext = std::mem::zeroed();

	o_table = (OTable *) palloc0(sizeof(OTable));
	len = offsetof(OTable, indices);
	if ((ptr - data) + len > length)
	{
		pfree(o_table);
		pub static mut NULL: return = std::mem::zeroed();
	}
	memcpy(o_table, ptr, len);
	ptr += len;

	tbl_cxt = OGetTableContext(o_table);
	oldcxt = MemoryContextSwitchTo(tbl_cxt);

	len = o_table->nindices * sizeof(OTableIndex);
	o_table->indices = (OTableIndex *) palloc0(len);
	for (i = 0; i < o_table->nindices; i++)
	{
		if (!deserialize_o_table_index(&o_table->indices[i], &ptr,
									   data, length, o_table->data_version))
		{
			MemoryContextSwitchTo(oldcxt);
			o_table_free(o_table);
			pub static mut NULL: return = std::mem::zeroed();
		}
	}
	if ((ptr - data) > length)
	{
		MemoryContextSwitchTo(oldcxt);
		o_table_free(o_table);
		pub static mut NULL: return = std::mem::zeroed();
	}

	len = o_table->nfields * sizeof(OTableField);
	if ((ptr - data) + len > length)
	{
		MemoryContextSwitchTo(oldcxt);
		o_table_free(o_table);
		pub static mut NULL: return = std::mem::zeroed();
	}
	o_table->fields = (OTableField *) palloc(len);
	memcpy(o_table->fields, ptr, len);
	ptr += len;

	o_table->missing = (AttrMissing *)
		palloc(o_table->nfields * sizeof(AttrMissing));

	for (i = 0; i < o_table->nfields; i++)
	{
		pub static mut ATTR_MISSING: *mut miss = &o_table->missing[i];
		pub static mut ISNULL: bool = false;

		if ((ptr - data) + (int) sizeof(bool) > length)
		{
			MemoryContextSwitchTo(oldcxt);
			o_table_free(o_table);
			pub static mut NULL: return = std::mem::zeroed();
		}
		memcpy(&miss->am_present, ptr, sizeof(bool));
		ptr += sizeof(bool);
		if (!datumRestoreSafe(&ptr, &isnull, &miss->am_value, data, length))
		{
			MemoryContextSwitchTo(oldcxt);
			o_table_free(o_table);
			pub static mut NULL: return = std::mem::zeroed();
		}
	}
	MemoryContextSwitchTo(oldcxt);

	len = sizeof(Oid);
	if ((ptr - data) + len > length)
	{
		o_table_free(o_table);
		pub static mut NULL: return = std::mem::zeroed();
	}
	memcpy(&o_table->tablespace, ptr, len);
	ptr += len;

	if (ptr - data != length)
	{
		o_table_free(o_table);
		pub static mut NULL: return = std::mem::zeroed();
	}
	pub static mut O_TABLE: return = std::mem::zeroed();
}

fn
o_tables_drop_columns_with_type_callback(o_table: &mut OTable,  *arg)
{
	pub static mut I: std::os::raw::c_int = 0;
	pub static mut UPDATED: bool = false;
	drop_arg: &mut OTablesDropAllWithTypeArg = (OTablesDropAllWithTypeArg *) arg;

	// Ignore search for rows of own class in table and base types
	if (drop_arg->type_data->typtype == TYPTYPE_BASE ||
		(drop_arg->type_data->typtype == TYPTYPE_COMPOSITE &&
		 drop_arg->type_data->typrelid == o_table->oids.reloid))
		return;

	// Drop columns containing type
	for (i = 0; i < o_table->nfields; i++)
	{
		pub static mut O_TABLE_FIELD: *mut o_field = &o_table->fields[i];

		if (drop_arg->type_oid == o_field->typid && !o_field->droped)
		{
			o_field->droped = true;
			updated = true;
		}
	}

	if (updated)
	{
		o_tables_table_meta_lock(o_table);
		o_tables_update(o_table, drop_arg->oxid, drop_arg->csn);
		o_tables_table_meta_unlock(o_table, InvalidOid);
	}
}

//
// Drops all columns of a specific type
//

o_tables_drop_columns_by_type(OXid oxid, CommitSeqNo csn, Oid type_oid)
{
	pub static mut ARG: OTablesDropAllWithTypeArg = std::mem::zeroed();
	pub static mut TUPLE: HeapTuple = std::mem::zeroed();

	tuple = SearchSysCache1(TYPEOID, ObjectIdGetDatum(type_oid));
	Assert(HeapTupleIsValid(tuple));
	ReleaseSysCache(tuple);

	ASAN_UNPOISON_MEMORY_REGION(&arg, sizeof(arg));

	arg.oxid = oxid;
	arg.csn = csn;
	arg.type_oid = type_oid;
	arg.type_data = (Form_pg_type) GETSTRUCT(tuple);

	o_tables_foreach(o_tables_drop_columns_with_type_callback,
					 &o_in_progress_snapshot, &arg);
}


o_table_fill_oids(oTable: &mut OTable, Relation rel, const newrnode: &mut RelFileNode, bool drop_pkey)
{
	Relation	toastRel,
				indexRel;
	pub static mut I: std::os::raw::c_int = 0;

	oTable->oids.datoid = MyDatabaseId;
	oTable->oids.reloid = rel->rd_id;
	oTable->oids.relnode = RelFileNodeGetNode(newrnode);

	if (rel->rd_rel->reltoastrelid)
	{
		toastRel = table_open(rel->rd_rel->reltoastrelid, AccessShareLock);
		ORelOidsSetFromRel(oTable->toast_oids, toastRel);
		table_close(toastRel, AccessShareLock);
	}
	else
	{
		// Parent partition can't have toast_oids
		ORelOidsSetInvalid(oTable->toast_oids);
	}
	if (oTable->index_bridging)
	{
		oTable->bridge_oids.datoid = MyDatabaseId;
		oTable->bridge_oids.relnode = GetNewRelFileNumber(MyDatabaseTableSpace, NULL,
														  rel->rd_rel->relpersistence);
		oTable->bridge_oids.reloid = oTable->bridge_oids.relnode;
	}

	for (i = 0; i < oTable->nindices; i++)
	{
		//
// There is a memmove in drop_primary_index, and also when dropping
// pkey for partition tables it calls this function after removing
// index from system catalogs
//
		if (!drop_pkey || oTable->indices[i].type != oIndexPrimary)
		{
			indexRel = relation_open(oTable->indices[i].oids.reloid, AccessShareLock);
			ORelOidsSetFromRel(oTable->indices[i].oids, indexRel);
			relation_close(indexRel, AccessShareLock);
		}
	}
}

static mut RECOVERY_NUM_O_TABLES_META_LOCKS: std::os::raw::c_int = 0;


o_tables_meta_lock()
{
	if (!is_recovery_process())
	{
		Assert(!LWLockHeldByMe(&checkpoint_state->oTablesMetaLock));
		LWLockAcquire(&checkpoint_state->oTablesMetaLock, LW_SHARED);

		// Make sure we've acquired oxid
		() get_current_oxid();
		add_o_tables_meta_lock_wal_record();
	}
	else
	{
		if (recovery_num_o_tables_meta_locks++ == 0)
			LWLockAcquire(&checkpoint_state->oTablesMetaLock, LW_SHARED);
	}
}


o_tables_meta_lock_no_wal()
{
	if (!is_recovery_process())
	{
		LWLockAcquire(&checkpoint_state->oTablesMetaLock, LW_SHARED);
	}
	else
	{
		if (recovery_num_o_tables_meta_locks++ == 0)
			LWLockAcquire(&checkpoint_state->oTablesMetaLock, LW_SHARED);
	}
}

//
// Release oTablesMetaLock and WAL-log the information required to replay
// DDL changes.
//

o_tables_meta_unlock(ORelOids oids, Oid oldRelnode)
{
	if (!is_recovery_process())
	{
		add_o_tables_meta_unlock_wal_record(oids, oldRelnode);
		() flush_local_wal(false, false);

		LWLockRelease(&checkpoint_state->oTablesMetaLock);
	}
	else
	{
		if (--recovery_num_o_tables_meta_locks == 0)
			LWLockRelease(&checkpoint_state->oTablesMetaLock);
	}
}


o_tables_meta_unlock_no_wal()
{
	if (!is_recovery_process())
	{
		LWLockRelease(&checkpoint_state->oTablesMetaLock);
	}
	else
	{
		if (--recovery_num_o_tables_meta_locks == 0)
			LWLockRelease(&checkpoint_state->oTablesMetaLock);
	}
}