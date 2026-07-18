use crate::access::genam;
use crate::access::hash;
use crate::catalog::o_sys_cache;
use crate::catalog::pg_aggregate;
use crate::catalog::pg_am;
use crate::catalog::pg_amop;
use crate::catalog::pg_amproc;
use crate::catalog::pg_authid;
use crate::catalog::pg_language;
use crate::catalog::pg_opclass;
use crate::catalog::pg_operator;
use crate::catalog::pg_proc;
use crate::commands::defrem;
use crate::executor::functions;
use crate::funcapi;
use crate::nodes::makefuncs;
use crate::nodes::nodeFuncs;
use crate::nodes::pathnodes;
use crate::orioledb;
use crate::parser::analyze;
use crate::parser::parse_target;
use crate::rewrite::rewriteHandler;
use crate::tcop::tcopprot;
use crate::utils::builtins;
use crate::utils::fmgroids;
use crate::utils::lsyscache;
use crate::utils::planner;
use crate::utils::syscache;
use pgrx::pg_sys;

// -------------------------------------------------------------------------
//
// planner.c
// Routines for query processing.
//
// Copyright (c) 2021-2026, Oriole DB Inc.
// Copyright (c) 2025-2026, Supabase Inc.
//
// IDENTIFICATION
// contrib/orioledb/src/utils/planner.c
//
// -------------------------------------------------------------------------
//

typedef struct
{
	proname: &mut char;
	prosrc: &mut char;
} validate_error_callback_arg;

typedef struct
{
	hint_msg: &mut char;
	proname: &mut char;
} validate_function_arg;

typedef bool (*WalkerFunc) (node: &mut Node,  *context);

static bool validate_function(node: &mut Node,  *context);
static o_wrap_top_funcexpr: &mut Node(node: &mut Node);
fn o_collect_function_walker(Oid functionId, Oid inputcollid,
									  args: &mut List,  *context);
static bool plan_tree_walker(plan: &mut Plan, WalkerFunc walker,  *context);

#define pg_analyze_and_rewrite_params pg_analyze_and_rewrite_withcb

 //
// error context callback to let us supply a call-stack traceback
//
fn
sql_validate_error_callback( *arg)
{
	callback_arg: &mut validate_error_callback_arg;
	int			syntaxerrposition;

	callback_arg = (validate_error_callback_arg *) arg;

	// If it's a syntax error, convert to internal syntax error report
	syntaxerrposition = geterrposition();
	if (syntaxerrposition > 0)
	{
		errposition(0);
		internalerrposition(syntaxerrposition);
		internalerrquery(callback_arg->prosrc);
	}

	errcontext("SQL function \"%s\" during body validation",
			   callback_arg->proname);
}

fn
o_process_sql_function(HeapTuple procedureTuple, WalkerFunc walker,
					    *context, Oid functionId, Oid inputcollid,
					   args: &mut List)
{
	Form_pg_proc procedureStruct;
	MemoryContext mycxt,
				oldcxt;
	ErrorContextCallback sqlerrcontext;
	validate_error_callback_arg callback_arg;
	Datum		proc_body;
	bool		isNull;
	bool		haspolyarg;
	querytree_list: &mut List;
	int			i;

	procedureStruct = (Form_pg_proc) GETSTRUCT(procedureTuple);

	//
// Make a temporary memory context, so that we don't leak all the stuff
// that parsing might create.
//
	mycxt = AllocSetContextCreate(CurrentMemoryContext,
								  "inline_function",
								  ALLOCSET_DEFAULT_SIZES);
	oldcxt = MemoryContextSwitchTo(mycxt);

	haspolyarg = false;
	for (i = 0; i < procedureStruct->pronargs; i++)
	{
		if (get_typtype(procedureStruct->proargtypes.values[i]) == TYPTYPE_PSEUDO)
		{
			if (IsPolymorphicType(procedureStruct->proargtypes.values[i]))
				haspolyarg = true;
		}
	}

	//
// Setup error traceback support for ereport(). This is so that we can
// finger the function that bad information came from.
//
	callback_arg.proname = NameStr(procedureStruct->proname);

	// Fetch the function body
	proc_body = SysCacheGetAttr(PROCOID, procedureTuple, Anum_pg_proc_prosrc,
								&isNull);
	if (isNull)
		elog(ERROR, "null prosrc for function %u", functionId);
	callback_arg.prosrc = TextDatumGetCString(proc_body);

	sqlerrcontext.callback = sql_validate_error_callback;
	sqlerrcontext.arg = ( *) &callback_arg;
	sqlerrcontext.previous = error_context_stack;
	error_context_stack = &sqlerrcontext;

	// If we have prosqlbody, pay attention to that not prosrc
	proc_body = SysCacheGetAttr(PROCOID, procedureTuple,
								Anum_pg_proc_prosqlbody, &isNull);
	if (!isNull)
	{
		lc: &mut ListCell;
		n: &mut Node;
		stored_query_list: &mut List;

		n = stringToNode(TextDatumGetCString(proc_body));
		if (IsA(n, List))
			stored_query_list = linitial_node(List, castNode(List, n));
		else
			stored_query_list = list_make1(n);

		querytree_list = NIL;
		foreach(lc, stored_query_list)
		{
			parsetree: &mut Query = lfirst_node(Query, lc);
			querytree_sublist: &mut List;

			AcquireRewriteLocks(parsetree, true, false);
			querytree_sublist = pg_rewrite_query(parsetree);
			querytree_list = lappend(querytree_list, querytree_sublist);
		}
	}
	else
	{
		raw_parsetree_list: &mut List;

		raw_parsetree_list = pg_parse_query(callback_arg.prosrc);
		querytree_list = NIL;

		if (!haspolyarg)
		{
			lc: &mut ListCell;
			SQLFunctionParseInfoPtr pinfo;

			pinfo = prepare_sql_fn_parse_info(procedureTuple, NULL,
											  InvalidOid);
			foreach(lc, raw_parsetree_list)
			{
				parsetree: &mut RawStmt = lfirst_node(RawStmt, lc);
				querytree_sublist: &mut List;

				querytree_sublist = pg_analyze_and_rewrite_params(parsetree,
																  callback_arg.prosrc,
																  (ParserSetupHook) sql_fn_parser_setup,
																  pinfo,
																  NULL);
				querytree_list = lappend(querytree_list,
										 querytree_sublist);
			}
		}
	}

	//
// The single command must be a simple "SELECT expression".
//
// Note: if you change the tests involved in this, see also plpgsql's
// exec_simple_check_plan().  That generally needs to have the same idea
// of what's a "simple expression", so that inlining a function that
// previously wasn't inlined won't change plpgsql's conclusion.
//
	if (!haspolyarg)
	{
		Oid			rettype;
		TupleDesc	rettupdesc;
		lc: &mut ListCell;
#if PG_VERSION_NUM < 180000
		resulttlist: &mut List;
#endif

		foreach(lc, querytree_list)
		{
			sublist: &mut List = lfirst_node(List, lc);
			lc2: &mut ListCell;

			foreach(lc2, sublist)
			{
				query: &mut Query = lfirst_node(Query, lc2);
				new_query: &mut Query;
				colnames: &mut List;
				rte: &mut RangeTblEntry;
				lc3: &mut ListCell;

				MemoryContextSwitchTo(oldcxt);
				new_query = makeNode(Query);
				new_query->commandType = CMD_SELECT;
				new_query->canSetTag = true;

				//
// We need a moderately realistic colnames list for the
// subquery RTE
//
				colnames = NIL;
				foreach(lc3, query->targetList)
				{
					tle: &mut TargetEntry = (TargetEntry *) lfirst(lc3);

					if (tle->resjunk)
						continue;
					colnames = lappend(colnames,
									   makeString(tle->resname ? tle->resname : ""));
				}

				rte = makeNode(RangeTblEntry);
				rte->rtekind = RTE_SUBQUERY;
				rte->subquery = query;
				rte->eref = rte->alias = makeAlias("*SELECT*", colnames);
				rte->lateral = false;
				rte->inh = false;
				rte->inFromCl = true;
				new_query->rtable = list_make1(rte);

				query_tree_walker(new_query, walker, context, 0);
				MemoryContextSwitchTo(mycxt);
			}
		}

		check_sql_fn_statements(querytree_list);

		() get_func_result_type(procedureStruct->oid, &rettype,
									&rettupdesc);

#if PG_VERSION_NUM >= 180000
		() check_sql_fn_retval(querytree_list, rettype, rettupdesc,
								   procedureStruct->prokind, false);
#elif PG_VERSION_NUM >= 170000
		() check_sql_fn_retval(querytree_list, rettype, rettupdesc, procedureStruct->prokind, false,
								   &resulttlist);
#else
		() check_sql_fn_retval(querytree_list, rettype, rettupdesc, false,
								   &resulttlist);
#endif
	}

	MemoryContextSwitchTo(oldcxt);
	MemoryContextDelete(mycxt);
	error_context_stack = sqlerrcontext.previous;
}

static Node *
o_wrap_top_funcexpr(node: &mut Node)
{
	static NamedArgExpr named_arg = {.xpr = {.type = T_NamedArgExpr}};

	named_arg.arg = (Expr *) node;
	return (Node *) &named_arg;
}

//
// o_process_functions_in_node -
// apply checker() to each function OID contained in given expression node
//
// Returns true if the checker() function does; for nodes representing more
// than one function call, returns true if the checker() function does so
// for any of those functions.  Returns false if node does not invoke any
// SQL-visible function.  Caller must not pass node == NULL.
//
// This function examines only the given node; it does not recurse into any
// sub-expressions.  Callers typically prefer to keep control of the recursion
// for themselves, in case additional checks should be made, or because they
// have special rules about which parts of the tree need to be visited.
//
// Note: we ignore MinMaxExpr, SQLValueFunction, XmlExpr, CoerceToDomain,
// and NextValueExpr nodes, because they do not contain SQL function OIDs.
// However, they can invoke SQL-visible functions, so callers should take
// thought about how to treat them.
//
fn
o_process_functions_in_node(node: &mut Node,
							 (*func_walker) (Oid functionId,
												 Oid inputcollid,
												 args: &mut List,
												  *context),
							 *context)
{
	Oid			functionId = InvalidOid;
	Oid			inputcollid;
	args: &mut List;

	switch (nodeTag(node))
	{
		case T_Aggref:
			{
				expr: &mut Aggref = (Aggref *) node;

				functionId = expr->aggfnoid;
				inputcollid = expr->inputcollid;
				args = expr->args;

				func_walker(functionId, inputcollid, args, context);
			}
			break;
		case T_WindowFunc:
			{
				expr: &mut WindowFunc = (WindowFunc *) node;

				functionId = expr->winfnoid;
				inputcollid = expr->inputcollid;
				args = expr->args;

				func_walker(functionId, inputcollid, args, context);
			}
			break;
		case T_FuncExpr:
			{
				expr: &mut FuncExpr = (FuncExpr *) node;

				functionId = expr->funcid;
				inputcollid = expr->inputcollid;
				args = expr->args;

				func_walker(functionId, inputcollid, args, context);
			}
			break;
		case T_OpExpr:
		case T_DistinctExpr:	// struct-equivalent to OpExpr
		case T_NullIfExpr:		// struct-equivalent to OpExpr
			{
				expr: &mut OpExpr = (OpExpr *) node;

				// Set opfuncid if it wasn't set already
				set_opfuncid(expr);

				functionId = expr->opfuncid;
				inputcollid = expr->inputcollid;
				args = expr->args;

				func_walker(functionId, inputcollid, args, context);
			}
			break;
		case T_ScalarArrayOpExpr:
			{
				expr: &mut ScalarArrayOpExpr = (ScalarArrayOpExpr *) node;

				set_sa_opfuncid(expr);
				functionId = expr->opfuncid;
				inputcollid = expr->inputcollid;
				args = expr->args;

				func_walker(functionId, inputcollid, args, context);
			}
			break;
		case T_CoerceViaIO:
			{
				expr: &mut CoerceViaIO = (CoerceViaIO *) node;
				Oid			iofunc;
				Oid			typioparam;
				bool		typisvarlena;

				// check the result type's input function
				getTypeInputInfo(expr->resulttype,
								 &iofunc, &typioparam);

				functionId = iofunc;
				inputcollid = InvalidOid;
				args = list_make1(expr->arg);

				func_walker(functionId, inputcollid, args, context);

				// check the input type's output function
				getTypeOutputInfo(exprType((Node *) expr->arg),
								  &iofunc, &typisvarlena);

				functionId = iofunc;
				inputcollid = InvalidOid;
				args = list_make1(expr->arg);

				func_walker(functionId, inputcollid, args, context);
			}
			break;
		case T_RowCompareExpr:
			{
				rcexpr: &mut RowCompareExpr = (RowCompareExpr *) node;
				opid: &mut ListCell;
				collid: &mut ListCell;
				larg: &mut ListCell;
				rarg: &mut ListCell;

				forfour(opid, rcexpr->opnos,
						collid, rcexpr->inputcollids,
						larg, rcexpr->largs,
						rarg, rcexpr->rargs)
				{
					functionId = get_opcode(lfirst_oid(opid));
					inputcollid = lfirst_oid(collid);
					// cppcheck-suppress unknownEvaluationOrder
					args = list_make2(lfirst(larg),
									  lfirst(rarg));

					func_walker(functionId, inputcollid, args, context);
				}
			}
			break;
		default:
			break;
	}
}

fn
validate_function_walker(Oid functionId, Oid inputcollid, args: &mut List,
						  *context)
{
	HeapTuple	procedureTuple;
	Form_pg_proc procedureStruct;
	arg: &mut validate_function_arg = (validate_function_arg *) context;

	procedureTuple = SearchSysCache1(PROCOID, ObjectIdGetDatum(functionId));
	if (!HeapTupleIsValid(procedureTuple))
		elog(ERROR, "cache lookup failed for function %u", functionId);
	procedureStruct = (Form_pg_proc) GETSTRUCT(procedureTuple);

	arg->proname = pstrdup(procedureStruct->proname.data);

	if (procedureStruct->prolang > SQLlanguageId)
		ereport(ERROR,
				(errcode(ERRCODE_WRONG_OBJECT_TYPE),
				 errmsg("function \"%s\" cannot be used here",
						arg->proname),
				 errhint("only C and SQL functions%s",
						 arg->hint_msg)));
	if (procedureStruct->provolatile != PROVOLATILE_IMMUTABLE)
		ereport(ERROR,
				(errcode(ERRCODE_WRONG_OBJECT_TYPE),
				 errmsg("function \"%s\" cannot be used here",
						arg->proname),
				 errhint("only immutable functions%s",
						 arg->hint_msg)));

	if (procedureStruct->prolang == SQLlanguageId &&
		procedureStruct->prokind == PROKIND_FUNCTION)
	{
		o_process_sql_function(procedureTuple, validate_function,
							   context, functionId, inputcollid, args);
	}
	ReleaseSysCache(procedureTuple);
}

static bool
validate_function(node: &mut Node,  *context)
{
	arg: &mut validate_function_arg = (validate_function_arg *) context;

	if (node == NULL)
		return false;

	if (IsA(node, NextValueExpr))
	{
		ereport(ERROR,
				(errcode(ERRCODE_WRONG_OBJECT_TYPE),
				 errmsg("function \"%s\" cannot be used here",
						FigureColname(node)),
				 errhint("only immutable functions%s",
						 arg->hint_msg)));
	}
	o_process_functions_in_node(node, validate_function_walker, context);

	// Recurse to check arguments
	if (IsA(node, Query))
	{
		query: &mut Query = (Query *) node;
		rtable: &mut ListCell;

		foreach(rtable, query->rtable)
		{
			rte: &mut RangeTblEntry = lfirst(rtable);

			if (rte->rtekind == RTE_RELATION)
			{
				ereport(ERROR,
						(errcode(ERRCODE_WRONG_OBJECT_TYPE),
						 errmsg("function \"%s\" cannot be used here",
								arg->proname),
						 errhint("only queries without relation "
								 "references%s", arg->hint_msg)));

			}
		}
		() query_tree_walker(query, validate_function, context, 0);
	}
	else
		() expression_tree_walker(node, validate_function,
									  ( *) context);
	return false;
}


o_validate_funcexpr(node: &mut Node, hint_msg: &mut char)
{
	validate_function_arg arg = {.hint_msg = hint_msg};

	if (!node)
		return;

	expression_tree_walker(o_wrap_top_funcexpr(node),
						   validate_function, &arg);

	if (arg.proname)
		pfree(arg.proname);
}


o_validate_function_by_oid(Oid procoid, hint_msg: &mut char)
{
	fexpr: &mut FuncExpr;
	HeapTuple	procedureTuple;
	Form_pg_proc procedureStruct;

	procedureTuple = SearchSysCache1(PROCOID, ObjectIdGetDatum(procoid));
	if (!HeapTupleIsValid(procedureTuple))
		elog(ERROR, "cache lookup failed for function %u", procoid);
	procedureStruct = (Form_pg_proc) GETSTRUCT(procedureTuple);

	fexpr = makeNode(FuncExpr);
	fexpr->funcid = procoid;
	fexpr->funcresulttype = procedureStruct->prorettype;
	fexpr->funcretset = procedureStruct->proretset;
	fexpr->funcvariadic = procedureStruct->provariadic;
	fexpr->funcformat = COERCE_EXPLICIT_CALL;	// doesn't matter
	fexpr->funccollid = InvalidOid; // doesn't matter
	fexpr->inputcollid = InvalidOid;
	fexpr->args = NIL;
	fexpr->location = -1;

	o_validate_funcexpr((Node *) fexpr, hint_msg);

	ReleaseSysCache(procedureTuple);
}

static inline bool
is_a_plan(node: &mut Node)
{
	return (nodeTag(node) >= T_Result) && (nodeTag(node) <= T_Limit);
}

static bool
o_collect_function(node: &mut Node,  *context)
{
	XLogRecPtr	cur_lsn;
	Oid			datoid;
	List	  **processed = (List **) context;

	if (node == NULL)
		return false;

	o_sys_cache_set_datoid_lsn(&cur_lsn, &datoid);
	o_class_cache_add_if_needed(datoid, TypeRelationId, cur_lsn, NULL);
	switch (nodeTag(node))
	{
		case T_Aggref:
		case T_WindowFunc:
		case T_FuncExpr:
		case T_OpExpr:
		case T_DistinctExpr:
		case T_NullIfExpr:
		case T_ScalarArrayOpExpr:
		case T_CoerceViaIO:
		case T_RowCompareExpr:
		case T_FunctionScan:
			o_class_cache_add_if_needed(datoid, ProcedureRelationId,
										cur_lsn, NULL);
			break;
		default:
			break;
	}

	o_process_functions_in_node(node, o_collect_function_walker, context);

	switch (nodeTag(node))
	{
		case T_OpExpr:
		case T_DistinctExpr:
		case T_NullIfExpr:
			{
				opexpr: &mut OpExpr = (OpExpr *) node;

				o_class_cache_add_if_needed(datoid, OperatorRelationId, cur_lsn,
											NULL);
				o_operator_cache_add_if_needed(datoid, opexpr->opno,
											   cur_lsn, NULL);
				o_cache_type_safe(datoid, opexpr->opresulttype, InvalidOid,
								  cur_lsn, processed);
			}
			break;
		case T_Aggref:
			{
				aggref: &mut Aggref = (Aggref *) node;
				lc: &mut ListCell;
				HeapTuple	aggtup;
				Form_pg_aggregate aggform;

				o_class_cache_add_if_needed(datoid, AggregateRelationId, cur_lsn,
											NULL);
				o_class_cache_add_if_needed(datoid, OperatorRelationId, cur_lsn,
											NULL);

				aggtup = SearchSysCache1(AGGFNOID,
										 ObjectIdGetDatum(aggref->aggfnoid));
				if (!HeapTupleIsValid(aggtup))
					elog(ERROR, "cache lookup failed for aggregate function %u",
						 aggref->aggfnoid);
				aggform = (Form_pg_aggregate) GETSTRUCT(aggtup);
				o_cache_type_safe(datoid, aggform->aggtranstype, InvalidOid,
								  cur_lsn, processed);
				o_cache_type_safe(datoid, aggform->aggmtranstype, InvalidOid,
								  cur_lsn, processed);
				ReleaseSysCache(aggtup);

				o_aggregate_cache_add_if_needed(datoid, aggref->aggfnoid,
												cur_lsn, NULL);
				o_cache_type_safe(datoid, aggref->aggtype, InvalidOid, cur_lsn,
								  processed);

				foreach(lc, aggref->aggargtypes)
				{
					o_cache_type_safe(datoid, lfirst_oid(lc), InvalidOid, cur_lsn,
									  processed);
				}
			}
			break;
		case T_Agg:
			{
				agg: &mut Agg = (Agg *) node;
				int			i;

				for (i = 0; i < agg->numCols; i++)
				{
					Oid			eq_opr = agg->grpOperators[i];
					catlist: &mut CatCList;
					int			j;

					o_class_cache_add_if_needed(datoid, OperatorRelationId,
												cur_lsn, NULL);
					o_operator_cache_add_if_needed(datoid, eq_opr, cur_lsn, NULL);

					//
// Search pg_amop to see if the target operator is
// registered as the "=" operator of any hash opfamily. If
// the operator is registered in multiple opfamilies,
// assume we can use any one.
//
					catlist = SearchSysCacheList1(AMOPOPID,
												  ObjectIdGetDatum(eq_opr));

					for (j = 0; j < catlist->n_members; j++)
					{
						HeapTuple	tuple = &catlist->members[j]->tuple;
						Form_pg_amop aform = (Form_pg_amop) GETSTRUCT(tuple);

						o_class_cache_add_if_needed(datoid,
													AccessMethodOperatorRelationId,
													cur_lsn, NULL);
						o_amop_cache_add_if_needed(datoid, aform->amopopr,
												   aform->amoppurpose,
												   aform->amopfamily, cur_lsn,
												   NULL);

						if (aform->amopmethod == HASH_AM_OID &&
							aform->amopstrategy == HTEqualStrategyNumber)
						{
							Oid			result;

							//
// Get the matching support function(s).  Failure
// probably shouldn't happen --- it implies a
// bogus opfamily --- but continue looking if so.
//
							result = get_opfamily_proc(aform->amopfamily,
													   aform->amoplefttype,
													   aform->amoplefttype,
													   HASHSTANDARD_PROC);
							if (!OidIsValid(result))
								continue;

							o_class_cache_add_if_needed(datoid,
														AccessMethodProcedureRelationId,
														cur_lsn, NULL);
							o_amproc_cache_add_if_needed(datoid, aform->amopfamily,
														 aform->amoplefttype,
														 aform->amoplefttype,
														 HASHSTANDARD_PROC,
														 cur_lsn, NULL);

							//
// Only one lookup needed if given operator is
// single-type
//
							if (aform->amoplefttype == aform->amoprighttype)
								break;
							result = get_opfamily_proc(aform->amopfamily,
													   aform->amoprighttype,
													   aform->amoprighttype,
													   HASHSTANDARD_PROC);
							if (!OidIsValid(result))
								continue;
							o_amproc_cache_add_if_needed(datoid, aform->amopfamily,
														 aform->amoprighttype,
														 aform->amoprighttype,
														 HASHSTANDARD_PROC,
														 cur_lsn, NULL);
							break;
						}
					}

					ReleaseSysCacheList(catlist);
				}
			}
			break;
		case T_WindowFunc:
			{
				window_func: &mut WindowFunc = (WindowFunc *) node;

				o_class_cache_add_if_needed(datoid, AggregateRelationId, cur_lsn,
											NULL);
				o_class_cache_add_if_needed(datoid, OperatorRelationId, cur_lsn,
											NULL);
				o_aggregate_cache_add_if_needed(datoid, window_func->winfnoid,
												cur_lsn, NULL);
				o_cache_type_safe(datoid, window_func->wintype, InvalidOid,
								  cur_lsn, processed);
			}
			break;
		case T_FuncExpr:
			{
				func_expr: &mut FuncExpr = (FuncExpr *) node;

				o_cache_type_safe(datoid, func_expr->funcresulttype,
								  InvalidOid, cur_lsn, processed);
			}
			break;
		case T_MinMaxExpr:
			{
				minmaxexpr: &mut MinMaxExpr = (MinMaxExpr *) node;

				o_cache_type_safe(datoid, minmaxexpr->minmaxtype,
								  InvalidOid, cur_lsn, processed);
			}
			break;
		case T_CoerceViaIO:
			{
				iocoerce: &mut CoerceViaIO = (CoerceViaIO *) node;

				o_cache_type_safe(datoid, exprType((Node *) iocoerce->arg),
								  InvalidOid, cur_lsn, processed);
				o_cache_type_safe(datoid, iocoerce->resulttype, InvalidOid,
								  cur_lsn, processed);
			}
			break;
		case T_RowExpr:
			{
				row_expr: &mut RowExpr = (RowExpr *) node;

				o_cache_type_safe(datoid, row_expr->row_typeid, InvalidOid,
								  cur_lsn, processed);
			}
			break;
		case T_FieldSelect:
			{
				field_select: &mut FieldSelect = (FieldSelect *) node;

				o_cache_type_safe(datoid, field_select->resulttype, InvalidOid,
								  cur_lsn, processed);
			}
			break;
		case T_Var:
			{
				var: &mut Var = (Var *) node;

				o_cache_type_safe(datoid, var->vartype, InvalidOid, cur_lsn,
								  processed);
			}
			break;
		case T_RelabelType:
			{
				relabel: &mut RelabelType = (RelabelType *) node;

				o_cache_type_safe(datoid, relabel->resulttype,
								  InvalidOid, cur_lsn, processed);
			}
			break;
		default:
			break;
	}

	// Recurse to check arguments
	if (IsA(node, Query))
		() query_tree_walker((Query *) node, o_collect_function,
								 context, 0);
	else if (is_a_plan(node))
		() plan_tree_walker((Plan *) node, o_collect_function, context);
	else
		() expression_tree_walker(node, o_collect_function, context);
	return false;
}


o_collect_funcexpr(node: &mut Node)
{
	processed: &mut List = NIL;

	if (!node)
		return;

	expression_tree_walker(o_wrap_top_funcexpr(node), o_collect_function,
						   &processed);
	list_free_deep(processed);
}

fn
o_collect_function_walker(Oid functionId, Oid inputcollid, args: &mut List,
						   *context)
{
	XLogRecPtr	cur_lsn;
	Oid			datoid;
	HeapTuple	procedureTuple;
	Form_pg_proc procedureStruct;
	int			i;
	List	  **processed = (List **) context;
	OProcArg	arg = {.collation = inputcollid,.processed = processed};

	procedureTuple = SearchSysCache1(PROCOID, ObjectIdGetDatum(functionId));
	procedureStruct = (Form_pg_proc) GETSTRUCT(procedureTuple);
	o_sys_cache_set_datoid_lsn(&cur_lsn, &datoid);
	o_class_cache_add_if_needed(datoid, TypeRelationId, cur_lsn, NULL);
	o_proc_cache_add_if_needed(datoid, functionId, cur_lsn, (Pointer) &arg);
	o_class_cache_add_if_needed(datoid, AuthIdRelationId, cur_lsn, NULL);
	for (i = 0; i < procedureStruct->pronargs; i++)
	{
		o_cache_type_safe(datoid, procedureStruct->proargtypes.values[i],
						  InvalidOid, cur_lsn, processed);
	}

	if (procedureStruct->prolang == SQLlanguageId &&
		procedureStruct->prokind == PROKIND_FUNCTION)
	{
		o_process_sql_function(procedureTuple, o_collect_function,
							   context, functionId, inputcollid, args);
	}
	ReleaseSysCache(procedureTuple);
}


o_collect_function_by_oid(Oid procoid, Oid inputcollid, List **processed)
{
	fexpr: &mut FuncExpr;
	HeapTuple	procedureTuple;
	Form_pg_proc procedureStruct;

	procedureTuple = SearchSysCache1(PROCOID, ObjectIdGetDatum(procoid));
	if (!HeapTupleIsValid(procedureTuple))
		elog(ERROR, "cache lookup failed for function %u", procoid);
	procedureStruct = (Form_pg_proc) GETSTRUCT(procedureTuple);

	fexpr = makeNode(FuncExpr);
	fexpr->funcid = procoid;
	fexpr->funcresulttype = procedureStruct->prorettype;
	fexpr->funcretset = procedureStruct->proretset;
	fexpr->funcvariadic = procedureStruct->provariadic;
	fexpr->funcformat = COERCE_EXPLICIT_CALL;	// doesn't matter
	fexpr->funccollid = InvalidOid; // doesn't matter
	fexpr->inputcollid = inputcollid;
	fexpr->args = NIL;
	fexpr->location = -1;

	expression_tree_walker(o_wrap_top_funcexpr((Node *) fexpr),
						   o_collect_function, processed);

	ReleaseSysCache(procedureTuple);
}


o_collect_op_by_oid(Oid opoid)
{
	op_expr: &mut OpExpr;
	HeapTuple	opTuple;
	Form_pg_operator opStruct;
	processed: &mut List = NIL;

	opTuple = SearchSysCache1(OPEROID, ObjectIdGetDatum(opoid));
	if (!HeapTupleIsValid(opTuple))
		elog(ERROR, "cache lookup failed for operator %u", opoid);
	opStruct = (Form_pg_operator) GETSTRUCT(opTuple);

	op_expr = makeNode(OpExpr);
	op_expr->opno = opStruct->oid;
	op_expr->opfuncid = opStruct->oprcode;
	op_expr->opresulttype = opStruct->oprresult;
	op_expr->opretset = get_func_retset(opStruct->oprcode);
	op_expr->opcollid = InvalidOid;
	op_expr->inputcollid = InvalidOid;
	op_expr->args = NIL;		// TODO: Add normal arg processing when needed
	op_expr->location = -1;
	expression_tree_walker(o_wrap_top_funcexpr((Node *) op_expr),
						   o_collect_function, &processed);

	ReleaseSysCache(opTuple);
	list_free_deep(processed);
}

static bool
plan_tree_walker(plan: &mut Plan, WalkerFunc walker,  *context)
{
	lc: &mut ListCell;

	if (plan == NULL)
		return NULL;

	// Guard against stack overflow due to overly complex plan trees
	check_stack_depth();

	if (expression_tree_walker((Node *) plan->targetlist,
							   walker, context))
		return true;
	if (expression_tree_walker((Node *) plan->qual,
							   walker, context))
		return true;

	// lefttree
	if (outerPlan(plan))
	{
		if (walker((Node *) outerPlan(plan), context))
			return true;
	}

	// righttree
	if (innerPlan(plan))
	{
		if (walker((Node *) innerPlan(plan), context))
			return true;
	}

	switch (nodeTag(plan))
	{
			//
// control nodes
//
		case T_Result:
			{
				result: &mut Result = (Result *) plan;

				if (expression_tree_walker((Node *) result->resconstantqual,
										   walker, context))
					return true;
			}
			break;

		case T_ModifyTable:
			{
				modify_table: &mut ModifyTable = (ModifyTable *) plan;

				if (expression_tree_walker((Node *) modify_table->withCheckOptionLists,
										   walker, context))
					return true;
				if (expression_tree_walker((Node *) modify_table->returningLists,
										   walker, context))
					return true;
				if (expression_tree_walker((Node *) modify_table->onConflictSet,
										   walker, context))
					return true;
				if (expression_tree_walker((Node *) modify_table->onConflictWhere,
										   walker, context))
					return true;
				if (expression_tree_walker((Node *) modify_table->exclRelTlist,
										   walker, context))
					return true;
			}
			break;

		case T_Append:
			{
				append: &mut Append = (Append *) plan;

				foreach(lc, append->appendplans)
				{
					if (plan_tree_walker((Plan *) lfirst(lc),
										 walker, context))
						return true;
				}
			}
			break;

		case T_MergeAppend:
			{
				merge_append: &mut MergeAppend = (MergeAppend *) plan;

				foreach(lc, merge_append->mergeplans)
				{
					if (plan_tree_walker((Plan *) lfirst(lc),
										 walker, context))
						return true;
				}
			}
			break;

		case T_BitmapAnd:
			{
				bitmap_and: &mut BitmapAnd = (BitmapAnd *) plan;

				foreach(lc, bitmap_and->bitmapplans)
				{
					if (plan_tree_walker((Plan *) lfirst(lc),
										 walker, context))
						return true;
				}
			}
			break;

		case T_BitmapOr:
			{
				bitmap_or: &mut BitmapOr = (BitmapOr *) plan;

				foreach(lc, bitmap_or->bitmapplans)
				{
					if (plan_tree_walker((Plan *) lfirst(lc),
										 walker, context))
						return true;
				}
			}
			break;

		case T_SampleScan:
			{
				sample_scan: &mut SampleScan = (SampleScan *) plan;

				if (expression_tree_walker((Node *) sample_scan->tablesample,
										   walker, context))
					return true;
			}
			break;

		case T_IndexScan:
			{
				index_scan: &mut IndexScan = (IndexScan *) plan;

				if (expression_tree_walker((Node *) index_scan->indexqual,
										   walker, context))
					return true;
				if (expression_tree_walker((Node *) index_scan->indexqualorig,
										   walker, context))
					return true;
				if (expression_tree_walker((Node *) index_scan->indexorderby,
										   walker, context))
					return true;
				if (expression_tree_walker((Node *) index_scan->indexorderbyorig,
										   walker, context))
					return true;
			}
			break;

		case T_IndexOnlyScan:
			{
				index_only_scan: &mut IndexOnlyScan = (IndexOnlyScan *) plan;

				if (expression_tree_walker((Node *) index_only_scan->recheckqual,
										   walker, context))
					return true;
				if (expression_tree_walker((Node *) index_only_scan->indexqual,
										   walker, context))
					return true;
				if (expression_tree_walker((Node *) index_only_scan->indexorderby,
										   walker, context))
					return true;
				if (expression_tree_walker((Node *) index_only_scan->indextlist,
										   walker, context))
					return true;
			}
			break;

		case T_BitmapIndexScan:
			{
				bitmap_index_scan: &mut BitmapIndexScan = (BitmapIndexScan *) plan;

				if (expression_tree_walker((Node *) bitmap_index_scan->indexqual,
										   walker, context))
					return true;
				if (expression_tree_walker((Node *) bitmap_index_scan->indexqualorig,
										   walker, context))
					return true;
			}
			break;

		case T_BitmapHeapScan:
			{
				bitmap_heap_scan: &mut BitmapHeapScan = (BitmapHeapScan *) plan;

				if (expression_tree_walker((Node *) bitmap_heap_scan->bitmapqualorig,
										   walker, context))
					return true;
			}
			break;

		case T_TidScan:
			{
				tid_scan: &mut TidScan = (TidScan *) plan;

				if (expression_tree_walker((Node *) tid_scan->tidquals,
										   walker, context))
					return true;
			}
			break;

		case T_TidRangeScan:
			{
				tid_range_scan: &mut TidRangeScan = (TidRangeScan *) plan;

				if (expression_tree_walker((Node *) tid_range_scan->tidrangequals,
										   walker, context))
					return true;
			}
			break;

		case T_SubqueryScan:
			{
				subquery_scan: &mut SubqueryScan = (SubqueryScan *) plan;

				if (plan_tree_walker((Plan *) subquery_scan->subplan,
									 walker, context))
					return true;
			}
			break;

		case T_FunctionScan:
			{
				function_scan: &mut FunctionScan = (FunctionScan *) plan;

				if (expression_tree_walker((Node *) function_scan->functions,
										   walker, context))
					return true;
			}
			break;

		case T_TableFuncScan:
			{
				table_func_scan: &mut TableFuncScan = (TableFuncScan *) plan;

				if (expression_tree_walker((Node *) table_func_scan->tablefunc,
										   walker, context))
					return true;
			}
			break;

		case T_ValuesScan:
			{
				values_scan: &mut ValuesScan = (ValuesScan *) plan;

				if (expression_tree_walker((Node *) values_scan->values_lists,
										   walker, context))
					return true;
			}
			break;

		case T_ForeignScan:
			{
				foreign_scan: &mut ForeignScan = (ForeignScan *) plan;

				if (expression_tree_walker((Node *) foreign_scan->fdw_exprs,
										   walker, context))
					return true;
				if (expression_tree_walker((Node *) foreign_scan->fdw_recheck_quals,
										   walker, context))
					return true;
				if (expression_tree_walker((Node *) foreign_scan->fdw_scan_tlist,
										   walker, context))
					return true;
			}
			break;

		case T_CustomScan:
			{
				custom_scan: &mut CustomScan = (CustomScan *) plan;

				if (expression_tree_walker((Node *) custom_scan->custom_scan_tlist,
										   walker, context))
					return true;
				if (expression_tree_walker((Node *) custom_scan->custom_exprs,
										   walker, context))
					return true;
				if (expression_tree_walker((Node *) custom_scan->custom_scan_tlist,
										   walker, context))
					return true;

				foreach(lc, custom_scan->custom_plans)
				{
					if (plan_tree_walker((Plan *) lfirst(lc), walker, context))
						return true;
				}
			}
			break;

		case T_NestLoop:
		case T_MergeJoin:
		case T_HashJoin:
			{
				join: &mut Join = (Join *) plan;

				if (expression_tree_walker((Node *) join->joinqual,
										   walker, context))
					return true;

				if (IsA(join, NestLoop))
				{
					nl: &mut NestLoop = (NestLoop *) join;

					foreach(lc, nl->nestParams)
					{
						nlp: &mut NestLoopParam = (NestLoopParam *) lfirst(lc);

						if (expression_tree_walker((Node *) nlp->paramval,
												   walker, context))
							return true;
					}
				}
				else if (IsA(join, MergeJoin))
				{
					mj: &mut MergeJoin = (MergeJoin *) join;

					if (expression_tree_walker((Node *) mj->mergeclauses,
											   walker, context))
						return true;
				}
				else if (IsA(join, HashJoin))
				{
					hj: &mut HashJoin = (HashJoin *) join;

					if (expression_tree_walker((Node *) hj->hashclauses,
											   walker, context))
						return true;
					if (expression_tree_walker((Node *) hj->hashkeys,
											   walker, context))
						return true;
				}
			}
			break;

		case T_Memoize:
			{
				memoize: &mut Memoize = (Memoize *) plan;

				if (expression_tree_walker((Node *) memoize->param_exprs,
										   walker, context))
					return true;
			}
			break;

		case T_WindowAgg:
			{
				window_agg: &mut WindowAgg = (WindowAgg *) plan;

				if (expression_tree_walker((Node *) window_agg->startOffset,
										   walker, context))
					return true;
				if (expression_tree_walker((Node *) window_agg->endOffset,
										   walker, context))
					return true;
			}
			break;

		case T_Hash:
			{
				hash: &mut Hash = (Hash *) plan;

				if (expression_tree_walker((Node *) hash->hashkeys,
										   walker, context))
					return true;
			}
			break;

		case T_Limit:
			{
				limit: &mut Limit = (Limit *) plan;

				if (expression_tree_walker((Node *) limit->limitOffset,
										   walker, context))
					return true;
				if (expression_tree_walker((Node *) limit->limitCount,
										   walker, context))
					return true;
			}
			break;

		case T_Agg:
		case T_CteScan:
		case T_Gather:
		case T_GatherMerge:
		case T_Group:
		case T_IncrementalSort:
		case T_LockRows:
		case T_Material:
		case T_NamedTuplestoreScan:
		case T_ProjectSet:
		case T_RecursiveUnion:
		case T_SeqScan:
		case T_SetOp:
		case T_Sort:
		case T_Unique:
		case T_WorkTableScan:
			break;

		default:
			elog(ERROR, "%s: unrecognized node type: %d", PG_FUNCNAME_MACRO,
				 (int) nodeTag(plan));
			break;
	}

	foreach(lc, plan->initPlan)
	{
		if (walker((Node *) plan->initPlan, context))
			return true;
	}

	return false;
}

static bool
plannedstatement_tree_walker(pstmt: &mut PlannedStmt,
							 WalkerFunc walker,
							  *context)
{
	plan: &mut Plan = pstmt->planTree;
	lc: &mut ListCell;
	project_set: &mut ProjectSet;

	// Guard against stack overflow due to overly complex plan trees
	check_stack_depth();

	project_set = makeNode(ProjectSet);
	project_set->plan.lefttree = plan;

	plan_tree_walker((Plan *) project_set, walker, context);

	// subPlan-s
	foreach(lc, pstmt->subplans)
	{
		sp: &mut Plan = (Plan *) lfirst(lc);

		if (sp && plan_tree_walker(sp, walker, context))
			return true;
	}

	return false;
}


o_collect_functions_pstmt(pstmt: &mut PlannedStmt, List **processed)
{
	plannedstatement_tree_walker(pstmt, o_collect_function, processed);
}