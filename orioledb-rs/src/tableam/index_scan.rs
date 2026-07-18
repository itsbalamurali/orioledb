use crate::access::nbtree;
use crate::access::skey;
use crate::btree::io;
use crate::btree::iterator;
use crate::commands::explain_format;
use crate::executor::nodeIndexscan;
use crate::orioledb;
use crate::parser::parse_coerce;
use crate::pgstat;
use crate::tableam::bitmap_scan;
use crate::tableam::descr;
use crate::tableam::index_scan;
use crate::tableam::key_range;
use crate::tableam::tree;
use crate::tuple::slot;
use crate::utils::datum;
use pgrx::pg_sys;

// -------------------------------------------------------------------------
//
// index_scan.c
// Routines for index scan of orioledb table
//
// Copyright (c) 2021-2026, Oriole DB Inc.
// Copyright (c) 2025-2026, Supabase Inc.
//
// IDENTIFICATION
// contrib/orioledb/src/tableam/index_scan.c
//
// -------------------------------------------------------------------------
//

#if PG_VERSION_NUM >= 180000

#endif

fn eanalyze_counter_explain(counter: &mut OEACallsCounter, label: &mut char,
									 ix_name: &mut char, es: &mut ExplainState);

#if PG_VERSION_NUM >= 180000

init_index_scan_state(o_plan_state: &mut OPlanState, ostate: &mut OScanState,
					  Relation index, econtext: &mut ExprContext, Snapshot snapshot,
					  IndexRuntimeKeyInfo **runtimeKeys, numRuntimeKeys: &mut int,
					  ScanKeyData **scanKeys, numScanKeys: &mut int)
#else

init_index_scan_state(o_plan_state: &mut OPlanState, ostate: &mut OScanState,
					  Relation index, econtext: &mut ExprContext,
					  IndexRuntimeKeyInfo **runtimeKeys, numRuntimeKeys: &mut int,
					  ScanKeyData **scanKeys, numScanKeys: &mut int)
#endif
{
	pub static mut SCAN: IndexScanDesc = std::mem::zeroed();

	ExecIndexBuildScanKeys(o_plan_state->plan_state, index, ostate->indexQuals, false, scanKeys,
						   numScanKeys, runtimeKeys, numRuntimeKeys, NULL,
						   NULL);

	scan = btbeginscan(index, *numScanKeys, 0);
	ostate->scandesc = *scan;
	pfree(scan);
	scan = &ostate->scandesc;

	scan->parallel_scan = NULL;
	scan->xs_temp_snap = false;
#if PG_VERSION_NUM >= 180000
	scan->xs_snapshot = snapshot;
#endif
}

static bool
row_key_tuple_is_valid(row_key: &mut OBtreeRowKeyBound, OTuple tup, id: &mut OIndexDescr,
					   bool low)
{
	pub static mut ROWKEYNUM: std::os::raw::c_int = 0;
	pub static mut VALID: bool = true;

	for (rowkeynum = 0; rowkeynum < row_key->nkeys; rowkeynum++)
	{
		pub static mut OB_TREE_VALUE_BOUND: *mut subkey1 = &row_key->keys[rowkeynum];
		pub static mut FLAGS: uint8 = subkey1->flags;
		pub static mut KEYNUM: std::os::raw::c_int = row_key->keynums[rowkeynum];

		if (!(flags & O_VALUE_BOUND_UNBOUNDED))
		{
			pub static mut ATTNUM: std::os::raw::c_int = 0;
			pub static mut ISNULL: bool = false;
			pub static mut VALUE: Datum = std::mem::zeroed();
			pub static mut CMP: std::os::raw::c_int = 0;
			pub static mut VALID_CMP: std::os::raw::c_int = 0;

			attnum = OIndexKeyAttnumToTupleAttnum(BTreeKeyLeafTuple,
												  id, keynum + 1);
			value = o_fastgetattr(tup, attnum, id->leafTupdesc,
								  &id->leafSpec, &isnull);
			cmp = o_idx_cmp_range_key_to_value(subkey1,
											   &id->fields[keynum],
											   value, isnull);
			if (!(flags & O_VALUE_BOUND_NULL) && isnull)
				valid = false;

			valid_cmp = low ? cmp > 0 : cmp < 0;

			if (valid_cmp)
				valid = false;
			else if (!valid_cmp && cmp != 0 && rowkeynum < row_key->nkeys - 1)
				break;
		}
		if (!valid)
			break;
	}

	pub static mut VALID: return = std::mem::zeroed();
}

static bool
is_tuple_valid(OTuple tup, id: &mut OIndexDescr, range: &mut OBTreeKeyRange,
			   BTScanOpaque so, int numPrefixExactKeys)
{
	pub static mut I: std::os::raw::c_int = 0;
	pub static mut OB_TREE_KEY_BOUND: *mut low = &range->low;
	pub static mut OB_TREE_KEY_BOUND: *mut high = &range->high;
	pub static mut VALID: bool = true;
	pub static mut KEYNUM: std::os::raw::c_int = 0;
	pub static mut BT_ARRAY_KEY_INFO: *mut arrayKeys = so->arrayKeys;

	Assert(low->nkeys == high->nkeys);

	for (i = numPrefixExactKeys + 1; valid && i < low->nkeys; i++)
	{
		int			attnum = OIndexKeyAttnumToTupleAttnum(BTreeKeyLeafTuple,
														  id, i + 1);
		pub static mut ISNULL: bool = false;
		Datum		value = o_fastgetattr(tup, attnum, id->leafTupdesc,
										  &id->leafSpec, &isnull);

		if (!(low->keys[i].flags & O_VALUE_BOUND_UNBOUNDED))
		{
			if (!(low->keys[i].flags & O_VALUE_BOUND_NULL) && isnull)
				valid = false;
			if (valid &&
				(o_idx_cmp_range_key_to_value(&low->keys[i], &id->fields[i],
											  value, isnull) > 0))
				valid = false;
		}

		if (valid && !(high->keys[i].flags & O_VALUE_BOUND_UNBOUNDED))
		{
			if (!(high->keys[i].flags & O_VALUE_BOUND_NULL) && isnull)
				valid = false;
			if (valid &&
				(o_idx_cmp_range_key_to_value(&high->keys[i], &id->fields[i],
											  value, isnull) < 0))
				valid = false;
		}
	}

	for (keynum = 0; valid && keynum < low->n_row_keys; keynum++)
	{
		if (!row_key_tuple_is_valid(&low->row_keys[keynum],
									tup, id, true))
			valid = false;
	}

	for (keynum = 0; valid && keynum < high->n_row_keys; keynum++)
	{
		if (!row_key_tuple_is_valid(&high->row_keys[keynum],
									tup, id, false))
			valid = false;
	}

	for (i = 0; i < so->numArrayKeys; i++)
	{
		pub static mut BT_ARRAY_KEY_INFO: *mut arrayKey = arrayKeys + i;
		pub static mut KEY: ScanKey = so->keyData + arrayKey->scan_key;

		Assert((key->sk_flags & SK_SEARCHARRAY) &&
			   key->sk_strategy == BTEqualStrategyNumber);

#if PG_VERSION_NUM >= 180000

		//
// Skip scan bounds are dynamic and checked earlier, no need for array
// element matches
//
		if (key->sk_flags & SK_BT_SKIP)
		{
			Assert(arrayKey->num_elems == -1);
			continue;
		}
#endif

		if (arrayKey->scan_key >= numPrefixExactKeys)
		{
			pub static mut LO: std::os::raw::c_int = 0;
			pub static mut HI: std::os::raw::c_int = arrayKey->num_elems - 1;
			pub static mut ISNULL: bool = false;
			int			attnum = OIndexKeyAttnumToTupleAttnum(BTreeKeyLeafTuple,
															  id,
															  key->sk_attno);
			Datum		value = o_fastgetattr(tup, attnum, id->leafTupdesc,
											  &id->leafSpec, &isnull);
			pub static mut FOUND: bool = false;
			pub static mut OB_TREE_VALUE_BOUND: *mut bound = &low->keys[key->sk_attno - 1];
			pub static mut O_INDEX_FIELD: *mut field = &id->fields[key->sk_attno - 1];
			pub static mut O_COMPARATOR: *mut comparator = std::ptr::null_mut();

			Assert(arrayKey->num_elems > 0);

			//
// The array elements are sorted in index-column order, so the
// membership test is a binary search rather than a linear scan.
// Pick the comparator once (out of the loop): a coercible bound
// uses the field comparator, otherwise the bound's own one.  The
// comparator orders by the datum's ascending value, so flip its
// result for a descending column to match the element order.
//
			if (o_bound_is_coercible(bound, field))
				comparator = field->comparator;
			else
				comparator = bound->comparator;

			while (lo <= hi)
			{
				int			mid = lo + (hi - lo) / 2;
				pub static mut CMP: std::os::raw::c_int = 0;

				cmp = o_call_comparator(comparator, value,
										arrayKey->elem_values[mid]);
				if (!field->ascending)
					cmp = -cmp;
				if (cmp == 0)
				{
					found = true;
					break;
				}
				else if (cmp < 0)
					hi = mid - 1;
				else
					lo = mid + 1;
			}

			if (!found)
				valid = false;
		}
	}

	pub static mut VALID: return = std::mem::zeroed();
}

#if PG_VERSION_NUM >= 180000
//
// Vendored copies of nbtree's static array-advancement primitives.
//
// Upstream PG18 (commit 92fe23d93aa) added btree skip scan via the static
// helpers _bt_array_set_low_or_high and _bt_skiparray_set_element in
// nbtutils.c.  OrioleDB drives its scan loop from key-range bounds rather
// than per-tuple btree callbacks and needs to call these primitives directly
// to reset and pin skip arrays between ranges; they are not exported via
// nbtree.h.
//
// The copies below are kept structurally identical to upstream so future
// rebases can diff them mechanically.  They only touch public fields of
// BTArrayKeyInfo / ScanKey; no nbtree internals beyond the public header.
//
fn
o_bt_array_set_low_or_high(Relation rel, ScanKey skey, array: &mut BTArrayKeyInfo,
						   bool low_not_high)
{
	Assert(skey->sk_flags & SK_SEARCHARRAY);

	if (array->num_elems != -1)
	{
		pub static mut SET_ELEM: std::os::raw::c_int = 0;

		Assert(!(skey->sk_flags & SK_BT_SKIP));

		if (!low_not_high)
			set_elem = array->num_elems - 1;

		array->cur_elem = set_elem;
		skey->sk_argument = array->elem_values[set_elem];
		return;
	}

	Assert(skey->sk_flags & SK_BT_SKIP);
	Assert(array->num_elems == -1);

	if (!array->attbyval && skey->sk_argument)
		pfree(DatumGetPointer(skey->sk_argument));

	skey->sk_argument = (Datum) 0;
	skey->sk_flags &= ~(SK_SEARCHNULL | SK_ISNULL |
						SK_BT_MINVAL | SK_BT_MAXVAL |
						SK_BT_NEXT | SK_BT_PRIOR);

	if (array->null_elem &&
		(low_not_high == ((skey->sk_flags & SK_BT_NULLS_FIRST) != 0)))
		skey->sk_flags |= (SK_SEARCHNULL | SK_ISNULL);
	else if (low_not_high)
		skey->sk_flags |= SK_BT_MINVAL;
	else
		skey->sk_flags |= SK_BT_MAXVAL;
}

//
// Pin a skip array to a concrete leading-column value from an in-flight
// tuple.  This is the orioledb counterpart of upstream's
// _bt_skiparray_set_element: it clears the MINVAL/MAXVAL/NEXT/PRIOR
// sentinel/transition flags and copies the tuple's datum into
// sk_argument so that the next o_key_data_to_key_range() call computes a
// point bound at this distinct leading value.
//
fn
o_bt_skiparray_set_element_from_tuple(ScanKey skey, array: &mut BTArrayKeyInfo,
									  Datum tupdatum, bool tupnull)
{
	Assert(skey->sk_flags & SK_BT_SKIP);
	Assert(skey->sk_flags & SK_SEARCHARRAY);
	Assert(array->num_elems == -1);

	if (tupnull)
	{
		Assert(array->null_elem);

		if (!array->attbyval && skey->sk_argument)
			pfree(DatumGetPointer(skey->sk_argument));
		skey->sk_argument = (Datum) 0;
		skey->sk_flags &= ~(SK_BT_MINVAL | SK_BT_MAXVAL |
							SK_BT_NEXT | SK_BT_PRIOR);
		skey->sk_flags |= (SK_SEARCHNULL | SK_ISNULL);
		return;
	}

	if (!array->attbyval && skey->sk_argument)
		pfree(DatumGetPointer(skey->sk_argument));
	skey->sk_flags &= ~(SK_SEARCHNULL | SK_ISNULL |
						SK_BT_MINVAL | SK_BT_MAXVAL |
						SK_BT_NEXT | SK_BT_PRIOR);
	skey->sk_argument = datumCopy(tupdatum, array->attbyval, array->attlen);
}
#endif

static bool
o_bt_advance_array_keys_increment(ostate: &mut OScanState, ScanDirection dir)
{
	pub static mut SCAN: IndexScanDesc = &ostate->scandesc;
	BTScanOpaque so = (BTScanOpaque) scan->opaque;
#if PG_VERSION_NUM >= 180000
	pub static mut REL: Relation = scan->indexRelation;
#endif
	pub static mut I: std::os::raw::c_int = 0;
	pub static mut HAVE_ARRAY_KEYS: bool = false;

	//
// We must advance the last array key most quickly, since it will
// correspond to the lowest-order index column among the available
// qualifications
//
	for (i = so->numArrayKeys - 1; i >= 0; i--)
	{
		pub static mut BT_ARRAY_KEY_INFO: *mut curArrayKey = &so->arrayKeys[i];
		pub static mut SKEY: ScanKey = &so->keyData[curArrayKey->scan_key];
		pub static mut CUR_ELEM: std::os::raw::c_int = curArrayKey->cur_elem;
		pub static mut NUM_ELEMS: std::os::raw::c_int = curArrayKey->num_elems;
		pub static mut ROLLED: bool = false;

		have_array_keys = true;

#if PG_VERSION_NUM >= 180000

		//
// Skip arrays (PG18+) drive per-distinct-value iteration. Handle them
// before the trailing-array check because numPrefixExactKeys caps
// itself at the skip array's scan_key (via
// o_adjust_num_prefix_exact_keys), so the trailing check would
// otherwise short-circuit the skip array's own advancement.
//
// Strategy: surface SK_BT_NEXT (forward) / SK_BT_PRIOR (backward) on
// the skip array's scan key instead of using SkipSupport's
// increment/decrement directly.  The flag makes
// o_key_data_to_key_range produce an exclusive bound past
// sk_argument; the iterator then probes for the next actual leading
// value present in the index, and o_iterate_index pins sk_argument to
// that tuple's value. Termination is natural: if no tuple is found
// past sk_argument (within low_compare..high_compare), the iterator
// returns NULL and the scan ends.
//
// Pre-check the relevant compare bound before setting the transition
// flag so we don't keep iterating once sk_argument has reached the
// global edge of the skip array.
//
		if (skey->sk_flags & SK_BT_SKIP)
		{
			pub static mut BOUND_KEY: ScanKey = std::mem::zeroed();
			pub static mut EXHAUSTED: bool = false;

			if (skey->sk_flags & (SK_BT_MINVAL | SK_BT_MAXVAL | SK_ISNULL))
			{
				//
// Still on the initial sentinel: either o_iterate_index never
// saw a tuple from the broad probe, or it saw only
// NULL-leading tuples (which observe_tuple does not
// transition through -- see comment there).  Either way there
// is no concrete sk_argument to advance from, so reset for
// direction reversal and report exhaustion.
//
// Critically, ISNULL must be handled here and not fall
// through to the "concrete state" path below: that path sets
// SK_BT_NEXT/PRIOR on top of sk_argument, but sentinel-ISNULL
// state has sk_argument == 0, and a subsequent
// o_key_data_to_key_range build would pass that NULL datum
// into o_fill_key_bounds -> comparator, segfaulting on
// by-reference types (text, etc.).
//
				o_bt_array_set_low_or_high(rel, skey, curArrayKey,
										   ScanDirectionIsForward(dir));
				continue;
			}

			if (skey->sk_flags & (SK_BT_NEXT | SK_BT_PRIOR))
			{
				//
// Already in transition state and the probe failed to find
// another tuple.  Reset and report exhaustion.
//
				skey->sk_flags &= ~(SK_BT_NEXT | SK_BT_PRIOR);
				o_bt_array_set_low_or_high(rel, skey, curArrayKey,
										   ScanDirectionIsForward(dir));
				continue;
			}

			bound_key = ScanDirectionIsForward(dir)
				? curArrayKey->high_compare
				: curArrayKey->low_compare;
			exhausted = false;
			if (bound_key)
			{
				//
// Strategy <= for high_compare in forward scan, >= for
// low_compare in backward scan: if sk_argument already fails
// the bound, we've walked past the skip array's global edge.
//
				if (!DatumGetBool(FunctionCall2Coll(&bound_key->sk_func,
													bound_key->sk_collation,
													skey->sk_argument,
													bound_key->sk_argument)))
					exhausted = true;
			}

			if (exhausted)
			{
				o_bt_array_set_low_or_high(rel, skey, curArrayKey,
										   ScanDirectionIsForward(dir));
				continue;
			}

			//
// Signal advancement to the next distinct value via the upstream
// NEXT/PRIOR flag mechanism.  sk_argument stays at the previous
// distinct value -- the iterator will probe past it.
//
			if (ScanDirectionIsForward(dir))
				skey->sk_flags |= SK_BT_NEXT;
			else
				skey->sk_flags |= SK_BT_PRIOR;
			pub static mut TRUE: return = std::mem::zeroed();
		}
#endif

		if (curArrayKey->scan_key >= ostate->numPrefixExactKeys)
			continue;

		if (ScanDirectionIsForward(dir) && ++cur_elem >= num_elems)
		{
			cur_elem = 0;
			rolled = true;
		}
		else if (ScanDirectionIsBackward(dir) && --cur_elem < 0)
		{
			cur_elem = num_elems - 1;
			rolled = true;
		}

		curArrayKey->cur_elem = cur_elem;
		skey->sk_argument = curArrayKey->elem_values[cur_elem];
		if (!rolled)
			pub static mut TRUE: return = std::mem::zeroed();

		// Need to advance next array key, if any
	}

	//
// The array keys are now exhausted.  (There isn't actually a distinct
// state that represents array exhaustion, since index scans don't always
// end after btgettuple returns "false".)
//
// Restore the array keys to the state they were in immediately before we
// were called.  This ensures that the arrays only ever ratchet in the
// current scan direction.  Without this, scans would overlook matching
// tuples if and when the scan's direction was subsequently reversed.
//
	if (have_array_keys)
		_bt_start_array_keys(scan, -dir);

	pub static mut FALSE: return = std::mem::zeroed();
}

#if PG_VERSION_NUM >= 180000
//
// Does the scan have any PG18 skip array?  A skip array (num_elems == -1)
// carries a dynamic range (low_compare / high_compare) that changes from one
// range to the next, so the o_key_data_update_array_key_range() shortcut --
// which only refreshes fixed array element values -- cannot represent it and
// switch_to_next_range() must rebuild the full key range instead.
//
static bool
scan_has_skip_array(BTScanOpaque so)
{
	pub static mut I: std::os::raw::c_int = 0;

	for (i = 0; i < so->numArrayKeys; i++)
		if (so->arrayKeys[i].num_elems == -1)
			pub static mut TRUE: return = std::mem::zeroed();
	pub static mut FALSE: return = std::mem::zeroed();
}
#endif

static bool
switch_to_next_range(indexDescr: &mut OIndexDescr, ostate: &mut OScanState,
					 MemoryContext tupleCxt)
{
	pub static mut OB_TREE_KEY_BOUND: *mut bound = std::ptr::null_mut();
	pub static mut SCAN: IndexScanDesc = &ostate->scandesc;
	BTScanOpaque so = (BTScanOpaque) scan->opaque;
	pub static mut OLDCONTEXT: MemoryContext = std::mem::zeroed();
	pub static mut RESULT: bool = true;

	if (!so->qual_ok)
		pub static mut FALSE: return = std::mem::zeroed();

#if PG_VERSION_NUM >= 170000

	if (so->numArrayKeys)
	{
		if (ostate->curKeyRangeIsLoaded)
		{
			result = o_bt_advance_array_keys_increment(ostate, ostate->scanDir);
		}
		else
		{
			_bt_start_array_keys(scan, ostate->scanDir);
			result = true;
		}
	}
	else
	{
		if (ostate->curKeyRangeIsLoaded)
		{
			result = false;
			so->needPrimScan = false;
			so->scanBehind = false;
			// elog(LOG, "no array keys");
		}
		else
		{
			result = true;
		}
	}
#else
	if (ostate->curKeyRangeIsLoaded)
		result = o_bt_advance_array_keys_increment(ostate, ostate->scanDir);
#endif

	if (!result)
	{
		ostate->curKeyRangeIsLoaded = true;
		pub static mut FALSE: return = std::mem::zeroed();
	}

	oldcontext = MemoryContextSwitchTo(ostate->cxt);

	if (!ostate->curKeyRangeIsLoaded
#if PG_VERSION_NUM >= 180000
	// skip arrays have dynamic ranges: rebuild fully, see comment above
		|| (so->numArrayKeys > 0 && scan_has_skip_array(so))
#endif
		)
		ostate->exact = o_key_data_to_key_range(&ostate->curKeyRange,
												so->keyData,
												so->numberOfKeys,
												(so->numArrayKeys > 0) ? so->arrayKeys : NULL,
												ostate->numPrefixExactKeys,
												indexDescr->nonLeafTupdesc->natts,
												indexDescr->fields);
	else
		o_key_data_update_array_key_range(&ostate->curKeyRange,
										  so->keyData,
										  so->numberOfKeys,
										  (so->numArrayKeys > 0) ? so->arrayKeys : NULL,
										  ostate->numPrefixExactKeys,
										  indexDescr->nonLeafTupdesc->natts,
										  indexDescr->fields);
	ostate->curKeyRangeIsLoaded = true;

	if (!ostate->exact)
	{
		bound = (ostate->scanDir == ForwardScanDirection
				 ? &ostate->curKeyRange.low
				 : &ostate->curKeyRange.high);

		// Re-use the existing iterator when possible
		if (!ostate->iterator)
			ostate->iterator = o_btree_iterator_create(&indexDescr->desc,
													   (Pointer) bound,
													   BTreeKeyBound,
													   &ostate->oSnapshot,
													   ostate->scanDir);
		else
			o_btree_iterator_advance(ostate->iterator,
									 (Pointer) bound,
									 BTreeKeyBound);
		o_btree_iterator_set_tuple_ctx(ostate->iterator, tupleCxt);
	}

#if PG_VERSION_NUM >= 180000

	//
// Mark the iterator as a probe range when any skip array is in its
// sentinel (MINVAL/MAXVAL/SearchNull-initial) or transition
// (SK_BT_NEXT/SK_BT_PRIOR) state.  o_iterate_index will use the first
// non-NULL tuple to pin sk_argument and reopen with a narrow point bound.
// If no tuple is found, the iterator simply returns NULL and the scan
// terminates.
//
	ostate->skipScanProbePending = false;
	if (so->numArrayKeys > 0)
	{
		for (int i = 0; i < so->numArrayKeys; i++)
		{
			pub static mut SKEY: ScanKey = &so->keyData[so->arrayKeys[i].scan_key];

			if ((skey->sk_flags & SK_BT_SKIP) &&
				(skey->sk_flags & (SK_BT_MINVAL | SK_BT_MAXVAL | SK_ISNULL |
								   SK_BT_NEXT | SK_BT_PRIOR)))
			{
				ostate->skipScanProbePending = true;
				break;
			}
		}
	}
#endif

	MemoryContextSwitchTo(oldcontext);

	pub static mut TRUE: return = std::mem::zeroed();
}

#if PG_VERSION_NUM >= 180000
//
// After o_iterate_index fetches the first tuple over a probe range,
// use the tuple's leading-column value to pin each prefix skip
// array's sk_argument.  The next iterator reopen with the updated
// scan-key state will build a narrow point bound on that value via
// o_key_data_to_key_range.
//
// Returns true if at least one skip array was transitioned (caller
// should reopen the iterator); false otherwise.
//
static bool
o_skip_arrays_observe_tuple(indexDescr: &mut OIndexDescr, ostate: &mut OScanState,
							OTuple tup)
{
	pub static mut SCAN: IndexScanDesc = &ostate->scandesc;
	BTScanOpaque so = (BTScanOpaque) scan->opaque;
	pub static mut TRANSITIONED: bool = false;

	if (so->numArrayKeys == 0)
		pub static mut FALSE: return = std::mem::zeroed();

	for (int i = 0; i < so->numArrayKeys; i++)
	{
		pub static mut BT_ARRAY_KEY_INFO: *mut array = &so->arrayKeys[i];
		pub static mut SKEY: ScanKey = &so->keyData[array->scan_key];
		pub static mut ATTNUM: AttrNumber = std::mem::zeroed();
		pub static mut VALUE: Datum = std::mem::zeroed();
		pub static mut ISNULL: bool = false;

		if (!(skey->sk_flags & SK_BT_SKIP))
			continue;

		// Only act on a skip array that is in sentinel or transition.
		if (!(skey->sk_flags &
			  (SK_BT_MINVAL | SK_BT_MAXVAL | SK_ISNULL |
			   SK_BT_NEXT | SK_BT_PRIOR)))
			continue;

		attnum = OIndexKeyAttnumToTupleAttnum(BTreeKeyLeafTuple, indexDescr,
											  skey->sk_attno);
		value = o_fastgetattr(tup, attnum, indexDescr->leafTupdesc,
							  &indexDescr->leafSpec, &isnull);

		//
// Don't transition on a NULL leading-column value.  Pinning
// sk_argument to NULL via set_element_from_tuple lands the skip array
// in the SK_ISNULL state, which is flag-indistinguishable from the
// initial sentinel-NULL state that _bt_start_array_keys uses for
// nulls-last + null_elem=true.  Without a way to tell those apart,
// o_key_data_to_key_range falls back to the broad range -- which
// would re-open the same iterator and re-emit the same NULL-leading
// tuple, infinite-looping.  Leave the array in its current
// (transition or sentinel) state and let the active iterator continue
// from this position, emitting any further NULL-leading tuples that
// match the trailing predicates and then exhausting.  Skip-scan
// optimization is forfeited for the NULL band, but correctness is
// preserved.
//
		if (isnull)
			continue;

		o_bt_skiparray_set_element_from_tuple(skey, array, value, isnull);
		transitioned = true;
	}

	pub static mut TRANSITIONED: return = std::mem::zeroed();
}
#endif

OTuple
o_iterate_index(indexDescr: &mut OIndexDescr, ostate: &mut OScanState,
				tupleCsn: &mut CommitSeqNo, MemoryContext tupleCxt,
				hint: &mut BTreeLocationHint)
{
	OTuple		tup = {0};
	pub static mut TUP_FETCHED: bool = false;
	pub static mut SCAN: IndexScanDesc = &ostate->scandesc;
	BTScanOpaque so = (BTScanOpaque) scan->opaque;

	if (ostate->exact || ostate->curKeyRange.empty)
	{
		if (!switch_to_next_range(indexDescr, ostate, tupleCxt))
		{
			O_TUPLE_SET_NULL(tup);
			pub static mut TUP: return = std::mem::zeroed();
		}
	}

	do
	{
		pub static mut OB_TREE_KEY_BOUND: *mut bound = std::ptr::null_mut();
		pub static mut TUP_IS_VALID: bool = true;

		if (ostate->exact)
		{
			if (hint)
				hint->blkno = OInvalidInMemoryBlkno;

			if (!so->numArrayKeys)
			{
				tup = o_btree_find_tuple_by_key(&indexDescr->desc,
												&ostate->curKeyRange.low,
												BTreeKeyBound, &ostate->oSnapshot,
												tupleCsn, tupleCxt, hint);
			}
			else
			{
				if (!ostate->iterator)
				{
					tup = o_btree_find_tuples_start(&indexDescr->desc,
													&ostate->curKeyRange.low,
													BTreeKeyBound, &ostate->oSnapshot,
													ostate->scanDir,
													tupleCsn, tupleCxt, hint,
													NULL, NULL, NULL, &ostate->iterator);
				}
				else
				{
					tup = o_btree_find_tuples_continue(ostate->iterator,
													   &ostate->curKeyRange.low,
													   BTreeKeyBound,
													   tupleCsn, hint, NULL);
				}
			}
			if (!O_TUPLE_IS_NULL(tup))
				tup_fetched = true;
		}
		else if (ostate->iterator)
		{
			bound = (ostate->scanDir == ForwardScanDirection
					 ? &ostate->curKeyRange.high : &ostate->curKeyRange.low);

			do
			{
				tup = o_btree_iterator_fetch(ostate->iterator, tupleCsn,
											 bound, BTreeKeyBound,
											 true, hint);

				if (O_TUPLE_IS_NULL(tup))
					tup_is_valid = true;
				else
				{
					tup_is_valid = is_tuple_valid(tup, indexDescr,
												  &ostate->curKeyRange,
												  so,
												  ostate->numPrefixExactKeys);
					if (tup_is_valid && indexDescr->desc.type == oIndexExclusion)
					{
						pub static mut TUPDESC: TupleDesc = std::mem::zeroed();
						pub static mut O_TUPLE_FIXED_FORMAT_SPEC: *mut spec = std::ptr::null_mut();
						int			i,
									attnum;
						pub static mut VALUE: Datum = std::mem::zeroed();
						pub static mut ISNULL: bool = false;

						tupdesc = indexDescr->leafTupdesc;
						spec = &indexDescr->leafSpec;

						for (i = 0; i < indexDescr->nKeyFields; i++)
						{
							pub static mut OB_TREE_KEY_BOUND: *mut low = &ostate->curKeyRange.low;
							pub static mut OB_TREE_VALUE_BOUND: *mut key = &low->keys[i];
							pub static mut CMP: std::os::raw::c_int = 0;

							attnum = i + 1;
							value = o_fastgetattr(tup, attnum, tupdesc, spec, &isnull);

							cmp = o_call_exclusion_fn(key->exclusion_fn, key->value, value, indexDescr->fields[i].collation);

							if (cmp != 0)
							{
								tup_is_valid = false;
								break;
							}
						}
					}
					if (tup_is_valid)
						tup_fetched = true;
				}
			} while (!tup_is_valid);
		}
		else
		{
			O_TUPLE_SET_NULL(tup);
			tup_fetched = true;
		}

#if PG_VERSION_NUM >= 180000

		//
// Probe-then-narrow.  The just-fetched tuple came from a range opened
// over one or more skip arrays in sentinel (initial broad probe) or
// transition (post-advance) state. Pin each such skip array's
// sk_argument to this tuple's leading-column value, drop the
// broad/probe iterator, and reopen with a narrow point-bound range
// for that distinct value via inline key-range rebuild.  Subsequent
// matching tuples come from the narrow iterator; when it exhausts,
// o_bt_advance_array_keys_increment sets SK_BT_NEXT (or SK_BT_PRIOR
// for backward) so the next probe range opens past sk_argument.
// Termination is natural: if a probe finds no tuple within
// low_compare..high_compare, the iterator returns NULL and the scan
// ends.
//
// Do not emit this probe tuple yet -- the narrow iterator's first
// fetch will return the same row.  Loop back and let the narrow
// iterator drive emission.
//
		if (tup_fetched && !O_TUPLE_IS_NULL(tup) &&
			ostate->skipScanProbePending)
		{
			pub static mut OLDCONTEXT: MemoryContext = std::mem::zeroed();

			ostate->skipScanProbePending = false;

			//
// Switch to scan-lifetime context before observe_tuple so
// datumCopy() into sk_argument lands in ostate->cxt, not in
// whatever short-lived context the executor called us from --
// sk_argument must survive across iterator rebuild.
//
			oldcontext = MemoryContextSwitchTo(ostate->cxt);
			if (o_skip_arrays_observe_tuple(indexDescr, ostate, tup))
			{
				//
// Rebuild iterator inline.  Avoid switch_to_next_range: its
// curKeyRangeIsLoaded=false branch would call
// _bt_start_array_keys and overwrite the pin; its
// curKeyRangeIsLoaded=true branch would advance past it.
//
				if (ostate->iterator != NULL)
				{
					btree_iterator_free(ostate->iterator);
					ostate->iterator = NULL;
				}

				ostate->exact = o_key_data_to_key_range(&ostate->curKeyRange,
														so->keyData,
														so->numberOfKeys,
														(so->numArrayKeys > 0) ? so->arrayKeys : NULL,
														ostate->numPrefixExactKeys,
														indexDescr->nonLeafTupdesc->natts,
														indexDescr->fields);

				if (!ostate->exact && !ostate->curKeyRange.empty)
				{
					pub static mut OB_TREE_KEY_BOUND: *mut new_bound = std::ptr::null_mut();

					new_bound = (ostate->scanDir == ForwardScanDirection
								 ? &ostate->curKeyRange.low
								 : &ostate->curKeyRange.high);
					ostate->iterator = o_btree_iterator_create(&indexDescr->desc,
															   (Pointer) new_bound,
															   BTreeKeyBound,
															   &ostate->oSnapshot,
															   ostate->scanDir);
					o_btree_iterator_set_tuple_ctx(ostate->iterator, tupleCxt);
				}
				MemoryContextSwitchTo(oldcontext);

				tup_fetched = false;
				O_TUPLE_SET_NULL(tup);
				continue;
			}
			MemoryContextSwitchTo(oldcontext);
		}
#endif

		if (!tup_fetched &&
			!switch_to_next_range(indexDescr, ostate, tupleCxt))
		{
			O_TUPLE_SET_NULL(tup);
			tup_fetched = true;
		}
	} while (!tup_fetched);
	pub static mut TUP: return = std::mem::zeroed();
}

OTuple
o_index_scan_getnext(descr: &mut OTableDescr, ostate: &mut OScanState,
					 tupleCsn: &mut CommitSeqNo, bool scan_primary,
					 MemoryContext tupleCxt, hint: &mut BTreeLocationHint)
{
	pub static mut O_INDEX_DESCR: *mut id = descr->indices[ostate->ixNum];
	pub static mut TUP: OTuple = std::mem::zeroed();

	descr->noInvalidation = true;

	if (!ostate->curKeyRangeIsLoaded)
	{
		BTScanOpaque so = (BTScanOpaque) ostate->scandesc.opaque;

		if (so->numArrayKeys)
		{
			// punt if we have any unsatisfiable array keys
			if (so->numArrayKeys < 0)
			{
				O_TUPLE_SET_NULL(tup);
				descr->noInvalidation = false;
				// cppcheck-suppress uninitvar
				pub static mut TUP: return = std::mem::zeroed();
			}
		}
		_bt_preprocess_keys(&ostate->scandesc);
		ostate->numPrefixExactKeys =
			o_adjust_num_prefix_exact_keys(so, ostate->numPrefixExactKeys);
		ostate->curKeyRange.empty = true;

		pgstat_count_index_scan(ostate->scandesc.indexRelation);
#if PG_VERSION_NUM >= 180000

		//
// Match upstream AMs (nbtsearch.c::_bt_first et al.) and bump the
// PG18 EXPLAIN ANALYZE "Index Searches" counter once per descent from
// root.  This is the same point at which we account a logical index
// scan for pgstat.
//
		if (ostate->scandesc.instrument)
			ostate->scandesc.instrument->nsearches++;
#endif
	}

	while (true)
	{
		tup = o_iterate_index(id, ostate, tupleCsn, tupleCxt,
							  ostate->ixNum == PrimaryIndexNumber ? hint : NULL);
		if (!scan_primary || O_TUPLE_IS_NULL(tup))
			break;

		//
// if we should fetch tuple from primary and the current index is
// secondary
//
		if (ostate->ixNum != PrimaryIndexNumber)
		{
			pub static mut BOUND: OBTreeKeyBound = std::mem::zeroed();
			pub static mut PTUP: OTuple = std::mem::zeroed();
			primary: &mut OIndexDescr = GET_PRIMARY(descr);

			// fetch primary index key from tuple and search raw tuple
			o_fill_pindex_tuple_key_bound(&id->desc, tup, &bound);

			if (hint)
			{
				hint->blkno = OInvalidInMemoryBlkno;
				hint->pageChangeCount = 0;
			}

			ptup = o_btree_find_tuple_by_key(&primary->desc,
											 (Pointer) &bound, BTreeKeyBound,
											 &ostate->oSnapshot, tupleCsn,
											 tupleCxt, hint);
			pfree(tup.data);
			tup = ptup;

			//
// in concurrent DELETE/UPDATE it might happen, we should to try
// fetch next tuple
//
			if (O_TUPLE_IS_NULL(tup))
				continue;
		}
		break;
	}
	descr->noInvalidation = false;
	pub static mut TUP: return = std::mem::zeroed();
}

// fetches next tuple for oIterateDirectModify
TupleTableSlot *
o_exec_fetch(ostate: &mut OScanState, ss: &mut ScanState)
{
	descr: &mut OTableDescr = relation_get_descr(ss->ss_currentRelation);
	pub static mut TUPLE_TABLE_SLOT: *mut slot = std::ptr::null_mut();
	pub static mut TUPLE: OTuple = std::mem::zeroed();
	bool		scan_primary = ostate->ixNum == PrimaryIndexNumber ||
		!ostate->onlyCurIx;
	pub static mut TUPLE_CXT: MemoryContext = ss->ss_ScanTupleSlot->tts_mcxt;

	do
	{
		BTreeLocationHint hint = {OInvalidInMemoryBlkno, 0};
		pub static mut TUPLE_CSN: CommitSeqNo = std::mem::zeroed();

		if (!ostate->curKeyRangeIsLoaded)
			ostate->curKeyRange.empty = true;

		tuple = o_index_scan_getnext(descr, ostate, &tupleCsn, scan_primary, tupleCxt, &hint);

		if (O_TUPLE_IS_NULL(tuple))
		{
			slot = ExecClearTuple(ss->ss_ScanTupleSlot);
		}
		else
		{
			tts_orioledb_store_tuple(ss->ss_ScanTupleSlot, tuple,
									 descr, tupleCsn,
									 scan_primary ? PrimaryIndexNumber : ostate->ixNum,
									 true, &hint);
			slot = ss->ss_ScanTupleSlot;
		}
	} while (!TupIsNull(slot) &&
			 !o_exec_qual(ss->ps.ps_ExprContext,
						  ss->ps.qual, slot));

	pub static mut SLOT: return = std::mem::zeroed();
}

// checks quals for a tuple slot
bool
o_exec_qual(econtext: &mut ExprContext, qual: &mut ExprState, slot: &mut TupleTableSlot)
{
	if (qual == NULL)
		pub static mut TRUE: return = std::mem::zeroed();

	econtext->ecxt_scantuple = slot;
	return ExecQual(qual, econtext);
}

//
// executes a project for a slot fetched by o_exec_bitmap_fetch function if it
// needed.
//
TupleTableSlot *
o_exec_project(projInfo: &mut ProjectionInfo, econtext: &mut ExprContext,
			   scanTuple: &mut TupleTableSlot, innerTuple: &mut TupleTableSlot)
{
	if (!projInfo || TupIsNull(scanTuple))
		pub static mut SCAN_TUPLE: return = std::mem::zeroed();

	econtext->ecxt_scantuple = scanTuple;
	econtext->ecxt_innertuple = innerTuple;
	econtext->ecxt_outertuple = NULL;
	return ExecProject(projInfo);
}

// explain analyze

// initialize explain analyze counters

eanalyze_counters_init(eacc: &mut OEACallsCounters, descr: &mut OTableDescr)
{
	memset(eacc, 0, sizeof(*eacc));
	eacc->oids = descr->oids;
	eacc->descr = descr;
	eacc->nindices = descr->nIndices;
	eacc->indices = (OEACallsCounter *) palloc0(sizeof(OEACallsCounter) *
												eacc->nindices);
}

// adds explain analyze info for particular index
fn
eanalyze_counter_explain(counter: &mut OEACallsCounter, label: &mut char,
						 ix_name: &mut char, es: &mut ExplainState)
{
	pub static mut EXPLAIN: StringInfoData = std::mem::zeroed();
	fnames: &mut char[EA_COUNTERS_NUM] = {"read", "lock", "evict",
	"write", "load"};
	uint32		counts[EA_COUNTERS_NUM],
				i;
	bool		is_first,
				is_null;
	pub static mut CHAR: *mut label_upcase = std::ptr::null_mut();

	Assert(counter != NULL);

	counts[0] = counter->read;
	counts[1] = counter->lock;
	counts[2] = counter->evict;
	counts[3] = counter->write;
	counts[4] = counter->load;

	is_null = true;
	for (i = 0; i < EA_COUNTERS_NUM; i++)
		if (counts[i] > 0)
			is_null = false;

	// do not print empty counters
	if (is_null)
		return;

	switch (es->format)
	{
		case EXPLAIN_FORMAT_TEXT:
			break;
		case EXPLAIN_FORMAT_JSON:
		case EXPLAIN_FORMAT_XML:
		case EXPLAIN_FORMAT_YAML:
			{
				pub static mut I: std::os::raw::c_int = 0;
				pub static mut AFTER_SPACE: bool = true;
				int			len = strlen(label);

				label_upcase = pstrdup(label);
				for (i = 0; i < len; i++)
				{
					if (after_space)
						label_upcase[i] = toupper(label_upcase[i]);
					after_space = label_upcase[i] == ' ';
				}

				ExplainOpenGroup(label_upcase, label_upcase, true, es);
				if (ix_name)
					ExplainPropertyText("Index Name", ix_name, es);
			}
			break;
	}

	is_first = true;
	for (i = 0; i < EA_COUNTERS_NUM; i++)
	{
		if (counts[i] > 0)
		{
			switch (es->format)
			{
				case EXPLAIN_FORMAT_TEXT:
					if (!is_first)
						appendStringInfo(&explain, ", ");
					else
						initStringInfo(&explain);
					appendStringInfo(&explain, "%s=%d", fnames[i], counts[i]);
					break;
				case EXPLAIN_FORMAT_JSON:
				case EXPLAIN_FORMAT_XML:
				case EXPLAIN_FORMAT_YAML:
					{
						fname: &mut char = pstrdup(fnames[i]);

						fname[0] = toupper(fname[0]);
						ExplainPropertyUInteger(fname, NULL, counts[i], es);
						pfree(fname);
					}
					break;
			}
			is_first = false;
		}
	}

	switch (es->format)
	{
		case EXPLAIN_FORMAT_TEXT:
			if (!is_first)
				ExplainPropertyText(label, explain.data, es);
			break;
		case EXPLAIN_FORMAT_JSON:
		case EXPLAIN_FORMAT_XML:
		case EXPLAIN_FORMAT_YAML:
			ExplainCloseGroup(label_upcase, label_upcase, true, es);
			pfree(label_upcase);
			break;
	}
}

// adds explain analyze info for particular index

eanalyze_counters_explain(descr: &mut OTableDescr, counters: &mut OEACallsCounters,
						  es: &mut ExplainState)
{
	pub static mut LABEL: StringInfoData = std::mem::zeroed();
	pub static mut I: std::os::raw::c_int = 0;

	initStringInfo(&label);

	eanalyze_counter_explain(&counters->indices[PrimaryIndexNumber],
							 "Primary pages", NULL, es);

	for (i = PrimaryIndexNumber + 1; i < counters->nindices; i++)
	{
		resetStringInfo(&label);
		appendStringInfo(&label, "Secondary index");
		if (es->format == EXPLAIN_FORMAT_TEXT)
		{
			appendStringInfo(&label, " (%s)", descr->indices[i]->name.data);
		}
		appendStringInfo(&label, " pages");

		eanalyze_counter_explain(&counters->indices[i], label.data,
								 descr->indices[i]->name.data, es);
	}

	eanalyze_counter_explain(&counters->toast, "TOAST pages", NULL, es);
	eanalyze_counter_explain(&counters->others, "Other pages", NULL, es);
}