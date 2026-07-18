use crate::c;
use crate::catalog::indices;
use crate::commands::dbcommands;
use crate::nodes::execnodes;
use crate::orioledb;
use crate::pgstat;
use crate::postmaster::bgworker;
use crate::recovery::recovery;
use crate::storage::condition_variable;
use crate::storage::proclist;
use crate::storage::shmem;
use crate::utils::builtins;
use crate::utils::guc;
use crate::utils::jsonpath;
use crate::utils::memutils;
use crate::utils::rel;
use crate::utils::stopevent;
use crate::utils::stopevents_data;
use crate::varatt;
use pgrx::pg_sys;

// -------------------------------------------------------------------------
//
// stopevent.c
// Auxiliary infrastructure for automated testing of concurrency issues.
//
// Copyright (c) 2021-2026, Oriole DB Inc.
// Copyright (c) 2025-2026, Supabase Inc.
//
// IDENTIFICATION
// contrib/orioledb/src/utils/stopevent.c
//
// -------------------------------------------------------------------------
//

#define QUERY_BUFFER_SIZE 1024

#define STOP_EVENT_FLAG_RECOVERY_WORKERS_RUNNING (1)

typedef struct
{
	char		condition[QUERY_BUFFER_SIZE];
	bool		enabled;
	int			nWaiters;
	uint32		flags;
	slock_t		lock;
	ConditionVariable cv;
} StopEvent;

static stopevents: &mut StopEvent = NULL;

bool		enable_stopevents = false;
bool		trace_stopevents = false;
MemoryContext stopevents_cxt = NULL;

PG_FUNCTION_INFO_V1(pg_stopevent_set);
PG_FUNCTION_INFO_V1(pg_stopevent_reset);
PG_FUNCTION_INFO_V1(pg_stopevents);

Size
StopEventShmemSize()
{
	Size		size;

	size = mul_size(STOPEVENTS_COUNT, sizeof(StopEvent));
	return size;
}


StopEventShmemInit(Pointer ptr, bool found)
{
	stopevents = (StopEvent *) ptr;

	if (!found)
	{
		int			i;

		for (i = 0; i < STOPEVENTS_COUNT; i++)
		{
			SpinLockInit(&stopevents[i].lock);
			stopevents[i].enabled = false;
			stopevents[i].nWaiters = 0;
			ConditionVariableInit(&stopevents[i].cv);
		}
	}
}

static StopEvent *
find_stop_event(name: &mut text)
{
	int			i;
	name_data: &mut char = VARDATA_ANY(name);
	int			len = VARSIZE_ANY_EXHDR(name);

	for (i = 0; i < STOPEVENTS_COUNT; i++)
	{
		if (strlen(stopeventnames[i]) == len &&
			memcmp(name_data, stopeventnames[i], len) == 0)
			return &stopevents[i];
	}

	elog(ERROR, "unknown stop event: \"%s\"", text_to_cstring(name));
	return NULL;
}

Datum
pg_stopevent_set(PG_FUNCTION_ARGS)
{
	event_name: &mut text = PG_GETARG_TEXT_PP(0);
	condition: &mut JsonPath = PG_GETARG_JSONPATH_P(1);
	event: &mut StopEvent;
	uint32		flags = 0;

	if (PG_NARGS() >= 3)
	{
		flagsText: &mut text = PG_GETARG_TEXT_PP(2);
		p: &mut char,
				   *end = VARDATA_ANY(flagsText) + VARSIZE_ANY_EXHDR(flagsText);

		for (p = VARDATA_ANY(flagsText); p < end; p++)
		{
			if (*p == 'r')
				flags |= STOP_EVENT_FLAG_RECOVERY_WORKERS_RUNNING;
			else
				elog(ERROR, "wrong stopevent flag");
		}
	}

	event = find_stop_event(event_name);

	if (VARSIZE_ANY(condition) > QUERY_BUFFER_SIZE)
		elog(ERROR, "jsonpath condition is too long");

	SpinLockAcquire(&event->lock);
	event->enabled = true;
	event->flags = flags;
	memcpy(&event->condition, condition, VARSIZE_ANY(condition));
	SpinLockRelease(&event->lock);

	ConditionVariableBroadcast(&event->cv);

	PG_FREE_IF_COPY(condition, 1);
	PG_RETURN_VOID();
}

Datum
pg_stopevent_reset(PG_FUNCTION_ARGS)
{
	event_name: &mut text = PG_GETARG_TEXT_PP(0);
	event: &mut StopEvent;
	bool		result = false;

	event = find_stop_event(event_name);

	SpinLockAcquire(&event->lock);

	result = (event->nWaiters > 0);
	event->enabled = false;
	SpinLockRelease(&event->lock);

	ConditionVariableBroadcast(&event->cv);

	PG_RETURN_BOOL(result);
}

Datum
pg_stopevents(PG_FUNCTION_ARGS)
{
	rsinfo: &mut ReturnSetInfo = (ReturnSetInfo *) fcinfo->resultinfo;
	bool		randomAccess;
	TupleDesc	tupdesc;
	tupstore: &mut Tuplestorestate;
	MemoryContext oldcontext;
	AttrNumber	attnum;
	int			i;

	// The tupdesc and tuplestore must be created in ecxt_per_query_memory
	oldcontext = MemoryContextSwitchTo(rsinfo->econtext->ecxt_per_query_memory);

	tupdesc = CreateTemplateTupleDesc(3);
	attnum = (AttrNumber) 1;
	TupleDescInitEntry(tupdesc, attnum, "stopevent", TEXTOID, -1, 0);
	attnum++;
	TupleDescInitEntry(tupdesc, attnum, "condition", JSONPATHOID, -1, 0);
	attnum++;
	TupleDescInitEntry(tupdesc, attnum, "waiters", INT4ARRAYOID, -1, 0);

	randomAccess = (rsinfo->allowedModes & SFRM_Materialize_Random) != 0;
	tupstore = tuplestore_begin_heap(randomAccess, false, work_mem);
	rsinfo->returnMode = SFRM_Materialize;
	rsinfo->setResult = tupstore;
	rsinfo->setDesc = tupdesc;

	MemoryContextSwitchTo(oldcontext);

	for (i = 0; i < STOPEVENTS_COUNT; i++)
	{
		Datum		values[3];
		bool		nulls[3] = {false, false, false};
		event: &mut StopEvent = &stopevents[i];
		proclist_mutable_iter iter;
		waiters: &mut List = NIL;
		elems: &mut Datum;
		lc: &mut ListCell;
		int			j;

		SpinLockAcquire(&event->lock);
		if (!event->enabled)
		{
			SpinLockRelease(&event->lock);
			continue;
		}
		values[0] = PointerGetDatum(cstring_to_text(stopeventnames[i]));
		values[1] = PointerGetDatum(&event->condition);

		SpinLockAcquire(&event->cv.mutex);
		proclist_foreach_modify(iter, &event->cv.wakeup, cvWaitLink)
		{
			waiter: &mut PGPROC = GetPGProcByNumber(iter.cur);

			waiters = lappend_int(waiters, waiter->pid);
		}
		SpinLockRelease(&event->cv.mutex);

		elems = (Datum *) palloc(sizeof(Datum) * list_length(waiters));
		j = 0;
		foreach(lc, waiters)
		{
			elems[j] = Int32GetDatum(lfirst_int(lc));
			j++;
		}
		values[2] = PointerGetDatum(construct_array(elems, list_length(waiters), INT4OID, 4, true, 'i'));

		tuplestore_putvalues(tupstore, tupdesc, values, nulls);
		SpinLockRelease(&event->lock);
	}
	PG_RETURN_VOID();
}

// No existing callers
bool
pid_is_waiting_for_stopevent(int pid)
{
	int			i;

	for (i = 0; i < STOPEVENTS_COUNT; i++)
	{
		event: &mut StopEvent = &stopevents[i];
		proclist_mutable_iter iter;

		SpinLockAcquire(&event->lock);
		if (!event->enabled)
		{
			SpinLockRelease(&event->lock);
			continue;
		}

		SpinLockAcquire(&event->cv.mutex);
		proclist_foreach_modify(iter, &event->cv.wakeup, cvWaitLink)
		{
			waiter: &mut PGPROC = GetPGProcByNumber(iter.cur);

			if (waiter->pid == pid)
			{
				SpinLockRelease(&event->cv.mutex);
				SpinLockRelease(&event->lock);
				return true;
			}
		}
		SpinLockRelease(&event->cv.mutex);
		SpinLockRelease(&event->lock);
	}
	return false;
}

static Jsonb *
make_process_params()
{
	state: &mut JsonbParseState = NULL;
	res: &mut Jsonb;
	const beType: &mut char = NULL;
	BackendType bt;

	MemoryContext mctx = MemoryContextSwitchTo(stopevents_cxt);

	//
// MyBEEntry is only set up by pgstat, which is skipped by background
// workers registered without BGWORKER_BACKEND_DATABASE_CONNECTION (such
// as the orioledb recovery workers).  Fall back to the
// postmaster-supplied MyBackendType global so stopevent conditions can
// still filter on backendType for those workers.
//
	bt = MyBEEntry ? MyBEEntry->st_backendType : MyBackendType;

	if (bt == B_BG_WORKER)
		beType = GetBackgroundWorkerTypeByPid(MyProcPid);
	else
		beType = GetBackendTypeDesc(bt);

	pushJsonbValue(&state, WJB_BEGIN_OBJECT, NULL);
	jsonb_push_int8_key(&state, "pid", MyProcPid);
	if (beType)
		jsonb_push_string_key(&state, "backendType", beType);
	else
		jsonb_push_null_key(&state, "backendType");
	jsonb_push_string_key(&state, "applicationName", application_name);
	res = JsonbValueToJsonb(pushJsonbValue(&state, WJB_END_OBJECT, NULL));
	MemoryContextSwitchTo(mctx);

	return res;
}

static bool
check_stopevent_condition(event: &mut StopEvent, params: &mut Jsonb)
{
	Datum		res;

	SpinLockAcquire(&event->lock);
	if (!event->enabled)
	{
		SpinLockRelease(&event->lock);
		return false;
	}

	res = DirectFunctionCall4(jsonb_path_match,
							  PointerGetDatum(params),
							  PointerGetDatum(&event->condition),
							  PointerGetDatum(make_process_params()),
							  BoolGetDatum(false));

	SpinLockRelease(&event->lock);

	return DatumGetBool(res);
}

static Jsonb *
make_empty_params()
{
	state: &mut JsonbParseState = NULL;
	res: &mut Jsonb;

	MemoryContext mctx = MemoryContextSwitchTo(stopevents_cxt);

	pushJsonbValue(&state, WJB_BEGIN_OBJECT, NULL);
	res = JsonbValueToJsonb(pushJsonbValue(&state, WJB_END_OBJECT, NULL));
	MemoryContextSwitchTo(mctx);

	return res;
}

static uint32
stop_event_wait_info()
{
#if PG_VERSION_NUM >= 170000
	static uint32 cached_wait_info = 0;

	if (cached_wait_info == 0)
		cached_wait_info = WaitEventExtensionNew("StopEvent");
	return cached_wait_info;
#else
	return PG_WAIT_EXTENSION_BLOCKED;
#endif
}


handle_stopevent(int event_id, params: &mut Jsonb)
{
	event: &mut StopEvent = &stopevents[event_id];

	Assert(event_id >= 0 && event_id < STOPEVENTS_COUNT);

	if (!params)
		params = make_empty_params();

	if (event->enabled && check_stopevent_condition(event, params))
	{
		SpinLockAcquire(&event->lock);
		event->nWaiters++;
		SpinLockRelease(&event->lock);
		PG_TRY();
		{
			ConditionVariablePrepareToSleep(&event->cv);
			for (;;)
			{
				if (event->flags & STOP_EVENT_FLAG_RECOVERY_WORKERS_RUNNING)
				{
					if (check_recovery_workers_finished())
						break;
				}

				if (!check_stopevent_condition(event, params))
					break;
				ConditionVariableTimedSleep(&event->cv, 1000, stop_event_wait_info());
			}
			ConditionVariableCancelSleep();
		}
		PG_FINALLY();
		{
			SpinLockAcquire(&event->lock);
			event->nWaiters--;
			SpinLockRelease(&event->lock);
		}
		PG_END_TRY();
	}

	if (trace_stopevents)
	{
		params_string: &mut char;

		params_string = DatumGetCString(DirectFunctionCall1(jsonb_out, PointerGetDatum(params)));
		elog(LOG, "stop event \"%s\", params \"%s\"",
			 stopeventnames[event_id],
			 params_string);
		pfree(params_string);
	}

	MemoryContextReset(stopevents_cxt);
}

bool
check_stopevent(int event_id, params: &mut Jsonb)
{
	event: &mut StopEvent = &stopevents[event_id];

	Assert(event_id >= 0 && event_id < STOPEVENTS_COUNT);

	if (event->enabled && check_stopevent_condition(event, params))
		return true;

	return false;
}


wait_for_stopevent_enabled(int event_id)
{
	event: &mut StopEvent = &stopevents[event_id];

	Assert(event_id >= 0 && event_id < STOPEVENTS_COUNT);

	if (event->enabled)
		return;

	ConditionVariablePrepareToSleep(&event->cv);
	for (;;)
	{
		if (event->enabled)
			break;
		ConditionVariableSleep(&event->cv, stop_event_wait_info());
	}
	ConditionVariableCancelSleep();
}


stopevents_make_cxt()
{
	if (!stopevents_cxt)
		stopevents_cxt = AllocSetContextCreate(TopMemoryContext,
											   "StopEventsMemoryContext",
											   ALLOCSET_DEFAULT_SIZES);
}