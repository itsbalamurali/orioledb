// Stop-event debugging infrastructure.
//
// Ported from `include/utils/stopevent.h` and `src/utils/stopevent.c`.
//
// Copyright (c) 2021-2026, Oriole DB Inc.
// Copyright (c) 2025-2026, Supabase Inc.

use std::ffi::{c_char, c_int, c_void, CStr};
use pgrx::pg_sys::{self, MemoryContext};
use std::sync::OnceLock;

pub const QUERY_BUFFER_SIZE: usize = 1024;
pub const STOP_EVENT_FLAG_RECOVERY_WORKERS_RUNNING: u32 = 1;

#[repr(C)]
pub struct StopEvent {
    pub condition: [c_char; QUERY_BUFFER_SIZE],
    pub enabled: bool,
    pub nWaiters: c_int,
    pub flags: u32,
    pub lock: pg_sys::slock_t,
    pub cv: pg_sys::ConditionVariable,
}

#[no_mangle]
pub static mut enable_stopevents: bool = false;

#[no_mangle]
pub static mut trace_stopevents: bool = false;

#[no_mangle]
pub static mut stopevents_cxt: MemoryContext = std::ptr::null_mut();

static mut STOPEVENTS: *mut StopEvent = std::ptr::null_mut();

static STOPEVENT_NAMES: OnceLock<Vec<&'static str>> = OnceLock::new();

pub fn get_stopevent_names() -> &'static [&'static str] {
    STOPEVENT_NAMES.get_or_init(|| {
        include_str!("../../../stopevents.txt")
            .lines()
            .map(|l| l.trim())
            .filter(|l| !l.is_empty())
            .collect()
    })
}

#[inline]
pub fn stopevents_enabled() -> bool {
    unsafe { enable_stopevents || trace_stopevents }
}

#[inline]
pub unsafe fn spin_lock_init(lock: *mut pg_sys::slock_t) {
    *lock = 0;
}

#[inline]
pub unsafe fn spin_lock_acquire(lock: *mut pg_sys::slock_t) {
    if pg_sys::tas(lock) != 0 {
        pg_sys::s_lock(
            lock,
            b"stopevent.rs\0".as_ptr() as *const c_char,
            0,
            b"spin_lock_acquire\0".as_ptr() as *const c_char,
        );
    }
}

#[inline]
pub unsafe fn spin_lock_release(lock: *mut pg_sys::slock_t) {
    *lock = 0;
}

#[no_mangle]
pub unsafe extern "C-unwind" fn StopEventShmemSize() -> usize {
    let count = get_stopevent_names().len();
    count * std::mem::size_of::<StopEvent>()
}

#[no_mangle]
pub unsafe extern "C-unwind" fn StopEventShmemInit(ptr: *mut c_void, found: bool) {
    STOPEVENTS = ptr as *mut StopEvent;
    if !found {
        let count = get_stopevent_names().len();
        for i in 0..count {
            let event = &mut *STOPEVENTS.add(i);
            spin_lock_init(&mut event.lock);
            event.enabled = false;
            event.nWaiters = 0;
            pg_sys::ConditionVariableInit(&mut event.cv);
        }
    }
}

unsafe fn get_pgproc_by_number(proc_num: pg_sys::ProcNumber) -> *mut pg_sys::PGPROC {
    (*pg_sys::ProcGlobal).allProcs.add(proc_num as usize)
}

unsafe fn find_stop_event(name: *mut pg_sys::text) -> *mut StopEvent {
    let name_str = pg_sys::text_to_cstring(name);
    let name_rust = CStr::from_ptr(name_str).to_string_lossy();
    let names = get_stopevent_names();
    if let Some(i) = names.iter().position(|&x| x == name_rust) {
        return STOPEVENTS.add(i);
    }
    pg_sys::ereport!(
        pgrx::PgLogLevel::ERROR,
        pg_sys::errcodes::PgSqlErrorCode::ERRCODE_INTERNAL_ERROR,
        format!("unknown stop event: \"{}\"", name_rust)
    );
    std::ptr::null_mut()
}

#[inline]
unsafe fn pg_getarg_datum(fcinfo: pg_sys::FunctionCallInfo, n: usize) -> pg_sys::Datum {
    let args = std::slice::from_raw_parts((*fcinfo).args.as_ptr(), (*fcinfo).nargs as usize);
    args[n].value
}

#[inline]
unsafe fn pg_getarg_text_pp(fcinfo: pg_sys::FunctionCallInfo, n: usize) -> *mut pg_sys::text {
    pg_sys::pg_detoast_datum_packed(pg_getarg_datum(fcinfo, n).value() as *mut pg_sys::varlena) as *mut pg_sys::text
}

#[inline]
unsafe fn pg_getarg_jsonpath_p(fcinfo: pg_sys::FunctionCallInfo, n: usize) -> *mut pg_sys::varlena {
    pg_sys::pg_detoast_datum(pg_getarg_datum(fcinfo, n).value() as *mut pg_sys::varlena)
}

#[inline]
unsafe fn varsize_any(ptr: *const pg_sys::varlena) -> usize {
    let first_byte = *(ptr as *const u8);
    if (first_byte & 0x01) == 0x01 {
        (first_byte >> 1) as usize
    } else {
        let header = *(ptr as *const u32);
        ((header >> 2) & 0x3FFFFFFF) as usize
    }
}

#[inline]
unsafe fn vardata_any(ptr: *const pg_sys::varlena) -> *const c_char {
    let first_byte = *(ptr as *const u8);
    if (first_byte & 0x01) == 0x01 {
        (ptr as *const u8).add(1) as *const c_char
    } else {
        (ptr as *const u8).add(4) as *const c_char
    }
}

#[inline]
unsafe fn varsize_any_exhdr(ptr: *const pg_sys::varlena) -> usize {
    let first_byte = *(ptr as *const u8);
    if (first_byte & 0x01) == 0x01 {
        ((first_byte >> 1) - 1) as usize
    } else {
        let header = *(ptr as *const u32);
        (((header >> 2) & 0x3FFFFFFF) - 4) as usize
    }
}

extern "C-unwind" {
    pub fn jsonb_push_int8_key(state: *mut *mut pg_sys::JsonbParseState, key: *const c_char, value: i64);
    pub fn jsonb_push_null_key(state: *mut *mut pg_sys::JsonbParseState, key: *const c_char);
    pub fn jsonb_push_string_key(state: *mut *mut pg_sys::JsonbParseState, key: *const c_char, value: *const c_char);
    pub fn check_recovery_workers_finished() -> bool;
    pub fn jsonb_path_match(fcinfo: pg_sys::FunctionCallInfo) -> pg_sys::Datum;
    pub fn jsonb_out(fcinfo: pg_sys::FunctionCallInfo) -> pg_sys::Datum;
}

#[no_mangle]
pub unsafe extern "C-unwind" fn pg_finfo_pg_stopevent_set() -> *const pg_sys::Pg_finfo_record {
    static MY_FINFO: pg_sys::Pg_finfo_record = pg_sys::Pg_finfo_record { api_version: 1 };
    &MY_FINFO
}

#[no_mangle]
pub unsafe extern "C-unwind" fn pg_stopevent_set(fcinfo: pg_sys::FunctionCallInfo) -> pg_sys::Datum {
    let event_name = pg_getarg_text_pp(fcinfo, 0);
    let condition = pg_getarg_jsonpath_p(fcinfo, 1);
    let mut flags: u32 = 0;

    if (*fcinfo).nargs >= 3 {
        let flags_text = pg_getarg_text_pp(fcinfo, 2);
        let flags_ptr = vardata_any(flags_text as *const pg_sys::varlena) as *const u8;
        let flags_len = varsize_any_exhdr(flags_text as *const pg_sys::varlena);
        let flags_slice = std::slice::from_raw_parts(flags_ptr, flags_len);
        for &c in flags_slice {
            if c == b'r' {
                flags |= STOP_EVENT_FLAG_RECOVERY_WORKERS_RUNNING;
            } else {
                pg_sys::ereport!(
                    pgrx::PgLogLevel::ERROR,
                    pg_sys::errcodes::PgSqlErrorCode::ERRCODE_INTERNAL_ERROR,
                    "wrong stopevent flag"
                );
            }
        }
    }

    let event = find_stop_event(event_name);
    let cond_size = varsize_any(condition);
    if cond_size > QUERY_BUFFER_SIZE {
        pg_sys::ereport!(
            pgrx::PgLogLevel::ERROR,
            pg_sys::errcodes::PgSqlErrorCode::ERRCODE_INTERNAL_ERROR,
            "jsonpath condition is too long"
        );
    }

    spin_lock_acquire(&mut (*event).lock);
    (*event).enabled = true;
    (*event).flags = flags;
    std::ptr::copy_nonoverlapping(
        condition as *const c_char,
        (*event).condition.as_mut_ptr(),
        cond_size,
    );
    spin_lock_release(&mut (*event).lock);

    pg_sys::ConditionVariableBroadcast(&mut (*event).cv);

    let orig_datum = pg_getarg_datum(fcinfo, 1);
    if orig_datum != pg_sys::Datum::from(condition) {
        pg_sys::pfree(condition as *mut c_void);
    }

    pg_sys::Datum::from(0)
}

#[no_mangle]
pub unsafe extern "C-unwind" fn pg_finfo_pg_stopevent_reset() -> *const pg_sys::Pg_finfo_record {
    static MY_FINFO: pg_sys::Pg_finfo_record = pg_sys::Pg_finfo_record { api_version: 1 };
    &MY_FINFO
}

#[no_mangle]
pub unsafe extern "C-unwind" fn pg_stopevent_reset(fcinfo: pg_sys::FunctionCallInfo) -> pg_sys::Datum {
    let event_name = pg_getarg_text_pp(fcinfo, 0);
    let event = find_stop_event(event_name);

    spin_lock_acquire(&mut (*event).lock);
    let result = (*event).nWaiters > 0;
    (*event).enabled = false;
    spin_lock_release(&mut (*event).lock);

    pg_sys::ConditionVariableBroadcast(&mut (*event).cv);

    pg_sys::Datum::from(result)
}

#[no_mangle]
pub unsafe extern "C-unwind" fn pg_finfo_pg_stopevents() -> *const pg_sys::Pg_finfo_record {
    static MY_FINFO: pg_sys::Pg_finfo_record = pg_sys::Pg_finfo_record { api_version: 1 };
    &MY_FINFO
}

#[no_mangle]
pub unsafe extern "C-unwind" fn pg_stopevents(fcinfo: pg_sys::FunctionCallInfo) -> pg_sys::Datum {
    let rsinfo = (*fcinfo).resultinfo as *mut pg_sys::ReturnSetInfo;
    if rsinfo.is_null() || (*rsinfo).type_ != pg_sys::NodeTag::T_ReturnSetInfo {
        pg_sys::ereport!(
            pgrx::PgLogLevel::ERROR,
            pg_sys::errcodes::PgSqlErrorCode::ERRCODE_E_R_I_E_TRIGGER_PROTOCOL_VIOLATED,
            "set-valued function called in context that cannot accept a set"
        );
    }

    let oldcontext = pg_sys::MemoryContextSwitchTo((*(*rsinfo).econtext).ecxt_per_query_memory);

    let tupdesc = pg_sys::CreateTemplateTupleDesc(3);
    pg_sys::TupleDescInitEntry(tupdesc, 1, b"stopevent\0".as_ptr() as *const c_char, pg_sys::TEXTOID, -1, 0);
    pg_sys::TupleDescInitEntry(tupdesc, 2, b"condition\0".as_ptr() as *const c_char, pg_sys::JSONPATHOID, -1, 0);
    pg_sys::TupleDescInitEntry(tupdesc, 3, b"waiters\0".as_ptr() as *const c_char, pg_sys::INT4ARRAYOID, -1, 0);

    let random_access = ((*rsinfo).allowedModes & pg_sys::SetFunctionReturnMode::SFRM_Materialize_Random as i32) != 0;
    let tupstore = pg_sys::tuplestore_begin_heap(random_access, false, pg_sys::work_mem);
    (*rsinfo).returnMode = pg_sys::SetFunctionReturnMode::SFRM_Materialize;
    (*rsinfo).setResult = tupstore;
    (*rsinfo).setDesc = tupdesc;

    pg_sys::MemoryContextSwitchTo(oldcontext);

    let count = get_stopevent_names().len();
    for i in 0..count {
        let event = STOPEVENTS.add(i);
        spin_lock_acquire(&mut (*event).lock);
        if !(*event).enabled {
            spin_lock_release(&mut (*event).lock);
            continue;
        }

        let mut values: [pg_sys::Datum; 3] = [pg_sys::Datum::from(0); 3];
        let mut nulls: [bool; 3] = [false; 3];

        let name_str = std::ffi::CString::new(get_stopevent_names()[i]).unwrap();
        values[0] = pg_sys::cstring_to_text(name_str.as_ptr()).into();
        values[1] = pg_sys::Datum::from(&mut (*event).condition as *mut [c_char; QUERY_BUFFER_SIZE] as *mut c_void);

        let mut waiters: *mut pg_sys::List = std::ptr::null_mut();
        spin_lock_acquire(&mut (*event).cv.mutex);
        
        let mut cur = (*event).cv.wakeup.head;
        while cur != pg_sys::INVALID_PROC_NUMBER {
            let waiter = get_pgproc_by_number(cur);
            waiters = pg_sys::lappend_int(waiters, (*waiter).pid);
            cur = (*waiter).cvWaitLink.next;
        }
        spin_lock_release(&mut (*event).cv.mutex);

        let waiters_len = pg_sys::list_length(waiters);
        let elems = pg_sys::palloc(std::mem::size_of::<pg_sys::Datum>() * waiters_len as usize) as *mut pg_sys::Datum;
        for j in 0..waiters_len {
            *elems.add(j as usize) = pg_sys::Datum::from(pg_sys::list_nth_int(waiters, j));
        }

        values[2] = pg_sys::Datum::from(pg_sys::construct_array(
            elems,
            waiters_len,
            pg_sys::INT4OID,
            4,
            true,
            'i' as c_char,
        ));

        pg_sys::tuplestore_putvalues(tupstore, tupdesc, values.as_mut_ptr(), nulls.as_mut_ptr());
        spin_lock_release(&mut (*event).lock);
    }

    pg_sys::Datum::from(0)
}

#[no_mangle]
pub unsafe extern "C-unwind" fn pid_is_waiting_for_stopevent(pid: c_int) -> bool {
    let count = get_stopevent_names().len();
    for i in 0..count {
        let event = STOPEVENTS.add(i);
        spin_lock_acquire(&mut (*event).lock);
        if !(*event).enabled {
            spin_lock_release(&mut (*event).lock);
            continue;
        }

        spin_lock_acquire(&mut (*event).cv.mutex);
        let mut cur = (*event).cv.wakeup.head;
        while cur != pg_sys::INVALID_PROC_NUMBER {
            let waiter = get_pgproc_by_number(cur);
            if (*waiter).pid == pid {
                spin_lock_release(&mut (*event).cv.mutex);
                spin_lock_release(&mut (*event).lock);
                return true;
            }
            cur = (*waiter).cvWaitLink.next;
        }
        spin_lock_release(&mut (*event).cv.mutex);
        spin_lock_release(&mut (*event).lock);
    }
    false
}

unsafe fn make_process_params() -> *mut pg_sys::Jsonb {
    let mut state: *mut pg_sys::JsonbParseState = std::ptr::null_mut();
    let old_context = pg_sys::MemoryContextSwitchTo(stopevents_cxt);

    let bt = if !pg_sys::MyBEEntry.is_null() {
        (*pg_sys::MyBEEntry).st_backendType
    } else {
        pg_sys::MyBackendType
    };

    let be_type = if bt == pg_sys::BackendType::B_BG_WORKER {
        pg_sys::GetBackgroundWorkerTypeByPid(pg_sys::MyProcPid)
    } else {
        pg_sys::GetBackendTypeDesc(bt)
    };

    pg_sys::pushJsonbValue(
        &mut state,
        pg_sys::JsonbIteratorToken::WJB_BEGIN_OBJECT,
        std::ptr::null_mut(),
    );

    jsonb_push_int8_key(
        &mut state,
        b"pid\0".as_ptr() as *const c_char,
        pg_sys::MyProcPid as i64,
    );

    if !be_type.is_null() {
        jsonb_push_string_key(
            &mut state,
            b"backendType\0".as_ptr() as *const c_char,
            be_type,
        );
    } else {
        jsonb_push_null_key(&mut state, b"backendType\0".as_ptr() as *const c_char);
    }

    jsonb_push_string_key(
        &mut state,
        b"applicationName\0".as_ptr() as *const c_char,
        pg_sys::application_name,
    );

    let jval = pg_sys::pushJsonbValue(
        &mut state,
        pg_sys::JsonbIteratorToken::WJB_END_OBJECT,
        std::ptr::null_mut(),
    );

    let res = pg_sys::JsonbValueToJsonb(jval);
    pg_sys::MemoryContextSwitchTo(old_context);
    res
}

unsafe fn make_empty_params() -> *mut pg_sys::Jsonb {
    let mut state: *mut pg_sys::JsonbParseState = std::ptr::null_mut();
    let old_context = pg_sys::MemoryContextSwitchTo(stopevents_cxt);

    pg_sys::pushJsonbValue(
        &mut state,
        pg_sys::JsonbIteratorToken::WJB_BEGIN_OBJECT,
        std::ptr::null_mut(),
    );

    let jval = pg_sys::pushJsonbValue(
        &mut state,
        pg_sys::JsonbIteratorToken::WJB_END_OBJECT,
        std::ptr::null_mut(),
    );

    let res = pg_sys::JsonbValueToJsonb(jval);
    pg_sys::MemoryContextSwitchTo(old_context);
    res
}

unsafe fn stop_event_wait_info() -> u32 {
    static mut CACHED_WAIT_INFO: u32 = 0;
    if CACHED_WAIT_INFO == 0 {
        CACHED_WAIT_INFO = pg_sys::WaitEventExtensionNew(b"StopEvent\0".as_ptr() as *const c_char);
    }
    CACHED_WAIT_INFO
}

unsafe fn check_stopevent_condition(event: *mut StopEvent, params: *mut pg_sys::Jsonb) -> bool {
    spin_lock_acquire(&mut (*event).lock);
    if !(*event).enabled {
        spin_lock_release(&mut (*event).lock);
        return false;
    }

    let res = pg_sys::DirectFunctionCall4Coll(
        Some(jsonb_path_match),
        pg_sys::InvalidOid,
        pg_sys::Datum::from(params),
        pg_sys::Datum::from(&mut (*event).condition as *mut [c_char; QUERY_BUFFER_SIZE]),
        pg_sys::Datum::from(make_process_params()),
        pg_sys::Datum::from(false),
    );

    spin_lock_release(&mut (*event).lock);
    res.value() != 0
}

struct WaiterGuard {
    event: *mut StopEvent,
}

impl Drop for WaiterGuard {
    fn drop(&mut self) {
        unsafe {
            spin_lock_acquire(&mut (*self.event).lock);
            (*self.event).nWaiters -= 1;
            spin_lock_release(&mut (*self.event).lock);
        }
    }
}

#[no_mangle]
pub unsafe extern "C-unwind" fn handle_stopevent(event_id: c_int, mut params: *mut pg_sys::Jsonb) {
    let count = get_stopevent_names().len();
    assert!((event_id as usize) < count);

    let event = STOPEVENTS.add(event_id as usize);

    if params.is_null() {
        params = make_empty_params();
    }

    if (*event).enabled && check_stopevent_condition(event, params) {
        spin_lock_acquire(&mut (*event).lock);
        (*event).nWaiters += 1;
        spin_lock_release(&mut (*event).lock);

        let _guard = WaiterGuard { event };

        pg_sys::ConditionVariablePrepareToSleep(&mut (*event).cv);
        loop {
            if ((*event).flags & STOP_EVENT_FLAG_RECOVERY_WORKERS_RUNNING) != 0 {
                if check_recovery_workers_finished() {
                    break;
                }
            }

            if !check_stopevent_condition(event, params) {
                break;
            }

            pg_sys::ConditionVariableTimedSleep(&mut (*event).cv, 1000, stop_event_wait_info());
        }
        pg_sys::ConditionVariableCancelSleep();
    }

    if trace_stopevents {
        let params_string = pg_sys::DirectFunctionCall1Coll(
            Some(jsonb_out),
            pg_sys::InvalidOid,
            pg_sys::Datum::from(params),
        );
        let params_cstr = CStr::from_ptr(params_string.value() as *const c_char);
        pg_sys::ereport!(
            pgrx::PgLogLevel::LOG,
            pg_sys::errcodes::PgSqlErrorCode::ERRCODE_SUCCESSFUL_COMPLETION,
            format!(
                "stop event \"{}\", params \"{}\"",
                get_stopevent_names()[event_id as usize],
                params_cstr.to_string_lossy()
            )
        );
    }

    pg_sys::MemoryContextReset(stopevents_cxt);
}

#[no_mangle]
pub unsafe extern "C-unwind" fn check_stopevent(event_id: c_int, params: *mut pg_sys::Jsonb) -> bool {
    let count = get_stopevent_names().len();
    assert!((event_id as usize) < count);

    let event = STOPEVENTS.add(event_id as usize);

    if (*event).enabled && check_stopevent_condition(event, params) {
        return true;
    }

    false
}

#[no_mangle]
pub unsafe extern "C-unwind" fn wait_for_stopevent_enabled(event_id: c_int) {
    let count = get_stopevent_names().len();
    assert!((event_id as usize) < count);

    let event = STOPEVENTS.add(event_id as usize);

    if (*event).enabled {
        return;
    }

    pg_sys::ConditionVariablePrepareToSleep(&mut (*event).cv);
    loop {
        if (*event).enabled {
            break;
        }
        pg_sys::ConditionVariableSleep(&mut (*event).cv, stop_event_wait_info());
    }
    pg_sys::ConditionVariableCancelSleep();
}

#[no_mangle]
pub unsafe extern "C-unwind" fn stopevents_make_cxt() {
    if stopevents_cxt.is_null() {
        stopevents_cxt = pg_sys::AllocSetContextCreateInternal(
            pg_sys::TopMemoryContext,
            b"StopEventsMemoryContext\0".as_ptr() as *const c_char,
            pg_sys::ALLOCSET_DEFAULT_MINSIZE as usize,
            pg_sys::ALLOCSET_DEFAULT_INITSIZE as usize,
            pg_sys::ALLOCSET_DEFAULT_MAXSIZE as usize,
        );
    }
}
