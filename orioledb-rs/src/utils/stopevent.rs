// Stop-event debugging infrastructure.
//
// Ported from `include/utils/stopevent.h` and `src/utils/stopevent.c`.
//
// Copyright (c) 2021-2026, Oriole DB Inc.
// Copyright (c) 2025-2026, Supabase Inc.

use pgrx::pg_sys::{Datum, MemoryContext};

/// Return `true` when stop-events are enabled (either trigger or trace mode).
///
/// Mirrors the `STOPEVENTS_ENABLED()` macro.
pub fn stopevents_enabled() -> bool {
    unsafe { enable_stopevents || trace_stopevents }
}

extern "C" {
    pub static mut enable_stopevents: bool;
    pub static mut trace_stopevents: bool;
    pub static mut stopevents_cxt: MemoryContext;

    pub fn StopEventShmemSize() -> usize;
    pub fn StopEventShmemInit(ptr: *mut u8, found: bool);

    pub fn pg_stopevent_set(fcinfo: *mut pgrx::pg_sys::FunctionCallInfoBaseData) -> Datum;
    pub fn pg_stopevent_reset(fcinfo: *mut pgrx::pg_sys::FunctionCallInfoBaseData) -> Datum;
    pub fn pg_stopevents(fcinfo: *mut pgrx::pg_sys::FunctionCallInfoBaseData) -> Datum;

    pub fn pid_is_waiting_for_stopevent(pid: i32) -> bool;

    /// Trigger a stop-event with the given `event_id` and optional JSON `params`.
    pub fn handle_stopevent(event_id: i32, params: *mut pgrx::pg_sys::Jsonb);

    /// Return `true` if the stop-event condition for `event_id` is met.
    pub fn check_stopevent(event_id: i32, params: *mut pgrx::pg_sys::Jsonb) -> bool;

    /// Block until the stop-event with `event_id` has been enabled.
    pub fn wait_for_stopevent_enabled(event_id: i32);

    /// Create the memory context used by stop-event state.
    pub fn stopevents_make_cxt();
}
