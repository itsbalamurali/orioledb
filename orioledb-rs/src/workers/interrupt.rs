/*-------------------------------------------------------------------------
 *
 * interrupt.rs
 *		Routines for background workers interrupt handling.
 *
 * Copyright (c) 2021-2026, Oriole DB Inc.
 * Copyright (c) 2025-2026, Supabase Inc.
 *
 * IDENTIFICATION
 *	  contrib/orioledb/orioledb-rs/src/workers/interrupt.rs
 *
 *-------------------------------------------------------------------------
 */

use std::ffi::{c_char, c_int};
use pgrx::pg_sys;

const fn make_sqlstate(c1: u8, c2: u8, c3: u8, c4: u8, c5: u8) -> i32 {
    ((c1 - b'0') as i32)
        + (((c2 - b'0') as i32) << 6)
        + (((c3 - b'0') as i32) << 12)
        + (((c4 - b'0') as i32) << 18)
        + (((c5 - b'0') as i32) << 24)
}

/*
 * Exit from an orioledb worker
 */
unsafe fn o_worker_shutdown(elevel: c_int) {
    debug_assert_eq!(pg_sys::MyBackendType, pg_sys::BackendType_B_BG_WORKER);

    let domain = std::ptr::null();
    let file = b"interrupt.rs\0".as_ptr() as *const c_char;
    let func = b"o_worker_shutdown\0".as_ptr() as *const c_char;
    let msg = b"terminating orioledb worker due to administrator command\0".as_ptr() as *const c_char;

    if pg_sys::errstart(elevel, domain) {
        let _ = pg_sys::errcode(make_sqlstate(b'5', b'7', b'P', b'0', b'1'));
        let _ = pg_sys::errmsg(msg);
        pg_sys::errfinish(file, 29, func);
    }
}

#[no_mangle]
pub unsafe extern "C" fn o_worker_handle_interrupts() {
    /*
     * In case of a pending shutdown request we just raise an ERROR message
     * currently.
     */
    if pg_sys::ShutdownRequestPending as i32 != 0 {
        o_worker_shutdown(pg_sys::ERROR as c_int);
    }
}
