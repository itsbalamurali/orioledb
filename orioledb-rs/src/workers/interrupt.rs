//! Routines for background workers interrupt handling.

use pgrx::{ereport, pg_sys, PgLogLevel, PgSqlErrorCode};

/// Terminate an orioledb worker due to administrator shutdown request.
///
/// Asserts that the current process is a background worker, then raises an
/// `ERRCODE_ADMIN_SHUTDOWN` error at the specified log level. This function
/// does not return.
fn o_worker_shutdown(elevel: i32) {
    // Workers can only be background workers
    debug_assert_eq!(unsafe { pg_sys::MyBackendType }, pg_sys::B_BG_WORKER);

    // Convert elevel to PgLogLevel
    let level = match elevel {
        pg_sys::WARNING => PgLogLevel::WARNING,
        pg_sys::NOTICE => PgLogLevel::NOTICE,
        pg_sys::LOG => PgLogLevel::LOG,
        pg_sys::INFO => PgLogLevel::INFO,
        pg_sys::DEBUG1 => PgLogLevel::DEBUG1,
        pg_sys::DEBUG2 => PgLogLevel::DEBUG2,
        pg_sys::DEBUG3 => PgLogLevel::DEBUG3,
        pg_sys::DEBUG4 => PgLogLevel::DEBUG4,
        pg_sys::DEBUG5 => PgLogLevel::DEBUG5,
        _ => PgLogLevel::ERROR,
    };

    // Report the shutdown error — this raises an exception and never returns
    ereport!(
        level,
        PgSqlErrorCode::ERRCODE_ADMIN_SHUTDOWN,
        "terminating orioledb worker due to administrator command"
    );
}

/// Handle interrupts for an orioledb worker.
///
/// Checks for a pending shutdown request (`ShutdownRequestPending`). If set,
/// the worker is terminated with an `ERROR` level message.
pub fn o_worker_handle_interrupts() {
    if unsafe { pg_sys::ShutdownRequestPending } {
        o_worker_shutdown(pg_sys::ERROR);
    }
}
