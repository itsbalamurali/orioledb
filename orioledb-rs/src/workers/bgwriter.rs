/*-------------------------------------------------------------------------
 *
 * bgwriter.rs
 *		Routines for background writer process.
 *
 * Copyright (c) 2021-2026, Oriole DB Inc.
 * Copyright (c) 2025-2026, Supabase Inc.
 *
 * IDENTIFICATION
 *	  contrib/orioledb/orioledb-rs/src/workers/bgwriter.rs
 *
 *-------------------------------------------------------------------------
 */

use std::ffi::{c_char, c_int, CString};
use pgrx::pg_sys;

const ORIOLEDB_BLCKSZ: usize = 8192;

// PagePool related
#[repr(C)]
pub struct PagePoolOps {
    pub alloc_page: Option<unsafe extern "C" fn(pool: *mut PagePool, pageReserveKind: c_int) -> u32>,
    pub alloc_metapage: Option<unsafe extern "C" fn(pool: *mut PagePool) -> u32>,
    pub free_page: Option<unsafe extern "C" fn(pool: *mut PagePool, blkno: u32, haveLock: bool)>,
    pub reserve_pages: Option<unsafe extern "C" fn(pool: *mut PagePool, pageReserveKind: c_int, count: c_int)>,
    pub release_reserved: Option<unsafe extern "C" fn(pool: *mut PagePool, kind_mask: u32)>,
    pub free_pages_count: Option<unsafe extern "C" fn(pool: *mut PagePool) -> u32>,
    pub dirty_pages_count: Option<unsafe extern "C" fn(pool: *mut PagePool) -> u32>,
    pub run_maintenance: Option<unsafe extern "C" fn(pool: *mut PagePool, evict: bool, shutdown_requested: *mut c_int) -> bool>,
    pub size: Option<unsafe extern "C" fn(pool: *mut PagePool) -> u32>,
    pub ucm_inc_usage: Option<unsafe extern "C" fn(pool: *mut PagePool, blkno: u32)>,
    pub ucm_init: Option<unsafe extern "C" fn(pool: *mut PagePool, blkno: u32)>,
}

#[repr(C)]
pub struct PagePool {
    pub ops: *const PagePoolOps,
    pub numPagesReserved: [u32; 4],
}

pub type OPagePoolType = c_int;
pub const OPagePoolMain: OPagePoolType = 0;
pub const OPagePoolFreeTree: OPagePoolType = 1;
pub const OPagePoolCatalog: OPagePoolType = 2;
pub const OPagePoolTypesCount: OPagePoolType = 3;

// Undo related
pub type UndoLocation = u64;

pub type UndoLogType = c_int;
pub const UndoLogRegular: UndoLogType = 0;
pub const UndoLogRegularPageLevel: UndoLogType = 1;
pub const UndoLogSystem: UndoLogType = 2;
pub const UndoLogsCount: u32 = 3;

#[repr(C)]
pub struct UndoMeta {
    pub lastUsedLocation: pg_sys::pg_atomic_uint64,
    pub advanceReservedLocation: pg_sys::pg_atomic_uint64,
    pub writeInProgressLocation: pg_sys::pg_atomic_uint64,
    pub writtenLocation: pg_sys::pg_atomic_uint64,
    pub lastUsedUndoLocationWhenUpdatedMinLocation: pg_sys::pg_atomic_uint64,
    pub minProcTransactionRetainLocation: pg_sys::pg_atomic_uint64,
    pub minProcRetainLocation: pg_sys::pg_atomic_uint64,
    pub minRewindRetainLocation: pg_sys::pg_atomic_uint64,
    pub minProcReservedLocation: pg_sys::pg_atomic_uint64,
    pub checkpointRetainStartLocation: pg_sys::pg_atomic_uint64,
    pub checkpointRetainEndLocation: pg_sys::pg_atomic_uint64,
    pub cleanedLocation: pg_sys::pg_atomic_uint64,
    pub cleanedCheckpointStartLocation: pg_sys::pg_atomic_uint64,
    pub cleanedCheckpointEndLocation: pg_sys::pg_atomic_uint64,
    pub minUndoLocationsMutex: pg_sys::slock_t,
    pub minUndoLocationsChangeCount: u32,
    pub sysXidUndoLocationChangeCount: u32,
    pub writeInProgressChangeCount: u32,
    pub undoWriteTrancheId: c_int,
    pub undoWriteLock: pg_sys::LWLock,
    pub undoStackLocationsFlushLockTrancheId: c_int,
}

#[no_mangle]
pub static mut IsBGWriter: bool = false;
#[no_mangle]
pub static mut BGWriterNum: c_int = -1;

extern "C" {
    // Postgres globals and functions not in pg_sys
    pub fn CheckDeadLockAlert();
    pub fn SignalHandlerForShutdownRequest(postgres_signal_arg: c_int);
    pub fn procsignal_sigusr1_handler(postgres_signal_arg: c_int);
    pub static mut BgWriterDelay: c_int;
    pub static mut bgwriter_lru_maxpages: c_int;

    // OrioleDB globals and functions
    pub static mut debug_disable_bgwriter: bool;
    pub static mut undo_circular_buffer_size: usize;
    pub static mut orioledb_s3_mode: bool;

    pub fn get_ppool(pool_type: OPagePoolType) -> *mut PagePool;
    pub fn get_undo_meta_by_type(undoType: UndoLogType) -> *mut UndoMeta;
    pub fn evict_undo_to_disk(
        undoType: UndoLogType,
        targetLocation: UndoLocation,
        minProcReservedLocation: UndoLocation,
        wait: bool,
    );
    pub fn update_min_undo_locations(undoType: UndoLogType, is_checkpoint: bool, evict: bool);
    pub fn check_pending_truncates();
    pub fn s3_headers_try_eviction_cycle();
}

unsafe fn ppool_free_pages_count(pool: *mut PagePool) -> u32 {
    ((*(*pool).ops).free_pages_count.unwrap())(pool)
}

unsafe fn ppool_size(pool: *mut PagePool) -> u32 {
    ((*(*pool).ops).size.unwrap())(pool)
}

unsafe fn ppool_dirty_pages_count(pool: *mut PagePool) -> u32 {
    ((*(*pool).ops).dirty_pages_count.unwrap())(pool)
}

unsafe fn ppool_run_maintenance(
    pool: *mut PagePool,
    evict: bool,
    shutdown_requested: *mut c_int,
) -> bool {
    ((*(*pool).ops).run_maintenance.unwrap())(pool, evict, shutdown_requested)
}

unsafe fn pg_atomic_read_u64(ptr: *const pg_sys::pg_atomic_uint64) -> u64 {
    std::ptr::read_volatile(ptr as *const u64)
}

unsafe fn MemoryContextSwitchTo(context: pg_sys::MemoryContext) -> pg_sys::MemoryContext {
    let old = pg_sys::CurrentMemoryContext;
    pg_sys::CurrentMemoryContext = context;
    old
}

unsafe fn o_elog(elevel: c_int, msg: &str) {
    let domain = std::ptr::null();
    let file = b"bgwriter.rs\0".as_ptr() as *const c_char;
    let func = b"bgwriter_main\0".as_ptr() as *const c_char;
    let c_msg = CString::new(msg).unwrap();

    if pg_sys::errstart(elevel, domain) {
        let _ = pg_sys::errmsg(c_msg.as_ptr());
        pg_sys::errfinish(file, 0, func);
    }
}

unsafe fn copy_to_c_char_array(src: &str, dest: &mut [std::os::raw::c_char]) {
    let bytes = src.as_bytes();
    let len = bytes.len().min(dest.len() - 1);
    for i in 0..len {
        dest[i] = bytes[i] as std::os::raw::c_char;
    }
    dest[len] = 0;
}

#[no_mangle]
pub unsafe extern "C" fn register_bgwriter(num: c_int) {
    let mut worker: pg_sys::BackgroundWorker = std::mem::zeroed();

    worker.bgw_flags = pg_sys::BGWORKER_SHMEM_ACCESS as i32;
    worker.bgw_start_time = pg_sys::BgWorkerStartTime_BgWorkerStart_PostmasterStart;
    worker.bgw_restart_time = 0;
    worker.bgw_main_arg = pg_sys::Datum::from(num as usize);

    copy_to_c_char_array("orioledb", &mut worker.bgw_library_name);
    copy_to_c_char_array("bgwriter_main", &mut worker.bgw_function_name);
    copy_to_c_char_array(
        &format!("orioledb background writer {}", num),
        &mut worker.bgw_name,
    );
    copy_to_c_char_array("orioledb background writer", &mut worker.bgw_type);

    pg_sys::RegisterBackgroundWorker(&mut worker);
}

#[no_mangle]
pub unsafe extern "C" fn bgwriter_main(main_arg: pg_sys::Datum) {
    BGWriterNum = main_arg.value() as i32;

    pg_sys::RegisterTimeout(pg_sys::TimeoutId_DEADLOCK_TIMEOUT, Some(CheckDeadLockAlert));

    pg_sys::RelationCacheInitialize();
    pg_sys::InitCatalogCache();
    pg_sys::SharedInvalBackendInit(false);

    pg_sys::InitializeSessionUserIdStandalone();
    pg_sys::pgstat_beinit();
    #[cfg(any(feature = "pg18", feature = "pg19"))]
    {
        pg_sys::pgstat_bestart_initial();
        pg_sys::pgstat_bestart_final();
    }
    #[cfg(not(any(feature = "pg18", feature = "pg19")))]
    {
        pg_sys::pgstat_bestart();
    }

    let appname = format!("orioledb background writer {}\0", BGWriterNum);
    pg_sys::pgstat_report_appname(appname.as_ptr() as *const c_char);

    pg_sys::SetProcessingMode(pg_sys::ProcessingMode_NormalProcessing);

    pg_sys::pqsignal(libc::SIGTERM, Some(SignalHandlerForShutdownRequest));
    pg_sys::pqsignal(libc::SIGUSR1, Some(procsignal_sigusr1_handler));
    pg_sys::BackgroundWorkerUnblockSignals();

    o_elog(
        pg_sys::LOG as c_int,
        &format!("orioledb background writer {} started", BGWriterNum),
    );
    IsBGWriter = true;

    if debug_disable_bgwriter {
        o_elog(
            pg_sys::LOG as c_int,
            &format!(
                "orioledb background writer {} stopped: orioledb.debug_disable_bgwriter = True",
                BGWriterNum
            ),
        );
        return;
    }

    pg_sys::CurTransactionContext = pg_sys::AllocSetContextCreateInternal(
        pg_sys::TopMemoryContext,
        b"orioledb bgwriter current transaction context\0".as_ptr() as *const c_char,
        0,
        8 * 1024,
        8 * 1024 * 1024,
    );
    pg_sys::TopTransactionContext = pg_sys::AllocSetContextCreateInternal(
        pg_sys::TopMemoryContext,
        b"orioledb bgwriter top transaction context\0".as_ptr() as *const c_char,
        0,
        8 * 1024,
        8 * 1024 * 1024,
    );

    pg_sys::ResetLatch(pg_sys::MyLatch);

    let wake_events =
        (pg_sys::WL_LATCH_SET | pg_sys::WL_POSTMASTER_DEATH | pg_sys::WL_TIMEOUT) as i32;

    let pg_try = pgrx::PgTryBuilder::new(|| {
        let _ = MemoryContextSwitchTo(pg_sys::CurTransactionContext);
        loop {
            if pg_sys::ShutdownRequestPending as i32 != 0 {
                break;
            }

            let rc = pg_sys::WaitLatch(
                pg_sys::MyLatch,
                wake_events,
                BgWriterDelay as i64,
                pg_sys::WaitEventActivity_WAIT_EVENT_BGWRITER_MAIN as u32,
            );
            pg_sys::ResetLatch(pg_sys::MyLatch);

             if (rc & pg_sys::WL_POSTMASTER_DEATH as i32) != 0 {
                pg_sys::ShutdownRequestPending = 1;
            }

            if pg_sys::InterruptPending as i32 != 0 {
                pg_sys::ProcessInterrupts();
            }

            for poolType in 0..OPagePoolTypesCount {
                if pg_sys::ShutdownRequestPending as i32 != 0 {
                    break;
                }

                let pool = get_ppool(poolType);
                let size = ppool_size(pool);
                let mut need_eviction = ppool_free_pages_count(pool) < size / 20;
                let mut need_write = ppool_dirty_pages_count(pool) > size / 2;

                if need_eviction || need_write {
                    let mut i = 0;
                    while need_eviction || need_write {
                        let mut shutdown_pending: c_int = pg_sys::ShutdownRequestPending as c_int;
                        ppool_run_maintenance(pool, need_eviction, &mut shutdown_pending);
                        pg_sys::ShutdownRequestPending = shutdown_pending;
                        i += 1;

                        if i >= bgwriter_lru_maxpages * (pg_sys::BLCKSZ as i32 / ORIOLEDB_BLCKSZ as i32) {
                            break;
                        }

                        if pg_sys::ShutdownRequestPending as i32 != 0 {
                            break;
                        }

                        let size = ppool_size(pool);
                        need_eviction = ppool_free_pages_count(pool) < size / 20;
                        need_write = ppool_dirty_pages_count(pool) > size / 2;
                    }

                    pg_sys::MemoryContextReset(pg_sys::CurTransactionContext);
                    pg_sys::MemoryContextReset(pg_sys::TopTransactionContext);
                }
            }

            for j in 0..UndoLogsCount {
                let undo_meta = get_undo_meta_by_type(j as UndoLogType);

                let writeInProgressLocation = pg_atomic_read_u64(
                    std::ptr::addr_of!((*undo_meta).writeInProgressLocation),
                );
                let lastUsedLocation = pg_atomic_read_u64(
                    std::ptr::addr_of!((*undo_meta).lastUsedLocation),
                );

                if writeInProgressLocation + undo_circular_buffer_size as u64 <
                    lastUsedLocation + (undo_circular_buffer_size as u64) / 20
                {
                    let minProcReservedLocation = pg_atomic_read_u64(
                        std::ptr::addr_of!((*undo_meta).minProcReservedLocation),
                    );
                    let targetLocation = lastUsedLocation - (19 * undo_circular_buffer_size as u64) / 20;

                    if targetLocation < minProcReservedLocation {
                        evict_undo_to_disk(
                            j as UndoLogType,
                            targetLocation,
                            minProcReservedLocation,
                            true,
                        );
                    }
                } else {
                    debug_assert!(BGWriterNum >= 0);
                    if BGWriterNum == 0 {
                        update_min_undo_locations(j as UndoLogType, false, true);
                    }
                }
            }

            check_pending_truncates();

            if orioledb_s3_mode {
                s3_headers_try_eviction_cycle();
            }

            pg_sys::ResetLatch(pg_sys::MyLatch);
        }
        o_elog(
            pg_sys::LOG as c_int,
            &format!("orioledb bgwriter {} is shut down", BGWriterNum),
        );
    })
    .catch_others(|cause| {
        pg_sys::LockReleaseSession(1);
        cause.rethrow();
    });
    pg_try.execute();
}
