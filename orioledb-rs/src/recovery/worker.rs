//! worker.rs
//!
//! Copyright (c) 2021-2026, Oriole DB Inc.
//! Copyright (c) 2025-2026, Supabase Inc.
//!
//! IDENTIFICATION
//!   contrib/orioledb/orioledb-rs/src/recovery/worker.rs

use std::ffi::{c_char, c_int, c_void, CString};
use pgrx::pg_sys;

pub type OXid = u64;
pub type CommitSeqNo = u64;
pub type OIndexNumber = u16;
pub type OTupleXactInfo = u64;
pub type UndoLocation = u64;

pub const O_PARALLEL_RECOVERY_MAGIC: u32 = 0xD42E9F13;

#[repr(C)]
pub struct shm_toc_estimator {
    pub allocated: usize,
    pub chunks: usize,
}

#[repr(C)]
pub struct ParallelRecoveryContext {
    pub nworkers: c_int,
    pub estimator: shm_toc_estimator,
    pub seg: *mut pg_sys::dsm_segment,
    pub private_memory: *mut c_void,
    pub toc: *mut pg_sys::shm_toc,
}

#[repr(C)]
pub struct RecoveryWorkerPtrs {
    pub commitPtr: pg_sys::pg_atomic_uint64,
    pub retainPtr: pg_sys::pg_atomic_uint64,
    pub flushedUndoLocCompletedCheckpointNumber: u32,
    pub hasTempFile: pg_sys::pg_atomic_flag,
}

#[repr(C)]
pub struct RecoveryUndoLocFlush {
    pub finishRequestCheckpointNumber: u32,
    pub immediateRequestCheckpointNumber: u32,
    pub completedCheckpointNumber: u32,
    pub recoveryMainCompletedCheckpointNumber: u32,
    pub exitLock: pg_sys::slock_t,
}

#[repr(u32)]
#[derive(Debug, Copy, Clone, PartialEq, Eq)]
pub enum RecoveryMsgType {
    Insert = 0,
    Update = 1,
    Delete = 2,
    BridgeErase = 3,
    Commit = 4,
    Rollback = 5,
    Finished = 6,
    Synchronize = 7,
    ToastConsistent = 8,
    Savepoint = 9,
    RollbackToSavepoint = 10,
    LeaderParallelIndexBuild = 11,
    WorkerParallelIndexBuild = 12,
    Init = 13,
    Reinsert = 14,
}

#[repr(C)]
#[derive(Copy, Clone)]
pub struct OTuple {
    pub data: *mut c_void,
    pub formatFlags: u8,
}

#[repr(C)]
pub struct OTableDescr {
    _unused: [u8; 0],
}

#[repr(C)]
pub struct OIndexDescr {
    _unused: [u8; 0],
}

#[repr(C)]
pub struct BTreeDescr {
    _unused: [u8; 0],
}

extern "C" {
    pub static mut recovery_first_queue: *mut c_void;
    pub static mut recovery_queue_data_size: u64;
    pub static mut recovery_pool_size_guc: c_int;
    pub static mut worker_ptrs: *mut RecoveryWorkerPtrs;
    pub static mut worker_ptrs_changes: *mut pg_sys::pg_atomic_uint32;
    pub static mut recovery_ptr: *mut pg_sys::pg_atomic_uint64;
    pub static mut worker_finish_count: *mut pg_sys::pg_atomic_uint32;
    pub static mut idx_worker_finish_count: *mut pg_sys::pg_atomic_uint32;
    pub static mut index_build_leader: c_int;
    pub static mut index_build_first_worker: c_int;
    pub static mut index_build_last_worker: c_int;
    pub static mut toast_consistent: bool;
    pub static mut recovery_oxid: OXid;
    pub static mut recovery_index_completed_pos: *mut pg_sys::pg_atomic_uint64;
    pub static mut recovery_index_cv: *mut pg_sys::ConditionVariable;
    pub static mut MyProcPid: c_int;
    pub static mut PostmasterPid: c_int;

    pub fn RegisterBackgroundWorker(worker: *mut pg_sys::BackgroundWorker);
    pub fn RegisterDynamicBackgroundWorker(
        worker: *mut pg_sys::BackgroundWorker,
        handle: *mut *mut pg_sys::BackgroundWorkerHandle,
    ) -> bool;
    pub fn shm_toc_initialize_estimator(estimator: *mut shm_toc_estimator);
    pub fn shm_toc_estimate(estimator: *mut shm_toc_estimator) -> usize;
    pub fn dsm_create(size: usize, flags: c_int) -> *mut pg_sys::dsm_segment;
    pub fn shm_toc_create(magic: u32, address: *mut c_void, nbytes: usize) -> *mut pg_sys::shm_toc;
    pub fn dsm_segment_address(seg: *mut pg_sys::dsm_segment) -> *mut c_void;
    pub fn dsm_detach(seg: *mut pg_sys::dsm_segment);
    pub fn palloc0(size: usize) -> *mut c_void;
    pub fn pfree(pointer: *mut c_void);
    pub fn MemoryContextAlloc(context: pg_sys::MemoryContext, size: usize) -> *mut c_void;
    pub static mut TopMemoryContext: pg_sys::MemoryContext;

    pub fn apply_modify_record(descr: *mut OTableDescr, id: *mut OIndexDescr, type_: u16, p: OTuple);
}

#[no_mangle]
pub unsafe extern "C" fn recovery_worker_register(worker_id: c_int) -> *mut pg_sys::BackgroundWorkerHandle {
    let mut worker: pg_sys::BackgroundWorker = std::mem::zeroed();
    worker.bgw_flags = pg_sys::BGWORKER_SHMEM_ACCESS as i32;
    worker.bgw_start_time = pg_sys::BgWorkerStartTime::BgWorkerStart_PostmasterStart;
    worker.bgw_restart_time = pg_sys::BGW_NEVER_RESTART;
    worker.bgw_main_arg = pg_sys::Datum::from(worker_id as usize);

    let library_name = CString::new("orioledb").unwrap();
    let function_name = CString::new("recovery_worker_main").unwrap();
    let name = CString::new(format!("orioledb recovery worker {}", worker_id)).unwrap();
    let bgw_type = CString::new("orioledb recovery worker").unwrap();

    std::ptr::copy_nonoverlapping(library_name.as_ptr(), worker.bgw_library_name.as_mut_ptr(), library_name.to_bytes().len() + 1);
    std::ptr::copy_nonoverlapping(function_name.as_ptr(), worker.bgw_function_name.as_mut_ptr(), function_name.to_bytes().len() + 1);
    std::ptr::copy_nonoverlapping(name.as_ptr(), worker.bgw_name.as_mut_ptr(), name.to_bytes().len() + 1);
    std::ptr::copy_nonoverlapping(bgw_type.as_ptr(), worker.bgw_type.as_mut_ptr(), bgw_type.to_bytes().len() + 1);

    let mut handle: *mut pg_sys::BackgroundWorkerHandle = std::ptr::null_mut();

    if MyProcPid == PostmasterPid {
        RegisterBackgroundWorker(&mut worker);
    } else {
        worker.bgw_notify_pid = MyProcPid;
        RegisterDynamicBackgroundWorker(&mut worker, &mut handle);
    }

    handle
}

#[no_mangle]
pub unsafe extern "C" fn CreateParallelRecoveryContext(nworkers: c_int) -> *mut ParallelRecoveryContext {
    let context = palloc0(std::mem::size_of::<ParallelRecoveryContext>()) as *mut ParallelRecoveryContext;
    (*context).nworkers = nworkers;
    shm_toc_initialize_estimator(&mut (*context).estimator);
    context
}

#[no_mangle]
pub unsafe extern "C" fn InitializeParallelRecoveryDSM(context: *mut ParallelRecoveryContext) {
    let segsize = shm_toc_estimate(&mut (*context).estimator);

    if (*context).nworkers > 0 {
        (*context).seg = dsm_create(segsize, pg_sys::DSM_CREATE_NULL_IF_MAXSEGMENTS as i32);
    }
    if !(*context).seg.is_null() {
        (*context).toc = shm_toc_create(
            O_PARALLEL_RECOVERY_MAGIC,
            dsm_segment_address((*context).seg),
            segsize,
        );
    } else {
        (*context).nworkers = 0;
        (*context).private_memory = MemoryContextAlloc(TopMemoryContext, segsize);
        (*context).toc = shm_toc_create(
            O_PARALLEL_RECOVERY_MAGIC,
            (*context).private_memory,
            segsize,
        );
    }
}

#[no_mangle]
pub unsafe extern "C" fn DestroyParallelRecoveryContext(context: *mut ParallelRecoveryContext) {
    if !(*context).seg.is_null() {
        dsm_detach((*context).seg);
        (*context).seg = std::ptr::null_mut();
    }
    if !(*context).private_memory.is_null() {
        pfree((*context).private_memory);
        (*context).private_memory = std::ptr::null_mut();
    }
    pfree(context as *mut c_void);
}
