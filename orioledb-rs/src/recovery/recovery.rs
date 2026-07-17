/*-------------------------------------------------------------------------
 *
 * recovery.rs
 *		General routines for orioledb recovery.
 *
 * Copyright (c) 2021-2026, Oriole DB Inc.
 * Copyright (c) 2025-2026, Supabase Inc.
 *
 * IDENTIFICATION
 *	  orioledb-rs/src/recovery/recovery.rs
 *
 *-------------------------------------------------------------------------
 */

use std::ffi::{c_int, c_void};
use pgrx::pg_sys;
use crate::recovery::worker::{
    OXid, CommitSeqNo, OTuple, BTreeDescr, RecoveryUndoLocFlush, RecoveryWorkerPtrs
};

extern "C" {
    pub fn RecoveryInProgress() -> bool;
    pub static mut iam_recovery: bool;
    pub static mut recovery_queue_size_guc: c_int;
    pub static mut recovery_pool_size_guc: c_int;
    pub static mut recovery_idx_pool_size_guc: c_int;
    pub static mut recovery_queue_data_size: u64;
    pub static mut recovery_first_queue: *mut c_void;
    pub static mut recovery_single_process: *mut bool;
    pub static mut worker_finish_count: *mut pg_sys::pg_atomic_uint32;
    pub static mut idx_worker_finish_count: *mut pg_sys::pg_atomic_uint32;
    pub static mut worker_ptrs_changes: *mut pg_sys::pg_atomic_uint32;
    pub static mut recovery_undo_loc_flush: *mut RecoveryUndoLocFlush;
    pub static mut worker_ptrs: *mut RecoveryWorkerPtrs;
    pub static mut recovery_ptr: *mut pg_sys::pg_atomic_uint64;
    pub static mut recovery_finished_list_ptr: *mut pg_sys::pg_atomic_uint64;
    pub static mut recovery_main_retain_ptr: *mut pg_sys::pg_atomic_uint64;
    pub static mut was_in_recovery: *mut bool;
    pub static mut after_recovery_cleaned: *mut pg_sys::pg_atomic_uint32;
    pub static mut recovery_index_completed_pos: *mut pg_sys::pg_atomic_uint64;
    pub static mut recovery_index_next_pos: *mut pg_sys::pg_atomic_uint64;
    pub static mut recovery_index_cv: *mut pg_sys::ConditionVariable;
    pub static mut recoveryHeapTransactionId: pg_sys::TransactionId;
}

#[inline]
fn cachelinealign(sz: usize) -> usize {
    (sz + 63) & !63
}

#[no_mangle]
pub unsafe extern "C" fn is_recovery_process() -> bool {
    iam_recovery
}

#[no_mangle]
pub unsafe extern "C" fn is_recovery_in_progress() -> bool {
    is_recovery_process() || RecoveryInProgress()
}

#[no_mangle]
pub unsafe extern "C" fn recovery_shmem_needs() -> usize {
    let mut size: usize = 0;
    let total_workers = recovery_pool_size_guc + recovery_idx_pool_size_guc;

    size = pg_sys::add_size(size, pg_sys::mul_size(cachelinealign((recovery_queue_size_guc * 1024) as usize), total_workers as usize));
    size = pg_sys::add_size(size, cachelinealign(std::mem::size_of::<bool>()));
    size = pg_sys::add_size(size, cachelinealign(std::mem::size_of::<pg_sys::pg_atomic_uint32>()));
    size = pg_sys::add_size(size, cachelinealign(std::mem::size_of::<pg_sys::pg_atomic_uint32>()));
    size = pg_sys::add_size(size, cachelinealign(std::mem::size_of::<pg_sys::pg_atomic_uint32>()));
    size = pg_sys::add_size(size, cachelinealign(std::mem::size_of::<RecoveryUndoLocFlush>()));
    size = pg_sys::add_size(size, cachelinealign(pg_sys::mul_size(std::mem::size_of::<RecoveryWorkerPtrs>(), total_workers as usize)));
    size = pg_sys::add_size(size, cachelinealign(pg_sys::mul_size(std::mem::size_of::<pg_sys::pg_atomic_uint64>(), 3)));
    size = pg_sys::add_size(size, cachelinealign(std::mem::size_of::<bool>()));
    size = pg_sys::add_size(size, cachelinealign(std::mem::size_of::<pg_sys::pg_atomic_uint32>()));
    size = pg_sys::add_size(size, cachelinealign(std::mem::size_of::<pg_sys::pg_atomic_uint64>()));
    size = pg_sys::add_size(size, cachelinealign(std::mem::size_of::<pg_sys::pg_atomic_uint64>()));
    size = pg_sys::add_size(size, cachelinealign(std::mem::size_of::<pg_sys::ConditionVariable>()));

    size
}

#[no_mangle]
pub unsafe extern "C" fn recovery_shmem_init(mut ptr: *mut c_void, found: bool) {
    recovery_queue_data_size = (recovery_queue_size_guc * 1024) as u64;
    recovery_first_queue = ptr;

    let total_workers = recovery_pool_size_guc + recovery_idx_pool_size_guc;
    ptr = ptr.add(pg_sys::mul_size(cachelinealign(recovery_queue_data_size as usize), total_workers as usize));

    recovery_single_process = ptr as *mut bool;
    ptr = ptr.add(cachelinealign(std::mem::size_of::<bool>()));

    worker_finish_count = ptr as *mut pg_sys::pg_atomic_uint32;
    ptr = ptr.add(cachelinealign(std::mem::size_of::<pg_sys::pg_atomic_uint32>()));

    idx_worker_finish_count = ptr as *mut pg_sys::pg_atomic_uint32;
    ptr = ptr.add(cachelinealign(std::mem::size_of::<pg_sys::pg_atomic_uint32>()));

    worker_ptrs_changes = ptr as *mut pg_sys::pg_atomic_uint32;
    ptr = ptr.add(cachelinealign(std::mem::size_of::<pg_sys::pg_atomic_uint32>()));

    recovery_undo_loc_flush = ptr as *mut RecoveryUndoLocFlush;
    ptr = ptr.add(cachelinealign(std::mem::size_of::<RecoveryUndoLocFlush>()));

    worker_ptrs = ptr as *mut RecoveryWorkerPtrs;
    ptr = ptr.add(cachelinealign(pg_sys::mul_size(std::mem::size_of::<RecoveryWorkerPtrs>(), total_workers as usize)));

    recovery_ptr = ptr as *mut pg_sys::pg_atomic_uint64;
    ptr = ptr.add(cachelinealign(std::mem::size_of::<pg_sys::pg_atomic_uint64>()));

    recovery_finished_list_ptr = ptr as *mut pg_sys::pg_atomic_uint64;
    ptr = ptr.add(cachelinealign(std::mem::size_of::<pg_sys::pg_atomic_uint64>()));

    recovery_main_retain_ptr = ptr as *mut pg_sys::pg_atomic_uint64;
    ptr = ptr.add(cachelinealign(std::mem::size_of::<pg_sys::pg_atomic_uint64>()));

    was_in_recovery = ptr as *mut bool;
    ptr = ptr.add(cachelinealign(std::mem::size_of::<bool>()));

    after_recovery_cleaned = ptr as *mut pg_sys::pg_atomic_uint32;
    ptr = ptr.add(cachelinealign(std::mem::size_of::<pg_sys::pg_atomic_uint32>()));

    recovery_index_completed_pos = ptr as *mut pg_sys::pg_atomic_uint64;
    ptr = ptr.add(cachelinealign(std::mem::size_of::<pg_sys::pg_atomic_uint64>()));

    recovery_index_next_pos = ptr as *mut pg_sys::pg_atomic_uint64;
    ptr = ptr.add(cachelinealign(std::mem::size_of::<pg_sys::pg_atomic_uint64>()));

    recovery_index_cv = ptr as *mut pg_sys::ConditionVariable;
    ptr = ptr.add(cachelinealign(std::mem::size_of::<pg_sys::ConditionVariable>()));

    if !found {
        *recovery_single_process = false;
        pg_sys::pg_atomic_init_u32(worker_finish_count, 0);
        pg_sys::pg_atomic_init_u32(idx_worker_finish_count, 0);
        pg_sys::pg_atomic_init_u32(worker_ptrs_changes, 0);
        pg_sys::pg_atomic_init_u64(recovery_ptr, pg_sys::InvalidXLogRecPtr as u64);
        pg_sys::pg_atomic_init_u64(recovery_finished_list_ptr, pg_sys::InvalidXLogRecPtr as u64);
        pg_sys::pg_atomic_init_u64(recovery_main_retain_ptr, pg_sys::InvalidXLogRecPtr as u64);

        std::ptr::write_bytes(recovery_undo_loc_flush as *mut c_void, 0, std::mem::size_of::<RecoveryUndoLocFlush>());
        pg_sys::SpinLockInit(&mut (*recovery_undo_loc_flush).exitLock);

        for i in 0..total_workers {
            pg_sys::pg_atomic_init_u64(&mut (*worker_ptrs.add(i as usize)).commitPtr, pg_sys::InvalidXLogRecPtr as u64);
            pg_sys::pg_atomic_init_u64(&mut (*worker_ptrs.add(i as usize)).retainPtr, pg_sys::InvalidXLogRecPtr as u64);
            (*worker_ptrs.add(i as usize)).flushedUndoLocCompletedCheckpointNumber = 0;
            pg_sys::pg_atomic_clear_flag(&mut (*worker_ptrs.add(i as usize)).hasTempFile);
        }

        *was_in_recovery = false;
        pg_sys::pg_atomic_init_u32(after_recovery_cleaned, 0);
        pg_sys::pg_atomic_init_u64(recovery_index_completed_pos, 0);
        pg_sys::pg_atomic_init_u64(recovery_index_next_pos, 0);
        pg_sys::ConditionVariableInit(recovery_index_cv);
    }
}

// FFI Declarations for rest of recovery functions
extern "C" {
    pub fn o_recovery_start_hook();
    pub fn orioledb_redo(record: *mut pg_sys::XLogReaderState);
    pub fn o_xact_redo_hook(xid: pg_sys::TransactionId, lsn: pg_sys::XLogRecPtr, commit: bool);
    pub fn o_recovery_finish_hook(cleanup: bool);
    pub fn o_emit_recovery_finish_rollbacks();
    pub fn recovery_map_oxid_csn(oxid: OXid, found: *mut bool) -> CommitSeqNo;
    pub fn idx_workers_shutdown();
    pub fn recovery_send_worker_oids(seg_handle: pg_sys::dsm_handle);
    pub fn update_proc_retain_undo_location(worker_id: c_int);
    pub fn recovery_get_effective_replay_ptr() -> pg_sys::XLogRecPtr;
    pub fn orioledb_recovery_stops_before_hook(
        record: *mut pg_sys::XLogReaderState,
        recordXid: *mut pg_sys::TransactionId,
        recordXtime: *mut pg_sys::TimestampTz,
    ) -> bool;
    pub fn recovery_rec_insert(desc: *mut BTreeDescr, tuple: OTuple, allocated: *mut bool, size: *mut c_int) -> OTuple;
    pub fn recovery_rec_update(desc: *mut BTreeDescr, tuple: OTuple, allocated: *mut bool, size: *mut c_int) -> OTuple;
    pub fn recovery_rec_delete(desc: *mut BTreeDescr, tuple: OTuple, allocated: *mut bool, size: *mut c_int, relreplident: c_char) -> OTuple;
    pub fn recovery_rec_delete_key(desc: *mut BTreeDescr, key: OTuple, allocated: *mut bool, size: *mut c_int) -> OTuple;
    pub fn recovery_cleanup_old_files(max_chkp_num: u32, before_recovery: bool);
    pub fn recovery_load_state_from_file(worker_id: c_int, chkpnum: u32, shutdown: bool);
    pub fn check_recovery_workers_finished() -> bool;
}
