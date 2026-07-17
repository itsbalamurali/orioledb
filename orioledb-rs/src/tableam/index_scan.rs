//! index_scan.rs
//!
//! Copyright (c) 2021-2026, Oriole DB Inc.
//! Copyright (c) 2025-2026, Supabase Inc.
//!
//! IDENTIFICATION
//!   contrib/orioledb/orioledb-rs/src/tableam/index_scan.rs

use pgrx::pg_sys::{self, Relation, Snapshot, MemoryContext};
use crate::tableam::bitmap_scan::OPlanState;
use crate::tableam::descr::{OIndexDescr, OTableDescr};

#[repr(C)]
pub struct OScanState {
    pub scandesc: pg_sys::IndexScanDescData,
    pub ixNum: std::ffi::c_int,
    pub cxt: MemoryContext,
    pub scanDir: pg_sys::ScanDirection::Type,
    pub addJunk: bool,
    pub onlyCurIx: bool,
    pub returning: bool,
    pub curKeyRangeIsLoaded: bool,
    pub numPrefixExactKeys: std::ffi::c_int,
    pub exact: bool,
    #[cfg(feature = "pg18")]
    pub skipScanProbePending: bool,
    pub curKeyRange: [u8; 128], // OBTreeKeyRange
    pub iterator: *mut std::ffi::c_void, // BTreeIterator
    pub indexQuals: *mut pg_sys::List,
    pub cmd: pg_sys::CmdType::Type,
    pub oSnapshot: [u8; 16], // OSnapshot
}

#[repr(C)]
pub struct OIndexPlanState {
    pub o_plan_state: OPlanState,
    pub ostate: OScanState,
    pub stripped_indexquals: *mut pg_sys::List,
    pub onlyCurIx: bool,
    pub iss_ScanKeys: *mut pg_sys::ScanKeyData,
    pub iss_NumScanKeys: std::ffi::c_int,
    pub iss_RuntimeKeys: *mut pg_sys::IndexRuntimeKeyInfo,
    pub iss_NumRuntimeKeys: std::ffi::c_int,
    pub iss_RuntimeKeysReady: bool,
    pub iss_RuntimeContext: *mut pg_sys::ExprContext,
    pub indexRelation: Relation,
}

#[no_mangle]
#[cfg(feature = "pg18")]
pub unsafe extern "C" fn init_index_scan_state(
    o_plan_state: *mut OPlanState,
    ostate: *mut OScanState,
    index: Relation,
    econtext: *mut pg_sys::ExprContext,
    snapshot: Snapshot,
    runtimeKeys: *mut *mut pg_sys::IndexRuntimeKeyInfo,
    numRuntimeKeys: *mut std::ffi::c_int,
    scanKeys: *mut *mut pg_sys::ScanKeyData,
    numScanKeys: *mut std::ffi::c_int,
) {
    extern "C" {
        fn init_index_scan_state_c(
            o_plan_state: *mut OPlanState,
            ostate: *mut OScanState,
            index: Relation,
            econtext: *mut pg_sys::ExprContext,
            snapshot: Snapshot,
            runtimeKeys: *mut *mut pg_sys::IndexRuntimeKeyInfo,
            numRuntimeKeys: *mut std::ffi::c_int,
            scanKeys: *mut *mut pg_sys::ScanKeyData,
            numScanKeys: *mut std::ffi::c_int,
        );
    }
    init_index_scan_state_c(o_plan_state, ostate, index, econtext, snapshot, runtimeKeys, numRuntimeKeys, scanKeys, numScanKeys);
}

#[no_mangle]
#[cfg(not(feature = "pg18"))]
pub unsafe extern "C" fn init_index_scan_state(
    o_plan_state: *mut OPlanState,
    ostate: *mut OScanState,
    index: Relation,
    econtext: *mut pg_sys::ExprContext,
    runtimeKeys: *mut *mut pg_sys::IndexRuntimeKeyInfo,
    numRuntimeKeys: *mut std::ffi::c_int,
    scanKeys: *mut *mut pg_sys::ScanKeyData,
    numScanKeys: *mut std::ffi::c_int,
) {
    extern "C" {
        fn init_index_scan_state_c(
            o_plan_state: *mut OPlanState,
            ostate: *mut OScanState,
            index: Relation,
            econtext: *mut pg_sys::ExprContext,
            runtimeKeys: *mut *mut pg_sys::IndexRuntimeKeyInfo,
            numRuntimeKeys: *mut std::ffi::c_int,
            scanKeys: *mut *mut pg_sys::ScanKeyData,
            numScanKeys: *mut std::ffi::c_int,
        );
    }
    init_index_scan_state_c(o_plan_state, ostate, index, econtext, runtimeKeys, numRuntimeKeys, scanKeys, numScanKeys);
}

#[no_mangle]
pub unsafe extern "C" fn o_iterate_index(
    indexDescr: *mut OIndexDescr,
    ostate: *mut OScanState,
    tupleCsn: *mut pg_sys::CommitSeqNo,
    tupleCxt: MemoryContext,
    hint: *mut pg_sys::BTreeLocationHint,
) -> pg_sys::OTuple {
    extern "C" {
        fn o_iterate_index_c(
            indexDescr: *mut OIndexDescr,
            ostate: *mut OScanState,
            tupleCsn: *mut pg_sys::CommitSeqNo,
            tupleCxt: MemoryContext,
            hint: *mut pg_sys::BTreeLocationHint,
        ) -> pg_sys::OTuple;
    }
    o_iterate_index_c(indexDescr, ostate, tupleCsn, tupleCxt, hint)
}

#[no_mangle]
pub unsafe extern "C" fn o_index_scan_getnext(
    descr: *mut OTableDescr,
    ostate: *mut OScanState,
    tupleCsn: *mut pg_sys::CommitSeqNo,
    scan_primary: bool,
    tupleCxt: MemoryContext,
    hint: *mut pg_sys::BTreeLocationHint,
) -> pg_sys::OTuple {
    extern "C" {
        fn o_index_scan_getnext_c(
            descr: *mut OTableDescr,
            ostate: *mut OScanState,
            tupleCsn: *mut pg_sys::CommitSeqNo,
            scan_primary: bool,
            tupleCxt: MemoryContext,
            hint: *mut pg_sys::BTreeLocationHint,
        ) -> pg_sys::OTuple;
    }
    o_index_scan_getnext_c(descr, ostate, tupleCsn, scan_primary, tupleCxt, hint)
}

#[no_mangle]
pub unsafe extern "C" fn o_exec_fetch(
    ostate: *mut OScanState,
    ss: *mut pg_sys::ScanState,
) -> *mut pg_sys::TupleTableSlot {
    extern "C" {
        fn o_exec_fetch_c(ostate: *mut OScanState, ss: *mut pg_sys::ScanState) -> *mut pg_sys::TupleTableSlot;
    }
    o_exec_fetch_c(ostate, ss)
}

#[no_mangle]
pub unsafe extern "C" fn o_exec_qual(
    econtext: *mut pg_sys::ExprContext,
    qual: *mut pg_sys::ExprState,
    slot: *mut pg_sys::TupleTableSlot,
) -> bool {
    extern "C" {
        fn o_exec_qual_c(
            econtext: *mut pg_sys::ExprContext,
            qual: *mut pg_sys::ExprState,
            slot: *mut pg_sys::TupleTableSlot,
        ) -> bool;
    }
    o_exec_qual_c(econtext, qual, slot)
}

#[no_mangle]
pub unsafe extern "C" fn o_exec_project(
    projInfo: *mut pg_sys::ProjectionInfo,
    econtext: *mut pg_sys::ExprContext,
    scanTuple: *mut pg_sys::TupleTableSlot,
    innerTuple: *mut pg_sys::TupleTableSlot,
) -> *mut pg_sys::TupleTableSlot {
    extern "C" {
        fn o_exec_project_c(
            projInfo: *mut pg_sys::ProjectionInfo,
            econtext: *mut pg_sys::ExprContext,
            scanTuple: *mut pg_sys::TupleTableSlot,
            innerTuple: *mut pg_sys::TupleTableSlot,
        ) -> *mut pg_sys::TupleTableSlot;
    }
    o_exec_project_c(projInfo, econtext, scanTuple, innerTuple)
}

#[no_mangle]
pub unsafe extern "C" fn o_adjust_num_prefix_exact_keys(so: pg_sys::BTScanOpaque, numPrefixExactKeys: std::ffi::c_int) -> std::ffi::c_int {
    extern "C" {
        fn o_adjust_num_prefix_exact_keys_c(so: pg_sys::BTScanOpaque, numPrefixExactKeys: std::ffi::c_int) -> std::ffi::c_int;
    }
    o_adjust_num_prefix_exact_keys_c(so, numPrefixExactKeys)
}

#[no_mangle]
pub unsafe extern "C" fn eanalyze_counters_init(eacc: *mut std::ffi::c_void, descr: *mut OTableDescr) {
    extern "C" {
        fn eanalyze_counters_init_c(eacc: *mut std::ffi::c_void, descr: *mut OTableDescr);
    }
    eanalyze_counters_init_c(eacc, descr);
}

#[no_mangle]
pub unsafe extern "C" fn eanalyze_counters_explain(
    descr: *mut OTableDescr,
    counters: *mut std::ffi::c_void,
    es: *mut pg_sys::ExplainState,
) {
    extern "C" {
        fn eanalyze_counters_explain_c(
            descr: *mut OTableDescr,
            counters: *mut std::ffi::c_void,
            es: *mut pg_sys::ExplainState,
        );
    }
    eanalyze_counters_explain_c(descr, counters, es);
}

#[no_mangle]
pub unsafe extern "C" fn o_get_num_prefix_exact_keys(scankey: pg_sys::ScanKey, nscankeys: std::ffi::c_int) -> std::ffi::c_int {
    extern "C" {
        fn o_get_num_prefix_exact_keys_c(scankey: pg_sys::ScanKey, nscankeys: std::ffi::c_int) -> std::ffi::c_int;
    }
    o_get_num_prefix_exact_keys_c(scankey, nscankeys)
}
