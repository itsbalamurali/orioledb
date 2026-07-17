/*-------------------------------------------------------------------------
 *
 * bitmap_scan.rs
 *		Bitmap scan implementation and helpers for OrioleDB.
 *
 * Copyright (c) 2021-2026, Oriole DB Inc.
 * Copyright (c) 2025-2026, Supabase Inc.
 *
 *-------------------------------------------------------------------------
 */

use pgrx::pg_sys::{self, Oid, MemoryContext};
use crate::tableam::key_bitmap::OKeyBitmap;
use crate::tableam::descr::OIndexDescr;

#[repr(C)]
pub struct OPlanState {
    pub type_: std::ffi::c_int,
    pub plan_state: *mut pg_sys::PlanState,
}

#[repr(C)]
pub struct OBitmapHeapPlanState {
    pub o_plan_state: OPlanState,
    pub bitmapqualplan: *mut pg_sys::Plan,
    pub bitmapqualplanstate: *mut pg_sys::PlanState,
    pub bitmapqualorig: *mut pg_sys::List,
    pub bitmapqualorig_state: *mut pg_sys::ExprState,
    pub typeoid: Oid,
    pub oSnapshot: [u8; 16], // OSnapshot
    pub cxt: MemoryContext,
    pub scan: *mut OBitmapScan,
    pub eaCounters: *mut std::ffi::c_void,
}

#[repr(C)]
pub struct OBitmapScan {
    pub ss: *mut pg_sys::ScanState,
    pub oSnapshot: [u8; 16],
    pub cxt: MemoryContext,
    pub typeoid: Oid,
    pub seq_scan: *mut std::ffi::c_void, // BTreeSeqScan
    pub arg: [u8; 16], // BitmapSeqScanArg
    pub bridge_iter: [u8; 48], // BridgeIterator
    pub stream_primary: bool,
    pub stream_children: *mut std::ffi::c_void,
    pub stream_nchildren: std::ffi::c_int,
    pub stream_cur: std::ffi::c_int,
    pub stream_dedup: *mut OKeyBitmap,
}

#[repr(C)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OKeyBitmapMode {
    None = 0,
    Uint64 = 1,
    Fixed = 2,
}

#[no_mangle]
pub unsafe extern "C" fn o_make_bitmap_scan(
    bitmap_state: *mut OBitmapHeapPlanState,
    ss: *mut pg_sys::ScanState,
    bitmapqualplanstate: *mut pg_sys::PlanState,
    rel: pg_sys::Relation,
    typeoid: Oid,
    oSnapshot: *mut std::ffi::c_void,
    cxt: MemoryContext,
) -> *mut OBitmapScan {
    extern "C" {
        fn o_make_bitmap_scan_c(
            bitmap_state: *mut OBitmapHeapPlanState,
            ss: *mut pg_sys::ScanState,
            bitmapqualplanstate: *mut pg_sys::PlanState,
            rel: pg_sys::Relation,
            typeoid: Oid,
            oSnapshot: *mut std::ffi::c_void,
            cxt: MemoryContext,
        ) -> *mut OBitmapScan;
    }
    o_make_bitmap_scan_c(bitmap_state, ss, bitmapqualplanstate, rel, typeoid, oSnapshot, cxt)
}

#[no_mangle]
pub unsafe extern "C" fn o_exec_bitmap_fetch(
    scan: *mut OBitmapScan,
    node: *mut pg_sys::CustomScanState,
) -> *mut pg_sys::TupleTableSlot {
    extern "C" {
        fn o_exec_bitmap_fetch_c(
            scan: *mut OBitmapScan,
            node: *mut pg_sys::CustomScanState,
        ) -> *mut pg_sys::TupleTableSlot;
    }
    o_exec_bitmap_fetch_c(scan, node)
}

#[no_mangle]
pub unsafe extern "C" fn o_free_bitmap_scan(scan: *mut OBitmapScan) {
    extern "C" {
        fn o_free_bitmap_scan_c(scan: *mut OBitmapScan);
    }
    o_free_bitmap_scan_c(scan);
}

#[no_mangle]
pub unsafe extern "C" fn o_keybitmap_pk_mode(
    primary: *mut OIndexDescr,
    fixedKeyLen: *mut std::ffi::c_int,
) -> OKeyBitmapMode {
    extern "C" {
        fn o_keybitmap_pk_mode_c(
            primary: *mut OIndexDescr,
            fixedKeyLen: *mut std::ffi::c_int,
        ) -> OKeyBitmapMode;
    }
    o_keybitmap_pk_mode_c(primary, fixedKeyLen)
}
