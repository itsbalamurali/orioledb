/*-------------------------------------------------------------------------
 *
 * scan.rs
 *		Scan Provider for orioledb tables.
 *
 * Copyright (c) 2021-2026, Oriole DB Inc.
 * Copyright (c) 2025-2026, Supabase Inc.
 *
 *-------------------------------------------------------------------------
 */

use pgrx::pg_sys::{self, Index, PlannerInfo, RangeTblEntry, RelOptInfo};
use crate::tableam::bitmap_scan::OPlanState;

#[repr(C)]
pub struct OCustomScanState {
    pub css: pg_sys::CustomScanState,
    pub eaCounters: [u8; 64], // OEACallsCounters size
    pub o_plan_state: *mut OPlanState,
}

#[no_mangle]
pub unsafe extern "C" fn orioledb_set_rel_pathlist_hook(
    root: *mut PlannerInfo,
    rel: *mut RelOptInfo,
    rti: Index,
    rte: *mut RangeTblEntry,
) {
    extern "C" {
        fn orioledb_set_rel_pathlist_hook_c(
            root: *mut PlannerInfo,
            rel: *mut RelOptInfo,
            rti: Index,
            rte: *mut RangeTblEntry,
        );
    }
    orioledb_set_rel_pathlist_hook_c(root, rel, rti, rte);
}

#[no_mangle]
pub unsafe extern "C" fn orioledb_set_plain_rel_pathlist_hook(
    root: *mut PlannerInfo,
    rel: *mut RelOptInfo,
    rte: *mut RangeTblEntry,
) -> bool {
    extern "C" {
        fn orioledb_set_plain_rel_pathlist_hook_c(
            root: *mut PlannerInfo,
            rel: *mut RelOptInfo,
            rte: *mut RangeTblEntry,
        ) -> bool;
    }
    orioledb_set_plain_rel_pathlist_hook_c(root, rel, rte)
}

#[no_mangle]
pub unsafe extern "C" fn is_o_custom_scan(scan: *mut pg_sys::CustomScan) -> bool {
    extern "C" {
        fn is_o_custom_scan_c(scan: *mut pg_sys::CustomScan) -> bool;
    }
    is_o_custom_scan_c(scan)
}

#[no_mangle]
pub unsafe extern "C" fn is_o_custom_scan_state(scan: *mut pg_sys::CustomScanState) -> bool {
    extern "C" {
        fn is_o_custom_scan_state_c(scan: *mut pg_sys::CustomScanState) -> bool;
    }
    is_o_custom_scan_state_c(scan)
}
