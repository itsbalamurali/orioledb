/*-------------------------------------------------------------------------
 *
 * func.rs
 *		SQL functions implementation for orioledb module.
 *
 * Copyright (c) 2021-2026, Oriole DB Inc.
 * Copyright (c) 2025-2026, Supabase Inc.
 *
 *-------------------------------------------------------------------------
 */

use pgrx::pg_sys::{self, Datum, FunctionCallInfo};

#[no_mangle]
pub unsafe extern "C" fn orioledb_tbl_structure(fcinfo: FunctionCallInfo) -> Datum {
    extern "C" {
        fn orioledb_tbl_structure_c(fcinfo: FunctionCallInfo) -> Datum;
    }
    orioledb_tbl_structure_c(fcinfo)
}

#[no_mangle]
pub unsafe extern "C" fn orioledb_idx_structure(fcinfo: FunctionCallInfo) -> Datum {
    extern "C" {
        fn orioledb_idx_structure_c(fcinfo: FunctionCallInfo) -> Datum;
    }
    orioledb_idx_structure_c(fcinfo)
}

#[no_mangle]
pub unsafe extern "C" fn orioledb_tbl_bin_structure(fcinfo: FunctionCallInfo) -> Datum {
    extern "C" {
        fn orioledb_tbl_bin_structure_c(fcinfo: FunctionCallInfo) -> Datum;
    }
    orioledb_tbl_bin_structure_c(fcinfo)
}

#[no_mangle]
pub unsafe extern "C" fn orioledb_tbl_check(fcinfo: FunctionCallInfo) -> Datum {
    extern "C" {
        fn orioledb_tbl_check_c(fcinfo: FunctionCallInfo) -> Datum;
    }
    orioledb_tbl_check_c(fcinfo)
}

#[no_mangle]
pub unsafe extern "C" fn verify_orioledb(fcinfo: FunctionCallInfo) -> Datum {
    extern "C" {
        fn verify_orioledb_c(fcinfo: FunctionCallInfo) -> Datum;
    }
    verify_orioledb_c(fcinfo)
}

#[no_mangle]
pub unsafe extern "C" fn orioledb_compression_max_level(fcinfo: FunctionCallInfo) -> Datum {
    extern "C" {
        fn orioledb_compression_max_level_c(fcinfo: FunctionCallInfo) -> Datum;
    }
    orioledb_compression_max_level_c(fcinfo)
}

#[no_mangle]
pub unsafe extern "C" fn orioledb_tbl_compression_check(fcinfo: FunctionCallInfo) -> Datum {
    extern "C" {
        fn orioledb_tbl_compression_check_c(fcinfo: FunctionCallInfo) -> Datum;
    }
    orioledb_tbl_compression_check_c(fcinfo)
}

#[no_mangle]
pub unsafe extern "C" fn orioledb_tbl_indices(fcinfo: FunctionCallInfo) -> Datum {
    extern "C" {
        fn orioledb_tbl_indices_c(fcinfo: FunctionCallInfo) -> Datum;
    }
    orioledb_tbl_indices_c(fcinfo)
}

#[no_mangle]
pub unsafe extern "C" fn orioledb_relation_size(fcinfo: FunctionCallInfo) -> Datum {
    extern "C" {
        fn orioledb_relation_size_c(fcinfo: FunctionCallInfo) -> Datum;
    }
    orioledb_relation_size_c(fcinfo)
}

#[no_mangle]
pub unsafe extern "C" fn orioledb_tbl_are_indices_equal(fcinfo: FunctionCallInfo) -> Datum {
    extern "C" {
        fn orioledb_tbl_are_indices_equal_c(fcinfo: FunctionCallInfo) -> Datum;
    }
    orioledb_tbl_are_indices_equal_c(fcinfo)
}

#[no_mangle]
pub unsafe extern "C" fn orioledb_table_pages(fcinfo: FunctionCallInfo) -> Datum {
    extern "C" {
        fn orioledb_table_pages_c(fcinfo: FunctionCallInfo) -> Datum;
    }
    orioledb_table_pages_c(fcinfo)
}

#[no_mangle]
pub unsafe extern "C" fn orioledb_tree_stat(fcinfo: FunctionCallInfo) -> Datum {
    extern "C" {
        fn orioledb_tree_stat_c(fcinfo: FunctionCallInfo) -> Datum;
    }
    orioledb_tree_stat_c(fcinfo)
}
