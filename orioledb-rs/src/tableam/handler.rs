//! handler.rs
//!
//! Copyright (c) 2021-2026, Oriole DB Inc.
//! Copyright (c) 2025-2026, Supabase Inc.
//!
//! IDENTIFICATION
//!   contrib/orioledb/orioledb-rs/src/tableam/handler.rs

use pgrx::pg_sys::{self, Oid, Relation, Size};
use crate::tableam::descr::OTableDescr;

#[no_mangle]
pub unsafe extern "C" fn is_orioledb_rel(rel: Relation) -> bool {
    extern "C" {
        fn is_orioledb_rel_c(rel: Relation) -> bool;
    }
    is_orioledb_rel_c(rel)
}

#[no_mangle]
pub unsafe extern "C" fn find_tree_in_descr(descr: *mut OTableDescr, oids: pg_sys::ORelOids) -> std::ffi::c_int {
    extern "C" {
        fn find_tree_in_descr_c(descr: *mut OTableDescr, oids: pg_sys::ORelOids) -> std::ffi::c_int;
    }
    find_tree_in_descr_c(descr, oids)
}

#[no_mangle]
pub unsafe extern "C" fn cleanup_btree(ix_key: [u8; 16], files: bool, fsync: bool) {
    extern "C" {
        fn cleanup_btree_c(ix_key: [u8; 16], files: bool, fsync: bool);
    }
    cleanup_btree_c(ix_key, files, fsync);
}

#[no_mangle]
pub unsafe extern "C" fn o_drop_shared_root_info(datoid: Oid, relnode: Oid) -> bool {
    extern "C" {
        fn o_drop_shared_root_info_c(datoid: Oid, relnode: Oid) -> bool;
    }
    o_drop_shared_root_info_c(datoid, relnode)
}

#[no_mangle]
pub unsafe extern "C" fn o_tableam_descr_init() {
    extern "C" {
        fn o_tableam_descr_init_c();
    }
    o_tableam_descr_init_c();
}

#[no_mangle]
pub unsafe extern "C" fn o_invalidate_descrs(datoid: Oid, reloid: Oid, relfilenode: Oid) {
    extern "C" {
        fn o_invalidate_descrs_c(datoid: Oid, reloid: Oid, relfilenode: Oid);
    }
    o_invalidate_descrs_c(datoid, reloid, relfilenode);
}

#[no_mangle]
pub unsafe extern "C" fn o_start_saving_inval_messages() -> bool {
    extern "C" {
        fn o_start_saving_inval_messages_c() -> bool;
    }
    o_start_saving_inval_messages_c()
}

#[no_mangle]
pub unsafe extern "C" fn o_stop_saving_inval_messages(was_saving: bool) {
    extern "C" {
        fn o_stop_saving_inval_messages_c(was_saving: bool);
    }
    o_stop_saving_inval_messages_c(was_saving);
}

#[no_mangle]
pub unsafe extern "C" fn o_replay_saved_inval_messages() {
    extern "C" {
        fn o_replay_saved_inval_messages_c();
    }
    o_replay_saved_inval_messages_c();
}

#[no_mangle]
pub unsafe extern "C" fn init_print_options(printOptions: *mut std::ffi::c_void, optionsArg: *mut pg_sys::VarChar) {
    extern "C" {
        fn init_print_options_c(printOptions: *mut std::ffi::c_void, optionsArg: *mut pg_sys::VarChar);
    }
    init_print_options_c(printOptions, optionsArg);
}

#[no_mangle]
pub unsafe extern "C" fn orioledb_free_rd_amcache(rel: Relation) {
    extern "C" {
        fn orioledb_free_rd_amcache_c(rel: Relation);
    }
    orioledb_free_rd_amcache_c(rel);
}

#[no_mangle]
pub unsafe extern "C" fn relation_get_descr(rel: Relation) -> *mut OTableDescr {
    extern "C" {
        fn relation_get_descr_c(rel: Relation) -> *mut OTableDescr;
    }
    relation_get_descr_c(rel)
}

#[no_mangle]
pub unsafe extern "C" fn table_descr_inc_refcnt(descr: *mut OTableDescr) {
    extern "C" {
        fn table_descr_inc_refcnt_c(descr: *mut OTableDescr);
    }
    table_descr_inc_refcnt_c(descr);
}

#[no_mangle]
pub unsafe extern "C" fn table_descr_dec_refcnt(descr: *mut OTableDescr) {
    extern "C" {
        fn table_descr_dec_refcnt_c(descr: *mut OTableDescr);
    }
    table_descr_dec_refcnt_c(descr);
}

#[no_mangle]
pub unsafe extern "C" fn orioledb_parallelscan_initialize_inner(pscan: pg_sys::ParallelTableScanDesc) -> Size {
    extern "C" {
        fn orioledb_parallelscan_initialize_inner_c(pscan: pg_sys::ParallelTableScanDesc) -> Size;
    }
    orioledb_parallelscan_initialize_inner_c(pscan)
}

#[no_mangle]
pub unsafe extern "C" fn orioledb_calculate_relation_size(rel: Relation, forkNumber: pg_sys::ForkNumber::Type, method: u8) -> i64 {
    extern "C" {
        fn orioledb_calculate_relation_size_c(rel: Relation, forkNumber: pg_sys::ForkNumber::Type, method: u8) -> i64;
    }
    orioledb_calculate_relation_size_c(rel, forkNumber, method)
}

#[no_mangle]
pub unsafe extern "C" fn orioledb_calculate_database_size(dbOid: Oid) -> i64 {
    extern "C" {
        fn orioledb_calculate_database_size_c(dbOid: Oid) -> i64;
    }
    orioledb_calculate_database_size_c(dbOid)
}
