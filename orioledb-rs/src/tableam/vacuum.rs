//! vacuum.rs
//!
//! Copyright (c) 2021-2026, Oriole DB Inc.
//! Copyright (c) 2025-2026, Supabase Inc.
//!
//! IDENTIFICATION
//!   contrib/orioledb/orioledb-rs/src/tableam/vacuum.rs

use pgrx::pg_sys::{self, Relation, BufferAccessStrategy};
use crate::tableam::descr::OTableDescr;

#[no_mangle]
pub unsafe extern "C" fn orioledb_vacuum_bridged_indexes(
    rel: Relation,
    descr: *mut OTableDescr,
    params: *mut pg_sys::VacuumParams,
    bstrategy: BufferAccessStrategy,
) {
    extern "C" {
        fn orioledb_vacuum_bridged_indexes_c(
            rel: Relation,
            descr: *mut OTableDescr,
            params: *mut pg_sys::VacuumParams,
            bstrategy: BufferAccessStrategy,
        );
    }
    orioledb_vacuum_bridged_indexes_c(rel, descr, params, bstrategy);
}
