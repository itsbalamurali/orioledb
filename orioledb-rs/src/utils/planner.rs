// Query planner integration hooks for OrioleDB.
//
// Ported from `include/utils/planner.h` and `src/utils/planner.c`.
//
// Copyright (c) 2021-2026, Oriole DB Inc.
// Copyright (c) 2025-2026, Supabase Inc.

use pgrx::pg_sys::{List, Node, Oid, PlannedStmt};

extern "C" {
    /// Validate that `node` (a FuncExpr tree) does not use disallowed features.
    ///
    /// `hint_msg` is a caller-supplied hint displayed with the error.
    pub fn o_validate_funcexpr(node: *mut Node, hint_msg: *mut std::ffi::c_char);

    /// Validate a function by its OID, reporting errors with `hint_msg`.
    pub fn o_validate_function_by_oid(procoid: Oid, hint_msg: *mut std::ffi::c_char);

    /// Collect function references from a FuncExpr node for caching.
    pub fn o_collect_funcexpr(node: *mut Node);

    /// Collect an operator's function reference by operator OID.
    pub fn o_collect_op_by_oid(opoid: Oid);

    /// Collect a function reference by OID and input collation.
    ///
    /// Appended to `processed` (a `List *` of already-seen OIDs).
    pub fn o_collect_function_by_oid(
        procoid: Oid,
        inputcollid: Oid,
        processed: *mut *mut List,
    );

    /// Collect all function references inside a `PlannedStmt`.
    pub fn o_collect_functions_pstmt(pstmt: *mut PlannedStmt, processed: *mut *mut List);
}
