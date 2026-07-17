/*-------------------------------------------------------------------------
 *
 * wal.rs
 * 		WAL declarations for orioledb.
 *
 * Copyright (c) 2021-2026, Oriole DB Inc.
 * Copyright (c) 2025-2026, Supabase Inc.
 *
 * IDENTIFICATION
 *	  orioledb-rs/src/recovery/wal.rs
 *
 *-------------------------------------------------------------------------
 */

use std::ffi::{c_char, c_void};
use pgrx::pg_sys;
use crate::recovery::worker::{BTreeDescr, OTuple, OXid};

#[repr(C)]
#[derive(Debug, Copy, Clone, PartialEq, Eq)]
pub struct ORelOids {
    pub datoid: pg_sys::Oid,
    pub reloid: pg_sys::Oid,
    pub relnode: pg_sys::Oid,
}

pub const ORIOLEDB_WAL_VERSION: u16 = 17;
pub const FIRST_ORIOLEDB_WAL_VERSION: u16 = 16;
pub const ORIOLEDB_CONTAINER_FLAGS_WAL_VERSION: u16 = 17;

extern "C" {
    pub fn add_modify_wal_record(
        rec_type: u8,
        desc: *mut BTreeDescr,
        tuple: OTuple,
        length: pg_sys::OffsetNumber,
        relreplident: c_char,
        version: u32,
        base_version: u32,
    );

    pub fn add_bridge_erase_wal_record(
        desc: *mut BTreeDescr,
        iptr: pg_sys::ItemPointer,
        version: u32,
        base_version: u32,
    );

    pub fn add_o_tables_meta_lock_wal_record();

    pub fn add_o_tables_meta_unlock_wal_record(oids: ORelOids, oldRelnode: pg_sys::Oid);

    pub fn add_switch_logical_xid_wal_record(
        logicalXid_top: pg_sys::TransactionId,
        logicalXid_sub: pg_sys::TransactionId,
    );

    pub fn add_savepoint_wal_record(
        parentSubid: pg_sys::SubTransactionId,
        parentLogicalXid: pg_sys::TransactionId,
    );

    pub fn add_rollback_to_savepoint_wal_record(parentSubid: pg_sys::SubTransactionId);

    pub fn add_database_copy_wal_record(
        dboid: pg_sys::Oid,
        src_tblspc: pg_sys::Oid,
        dst_tblspc: pg_sys::Oid,
    );

    pub fn local_wal_is_empty() -> bool;

    pub fn flush_local_wal(isCommit: bool, withXactTime: bool) -> pg_sys::XLogRecPtr;

    pub fn wal_commit(
        oxid: OXid,
        logicalXid: pg_sys::TransactionId,
        isAutonomous: bool,
    ) -> pg_sys::XLogRecPtr;

    pub fn wal_joint_commit(
        oxid: OXid,
        logicalXid: pg_sys::TransactionId,
        xid: pg_sys::TransactionId,
        subTransaction: bool,
    ) -> pg_sys::XLogRecPtr;

    pub fn wal_after_commit();

    pub fn wal_rollback(
        oxid: OXid,
        logicalXid: pg_sys::TransactionId,
        isAutonomous: bool,
    );

    pub fn wal_emit_recovery_finish_rollback(
        oxid: OXid,
        logicalXid: pg_sys::TransactionId,
    );

    pub fn o_wal_insert(
        desc: *mut BTreeDescr,
        tuple: OTuple,
        relreplident: c_char,
        version: u32,
    );

    pub fn o_wal_update(
        desc: *mut BTreeDescr,
        tuple: OTuple,
        oldtuple: OTuple,
        relreplident: c_char,
        version: u32,
    );

    pub fn o_wal_delete(
        desc: *mut BTreeDescr,
        tuple: OTuple,
        relreplident: c_char,
        version: u32,
    );

    pub fn o_wal_delete_key(
        desc: *mut BTreeDescr,
        key: OTuple,
        is_bridge_index: bool,
        version: u32,
    );

    pub fn o_wal_reinsert(
        desc: *mut BTreeDescr,
        oldtuple: OTuple,
        newtuple: OTuple,
        relreplident: c_char,
        version: u32,
    );

    pub fn add_truncate_wal_record(oids: ORelOids);

    pub fn get_local_wal_has_material_changes() -> bool;

    pub fn set_local_wal_has_material_changes(value: bool);
}
