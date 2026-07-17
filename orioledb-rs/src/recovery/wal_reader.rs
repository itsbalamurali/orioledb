//! wal_reader.rs
//!
//! Copyright (c) 2021-2026, Oriole DB Inc.
//! Copyright (c) 2025-2026, Supabase Inc.
//!
//! IDENTIFICATION
//!   contrib/orioledb/orioledb-rs/src/recovery/wal_reader.rs

use std::ffi::{c_char, c_int, c_void};
use pgrx::pg_sys;
use crate::recovery::worker::{OXid, CommitSeqNo, OTuple};
use crate::recovery::wal::ORelOids;
use crate::transam::oxid::OSnapshot;

pub const O_BTREE_MAX_TUPLE_SIZE: usize = 2688;

#[repr(C)]
#[derive(Copy, Clone)]
pub struct OFixedTuple {
    pub tuple: OTuple,
    pub fixedData: [c_char; O_BTREE_MAX_TUPLE_SIZE],
}

#[repr(u32)]
#[derive(Debug, Copy, Clone, PartialEq, Eq)]
pub enum WalParseResult {
    Ok = 0,
    Stop = 1,
    Eof = 2,
    BadType = 3,
    BadVersion = 4,
}

#[repr(C)]
#[derive(Copy, Clone)]
pub struct WalRecordFinishUnion {
    pub xmin: OXid,
    pub csn: CommitSeqNo,
}

#[repr(C)]
#[derive(Copy, Clone)]
pub struct WalRecordSwxidUnion {
    pub topXid: pg_sys::TransactionId,
    pub subXid: pg_sys::TransactionId,
}

#[repr(C)]
#[derive(Copy, Clone)]
pub struct WalRecordJointCommitUnion {
    pub xid: pg_sys::TransactionId,
    pub xmin: OXid,
    pub csn: CommitSeqNo,
}

#[repr(C)]
#[derive(Copy, Clone)]
pub struct WalRecordRelationUnion {
    pub treeType: u8,
    pub snapshot: OSnapshot,
    pub version: u32,
    pub base_version: u32,
}

#[repr(C)]
#[derive(Copy, Clone)]
pub struct WalRecordRelreplidentUnion {
    pub relreplident_ix_oid: pg_sys::Oid,
}

#[repr(C)]
#[derive(Copy, Clone)]
pub struct WalRecordUnlockUnion {
    pub oids: ORelOids,
    pub oldRelnode: pg_sys::Oid,
}

#[repr(C)]
#[derive(Copy, Clone)]
pub struct WalRecordTruncateUnion {
    pub oids: ORelOids,
}

#[repr(C)]
#[derive(Copy, Clone)]
pub struct WalRecordSavepointUnion {
    pub parentSubid: pg_sys::SubTransactionId,
    pub parentLogicalXid: pg_sys::TransactionId,
}

#[repr(C)]
#[derive(Copy, Clone)]
pub struct WalRecordRbToSpUnion {
    pub parentSubid: pg_sys::SubTransactionId,
    pub xmin: OXid,
    pub csn: CommitSeqNo,
}

#[repr(C)]
#[derive(Copy, Clone)]
pub struct WalRecordBridgeEraseUnion {
    pub iptr: pg_sys::ItemPointerData,
}

#[repr(C)]
#[derive(Copy, Clone)]
pub struct WalRecordModifyUnion {
    pub t1: OTuple,
    pub len1: pg_sys::OffsetNumber,
    pub t2: OTuple,
    pub len2: pg_sys::OffsetNumber,
    pub read_two_tuples: bool,
}

#[repr(C)]
#[derive(Copy, Clone)]
pub struct WalRecordDbcopyUnion {
    pub datOid: pg_sys::Oid,
    pub src_tblspc: pg_sys::Oid,
    pub dst_tblspc: pg_sys::Oid,
}

#[repr(C)]
#[derive(Copy, Clone)]
pub union WalRecordUnion {
    pub finish: WalRecordFinishUnion,
    pub swxid: WalRecordSwxidUnion,
    pub joint_commit: WalRecordJointCommitUnion,
    pub relation: WalRecordRelationUnion,
    pub relreplident: WalRecordRelreplidentUnion,
    pub unlock: WalRecordUnlockUnion,
    pub truncate: WalRecordTruncateUnion,
    pub savepoint: WalRecordSavepointUnion,
    pub rb_to_sp: WalRecordRbToSpUnion,
    pub bridge_erase: WalRecordBridgeEraseUnion,
    pub modify: WalRecordModifyUnion,
    pub dbcopy: WalRecordDbcopyUnion,
}

#[repr(C)]
#[derive(Copy, Clone)]
pub struct WalRecord {
    pub type_: u32,
    pub offset: u32,
    pub data: *mut c_char,
    pub oids: ORelOids,
    pub oxid: OXid,
    pub logicalXid: pg_sys::TransactionId,
    pub heapXid: pg_sys::TransactionId,
    pub relreplident: c_char,
    pub u: WalRecordUnion,
}

#[repr(C)]
#[derive(Debug, Copy, Clone)]
pub struct WalContainerXactInfo {
    pub xactTime: pg_sys::TimestampTz,
    pub xid: pg_sys::TransactionId,
}

#[repr(C)]
#[derive(Debug, Copy, Clone)]
pub struct WalContainerOriginInfo {
    pub id: pg_sys::RepOriginId,
    pub lsn: pg_sys::XLogRecPtr,
}

#[repr(C)]
#[derive(Debug, Copy, Clone)]
pub struct WalContainer {
    pub version: u16,
    pub flags: u8,
    pub xact_info: WalContainerXactInfo,
    pub origin_info: WalContainerOriginInfo,
}

pub type WalCheckVersionFn = Option<unsafe extern "C" fn(r: *const WalReaderState) -> c_int>;
pub type WalOnContainerFn = Option<unsafe extern "C" fn(r: *mut WalReaderState) -> c_int>;
pub type WalOnRecordFn = Option<unsafe extern "C" fn(r: *mut WalReaderState, rec: *mut WalRecord) -> c_int>;

#[repr(C)]
pub struct WalReaderState {
    pub start: *mut c_char,
    pub end: *mut c_char,
    pub ptr: *mut c_char,
    pub container: WalContainer,
    pub ctx: *mut c_void,
    pub check_version: WalCheckVersionFn,
    pub on_container: WalOnContainerFn,
    pub on_record: WalOnRecordFn,
}

extern "C" {
    pub fn build_fixed_tuples(rec: *const WalRecord, tuple1: *mut OFixedTuple, tuple2: *mut OFixedTuple);
    pub fn wal_type_name(type_: u32) -> *const c_char;
    pub fn wal_parse_container(r: *mut WalReaderState, allow_logging: bool) -> c_int;
}
