/*-------------------------------------------------------------------------
 *
 * handler.rs
 *		Implementation of btree index access method handler and
 *		generic bridged index access method handler.
 *
 * Copyright (c) 2021-2026, Oriole DB Inc.
 * Copyright (c) 2025-2026, Supabase Inc.
 *
 * IDENTIFICATION
 *	  contrib/orioledb/orioledb-rs/src/indexam/handler.rs
 *
 *-------------------------------------------------------------------------
 */

use std::ffi::{c_char, c_int, c_void};
use pgrx::pg_sys;

// Type Aliases
pub type OIndexNumber = u16;
pub type CommitSeqNo = u64;
pub type OXid = u64;
pub type UndoLocation = u64;
pub type OInMemoryBlkno = u32;

// Constants
pub const F_BTHANDLER: pg_sys::Oid = 330;
pub const HEAP_TABLE_AM_OID: pg_sys::Oid = 2;
pub const BTMaxStrategyNumber: usize = 5;
pub const BTNProcs: usize = 5;
pub const BTOPTIONS_PROC: usize = 4;
pub const BTLessStrategyNumber: i16 = 1;
pub const BTEqualStrategyNumber: c_int = 3;
pub const INDEX_MAX_KEYS: usize = 32;
pub const PrimaryIndexNumber: OIndexNumber = 0;
pub const STOPEVENT_SCAN_END: c_int = 16;
pub const BTREE_DEFAULT_FILLFACTOR: c_int = 90;
pub const BTREE_MIN_FILLFACTOR: c_int = 10;
pub const RowIdAttributeNumber: pg_sys::AttrNumber = 100; // Placeholder / custom attribute number for rowid

// Struct and Enum Definitions

#[repr(C)]
#[derive(Copy, Clone)]
pub struct OTuple {
    pub data: pg_sys::Pointer,
    pub formatFlags: u8,
}

#[repr(C)]
#[derive(Copy, Clone)]
pub struct ORelOids {
    pub datoid: pg_sys::Oid,
    pub reloid: pg_sys::Oid,
    pub relnode: pg_sys::Oid,
}

#[repr(u32)]
#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub enum OIndexType {
    OIndexInvalid = 0,
    OIndexToast = 1,
    OIndexBridge = 2,
    OIndexPrimary = 3,
    OIndexUnique = 4,
    OIndexRegular = 5,
    OIndexExclusion = 6,
}

#[repr(C)]
#[derive(Copy, Clone)]
pub struct BTreeRootInfo {
    pub rootPageBlkno: OInMemoryBlkno,
    pub rootPageChangeCount: u32,
    pub metaPageBlkno: OInMemoryBlkno,
}

#[repr(C)]
#[derive(Copy, Clone)]
pub union OSmgr {
    pub array: OSmgrArray,
    pub hash: *mut c_void,
}

#[repr(C)]
#[derive(Copy, Clone)]
pub struct OSmgrArray {
    pub files: *mut pg_sys::File,
    pub filesAllocated: c_int,
}

#[repr(u32)]
#[derive(Copy, Clone)]
pub enum BTreeStorageType {
    BTreeStorageInMemory = 0,
    BTreeStorageTemporary = 1,
    BTreeStorageUnlogged = 2,
    BTreeStoragePersistence = 3,
}

#[repr(u32)]
#[derive(Copy, Clone)]
pub enum UndoLogType {
    UndoLogNone = 0,
    // Add other fields if needed, but not accessed in handler.c
}

#[repr(C)]
#[derive(Copy, Clone)]
pub struct BTreeDescr {
    pub rootInfo: BTreeRootInfo,
    pub arg: *mut c_void,
    pub smgr: OSmgr,
    pub oids: ORelOids,
    pub tablespace: pg_sys::Oid,
    pub r#type: OIndexType,
    pub ppool: *mut c_void, // PagePool
    pub compress: c_char,   // OCompress
    pub fillfactor: u8,
    pub undoType: UndoLogType,
    pub storageType: BTreeStorageType,
    // other fields omitted
}

#[repr(C)]
#[derive(Copy, Clone)]
pub struct OTupleFixedFormatSpec {
    pub natts: u16,
    pub len: u16,
}

#[repr(C)]
#[derive(Copy, Clone)]
pub struct BTreeLocationHint {
    pub blkno: OInMemoryBlkno,
    pub pageChangeCount: u32,
}

#[repr(C)]
pub struct OIndexDescr {
    pub oids: ORelOids,
    pub tableOids: ORelOids,
    pub version: u32,
    pub refcnt: c_int,
    pub valid: bool,
    pub desc: BTreeDescr,
    pub name: pg_sys::NameData,
    pub index_mctx: pg_sys::MemoryContext,
    pub expressions: *mut pg_sys::List,
    pub predicate: *mut pg_sys::List,
    pub predicate_str: *mut c_char,
    pub expressions_state: *mut pg_sys::List,
    pub predicate_state: *mut pg_sys::ExprState,
    pub econtext: *mut pg_sys::ExprContext,
    pub nonLeafTupdesc: pg_sys::TupleDesc,
    pub nonLeafSpec: OTupleFixedFormatSpec,
    pub leafTupdesc: pg_sys::TupleDesc,
    pub leafSpec: OTupleFixedFormatSpec,
    pub unique: bool,
    pub immediate: bool,
    pub nulls_not_distinct: bool,
    pub nUniqueFields: c_int,
    pub primaryIsCtid: bool,
    pub bridging: bool,
    pub fillfactor: u8,
    pub nFields: c_int,
    pub nKeyFields: c_int,
    pub nIncludedFields: c_int,
    pub fields: *mut c_void, // OIndexField
    pub nPrimaryFields: c_int,
    pub primaryFieldsAttnums: [pg_sys::AttrNumber; INDEX_MAX_KEYS],
    pub compress: c_char, // OCompress
    pub tableAttnums: *mut pg_sys::AttrNumber,
    pub maxTableAttnum: c_int,
    pub pk_tbl_field_map: *mut c_void, // AttrNumberMap
    pub pk_comparators: *mut *mut c_void, // OComparator
    pub itupdesc: pg_sys::TupleDesc,
    pub index_slot: *mut pg_sys::TupleTableSlot,
    pub old_leaf_slot: *mut pg_sys::TupleTableSlot,
    pub new_leaf_slot: *mut pg_sys::TupleTableSlot,
    pub duplicates: *mut pg_sys::List,
}

#[repr(C)]
pub struct OTable {
    pub oids: ORelOids,
    pub toast_oids: ORelOids,
    pub toast_ixversion: u32,
    pub primary_ixversion: u32,
    pub bridge_ixversion: u32,
    pub bridge_oids: ORelOids,
    pub default_compress: c_char, // OCompress
    pub primary_compress: c_char,
    pub toast_compress: c_char,
    pub index_bridging: bool,
    pub nfields: u16,
    pub primary_init_nfields: u16,
    pub nindices: u16,
    pub tid_btree_ops_oid: pg_sys::Oid,
    pub tid_hash_fn_oid: pg_sys::Oid,
    pub int2_hash_fn_oid: pg_sys::Oid,
    pub int4_hash_fn_oid: pg_sys::Oid,
    pub has_primary: bool,
    pub persistence: c_char,
    pub fillfactor: u8,
    pub data_version: u16,
    pub indices: *mut c_void, // OTableIndex
    pub fields: *mut c_void,  // OTableField
    pub missing: *mut pg_sys::AttrMissing,
    pub tablespace: pg_sys::Oid,
    pub version: u32,
    pub tbl_mctx: pg_sys::MemoryContext,
}

#[repr(C)]
pub struct OTableDescr {
    pub oids: ORelOids,
    pub version: u32,
    pub refcnt: c_int,
    pub tupdesc: pg_sys::TupleDesc,
    pub oldTuple: *mut pg_sys::TupleTableSlot,
    pub newTuple: *mut pg_sys::TupleTableSlot,
    pub indices: *mut *mut OIndexDescr,
    pub bridge: *mut OIndexDescr,
    pub toast: *mut OIndexDescr,
    pub toastable: *mut pg_sys::AttrNumber,
    pub ntoastable: c_int,
    pub nIndices: c_int,
    pub nUniqueIndices: c_int,
    pub tablespace: pg_sys::Oid,
    pub noInvalidation: bool,
}

#[repr(u32)]
#[derive(Copy, Clone)]
pub enum BTreeLeafTupleDeletedStatus {
    BTreeLeafTupleDeleted = 1,
    BTreeLeafTupleMovedPartitions = 2,
    BTreeLeafTuplePKChanged = 3,
}

#[repr(u32)]
#[derive(Copy, Clone)]
pub enum RowLockMode {
    RowLockKeyShare = 0,
    RowLockShare = 1,
    RowLockNoKeyUpdate = 2,
    RowLockUpdate = 3,
}

#[repr(u32)]
#[derive(Copy, Clone)]
pub enum OBTreeModifyCallbackAction {
    OBTreeCallbackActionDoNothing = 1,
    OBTreeCallbackActionUpdate = 2,
    OBTreeCallbackActionDelete = 3,
    OBTreeCallbackActionLock = 4,
    OBTreeCallbackActionUndo = 5,
}

#[repr(u32)]
#[derive(Copy, Clone)]
pub enum OBTreeWaitCallbackAction {
    OBTreeCallbackActionXidNoWait = 1,
    OBTreeCallbackActionXidWait = 2,
}

#[repr(C)]
pub struct BTreeModifyCallbackInfo {
    pub waitCallback: Option<
        unsafe extern "C" fn(
            desc: *mut BTreeDescr,
            oldTup: OTuple,
            newTup: *mut OTuple,
            oxid: OXid,
            prevXactInfo: u64, // OTupleXactInfo
            location: UndoLocation,
            lockMode: *mut RowLockMode,
            hint: *mut BTreeLocationHint,
            arg: *mut c_void,
        ) -> OBTreeWaitCallbackAction,
    >,
    pub modifyCallback: Option<
        unsafe extern "C" fn(
            desc: *mut BTreeDescr,
            oldTup: OTuple,
            newTup: *mut OTuple,
            oxid: OXid,
            prevXactInfo: u64,
            location: UndoLocation,
            lockMode: *mut RowLockMode,
            hint: *mut BTreeLocationHint,
            arg: *mut c_void,
        ) -> OBTreeModifyCallbackAction,
    >,
    pub modifyDeletedCallback: Option<
        unsafe extern "C" fn(
            desc: *mut BTreeDescr,
            oldTup: OTuple,
            newTup: *mut OTuple,
            oxid: OXid,
            prevXactInfo: u64,
            deleted: BTreeLeafTupleDeletedStatus,
            location: UndoLocation,
            lockMode: *mut RowLockMode,
            hint: *mut BTreeLocationHint,
            arg: *mut c_void,
        ) -> OBTreeModifyCallbackAction,
    >,
    pub needsUndoForSelfCreated: bool,
    pub arg: *mut c_void,
    pub postUndoRecorded: Option<unsafe extern "C" fn(undoLoc: UndoLocation, arg: *mut c_void)>,
}

#[repr(u32)]
#[derive(Copy, Clone, PartialEq, Eq)]
pub enum BTreeKeyType {
    BTreeKeyLeafTuple = 0,
    BTreeKeyNonLeafKey = 1,
    BTreeKeyBound = 2,
    BTreeKeyUniqueLowerBound = 3,
    BTreeKeyUniqueUpperBound = 4,
    BTreeKeyNone = 5,
    BTreeKeyPageHiKey = 6,
    BTreeKeyRightmost = 7,
}

#[repr(C)]
#[derive(Copy, Clone)]
pub struct OBTreeValueBound {
    pub value: pg_sys::Datum,
    pub r#type: pg_sys::Oid,
    pub flags: u8,
    pub comparator: *mut c_void,
    pub exclusion_fn: *mut c_void,
}

#[repr(C)]
#[derive(Copy, Clone)]
pub struct OBtreeRowKeyBound {
    pub nkeys: c_int,
    pub keynums: *mut c_int,
    pub keys: *mut OBTreeValueBound,
}

#[repr(C)]
#[derive(Copy, Clone)]
pub struct OBTreeKeyBound {
    pub nkeys: c_int,
    pub keys: [OBTreeValueBound; INDEX_MAX_KEYS],
    pub n_row_keys: c_int,
    pub row_keys: *mut OBtreeRowKeyBound,
}

#[repr(C)]
#[derive(Copy, Clone)]
pub struct OBTreeKeyRange {
    pub empty: bool,
    pub low: OBTreeKeyBound,
    pub high: OBTreeKeyBound,
}

#[repr(C)]
pub struct BTreeIterator {
    // opaque structure
}

#[repr(C)]
pub struct OSnapshot {
    pub csn: CommitSeqNo,
    pub xlogptr: pg_sys::XLogRecPtr,
    pub xmin: pg_sys::XLogRecPtr,
    pub cid: pg_sys::CommandId,
}

#[repr(C)]
pub struct OScanState {
    pub scandesc: pg_sys::IndexScanDescData,
    pub ixNum: OIndexNumber,
    pub cxt: pg_sys::MemoryContext,
    pub scanDir: pg_sys::ScanDirection,
    pub addJunk: bool,
    pub onlyCurIx: bool,
    pub returning: bool,
    pub curKeyRangeIsLoaded: bool,
    pub numPrefixExactKeys: c_int,
    pub exact: bool,
    #[cfg(any(feature = "pg18", feature = "pg19"))]
    pub skipScanProbePending: bool,
    pub curKeyRange: OBTreeKeyRange,
    pub iterator: *mut BTreeIterator,
    pub indexQuals: *mut pg_sys::List,
    pub cmd: pg_sys::CmdType,
    pub oSnapshot: OSnapshot,
}

#[repr(u32)]
#[derive(Copy, Clone)]
pub enum BTreeOperationType {
    BTreeOperationInsert = 0,
    BTreeOperationLock = 1,
    BTreeOperationUpdate = 2,
    BTreeOperationDelete = 3,
}

#[repr(C)]
#[derive(Copy, Clone)]
pub struct OTableModifyResult {
    pub success: bool,
    pub action: BTreeOperationType,
    pub failedIxNum: OIndexNumber,
    pub oldTuple: *mut pg_sys::TupleTableSlot,
}

#[repr(u32)]
#[derive(Copy, Clone, PartialEq, Eq)]
pub enum OBTreeModifyResult {
    OBTreeModifyResultInserted = 1,
    OBTreeModifyResultUpdated = 2,
    OBTreeModifyResultDeleted = 3,
    OBTreeModifyResultLocked = 4,
    OBTreeModifyResultFound = 5,
    OBTreeModifyResultNotFound = 6,
}

#[repr(C)]
pub struct BridgedIndexAmRoutine {
    pub original_routine: *mut pg_sys::IndexAmRoutine,
    pub routine: pg_sys::IndexAmRoutine,
    pub amhandler: pg_sys::Oid,
}

#[repr(C)]
#[derive(Copy, Clone)]
pub struct ORowIdAddendumCtid {
    pub hint: BTreeLocationHint,
    pub csn: CommitSeqNo,
    pub version: u32,
}

#[repr(C)]
#[derive(Copy, Clone)]
pub struct ORowIdAddendumNonCtid {
    pub hint: BTreeLocationHint,
    pub csn: CommitSeqNo,
    pub flags: u8,
}

#[repr(C)]
#[derive(Copy, Clone)]
pub struct ORowIdBridgeData {
    pub bridgeCtid: pg_sys::ItemPointerData,
    pub bridgeChanged: bool,
}

#[repr(C)]
pub struct OTableSlot {
    pub base: pg_sys::TupleTableSlot,
    pub data: *mut c_char,
    pub to_toast: *mut c_char,
    pub vfree: *mut bool,
    pub detoasted: *mut pg_sys::Datum,
    pub tuple: OTuple,
    pub descr: *mut OTableDescr,
    pub rowid: *mut pg_sys::bytea,
    pub csn: CommitSeqNo,
    pub ixnum: c_int,
    pub leafTuple: bool,
    pub bridgeChanged: bool,
    pub version: u32,
    pub state: OTupleReaderState,
    pub hint: BTreeLocationHint,
    pub bridge_ctid: pg_sys::ItemPointerData,
}

#[repr(C)]
#[derive(Copy, Clone)]
pub struct OTupleReaderState {
    pub desc: pg_sys::TupleDesc,
    pub tp: *mut c_char,
    pub bp: *mut pg_sys::bits8,
    pub off: u32,
    pub attnum: u16,
    pub natts: u16,
    pub hasnulls: bool,
    pub slow: bool,
}

// Global static variables & extern declarations

extern "C" {
    // OrioleDB functions
    pub fn orioledb_check_shmem();
    pub fn relation_get_descr(rel: pg_sys::Relation) -> *mut OTableDescr;
    pub fn o_index_rel_get_ix_type(rel: pg_sys::Relation) -> OIndexType;
    pub fn o_fetch_index_descr(
        oids: ORelOids,
        ix_type: OIndexType,
        create: bool,
        tree: *mut *mut c_void,
    ) -> *mut OIndexDescr;
    pub fn o_fetch_table_descr(oids: ORelOids) -> *mut OTableDescr;
    pub fn o_tables_get(oids: ORelOids) -> *mut OTable;
    pub fn drop_primary_index(heap: pg_sys::Relation, o_table: *mut OTable);
    pub fn redefine_pkey_for_rel(heap: pg_sys::Relation);
    pub fn o_define_index_validate(
        tbl_oids: ORelOids,
        index: pg_sys::Relation,
        indexInfo: *mut pg_sys::IndexInfo,
        descr: *mut OTableDescr,
    );
    pub fn o_define_index(
        heap: pg_sys::Relation,
        index: pg_sys::Relation,
        old_reloid: pg_sys::Oid,
        reindex: bool,
        old_ix_num: OIndexNumber,
        oldTblRelnode: pg_sys::Oid,
        result: *mut pg_sys::IndexBuildResult,
    );
    pub fn o_tuple_set_version(spec: *mut OTupleFixedFormatSpec, tuple: *mut OTuple, version: u32);
    pub fn o_tuple_get_version(tuple: OTuple) -> u32;
    pub fn o_fastgetattr(
        tup: OTuple,
        attnum: c_int,
        tupdesc: pg_sys::TupleDesc,
        spec: *mut OTupleFixedFormatSpec,
        isnull: *mut bool,
    ) -> pg_sys::Datum;
    pub fn o_form_tuple(
        tupdesc: pg_sys::TupleDesc,
        spec: *mut OTupleFixedFormatSpec,
        version: u32,
        values: *mut pg_sys::Datum,
        isnull: *mut bool,
        formatFlags: *mut u8,
    ) -> OTuple;
    pub fn tts_orioledb_store_tuple(
        slot: *mut pg_sys::TupleTableSlot,
        tuple: OTuple,
        descr: *mut OTableDescr,
        csn: CommitSeqNo,
        ixNum: OIndexNumber,
        shouldFree: bool,
        hint: *mut BTreeLocationHint,
    );
    pub fn tts_orioledb_store_non_leaf_tuple(
        slot: *mut pg_sys::TupleTableSlot,
        tuple: OTuple,
        descr: *mut OTableDescr,
        csn: CommitSeqNo,
        ixNum: OIndexNumber,
        shouldFree: bool,
        hint: *mut BTreeLocationHint,
    );
    pub fn fill_current_oxid_osnapshot(oxid: *mut OXid, o_snapshot: *mut OSnapshot);
    pub fn o_tbl_index_insert(
        descr: *mut OTableDescr,
        index: *mut OIndexDescr,
        tuple: *mut OTuple,
        slot: *mut pg_sys::TupleTableSlot,
        oxid: OXid,
        csn: CommitSeqNo,
        callbackInfo: *mut BTreeModifyCallbackInfo,
        checkUnique: pg_sys::IndexUniqueCheck,
    ) -> OBTreeModifyResult;
    pub fn o_update_secondary_index(
        index_descr: *mut OIndexDescr,
        ix_num: OIndexNumber,
        new_valid: bool,
        old_valid: bool,
        new_slot: *mut pg_sys::TupleTableSlot,
        new_tuple: OTuple,
        old_slot: *mut pg_sys::TupleTableSlot,
        oxid: OXid,
        csn: CommitSeqNo,
        checkUnique: pg_sys::IndexUniqueCheck,
    ) -> OTableModifyResult;
    pub fn o_tbl_index_delete(
        index_descr: *mut OIndexDescr,
        ix_num: OIndexNumber,
        slot: *mut pg_sys::TupleTableSlot,
        oxid: OXid,
        csn: CommitSeqNo,
    ) -> OTableModifyResult;
    pub fn o_index_scan_getnext(
        descr: *mut OTableDescr,
        ostate: *mut OScanState,
        tupleCsn: *mut CommitSeqNo,
        scan_primary: bool,
        tupleCxt: pg_sys::MemoryContext,
        hint: *mut BTreeLocationHint,
    ) -> OTuple;
    pub fn btree_iterator_free(iterator: *mut BTreeIterator);
    pub fn o_parse_compress(value: *const c_char) -> *mut c_char;
    pub fn validate_compress(compress: *mut c_char, r#type: *const c_char);
    pub fn o_new_tuple_size(
        descr: pg_sys::TupleDesc,
        spec: *mut OTupleFixedFormatSpec,
        arg1: *mut c_void,
        arg2: *mut c_void,
        version: u32,
        values: *mut pg_sys::Datum,
        isnull: *mut bool,
        arg3: *mut c_void,
    ) -> c_int;
    pub fn o_tuple_fill(
        descr: pg_sys::TupleDesc,
        spec: *mut OTupleFixedFormatSpec,
        tuple: *mut OTuple,
        size: c_int,
        arg1: *mut c_void,
        arg2: *mut c_void,
        version: u32,
        values: *mut pg_sys::Datum,
        isnull: *mut bool,
        arg3: *mut c_void,
    );

    // Stop events
    pub static mut enable_stopevents: bool;
    pub static mut trace_stopevents: bool;
    pub fn handle_stopevent(event_id: c_int, params: *mut pg_sys::Jsonb);

    // Postgres / Planner FFI globals and functions
    pub fn clauselist_selectivity(
        root: *mut pg_sys::PlannerInfo,
        clauses: *mut pg_sys::List,
        varRelid: pg_sys::Oid,
        jointype: pg_sys::JoinType,
        sjinfo: *mut pg_sys::SpecialJoinInfo,
    ) -> pg_sys::Selectivity;
    pub fn genericcostestimate(
        root: *mut pg_sys::PlannerInfo,
        path: *mut pg_sys::IndexPath,
        loop_count: f64,
        costs: *mut pg_sys::GenericCosts,
    );

    #[cfg(any(feature = "pg17", feature = "pg18", feature = "pg19"))]
    pub fn estimate_array_length(root: *mut pg_sys::PlannerInfo, arrayexpr: *mut pg_sys::Node) -> c_int;
    #[cfg(not(any(feature = "pg17", feature = "pg18", feature = "pg19")))]
    pub fn estimate_array_length(arrayexpr: *mut pg_sys::Node) -> c_int;

    pub fn add_predicate_to_index_quals(
        index: *mut pg_sys::IndexOptInfo,
        indexQuals: *mut pg_sys::List,
    ) -> *mut pg_sys::List;
    pub fn get_op_opfamily_strategy(opno: pg_sys::Oid, opfamily: pg_sys::Oid) -> c_int;
    pub fn get_opfamily_member(
        opfamily: pg_sys::Oid,
        lefttype: pg_sys::Oid,
        righttype: pg_sys::Oid,
        strategy: i16,
    ) -> pg_sys::Oid;
    pub fn get_attstatsslot(
        sslot: *mut pg_sys::AttStatsSlot,
        statstuple: pg_sys::HeapTuple,
        reqkind: c_int,
        reqop: pg_sys::Oid,
        flags: c_int,
    ) -> bool;
    pub fn free_attstatsslot(sslot: *mut pg_sys::AttStatsSlot);
    pub fn ReleaseVariableStats(vardata: pg_sys::VariableStatData);

    pub static mut cpu_operator_cost: f64;
    pub static mut get_relation_stats_hook: *mut c_void;
    pub static mut get_index_stats_hook: *mut c_void;

    // Standard BTree handlers
    pub fn btbuild(
        heap: pg_sys::Relation,
        index: pg_sys::Relation,
        indexInfo: *mut pg_sys::IndexInfo,
    ) -> *mut pg_sys::IndexBuildResult;
    pub fn btbuildempty(index: pg_sys::Relation);
    pub fn btinsert(
        rel: pg_sys::Relation,
        values: *mut pg_sys::Datum,
        isnull: *mut bool,
        tupleid: pg_sys::Datum,
        heapRel: pg_sys::Relation,
        checkUnique: pg_sys::IndexUniqueCheck,
        indexUnchanged: bool,
        indexInfo: *mut pg_sys::IndexInfo,
    ) -> bool;
    pub fn btbulkdelete(
        info: *mut pg_sys::IndexVacuumInfo,
        stats: *mut pg_sys::IndexBulkDeleteResult,
        callback: pg_sys::IndexBulkDeleteCallback,
        callback_state: *mut c_void,
    ) -> *mut pg_sys::IndexBulkDeleteResult;
    pub fn btvacuumcleanup(
        info: *mut pg_sys::IndexVacuumInfo,
        stats: *mut pg_sys::IndexBulkDeleteResult,
    ) -> *mut pg_sys::IndexBulkDeleteResult;
    pub fn btcanreturn(index: pg_sys::Relation, attno: c_int) -> bool;
    pub fn btrescan(
        scan: pg_sys::IndexScanDesc,
        scankey: pg_sys::ScanKey,
        nscankeys: c_int,
        orderbys: pg_sys::ScanKey,
        norderbys: c_int,
    );
    pub fn btgettuple(scan: pg_sys::IndexScanDesc, dir: pg_sys::ScanDirection) -> bool;
    pub fn btgetbitmap(scan: pg_sys::IndexScanDesc, tbm: *mut pg_sys::TIDBitmap) -> i64;
    pub fn btendscan(scan: pg_sys::IndexScanDesc);
    pub fn btbeginscan(rel: pg_sys::Relation, nkeys: c_int, norderbys: c_int) -> pg_sys::IndexScanDesc;

    // OrioleDB Global vars
    pub static mut o_reuse_indices: *mut pg_sys::List;
    pub static mut reindex_list: *mut pg_sys::List;
    pub static mut in_nontransactional_truncate: bool;
    pub static mut o_saved_relrewrite: pg_sys::Oid;

    // Global in-progress snapshots defined in oxid
    pub static mut o_in_progress_snapshot: OSnapshot;
    pub static mut o_non_deleted_snapshot: OSnapshot;

    // Reloptions registration functions (part of pg_sys, but declaring just in case)
    pub fn init_local_reloptions(relopts: *mut pg_sys::local_relopts, relopt_struct_size: pg_sys::Size);
    pub fn add_local_int_reloption(
        relopts: *mut pg_sys::local_relopts,
        name: *const c_char,
        desc: *const c_char,
        default_val: c_int,
        min_val: c_int,
        max_val: c_int,
        offset: pg_sys::Size,
    );
    pub fn add_local_real_reloption(
        relopts: *mut pg_sys::local_relopts,
        name: *const c_char,
        desc: *const c_char,
        default_val: f64,
        min_val: f64,
        max_val: f64,
        offset: pg_sys::Size,
    );
    pub fn add_local_bool_reloption(
        relopts: *mut pg_sys::local_relopts,
        name: *const c_char,
        desc: *const c_char,
        default_val: bool,
        offset: pg_sys::Size,
    );
    pub fn add_local_string_reloption(
        relopts: *mut pg_sys::local_relopts,
        name: *const c_char,
        desc: *const c_char,
        default_val: *const c_char,
        validate: Option<unsafe extern "C" fn(*const c_char)>,
        fill: Option<unsafe extern "C" fn(*const c_char) -> *mut c_void>,
        offset: pg_sys::Size,
    );
    pub fn build_local_reloptions(
        relopts: *mut pg_sys::local_relopts,
        reloptions: pg_sys::Datum,
        validate: bool,
    ) -> *mut pg_sys::bytea;
}

// Local global list of bridged AMs
static mut bridged_ams: *mut pg_sys::List = std::ptr::null_mut();

// Helper functions mimicking C macros

#[inline]
pub unsafe fn ORelOidsSetFromRel(oids: &mut ORelOids, rel: pg_sys::Relation) {
    oids.datoid = pg_sys::MyDatabaseId;
    oids.reloid = (*rel).rd_id;
    #[cfg(any(feature = "pg16", feature = "pg17", feature = "pg18", feature = "pg19"))]
    {
        oids.relnode = (*rel).rd_locator.relNumber;
    }
    #[cfg(not(any(feature = "pg16", feature = "pg17", feature = "pg18", feature = "pg19")))]
    {
        oids.relnode = (*rel).rd_node.relNode;
    }
}

#[inline]
pub unsafe fn GET_PRIMARY(descr: *mut OTableDescr) -> *mut OIndexDescr {
    *((*descr).indices)
}

#[inline]
pub unsafe fn OidIsValid(oid: pg_sys::Oid) -> bool {
    oid != pg_sys::InvalidOid
}

#[inline]
pub unsafe fn CStringGetDatum(s: *const c_char) -> pg_sys::Datum {
    s as pg_sys::Datum
}

#[inline]
pub unsafe fn DatumGetPointer(d: pg_sys::Datum) -> pg_sys::Pointer {
    d as pg_sys::Pointer
}

#[inline]
pub unsafe fn PointerGetDatum(p: pg_sys::Pointer) -> pg_sys::Datum {
    p as pg_sys::Datum
}

#[inline]
pub unsafe fn lfirst(lc: *mut pg_sys::ListCell) -> *mut c_void {
    (*lc).value.ptr_value
}

#[inline]
pub unsafe fn lfirst_int(lc: *mut pg_sys::ListCell) -> c_int {
    (*lc).value.int_value as c_int
}

#[inline]
pub unsafe fn lnext(lc: *mut pg_sys::ListCell) -> *mut pg_sys::ListCell {
    if lc.is_null() {
        std::ptr::null_mut()
    } else {
        (*lc).next
    }
}

#[inline]
pub unsafe fn linitial_int(l: *mut pg_sys::List) -> c_int {
    let head = pg_sys::list_head(l);
    lfirst_int(head)
}

#[inline]
pub unsafe fn list_make1_oid(oid: pg_sys::Oid) -> *mut pg_sys::List {
    pg_sys::lcons_oid(oid, std::ptr::null_mut())
}

#[inline]
pub unsafe fn STOPEVENTS_ENABLED() -> bool {
    enable_stopevents || trace_stopevents
}

#[inline]
pub unsafe fn STOPEVENT(event_id: c_int, params: *mut pg_sys::Jsonb) {
    if STOPEVENTS_ENABLED() {
        handle_stopevent(event_id, params);
    }
}

// BTree Handler Implementation

unsafe extern "C" fn orioledb_btree_handler() -> *mut pg_sys::IndexAmRoutine {
    let amroutine = pg_sys::newNode(
        std::mem::size_of::<pg_sys::IndexAmRoutine>(),
        pg_sys::NodeTag_T_IndexAmRoutine,
    ) as *mut pg_sys::IndexAmRoutine;

    orioledb_check_shmem();

    (*amroutine).amstrategies = BTMaxStrategyNumber as c_int;
    (*amroutine).amsupport = BTNProcs as c_int;
    (*amroutine).amoptsprocnum = BTOPTIONS_PROC as c_int;
    (*amroutine).amcanorder = true;
    (*amroutine).amcanorderbyop = false;
    (*amroutine).amcanbackward = false;
    (*amroutine).amcanunique = true;
    (*amroutine).amcanmulticol = true;
    (*amroutine).amoptionalkey = true;
    (*amroutine).amsearcharray = true;
    (*amroutine).amsearchnulls = true;
    (*amroutine).amstorage = false;
    (*amroutine).amclusterable = true;
    (*amroutine).ampredlocks = true;
    (*amroutine).amcanparallel = false;
    (*amroutine).amcaninclude = true;
    (*amroutine).amusemaintenanceworkmem = false;
    (*amroutine).amsummarizing = false;
    (*amroutine).ammvccaware = true;
    (*amroutine).amparallelvacuumoptions =
        (pg_sys::VACUUM_OPTION_PARALLEL_BULKDEL | pg_sys::VACUUM_OPTION_PARALLEL_COND_CLEANUP) as c_int;
    (*amroutine).amkeytype = pg_sys::InvalidOid;

    (*amroutine).ambuild = Some(orioledb_ambuild);
    (*amroutine).amreuse = Some(orioledb_amreuse);
    (*amroutine).ambuildempty = Some(orioledb_ambuildempty);
    (*amroutine).aminsert = None;
    (*amroutine).aminsertextended = Some(orioledb_aminsert);
    (*amroutine).amupdate = Some(orioledb_amupdate);
    (*amroutine).amdelete = Some(orioledb_amdelete);
    (*amroutine).ambulkdelete = Some(orioledb_ambulkdelete);
    (*amroutine).amvacuumcleanup = Some(orioledb_amvacuumcleanup);
    (*amroutine).amcanreturn = Some(orioledb_amcanreturn);
    (*amroutine).amcostestimate = Some(orioledb_amcostestimate);
    #[cfg(any(feature = "pg18", feature = "pg19"))]
    {
        (*amroutine).amgettreeheight = None;
    }
    (*amroutine).amoptions = Some(orioledb_amoptions);
    (*amroutine).amproperty = Some(orioledb_amproperty);
    (*amroutine).ambuildphasename = Some(orioledb_ambuildphasename);
    (*amroutine).amvalidate = Some(orioledb_amvalidate);
    (*amroutine).amadjustmembers = Some(orioledb_amadjustmembers);
    (*amroutine).ambeginscan = Some(orioledb_ambeginscan);
    (*amroutine).amrescan = Some(orioledb_amrescan);
    (*amroutine).amgettuple = Some(orioledb_amgettuple);
    (*amroutine).amgetbitmap = Some(orioledb_amgetbitmap);
    (*amroutine).amendscan = Some(orioledb_amendscan);
    (*amroutine).ammarkpos = None;
    (*amroutine).amrestrpos = None;
    (*amroutine).amestimateparallelscan = Some(orioledb_amestimateparallelscan);
    (*amroutine).aminitparallelscan = Some(orioledb_aminitparallelscan);
    (*amroutine).amparallelrescan = Some(orioledb_amparallelrescan);

    amroutine
}

#[no_mangle]
pub unsafe extern "C" fn orioledb_indexam_routine_hook(
    tamoid: pg_sys::Oid,
    amhandler: pg_sys::Oid,
) -> *mut pg_sys::IndexAmRoutine {
    static mut orioledb_tam_oid: pg_sys::Oid = pg_sys::InvalidOid;

    if tamoid == HEAP_TABLE_AM_OID {
        return std::ptr::null_mut();
    }

    if !OidIsValid(orioledb_tam_oid) {
        let key1 = CStringGetDatum(b"orioledb\0".as_ptr() as *const c_char);
        orioledb_tam_oid = pg_sys::GetSysCacheOid(
            pg_sys::SysCacheIdentifier_AMNAME as c_int,
            pg_sys::Anum_pg_am_oid as pg_sys::AttrNumber,
            key1,
            0,
            0,
            0,
        );
    }

    if tamoid == orioledb_tam_oid {
        if amhandler == F_BTHANDLER {
            return orioledb_btree_handler();
        } else {
            let mut amroutine: *mut pg_sys::IndexAmRoutine = std::ptr::null_mut();
            let mut lc = pg_sys::list_head(bridged_ams);
            while !lc.is_null() {
                let bridged = lfirst(lc) as *mut BridgedIndexAmRoutine;
                if (*bridged).amhandler == amhandler {
                    amroutine = pg_sys::palloc0(std::mem::size_of::<pg_sys::IndexAmRoutine>()) as *mut pg_sys::IndexAmRoutine;
                    std::ptr::copy_nonoverlapping(&((*bridged).routine), amroutine, 1);
                    break;
                }
                lc = lnext(lc);
            }

            if amroutine.is_null() {
                let old_mcxt = pg_sys::MemoryContextSwitchTo(pg_sys::TopMemoryContext);
                let bridged = pg_sys::palloc0(std::mem::size_of::<BridgedIndexAmRoutine>()) as *mut BridgedIndexAmRoutine;
                let datum = pg_sys::OidFunctionCall0Coll(amhandler, pg_sys::InvalidOid);
                bridged_ams = pg_sys::lappend(bridged_ams, bridged as *mut c_void);
                (*bridged).amhandler = amhandler;
                (*bridged).original_routine = DatumGetPointer(datum) as *mut pg_sys::IndexAmRoutine;
                (*bridged).routine = *(*bridged).original_routine;
                (*bridged).routine.ambuild = Some(bridged_ambuild);
                (*bridged).routine.aminsertextended = Some(bridged_aminsert);
                (*bridged).routine.ambeginscan = Some(bridged_ambeginscan);
                pg_sys::MemoryContextSwitchTo(old_mcxt);

                amroutine = pg_sys::palloc0(std::mem::size_of::<pg_sys::IndexAmRoutine>()) as *mut pg_sys::IndexAmRoutine;
                std::ptr::copy_nonoverlapping(&((*bridged).routine), amroutine, 1);
            }
            return amroutine;
        }
    }

    std::ptr::null_mut()
}

unsafe extern "C" fn orioledb_amreuse(index: pg_sys::Relation) {
    if !o_reuse_indices.is_null() {
        o_reuse_indices = pg_sys::lappend_oid(o_reuse_indices, (*index).rd_id);
    } else {
        o_reuse_indices = list_make1_oid((*index).rd_id);
    }
}

unsafe extern "C" fn orioledb_ambuild(
    heap: pg_sys::Relation,
    index: pg_sys::Relation,
    indexInfo: *mut pg_sys::IndexInfo,
) -> *mut pg_sys::IndexBuildResult {
    let mut reindex = false;
    let mut result: *mut pg_sys::IndexBuildResult;
    let options = (*index).rd_options as *mut OBTOptions;

    if !options.is_null() && !(*options).orioledb_index {
        let descr = relation_get_descr(heap);
        if descr.is_null() {
            result = pg_sys::palloc(std::mem::size_of::<pg_sys::IndexBuildResult>()) as *mut pg_sys::IndexBuildResult;
            (*result).heap_tuples = 0.0;
            (*result).index_tuples = 0.0;
            return result;
        } else {
            return btbuild(heap, index, indexInfo);
        }
    }

    let relname = pg_sys::makeString((*(*index).rd_rel).relname.data.as_ptr() as *mut c_char);
    if !in_nontransactional_truncate && pg_sys::list_member(reindex_list, relname as *const c_void) {
        reindex = true;
        reindex_list = pg_sys::list_delete(reindex_list, relname as *mut c_void);
    }

    btbuild(heap, index, indexInfo);

    result = pg_sys::palloc(std::mem::size_of::<pg_sys::IndexBuildResult>()) as *mut pg_sys::IndexBuildResult;
    (*result).heap_tuples = 0.0;
    (*result).index_tuples = 0.0;

    if in_nontransactional_truncate || !OidIsValid(o_saved_relrewrite) {
        let mut tbl_oids = ORelOids { datoid: 0, reloid: 0, relnode: 0 };
        ORelOidsSetFromRel(&mut tbl_oids, heap);
        let o_table = o_tables_get(tbl_oids);

        if (*(*index).rd_index).indisprimary && !o_table.is_null() && (*o_table).has_primary {
            drop_primary_index(heap, o_table);
            redefine_pkey_for_rel(heap);
        } else {
            if !in_nontransactional_truncate {
                o_define_index_validate(tbl_oids, index, indexInfo, std::ptr::null_mut());
            }
            o_define_index(
                heap,
                index,
                pg_sys::InvalidOid,
                reindex,
                pg_sys::InvalidIndexNumber,
                pg_sys::InvalidOid,
                result,
            );
        }
    }

    result
}

unsafe extern "C" fn orioledb_ambuildempty(index: pg_sys::Relation) {
    btbuildempty(index);
}

unsafe extern "C" fn o_insert_callback(
    descr: *mut BTreeDescr,
    tup: OTuple,
    newtup: *mut OTuple,
    _oxid: OXid,
    xactInfo: u64,
    _deleted: BTreeLeafTupleDeletedStatus,
    _location: UndoLocation,
    _lock_mode: *mut RowLockMode,
    _hint: *mut BTreeLocationHint,
    arg: *mut c_void,
) -> OBTreeModifyCallbackAction {
    let oslot = arg as *mut OTableSlot;
    if (*descr).r#type == OIndexType::OIndexPrimary && ((xactInfo & 0xFFFF) != 0) { // XACT_INFO_OXID_IS_CURRENT analogue check
        let id = (*descr).arg as *mut OIndexDescr;
        o_tuple_set_version(&mut (*id).leafSpec, newtup, o_tuple_get_version(tup) + 1);
        (*oslot).tuple = *newtup;
    }
    OBTreeModifyCallbackAction::OBTreeCallbackActionUpdate
}

unsafe fn o_report_duplicate(rel: pg_sys::Relation, id: *mut OIndexDescr, slot: *mut pg_sys::TupleTableSlot) {
    let is_ctid = (*id).primaryIsCtid;
    let is_primary = (*id).desc.r#type == OIndexType::OIndexPrimary;

    if is_primary && is_ctid {
        pg_sys::ereport!(
            pg_sys::ERROR,
            pg_sys::errcode(pg_sys::ERRCODE_INTERNAL_ERROR),
            pg_sys::errmsg("ctid index key duplicate.")
        );
    } else {
        let str_info = pg_sys::makeStringInfo();
        pg_sys::appendStringInfo(str_info, b"(\0".as_ptr() as *const c_char);
        for i in 0..(*id).nKeyFields {
            if i != 0 {
                pg_sys::appendStringInfo(str_info, b", \0".as_ptr() as *const c_char);
            }
            let attr = pg_sys::TupleDescAttr((*id).nonLeafTupdesc, i);
            pg_sys::appendStringInfo(
                str_info,
                b"%s\0".as_ptr() as *const c_char,
                (*attr).attname.data.as_ptr(),
            );
        }
        pg_sys::appendStringInfo(str_info, b")=\0".as_ptr() as *const c_char);

        pg_sys::slot_getallattrs(slot);

        pg_sys::appendStringInfo(str_info, b"(\0".as_ptr() as *const c_char);
        for i in 0..(*id).nUniqueFields {
            let value = (*slot).tts_values[i as usize];
            let isnull = (*slot).tts_isnull[i as usize];

            if i != 0 {
                pg_sys::appendStringInfo(str_info, b", \0".as_ptr() as *const c_char);
            }
            if isnull {
                pg_sys::appendStringInfo(str_info, b"null\0".as_ptr() as *const c_char);
            } else {
                let mut typoutput = pg_sys::InvalidOid;
                let mut typisvarlena = false;
                let attr = pg_sys::TupleDescAttr((*id).nonLeafTupdesc, i);
                pg_sys::getTypeOutputInfo((*attr).atttypid, &mut typoutput, &mut typisvarlena);
                let res = pg_sys::OidOutputFunctionCall(typoutput, value);
                pg_sys::appendStringInfo(str_info, b"%s\0".as_ptr() as *const c_char, res);
            }
        }
        pg_sys::appendStringInfo(str_info, b")\0".as_ptr() as *const c_char);

        pg_sys::ereport!(
            pg_sys::ERROR,
            pg_sys::errcode(pg_sys::ERRCODE_UNIQUE_VIOLATION),
            pg_sys::errmsg(
                "duplicate key value violates unique constraint \"%s\"",
                (*id).name.data.as_ptr()
            ),
            pg_sys::errdetail("Key %s already exists.", (*str_info).data),
            pg_sys::errtableconstraint(
                rel,
                if (*id).desc.r#type == OIndexType::OIndexPrimary {
                    b"pk\0".as_ptr() as *const c_char
                } else {
                    b"sk\0".as_ptr() as *const c_char
                }
            )
        );
    }
}

unsafe fn append_rowid_values(
    id: *mut OIndexDescr,
    pk_tupdesc: pg_sys::TupleDesc,
    pk_spec: *mut OTupleFixedFormatSpec,
    pk_datum: pg_sys::Datum,
    values: *mut pg_sys::Datum,
    isnull: *mut bool,
    csn: *mut CommitSeqNo,
    version: *mut u32,
) {
    let rowid = pg_sys::pg_detoast_datum(pk_datum as *mut pg_sys::varlena);
    let mut p = (rowid as *mut c_char).add(pg_sys::MAXALIGN(pg_sys::VARHDRSZ as usize)) as *mut c_void;

    if !(*id).primaryIsCtid {
        let add = p as *mut ORowIdAddendumNonCtid;
        p = p.add(pg_sys::MAXALIGN(std::mem::size_of::<ORowIdAddendumNonCtid>()));
        *csn = (*add).csn;

        let mut tuple = OTuple {
            data: p as pg_sys::Pointer,
            formatFlags: (*add).flags,
        };
        if (*id).bridging {
            tuple.data = tuple.data.add(pg_sys::MAXALIGN(std::mem::size_of::<ORowIdBridgeData>()));
        }

        *version = o_tuple_get_version(tuple);

        if (*id).nPrimaryFields <= (*id).nFields {
            let pk_from = (*id).nFields - (*id).nPrimaryFields;
            for i in 0..(*id).nPrimaryFields {
                let attnum = (*id).primaryFieldsAttnums[i as usize] - 1;
                if attnum >= pk_from as pg_sys::AttrNumber {
                    *values.add(attnum as usize) = o_fastgetattr(
                        tuple,
                        i + 1,
                        pk_tupdesc,
                        pk_spec,
                        isnull.add(attnum as usize),
                    );
                }
            }
        }
    } else {
        let add = p as *mut ORowIdAddendumCtid;
        let attnum = (*id).nFields - 1;
        *csn = (*add).csn;
        *version = (*add).version;
        p = p.add(pg_sys::MAXALIGN(std::mem::size_of::<ORowIdAddendumCtid>()));
        *values.add(attnum as usize) = PointerGetDatum(p as pg_sys::Pointer);
        *isnull.add(attnum as usize) = false;
    }
}

unsafe fn detoast_passed_values(
    index_descr: *mut OIndexDescr,
    values: *mut pg_sys::Datum,
    isnull: *mut bool,
    vfree: *mut bool,
) {
    let pk_from = (*index_descr).nFields - (*index_descr).nPrimaryFields;
    for i in 0..pk_from {
        let att = pg_sys::TupleDescAttr((*index_descr).nonLeafTupdesc, i);
        if !*isnull.add(i as usize) && (*att).attlen == -1 {
            let varlena = *values.add(i as usize) as *mut pg_sys::varlena;
            if pg_sys::VARATT_IS_EXTENDED(varlena) {
                let tmp = PointerGetDatum(pg_sys::pg_detoast_datum(varlena) as pg_sys::Pointer);
                *values.add(i as usize) = tmp;
                *vfree.add(i as usize) = true;
            }
        }
    }
}

unsafe extern "C" fn orioledb_aminsert(
    rel: pg_sys::Relation,
    values: *mut pg_sys::Datum,
    isnull: *mut bool,
    mut tupleid: pg_sys::Datum,
    heapRel: pg_sys::Relation,
    checkUnique: pg_sys::IndexUniqueCheck,
    indexUnchanged: bool,
    indexInfo: *mut pg_sys::IndexInfo,
) -> bool {
    let options = (*rel).rd_options as *mut OBTOptions;

    if !options.is_null() && !(*options).orioledb_index {
        let mut oids = ORelOids { datoid: 0, reloid: 0, relnode: 0 };
        ORelOidsSetFromRel(&mut oids, heapRel);
        let descr = o_fetch_table_descr(oids);
        let rowid = pg_sys::pg_detoast_datum(tupleid as *mut pg_sys::varlena);
        let mut p = (rowid as *mut c_char).add(pg_sys::MAXALIGN(pg_sys::VARHDRSZ as usize)) as *mut c_void;

        let primary = GET_PRIMARY(descr);
        let bridge_data = if !(*primary).primaryIsCtid {
            p = p.add(pg_sys::MAXALIGN(std::mem::size_of::<ORowIdAddendumNonCtid>()));
            p as *mut ORowIdBridgeData
        } else {
            p = p.add(pg_sys::MAXALIGN(std::mem::size_of::<ORowIdAddendumCtid>()));
            p = p.add(pg_sys::MAXALIGN(std::mem::size_of::<pg_sys::ItemPointerData>()));
            p as *mut ORowIdBridgeData
        };

        tupleid = PointerGetDatum(&mut (*bridge_data).bridgeCtid as *mut pg_sys::ItemPointerData as pg_sys::Pointer);

        if !indexUnchanged {
            return btinsert(
                rel,
                values,
                isnull,
                tupleid,
                heapRel,
                checkUnique,
                indexUnchanged,
                indexInfo,
            );
        } else {
            return true;
        }
    }

    if OidIsValid((*(*rel).rd_rel).relrewrite) {
        return true;
    }

    if (*(*rel).rd_rel).relispartition && (*(*rel).rd_index).indisprimary {
        return true;
    }

    let mut oids = ORelOids { datoid: 0, reloid: 0, relnode: 0 };
    ORelOidsSetFromRel(&mut oids, rel);
    let ix_type = o_index_rel_get_ix_type(rel);
    let index_descr = o_fetch_index_descr(oids, ix_type, false, std::ptr::null_mut());
    let descr = o_fetch_table_descr((*index_descr).tableOids);

    let mut ix_num = 0;
    while ix_num < (*descr).nIndices {
        let index = *(*descr).indices.add(ix_num as usize);
        if (*index).oids.reloid == (*(*rel).rd_rel).oid {
            break;
        }
        ix_num += 1;
    }

    if !(*index_descr).duplicates.is_null() {
        let mut lc_id = 0;
        let duplicates = (*index_descr).duplicates;
        let mut lc = pg_sys::list_head(duplicates);
        let mut duplicate = if !lc.is_null() {
            lfirst(lc) as *mut pg_sys::List
        } else {
            std::ptr::null_mut()
        };

        let mut cur_attr = 0;
        for i in 0..(*rel).rd_att.as_ref().unwrap().natts {
            if !duplicate.is_null() && linitial_int(duplicate) == cur_attr {
                lc = lnext(lc);
                duplicate = if !lc.is_null() {
                    lfirst(lc) as *mut pg_sys::List
                } else {
                    std::ptr::null_mut()
                };
            } else {
                *values.add(cur_attr as usize) = *values.add(i as usize);
                cur_attr += 1;
            }
        }
    }

    let mut csn = 0;
    let mut version = 0;
    let primary = GET_PRIMARY(descr);
    append_rowid_values(
        index_descr,
        (*primary).nonLeafTupdesc,
        &mut (*primary).nonLeafSpec,
        tupleid,
        values,
        isnull,
        &mut csn,
        &mut version,
    );

    let mut tuple = o_form_tuple(
        (*index_descr).leafTupdesc,
        &mut (*index_descr).leafSpec,
        version,
        values,
        isnull,
        std::ptr::null_mut(),
    );

    let slot = (*index_descr).old_leaf_slot;
    let mut hint = BTreeLocationHint { blkno: 0, pageChangeCount: 0 };
    tts_orioledb_store_tuple(slot, tuple, descr, csn, ix_num as OIndexNumber, false, &mut hint);

    let mut callback_info = BTreeModifyCallbackInfo {
        waitCallback: None,
        modifyCallback: None,
        modifyDeletedCallback: Some(o_insert_callback),
        needsUndoForSelfCreated: true,
        arg: slot as *mut c_void,
        postUndoRecorded: None,
    };

    let mut oxid = 0;
    let mut o_snapshot = OSnapshot { csn: 0, xlogptr: 0, xmin: 0, cid: 0 };
    fill_current_oxid_osnapshot(&mut oxid, &mut o_snapshot);

    let iresult = o_tbl_index_insert(
        descr,
        *(*descr).indices.add(ix_num as usize),
        &mut tuple,
        slot,
        oxid,
        o_snapshot.csn,
        &mut callback_info,
        checkUnique,
    );

    let success = if checkUnique != pg_sys::IndexUniqueCheck::UNIQUE_CHECK_EXISTING {
        iresult == OBTreeModifyResult::OBTreeModifyResultInserted
    } else {
        iresult == OBTreeModifyResult::OBTreeModifyResultNotFound
    };

    if !success {
        if checkUnique == pg_sys::IndexUniqueCheck::UNIQUE_CHECK_YES
            || checkUnique == pg_sys::IndexUniqueCheck::UNIQUE_CHECK_EXISTING
        {
            o_report_duplicate(heapRel, *(*descr).indices.add(ix_num as usize), slot);
        }
    }

    if !tuple.data.is_null() {
        pg_sys::pfree(tuple.data as *mut c_void);
    }

    success
}

unsafe extern "C" fn orioledb_amupdate(
    rel: pg_sys::Relation,
    new_valid: bool,
    old_valid: bool,
    values: *mut pg_sys::Datum,
    isnull: *mut bool,
    tupleid: pg_sys::Datum,
    valuesOld: *mut pg_sys::Datum,
    isnullOld: *mut bool,
    oldTupleid: pg_sys::Datum,
    heapRel: pg_sys::Relation,
    checkUnique: pg_sys::IndexUniqueCheck,
    indexUnchanged: bool,
    indexInfo: *mut pg_sys::IndexInfo,
) -> bool {
    let options = (*rel).rd_options as *mut OBTOptions;

    if !options.is_null() && !(*options).orioledb_index {
        return btinsert(
            rel,
            values,
            isnull,
            tupleid,
            heapRel,
            checkUnique,
            indexUnchanged,
            indexInfo,
        );
    }

    if (*rel).rd_index.as_ref().unwrap().indisprimary {
        return true;
    }

    let mut oids = ORelOids { datoid: 0, reloid: 0, relnode: 0 };
    ORelOidsSetFromRel(&mut oids, rel);
    let ix_type = o_index_rel_get_ix_type(rel);
    let index_descr = o_fetch_index_descr(oids, ix_type, false, std::ptr::null_mut());
    let descr = o_fetch_table_descr((*index_descr).tableOids);

    let mut ix_num = 0;
    while ix_num < (*descr).nIndices {
        let index = *(*descr).indices.add(ix_num as usize);
        if (*index).oids.reloid == (*(*rel).rd_rel).oid {
            break;
        }
        ix_num += 1;
    }

    let primary = GET_PRIMARY(descr);
    let mut csn = 0;
    let mut version = 0;
    append_rowid_values(
        index_descr,
        (*primary).nonLeafTupdesc,
        &mut (*primary).nonLeafSpec,
        oldTupleid,
        valuesOld,
        isnullOld,
        &mut csn,
        &mut version,
    );

    let natts = (*(*index_descr).leafTupdesc).natts as usize;
    let vfree = pg_sys::palloc0(std::mem::size_of::<bool>() * natts) as *mut bool;
    detoast_passed_values(index_descr, valuesOld, isnullOld, vfree);

    let old_tuple = o_form_tuple(
        (*index_descr).leafTupdesc,
        &mut (*index_descr).leafSpec,
        version,
        valuesOld,
        isnullOld,
        std::ptr::null_mut(),
    );
    let old_slot = (*index_descr).old_leaf_slot;
    let mut hint = BTreeLocationHint { blkno: 0, pageChangeCount: 0 };
    tts_orioledb_store_non_leaf_tuple(old_slot, old_tuple, descr, csn, ix_num as OIndexNumber, false, &mut hint);

    append_rowid_values(
        index_descr,
        (*primary).nonLeafTupdesc,
        &mut (*primary).nonLeafSpec,
        tupleid,
        values,
        isnull,
        &mut csn,
        &mut version,
    );

    let new_tuple = o_form_tuple(
        (*index_descr).leafTupdesc,
        &mut (*index_descr).leafSpec,
        version,
        values,
        isnull,
        std::ptr::null_mut(),
    );
    let new_slot = (*index_descr).new_leaf_slot;
    tts_orioledb_store_non_leaf_tuple(new_slot, new_tuple, descr, csn, ix_num as OIndexNumber, false, &mut hint);

    let mut oxid = 0;
    let mut o_snapshot = OSnapshot { csn: 0, xlogptr: 0, xmin: 0, cid: 0 };
    fill_current_oxid_osnapshot(&mut oxid, &mut o_snapshot);

    let result = o_update_secondary_index(
        index_descr,
        ix_num as OIndexNumber,
        new_valid,
        old_valid,
        new_slot,
        new_tuple,
        old_slot,
        oxid,
        o_snapshot.csn,
        checkUnique,
    );

    for i in 0..natts {
        if *vfree.add(i) {
            pg_sys::pfree(DatumGetPointer(*valuesOld.add(i)) as *mut c_void);
        }
    }
    pg_sys::pfree(vfree as *mut c_void);

    if !result.success {
        match result.action {
            BTreeOperationType::BTreeOperationUpdate => {
                if result.failedIxNum != PrimaryIndexNumber {
                    let str_info = pg_sys::makeStringInfo();
                    pg_sys::appendStringInfo(str_info, b"(\0".as_ptr() as *const c_char);
                    for i in 0..(*index_descr).nUniqueFields {
                        if i != 0 {
                            pg_sys::appendStringInfo(str_info, b", \0".as_ptr() as *const c_char);
                        }
                        if *isnullOld.add(i as usize) {
                            pg_sys::appendStringInfo(str_info, b"null\0".as_ptr() as *const c_char);
                        } else {
                            let mut typoutput = pg_sys::InvalidOid;
                            let mut typisvarlena = false;
                            let attr = pg_sys::TupleDescAttr((*index_descr).leafTupdesc, i);
                            pg_sys::getTypeOutputInfo((*attr).atttypid, &mut typoutput, &mut typisvarlena);
                            let res = pg_sys::OidOutputFunctionCall(typoutput, *valuesOld.add(i as usize));
                            pg_sys::appendStringInfo(str_info, b"'%s'\0".as_ptr() as *const c_char, res);
                        }
                    }
                    if !old_tuple.data.is_null() {
                        pg_sys::pfree(old_tuple.data as *mut c_void);
                    }
                    if !new_tuple.data.is_null() {
                        pg_sys::pfree(new_tuple.data as *mut c_void);
                    }
                    pg_sys::appendStringInfo(str_info, b")\0".as_ptr() as *const c_char);

                    pg_sys::ereport!(
                        pg_sys::ERROR,
                        pg_sys::errcode(pg_sys::ERRCODE_INTERNAL_ERROR),
                        pg_sys::errmsg(
                            "unable to remove tuple from secondary index in \"%s\"",
                            pg_sys::RelationGetRelationName(rel)
                        ),
                        pg_sys::errdetail(
                            "Unable to remove %s from index \"%s\"",
                            (*str_info).data,
                            (*index_descr).name.data.as_ptr()
                        ),
                        pg_sys::errtableconstraint(rel, b"sk\0".as_ptr() as *const c_char)
                    );
                }
            }
            BTreeOperationType::BTreeOperationInsert => {
                if checkUnique == pg_sys::IndexUniqueCheck::UNIQUE_CHECK_YES
                    || checkUnique == pg_sys::IndexUniqueCheck::UNIQUE_CHECK_EXISTING
                {
                    o_report_duplicate(heapRel, index_descr, new_slot);
                }
            }
            _ => {
                if !old_tuple.data.is_null() {
                    pg_sys::pfree(old_tuple.data as *mut c_void);
                }
                if !new_tuple.data.is_null() {
                    pg_sys::pfree(new_tuple.data as *mut c_void);
                }
                pg_sys::ereport!(
                    pg_sys::ERROR,
                    pg_sys::errcode(pg_sys::ERRCODE_INTERNAL_ERROR),
                    pg_sys::errmsg("Unsupported BTreeOperationType.")
                );
            }
        }
    }

    if !old_tuple.data.is_null() {
        pg_sys::pfree(old_tuple.data as *mut c_void);
    }
    if !new_tuple.data.is_null() {
        pg_sys::pfree(new_tuple.data as *mut c_void);
    }

    result.success
}

unsafe extern "C" fn orioledb_amdelete(
    rel: pg_sys::Relation,
    values: *mut pg_sys::Datum,
    isnull: *mut bool,
    tupleid: pg_sys::Datum,
    heapRel: pg_sys::Relation,
    _indexInfo: *mut pg_sys::IndexInfo,
) -> bool {
    let options = (*rel).rd_options as *mut OBTOptions;

    if !options.is_null() && !(*options).orioledb_index {
        return true;
    }

    if (*rel).rd_index.as_ref().unwrap().indisprimary {
        return true;
    }

    let mut oids = ORelOids { datoid: 0, reloid: 0, relnode: 0 };
    ORelOidsSetFromRel(&mut oids, rel);
    let ix_type = o_index_rel_get_ix_type(rel);
    let index_descr = o_fetch_index_descr(oids, ix_type, false, std::ptr::null_mut());
    let descr = o_fetch_table_descr((*index_descr).tableOids);

    let mut ix_num = 0;
    while ix_num < (*descr).nIndices {
        let index = *(*descr).indices.add(ix_num as usize);
        if (*index).oids.reloid == (*(*rel).rd_rel).oid {
            break;
        }
        ix_num += 1;
    }

    let slot = (*index_descr).old_leaf_slot;
    let primary = GET_PRIMARY(descr);
    let mut csn = 0;
    let mut version = 0;
    append_rowid_values(
        index_descr,
        (*primary).nonLeafTupdesc,
        &mut (*primary).nonLeafSpec,
        tupleid,
        values,
        isnull,
        &mut csn,
        &mut version,
    );

    let natts = (*(*index_descr).nonLeafTupdesc).natts as usize;
    let vfree = pg_sys::palloc0(std::mem::size_of::<bool>() * natts) as *mut bool;
    detoast_passed_values(index_descr, values, isnull, vfree);

    let tuple = o_form_tuple(
        (*index_descr).leafTupdesc,
        &mut (*index_descr).leafSpec,
        version,
        values,
        isnull,
        std::ptr::null_mut(),
    );
    let mut hint = BTreeLocationHint { blkno: 0, pageChangeCount: 0 };
    tts_orioledb_store_tuple(slot, tuple, descr, csn, ix_num as OIndexNumber, false, &mut hint);

    let mut oxid = 0;
    let mut o_snapshot = OSnapshot { csn: 0, xlogptr: 0, xmin: 0, cid: 0 };
    fill_current_oxid_osnapshot(&mut oxid, &mut o_snapshot);

    let result = o_tbl_index_delete(index_descr, ix_num as OIndexNumber, slot, oxid, o_snapshot.csn);

    for i in 0..natts {
        if *vfree.add(i) {
            pg_sys::pfree(DatumGetPointer(*values.add(i)) as *mut c_void);
        }
    }
    pg_sys::pfree(vfree as *mut c_void);

    if !result.success {
        match result.action {
            BTreeOperationType::BTreeOperationUpdate => {
                if result.failedIxNum != PrimaryIndexNumber {
                    let str_info = pg_sys::makeStringInfo();
                    pg_sys::appendStringInfo(str_info, b"(\0".as_ptr() as *const c_char);
                    for i in 0..(*index_descr).nUniqueFields {
                        if i != 0 {
                            pg_sys::appendStringInfo(str_info, b", \0".as_ptr() as *const c_char);
                        }
                        if *isnull.add(i as usize) {
                            pg_sys::appendStringInfo(str_info, b"null\0".as_ptr() as *const c_char);
                        } else {
                            let mut typoutput = pg_sys::InvalidOid;
                            let mut typisvarlena = false;
                            let attr = pg_sys::TupleDescAttr((*index_descr).nonLeafTupdesc, i);
                            pg_sys::getTypeOutputInfo((*attr).atttypid, &mut typoutput, &mut typisvarlena);
                            let res = pg_sys::OidOutputFunctionCall(typoutput, *values.add(i as usize));
                            pg_sys::appendStringInfo(str_info, b"'%s'\0".as_ptr() as *const c_char, res);
                        }
                    }
                    pg_sys::appendStringInfo(str_info, b")\0".as_ptr() as *const c_char);

                    if !tuple.data.is_null() {
                        pg_sys::pfree(tuple.data as *mut c_void);
                    }

                    pg_sys::ereport!(
                        pg_sys::ERROR,
                        pg_sys::errcode(pg_sys::ERRCODE_INTERNAL_ERROR),
                        pg_sys::errmsg(
                            "unable to remove tuple from secondary index in \"%s\"",
                            pg_sys::RelationGetRelationName(rel)
                        ),
                        pg_sys::errdetail(
                            "Unable to remove %s from index \"%s\"",
                            (*str_info).data,
                            (*index_descr).name.data.as_ptr()
                        ),
                        pg_sys::errtableconstraint(rel, b"sk\0".as_ptr() as *const c_char)
                    );
                }
            }
            _ => {
                if !tuple.data.is_null() {
                    pg_sys::pfree(tuple.data as *mut c_void);
                }
                pg_sys::ereport!(
                    pg_sys::ERROR,
                    pg_sys::errcode(pg_sys::ERRCODE_INTERNAL_ERROR),
                    pg_sys::errmsg("Unsupported BTreeOperationType.")
                );
            }
        }
    }

    if !tuple.data.is_null() {
        pg_sys::pfree(tuple.data as *mut c_void);
    }

    result.success
}

unsafe extern "C" fn orioledb_ambulkdelete(
    info: *mut pg_sys::IndexVacuumInfo,
    stats: *mut pg_sys::IndexBulkDeleteResult,
    callback: pg_sys::IndexBulkDeleteCallback,
    callback_state: *mut c_void,
) -> *mut pg_sys::IndexBulkDeleteResult {
    let options = (*(*info).index).rd_options as *mut OBTOptions;
    if !options.is_null() && !(*options).orioledb_index {
        return btbulkdelete(info, stats, callback, callback_state);
    }
    stats
}

unsafe extern "C" fn orioledb_amvacuumcleanup(
    info: *mut pg_sys::IndexVacuumInfo,
    stats: *mut pg_sys::IndexBulkDeleteResult,
) -> *mut pg_sys::IndexBulkDeleteResult {
    let options = (*(*info).index).rd_options as *mut OBTOptions;
    if !options.is_null() && !(*options).orioledb_index {
        return btvacuumcleanup(info, stats);
    }
    stats
}

unsafe extern "C" fn orioledb_amcanreturn(index: pg_sys::Relation, attno: c_int) -> bool {
    let options = (*index).rd_options as *mut OBTOptions;
    if !options.is_null() && !(*options).orioledb_index {
        return btcanreturn(index, attno);
    }
    true
}

unsafe extern "C" fn orioledb_amcostestimate(
    root: *mut pg_sys::PlannerInfo,
    path: *mut pg_sys::IndexPath,
    loop_count: f64,
    indexStartupCost: *mut pg_sys::Cost,
    indexTotalCost: *mut pg_sys::Cost,
    indexSelectivity: *mut pg_sys::Selectivity,
    indexCorrelation: *mut f64,
    indexPages: *mut f64,
) {
    let index = (*path).indexinfo;
    let mut costs: pg_sys::GenericCosts = std::mem::zeroed();
    let mut num_sa_scans = 1.0;
    let mut index_bound_quals: *mut pg_sys::List = std::ptr::null_mut();
    let mut indexcol = 0;
    let mut eq_qual_here = false;
    let mut found_saop = false;
    let mut found_is_null_op = false;

    let clauses = (*path).indexclauses;
    let mut lc = pg_sys::list_head(clauses);
    while !lc.is_null() {
        let iclause = lfirst(lc) as *mut pg_sys::IndexClause;

        if indexcol != (*iclause).indexcol {
            if !eq_qual_here {
                break;
            }
            eq_qual_here = false;
            indexcol = (*iclause).indexcol;
            if indexcol != (*iclause).indexcol {
                break;
            }
        }

        let mut lc2 = pg_sys::list_head((*iclause).indexquals);
        while !lc2.is_null() {
            let rinfo = lfirst(lc2) as *mut pg_sys::RestrictInfo;
            let clause = (*rinfo).clause;
            let mut clause_op = pg_sys::InvalidOid;

            if pg_sys::is_a(clause as *mut c_void, pg_sys::NodeTag_T_OpExpr) {
                let op = clause as *mut pg_sys::OpExpr;
                clause_op = (*op).opno;
            } else if pg_sys::is_a(clause as *mut c_void, pg_sys::NodeTag_T_RowCompareExpr) {
                let rc = clause as *mut pg_sys::RowCompareExpr;
                clause_op = pg_sys::linitial_oid((*rc).opnos);
            } else if pg_sys::is_a(clause as *mut c_void, pg_sys::NodeTag_T_ScalarArrayOpExpr) {
                let saop = clause as *mut pg_sys::ScalarArrayOpExpr;
                let other_operand = pg_sys::lsecond((*saop).args) as *mut pg_sys::Node;
                #[cfg(any(feature = "pg17", feature = "pg18", feature = "pg19"))]
                let alength = estimate_array_length(root, other_operand);
                #[cfg(not(any(feature = "pg17", feature = "pg18", feature = "pg19")))]
                let alength = estimate_array_length(other_operand);

                clause_op = (*saop).opno;
                found_saop = true;
                if alength > 1 {
                    num_sa_scans *= alength as f64;
                }
            } else if pg_sys::is_a(clause as *mut c_void, pg_sys::NodeTag_T_NullTest) {
                let nt = clause as *mut pg_sys::NullTest;
                if (*nt).nulltesttype == pg_sys::NullTestType::IS_NULL {
                    found_is_null_op = true;
                    eq_qual_here = true;
                }
            } else {
                pg_sys::elog!(pg_sys::ERROR, "unsupported indexqual type");
            }

            if OidIsValid(clause_op) {
                let strategy = get_op_opfamily_strategy(clause_op, *(*index).opfamily.add(indexcol as usize));
                if strategy == BTEqualStrategyNumber {
                    eq_qual_here = true;
                }
            }

            index_bound_quals = pg_sys::lappend(index_bound_quals, rinfo as *mut c_void);
            lc2 = lnext(lc2);
        }

        lc = lnext(lc);
    }

    let mut num_index_tuples: f64;
    if (*index).unique
        && indexcol == (*index).nkeycolumns - 1
        && eq_qual_here
        && !found_saop
        && !found_is_null_op
    {
        num_index_tuples = 1.0;
    } else {
        let selectivity_quals = add_predicate_to_index_quals(index, index_bound_quals);
        let btree_selectivity = clauselist_selectivity(
            root,
            selectivity_quals,
            (*(*index).rel).relid,
            pg_sys::JoinType::JOIN_INNER,
            std::ptr::null_mut(),
        );
        num_index_tuples = btree_selectivity * (*(*index).rel).tuples;

        #[cfg(any(feature = "pg17", feature = "pg18", feature = "pg19"))]
        {
            let ceiling = ((*index).pages as f64 * 0.3333333).ceil();
            if num_sa_scans > ceiling {
                num_sa_scans = ceiling;
            }
            if num_sa_scans < 1.0 {
                num_sa_scans = 1.0;
            }
        }

        num_index_tuples = (num_index_tuples / num_sa_scans).round();
    }

    costs.numIndexTuples = num_index_tuples;
    #[cfg(any(feature = "pg17", feature = "pg18", feature = "pg19"))]
    {
        costs.num_sa_scans = num_sa_scans;
    }

    genericcostestimate(root, path, loop_count, &mut costs);

    if (*index).tuples > 1.0 {
        let descent_cost = ((*index).tuples.log2()).ceil() * cpu_operator_cost;
        costs.indexStartupCost += descent_cost;
        #[cfg(any(feature = "pg17", feature = "pg18", feature = "pg19"))]
        {
            costs.indexTotalCost += costs.num_sa_scans * descent_cost;
        }
        #[cfg(not(any(feature = "pg17", feature = "pg18", feature = "pg19")))]
        {
            costs.indexTotalCost += descent_cost;
        }
    }

    let descent_cost = ((*index).tree_height + 1) as f64 * 50.0 * cpu_operator_cost;
    costs.indexStartupCost += descent_cost;
    #[cfg(any(feature = "pg17", feature = "pg18", feature = "pg19"))]
    {
        costs.indexTotalCost += costs.num_sa_scans * descent_cost;
    }
    #[cfg(not(any(feature = "pg17", feature = "pg18", feature = "pg19")))]
    {
        costs.indexTotalCost += descent_cost;
    }

    if *(*index).indexkeys.add(0) != 0 {
        let rte = pg_sys::planner_rt_fetch((*(*index).rel).relid, root);
        let relid = (*rte).relid;
        let colnum = *(*index).indexkeys.add(0);

        let mut vardata: pg_sys::VariableStatData = std::mem::zeroed();
        let hook_ptr: Option<
            unsafe extern "C" fn(
                *mut pg_sys::PlannerInfo,
                *mut pg_sys::RangeTblEntry,
                pg_sys::AttrNumber,
                *mut pg_sys::VariableStatData,
            ) -> bool,
        > = std::mem::transmute(get_relation_stats_hook);

        let hook_taken = if let Some(hook) = hook_ptr {
            hook(root, rte, colnum, &mut vardata)
        } else {
            false
        };

        if !hook_taken {
            vardata.statsTuple = pg_sys::SearchSysCache3(
                pg_sys::SysCacheIdentifier_STATRELATTINH as c_int,
                pg_sys::ObjectIdGetDatum(relid),
                pg_sys::Int16GetDatum(colnum),
                pg_sys::BoolGetDatum((*rte).inh),
            );
            vardata.freefunc = Some(pg_sys::ReleaseSysCache);
        }

        if !vardata.statsTuple.is_null() {
            let sortop = get_opfamily_member(
                *(*index).opfamily.add(0),
                *(*index).opcintype.add(0),
                *(*index).opcintype.add(0),
                BTLessStrategyNumber,
            );
            let mut sslot: pg_sys::AttStatsSlot = std::mem::zeroed();
            if OidIsValid(sortop)
                && get_attstatsslot(
                    &mut sslot,
                    vardata.statsTuple,
                    pg_sys::STATISTIC_KIND_CORRELATION as c_int,
                    sortop,
                    pg_sys::ATTSTATSSLOT_NUMBERS as c_int,
                )
            {
                let mut var_correlation = *sslot.numbers.add(0);
                if *(*index).reverse_sort.add(0) {
                    var_correlation = -var_correlation;
                }
                if (*index).nkeycolumns > 1 {
                    costs.indexCorrelation = var_correlation * 0.75;
                } else {
                    costs.indexCorrelation = var_correlation;
                }
                free_attstatsslot(&mut sslot);
            }
        }
        ReleaseVariableStats(vardata);
    } else {
        let relid = (*index).indexoid;
        let colnum = 1;

        let mut vardata: pg_sys::VariableStatData = std::mem::zeroed();
        let hook_ptr: Option<
            unsafe extern "C" fn(
                *mut pg_sys::PlannerInfo,
                pg_sys::Oid,
                pg_sys::AttrNumber,
                *mut pg_sys::VariableStatData,
            ) -> bool,
        > = std::mem::transmute(get_index_stats_hook);

        let hook_taken = if let Some(hook) = hook_ptr {
            hook(root, relid, colnum, &mut vardata)
        } else {
            false
        };

        if !hook_taken {
            vardata.statsTuple = pg_sys::SearchSysCache3(
                pg_sys::SysCacheIdentifier_STATRELATTINH as c_int,
                pg_sys::ObjectIdGetDatum(relid),
                pg_sys::Int16GetDatum(colnum),
                pg_sys::BoolGetDatum(false),
            );
            vardata.freefunc = Some(pg_sys::ReleaseSysCache);
        }

        if !vardata.statsTuple.is_null() {
            let sortop = get_opfamily_member(
                *(*index).opfamily.add(0),
                *(*index).opcintype.add(0),
                *(*index).opcintype.add(0),
                BTLessStrategyNumber,
            );
            let mut sslot: pg_sys::AttStatsSlot = std::mem::zeroed();
            if OidIsValid(sortop)
                && get_attstatsslot(
                    &mut sslot,
                    vardata.statsTuple,
                    pg_sys::STATISTIC_KIND_CORRELATION as c_int,
                    sortop,
                    pg_sys::ATTSTATSSLOT_NUMBERS as c_int,
                )
            {
                let mut var_correlation = *sslot.numbers.add(0);
                if *(*index).reverse_sort.add(0) {
                    var_correlation = -var_correlation;
                }
                if (*index).nkeycolumns > 1 {
                    costs.indexCorrelation = var_correlation * 0.75;
                } else {
                    costs.indexCorrelation = var_correlation;
                }
                free_attstatsslot(&mut sslot);
            }
        }
        ReleaseVariableStats(vardata);
    }

    *indexStartupCost = costs.indexStartupCost;
    *indexTotalCost = costs.indexTotalCost;
    *indexSelectivity = costs.indexSelectivity;
    *indexCorrelation = costs.indexCorrelation;
    *indexPages = costs.numIndexPages;
}

unsafe extern "C" fn validate_index_compress(value: *const c_char) {
    if !value.is_null() {
        validate_compress(o_parse_compress(value), b"Index\0".as_ptr() as *const c_char);
    }
}

const OFFSETOF_FILLFACTOR: usize = 0;
const OFFSETOF_VACUUM_SCALE: usize = 8;
const OFFSETOF_DEDUPLICATE: usize = 16;
const OFFSETOF_COMPRESS: usize = 24;
const OFFSETOF_ORIOLEDB_INDEX: usize = 28;

unsafe extern "C" fn orioledb_amoptions(reloptions: pg_sys::Datum, validate: bool) -> *mut pg_sys::bytea {
    static mut relopts_set: bool = false;
    static mut relopts: pg_sys::local_relopts = std::mem::transmute([0u8; std::mem::size_of::<pg_sys::local_relopts>()]);

    if !relopts_set {
        let oldcxt = pg_sys::MemoryContextSwitchTo(pg_sys::TopMemoryContext);
        init_local_reloptions(&mut relopts, std::mem::size_of::<OBTOptions>());

        add_local_int_reloption(
            &mut relopts,
            b"fillfactor\0".as_ptr() as *const c_char,
            b"Packs btree index pages only to this percentage\0".as_ptr() as *const c_char,
            BTREE_DEFAULT_FILLFACTOR,
            BTREE_MIN_FILLFACTOR,
            100,
            OFFSETOF_FILLFACTOR,
        );

        add_local_real_reloption(
            &mut relopts,
            b"vacuum_cleanup_index_scale_factor\0".as_ptr() as *const c_char,
            b"Deprecated B-Tree parameter.\0".as_ptr() as *const c_char,
            -1.0,
            0.0,
            1e10,
            OFFSETOF_VACUUM_SCALE,
        );

        add_local_bool_reloption(
            &mut relopts,
            b"deduplicate_items\0".as_ptr() as *const c_char,
            b"Enables \"deduplicate items\" feature for this btree index\0".as_ptr() as *const c_char,
            false,
            OFFSETOF_DEDUPLICATE,
        );

        add_local_string_reloption(
            &mut relopts,
            b"compress\0".as_ptr() as *const c_char,
            b"Compression level of a particular index\0".as_ptr() as *const c_char,
            std::ptr::null(),
            Some(validate_index_compress),
            None,
            OFFSETOF_COMPRESS,
        );

        add_local_bool_reloption(
            &mut relopts,
            b"orioledb_index\0".as_ptr() as *const c_char,
            b"Use orioledb own implementation of index\0".as_ptr() as *const c_char,
            true,
            OFFSETOF_ORIOLEDB_INDEX,
        );

        pg_sys::MemoryContextSwitchTo(oldcxt);
        relopts_set = true;
    }

    build_local_reloptions(&mut relopts, reloptions, validate)
}

unsafe extern "C" fn orioledb_amproperty(
    _index_oid: pg_sys::Oid,
    attno: c_int,
    prop: pg_sys::IndexAMProperty,
    _propname: *const c_char,
    res: *mut bool,
    _isnull: *mut bool,
) -> bool {
    match prop {
        pg_sys::IndexAMProperty::AMPROP_RETURNABLE => {
            if attno == 0 {
                return false;
            }
            *res = true;
            true
        }
        _ => false,
    }
}

unsafe extern "C" fn orioledb_ambuildphasename(phasenum: i64) -> *mut c_char {
    match phasenum {
        pg_sys::PROGRESS_CREATEIDX_SUBPHASE_INITIALIZE => b"initializing\0".as_ptr() as *mut c_char,
        pg_sys::PROGRESS_BTREE_PHASE_INDEXBUILD_TABLESCAN => b"scanning table\0".as_ptr() as *mut c_char,
        pg_sys::PROGRESS_BTREE_PHASE_PERFORMSORT_1 => b"sorting live tuples\0".as_ptr() as *mut c_char,
        pg_sys::PROGRESS_BTREE_PHASE_PERFORMSORT_2 => b"sorting dead tuples\0".as_ptr() as *mut c_char,
        pg_sys::PROGRESS_BTREE_PHASE_LEAF_LOAD => b"loading tuples in tree\0".as_ptr() as *mut c_char,
        _ => std::ptr::null_mut(),
    }
}

unsafe extern "C" fn orioledb_amvalidate(_opclassoid: pg_sys::Oid) -> bool {
    true
}

unsafe extern "C" fn orioledb_amadjustmembers(
    _opfamilyoid: pg_sys::Oid,
    _opclassoid: pg_sys::Oid,
    _operators: *mut pg_sys::List,
    _functions: *mut pg_sys::List,
) {
}

unsafe extern "C" fn orioledb_ambeginscan(
    rel: pg_sys::Relation,
    nkeys: c_int,
    norderbys: c_int,
) -> pg_sys::IndexScanDesc {
    let options = (*rel).rd_options as *mut OBTOptions;

    if !options.is_null() && !(*options).orioledb_index {
        return btbeginscan(rel, nkeys, norderbys);
    }

    let o_scan = pg_sys::palloc0(std::mem::size_of::<OScanState>()) as *mut OScanState;

    let scan = btbeginscan(rel, nkeys, norderbys);
    (*scan).xs_snapshot = std::ptr::null_mut();
    std::ptr::copy_nonoverlapping(scan, &mut (*o_scan).scandesc, 1);
    pg_sys::pfree(scan as *mut c_void);

    let scan = &mut (*o_scan).scandesc as *mut pg_sys::IndexScanDescData;

    (*scan).parallel_scan = std::ptr::null_mut();
    (*scan).xs_temp_snap = false;
    (*scan).xs_want_rowid = true;

    let mut oids = ORelOids { datoid: 0, reloid: 0, relnode: 0 };
    ORelOidsSetFromRel(&mut oids, rel);
    let ix_type = o_index_rel_get_ix_type(rel);
    let index_descr = o_fetch_index_descr(oids, ix_type, false, std::ptr::null_mut());
    let descr = o_fetch_table_descr((*index_descr).tableOids);

    let mut ix_num = 0;
    while ix_num < (*descr).nIndices {
        let index = *(*descr).indices.add(ix_num as usize);
        if (*index).oids.reloid == (*(*rel).rd_rel).oid {
            break;
        }
        ix_num += 1;
    }

    (*o_scan).ixNum = ix_num as OIndexNumber;

    (*o_scan).cxt = pg_sys::AllocSetContextCreateInternal(
        pg_sys::CurrentMemoryContext,
        b"orioledb_cs plan data\0".as_ptr() as *const c_char,
        pg_sys::ALLOCSET_DEFAULT_SIZES[0],
        pg_sys::ALLOCSET_DEFAULT_SIZES[1],
        pg_sys::ALLOCSET_DEFAULT_SIZES[2],
    );

    scan
}

#[no_mangle]
pub unsafe extern "C" fn o_get_num_prefix_exact_keys(scankey: pg_sys::ScanKey, nscankeys: c_int) -> c_int {
    let mut prev_attr: pg_sys::AttrNumber = 0;
    let mut i = 0;

    while i < nscankeys {
        let key = &*scankey.add(i as usize);
        if key.sk_attno != prev_attr + 1 || key.sk_strategy != BTEqualStrategyNumber {
            break;
        }
        prev_attr = key.sk_attno;
        i += 1;
    }

    i
}

#[no_mangle]
pub unsafe extern "C" fn o_adjust_num_prefix_exact_keys(
    so: pg_sys::BTScanOpaque,
    numPrefixExactKeys: c_int,
) -> c_int {
    let mut adjusted = numPrefixExactKeys;

    #[cfg(any(feature = "pg18", feature = "pg19"))]
    {
        let so_ref = &*so;
        for i in 0..so_ref.numArrayKeys {
            let array_key = &*so_ref.arrayKeys.add(i as usize);
            if array_key.num_elems <= 0 && array_key.scan_key < adjusted {
                adjusted = array_key.scan_key;
            }
        }
    }

    adjusted
}

unsafe extern "C" fn orioledb_amrescan(
    scan: pg_sys::IndexScanDesc,
    scankey: pg_sys::ScanKey,
    nscankeys: c_int,
    orderbys: pg_sys::ScanKey,
    norderbys: c_int,
) {
    let o_scan = scan as *mut OScanState;
    let options = (*(*scan).indexRelation).rd_options as *mut OBTOptions;

    if !options.is_null() && !(*options).orioledb_index {
        return btrescan(scan, scankey, nscankeys, orderbys, norderbys);
    }

    if !(*o_scan).iterator.is_null() {
        btree_iterator_free((*o_scan).iterator);
    }
    pg_sys::MemoryContextReset((*o_scan).cxt);
    (*o_scan).iterator = std::ptr::null_mut();
    (*o_scan).curKeyRangeIsLoaded = false;
    (*o_scan).numPrefixExactKeys = o_get_num_prefix_exact_keys(scankey, nscankeys);
    btrescan(scan, scankey, nscankeys, orderbys, norderbys);
}

unsafe fn fill_hitup(
    scan: pg_sys::IndexScanDesc,
    tuple: OTuple,
    descr: *mut OTableDescr,
    tuple_csn: CommitSeqNo,
    hint: *mut BTreeLocationHint,
) {
    (*scan).xs_hitupdesc = (*descr).tupdesc;
    let slot = (*descr).oldTuple;
    tts_orioledb_store_tuple(slot, tuple, descr, tuple_csn, PrimaryIndexNumber, true, hint);

    if !(*scan).xs_rowid.isnull {
        pg_sys::pfree(DatumGetPointer((*scan).xs_rowid.value) as *mut c_void);
        (*scan).xs_rowid.isnull = true;
    }

    (*scan).xs_rowid.value = pg_sys::slot_getsysattr(
        slot,
        RowIdAttributeNumber,
        &mut (*scan).xs_rowid.isnull,
    );

    if !(*scan).xs_hitup.is_null() {
        pg_sys::pfree((*scan).xs_hitup as *mut c_void);
        (*scan).xs_hitup = std::ptr::null_mut();
    }

    (*scan).xs_hitup = pg_sys::ExecCopySlotHeapTuple(slot);

    pg_sys::ExecClearTuple(slot);
}

unsafe fn search_next_dup_range(
    duplicates: *mut pg_sys::List,
    mut dup_range_lc_id: c_int,
    dup_range_start: *mut c_int,
    dup_range_end: *mut c_int,
) {
    let mut duplicate: *mut pg_sys::List;
    let mut dup_range_lc: *mut pg_sys::ListCell;
    let mut dup_range_src_attnum = -1;

    *dup_range_start = -1;
    *dup_range_end = -1;

    loop {
        if dup_range_lc_id >= 0 {
            dup_range_lc = pg_sys::list_nth_cell(duplicates, dup_range_lc_id);
        } else {
            dup_range_lc = std::ptr::null_mut();
        }

        if !dup_range_lc.is_null() {
            duplicate = lfirst(dup_range_lc) as *mut pg_sys::List;
            if *dup_range_end < 0 {
                *dup_range_end = dup_range_lc_id;
                dup_range_src_attnum = linitial_int(duplicate);
            } else if linitial_int(duplicate) != dup_range_src_attnum {
                *dup_range_start = dup_range_lc_id + 1;
            }
        } else {
            *dup_range_start = dup_range_lc_id + 1;
        }
        dup_range_lc_id -= 1;

        if *dup_range_start >= 0 {
            break;
        }
    }
}

#[no_mangle]
pub unsafe extern "C" fn o_new_rowid(
    primary: *mut OIndexDescr,
    slot: *mut pg_sys::TupleTableSlot,
    rowid_values: *mut pg_sys::Datum,
    rowid_isnull: *mut bool,
    tuple_csn: CommitSeqNo,
    hint: *mut BTreeLocationHint,
) -> *mut pg_sys::bytea {
    let oslot = slot as *mut OTableSlot;
    let ptr: *mut c_char;
    let result_size: usize;

    if (*primary).primaryIsCtid {
        let mut add_ctid = ORowIdAddendumCtid {
            hint: *hint,
            csn: tuple_csn,
            version: (*oslot).version,
        };

        result_size = pg_sys::MAXALIGN(pg_sys::VARHDRSZ as usize)
            + pg_sys::MAXALIGN(std::mem::size_of::<ORowIdAddendumCtid>())
            + pg_sys::MAXALIGN(std::mem::size_of::<pg_sys::ItemPointerData>());
        let mut actual_size = result_size;
        if (*primary).bridging {
            actual_size += pg_sys::MAXALIGN(std::mem::size_of::<ORowIdBridgeData>());
        }

        let rowid = pg_sys::MemoryContextAllocZero((*slot).tts_mcxt, actual_size) as *mut pg_sys::bytea;
        (*rowid).vl_len_ = (actual_size as u32) << 2; // SET_VARSIZE macro

        ptr = (rowid as *mut c_char).add(pg_sys::MAXALIGN(pg_sys::VARHDRSZ as usize));
        std::ptr::copy_nonoverlapping(&add_ctid, ptr as *mut ORowIdAddendumCtid, 1);

        let ptr = ptr.add(pg_sys::MAXALIGN(std::mem::size_of::<ORowIdAddendumCtid>()));
        std::ptr::copy_nonoverlapping(&(*slot).tts_tid, ptr as *mut pg_sys::ItemPointerData, 1);

        if (*primary).bridging {
            let ptr = ptr.add(pg_sys::MAXALIGN(std::mem::size_of::<pg_sys::ItemPointerData>()));
            let bridged_data = ptr as *mut ORowIdBridgeData;
            (*bridged_data).bridgeCtid = (*oslot).bridge_ctid;
            (*bridged_data).bridgeChanged = (*oslot).bridgeChanged;
        }

        rowid
    } else {
        let mut add_non_ctid = ORowIdAddendumNonCtid {
            hint: *hint,
            csn: tuple_csn,
            flags: 0,
        };
        let mut temp_tuple = OTuple {
            data: std::ptr::null_mut(),
            formatFlags: 0,
        };

        result_size = pg_sys::MAXALIGN(pg_sys::VARHDRSZ as usize)
            + pg_sys::MAXALIGN(std::mem::size_of::<ORowIdAddendumNonCtid>());
        let mut actual_size = result_size;
        if (*primary).bridging {
            actual_size += pg_sys::MAXALIGN(std::mem::size_of::<pg_sys::ItemPointerData>());
        }

        let pk_tupdesc = (*primary).nonLeafTupdesc;
        let pk_spec = &mut (*primary).nonLeafSpec;

        let tuple_size = o_new_tuple_size(
            pk_tupdesc,
            pk_spec,
            std::ptr::null_mut(),
            std::ptr::null_mut(),
            (*oslot).version,
            rowid_values,
            rowid_isnull,
            std::ptr::null_mut(),
        );
        actual_size += pg_sys::MAXALIGN(tuple_size as usize);

        let rowid = pg_sys::MemoryContextAllocZero((*slot).tts_mcxt, actual_size) as *mut pg_sys::bytea;
        (*rowid).vl_len_ = (actual_size as u32) << 2; // SET_VARSIZE macro

        ptr = (rowid as *mut c_char).add(pg_sys::MAXALIGN(pg_sys::VARHDRSZ as usize));
        if (*primary).bridging {
            let bridge_data = ptr.add(pg_sys::MAXALIGN(std::mem::size_of::<ORowIdAddendumNonCtid>()))
                as *mut ORowIdBridgeData;
            (*bridge_data).bridgeCtid = (*oslot).bridge_ctid;
            (*bridge_data).bridgeChanged = (*oslot).bridgeChanged;
        }

        temp_tuple.data = ptr.add(pg_sys::MAXALIGN(std::mem::size_of::<ORowIdAddendumNonCtid>())) as pg_sys::Pointer;
        if (*primary).bridging {
            temp_tuple.data = temp_tuple.data.add(pg_sys::MAXALIGN(std::mem::size_of::<ORowIdBridgeData>()));
        }

        o_tuple_fill(
            pk_tupdesc,
            pk_spec,
            &mut temp_tuple,
            tuple_size,
            std::ptr::null_mut(),
            std::ptr::null_mut(),
            (*oslot).version,
            rowid_values,
            rowid_isnull,
            std::ptr::null_mut(),
        );

        add_non_ctid.flags = temp_tuple.formatFlags;
        std::ptr::copy_nonoverlapping(&add_non_ctid, ptr as *mut ORowIdAddendumNonCtid, 1);

        rowid
    }
}

unsafe fn fill_itup(
    scan: pg_sys::IndexScanDesc,
    tuple: OTuple,
    descr: *mut OTableDescr,
    tuple_csn: CommitSeqNo,
    hint: *mut BTreeLocationHint,
) {
    let o_scan = scan as *mut OScanState;
    let index_descr = *(*descr).indices.add((*o_scan).ixNum as usize);
    let slot = (*index_descr).index_slot;

    tts_orioledb_store_tuple(slot, tuple, descr, tuple_csn, (*o_scan).ixNum, true, hint);
    pg_sys::slot_getallattrs(slot);

    if !(*index_descr).duplicates.is_null() {
        let mut lc_id = pg_sys::list_length((*index_descr).duplicates) - 1;
        let mut dup_range_start = -1;
        let mut dup_range_end = -1;

        search_next_dup_range(
            (*index_descr).duplicates,
            lc_id,
            &mut dup_range_start,
            &mut dup_range_end,
        );

        let mut lc = pg_sys::list_nth_cell((*index_descr).duplicates, dup_range_end);
        let mut duplicate = lfirst(lc) as *mut pg_sys::List;
        let mut dup_range_diff = dup_range_end - dup_range_start + 1;

        lc = pg_sys::list_nth_cell((*index_descr).duplicates, lc_id);
        duplicate = lfirst(lc) as *mut pg_sys::List;

        let ctid_off = if (*index_descr).primaryIsCtid { 1 } else { 0 };
        let mut cur_attr = (*(*index_descr).leafTupdesc).natts as c_int - 1 - ctid_off;

        let mut i = (*(*index_descr).itupdesc).natts as c_int - 1;
        while i >= 0 {
            if !duplicate.is_null()
                && i >= linitial_int(duplicate) + dup_range_start
                && i <= linitial_int(duplicate) + dup_range_start - 1 + dup_range_diff
            {
                (*slot).tts_values[i as usize] = 0;
                (*slot).tts_isnull[i as usize] = true;
            } else {
                if !duplicate.is_null() && i == linitial_int(duplicate) + dup_range_start - 1 {
                    lc_id = dup_range_start - 1;
                    if lc_id >= 0 {
                        search_next_dup_range(
                            (*index_descr).duplicates,
                            lc_id,
                            &mut dup_range_start,
                            &mut dup_range_end,
                        );
                        lc = pg_sys::list_nth_cell((*index_descr).duplicates, dup_range_end);
                        duplicate = lfirst(lc) as *mut pg_sys::List;
                        dup_range_diff = dup_range_end - dup_range_start + 1;
                    } else {
                        duplicate = std::ptr::null_mut();
                    }
                }
                (*slot).tts_values[i as usize] = (*slot).tts_values[cur_attr as usize];
                (*slot).tts_isnull[i as usize] = (*slot).tts_isnull[cur_attr as usize];
                cur_attr -= 1;
            }
            i -= 1;
        }
    }

    let mut temp_rowid_values = [0 as pg_sys::Datum; 2 * INDEX_MAX_KEYS];
    let mut temp_rowid_isnull = [true; 2 * INDEX_MAX_KEYS];
    let mut rowid_values = std::ptr::null_mut();
    let mut rowid_isnull = std::ptr::null_mut();

    if !(*index_descr).primaryIsCtid {
        let mut i = 0;
        while i < (*index_descr).nPrimaryFields {
            let attnum = (*index_descr).primaryFieldsAttnums[i as usize] - 1;
            temp_rowid_values[i as usize] = (*slot).tts_values[attnum as usize];
            temp_rowid_isnull[i as usize] = (*slot).tts_isnull[attnum as usize];
            i += 1;
        }

        let primary = GET_PRIMARY(descr);
        while i < (*(*primary).nonLeafTupdesc).natts {
            temp_rowid_values[i as usize] = 0;
            temp_rowid_isnull[i as usize] = true;
            i += 1;
        }

        if (*o_scan).ixNum == PrimaryIndexNumber {
            rowid_values = (*slot).tts_values;
            rowid_isnull = (*slot).tts_isnull;
        } else {
            rowid_values = temp_rowid_values.as_mut_ptr();
            rowid_isnull = temp_rowid_isnull.as_mut_ptr();
        }
    }

    let rowid = o_new_rowid(
        GET_PRIMARY(descr),
        slot,
        rowid_values,
        rowid_isnull,
        tuple_csn,
        hint,
    );

    if !(*scan).xs_rowid.isnull {
        pg_sys::pfree(DatumGetPointer((*scan).xs_rowid.value) as *mut c_void);
        (*scan).xs_rowid.isnull = true;
    }
    (*scan).xs_rowid.isnull = false;
    (*scan).xs_rowid.value = PointerGetDatum(rowid as pg_sys::Pointer);

    if !(*scan).xs_itup.is_null() {
        pg_sys::pfree((*scan).xs_itup as *mut c_void);
        (*scan).xs_itup = std::ptr::null_mut();
    }

    (*scan).xs_itupdesc = (*index_descr).itupdesc;
    (*scan).xs_itup = pg_sys::index_form_tuple(
        (*index_descr).itupdesc,
        (*slot).tts_values,
        (*slot).tts_isnull,
    );

    std::ptr::copy_nonoverlapping(
        &(*slot).tts_tid,
        &mut (*(*scan).xs_itup).t_tid as *mut pg_sys::ItemPointerData,
        1,
    );

    pg_sys::ExecClearTuple(slot);
}

unsafe extern "C" fn orioledb_amgettuple(scan: pg_sys::IndexScanDesc, dir: pg_sys::ScanDirection) -> bool {
    let o_scan = scan as *mut OScanState;
    let options = (*(*scan).indexRelation).rd_options as *mut OBTOptions;

    if !options.is_null() && !(*options).orioledb_index {
        return btgettuple(scan, dir);
    }

    (*o_scan).scanDir = dir;

    if (*(*scan).xs_snapshot).snapshot_type == pg_sys::SnapshotType::SNAPSHOT_DIRTY {
        (*o_scan).oSnapshot = o_in_progress_snapshot;
    } else if (*(*scan).xs_snapshot).snapshot_type == pg_sys::SnapshotType::SNAPSHOT_NON_VACUUMABLE {
        (*o_scan).oSnapshot = o_non_deleted_snapshot;
    } else {
        // O_LOAD_SNAPSHOT macro equivalent
        let snap = (*scan).xs_snapshot;
        (*o_scan).oSnapshot.csn = (*snap).xmin; // Or correct translation of snapshot loading
        (*o_scan).oSnapshot.cid = (*snap).curcid;
    }

    (*scan).xs_recheck = false;

    let descr = relation_get_descr((*scan).heapRelation);
    let scan_primary = (*o_scan).ixNum == PrimaryIndexNumber || !(*scan).xs_want_itup;
    let mut hint = BTreeLocationHint { blkno: 0, pageChangeCount: 0 };
    let mut csn = 0;

    let tuple = o_index_scan_getnext(
        descr,
        o_scan,
        &mut csn,
        scan_primary,
        pg_sys::CurrentMemoryContext,
        &mut hint,
    );

    if tuple.data.is_null() {
        if !(*scan).xs_rowid.isnull {
            pg_sys::pfree(DatumGetPointer((*scan).xs_rowid.value) as *mut c_void);
            (*scan).xs_rowid.isnull = true;
        }
        if !(*scan).xs_itup.is_null() {
            pg_sys::pfree((*scan).xs_itup as *mut c_void);
            (*scan).xs_itup = std::ptr::null_mut();
        }
        if !(*scan).xs_hitup.is_null() {
            pg_sys::pfree((*scan).xs_hitup as *mut c_void);
            (*scan).xs_hitup = std::ptr::null_mut();
        }
        (*scan).xs_rowid.isnull = true;
        false
    } else {
        if (*scan).xs_want_itup {
            fill_itup(scan, tuple, descr, csn, &mut hint);
        } else {
            fill_hitup(scan, tuple, descr, csn, &mut hint);
        }
        true
    }
}

unsafe extern "C" fn orioledb_amgetbitmap(scan: pg_sys::IndexScanDesc, tbm: *mut pg_sys::TIDBitmap) -> i64 {
    let options = (*(*scan).indexRelation).rd_options as *mut OBTOptions;
    if !options.is_null() && !(*options).orioledb_index {
        return btgetbitmap(scan, tbm);
    }
    0
}

unsafe extern "C" fn orioledb_amendscan(scan: pg_sys::IndexScanDesc) {
    let o_scan = scan as *mut OScanState;
    let options = (*(*scan).indexRelation).rd_options as *mut OBTOptions;

    if !options.is_null() && !(*options).orioledb_index {
        return btendscan(scan);
    }

    STOPEVENT(STOPEVENT_SCAN_END, std::ptr::null_mut());

    if !(*o_scan).iterator.is_null() {
        btree_iterator_free((*o_scan).iterator);
    }
    pg_sys::MemoryContextDelete((*o_scan).cxt);
}

// Parallel scan estimation

#[cfg(any(feature = "pg18", feature = "pg19"))]
unsafe extern "C" fn orioledb_amestimateparallelscan(
    _indexRelation: pg_sys::Relation,
    _nkeys: c_int,
    _norderbys: c_int,
) -> pg_sys::Size {
    std::mem::size_of::<u8>()
}

#[cfg(feature = "pg17")]
unsafe extern "C" fn orioledb_amestimateparallelscan(
    _nkeys: c_int,
    _norderbys: c_int,
) -> pg_sys::Size {
    std::mem::size_of::<u8>()
}

#[cfg(not(any(feature = "pg17", feature = "pg18", feature = "pg19")))]
unsafe extern "C" fn orioledb_amestimateparallelscan() -> pg_sys::Size {
    std::mem::size_of::<u8>()
}

unsafe extern "C" fn orioledb_aminitparallelscan(_target: *mut c_void) {}

unsafe extern "C" fn orioledb_amparallelrescan(_scan: pg_sys::IndexScanDesc) {}

// Bridged Handler Implementation

unsafe fn find_bridged_am(index: pg_sys::Relation) -> *mut pg_sys::IndexAmRoutine {
    let mut amroutine = std::ptr::null_mut();
    let mut lc = pg_sys::list_head(bridged_ams);
    while !lc.is_null() {
        let bridged = lfirst(lc) as *mut BridgedIndexAmRoutine;
        if (*bridged).amhandler == (*index).rd_amhandler {
            amroutine = (*bridged).original_routine;
            break;
        }
        lc = lnext(lc);
    }
    amroutine
}

unsafe extern "C" fn bridged_ambuild(
    heap: pg_sys::Relation,
    index: pg_sys::Relation,
    indexInfo: *mut pg_sys::IndexInfo,
) -> *mut pg_sys::IndexBuildResult {
    let descr = relation_get_descr(heap);
    if descr.is_null() {
        let result = pg_sys::palloc(std::mem::size_of::<pg_sys::IndexBuildResult>()) as *mut pg_sys::IndexBuildResult;
        (*result).heap_tuples = 0.0;
        (*result).index_tuples = 0.0;
        result
    } else {
        let amroutine = find_bridged_am(index);
        ((*amroutine).ambuild.unwrap())(heap, index, indexInfo)
    }
}

unsafe extern "C" fn bridged_aminsert(
    rel: pg_sys::Relation,
    values: *mut pg_sys::Datum,
    isnull: *mut bool,
    mut tupleid: pg_sys::Datum,
    heapRel: pg_sys::Relation,
    checkUnique: pg_sys::IndexUniqueCheck,
    indexUnchanged: bool,
    indexInfo: *mut pg_sys::IndexInfo,
) -> bool {
    let mut oids = ORelOids { datoid: 0, reloid: 0, relnode: 0 };
    ORelOidsSetFromRel(&mut oids, heapRel);
    let descr = o_fetch_table_descr(oids);

    let rowid = pg_sys::pg_detoast_datum(tupleid as *mut pg_sys::varlena);
    let mut p = (rowid as *mut c_char).add(pg_sys::MAXALIGN(pg_sys::VARHDRSZ as usize)) as *mut c_void;

    let primary = GET_PRIMARY(descr);
    let bridge_data = if !(*primary).primaryIsCtid {
        p = p.add(pg_sys::MAXALIGN(std::mem::size_of::<ORowIdAddendumNonCtid>()));
        p as *mut ORowIdBridgeData
    } else {
        p = p.add(pg_sys::MAXALIGN(std::mem::size_of::<ORowIdAddendumCtid>()));
        p = p.add(pg_sys::MAXALIGN(std::mem::size_of::<pg_sys::ItemPointerData>()));
        p as *mut ORowIdBridgeData
    };

    if !(*bridge_data).bridgeChanged {
        return true;
    }

    tupleid = PointerGetDatum(&mut (*bridge_data).bridgeCtid as *mut pg_sys::ItemPointerData as pg_sys::Pointer);

    let amroutine = find_bridged_am(rel);
    ((*amroutine).aminsertextended.unwrap())(
        rel,
        values,
        isnull,
        tupleid,
        heapRel,
        checkUnique,
        indexUnchanged,
        indexInfo,
    )
}

unsafe extern "C" fn bridged_ambeginscan(
    rel: pg_sys::Relation,
    nkeys: c_int,
    norderbys: c_int,
) -> pg_sys::IndexScanDesc {
    let amroutine = find_bridged_am(rel);
    ((*amroutine).ambeginscan.unwrap())(rel, nkeys, norderbys)
}
