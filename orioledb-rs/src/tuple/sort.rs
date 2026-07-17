/*-------------------------------------------------------------------------
 *
 * sort.rs
 * 		Implementation of orioledb tuple sorting.
 *
 * Copyright (c) 2021-2026, Oriole DB Inc.
 * Copyright (c) 2025-2026, Supabase Inc.
 *
 * IDENTIFICATION
 *	  contrib/orioledb/src/tuple/sort.rs
 *
 *-------------------------------------------------------------------------
 */

use std::ffi::{c_char, c_int, c_void};
use pgrx::pg_sys::{Datum, TupleDesc, SortTuple, Tuplesortstate, Oid, NameData};
use crate::tuple::format::{
    OTuple, OTupleFixedFormatSpec, OTupleAttrCompact, o_fastgetattr, o_tuple_size,
    ORelOids, OIndexType, maxalign, O_TUPLE_FLAGS_FIXED_FORMAT, OInMemoryBlkno, BTreeRootInfo,
};

#[repr(C)]
pub struct OIndexBuildSortArg {
    pub tupDesc: TupleDesc,
    pub id: *mut OIndexDescr,
    pub enforceUnique: bool,
}

#[repr(C)]
pub struct OComparator {
    _unused: [u8; 0],
}

#[repr(C)]
pub struct OExclusionFn {
    _unused: [u8; 0],
}

#[repr(C)]
pub struct OHashFn {
    _unused: [u8; 0],
}

#[repr(C)]
pub struct OIndexField {
    pub inputtype: Oid,
    pub opfamily: Oid,
    pub opclass: Oid,
    pub collation: Oid,
    pub ascending: bool,
    pub nullfirst: bool,
    pub comparator: *mut OComparator,
    pub exclusion_fn: *mut OExclusionFn,
    pub hash_fn: *mut OHashFn,
}

#[repr(C)]
pub struct SeqBufDescPrivate {
    _unused: [u8; 0],
}

#[repr(C)]
#[derive(Copy, Clone, Debug)]
pub struct OSmgrArray {
    pub files: *mut pgrx::pg_sys::File,
    pub filesAllocated: std::ffi::c_int,
}

#[repr(C)]
pub union OSmgr {
    pub array: OSmgrArray,
    pub hash: *mut std::ffi::c_void,
}

#[repr(C)]
pub struct PagePool {
    _unused: [u8; 0],
}

#[repr(C)]
pub struct BTreeDescr {
    pub rootInfo: BTreeRootInfo,
    pub arg: *mut std::ffi::c_void,
    pub smgr: OSmgr,
    pub oids: ORelOids,
    pub tablespace: Oid,
    pub type_: OIndexType,
    pub ppool: *mut PagePool,
    pub compress: std::ffi::c_int,
    pub fillfactor: u8,
    pub undoType: std::ffi::c_int,
    pub storageType: std::ffi::c_int,
    pub freeBuf: SeqBufDescPrivate,
    pub nextChkp: [SeqBufDescPrivate; 2],
    pub tmpBuf: [SeqBufDescPrivate; 2],
}

#[repr(C)]
pub struct OIndexDescr {
    pub oids: ORelOids,
    pub tableOids: ORelOids,
    pub version: u32,
    pub refcnt: std::ffi::c_int,
    pub valid: bool,
    pub desc: BTreeDescr,
    pub name: NameData,
    pub index_mctx: pgrx::pg_sys::MemoryContext,
    pub expressions: *mut pgrx::pg_sys::List,
    pub predicate: *mut pgrx::pg_sys::List,
    pub predicate_str: *mut c_char,
    pub expressions_state: *mut pgrx::pg_sys::List,
    pub predicate_state: *mut pgrx::pg_sys::ExprState,
    pub econtext: *mut pgrx::pg_sys::ExprContext,
    pub nonLeafTupdesc: TupleDesc,
    pub nonLeafSpec: OTupleFixedFormatSpec,
    pub leafTupdesc: TupleDesc,
    pub leafSpec: OTupleFixedFormatSpec,
    pub unique: bool,
    pub immediate: bool,
    pub nulls_not_distinct: bool,
    pub nUniqueFields: std::ffi::c_int,
    pub primaryIsCtid: bool,
    pub bridging: bool,
    pub fillfactor: u8,
    pub nFields: std::ffi::c_int,
    pub nKeyFields: std::ffi::c_int,
    pub nIncludedFields: std::ffi::c_int,
    pub fields: *mut OIndexField,
    pub nPrimaryFields: std::ffi::c_int,
    pub primaryFieldsAttnums: [pgrx::pg_sys::AttrNumber; 16], // INDEX_MAX_KEYS is 16
    pub compress: std::ffi::c_int,
    pub tableAttnums: *mut pgrx::pg_sys::AttrNumber,
    pub maxTableAttnum: std::ffi::c_int,
    pub pk_tbl_field_map: *mut std::ffi::c_void,
    pub pk_comparators: *mut *mut OComparator,
    pub itupdesc: TupleDesc,
    pub index_slot: *mut pgrx::pg_sys::TupleTableSlot,
    pub old_leaf_slot: *mut pgrx::pg_sys::TupleTableSlot,
    pub new_leaf_slot: *mut pgrx::pg_sys::TupleTableSlot,
    pub duplicates: *mut pgrx::pg_sys::List,
}

#[cfg(any(feature = "pg17", feature = "pg18", feature = "pg19"))]
use pgrx::pg_sys::TuplesortPublic;

#[cfg(not(any(feature = "pg17", feature = "pg18", feature = "pg19")))]
#[repr(C)]
pub struct TuplesortPublic {
    pub sortopt: std::ffi::c_int,
    pub maincontext: pgrx::pg_sys::MemoryContext,
    pub sortcontext: pgrx::pg_sys::MemoryContext,
    pub tuplecontext: pgrx::pg_sys::MemoryContext,
    pub allowedMem: usize,
    pub availMem: usize,
    pub nKeys: std::ffi::c_int,
    pub sortKeys: pgrx::pg_sys::SortSupport,
    pub removeabbrev: Option<unsafe extern "C" fn(state: *mut Tuplesortstate, stups: *mut SortTuple, count: c_int)>,
    pub comparetup: Option<unsafe extern "C" fn(a: *const SortTuple, b: *const SortTuple, state: *mut Tuplesortstate) -> c_int>,
    pub writetup: Option<unsafe extern "C" fn(state: *mut Tuplesortstate, tape: *mut pgrx::pg_sys::LogicalTape, stup: *mut SortTuple)>,
    pub readtup: Option<unsafe extern "C" fn(state: *mut Tuplesortstate, stup: *mut SortTuple, tape: *mut pgrx::pg_sys::LogicalTape, len: std::ffi::c_uint)>,
    pub arg: *mut std::ffi::c_void,
}

pub unsafe fn TuplesortstateGetPublic(state: *mut Tuplesortstate) -> *mut TuplesortPublic {
    state as *mut TuplesortPublic
}

#[repr(C)]
#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub enum BTreeKeyType {
    LeafTuple = 0,
    NonLeafKey = 1,
    Bound = 2,
    PageHiKey = 3,
    UniqueLowerBound = 4,
    UniqueUpperBound = 5,
}

pub unsafe fn OIgnoreColumn(descr: *const OIndexDescr, attnum: usize) -> bool {
    let descr = &*descr;
    let type_val = descr.desc.type_;
    (type_val != OIndexType::Toast && type_val != OIndexType::Bridge)
        && (attnum >= descr.nKeyFields as usize)
        && (attnum < (descr.nKeyFields + descr.nIncludedFields) as usize)
}

pub unsafe fn OIndexKeyAttnumToTupleAttnum(key_type: BTreeKeyType, idx: *mut OIndexDescr, attnum: i32) -> u16 {
    let idx = &*idx;
    let type_val = idx.desc.type_;
    if key_type == BTreeKeyType::LeafTuple && type_val == OIndexType::Primary {
        assert!((attnum - 1) < (*idx.leafTupdesc).natts);
        let table_attnum = *idx.tableAttnums.add((attnum - 1) as usize);
        (table_attnum as i32 + if idx.bridging && !idx.primaryIsCtid { 1 } else { 0 }) as u16
    } else {
        attnum as u16
    }
}

pub unsafe fn ApplySortComparator(
    datum1: Datum,
    isnull1: bool,
    datum2: Datum,
    isnull2: bool,
    ssup: pgrx::pg_sys::SortSupport,
) -> i32 {
    if isnull1 {
        if isnull2 {
            0
        } else {
            if (*ssup).ssup_nulls_first { -1 } else { 1 }
        }
    } else {
        if isnull2 {
            if (*ssup).ssup_nulls_first { 1 } else { -1 }
        } else {
            let comp = (*ssup).comparator.expect("ssup->comparator is NULL");
            comp(datum1, datum2, ssup)
        }
    }
}

pub unsafe fn ApplySortAbbrevFullComparator(
    datum1: Datum,
    isnull1: bool,
    datum2: Datum,
    isnull2: bool,
    ssup: pgrx::pg_sys::SortSupport,
) -> i32 {
    if isnull1 {
        if isnull2 {
            0
        } else {
            if (*ssup).ssup_nulls_first { -1 } else { 1 }
        }
    } else {
        if isnull2 {
            if (*ssup).ssup_nulls_first { 1 } else { -1 }
        } else {
            let comp = (*ssup).abbrev_full_comparator.expect("abbrev_full_comparator is NULL");
            comp(datum1, datum2, ssup)
        }
    }
}

unsafe fn write_o_tuple(ptr: *mut std::ffi::c_void, tup: OTuple, tupsize: usize) {
    let mut p = ptr as *mut u8;
    *p = tup.formatFlags;
    p = p.add(8);
    std::ptr::copy_nonoverlapping(tup.data, p as *mut c_char, tupsize);
}

unsafe fn read_o_tuple(ptr: *mut std::ffi::c_void) -> OTuple {
    let mut p = ptr as *mut u8;
    let format_flags = *p;
    p = p.add(8);
    OTuple {
        data: p as *mut c_char,
        formatFlags: format_flags,
    }
}

unsafe extern "C" fn comparetup_orioledb_index(
    a: *const SortTuple,
    b: *const SortTuple,
    state: *mut Tuplesortstate,
) -> c_int {
    let base = TuplesortstateGetPublic(state);
    let mut sortKey = (*base).sortKeys;
    let arg = (*base).arg as *mut OIndexBuildSortArg;
    let spec = &mut (*(*arg).id).leafSpec;

    let a = &*a;
    let b = &*b;

    let mut compare = ApplySortComparator(a.datum1, a.isnull1, b.datum1, b.isnull1, sortKey);
    if compare != 0 {
        return compare;
    }

    let ltup = read_o_tuple(a.tuple);
    let rtup = read_o_tuple(b.tuple);
    let tupDesc = (*arg).tupDesc;

    if (*sortKey).abbrev_converter.is_some() {
        let attno = (*sortKey).ssup_attno;
        let mut isnull1 = false;
        let mut isnull2 = false;
        let datum1 = o_fastgetattr(ltup, attno as i32, tupDesc, spec, &mut isnull1);
        let datum2 = o_fastgetattr(rtup, attno as i32, tupDesc, spec, &mut isnull2);

        compare = ApplySortAbbrevFullComparator(datum1, isnull1, datum2, isnull2, sortKey);
        if compare != 0 {
            return compare;
        }
    }

    let mut equal_hasnull = false;
    if a.isnull1 {
        equal_hasnull = true;
    }

    sortKey = sortKey.add(1);
    for nkey in 1..(*base).nKeys {
        if !OIgnoreColumn((*arg).id, nkey as usize) {
            let attno = (*sortKey).ssup_attno;
            let mut isnull1 = false;
            let mut isnull2 = false;
            let datum1 = o_fastgetattr(ltup, attno as i32, tupDesc, spec, &mut isnull1);
            let datum2 = o_fastgetattr(rtup, attno as i32, tupDesc, spec, &mut isnull2);

            compare = ApplySortComparator(datum1, isnull1, datum2, isnull2, sortKey);
            if compare != 0 {
                return compare;
            }

            if isnull1 {
                equal_hasnull = true;
            }
        }
        sortKey = sortKey.add(1);
    }

    if (*arg).enforceUnique && !(!(*(*arg).id).nulls_not_distinct && equal_hasnull) {
        let name = std::ffi::CStr::from_ptr((*(*arg).id).name.data.as_ptr() as *const c_char).to_string_lossy();
        pgrx::ereport!(
            pgrx::pg_sys::ERROR,
            pgrx::pg_sys::ERRCODE_UNIQUE_VIOLATION,
            format!("could not create unique index \"{}\"", name),
            "Duplicate keys exist."
        );
    }

    0
}

unsafe extern "C" fn writetup_orioledb_index(
    state: *mut Tuplesortstate,
    tape: *mut pgrx::pg_sys::LogicalTape,
    stup: *mut SortTuple,
) {
    let base = TuplesortstateGetPublic(state);
    let arg = (*base).arg as *mut OIndexBuildSortArg;
    let spec = &mut (*(*arg).id).leafSpec;

    let tuple = read_o_tuple((*stup).tuple);
    let size = o_tuple_size(tuple, spec);
    let tuplen = (size + std::mem::size_of::<c_int>() + 1) as c_int;

    pgrx::pg_sys::LogicalTapeWrite(tape, &tuplen as *const c_int as *mut std::ffi::c_void, std::mem::size_of::<c_int>());
    pgrx::pg_sys::LogicalTapeWrite(tape, tuple.data as *mut std::ffi::c_void, size);
    pgrx::pg_sys::LogicalTapeWrite(tape, &tuple.formatFlags as *const u8 as *mut std::ffi::c_void, 1);

    if ((*base).sortopt & pgrx::pg_sys::TUPLESORT_RANDOMACCESS as i32) != 0 {
        pgrx::pg_sys::LogicalTapeWrite(tape, &tuplen as *const c_int as *mut std::ffi::c_void, std::mem::size_of::<c_int>());
    }
}

unsafe extern "C" fn readtup_orioledb_index(
    state: *mut Tuplesortstate,
    stup: *mut SortTuple,
    tape: *mut pgrx::pg_sys::LogicalTape,
    len: std::ffi::c_uint,
) {
    let base = TuplesortstateGetPublic(state);
    let arg = (*base).arg as *mut OIndexBuildSortArg;
    let spec = &mut (*(*arg).id).leafSpec;

    let tuplen = len - std::mem::size_of::<c_int>() as u32 - 1;
    let tup = pgrx::pg_sys::tuplesort_readtup_alloc(state, 8 + tuplen as usize) as *mut u8;

    pgrx::pg_sys::LogicalTapeReadExact(tape, tup.add(8) as *mut std::ffi::c_void, tuplen as usize);
    pgrx::pg_sys::LogicalTapeReadExact(tape, tup as *mut std::ffi::c_void, 1);

    if ((*base).sortopt & pgrx::pg_sys::TUPLESORT_RANDOMACCESS as i32) != 0 {
        let mut temp_tuplen: u32 = 0;
        pgrx::pg_sys::LogicalTapeReadExact(tape, &mut temp_tuplen as *mut u32 as *mut std::ffi::c_void, std::mem::size_of::<c_int>());
    }

    (*stup).tuple = tup as *mut std::ffi::c_void;
    let tuple = read_o_tuple((*stup).tuple);
    let mut isnull = false;
    (*stup).datum1 = o_fastgetattr(
        tuple,
        (*(*base).sortKeys.add(0)).ssup_attno as i32,
        (*arg).tupDesc,
        spec,
        &mut isnull,
    );
    (*stup).isnull1 = isnull;
}

unsafe extern "C" fn removeabbrev_orioledb_index(
    state: *mut Tuplesortstate,
    stups: *mut SortTuple,
    count: c_int,
) {
    let base = TuplesortstateGetPublic(state);
    let arg = (*base).arg as *mut OIndexBuildSortArg;
    let spec = &mut (*(*arg).id).leafSpec;

    for i in 0..count {
        let stup = &mut *stups.offset(i as isize);
        let tup = read_o_tuple(stup.tuple);
        let mut isnull = false;
        stup.datum1 = o_fastgetattr(
            tup,
            (*(*base).sortKeys.add(0)).ssup_attno as i32,
            (*arg).tupDesc,
            spec,
            &mut isnull,
        );
        stup.isnull1 = isnull;
    }
}

extern "C" {
    pub fn o_finish_sort_support_function(comparator: *mut std::ffi::c_void, ssup: pgrx::pg_sys::SortSupport);
    pub fn oFillFieldOpClassAndComparator(
        field: *mut OIndexField,
        datoid: pgrx::pg_sys::Oid,
        opclass: pgrx::pg_sys::Oid,
        type_: pgrx::pg_sys::Oid,
        collation: pgrx::pg_sys::Oid,
        hash_fn_oid: pgrx::pg_sys::Oid,
    );
    pub fn tuplesort_gettuple_common(state: *mut Tuplesortstate, forward: bool, stup: *mut SortTuple) -> bool;
}

#[no_mangle]
pub unsafe extern "C" fn tuplesort_begin_orioledb_index(
    idx: *mut OIndexDescr,
    workMem: c_int,
    randomAccess: bool,
    coordinate: pgrx::pg_sys::SortCoordinate,
) -> *mut Tuplesortstate {
    let state = pgrx::pg_sys::tuplesort_begin_common(workMem, coordinate, randomAccess);
    let base = TuplesortstateGetPublic(state);

    let oldcontext = pgrx::pg_sys::MemoryContextSwitchTo((*base).maincontext);

    let arg = pgrx::pg_sys::palloc0(std::mem::size_of::<OIndexBuildSortArg>()) as *mut OIndexBuildSortArg;
    (*arg).id = idx;
    (*arg).tupDesc = (*idx).leafTupdesc;
    (*arg).enforceUnique = (*idx).unique;

    let sort_fields = if (*idx).unique {
        (*idx).nKeyFields
    } else {
        (*idx).nFields
    };

    (*base).sortKeys = pgrx::pg_sys::palloc0(sort_fields as usize * std::mem::size_of::<pgrx::pg_sys::SortSupportData>()) as pgrx::pg_sys::SortSupport;
    (*base).nKeys = sort_fields;

    (*base).removeabbrev = Some(removeabbrev_orioledb_index);
    (*base).comparetup = Some(comparetup_orioledb_index);
    (*base).writetup = Some(writetup_orioledb_index);
    (*base).readtup = Some(readtup_orioledb_index);
    (*base).arg = arg as *mut std::ffi::c_void;

    for i in 0..sort_fields {
        if !OIgnoreColumn(idx, i as usize) {
            let sortKey = (*base).sortKeys.add(i as usize);
            (*sortKey).ssup_cxt = pgrx::pg_sys::CurrentMemoryContext;
            (*sortKey).ssup_collation = (*(*idx).fields.add(i as usize)).collation;
            (*sortKey).ssup_nulls_first = (*(*idx).fields.add(i as usize)).nullfirst;
            (*sortKey).ssup_attno = OIndexKeyAttnumToTupleAttnum(BTreeKeyType::LeafTuple, idx, i + 1) as i16;
            (*sortKey).abbreviate = i == 0;
            (*sortKey).ssup_reverse = !(*(*idx).fields.add(i as usize)).ascending;

            o_finish_sort_support_function((*(*idx).fields.add(i as usize)).comparator as *mut std::ffi::c_void, sortKey);
        }
    }

    pgrx::pg_sys::MemoryContextSwitchTo(oldcontext);

    state
}

#[no_mangle]
pub unsafe extern "C" fn tuplesort_begin_orioledb_toast(
    toast: *mut OIndexDescr,
    primary: *mut OIndexDescr,
    workMem: c_int,
    randomAccess: bool,
    coordinate: pgrx::pg_sys::SortCoordinate,
) -> *mut Tuplesortstate {
    let state = pgrx::pg_sys::tuplesort_begin_common(workMem, coordinate, randomAccess);
    let base = TuplesortstateGetPublic(state);

    let oldcontext = pgrx::pg_sys::MemoryContextSwitchTo((*base).maincontext);

    let arg = pgrx::pg_sys::palloc0(std::mem::size_of::<OIndexBuildSortArg>()) as *mut OIndexBuildSortArg;
    (*arg).id = primary;
    (*arg).tupDesc = (*toast).leafTupdesc;
    (*arg).enforceUnique = true;

    let key_fields = (*primary).nKeyFields;
    let nkeys = key_fields + 2;

    (*base).sortKeys = pgrx::pg_sys::palloc0(nkeys as usize * std::mem::size_of::<pgrx::pg_sys::SortSupportData>()) as pgrx::pg_sys::SortSupport;
    (*base).nKeys = nkeys;

    (*base).removeabbrev = Some(removeabbrev_orioledb_index);
    (*base).comparetup = Some(comparetup_orioledb_index);
    (*base).writetup = Some(writetup_orioledb_index);
    (*base).readtup = Some(readtup_orioledb_index);
    (*base).arg = arg as *mut std::ffi::c_void;

    for i in 0..key_fields {
        let sortKey = (*base).sortKeys.add(i as usize);
        (*sortKey).ssup_cxt = pgrx::pg_sys::CurrentMemoryContext;
        (*sortKey).ssup_collation = (*(*primary).fields.add(i as usize)).collation;
        (*sortKey).ssup_nulls_first = (*(*primary).fields.add(i as usize)).nullfirst;
        (*sortKey).ssup_attno = (i + 1) as i16;
        (*sortKey).abbreviate = i == 0;
        (*sortKey).ssup_reverse = !(*(*primary).fields.add(i as usize)).ascending;

        o_finish_sort_support_function((*(*primary).fields.add(i as usize)).comparator as *mut std::ffi::c_void, sortKey);
    }

    let mut field = OIndexField {
        inputtype: 0,
        opfamily: 0,
        opclass: 0,
        collation: pgrx::pg_sys::DEFAULT_COLLATION_OID,
        ascending: false,
        nullfirst: false,
        comparator: std::ptr::null_mut(),
        exclusion_fn: std::ptr::null_mut(),
        hash_fn: std::ptr::null_mut(),
    };

    const INT2OID: pgrx::pg_sys::Oid = 21;
    const INT4OID: pgrx::pg_sys::Oid = 23;
    const INT2_BTREE_OPS_OID: pgrx::pg_sys::Oid = 1979;
    const INT4_BTREE_OPS_OID: pgrx::pg_sys::Oid = 1978;
    const F_HASHINT2: pgrx::pg_sys::Oid = 450;
    const F_HASHINT4: pgrx::pg_sys::Oid = 451;

    let sortKey = (*base).sortKeys.add(key_fields as usize);
    (*sortKey).ssup_cxt = pgrx::pg_sys::CurrentMemoryContext;
    (*sortKey).ssup_collation = pgrx::pg_sys::DEFAULT_COLLATION_OID;
    (*sortKey).ssup_nulls_first = false;
    (*sortKey).ssup_attno = (key_fields + 1) as i16;
    (*sortKey).abbreviate = false;
    (*sortKey).ssup_reverse = false;
    oFillFieldOpClassAndComparator(
        &mut field,
        (*primary).oids.datoid,
        INT2_BTREE_OPS_OID,
        INT2OID,
        pgrx::pg_sys::InvalidOid,
        F_HASHINT2,
    );
    o_finish_sort_support_function(field.comparator as *mut std::ffi::c_void, sortKey);

    let sortKey = (*base).sortKeys.add((key_fields + 1) as usize);
    (*sortKey).ssup_cxt = pgrx::pg_sys::CurrentMemoryContext;
    (*sortKey).ssup_collation = pgrx::pg_sys::DEFAULT_COLLATION_OID;
    (*sortKey).ssup_nulls_first = false;
    (*sortKey).ssup_attno = (key_fields + 2) as i16;
    (*sortKey).abbreviate = false;
    (*sortKey).ssup_reverse = false;
    oFillFieldOpClassAndComparator(
        &mut field,
        (*primary).oids.datoid,
        INT4_BTREE_OPS_OID,
        INT4OID,
        pgrx::pg_sys::InvalidOid,
        F_HASHINT4,
    );
    o_finish_sort_support_function(field.comparator as *mut std::ffi::c_void, sortKey);

    pgrx::pg_sys::MemoryContextSwitchTo(oldcontext);

    state
}

#[no_mangle]
pub unsafe extern "C" fn tuplesort_getotuple(state: *mut Tuplesortstate, forward: bool) -> OTuple {
    let base = TuplesortstateGetPublic(state);
    let oldcontext = pgrx::pg_sys::MemoryContextSwitchTo((*base).sortcontext);
    let mut stup = std::mem::MaybeUninit::<SortTuple>::uninit();

    let found = tuplesort_gettuple_common(state, forward, stup.as_mut_ptr());

    pgrx::pg_sys::MemoryContextSwitchTo(oldcontext);

    if found {
        read_o_tuple(stup.assume_init().tuple)
    } else {
        OTuple {
            data: std::ptr::null_mut(),
            formatFlags: 0,
        }
    }
}

#[no_mangle]
pub unsafe extern "C" fn tuplesort_putotuple(state: *mut Tuplesortstate, tup: OTuple) {
    let base = TuplesortstateGetPublic(state);
    let arg = (*base).arg as *mut OIndexBuildSortArg;
    let spec = &mut (*(*arg).id).leafSpec;
    let oldcontext = pgrx::pg_sys::MemoryContextSwitchTo((*base).tuplecontext);

    let tupsize = o_tuple_size(tup, spec);
    let tuple_alloc = pgrx::pg_sys::MemoryContextAlloc((*base).tuplecontext, 8 + tupsize);
    write_o_tuple(tuple_alloc, tup, tupsize);
    let written_tup = read_o_tuple(tuple_alloc);

    let mut isnull = false;
    let mut stup = SortTuple {
        datum1: o_fastgetattr(
            written_tup,
            (*(*base).sortKeys.add(0)).ssup_attno as i32,
            (*arg).tupDesc,
            spec,
            &mut isnull,
        ),
        isnull1: isnull,
        tuple: tuple_alloc,
    };

    let abbreviate = (*(*base).sortKeys.add(0)).abbrev_converter.is_some() && !stup.isnull1;

    #[cfg(any(feature = "pg17", feature = "pg18", feature = "pg19"))]
    {
        extern "C" {
            pub fn TupleSortUseBumpTupleCxt(sortopt: std::ffi::c_int) -> bool;
            pub fn GetMemoryChunkSpace(pointer: *mut std::ffi::c_void) -> usize;
            pub fn tuplesort_puttuple_common(
                state: *mut Tuplesortstate,
                stup: *mut SortTuple,
                abbreviate: bool,
                tuplen: usize,
            );
        }
        let tuplen = if TupleSortUseBumpTupleCxt((*base).sortopt) {
            maxalign(tupsize)
        } else {
            GetMemoryChunkSpace(stup.tuple)
        };
        tuplesort_puttuple_common(state, &mut stup, abbreviate, tuplen);
    }

    #[cfg(not(any(feature = "pg17", feature = "pg18", feature = "pg19")))]
    {
        extern "C" {
            pub fn tuplesort_puttuple_common(
                state: *mut Tuplesortstate,
                stup: *mut SortTuple,
                abbreviate: bool,
            );
        }
        tuplesort_puttuple_common(state, &mut stup, abbreviate);
    }

    pgrx::pg_sys::MemoryContextSwitchTo(oldcontext);
}
