/*-------------------------------------------------------------------------
 *
 * format.rs
 * 		Routines for accessing tuples in orioledb format.
 *
 * Copyright (c) 2021-2026, Oriole DB Inc.
 * Copyright (c) 2025-2026, Supabase Inc.
 *
 * IDENTIFICATION
 *	  contrib/orioledb/src/tuple/format.rs
 *
 *-------------------------------------------------------------------------
 */

use std::ffi::{c_char, c_int};
use pgrx::pg_sys::{Datum, TupleDesc, bits8, FormData_pg_attribute};

pub const O_TUPLE_FLAGS_FIXED_FORMAT: u8 = 0x1;

#[repr(C)]
#[derive(Copy, Clone, Debug)]
pub struct ORelOids {
    pub datoid: pgrx::pg_sys::Oid,
    pub reloid: pgrx::pg_sys::Oid,
    pub relnode: pgrx::pg_sys::Oid,
}

#[repr(C)]
#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub enum OIndexType {
    Invalid = 0,
    Toast = 1,
    Bridge = 2,
    Primary = 3,
    Unique = 4,
    Regular = 5,
    Exclusion = 6,
}


#[repr(C)]
#[derive(Copy, Clone, Debug)]
pub struct OTuple {
    pub data: *mut c_char,
    pub formatFlags: u8,
}

impl OTuple {
    pub fn is_null(&self) -> bool {
        self.data.is_null()
    }

    pub fn set_null(&mut self) {
        self.data = std::ptr::null_mut();
        self.formatFlags = 0;
    }
}

#[repr(C)]
#[derive(Copy, Clone, Debug)]
pub struct OTupleReaderState {
    pub desc: TupleDesc,
    pub tp: *mut c_char,
    pub bp: *mut bits8,
    pub off: u32,
    pub attnum: u16,
    pub natts: u16,
    pub hasnulls: bool,
    pub slow: bool,
}

#[repr(C)]
#[derive(Copy, Clone, Debug)]
pub struct OTupleHeaderData {
    pub flags_and_len: u16, // hasnulls: 1, len: 15
    pub natts: u16,
    pub version: u32,
}

impl OTupleHeaderData {
    pub fn hasnulls(&self) -> bool {
        (self.flags_and_len & 1) != 0
    }

    pub fn set_hasnulls(&mut self, val: bool) {
        if val {
            self.flags_and_len |= 1;
        } else {
            self.flags_and_len &= !1;
        }
    }

    pub fn len(&self) -> u16 {
        self.flags_and_len >> 1
    }

    pub fn set_len(&mut self, val: u16) {
        self.flags_and_len = (self.flags_and_len & 1) | (val << 1);
    }
}

#[repr(C)]
#[derive(Copy, Clone, Debug)]
pub struct OTupleFixedFormatSpec {
    pub natts: u16,
    pub len: u16,
}

pub type OTupleHeader = *mut OTupleHeaderData;

pub const SizeOfOTupleHeader: usize = maxalign(std::mem::size_of::<OTupleHeaderData>());

#[repr(C)]
#[derive(Copy, Clone, Debug)]
pub struct BridgeData {
    pub is_pkey: bool,
    pub bridge_iptr: pgrx::pg_sys::ItemPointerData,
    pub attnum: pgrx::pg_sys::AttrNumber,
}

pub type OTupleAttrCompact = pgrx::pg_sys::CompactAttribute;
pub type OTupleAttrFull = FormData_pg_attribute;

pub const fn maxalign(len: usize) -> usize {
    (len + 7) & !7
}

pub fn BITMAPLEN(natts: usize) -> usize {
    (natts + 7) / 8
}

pub unsafe fn att_isnull(attnum: usize, bits: *const u8) -> bool {
    let byte = attnum >> 3;
    let bit = attnum & 0x07;
    (*bits.add(byte) & (1 << bit)) == 0
}

pub unsafe fn TupleDescAttr(tupdesc: TupleDesc, i: usize) -> *mut FormData_pg_attribute {
    pgrx::pg_sys::TupleDescAttr(tupdesc, i as c_int)
}

pub unsafe fn OTupleDescAttrFast(tupdesc: TupleDesc, i: usize) -> *mut OTupleAttrCompact {
    pgrx::pg_sys::TupleDescCompactAttr(tupdesc, i as c_int)
}

pub unsafe fn OTupleDescAttrSlow(tupdesc: TupleDesc, i: usize) -> *mut OTupleAttrFull {
    TupleDescAttr(tupdesc, i)
}

pub fn att_align_nominal(cur_offset: usize, attalign: c_char) -> usize {
    let mask = match attalign as u8 as char {
        'd' => 7,
        'i' => 3,
        's' => 1,
        _ => 0,
    };
    (cur_offset + mask) & !mask
}

pub fn att_align_nominal_ptr(ptr: *mut c_char, attalign: c_char) -> *mut c_char {
    let addr = ptr as usize;
    let aligned = att_align_nominal(addr, attalign);
    aligned as *mut c_char
}

pub fn att_nominal_alignby(cur_offset: usize, attalignby: u8) -> usize {
    let alignby = attalignby as usize;
    (cur_offset + (alignby - 1)) & !(alignby - 1)
}

pub unsafe fn att_pointer_alignby(cur_offset: usize, attalignby: u8, attlen: i16, attptr: *const c_char) -> usize {
    if attlen == -1 && *(attptr as *const u8) != 0 {
        cur_offset
    } else {
        att_nominal_alignby(cur_offset, attalignby)
    }
}

pub fn o_att_align_nominal(att: &OTupleAttrCompact, cur_offset: usize) -> usize {
    att_nominal_alignby(cur_offset, att.attalignby)
}

pub unsafe fn o_att_align_pointer(att: &OTupleAttrCompact, cur_offset: usize, attlen: i16, attptr: *const c_char) -> usize {
    att_pointer_alignby(cur_offset, att.attalignby, attlen, attptr)
}

#[repr(C, packed)]
pub struct varattrib_1b {
    pub va_header: u8,
    pub va_data: [u8; 0],
}

#[repr(C, packed)]
pub struct varattrib_4b {
    pub va_header: u32,
    pub va_data: [u8; 0],
}

pub unsafe fn VARATT_IS_SHORT(ptr: *const c_char) -> bool {
    let va = &*(ptr as *const varattrib_1b);
    (va.va_header & 0x80) != 0
}

pub unsafe fn VARSIZE_SHORT(ptr: *const c_char) -> usize {
    let va = &*(ptr as *const varattrib_1b);
    (va.va_header & 0x7F) as usize
}

pub unsafe fn VARATT_IS_EXTERNAL(ptr: *const c_char) -> bool {
    let va = &*(ptr as *const varattrib_1b);
    (va.va_header & 0xFF) == 0x01
}

pub unsafe fn VARATT_IS_COMPRESSED(ptr: *const c_char) -> bool {
    let va = &*(ptr as *const varattrib_1b);
    (va.va_header & 0xC0) == 0x40
}

pub unsafe fn VARATT_IS_EXTERNAL_EXPANDED(ptr: *const c_char) -> bool {
    pgrx::pg_sys::VARATT_IS_EXTERNAL_EXPANDED(ptr as *mut pgrx::pg_sys::varlena)
}

pub unsafe fn VARSIZE(ptr: *const c_char) -> usize {
    let header_4b = *(ptr as *const u32);
    (header_4b & 0x3FFFFFFF) as usize
}

pub unsafe fn VARSIZE_ANY(ptr: *const c_char) -> usize {
    let header = *(ptr as *const u8);
    if header == 0x01 {
        18
    } else if (header & 0x80) != 0 {
        (header & 0x7F) as usize
    } else {
        VARSIZE(ptr)
    }
}

pub unsafe fn VARSIZE_ANY_EXHDR(value: Datum) -> usize {
    pgrx::pg_sys::VARSIZE_ANY_EXHDR(value.value() as *mut pgrx::pg_sys::varlena) as usize
}

pub unsafe fn VARATT_CAN_MAKE_SHORT(ptr: *const c_char) -> bool {
    VARSIZE(ptr) <= 129
}

pub unsafe fn VARATT_CONVERTED_SHORT_SIZE(ptr: *const c_char) -> usize {
    VARSIZE(ptr) - 3
}

pub unsafe fn att_align_pointer(
    cur_offset: usize,
    attalign: c_char,
    attlen: i16,
    attptr: *const c_char,
) -> usize {
    if attlen == -1 {
        if attalign as u8 as char == 'd' {
            if VARATT_IS_SHORT(attptr) {
                cur_offset
            } else {
                att_align_nominal(cur_offset, attalign)
            }
        } else {
            cur_offset
        }
    } else {
        att_align_nominal(cur_offset, attalign)
    }
}

pub unsafe fn att_addlength_pointer(cur_offset: usize, attlen: i16, attptr: *const c_char) -> usize {
    if attlen > 0 {
        cur_offset + attlen as usize
    } else if attlen == -1 {
        cur_offset + VARSIZE_ANY(attptr)
    } else {
        cur_offset + std::ffi::CStr::from_ptr(attptr).to_bytes().len() + 1
    }
}

pub unsafe fn att_align_datum(cur_offset: usize, attalign: c_char, attlen: i16, attval: Datum) -> usize {
    if attlen == -1 {
        att_align_pointer(cur_offset, attalign, attlen, attval.value() as *const c_char)
    } else {
        att_align_nominal(cur_offset, attalign)
    }
}

pub unsafe fn att_addlength_datum(cur_offset: usize, attlen: i16, attval: Datum) -> usize {
    att_addlength_pointer(cur_offset, attlen, attval.value() as *const c_char)
}

#[cfg(target_endian = "big")]
pub unsafe fn SET_TOAST_POINTER(ptr: *mut std::ffi::c_void) {
    *(ptr as *mut u8) = 0x80;
}
#[cfg(target_endian = "big")]
pub unsafe fn IS_TOAST_POINTER(ptr: *const std::ffi::c_void) -> bool {
    *(ptr as *const u8) == 0x80
}

#[cfg(not(target_endian = "big"))]
pub unsafe fn SET_TOAST_POINTER(ptr: *mut std::ffi::c_void) {
    *(ptr as *mut u8) = 0x01;
}
#[cfg(not(target_endian = "big"))]
pub unsafe fn IS_TOAST_POINTER(ptr: *const std::ffi::c_void) -> bool {
    *(ptr as *const u8) == 0x01
}

pub fn ATT_IS_PACKABLE(att: &FormData_pg_attribute) -> bool {
    att.attlen == -1 && att.attstorage != 'p' as i8
}

pub fn VARLENA_ATT_IS_PACKABLE(att: &FormData_pg_attribute) -> bool {
    att.attstorage != 'p' as i8
}

pub trait OTupleAttr {
    fn attlen(&self) -> i16;
    fn attbyval(&self) -> bool;
}

impl OTupleAttr for FormData_pg_attribute {
    fn attlen(&self) -> i16 { self.attlen }
    fn attbyval(&self) -> bool { self.attbyval }
}

impl OTupleAttr for pgrx::pg_sys::CompactAttribute {
    fn attlen(&self) -> i16 { self.attlen }
    fn attbyval(&self) -> bool { self.attbyval }
}

pub unsafe fn fetchatt<T: OTupleAttr>(att: *const T, attval: *const c_char) -> Datum {
    let att = &*att;
    if att.attbyval() {
        match att.attlen() {
            1 => Datum::from(*(attval as *const u8) as usize),
            2 => Datum::from(*(attval as *const u16) as usize),
            4 => Datum::from(*(attval as *const u32) as usize),
            8 => Datum::from(*(attval as *const u64) as usize),
            _ => panic!("Unsupported attlen for attbyval: {}", att.attlen()),
        }
    } else {
        Datum::from(attval as usize)
    }
}

pub unsafe fn store_att_byval(ptr: *mut c_char, newval: Datum, attlen: i16) {
    match attlen {
        1 => *(ptr as *mut u8) = newval.value() as u8,
        2 => *(ptr as *mut u16) = newval.value() as u16,
        4 => *(ptr as *mut u32) = newval.value() as u32,
        8 => *(ptr as *mut u64) = newval.value() as u64,
        _ => panic!("Unsupported attlen for store_att_byval: {}", attlen),
    }
}

pub unsafe fn o_fastgetattr(
    tup: OTuple,
    attnum: i32,
    tuple_desc: TupleDesc,
    spec: *const OTupleFixedFormatSpec,
    isnull: &mut bool,
) -> Datum {
    assert!(attnum > 0);
    *isnull = false;
    if (tup.formatFlags & O_TUPLE_FLAGS_FIXED_FORMAT) != 0 {
        if (attnum - 1) < (*spec).natts as i32 {
            let att = OTupleDescAttrFast(tuple_desc, (attnum - 1) as usize);
            if (*att).attcacheoff >= 0 {
                fetchatt(att, tup.data.offset((*att).attcacheoff as isize))
            } else {
                o_toast_nocachegetattr(tup, attnum, tuple_desc, spec, isnull)
            }
        } else {
            *isnull = true;
            Datum::from(0)
        }
    } else {
        let header = tup.data as *const OTupleHeaderData;
        if !(*header).hasnulls() {
            let att = OTupleDescAttrFast(tuple_desc, (attnum - 1) as usize);
            if (*att).attcacheoff >= 0 {
                fetchatt(att, tup.data.add(SizeOfOTupleHeader).offset((*att).attcacheoff as isize))
            } else {
                o_toast_nocachegetattr(tup, attnum, tuple_desc, spec, isnull)
            }
        } else {
            let bits = tup.data.add(SizeOfOTupleHeader) as *const u8;
            if att_isnull((attnum - 1) as usize, bits) {
                *isnull = true;
                Datum::from(0)
            } else {
                o_toast_nocachegetattr(tup, attnum, tuple_desc, spec, isnull)
            }
        }
    }
}

pub unsafe fn o_fastgetattr_ptr(
    tup: OTuple,
    attnum: i32,
    tuple_desc: TupleDesc,
    spec: *const OTupleFixedFormatSpec,
) -> *mut c_char {
    assert!(attnum > 0);
    if (tup.formatFlags & O_TUPLE_FLAGS_FIXED_FORMAT) != 0 {
        if (attnum - 1) < (*spec).natts as i32 {
            let att = OTupleDescAttrFast(tuple_desc, (attnum - 1) as usize);
            if (*att).attcacheoff >= 0 {
                tup.data.offset((*att).attcacheoff as isize)
            } else {
                o_toast_nocachegetattr_ptr(tup, attnum, tuple_desc, spec)
            }
        } else {
            std::ptr::null_mut()
        }
    } else {
        let header = tup.data as *const OTupleHeaderData;
        if !(*header).hasnulls() {
            let att = OTupleDescAttrFast(tuple_desc, (attnum - 1) as usize);
            if (*att).attcacheoff >= 0 {
                tup.data.add(SizeOfOTupleHeader).offset((*att).attcacheoff as isize)
            } else {
                o_toast_nocachegetattr_ptr(tup, attnum, tuple_desc, spec)
            }
        } else {
            let bits = tup.data.add(SizeOfOTupleHeader) as *const u8;
            if att_isnull((attnum - 1) as usize, bits) {
                std::ptr::null_mut()
            } else {
                o_toast_nocachegetattr_ptr(tup, attnum, tuple_desc, spec)
            }
        }
    }
}

pub unsafe fn o_tuple_size(tup: OTuple, spec: *const OTupleFixedFormatSpec) -> usize {
    if (tup.formatFlags & O_TUPLE_FLAGS_FIXED_FORMAT) != 0 {
        (*spec).len as usize
    } else {
        let header = tup.data as *const OTupleHeaderData;
        (*header).len() as usize
    }
}

pub unsafe fn o_has_nulls(tup: OTuple) -> bool {
    if (tup.formatFlags & O_TUPLE_FLAGS_FIXED_FORMAT) != 0 {
        false
    } else {
        let header = tup.data as *const OTupleHeaderData;
        (*header).hasnulls()
    }
}

#[no_mangle]
pub unsafe extern "C" fn o_tuple_init_reader(
    state: *mut OTupleReaderState,
    tuple: OTuple,
    desc: TupleDesc,
    spec: *mut OTupleFixedFormatSpec,
) {
    let data = tuple.data;
    let header = data as *mut OTupleHeaderData;
    let state = &mut *state;

    if (tuple.formatFlags & O_TUPLE_FLAGS_FIXED_FORMAT) != 0 {
        state.bp = std::ptr::null_mut();
        state.tp = data;
        state.hasnulls = false;
        state.natts = (*spec).natts;
    } else if (*header).hasnulls() {
        state.bp = data.add(SizeOfOTupleHeader) as *mut bits8;
        state.tp = data.add(SizeOfOTupleHeader).add(maxalign(BITMAPLEN((*header).natts as usize)));
        state.hasnulls = true;
        state.natts = (*header).natts;
    } else {
        state.bp = std::ptr::null_mut();
        state.tp = data.add(SizeOfOTupleHeader);
        state.hasnulls = false;
        state.natts = (*header).natts;
    }
    state.off = 0;
    state.attnum = 0;
    state.desc = desc;
    state.slow = false;
}

#[no_mangle]
pub unsafe extern "C" fn o_tuple_next_field_offset(
    state: *mut OTupleReaderState,
    att: *mut OTupleAttrCompact,
) -> u32 {
    let state = &mut *state;
    let att = &mut *att;
    let off: u32;

    if !state.slow && att.attcacheoff >= 0 {
        state.off = att.attcacheoff as u32;
    } else if att.attlen == -1 {
        if !state.slow && state.off as usize == o_att_align_nominal(att, state.off as usize) {
            att.attcacheoff = state.off as i32;
        } else {
            state.off = o_att_align_pointer(att, state.off as usize, -1, state.tp.add(state.off as usize)) as u32;
            state.slow = true;
        }
    } else {
        state.off = o_att_align_nominal(att, state.off as usize) as u32;
        if !state.slow {
            att.attcacheoff = state.off as i32;
        }
    }

    off = state.off;

    if !att.attbyval && att.attlen < 0 && IS_TOAST_POINTER(state.tp.add(state.off as usize) as *const std::ffi::c_void) {
        state.off += std::mem::size_of::<OToastValue>() as u32;
    } else {
        state.off = att_addlength_pointer(state.off as usize, att.attlen, state.tp.add(state.off as usize)) as u32;
    }

    if att.attlen <= 0 {
        state.slow = true;
    }

    state.attnum += 1;

    off
}

#[no_mangle]
pub unsafe extern "C" fn o_tuple_read_next_field(
    state: *mut OTupleReaderState,
    isnull: *mut bool,
) -> Datum {
    let state = &mut *state;
    let att = OTupleDescAttrFast(state.desc, state.attnum as usize);

    if state.attnum >= state.natts {
        if (*att).atthasmissing {
            let result = pgrx::pg_sys::getmissingattr(state.desc, (state.attnum + 1) as i32, isnull);
            state.attnum += 1;
            return result;
        } else {
            *isnull = true;
            state.attnum += 1;
            return Datum::from(0);
        }
    }

    if state.hasnulls && att_isnull(state.attnum as usize, state.bp) {
        *isnull = true;
        state.slow = true;
        state.attnum += 1;
        return Datum::from(0);
    }

    *isnull = false;
    let off = o_tuple_next_field_offset(state, att);

    fetchatt(att, state.tp.add(off as usize))
}

pub unsafe fn o_tuple_read_next_field_ptr(state: &mut OTupleReaderState) -> *mut c_char {
    if state.attnum >= state.natts {
        return std::ptr::null_mut();
    }

    if state.hasnulls && att_isnull(state.attnum as usize, state.bp) {
        state.slow = true;
        state.attnum += 1;
        return std::ptr::null_mut();
    }

    let att = OTupleDescAttrFast(state.desc, state.attnum as usize);
    let off = o_tuple_next_field_offset(state, att);

    state.tp.add(off as usize)
}

#[no_mangle]
pub unsafe extern "C" fn o_tuple_get_last_iptr(
    desc: TupleDesc,
    spec: *mut OTupleFixedFormatSpec,
    tuple: OTuple,
    isnull: *mut bool,
) -> pgrx::pg_sys::ItemPointer {
    if (tuple.formatFlags & O_TUPLE_FLAGS_FIXED_FORMAT) == 0 {
        let header = tuple.data as *const OTupleHeaderData;
        let bp = tuple.data.add(SizeOfOTupleHeader) as *const u8;

        if (*header).hasnulls() && att_isnull((*desc).natts as usize - 1, bp) {
            *isnull = true;
            return std::ptr::null_mut();
        }

        *isnull = false;
        tuple.data.add((*header).len() as usize - std::mem::size_of::<pgrx::pg_sys::ItemPointerData>()) as pgrx::pg_sys::ItemPointer
    } else {
        if (*spec).natts < (*desc).natts as u16 {
            *isnull = true;
            return std::ptr::null_mut();
        }

        *isnull = false;
        tuple.data.add((*spec).len as usize - std::mem::size_of::<pgrx::pg_sys::ItemPointerData>()) as pgrx::pg_sys::ItemPointer
    }
}

#[no_mangle]
pub unsafe extern "C" fn o_toast_nocachegetattr_ptr(
    tuple: OTuple,
    mut attnum: c_int,
    tupleDesc: TupleDesc,
    spec: *const OTupleFixedFormatSpec,
) -> *mut c_char {
    let tup = tuple.data as *mut OTupleHeaderData;
    let tp: *mut c_char;
    let mut slow = false;
    let mut result: *mut c_char = std::ptr::null_mut();

    attnum -= 1;

    if (tuple.formatFlags & O_TUPLE_FLAGS_FIXED_FORMAT) != 0 {
        tp = tuple.data;
    } else if (*tup).hasnulls() {
        let byte = attnum >> 3;
        let finalbit = attnum & 0x07;
        let bp = tuple.data.add(SizeOfOTupleHeader) as *const u8;

        if ((!*bp.add(byte as usize)) & ((1 << finalbit) - 1)) != 0 {
            slow = true;
        } else {
            for i in 0..byte {
                if *bp.add(i as usize) != 0xFF {
                    slow = true;
                    break;
                }
            }
        }
        tp = tuple.data.add(SizeOfOTupleHeader).add(maxalign(BITMAPLEN((*tup).natts as usize)));
    } else {
        tp = tuple.data.add(SizeOfOTupleHeader);
    }

    if !slow {
        let att = OTupleDescAttrFast(tupleDesc, attnum as usize);
        if (*att).attcacheoff >= 0 {
            return tp.offset((*att).attcacheoff as isize);
        }
    }

    let mut reader = std::mem::MaybeUninit::<OTupleReaderState>::uninit();
    o_tuple_init_reader(reader.as_mut_ptr(), tuple, tupleDesc, spec as *mut OTupleFixedFormatSpec);
    let mut reader = reader.assume_init();

    for _ in 0..=attnum {
        result = o_tuple_read_next_field_ptr(&mut reader);
    }
    assert!(!result.is_null());

    result
}

#[no_mangle]
pub unsafe extern "C" fn o_toast_nocachegetattr(
    tuple: OTuple,
    mut attnum: c_int,
    tupleDesc: TupleDesc,
    spec: *const OTupleFixedFormatSpec,
    is_null: *mut bool,
) -> Datum {
    let tup = tuple.data as *mut OTupleHeaderData;
    let tp: *mut c_char;
    let mut slow = false;
    let mut result = Datum::from(0);

    *is_null = false;
    attnum -= 1;

    if (tuple.formatFlags & O_TUPLE_FLAGS_FIXED_FORMAT) != 0 {
        tp = tuple.data;
    } else if (*tup).hasnulls() {
        let byte = attnum >> 3;
        let finalbit = attnum & 0x07;
        let bp = tuple.data.add(SizeOfOTupleHeader) as *const u8;

        if ((!*bp.add(byte as usize)) & ((1 << finalbit) - 1)) != 0 {
            slow = true;
        } else {
            for i in 0..byte {
                if *bp.add(i as usize) != 0xFF {
                    slow = true;
                    break;
                }
            }
        }
        tp = tuple.data.add(SizeOfOTupleHeader).add(maxalign(BITMAPLEN((*tup).natts as usize)));
    } else {
        tp = tuple.data.add(SizeOfOTupleHeader);
    }

    if !slow {
        let att = OTupleDescAttrFast(tupleDesc, attnum as usize);
        if (*att).attcacheoff >= 0 {
            return fetchatt(att, tp.offset((*att).attcacheoff as isize));
        }
    }

    let mut reader = std::mem::MaybeUninit::<OTupleReaderState>::uninit();
    o_tuple_init_reader(reader.as_mut_ptr(), tuple, tupleDesc, spec as *mut OTupleFixedFormatSpec);
    let mut reader = reader.assume_init();

    for _ in 0..=attnum {
        result = o_tuple_read_next_field(&mut reader, is_null);
    }

    if *is_null && (tuple.formatFlags & O_TUPLE_FLAGS_FIXED_FORMAT) == 0 && !(*tup).hasnulls() && (*tup).natts < (*tupleDesc).natts as u16 {
        *is_null = true;
        return Datum::from(0);
    }

    assert!(!*is_null);

    result
}

#[no_mangle]
pub unsafe extern "C" fn o_tuple_get_data(
    tuple: OTuple,
    size: *mut c_int,
    spec: *mut OTupleFixedFormatSpec,
) -> *mut c_char {
    if (tuple.formatFlags & O_TUPLE_FLAGS_FIXED_FORMAT) == 0 {
        let header = tuple.data as *mut OTupleHeaderData;
        let hasnull_off = if (*header).hasnulls() {
            maxalign(BITMAPLEN((*header).natts as usize))
        } else {
            0
        };
        let hoff = SizeOfOTupleHeader + hasnull_off;
        *size = ((*header).len() as usize - hoff) as c_int;
        tuple.data.add(hoff)
    } else {
        *size = (*spec).len as c_int;
        tuple.data
    }
}

unsafe fn o_tuple_compute_data_size(
    tupleDesc: TupleDesc,
    iptr: pgrx::pg_sys::ItemPointer,
    bridge_data: *mut BridgeData,
    values: *mut Datum,
    isnull: *mut bool,
    to_toast: *mut c_char,
    natts: c_int,
) -> usize {
    let mut data_length = 0;
    let has_bridge_ctid = !bridge_data.is_null() && (*bridge_data).attnum != pgrx::pg_sys::InvalidAttrNumber as i16;
    let mut ctid_off = 0;

    if !iptr.is_null() {
        ctid_off += 1;
    }
    if has_bridge_ctid {
        ctid_off += 1;
    }

    for i in 0..natts {
        let val: Datum;
        if i == 0 && !iptr.is_null() {
            val = Datum::from(iptr as usize);
        } else if has_bridge_ctid && i == ((*bridge_data).attnum - 1) as i32 {
            val = Datum::from(&(*bridge_data).bridge_iptr as *const pgrx::pg_sys::ItemPointerData as usize);
        } else {
            if !to_toast.is_null() && *to_toast.offset((i - ctid_off) as isize) == ORIOLEDB_TO_TOAST_ON as i8 {
                data_length += std::mem::size_of::<OToastValue>();
                continue;
            }
            if *isnull.offset((i - ctid_off) as isize) {
                continue;
            }
            val = *values.offset((i - ctid_off) as isize);
        }

        let atti = &*TupleDescAttr(tupleDesc, i as usize);
        if ATT_IS_PACKABLE(atti) && VARATT_CAN_MAKE_SHORT(val.value() as *const c_char) {
            data_length += VARATT_CONVERTED_SHORT_SIZE(val.value() as *const c_char);
        } else if atti.attlen == -1 && VARATT_IS_EXTERNAL_EXPANDED(val.value() as *const c_char) {
            data_length = att_align_nominal(data_length, atti.attalign);
            data_length += pgrx::pg_sys::EOH_get_flat_size(val.value() as *mut pgrx::pg_sys::ExpandedObjectHeader);
        } else {
            data_length = att_align_datum(data_length, atti.attalign, atti.attlen, val);
            data_length = att_addlength_datum(data_length, atti.attlen, val);
        }
    }

    data_length
}

#[no_mangle]
pub unsafe extern "C" fn o_new_tuple_size(
    tupleDesc: TupleDesc,
    spec: *mut OTupleFixedFormatSpec,
    iptr: pgrx::pg_sys::ItemPointer,
    bridge_data: *mut BridgeData,
    version: u32,
    values: *mut Datum,
    isnull: *mut bool,
    to_toast: *mut c_char,
) -> pgrx::pg_sys::Size {
    let mut hasnull = false;
    let mut fixedFormat = version == 0;
    let mut natts = (*tupleDesc).natts;
    let mut ctid_off = 0;
    let has_bridge_ctid = !bridge_data.is_null() && (*bridge_data).attnum != pgrx::pg_sys::InvalidAttrNumber as i16;

    if !iptr.is_null() {
        ctid_off += 1;
    }
    if has_bridge_ctid {
        ctid_off += 1;
    }

    for i in ctid_off..natts {
        if *isnull.offset((i - ctid_off) as isize) {
            fixedFormat = false;
            hasnull = true;
        } else if i >= (*spec).natts as i32 {
            fixedFormat = false;
        }
    }

    let mut result: usize;
    if !fixedFormat {
        result = SizeOfOTupleHeader;
        if hasnull {
            result += maxalign(BITMAPLEN(natts as usize));
        }
    } else {
        result = 0;
        natts = (*spec).natts as i32;
    }

    result += o_tuple_compute_data_size(tupleDesc, iptr, bridge_data, values, isnull, to_toast, natts);

    result as pgrx::pg_sys::Size
}

#[no_mangle]
pub unsafe extern "C" fn o_tuple_fill(
    tupleDesc: TupleDesc,
    spec: *mut OTupleFixedFormatSpec,
    tuple: *mut OTuple,
    tuple_size: pgrx::pg_sys::Size,
    iptr: pgrx::pg_sys::ItemPointer,
    bridge_data: *mut BridgeData,
    version: u32,
    values: *mut Datum,
    isnull: *mut bool,
    to_toast: *mut c_char,
) {
    let tup = (*tuple).data as *mut OTupleHeaderData;
    let mut bitP: *mut bits8 = std::ptr::null_mut();
    let mut bitmask: bits8 = 0;
    let mut natts = (*tupleDesc).natts;
    let hoff: usize;
    let mut ctid_off = 0;
    let mut len: usize;
    let mut hasnull = false;
    let mut fixedFormat = version == 0;
    let data: *mut c_char;
    let has_bridge_ctid = !bridge_data.is_null() && (*bridge_data).attnum != pgrx::pg_sys::InvalidAttrNumber as i16;

    if !iptr.is_null() {
        ctid_off += 1;
    }
    if !bridge_data.is_null() && (*bridge_data).is_pkey {
        ctid_off += 1;
    }

    for i in ctid_off..natts {
        if *isnull.offset((i - ctid_off) as isize) {
            fixedFormat = false;
            hasnull = true;
        } else if i >= (*spec).natts as i32 {
            fixedFormat = false;
        }
    }

    if !fixedFormat {
        (*tup).set_hasnulls(hasnull);
        (*tup).set_len(tuple_size as u16);
        (*tup).natts = natts as u16;
        (*tup).version = version;
        len = SizeOfOTupleHeader;
        if hasnull {
            len += maxalign(BITMAPLEN(natts as usize));
        }
        hoff = len;
        if hasnull {
            bitP = (*tuple).data.add(SizeOfOTupleHeader - 1) as *mut bits8;
            bitmask = 0x80;
        }
        (*tuple).formatFlags = 0;
    } else {
        len = 0;
        hoff = 0;
        natts = (*spec).natts as i32;
        hasnull = false;
        (*tuple).formatFlags = O_TUPLE_FLAGS_FIXED_FORMAT;
    }

    data = (*tuple).data.add(hoff);
    let mut data_ptr = data;

    for i in 0..natts {
        let att = &*TupleDescAttr(tupleDesc, i as usize);
        let mut data_length = 0;
        let value: Datum;
        let null: bool;
        let cur_to_toast: bool;

        if i == 0 && !iptr.is_null() {
            cur_to_toast = false;
            value = Datum::from(iptr as usize);
            null = false;
        } else if has_bridge_ctid && i == ((*bridge_data).attnum - 1) as i32 {
            cur_to_toast = false;
            value = Datum::from(&(*bridge_data).bridge_iptr as *const pgrx::pg_sys::ItemPointerData as usize);
            null = false;
        } else {
            cur_to_toast = !to_toast.is_null() && *to_toast.offset((i - ctid_off) as isize) == ORIOLEDB_TO_TOAST_ON as i8;
            value = *values.offset((i - ctid_off) as isize);
            null = *isnull.offset((i - ctid_off) as isize);
        }

        if cur_to_toast {
            let mut toastValue = OToastValue {
                pointer: 0,
                compression: 0,
                raw_size: 0,
                toasted_size: 0,
            };
            SET_TOAST_POINTER(&mut toastValue as *mut OToastValue as *mut std::ffi::c_void);
            toastValue.raw_size = crate::tuple::toast::o_get_raw_size(value);
            toastValue.toasted_size = crate::tuple::toast::o_get_src_size(value);

            let val_ptr = value.value() as *const c_char;
            if VARATT_IS_COMPRESSED(val_ptr) {
                let mut cmp = att.attcompression;
                if cmp == pgrx::pg_sys::InvalidCompressionMethod as i8 {
                    cmp = pgrx::pg_sys::default_toast_compression;
                }
                toastValue.compression = match cmp as u8 as char {
                    'p' => pgrx::pg_sys::TOAST_PGLZ_COMPRESSION_ID as u8,
                    'l' => pgrx::pg_sys::TOAST_LZ4_COMPRESSION_ID as u8,
                    _ => pgrx::pg_sys::TOAST_INVALID_COMPRESSION_ID as u8,
                };
            } else {
                toastValue.compression = pgrx::pg_sys::TOAST_INVALID_COMPRESSION_ID as u8;
            }

            data_length = std::mem::size_of::<OToastValue>();
            std::ptr::copy_nonoverlapping(&toastValue as *const OToastValue as *const u8, data_ptr as *mut u8, data_length);
        }

        if hasnull {
            if bitmask != 0x80 {
                bitmask <<= 1;
            } else {
                bitP = bitP.add(1);
                *bitP = 0x0;
                bitmask = 1;
            }

            if null {
                continue;
            }

            *bitP |= bitmask;
        }

        if cur_to_toast {
            data_ptr = data_ptr.add(data_length);
            continue;
        }

        if att.attbyval {
            data_ptr = att_align_nominal_ptr(data_ptr, att.attalign);
            store_att_byval(data_ptr, value, att.attlen);
            data_length = att.attlen as usize;
        } else if att.attlen == -1 {
            let val = value.value() as *mut pgrx::pg_sys::varlena;
            let val_ptr = val as *const c_char;

            if pgrx::pg_sys::VARATT_IS_EXTERNAL(val) {
                if VARATT_IS_EXTERNAL_EXPANDED(val_ptr) {
                    let eoh = pgrx::pg_sys::DatumGetEOHP(value);
                    data_ptr = att_align_nominal_ptr(data_ptr, att.attalign);
                    data_length = pgrx::pg_sys::EOH_get_flat_size(eoh);
                    pgrx::pg_sys::EOH_flatten_into(eoh, data_ptr as *mut std::ffi::c_void, data_length);
                } else {
                    data_length = pgrx::pg_sys::VARSIZE_EXTERNAL(val) as usize;
                    std::ptr::copy_nonoverlapping(val as *const u8, data_ptr as *mut u8, data_length);
                }
            } else if pgrx::pg_sys::VARATT_IS_SHORT(val) {
                data_length = pgrx::pg_sys::VARSIZE_SHORT(val) as usize;
                std::ptr::copy_nonoverlapping(val as *const u8, data_ptr as *mut u8, data_length);
            } else if VARLENA_ATT_IS_PACKABLE(att) && pgrx::pg_sys::VARATT_CAN_MAKE_SHORT(val) {
                data_length = pgrx::pg_sys::VARATT_CONVERTED_SHORT_SIZE(val) as usize;
                *(data_ptr as *mut u8) = (data_length | 0x80) as u8;
                std::ptr::copy_nonoverlapping(
                    (val as *const u8).add(4),
                    data_ptr.add(1) as *mut u8,
                    data_length - 1
                );
            } else {
                data_ptr = att_align_nominal_ptr(data_ptr, att.attalign);
                data_length = pgrx::pg_sys::VARSIZE(val) as usize;
                std::ptr::copy_nonoverlapping(val as *const u8, data_ptr as *mut u8, data_length);
            }
        } else if att.attlen == -2 {
            data_length = std::ffi::CStr::from_ptr(value.value() as *const c_char).to_bytes().len() + 1;
            std::ptr::copy_nonoverlapping(value.value() as *const u8, data_ptr as *mut u8, data_length);
        } else {
            data_ptr = att_align_nominal_ptr(data_ptr, att.attalign);
            assert!(att.attlen > 0);
            data_length = att.attlen as usize;
            std::ptr::copy_nonoverlapping(value.value() as *const u8, data_ptr as *mut u8, data_length);
        }

        data_ptr = data_ptr.add(data_length);
    }

    assert_eq!(data_ptr.offset_from((*tuple).data), tuple_size as isize);
}

#[no_mangle]
pub unsafe extern "C" fn o_form_tuple(
    tupleDesc: TupleDesc,
    spec: *mut OTupleFixedFormatSpec,
    version: u32,
    values: *mut Datum,
    isnull: *mut bool,
    bridge_data: *mut BridgeData,
) -> OTuple {
    let mut result = OTuple {
        data: std::ptr::null_mut(),
        formatFlags: 0,
    };
    let len = o_new_tuple_size(tupleDesc, spec, std::ptr::null_mut(), bridge_data, version, values, isnull, std::ptr::null_mut());
    result.data = pgrx::pg_sys::palloc0(len) as *mut c_char;
    o_tuple_fill(tupleDesc, spec, &mut result, len, std::ptr::null_mut(), bridge_data, version, values, isnull, std::ptr::null_mut());
    result
}

#[no_mangle]
pub unsafe extern "C" fn o_tuple_get_version(tuple: OTuple) -> u32 {
    if (tuple.formatFlags & O_TUPLE_FLAGS_FIXED_FORMAT) != 0 {
        0
    } else {
        let header = tuple.data as *mut OTupleHeaderData;
        (*header).version
    }
}

#[no_mangle]
pub unsafe extern "C" fn o_tuple_set_version(
    spec: *mut OTupleFixedFormatSpec,
    tuple: *mut OTuple,
    version: u32,
) {
    let mut header = (*tuple).data as *mut OTupleHeaderData;

    if ((*tuple).formatFlags & O_TUPLE_FLAGS_FIXED_FORMAT) == 0 {
        (*header).version = version;
        if (*header).version == 0 && !(*header).hasnulls() && (*header).natts == (*spec).natts {
            assert_eq!((*header).len() as usize, (*spec).len as usize + std::mem::size_of::<OTupleHeaderData>());
            (*tuple).formatFlags |= O_TUPLE_FLAGS_FIXED_FORMAT;
            std::ptr::copy(
                (*tuple).data.add(std::mem::size_of::<OTupleHeaderData>()),
                (*tuple).data,
                (*spec).len as usize
            );
        }
        return;
    }

    if version == 0 {
        return;
    }

    (*tuple).data = pgrx::pg_sys::repalloc(
        (*tuple).data as *mut std::ffi::c_void,
        ((*spec).len as usize + std::mem::size_of::<OTupleHeaderData>()) as pgrx::pg_sys::Size
    ) as *mut c_char;

    std::ptr::copy(
        (*tuple).data,
        (*tuple).data.add(std::mem::size_of::<OTupleHeaderData>()),
        (*spec).len as usize
    );
    (*tuple).formatFlags &= !O_TUPLE_FLAGS_FIXED_FORMAT;

    header = (*tuple).data as *mut OTupleHeaderData;
    (*header).natts = (*spec).natts;
    let new_len = std::mem::size_of::<OTupleHeaderData>() + (*spec).len as usize;
    (*header).set_len(new_len as u16);
    (*header).set_hasnulls(false);
    (*header).version = version;
}

#[no_mangle]
pub unsafe extern "C" fn o_tuple_set_ctid(tuple: OTuple, iptr: pgrx::pg_sys::ItemPointer) {
    let data = tuple.data;
    let header = data as *mut OTupleHeaderData;

    if (tuple.formatFlags & O_TUPLE_FLAGS_FIXED_FORMAT) != 0 {
        *(data as pgrx::pg_sys::ItemPointer) = *iptr;
    } else if (*header).hasnulls() {
        let dest = data.add(SizeOfOTupleHeader).add(maxalign(BITMAPLEN((*header).natts as usize))) as pgrx::pg_sys::ItemPointer;
        *dest = *iptr;
    } else {
        let dest = data.add(SizeOfOTupleHeader) as pgrx::pg_sys::ItemPointer;
        *dest = *iptr;
    }
}

pub const ORIOLEDB_TO_TOAST_OFF: c_char = '\0' as c_char;
pub const ORIOLEDB_TO_TOAST_ON: c_char = 'y' as c_char;
pub const ORIOLEDB_TO_TOAST_COMPRESSION_TRIED: c_char = 'c' as c_char;

#[repr(C)]
#[derive(Copy, Clone, Debug)]
pub struct OToastValue {
    pub pointer: u8,
    pub compression: u8,
    pub raw_size: i32,
    pub toasted_size: i32,
}
