/*-------------------------------------------------------------------------
 *
 * key_range.rs
 *		Function dealing with key ranges for planning and execution stage
 *		in OrioleDB.
 *
 * Copyright (c) 2021-2026, Oriole DB Inc.
 * Copyright (c) 2025-2026, Supabase Inc.
 *
 *-------------------------------------------------------------------------
 */

use pgrx::pg_sys::{self, Datum, Oid};
use std::ptr;

#[inline]
pub unsafe fn OidIsValid(oid: Oid) -> bool {
    oid != pg_sys::InvalidOid
}

pub const O_VALUE_BOUND_INCLUSIVE: u8 = 0x01;
pub const O_VALUE_BOUND_NULL: u8 = 0x02;
pub const O_VALUE_BOUND_UNBOUNDED: u8 = 0x04;
pub const O_VALUE_BOUND_LOWER: u8 = 0x08;
pub const O_VALUE_BOUND_UPPER: u8 = 0x10;
pub const O_VALUE_BOUND_COERCIBLE: u8 = 0x20;
pub const O_VALUE_BOUND_NON_COERCIBLE: u8 = 0x40;
pub const O_VALUE_BOUND_DIRECTIONS: u8 = O_VALUE_BOUND_LOWER | O_VALUE_BOUND_UPPER;
pub const O_VALUE_BOUND_NO_VALUE: u8 = O_VALUE_BOUND_NULL | O_VALUE_BOUND_UNBOUNDED;
pub const O_VALUE_BOUND_MINUS_INFINITY: u8 = O_VALUE_BOUND_LOWER | O_VALUE_BOUND_UNBOUNDED;
pub const O_VALUE_BOUND_PLUS_INFINITY: u8 = O_VALUE_BOUND_UPPER | O_VALUE_BOUND_UNBOUNDED;
pub const O_VALUE_BOUND_PLAIN_VALUE: u8 = O_VALUE_BOUND_LOWER | O_VALUE_BOUND_INCLUSIVE | O_VALUE_BOUND_COERCIBLE;

pub const INDEX_MAX_KEYS: usize = 32;

// Opaque comparator
#[repr(C)]
pub struct OComparator {
    _unused: [u8; 0],
}

#[repr(C)]
pub struct OExclusionFn {
    pub operator: Oid,
    pub finfo: pg_sys::FmgrInfo,
}

#[repr(C)]
pub struct OHashFnKey {
    pub datoid: Oid,
    pub hash_fn_oid: Oid,
}

#[repr(C)]
pub struct OHashFn {
    pub key: OHashFnKey,
    pub finfo: pg_sys::FmgrInfo,
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
#[derive(Debug, Clone, Copy)]
pub struct OBTreeValueBound {
    pub value: Datum,
    pub type_: Oid,
    pub flags: u8,
    pub comparator: *mut OComparator,
    pub exclusion_fn: *mut OExclusionFn,
}

#[repr(C)]
#[derive(Debug, Clone, Copy)]
pub struct OBtreeRowKeyBound {
    pub nkeys: std::ffi::c_int,
    pub keynums: *mut std::ffi::c_int,
    pub keys: *mut OBTreeValueBound,
}

#[repr(C)]
pub struct OBTreeKeyBound {
    pub nkeys: std::ffi::c_int,
    pub keys: [OBTreeValueBound; INDEX_MAX_KEYS],
    pub n_row_keys: std::ffi::c_int,
    pub row_keys: *mut OBtreeRowKeyBound,
}

#[repr(C)]
pub struct OBTreeKeyRange {
    pub empty: bool,
    pub low: OBTreeKeyBound,
    pub high: OBTreeKeyBound,
}

extern "C" {
    // Declared in tree.h / descr.h
    pub fn o_find_comparator(opfamily: Oid, lefttype: Oid, righttype: Oid, collation: Oid) -> *mut OComparator;
    pub fn o_idx_cmp_value_bounds(bound1: *const OBTreeValueBound, bound2: *const OBTreeValueBound, field: *const OIndexField, equal: *mut bool) -> std::ffi::c_int;
}

#[no_mangle]
pub unsafe extern "C" fn o_bound_is_coercible(bound: *mut OBTreeValueBound, field: *mut OIndexField) -> bool {
    let bound = &mut *bound;
    let field = &*field;
    if (bound.flags & O_VALUE_BOUND_COERCIBLE) != 0 {
        return true;
    }
    if (bound.flags & O_VALUE_BOUND_NON_COERCIBLE) != 0 {
        return false;
    }
    let result = pg_sys::IsBinaryCoercible(bound.type_, field.inputtype);
    if result {
        bound.flags |= O_VALUE_BOUND_COERCIBLE;
    } else {
        bound.flags |= O_VALUE_BOUND_NON_COERCIBLE;
    }
    result
}

unsafe fn o_fill_row_key_bound(
    bound: &mut OBTreeKeyBound,
    first_subkey: bool,
    last_subkey: bool,
    subattnum: std::ffi::c_int,
    flags: u8,
) -> *mut OBTreeValueBound {
    if first_subkey {
        bound.n_row_keys += 1;
        if bound.n_row_keys == 1 {
            bound.row_keys = pg_sys::palloc0(std::mem::size_of::<OBtreeRowKeyBound>()) as *mut OBtreeRowKeyBound;
        } else {
            bound.row_keys = pg_sys::repalloc(bound.row_keys as *mut std::ffi::c_void, (bound.n_row_keys as usize) * std::mem::size_of::<OBtreeRowKeyBound>()) as *mut OBtreeRowKeyBound;
        }
    }
    let rowkey = &mut *bound.row_keys.add((bound.n_row_keys - 1) as usize);
    if first_subkey {
        rowkey.nkeys = 0;
    }
    rowkey.nkeys += 1;
    if rowkey.nkeys == 1 {
        rowkey.keys = pg_sys::palloc0(std::mem::size_of::<OBTreeValueBound>()) as *mut OBTreeValueBound;
        rowkey.keynums = pg_sys::palloc0(std::mem::size_of::<std::ffi::c_int>()) as *mut std::ffi::c_int;
    } else {
        rowkey.keys = pg_sys::repalloc(rowkey.keys as *mut std::ffi::c_void, (rowkey.nkeys as usize) * std::mem::size_of::<OBTreeValueBound>()) as *mut OBTreeValueBound;
        rowkey.keynums = pg_sys::repalloc(rowkey.keynums as *mut std::ffi::c_void, (rowkey.nkeys as usize) * std::mem::size_of::<std::ffi::c_int>()) as *mut std::ffi::c_int;
    }
    
    let result = rowkey.keys.add((rowkey.nkeys - 1) as usize);
    *rowkey.keynums.add((rowkey.nkeys - 1) as usize) = subattnum;
    (*result).flags = flags;
    if !last_subkey {
        (*result).flags |= O_VALUE_BOUND_INCLUSIVE;
    }
    result
}

#[no_mangle]
pub unsafe extern "C" fn o_key_data_update_array_key_range(
    res: *mut OBTreeKeyRange,
    keyData: *mut pg_sys::ScanKeyData,
    numberOfKeys: std::ffi::c_int,
    arrayKeys: *mut pg_sys::BTArrayKeyInfo,
    numPrefixExactKeys: std::ffi::c_int,
    _resultNKeys: std::ffi::c_int,
    _fields: *mut OIndexField,
) {
    let res = &mut *res;
    let mut current_array_key = arrayKeys;
    for i in 0..numberOfKeys {
        let key = &*keyData.offset(i as isize);
        let attnum = (key.sk_attno - 1) as usize;
        
        if (key.sk_flags & pg_sys::SK_SEARCHARRAY as std::ffi::c_int) != 0
            && key.sk_strategy == pg_sys::BTEqualStrategyNumber as u16
        {
            if !current_array_key.is_null() {
                let array_key = &*current_array_key;
                if array_key.num_elems > 0 {
                    if i < numPrefixExactKeys {
                        let cur_val = *array_key.elem_values.offset(array_key.cur_elem as isize);
                        res.low.keys[attnum].value = cur_val;
                        res.high.keys[attnum].value = cur_val;
                    }
                    current_array_key = current_array_key.offset(1);
                }
            }
        }
    }
}

unsafe fn o_fill_key_bounds(
    v: Datum,
    type_: Oid,
    low: *mut OBTreeValueBound,
    high: *mut OBTreeValueBound,
    field: &OIndexField,
) {
    if low.is_null() && high.is_null() {
        return;
    }
    
    let coercible = if type_ == field.opclass || type_ == field.inputtype || pg_sys::IsBinaryCoercible(type_, field.inputtype) {
        true
    } else {
        false
    };
    
    let comparator = if coercible {
        ptr::null_mut()
    } else {
        o_find_comparator(field.opfamily, type_, field.inputtype, field.collation)
    };
    
    let flag = if coercible { O_VALUE_BOUND_COERCIBLE } else { O_VALUE_BOUND_NON_COERCIBLE };
    
    if !low.is_null() {
        let low = &mut *low;
        low.value = v;
        low.type_ = type_;
        low.comparator = comparator;
        low.exclusion_fn = ptr::null_mut();
        low.flags |= flag;
    }
    
    if !high.is_null() {
        let high = &mut *high;
        high.value = v;
        high.type_ = type_;
        high.comparator = comparator;
        high.exclusion_fn = ptr::null_mut();
        high.flags |= flag;
    }
}

fn o_key_range_is_unbounded(range: &OBTreeKeyRange, attnum: usize) -> bool {
    range.low.keys[attnum].flags == O_VALUE_BOUND_MINUS_INFINITY
        && range.high.keys[attnum].flags == O_VALUE_BOUND_PLUS_INFINITY
}

#[no_mangle]
pub unsafe extern "C" fn o_key_data_to_key_range(
    res: *mut OBTreeKeyRange,
    keyData: *mut pg_sys::ScanKeyData,
    numberOfKeys: std::ffi::c_int,
    arrayKeys: *mut pg_sys::BTArrayKeyInfo,
    numPrefixExactKeys: std::ffi::c_int,
    resultNKeys: std::ffi::c_int,
    fields: *mut OIndexField,
) -> bool {
    let res = &mut *res;
    let fields_slice = std::slice::from_raw_parts(fields, resultNKeys as usize);
    let mut exact = true;
    
    res.empty = false;
    res.low.nkeys = resultNKeys;
    res.high.nkeys = resultNKeys;
    
    for i in 0..(resultNKeys as usize) {
        res.low.keys[i].flags = O_VALUE_BOUND_MINUS_INFINITY;
        res.high.keys[i].flags = O_VALUE_BOUND_PLUS_INFINITY;
    }
    
    let mut current_array_key = arrayKeys;
    for i in 0..numberOfKeys {
        let mut set_low = false;
        let mut set_high = false;
        let key = &*keyData.offset(i as isize);
        let attnum = (key.sk_attno - 1) as usize;
        if attnum >= resultNKeys as usize {
            continue;
        }
        
        let mut low = OBTreeValueBound {
            value: Datum::from(0usize),
            type_: pg_sys::InvalidOid,
            flags: O_VALUE_BOUND_MINUS_INFINITY,
            comparator: ptr::null_mut(),
            exclusion_fn: ptr::null_mut(),
        };
        let mut high = OBTreeValueBound {
            value: Datum::from(0usize),
            type_: pg_sys::InvalidOid,
            flags: O_VALUE_BOUND_PLUS_INFINITY,
            comparator: ptr::null_mut(),
            exclusion_fn: ptr::null_mut(),
        };
        let field = &fields_slice[attnum];
        
        let strategy = key.sk_strategy as std::ffi::c_uint;
        if strategy == pg_sys::BTLessStrategyNumber {
            if (key.sk_flags & pg_sys::SK_SEARCHNOTNULL as std::ffi::c_int) != 0 {
                if !field.nullfirst {
                    high.flags = O_VALUE_BOUND_UPPER | O_VALUE_BOUND_NULL;
                }
            } else {
                set_high = true;
                high.flags = O_VALUE_BOUND_UPPER;
                if field.nullfirst {
                    set_low = true;
                    low.flags = O_VALUE_BOUND_LOWER | O_VALUE_BOUND_NULL;
                }
            }
        } else if strategy == pg_sys::BTLessEqualStrategyNumber {
            if (key.sk_flags & pg_sys::SK_SEARCHNOTNULL as std::ffi::c_int) != 0 {
                if !field.nullfirst {
                    high.flags = O_VALUE_BOUND_UPPER | O_VALUE_BOUND_NULL;
                }
            } else {
                set_high = true;
                high.flags = O_VALUE_BOUND_UPPER | O_VALUE_BOUND_INCLUSIVE;
                if field.nullfirst {
                    set_low = true;
                    low.flags = O_VALUE_BOUND_LOWER | O_VALUE_BOUND_NULL;
                }
            }
        } else if strategy == pg_sys::BTEqualStrategyNumber {
            if (key.sk_flags & pg_sys::SK_SEARCHNULL as std::ffi::c_int) != 0 {
                low.flags = O_VALUE_BOUND_LOWER | O_VALUE_BOUND_INCLUSIVE | O_VALUE_BOUND_NULL;
                high.flags = O_VALUE_BOUND_UPPER | O_VALUE_BOUND_INCLUSIVE | O_VALUE_BOUND_NULL;
            } else if !field.exclusion_fn.is_null() {
                low.exclusion_fn = field.exclusion_fn;
                low.value = key.sk_argument;
                low.type_ = field.inputtype;
            } else {
                low.flags = O_VALUE_BOUND_LOWER | O_VALUE_BOUND_INCLUSIVE;
                high.flags = O_VALUE_BOUND_UPPER | O_VALUE_BOUND_INCLUSIVE;
                set_low = true;
                set_high = true;
            }
        } else if strategy == pg_sys::BTGreaterStrategyNumber || strategy == pg_sys::BTGreaterEqualStrategyNumber {
            if (key.sk_flags & pg_sys::SK_SEARCHNOTNULL as std::ffi::c_int) != 0 {
                if field.nullfirst {
                    low.flags = O_VALUE_BOUND_LOWER | O_VALUE_BOUND_NULL;
                }
            } else {
                set_low = true;
                low.flags = O_VALUE_BOUND_LOWER;
                if strategy == pg_sys::BTGreaterEqualStrategyNumber {
                    low.flags |= O_VALUE_BOUND_INCLUSIVE;
                }
                if !field.nullfirst {
                    set_high = true;
                    high.flags = O_VALUE_BOUND_UPPER | O_VALUE_BOUND_NULL;
                }
            }
        }
        
        if (key.sk_flags & pg_sys::SK_SEARCHARRAY as std::ffi::c_int) != 0
            && key.sk_strategy == pg_sys::BTEqualStrategyNumber as u16
        {
            if !current_array_key.is_null() {
                let array_key = &*current_array_key;
                if array_key.num_elems > 0 || array_key.num_elems == -1 {
                    // Skip scan handling (PG18+)
                    #[cfg(feature = "pg18")]
                    {
                        if (key.sk_flags & pg_sys::SK_BT_SKIP as std::ffi::c_int) != 0 {
                            let have_minval = (key.sk_flags & pg_sys::SK_BT_MINVAL as std::ffi::c_int) != 0;
                            let have_maxval = (key.sk_flags & pg_sys::SK_BT_MAXVAL as std::ffi::c_int) != 0;
                            let have_next = (key.sk_flags & pg_sys::SK_BT_NEXT as std::ffi::c_int) != 0;
                            let have_prior = (key.sk_flags & pg_sys::SK_BT_PRIOR as std::ffi::c_int) != 0;
                            let have_isnull = (key.sk_flags & pg_sys::SK_ISNULL as std::ffi::c_int) != 0;
                            let sentinel = have_minval || have_maxval || have_isnull;
                            
                            if !sentinel && !have_next && !have_prior {
                                low.flags = O_VALUE_BOUND_LOWER | O_VALUE_BOUND_INCLUSIVE;
                                high.flags = O_VALUE_BOUND_UPPER | O_VALUE_BOUND_INCLUSIVE;
                                let sub_type = if OidIsValid(key.sk_subtype) { key.sk_subtype } else { field.inputtype };
                                o_fill_key_bounds(key.sk_argument, sub_type, &mut low, &mut high, field);
                                res.low.keys[attnum] = low;
                                res.high.keys[attnum] = high;
                                current_array_key = current_array_key.offset(1);
                                continue;
                            }
                            
                            if have_next {
                                low.flags = O_VALUE_BOUND_LOWER;
                                let sub_type = if OidIsValid(key.sk_subtype) { key.sk_subtype } else { field.inputtype };
                                o_fill_key_bounds(key.sk_argument, sub_type, &mut low, ptr::null_mut(), field);
                                res.low.keys[attnum] = low;
                            } else if !array_key.low_compare.is_null() {
                                let lk = &*array_key.low_compare;
                                low.flags = O_VALUE_BOUND_LOWER;
                                if lk.sk_strategy == pg_sys::BTGreaterEqualStrategyNumber as std::ffi::c_int {
                                    low.flags |= O_VALUE_BOUND_INCLUSIVE;
                                }
                                let sub_type = if OidIsValid(lk.sk_subtype) { lk.sk_subtype } else { field.inputtype };
                                o_fill_key_bounds(lk.sk_argument, sub_type, &mut low, ptr::null_mut(), field);
                                res.low.keys[attnum] = low;
                            } else if !array_key.null_elem && field.nullfirst {
                                res.low.keys[attnum].flags = O_VALUE_BOUND_LOWER | O_VALUE_BOUND_NULL;
                            }
                            
                            if have_prior {
                                high.flags = O_VALUE_BOUND_UPPER;
                                let sub_type = if OidIsValid(key.sk_subtype) { key.sk_subtype } else { field.inputtype };
                                o_fill_key_bounds(key.sk_argument, sub_type, ptr::null_mut(), &mut high, field);
                                res.high.keys[attnum] = high;
                            } else if !array_key.high_compare.is_null() {
                                let hk = &*array_key.high_compare;
                                high.flags = O_VALUE_BOUND_UPPER;
                                if hk.sk_strategy == pg_sys::BTLessEqualStrategyNumber as std::ffi::c_int {
                                    high.flags |= O_VALUE_BOUND_INCLUSIVE;
                                }
                                let sub_type = if OidIsValid(hk.sk_subtype) { hk.sk_subtype } else { field.inputtype };
                                o_fill_key_bounds(hk.sk_argument, sub_type, ptr::null_mut(), &mut high, field);
                                res.high.keys[attnum] = high;
                            } else if !array_key.null_elem && !field.nullfirst {
                                res.high.keys[attnum].flags = O_VALUE_BOUND_UPPER | O_VALUE_BOUND_NULL;
                            }
                            current_array_key = current_array_key.offset(1);
                            continue;
                        }
                    }
                    
                    if o_key_range_is_unbounded(res, attnum) {
                        if i < numPrefixExactKeys {
                            let val = *array_key.elem_values.offset(array_key.cur_elem as isize);
                            o_fill_key_bounds(val, key.sk_subtype, if set_low { &mut low } else { ptr::null_mut() }, if set_high { &mut high } else { ptr::null_mut() }, field);
                        } else {
                            let first_val = *array_key.elem_values.offset(0);
                            let last_val = *array_key.elem_values.offset((array_key.num_elems - 1) as isize);
                            o_fill_key_bounds(first_val, key.sk_subtype, if set_low { &mut low } else { ptr::null_mut() }, ptr::null_mut(), field);
                            o_fill_key_bounds(last_val, key.sk_subtype, ptr::null_mut(), if set_high { &mut high } else { ptr::null_mut() }, field);
                        }
                        if set_low {
                            res.low.keys[attnum] = low;
                        }
                        if set_high {
                            res.high.keys[attnum] = high;
                        }
                    }
                    current_array_key = current_array_key.offset(1);
                }
            }
        } else if (key.sk_flags & pg_sys::SK_ROW_HEADER as std::ffi::c_int) != 0 {
            let mut subkey = key.sk_argument.value() as *mut pg_sys::ScanKeyData;
            let mut first_subkey = true;
            let mut last_subkey = false;
            
            while !last_subkey {
                last_subkey = ((*subkey).sk_flags & pg_sys::SK_ROW_END as std::ffi::c_int) != 0;
                let subattnum = ((*subkey).sk_attno - 1) as usize;
                let subfield = &fields_slice[subattnum];
                
                let sublow = if set_low {
                    o_fill_row_key_bound(&mut res.low, first_subkey, last_subkey, subattnum as std::ffi::c_int, low.flags)
                } else {
                    ptr::null_mut()
                };
                
                let subhigh = if set_high {
                    o_fill_row_key_bound(&mut res.high, first_subkey, last_subkey, subattnum as std::ffi::c_int, high.flags)
                } else {
                    ptr::null_mut()
                };
                
                o_fill_key_bounds((*subkey).sk_argument, (*subkey).sk_subtype, sublow, subhigh, subfield);
                first_subkey = false;
                if !last_subkey {
                    subkey = subkey.offset(1);
                }
            }
        } else {
            let mut type_ = key.sk_subtype;
            if !OidIsValid(type_) {
                type_ = field.inputtype;
            }
            
            o_fill_key_bounds(key.sk_argument, type_, if set_low { &mut low } else { ptr::null_mut() }, if set_high { &mut high } else { ptr::null_mut() }, field);
            if o_idx_cmp_value_bounds(&low, &res.low.keys[attnum], field, ptr::null_mut()) >= 0 {
                res.low.keys[attnum] = low;
            }
            if o_idx_cmp_value_bounds(&high, &res.high.keys[attnum], field, ptr::null_mut()) <= 0 {
                res.high.keys[attnum] = high;
            }
        }
    }
    
    for i in 0..(resultNKeys as usize) {
        let mut equals = false;
        if o_idx_cmp_value_bounds(&res.low.keys[i], &res.high.keys[i], &fields_slice[i], &mut equals) >= 0 {
            res.empty = true;
            return false;
        }
        if !equals {
            exact = false;
        }
    }
    exact
}
