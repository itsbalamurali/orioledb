//! key_bitmap.rs
//!
//! Copyright (c) 2021-2026, Oriole DB Inc.
//! Copyright (c) 2025-2026, Supabase Inc.
//!
//! IDENTIFICATION
//!   contrib/orioledb/orioledb-rs/src/tableam/key_bitmap.rs

use std::collections::{BTreeMap, BTreeSet};

pub const OKBM_CHUNK_BITS: u32 = 10;
pub const OKBM_CHUNK_VALUES: u64 = 1 << OKBM_CHUNK_BITS;
pub const OKBM_LOW_MASK: u64 = OKBM_CHUNK_VALUES - 1;
pub const OKBM_BITMAP_BYTES: usize = (OKBM_CHUNK_VALUES / 8) as usize; // 128 bytes
pub const OKBM_FIXED_BYTES: usize = 24;

pub struct OKeyBitmap {
    pub fixed: bool,
    // Non-fixed mode: chunk id -> 128-byte bitmap
    pub tree: BTreeMap<u64, [u8; OKBM_BITMAP_BYTES]>,
    // Fixed mode: set of 24-byte keys
    pub ftree: BTreeSet<[u8; OKBM_FIXED_BYTES]>,
    // Seek arrays for finalized queries
    pub chunks: Vec<u64>,
    pub fkeys: Vec<[u8; OKBM_FIXED_BYTES]>,
    pub finalized: bool,
}

#[no_mangle]
pub unsafe extern "C" fn o_keybitmap_create() -> *mut OKeyBitmap {
    let bm = Box::new(OKeyBitmap {
        fixed: false,
        tree: BTreeMap::new(),
        ftree: BTreeSet::new(),
        chunks: Vec::new(),
        fkeys: Vec::new(),
        finalized: false,
    });
    Box::into_raw(bm)
}

#[no_mangle]
pub unsafe extern "C" fn o_keybitmap_create_fixed() -> *mut OKeyBitmap {
    let bm = Box::new(OKeyBitmap {
        fixed: true,
        tree: BTreeMap::new(),
        ftree: BTreeSet::new(),
        chunks: Vec::new(),
        fkeys: Vec::new(),
        finalized: false,
    });
    Box::into_raw(bm)
}

#[no_mangle]
pub unsafe extern "C" fn o_keybitmap_free(bm: *mut OKeyBitmap) {
    if !bm.is_null() {
        let _ = Box::from_raw(bm);
    }
}

#[no_mangle]
pub unsafe extern "C" fn o_keybitmap_insert_key(bm: *mut OKeyBitmap, key: *const u8) {
    if bm.is_null() || key.is_null() {
        return;
    }
    let bm = &mut *bm;
    let mut k = [0u8; OKBM_FIXED_BYTES];
    std::ptr::copy_nonoverlapping(key, k.as_mut_ptr(), OKBM_FIXED_BYTES);
    bm.ftree.insert(k);
    bm.finalized = false;
}

#[no_mangle]
pub unsafe extern "C" fn o_keybitmap_test_key(bm: *mut OKeyBitmap, key: *const u8) -> bool {
    if bm.is_null() || key.is_null() {
        return false;
    }
    let bm = &*bm;
    let mut k = [0u8; OKBM_FIXED_BYTES];
    std::ptr::copy_nonoverlapping(key, k.as_mut_ptr(), OKBM_FIXED_BYTES);
    bm.ftree.contains(&k)
}

#[no_mangle]
pub unsafe extern "C" fn o_keybitmap_emit_key(bm: *mut OKeyBitmap, key: *const u8) -> bool {
    if bm.is_null() || key.is_null() {
        return false;
    }
    let bm = &mut *bm;
    let mut k = [0u8; OKBM_FIXED_BYTES];
    std::ptr::copy_nonoverlapping(key, k.as_mut_ptr(), OKBM_FIXED_BYTES);
    if bm.ftree.contains(&k) {
        false
    } else {
        bm.ftree.insert(k);
        bm.finalized = false;
        true
    }
}

#[no_mangle]
pub unsafe extern "C" fn o_keybitmap_insert(bm: *mut OKeyBitmap, value: u64) {
    if bm.is_null() {
        return;
    }
    let bm = &mut *bm;
    let chunk = value >> OKBM_CHUNK_BITS;
    let offset = (value & OKBM_LOW_MASK) as usize;
    let byte_idx = offset >> 3;
    let bit_idx = offset & 7;
    
    let entry = bm.tree.entry(chunk).or_insert([0u8; OKBM_BITMAP_BYTES]);
    entry[byte_idx] |= 1 << bit_idx;
    bm.finalized = false;
}

#[no_mangle]
pub unsafe extern "C" fn o_keybitmap_test(bm: *mut OKeyBitmap, value: u64) -> bool {
    if bm.is_null() {
        return false;
    }
    let bm = &*bm;
    let chunk = value >> OKBM_CHUNK_BITS;
    let offset = (value & OKBM_LOW_MASK) as usize;
    let byte_idx = offset >> 3;
    let bit_idx = offset & 7;
    
    if let Some(entry) = bm.tree.get(&chunk) {
        (entry[byte_idx] & (1 << bit_idx)) != 0
    } else {
        false
    }
}

#[no_mangle]
pub unsafe extern "C" fn o_keybitmap_emit(bm: *mut OKeyBitmap, value: u64) -> bool {
    if bm.is_null() {
        return false;
    }
    let bm = &mut *bm;
    let chunk = value >> OKBM_CHUNK_BITS;
    let offset = (value & OKBM_LOW_MASK) as usize;
    let byte_idx = offset >> 3;
    let bit_idx = offset & 7;
    
    let entry = bm.tree.entry(chunk).or_insert([0u8; OKBM_BITMAP_BYTES]);
    if (entry[byte_idx] & (1 << bit_idx)) != 0 {
        false
    } else {
        entry[byte_idx] |= 1 << bit_idx;
        bm.finalized = false;
        true
    }
}

#[no_mangle]
pub unsafe extern "C" fn o_keybitmap_is_empty(bm: *mut OKeyBitmap) -> bool {
    if bm.is_null() {
        return true;
    }
    let bm = &*bm;
    if bm.fixed {
        bm.ftree.is_empty()
    } else {
        bm.tree.is_empty()
    }
}

#[no_mangle]
pub unsafe extern "C" fn o_keybitmap_union(a: *mut OKeyBitmap, b: *mut OKeyBitmap) {
    if a.is_null() || b.is_null() {
        return;
    }
    let a = &mut *a;
    let b = &*b;
    assert_eq!(a.fixed, b.fixed);
    if a.fixed {
        for &k in &b.ftree {
            a.ftree.insert(k);
        }
    } else {
        for (&chunk, b_entry) in &b.tree {
            let a_entry = a.tree.entry(chunk).or_insert([0u8; OKBM_BITMAP_BYTES]);
            for i in 0..OKBM_BITMAP_BYTES {
                a_entry[i] |= b_entry[i];
            }
        }
    }
    a.finalized = false;
}

#[no_mangle]
pub unsafe extern "C" fn o_keybitmap_intersect(a: *mut OKeyBitmap, b: *mut OKeyBitmap) {
    if a.is_null() || b.is_null() {
        return;
    }
    let a = &mut *a;
    let b = &*b;
    assert_eq!(a.fixed, b.fixed);
    if a.fixed {
        a.ftree.retain(|k| b.ftree.contains(k));
    } else {
        let mut to_remove = Vec::new();
        for (chunk, a_entry) in &mut a.tree {
            if let Some(b_entry) = b.tree.get(chunk) {
                let mut empty = true;
                for i in 0..OKBM_BITMAP_BYTES {
                    a_entry[i] &= b_entry[i];
                    if a_entry[i] != 0 {
                        empty = false;
                    }
                }
                if empty {
                    to_remove.push(*chunk);
                }
            } else {
                to_remove.push(*chunk);
            }
        }
        for chunk in to_remove {
            a.tree.remove(&chunk);
        }
    }
    a.finalized = false;
}

unsafe fn okbm_finalize(bm: &mut OKeyBitmap) {
    if bm.finalized {
        return;
    }
    if bm.fixed {
        bm.fkeys = bm.ftree.iter().cloned().collect();
    } else {
        bm.chunks = bm.tree.keys().cloned().collect();
    }
    bm.finalized = true;
}

#[no_mangle]
pub unsafe extern "C" fn o_keybitmap_range_is_valid(bm: *mut OKeyBitmap, low: u64, high: u64) -> bool {
    if bm.is_null() || high <= low {
        return false;
    }
    let bm = &mut *bm;
    okbm_finalize(bm);
    
    let chunk_low = low >> OKBM_CHUNK_BITS;
    let chunk_high = (high - 1) >> OKBM_CHUNK_BITS;
    
    let idx = match bm.chunks.binary_search(&chunk_low) {
        Ok(i) => i,
        Err(i) => i,
    };
    
    for &chunk in &bm.chunks[idx..] {
        if chunk > chunk_high {
            break;
        }
        if let Some(entry) = bm.tree.get(&chunk) {
            let i_start = if chunk == chunk_low {
                ((low & OKBM_LOW_MASK) >> 3) as usize
            } else {
                0
            };
            let start_mask = if chunk == chunk_low {
                0xFF << (low & 7)
            } else {
                0xFF
            };
            
            let i_end = if chunk == chunk_high {
                (((high - 1) & OKBM_LOW_MASK) >> 3) as usize
            } else {
                OKBM_BITMAP_BYTES - 1
            };
            let end_mask = if chunk == chunk_high {
                0xFF >> (7 - ((high - 1) & 7))
            } else {
                0xFF
            };
            
            for i in i_start..=i_end {
                let mut mask = if i == i_start { start_mask } else { 0xFF };
                if i == i_end {
                    mask &= end_mask;
                }
                if (entry[i] & mask) != 0 {
                    return true;
                }
            }
        }
    }
    false
}

#[no_mangle]
pub unsafe extern "C" fn o_keybitmap_get_next(bm: *mut OKeyBitmap, prev: u64, found: *mut bool) -> u64 {
    if bm.is_null() {
        if !found.is_null() {
            *found = false;
        }
        return 0;
    }
    let bm = &mut *bm;
    okbm_finalize(bm);
    
    let chunk_prev = prev >> OKBM_CHUNK_BITS;
    let off_prev = (prev & OKBM_LOW_MASK) as usize;
    
    let idx = match bm.chunks.binary_search(&chunk_prev) {
        Ok(i) => i,
        Err(i) => i,
    };
    
    for &chunk in &bm.chunks[idx..] {
        if let Some(entry) = bm.tree.get(&chunk) {
            let start_off = if chunk == chunk_prev { off_prev } else { 0 };
            let mut i = start_off >> 3;
            let mut mask = 0xFF << (start_off & 7);
            while i < OKBM_BITMAP_BYTES {
                let val = entry[i] & mask;
                if val != 0 {
                    let mut result = i << 3;
                    let mut temp_mask = val;
                    while (temp_mask & 1) == 0 {
                        result += 1;
                        temp_mask >>= 1;
                    }
                    if !found.is_null() {
                        *found = true;
                    }
                    return (chunk << OKBM_CHUNK_BITS) + result as u64;
                }
                mask = 0xFF;
                i += 1;
            }
        }
    }
    
    if !found.is_null() {
        *found = false;
    }
    0
}

#[no_mangle]
pub unsafe extern "C" fn o_keybitmap_range_is_valid_key(bm: *mut OKeyBitmap, low: *const u8, high: *const u8) -> bool {
    if bm.is_null() || low.is_null() || high.is_null() {
        return false;
    }
    let bm = &mut *bm;
    assert!(bm.fixed);
    
    let mut l = [0u8; OKBM_FIXED_BYTES];
    let mut h = [0u8; OKBM_FIXED_BYTES];
    std::ptr::copy_nonoverlapping(low, l.as_mut_ptr(), OKBM_FIXED_BYTES);
    std::ptr::copy_nonoverlapping(high, h.as_mut_ptr(), OKBM_FIXED_BYTES);
    
    if l >= h {
        return false;
    }
    
    okbm_finalize(bm);
    
    let idx = match bm.fkeys.binary_search(&l) {
        Ok(i) => i,
        Err(i) => i,
    };
    
    if idx >= bm.fkeys.len() {
        return false;
    }
    
    bm.fkeys[idx] < h
}

#[no_mangle]
pub unsafe extern "C" fn o_keybitmap_get_next_key(bm: *mut OKeyBitmap, prev: *const u8, result: *mut u8) -> bool {
    if bm.is_null() || prev.is_null() || result.is_null() {
        return false;
    }
    let bm = &mut *bm;
    assert!(bm.fixed);
    
    let mut p = [0u8; OKBM_FIXED_BYTES];
    std::ptr::copy_nonoverlapping(prev, p.as_mut_ptr(), OKBM_FIXED_BYTES);
    
    okbm_finalize(bm);
    
    let idx = match bm.fkeys.binary_search(&p) {
        Ok(i) => i,
        Err(i) => i,
    };
    
    if idx >= bm.fkeys.len() {
        return false;
    }
    
    std::ptr::copy_nonoverlapping(bm.fkeys[idx].as_ptr(), result, OKBM_FIXED_BYTES);
    true
}
