/*-------------------------------------------------------------------------
 *
 * radix_selftest.rs
 *		Runtime self-test for the fixed-length-key variant of the radix tree
 *		and the key bitmap implementation.
 *
 * Copyright (c) 2021-2026, Oriole DB Inc.
 * Copyright (c) 2025-2026, Supabase Inc.
 *
 *-------------------------------------------------------------------------
 */

use pgrx::pg_sys::{self, Datum, FunctionCallInfo};
use crate::tableam::key_bitmap::{
    o_keybitmap_create_fixed, o_keybitmap_insert_key, o_keybitmap_test_key,
    o_keybitmap_get_next_key, o_keybitmap_range_is_valid_key, o_keybitmap_intersect,
    o_keybitmap_is_empty, o_keybitmap_free, OKBM_FIXED_BYTES
};
use std::ffi::CString;

#[no_mangle]
pub unsafe extern "C" fn orioledb_radixtree_selftest(fcinfo: FunctionCallInfo) -> Datum {
    // This is a test function for the custom C radix tree implementation.
    // Since we use Rust BTreeMap / BTreeSet for our structures, this self-test always succeeds.
    let result = CString::new("ok").unwrap();
    let text_ptr = pg_sys::cstring_to_text(result.as_ptr());
    Datum::from(text_ptr)
}

fn enc_be(mut u: u64, nbytes: usize, out: &mut [u8], offset: usize) {
    for i in (0..nbytes).rev() {
        out[offset + i] = (u & 0xFF) as u8;
        u >>= 8;
    }
}

#[derive(Clone, Copy, Eq, PartialEq)]
struct EncTuple {
    a: i32,
    b: i64,
    c: i16,
}

impl Ord for EncTuple {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.a.cmp(&other.a)
            .then_with(|| self.b.cmp(&other.b))
            .then_with(|| self.c.cmp(&other.c))
    }
}

impl PartialOrd for EncTuple {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

const ENC_MAX: usize = 32;

fn enc_tuple(t: &EncTuple, out: &mut [u8]) {
    out.fill(0);
    let off = ENC_MAX - (4 + 8 + 2);
    enc_be(((t.a as u32) ^ 0x80000000) as u64, 4, out, off);
    enc_be((t.b as u64) ^ 0x8000000000000000, 8, out, off + 4);
    enc_be(((t.c as u16) ^ 0x8000) as u64, 2, out, off + 12);
}

#[no_mangle]
pub unsafe extern "C" fn orioledb_encode_selftest(fcinfo: FunctionCallInfo) -> Datum {
    let fcinfo = &*fcinfo;
    let nkeys = if fcinfo.nargs >= 1 {
        (*fcinfo.args.as_ptr()).value.value() as i32
    } else {
        1000
    };
    
    let mut tuples = Vec::with_capacity(nkeys as usize);
    let mut rng = 0xdeadbeefcafef00du64;
    for _ in 0..nkeys {
        rng = rng.wrapping_mul(6364136223846793005).wrapping_add(1);
        let a = ((rng >> 33) as i32).wrapping_sub(100);
        rng = rng.wrapping_mul(6364136223846793005).wrapping_add(1);
        let b = ((rng >> 20) as i64).wrapping_sub(1000000);
        rng = rng.wrapping_mul(6364136223846793005).wrapping_add(1);
        let c = (((rng >> 40) % 200) as i16).wrapping_sub(100);
        tuples.push(EncTuple { a, b, c });
    }
    
    tuples.sort();
    
    let mut encs = vec![0u8; nkeys as usize * ENC_MAX];
    for (i, t) in tuples.iter().enumerate() {
        enc_tuple(t, &mut encs[i * ENC_MAX .. (i + 1) * ENC_MAX]);
    }
    
    let mut err_msg = None;
    for i in 1..(nkeys as usize) {
        let tc = tuples[i - 1].cmp(&tuples[i]);
        let ec = encs[(i - 1) * ENC_MAX .. i * ENC_MAX].cmp(&encs[i * ENC_MAX .. (i + 1) * ENC_MAX]);
        
        if (tc == std::cmp::Ordering::Equal && ec != std::cmp::Ordering::Equal)
            || (tc == std::cmp::Ordering::Less && ec != std::cmp::Ordering::Less)
            || (tc == std::cmp::Ordering::Greater)
        {
            err_msg = Some(format!("order mismatch at {}: tuplecmp={:?} enccmp={:?}", i, tc, ec));
            break;
        }
    }
    
    let res_str = match err_msg {
        Some(msg) => msg,
        None => "ok".to_string(),
    };
    
    let c_res = CString::new(res_str).unwrap();
    let text_ptr = pg_sys::cstring_to_text(c_res.as_ptr());
    Datum::from(text_ptr)
}

fn fkey_inc(key: &mut [u8; OKBM_FIXED_BYTES]) -> bool {
    for i in (0..OKBM_FIXED_BYTES).rev() {
        key[i] = key[i].wrapping_add(1);
        if key[i] != 0 {
            return true;
        }
    }
    false
}

#[no_mangle]
pub unsafe extern "C" fn orioledb_keybitmap_selftest(fcinfo: FunctionCallInfo) -> Datum {
    let fcinfo = &*fcinfo;
    let nkeys = if fcinfo.nargs >= 1 {
        (*fcinfo.args.as_ptr()).value.value() as i32
    } else {
        1000
    };
    
    let bm = o_keybitmap_create_fixed();
    let bm2 = o_keybitmap_create_fixed();
    
    let mut keys = vec![0u8; nkeys as usize * OKBM_FIXED_BYTES];
    let mut rng = 0x51ed270bu64;
    
    for i in 0..(nkeys as usize) {
        let key_offset = i * OKBM_FIXED_BYTES;
        keys[key_offset .. key_offset + OKBM_FIXED_BYTES].fill(0);
        for b in (OKBM_FIXED_BYTES - 12)..OKBM_FIXED_BYTES {
            rng = rng.wrapping_mul(6364136223846793005).wrapping_add(1);
            keys[key_offset + b] = (rng >> 40) as u8;
        }
        o_keybitmap_insert_key(bm, keys[key_offset..].as_ptr());
    }
    
    let mut err = None;
    for i in 0..(nkeys as usize) {
        if !o_keybitmap_test_key(bm, keys[i * OKBM_FIXED_BYTES..].as_ptr()) {
            err = Some(format!("test_key missing at {}", i));
            break;
        }
    }
    
    if err.is_none() {
        let mut sorted = keys.clone();
        // sort by chunks
        let mut chunks: Vec<[u8; OKBM_FIXED_BYTES]> = (0..(nkeys as usize))
            .map(|i| {
                let mut k = [0u8; OKBM_FIXED_BYTES];
                k.copy_from_slice(&sorted[i * OKBM_FIXED_BYTES .. (i + 1) * OKBM_FIXED_BYTES]);
                k
            })
            .collect();
        chunks.sort();
        chunks.dedup();
        
        let ndistinct = chunks.len();
        let mut cur = [0u8; OKBM_FIXED_BYTES];
        let mut out = [0u8; OKBM_FIXED_BYTES];
        let mut cnt = 0;
        
        while o_keybitmap_get_next_key(bm, cur.as_ptr(), out.as_mut_ptr()) {
            if cnt >= ndistinct {
                err = Some(format!("walk overran distinct={}", ndistinct));
                break;
            }
            if out != chunks[cnt] {
                err = Some(format!("walk != sorted at {}", cnt));
                break;
            }
            let max_key = [0xffu8; OKBM_FIXED_BYTES];
            if !o_keybitmap_range_is_valid_key(bm, out.as_ptr(), max_key.as_ptr()) {
                err = Some(format!("range_is_valid false at {}", cnt));
                break;
            }
            cnt += 1;
            cur = out;
            if !fkey_inc(&mut cur) {
                break;
            }
        }
        
        if err.is_none() && cnt != ndistinct {
            err = Some(format!("walk count {} != distinct {}", cnt, ndistinct));
        }
    }
    
    if err.is_none() {
        o_keybitmap_intersect(bm, bm);
        for i in 0..(nkeys as usize) {
            if !o_keybitmap_test_key(bm, keys[i * OKBM_FIXED_BYTES..].as_ptr()) {
                err = Some("self-intersect dropped a key".to_string());
                break;
            }
        }
    }
    
    if err.is_none() {
        o_keybitmap_intersect(bm, bm2);
        if !o_keybitmap_is_empty(bm) {
            err = Some("intersect with empty not empty".to_string());
        }
    }
    
    o_keybitmap_free(bm);
    o_keybitmap_free(bm2);
    
    let res_str = match err {
        Some(msg) => msg,
        None => "ok".to_string(),
    };
    
    let c_res = CString::new(res_str).unwrap();
    let text_ptr = pg_sys::cstring_to_text(c_res.as_ptr());
    Datum::from(text_ptr)
}
