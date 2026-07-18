// Usage count map (UCM) implementation.
//
// Real Rust port of include/utils/ucm.h and src/utils/ucm.c.
//
// Copyright (c) 2021-2026, Oriole DB Inc.
// Copyright (c) 2025-2026, Supabase Inc.

use pgrx::pg_sys::{self, pg_atomic_uint32, pg_atomic_uint64};

use crate::utils::page_pool::OInMemoryBlkno;

pub const UCM_INVALID_LEVEL: u32 = 0xF;
pub const UCM_USAGE_LEVELS: u32 = 0x7;
pub const UCM_FREE_PAGES_LEVEL: u32 = 0x7;
pub const UCM_LEVELS: u32 = 0x8;

const UCM_BRANCH_FACTOR: u32 = 15;
const UCM_LEVEL_BITS: u32 = 4;
const UCM_LEVEL_MASK: u32 = 0xF;

#[no_mangle]
pub static mut skip_ucm: bool = false;

const PAGE_STATE_CHANGE_USAGE_COUNT_MASK: u64 = 0x00F0_0000_0000_0000;
const PAGE_STATE_CHANGE_USAGE_COUNT_SHIFT: u32 = 52;

#[inline]
fn page_state_get_usage_count(state: u64) -> u32 {
    ((state & PAGE_STATE_CHANGE_USAGE_COUNT_MASK) >> PAGE_STATE_CHANGE_USAGE_COUNT_SHIFT) as u32
}

#[inline]
fn page_state_set_usage_count(state: u64, usage_count: u32) -> u64 {
    (state & !PAGE_STATE_CHANGE_USAGE_COUNT_MASK)
        | ((usage_count as u64) << PAGE_STATE_CHANGE_USAGE_COUNT_SHIFT)
}

#[repr(C)]
#[derive(Debug)]
pub struct OrioleDBPageHeader {
    pub state: pg_atomic_uint64,
    pub page_change_count: u32,
    pub checkpoint_num: u32,
}

#[repr(C)]
pub struct UsageCountMap {
    pub epoch: *mut pg_atomic_uint32,
    pub ucm: *mut pg_atomic_uint32,
    pub offset: OInMemoryBlkno,
    pub size: OInMemoryBlkno,
    pub total: i32,
    pub non_leaf: i32,
    pub root_factor: i32,
    pub usage_counter: u32,
}

#[inline]
fn atomic_u32_read(ptr: *mut pg_atomic_uint32) -> u32 {
    unsafe { pg_sys::pg_atomic_read_u32(ptr) }
}

#[inline]
fn atomic_u32_init(ptr: *mut pg_atomic_uint32, val: u32) {
    unsafe { pg_sys::pg_atomic_init_u32(ptr, val) }
}

#[inline]
fn atomic_u32_cas(ptr: *mut pg_atomic_uint32, current: &mut u32, new: u32) -> bool {
    unsafe { pg_sys::pg_atomic_compare_exchange_u32(ptr, current, new) }
}

#[inline]
fn atomic_u64_read(ptr: *mut pg_atomic_uint64) -> u64 {
    unsafe { pg_sys::pg_atomic_read_u64(ptr) }
}

#[inline]
fn atomic_u64_cas(ptr: *mut pg_atomic_uint64, current: &mut u64, new: u64) -> bool {
    unsafe { pg_sys::pg_atomic_compare_exchange_u64(ptr, current, new) }
}

#[inline]
unsafe fn page_header_of(blkno: OInMemoryBlkno) -> *mut OrioleDBPageHeader {
    debug_assert!(!crate::o_shared_buffers.is_null());
    debug_assert!((blkno as usize) < crate::orioledb_buffers_count);
    crate::o_shared_buffers.add((blkno as usize) * 8192) as *mut OrioleDBPageHeader
}

#[inline]
unsafe fn page_state_ptr_of(blkno: OInMemoryBlkno) -> *mut pg_atomic_uint64 {
    std::ptr::addr_of_mut!((*page_header_of(blkno)).state)
}

#[no_mangle]
pub unsafe extern "C-unwind" fn estimate_ucm_space(map: *mut UsageCountMap, offset: OInMemoryBlkno, size: OInMemoryBlkno) -> usize {
    let n_leaf_groups = (size + UCM_BRANCH_FACTOR - 1) / UCM_BRANCH_FACTOR;
    let n_leaf_vars = n_leaf_groups;

    let mut n_non_leaf_vars = 0u32;
    let mut n = n_leaf_vars;
    (*map).root_factor = UCM_BRANCH_FACTOR as i32;
    while n > UCM_BRANCH_FACTOR {
        n_non_leaf_vars += 1;
        n_non_leaf_vars *= UCM_BRANCH_FACTOR;
        n += UCM_BRANCH_FACTOR - 1;
        n /= UCM_BRANCH_FACTOR;
        (*map).root_factor *= UCM_BRANCH_FACTOR as i32;
    }

    (*map).offset = offset;
    (*map).size = size;
    (*map).total = (n_non_leaf_vars + n_leaf_vars) as i32;
    (*map).non_leaf = n_non_leaf_vars as i32;
    std::mem::size_of::<u32>() + std::mem::size_of::<pg_atomic_uint32>() * (*map).total as usize
}

fn get_value_frame(value: u32) -> u32 {
    let mut result = 0u32;
    let mut mask = UCM_LEVEL_MASK;
    let mut one: u32 = 1;
    for _ in 0..UCM_LEVELS {
        if value & mask != 0 {
            result += one;
        }
        one <<= UCM_LEVEL_BITS;
        mask <<= UCM_LEVEL_BITS;
    }
    result
}

fn init_ucm_non_leaf_recursive(map: &UsageCountMap, i: i32) -> u32 {
    if i < map.non_leaf {
        let mut value = 0u32;
        for j in (i + 1) * UCM_BRANCH_FACTOR as i32..(i + 2) * UCM_BRANCH_FACTOR as i32 {
            value += get_value_frame(init_ucm_non_leaf_recursive(map, j));
        }
        atomic_u32_init(unsafe { map.ucm.add(i as usize) }, value);
        value
    } else if i < map.total {
        atomic_u32_read(unsafe { map.ucm.add(i as usize) })
    } else {
        0
    }
}

#[no_mangle]
pub unsafe extern "C-unwind" fn init_ucm(map: *mut UsageCountMap, ptr: *mut u8, found: bool) {
    let epoch = ptr as *mut pg_atomic_uint32;
    let ucm = ptr.add(64) as *mut pg_atomic_uint32;
    (*map).epoch = epoch;
    (*map).ucm = ucm;

    if found {
        return;
    }

    atomic_u32_init(epoch, 0);

    let mut blkno = 0u32;
    for i in (*map).non_leaf..(*map).total {
        let pages_count = std::cmp::min((*map).size - blkno, UCM_BRANCH_FACTOR);
        atomic_u32_init(
            (*map).ucm.add(i as usize),
            (pages_count as u32) << (UCM_FREE_PAGES_LEVEL * UCM_LEVEL_BITS),
        );
        blkno += UCM_BRANCH_FACTOR;
    }

    for i in 0..UCM_BRANCH_FACTOR as i32 {
        init_ucm_non_leaf_recursive(&*map, i);
    }
}

fn ucm_inc_recursive(map: &UsageCountMap, i: i32, prev: i32, next: i32) {
    let prev_mask = if prev != UCM_INVALID_LEVEL as i32 {
        UCM_LEVEL_MASK << (prev as u32 * UCM_LEVEL_BITS)
    } else {
        0
    };
    let prev_one = if prev != UCM_INVALID_LEVEL as i32 {
        1u32 << (prev as u32 * UCM_LEVEL_BITS)
    } else {
        0
    };
    let next_mask = if next != UCM_INVALID_LEVEL as i32 {
        UCM_LEVEL_MASK << (next as u32 * UCM_LEVEL_BITS)
    } else {
        0
    };
    let next_one = if next != UCM_INVALID_LEVEL as i32 {
        1u32 << (next as u32 * UCM_LEVEL_BITS)
    } else {
        0
    };

    let mut val = atomic_u32_read(unsafe { map.ucm.add(i as usize) });
    let new_val = loop {
        if (val & prev_mask) < prev_one || (val & next_mask) > (next_mask - next_one) {
            loop {
                if (val & prev_mask) >= prev_one && (val & next_mask) <= (next_mask - next_one) {
                    break;
                }
                val = atomic_u32_read(unsafe { map.ucm.add(i as usize) });
            }
        }

        let nv = val - prev_one + next_one;

        if atomic_u32_cas(unsafe { map.ucm.add(i as usize) }, &mut val, nv) {
            break nv;
        }
    };

    if i >= UCM_BRANCH_FACTOR as i32 {
        ucm_inc_recursive(
            map,
            (i / UCM_BRANCH_FACTOR as i32) - 1,
            if (new_val & prev_mask) == 0 {
                prev
            } else {
                UCM_INVALID_LEVEL as i32
            },
            if (val & next_mask) == 0 {
                next
            } else {
                UCM_INVALID_LEVEL as i32
            },
        );
    }
}

#[no_mangle]
pub unsafe extern "C-unwind" fn ucm_inc(map: *const UsageCountMap, blkno: OInMemoryBlkno, prev: i32, next: i32) {
    ucm_inc_recursive(
        &*map,
        (*map).non_leaf + (blkno / UCM_BRANCH_FACTOR) as i32,
        prev,
        next,
    );
}

pub fn ucm_update_state(map: &mut UsageCountMap, _blkno: OInMemoryBlkno, state: u64) -> u64 {
    let epoch = atomic_u32_read(map.epoch);
    let usage_count = page_state_get_usage_count(state);

    if usage_count == UCM_INVALID_LEVEL || usage_count == UCM_FREE_PAGES_LEVEL {
        return state;
    }

    debug_assert!(usage_count < UCM_USAGE_LEVELS);

    map.usage_counter += 1;

    let mask = (1u32 << ((UCM_USAGE_LEVELS + usage_count - epoch) % UCM_USAGE_LEVELS)) - 1;

    if (map.usage_counter & mask) == 0
        && (usage_count + 1) % UCM_USAGE_LEVELS != epoch
    {
        page_state_set_usage_count(state, (usage_count + 1) % UCM_USAGE_LEVELS)
    } else {
        state
    }
}

pub fn ucm_after_update_state(map: &UsageCountMap, blkno: OInMemoryBlkno, old_state: u64, new_state: u64) {
    let old_usage = page_state_get_usage_count(old_state);
    let new_usage = page_state_get_usage_count(new_state);

    if old_usage != new_usage {
        unsafe {
            ucm_inc(map, blkno - map.offset, old_usage as i32, new_usage as i32);
        }
    }
}

fn page_inc_usage_count_internal(map: &mut UsageCountMap, blkno: OInMemoryBlkno, mut state: u64) {
    let epoch = atomic_u32_read(map.epoch);
    let usage_count = page_state_get_usage_count(state);

    debug_assert!(usage_count < UCM_USAGE_LEVELS);

    let page_state_ptr = unsafe { page_state_ptr_of(blkno) };

    map.usage_counter += 1;

    let mask = (1u32 << ((UCM_USAGE_LEVELS + usage_count - epoch) % UCM_USAGE_LEVELS)) - 1;

    if (map.usage_counter & mask) == 0 && (usage_count + 1) % UCM_USAGE_LEVELS != epoch {
        let new_state = page_state_set_usage_count(state, (usage_count + 1) % UCM_USAGE_LEVELS);
        if atomic_u64_cas(page_state_ptr, &mut state, new_state) {
            unsafe {
                ucm_inc(
                    map,
                    blkno - map.offset,
                    usage_count as i32,
                    ((usage_count + 1) % UCM_USAGE_LEVELS) as i32,
                );
            }
        }
    }
}

#[no_mangle]
pub unsafe extern "C-unwind" fn page_inc_usage_count(map: *mut UsageCountMap, blkno: OInMemoryBlkno) {
    let page_state_ptr = page_state_ptr_of(blkno);
    let state = atomic_u64_read(page_state_ptr);
    let usage_count = page_state_get_usage_count(state);

    if usage_count == UCM_INVALID_LEVEL
        || usage_count == UCM_FREE_PAGES_LEVEL
        || skip_ucm
    {
        return;
    }

    page_inc_usage_count_internal(&mut *map, blkno, state);
}

#[no_mangle]
pub unsafe extern "C-unwind" fn page_change_usage_count(map: *const UsageCountMap, blkno: OInMemoryBlkno, usage_count: u32) {
    let page_state_ptr = page_state_ptr_of(blkno);
    let mut state = atomic_u64_read(page_state_ptr);

    loop {
        let new_state = page_state_set_usage_count(state, usage_count);
        if atomic_u64_cas(page_state_ptr, &mut state, new_state) {
            break;
        }
    }
    ucm_inc(
        map,
        blkno - (*map).offset,
        page_state_get_usage_count(state) as i32,
        usage_count as i32,
    );
}

fn page_try_change_usage_count(map: &UsageCountMap, blkno: OInMemoryBlkno, old_state: u64, new_usage_count: u32) -> bool {
    let page_state_ptr = unsafe { page_state_ptr_of(blkno) };
    let old_usage = page_state_get_usage_count(old_state);
    let new_state = page_state_set_usage_count(old_state, new_usage_count);
    let mut cur = old_state;

    if atomic_u64_cas(page_state_ptr, &mut cur, new_state) {
        unsafe {
            ucm_inc(map, blkno - map.offset, old_usage as i32, new_usage_count as i32);
        }
        true
    } else {
        false
    }
}

fn ucm_check_recursive(map: &UsageCountMap, i: i32) -> bool {
    if i < map.non_leaf {
        let value = atomic_u32_read(unsafe { map.ucm.add(i as usize) });
        let j_max = std::cmp::min((i + 2) * UCM_BRANCH_FACTOR as i32, map.total);
        let mut expected = 0u32;
        let mut result = true;
        for j in (i + 1) * UCM_BRANCH_FACTOR as i32..j_max {
            result = result && ucm_check_recursive(map, j);
            expected += get_value_frame(atomic_u32_read(unsafe { map.ucm.add(j as usize) }));
        }
        if value != expected {
            pgrx::notice!("wrong value of internal ucm [{}]: expected {:#x}, have {:#x}", i, expected, value);
            result = false;
        }
        result
    } else if i < map.total {
        let group_num = i - map.non_leaf;
        let blkno_max = std::cmp::min((group_num + 1) * UCM_BRANCH_FACTOR as i32, map.size as i32) as u32;
        let mut expected = 0u32;
        let value = atomic_u32_read(unsafe { map.ucm.add(i as usize) });
        let mut result = true;
        for blkno in (group_num as u32 * UCM_BRANCH_FACTOR)..blkno_max {
            let page_state_ptr = unsafe { page_state_ptr_of(blkno + map.offset) };
            let usage_count = page_state_get_usage_count(atomic_u64_read(page_state_ptr));
            if usage_count < UCM_LEVELS {
                expected += 1 << (UCM_LEVEL_BITS * usage_count);
            } else if usage_count != UCM_INVALID_LEVEL {
                pgrx::notice!("wrong value of ucm [{}]: expected {:#x}, have {:#x}", i, expected, value);
                result = false;
            }
        }
        if value != expected {
            pgrx::notice!("wrong value of leaf ucm [{}]: expected {:#x}, have {:#x}", i, expected, value);
            result = false;
        }
        result
    } else {
        true
    }
}

#[no_mangle]
pub unsafe extern "C-unwind" fn ucm_check_map(map: *const UsageCountMap) -> bool {
    let mut result = true;
    for i in 0..UCM_BRANCH_FACTOR as i32 {
        result = result && ucm_check_recursive(&*map, i);
    }
    result
}

#[no_mangle]
pub unsafe extern "C-unwind" fn ucm_epoch_needs_shift(map: *const UsageCountMap) -> bool {
    let epoch = atomic_u32_read((*map).epoch);
    let mut mask = 0xFFFF_FFFFu32;
    for i in (UCM_USAGE_LEVELS - 2)..UCM_USAGE_LEVELS {
        let shift = ((i + epoch) % UCM_USAGE_LEVELS) * UCM_LEVEL_BITS;
        mask &= !(UCM_LEVEL_MASK << shift);
    }
    for i in 0..UCM_BRANCH_FACTOR as i32 {
        if atomic_u32_read((*map).ucm.add(i as usize)) & mask != 0 {
            return false;
        }
    }
    true
}

#[no_mangle]
pub unsafe extern "C-unwind" fn ucm_epoch_shift(map: *const UsageCountMap) {
    let epoch = atomic_u32_read((*map).epoch);
    let next_epoch = if epoch == UCM_USAGE_LEVELS - 1 { 0 } else { epoch + 1 };
    let mut cur = epoch;
    atomic_u32_cas((*map).epoch, &mut cur, next_epoch);
}

#[no_mangle]
pub unsafe extern "C-unwind" fn ucm_next_blkno(map: *mut UsageCountMap, init_blkno: OInMemoryBlkno, mask_src: u32) -> OInMemoryBlkno {
    let epoch = atomic_u32_read((*map).epoch);

    let mut mask: u32 = 0;
    for i in 0..UCM_USAGE_LEVELS {
        if mask_src & (1 << i) != 0 {
            let shift = ((i + epoch) % UCM_USAGE_LEVELS) * UCM_LEVEL_BITS;
            mask |= UCM_LEVEL_MASK << shift;
        }
    }

    let mut location = (init_blkno - (*map).offset) as i64;
    let mut factor = (*map).root_factor as i64;
    let mut base = 0i64;
    let mut num_iterations = 0i64;

    loop {
        let i = base + (location / factor) % UCM_BRANCH_FACTOR as i64;

        if factor == 1 && location < (*map).size as i64 {
            let page_state_ptr = page_state_ptr_of((location as u32) + (*map).offset);
            let state = atomic_u64_read(page_state_ptr);
            let usage_count = page_state_get_usage_count(state);
            if usage_count < UCM_LEVELS {
                let j = ((UCM_LEVELS + usage_count - epoch) % UCM_LEVELS) as u32;
                if mask_src & (1 << j) != 0 {
                    page_inc_usage_count_internal(&mut *map, (location as u32) + (*map).offset, state);
                    return (location as u32) + (*map).offset;
                }
            }
        }

        if i < (*map).total as i64 && atomic_u32_read((*map).ucm.add(i as usize)) & mask != 0 {
            base = (i + 1) * UCM_BRANCH_FACTOR as i64;
            factor /= UCM_BRANCH_FACTOR as i64;
            num_iterations = 0;
        } else {
            if num_iterations > 2 * UCM_BRANCH_FACTOR as i64 {
                if base == 0 {
                    let next_epoch = if epoch == UCM_USAGE_LEVELS - 1 { 0 } else { epoch + 1 };
                    let mut cur = epoch;
                    atomic_u32_cas((*map).epoch, &mut cur, next_epoch);
                    return ucm_next_blkno(map, init_blkno, mask_src);
                }
                factor *= UCM_BRANCH_FACTOR as i64;
                let new_i = (i / UCM_BRANCH_FACTOR as i64) - 1;
                base = (new_i / UCM_BRANCH_FACTOR as i64) * UCM_BRANCH_FACTOR as i64;
                num_iterations = 0;
            }
            let j = (location / factor) % UCM_BRANCH_FACTOR as i64;
            location = (location / factor) * factor;
            location += (((j + 1) % UCM_BRANCH_FACTOR as i64) - j) * factor;
            num_iterations += 1;
        }
    }
}

#[no_mangle]
pub unsafe extern "C-unwind" fn ucm_occupy_free_page(map: *const UsageCountMap) -> OInMemoryBlkno {
    let mask = UCM_LEVEL_MASK << (UCM_FREE_PAGES_LEVEL * UCM_LEVEL_BITS);
    let mut location = 0i64;
    let mut factor = (*map).root_factor as i64;
    let mut base = 0i64;
    let mut num_iterations = 0i64;

    loop {
        debug_assert!(factor > 0);

        let i = base + (location / factor) % UCM_BRANCH_FACTOR as i64;

        if factor == 1 && location < (*map).size as i64 {
            let blkno = (location as u32) + (*map).offset;
            let page_state_ptr = page_state_ptr_of(blkno);
            let state = atomic_u64_read(page_state_ptr);
            if page_state_get_usage_count(state) == UCM_FREE_PAGES_LEVEL
                && page_try_change_usage_count(&*map, blkno, state, UCM_INVALID_LEVEL)
            {
                return blkno;
            }
        }

        if i < (*map).total as i64 && atomic_u32_read((*map).ucm.add(i as usize)) & mask != 0 {
            base = (i + 1) * UCM_BRANCH_FACTOR as i64;
            factor /= UCM_BRANCH_FACTOR as i64;
            num_iterations = 0;
        } else {
            if num_iterations > 2 * UCM_BRANCH_FACTOR as i64 && base != 0 {
                factor *= UCM_BRANCH_FACTOR as i64;
                let new_i = (i / UCM_BRANCH_FACTOR as i64) - 1;
                base = (new_i / UCM_BRANCH_FACTOR as i64) * UCM_BRANCH_FACTOR as i64;
                num_iterations = 0;
            }
            let j = (location / factor) % UCM_BRANCH_FACTOR as i64;
            location = (location / factor) * factor;
            location += (((j + 1) % UCM_BRANCH_FACTOR as i64) - j) * factor;
            num_iterations += 1;
        }
    }
}
