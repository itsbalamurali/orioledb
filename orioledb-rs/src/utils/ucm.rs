//! OrioleDB usage count map (UCM) implementation.
//!
//! The UCM is a compact, cache-friendly map from in-memory block number to a
//! small usage-count used by the page-eviction policy. It is a fixed-shape
//! tree: a leaf level where each group of `UCM_BRANCH_FACTOR` pages owns one
//! `u32` word, and one or more non-leaf levels that summarize their children.
//! Every word is a `pg_atomic_u32` so the map is safe to read/write
//! concurrently from many backends.
//!
//! This mirrors `src/utils/ucm.c`.

use pgrx::pg_sys;

/// Number of leaf pages summarized by a single non-leaf word.
const UCM_BRANCH_FACTOR: i32 = 15;

/// Number of bits used to store one usage level inside a word.
const UCM_LEVEL_BITS: i32 = 4;

/// Bit mask for one usage level inside a word.
const UCM_LEVEL_MASK: u32 = 0xF;

/// PostgreSQL cache-line size, in bytes.
const PG_CACHE_LINE_SIZE: usize = 128;

/// Sentinel level meaning "invalid" (page is not tracked).
const UCM_INVALID_LEVEL: i32 = 0xF;

/// Number of usable usage levels.
const UCM_USAGE_LEVELS: i32 = 0x7;

/// Level used for free pages.
const UCM_FREE_PAGES_LEVEL: i32 = 0x7;

/// Total number of levels stored per word (including invalid).
const UCM_LEVELS: i32 = 0x8;

/// When true, the UCM is bypassed (used by some debug paths).
pub static mut SKIP_UCM: bool = false;

/// In-memory block number type.
pub type OInMemoryBlkno = u32;

/// A usage-count map over a range of in-memory pages.
#[repr(C)]
pub struct UsageCountMap {
    /// Epoch used to rotate the usage levels over time.
    pub epoch: *mut pg_sys::pg_atomic_uint32,
    /// Per-node atomic words (non-leaf then leaf).
    pub ucm: *mut pg_sys::pg_atomic_uint32,
    /// First block number covered by this map.
    pub offset: OInMemoryBlkno,
    /// Number of pages covered by this map.
    pub size: OInMemoryBlkno,
    /// Total number of words (non-leaf + leaf).
    pub total: i32,
    /// Number of non-leaf words.
    pub non_leaf: i32,
    /// Branch factor at the root of the map tree.
    pub root_factor: i32,
    /// Monotonic counter feeding the probabilistic update.
    pub usage_counter: u32,
}

/// Returns the number of set levels within a single `u32` word value.
fn get_value_frame(mut value: u32) -> i32 {
    let mut result = 0i32;
    for _ in 0..UCM_LEVELS {
        if value & UCM_LEVEL_MASK != 0 {
            result += 1;
        }
        value >>= UCM_LEVEL_BITS;
    }
    result
}

/// Estimates the shared-memory size required by a UCM covering `size`
/// pages starting at `offset`.
pub fn estimate_ucm_space(
    map: &mut UsageCountMap,
    offset: OInMemoryBlkno,
    size: OInMemoryBlkno,
) -> usize {
    map.offset = offset;
    map.size = size;

    let leaf_groups = (size as i32 + UCM_BRANCH_FACTOR - 1) / UCM_BRANCH_FACTOR;
    let mut n_leaf_vars = leaf_groups;
    let mut n_non_leaf_vars = 0;
    let mut n = leaf_groups;

    map.root_factor = UCM_BRANCH_FACTOR;
    while n > UCM_BRANCH_FACTOR {
        n_non_leaf_vars += 1;
        n_non_leaf_vars *= UCM_BRANCH_FACTOR;
        n += UCM_BRANCH_FACTOR - 1;
        n /= UCM_BRANCH_FACTOR;
        map.root_factor *= UCM_BRANCH_FACTOR;
    }

    map.total = n_non_leaf_vars + n_leaf_vars;
    map.non_leaf = n_non_leaf_vars;

    PG_CACHE_LINE_SIZE + std::mem::size_of::<pg_sys::pg_atomic_uint32>() * map.total as usize
}

fn init_ucm_non_leaf_recursive(map: &UsageCountMap, i: i32) -> u32 {
    if i < map.non_leaf {
        let mut value = 0u32;
        for j in (i + 1) * UCM_BRANCH_FACTOR..(i + 2) * UCM_BRANCH_FACTOR {
            value += init_ucm_non_leaf_recursive(map, j) as u32;
        }
        unsafe {
            pg_sys::pg_atomic_init_u32(&mut *map.ucm.add(i as usize), value);
        }
        value
    } else if i < map.total {
        unsafe { pg_sys::pg_atomic_read_u32(&mut *map.ucm.add(i as usize)) }
    } else {
        0
    }
}

/// Initializes a UCM's backing shared memory.
pub fn init_ucm(map: &mut UsageCountMap, ptr: *mut std::os::raw::c_void, found: bool) {
    map.epoch = ptr as *mut pg_sys::pg_atomic_uint32;
    let ptr = unsafe { ptr.add(PG_CACHE_LINE_SIZE) } as *mut pg_sys::pg_atomic_uint32;
    map.ucm = ptr;

    if found {
        return;
    }

    unsafe {
        pg_sys::pg_atomic_init_u32(&mut *map.epoch, 0);
    }

    // Initialize leaf variables.
    let mut blkno = 0u32;
    for i in map.non_leaf..map.total {
        let pages_count = (map.size - blkno).min(UCM_BRANCH_FACTOR as u32);
        unsafe {
            pg_sys::pg_atomic_init_u32(
                &mut *map.ucm.add(i as usize),
                pages_count << (UCM_FREE_PAGES_LEVEL * UCM_LEVEL_BITS),
            );
        }
        blkno += UCM_BRANCH_FACTOR as u32;
    }

    // Recursively initialize non-leaf variables.
    for i in 0..UCM_BRANCH_FACTOR {
        init_ucm_non_leaf_recursive(map, i);
    }
}

fn ucm_inc_recursive(map: &UsageCountMap, i: i32, prev: i32, next: i32) {
    assert!(prev < UCM_LEVELS || prev == UCM_INVALID_LEVEL);
    assert!(next < UCM_LEVELS || next == UCM_INVALID_LEVEL);

    let (prev_mask, prev_one) = if prev != UCM_INVALID_LEVEL {
        (
            UCM_LEVEL_MASK << (prev * UCM_LEVEL_BITS),
            1u32 << (prev * UCM_LEVEL_BITS),
        )
    } else {
        (0u32, 0u32)
    };

    let (next_mask, next_one) = if next != UCM_INVALID_LEVEL {
        (
            UCM_LEVEL_MASK << (next * UCM_LEVEL_BITS),
            1u32 << (next * UCM_LEVEL_BITS),
        )
    } else {
        (0u32, 0u32)
    };

    let mut val = unsafe { pg_sys::pg_atomic_read_u32(&mut *map.ucm.add(i as usize)) };
    loop {
        if (val & prev_mask) < prev_one || (val & next_mask) > (next_mask - next_one) {
            // Spin until the invariants hold, then attempt the update.
            let mut delay = pg_sys::SpinDelayStatus::default();
            pg_sys::init_local_spin_delay(&mut delay);
            while (val & prev_mask) < prev_one || (val & next_mask) > (next_mask - next_one) {
                pg_sys::perform_spin_delay(&mut delay);
                val = unsafe { pg_sys::pg_atomic_read_u32(&mut *map.ucm.add(i as usize)) };
            }
            pg_sys::finish_spin_delay(&mut delay);
        }

        let new_val = val - prev_one + next_one;

        let swapped = unsafe {
            pg_sys::pg_atomic_compare_exchange_u32(
                &mut *map.ucm.add(i as usize),
                &mut val,
                new_val,
            )
        };
        if swapped {
            if i >= UCM_BRANCH_FACTOR {
                ucm_inc_recursive(
                    map,
                    (i / UCM_BRANCH_FACTOR) - 1,
                    if (new_val & prev_mask) == 0 {
                        prev
                    } else {
                        UCM_INVALID_LEVEL
                    },
                    if (val & next_mask) == 0 {
                        next
                    } else {
                        UCM_INVALID_LEVEL
                    },
                );
            }
            return;
        }
    }
}

/// Atomically increments (or decrements) the usage count of `blkno`.
pub fn ucm_inc(map: &UsageCountMap, blkno: OInMemoryBlkno, prev: i32, next: i32) {
    ucm_inc_recursive(
        map,
        map.non_leaf + (blkno / UCM_BRANCH_FACTOR as u32) as i32,
        prev,
        next,
    );
}

fn page_inc_usage_count_internal(map: &UsageCountMap, blkno: OInMemoryBlkno, state: u64) {
    let epoch = unsafe { pg_sys::pg_atomic_read_u32(&mut *map.epoch) };
    let usage_count = o_page_state_get_usage_count(state);
    assert!((usage_count as i32) < UCM_USAGE_LEVELS);

    map.usage_counter += 1;

    let mask = (1u32 << ((UCM_USAGE_LEVELS + usage_count as i32 - epoch as i32) % UCM_USAGE_LEVELS)) - 1;

    if (map.usage_counter & mask) == 0
        && ((usage_count + 1) % UCM_USAGE_LEVELS as u32) != epoch
    {
        let new_state =
            o_page_state_set_usage_count(state, (usage_count + 1) % UCM_USAGE_LEVELS as u32);
        if unsafe {
            pg_sys::pg_atomic_compare_exchange_u64(
                crate::btree::io::page_header_state(blkno),
                &mut { state } as *mut u64,
                new_state,
            )
        } {
            ucm_inc(
                map,
                blkno - map.offset,
                usage_count as i32,
                (usage_count + 1) % UCM_USAGE_LEVELS as u32 as i32,
            );
        }
    }
}

/// Increments the usage count of a page, probabilistically.
pub fn page_inc_usage_count(map: &UsageCountMap, blkno: OInMemoryBlkno) {
    let state = crate::btree::io::page_state(blkno);
    let usage_count = o_page_state_get_usage_count(state);

    if usage_count as i32 == UCM_INVALID_LEVEL
        || usage_count as i32 == UCM_FREE_PAGES_LEVEL
        || unsafe { SKIP_UCM }
    {
        return;
    }

    page_inc_usage_count_internal(map, blkno, state);
}

/// Sets the usage count of a page to an explicit value.
pub fn page_change_usage_count(map: &UsageCountMap, blkno: OInMemoryBlkno, usage_count: u32) {
    let mut state = crate::btree::io::page_state(blkno);
    loop {
        let new_state = o_page_state_set_usage_count(state, usage_count);
        if unsafe {
            pg_sys::pg_atomic_compare_exchange_u64(
                crate::btree::io::page_header_state(blkno),
                &mut state as *mut u64,
                new_state,
            )
        } {
            break;
        }
    }
    ucm_inc(
        map,
        blkno - map.offset,
        o_page_state_get_usage_count(state) as i32,
        usage_count as i32,
    );
}

fn page_try_change_usage_count(
    map: &UsageCountMap,
    blkno: OInMemoryBlkno,
    old_state: u64,
    new_usage_count: u32,
) -> bool {
    let old_usage_count = o_page_state_get_usage_count(old_state);
    let new_state = o_page_state_set_usage_count(old_state, new_usage_count);

    if unsafe {
        pg_sys::pg_atomic_compare_exchange_u64(
            crate::btree::io::page_header_state(blkno),
            &mut { old_state } as *mut u64,
            new_state,
        )
    } {
        ucm_inc(
            map,
            blkno - map.offset,
            old_usage_count as i32,
            new_usage_count as i32,
        );
        true
    } else {
        false
    }
}

fn ucm_check_recursive(map: &UsageCountMap, i: i32) -> bool {
    if i < map.non_leaf {
        // Non-leaf: recompute the expected sum of children.
        let mut expected = 0u32;
        let mut value = unsafe { pg_sys::pg_atomic_read_u32(&mut *map.ucm.add(i as usize)) };
        let j_max = ((i + 2) * UCM_BRANCH_FACTOR).min(map.total);
        for j in (i + 1) * UCM_BRANCH_FACTOR..j_max {
            let ok = ucm_check_recursive(map, j);
            expected += get_value_frame(unsafe { pg_sys::pg_atomic_read_u32(&mut *map.ucm.add(j as usize)) });
            if !ok {
                return false;
            }
        }
        if value != expected {
            pgrx::log!("NOTICE: wrong value of internal ucm[{}]: expected {:x}, have {:x}", i, expected, value);
            return false;
        }
        true
    } else if i < map.total {
        let group_num = i - map.non_leaf;
        let mut result = true;
        let mut expected = 0u32;
        let value = unsafe { pg_sys::pg_atomic_read_u32(&mut *map.ucm.add(i as usize)) };
        let blkno_max = ((group_num + 1) * UCM_BRANCH_FACTOR).min(map.size as i32) as u32;
        for blkno in (group_num * UCM_BRANCH_FACTOR) as u32..blkno_max {
            let usage_count =
                o_page_state_get_usage_count(crate::btree::io::page_state(blkno + map.offset));
            if (usage_count as i32) < UCM_LEVELS {
                expected += 1u32 << (UCM_LEVEL_BITS * usage_count as i32);
            } else if (usage_count as i32) != UCM_INVALID_LEVEL {
                pgrx::log!(
                    "NOTICE: wrong value of ucm[{}]: expected {:x}, have {:x}",
                    i,
                    expected,
                    value
                );
                result = false;
            }
        }
        if value != expected {
            pgrx::log!(
                "NOTICE: wrong value of leaf ucm[{}]: expected {:x}, have {:x}",
                i,
                expected,
                value
            );
            result = false;
        }
        result
    } else {
        true
    }
}

/// Verifies the internal consistency of the whole map.
pub fn ucm_check_map(map: &UsageCountMap) -> bool {
    let mut result = true;
    for i in 0..UCM_BRANCH_FACTOR {
        result = result && ucm_check_recursive(map, i);
    }
    result
}

/// Whether the usage-level epoch needs to be shifted.
pub fn ucm_epoch_needs_shift(map: &UsageCountMap) -> bool {
    let epoch = unsafe { pg_sys::pg_atomic_read_u32(&mut *map.epoch) };
    let mut mask = 0xFFFFFFFFu32;
    for i in UCM_USAGE_LEVELS - 2..UCM_USAGE_LEVELS {
        let shift = ((i + epoch as i32) % UCM_USAGE_LEVELS) * UCM_LEVEL_BITS;
        mask &= !(UCM_LEVEL_MASK << shift);
    }
    for i in 0..UCM_BRANCH_FACTOR {
        if unsafe { pg_sys::pg_atomic_read_u32(&mut *map.ucm.add(i as usize)) } & mask != 0 {
            return false;
        }
    }
    true
}

/// Advances the usage-level epoch by one.
pub fn ucm_epoch_shift(map: &mut UsageCountMap) {
    let mut epoch = unsafe { pg_sys::pg_atomic_read_u32(&mut *map.epoch) };
    let next_epoch = if epoch == (UCM_USAGE_LEVELS - 1) as u32 {
        0
    } else {
        epoch + 1
    };
    unsafe {
        pg_sys::pg_atomic_compare_exchange_u32(&mut *map.epoch, &mut epoch, next_epoch);
    }
}

/// Finds the next block whose usage count matches one of the requested
/// levels, incrementing its count and returning it.
pub fn ucm_next_blkno(
    map: &UsageCountMap,
    init_blkno: OInMemoryBlkno,
    mask_src: u32,
) -> OInMemoryBlkno {
    let mut epoch = unsafe { pg_sys::pg_atomic_read_u32(&mut *map.epoch) };

    'retry: loop {
        let mut mask = 0u32;
        for i in 0..UCM_USAGE_LEVELS {
            if (mask_src & (1 << i)) != 0 {
                let shift = ((i + epoch as i32) % UCM_USAGE_LEVELS) * UCM_LEVEL_BITS;
                mask |= UCM_LEVEL_MASK << shift;
            }
        }

        let mut location = init_blkno as i64 - map.offset as i64;
        let mut factor = map.root_factor as i64;
        let mut base = 0i64;
        let mut num_iterations = 0i64;
        loop {
            let i = base + (location / factor) % UCM_BRANCH_FACTOR as i64;

            if factor == 1 && location < map.size as i64 {
                let state = crate::btree::io::page_state((location + map.offset as i64) as u32);
                let usage_count = o_page_state_get_usage_count(state);
                if (usage_count as i32) < UCM_LEVELS {
                    let j = ((UCM_USAGE_LEVELS + usage_count as i32 - epoch as i32) % UCM_USAGE_LEVELS) as u32;
                    if (mask_src & (1 << j)) != 0 {
                        page_inc_usage_count_internal(
                            map,
                            (location + map.offset as i64) as u32,
                            state,
                        );
                        return (location + map.offset as i64) as u32;
                    }
                }
            }

            if i < map.total as i64
                && unsafe { pg_sys::pg_atomic_read_u32(&mut *map.ucm.add(i as usize)) } & mask != 0
            {
                base = (i + 1) * UCM_BRANCH_FACTOR as i64;
                factor /= UCM_BRANCH_FACTOR as i64;
                num_iterations = 0;
            } else {
                if num_iterations > 2 * UCM_BRANCH_FACTOR as i64 {
                    if base == 0 {
                        let next_epoch = if epoch == (UCM_USAGE_LEVELS - 1) as u32 {
                            0
                        } else {
                            epoch + 1
                        };
                        unsafe {
                            pg_sys::pg_atomic_compare_exchange_u32(
                                &mut *map.epoch,
                                &mut epoch,
                                next_epoch,
                            );
                        }
                        continue 'retry;
                    }
                    factor *= UCM_BRANCH_FACTOR as i64;
                    let ii = (i / UCM_BRANCH_FACTOR as i64) - 1;
                    base = (ii / UCM_BRANCH_FACTOR as i64) * UCM_BRANCH_FACTOR as i64;
                    num_iterations = 0;
                }
                let j = (location / factor) % UCM_BRANCH_FACTOR as i64;
                location = (location / factor) * factor;
                location += ((j + 1) % UCM_BRANCH_FACTOR as i64 - j) * factor;
                num_iterations += 1;
            }
        }
    }
}

/// Occupies a currently-free page, marking it invalid, and returns its block
/// number. Used by the eviction/page-allocation path.
pub fn ucm_occupy_free_page(map: &UsageCountMap) -> OInMemoryBlkno {
    let mut location = 0i64;
    let mut factor = map.root_factor as i64;
    let mut base = 0i64;
    let mut num_iterations = 0i64;
    let mask = UCM_LEVEL_MASK << (UCM_FREE_PAGES_LEVEL * UCM_LEVEL_BITS);

    loop {
        let i = base + (location / factor) % UCM_BRANCH_FACTOR as i64;

        if factor == 1 && location < map.size as i64 {
            let blkno = (location + map.offset as i64) as u32;
            let state = crate::btree::io::page_state(blkno);
            if o_page_state_get_usage_count(state) as i32 == UCM_FREE_PAGES_LEVEL
                && page_try_change_usage_count(map, blkno, state, UCM_INVALID_LEVEL as u32)
            {
                return blkno;
            }
        }

        if i < map.total as i64
            && unsafe { pg_sys::pg_atomic_read_u32(&mut *map.ucm.add(i as usize)) } & mask != 0
        {
            base = (i + 1) * UCM_BRANCH_FACTOR as i64;
            factor /= UCM_BRANCH_FACTOR as i64;
            num_iterations = 0;
        } else {
            if num_iterations > 2 * UCM_BRANCH_FACTOR as i64 && base != 0 {
                factor *= UCM_BRANCH_FACTOR as i64;
                let ii = (i / UCM_BRANCH_FACTOR as i64) - 1;
                base = (ii / UCM_BRANCH_FACTOR as i64) * UCM_BRANCH_FACTOR as i64;
                num_iterations = 0;
            }
            let j = (location / factor) % UCM_BRANCH_FACTOR as i64;
            location = (location / factor) * factor;
            location += ((j + 1) % UCM_BRANCH_FACTOR as i64 - j) * factor;
            num_iterations += 1;
        }
    }
}

// ---------------------------------------------------------------------------
// Page-state accessors (bit math, no backend state)
// ---------------------------------------------------------------------------
//
// These mirror the `O_PAGE_STATE_*` macros from `btree/page_state.h`. They are
// pure functions over the page-header `state` word. The raw word for a given
// block number is obtained through `crate::btree::io`, which owns the shared
// buffer.

/// Extracts the usage count from a page-header state word.
pub fn o_page_state_get_usage_count(state: u64) -> u32 {
    ((state >> 8) & 0xFF) as u32
}

/// Returns `state` with the usage count replaced by `usage_count`.
pub fn o_page_state_set_usage_count(state: u64, usage_count: u32) -> u64 {
    (state & !(0xFFu64 << 8)) | ((usage_count as u64 & 0xFF) << 8)
}

/// Combined update used by the page-state fast path.
pub fn ucm_update_state(map: &mut UsageCountMap, blkno: OInMemoryBlkno, state: u64) -> u64 {
    let epoch = unsafe { pg_sys::pg_atomic_read_u32(&mut *map.epoch) };
    let usage_count = o_page_state_get_usage_count(state);

    if usage_count as i32 == UCM_INVALID_LEVEL || usage_count as i32 == UCM_FREE_PAGES_LEVEL {
        return state;
    }
    assert!((usage_count as i32) < UCM_USAGE_LEVELS);

    map.usage_counter += 1;
    let mask = (1u32 << ((UCM_USAGE_LEVELS + usage_count as i32 - epoch as i32) % UCM_USAGE_LEVELS)) - 1;

    if (map.usage_counter & mask) == 0
        && ((usage_count + 1) % UCM_USAGE_LEVELS as u32) != epoch
    {
        o_page_state_set_usage_count(state, (usage_count + 1) % UCM_USAGE_LEVELS as u32)
    } else {
        state
    }
}

/// Records the UCM increment implied by a page-state transition.
pub fn ucm_after_update_state(
    map: &UsageCountMap,
    blkno: OInMemoryBlkno,
    old_state: u64,
    new_state: u64,
) {
    let old_usage = o_page_state_get_usage_count(old_state);
    let new_usage = o_page_state_get_usage_count(new_state);
    if old_usage != new_usage {
        ucm_inc(map, blkno - map.offset, old_usage as i32, new_usage as i32);
    }
}
