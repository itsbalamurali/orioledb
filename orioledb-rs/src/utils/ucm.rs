use crate::btree::page_state;
use crate::c;
use crate::orioledb;
use crate::utils::dsa;
use crate::utils::ucm;
use pgrx::pg_sys;

// -------------------------------------------------------------------------
//
// ucm.c
// OrioleDB usage count map (UCM) implementation.
//
// Copyright (c) 2021-2026, Oriole DB Inc.
// Copyright (c) 2025-2026, Supabase Inc.
//
// IDENTIFICATION
// contrib/orioledb/src/utils/ucm.c
//
// -------------------------------------------------------------------------
//

#define UCM_BRANCH_FACTOR	15
#define UCM_LEVEL_BITS		4
#define UCM_LEVEL_MASK		0xF

pub static mut SKIP_UCM: bool = false;

static int	init_ucm_non_leaf_recursive(map: &mut UsageCountMap, int i);
fn ucm_inc_recursive(map: &mut UsageCountMap, int i, int prev, int next);
static bool ucm_check_recursive(map: &mut UsageCountMap, int i);

//
// Estimate shaed memory space for UCM data structure.
//
Size
estimate_ucm_space(map: &mut UsageCountMap, OInMemoryBlkno offset, OInMemoryBlkno size)
{
	pub static mut N_LEAF_GROUPS: std::os::raw::c_int = 0;
	pub static mut N_LEAF_VARS: std::os::raw::c_int = 0;
	pub static mut N_NON_LEAF_VARS: std::os::raw::c_int = 0;
	pub static mut N: std::os::raw::c_int = 0;

	map->offset = offset;
	map->size = size;
	n_leaf_groups = (map->size + UCM_BRANCH_FACTOR - 1) / UCM_BRANCH_FACTOR;
	n_leaf_vars = n_leaf_groups;

	n_non_leaf_vars = 0;
	n = n_leaf_vars;
	map->rootFactor = UCM_BRANCH_FACTOR;
	while (n > UCM_BRANCH_FACTOR)
	{
		n_non_leaf_vars += 1;
		n_non_leaf_vars *= UCM_BRANCH_FACTOR;
		n += UCM_BRANCH_FACTOR - 1;
		n /= UCM_BRANCH_FACTOR;
		map->rootFactor *= UCM_BRANCH_FACTOR;
	}

	map->total = n_non_leaf_vars + n_leaf_vars;
	map->nonLeaf = n_non_leaf_vars;
	return PG_CACHE_LINE_SIZE + sizeof(pg_atomic_uint32) * map->total;
}

static int
get_value_frame(uint32 value)
{
	pub static mut I: std::os::raw::c_int = 0;
	uint32		mask = UCM_LEVEL_MASK,
				one = 1,
				result = 0;

	for (i = 0; i < UCM_LEVELS; i++)
	{
		if (value & mask)
			result += one;

		one <<= UCM_LEVEL_BITS;
		mask <<= UCM_LEVEL_BITS;
	}

	pub static mut RESULT: return = std::mem::zeroed();
}

static int
init_ucm_non_leaf_recursive(map: &mut UsageCountMap, int i)
{
	if (i < map->nonLeaf)
	{
		pub static mut J: std::os::raw::c_int = 0;
		pub static mut VALUE: uint32 = std::mem::zeroed();

		value = 0;
		for (j = (i + 1) * UCM_BRANCH_FACTOR; j < (i + 2) * UCM_BRANCH_FACTOR; j++)
		{
			value += get_value_frame(init_ucm_non_leaf_recursive(map, j));
		}
		pg_atomic_init_u32(&map->ucm[i], value);
		pub static mut VALUE: return = std::mem::zeroed();
	}
	else if (i < map->total)
	{
		return pg_atomic_read_u32(&map->ucm[i]);
	}
	else
	{
		pub static mut 0: return = std::mem::zeroed();
	}
}

//
// Initialize UCM shared memory.
//

init_ucm(map: &mut UsageCountMap, Pointer ptr, bool found)
{
	pub static mut I: std::os::raw::c_int = 0;
	pub static mut BLKNO: OInMemoryBlkno = std::mem::zeroed();

	map->epoch = (pg_atomic_uint32 *) ptr;
	ptr += PG_CACHE_LINE_SIZE;

	map->ucm = (pg_atomic_uint32 *) ptr;

	if (found)
		return;

	pg_atomic_init_u32(map->epoch, 0);

	// Init leaf variables
	blkno = 0;
	for (i = map->nonLeaf; i < map->total; i++)
	{
		uint32		pagesCount = Min(map->size - blkno, UCM_BRANCH_FACTOR);

		pg_atomic_init_u32(&map->ucm[i],
						   pagesCount << (UCM_FREE_PAGES_LEVEL * UCM_LEVEL_BITS));
		blkno += UCM_BRANCH_FACTOR;
	}

	// Recursively inin non-leaf variables
	for (i = 0; i < UCM_BRANCH_FACTOR; i++)
		init_ucm_non_leaf_recursive(map, i);
}

//
// Worker function, which recursively increments value of ucm map.
//
fn
ucm_inc_recursive(map: &mut UsageCountMap, int i, int32 prev, int32 next)
{
	uint32		val,
				new_val,
				prev_mask,
				next_mask,
				prev_one,
				next_one;

	Assert(prev < UCM_LEVELS || prev == UCM_INVALID_LEVEL);
	Assert(next < UCM_LEVELS || next == UCM_INVALID_LEVEL);

	if (prev != UCM_INVALID_LEVEL)
	{
		prev_mask = UCM_LEVEL_MASK << (prev * UCM_LEVEL_BITS);
		prev_one = 1 << (prev * UCM_LEVEL_BITS);
	}
	else
	{
		prev_mask = 0;
		prev_one = 0;
	}

	if (next != UCM_INVALID_LEVEL)
	{
		next_mask = UCM_LEVEL_MASK << (next * UCM_LEVEL_BITS);
		next_one = 1 << (next * UCM_LEVEL_BITS);
	}
	else
	{
		next_mask = 0;
		next_one = 0;
	}

	val = pg_atomic_read_u32(&map->ucm[i]);
	while (true)
	{
		if ((val & prev_mask) < prev_one || (val & next_mask) > (next_mask - next_one))
		{
			pub static mut DELAY_STATUS: SpinDelayStatus = std::mem::zeroed();

			init_local_spin_delay(&delayStatus);

			while ((val & prev_mask) < prev_one || (val & next_mask) > (next_mask - next_one))
			{
				perform_spin_delay(&delayStatus);
				val = pg_atomic_read_u32(&map->ucm[i]);
			}
			finish_spin_delay(&delayStatus);
		}

		new_val = val - prev_one + next_one;

		if (pg_atomic_compare_exchange_u32(&map->ucm[i], &val, new_val))
			break;
	}

	if (i >= UCM_BRANCH_FACTOR)
		ucm_inc_recursive(map, (i / UCM_BRANCH_FACTOR) - 1,
						  ((new_val & prev_mask) == 0) ? prev : UCM_INVALID_LEVEL,
						  ((val & next_mask) == 0) ? next : UCM_INVALID_LEVEL);
}


ucm_inc(map: &mut UsageCountMap, OInMemoryBlkno blkno, int prev, int next)
{
	ucm_inc_recursive(map, map->nonLeaf + blkno / UCM_BRANCH_FACTOR, prev, next);
}

fn
page_inc_usage_count_internal(map: &mut UsageCountMap, OInMemoryBlkno blkno,
							  uint64 state)
{
	uint32		epoch = pg_atomic_read_u32(map->epoch),
				mask;
	uint32		usageCount = O_PAGE_STATE_GET_USAGE_COUNT(state);

	Assert(usageCount < UCM_USAGE_LEVELS);

	map->usageCounter++;

	mask = (1 << ((UCM_USAGE_LEVELS + usageCount - epoch) % UCM_USAGE_LEVELS)) - 1;

	if ((map->usageCounter & mask) == 0 && (usageCount + 1) % UCM_USAGE_LEVELS != epoch)
	{
		Page		p = O_GET_IN_MEMORY_PAGE(blkno);

		if (pg_atomic_compare_exchange_u64(&(O_PAGE_HEADER(p)->state),
										   &state,
										   O_PAGE_STATE_SET_USAGE_COUNT(state, (usageCount + 1) % UCM_USAGE_LEVELS)))
		{
			ucm_inc(map, blkno - map->offset, usageCount, (usageCount + 1) % UCM_USAGE_LEVELS);
		}
	}
}


page_inc_usage_count(map: &mut UsageCountMap, OInMemoryBlkno blkno)
{
	Page		p = O_GET_IN_MEMORY_PAGE(blkno);
	header: &mut OrioleDBPageHeader = (OrioleDBPageHeader *) p;
	uint64		state = pg_atomic_read_u64(&header->state);
	uint32		usageCount = O_PAGE_STATE_GET_USAGE_COUNT(state);

	if (usageCount == UCM_INVALID_LEVEL ||
		usageCount == UCM_FREE_PAGES_LEVEL ||
		skip_ucm)
		return;

	page_inc_usage_count_internal(map, blkno, state);
}


page_change_usage_count(map: &mut UsageCountMap, OInMemoryBlkno blkno, uint32 usageCount)
{
	Page		p = O_GET_IN_MEMORY_PAGE(blkno);
	header: &mut OrioleDBPageHeader = (OrioleDBPageHeader *) p;
	uint64		state = pg_atomic_read_u64(&header->state);

	while (true)
	{
		if (pg_atomic_compare_exchange_u64(&(O_PAGE_HEADER(p)->state),
										   &state,
										   O_PAGE_STATE_SET_USAGE_COUNT(state, usageCount)))
			break;
	}
	ucm_inc(map, blkno - map->offset, O_PAGE_STATE_GET_USAGE_COUNT(state), usageCount);
}

static bool
page_try_change_usage_count(map: &mut UsageCountMap, OInMemoryBlkno blkno,
							uint64 oldState, uint32 newUsageCount)
{
	Page		p = O_GET_IN_MEMORY_PAGE(blkno);
	uint32		oldUsageCount = O_PAGE_STATE_GET_USAGE_COUNT(oldState);
	uint64		newState = O_PAGE_STATE_SET_USAGE_COUNT(oldState, newUsageCount);

	if (pg_atomic_compare_exchange_u64(&(O_PAGE_HEADER(p)->state),
									   &oldState,
									   newState))
	{
		ucm_inc(map, blkno - map->offset, oldUsageCount, newUsageCount);
		pub static mut TRUE: return = std::mem::zeroed();
	}
	else
	{
		pub static mut FALSE: return = std::mem::zeroed();
	}
}

static bool
ucm_check_recursive(map: &mut UsageCountMap, int i)
{
	if (i < map->nonLeaf)
	{
		// Non-leaf
		int			j,
					j_max;
		uint32		expected = 0,
					value;
		pub static mut RESULT: bool = true;

		value = pg_atomic_read_u32(&map->ucm[i]);
		j_max = Min((i + 2) * UCM_BRANCH_FACTOR, map->total);
		for (j = (i + 1) * UCM_BRANCH_FACTOR; j < j_max; j++)
		{
			result = result && ucm_check_recursive(map, j);
			expected += get_value_frame(pg_atomic_read_u32(&map->ucm[j]));
		}

		if (value != expected)
		{
			elog(NOTICE, "wrong value of internal ucm[%d]: expected %x, have %x",
				 i, expected, value);
			result = false;
		}
		pub static mut RESULT: return = std::mem::zeroed();
	}
	else if (i < map->total)
	{
		int			group_num = i - map->nonLeaf,
					blkno,
					blkno_max;
		pub static mut RESULT: bool = true;
		uint32		expected = 0,
					value,
					usageCount;

		value = pg_atomic_read_u32(&map->ucm[i]);
		blkno_max = Min((group_num + 1) * UCM_BRANCH_FACTOR, map->size);
		for (blkno = group_num * UCM_BRANCH_FACTOR; blkno < blkno_max; blkno++)
		{
			Page		p = O_GET_IN_MEMORY_PAGE(blkno + map->offset);

			usageCount = O_PAGE_STATE_GET_USAGE_COUNT(pg_atomic_read_u64(&(O_PAGE_HEADER(p)->state)));

			if (usageCount < UCM_LEVELS)
			{
				expected += (1 << (UCM_LEVEL_BITS * usageCount));
			}
			else if (usageCount != UCM_INVALID_LEVEL)
			{
				elog(NOTICE, "wrong value of ucm[%d]: expected %x, have %x",
					 i, expected, value);
				result = false;
			}
		}

		if (value != expected)
		{
			elog(NOTICE, "wrong value of leaf ucm[%d]: expected %x, have %x",
				 i, expected, value);
			result = false;
		}

		pub static mut RESULT: return = std::mem::zeroed();
	}
	else
	{
		pub static mut 0: return = std::mem::zeroed();
	}
}

bool
ucm_check_map(map: &mut UsageCountMap)
{
	pub static mut RESULT: bool = true;
	pub static mut I: std::os::raw::c_int = 0;

	for (i = 0; i < UCM_BRANCH_FACTOR; i++)
		result = result && ucm_check_recursive(map, i);

	pub static mut RESULT: return = std::mem::zeroed();
}

bool
ucm_epoch_needs_shift(map: &mut UsageCountMap)
{
	uint32		mask,
				epoch;
	pub static mut I: std::os::raw::c_int = 0;

	epoch = pg_atomic_read_u32(map->epoch);
	mask = 0xFFFFFFFF;
	for (i = UCM_USAGE_LEVELS - 2; i < UCM_USAGE_LEVELS; i++)
	{
		int			shift = ((i + epoch) % UCM_USAGE_LEVELS) * UCM_LEVEL_BITS;

		mask &= ~(UCM_LEVEL_MASK << shift);
	}

	for (i = 0; i < UCM_BRANCH_FACTOR; i++)
	{
		if (pg_atomic_read_u32(&map->ucm[i]) & mask)
			pub static mut FALSE: return = std::mem::zeroed();
	}
	pub static mut TRUE: return = std::mem::zeroed();
}


ucm_epoch_shift(map: &mut UsageCountMap)
{
	uint32		epoch,
				next_epoch;

	epoch = pg_atomic_read_u32(map->epoch);
	if (epoch == UCM_USAGE_LEVELS - 1)
		next_epoch = 0;
	else
		next_epoch = epoch + 1;
	pg_atomic_compare_exchange_u32(map->epoch, &epoch, next_epoch);
}

OInMemoryBlkno
ucm_next_blkno(map: &mut UsageCountMap, OInMemoryBlkno init_blkno,
			   uint32 mask_src)
{
	pub static mut LOCATION: int64 = std::mem::zeroed();
	pub static mut I: int64 = std::mem::zeroed();
	int64		factor,
				base;
	pub static mut NUM_ITERATIONS: int64 = std::mem::zeroed();
	pub static mut MASK: uint32 = std::mem::zeroed();
	pub static mut EPOCH: uint32 = std::mem::zeroed();

	epoch = pg_atomic_read_u32(map->epoch);

retry:

	mask = 0;
	for (i = 0; i < UCM_USAGE_LEVELS; i++)
	{
		if (mask_src & (1 << i))
		{
			int			shift = ((i + epoch) % UCM_USAGE_LEVELS) * UCM_LEVEL_BITS;

			mask |= UCM_LEVEL_MASK << shift;
		}
	}

	location = init_blkno - map->offset;
	factor = map->rootFactor;
	base = 0;
	num_iterations = 0;
	while (true)
	{
		i = base + (location / factor) % UCM_BRANCH_FACTOR;

		if (factor == 1 && location < map->size)
		{
			// Work with pages themselves
			header: &mut OrioleDBPageHeader = (OrioleDBPageHeader *) O_GET_IN_MEMORY_PAGE(location + map->offset);
			pub static mut STATE: uint64 = std::mem::zeroed();
			pub static mut USAGE_COUNT: uint32 = std::mem::zeroed();

			state = pg_atomic_read_u64(&header->state);
			usageCount = O_PAGE_STATE_GET_USAGE_COUNT(state);
			if (usageCount < UCM_LEVELS)
			{
				int			j = (UCM_LEVELS + usageCount - epoch) % UCM_LEVELS;

				if (mask_src & (1 << j))
				{
					page_inc_usage_count_internal(map, location + map->offset,
												  state);
					return location + map->offset;
				}
			}
		}

		if (i < map->total && (pg_atomic_read_u32(&map->ucm[i]) & mask))
		{
			// Required usage counts should be here, so step into
			base = (i + 1) * UCM_BRANCH_FACTOR;
			factor /= UCM_BRANCH_FACTOR;
			num_iterations = 0;
		}
		else
		{
			// Not found, so step over
			pub static mut J: int64 = std::mem::zeroed();

			if (num_iterations > 2 * UCM_BRANCH_FACTOR)
			{
				//
// Made two rounds and didn't found required usage counts.  So
// give up and retry at upper level.
//
				if (base == 0)
				{
					pub static mut NEXT_EPOCH: uint32 = std::mem::zeroed();

					if (epoch == UCM_USAGE_LEVELS - 1)
						next_epoch = 0;
					else
						next_epoch = epoch + 1;

					pg_atomic_compare_exchange_u32(map->epoch,
												   &epoch,
												   next_epoch);
					pub static mut RETRY: goto = std::mem::zeroed();
				}
				factor *= UCM_BRANCH_FACTOR;
				i = (i / UCM_BRANCH_FACTOR) - 1;
				base = (i / UCM_BRANCH_FACTOR) * UCM_BRANCH_FACTOR;
				num_iterations = 0;
			}

			j = (location / factor) % UCM_BRANCH_FACTOR;
			location = (location / factor) * factor;
			location += ((j + 1) % UCM_BRANCH_FACTOR - j) * factor;
			num_iterations++;
		}
	}
}

OInMemoryBlkno
ucm_occupy_free_page(map: &mut UsageCountMap)
{
	pub static mut LOCATION: int64 = std::mem::zeroed();
	pub static mut I: int64 = std::mem::zeroed();
	int64		factor,
				base;
	pub static mut NUM_ITERATIONS: int64 = std::mem::zeroed();
	pub static mut MASK: uint32 = std::mem::zeroed();

	mask = UCM_LEVEL_MASK << (UCM_FREE_PAGES_LEVEL * UCM_LEVEL_BITS);
	location = 0;
	factor = map->rootFactor;
	base = 0;
	num_iterations = 0;
	while (true)
	{
		Assert(factor > 0);

		i = base + (location / factor) % UCM_BRANCH_FACTOR;

		if (factor == 1 && location < map->size)
		{
			// Work with pages themselves
			pub static mut BLKNO: OInMemoryBlkno = location + map->offset;
			header: &mut OrioleDBPageHeader = (OrioleDBPageHeader *) O_GET_IN_MEMORY_PAGE(blkno);
			pub static mut STATE: uint64 = std::mem::zeroed();

			state = pg_atomic_read_u64(&header->state);
			if (O_PAGE_STATE_GET_USAGE_COUNT(state) == UCM_FREE_PAGES_LEVEL &&
				page_try_change_usage_count(map, blkno,
											state, UCM_INVALID_LEVEL))
			{
				pub static mut BLKNO: return = std::mem::zeroed();
			}
		}

		if (i < map->total && (pg_atomic_read_u32(&map->ucm[i]) & mask))
		{
			// Required usage counts should be here, so step into
			base = (i + 1) * UCM_BRANCH_FACTOR;
			factor /= UCM_BRANCH_FACTOR;
			num_iterations = 0;
		}
		else
		{
			// Not found, so step over
			pub static mut J: int64 = std::mem::zeroed();

			if (num_iterations > 2 * UCM_BRANCH_FACTOR && base != 0)
			{
				//
// Made two rounds and didn't found required usage counts.  So
// give up and retry at upper level.
//
				factor *= UCM_BRANCH_FACTOR;
				i = (i / UCM_BRANCH_FACTOR) - 1;
				base = (i / UCM_BRANCH_FACTOR) * UCM_BRANCH_FACTOR;
				num_iterations = 0;
			}

			j = (location / factor) % UCM_BRANCH_FACTOR;
			location = (location / factor) * factor;
			location += ((j + 1) % UCM_BRANCH_FACTOR - j) * factor;
			num_iterations++;
		}
	}
}