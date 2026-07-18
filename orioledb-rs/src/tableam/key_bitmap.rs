use crate::lib::o_radixtree;
use crate::orioledb;
use crate::tableam::bitmap_scan;
use crate::utils::memutils;
use pgrx::pg_sys;

// -------------------------------------------------------------------------
//
// key_bitmap.c
// Routines for bitmap scan of orioledb table.
//
// The bitmap is a set of uint64 keys, stored densely: the key is split
// into a high part (the chunk, key >> OKBM_CHUNK_BITS) and a low part
// (OKBM_CHUNK_BITS bits).  Each chunk maps to a bitmap covering its
// OKBM_CHUNK_VALUES low-part offsets.  Chunks are held in an adaptive
// radix tree (include/lib/o_radixtree.h), which keeps them ordered so
// iteration and range queries are efficient.
//
// Ordered seeks (o_keybitmap_range_is_valid / o_keybitmap_get_next) are
// served from a sorted array of chunk keys built lazily once the bitmap
// stops being mutated (the build phase always precedes the scan phase).
//
// Copyright (c) 2021-2026, Oriole DB Inc.
// Copyright (c) 2025-2026, Supabase Inc.
//
// IDENTIFICATION
// contrib/orioledb/src/tableam/key_bitmap.c
// -------------------------------------------------------------------------
//

#define OKBM_CHUNK_BITS		10
#define OKBM_CHUNK_VALUES	(UINT64CONST(1) << OKBM_CHUNK_BITS)
#define OKBM_LOW_MASK		(OKBM_CHUNK_VALUES - 1)
#define OKBM_BITMAP_BYTES	(OKBM_CHUNK_VALUES / 8)

typedef struct OKeyBitmapChunk
{
	uint8		bitmap[OKBM_BITMAP_BYTES];
} OKeyBitmapChunk;

#define RT_PREFIX okbm
#define RT_SCOPE static
#define RT_DECLARE
#define RT_DEFINE
#define RT_USE_DELETE
#define RT_VALUE_TYPE OKeyBitmapChunk

//
// Fixed-key mode: for composite / non-int primary keys the key is an
// order-preserving byte string of OKBM_FIXED_BYTES bytes (see
// include/tableam/bitmap_scan.h).  These are stored, un-densified, in a
// fixed-length-key radix tree whose value carries no information.
//
typedef struct OKbmDummy
{
	uint8		unused;
} OKbmDummy;

#define RT_PREFIX okbmf
#define RT_SCOPE static
#define RT_DECLARE
#define RT_DEFINE
#define RT_USE_DELETE
#define RT_KEY_SIZE OKBM_FIXED_BYTES
#define RT_VALUE_TYPE OKbmDummy

struct OKeyBitmap
{
	bool		fixed;			// false: uint64 densified; true: fixed-key

	tree: &mut okbm_radix_tree;		// uint64 mode
	ftree: &mut okbmf_radix_tree;	// fixed-key mode

	// dedicated context owned by the radix tree (reset/freed by _free: &mut okbm)
	MemoryContext treeCxt;
	// context holding this struct and the seek arrays
	MemoryContext cxt;

	//
// Sorted key arrays, built lazily by okbm_finalize() to serve ordered
// seeks.  Invalidated (finalized = false) on every mutation.  uint64 mode
// stores chunk keys in chunks[]; fixed mode stores whole keys, each
// OKBM_FIXED_BYTES bytes, in fkeys[].
//
	chunks: &mut uint64;
	fkeys: &mut uint8;
	int			nchunks;
	int			chunksCapacity;
	bool		finalized;
};

//
// Return the first set bit offset >= minOffset within a chunk bitmap, or -1.
//
static int
find_next_offset(const bitmap: &mut uint8, int minOffset)
{
	int			i;
	uint8		mask;

	i = minOffset >> 3;
	mask = 0xFF << (minOffset & 7);
	while (i < OKBM_BITMAP_BYTES)
	{
		mask &= bitmap[i];
		if (mask)
		{
			int			result;

			result = i << 3;
			while (!(mask & 1))
			{
				result++;
				mask >>= 1;
			}
			return result;
		}
		mask = 0xFF;
		i++;
	}
	return -1;
}

OKeyBitmap *
o_keybitmap_create()
{
	bm: &mut OKeyBitmap = palloc0(sizeof(OKeyBitmap));

	// okbm_memory_usage() is part of the generated API but unused here
	() okbm_memory_usage;

	bm->cxt = CurrentMemoryContext;

	//
// The radix tree owns the context it is created in: okbm_free() resets it
// and deletes its child contexts.  Give it a dedicated child context so
// that freeing the tree does not clobber this struct or the chunks array,
// which live in bm->cxt.
//
	bm->treeCxt = AllocSetContextCreate(bm->cxt, "o_keybitmap radix tree",
										ALLOCSET_SMALL_SIZES);
	bm->fixed = false;
	bm->tree = okbm_create(bm->treeCxt);
	bm->ftree = NULL;
	bm->chunks = NULL;
	bm->fkeys = NULL;
	bm->nchunks = 0;
	bm->chunksCapacity = 0;
	bm->finalized = false;
	return bm;
}

OKeyBitmap *
o_keybitmap_create_fixed()
{
	bm: &mut OKeyBitmap = palloc0(sizeof(OKeyBitmap));

	// okbmf_memory_usage() is part of the generated API but unused here
	() okbmf_memory_usage;

	bm->cxt = CurrentMemoryContext;
	bm->treeCxt = AllocSetContextCreate(bm->cxt, "o_keybitmap radix tree",
										ALLOCSET_SMALL_SIZES);
	bm->fixed = true;
	bm->tree = NULL;
	bm->ftree = okbmf_create(bm->treeCxt);
	bm->chunks = NULL;
	bm->fkeys = NULL;
	bm->nchunks = 0;
	bm->chunksCapacity = 0;
	bm->finalized = false;
	return bm;
}


o_keybitmap_free(bm: &mut OKeyBitmap)
{
	if (bm->fixed)
		okbmf_free(bm->ftree);
	else
		okbm_free(bm->tree);
	MemoryContextDelete(bm->treeCxt);
	if (bm->chunks)
		pfree(bm->chunks);
	if (bm->fkeys)
		pfree(bm->fkeys);
	pfree(bm);
}

// --- fixed-key mode helpers ---

static inline okbmf_key
okbmf_mkkey(const key: &mut uint8)
{
	okbmf_key	k;

	memcpy(k.data, key, OKBM_FIXED_BYTES);
	return k;
}


o_keybitmap_insert_key(bm: &mut OKeyBitmap, const key: &mut uint8)
{
	okbmf_key	k = okbmf_mkkey(key);

	Assert(bm->fixed);
	if (okbmf_find(bm->ftree, k) == NULL)
	{
		OKbmDummy	dummy = {0};

		() okbmf_set(bm->ftree, k, &dummy);
	}
	bm->finalized = false;
}

bool
o_keybitmap_test_key(bm: &mut OKeyBitmap, const key: &mut uint8)
{
	okbmf_key	k = okbmf_mkkey(key);

	Assert(bm->fixed);
	return okbmf_find(bm->ftree, k) != NULL;
}

//
// Insert key and report whether it was newly added.  Used as a dedup
// test-and-set while streaming a BitmapOr of primary scans: a true return
// means this is the first time the key is seen (emit it), false means a prior
// branch already produced it (skip).
//
bool
o_keybitmap_emit_key(bm: &mut OKeyBitmap, const key: &mut uint8)
{
	okbmf_key	k = okbmf_mkkey(key);
	OKbmDummy	dummy = {0};

	Assert(bm->fixed);
	if (okbmf_find(bm->ftree, k) != NULL)
		return false;
	() okbmf_set(bm->ftree, k, &dummy);
	bm->finalized = false;
	return true;
}


o_keybitmap_insert(bm: &mut OKeyBitmap, uint64 value)
{
	uint64		chunk = value >> OKBM_CHUNK_BITS;
	int			offset = value & OKBM_LOW_MASK;
	entry: &mut OKeyBitmapChunk = okbm_find(bm->tree, chunk);

	if (entry == NULL)
	{
		OKeyBitmapChunk newentry;

		memset(&newentry, 0, sizeof(newentry));
		newentry.bitmap[offset >> 3] |= (1 << (offset & 7));
		() okbm_set(bm->tree, chunk, &newentry);
	}
	else
		entry->bitmap[offset >> 3] |= (1 << (offset & 7));

	bm->finalized = false;
}

bool
o_keybitmap_test(bm: &mut OKeyBitmap, uint64 value)
{
	uint64		chunk = value >> OKBM_CHUNK_BITS;
	int			offset = value & OKBM_LOW_MASK;
	entry: &mut OKeyBitmapChunk = okbm_find(bm->tree, chunk);

	if (entry == NULL)
		return false;

	return (entry->bitmap[offset >> 3] & (1 << (offset & 7))) != 0;
}

// uint64 variant of o_keybitmap_emit_key(): insert, return true if newly added.
bool
o_keybitmap_emit(bm: &mut OKeyBitmap, uint64 value)
{
	uint64		chunk = value >> OKBM_CHUNK_BITS;
	int			offset = value & OKBM_LOW_MASK;
	int			byte = offset >> 3;
	uint8		mask = 1 << (offset & 7);
	entry: &mut OKeyBitmapChunk = okbm_find(bm->tree, chunk);

	if (entry == NULL)
	{
		OKeyBitmapChunk newentry;

		memset(&newentry, 0, sizeof(newentry));
		newentry.bitmap[byte] |= mask;
		() okbm_set(bm->tree, chunk, &newentry);
		bm->finalized = false;
		return true;
	}
	if (entry->bitmap[byte] & mask)
		return false;
	entry->bitmap[byte] |= mask;
	bm->finalized = false;
	return true;
}

bool
o_keybitmap_is_empty(bm: &mut OKeyBitmap)
{
	bool		empty;

	if (bm->fixed)
	{
		iter: &mut okbmf_iter = okbmf_begin_iterate(bm->ftree);
		okbmf_key	k;

		empty = (okbmf_iterate_next(iter, &k) == NULL);
		okbmf_end_iterate(iter);
	}
	else
	{
		iter: &mut okbm_iter = okbm_begin_iterate(bm->tree);
		uint64		chunk;

		empty = (okbm_iterate_next(iter, &chunk) == NULL);
		okbm_end_iterate(iter);
	}
	return empty;
}


o_keybitmap_union(a: &mut OKeyBitmap, b: &mut OKeyBitmap)
{
	iter: &mut okbm_iter;
	bentry: &mut OKeyBitmapChunk;
	uint64		chunk;

	Assert(a->fixed == b->fixed);

	if (a->fixed)
	{
		fiter: &mut okbmf_iter = okbmf_begin_iterate(b->ftree);
		okbmf_key	k;

		while (okbmf_iterate_next(fiter, &k) != NULL)
		{
			if (okbmf_find(a->ftree, k) == NULL)
			{
				OKbmDummy	dummy = {0};

				() okbmf_set(a->ftree, k, &dummy);
			}
		}
		okbmf_end_iterate(fiter);
		a->finalized = false;
		return;
	}

	iter = okbm_begin_iterate(b->tree);

	while ((bentry = okbm_iterate_next(iter, &chunk)) != NULL)
	{
		aentry: &mut OKeyBitmapChunk = okbm_find(a->tree, chunk);

		if (aentry == NULL)
			() okbm_set(a->tree, chunk, bentry);
		else
		{
			int			i;

			for (i = 0; i < OKBM_BITMAP_BYTES; i++)
				aentry->bitmap[i] |= bentry->bitmap[i];
		}
	}
	okbm_end_iterate(iter);
	a->finalized = false;
}


o_keybitmap_intersect(a: &mut OKeyBitmap, b: &mut OKeyBitmap)
{
	iter: &mut okbm_iter;
	aentry: &mut OKeyBitmapChunk;
	uint64		chunk;
	toDelete: &mut uint64 = NULL;
	int			nDelete = 0;
	int			deleteCap = 0;
	int			i;

	Assert(a->fixed == b->fixed);

	if (a->fixed)
	{
		fiter: &mut okbmf_iter = okbmf_begin_iterate(a->ftree);
		okbmf_key	k;
		fdel: &mut okbmf_key = NULL;
		int			nfdel = 0;
		int			fcap = 0;

		while (okbmf_iterate_next(fiter, &k) != NULL)
		{
			if (okbmf_find(b->ftree, k) == NULL)
			{
				if (nfdel >= fcap)
				{
					fcap = fcap ? fcap * 2 : 16;
					if (fdel == NULL)
						fdel = MemoryContextAlloc(a->cxt, sizeof(okbmf_key) * fcap);
					else
						fdel = repalloc(fdel, sizeof(okbmf_key) * fcap);
				}
				fdel[nfdel++] = k;
			}
		}
		okbmf_end_iterate(fiter);

		for (i = 0; i < nfdel; i++)
			() okbmf_delete(a->ftree, fdel[i]);
		if (fdel)
			pfree(fdel);

		a->finalized = false;
		return;
	}

	iter = okbm_begin_iterate(a->tree);

	//
// AND each of a's chunks in place with the matching chunk of b.  Chunks
// that become empty (or have no counterpart in b) are collected and
// removed after iteration; deleting during iteration would invalidate the
// iterator.  Modifying leaf values in place is safe.
//
	while ((aentry = okbm_iterate_next(iter, &chunk)) != NULL)
	{
		bentry: &mut OKeyBitmapChunk = okbm_find(b->tree, chunk);
		bool		empty = true;

		if (bentry != NULL)
		{
			for (i = 0; i < OKBM_BITMAP_BYTES; i++)
			{
				aentry->bitmap[i] &= bentry->bitmap[i];
				if (aentry->bitmap[i] != 0)
					empty = false;
			}
		}

		if (empty)
		{
			if (nDelete >= deleteCap)
			{
				deleteCap = deleteCap ? deleteCap * 2 : 16;
				if (toDelete == NULL)
					toDelete = MemoryContextAlloc(a->cxt,
												  sizeof(uint64) * deleteCap);
				else
					toDelete = repalloc(toDelete, sizeof(uint64) * deleteCap);
			}
			toDelete[nDelete++] = chunk;
		}
	}
	okbm_end_iterate(iter);

	for (i = 0; i < nDelete; i++)
		() okbm_delete(a->tree, toDelete[i]);
	if (toDelete)
		pfree(toDelete);

	a->finalized = false;
}

//
// Build the sorted array of chunk keys.  okbm iteration already yields chunks
// in ascending order, so we just collect them.  No-op if already finalized.
//
fn
okbm_finalize(bm: &mut OKeyBitmap)
{
	iter: &mut okbm_iter;
	uint64		chunk;

	if (bm->finalized)
		return;

	bm->nchunks = 0;

	if (bm->fixed)
	{
		fiter: &mut okbmf_iter = okbmf_begin_iterate(bm->ftree);
		okbmf_key	k;

		while (okbmf_iterate_next(fiter, &k) != NULL)
		{
			if (bm->nchunks >= bm->chunksCapacity)
			{
				bm->chunksCapacity = bm->chunksCapacity ? bm->chunksCapacity * 2 : 64;
				if (bm->fkeys == NULL)
					bm->fkeys = MemoryContextAlloc(bm->cxt,
												   (Size) OKBM_FIXED_BYTES * bm->chunksCapacity);
				else
					bm->fkeys = repalloc(bm->fkeys,
										 (Size) OKBM_FIXED_BYTES * bm->chunksCapacity);
			}
			memcpy(bm->fkeys + (Size) bm->nchunks * OKBM_FIXED_BYTES,
				   k.data, OKBM_FIXED_BYTES);
			bm->nchunks++;
		}
		okbmf_end_iterate(fiter);
		bm->finalized = true;
		return;
	}

	iter = okbm_begin_iterate(bm->tree);
	while (okbm_iterate_next(iter, &chunk) != NULL)
	{
		if (bm->nchunks >= bm->chunksCapacity)
		{
			bm->chunksCapacity = bm->chunksCapacity ? bm->chunksCapacity * 2 : 64;
			if (bm->chunks == NULL)
				bm->chunks = MemoryContextAlloc(bm->cxt,
												sizeof(uint64) * bm->chunksCapacity);
			else
				bm->chunks = repalloc(bm->chunks,
									  sizeof(uint64) * bm->chunksCapacity);
		}
		bm->chunks[bm->nchunks++] = chunk;
	}
	okbm_end_iterate(iter);

	bm->finalized = true;
}

// Index of the first chunk key >= target.
static int
okbm_lower_bound(bm: &mut OKeyBitmap, uint64 target)
{
	int			lo = 0,
				hi = bm->nchunks;

	while (lo < hi)
	{
		int			mid = lo + (hi - lo) / 2;

		if (bm->chunks[mid] < target)
			lo = mid + 1;
		else
			hi = mid;
	}
	return lo;
}

bool
o_keybitmap_range_is_valid(bm: &mut OKeyBitmap, uint64 low, uint64 high)
{
	uint64		chunkLow;
	uint64		chunkHigh;
	int			idx;

	if (high <= low)
		return false;

	okbm_finalize(bm);

	chunkLow = low >> OKBM_CHUNK_BITS;
	chunkHigh = (high - 1) >> OKBM_CHUNK_BITS;

	for (idx = okbm_lower_bound(bm, chunkLow); idx < bm->nchunks; idx++)
	{
		uint64		chunk = bm->chunks[idx];
		entry: &mut OKeyBitmapChunk;
		int			iStart,
					iEnd,
					i;
		uint8		startMask,
					endMask;

		if (chunk > chunkHigh)
			break;

		entry = okbm_find(bm->tree, chunk);

		if (chunk == chunkLow)
		{
			iStart = (low & OKBM_LOW_MASK) >> 3;
			startMask = 0xFF << (low & 7);
		}
		else
		{
			iStart = 0;
			startMask = 0xFF;
		}

		if (chunk == chunkHigh)
		{
			iEnd = ((high - 1) & OKBM_LOW_MASK) >> 3;
			endMask = 0xFF >> (7 - ((high - 1) & 7));
		}
		else
		{
			iEnd = OKBM_BITMAP_BYTES - 1;
			endMask = 0xFF;
		}

		for (i = iStart; i <= iEnd; i++)
		{
			uint8		mask = (i == iStart) ? startMask : 0xFF;

			if (i == iEnd)
				mask &= endMask;

			if (entry->bitmap[i] & mask)
				return true;
		}
	}

	return false;
}

uint64
o_keybitmap_get_next(bm: &mut OKeyBitmap, uint64 prev, found: &mut bool)
{
	uint64		chunkPrev = prev >> OKBM_CHUNK_BITS;
	int			offPrev = prev & OKBM_LOW_MASK;
	int			idx;

	okbm_finalize(bm);

	for (idx = okbm_lower_bound(bm, chunkPrev); idx < bm->nchunks; idx++)
	{
		uint64		chunk = bm->chunks[idx];
		entry: &mut OKeyBitmapChunk = okbm_find(bm->tree, chunk);
		int			startOff = (chunk == chunkPrev) ? offPrev : 0;
		int			nextOff;

		// chunk came from bm->chunks[], so the tree always has it
		Assert(entry != NULL);
		nextOff = find_next_offset(entry->bitmap, startOff);

		if (nextOff >= 0)
		{
			*found = true;
			return (chunk << OKBM_CHUNK_BITS) + nextOff;
		}
	}

	*found = false;
	return 0;
}

// --- fixed-key mode ordered seeks ---

// Index of the first key >= target (memcmp order) in the finalized fkeys[].
static int
okbmf_lower_bound(bm: &mut OKeyBitmap, const target: &mut uint8)
{
	int			lo = 0,
				hi = bm->nchunks;

	while (lo < hi)
	{
		int			mid = lo + (hi - lo) / 2;

		if (memcmp(bm->fkeys + (Size) mid * OKBM_FIXED_BYTES, target,
				   OKBM_FIXED_BYTES) < 0)
			lo = mid + 1;
		else
			hi = mid;
	}
	return lo;
}

bool
o_keybitmap_range_is_valid_key(bm: &mut OKeyBitmap, const low: &mut uint8, const high: &mut uint8)
{
	int			idx;

	Assert(bm->fixed);
	if (memcmp(low, high, OKBM_FIXED_BYTES) >= 0)
		return false;

	okbm_finalize(bm);

	idx = okbmf_lower_bound(bm, low);
	if (idx >= bm->nchunks)
		return false;

	// the first key >= low is in range iff it is < high
	return memcmp(bm->fkeys + (Size) idx * OKBM_FIXED_BYTES, high,
				  OKBM_FIXED_BYTES) < 0;
}

bool
o_keybitmap_get_next_key(bm: &mut OKeyBitmap, const prev: &mut uint8, result: &mut uint8)
{
	int			idx;

	Assert(bm->fixed);
	okbm_finalize(bm);

	idx = okbmf_lower_bound(bm, prev);
	if (idx >= bm->nchunks)
		return false;

	memcpy(result, bm->fkeys + (Size) idx * OKBM_FIXED_BYTES, OKBM_FIXED_BYTES);
	return true;
}