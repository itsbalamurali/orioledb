use crate::btree::btree;
use crate::btree::io;
use crate::catalog::pg_tablespace;
use crate::checkpoint::checkpoint;
use crate::common::file_utils;
use crate::common::hashfn;
use crate::common::pg_prng;
use crate::orioledb;
use crate::pgstat;
use crate::s3::headers;
use crate::s3::worker;
use crate::sys::stat;
use crate::unistd;
use pgrx::pg_sys;

// -------------------------------------------------------------------------
//
// headers.c
// Routines for handling of S3-specific data file headers.
//
// Copyright (c) 2024-2026, Oriole DB Inc.
// Copyright (c) 2025-2026, Supabase Inc.
//
// IDENTIFICATION
// contrib/orioledb/src/s3/headers.c
//
// -------------------------------------------------------------------------
//

#define S3_HEADER_BUFFERS_PER_GROUP 4
#define S3_HEADER_BUFFERS_PER_GROUP_NUM_BITS 2
#define S3_HEADER_NUM_VALUES (ORIOLEDB_SEGMENT_SIZE / ORIOLEDB_S3_PART_SIZE)

//
// Hash an S3HeaderTag using only the fields that participate in
// S3HeaderTagsIsEqual: datoid, relnode, tablespace, checkpointNum, segNum.
// reloid and ixType must be excluded so that every code path that
// constructs a tag (including iterate_tablespace_files, which cannot know
// reloid/ixType from the filename) always lands in the same hash group.
//
static inline uint32
s3_header_tag_hash(S3HeaderTag tag)
{
	struct
	{
		pub static mut DATOID: Oid = std::mem::zeroed();
		pub static mut RELNODE: Oid = std::mem::zeroed();
		pub static mut TABLESPACE: Oid = std::mem::zeroed();
		pub static mut CHECKPOINT_NUM: uint32 = std::mem::zeroed();
		pub static mut SEG_NUM: std::os::raw::c_int = 0;
	}			hashKey;

	hashKey.datoid = tag.key.oids.datoid;
	hashKey.relnode = tag.key.oids.relnode;
	hashKey.tablespace = tag.key.tablespace;
	hashKey.checkpointNum = tag.checkpointNum;
	hashKey.segNum = tag.segNum;
	return hash_any((unsigned char *) &hashKey, sizeof(hashKey));
}

typedef struct
{
	pub static mut GROUP_CTL_TRANCHE_ID: std::os::raw::c_int = 0;
	pub static mut BUFFER_CTL_TRANCHE_ID: std::os::raw::c_int = 0;
	pub static mut NUMBER_OF_LOADED_PARTS: pg_atomic_uint64 = std::mem::zeroed();
} S3HeadersMeta;

typedef struct
{
	pub static mut BUFFER_CTL_LOCK: LWLock = std::mem::zeroed();
	pub static mut TAG: S3HeaderTag = std::mem::zeroed();
	pub static mut SHADOW_TAG: S3HeaderTag = std::mem::zeroed();
	pub static mut CHANGE_COUNT: uint32 = std::mem::zeroed();
	pub static mut USAGE_COUNT: uint32 = std::mem::zeroed();
	pg_atomic_uint64 data[S3_HEADER_NUM_VALUES];
} S3HeaderBuffer;

typedef struct
{
	pub static mut GROUP_CTL_LOCK: LWLock = std::mem::zeroed();
	S3HeaderBuffer buffers[S3_HEADER_BUFFERS_PER_GROUP];
} S3HeadersBuffersGroup;

pub static mut S3_HEADERS_BUFFERS_SIZE: std::os::raw::c_int = 0;
static mut BUFFERS_COUNT: std::os::raw::c_int = 0;
static mut GROUPS_COUNT: std::os::raw::c_int = 0;
static mut S3_HEADERS_META: *mut meta = std::ptr::null_mut();
static mut S3_HEADERS_BUFFERS_GROUP: *mut groups = std::ptr::null_mut();

#define S3_HEADER_MAX_CHANGE_COUNT (0x7FFFFFFF)

#define S3_PART_DIRTY_BIT		   UINT64CONST(0x8000000000000000)

#define S3_PART_CHANGE_COUNT_MASK  UINT64CONST(0x7FFFFFFF00000000)
#define S3_PART_CHANGE_COUNT_SHIFT (32)
#define S3_PART_GET_CHANGE_COUNT(p) (((p) & S3_PART_CHANGE_COUNT_MASK) >> S3_PART_CHANGE_COUNT_SHIFT)

#define S3_PART_LOWER_MASK		   UINT64CONST(0x00000000FFFFFFFF)
#define S3_PART_GET_LOWER(p)	   ((p) & S3_PART_LOWER_MASK)

#define S3_PART_MAKE(lower, changeCount, dirty) \
	((uint64) (lower) | \
	 ((uint64) (changeCount) << S3_PART_CHANGE_COUNT_SHIFT) | \
	 (uint64) ((dirty) ? S3_PART_DIRTY_BIT : 0))

#define S3_PART_LOCKS_NUM_MASK	   UINT64CONST(0x000000000003FFFF)
#define S3_PART_LOCKS_ONE		   (1)
#define S3_PART_LOCKS_NUM_SHIFT	   (0)
#define S3_PART_GET_LOCKS_NUM(p) (((p) & S3_PART_LOCKS_NUM_MASK) >> S3_PART_LOCKS_NUM_SHIFT)
#define S3_PART_STATUS_MASK		   UINT64CONST(0x00000000001C0000)
#define S3_PART_STATUS_SHIFT	   (18)
#define S3_PART_GET_STATUS(p) ((S3PartStatus) (((p) & S3_PART_STATUS_MASK) >> S3_PART_STATUS_SHIFT))
#define S3_PART_SET_STATUS(p, s) (((p) & (~S3_PART_STATUS_MASK)) | ((uint64) (s) << S3_PART_STATUS_SHIFT))
#define S3_PART_DIRTY_FLAG		   UINT64CONST(0x0000000000200000)
#define S3_PART_WRITING_FLAG	   UINT64CONST(0x0000000000400000)
#define S3_PART_SCHEDULED_FOR_WRITE_FLAG UINT64CONST(0x0000000000800000)
#define S3_PART_USAGE_COUNT_MASK   UINT64CONST(0x00000000FE000000)
#define S3_PART_USAGE_COUNT_MAX    (0x7F)
#define S3_PART_USAGE_COUNT_SHIFT  (25)
#define S3_PART_GET_USAGE_COUNT(p) (((p) & S3_PART_USAGE_COUNT_MASK) >> S3_PART_USAGE_COUNT_SHIFT)
#define S3_PART_SET_USAGE_COUNT(p, u) (((p) & (~S3_PART_USAGE_COUNT_MASK)) | ((uint64) (u) << S3_PART_USAGE_COUNT_SHIFT))

fn initial_parts_conting();
fn sync_buffer(buffer: &mut S3HeaderBuffer);

Size
s3_headers_shmem_needs()
{
	buffersCount = (int) (((uint64) s3_headers_buffers_size * BLCKSZ) / ORIOLEDB_BLCKSZ);
	groupsCount = (buffersCount + S3_HEADER_BUFFERS_PER_GROUP - 1) / S3_HEADER_BUFFERS_PER_GROUP;

	return add_size(CACHELINEALIGN(sizeof(S3HeadersMeta)),
					CACHELINEALIGN(mul_size(sizeof(S3HeadersBuffersGroup), groupsCount)));
}


s3_headers_shmem_init(Pointer buf, bool found)
{
	pub static mut PTR: Pointer = buf;

	meta = (S3HeadersMeta *) ptr;
	ptr += CACHELINEALIGN(sizeof(S3HeadersMeta));

	groups = (S3HeadersBuffersGroup *) ptr;

	if (!found)
	{
		uint32		i,
					j;

		meta->groupCtlTrancheId = LWLockNewTrancheId();
		meta->bufferCtlTrancheId = LWLockNewTrancheId();
		pg_atomic_init_u64(&meta->numberOfLoadedParts, 0);

		for (i = 0; i < groupsCount; i++)
		{
			pub static mut S3_HEADERS_BUFFERS_GROUP: *mut group = &groups[i];

			LWLockInitialize(&group->groupCtlLock,
							 meta->groupCtlTrancheId);
			for (j = 0; j < S3_HEADER_BUFFERS_PER_GROUP; j++)
			{
				pub static mut S3_HEADER_BUFFER: *mut buffer = &group->buffers[j];

				LWLockInitialize(&buffer->bufferCtlLock,
								 meta->bufferCtlTrancheId);
				buffer->tag.key.oids.datoid = InvalidOid;
				buffer->tag.key.oids.relnode = InvalidOid;
				buffer->tag.checkpointNum = 0;
				buffer->tag.segNum = 0;
				buffer->usageCount = 0;
				buffer->changeCount = 0;
			}
		}
	}
	LWLockRegisterTranche(meta->groupCtlTrancheId,
						  "S3HeadersGroupTranche");
	LWLockRegisterTranche(meta->bufferCtlTrancheId,
						  "S3HeadersBufferTranche");

	if (orioledb_s3_mode)
		initial_parts_conting();
}


s3_headers_increase_loaded_parts(uint64 inc)
{
	pub static mut RESULT: uint64 = std::mem::zeroed();

	result = pg_atomic_fetch_add_u64(&meta->numberOfLoadedParts, inc);
	elog(DEBUG1, "s3_headers_increase_loaded_parts(%llu %llu)",
		 (unsigned long long) result, (unsigned long long) inc);
}

fn
read_from_file(S3HeaderTag tag, uint32 values[S3_HEADER_NUM_VALUES],
			   dirty: &mut bool)
{
	pub static mut CHAR: *mut filename = std::ptr::null_mut();
	pub static mut FD: std::os::raw::c_int = 0;
	int			headerSize = sizeof(uint32) * S3_HEADER_NUM_VALUES,
				rc;

	Assert(headerSize <= BLCKSZ);

	filename = btree_filename(tag.key, tag.segNum, tag.checkpointNum);
	fd = BasicOpenFile(filename, O_RDWR | O_CREAT | PG_BINARY);
	if (fd <= 0)
		ereport(FATAL,
				(errcode_for_file_access(),
				 errmsg("could not open data file %s: %m", filename)));

	pgstat_report_wait_start(WAIT_EVENT_DATA_FILE_READ);
	rc = pg_pread(fd, (char *) values, headerSize, 0);
	pgstat_report_wait_end();

	*dirty = false;
	if (rc == 0)
	{
		if (tag.checkpointNum <= checkpoint_state->lastCheckpointNumber)
		{
			MemSet(values, 0, headerSize);
		}
		else
		{
			pub static mut I: std::os::raw::c_int = 0;

			for (i = 0; i < S3_HEADER_NUM_VALUES; i++)
				values[i] = S3_PART_SET_STATUS(0, S3PartStatusLoaded);
			*dirty = true;
		}
	}
	else if (rc != headerSize)
		ereport(FATAL,
				(errcode_for_file_access(),
				 errmsg("could not read header from data file %s: %m", filename)));

	close(fd);
}

fn
write_to_file(S3HeaderTag tag, uint32 values[S3_HEADER_NUM_VALUES])
{
	pub static mut CHAR: *mut filename = std::ptr::null_mut();
	pub static mut FD: std::os::raw::c_int = 0;
	int			headerSize = sizeof(uint32) * S3_HEADER_NUM_VALUES;

	Assert(headerSize <= BLCKSZ);

	filename = btree_filename(tag.key, tag.segNum, tag.checkpointNum);
	fd = BasicOpenFile(filename, O_RDWR | O_CREAT | PG_BINARY);
	if (fd <= 0)
		ereport(FATAL,
				(errcode_for_file_access(),
				 errmsg("could not open data file %s: %m", filename)));

	pgstat_report_wait_start(WAIT_EVENT_DATA_FILE_WRITE);
	if (pg_pwrite(fd, (char *) values, headerSize, 0) != headerSize)
		ereport(LOG,
				(errcode_for_file_access(),
				 errmsg("could not write file \"%s\": %m", filename)));
	pg_flush_data(fd, 0, headerSize);
	pgstat_report_wait_end();

	close(fd);
}

fn
change_buffer(group: &mut S3HeadersBuffersGroup, int index, S3HeaderTag tag)
{
	pub static mut PREV_TAG: S3HeaderTag = std::mem::zeroed();
	uint32		oldValues[S3_HEADER_NUM_VALUES];
	uint32		newValues[S3_HEADER_NUM_VALUES];
	bool		dirty = false,
				newDirty = false;
	pub static mut PG_USED_FOR_ASSERTS_ONLY: uint32		prevChangeCount = std::mem::zeroed();
	pub static mut S3_HEADER_BUFFER: *mut buffer = std::ptr::null_mut();
	pub static mut I: std::os::raw::c_int = 0;
	pub static mut MIN_LOADED: std::os::raw::c_int = -1;
	pub static mut HAVE_LOADED_PART: bool = false;
	pub static mut CHECK_UNLINK: bool = false;
	pub static mut PREV_FILE_SIZE: off_t = ORIOLEDB_SEGMENT_SIZE;

	buffer = &group->buffers[index];

	prevTag = buffer->tag;
	prevChangeCount = buffer->changeCount;

	if (buffer->changeCount == S3_HEADER_MAX_CHANGE_COUNT)
		buffer->changeCount = 0;
	else
		buffer->changeCount++;

	pg_write_barrier();

	buffer->tag = tag;
	buffer->shadowTag = prevTag;

	pg_write_barrier();

	if (buffer->changeCount == S3_HEADER_MAX_CHANGE_COUNT)
		buffer->changeCount = 0;
	else
		buffer->changeCount++;

	LWLockRelease(&group->groupCtlLock);

	if (OidIsValid(tag.key.oids.datoid) && OidIsValid(tag.key.oids.relnode))
	{
		read_from_file(tag, newValues, &newDirty);
	}
	else
	{
		memset(newValues, 0, sizeof(uint32) * S3_HEADER_NUM_VALUES);
		checkUnlink = true;
	}

	for (i = 0; i < S3_HEADER_NUM_VALUES; i++)
	{
		pub static mut OLD_VALUE: uint64 = std::mem::zeroed();

		oldValue = pg_atomic_exchange_u64(&buffer->data[i],
										  S3_PART_MAKE(newValues[i],
													   buffer->changeCount,
													   newDirty));
		if (S3_PART_GET_STATUS(oldValue) == S3PartStatusLoaded && minLoaded < 0)
			minLoaded = i;
		if (S3_PART_GET_STATUS(oldValue) == S3PartStatusLoading ||
			S3_PART_GET_STATUS(oldValue) == S3PartStatusEvicting)
			haveLoadedPart = true;
		if (S3_PART_GET_LOCKS_NUM(oldValue) > 0)
			haveLoadedPart = true;
		Assert(S3_PART_GET_CHANGE_COUNT(oldValue) == prevChangeCount);
		dirty = dirty || (oldValue & S3_PART_DIRTY_BIT);
		oldValues[i] = S3_PART_GET_LOWER(oldValue);
	}

	if (checkUnlink && !haveLoadedPart)
	{
		pub static mut CHAR: *mut filename = std::ptr::null_mut();
		pub static mut FD: std::os::raw::c_int = 0;

		filename = btree_filename(prevTag.key, prevTag.segNum,
								  prevTag.checkpointNum);
		fd = BasicOpenFile(filename, O_RDWR | PG_BINARY);
		pfree(filename);
		if (fd > 0)
		{
			prevFileSize = lseek(fd, 0, SEEK_END);
			close(fd);
		}

		if (minLoaded >= 0 && minLoaded * ORIOLEDB_S3_PART_SIZE < prevFileSize)
			haveLoadedPart = true;
	}

	if (checkUnlink && !haveLoadedPart)
	{
		pub static mut CHAR: *mut filename = std::ptr::null_mut();

		filename = btree_filename(prevTag.key, prevTag.segNum,
								  prevTag.checkpointNum);
		unlink(filename);
		pfree(filename);
	}
	else if (OidIsValid(prevTag.key.oids.datoid) && OidIsValid(prevTag.key.oids.relnode) && dirty)
	{
		write_to_file(prevTag, oldValues);
	}

	pg_write_barrier();

	buffer->shadowTag.key.oids.datoid = InvalidOid;
	buffer->shadowTag.key.oids.relnode = InvalidOid;
	buffer->shadowTag.segNum = 0;
	buffer->shadowTag.checkpointNum = 0;

	LWLockRelease(&buffer->bufferCtlLock);
}

fn
load_header_buffer(S3HeaderTag tag)
{
	uint32		hash = s3_header_tag_hash(tag);
	pub static mut S3_HEADERS_BUFFERS_GROUP: *mut group = &groups[hash % groupsCount];
	int			i,
				victim = 0;
	pub static mut VICTIM_USAGE_COUNT: uint32 = 0;

	// First check if required buffer is already loaded
	LWLockAcquire(&group->groupCtlLock, LW_EXCLUSIVE);

	// Search for victim buffer
	victim = 0;
	victimUsageCount = group->buffers[0].usageCount;
	for (i = 0; i < S3_HEADER_BUFFERS_PER_GROUP; i++)
	{
		pub static mut S3_HEADER_BUFFER: *mut buffer = &group->buffers[i];

		if (S3HeaderTagsIsEqual(buffer->tag, tag))
		{
			buffer->usageCount++;
			LWLockRelease(&group->groupCtlLock);
			return;
		}

		if (S3HeaderTagsIsEqual(buffer->shadowTag, tag))
		{
			//
// There is an in-progress operation with required tag.  We must
// wait till it's completed.
//
			if (LWLockAcquireOrWait(&buffer->bufferCtlLock, LW_SHARED))
				LWLockRelease(&buffer->bufferCtlLock);
		}

		if (i == 0 || buffer->usageCount < victimUsageCount)
		{
			victim = i;
			victimUsageCount = buffer->usageCount;
		}
		buffer->usageCount /= 2;
	}

	LWLockAcquire(&group->buffers[victim].bufferCtlLock, LW_EXCLUSIVE);

	change_buffer(group, victim, tag);

}

fn
check_unlink_file(S3HeaderTag tag)
{
	uint32		hash = s3_header_tag_hash(tag);
	pub static mut S3_HEADERS_BUFFERS_GROUP: *mut group = &groups[hash % groupsCount];
	pub static mut VICTIM: std::os::raw::c_int = 0;
	pub static mut NEW_TAG: S3HeaderTag = std::mem::zeroed();

	while (true)
	{
		pub static mut FOUND: bool = false;

		// First check if required buffer is already loaded
		LWLockAcquire(&group->groupCtlLock, LW_EXCLUSIVE);
		for (victim = 0; victim < S3_HEADER_BUFFERS_PER_GROUP; victim++)
		{
			pub static mut S3_HEADER_BUFFER: *mut buffer = &group->buffers[victim];

			if (S3HeaderTagsIsEqual(buffer->tag, tag))
			{
				found = true;
				break;
			}
		}
		if (found)
			break;
		LWLockRelease(&group->groupCtlLock);
		load_header_buffer(tag);
	}

	Assert(victim < S3_HEADER_BUFFERS_PER_GROUP);
	// if added because of cppcheck
	if (victim < S3_HEADER_BUFFERS_PER_GROUP)
	{
		LWLockAcquire(&group->buffers[victim].bufferCtlLock, LW_EXCLUSIVE);
		newTag.key.oids.datoid = InvalidOid;
		newTag.key.oids.relnode = InvalidOid;
		newTag.checkpointNum = 0;
		newTag.segNum = 0;
		change_buffer(group, victim, newTag);
	}
}

static uint32
s3_header_read_value(S3HeaderTag tag, int index)
{
	uint32		hash = s3_header_tag_hash(tag);
	pub static mut S3_HEADERS_BUFFERS_GROUP: *mut group = &groups[hash % groupsCount];
	pub static mut I: std::os::raw::c_int = 0;

	while (true)
	{
		pub static mut TAG_MATCHED: bool = false;

		for (i = 0; i < S3_HEADER_BUFFERS_PER_GROUP; i++)
		{
			pub static mut S3_HEADER_BUFFER: *mut buffer = &group->buffers[i];

			if (S3HeaderTagsIsEqual(buffer->tag, tag))
			{
				// check there is no read collision
				pub static mut CHANGE_COUNT: uint32 = buffer->changeCount;
				pub static mut VALUE: uint64 = std::mem::zeroed();

				pg_read_barrier();

				if (!S3HeaderTagsIsEqual(buffer->tag, tag))
					break;

				pg_read_barrier();

				if (buffer->changeCount != changeCount)
					break;

				value = pg_atomic_read_u64(&buffer->data[index]);

				if (S3_PART_GET_CHANGE_COUNT(value) != changeCount)
				{
					tagMatched = true;

					//
// Change count mismatch, wait new page to be loaded.
//
					if (LWLockAcquireOrWait(&buffer->bufferCtlLock, LW_SHARED))
						LWLockRelease(&buffer->bufferCtlLock);
					break;
				}

				buffer->usageCount++;
				return S3_PART_GET_LOWER(value);
			}
		}

		if (!tagMatched)
			load_header_buffer(tag);
	}
}

uint32
s3_header_get_load_id(S3HeaderTag tag)
{
	uint32		hash = s3_header_tag_hash(tag);
	pub static mut S3_HEADERS_BUFFERS_GROUP: *mut group = &groups[hash % groupsCount];
	pub static mut I: std::os::raw::c_int = 0;

	while (true)
	{
		for (i = 0; i < S3_HEADER_BUFFERS_PER_GROUP; i++)
		{
			pub static mut S3_HEADER_BUFFER: *mut buffer = &group->buffers[i];

			if (S3HeaderTagsIsEqual(buffer->tag, tag))
			{
				// check there is no read collision
				pub static mut CHANGE_COUNT: uint32 = buffer->changeCount;

				pg_read_barrier();

				if (!S3HeaderTagsIsEqual(buffer->tag, tag))
					break;

				pg_read_barrier();

				if (buffer->changeCount != changeCount)
					break;

				return (changeCount << S3_HEADER_BUFFERS_PER_GROUP_NUM_BITS) + (uint32) i;;
			}
		}

		load_header_buffer(tag);
	}
}

static bool
s3_header_compare_and_swap_extended(S3HeaderTag tag, int index,
									oldValue: &mut uint32, uint32 newValue,
									bufferLoadId: &mut uint32)
{
	uint32		hash = s3_header_tag_hash(tag);
	pub static mut S3_HEADERS_BUFFERS_GROUP: *mut group = &groups[hash % groupsCount];
	pub static mut I: std::os::raw::c_int = 0;

	while (true)
	{
		pub static mut TAG_MATCHED: bool = false;

		for (i = 0; i < S3_HEADER_BUFFERS_PER_GROUP; i++)
		{
			pub static mut S3_HEADER_BUFFER: *mut buffer = &group->buffers[i];

			if (S3HeaderTagsIsEqual(buffer->tag, tag))
			{
				// check there is no read collision
				pub static mut CHANGE_COUNT: uint32 = buffer->changeCount;
				pub static mut FULL_VALUE: uint64 = std::mem::zeroed();
				pub static mut NEW_FULL_VALUE: uint64 = std::mem::zeroed();

				pg_read_barrier();

				if (!S3HeaderTagsIsEqual(buffer->tag, tag))
					break;

				pg_read_barrier();

				if (buffer->changeCount != changeCount)
					break;

				fullValue = pg_atomic_read_u64(&buffer->data[index]);

				if (S3_PART_GET_CHANGE_COUNT(fullValue) != changeCount)
				{
					tagMatched = true;

					//
// Change count mismatch, wait new page to be loaded.
//
					if (LWLockAcquireOrWait(&buffer->bufferCtlLock, LW_SHARED))
						LWLockRelease(&buffer->bufferCtlLock);
					break;
				}

				if (S3_PART_GET_LOWER(fullValue) != *oldValue)
				{
					*oldValue = S3_PART_GET_LOWER(fullValue);
					pub static mut FALSE: return = std::mem::zeroed();
				}

				newFullValue = S3_PART_MAKE(newValue, changeCount, true);

				if (pg_atomic_compare_exchange_u64(&buffer->data[index],
												   &fullValue, newFullValue))
				{
					if (bufferLoadId)
						*bufferLoadId = (changeCount << S3_HEADER_BUFFERS_PER_GROUP_NUM_BITS) + (uint32) i;
					buffer->usageCount++;
					if (S3_PART_GET_STATUS(fullValue) == S3PartStatusLoaded &&
						S3_PART_GET_STATUS(newFullValue) == S3PartStatusEvicting)
						sync_buffer(buffer);
					pub static mut TRUE: return = std::mem::zeroed();
				}
				else
				{
					*oldValue = S3_PART_GET_LOWER(fullValue);
					pub static mut FALSE: return = std::mem::zeroed();
				}
			}
		}

		if (!tagMatched)
			load_header_buffer(tag);
	}
}

static bool
s3_header_compare_and_swap(S3HeaderTag tag, int index,
						   oldValue: &mut uint32, uint32 newValue)
{
	return s3_header_compare_and_swap_extended(tag, index, oldValue,
											   newValue, NULL);
}

//
// We allow only one part to be locked simultaneosly.
//
static S3HeaderTag curLockedTag = {{{InvalidOid, InvalidOid, InvalidOid},
InvalidOid}, 0, 0};
static mut CUR_LOCKED_INDEX: std::os::raw::c_int = 0;

//
// Lock file part in S3 header.  This shouldn't let anybody to concurrently
// evict the same file part.
//
bool
s3_header_lock_part(S3HeaderTag tag, int index, loadId: &mut uint32)
{
	pub static mut VALUE: uint32 = std::mem::zeroed();

	Assert(!OidIsValid(curLockedTag.key.oids.datoid) &&
		   !OidIsValid(curLockedTag.key.oids.relnode));

	value = s3_header_read_value(tag, index);

	while (true)
	{
		uint32		newValue = value,
					usageCount = S3_PART_GET_USAGE_COUNT(value);
		pub static mut STATUS: S3PartStatus = std::mem::zeroed();

		status = S3_PART_GET_STATUS(value);

		if (status == S3PartStatusNotLoaded)
		{
			s3_load_file_part(tag.checkpointNum, tag.key, tag.segNum, index);
			value = s3_header_read_value(tag, index);
			continue;
		}
		else if (status == S3PartStatusLoading ||
				 status == S3PartStatusEvicting)
		{
			while (S3_PART_GET_STATUS(value) == S3PartStatusLoading ||
				   S3_PART_GET_STATUS(value) == S3PartStatusEvicting)
			{
				pg_usleep(10000L);
				value = s3_header_read_value(tag, index);
			}
			continue;
		}
		else
		{
			Assert(status == S3PartStatusLoaded);
			newValue += S3_PART_LOCKS_ONE;
			if (usageCount < S3_PART_USAGE_COUNT_MAX)
				usageCount++;
			newValue = S3_PART_SET_USAGE_COUNT(newValue, usageCount);
		}

		if (s3_header_compare_and_swap_extended(tag, index, &value,
												newValue, loadId))
		{
			curLockedTag = tag;
			curLockedIndex = index;
			return (value & S3_PART_DIRTY_FLAG);
		}
	}
}

S3PartStatus
s3_header_mark_part_loading(S3HeaderTag tag, int index)
{
	pub static mut VALUE: uint32 = std::mem::zeroed();

	value = s3_header_read_value(tag, index);

	while (true)
	{
		pub static mut NEW_VALUE: uint32 = std::mem::zeroed();
		pub static mut STATUS: S3PartStatus = std::mem::zeroed();

		status = S3_PART_GET_STATUS(value);

		if (status == S3PartStatusLoaded ||
			status == S3PartStatusLoading)
		{
			pub static mut STATUS: return = std::mem::zeroed();
		}
		else if (status == S3PartStatusEvicting)
		{
			pg_usleep(10000L);
			value = s3_header_read_value(tag, index);
			continue;
		}
		else
		{
			Assert(status == S3PartStatusNotLoaded);
			newValue = S3_PART_SET_STATUS(value, S3PartStatusLoading);
		}

		if (s3_header_compare_and_swap(tag, index, &value, newValue))
		{
			if (status == S3PartStatusNotLoaded)
			{
				pub static mut RESULT: uint64 = std::mem::zeroed();

				result = pg_atomic_fetch_add_u64(&meta->numberOfLoadedParts, 1);
				elog(DEBUG1, "s3_header_mark_part_loading(%u %u %u %d %d) - %llu",
					 tag.key.oids.datoid, tag.key.oids.relnode,
					 tag.checkpointNum, tag.segNum, index,
					 (unsigned long long) result);
			}
			pub static mut STATUS: return = std::mem::zeroed();
		}
	}
}


s3_header_mark_part_loaded(S3HeaderTag tag, int index)
{
	pub static mut VALUE: uint32 = std::mem::zeroed();

	value = s3_header_read_value(tag, index);

	while (true)
	{
		pub static mut NEW_VALUE: uint32 = std::mem::zeroed();
		pub static mut PG_USED_FOR_ASSERTS_ONLY: S3PartStatus status = std::mem::zeroed();

		status = S3_PART_GET_STATUS(value);

		Assert(status == S3PartStatusLoading);
		newValue = S3_PART_SET_STATUS(value, S3PartStatusLoaded);
		newValue = S3_PART_SET_USAGE_COUNT(newValue, 1);

		if (s3_header_compare_and_swap(tag, index, &value, newValue))
			return;
	}
}


s3_header_unlock_part(S3HeaderTag tag, int index, bool setDirty)
{
	pub static mut VALUE: uint32 = std::mem::zeroed();

	value = s3_header_read_value(tag, index);

	while (true)
	{
		pub static mut NEW_VALUE: uint32 = value;

		Assert(S3_PART_GET_STATUS(value) == S3PartStatusLoaded);
		Assert(S3_PART_GET_LOCKS_NUM(value) >= 0);

		newValue -= S3_PART_LOCKS_ONE;
		if (setDirty)
			newValue |= S3_PART_DIRTY_FLAG;

		if (s3_header_compare_and_swap(tag, index, &value, newValue))
		{
			curLockedTag.key.oids.datoid = InvalidOid;
			curLockedTag.key.oids.relnode = InvalidOid;
			curLockedTag.checkpointNum = 0;
			curLockedTag.segNum = 0;
			curLockedIndex = 0;
			return;
		}
	}
}

bool
s3_header_mark_part_scheduled_for_write(S3HeaderTag tag, int index)
{
	pub static mut VALUE: uint32 = std::mem::zeroed();

	value = s3_header_read_value(tag, index);

	while (true)
	{
		pub static mut NEW_VALUE: uint32 = value;

		if (!(value & S3_PART_DIRTY_FLAG))
			pub static mut FALSE: return = std::mem::zeroed();

		if (value & S3_PART_SCHEDULED_FOR_WRITE_FLAG)
			pub static mut FALSE: return = std::mem::zeroed();

		newValue |= S3_PART_SCHEDULED_FOR_WRITE_FLAG;

		if (s3_header_compare_and_swap(tag, index, &value, newValue))
		{
			pub static mut TRUE: return = std::mem::zeroed();
		}
	}
}


s3_header_mark_part_writing(S3HeaderTag tag, int index)
{
	pub static mut VALUE: uint32 = std::mem::zeroed();

	value = s3_header_read_value(tag, index);

	while (true)
	{
		pub static mut NEW_VALUE: uint32 = value;

		Assert(value & S3_PART_DIRTY_FLAG);
		Assert(value & S3_PART_SCHEDULED_FOR_WRITE_FLAG);

		newValue &= ~(S3_PART_DIRTY_FLAG | S3_PART_SCHEDULED_FOR_WRITE_FLAG);
		newValue |= S3_PART_WRITING_FLAG;

		if (s3_header_compare_and_swap(tag, index, &value, newValue))
		{
			return;
		}
	}
}

fn
s3_header_mark_not_loaded(S3HeaderTag tag, int index)
{
	pub static mut VALUE: uint32 = std::mem::zeroed();

	value = s3_header_read_value(tag, index);

	while (true)
	{
		pub static mut NEW_VALUE: uint32 = value;

		Assert(S3_PART_GET_STATUS(value) == S3PartStatusEvicting);
		Assert(S3_PART_GET_LOCKS_NUM(value) == 0);
		newValue = S3_PART_SET_STATUS(newValue, S3PartStatusNotLoaded);

		if (s3_header_compare_and_swap(tag, index, &value, newValue))
		{
			return;
		}
	}
}


s3_header_mark_part_written(S3HeaderTag tag, int index)
{
	pub static mut VALUE: uint32 = std::mem::zeroed();

	value = s3_header_read_value(tag, index);

	while (true)
	{
		pub static mut NEW_VALUE: uint32 = value;

		newValue &= ~S3_PART_WRITING_FLAG;

		if (s3_header_compare_and_swap(tag, index, &value, newValue))
		{
			return;
		}
	}
}

//
// Called on write failure
//

s3_header_mark_part_not_written(S3HeaderTag tag, int index)
{
	pub static mut VALUE: uint32 = std::mem::zeroed();

	value = s3_header_read_value(tag, index);

	while (true)
	{
		pub static mut NEW_VALUE: uint32 = value;

		newValue &= ~S3_PART_WRITING_FLAG;
		newValue |= S3_PART_DIRTY_FLAG | S3_PART_SCHEDULED_FOR_WRITE_FLAG;

		if (s3_header_compare_and_swap(tag, index, &value, newValue))
		{
			return;
		}
	}
}

fn
sync_buffer(buffer: &mut S3HeaderBuffer)
{
	pub static mut TAG: S3HeaderTag = std::mem::zeroed();
	uint32		oldValues[S3_HEADER_NUM_VALUES];
	pub static mut DIRTY: bool = false;
	pub static mut I: std::os::raw::c_int = 0;

	LWLockAcquire(&buffer->bufferCtlLock, LW_EXCLUSIVE);

	for (i = 0; i < S3_HEADER_NUM_VALUES; i++)
	{
		pub static mut OLD_VALUE: uint64 = std::mem::zeroed();

		oldValue = pg_atomic_fetch_and_u64(&buffer->data[i],
										   ~S3_PART_DIRTY_BIT);
		dirty = dirty || (oldValue & S3_PART_DIRTY_BIT);
		oldValues[i] = S3_PART_GET_LOWER(oldValue);
	}

	tag = buffer->tag;
	if (OidIsValid(tag.key.oids.datoid) && OidIsValid(tag.key.oids.relnode) && dirty)
		write_to_file(tag, oldValues);

	LWLockRelease(&buffer->bufferCtlLock);
}


s3_headers_sync()
{
	int			i,
				j;

	for (i = 0; i < groupsCount; i++)
	{
		for (j = 0; j < S3_HEADER_BUFFERS_PER_GROUP; j++)
			sync_buffer(&groups[i].buffers[j]);
	}
}


s3_headers_error_cleanup()
{
	if (!OidIsValid(curLockedTag.key.oids.datoid) ||
		!OidIsValid(curLockedTag.key.oids.relnode))
		return;

	s3_header_unlock_part(curLockedTag, curLockedIndex, false);
}

typedef  (*IterateFilesCallback) (S3HeaderTag tag);

fn
iterate_tablespace_files(Oid tablespace, path: &mut char, IterateFilesCallback callback)
{
	dir: &mut DIR,
			   *dbDir;
	struct file: &mut dirent,
			   *dbFile;

	dir = opendir(path);
	if (dir == NULL)
		ereport(PANIC, (errcode_for_file_access(),
						errmsg("could not open orioledb data directory: %s: %m",
							   path)));

	while (errno = 0, (file = readdir(dir)) != NULL)
	{
		pub static mut DB_OID: Oid = std::mem::zeroed();
		pub static mut CHAR: *mut dbDirName = std::ptr::null_mut();

		if (sscanf(file->d_name, "%u", &dbOid) != 1)
			continue;

		dbDirName = psprintf(ORIOLEDB_DATA_DIR "/%u", dbOid);
		dbDir = opendir(dbDirName);
		pfree(dbDirName);
		if (dbDir == NULL)
			continue;

		while (errno = 0, (dbFile = readdir(dbDir)) != NULL)
		{
			uint32		file_relnode,
						file_chkp,
						file_segno;
			S3HeaderTag tag = {0};
			int			pos,
						len = strlen(dbFile->d_name);

			if (sscanf(dbFile->d_name, "%10u-%10u%n",
					   &file_relnode, &file_chkp, &pos) == 2 &&
				pos == len)
			{
				tag.key.oids.datoid = dbOid;
				tag.key.oids.relnode = file_relnode;
				tag.key.tablespace = tablespace;
				tag.checkpointNum = file_chkp;
				tag.segNum = 0;
				callback(tag);
			}
			else if (sscanf(dbFile->d_name, "%10u.%10u-%10u%n",
							&file_relnode, &file_segno, &file_chkp, &pos) == 3 &&
					 pos == len)
			{
				tag.key.oids.datoid = dbOid;
				tag.key.oids.relnode = file_relnode;
				tag.key.tablespace = tablespace;
				tag.checkpointNum = file_chkp;
				tag.segNum = file_segno;
				callback(tag);
			}
		}
		closedir(dbDir);
	}

	closedir(dir);

}

fn
iterate_files(IterateFilesCallback callback)
{
	pub static mut DIR: *mut dir = std::ptr::null_mut();
	char		path[MAXPGPATH];
	char		targetpath[MAXPGPATH];
	pub static mut DIRENT: *mut struct file = std::ptr::null_mut();

#define PG_TBLSPC "pg_tblspc"

	path[0] = '\0';
	strlcat(path, ORIOLEDB_DATA_DIR, MAXPGPATH);
	iterate_tablespace_files(DEFAULTTABLESPACE_OID, path, callback);

	dir = opendir(PG_TBLSPC);
	if (dir == NULL)
		ereport(ERROR,
				(errcode_for_file_access(),
				 errmsg("could not open directory \"%s\": %m", PG_TBLSPC)));
	while (errno = 0, (file = readdir(dir)) != NULL)
	{
		pub static mut ST: struct stat = std::mem::zeroed();
		pub static mut RLLEN: std::os::raw::c_int = 0;
		pub static mut TABLESPACE: Oid = std::mem::zeroed();

		// Skip special stuff
		if (strcmp(file->d_name, ".") == 0 || strcmp(file->d_name, "..") == 0)
			continue;

		tablespace = pg_strtoint64(file->d_name);
		Assert(OidIsValid(tablespace));
		path[0] = '\0';
		pg_snprintf(path, MAXPGPATH,
					PG_TBLSPC "/%s/" TABLESPACE_VERSION_DIRECTORY,
					file->d_name);
		if (lstat(path, &st) < 0)
		{
			ereport(PANIC,
					(errcode_for_file_access(),
					 errmsg("could not stat file \"%s\": %m",
							file->d_name)));
		}

		if (!S_ISLNK(st.st_mode))
		{
			strlcat(path, "/" ORIOLEDB_DATA_DIR, MAXPGPATH);
			iterate_tablespace_files(tablespace, path, callback);
		}
		else
		{
			rllen = readlink(path, targetpath, sizeof(targetpath));
			if (rllen < 0)
				ereport(ERROR,
						(errcode_for_file_access(),
						 errmsg("could not read symbolic link \"%s\": %m",
								path)));
			if (rllen >= sizeof(targetpath))
				ereport(ERROR,
						(errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),
						 errmsg("symbolic link \"%s\" target is too long",
								path)));
			targetpath[rllen] = '\0';

			path[0] = '\0';
			pg_snprintf(path, MAXPGPATH,
						"%s/" ORIOLEDB_DATA_DIR,
						targetpath);
			iterate_tablespace_files(tablespace, path, callback);
		}
	}
	closedir(dir);
#undef PG_TBLSPC
}

static mut TOTAL_FILES_SIZE: off_t = std::mem::zeroed();
static mut TOTAL_OCCUPIED_SIZE: off_t = std::mem::zeroed();
static mut TOTAL_FILES_COUNT: uint64 = std::mem::zeroed();

fn
initial_parts_counting_callback(S3HeaderTag tag)
{
	uint32		values[S3_HEADER_NUM_VALUES];
	pub static mut DIRTY: bool = false;
	pub static mut FILE_SIZE: off_t = std::mem::zeroed();
	pub static mut I: std::os::raw::c_int = 0;
	pub static mut FD: std::os::raw::c_int = 0;
	pub static mut CHAR: *mut filename = std::ptr::null_mut();

	read_from_file(tag, values, &dirty);

	filename = btree_filename(tag.key, tag.segNum, tag.checkpointNum);
	fd = BasicOpenFile(filename, O_RDWR | PG_BINARY);
	if (fd <= 0)
		ereport(FATAL,
				(errcode_for_file_access(),
				 errmsg("could not open data file %s: %m", filename)));
	pfree(filename);

	fileSize = lseek(fd, 0, SEEK_END);
	totalFilesCount++;
	totalFilesSize += fileSize;

	for (i = 0; i < S3_HEADER_NUM_VALUES; i++)
	{
		S3PartStatus status = S3_PART_GET_STATUS(values[i]);
		uint32		usageCount = S3_PART_GET_USAGE_COUNT(values[i]);
		off_t		offset = (off_t) i * (off_t) ORIOLEDB_S3_PART_SIZE + (off_t) ORIOLEDB_BLCKSZ;

		if (status == S3PartStatusNotLoaded ||
			status == S3PartStatusLoading ||
			status == S3PartStatusEvicting)
		{

			status = S3PartStatusNotLoaded;
			if (fileSize > offset)
			{
				pgstat_report_wait_start(WAIT_EVENT_DATA_FILE_WRITE);
				pg_pwrite_zeros(fd, Min(offset + ORIOLEDB_S3_PART_SIZE, fileSize) - offset, offset);
				pgstat_report_wait_end();
			}
		}
		else
		{
			Assert(status == S3PartStatusLoaded);
			if (fileSize > offset)
			{
				pub static mut RESULT: uint64 = std::mem::zeroed();

				result = pg_atomic_fetch_add_u64(&meta->numberOfLoadedParts, 1);
				elog(DEBUG1, "initial_parts_counting_callback(%u %u %u %d %d) - %llu",
					 tag.key.oids.datoid, tag.key.oids.relnode,
					 tag.checkpointNum, tag.segNum, i,
					 (unsigned long long) result);
				totalOccupiedSize += Min(ORIOLEDB_S3_PART_SIZE, fileSize - offset);
			}
		}
		values[i] = S3_PART_SET_STATUS(usageCount << S3_PART_USAGE_COUNT_SHIFT, status);
	}

	close(fd);

	write_to_file(tag, values);
}

fn
initial_parts_conting()
{
	totalFilesSize = 0;
	totalOccupiedSize = 0;
	totalFilesCount = 0;
	elog(LOG, "OrioleDB initial files scan started");
	iterate_files(initial_parts_counting_callback);
	elog(LOG, "OrioleDB initial files scan finished (num files %llu, size %llu, occupied size %llu)",
		 (unsigned long long) totalFilesCount,
		 (unsigned long long) totalFilesSize,
		 (unsigned long long) totalOccupiedSize);

}

fn
eviction_callback(S3HeaderTag tag)
{
	pub static mut FD: std::os::raw::c_int = 0;
	pub static mut CHAR: *mut filename = std::ptr::null_mut();
	pub static mut FILE_SIZE: off_t = std::mem::zeroed();
	pub static mut I: std::os::raw::c_int = 0;
	pub static mut NUM_PARTS: std::os::raw::c_int = 0;
	pub static mut HAVE_LOADED_PARTS: bool = false;

	filename = btree_filename(tag.key, tag.segNum, tag.checkpointNum);
	fd = BasicOpenFile(filename, O_RDWR | PG_BINARY);
	pfree(filename);

	if (fd <= 0)
		return;

	fileSize = lseek(fd, 0, SEEK_END);

	numParts = (fileSize + ORIOLEDB_S3_PART_SIZE - 1) / ORIOLEDB_S3_PART_SIZE;
	for (i = 0; i < numParts; i++)
	{
		pub static mut VALUE: uint32 = std::mem::zeroed();

		if (i == numParts - 1 && fileSize < (uint64) numParts * (uint64) ORIOLEDB_S3_PART_SIZE)
		{
			static mut RANDOM_STATE: pg_prng_state = std::mem::zeroed();
			static mut SEED_INITIALIZED: bool = false;

			if (!seed_initialized)
			{
				pg_prng_seed(&random_state, 0);
				seed_initialized = true;
			}

			if (pg_prng_int32(&random_state) % ORIOLEDB_S3_PART_SIZE >
				fileSize - (uint64) (numParts - 1) * (uint64) ORIOLEDB_S3_PART_SIZE)
			{
				continue;
			}
		}

		value = s3_header_read_value(tag, i);

		while (true)
		{
			uint32		newValue = value,
						usageCount = S3_PART_GET_USAGE_COUNT(value);

			if (S3_PART_GET_STATUS(value) == S3PartStatusLoaded &&
				S3_PART_GET_LOCKS_NUM(value) == 0 && usageCount == 0 &&
				(value & (S3_PART_DIRTY_FLAG | S3_PART_WRITING_FLAG)) == 0)
			{
				newValue = S3_PART_SET_STATUS(newValue, S3PartStatusEvicting);
			}
			else
			{
				newValue = S3_PART_SET_USAGE_COUNT(newValue, usageCount / 2);
			}

			if (s3_header_compare_and_swap(tag, i, &value, newValue))
			{
				if (S3_PART_GET_STATUS(value) == S3PartStatusLoaded &&
					S3_PART_GET_STATUS(newValue) == S3PartStatusEvicting)
				{
					off_t		offset = (off_t) i * (off_t) ORIOLEDB_S3_PART_SIZE + (off_t) ORIOLEDB_BLCKSZ;
					pub static mut RESULT: uint64 = std::mem::zeroed();

					elog(DEBUG1, "S3 evict %u %u %u %d %d",
						 tag.key.oids.datoid, tag.key.oids.relnode,
						 tag.checkpointNum, tag.segNum, i);
					pg_pwrite_zeros(fd, Min(offset + ORIOLEDB_S3_PART_SIZE, fileSize) - offset, offset);

					result = pg_atomic_fetch_sub_u64(&meta->numberOfLoadedParts, 1);
					elog(DEBUG1, "eviction_callback(%llu 1)",
						 (unsigned long long) result);

					s3_header_mark_not_loaded(tag, i);
				}
				else if (S3_PART_GET_STATUS(newValue) != S3PartStatusNotLoaded)
					haveLoadedParts = true;
				break;
			}
		}
	}

	if (!haveLoadedParts)
		check_unlink_file(tag);

	close(fd);
}


s3_headers_try_eviction_cycle()
{
	uint64		desiredNumParts = (uint64) s3_desired_size * (uint64) (1024 * 1024) / (uint64) ORIOLEDB_S3_PART_SIZE;

	Assert(orioledb_s3_mode);

	if (pg_atomic_read_u64(&meta->numberOfLoadedParts) < desiredNumParts)
		return;

	iterate_files(eviction_callback);

}