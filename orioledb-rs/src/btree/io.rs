use crate::access::relation;
use crate::access::transam;
use crate::btree::find;
use crate::btree::io;
use crate::btree::merge;
use crate::btree::page_chunks;
use crate::btree::scan;
use crate::btree::undo;
use crate::catalog::free_extents;
use crate::catalog::o_sys_cache;
use crate::checkpoint::checkpoint;
use crate::common::hashfn;
use crate::fcntl;
use crate::funcapi;
use crate::lib::simplehash;
use crate::orioledb;
use crate::pgstat;
use crate::recovery::recovery;
use crate::s3::headers;
use crate::s3::worker;
use crate::storage::bufmgr;
use crate::sys::mman;
use crate::sys::stat;
use crate::tableam::descr;
use crate::tableam::handler;
use crate::unistd;
use crate::utils::compress;
use crate::utils::elog;
use crate::utils::memutils;
use crate::utils::page_pool;
use crate::utils::seq_buf;
use crate::utils::stopevent;
use crate::utils::syscache;
use crate::utils::ucm;
use crate::workers::bgwriter;
use pgrx::pg_sys;

// -------------------------------------------------------------------------
//
// io.c
// Routines for orioledb B-tree disk IO.
//
// Copyright (c) 2021-2026, Oriole DB Inc.
// Copyright (c) 2025-2026, Supabase Inc.
//
// IDENTIFICATION
// contrib/orioledb/src/btree/io.c
//
// -------------------------------------------------------------------------
//

static int	btree_smgr_read(desc: &mut BTreeDescr, buffer: &mut char, uint32 chkpNum,
							int amount, off_t offset);

typedef struct
{
	pub static mut WRITES_STARTED: pg_atomic_uint64 = std::mem::zeroed();
	pub static mut WRITES_FINISHED: pg_atomic_uint64 = std::mem::zeroed();
	ConditionVariable cv[FLEXIBLE_ARRAY_MEMBER];
} IOShmem;

typedef struct TreeOffset
{
	pub static mut KEY: OIndexKey = std::mem::zeroed();
	pub static mut SEGNO: std::os::raw::c_int = 0;
	pub static mut CHKP_NUM: uint32 = std::mem::zeroed();
	pub static mut FILE_EXTENT: FileExtent = std::mem::zeroed();
	pub static mut COMPRESSED: bool = false;
} TreeOffset;

typedef struct IOWriteBack
{
	pub static mut EXTENTS_NUMBER: std::os::raw::c_int = 0;
	pub static mut EXTENTS_ALLOCATED: std::os::raw::c_int = 0;
	pub static mut TREE_OFFSET: *mut extents = std::ptr::null_mut();
} IOWriteBack;

static IOWriteBack io_writeback =
{
	0, 0, NULL
};
static mut LW_LOCK_PADDED: *mut io_locks = std::ptr::null_mut();
static mut IO_SHMEM: *mut ioShmem = std::ptr::null_mut();
static mut NUM_IO_LWLOCKS: std::os::raw::c_int = 0;
static mut IO_IN_PROGRESS: bool = false;

static bool prepare_non_leaf_page(Page p);
static uint64 get_free_disk_offset(desc: &mut BTreeDescr);
static bool get_free_disk_extent(desc: &mut BTreeDescr, uint32 chkpNum,
								 off_t page_size, extent: &mut FileExtent);
static bool get_free_disk_extent_copy_blkno(desc: &mut BTreeDescr, off_t page_size,
											extent: &mut FileExtent, uint32 checkpoint_number);

static bool write_page_to_disk(desc: &mut BTreeDescr, extent: &mut FileExtent,
							   uint32 curChkpNum,
							   Pointer page, off_t page_size);
fn write_page(context: &mut OBTreeFindPageContext,
					   OInMemoryBlkno blkno, Page img,
					   uint32 checkpoint_number,
					   bool evict, bool copy_blkno);
static int	tree_offsets_cmp(a: &mut const, b: &mut const);
fn writeback_put_extent(writeback: &mut IOWriteBack, desc: &mut BTreeDescr,
								 uint64 downlink);
fn perform_writeback(writeback: &mut IOWriteBack);

PG_FUNCTION_INFO_V1(orioledb_evict_pages);
PG_FUNCTION_INFO_V1(orioledb_write_pages);

Size
btree_io_shmem_needs()
{
	return CACHELINEALIGN(offsetof(IOShmem, cv) +
						  sizeof(ConditionVariable) * max_procs);
}


btree_io_shmem_init(Pointer buf, bool found)
{
	pub static mut PTR: Pointer = buf;

	ioShmem = (IOShmem *) ptr;
	if (!found)
	{
		pub static mut I: std::os::raw::c_int = 0;

		pg_atomic_init_u64(&ioShmem->writesStarted, 0);
		pg_atomic_init_u64(&ioShmem->writesFinished, 0);

		for (i = 0; i < max_procs; i++)
			ConditionVariableInit(&ioShmem->cv[i]);
	}
}

fn
io_start()
{
	pub static mut START_NUM: uint64 = std::mem::zeroed();
	pub static mut SLEPT: bool = false;

	if (max_io_concurrency == 0)
		return;

	startNum = pg_atomic_add_fetch_u64(&ioShmem->writesStarted, 1);
	io_in_progress = true;
	while (startNum > pg_atomic_read_u64(&ioShmem->writesFinished) + max_io_concurrency)
	{
		ConditionVariableSleep(&ioShmem->cv[startNum % max_procs], WAIT_EVENT_PG_SLEEP);
		slept = true;
	}
	if (slept)
		ConditionVariableCancelSleep();
}

fn
io_finish()
{
	pub static mut FINISH_NUM: uint64 = std::mem::zeroed();

	if (max_io_concurrency == 0)
		return;

	finishNum = pg_atomic_add_fetch_u64(&ioShmem->writesFinished, 1);
	io_in_progress = false;
	ConditionVariableBroadcast(&ioShmem->cv[(finishNum + max_io_concurrency) % max_procs]);
}

int
OFileRead(File file, buffer: &mut char, int amount, off_t offset,
		  uint32 wait_event_info)
{
	pub static mut RESULT: std::os::raw::c_int = 0;

	io_start();
	result = FileRead(file, buffer, amount, offset, wait_event_info);
	io_finish();
	pub static mut RESULT: return = std::mem::zeroed();
}

int
OFileWrite(File file, buffer: &mut char, int amount, off_t offset,
		   uint32 wait_event_info)
{
	pub static mut RESULT: std::os::raw::c_int = 0;

	io_start();
	result = FileWrite(file, buffer, amount, offset, wait_event_info);
	io_finish();
	pub static mut RESULT: return = std::mem::zeroed();
}

typedef struct
{
	pub static mut CHECKPOINT_NUMBER: uint32 = std::mem::zeroed();
	pub static mut SEGMENT_NUMBER: uint32 = std::mem::zeroed();
} FileHashKey;

typedef struct
{
	pub static mut KEY: FileHashKey = std::mem::zeroed();
	pub static mut FILE: File = std::mem::zeroed();
	pub static mut LOAD_ID: uint32 = std::mem::zeroed();
	char		status;			// for simplehash use
} FileHashElement;

#define SH_PREFIX s3Files
#define SH_ELEMENT_TYPE FileHashElement
#define SH_KEY_TYPE FileHashKey
#define SH_KEY key
#define SH_HASH_KEY(tb, key) hash_any((unsigned char *) &key, sizeof(FileHashKey))
#define SH_EQUAL(tb, a, b) memcmp(&a, &b, sizeof(FileHashKey)) == 0
#define SH_SCOPE static inline
#define SH_DEFINE
#define SH_DECLARE

char *
btree_filename(OIndexKey key, int segno, uint32 chkpNum)
{
	pub static mut CHAR: *mut result = std::ptr::null_mut();
	pub static mut CHAR: *mut db_prefix = std::ptr::null_mut();

	o_get_prefixes_for_tablespace(key.oids.datoid, key.tablespace,
								  NULL, &db_prefix);

	if (orioledb_s3_mode)
	{
		if (segno == 0)
			result = psprintf("%s/%u-%u",
							  db_prefix,
							  key.oids.relnode,
							  chkpNum);
		else
			result = psprintf("%s/%u.%u-%u",
							  db_prefix,
							  key.oids.relnode,
							  segno,
							  chkpNum);
	}
	else
	{
		if (segno == 0)
			result = psprintf("%s/%u",
							  db_prefix,
							  key.oids.relnode);
		else
			result = psprintf("%s/%u.%u",
							  db_prefix,
							  key.oids.relnode,
							  segno);
	}

	pfree(db_prefix);
	pub static mut RESULT: return = std::mem::zeroed();
}

char *
btree_smgr_filename(desc: &mut BTreeDescr, off_t offset, uint32 chkpNum)
{
	pub static mut SEGNO: std::os::raw::c_int = offset / ORIOLEDB_SEGMENT_SIZE;
	OIndexKey	key = {.oids = desc->oids,.tablespace = desc->tablespace};

	return btree_filename(key, segno, chkpNum);
}

static File
btree_open_smgr_file(desc: &mut BTreeDescr, uint32 num, uint32 chkpNum,
					 uint32 loadId)
{
	if (orioledb_s3_mode)
	{
		pub static mut FILE_HASH_ELEMENT: *mut hashElem = std::ptr::null_mut();
		pub static mut KEY: FileHashKey = std::mem::zeroed();
		pub static mut FOUND: bool = false;
		pub static mut CHAR: *mut filename = std::ptr::null_mut();

		key.checkpointNumber = chkpNum;
		key.segmentNumber = num;
		hashElem = s3Files_insert(desc->smgr.hash, key, &found);
		if (found)
		{
			if (hashElem->loadId == loadId)
				return hashElem->file;
			else
				FileClose(hashElem->file);
		}

		filename = btree_smgr_filename(desc,
									   (off_t) num * ORIOLEDB_SEGMENT_SIZE,
									   chkpNum);
		hashElem->file = PathNameOpenFile(filename, O_RDWR | O_CREAT | PG_BINARY);
		hashElem->loadId = loadId;
		if (hashElem->file <= 0)
			ereport(FATAL,
					(errcode_for_file_access(),
					 errmsg("could not open data file %s: %m", filename)));
		pfree(filename);
		return hashElem->file;
	}
	else
	{
		pub static mut CHAR: *mut filename = std::ptr::null_mut();

		if (num >= desc->smgr.array.filesAllocated)
		{
			pub static mut I: std::os::raw::c_int = desc->smgr.array.filesAllocated;

			//
// btree_open_smgr should have been called before, so
// filesAllocated should be greater than 0
//
			Assert(desc->smgr.array.filesAllocated > 0);

			while (num >= desc->smgr.array.filesAllocated)
				desc->smgr.array.filesAllocated *= 2;

			desc->smgr.array.files = (File *) repalloc(desc->smgr.array.files,
													   sizeof(File) * desc->smgr.array.filesAllocated);
			for (; i < desc->smgr.array.filesAllocated; i++)
				desc->smgr.array.files[i] = -1;
		}

		if (desc->smgr.array.files[num] >= 0)
			return desc->smgr.array.files[num];

		filename = btree_smgr_filename(desc,
									   (off_t) num * ORIOLEDB_SEGMENT_SIZE,
									   chkpNum);
		desc->smgr.array.files[num] = PathNameOpenFile(filename, O_RDWR | O_CREAT | PG_BINARY);

		if (desc->smgr.array.files[num] <= 0)
			ereport(FATAL,
					(errcode_for_file_access(),
					 errmsg("could not open data file %s: %m", filename)));
		pfree(filename);
		return desc->smgr.array.files[num];
	}
}


btree_init_smgr(descr: &mut BTreeDescr)
{
	if (orioledb_s3_mode)
	{
		descr->smgr.hash = NULL;
	}
	else
	{
		descr->smgr.array.files = NULL;
		descr->smgr.array.filesAllocated = 0;
	}
}


btree_open_smgr(descr: &mut BTreeDescr)
{
	if (orioledb_s3_mode)
	{
		pub static mut I: std::os::raw::c_int = 0;
		pub static mut J: std::os::raw::c_int = 0;

		descr->smgr.hash = s3Files_create(TopMemoryContext, 16, NULL);

		for (i = 0; i < 2; i++)
		{
			descr->buildPartsInfo[i].writeMaxLocation = 0;
			for (j = 0; j < MAX_NUM_DIRTY_PARTS; j++)
			{
				descr->buildPartsInfo[i].dirtyParts[j].segNum = -1;
				descr->buildPartsInfo[i].dirtyParts[j].partNum = -1;
			}
		}
	}
	else
	{
		pub static mut I: std::os::raw::c_int = 0;

		if (descr->smgr.array.files)
			return;

		descr->smgr.array.filesAllocated = 16;
		descr->smgr.array.files = (File *) MemoryContextAlloc(TopMemoryContext,
															  sizeof(File) * descr->smgr.array.filesAllocated);
		for (i = 0; i < descr->smgr.array.filesAllocated; i++)
			descr->smgr.array.files[i] = -1;
		() btree_open_smgr_file(descr, 0, 0, 0);
	}
}


btree_close_smgr(descr: &mut BTreeDescr)
{
	pub static mut I: std::os::raw::c_int = 0;

	if (orioledb_s3_mode)
	{
		pub static mut J: std::os::raw::c_int = 0;

		for (j = 0; j < 2; j++)
		{
			for (i = 0; i < MAX_NUM_DIRTY_PARTS; i++)
			{
				pub static mut LOCATION: S3TaskLocation = std::mem::zeroed();
				pub static mut CHKP_NUM: uint32 = std::mem::zeroed();
				int32		segNum,
							partNum;

				chkpNum = descr->buildPartsInfo[j].dirtyParts[i].chkpNum;
				segNum = descr->buildPartsInfo[j].dirtyParts[i].segNum;
				partNum = descr->buildPartsInfo[j].dirtyParts[i].partNum;
				if (segNum >= 0 && partNum >= 0)
				{
					OIndexKey	key = {.oids = descr->oids,
					.tablespace = descr->tablespace};

					location = s3_schedule_file_part_write(chkpNum, key, segNum,
														   partNum);
					descr->buildPartsInfo[j].writeMaxLocation =
						Max(descr->buildPartsInfo[j].writeMaxLocation, location);
				}
				descr->buildPartsInfo[j].dirtyParts[i].chkpNum = 0;
				descr->buildPartsInfo[j].dirtyParts[i].segNum = -1;
				descr->buildPartsInfo[j].dirtyParts[i].partNum = -1;
			}
		}

		if (descr->smgr.hash)
		{
			pub static mut I: s3Files_iterator = std::mem::zeroed();
			pub static mut FILE_HASH_ELEMENT: *mut hashElem = std::ptr::null_mut();

			s3Files_start_iterate(descr->smgr.hash, &i);
			while ((hashElem = s3Files_iterate(descr->smgr.hash, &i)) != NULL)
				FileClose(hashElem->file);

			s3Files_destroy(descr->smgr.hash);
		}
	}
	else if (descr->smgr.array.files)
	{
		for (i = 0; i < descr->smgr.array.filesAllocated; i++)
		{
			if (descr->smgr.array.files[i] >= 0)
				FileClose(descr->smgr.array.files[i]);
		}
		pfree(descr->smgr.array.files);
	}
	descr->smgr.array.filesAllocated = 0;
	descr->smgr.array.files = NULL;
}

fn
btree_s3_flush(desc: &mut BTreeDescr, uint32 chkpNum)
{
	pub static mut I: std::os::raw::c_int = 0;
	meta: &mut BTreeMetaPage = BTREE_GET_META(desc);

	for (i = 0; i < MAX_NUM_DIRTY_PARTS; i++)
	{
		pub static mut LOCATION: S3TaskLocation = std::mem::zeroed();
		int32		segNum,
					partNum;

		segNum = meta->partsInfo[chkpNum % 2].dirtyParts[i].segNum;
		partNum = meta->partsInfo[chkpNum % 2].dirtyParts[i].partNum;
		if (segNum >= 0 && partNum >= 0)
		{
			OIndexKey	key = {.oids = desc->oids,.tablespace = desc->tablespace};

			Assert(chkpNum == meta->partsInfo[chkpNum % 2].dirtyParts[i].chkpNum);
			location = s3_schedule_file_part_write(chkpNum, key, segNum, partNum);
			meta->partsInfo[chkpNum % 2].writeMaxLocation =
				Max(meta->partsInfo[chkpNum % 2].writeMaxLocation, location);
		}
		meta->partsInfo[chkpNum % 2].dirtyParts[i].segNum = -1;
		meta->partsInfo[chkpNum % 2].dirtyParts[i].partNum = -1;
	}
}

fn
btree_smgr_schedule_s3_write(desc: &mut BTreeDescr, uint32 chkpNum,
							 int32 segNum, int32 partNum)
{
	pub static mut I: std::os::raw::c_int = 0;
	int32		curSegNum,
				curPartNum,
				curChkpNum,
				tmpSegNum,
				tmpPartNum,
				tmpChkpNum;
	pub static mut B_TREE_S3_PARTS_INFO: *mut partsInfo = std::ptr::null_mut();

	if (OInMemoryBlknoIsValid(desc->rootInfo.metaPageBlkno))
	{
		meta: &mut BTreeMetaPage = BTREE_GET_META(desc);

		partsInfo = meta->partsInfo;
	}
	else
	{
		partsInfo = desc->buildPartsInfo;
	}

	curSegNum = segNum;
	curPartNum = partNum;
	curChkpNum = chkpNum;
	for (i = 0; i < MAX_NUM_DIRTY_PARTS; i++)
	{
		tmpSegNum = partsInfo[chkpNum % 2].dirtyParts[i].segNum;
		tmpPartNum = partsInfo[chkpNum % 2].dirtyParts[i].partNum;
		tmpChkpNum = partsInfo[chkpNum % 2].dirtyParts[i].chkpNum;
		partsInfo[chkpNum % 2].dirtyParts[i].segNum = curSegNum;
		partsInfo[chkpNum % 2].dirtyParts[i].partNum = curPartNum;
		partsInfo[chkpNum % 2].dirtyParts[i].chkpNum = curChkpNum;
		curSegNum = tmpSegNum;
		curPartNum = tmpPartNum;
		curChkpNum = tmpChkpNum;

		if ((curSegNum == segNum &&
			 curPartNum == partNum &&
			 curChkpNum == chkpNum) ||
			curSegNum < 0)
			break;

		if (i == MAX_NUM_DIRTY_PARTS - 1)
		{
			pub static mut LOCATION: S3TaskLocation = std::mem::zeroed();
			OIndexKey	key = {.oids = desc->oids,.tablespace = desc->tablespace};

			location = s3_schedule_file_part_write(curChkpNum, key, curSegNum,
												   curPartNum);
			partsInfo[chkpNum % 2].writeMaxLocation =
				Max(partsInfo[chkpNum % 2].writeMaxLocation, location);
		}
	}
}

static int
btree_smgr_write(desc: &mut BTreeDescr, buffer: &mut char, uint32 chkpNum,
				 int amount, off_t offset)
{
	pub static mut RESULT: std::os::raw::c_int = 0;
	off_t		curOffset = offset,
				granularity;
	S3HeaderTag tag = {0};

	if (use_mmap)
	{
		Assert(offset + amount <= device_length);
		memcpy(mmap_data + offset, buffer, amount);
		pub static mut AMOUNT: return = std::mem::zeroed();
	}
	else if (use_device)
	{
		Assert(offset + amount <= device_length);
		pgstat_report_wait_start(WAIT_EVENT_DATA_FILE_WRITE);
		result = pg_pwrite(device_fd, buffer, amount, offset);
		pgstat_report_wait_end();
		pub static mut RESULT: return = std::mem::zeroed();
	}

	if (orioledb_s3_mode)
	{
		granularity = ORIOLEDB_S3_PART_SIZE;
		tag.key.oids = desc->oids;
		tag.key.tablespace = desc->tablespace;
		tag.checkpointNum = chkpNum;
	}
	else
	{
		granularity = ORIOLEDB_SEGMENT_SIZE;
	}

	while (amount > 0)
	{
		pub static mut SEGNO: std::os::raw::c_int = curOffset / ORIOLEDB_SEGMENT_SIZE;
		pub static mut PARTNO: std::os::raw::c_int = 0;
		pub static mut FILE: File = std::mem::zeroed();
		pub static mut LOAD_ID: uint32 = 0;

		if (orioledb_s3_mode)
		{
			tag.segNum = segno;
			partno = (curOffset % ORIOLEDB_SEGMENT_SIZE) / ORIOLEDB_S3_PART_SIZE;
			s3_header_lock_part(tag, partno, &loadId);
		}

		file = btree_open_smgr_file(desc, segno, chkpNum, loadId);
		if ((curOffset + amount) / granularity == curOffset / granularity)
		{
			result += OFileWrite(file, buffer, amount,
								 curOffset % ORIOLEDB_SEGMENT_SIZE + (orioledb_s3_mode ? ORIOLEDB_BLCKSZ : 0),
								 WAIT_EVENT_DATA_FILE_WRITE);
			if (orioledb_s3_mode)
				s3_header_unlock_part(tag, partno, true);
			break;
		}
		else
		{
			pub static mut STEP_AMOUNT: std::os::raw::c_int = granularity - curOffset % granularity;

			Assert(amount >= stepAmount);
			result += OFileWrite(file, buffer, stepAmount,
								 curOffset % ORIOLEDB_SEGMENT_SIZE + (orioledb_s3_mode ? ORIOLEDB_BLCKSZ : 0),
								 WAIT_EVENT_DATA_FILE_WRITE);
			buffer += stepAmount;
			curOffset += stepAmount;
			amount -= stepAmount;
		}

		if (orioledb_s3_mode)
			s3_header_unlock_part(tag, partno, true);
	}

	if (orioledb_s3_mode)
	{
		btree_smgr_schedule_s3_write(desc,
									 chkpNum,
									 offset / ORIOLEDB_SEGMENT_SIZE,
									 (offset % ORIOLEDB_SEGMENT_SIZE) / ORIOLEDB_S3_PART_SIZE);
		if (offset / ORIOLEDB_S3_PART_SIZE != (offset + amount - 1) / ORIOLEDB_S3_PART_SIZE)
			btree_smgr_schedule_s3_write(desc,
										 chkpNum,
										 (offset + amount - 1) / ORIOLEDB_SEGMENT_SIZE,
										 ((offset + amount - 1) % ORIOLEDB_SEGMENT_SIZE) / ORIOLEDB_S3_PART_SIZE);
	}

	pub static mut RESULT: return = std::mem::zeroed();
}

static int
btree_smgr_read(desc: &mut BTreeDescr, buffer: &mut char, uint32 chkpNum,
				int amount, off_t offset)
{
	pub static mut RESULT: std::os::raw::c_int = 0;
	pub static mut GRANULARITY: off_t = std::mem::zeroed();
	S3HeaderTag tag = {0};

	if (use_mmap)
	{
		Assert(offset + amount <= device_length);
		memcpy(buffer, mmap_data + offset, amount);
		pub static mut AMOUNT: return = std::mem::zeroed();
	}
	else if (use_device)
	{
		Assert(offset + amount <= device_length);
		pgstat_report_wait_start(WAIT_EVENT_DATA_FILE_READ);
		result = pg_pread(device_fd, buffer, amount, offset);
		pgstat_report_wait_end();
		pub static mut RESULT: return = std::mem::zeroed();
	}

	if (orioledb_s3_mode)
	{
		granularity = ORIOLEDB_S3_PART_SIZE;
		tag.key.oids = desc->oids;
		tag.key.tablespace = desc->tablespace;
		tag.checkpointNum = chkpNum;
	}
	else
	{
		granularity = ORIOLEDB_SEGMENT_SIZE;
	}

	while (amount > 0)
	{
		pub static mut SEGNO: std::os::raw::c_int = offset / ORIOLEDB_SEGMENT_SIZE;
		pub static mut PARTNO: std::os::raw::c_int = 0;
		pub static mut FILE: File = std::mem::zeroed();
		pub static mut LOAD_ID: uint32 = 0;

		if (orioledb_s3_mode)
		{
			tag.segNum = segno;
			partno = (offset % ORIOLEDB_SEGMENT_SIZE) / ORIOLEDB_S3_PART_SIZE;
			s3_header_lock_part(tag, partno, &loadId);
		}

		file = btree_open_smgr_file(desc, segno, chkpNum, loadId);
		if ((offset + amount) / granularity == offset / granularity)
		{
			result += OFileRead(file, buffer, amount,
								offset % ORIOLEDB_SEGMENT_SIZE + (orioledb_s3_mode ? ORIOLEDB_BLCKSZ : 0),
								WAIT_EVENT_DATA_FILE_READ);
			if (orioledb_s3_mode)
				s3_header_unlock_part(tag, partno, false);
			break;
		}
		else
		{
			pub static mut STEP_AMOUNT: std::os::raw::c_int = granularity - offset % granularity;

			Assert(amount >= stepAmount);
			result += OFileRead(file, buffer, stepAmount,
								offset % ORIOLEDB_SEGMENT_SIZE + (orioledb_s3_mode ? ORIOLEDB_BLCKSZ : 0),
								WAIT_EVENT_DATA_FILE_READ);
			buffer += stepAmount;
			offset += stepAmount;
			amount -= stepAmount;
		}

		if (orioledb_s3_mode)
			s3_header_unlock_part(tag, partno, false);
	}

	pub static mut RESULT: return = std::mem::zeroed();
}


btree_smgr_writeback(desc: &mut BTreeDescr, uint32 chkpNum,
					 off_t offset, int amount)
{
	if (use_mmap)
	{
		Assert(offset + amount <= device_length);
		msync(mmap_data + offset, amount, MS_ASYNC);
		return;
	}
	else if (use_device)
	{
		return;
	}

	while (amount > 0)
	{
		pub static mut SEGNO: std::os::raw::c_int = offset / ORIOLEDB_SEGMENT_SIZE;
		pub static mut FILE: File = std::mem::zeroed();
		pub static mut LOAD_ID: uint32 = 0;

		if (orioledb_s3_mode)
		{
			S3HeaderTag tag = {
				.key = {.oids = desc->oids,.tablespace = desc->tablespace},
				.checkpointNum = chkpNum,
			.segNum = segno};

			loadId = s3_header_get_load_id(tag);
		}

		file = btree_open_smgr_file(desc, segno, chkpNum, loadId);
		if ((offset + amount) / ORIOLEDB_SEGMENT_SIZE == segno)
		{
			FileWriteback(file,
						  offset % ORIOLEDB_SEGMENT_SIZE + (orioledb_s3_mode ? ORIOLEDB_BLCKSZ : 0),
						  amount, WAIT_EVENT_DATA_FILE_FLUSH);
			break;
		}
		else
		{
			pub static mut STEP_AMOUNT: std::os::raw::c_int = ORIOLEDB_SEGMENT_SIZE - offset % ORIOLEDB_SEGMENT_SIZE;

			Assert(amount >= stepAmount);
			FileWriteback(file,
						  offset % ORIOLEDB_SEGMENT_SIZE + (orioledb_s3_mode ? ORIOLEDB_BLCKSZ : 0),
						  stepAmount, WAIT_EVENT_DATA_FILE_FLUSH);
			offset += stepAmount;
			amount -= stepAmount;
		}
	}
}


btree_smgr_sync(desc: &mut BTreeDescr, uint32 chkpNum, off_t length)
{
	pub static mut NUM: std::os::raw::c_int = 0;

	if (orioledb_s3_mode)
		btree_s3_flush(desc, chkpNum);

	if (use_mmap || use_device)
		return;

	for (num = 0; num < length / ORIOLEDB_SEGMENT_SIZE; num++)
	{
		pub static mut FILE: File = std::mem::zeroed();
		pub static mut LOAD_ID: uint32 = 0;

		if (orioledb_s3_mode)
		{
			S3HeaderTag tag = {
				.key = {.oids = desc->oids,.tablespace = desc->tablespace},
				.checkpointNum = chkpNum,
			.segNum = num};

			loadId = s3_header_get_load_id(tag);
		}

		file = btree_open_smgr_file(desc, num, chkpNum, loadId);
		FileSync(file, WAIT_EVENT_DATA_FILE_SYNC);
	}
}

//
// Punch a hole in a raw OS file descriptor. Logs a WARNING on failure and
// returns; callers don't need to handle the return value because the data
// is being discarded either way.
//

punch_fd_hole(int fd, off_t offset, off_t length, const fileName: &mut char)
{
	pub static mut RET: std::os::raw::c_int = 0;

#ifdef __APPLE__
	{
		pub static mut HOLE: fpunchhole_t = std::mem::zeroed();

		memset(&hole, 0, sizeof(hole));
		hole.fp_offset = offset;
		hole.fp_length = length;
		ret = fcntl(fd, F_PUNCHHOLE, &hole);
	}
#else
	ret = fallocate(fd, FALLOC_FL_PUNCH_HOLE | FALLOC_FL_KEEP_SIZE,
					offset, length);
#endif
	if (ret < 0)
	{
		pub static mut SAVE_ERRNO: std::os::raw::c_int = errno;

		ereport(WARNING,
				(errcode_for_file_access(),
				 errmsg("could not punch hole in file %s offset=%lld length=%lld (%d %s)",
						fileName, (long long) offset, (long long) length,
						save_errno, strerror(save_errno))));
	}
}


btree_smgr_punch_hole(desc: &mut BTreeDescr, uint32 chkpNum,
					  off_t offset, int length)
{
	Assert(!orioledb_s3_mode && !use_mmap && !use_device);

	while (length > 0)
	{
		pub static mut FILE: File = std::mem::zeroed();
		pub static mut SEGNO: std::os::raw::c_int = offset / ORIOLEDB_SEGMENT_SIZE;
		pub static mut SEGOFFSET: off_t = std::mem::zeroed();
		pub static mut SEGLENGTH: std::os::raw::c_int = 0;

		file = btree_open_smgr_file(desc, segno, chkpNum, 0);

		segoffset = offset % ORIOLEDB_SEGMENT_SIZE;
		if ((offset + length) / ORIOLEDB_SEGMENT_SIZE == segno)
		{
			seglength = length;
			length = 0;
		}
		else
		{
			seglength = ORIOLEDB_SEGMENT_SIZE - segoffset;
			Assert(length >= seglength);

			offset += seglength;
			length -= seglength;
		}
		punch_fd_hole(FileGetRawDesc(file), segoffset, seglength,
					  FilePathName(file));
	}
}


btree_io_error_cleanup()
{
	if (io_in_progress)
		io_finish();
}


request_btree_io_lwlocks()
{
	num_io_lwlocks = max_procs * 4;
	RequestNamedLWLockTranche("orioledb_btree_io", num_io_lwlocks);
}


init_btree_io_lwlocks()
{
	io_locks = GetNamedLWLockTranche("orioledb_btree_io");
}

//
// Assign number of IO operation to particular (blkno; offnum) pair.
//
int
assign_io_num(OInMemoryBlkno blkno, OffsetNumber offnum)
{
	pub static mut LOCKNUM: std::os::raw::c_int = 0;
	pub static mut I: std::os::raw::c_int = 0;
	pub static mut CRC: pg_crc32c = std::mem::zeroed();

	INIT_CRC32C(crc);
	COMP_CRC32C(crc, &blkno, sizeof(blkno));
	COMP_CRC32C(crc, &offnum, sizeof(offnum));
	FIN_CRC32C(crc);

	locknum = crc % num_io_lwlocks;

	for (i = 0; i < num_io_lwlocks; i++)
	{
		if (LWLockConditionalAcquire(&io_locks[locknum].lock, LW_EXCLUSIVE))
			pub static mut LOCKNUM: return = std::mem::zeroed();
		locknum = (locknum + 1) % num_io_lwlocks;
	}

	LWLockAcquire(&io_locks[locknum].lock, LW_EXCLUSIVE);
	pub static mut LOCKNUM: return = std::mem::zeroed();
}

//
// Wait until particular IO operation is completed.
//

wait_for_io_completion(int ionum)
{
	LWLockAcquire(&io_locks[ionum].lock, LW_SHARED);
	LWLockRelease(&io_locks[ionum].lock);
}

//
// Report given IO operation to be finished.
//

unlock_io(int ionum)
{
	LWLockRelease(&io_locks[ionum].lock);
}

//
// Get next disk free offset for uncompressed on disk B-tree.
// Returns InvalidFileExtentOff if fails.
//
static uint64
get_free_disk_offset(desc: &mut BTreeDescr)
{
	metaPage: &mut BTreeMetaPage = BTREE_GET_META(desc);
	pub static mut LW_LOCK: *mut metaLock = &metaPage->metaLock;
	uint64		result,
				numFreeBlocks;
	pub static mut FREE_BUF_NUM: uint32 = std::mem::zeroed();
	pub static mut GOT_BLOCK: bool = false;

	Assert(!orioledb_s3_mode);

	//
// Switch to the next sequential buffer with free blocks numbers in
// needed.
//
	numFreeBlocks = pg_atomic_read_u64(&metaPage->numFreeBlocks);
	free_buf_num = metaPage->freeBuf.tag.num;
	while (numFreeBlocks == 0 &&
		   can_use_checkpoint_extents(desc, free_buf_num + 1))
	{
		SeqBufTag	tag = {0},
					old_tag = desc->freeBuf.shared->tag;
		pub static mut REPLACE_RESULT: SeqBufReplaceResult = std::mem::zeroed();

		if (orioledb_use_sparse_files)
		{
			try_to_punch_holes(desc);
			Assert(free_buf_num + 1 <= metaPage->punchHolesChkpNum);
		}

		tag.key.oids = desc->oids;
		tag.key.tablespace = desc->tablespace;
		tag.num = free_buf_num + 1;
		tag.type = 't';

		LWLockAcquire(metaLock, LW_EXCLUSIVE);
		replaceResult = seq_buf_try_replace(&desc->freeBuf,
											&tag,
											&metaPage->numFreeBlocks,
											use_device ? sizeof(FileExtent) : sizeof(uint32));
		if (replaceResult == SeqBufReplaceSuccess)
		{
			if (old_tag.type == 'm')
			{
				uint32		chkpNum = o_get_latest_chkp_num(tag.key.oids.datoid,
															tag.key.oids.relnode,
															checkpoint_state->lastCheckpointNumber,
															NULL);

				if (old_tag.num < chkpNum)
					seq_buf_remove_file(&old_tag);
			}
			else
			{
				Assert(old_tag.type == 't');
				if (!orioledb_use_sparse_files ||
					old_tag.num <= metaPage->punchHolesChkpNum)
					seq_buf_remove_file(&old_tag);
			}
		}
		LWLockRelease(metaLock);
		if (replaceResult == SeqBufReplaceError)
		{
			pub static mut INVALID_FILE_EXTENT_OFF: return = std::mem::zeroed();
		}
		// SeqBufReplaceAlready requires no action, just retry if needed

		numFreeBlocks = pg_atomic_read_u64(&metaPage->numFreeBlocks);
		free_buf_num = metaPage->freeBuf.tag.num;
	}

	//
// Try to get free block number from the buffer.  If not success, then
// extend the file.
//
	LWLockAcquire(metaLock, LW_SHARED);
	gotBlock = false;
	while (numFreeBlocks > 0)
	{
		if (pg_atomic_compare_exchange_u64(&metaPage->numFreeBlocks,
										   &numFreeBlocks,
										   numFreeBlocks - 1))
		{
			gotBlock = true;
			break;
		}
	}

	if (gotBlock)
	{

		if (use_device)
		{
			pub static mut EXTENT: FileExtent = std::mem::zeroed();

			if (seq_buf_read_file_extent(&desc->freeBuf, &extent))
				result = extent.off;
			else
				result = InvalidFileExtentOff;
		}
		else
		{
			pub static mut OFFSET: uint32 = std::mem::zeroed();

			if (seq_buf_read_u32(&desc->freeBuf, &offset))
				result = offset;
			else
				result = InvalidFileExtentOff;
		}
	}
	else
	{
		if (use_device)
			result = orioledb_device_alloc(desc, ORIOLEDB_BLCKSZ) / ORIOLEDB_COMP_BLCKSZ;
		else
			result = pg_atomic_fetch_add_u64(&metaPage->datafileLength[0], 1);
	}
	LWLockRelease(metaLock);
	pub static mut RESULT: return = std::mem::zeroed();
}

//
// Fills free file extent for B-tree.
//
// FileExtentIsValid(extent) == false if fails.
//
static bool
get_free_disk_extent(desc: &mut BTreeDescr, uint32 chkpNum,
					 off_t page_size, extent: &mut FileExtent)
{
	if (orioledb_s3_mode)
	{
		int			len = OCompressIsValid(desc->compress) ? FileExtentLen(page_size) : 1;
		int			threshold = ORIOLEDB_S3_PART_SIZE / (OCompressIsValid(desc->compress) ? ORIOLEDB_COMP_BLCKSZ : ORIOLEDB_BLCKSZ);
		metaPage: &mut BTreeMetaPage = BTREE_GET_META(desc);

		extent->off = pg_atomic_fetch_add_u64(&metaPage->datafileLength[chkpNum % 2], len);
		extent->len = len;

		if ((extent->off + threshold - 1) / threshold !=
			(extent->off + threshold - 1 + len) / threshold)
		{
			Assert((extent->off + threshold - 1) / threshold + 1 ==
				   (extent->off + threshold - 1 + len) / threshold);
			s3_headers_increase_loaded_parts(1);
		}

		extent->off |= (uint64) chkpNum << S3_CHKP_NUM_SHIFT;

		return FileExtentIsValid(*extent);
	}

	//
// User temporary trees maintain a pure backend-local free space map.
// Serve the allocation from that list first, falling back to extending
// the data file.  This avoids any dependency on checkpoint-tagged seq
// bufs.
//
	if (btree_desc_is_local_temp(desc))
	{
		metaPage: &mut BTreeMetaPage = BTREE_GET_META(desc);
		uint16		len = OCompressIsValid(desc->compress) ? FileExtentLen(page_size) : 1;

		if (!local_free_extents_pop(desc, len, extent))
		{
			extent->len = len;
			if (use_device)
				extent->off = orioledb_device_alloc(desc, len * ORIOLEDB_COMP_BLCKSZ) / ORIOLEDB_COMP_BLCKSZ;
			else
				extent->off = pg_atomic_fetch_add_u64(&metaPage->datafileLength[0], len);
		}
		return FileExtentIsValid(*extent);
	}

	if (!OCompressIsValid(desc->compress))
	{
		Assert(page_size == ORIOLEDB_BLCKSZ);

		extent->off = get_free_disk_offset(desc);
		extent->len = 1;
	}
	else
	{
		// Try to add free extents if we didn't manage to do after checkpoint
		add_free_extents_from_tmp(desc, remove_old_checkpoint_files);
		*extent = get_extent(desc, FileExtentLen(page_size));
	}

	return FileExtentIsValid(*extent);
}

//
// Fills free file extent for B-tree under copy blkno lock.
//
// FileExtentIsValid(extent) == false if fails.
//
static bool
get_free_disk_extent_copy_blkno(desc: &mut BTreeDescr, off_t page_size,
								extent: &mut FileExtent, uint32 checkpoint_number)
{
	metaPage: &mut BTreeMetaPage = BTREE_GET_META(desc);

	LWLockAcquire(&metaPage->copyBlknoLock, LW_SHARED);

	if (!get_free_disk_extent(desc, checkpoint_number, page_size, extent))
	{
		LWLockRelease(&metaPage->copyBlknoLock);
		pub static mut FALSE: return = std::mem::zeroed();
	}

	if ((desc->storageType == BTreeStoragePersistence || desc->storageType == BTreeStorageUnlogged) &&
		checkpoint_state->treeType == desc->type &&
		checkpoint_state->datoid == desc->oids.datoid &&
		checkpoint_state->relnode == desc->oids.relnode &&
		checkpoint_state->curKeyType != CurKeyFinished)
	{
		//
// We're writing to the next checkpoint, while current checkpoint is
// concurrently taking.  So, indicate this page is free in the
// checkpoint currently taking.  We have to take a lock in order to be
// sure that checkpoint map file will be finishing concurrently.
// Otherwise we might loose this block number.
//
		int			prev_chkp_index = (checkpoint_number - 1) % 2;
		pub static mut SUCCESS: bool = false;

		if (OCompressIsValid(desc->compress) || use_device)
		{
			success = seq_buf_write_file_extent(&desc->nextChkp[prev_chkp_index], *extent);
		}
		else
		{
			pub static mut OFFSET: uint32 = extent->off;

			Assert(offset < UINT32_MAX);
			success = seq_buf_write_u32(&desc->nextChkp[prev_chkp_index], offset);
		}

		if (!success)
		{
			LWLockRelease(&metaPage->copyBlknoLock);
			pub static mut FALSE: return = std::mem::zeroed();
		}
	}

	LWLockRelease(&metaPage->copyBlknoLock);

	return FileExtentIsValid(*extent);
}

#ifdef IS_DEV

// Functions for eviction_page_checkpoint_numbers test included under IS_DEV
PG_FUNCTION_INFO_V1(reset_read_page_checkpoint_stats);
PG_FUNCTION_INFO_V1(fetch_read_page_checkpoint_stats);

Datum
reset_read_page_checkpoint_stats(PG_FUNCTION_ARGS)
{
	min_read_page_checkpoint = UINT32_MAX;
	max_read_page_checkpoint = 0;
	PG_RETURN_VOID();
}

Datum
fetch_read_page_checkpoint_stats(PG_FUNCTION_ARGS)
{
	rsinfo: &mut ReturnSetInfo = (ReturnSetInfo *) fcinfo->resultinfo;
	bool		nulls[2] = {false};
	Datum		values[2];

	InitMaterializedSRF(fcinfo, 0);

	values[0] = UInt32GetDatum(min_read_page_checkpoint);
	values[1] = UInt32GetDatum(max_read_page_checkpoint);

	tuplestore_putvalues(rsinfo->setResult, rsinfo->setDesc, values, nulls);

	return (Datum) 0;
}

// Store checkpoint statistics for page reads for eviction_page_checkpoint_numbers test
fn
store_read_page_checkpoint_stats(uint32 checkpointNum)
{
	// Remember for checkpoint read test only
	max_read_page_checkpoint = Max(max_read_page_checkpoint, checkpointNum);
	min_read_page_checkpoint = Min(min_read_page_checkpoint, checkpointNum);
	elog(DEBUG1, "Remember read_page_checkpoin: min %u max %u", min_read_page_checkpoint, max_read_page_checkpoint);
}

#endif

//
// Now we have only one page version (1). When we have
// different versions we'll need to bump
// ORIOLEDB_PAGE_VERSION and implement on-the-fly conversion
// function from all previous page versions to use _after_
// decompression.
//
static bool
check_orioledb_page_version(OrioleDBOndiskPageHeader ondisk_page_header)
{
	if (ondisk_page_header.page_version != ORIOLEDB_PAGE_VERSION)
		elog(FATAL, "Page version %u of OrioleDB cluster is not among supported for conversion %u", ondisk_page_header.page_version, ORIOLEDB_PAGE_VERSION);

	pub static mut FALSE: return = std::mem::zeroed();
}

fn
convert_orioledb_page_version(Pointer img)
{
	Assert(ORIOLEDB_PAGE_VERSION == 1);
	elog(FATAL, "Page version conversion is not implemented");
}

//
// Now we have only one compresss version (1). When we have
// different versions we'll need to bump
// ORIOLEDB_COMPRESS_VERSION and add other variants of
// decompress function from all previous page versions in
// this function
//
static bool
check_orioledb_compress_version(OrioleDBOndiskPageHeader ondisk_page_header)
{
	if (ondisk_page_header.compress_version != ORIOLEDB_COMPRESS_VERSION)
		elog(FATAL, "Page version %u of OrioleDB cluster is not among supported for conversion %u", ondisk_page_header.compress_version, ORIOLEDB_PAGE_VERSION);

	pub static mut FALSE: return = std::mem::zeroed();
}

//
// Reads a page from disk to the img from a valid downlink. It's fills an empty
// array of offsets for the page.
//
bool
read_page_from_disk(desc: &mut BTreeDescr, Pointer img, uint64 downlink,
					extent: &mut FileExtent)
{
	off_t		byte_offset,
				read_size;
	uint64		offset = DOWNLINK_GET_DISK_OFF(downlink);
	pub static mut CHKP_NUM: uint32 = 0;
	uint16		len = DOWNLINK_GET_DISK_LEN(downlink);
	pub static mut ERR: bool = false;
	OrioleDBOndiskPageHeader ondisk_page_header = {0};
	pub static mut NEEDS_PAGE_VERSION_CONVERT: bool = false;

	Assert(FileExtentOffIsValid(offset));
	Assert(FileExtentLenIsValid(len));

	extent->off = offset;
	extent->len = len;

	if (orioledb_s3_mode)
	{
		chkpNum = S3_GET_CHKP_NUM(offset);
		offset &= S3_OFFSET_MASK;
	}

	if (!OCompressIsValid(desc->compress))
	{
		// easy case, read page from uncompressed index
		Assert(len == 1);

		if (use_device)
			byte_offset = (off_t) offset * (off_t) ORIOLEDB_COMP_BLCKSZ;
		else
			byte_offset = (off_t) offset * (off_t) ORIOLEDB_BLCKSZ;
		read_size = ORIOLEDB_BLCKSZ;

		err = btree_smgr_read(desc, img, chkpNum, read_size, byte_offset) != read_size;
		if (err)
			pub static mut FALSE: return = std::mem::zeroed();

		ondisk_page_header = *((OrioleDBOndiskPageHeader *) img);
		needs_page_version_convert = check_orioledb_page_version(ondisk_page_header);

		elog(DEBUG1, "Read plain disk page: checkpoint %u", ondisk_page_header.checkpointNum);
	}
	else
	{
		char		buf[ORIOLEDB_BLCKSZ];
		bool		compressed = len != (ORIOLEDB_BLCKSZ / ORIOLEDB_COMP_BLCKSZ);

		if (compressed)
		{
			pub static mut PG_USED_FOR_ASSERTS_ONLY: bool		needs_compress_version_convert = std::mem::zeroed();

			byte_offset = (off_t) offset * (off_t) ORIOLEDB_COMP_BLCKSZ;
			read_size = len * ORIOLEDB_COMP_BLCKSZ;

			err = btree_smgr_read(desc, buf, chkpNum, read_size, byte_offset) != read_size;
			if (err)
				pub static mut FALSE: return = std::mem::zeroed();

			ondisk_page_header = *((OrioleDBOndiskPageHeader *) buf);
			needs_page_version_convert = check_orioledb_page_version(ondisk_page_header);

			needs_compress_version_convert = check_orioledb_compress_version(ondisk_page_header);
			Assert(!needs_compress_version_convert);
			o_decompress_page(buf + O_PAGE_HEADER_SIZE, ondisk_page_header.compress_page_size, img);
			elog(DEBUG1, "Read disk page: checkpoint %u size %d", ondisk_page_header.checkpointNum, ondisk_page_header.compress_page_size);

			//
// Decompressed page has its own OrioleDBPageHeader with the same
// checkpointNum as is external OrioleDBOndiskPageHeader. It is
// redundant and unused, just check it.
//
			Assert(((BTreePageHeader *) img)->o_header.checkpointNum == ondisk_page_header.checkpointNum);
		}
		else
		{
			byte_offset = (off_t) offset * (off_t) ORIOLEDB_COMP_BLCKSZ;
			read_size = O_PAGE_HEADER_SIZE;

			// details about written image parts are in write_page_to_disk
			err = btree_smgr_read(desc, (Pointer) &ondisk_page_header, chkpNum, read_size, byte_offset) != read_size;
			byte_offset += read_size;

			if (err)
				pub static mut FALSE: return = std::mem::zeroed();

			read_size = ORIOLEDB_BLCKSZ - O_PAGE_HEADER_SIZE;
			err = btree_smgr_read(desc, img + O_PAGE_HEADER_SIZE, chkpNum, read_size, byte_offset) != read_size;
			if (err)
				pub static mut FALSE: return = std::mem::zeroed();

			needs_page_version_convert = check_orioledb_page_version(ondisk_page_header);
			elog(DEBUG1, "Read disk page: checkpoint %u size %d", ondisk_page_header.checkpointNum, ORIOLEDB_BLCKSZ);
		}
	}

	//
// At this point, page is fully read and decompressed. Do conversion of
// needed data from OrioleDBOndiskPageHeader to OrioleDBPageHeader. Do
// conversion of page version (not implemented yet);
//
	Assert(!err);

	if (needs_page_version_convert)
		convert_orioledb_page_version(img);

	//
// Convert needed data from OrioleDBOndiskPageHeader to
// OrioleDBPageHeader. Erase what's unused to be safe.
//
	memset(img, 0, O_PAGE_HEADER_SIZE);
	((BTreePageHeader *) img)->o_header.checkpointNum = ondisk_page_header.checkpointNum;

#ifdef IS_DEV
	// For eviction/page checkpoint number test
	store_read_page_checkpoint_stats(((BTreePageHeader *) img)->o_header.checkpointNum);
#endif

	pub static mut TRUE: return = std::mem::zeroed();
}

//
// Writes a page to the disk. An array of file offsets must be valid.
//
static bool
write_page_to_disk(desc: &mut BTreeDescr, extent: &mut FileExtent, uint32 curChkpNum,
				   Pointer page, off_t page_size)
{

	off_t		byte_offset,
				write_size;
	pub static mut ERR: bool = false;
	pub static mut CHKP_NUM: uint32 = 0;
	char		buf[ORIOLEDB_BLCKSZ];

	Assert(FileExtentOffIsValid(extent->off));

	byte_offset = (off_t) extent->off;

	if (orioledb_s3_mode)
	{
		chkpNum = S3_GET_CHKP_NUM(byte_offset);
		byte_offset &= S3_OFFSET_MASK;
	}

	if (!OCompressIsValid(desc->compress))
	{
		pub static mut ORIOLE_DB_ONDISK_PAGE_HEADER: *mut ondisk_page_header = std::ptr::null_mut();

		//
// Easy case, write whole page to uncompressed index.
//
		Assert(extent->len == 1);
		Assert(page_size == ORIOLEDB_BLCKSZ);

		if (use_device)
			byte_offset *= (off_t) ORIOLEDB_COMP_BLCKSZ;
		else
			byte_offset *= (off_t) ORIOLEDB_BLCKSZ;
		write_size = ORIOLEDB_BLCKSZ;

		memset(buf, 0, O_PAGE_HEADER_SIZE);
		ondisk_page_header = (OrioleDBOndiskPageHeader *) buf;
		ondisk_page_header->checkpointNum = curChkpNum;
		ondisk_page_header->page_version = ORIOLEDB_PAGE_VERSION;
		memcpy(&buf[O_PAGE_HEADER_SIZE], page + O_PAGE_HEADER_SIZE, ORIOLEDB_BLCKSZ - O_PAGE_HEADER_SIZE);

		err = btree_smgr_write(desc, buf, chkpNum, write_size, byte_offset) != write_size;

		elog(DEBUG1, "Wrote plain disk page: checkpoint %u", curChkpNum);
	}
	else
	{
		OrioleDBOndiskPageHeader ondisk_page_header = {0};

		byte_offset *= (off_t) ORIOLEDB_COMP_BLCKSZ;

		//
// overflow protection
//
		Assert(sizeof(((OrioleDBOndiskPageHeader *) 0)->compress_page_size) == sizeof(uint16));
		Assert(ORIOLEDB_BLCKSZ < UINT16_MAX);

		// Write header first
		ondisk_page_header.compress_page_size = page_size;
		ondisk_page_header.checkpointNum = curChkpNum;
		ondisk_page_header.compress_version = ORIOLEDB_COMPRESS_VERSION;
		ondisk_page_header.page_version = ORIOLEDB_PAGE_VERSION;

		write_size = O_PAGE_HEADER_SIZE;
		err = btree_smgr_write(desc, (char *) &ondisk_page_header, chkpNum, write_size, byte_offset) != write_size;
		byte_offset += write_size;

		if (err)
			pub static mut FALSE: return = std::mem::zeroed();

		// Write everything left except header, which is already written
		if (page_size != ORIOLEDB_BLCKSZ)
		{
			//
// Compressed chunks don't have external header, just make up for
// length
//
			write_size = extent->len * ORIOLEDB_COMP_BLCKSZ - O_PAGE_HEADER_SIZE;
			err = btree_smgr_write(desc, page, chkpNum, write_size, byte_offset) != write_size;
		}
		else
		{
			//
// For non-compresses page cut already written header and make up
// for length
//
			page += O_PAGE_HEADER_SIZE;
			write_size = ORIOLEDB_BLCKSZ - O_PAGE_HEADER_SIZE;
			err = btree_smgr_write(desc, page, chkpNum, write_size, byte_offset) != write_size;
		}

		elog(DEBUG1, "Wrote disk page: checkpoint %u size %d", curChkpNum, (int) page_size);

	}

	return !err;
}

//
// Load the page where context is pointing from disk to memory, assuming parent
// page is locked.
//

load_page(context: &mut OBTreeFindPageContext)
{
	pub static mut ORIOLE_DB_PAGE_DESC: *mut page_desc = std::ptr::null_mut();
	pub static mut B_TREE_DESCR: *mut desc = context->desc;
	pub static mut PARENT_BLKNO: OInMemoryBlkno = std::mem::zeroed();
	pub static mut PARENT_PAGE: Page = std::mem::zeroed();
	pub static mut B_TREE_PAGE_ITEM_LOCATOR: *mut parent_loc = std::ptr::null_mut();
	pub static mut CSN: CommitSeqNo = std::mem::zeroed();
	pub static mut DOWNLINK: uint64 = std::mem::zeroed();
	int			context_index,
				ionum;
	pub static mut PARENT_CHANGE_COUNT: uint32 = std::mem::zeroed();
	pub static mut B_TREE_NON_LEAF_TUPHDR: *mut int_hdr = std::ptr::null_mut();
	pub static mut BLKNO: OInMemoryBlkno = std::mem::zeroed();
	pub static mut TARGET_HIKEY: OFixedKey = std::mem::zeroed();
	pub static mut TARGET_LEVEL: std::os::raw::c_int = 0;
	pub static mut PAGE: Page = std::mem::zeroed();
	char		buf[ORIOLEDB_BLCKSZ];
	pub static mut WAS_MODIFY: bool = false;
	pub static mut WAS_DOWNLINK_LOCATION: bool = false;
	pub static mut WAS_FETCH: bool = false;
	pub static mut WAS_IMAGE: bool = false;
	pub static mut WAS_KEEP_LOKEY: bool = false;
	pub static mut CHKP_NUM: uint32 = 0;

	context_index = context->index;
	parent_blkno = context->items[context_index].blkno;
	parent_loc = &context->items[context_index].locator;
	parent_change_count = context->items[context_index].pageChangeCount;
	parent_page = O_GET_IN_MEMORY_PAGE(parent_blkno);

	ionum = assign_io_num(parent_blkno, BTREE_PAGE_LOCATOR_GET_OFFSET(parent_page, parent_loc));

	// Modify parent downlink: indicate that IO is in-progress
	page_block_reads(parent_blkno);
	int_hdr = (BTreeNonLeafTuphdr *) BTREE_PAGE_LOCATOR_GET_ITEM(parent_page, parent_loc);
	Assert(DOWNLINK_IS_ON_DISK(int_hdr->downlink));

	downlink = int_hdr->downlink;

	int_hdr->downlink = MAKE_IO_DOWNLINK(ionum);
	Assert(PAGE_GET_N_ONDISK(parent_page) > 0);
	PAGE_DEC_N_ONDISK(parent_page);

	BTREE_PAGE_LOCATOR_NEXT(parent_page, parent_loc);
	if (BTREE_PAGE_LOCATOR_IS_VALID(parent_page, parent_loc))
		copy_fixed_page_key(desc, &target_hikey, parent_page, parent_loc);
	else if (!O_PAGE_IS(parent_page, RIGHTMOST))
		copy_fixed_hikey(desc, &target_hikey, parent_page);
	else
		clear_fixed_key(&target_hikey);
	target_level = PAGE_GET_LEVEL(parent_page) - 1;

	unlock_page(parent_blkno);

	// Prepare new page metaPage-data
	ppool_reserve_pages(desc->ppool, PPOOL_RESERVE_FIND, 1);
	blkno = ppool_alloc_page(desc->ppool, PPOOL_RESERVE_FIND);
	lock_page(blkno);
	page_block_reads(blkno);

	Assert(OInMemoryBlknoIsValid(blkno));
	page = O_GET_IN_MEMORY_PAGE(blkno);
	page_desc = O_GET_IN_MEMORY_PAGEDESC(blkno);

	page_desc->flags = 0;

	// Read page data and put it to the page
	if (!read_page_from_disk(desc, buf, downlink, &page_desc->fileExtent))
	{
		int_hdr->downlink = downlink;
		PAGE_INC_N_ONDISK(parent_page);
		unlock_io(ionum);
		if (orioledb_s3_mode)
			chkpNum = S3_GET_CHKP_NUM(page_desc->fileExtent.off);

		ereport(ERROR, (errcode_for_file_access(),
						errmsg("could not read page with file offset " UINT64_FORMAT " from %s: %m",
							   DOWNLINK_GET_DISK_OFF(downlink),
							   btree_smgr_filename(desc, DOWNLINK_GET_DISK_OFF(downlink), chkpNum))));
	}

	put_page_image(blkno, buf);
	ppool_ucm_init(desc->ppool, blkno);

	//
// Stamp the page's identity from the descriptor of the tree we are
// descending, not from the parent's page descriptor: the parent was
// unlocked above and may have been evicted/reused (potentially by the
// reentrant eviction inside ppool_alloc_page() just above), in which case
// its page descriptor's oids would be invalid (0,0,0) or belong to
// another tree.  The loaded page belongs to desc, so use desc's oids/type
// -- which is exactly what init_new_btree_page() does.
//
	page_desc->type = desc->type;
	page_desc->oids = desc->oids;

	Assert(O_PAGE_IS(page, LEAF) ||
		   (PAGE_GET_N_ONDISK(page) == BTREE_PAGE_ITEMS_COUNT(page)));

	if (orioledb_s3_mode && !O_PAGE_IS(page, LEAF))
	{
		pub static mut LOC: BTreePageItemLocator = std::mem::zeroed();

		//
// In S3 mode schedule load of all the page children for faster
// warmup.
//
		BTREE_PAGE_FOREACH_ITEMS(page, &loc)
		{
			pub static mut B_TREE_NON_LEAF_TUPHDR: *mut tupHdr = std::ptr::null_mut();

			tupHdr = (BTreeNonLeafTuphdr *) BTREE_PAGE_LOCATOR_GET_ITEM(page, &loc);
			() s3_schedule_downlink_load(desc, tupHdr->downlink);
		}
	}

	unlock_page(blkno);

	EA_LOAD_INC(blkno);

	if (STOPEVENTS_ENABLED())
	{
		pub static mut JSONB: *mut params = std::ptr::null_mut();

		params = btree_page_stopevent_params(desc, page);
		STOPEVENT(STOPEVENT_LOAD_PAGE_REFIND, params);
	}

	// re-find parent page (it might be changed due to concurrent operations)
	csn = context->csn;
	was_modify = BTREE_PAGE_FIND_IS(context, MODIFY);
	was_image = BTREE_PAGE_FIND_IS(context, IMAGE);
	BTREE_PAGE_FIND_UNSET(context, IMAGE);
	if (!was_modify)
	{
		was_fetch = BTREE_PAGE_FIND_IS(context, FETCH);
		Assert(was_fetch || was_image);
		BTREE_PAGE_FIND_UNSET(context, FETCH);
		BTREE_PAGE_FIND_SET(context, MODIFY);
	}
	was_keep_lokey = BTREE_PAGE_FIND_IS(context, KEEP_LOKEY);
	if (was_keep_lokey)
		BTREE_PAGE_FIND_UNSET(context, KEEP_LOKEY);
	was_downlink_location = BTREE_PAGE_FIND_IS(context, DOWNLINK_LOCATION);
	if (!was_downlink_location)
		BTREE_PAGE_FIND_SET(context, DOWNLINK_LOCATION);
	context->csn = COMMITSEQNO_INPROGRESS;
	if (PAGE_GET_LEVEL(page) != target_level)
		ereport(PANIC, (errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
						errmsg("error reading downlink %X/%X in relfile (%u, %u)",
							   (uint32) (downlink >> 32), (uint32) (downlink),
							   desc->oids.datoid, desc->oids.relnode),
						errdetail("Level mismatch, expected: %d, found: %d",
								  PAGE_GET_LEVEL(page), target_level)));

	if (O_PAGE_IS(page, RIGHTMOST))
	{
		pub static mut PG_USED_FOR_ASSERTS_ONLY: OFindPageResult result = std::mem::zeroed();

		if (!O_TUPLE_IS_NULL(target_hikey.tuple))
			ereport(PANIC, (errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
							errmsg("error reading downlink %X/%X in relfile (%u, %u)",
								   (uint32) (downlink >> 32), (uint32) (downlink),
								   desc->oids.datoid, desc->oids.relnode),
							errdetail("Hikeys don't match.")));
		result = refind_page(context, NULL, BTreeKeyRightmost,
							 PAGE_GET_LEVEL(page) + 1,
							 parent_blkno, parent_change_count);
		Assert(result == OFindPageResultSuccess);
	}
	else
	{
		pub static mut HIKEY: OTuple = std::mem::zeroed();
		pub static mut PG_USED_FOR_ASSERTS_ONLY: OFindPageResult result = std::mem::zeroed();

		BTREE_PAGE_GET_HIKEY(hikey, page);

		if (O_TUPLE_IS_NULL(target_hikey.tuple) ||
			o_btree_cmp(desc, &hikey, BTreeKeyNonLeafKey, &target_hikey, BTreeKeyNonLeafKey) != 0)
			ereport(PANIC, (errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
							errmsg("error reading downlink %X/%X in relfile (%u, %u)",
								   (uint32) (downlink >> 32), (uint32) (downlink),
								   desc->oids.datoid, desc->oids.relnode),
							errdetail("Hikeys don't match.")));
		result = refind_page(context, &hikey, BTreeKeyPageHiKey,
							 PAGE_GET_LEVEL(page) + 1, parent_blkno,
							 parent_change_count);
		Assert(result == OFindPageResultSuccess);
	}

	// restore context state
	context->csn = csn;
	if (!was_modify)
	{
		if (was_fetch)
			BTREE_PAGE_FIND_SET(context, FETCH);
		BTREE_PAGE_FIND_UNSET(context, MODIFY);
	}
	if (was_image)
		BTREE_PAGE_FIND_SET(context, IMAGE);
	if (was_keep_lokey)
		BTREE_PAGE_FIND_SET(context, KEEP_LOKEY);
	if (!was_downlink_location)
		BTREE_PAGE_FIND_UNSET(context, DOWNLINK_LOCATION);

	context_index = context->index;
	parent_blkno = context->items[context_index].blkno;
	parent_loc = &context->items[context_index].locator;
	parent_change_count = context->items[context_index].pageChangeCount;

	// Replace parent downlink with orioledb downlink
	page_block_reads(parent_blkno);
	parent_page = O_GET_IN_MEMORY_PAGE(parent_blkno);
	int_hdr = (BTreeNonLeafTuphdr *) BTREE_PAGE_LOCATOR_GET_ITEM(parent_page, parent_loc);
	Assert(int_hdr->downlink == MAKE_IO_DOWNLINK(ionum));
	int_hdr->downlink = MAKE_IN_MEMORY_DOWNLINK(blkno, O_PAGE_HEADER(page)->pageChangeCount);

	unlock_io(ionum);
}

//
// Returns pointer to writable image. It compresses page if needed.
//
static inline Pointer
get_write_img(desc: &mut BTreeDescr, Page page, size: &mut size_t)
{
	pub static mut RESULT: Pointer = std::ptr::null_mut();

	if (OCompressIsValid(desc->compress))
	{
		result = o_compress_page(page, size, desc->compress);
		if (*size > (ORIOLEDB_BLCKSZ - ORIOLEDB_COMP_BLCKSZ - O_PAGE_HEADER_SIZE))
		{
			//
// No sense to write compressed page
//
			result = page;
			*size = ORIOLEDB_BLCKSZ;
		}
	}
	else
	{
		result = page;
		*size = ORIOLEDB_BLCKSZ;
	}
	pub static mut RESULT: return = std::mem::zeroed();
}

#ifdef USE_ASSERT_CHECKING
fn
prewrite_image_check(Page p)
{
	if (!O_PAGE_IS(p, LEAF))
	{
		pub static mut LOC: BTreePageItemLocator = std::mem::zeroed();

		BTREE_PAGE_FOREACH_ITEMS(p, &loc)
		{
			tuphdr: &mut BTreeNonLeafTuphdr = (BTreeNonLeafTuphdr *) BTREE_PAGE_LOCATOR_GET_ITEM(p, &loc);

			Assert(DOWNLINK_IS_ON_DISK(tuphdr->downlink));
		}
	}
}
#endif

//
// Returns downlink to the page or InvalidDiskDownlink if fails.
//
uint64
perform_page_io(desc: &mut BTreeDescr, OInMemoryBlkno blkno,
				Page img, uint32 checkpoint_number, bool copy_blkno,
				dirty_parent: &mut bool)
{
	Page		page = O_GET_IN_MEMORY_PAGE(blkno);
	header: &mut BTreePageHeader = (BTreePageHeader *) page;
	page_desc: &mut OrioleDBPageDesc = O_GET_IN_MEMORY_PAGEDESC(blkno);
	pub static mut WRITE_IMG: Pointer = std::ptr::null_mut();
	pub static mut WRITE_SIZE: size_t = std::mem::zeroed();
	pub static mut CHKP_INDEX: std::os::raw::c_int = 0;
	bool		less_num,
				err = false;

#ifdef USE_ASSERT_CHECKING
	prewrite_image_check(img);
#endif

	EA_WRITE_INC(blkno);

	less_num = header->o_header.checkpointNum < checkpoint_number;
	if (less_num)
	{
		//
// Page wasn't yet written during given checkpoint, so we have to
// relocate it in order to implement copy-on-write checkpointing.
//
		if ((uintptr_t) page != (uintptr_t) img)
		{
			//
// we need to update the written checkpoint number for the img too
//
			header = (BTreePageHeader *) img;
			header->o_header.checkpointNum = checkpoint_number;
			header = (BTreePageHeader *) page;
		}
		header->o_header.checkpointNum = checkpoint_number;
	}
	else
	{
		Assert(header->o_header.checkpointNum == checkpoint_number);
	}

	write_img = get_write_img(desc, img, &write_size);

	//
// Determine the file position to write this page.
//
	chkp_index = checkpoint_number % 2;
	if (orioledb_s3_mode)
	{
		if (less_num)
		{
			err = !get_free_disk_extent(desc, checkpoint_number, write_size, &page_desc->fileExtent);
			*dirty_parent = true;
		}
		else
		{
			if (!OCompressIsValid(desc->compress))
			{
				// easy case: no dirty_parent: &mut compression = false;
			}
			else
			{
				uint16		old_len = page_desc->fileExtent.len,
							new_len = FileExtentLen(write_size);

				if (old_len < new_len)
				{
					err = !get_free_disk_extent(desc, checkpoint_number, write_size, &page_desc->fileExtent);
					*dirty_parent = true;
				}
				else if (old_len > new_len)
				{
					page_desc->fileExtent.len = new_len;
					*dirty_parent = true;
				}
				else
				{
					*dirty_parent = false;
				}
			}
		}
	}
	else if (less_num)
	{
		//
// Page wasn't yet written during given checkpoint, so we have to
// relocate it in order to implement copy-on-write checkpointing.
//

		if (FileExtentIsValid(page_desc->fileExtent))
		{
#ifdef USE_ASSERT_CHECKING

			//
// Shared seq_bufs should be initialized by checkpointer.  User
// temporary trees keep their own backend-local free space map and
// do not use these shared buffers at all; system trees that
// happen to be BTreeStorageTemporary still share a pool and only
// skip the nextChkp assertion (no .map file).
//
			if (!btree_desc_is_local_temp(desc))
			{
				if (desc->storageType != BTreeStorageTemporary)
				{
					SpinLockAcquire(&desc->nextChkp[chkp_index].shared->lock);
					Assert(desc->nextChkp[chkp_index].shared->tag.num == checkpoint_number);
					SpinLockRelease(&desc->nextChkp[chkp_index].shared->lock);
				}
				SpinLockAcquire(&desc->tmpBuf[chkp_index].shared->lock);
				Assert(desc->tmpBuf[chkp_index].shared->tag.num == checkpoint_number);
				SpinLockRelease(&desc->tmpBuf[chkp_index].shared->lock);
			}
#endif
			free_extent_for_checkpoint(desc, &page_desc->fileExtent, checkpoint_number);
		}

		// Get free disk page to locate new page image
		if (copy_blkno)
		{
			err = !get_free_disk_extent_copy_blkno(desc, write_size,
												   &page_desc->fileExtent,
												   checkpoint_number);
		}
		else
		{
			err = !get_free_disk_extent(desc, checkpoint_number, write_size, &page_desc->fileExtent);
		}

		*dirty_parent = true;
	}
	else
	{
		//
// Has been already written during given checkpoint, so rewrite page
// in-place.
//
		Assert(FileExtentIsValid(page_desc->fileExtent));
		if (!OCompressIsValid(desc->compress))
		{
			// easy case: no dirty_parent: &mut compression = false;
		}
		else
		{
			uint16		old_len = page_desc->fileExtent.len,
						new_len = FileExtentLen(write_size);

			//
// check: is current image take as much space as previous written
// page?
//
			if (old_len < new_len)
			{
				free_extent_for_checkpoint(desc, &page_desc->fileExtent, checkpoint_number);
				// allocate more file blocks
				if (copy_blkno)
				{
					err = !get_free_disk_extent_copy_blkno(desc, write_size,
														   &page_desc->fileExtent,
														   checkpoint_number);
				}
				else
				{
					err = !get_free_disk_extent(desc, checkpoint_number,
												write_size, &page_desc->fileExtent);
				}
			}
			else if (old_len > new_len)
			{
				//
// free space
//
				pub static mut FREE_EXTENT: FileExtent = std::mem::zeroed();

				free_extent.len = page_desc->fileExtent.len - new_len;
				free_extent.off = page_desc->fileExtent.off + new_len;

				if (!seq_buf_write_file_extent(&desc->nextChkp[chkp_index], free_extent) ||
					!seq_buf_write_file_extent(&desc->tmpBuf[chkp_index], free_extent))
				{
					err = true;
				}
				page_desc->fileExtent.len = new_len;
			}

			*dirty_parent = old_len != new_len;
		}
	}

	if (err)
	{
		ereport(PANIC, (errcode_for_file_access(),
						errmsg("could not (re) allocate file blocks for page %d to file %s: %m",
							   blkno, btree_smgr_filename(desc, 0, checkpoint_number))));
	}

	Assert(FileExtentIsValid(page_desc->fileExtent));

	if (!write_page_to_disk(desc, &page_desc->fileExtent, checkpoint_number, write_img, write_size))
	{
		ereport(PANIC, (errcode_for_file_access(),
						errmsg("could not write page %d to file %s with offset %lu: %m",
							   blkno,
							   btree_smgr_filename(desc, page_desc->fileExtent.off, checkpoint_number),
							   (unsigned long) page_desc->fileExtent.off)));

		pub static mut INVALID_DISK_DOWNLINK: return = std::mem::zeroed();
	}

	Assert(FileExtentIsValid(page_desc->fileExtent));
	return MAKE_ON_DISK_DOWNLINK(page_desc->fileExtent);
}

//
// Performs page write for autonomous checkpoint images.
//
// Returns downlink to the page.
//
uint64
perform_page_io_autonomous(desc: &mut BTreeDescr, uint32 chkpNum, Page img, extent: &mut FileExtent)
{
	pub static mut WRITE_IMG: Pointer = std::ptr::null_mut();
	pub static mut WRITE_SIZE: size_t = std::mem::zeroed();

#ifdef USE_ASSERT_CHECKING
	prewrite_image_check(img);
#endif

	write_img = get_write_img(desc, img, &write_size);

	if (!get_free_disk_extent(desc, chkpNum, write_size, extent))
	{
		ereport(PANIC, (errcode_for_file_access(),
						errmsg("could not get free file offset for write page to file %s: %m",
							   btree_smgr_filename(desc, 0, 0))));

		pub static mut INVALID_DISK_DOWNLINK: return = std::mem::zeroed();
	}

	Assert(FileExtentIsValid(*extent));

	if (!write_page_to_disk(desc, extent, chkpNum, write_img, write_size))
	{
		pub static mut OFFSET: uint64 = std::mem::zeroed();

		if (orioledb_s3_mode)
		{
			offset = extent->off & S3_OFFSET_MASK;
			chkpNum = S3_GET_CHKP_NUM(extent->off);
		}
		else
		{
			offset = extent->off;
			chkpNum = 0;
		}

		ereport(PANIC, (errcode_for_file_access(),
						errmsg("could not write autonomous page to file %s with offset %lu: %m",
							   btree_smgr_filename(desc, offset, chkpNum),
							   (unsigned long) offset)));

		pub static mut INVALID_DISK_DOWNLINK: return = std::mem::zeroed();
	}

	Assert(FileExtentIsValid(*extent));
	return MAKE_ON_DISK_DOWNLINK(*extent);
}

//
// Performs page write for tree build.
//
// Returns downlink to the page.
//
uint64
perform_page_io_build(desc: &mut BTreeDescr, Page img,
					  extent: &mut FileExtent, metaPage: &mut BTreeMetaPage)
{
	pub static mut WRITE_IMG: Pointer = std::ptr::null_mut();
	pub static mut WRITE_SIZE: size_t = std::mem::zeroed();
	pub static mut CHKP_NUM: uint32 = std::mem::zeroed();

	btree_page_update_max_key_len(desc, img);

#ifdef USE_ASSERT_CHECKING
	prewrite_image_check(img);
#endif

	write_img = get_write_img(desc, img, &write_size);

	if (orioledb_s3_mode)
		chkpNum = checkpoint_state->lastCheckpointNumber;
	else
		chkpNum = 0;

	if (!OCompressIsValid(desc->compress))
	{
		Assert(write_size == ORIOLEDB_BLCKSZ);

		extent->len = 1;
		if (use_device)
			extent->off = orioledb_device_alloc(desc, ORIOLEDB_BLCKSZ) / ORIOLEDB_COMP_BLCKSZ;
		else
			extent->off = pg_atomic_fetch_add_u64(&metaPage->datafileLength[chkpNum % 2], 1);
	}
	else
	{
		extent->len = FileExtentLen(write_size);
		if (use_device)
			extent->off = orioledb_device_alloc(desc, ORIOLEDB_BLCKSZ) / ORIOLEDB_COMP_BLCKSZ;
		else
			extent->off = pg_atomic_fetch_add_u64(&metaPage->datafileLength[chkpNum % 2], extent->len);
	}

	if (orioledb_s3_mode)
	{
		int			threshold = ORIOLEDB_S3_PART_SIZE / (OCompressIsValid(desc->compress) ? ORIOLEDB_COMP_BLCKSZ : ORIOLEDB_BLCKSZ);

		if ((extent->off + threshold - 1) / threshold !=
			(extent->off + threshold - 1 + extent->len) / threshold)
		{
			pub static mut TAG: S3HeaderTag = std::mem::zeroed();
			uint64		offset = (extent->off + extent->len - 1) * (OCompressIsValid(desc->compress) ? ORIOLEDB_COMP_BLCKSZ : ORIOLEDB_BLCKSZ);
			pub static mut INDEX: std::os::raw::c_int = 0;

			Assert((extent->off + threshold - 1) / threshold + 1 ==
				   (extent->off + threshold - 1 + extent->len) / threshold);

			tag.key.oids = desc->oids;
			tag.key.tablespace = desc->tablespace;
			tag.checkpointNum = chkpNum;
			tag.segNum = offset / ORIOLEDB_SEGMENT_SIZE;
			index = (offset % ORIOLEDB_SEGMENT_SIZE) / ORIOLEDB_S3_PART_SIZE;
			s3_header_mark_part_loading(tag, index);
			s3_header_mark_part_loaded(tag, index);
			s3_headers_increase_loaded_parts(1);
		}

		extent->off |= (uint64) chkpNum << S3_CHKP_NUM_SHIFT;
	}

	Assert(FileExtentIsValid(*extent));

	if (!write_page_to_disk(desc, extent, 0, write_img, write_size))
	{
		ereport(PANIC, (errcode_for_file_access(),
						errmsg("could not write autonomous page to file %s with offset %lu: %m",
							   btree_smgr_filename(desc, extent[0].off, chkpNum),
							   (unsigned long) extent[0].off)));

		pub static mut INVALID_DISK_DOWNLINK: return = std::mem::zeroed();
	}

	Assert(FileExtentIsValid(*extent));
	return MAKE_ON_DISK_DOWNLINK(*extent);
}

//
// Prepare internal page for writing to disk.
//
static bool
prepare_non_leaf_page(Page p)
{
	pub static mut LOC: BTreePageItemLocator = std::mem::zeroed();

	BTREE_PAGE_FOREACH_ITEMS(p, &loc)
	{
		tuphdr: &mut BTreeNonLeafTuphdr = (BTreeNonLeafTuphdr *) BTREE_PAGE_LOCATOR_GET_ITEM(p, &loc);

		if (DOWNLINK_IS_IN_IO(tuphdr->downlink))
			pub static mut FALSE: return = std::mem::zeroed();

		if (DOWNLINK_IS_IN_MEMORY(tuphdr->downlink))
		{
			OInMemoryBlkno child = DOWNLINK_GET_IN_MEMORY_BLKNO(tuphdr->downlink);
			desc: &mut OrioleDBPageDesc = O_GET_IN_MEMORY_PAGEDESC(child);

			if (!try_lock_page(child))
				pub static mut FALSE: return = std::mem::zeroed();

			//
// It's worth less to write non-leaf page, if it's going to anyway
// become dirty after writing of child.
//
			if (IS_DIRTY(child) || desc->ionum >= 0)
			{
				unlock_page(child);
				pub static mut FALSE: return = std::mem::zeroed();
			}

			// XXX: should we also consider checkpoint number of child page?
			Assert(FileExtentIsValid(desc->fileExtent));
			tuphdr->downlink = MAKE_ON_DISK_DOWNLINK(desc->fileExtent);
			unlock_page(child);
		}
	}

	PAGE_SET_N_ONDISK(p, BTREE_PAGE_ITEMS_COUNT(p));
	pub static mut TRUE: return = std::mem::zeroed();
}

//
// Evict the page, assuming target page and its parent are locked.
//
fn
write_page(context: &mut OBTreeFindPageContext, OInMemoryBlkno blkno, Page img,
		   uint32 checkpoint_number,
		   bool evict, bool copy_blkno)
{
	pub static mut B_TREE_DESCR: *mut desc = context->desc;
	pub static mut PARENT_BLKNO: OInMemoryBlkno = OInvalidInMemoryBlkno;
	pub static mut PARENT_PAGE: Page = std::ptr::null_mut();
	Page		p = O_GET_IN_MEMORY_PAGE(blkno);
	pub static mut B_TREE_PAGE_ITEM_LOCATOR: *mut parent_loc = std::ptr::null_mut();
	int			ionum = -1,
				context_index;
	pub static mut B_TREE_NON_LEAF_TUPHDR: *mut int_hdr = std::ptr::null_mut();
	pub static mut PARENT_CHANGE_COUNT: uint32 = 0;
	page_desc: &mut OrioleDBPageDesc = O_GET_IN_MEMORY_PAGEDESC(blkno);
	pub static mut IS_ROOT: bool = desc->rootInfo.rootPageBlkno == blkno;

	// rootPageBlkno can not be evicted here
	Assert(!evict || !is_root);
	Assert(OInMemoryBlknoIsValid(desc->rootInfo.rootPageBlkno));
	Assert(page_is_locked(blkno) || O_PAGE_IS_LOCAL(blkno));
	EA_EVICT_INC(blkno);

	if (!is_root)
	{
		context_index = context->index;
		parent_blkno = context->items[context_index].blkno;
		parent_loc = &context->items[context_index].locator;
		parent_change_count = context->items[context_index].pageChangeCount;

		parent_page = O_GET_IN_MEMORY_PAGE(parent_blkno);

		ionum = assign_io_num(parent_blkno, BTREE_PAGE_LOCATOR_GET_OFFSET(parent_page, parent_loc));

		// Prepare to modify downlink in parent page
		page_block_reads(parent_blkno);
		int_hdr = (BTreeNonLeafTuphdr *) BTREE_PAGE_LOCATOR_GET_ITEM(parent_page, parent_loc);
	}
	else
	{
		//
// Root page still need ionum to prevent changing of checkpoint
// number.
//
		ionum = assign_io_num(blkno, MaxOffsetNumber);
	}

	if (!IS_DIRTY(blkno))
	{
		Assert(evict);

		//
// Easy case: page isn't dirty and doesn't need to be written to the
// disk.  Then we just have to change downlink in the parent.
//
		Assert(FileExtentIsValid(page_desc->fileExtent));
		int_hdr->downlink = MAKE_ON_DISK_DOWNLINK(page_desc->fileExtent);
		PAGE_INC_N_ONDISK(parent_page);

		// Concurrent readers should give up when we release the lock...
		O_PAGE_CHANGE_COUNT_INC(p);
		unlock_page(blkno);
		unlock_io(ionum);
	}
	else
	{
		uint64		new_downlink,
					old_downlink = 0;
		pub static mut DIRTY_PARENT: bool = false;

		// Mark parent downlink as IO in-progress.
		if (evict)
		{
			old_downlink = int_hdr->downlink;
			int_hdr->downlink = MAKE_IO_DOWNLINK(ionum);
			O_PAGE_CHANGE_COUNT_INC(p);
		}
		// Caller (walk_page()) ensured that there is no IO in progress
		Assert(page_desc->ionum < 0);
		page_desc->ionum = ionum;
		if (!is_root)
			unlock_page(parent_blkno);

		// Perform actual IO
		if (evict)
		{
			unlock_page(blkno);
			new_downlink = perform_page_io(desc, blkno, p,
										   checkpoint_number, copy_blkno, &dirty_parent);

			if (DiskDownlinkIsValid(new_downlink))
				writeback_put_extent(&io_writeback, desc, new_downlink);

			// Page is not dirty anymore
			CLEAN_DIRTY(desc->ppool, blkno);
		}
		else
		{
			// Non-leaf pages are already copied by caller
			if (O_PAGE_IS(p, LEAF))
				memcpy(img, p, ORIOLEDB_BLCKSZ);

			CLEAN_DIRTY_CONCURRENT(blkno);
			unlock_page(blkno);

			if (STOPEVENTS_ENABLED())
			{
				pub static mut JSONB: *mut params = std::ptr::null_mut();

				params = btree_page_stopevent_params(desc, p);
				STOPEVENT(STOPEVENT_AFTER_IONUM_SET, params);
			}
			new_downlink = perform_page_io(desc, blkno, img,
										   checkpoint_number, copy_blkno, &dirty_parent);

			if (DiskDownlinkIsValid(new_downlink))
				writeback_put_extent(&io_writeback, desc, new_downlink);

			// Clean dirty only if there are no concurrent writes
			lock_page(blkno);
			if (!IS_DIRTY_CONCURRENT(blkno))
				CLEAN_DIRTY(desc->ppool, blkno);
			unlock_page(blkno);

			if (!DiskDownlinkIsValid(new_downlink))
			{
				page_desc->ionum = -1;
				unlock_io(ionum);
				ereport(ERROR, (errcode_for_file_access(),
								errmsg("could not evict page %d to disk: %m", blkno)));
			}
			else if (!dirty_parent)
			{
				page_desc->ionum = -1;
				unlock_io(ionum);
				perform_writeback(&io_writeback);
				return;
			}
		}

		if (!is_root)
		{
			pub static mut PG_USED_FOR_ASSERTS_ONLY: OFindPageResult result = std::mem::zeroed();

			// Refind parent
			BTREE_PAGE_FIND_SET(context, DOWNLINK_LOCATION);
			if (O_PAGE_IS(p, RIGHTMOST))
			{
				result = refind_page(context, NULL, BTreeKeyRightmost,
									 PAGE_GET_LEVEL(p) + 1,
									 parent_blkno, parent_change_count);
			}
			else
			{
				pub static mut HIKEY: OTuple = std::mem::zeroed();

				BTREE_PAGE_GET_HIKEY(hikey, p);
				result = refind_page(context, &hikey, BTreeKeyPageHiKey,
									 PAGE_GET_LEVEL(p) + 1,
									 parent_blkno, parent_change_count);
			}
			Assert(result == OFindPageResultSuccess);

			BTREE_PAGE_FIND_UNSET(context, DOWNLINK_LOCATION);

			context_index = context->index;
			parent_blkno = context->items[context_index].blkno;
			parent_loc = &context->items[context_index].locator;
			parent_change_count = context->items[context_index].pageChangeCount;

			// Replace parent downlink with on-disk link
			parent_page = O_GET_IN_MEMORY_PAGE(parent_blkno);
			page_block_reads(parent_blkno);
			int_hdr = (BTreeNonLeafTuphdr *) BTREE_PAGE_LOCATOR_GET_ITEM(parent_page, parent_loc);

			if (!DiskDownlinkIsValid(new_downlink))
			{
				// error happens on write, rollback changes in shared memory
				if (evict)
					int_hdr->downlink = old_downlink;
				page_desc->ionum = -1;
				unlock_io(ionum);
				unlock_page(parent_blkno);
				ereport(ERROR, (errcode_for_file_access(),
								errmsg("could not evict page %d to disk: %m", blkno)));
			}
			else
			{
				if (dirty_parent)
					MARK_DIRTY(desc, parent_blkno);

				if (evict)
				{
					int_hdr->downlink = new_downlink;
					PAGE_INC_N_ONDISK(parent_page);
				}
			}
		}
		page_desc->ionum = -1;
		unlock_io(ionum);
	}

	if (!is_root)
		unlock_page(parent_blkno);

	if (evict)
		ppool_free_page(desc->ppool, blkno, false);

	perform_writeback(&io_writeback);
}

fn
btree_finalize_private_seq_bufs(desc: &mut BTreeDescr, evicted_data: &mut EvictedTreeData)
{
	pub static mut CHKP_INDEX: std::os::raw::c_int = 0;
	bool		is_compressed = OCompressIsValid(desc->compress);

	Assert(desc->storageType == BTreeStorageTemporary ||
		   desc->storageType == BTreeStoragePersistence ||
		   desc->storageType == BTreeStorageUnlogged);

	// we must not evict BTree under checkpoint

	if (desc->storageType == BTreeStoragePersistence || desc->storageType == BTreeStorageUnlogged)
	{
		chkp_index = SEQ_BUF_SHARED_EXIST(desc->nextChkp[0].shared) ? 0 : 1;

		Assert(!SEQ_BUF_SHARED_EXIST(desc->nextChkp[1 - chkp_index].shared));
		Assert(!SEQ_BUF_SHARED_EXIST(desc->tmpBuf[1 - chkp_index].shared));
		Assert(is_compressed || SEQ_BUF_SHARED_EXIST(desc->freeBuf.shared));
		Assert(SEQ_BUF_SHARED_EXIST(desc->nextChkp[chkp_index].shared));
		Assert(SEQ_BUF_SHARED_EXIST(desc->tmpBuf[chkp_index].shared));
	}
	else
	{
		chkp_index = SEQ_BUF_SHARED_EXIST(desc->tmpBuf[0].shared) ? 0 : 1;

		Assert(!SEQ_BUF_SHARED_EXIST(desc->tmpBuf[1 - chkp_index].shared));
		Assert(is_compressed || SEQ_BUF_SHARED_EXIST(desc->freeBuf.shared));
		Assert(SEQ_BUF_SHARED_EXIST(desc->tmpBuf[chkp_index].shared));
	}

	if (is_compressed)
	{
		evicted_data->freeBuf.tag = desc->freeBuf.tag;
		evicted_data->freeBuf.offset = 0;
	}
	else
	{
		evicted_data->freeBuf.tag = desc->freeBuf.shared->tag;
		evicted_data->freeBuf.offset = seq_buf_finalize(&desc->freeBuf);
		FREE_PAGE_IF_VALID(desc->ppool, desc->freeBuf.shared->pages[0]);
		FREE_PAGE_IF_VALID(desc->ppool, desc->freeBuf.shared->pages[1]);
	}

	//
// We must always finalize seq bufs (not just close them) to save the
// correct offset into evicted data.  On restore, init_seq_buf() uses a
// non-NULL evicted pointer to skip the skip_len reservation (e.g.
// CheckpointFileHeader).  If the offset is left at 0, the header space
// won't be reserved, and seq_buf_finalize() at checkpoint time will
// return a size smaller than sizeof(CheckpointFileHeader).
//
	if (desc->storageType == BTreeStoragePersistence || desc->storageType == BTreeStorageUnlogged)
	{
		evicted_data->nextChkp.tag = desc->nextChkp[chkp_index].shared->tag;
		evicted_data->nextChkp.offset = seq_buf_finalize(&desc->nextChkp[chkp_index]);
		FREE_PAGE_IF_VALID(desc->ppool, desc->nextChkp[chkp_index].shared->pages[0]);
		FREE_PAGE_IF_VALID(desc->ppool, desc->nextChkp[chkp_index].shared->pages[1]);

		evicted_data->tmpBuf.tag = desc->tmpBuf[chkp_index].shared->tag;
		evicted_data->tmpBuf.offset = seq_buf_finalize(&desc->tmpBuf[chkp_index]);
		FREE_PAGE_IF_VALID(desc->ppool, desc->tmpBuf[chkp_index].shared->pages[0]);
		FREE_PAGE_IF_VALID(desc->ppool, desc->tmpBuf[chkp_index].shared->pages[1]);
	}
	else
	{
		evicted_data->tmpBuf.tag = desc->tmpBuf[chkp_index].shared->tag;
		evicted_data->tmpBuf.offset = seq_buf_finalize(&desc->tmpBuf[chkp_index]);
		FREE_PAGE_IF_VALID(desc->ppool, desc->tmpBuf[chkp_index].shared->pages[0]);
		FREE_PAGE_IF_VALID(desc->ppool, desc->tmpBuf[chkp_index].shared->pages[1]);
	}
}

//
// Evict the tree, assuming rootPageBlkno page is locked.
//
static bool
evict_btree(desc: &mut BTreeDescr, uint32 checkpoint_number)
{
	pub static mut ROOT_BLKNO: OInMemoryBlkno = desc->rootInfo.rootPageBlkno;
	Page		rootPageBlkno = O_GET_IN_MEMORY_PAGE(root_blkno);
	root_desc: &mut OrioleDBPageDesc = O_GET_IN_MEMORY_PAGEDESC(root_blkno);
	metaPage: &mut BTreeMetaPage = BTREE_GET_META(desc);
	CheckpointFileHeader file_header = {0};
	EvictedTreeData evicted_tree_data = {{0}};
	pub static mut NEW_DOWNLINK: uint64 = std::mem::zeroed();
	char		img[ORIOLEDB_BLCKSZ];
	pub static mut WAS_DIRTY: bool = false;
	pub static mut CHKP_NUM: uint32 = 0;
	pub static mut NOT_MODIFIED: bool = false;
	bool		hasMetaLock = LWLockHeldByMe(&checkpoint_state->oTablesMetaLock);
	pub static mut EVICT_KEY: SharedRootInfoKey = std::mem::zeroed();
	pub static mut EVICT_LOCK_NO: std::os::raw::c_int = 0;

	Assert(ORootPageIsValid(desc) && OMetaPageIsValid(desc) &&
		   (O_PAGE_STATE_IS_LOCKED(pg_atomic_read_u64(&(O_PAGE_HEADER(rootPageBlkno)->state))) || O_PAGE_IS_LOCAL(root_blkno)));

	//
// Try to acquire oSharedRootInfoInsertLocks early to avoid deadlocks. If
// we can't get it, bail out — the page will be evicted later.
//
	evict_key.datoid = desc->oids.datoid;
	evict_key.relnode = desc->oids.relnode;
	evict_lockNo = tag_hash(&evict_key, sizeof(evict_key)) % SHARED_ROOT_INFO_INSERT_NUM_LOCKS;
	if (!LWLockConditionalAcquire(&checkpoint_state->oSharedRootInfoInsertLocks[evict_lockNo],
								  LW_EXCLUSIVE))
	{
		unlock_page(root_blkno);
		pub static mut FALSE: return = std::mem::zeroed();
	}

	//
// Additional protection: don't evict the tree root page if the resource
// owner hasn't released its seq scans yet.  According to the locks they
// must be already finished, but not yet released from shmem.
//
	if (meta_page_get_num_seq_scans(desc->rootInfo.metaPageBlkno) != 0)
	{
		LWLockRelease(&checkpoint_state->oSharedRootInfoInsertLocks[evict_lockNo]);
		unlock_page(root_blkno);
		pub static mut FALSE: return = std::mem::zeroed();
	}

	// we check it before
	Assert(!RightLinkIsValid(BTREE_PAGE_GET_RIGHTLINK(rootPageBlkno)));
	if (orioledb_s3_mode)
	{
		btree_s3_flush(desc, checkpoint_number);
	}

	was_dirty = IS_DIRTY(root_blkno);

	//
// Checking FileExtentIsValid() is essential for just created temporary
// trees which aren't dirty, but don't have fileExtent initialized.
//
	if (was_dirty || !FileExtentIsValid(root_desc->fileExtent))
	{
		pub static mut NOT_USED: bool = false;

		CLEAN_DIRTY(desc->ppool, root_blkno);

		// Code above ensured there is no IO in progress
		Assert(root_desc->ionum < 0);
		root_desc->ionum = assign_io_num(root_blkno, InvalidOffsetNumber);
		memcpy(img, rootPageBlkno, ORIOLEDB_BLCKSZ);
		unlock_page(root_blkno);

		new_downlink = perform_page_io(desc, root_blkno, img, checkpoint_number,
									   false, &not_used);
		if (!DiskDownlinkIsValid(new_downlink))
		{
			elog(FATAL, "Can not evict rootPageBlkno page on disk.");
		}

		writeback_put_extent(&io_writeback, desc, new_downlink);
		unlock_io(root_desc->ionum);
		root_desc->ionum = -1;
	}
	else
	{
		Assert(FileExtentIsValid(root_desc->fileExtent));
		new_downlink = MAKE_ON_DISK_DOWNLINK(root_desc->fileExtent);
		unlock_page(root_blkno);
	}

	if (!hasMetaLock)
	{
		if (!LWLockConditionalAcquire(&checkpoint_state->oTablesMetaLock,
									  LW_SHARED))
		{
			LWLockRelease(&checkpoint_state->oSharedRootInfoInsertLocks[evict_lockNo]);
			pub static mut FALSE: return = std::mem::zeroed();
		}
	}

	file_header.rootDownlink = new_downlink;

	ppool_free_page(desc->ppool, root_blkno, false);

	if (orioledb_s3_mode)
		chkpNum = S3_GET_CHKP_NUM(DOWNLINK_GET_DISK_OFF(new_downlink));

	file_header.datafileLength = pg_atomic_read_u64(&metaPage->datafileLength[chkpNum % 2]);
	file_header.leafPagesNum = pg_atomic_read_u32(&metaPage->leafPagesNum);
	file_header.ctid = pg_atomic_read_u64(&metaPage->ctid);
	file_header.bridgeCtid = pg_atomic_read_u64(&metaPage->bridge_ctid);
	file_header.numFreeBlocks = pg_atomic_read_u64(&metaPage->numFreeBlocks);
	Assert(meta_page_get_num_seq_scans(desc->rootInfo.metaPageBlkno) == 0);

	evicted_tree_data.key.datoid = desc->oids.datoid;
	evicted_tree_data.key.relnode = desc->oids.relnode;
	evicted_tree_data.file_header = file_header;
	evicted_tree_data.maxLocation[0] = metaPage->partsInfo[0].writeMaxLocation;
	evicted_tree_data.maxLocation[1] = metaPage->partsInfo[1].writeMaxLocation;
	evicted_tree_data.dirtyFlag1 = metaPage->dirtyFlag1;
	evicted_tree_data.dirtyFlag2 = metaPage->dirtyFlag2;
	evicted_tree_data.punchHolesChkpNum = metaPage->punchHolesChkpNum;

	notModified = (!metaPage->dirtyFlag1 && !metaPage->dirtyFlag2);

	//
// Free all private seq buf pages and get their offsets
//
	if (!orioledb_s3_mode || desc->storageType == BTreeStorageTemporary)
		btree_finalize_private_seq_bufs(desc, &evicted_tree_data);

	ppool_free_page(desc->ppool, desc->rootInfo.metaPageBlkno, false);

	desc->rootInfo.rootPageBlkno = OInvalidInMemoryBlkno;
	desc->rootInfo.metaPageBlkno = OInvalidInMemoryBlkno;

	perform_writeback(&io_writeback);

	//
// Check if we can skip the evicted data if tree has no modification after
// writing the last *.map file.
//
// For compressed trees we must always store evicted data.  Otherwise, on
// reload was_evicted will be false and o_tree_init_free_extents() will
// try to re-insert free extents that are already present in the in-memory
// system trees (they are not cleaned up on eviction), causing assertion
// failures in free_extent().
//
	if (desc->storageType != BTreeStoragePersistence || !notModified ||
		OCompressIsValid(desc->compress))
		insert_evicted_data(&evicted_tree_data);

	elog(DEBUG1, "evict_btree: (%u, %u) chkpNum=%u notModified=%d",
		 desc->oids.datoid, desc->oids.relnode,
		 chkpNum, notModified);

	//
// Shared descr drops to signalize other backends that tree is evicted.
// Backends and workers can create a new SharedRootInfo* after this.
//
	o_drop_shared_root_info(desc->oids.datoid, desc->oids.relnode);

	LWLockRelease(&checkpoint_state->oSharedRootInfoInsertLocks[evict_lockNo]);

	if (!hasMetaLock)
		LWLockRelease(&checkpoint_state->oTablesMetaLock);

	pub static mut TRUE: return = std::mem::zeroed();
}

BTreeDescr *
index_oids_get_btree_descr(ORelOids oids, OIndexType type)
{
	pub static mut O_INDEX_DESCR: *mut indexDescr = std::ptr::null_mut();
	pub static mut B_TREE_DESCR: *mut desc = std::ptr::null_mut();
	pub static mut NESTED: bool = false;

	// Check is this table is visible for us
	indexDescr = o_fetch_index_descr(oids, type, false, &nested);

	if (indexDescr == NULL)
		pub static mut NULL: return = std::mem::zeroed();

	desc = &indexDescr->desc;

	if (!o_btree_try_use_shmem(desc))
		pub static mut NULL: return = std::mem::zeroed();

	pub static mut DESC: return = std::mem::zeroed();
}

typedef struct
{
	pub static mut INDEX_REGULAR_LOCK: bool = false;
	pub static mut INDEX_CHECKPOINTER_LOCK: bool = false;
	pub static mut TABLE_REGULAR_LOCK: bool = false;
	pub static mut TABLE_CHECKPOINTER_LOCK: bool = false;
	pub static mut TABLE_OIDS: ORelOids = std::mem::zeroed();
} EvictBtreeLocksState;

//
// Acquire all the locks required to completely evict the tree.  We need to
// take both regular and checkpointer locks.  Also, for PK we need to lock
// the table as well, because a concurrent seq scan can lock only the table.
//
static BTreeDescr *
get_evict_btree_locks(OInMemoryBlkno blkno, ORelOids oids, OIndexType type,
					  state: &mut EvictBtreeLocksState)
{
	pub static mut B_TREE_DESCR: *mut desc = std::ptr::null_mut();
	pub static mut O_INDEX_DESCR: *mut id = std::ptr::null_mut();
	bool		recovery = is_recovery_in_progress();
	pub static mut NESTED: bool = false;

	if (!recovery && !(state->indexRegularLock = o_tables_rel_try_lock_extended(&oids, AccessExclusiveLock, &nested, false)))
		pub static mut NULL: return = std::mem::zeroed();

	if (nested)
		pub static mut NULL: return = std::mem::zeroed();

	if (!(state->indexCheckpointerLock = o_tables_rel_try_lock_extended(&oids, AccessExclusiveLock, &nested, true)))
		pub static mut NULL: return = std::mem::zeroed();

	if (nested)
		pub static mut NULL: return = std::mem::zeroed();

	desc = index_oids_get_btree_descr(oids, type);

	if (desc == NULL ||
		desc->rootInfo.rootPageBlkno != blkno)
		pub static mut NULL: return = std::mem::zeroed();

	if (desc->type != oIndexPrimary)
		pub static mut DESC: return = std::mem::zeroed();

	id = (OIndexDescr *) desc->arg;
	state->tableOids = id->tableOids;

	//
// if primary index is ctid, then we don't need to lock the table, because
// ctid is the table itself
//
	if (id->primaryIsCtid)
		pub static mut DESC: return = std::mem::zeroed();

	if (!recovery && !(state->tableRegularLock = o_tables_rel_try_lock_extended(&state->tableOids, AccessExclusiveLock, &nested, false)))
		pub static mut NULL: return = std::mem::zeroed();

	if (nested)
		pub static mut NULL: return = std::mem::zeroed();

	if (!(state->tableCheckpointerLock = o_tables_rel_try_lock_extended(&state->tableOids, AccessExclusiveLock, &nested, true)))
		pub static mut NULL: return = std::mem::zeroed();

	if (nested)
		pub static mut NULL: return = std::mem::zeroed();

	desc = index_oids_get_btree_descr(oids, type);

	if (desc == NULL ||
		desc->rootInfo.rootPageBlkno != blkno)
		pub static mut NULL: return = std::mem::zeroed();

	pub static mut DESC: return = std::mem::zeroed();
}

fn
release_evict_btree_locks(ORelOids oids, state: &mut EvictBtreeLocksState)
{
	if (state->indexRegularLock)
		o_tables_rel_unlock_extended(&oids, AccessExclusiveLock, false);
	if (state->indexCheckpointerLock)
		o_tables_rel_unlock_extended(&oids, AccessExclusiveLock, true);
	if (state->tableRegularLock)
		o_tables_rel_unlock_extended(&state->tableOids, AccessExclusiveLock, false);
	if (state->tableCheckpointerLock)
		o_tables_rel_unlock_extended(&state->tableOids, AccessExclusiveLock, true);
}

//
// Pre-lock checks for walk_page().  Validates page state and resolves the
// btree descriptor before the page lock is acquired.
//
// Returns the BTreeDescr pointer on success (caller should proceed to lock),
// or NULL if the page should be skipped.  oids: &mut Sets as a side effect.
//
static BTreeDescr *
walk_page_prelock_check(OInMemoryBlkno blkno, bool evict,
						page_desc: &mut OrioleDBPageDesc, Page p,
						oids: &mut ORelOids)
{
	pub static mut B_TREE_DESCR: *mut desc = std::ptr::null_mut();

	if (!ORelOidsIsValid(page_desc->oids) || page_desc->type == oIndexInvalid)
		pub static mut NULL: return = std::mem::zeroed();

	//
// Read field2 directly rather than via PAGE_GET_N_ONDISK(): we don't hold
// the page lock here, so a concurrent leaf/non-leaf transition could fire
// the macro's debug assert even though the outer flag check just passed.
// The result of this comparison is racy by design and gets re-validated
// once the page is locked.
//
	if (!O_PAGE_IS(p, LEAF) && evict &&
		((BTreePageHeader *) p)->field2 != BTREE_PAGE_ITEMS_COUNT(p))
		pub static mut NULL: return = std::mem::zeroed();

	if (!evict && !IS_DIRTY(blkno))
		pub static mut NULL: return = std::mem::zeroed();

	// Important to access the shared memory oids: &mut once = *((volatile ORelOids *) &page_desc->oids);

	//
// index_oids_get_btree_descr() might imply page eviction.  We shouldn't
// do this while holding a page lock.  So, we need to do this before
// locking the page.
//
	if (IS_SYS_TREE_OIDS(*oids))
	{
		if (sys_tree_get_storage_type(oids->relnode) != BTreeStorageInMemory)
			desc = get_sys_tree(oids->relnode);
		else
			pub static mut NULL: return = std::mem::zeroed();
	}
	else
	{
		// Check is this index is visible for us
		desc = index_oids_get_btree_descr(*oids, page_desc->type);

		if (desc == NULL)
			pub static mut NULL: return = std::mem::zeroed();
	}

	pub static mut DESC: return = std::mem::zeroed();
}

typedef enum WalkPageCheckResult
{
	WalkPageCheckPassed,
	WalkPageCheckFailed,
	WalkPageCheckWaitIO
} WalkPageCheckResult;

//
// Locked-page validity checks for walk_page().  Must be called with the page
// lock held.
//
// Returns WalkPageCheckPassed if all checks pass (page remains locked).
// Returns WalkPageCheckFailed if a check fails (page is unlocked).
// Returns WalkPageCheckWaitIO if IO is in progress (page is unlocked,
// *ionum is set for the caller to wait on).
//
// When !evict, also prepares the non-leaf page image into img.
//
static WalkPageCheckResult
walk_page_check_locked(OInMemoryBlkno blkno, bool evict,
					   page_desc: &mut OrioleDBPageDesc, Page p,
					   ORelOids oids, img: &mut char, ionum: &mut int)
{
	if (!ORelOidsIsValid(page_desc->oids) ||
		page_desc->type == oIndexInvalid ||
		!ORelOidsIsEqual(oids, page_desc->oids))
	{
		unlock_page(blkno);
		pub static mut WALK_PAGE_CHECK_FAILED: return = std::mem::zeroed();
	}

	if (!evict && !IS_DIRTY(blkno))
	{
		unlock_page(blkno);
		pub static mut WALK_PAGE_CHECK_FAILED: return = std::mem::zeroed();
	}

	if (O_PAGE_IS(p, PRE_CLEANUP))
	{
		unlock_page(blkno);
		pub static mut WALK_PAGE_CHECK_FAILED: return = std::mem::zeroed();
	}

	// On concurrent IO, unlock and let the caller decide to wait or ionum: &mut skip = page_desc->ionum;
	if (*ionum >= 0)
	{
		unlock_page(blkno);
		pub static mut WALK_PAGE_CHECK_WAIT_IO: return = std::mem::zeroed();
	}

	if (!O_PAGE_IS(p, LEAF) && evict && PAGE_GET_N_ONDISK(p) != BTREE_PAGE_ITEMS_COUNT(p))
	{
		unlock_page(blkno);
		pub static mut WALK_PAGE_CHECK_FAILED: return = std::mem::zeroed();
	}

	if (!O_PAGE_IS(p, LEAF) && !evict)
	{
		memcpy(img, p, ORIOLEDB_BLCKSZ);
		if (!prepare_non_leaf_page(img))
		{
			unlock_page(blkno);
			pub static mut WALK_PAGE_CHECK_FAILED: return = std::mem::zeroed();
		}
	}

	if (RightLinkIsValid(BTREE_PAGE_GET_RIGHTLINK(p)))
	{
		unlock_page(blkno);
		pub static mut WALK_PAGE_CHECK_FAILED: return = std::mem::zeroed();
	}

	pub static mut WALK_PAGE_CHECK_PASSED: return = std::mem::zeroed();
}

//
// Handle root page eviction in walk_page().  Called with the page lock held.
// Manages all lock/unlock internally, including the two-pass protocol:
// release page lock, acquire evict btree locks, re-lock and re-validate.
// Guarantees release_evict_btree_locks() is called after get_evict_btree_locks().
//
static OWalkPageResult
walk_page_evict_root(desc: &mut BTreeDescr, OInMemoryBlkno blkno,
					 page_desc: &mut OrioleDBPageDesc, Page p,
					 ORelOids oids)
{
	pub static mut LOCKS_STATE: EvictBtreeLocksState = std::mem::zeroed();
	pub static mut CHECKPOINT_NUMBER: uint32 = std::mem::zeroed();
	pub static mut COPY_BLKNO: bool = false;
	pub static mut RESULT: bool = false;
	pub static mut IONUM: std::os::raw::c_int = 0;

	if (tree_is_under_checkpoint(desc))
	{
		unlock_page(blkno);
		pub static mut O_WALK_PAGE_SKIPPED: return = std::mem::zeroed();
	}

	// Release page lock before acquiring evict btree locks
	unlock_page(blkno);

	memset(&locksState, 0, sizeof(locksState));

	desc = get_evict_btree_locks(blkno, oids, page_desc->type, &locksState);

	if (!desc)
	{
		release_evict_btree_locks(oids, &locksState);
		pub static mut O_WALK_PAGE_SKIPPED: return = std::mem::zeroed();
	}

	//
// Re-lock the page and re-validate all checks after acquiring evict btree
// locks.
//
	if (!try_lock_page(blkno))
	{
		release_evict_btree_locks(oids, &locksState);
		pub static mut O_WALK_PAGE_SKIPPED: return = std::mem::zeroed();
	}

	if (walk_page_check_locked(blkno, true, page_desc, p,
							   oids, NULL, &ionum) != WalkPageCheckPassed)
	{
		release_evict_btree_locks(oids, &locksState);
		pub static mut O_WALK_PAGE_SKIPPED: return = std::mem::zeroed();
	}

	if (tree_is_under_checkpoint(desc))
	{
		unlock_page(blkno);
		release_evict_btree_locks(oids, &locksState);
		pub static mut O_WALK_PAGE_SKIPPED: return = std::mem::zeroed();
	}

	if (desc->rootInfo.rootPageBlkno != blkno)
	{
		unlock_page(blkno);
		release_evict_btree_locks(oids, &locksState);
		pub static mut O_WALK_PAGE_SKIPPED: return = std::mem::zeroed();
	}

	if (!get_checkpoint_number(desc, blkno, &checkpoint_number, &copy_blkno))
	{
		unlock_page(blkno);
		release_evict_btree_locks(oids, &locksState);
		pub static mut O_WALK_PAGE_SKIPPED: return = std::mem::zeroed();
	}

	result = evict_btree(desc, checkpoint_number);
	o_invalidate_oids(oids);

	release_evict_btree_locks(oids, &locksState);

	return result ? OWalkPageEvicted : OWalkPageSkipped;
}

//
// Examine single page and evict it if possible.
//
// Note that here we skip seq buf pages, as we will evict them together with the
// tree in evict_btree() when we evict the root page.
//
OWalkPageResult
walk_page(OInMemoryBlkno blkno, bool evict)
{
	page_desc: &mut OrioleDBPageDesc = O_GET_IN_MEMORY_PAGEDESC(blkno);
	pub static mut CONTEXT: OBTreeFindPageContext = std::mem::zeroed();
	pub static mut B_TREE_DESCR: *mut desc = std::ptr::null_mut();
	Page		p = O_GET_IN_MEMORY_PAGE(blkno),
				parent_page;
	pub static mut OIDS: ORelOids = std::mem::zeroed();
	pub static mut B_TREE_NON_LEAF_TUPHDR: *mut int_hdr = std::ptr::null_mut();
	pub static mut CHECKPOINT_NUMBER: uint32 = std::mem::zeroed();
	bool		copy_blkno,
				merge_tried = false;
	pub static mut FIND_RESULT: OFindPageResult = std::mem::zeroed();
	pub static mut IONUM: std::os::raw::c_int = 0;
	char		img[ORIOLEDB_BLCKSZ];
	pub static mut IS_ROOT: bool = false;
	pub static mut CHECK_RESULT: WalkPageCheckResult = std::mem::zeroed();

	p = O_GET_IN_MEMORY_PAGE(blkno);
retry:

	desc = walk_page_prelock_check(blkno, evict, page_desc, p, &oids);
	if (!desc)
		pub static mut O_WALK_PAGE_SKIPPED: return = std::mem::zeroed();

	if (!try_lock_page(blkno))
		pub static mut O_WALK_PAGE_SKIPPED: return = std::mem::zeroed();

	checkResult = walk_page_check_locked(blkno, evict, page_desc, p,
										 oids, img, &ionum);
	if (checkResult == WalkPageCheckWaitIO)
	{
		wait_for_io_completion(ionum);
		pub static mut RETRY: goto = std::mem::zeroed();
	}
	if (checkResult == WalkPageCheckFailed)
		pub static mut O_WALK_PAGE_SKIPPED: return = std::mem::zeroed();

	// Try to merge sparse page instead of eviction
	if (!merge_tried && is_page_too_sparse(desc, p))
	{
		pub static mut RESULT: bool = false;

		result = btree_try_merge_and_unlock(desc, blkno, true, false);

		// Merge shouldn't leave us with locked pages.
		Assert(!have_locked_pages());

		if (result)
		{
			pub static mut O_WALK_PAGE_MERGED: return = std::mem::zeroed();
		}
		else
		{
			merge_tried = true;
			pub static mut RETRY: goto = std::mem::zeroed();
		}
	}

	Assert(desc != NULL);
	Assert(ORootPageIsValid(desc) && OMetaPageIsValid(desc));
	is_root = desc->rootInfo.rootPageBlkno == blkno;

	// If page is rootPageBlkno, we don't need to search parent page.
	context.desc = desc;
	context.index = 0;
	if (!is_root)
	{
		init_page_find_context(&context, desc, COMMITSEQNO_INPROGRESS, BTREE_PAGE_FIND_MODIFY
							   | BTREE_PAGE_FIND_TRY_LOCK
							   | BTREE_PAGE_FIND_DOWNLINK_LOCATION
							   | BTREE_PAGE_FIND_NO_FIX_SPLIT);
		if (O_PAGE_IS(p, RIGHTMOST))
		{
			findResult = find_page(&context, NULL, BTreeKeyRightmost, PAGE_GET_LEVEL(p) + 1);
		}
		else
		{
			pub static mut HIKEY: OTuple = std::mem::zeroed();

			BTREE_PAGE_GET_HIKEY(hikey, p);
			findResult = find_page(&context, &hikey, BTreeKeyPageHiKey, PAGE_GET_LEVEL(p) + 1);
		}

		if (findResult != OFindPageResultSuccess)
		{
			Assert(findResult == OFindPageResultFailure);
			unlock_page(blkno);
			Assert(!have_locked_pages());
			pub static mut O_WALK_PAGE_SKIPPED: return = std::mem::zeroed();
		}

		BTREE_PAGE_FIND_UNSET(&context, TRY_LOCK);
		parent_page = O_GET_IN_MEMORY_PAGE(context.items[context.index].blkno);

		int_hdr = (BTreeNonLeafTuphdr *) BTREE_PAGE_LOCATOR_GET_ITEM(parent_page, &context.items[context.index].locator);

		if (!DOWNLINK_IS_IN_MEMORY(int_hdr->downlink) ||
			DOWNLINK_GET_IN_MEMORY_BLKNO(int_hdr->downlink) != blkno)
		{
			//
// We didn't find downlink pointing to this page.  This could
// happened because of concurrent split.  Give up then...
//
			unlock_page(blkno);
			unlock_page(context.items[context.index].blkno);
			pub static mut O_WALK_PAGE_SKIPPED: return = std::mem::zeroed();
		}
	}
	else if (IS_SYS_TREE_OIDS(oids))
	{
		Assert(is_root);
		unlock_page(blkno);
		pub static mut O_WALK_PAGE_SKIPPED: return = std::mem::zeroed();
	}

	if (!get_checkpoint_number(desc, blkno, &checkpoint_number, &copy_blkno))
	{
		unlock_page(blkno);

		if (!is_root)
		{
			unlock_page(context.items[context.index].blkno);
		}
		pub static mut O_WALK_PAGE_SKIPPED: return = std::mem::zeroed();
	}

	if (evict && is_root)
		return walk_page_evict_root(desc, blkno, page_desc, p, oids);

	STOPEVENT(STOPEVENT_BEFORE_WRITE_PAGE, NULL);

	write_page(&context, blkno, img, checkpoint_number, evict, copy_blkno);

	STOPEVENT(STOPEVENT_AFTER_WRITE_PAGE, NULL);

	return evict ? OWalkPageEvicted : OWalkPageWritten;
}

//
// Recursively write pages in the tree. Stop reqursion if we reach maxLevel,
// when it has non-negtive value. To write all pages, set maxLevel to -1.
//
static bool
write_tree_pages_recursive(UndoLogType undoType,
						   OInMemoryBlkno blkno, uint32 loadId,
						   int maxLevel, bool evict)
{
	pub static mut P: Page = std::mem::zeroed();
	pub static mut LEVEL: std::os::raw::c_int = 0;
	OInMemoryBlkno childPageNumbers[BTREE_PAGE_MAX_CHUNK_ITEMS];
	uint32		childPageChangeCounts[BTREE_PAGE_MAX_CHUNK_ITEMS];
	pub static mut CHILD_PAGES_COUNT: std::os::raw::c_int = 0;
	pub static mut I: std::os::raw::c_int = 0;
	pub static mut LOC: BTreePageItemLocator = std::mem::zeroed();

	if (!OInMemoryBlknoIsValid(blkno))
		pub static mut FALSE: return = std::mem::zeroed();

	lock_page(blkno);
	p = O_GET_IN_MEMORY_PAGE(blkno);

	//
// For local pool pages, the slot may have been reclaimed by a reentrant
// eviction triggered while we were processing a sibling downlink
// collected earlier.  Treat a NULL slot as a missing page.
//
	if (O_PAGE_IS_LOCAL(blkno) && p == NULL)
	{
		unlock_page(blkno);
		pub static mut FALSE: return = std::mem::zeroed();
	}
	if (O_PAGE_GET_CHANGE_COUNT(p) != loadId)
	{
		unlock_page(blkno);
		pub static mut FALSE: return = std::mem::zeroed();
	}
	level = PAGE_GET_LEVEL(p);

	if (!O_PAGE_IS(p, LEAF))
	{
		BTREE_PAGE_FOREACH_ITEMS(p, &loc)
		{
			tuphdr: &mut BTreeNonLeafTuphdr = (BTreeNonLeafTuphdr *) BTREE_PAGE_LOCATOR_GET_ITEM(p, &loc);

			if (DOWNLINK_IS_IN_MEMORY(tuphdr->downlink))
			{
				childPageNumbers[childPagesCount] = DOWNLINK_GET_IN_MEMORY_BLKNO(tuphdr->downlink);
				childPageChangeCounts[childPagesCount] = DOWNLINK_GET_IN_MEMORY_CHANGECOUNT(tuphdr->downlink);
				childPagesCount++;
			}
		}
	}

	unlock_page(blkno);

	for (i = 0; i < childPagesCount; i++)
		() write_tree_pages_recursive(undoType,
										  childPageNumbers[i],
										  childPageChangeCounts[i],
										  maxLevel,
										  evict);

	if (level <= maxLevel || maxLevel == -1)
	{
		while (true)
		{
			reserve_undo_size(GET_PAGE_LEVEL_UNDO_TYPE(undoType),
							  2 * O_MERGE_UNDO_IMAGE_SIZE);
			if (walk_page(blkno, evict) != OWalkPageMerged)
				break;
		}
		release_undo_size(GET_PAGE_LEVEL_UNDO_TYPE(undoType));
	}

	pub static mut TRUE: return = std::mem::zeroed();
}


write_tree_pages(desc: &mut BTreeDescr, int maxLevel, bool evict)
{
	o_btree_load_shmem(desc);
	if (!write_tree_pages_recursive(desc->undoType,
									desc->rootInfo.rootPageBlkno,
									desc->rootInfo.rootPageChangeCount,
									maxLevel, evict))
	{
		desc->rootInfo.rootPageBlkno = OInvalidInMemoryBlkno;
		desc->rootInfo.metaPageBlkno = OInvalidInMemoryBlkno;
		desc->rootInfo.rootPageChangeCount = 0;
		o_btree_load_shmem(desc);
		() write_tree_pages_recursive(desc->undoType,
										  desc->rootInfo.rootPageBlkno,
										  desc->rootInfo.rootPageChangeCount,
										  maxLevel, evict);
	}
}

fn
write_relation_pages(Oid relid, int maxLevel, bool evict)
{
	pub static mut O_TABLE_DESCR: *mut descr = std::ptr::null_mut();
	pub static mut B_TREE_DESCR: *mut td = std::ptr::null_mut();
	pub static mut REL: Relation = std::mem::zeroed();
	pub static mut TREEN: std::os::raw::c_int = 0;

	orioledb_check_shmem();

	rel = relation_open(relid, AccessShareLock);

	if (!rel)
		ereport(ERROR,
				(errcode(ERRCODE_WRONG_OBJECT_TYPE),
				 errmsg("relation oid %u does not exists", relid)));

	descr = relation_get_descr(rel);

	for (treen = 0; treen < descr->nIndices; treen++)
	{
		td = &descr->indices[treen]->desc;
		write_tree_pages(td, maxLevel, evict);
	}
	td = &descr->toast->desc;
	write_tree_pages(td, maxLevel, evict);

	relation_close(rel, AccessShareLock);
}

Datum
orioledb_evict_pages(PG_FUNCTION_ARGS)
{
	Oid			relid = PG_GETARG_OID(0);
	int			maxLevel = PG_GETARG_INT32(1);

	write_relation_pages(relid, maxLevel, true);

	PG_RETURN_VOID();
}

Datum
orioledb_write_pages(PG_FUNCTION_ARGS)
{
	Oid			relid = PG_GETARG_OID(0);
	pub static mut MAX_LEVEL: std::os::raw::c_int = ORIOLEDB_MAX_DEPTH;

	write_relation_pages(relid, maxLevel, false);

	PG_RETURN_VOID();
}

static int
tree_offsets_cmp(a: &mut const, b: &mut const)
{
	TreeOffset	val1 = *(TreeOffset *) a;
	TreeOffset	val2 = *(TreeOffset *) b;

	if (val1.key.oids.datoid != val2.key.oids.datoid)
		return (val1.key.oids.datoid < val2.key.oids.datoid) ? -1 : 1;
	else if (val1.key.oids.relnode != val2.key.oids.relnode)
		return (val1.key.oids.relnode < val2.key.oids.relnode) ? -1 : 1;
	else if (val1.chkpNum != val2.chkpNum)
		return (val1.chkpNum < val2.chkpNum) ? -1 : 1;
	else if (val1.segno != val2.segno)
		return (val1.segno < val2.segno) ? -1 : 1;
	else if (val1.fileExtent.off != val2.fileExtent.off)
		return val1.fileExtent.off < val2.fileExtent.off ? -1 : 1;
	else if (val1.fileExtent.len != val2.fileExtent.len)
	{
		//
// an extent with bigger length will be placed first, it helps to
// simplify process this case in perform_writeback()
//
		return val1.fileExtent.len > val2.fileExtent.len ? -1 : 1;
	}

	pub static mut 0: return = std::mem::zeroed();
}

fn
writeback_put_extent(writeback: &mut IOWriteBack, desc: &mut BTreeDescr,
					 uint64 downlink)
{
	pub static mut OFFSET: TreeOffset = std::mem::zeroed();
	pub static mut BLCKSZ: off_t = 0;
	pub static mut LAST_SEGNO: std::os::raw::c_int = 0;
	pub static mut EXTENT: FileExtent = std::mem::zeroed();

	Assert(DOWNLINK_IS_ON_DISK(downlink));
	extent.len = DOWNLINK_GET_DISK_LEN(downlink);
	extent.off = DOWNLINK_GET_DISK_OFF(downlink);

	if (!ORelOidsIsValid(desc->oids) || desc->type == oIndexInvalid)
		return;

	if (orioledb_s3_mode)
	{
		offset.chkpNum = S3_GET_CHKP_NUM(extent.off);
		extent.off &= S3_OFFSET_MASK;
	}
	else
	{
		offset.chkpNum = 0;
	}

	Assert(extent.len > 0);
	Assert(extent.len <= (ORIOLEDB_BLCKSZ / ORIOLEDB_COMP_BLCKSZ));

	offset.key.oids = desc->oids;
	offset.key.tablespace = desc->tablespace;
	offset.compressed = OCompressIsValid(desc->compress);
	blcksz = offset.compressed ? ORIOLEDB_COMP_BLCKSZ : ORIOLEDB_BLCKSZ;
	offset.segno = blcksz * extent.off / ORIOLEDB_SEGMENT_SIZE;
	last_segno = blcksz * (extent.off + extent.len - 1) / ORIOLEDB_SEGMENT_SIZE;

	while (offset.segno <= last_segno)
	{
		if (writeback->extents == NULL)
		{
			writeback->extentsNumber = 0;
			writeback->extentsAllocated = 16;
			writeback->extents = (TreeOffset *) MemoryContextAlloc(TopMemoryContext,
																   sizeof(TreeOffset) * writeback->extentsAllocated);
		}
		else if (writeback->extentsNumber >= writeback->extentsAllocated)
		{
			writeback->extentsAllocated *= 2;
			writeback->extents = (TreeOffset *) repalloc(writeback->extents,
														 sizeof(TreeOffset) * writeback->extentsAllocated);
		}

		offset.fileExtent = extent;
		if (offset.segno != last_segno)
			offset.fileExtent.len = ORIOLEDB_SEGMENT_SIZE / blcksz - extent.off % (ORIOLEDB_SEGMENT_SIZE / blcksz);
		writeback->extents[writeback->extentsNumber] = offset;
		writeback->extentsNumber++;
		offset.segno++;
		extent.off += offset.fileExtent.len;
		extent.len -= offset.fileExtent.len;
	}
}

fn
perform_writeback(writeback: &mut IOWriteBack)
{
	int			i,
				len = 0,
				flushAfter;
	pub static mut OFFSET: uint64 = InvalidFileExtentOff - 1;
	pub static mut BLCKSZ: off_t = 0;
	ORelOids	oids = {0};
	pub static mut FILE: File = -1;
	pub static mut SEGNO: std::os::raw::c_int = 0;
	pub static mut CHKP_NUM: std::os::raw::c_int = 0;

	if (use_device && !use_mmap)
	{
		writeback->extentsNumber = 0;
		return;
	}

	flushAfter = IsBGWriter ? bgwriter_flush_after : backend_flush_after;
	flushAfter *= BLCKSZ / ORIOLEDB_BLCKSZ;

	// PG defaults: flushAfter == 0 turns off writeback
	if (flushAfter == 0)
	{
		writeback->extentsNumber = 0;
		return;
	}

	if (writeback->extentsNumber < flushAfter)
		return;

	pg_qsort(writeback->extents, writeback->extentsNumber,
			 sizeof(TreeOffset), tree_offsets_cmp);

	for (i = 0; i < writeback->extentsNumber; i++)
	{
		pub static mut CUR: TreeOffset = writeback->extents[i];

		if (oids.datoid != cur.key.oids.datoid ||
			oids.relnode != cur.key.oids.relnode ||
			segno != cur.segno || chkpNum != cur.chkpNum)
		{
			if (use_mmap)
			{
				if (len > 0)
					msync(mmap_data + (off_t) segno * ORIOLEDB_SEGMENT_SIZE + (off_t) offset * blcksz, (off_t) len * blcksz, MS_ASYNC);
			}
			else
			{
				if (len > 0)
				{
					FileWriteback(file, (off_t) offset * blcksz,
								  (off_t) len * blcksz,
								  WAIT_EVENT_DATA_FILE_FLUSH);
				}
				if (file >= 0)
					FileClose(file);
			}

			blcksz = cur.compressed ? ORIOLEDB_COMP_BLCKSZ : ORIOLEDB_BLCKSZ;
			oids = cur.key.oids;
			segno = cur.segno;
			chkpNum = cur.chkpNum;
			if (!use_mmap)
			{
				pub static mut CHAR: *mut filename = std::ptr::null_mut();

				filename = btree_filename(cur.key, segno, chkpNum);
				file = PathNameOpenFile(filename, O_RDWR | O_CREAT | PG_BINARY);
				pfree(filename);
				offset = cur.fileExtent.off;
				len = cur.fileExtent.len;
			}
		}
		else
		{
			if (cur.fileExtent.off == offset)
			{
				continue;
			}
			else if (cur.fileExtent.off == offset + len)
			{
				len += cur.fileExtent.len;
			}
			else
			{
				if (use_mmap)
					msync(mmap_data + (off_t) segno * ORIOLEDB_SEGMENT_SIZE + (off_t) offset * blcksz, (off_t) len * blcksz, MS_ASYNC);
				else
					FileWriteback(file, (off_t) offset * blcksz,
								  (off_t) len * blcksz,
								  WAIT_EVENT_DATA_FILE_FLUSH);
				offset = cur.fileExtent.off;
				len = cur.fileExtent.len;
			}
		}
	}

	if (len > 0)
	{
		Assert(blcksz != 0);
		if (use_mmap)
			msync(mmap_data + (off_t) segno * ORIOLEDB_SEGMENT_SIZE + (off_t) offset * blcksz, (off_t) len * blcksz, MS_ASYNC);
		else
			FileWriteback(file, (off_t) offset * blcksz,
						  (off_t) len * blcksz,
						  WAIT_EVENT_DATA_FILE_FLUSH);
	}

	if (!use_mmap && file >= 0)
		FileClose(file);

	writeback->extentsNumber = 0;
}

typedef  (*RelnodeFileCallback) (const filename: &mut char, uint32 segno,
									 ext: &mut char,  *arg);

//
// Iterate all the files belonging to given (datoid, relnode) pair and call
// the callback for each filename.
//
// Guarantees that at first we process the first data file.
//
static bool
iterate_relnode_files(OIndexKey key, RelnodeFileCallback callback,  *arg)
{
	pub static mut DIRENT: *mut struct file = std::ptr::null_mut();
	pub static mut DIR: *mut dir = std::ptr::null_mut();
	pub static mut CHAR: *mut filename = std::ptr::null_mut();
	pub static mut FIRST_FILE_DELETED: bool = false;
	pub static mut CHAR: *mut db_prefix = std::ptr::null_mut();

	o_get_prefixes_for_tablespace(key.oids.datoid, key.tablespace,
								  NULL, &db_prefix);

	dir = opendir(db_prefix);

	if (dir == NULL)
		pub static mut FALSE: return = std::mem::zeroed();

	while (errno = 0, (file = readdir(dir)) != NULL)
	{
		uint32		file_relnode,
					file_chkp = 0,
					file_segno = 0;
		char		file_ext[5];
		pub static mut CHAR: *mut file_ext_p = std::ptr::null_mut();

		if ((sscanf(file->d_name, "%10u-%10u.%4s",
					&file_relnode, &file_chkp, file_ext) == 3 &&
			 (!strcmp(file_ext, "tmp") || !strcmp(file_ext, "map") ||
			  !strcmp(file_ext, "evt")) &&
			 (file_ext_p = file_ext)) ||
			sscanf(file->d_name, "%10u.%10u", &file_relnode, &file_segno) == 2 ||
			sscanf(file->d_name, "%10u", &file_relnode) == 1)
		{
			if (key.oids.relnode == file_relnode)
			{
				if (!orioledb_s3_mode && !first_file_deleted)
				{
					filename = psprintf("%s/%u", db_prefix, key.oids.relnode);

					//
// The first-file callback exists for callers that care
// about ordering the base file relative to its segments
// (e.g. durable unlink, precommit fsync).  Skip it when
// the base file is absent: after a crash, a secondary
// file like "<relnode>.1" or "<relnode>-<chkp>.map" can
// exist on disk while the base file was never durably
// created, and fsync/unlink of a missing path would
// ereport ERROR (PANIC during startup recovery).
//
					if (access(filename, F_OK) == 0)
						callback(filename, 0, NULL, arg);
					pfree(filename);
					first_file_deleted = true;
				}

				if (file_segno != 0 || file_ext_p != NULL)
				{
					filename = psprintf("%s/%s", db_prefix, file->d_name);
					callback(filename, file_segno, file_ext_p, arg);
					pfree(filename);
				}
			}
		}
	}

	closedir(dir);
	pfree(db_prefix);
	pub static mut TRUE: return = std::mem::zeroed();
}

fn
unlink_callback(const filename: &mut char, uint32 segno, ext: &mut char,  *arg)
{
	//
// Recovery determines relation data presence by presence of the first
// data file.  So, we durably delete the first data file to avoid
// situation when partially deleted file data is visible.
//
	bool		fsync = *(bool *) arg;

	if (segno == 0 && ext == NULL && fsync)
		durable_unlink(filename, ERROR);
	else
		unlink(filename);
}

bool
cleanup_btree_files(OIndexKey key, bool fsync)
{
	return iterate_relnode_files(key, unlink_callback, ( *) &fsync);
}

fn
fsync_callback(const filename: &mut char, uint32 segno, ext: &mut char,  *arg)
{
	if (ext == NULL || strcmp(ext, "tmp") != 0)
		fsync_fname(filename, false);
}

bool
fsync_btree_files(OIndexKey key)
{
	return iterate_relnode_files(key, fsync_callback, NULL);
}


try_to_punch_holes(desc: &mut BTreeDescr)
{
	pub static mut B_TREE_META_PAGE: *mut metaPage = std::ptr::null_mut();
	pub static mut FILE: File = std::mem::zeroed();
	pub static mut FILE_SIZE: uint64 = std::mem::zeroed();
	filename: &mut char,
				buf[ORIOLEDB_BLCKSZ];
	uint64		len = 0,
				i,
				buf_len;
	pub static mut CHKP_NUM: uint32 = std::mem::zeroed();
	pub static mut LW_LOCK: *mut metaLock = std::ptr::null_mut();
	pub static mut LW_LOCK: *mut punchHolesLock = std::ptr::null_mut();

	Assert(orioledb_use_sparse_files);
	Assert(!OCompressIsValid(desc->compress));

	o_btree_load_shmem(desc);
	metaPage = BTREE_GET_META(desc);
	metaLock = &metaPage->metaLock;
	punchHolesLock = &metaPage->punchHolesLock;

	chkp_num = metaPage->punchHolesChkpNum + 1;
	while (can_use_checkpoint_extents(desc, chkp_num))
	{
		pub static mut TAG: SeqBufTag = std::mem::zeroed();
		pub static mut REMOVE_FILE: bool = false;

		LWLockAcquire(punchHolesLock, LW_EXCLUSIVE);

		if (chkp_num == metaPage->punchHolesChkpNum + 1)
		{
			if (chkp_num < metaPage->freeBuf.tag.num)
				removeFile = true;
		}
		else
		{
			chkp_num = metaPage->punchHolesChkpNum + 1;
			// Try for next checkpoint number
			LWLockRelease(punchHolesLock);
			continue;
		}

		tag.key.oids = desc->oids;
		tag.key.tablespace = desc->tablespace;
		tag.type = 't';
		tag.num = chkp_num;
		if (!seq_buf_file_exist(&tag))
		{
			// table may be deleted or *.tmp file not created
			LWLockAcquire(metaLock, LW_EXCLUSIVE);
			Assert(chkp_num == metaPage->punchHolesChkpNum + 1);
			metaPage->punchHolesChkpNum = chkp_num;
			LWLockRelease(metaLock);
			LWLockRelease(punchHolesLock);
			chkp_num++;
			continue;
		}

		// free extents from *.tmp file
		filename = get_seq_buf_filename(&tag);
		file = PathNameOpenFile(filename, O_RDONLY | PG_BINARY);
		if (file < 0)
			ereport(FATAL, (errcode_for_file_access(),
							errmsg("could not open file %s: %m", filename)));
		file_size = FileSize(file);

		//
// Each -N.tmp file is a self-contained list of freed block offsets
// and must be read from its own offset 0.  Reset the read cursor /
// byte counter for every file: when a single try_to_punch_holes()
// call drains more than one checkpoint's tmp file (several
// checkpoints accumulated undrained files), a stale len would seek
// past the next file's EOF and trip the file_size != len check below.
// (add_free_extents_from_tmp() declares len inside its loop for the
// same reason.)
//
		len = 0;

		while (true)
		{
			pub static mut BLOCK_NUMBER: *mut cur_off = std::ptr::null_mut();

			buf_len = OFileRead(file, buf, ORIOLEDB_BLCKSZ, len, WAIT_EVENT_DATA_FILE_READ);
			if (buf_len <= 0)
				break;

			cur_off = (BlockNumber *) buf;
			for (i = 0; i < buf_len; i += sizeof(BlockNumber))
			{
				btree_smgr_punch_hole(desc, chkp_num,
									  (off_t) (*cur_off) * (off_t) ORIOLEDB_BLCKSZ,
									  ORIOLEDB_BLCKSZ);
				cur_off++;
			}
			len += buf_len;
		}
		if (file_size != len)
			ereport(FATAL, (errcode_for_file_access(),
							errmsg("could not read data from checkpoint tmp file: %s " UINT64_FORMAT " " UINT64_FORMAT ": %m",
								   filename, len, file_size)));

		pfree(filename);
		FileClose(file);

		if (removeFile)
			seq_buf_remove_file(&tag);

		LWLockAcquire(metaLock, LW_EXCLUSIVE);
		Assert(chkp_num == metaPage->punchHolesChkpNum + 1);
		metaPage->punchHolesChkpNum = chkp_num;
		LWLockRelease(metaLock);

		LWLockRelease(punchHolesLock);

		// Try for next checkpoint number
		chkp_num++;
	}
}