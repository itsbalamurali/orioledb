use crate::access::xlog_internal;
use crate::btree::io;
use crate::c;
use crate::catalog::o_sys_cache;
use crate::fcntl;
use crate::openssl::sha;
use crate::orioledb;
use crate::pgstat;
use crate::postmaster::bgworker;
use crate::postmaster::bgwriter;
use crate::postmaster::interrupt;
use crate::s3::checksum;
use crate::s3::headers;
use crate::s3::queue;
use crate::s3::requests;
use crate::s3::worker;
use crate::storage::bufmgr;
use crate::storage::latch;
use crate::storage::proc;
use crate::storage::sinvaladt;
use crate::transam::undo;
use crate::unistd;
use crate::utils::memutils;
use crate::utils::snapmgr;
use crate::utils::syscache;
use crate::utils::timeout;
use pgrx::pg_sys;

// -------------------------------------------------------------------------
//
// worker.c
// Routines for S3 worker process.
//
// Copyright (c) 2024-2026, Oriole DB Inc.
// Copyright (c) 2025-2026, Supabase Inc.
//
// IDENTIFICATION
// contrib/orioledb/src/s3/worker.c
//
// -------------------------------------------------------------------------
//

static S3TaskLocation s3_schedule_file_part_read(uint32 chkpNum, OIndexKey key,
												 int32 segNum, int32 partNum);

#define WORKERS_FILE_CHECKSUMS_MAX_LEN 100

typedef struct S3WorkerCtl
{
	pub static mut FILE_CHECKSUMS_CNT: pg_atomic_uint32 = std::mem::zeroed();
	pub static mut FILE_CHECKSUMS_FLUSHED_CV: ConditionVariable = std::mem::zeroed();

	// S3 workers are in progress of putting PostgreSQL files into S3 bucket
	pg_atomic_flag workersInProgress[FLEXIBLE_ARRAY_MEMBER];
} S3WorkerCtl;

static mut S3_TASK_LOCATION: *mut volatile workers_locations = std::ptr::null_mut();
static mut S3_FILE_CHECKSUM: *mut workers_file_checksums = std::ptr::null_mut();

static mut S3_WORKER_CTL: *mut workers_ctl = std::ptr::null_mut();
static mut S3_CHECKSUM_STATE: *mut checksum_state = std::ptr::null_mut();

static mut WORKER_NUM: std::os::raw::c_int = 0;

Size
s3_workers_shmem_needs()
{
	pub static mut SIZE: Size = 0;

	size = CACHELINEALIGN(offsetof(S3WorkerCtl, workersInProgress) +
						  sizeof(pg_atomic_flag) * s3_num_workers);
	size = add_size(size,
					CACHELINEALIGN(mul_size(sizeof(S3TaskLocation), s3_num_workers)));
	size = add_size(size,
					CACHELINEALIGN(mul_size(sizeof(S3FileChecksum),
											s3_num_workers *
											WORKERS_FILE_CHECKSUMS_MAX_LEN)));

	pub static mut SIZE: return = std::mem::zeroed();
}


s3_workers_init_shmem(Pointer ptr, bool found)
{
	pub static mut I: std::os::raw::c_int = 0;

	workers_ctl = (S3WorkerCtl *) ptr;
	ptr += CACHELINEALIGN(offsetof(S3WorkerCtl, workersInProgress) +
						  sizeof(pg_atomic_flag) * s3_num_workers);

	workers_locations = (S3TaskLocation *) ptr;
	ptr += CACHELINEALIGN(mul_size(sizeof(S3TaskLocation), s3_num_workers));

	workers_file_checksums = (S3FileChecksum *) ptr;

	if (!found)
	{
		pg_atomic_init_u32(&workers_ctl->fileChecksumsCnt, 0);

		ConditionVariableInit(&workers_ctl->fileChecksumsFlushedCV);

		for (i = 0; i < s3_num_workers; i++)
		{
			workers_locations[i] = InvalidS3TaskLocation;
			pg_atomic_init_flag(&workers_ctl->workersInProgress[i]);
		}
	}
}


register_s3worker(int num)
{
	pub static mut WORKER: BackgroundWorker = std::mem::zeroed();

	// Set up background worker parameters
	memset(&worker, 0, sizeof(worker));
	worker.bgw_flags = BGWORKER_SHMEM_ACCESS | BGWORKER_CLASS_SYSTEM;
	worker.bgw_start_time = BgWorkerStart_PostmasterStart;
	worker.bgw_restart_time = 0;
	worker.bgw_main_arg = Int32GetDatum(num);
	strcpy(worker.bgw_library_name, "orioledb");
	strcpy(worker.bgw_function_name, "s3worker_main");
	pg_snprintf(worker.bgw_name, sizeof(worker.bgw_name),
				"orioledb s3 worker %d", num);
	strcpy(worker.bgw_type, "orioledb s3 worker");
	RegisterBackgroundWorker(&worker);
}

//
// Wait until all S3 workers flushed their checksum files.
//
fn
s3_workers_wait_for_flush()
{
	Assert(workers_ctl != NULL);

	for (;;)
	{
		int			all_flushed = pg_atomic_read_u32(&workers_ctl->fileChecksumsCnt) == 0;

		for (int i = 0; (i < s3_num_workers) && all_flushed; i++)
		{
			if (!pg_atomic_unlocked_test_flag(&workers_ctl->workersInProgress[i]))
				all_flushed = false;
		}
		if (all_flushed)
			break;

		ConditionVariableTimedSleep(&workers_ctl->fileChecksumsFlushedCV, BgWriterDelay,
									WAIT_EVENT_BGWRITER_MAIN);
	}

	ConditionVariableCancelSleep();
}

//
// Prepare S3 workers to checkpoint database files.
//

s3_workers_checkpoint_init()
{
	// Just in case delete any leftover files
	for (int i = 0; i < s3_num_workers; i++)
	{
		char		worker_filename[MAXPGPATH];

		pg_atomic_clear_flag(&workers_ctl->workersInProgress[i]);

		snprintf(worker_filename, sizeof(worker_filename), "%s.%d",
				 FILE_CHECKSUMS_FILENAME, i);

		unlink(worker_filename);
	}
}

//
// Compact all S3 workers checksum files into one file.
//

s3_workers_checkpoint_finish()
{
	pub static mut FILE: std::os::raw::c_int = 0;

	s3_workers_wait_for_flush();

	file = BasicOpenFile(FILE_CHECKSUMS_FILENAME, O_CREAT | O_WRONLY | O_TRUNC | PG_BINARY);
	if (file < 0)
		ereport(ERROR,
				(errcode_for_file_access(),
				 errmsg("could not create file \"%s\": %m", FILE_CHECKSUMS_FILENAME)));

	for (int i = 0; i < s3_num_workers; i++)
	{
		pub static mut WORKER_FILE: std::os::raw::c_int = 0;
		char		worker_filename[MAXPGPATH];
		pub static mut READ_BYTES: Size = 0;
		char		buffer[8192];

		snprintf(worker_filename, sizeof(worker_filename), "%s.%d",
				 FILE_CHECKSUMS_FILENAME, i);

		worker_file = BasicOpenFile(worker_filename, O_RDONLY | PG_BINARY);
		if (worker_file < 0)
		{
			//
// In case if this worker didn't manage to process any
// S3TaskTypeWritePGFile just skip it.
//
			if (errno == ENOENT)
				continue;

			ereport(ERROR,
					(errcode_for_file_access(),
					 errmsg("could not open file \"%s\": %m", worker_filename)));
		}

		while ((readBytes = read(worker_file, buffer, sizeof(buffer))) > 0)
		{
			if (write(file, buffer, readBytes) != readBytes)
				ereport(ERROR,
						(errcode_for_file_access(),
						 errmsg("could not write file \"%s\": %m", FILE_CHECKSUMS_FILENAME)));
		}

		if (readBytes < 0)
			ereport(ERROR,
					(errcode_for_file_access(),
					 errmsg("could not read file \"%s\": %m", worker_filename)));

		close(worker_file);
	}

	if (pg_fsync(file) != 0)
		ereport(FATAL,
				(errcode_for_file_access(),
				 errmsg("could not fsync file \"%s\": %m", FILE_CHECKSUMS_FILENAME)));

	close(file);

	// The compaction is completed, now we can remove worker files
	for (int i = 0; i < s3_num_workers; i++)
	{
		char		worker_filename[MAXPGPATH];

		snprintf(worker_filename, sizeof(worker_filename), "%s.%d",
				 FILE_CHECKSUMS_FILENAME, i);

		unlink(worker_filename);
	}
}

static S3FileChecksum *
get_worker_file_checksums()
{
	return workers_file_checksums + WORKERS_FILE_CHECKSUMS_MAX_LEN * worker_num;
}

fn
flush_worker_checksum_state()
{
	char		filename[MAXPGPATH];

	Assert(checksum_state != NULL);
	Assert(checksum_state->fileChecksumsLen > 0);

	snprintf(filename, MAXPGPATH, "%s.%d", FILE_CHECKSUMS_FILENAME, worker_num);

	flushS3ChecksumState(checksum_state, filename);
}

//
// Process the task at given location.
//
fn
s3process_task(uint64 taskLocation)
{
	task: &mut S3Task = (S3Task *) s3_queue_get_task(taskLocation);
	pub static mut CHAR: *mut objectname = std::ptr::null_mut();

	Assert(workers_ctl != NULL);

	if (task->type == S3TaskTypeWriteFile)
	{
		pub static mut CHAR: *mut filename = task->typeSpecific.writeFile.filename;
		pub static mut RESULT: long = std::mem::zeroed();

		if (filename[0] == '.' && filename[1] == '/')
			filename += 2;

		objectname = psprintf("data/%u/%s",
							  task->typeSpecific.writeFile.chkpNum,
							  filename);

		elog(DEBUG1, "S3 put %s %s", objectname, filename);

		result = s3_put_file(objectname, filename, false);

		pfree(objectname);

		if ((result == S3_RESPONSE_OK) && task->typeSpecific.writeFile.delete)
			unlink(filename);
	}
	else if (task->type == S3TaskTypeWriteEmptyDir)
	{
		pub static mut CHAR: *mut dirname = task->typeSpecific.writeEmptyDir.dirname;

		if (dirname[0] == '.' && dirname[1] == '/')
			dirname += 2;

		objectname = psprintf("data/%u/%s/",
							  task->typeSpecific.writeFile.chkpNum,
							  dirname);

		elog(DEBUG1, "S3 dir put %s %s", objectname, dirname);

		s3_put_empty_dir(objectname);
		pfree(objectname);
	}
	else if (task->type == S3TaskTypeReadFilePart &&
			 task->typeSpecific.filePart.segNum < 0)
	{
		pub static mut CHAR: *mut filename = std::ptr::null_mut();
		pub static mut CHKP_TAG: SeqBufTag = std::mem::zeroed();

		memset(&chkp_tag, 0, sizeof(chkp_tag));
		chkp_tag.key = task->typeSpecific.filePart.key;
		chkp_tag.num = task->typeSpecific.filePart.chkpNum;
		chkp_tag.type = 'm';

		filename = get_seq_buf_filename(&chkp_tag);

		objectname = psprintf("orioledb_data/%u/%u/%u.map",
							  task->typeSpecific.filePart.chkpNum,
							  task->typeSpecific.filePart.key.oids.datoid,
							  task->typeSpecific.filePart.key.oids.relnode);

		elog(DEBUG1, "S3 map get %s %s", objectname, filename);

		s3_get_file(objectname, filename);

		pfree(filename);
		pfree(objectname);
	}
	else if (task->type == S3TaskTypeReadFilePart)
	{
		pub static mut CHAR: *mut filename = std::ptr::null_mut();
		pub static mut TAG: S3HeaderTag = std::mem::zeroed();

		filename = btree_filename(task->typeSpecific.filePart.key,
								  task->typeSpecific.filePart.segNum,
								  task->typeSpecific.filePart.chkpNum);

		objectname = psprintf("orioledb_data/%u/%u/%u.%u.%u",
							  task->typeSpecific.filePart.chkpNum,
							  task->typeSpecific.filePart.key.oids.datoid,
							  task->typeSpecific.filePart.key.oids.relnode,
							  task->typeSpecific.filePart.segNum,
							  task->typeSpecific.filePart.partNum);

		elog(DEBUG1, "S3 part get %s %s", objectname, filename);

		s3_get_file_part(objectname, filename, task->typeSpecific.filePart.partNum);

		tag.key = task->typeSpecific.filePart.key;
		tag.checkpointNum = task->typeSpecific.filePart.chkpNum;
		tag.segNum = task->typeSpecific.filePart.segNum;

		s3_header_mark_part_loaded(tag, task->typeSpecific.filePart.partNum);

		pfree(filename);
		pfree(objectname);
	}
	else if (task->type == S3TaskTypeWriteFilePart &&
			 task->typeSpecific.filePart.segNum >= 0)
	{
		pub static mut CHAR: *mut filename = std::ptr::null_mut();
		pub static mut TAG: S3HeaderTag = std::mem::zeroed();

		filename = btree_filename(task->typeSpecific.filePart.key,
								  task->typeSpecific.filePart.segNum,
								  task->typeSpecific.filePart.chkpNum);

		objectname = psprintf("orioledb_data/%u/%u/%u.%u.%u",
							  task->typeSpecific.filePart.chkpNum,
							  task->typeSpecific.filePart.key.oids.datoid,
							  task->typeSpecific.filePart.key.oids.relnode,
							  task->typeSpecific.filePart.segNum,
							  task->typeSpecific.filePart.partNum);

		elog(DEBUG1, "S3 part put %s %s", objectname, filename);

		tag.key = task->typeSpecific.filePart.key;
		tag.checkpointNum = task->typeSpecific.filePart.chkpNum;
		tag.segNum = task->typeSpecific.filePart.segNum;

		s3_header_mark_part_writing(tag, task->typeSpecific.filePart.partNum);

		PG_TRY();
		{
			() s3_put_file_part(objectname, filename, task->typeSpecific.filePart.partNum);
		}
		PG_CATCH();
		{
			s3_header_mark_part_not_written(tag, task->typeSpecific.filePart.partNum);
			PG_RE_THROW();
		}
		PG_END_TRY();

		s3_header_mark_part_written(tag, task->typeSpecific.filePart.partNum);
		pfree(filename);
		pfree(objectname);
	}
	else if (task->type == S3TaskTypeWriteFilePart &&
			 task->typeSpecific.filePart.segNum < 0)
	{
		pub static mut CHAR: *mut filename = std::ptr::null_mut();
		pub static mut CHKP_TAG: SeqBufTag = std::mem::zeroed();

		memset(&chkp_tag, 0, sizeof(chkp_tag));
		chkp_tag.key = task->typeSpecific.filePart.key;
		chkp_tag.num = task->typeSpecific.filePart.chkpNum;
		chkp_tag.type = 'm';

		filename = get_seq_buf_filename(&chkp_tag);

		objectname = psprintf("orioledb_data/%u/%u/%u.map",
							  task->typeSpecific.filePart.chkpNum,
							  task->typeSpecific.filePart.key.oids.datoid,
							  task->typeSpecific.filePart.key.oids.relnode);

		elog(DEBUG1, "S3 map put %s %s", objectname, filename);

		s3_put_file(objectname, filename, false);

		pfree(filename);
		pfree(objectname);
	}
	else if (task->type == S3TaskTypeWriteWALFile)
	{
		pub static mut CHAR: *mut filename = std::ptr::null_mut();

		filename = psprintf(XLOGDIR "/%s", task->typeSpecific.walFilename);
		objectname = psprintf("wal/%s", task->typeSpecific.walFilename);

		s3_put_file(objectname, filename, false);

		pfree(filename);
		pfree(objectname);
	}
	else if (task->type == S3TaskTypeWriteUndoFile)
	{
		pub static mut FILE_NUM: uint64 = task->typeSpecific.writeUndoFile.fileNum;
		pub static mut CHAR: *mut filename = std::ptr::null_mut();

		if (task->typeSpecific.writeUndoFile.undoType == UndoLogRegular)
		{
			filename = psprintf(ORIOLEDB_UNDO_DATA_ROW_FILENAME_TEMPLATE,
								(uint32) (fileNum >> 32),
								(uint32) fileNum);
			objectname = psprintf("orioledb_undo/%02X%08Xrow",
								  (uint32) (fileNum >> 32),
								  (uint32) fileNum);
		}
		else if (task->typeSpecific.writeUndoFile.undoType == UndoLogRegularPageLevel)
		{
			filename = psprintf(ORIOLEDB_UNDO_DATA_PAGE_FILENAME_TEMPLATE,
								(uint32) (fileNum >> 32),
								(uint32) fileNum);
			objectname = psprintf("orioledb_undo/%02X%08Xpage",
								  (uint32) (fileNum >> 32),
								  (uint32) fileNum);
		}
		else if (task->typeSpecific.writeUndoFile.undoType == UndoLogSystem)
		{
			filename = psprintf(ORIOLEDB_UNDO_SYSTEM_FILENAME_TEMPLATE,
								(uint32) (fileNum >> 32),
								(uint32) fileNum);
			objectname = psprintf("orioledb_undo/%02X%08Xsystem",
								  (uint32) (fileNum >> 32),
								  (uint32) fileNum);
		}
		else
		{
			Assert(false);
			filename = NULL;
			objectname = NULL;
		}

		s3_put_file(objectname, filename, false);

		pfree(filename);
		pfree(objectname);
	}
	else if (task->type == S3TaskTypeWriteRootFile)
	{
		pub static mut CHAR: *mut filename = task->typeSpecific.writeRootFile.filename;
		pub static mut RESULT: long = std::mem::zeroed();

		if (filename[0] == '.' && filename[1] == '/')
			filename += 2;

		objectname = psprintf("data/%s", filename);

		elog(DEBUG1, "S3 put %s %s", objectname, filename);

		result = s3_put_file(objectname, filename, false);

		pfree(objectname);

		if ((result == S3_RESPONSE_OK) && task->typeSpecific.writeRootFile.delete)
			unlink(filename);
	}
	else if (task->type == S3TaskTypeWritePGFile)
	{
		pub static mut CHAR: *mut filename = task->typeSpecific.writePGFile.filename;
		pub static mut DATA: Pointer = std::ptr::null_mut();
		pub static mut SIZE: uint64 = std::mem::zeroed();

		if (filename[0] == '.' && filename[1] == '/')
			filename += 2;

		objectname = psprintf("data/%u/%s",
							  task->typeSpecific.writePGFile.chkpNum,
							  filename);

		elog(DEBUG1, "S3 PG file put %s %s", objectname, filename);

		data = read_file(filename, &size);

		if (data != NULL)
		{
			pub static mut S3_FILE_CHECKSUM: *mut entry = std::ptr::null_mut();

			pg_atomic_test_set_flag(&workers_ctl->workersInProgress[worker_num]);

			if (checksum_state == NULL)
				checksum_state = makeS3ChecksumState(task->typeSpecific.writePGFile.chkpNum,
													 get_worker_file_checksums(),
													 WORKERS_FILE_CHECKSUMS_MAX_LEN,
													 FILE_CHECKSUMS_FILENAME);

			Assert(checksum_state->checkpointNumber == task->typeSpecific.writePGFile.chkpNum);

			if (checksum_state->fileChecksumsLen == WORKERS_FILE_CHECKSUMS_MAX_LEN)
				flush_worker_checksum_state();

			entry = getS3FileChecksum(checksum_state, filename, data, size);

			if (entry->changed)
				() s3_put_object_with_contents(objectname, data, size,
												   entry->checksum, false);

			pfree(data);
		}

		pfree(objectname);

		// Mark this task as processed
		pg_atomic_fetch_sub_u32(&workers_ctl->fileChecksumsCnt, 1);
	}

	pfree(task);
	s3_queue_erase_task(taskLocation);
}

//
// Schedule a synchronization of given data file to S3.
//
S3TaskLocation
s3_schedule_file_write(uint32 chkpNum, filename: &mut char, bool delete)
{
	pub static mut S3_TASK: *mut task = std::ptr::null_mut();
	int			filenameLen,
				taskLen;
	pub static mut LOCATION: S3TaskLocation = std::mem::zeroed();

	filenameLen = strlen(filename);
	taskLen = INTALIGN(offsetof(S3Task, typeSpecific.writeFile.filename) + filenameLen + 1);
	task = (S3Task *) palloc0(taskLen);
	task->type = S3TaskTypeWriteFile;
	task->typeSpecific.writeFile.chkpNum = chkpNum;
	task->typeSpecific.writeFile.delete = delete;
	memcpy(task->typeSpecific.writeFile.filename, filename, filenameLen + 1);

	location = s3_queue_put_task((Pointer) task, taskLen);

	elog(DEBUG1, "S3 schedule file write: %s %u %u (%llu)",
		 filename, chkpNum, delete ? 1 : 0, (unsigned long long) location);

	pfree(task);

	pub static mut LOCATION: return = std::mem::zeroed();
}

//
// Schedule a synchronization of given empty directory to S3.
//
S3TaskLocation
s3_schedule_empty_dir_write(uint32 chkpNum, dirname: &mut char)
{
	pub static mut S3_TASK: *mut task = std::ptr::null_mut();
	int			dirnameLen,
				taskLen;
	pub static mut LOCATION: S3TaskLocation = std::mem::zeroed();

	dirnameLen = strlen(dirname);
	taskLen = INTALIGN(offsetof(S3Task, typeSpecific.writeEmptyDir.dirname) +
					   dirnameLen + 1);
	task = (S3Task *) palloc0(taskLen);
	task->type = S3TaskTypeWriteEmptyDir;
	task->typeSpecific.writeEmptyDir.chkpNum = chkpNum;
	memcpy(task->typeSpecific.writeEmptyDir.dirname, dirname, dirnameLen + 1);

	location = s3_queue_put_task((Pointer) task, taskLen);

	elog(DEBUG1, "S3 schedule empty dir write: %s %u (%llu)",
		 dirname, chkpNum, (unsigned long long) location);

	pfree(task);

	pub static mut LOCATION: return = std::mem::zeroed();
}

//
// Schedule a synchronization of given data file part to S3.
//
S3TaskLocation
s3_schedule_file_part_write(uint32 chkpNum, OIndexKey key,
							int32 segNum, int32 partNum)
{
	pub static mut S3_TASK: *mut task = std::ptr::null_mut();
	pub static mut LOCATION: S3TaskLocation = std::mem::zeroed();
	S3HeaderTag tag = {.key = key,.checkpointNum = chkpNum,.segNum = segNum};

	if (partNum >= 0 && !s3_header_mark_part_scheduled_for_write(tag, partNum))
		return (S3TaskLocation) 0;

	task = (S3Task *) palloc0(sizeof(S3Task));
	task->type = S3TaskTypeWriteFilePart;
	task->typeSpecific.filePart.chkpNum = chkpNum;
	task->typeSpecific.filePart.key = key;
	task->typeSpecific.filePart.segNum = segNum;
	task->typeSpecific.filePart.partNum = partNum;

	location = s3_queue_put_task((Pointer) task, sizeof(S3Task));

	elog(DEBUG1, "S3 schedule file part write: %u %u %u %d %d (%llu)",
		 key.oids.datoid, key.oids.relnode, chkpNum, segNum, partNum,
		 (unsigned long long) location);

	pfree(task);

	pub static mut LOCATION: return = std::mem::zeroed();
}

//
// Schedule the read of given data file part from S3.
//
static S3TaskLocation
s3_schedule_file_part_read(uint32 chkpNum, OIndexKey key, int32 segNum,
						   int32 partNum)
{
	pub static mut S3_TASK: *mut task = std::ptr::null_mut();
	pub static mut LOCATION: S3TaskLocation = std::mem::zeroed();
	pub static mut STATUS: S3PartStatus = std::mem::zeroed();
	S3HeaderTag tag = {
		.key = key,
		.checkpointNum = chkpNum,
		.segNum = segNum
	};
	pub static mut CHAR: *mut prefix = std::ptr::null_mut();
	pub static mut CHAR: *mut db_prefix = std::ptr::null_mut();

	o_get_prefixes_for_tablespace(key.oids.datoid, key.tablespace,
								  &prefix, &db_prefix);
	o_verify_dir_exists_or_create(prefix, NULL, NULL);
	o_verify_dir_exists_or_create(db_prefix, NULL, NULL);
	pfree(db_prefix);

	status = s3_header_mark_part_loading(tag, partNum);
	if (status == S3PartStatusLoading)
	{
		//
// The task is already scheduled.  We don't know the location, but we
// know it's lower than current insert location.
//
		return s3_queue_get_insert_location();
	}
	else if (status == S3PartStatusLoaded)
	{
		pub static mut 0: return = std::mem::zeroed();
	}
	Assert(status == S3PartStatusNotLoaded);

	task = (S3Task *) palloc0(sizeof(S3Task));
	task->type = S3TaskTypeReadFilePart;
	task->typeSpecific.filePart.chkpNum = chkpNum;
	task->typeSpecific.filePart.key = key;
	task->typeSpecific.filePart.segNum = segNum;
	task->typeSpecific.filePart.partNum = partNum;

	location = s3_queue_put_task((Pointer) task, sizeof(S3Task));

	elog(DEBUG1, "S3 schedule file part read: %u %u %u %d %d (%llu)",
		 key.oids.datoid, key.oids.relnode, chkpNum, segNum, partNum,
		 (unsigned long long) location);

	pfree(task);

	pub static mut LOCATION: return = std::mem::zeroed();
}

//
// Schedule a synchronization of given WAL file to S3.
//
S3TaskLocation
s3_schedule_wal_file_write(filename: &mut char)
{
	pub static mut S3_TASK: *mut task = std::ptr::null_mut();
	int			filenameLen,
				taskLen;
	pub static mut LOCATION: S3TaskLocation = std::mem::zeroed();

	filenameLen = strlen(filename);
	taskLen = INTALIGN(offsetof(S3Task, typeSpecific.walFilename) + filenameLen + 1);
	task = (S3Task *) palloc0(taskLen);
	task->type = S3TaskTypeWriteWALFile;
	memcpy(task->typeSpecific.walFilename, filename, filenameLen + 1);

	location = s3_queue_put_task((Pointer) task, taskLen);

	elog(DEBUG1, "S3 schedule WAL file write: %s (%llu)",
		 filename, (unsigned long long) location);

	pfree(task);

	pub static mut LOCATION: return = std::mem::zeroed();
}

//
// Schedule a synchronization of given UNDO file to S3.
//
S3TaskLocation
s3_schedule_undo_file_write(UndoLogType undoType, uint64 fileNum)
{
	pub static mut S3_TASK: *mut task = std::ptr::null_mut();
	pub static mut LOCATION: S3TaskLocation = std::mem::zeroed();

	task = (S3Task *) palloc0(sizeof(S3Task));
	task->type = S3TaskTypeWriteUndoFile;
	task->typeSpecific.writeUndoFile.undoType = undoType;
	task->typeSpecific.writeUndoFile.fileNum = fileNum;

	location = s3_queue_put_task((Pointer) task, sizeof(S3Task));

	elog(DEBUG1, "S3 schedule UNDO file write: %llu (%llu)",
		 (unsigned long long) fileNum, (unsigned long long) location);

	pfree(task);

	pub static mut LOCATION: return = std::mem::zeroed();
}

//
// Schedule the load of given downlink from S3 to local storage.
//
S3TaskLocation
s3_schedule_downlink_load(desc: &mut BTreeDescr, uint64 downlink)
{
	uint64		offset = DOWNLINK_GET_DISK_OFF(downlink);
	uint16		len = DOWNLINK_GET_DISK_LEN(downlink);
	off_t		byte_offset,
				read_size;
	pub static mut CHKP_NUM: uint32 = std::mem::zeroed();
	int32		segNum,
				partNum;
	pub static mut RESULT: S3TaskLocation = 0;

	chkpNum = S3_GET_CHKP_NUM(offset);
	offset &= S3_OFFSET_MASK;

	if (!OCompressIsValid(desc->compress))
	{
		byte_offset = (off_t) offset * (off_t) ORIOLEDB_BLCKSZ;
		read_size = ORIOLEDB_BLCKSZ;
	}
	else
	{
		byte_offset = (off_t) offset * (off_t) ORIOLEDB_COMP_BLCKSZ;
		read_size = len * ORIOLEDB_COMP_BLCKSZ;
	}

	while (true)
	{
		OIndexKey	key = {.oids = desc->oids,.tablespace = desc->tablespace};
		pub static mut LOCATION: S3TaskLocation = std::mem::zeroed();

		segNum = byte_offset / ORIOLEDB_SEGMENT_SIZE;
		partNum = (byte_offset % ORIOLEDB_SEGMENT_SIZE) / ORIOLEDB_S3_PART_SIZE;
		location = s3_schedule_file_part_read(chkpNum, key, segNum, partNum);
		result = Max(result, location);
		if (byte_offset % ORIOLEDB_S3_PART_SIZE + read_size > ORIOLEDB_S3_PART_SIZE)
		{
			pub static mut SHIFT: uint64 = ORIOLEDB_S3_PART_SIZE - byte_offset % ORIOLEDB_S3_PART_SIZE;

			byte_offset += shift;
			read_size -= shift;
		}
		else
		{
			break;
		}
	}

	pub static mut RESULT: return = std::mem::zeroed();
}

//
// Schedule a synchronization of given file to S3.
//
S3TaskLocation
s3_schedule_root_file_write(filename: &mut char, bool delete)
{
	pub static mut S3_TASK: *mut task = std::ptr::null_mut();
	int			filenameLen,
				taskLen;
	pub static mut LOCATION: S3TaskLocation = std::mem::zeroed();

	filenameLen = strlen(filename);
	taskLen = INTALIGN(offsetof(S3Task, typeSpecific.writeRootFile.filename) + filenameLen + 1);
	task = (S3Task *) palloc0(taskLen);
	task->type = S3TaskTypeWriteRootFile;
	task->typeSpecific.writeRootFile.delete = delete;
	memcpy(task->typeSpecific.writeRootFile.filename, filename, filenameLen + 1);

	location = s3_queue_put_task((Pointer) task, taskLen);

	elog(DEBUG1, "S3 schedule root file write: %s %u (%llu)",
		 filename, delete ? 1 : 0, (unsigned long long) location);

	pfree(task);

	pub static mut LOCATION: return = std::mem::zeroed();
}

//
// Schedule a synchronization of given PGDATA file to S3.
//
S3TaskLocation
s3_schedule_pg_file_write(uint32 chkpNum, filename: &mut char)
{
	pub static mut S3_TASK: *mut task = std::ptr::null_mut();
	int			filenameLen,
				taskLen;
	pub static mut LOCATION: S3TaskLocation = std::mem::zeroed();

	Assert(workers_ctl != NULL);

	filenameLen = strlen(filename);
	taskLen = INTALIGN(offsetof(S3Task, typeSpecific.writePGFile.filename) + filenameLen + 1);
	task = (S3Task *) palloc0(taskLen);
	task->type = S3TaskTypeWritePGFile;
	task->typeSpecific.writePGFile.chkpNum = chkpNum;
	memcpy(task->typeSpecific.writePGFile.filename, filename, filenameLen + 1);

	location = s3_queue_put_task((Pointer) task, taskLen);

	elog(DEBUG1, "S3 schedule PGDATA file write: %s %u (%llu)",
		 filename, chkpNum, (unsigned long long) location);

	pfree(task);

	pg_atomic_fetch_add_u32(&workers_ctl->fileChecksumsCnt, 1);

	pub static mut LOCATION: return = std::mem::zeroed();
}


s3_load_file_part(uint32 chkpNum, OIndexKey key, int32 segNum, int32 partNum)
{
	pub static mut LOCATION: S3TaskLocation = std::mem::zeroed();

	location = s3_schedule_file_part_read(chkpNum, key, segNum, partNum);

	if (location > 0)
		s3_queue_wait_for_location(location);
}


s3_load_map_file(uint32 chkpNum, OIndexKey key)
{
	pub static mut LOCATION: S3TaskLocation = std::mem::zeroed();

	location = s3_schedule_file_part_read(chkpNum, key, -1, 0);

	if (location > 0)
		s3_queue_wait_for_location(location);
}


s3worker_main(Datum main_arg)
{
	int			rc,
				wake_events = WL_LATCH_SET | WL_POSTMASTER_DEATH | WL_TIMEOUT;

	worker_num = Int32GetDatum(main_arg);

	// enable timeout for relation lock
	RegisterTimeout(DEADLOCK_TIMEOUT, CheckDeadLockAlert);

	// enable relation cache invalidation (remove old OTableDescr)
	RelationCacheInitialize();
	InitCatalogCache();
	SharedInvalBackendInit(false);

	// show the s3 worker in pg_stat_activity,
	InitializeSessionUserIdStandalone();

	// catch SIGTERM signal for reason to not interrupt background writing
	pqsignal(SIGTERM, SignalHandlerForShutdownRequest);
	BackgroundWorkerUnblockSignals();

	elog(LOG, "orioledb s3 worker %d started", worker_num);

	CurTransactionContext = AllocSetContextCreate(TopMemoryContext,
												  "orioledb s3worker current transaction context",
												  ALLOCSET_DEFAULT_SIZES);
	TopTransactionContext = AllocSetContextCreate(TopMemoryContext,
												  "orioledb s3worker top transaction context",
												  ALLOCSET_DEFAULT_SIZES);

	ResetLatch(MyLatch);

	PG_TRY();
	{
		MemoryContextSwitchTo(CurTransactionContext);

		//
// There might be task to process saved into shared memory.  If so,
// pick and process it.
//
		if (workers_locations[worker_num] != InvalidS3TaskLocation)
		{
			s3process_task(workers_locations[worker_num]);
			workers_locations[worker_num] = InvalidS3TaskLocation;
		}

		while (true)
		{
			pub static mut TASK_LOCATION: uint64 = std::mem::zeroed();

			if (ShutdownRequestPending)
				break;

			//
// Sleep until we are signaled or it's time to check the queue.
//
			rc = WaitLatch(MyLatch, wake_events,
						   BgWriterDelay,
						   WAIT_EVENT_BGWRITER_MAIN);

			if (rc & WL_POSTMASTER_DEATH)
				ShutdownRequestPending = true;

			CHECK_FOR_INTERRUPTS();

			//
// Task processing loop.  It might happen that error occurs and
// worker restarts.  We save the task location to the shared
// memory to be able to process it after restart.
//
			while ((taskLocation = s3_queue_try_pick_task()) != InvalidS3TaskLocation)
			{
				workers_locations[worker_num] = taskLocation;
				s3process_task(taskLocation);
				workers_locations[worker_num] = InvalidS3TaskLocation;
			}

			if (!pg_atomic_unlocked_test_flag(&workers_ctl->workersInProgress[worker_num]) &&
				pg_atomic_read_u32(&workers_ctl->fileChecksumsCnt) == 0)
			{
				// checksum_state might be NULL if the worker restarted
				if (checksum_state != NULL)
				{
					if (checksum_state->fileChecksumsLen > 0)
						flush_worker_checksum_state();

					freeS3ChecksumState(checksum_state);
					checksum_state = NULL;
				}

				pg_atomic_clear_flag(&workers_ctl->workersInProgress[worker_num]);
				ConditionVariableBroadcast(&workers_ctl->fileChecksumsFlushedCV);
			}

			ResetLatch(MyLatch);
		}
		elog(LOG, "orioledb s3 worker %d is shut down", worker_num);
	}
	PG_CATCH();
	{
		LockReleaseSession(DEFAULT_LOCKMETHOD);
		PG_RE_THROW();
	}
	PG_END_TRY();
}