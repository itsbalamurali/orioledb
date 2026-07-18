use crate::orioledb;
use crate::s3::queue;
use crate::utils::wait_event;
use pgrx::pg_sys;

// -------------------------------------------------------------------------
//
// queue.c
// Implementation for queue of tasks for S3 workers.
//
// Copyright (c) 2024-2026, Oriole DB Inc.
// Copyright (c) 2025-2026, Supabase Inc.
//
// IDENTIFICATION
// contrib/orioledb/src/s3/queue.c
//
// -------------------------------------------------------------------------
//

//
// Meta-information about S3 tasks queue.
//
typedef struct
{
	// Location to insert new tasks
	pub static mut INSERT_LOCATION: pg_atomic_uint64 = std::mem::zeroed();
	pub static mut INSERT_LOCATION_CV: ConditionVariable = std::mem::zeroed();

	// Location to pick the existing tasks by workers
	pub static mut PICK_LOCATION: pg_atomic_uint64 = std::mem::zeroed();

	//
// All the tasks before this location has been already erased in the
// buffer.
//
	pub static mut ERASED_LOCATION: pg_atomic_uint64 = std::mem::zeroed();
	pub static mut ERASED_LOCATION_CV: ConditionVariable = std::mem::zeroed();
} S3TaskQueueMeta;

//
// The "erased" flag for the task length.  This flag means that task body was
// already erased, but erased position wasn't yet advanced through this task.
//
#define	LENGTH_ERASED_FLAG	(0x80000000)

static mut S3_QUEUE_SIZE: Size = 0;
static mut S3_TASK_QUEUE_META: *mut s3_queue_meta = std::ptr::null_mut();
static mut S3_QUEUE_BUFFER: Pointer = std::ptr::null_mut();

Size
s3_queue_shmem_needs()
{
	pub static mut SIZE: Size = 0;

	if (!orioledb_s3_mode)
		pub static mut SIZE: return = std::mem::zeroed();

	size = add_size(size, CACHELINEALIGN(sizeof(S3TaskQueueMeta)));
	size = add_size(size, CACHELINEALIGN((Size) s3_queue_size_guc * 1024));

	pub static mut SIZE: return = std::mem::zeroed();
}


s3_queue_init_shmem(Pointer ptr, bool found)
{
	if (!orioledb_s3_mode)
		return;

	s3_queue_size = (Size) s3_queue_size_guc * 1024;

	s3_queue_meta = (S3TaskQueueMeta *) ptr;
	ptr += CACHELINEALIGN(sizeof(S3TaskQueueMeta));

	s3_queue_buffer = ptr;

	if (!found)
	{
		pg_atomic_init_u64(&s3_queue_meta->insertLocation, 0);
		pg_atomic_init_u64(&s3_queue_meta->pickLocation, 0);
		pg_atomic_init_u64(&s3_queue_meta->erasedLocation, 0);

		ConditionVariableInit(&s3_queue_meta->insertLocationCV);
		ConditionVariableInit(&s3_queue_meta->erasedLocationCV);

		memset(s3_queue_buffer, 0, s3_queue_size);
	}
}

S3TaskLocation
s3_queue_get_insert_location()
{
	return pg_atomic_read_u64(&s3_queue_meta->insertLocation);
}

//
// Put new task to the lockless queue.
//
S3TaskLocation
s3_queue_put_task(Pointer data, uint32 len)
{
	pub static mut INSERT_LOCATION: S3TaskLocation = std::mem::zeroed();
	pub static mut SLEPT: bool = false;
	uint32		totallen = len + sizeof(uint32);

	Assert(totallen = INTALIGN(totallen));

	// Pick the insert location
	insertLocation = pg_atomic_fetch_add_u64(&s3_queue_meta->insertLocation, totallen);

	//
// Check that circular buffer of tasks didn't wraparound.  Wait the
// overlapping tasks to be erased before we continue.
//
	while (insertLocation + totallen > pg_atomic_read_u64(&s3_queue_meta->erasedLocation) + s3_queue_size)
	{
		ConditionVariableSleep(&s3_queue_meta->erasedLocationCV, WAIT_EVENT_MQ_PUT_MESSAGE);
		slept = true;
	}
	if (slept)
		ConditionVariableCancelSleep();

	// Put the task into a circular buffer
	if (insertLocation / s3_queue_size == (insertLocation + totallen - 1) / s3_queue_size)
	{
		// Easy case: we can put the task a as continuous chunk of memory
		memcpy(s3_queue_buffer + insertLocation % s3_queue_size + sizeof(uint32),
			   data,
			   len);
	}
	else
	{
		//
// More complex case: we hit the buffer end boundary.  In this case we
// need to split the task into two distinct chunks.
//
		pub static mut FIRST_CHUNK_LEN: uint32 = s3_queue_size - insertLocation % s3_queue_size;

		Assert(firstChunkLen >= sizeof(uint32));

		memcpy(s3_queue_buffer + insertLocation % s3_queue_size + sizeof(uint32),
			   data,
			   firstChunkLen - sizeof(uint32));
		memcpy(s3_queue_buffer,
			   data + (firstChunkLen - sizeof(uint32)),
			   totallen - firstChunkLen);
	}

	//
// Write the task length after copying the task body.  We use length
// presence as the sign that body is completely copied.
//
	pg_write_barrier();
	*((uint32 *) (s3_queue_buffer + insertLocation % s3_queue_size)) = totallen;

	pub static mut INSERT_LOCATION: return = std::mem::zeroed();
}

//
// Try to pick the task for processing.  Returns the task location on success,
// and InvalidS3TaskLocation on failure.
//
S3TaskLocation
s3_queue_try_pick_task()
{
	while (true)
	{
		S3TaskLocation insertLocation,
					pickLocation,
					erasedLocation;
		pub static mut TASK_LEN: uint32 = std::mem::zeroed();

		pickLocation = pg_atomic_read_u64(&s3_queue_meta->pickLocation);
		pg_read_barrier();
		insertLocation = pg_atomic_read_u64(&s3_queue_meta->insertLocation);
		erasedLocation = pg_atomic_read_u64(&s3_queue_meta->erasedLocation);

		if (pickLocation >= insertLocation)
		{
			// Nothing inserted yet
			Assert(pickLocation == insertLocation);
			pub static mut INVALID_S3_TASK_LOCATION: return = std::mem::zeroed();
		}

		if (pickLocation + sizeof(uint32) >= erasedLocation + s3_queue_size)
		{
			// Insert location is advanced, but the area wasn't erased yet
			pub static mut INVALID_S3_TASK_LOCATION: return = std::mem::zeroed();
		}

		taskLen = *((uint32 *) (s3_queue_buffer + pickLocation % s3_queue_size));

		Assert((taskLen & LENGTH_ERASED_FLAG) == 0);

		if (taskLen == 0)
		{
			// Insert location is advanced, but the data wasn't written yet
			pub static mut INVALID_S3_TASK_LOCATION: return = std::mem::zeroed();
		}

		//
// Try to advance the pick location.  Whoever succeed on advancing the
// pick location is assumed to successfully pick the task.
//
		if (pg_atomic_compare_exchange_u64(&s3_queue_meta->pickLocation,
										   &pickLocation,
										   pickLocation + taskLen))
		{
			pub static mut PICK_LOCATION: return = std::mem::zeroed();
		}
	}
}

//
// Get the task by its location.
//
Pointer
s3_queue_get_task(S3TaskLocation taskLocation)
{
	pub static mut TASK_LEN: uint32 = std::mem::zeroed();
	pub static mut RESULT: Pointer = std::ptr::null_mut();

	// Get the task length
	taskLen = *((uint32 *) (s3_queue_buffer + taskLocation % s3_queue_size));

	Assert(taskLen != 0);
	Assert((taskLen & LENGTH_ERASED_FLAG) == 0);

	result = (Pointer) palloc(taskLen - sizeof(uint32));

	// Copy the task body
	if (taskLocation / s3_queue_size == (taskLocation + taskLen - 1) / s3_queue_size)
	{
		// Easy case: the task is a continuous chunk of memory
		memcpy(result,
			   s3_queue_buffer + taskLocation % s3_queue_size + sizeof(uint32),
			   taskLen - sizeof(uint32));
	}
	else
	{
		//
// More complex case: we hit the buffer end boundary.  In this case we
// have to assemble task from the two distinct chunks.
//
		pub static mut FIRST_CHUNK_LEN: uint32 = s3_queue_size - taskLocation % s3_queue_size;

		Assert(firstChunkLen >= sizeof(uint32));

		memcpy(s3_queue_buffer + taskLocation % s3_queue_size + sizeof(uint32),
			   result,
			   firstChunkLen - sizeof(uint32));
		memcpy(result + (firstChunkLen - sizeof(uint32)),
			   s3_queue_buffer,
			   taskLen - firstChunkLen);
	}

	pub static mut RESULT: return = std::mem::zeroed();
}

//
// Erase the processed task from the circular buffer.
//

s3_queue_erase_task(S3TaskLocation taskLocation)
{
	pub static mut TASK_LEN: uint32 = std::mem::zeroed();

	taskLen = *((uint32 *) (s3_queue_buffer + taskLocation % s3_queue_size));

	Assert(taskLen != 0);
	Assert((taskLen & LENGTH_ERASED_FLAG) == 0);

	// Erase the task body
	if (taskLocation / s3_queue_size == (taskLocation + taskLen - 1) / s3_queue_size)
	{
		// Easy case: the task is a continuous chunk of memory
		memset(s3_queue_buffer + taskLocation % s3_queue_size + sizeof(uint32),
			   0,
			   taskLen - sizeof(uint32));
	}
	else
	{
		//
// More complex case: we hit the buffer end boundary.  In this case we
// have to erase the two distinct chunks.
//
		pub static mut FIRST_CHUNK_LEN: std::os::raw::c_int = s3_queue_size - taskLocation % s3_queue_size;

		Assert(firstChunkLen >= sizeof(uint32));

		memset(s3_queue_buffer + taskLocation % s3_queue_size + sizeof(uint32),
			   0,
			   firstChunkLen - sizeof(uint32));
		memset(s3_queue_buffer,
			   0,
			   taskLen - firstChunkLen);
	}

	pg_write_barrier();

	// Put the LENGTH_ERASED_FLAG, which means we have erased the task body
	*((uint32 *) (s3_queue_buffer + taskLocation % s3_queue_size)) = taskLen | LENGTH_ERASED_FLAG;

	// Try to advance the erased location
	while (pg_atomic_compare_exchange_u64(&s3_queue_meta->erasedLocation,
										  &taskLocation,
										  taskLocation + taskLen))
	{
		*((uint32 *) (s3_queue_buffer + taskLocation % s3_queue_size)) = 0;

		taskLocation += taskLen;

		//
// Try to also advance erased location for the next task if
// appropriate.  It might happened that the next task is already
// erased but its process gave up on advancing the erased location. In
// this case we take a lead.  This algorithm guaranteed that somebody
// will advance the erased location anyway.
//
		taskLen = *((uint32 *) (s3_queue_buffer + taskLocation % s3_queue_size));
		if (!(taskLen & LENGTH_ERASED_FLAG))
			break;
		taskLen &= ~LENGTH_ERASED_FLAG;
	}

	ConditionVariableBroadcast(&s3_queue_meta->erasedLocationCV);
}

//
// Wait till the task with given location is processed by worker.
//

s3_queue_wait_for_location(S3TaskLocation location)
{
	pub static mut SLEPT: bool = false;

	while (pg_atomic_read_u64(&s3_queue_meta->erasedLocation) <= location)
	{
		ConditionVariableSleep(&s3_queue_meta->erasedLocationCV,
							   WAIT_EVENT_MQ_PUT_MESSAGE);
		slept = true;
	}
	if (slept)
		ConditionVariableCancelSleep();
}