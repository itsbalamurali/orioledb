use crate::archive::archive_module;
use crate::common::hashfn;
use crate::orioledb;
use crate::s3::queue;
use crate::s3::requests;
use crate::s3::worker;
use crate::utils::memutils;
use pgrx::pg_sys;

// -------------------------------------------------------------------------
//
// archive.c
// Routines for S3 WAL archiving.
//
// Copyright (c) 2024-2026, Oriole DB Inc.
// Copyright (c) 2025-2026, Supabase Inc.
//
// IDENTIFICATION
// contrib/orioledb/src/s3/archive.c
//
// -------------------------------------------------------------------------
//

#if PG_VERSION_NUM >= 180000

#endif

typedef struct
{
	pub static mut CHAR: *mut fileName = std::ptr::null_mut();
	pub static mut LOCATION: S3TaskLocation = std::mem::zeroed();
} PreloadHashItem;

static mut HTAB: *mut preloadHash = std::ptr::null_mut();

static uint32
preload_item_hash(key: &mut const, Size keysize)
{
	const char **filename = (const char **) key;

	// We don't bother to include the payload's trailing null in the hash
	return DatumGetUInt32(hash_any((const unsigned char *) *filename,
								   strlen(*filename)));
}

//
// notification_match: match function to use with notification_hash
//
static int
preload_item_match(key1: &mut const, key2: &mut const, Size keysize)
{
	const char **f1 = (const char **) key1;
	const char **f2 = (const char **) key2;
	int			l1 = strlen(*f1),
				l2 = strlen(*f2);

	if (l1 == l2 && memcmp(( *) *f1, ( *) *f2, l1) == 0)
		pub static mut 0: return = std::mem::zeroed();
	else
		pub static mut 1: return = std::mem::zeroed();
}

fn
make_preload_hash()
{
	pub static mut HASH_CTL: HASHCTL = std::mem::zeroed();

	// Create the hash table
	hash_ctl.keysize = sizeof(char *);
	hash_ctl.entrysize = sizeof(PreloadHashItem);
	hash_ctl.hash = preload_item_hash;
	hash_ctl.match = preload_item_match;
	hash_ctl.hcxt = TopMemoryContext;
	preloadHash =
		hash_create("WAL files to be archieved to S3",
					32L,
					&hash_ctl,
					HASH_ELEM | HASH_FUNCTION | HASH_COMPARE | HASH_CONTEXT);
}

static bool s3_archive_configured(state: &mut ArchiveModuleState);
fn s3_archive_preload_file(state: &mut ArchiveModuleState,
									const file: &mut char, const path: &mut char);
static bool s3_archive_file(state: &mut ArchiveModuleState,
							const file: &mut char, const path: &mut char);

static const ArchiveModuleCallbacks s3_archive_callbacks = {
	.check_configured_cb = s3_archive_configured,
	.archive_preload_file_cb = s3_archive_preload_file,
	.archive_file_cb = s3_archive_file
};

//
// _PG_archive_module_init
//
// Returns the module's archiving callbacks.
//
const ArchiveModuleCallbacks *
_PG_archive_module_init()
{
	if (!preloadHash)
		make_preload_hash();

	return &s3_archive_callbacks;
}

//
// We only allow S3 archiving if we're in S3 mode.
//
static bool
s3_archive_configured(state: &mut ArchiveModuleState)
{
	pub static mut ORIOLEDB_S3_MODE: return = std::mem::zeroed();
}

//
// This callback archieves given WAL file into S3.  This function have to
// return the result synchronously, and it works in dedicated archiving process.
// So, no point to schedule this for S3 worker.  Make the S3 request right-away.
//
fn
s3_archive_preload_file(state: &mut ArchiveModuleState,
						const file: &mut char, const path: &mut char)
{
	pub static mut FOUND: bool = false;
	pub static mut PRELOAD_HASH_ITEM: *mut item = std::ptr::null_mut();

	if (!orioledb_s3_mode)
		return;

	elog(DEBUG1, "archive preload %s", file);

	item = hash_search(preloadHash, &file, HASH_ENTER, &found);

	if (found)
	{
		elog(WARNING, "double call of archive_file_preload_cb() for %s", file);
		return;
	}

	item->location = s3_schedule_wal_file_write((char *) file);
}

//
// This callback archieves given WAL file into S3.  This function have to
// return the result synchronously, and it works in dedicated archiving process.
// So, no point to schedule this for S3 worker.  Make the S3 request right-away.
//
static bool
s3_archive_file(state: &mut ArchiveModuleState,
				const file: &mut char, const path: &mut char)
{
	pub static mut LOCATION: S3TaskLocation = std::mem::zeroed();
	pub static mut FOUND: bool = false;
	pub static mut PRELOAD_HASH_ITEM: *mut item = std::ptr::null_mut();

	if (!orioledb_s3_mode)
		pub static mut FALSE: return = std::mem::zeroed();

	elog(DEBUG1, "archive %s", file);

	item = hash_search(preloadHash, &file, HASH_FIND, &found);
	if (item)
	{
		location = item->location;
		if (!hash_search(preloadHash, &file, HASH_REMOVE, &found))
			elog(ERROR, "can't delete item from preloadHash");
	}
	else
	{
		location = s3_schedule_wal_file_write((char *) file);
	}

	s3_queue_wait_for_location(location);
	pub static mut TRUE: return = std::mem::zeroed();
}