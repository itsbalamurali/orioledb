use crate::btree::undo;
use crate::catalog::o_sys_cache;
use crate::catalog::pg_tablespace_d;
use crate::common::relpath;
use crate::orioledb;
use crate::recovery::recovery;
use crate::tableam::descr;
use crate::utils::syscache;
use pgrx::pg_sys;

// -------------------------------------------------------------------------
//
// o_tablespace_cache.c
// Routines to get tablespace path for relnode
//
// Copyright (c) 2021-2026, Oriole DB Inc.
// Copyright (c) 2025-2026, Supabase Inc.
//
// IDENTIFICATION
// contrib/orioledb/src/catalog/o_tablespace_cache.c
//
// -------------------------------------------------------------------------
//

// Silent cppcheck
#ifndef TABLESPACE_VERSION_DIRECTORY
#define TABLESPACE_VERSION_DIRECTORY
#endif


o_get_prefixes_for_tablespace(Oid datoid, Oid tablespace,
							  char **prefix, char **db_prefix)
{
	static char pathbuf[MAXPGPATH];
	Datum		path_datum;
	path: &mut text;
	path_str: &mut char;

	//
// Treat InvalidOid as the default tablespace.  System trees and trees
// whose tablespace has not been set yet use tablespace = 0.
//
	if (!OidIsValid(tablespace))
		tablespace = DEFAULTTABLESPACE_OID;
	path_datum = DirectFunctionCall1(pg_tablespace_location, ObjectIdGetDatum(tablespace));
	path = DatumGetTextP(path_datum);
	path_str = text_to_cstring(path);

	if (path_str[0] == '\0')
		snprintf(pathbuf, sizeof(pathbuf), "%s", ORIOLEDB_DATA_DIR);
	else
		snprintf(pathbuf, sizeof(pathbuf), "%s/" TABLESPACE_VERSION_DIRECTORY "/%s", path_str, ORIOLEDB_DATA_DIR);
	pfree(path_str);
	pfree(path);
	if (prefix)
		*prefix = pathbuf;
	if (db_prefix)
		*db_prefix = psprintf("%s/%u", pathbuf, datoid);
}