use crate::orioledb;
use crate::postmaster::interrupt;
use crate::workers::interrupt;
use pgrx::pg_sys;

// -------------------------------------------------------------------------
//
// interrupt.c
// Routines for background workers interrupt handling.
//
// Copyright (c) 2024-2026, Oriole DB Inc.
// Copyright (c) 2025-2026, Supabase Inc.
//
// IDENTIFICATION
// contrib/orioledb/src/workers/interrupt.c
//
// -------------------------------------------------------------------------
//

fn o_worker_shutdown(int elevel);

//
// Exit from an orioledb worker
//
fn
o_worker_shutdown(int elevel)
{
	Assert(MyBackendType == B_BG_WORKER);
	ereport(elevel,
			(errcode(ERRCODE_ADMIN_SHUTDOWN),
			 errmsg("terminating orioledb worker due to administrator command")));
}


o_worker_handle_interrupts()
{
	//
// In case of a pending shutdown request we just raise an ERROR message
// currently.
//
	if (ShutdownRequestPending)
		o_worker_shutdown(ERROR);
}