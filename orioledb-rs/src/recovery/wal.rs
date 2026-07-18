use crate::access::xloginsert;
use crate::catalog::sys_trees;
use crate::orioledb;
use crate::recovery::recovery;
use crate::recovery::wal;
use crate::recovery::wal_record;
use crate::replication::message;
use crate::replication::origin;
use crate::storage::proc;
use crate::tableam::descr;
use crate::transam::oxid;
use pgrx::pg_sys;

// -------------------------------------------------------------------------
//
// wal.c
// Routines dealing with WAL for orioledb.
//
// Copyright (c) 2021-2026, Oriole DB Inc.
// Copyright (c) 2025-2026, Supabase Inc.
//
// IDENTIFICATION
// contrib/orioledb/src/recovery/wal.c
//
// -------------------------------------------------------------------------
//

fn add_rel_wal_record(ORelOids oids, OIndexType type, uint32 version, uint32 base_version);

typedef struct
{
	pub static mut BUFFER_OFFSET: std::os::raw::c_int = 0;
	pub static mut HAS_MATERIAL_CHANGES: bool = false;
	pub static mut CONTAINS_XID: bool = false;
	pub static mut CONTAINS_SWITCH_XID: bool = false;
	pub static mut OIDS: ORelOids = std::mem::zeroed();
	pub static mut IX_TYPE: OIndexType = std::mem::zeroed();
	char		buffer[LOCAL_WAL_BUFFER_SIZE];
} LocalWal;

static mut LOCAL_WAL: LocalWal = std::mem::zeroed();

fn add_finish_wal_record(uint8 rec_type, OXid xmin);
fn add_joint_commit_wal_record(TransactionId xid, OXid xmin);
fn add_xid_wal_record(OXid oxid, TransactionId logicalXid);
fn add_xid_wal_record_if_needed();
fn flush_local_wal_if_needed(int required_length);
static inline  add_local_modify(uint8 record_type, OTuple record, OffsetNumber length, OTuple record2, OffsetNumber length2);
fn add_modify_wal_record_extended(uint8 rec_type, desc: &mut BTreeDescr,
										   OTuple tuple, OffsetNumber length, OTuple tuple2, OffsetNumber length2, char relreplident, uint32 version, uint32 base_version);
fn add_relreplident_wal_record(char relreplident);
static XLogRecPtr log_logical_wal_container(Pointer ptr, int length, bool withXactTime);

#define XID_RESERVED_LENGTH ((local_wal.contains_xid) ? 0 : sizeof(WALRecXid))


add_modify_wal_record(uint8 rec_type, desc: &mut BTreeDescr,
					  OTuple tuple, OffsetNumber length, char relreplident, uint32 version, uint32 base_version)
{
	pub static mut NULLTUP: OTuple = std::mem::zeroed();

	O_TUPLE_SET_NULL(nulltup);
	add_modify_wal_record_extended(rec_type, desc, tuple, length, nulltup, 0, relreplident, version, base_version);
}

//
// Extended version of add_modify_wal_record for WAL records that can accommodate two tuples.
// This is used for UPDATE/DELETE with REPLICA IDENTITY FULL and for REINSERT
//
fn
add_modify_wal_record_extended(uint8 rec_type, desc: &mut BTreeDescr,
							   OTuple tuple, OffsetNumber length, OTuple tuple2, OffsetNumber length2, char relreplident, uint32 version, uint32 base_version)
{
	pub static mut REQUIRED_LENGTH: std::os::raw::c_int = 0;
	pub static mut OIDS: ORelOids = desc->oids;
	pub static mut TYPE: OIndexType = desc->type;
	pub static mut WRITE_TWO_TUPLES: bool = false;

	elog(DEBUG4, "[%s] rec_type %d oids [ %u %u %u ]", __func__, rec_type, oids.datoid, oids.reloid, oids.relnode);

	// Do not write WAL during recovery
	if (OXidIsValid(recovery_oxid))
		return;

	if (!IS_SYS_TREE_OIDS(oids) && type == oIndexPrimary)
	{
		id: &mut OIndexDescr = (OIndexDescr *) desc->arg;

		oids = id->tableOids;
		type = oIndexInvalid;
	}

	Assert(!is_recovery_process());
	Assert(rec_type == WAL_REC_INSERT || rec_type == WAL_REC_UPDATE || rec_type == WAL_REC_DELETE || rec_type == WAL_REC_REINSERT);
	Assert(!O_TUPLE_IS_NULL(tuple));

	write_two_tuples = (rec_type == WAL_REC_REINSERT || (rec_type == WAL_REC_UPDATE && relreplident == REPLICA_IDENTITY_FULL));

	if (!write_two_tuples)
	{
		Assert(length2 == 0);
		Assert(O_TUPLE_IS_NULL(tuple2));
		required_length = sizeof(WALRecModify1) + length;
	}
	else
	{
		Assert(length2 > 0);
		Assert(!O_TUPLE_IS_NULL(tuple2));
		required_length = sizeof(WALRecModify2) + length + length2;
	}

	elog(DEBUG4, "add_modify_wal_record_extended length1 %d length2 %d", length, length2);
	if (!ORelOidsIsEqual(local_wal.oids, oids) || type != local_wal.ix_type)
		required_length += sizeof(WALRecRelation);

	if (relreplident != REPLICA_IDENTITY_DEFAULT)
		required_length += sizeof(WALRecRelReplident);

	flush_local_wal_if_needed(required_length);
	Assert(local_wal.buffer_offset + required_length + XID_RESERVED_LENGTH <= LOCAL_WAL_BUFFER_SIZE);

	add_xid_wal_record_if_needed();

	if (!ORelOidsIsEqual(local_wal.oids, oids) || type != local_wal.ix_type)
	{
		add_rel_wal_record(oids, type, version, base_version);
		if (relreplident != REPLICA_IDENTITY_DEFAULT)
			add_relreplident_wal_record(relreplident);
	}

	add_local_modify(rec_type, tuple, length, tuple2, length2);
}


add_bridge_erase_wal_record(desc: &mut BTreeDescr, ItemPointer iptr, uint32 version, uint32 base_version)
{
	pub static mut REQUIRED_LENGTH: std::os::raw::c_int = 0;
	pub static mut OIDS: ORelOids = desc->oids;
	pub static mut TYPE: OIndexType = desc->type;
	pub static mut WAL_REC_BRIDGE_ERASE: *mut rec = std::ptr::null_mut();

	// Do not write WAL during recovery
	if (OXidIsValid(recovery_oxid))
		return;

	if (!IS_SYS_TREE_OIDS(oids) && type == oIndexPrimary)
	{
		id: &mut OIndexDescr = (OIndexDescr *) desc->arg;

		oids = id->tableOids;
		type = oIndexInvalid;
	}

	Assert(!is_recovery_process());

	required_length = sizeof(WALRecBridgeErase);

	if (!ORelOidsIsEqual(local_wal.oids, oids) || type != local_wal.ix_type)
		required_length += sizeof(WALRecRelation);

	flush_local_wal_if_needed(required_length);
	Assert(local_wal.buffer_offset + required_length + XID_RESERVED_LENGTH <= LOCAL_WAL_BUFFER_SIZE);

	if (OXidIsValid(get_current_oxid_if_any()))
		add_xid_wal_record_if_needed();

	if (!ORelOidsIsEqual(local_wal.oids, oids) || type != local_wal.ix_type)
		add_rel_wal_record(oids, type, version, base_version);

	Assert(local_wal.buffer_offset + sizeof(*rec) + XID_RESERVED_LENGTH <= LOCAL_WAL_BUFFER_SIZE);

	rec = (WALRecBridgeErase *) (&local_wal.buffer[local_wal.buffer_offset]);
	rec->recType = WAL_REC_BRIDGE_ERASE;
	memcpy(rec->iptr, iptr, sizeof(rec->iptr));
	local_wal.buffer_offset += sizeof(*rec);
}

//
// Adds the record to the local_wal.buffer.
//
static inline 
add_local_modify(uint8 record_type, OTuple record1, OffsetNumber length1, OTuple record2, OffsetNumber length2)
{
	Assert(!O_TUPLE_IS_NULL(record1));
	Assert(length1);

	if (!O_TUPLE_IS_NULL(record2))
	{
		// Two-tuple modify record
		pub static mut WAL_REC_MODIFY2: *mut wal_rec = std::ptr::null_mut();

		Assert(length2);
		Assert(local_wal.buffer_offset + sizeof(*wal_rec) + length1 + length2 + XID_RESERVED_LENGTH <= LOCAL_WAL_BUFFER_SIZE);
		wal_rec = (WALRecModify2 *) (&local_wal.buffer[local_wal.buffer_offset]);
		wal_rec->recType = record_type;
		wal_rec->tupleFormatFlags1 = record1.formatFlags;
		wal_rec->tupleFormatFlags2 = record2.formatFlags;
		memcpy(wal_rec->length1, &length1, sizeof(OffsetNumber));
		memcpy(wal_rec->length2, &length2, sizeof(OffsetNumber));
		local_wal.buffer_offset += sizeof(*wal_rec);

		memcpy(&local_wal.buffer[local_wal.buffer_offset], record1.data, length1);
		local_wal.buffer_offset += length1;
		memcpy(&local_wal.buffer[local_wal.buffer_offset], record2.data, length2);
		local_wal.buffer_offset += length2;
	}
	else
	{
		// One-tuple modify record
		pub static mut WAL_REC_MODIFY1: *mut wal_rec = std::ptr::null_mut();

		Assert(local_wal.buffer_offset + sizeof(*wal_rec) + length1 + XID_RESERVED_LENGTH <= LOCAL_WAL_BUFFER_SIZE);
		Assert(length2 == 0);

		wal_rec = (WALRecModify1 *) (&local_wal.buffer[local_wal.buffer_offset]);
		wal_rec->recType = record_type;
		wal_rec->tupleFormatFlags = record1.formatFlags;
		memcpy(wal_rec->length, &length1, sizeof(OffsetNumber));
		local_wal.buffer_offset += sizeof(*wal_rec);

		memcpy(&local_wal.buffer[local_wal.buffer_offset], record1.data, length1);
		local_wal.buffer_offset += length1;
	}

	local_wal.has_material_changes = true;
}

XLogRecPtr
wal_commit(OXid oxid, TransactionId logicalXid, bool isAutonomous)
{
	pub static mut WAL_POS: XLogRecPtr = std::mem::zeroed();
	pub static mut REC_LENGTH: std::os::raw::c_int = 0;

	Assert(!is_recovery_process());

	if (!local_wal.has_material_changes)
	{
		local_wal.buffer_offset = 0;
		local_wal.ix_type = oIndexInvalid;
		ORelOidsSetInvalid(local_wal.oids);
		pub static mut INVALID_X_LOG_REC_PTR: return = std::mem::zeroed();
	}

	recLength = sizeof(WALRecFinish) + ((synchronous_commit >= SYNCHRONOUS_COMMIT_REMOTE_APPLY) ? sizeof(WALRec) : 0);
	flush_local_wal_if_needed(recLength);
	Assert(local_wal.buffer_offset + recLength + XID_RESERVED_LENGTH <= LOCAL_WAL_BUFFER_SIZE);

	if (!local_wal.contains_xid)
		add_xid_wal_record(oxid, logicalXid);

	add_finish_wal_record(WAL_REC_COMMIT, pg_atomic_read_u64(&xid_meta->runXmin));
	walPos = flush_local_wal(true, !isAutonomous);
	local_wal.has_material_changes = false;

	pub static mut WAL_POS: return = std::mem::zeroed();
}

XLogRecPtr
wal_joint_commit(OXid oxid, TransactionId logicalXid, TransactionId xid,
				 bool subTransaction)
{
	pub static mut WAL_POS: XLogRecPtr = std::mem::zeroed();

	Assert(!is_recovery_process());

	flush_local_wal_if_needed(sizeof(WALRecJointCommit));
	Assert(local_wal.buffer_offset + sizeof(WALRecJointCommit) + XID_RESERVED_LENGTH <= LOCAL_WAL_BUFFER_SIZE);

	if (!local_wal.contains_xid)
		add_xid_wal_record(oxid, logicalXid);

	add_joint_commit_wal_record(xid, pg_atomic_read_u64(&xid_meta->runXmin));
	walPos = flush_local_wal(!subTransaction, false);
	local_wal.has_material_changes = false;

	//
// Don't need to flush local WAL, because we only commit if builtin
// transaction commits.
//
	pub static mut WAL_POS: return = std::mem::zeroed();
}


wal_after_commit()
{
	curProcData: &mut ODBProcData = GET_CUR_PROCDATA();

	pg_atomic_write_u64(&curProcData->commitInProgressXlogLocation, OWalInvalidCommitPos);
}


wal_rollback(OXid oxid, TransactionId logicalXid, bool isAutonomous)
{
	pub static mut WAIT_POS: XLogRecPtr = std::mem::zeroed();

	if (!local_wal.has_material_changes)
	{
		local_wal.buffer_offset = 0;
		local_wal.ix_type = oIndexInvalid;
		ORelOidsSetInvalid(local_wal.oids);
		return;
	}

	Assert(!is_recovery_process());
	flush_local_wal_if_needed(sizeof(WALRecFinish));
	Assert(local_wal.buffer_offset + sizeof(WALRecFinish) + XID_RESERVED_LENGTH <= LOCAL_WAL_BUFFER_SIZE);

	if (!local_wal.contains_xid)
		add_xid_wal_record(oxid, logicalXid);

	add_finish_wal_record(WAL_REC_ROLLBACK,
						  pg_atomic_read_u64(&xid_meta->runXmin));
	wait_pos = flush_local_wal(false, !isAutonomous);
	local_wal.has_material_changes = false;

	elog(DEBUG4, "ROLLBACK oxid " UINT64_FORMAT " logicalXid %u",
		 oxid, logicalXid);

	if (synchronous_commit > SYNCHRONOUS_COMMIT_OFF)
		XLogFlush(wait_pos);
}

//
// Emit a stand-alone WAL_REC_ROLLBACK on behalf of an in-flight oxid that
// recovery_finish() aborted in memory after end-of-redo.
//
// Streaming standbys eagerly apply each modify record marked
// COMMITSEQNO_INPROGRESS and rely on a later WAL_REC_COMMIT/ROLLBACK to
// resolve the verdict.  The primary's normal abort path (wal_rollback) gates
// on local_wal.has_material_changes — but the startup process that runs
// recovery_finish() never wrote those records into its own local_wal buffer
// (the original primary did, before it crashed), so wal_rollback() would
// silently no-op.  Without an explicit ROLLBACK marker on the wire, the
// standby holds the oxid INPROGRESS forever and livelocks on the next
// conflicting modify (orioledb/orioledb#876).
//
// Must be called after LocalSetXLogInsertAllowed() — i.e. from the
// after_checkpoint_cleanup_hook with flags=0, not from inside rm_cleanup
// itself, which still runs with XLogInsertAllowed() == false.
//

wal_emit_recovery_finish_rollback(OXid oxid, TransactionId logicalXid)
{
	pub static mut WAIT_POS: XLogRecPtr = std::mem::zeroed();

	Assert(!is_recovery_process());
	Assert(local_wal.buffer_offset == 0);
	Assert(!local_wal.contains_xid);

	add_xid_wal_record(oxid, logicalXid);
	add_finish_wal_record(WAL_REC_ROLLBACK,
						  pg_atomic_read_u64(&xid_meta->runXmin));
	wait_pos = flush_local_wal(false, false);
	local_wal.has_material_changes = false;

	elog(DEBUG1, "recovery-finish ROLLBACK oxid " UINT64_FORMAT " logicalXid %u %X/%X",
		 oxid, logicalXid, LSN_FORMAT_ARGS(wait_pos));

	XLogFlush(wait_pos);
}

fn
add_finish_wal_record(uint8 rec_type, OXid xmin)
{
	pub static mut WAL_REC_FINISH: *mut rec = std::ptr::null_mut();
	pub static mut PG_USED_FOR_ASSERTS_ONLY: int			recLength = std::mem::zeroed();
	pub static mut CSN: CommitSeqNo = std::mem::zeroed();

	Assert(!is_recovery_process());
	Assert(rec_type == WAL_REC_COMMIT || rec_type == WAL_REC_ROLLBACK);

	recLength = sizeof(WALRecFinish);
	if (rec_type == WAL_REC_COMMIT &&
		synchronous_commit >= SYNCHRONOUS_COMMIT_REMOTE_APPLY)
		recLength += sizeof(WALRec);

	Assert(local_wal.buffer_offset + recLength + XID_RESERVED_LENGTH <= LOCAL_WAL_BUFFER_SIZE);

	if (rec_type == WAL_REC_COMMIT &&
		synchronous_commit >= SYNCHRONOUS_COMMIT_REMOTE_APPLY)
	{
		feedbackRec: &mut WALRec = (WALRec *) (&local_wal.buffer[local_wal.buffer_offset]);

		feedbackRec->recType = WAL_REC_REPLAY_FEEDBACK;
		local_wal.buffer_offset += sizeof(*feedbackRec);
	}

	rec = (WALRecFinish *) (&local_wal.buffer[local_wal.buffer_offset]);
	rec->recType = rec_type;
	memcpy(rec->xmin, &xmin, sizeof(xmin));
	csn = pg_atomic_read_u64(&TRANSAM_VARIABLES->nextCommitSeqNo);
	memcpy(rec->csn, &csn, sizeof(csn));

	local_wal.buffer_offset += sizeof(*rec);

	local_wal.contains_switch_xid = false;
}

fn
add_joint_commit_wal_record(TransactionId xid, OXid xmin)
{
	pub static mut WAL_REC_JOINT_COMMIT: *mut rec = std::ptr::null_mut();
	pub static mut CSN: CommitSeqNo = std::mem::zeroed();

	Assert(!is_recovery_process());

	flush_local_wal_if_needed(sizeof(*rec));

	Assert(local_wal.buffer_offset + sizeof(*rec) + XID_RESERVED_LENGTH <= LOCAL_WAL_BUFFER_SIZE);

	add_xid_wal_record_if_needed();

	rec = (WALRecJointCommit *) (&local_wal.buffer[local_wal.buffer_offset]);
	rec->recType = WAL_REC_JOINT_COMMIT;
	memcpy(rec->xid, &xid, sizeof(xid));
	memcpy(rec->xmin, &xmin, sizeof(xmin));
	csn = pg_atomic_read_u64(&TRANSAM_VARIABLES->nextCommitSeqNo);
	memcpy(rec->csn, &csn, sizeof(csn));
	local_wal.buffer_offset += sizeof(*rec);

	local_wal.contains_switch_xid = false;
}

//
// Returns size of a new record.
//
fn
add_xid_wal_record(OXid oxid, TransactionId logicalXid)
{
	pub static mut WAL_REC_XID: *mut rec = std::ptr::null_mut();
	pub static mut HEAP_XID: TransactionId = std::mem::zeroed();

	Assert(!local_wal.contains_xid);
	local_wal.contains_xid = true;
	Assert(!is_recovery_process());
	Assert(OXidIsValid(oxid));
	Assert(local_wal.buffer_offset + sizeof(*rec) <= LOCAL_WAL_BUFFER_SIZE);

	heapXid = GetTopTransactionIdIfAny();

	rec = (WALRecXid *) (&local_wal.buffer[local_wal.buffer_offset]);
	rec->recType = WAL_REC_XID;
	memcpy(rec->oxid, &oxid, sizeof(OXid));
	memcpy(rec->logicalXid, &logicalXid, sizeof(TransactionId));
	memcpy(rec->heapXid, &heapXid, sizeof(TransactionId));

	local_wal.buffer_offset += sizeof(*rec);
}

fn
add_xid_wal_record_if_needed()
{
	if (!local_wal.contains_xid)
	{
		OXid		oxid = get_current_oxid_if_any();
		TransactionId logicalXid = get_current_logical_xid();

		Assert(oxid != InvalidOXid);
		add_xid_wal_record(oxid, logicalXid);
	}
}

fn
add_relreplident_wal_record(char relreplident)
{
	rec: &mut WALRecRelReplident = (WALRecRelReplident *) (&local_wal.buffer[local_wal.buffer_offset]);
	pub static mut IX_OID: Oid = InvalidOid;

	Assert(!is_recovery_process());
	Assert(local_wal.buffer_offset + sizeof(*rec) + XID_RESERVED_LENGTH <= LOCAL_WAL_BUFFER_SIZE);

	rec->recType = WAL_REC_RELREPLIDENT;
	rec->relreplident = relreplident;
	memcpy(rec->relreplident_ix_oid, &ix_oid, sizeof(Oid));

	local_wal.buffer_offset += sizeof(*rec);
}

fn
add_rel_wal_record(ORelOids oids, OIndexType type, uint32 version, uint32 base_version)
{
	pub static mut RUN_XMIN: OXid = std::mem::zeroed();
	pub static mut CSN: CommitSeqNo = std::mem::zeroed();
	pub static mut CID: CommandId = std::mem::zeroed();

	rec: &mut WALRecRelation = (WALRecRelation *) (&local_wal.buffer[local_wal.buffer_offset]);

	Assert(!is_recovery_process());
	Assert(local_wal.buffer_offset + sizeof(*rec) + XID_RESERVED_LENGTH <= LOCAL_WAL_BUFFER_SIZE);

	rec->recType = WAL_REC_RELATION;
	rec->treeType = type;
	memcpy(rec->datoid, &oids.datoid, sizeof(Oid));
	memcpy(rec->reloid, &oids.reloid, sizeof(Oid));
	memcpy(rec->relnode, &oids.relnode, sizeof(Oid));

	// Since ORIOLEDB_WAL_VERSION = 17
	runXmin = pg_atomic_read_u64(&xid_meta->runXmin);
	memcpy(rec->xmin, &runXmin, sizeof(runXmin));

	csn = pg_atomic_read_u64(&TRANSAM_VARIABLES->nextCommitSeqNo);
	memcpy(rec->csn, &csn, sizeof(csn));

	cid = o_get_current_command();
	memcpy(rec->cid, &cid, sizeof(cid));

	memcpy(rec->version, &version, sizeof(version));
	memcpy(rec->baseVersion, &base_version, sizeof(base_version));

	elog(DEBUG4, "[%s] WAL_REC_RELATION ADD oids [ %u %u %u ] type %d xmin/csn/cid " UINT64_FORMAT "/" UINT64_FORMAT "/%u version %u base_version %u", __func__,
		 oids.datoid, oids.reloid, oids.relnode,
		 type, runXmin, csn, cid, version, base_version);

	local_wal.buffer_offset += sizeof(*rec);

	local_wal.ix_type = type;
	local_wal.oids = oids;
}


add_o_tables_meta_lock_wal_record()
{
	pub static mut WAL_REC: *mut rec = std::ptr::null_mut();

	Assert(!is_recovery_process());
	flush_local_wal_if_needed(sizeof(*rec));
	Assert(local_wal.buffer_offset + sizeof(*rec) + XID_RESERVED_LENGTH <= LOCAL_WAL_BUFFER_SIZE);

	add_xid_wal_record_if_needed();

	rec = (WALRec *) (&local_wal.buffer[local_wal.buffer_offset]);

	rec->recType = WAL_REC_O_TABLES_META_LOCK;

	local_wal.buffer_offset += sizeof(*rec);
}


add_o_tables_meta_unlock_wal_record(ORelOids oids, Oid oldRelnode)
{
	pub static mut WAL_REC_O_TABLES_UNLOCK_META: *mut rec = std::ptr::null_mut();

	Assert(!is_recovery_process());
	flush_local_wal_if_needed(sizeof(*rec));
	Assert(local_wal.buffer_offset + sizeof(*rec) + XID_RESERVED_LENGTH <= LOCAL_WAL_BUFFER_SIZE);

	add_xid_wal_record_if_needed();

	rec = (WALRecOTablesUnlockMeta *) (&local_wal.buffer[local_wal.buffer_offset]);

	rec->recType = WAL_REC_O_TABLES_META_UNLOCK;
	memcpy(rec->datoid, &oids.datoid, sizeof(Oid));
	memcpy(rec->reloid, &oids.reloid, sizeof(Oid));
	memcpy(rec->old_relnode, &oldRelnode, sizeof(Oid));
	memcpy(rec->new_relnode, &oids.relnode, sizeof(Oid));

	local_wal.buffer_offset += sizeof(*rec);
}


add_database_copy_wal_record(Oid dboid, Oid src_tblspc, Oid dst_tblspc)
{
	pub static mut WAL_REC_DB_COPY: *mut rec = std::ptr::null_mut();

	Assert(!is_recovery_process());
	flush_local_wal_if_needed(sizeof(*rec));
	Assert(local_wal.buffer_offset + sizeof(*rec) + XID_RESERVED_LENGTH <= LOCAL_WAL_BUFFER_SIZE);

	add_xid_wal_record_if_needed();

	rec = (WALRecDbCopy *) (&local_wal.buffer[local_wal.buffer_offset]);

	rec->recType = WAL_REC_DATABASE_COPY;
	memcpy(rec->datid, &dboid, sizeof(Oid));
	memcpy(rec->src_tblspc, &src_tblspc, sizeof(Oid));
	memcpy(rec->dst_tblspc, &dst_tblspc, sizeof(Oid));

	local_wal.buffer_offset += sizeof(*rec);
}


add_switch_logical_xid_wal_record(TransactionId logicalXid_top, TransactionId logicalXid_sub)
{
	pub static mut WAL_REC_SWITCH_LOGICAL_XID: *mut rec = std::ptr::null_mut();

	if (local_wal.contains_switch_xid)
		return;

	local_wal.contains_switch_xid = true;

	Assert(!is_recovery_process());
	Assert(TransactionIdIsValid(logicalXid_top));
	Assert(TransactionIdIsValid(logicalXid_sub));
	flush_local_wal_if_needed(sizeof(*rec));
	Assert(local_wal.buffer_offset + sizeof(*rec) <= LOCAL_WAL_BUFFER_SIZE);

	rec = (WALRecSwitchLogicalXid *) (&local_wal.buffer[local_wal.buffer_offset]);

	rec->recType = WAL_REC_SWITCH_LOGICAL_XID;
	memcpy(rec->topXid, &logicalXid_top, sizeof(TransactionId));
	memcpy(rec->subXid, &logicalXid_sub, sizeof(TransactionId));

	local_wal.buffer_offset += sizeof(*rec);
}


add_savepoint_wal_record(SubTransactionId parentSubid,
						 TransactionId prentLogicalXid)
{
	pub static mut WAL_REC_SAVEPOINT: *mut rec = std::ptr::null_mut();
	TransactionId logicalXid = get_current_logical_xid();

	Assert(!is_recovery_process());
	flush_local_wal_if_needed(sizeof(*rec));
	Assert(local_wal.buffer_offset + sizeof(*rec) + XID_RESERVED_LENGTH <= LOCAL_WAL_BUFFER_SIZE);

	add_xid_wal_record_if_needed();

	rec = (WALRecSavepoint *) (&local_wal.buffer[local_wal.buffer_offset]);

	rec->recType = WAL_REC_SAVEPOINT;
	memcpy(rec->parentSubid, &parentSubid, sizeof(SubTransactionId));
	memcpy(rec->parentLogicalXid, &prentLogicalXid, sizeof(TransactionId));
	memcpy(rec->logicalXid, &logicalXid, sizeof(TransactionId));

	local_wal.buffer_offset += sizeof(*rec);
}


add_rollback_to_savepoint_wal_record(SubTransactionId parentSubid)
{
	pub static mut WAL_REC_ROLLBACK_TO_SAVEPOINT: *mut rec = std::ptr::null_mut();
	pub static mut RUN_XMIN: OXid = std::mem::zeroed();
	pub static mut CSN: CommitSeqNo = std::mem::zeroed();

	Assert(!is_recovery_process());
	flush_local_wal_if_needed(sizeof(*rec));
	local_wal.contains_xid = false;
	Assert(local_wal.buffer_offset + sizeof(*rec) + XID_RESERVED_LENGTH <= LOCAL_WAL_BUFFER_SIZE);

	add_xid_wal_record_if_needed();

	rec = (WALRecRollbackToSavepoint *) (&local_wal.buffer[local_wal.buffer_offset]);

	rec->recType = WAL_REC_ROLLBACK_TO_SAVEPOINT;
	memcpy(rec->parentSubid, &parentSubid, sizeof(SubTransactionId));

	runXmin = pg_atomic_read_u64(&xid_meta->runXmin);
	memcpy(rec->xmin, &runXmin, sizeof(runXmin));
	csn = pg_atomic_read_u64(&TRANSAM_VARIABLES->nextCommitSeqNo);
	memcpy(rec->csn, &csn, sizeof(csn));

	elog(DEBUG4, "[%s] xmin " UINT64_FORMAT " csn " UINT64_FORMAT,
		 __func__, runXmin, csn);

	local_wal.buffer_offset += sizeof(*rec);

	flush_local_wal(false, false);

	//
// Force adding xid record on future changes going after this rollback to
// sp, this is necessary for correct xids restoring in logical decoder
//
	local_wal.contains_xid = false;
}

bool
local_wal_is_empty()
{
	return (local_wal.buffer_offset == 0);
}

static inline 
reset_local_wal_buffer()
{
	local_wal.buffer_offset = 0;
	local_wal.contains_xid = false;
	local_wal.contains_switch_xid = false;
	local_wal.ix_type = oIndexInvalid;
	ORelOidsSetInvalid(local_wal.oids);
}

//
// Returns end position of a new WAL container.
//
XLogRecPtr
flush_local_wal(bool isCommit, bool withXactTime)
{
	pub static mut LOCATION: XLogRecPtr = std::mem::zeroed();
	pub static mut LENGTH: std::os::raw::c_int = local_wal.buffer_offset;

	Assert(!is_recovery_process());
	Assert(length > 0);

	//
// Put the xlog location of our commit record to the shared memory.  This
// will help concurrent checkpointer to wait till we do
// write_to_xids_queue().
//
	if (isCommit)
		pg_atomic_write_u64(&GET_CUR_PROCDATA()->commitInProgressXlogLocation, OWalTmpCommitPos);

	//
// The buffer already holds a finish record (COMMIT/ROLLBACK/JOINT_COMMIT)
// at this point, it's too late to append another ROLLBACK in case of
// error. Mirror RecordTransactionCommit() and escalate any failure to
// PANIC.
//
	START_CRIT_SECTION();

	location = log_logical_wal_container(local_wal.buffer, length, withXactTime);

	if (isCommit)
		pg_atomic_write_u64(&GET_CUR_PROCDATA()->commitInProgressXlogLocation, location);

	reset_local_wal_buffer();
	local_wal.has_material_changes = true;

	END_CRIT_SECTION();

	pub static mut LOCATION: return = std::mem::zeroed();
}

fn
flush_local_wal_if_needed(int required_length)
{
	Assert(!is_recovery_process());
	if (local_wal.buffer_offset + required_length + XID_RESERVED_LENGTH > LOCAL_WAL_BUFFER_SIZE)
	{
		START_CRIT_SECTION();
		log_logical_wal_container(local_wal.buffer, local_wal.buffer_offset, false);
		reset_local_wal_buffer();
		local_wal.has_material_changes = true;
		END_CRIT_SECTION();
	}
}

static XLogRecPtr
log_logical_wal_container(Pointer ptr, int length, bool withXactTime)
{
	pub static mut WAL_VERSION: uint16 = ORIOLEDB_WAL_VERSION;
	pub static mut FLAGS: uint8 = 0;
	pub static mut REC: WALRecXactInfo = std::mem::zeroed();
	pub static mut ORIGIN: WALRecOriginInfo = std::mem::zeroed();
	pub static mut HAS_ORIGIN: bool = replorigin_session_origin != InvalidRepOriginId;

	Assert(ORIOLEDB_WAL_VERSION >= FIRST_ORIOLEDB_WAL_VERSION);

	XLogBeginInsert();
	XLogRegisterData((char *) (&wal_version), sizeof(wal_version));

	if (withXactTime)
		flags |= WAL_CONTAINER_HAS_XACT_INFO;

	if (hasOrigin)
		flags |= WAL_CONTAINER_HAS_ORIGIN_INFO;

	XLogRegisterData((char *) (&flags), sizeof(flags));

	if (withXactTime)
	{
		TimestampTz xactTime = GetCurrentTransactionStopTimestamp();
		TransactionId xid = GetTopTransactionIdIfAny();

		memcpy(rec.xactTime, &xactTime, sizeof(xactTime));
		memcpy(rec.xid, &xid, sizeof(xid));

		XLogRegisterData((char *) &rec, sizeof(rec));
	}

	if (hasOrigin)
	{
		pub static mut ORIGIN_ID: RepOriginId = replorigin_session_origin;
		pub static mut ORIGIN_LSN: XLogRecPtr = replorigin_session_origin_lsn;

		memcpy(origin.origin_id, &origin_id, sizeof(origin_id));
		memcpy(origin.origin_lsn, &origin_lsn, sizeof(origin_lsn));
		XLogRegisterData((char *) &origin, sizeof(origin));
	}

	XLogRegisterData(ptr, length);
	return XLogInsert(ORIOLEDB_RMGR_ID, ORIOLEDB_XLOG_CONTAINER);
}

//
// Makes WAL insert record.
//

o_wal_insert(desc: &mut BTreeDescr, OTuple tuple, char relreplident, uint32 version)
{
	pub static mut WAL_RECORD: OTuple = std::mem::zeroed();
	pub static mut CALL_PFREE: bool = false;
	pub static mut SIZE: std::os::raw::c_int = 0;

	elog(DEBUG4, "[%s] [ %u %u %u ] version %u", __func__,
		 desc->oids.datoid, desc->oids.reloid, desc->oids.relnode,
		 version);

	Assert(!O_TUPLE_IS_NULL(tuple));
	wal_record = recovery_rec_insert(desc, tuple, &call_pfree, &size);
	Assert(desc->type != oIndexToast);
	add_modify_wal_record(WAL_REC_INSERT, desc, wal_record, size, relreplident,
						  version, O_TABLE_INVALID_VERSION	// Asserted no base
// version for non TOAST );
	if (call_pfree)
		pfree(wal_record.data);
}

//
// Makes WAL update record.
//

o_wal_update(desc: &mut BTreeDescr, OTuple tuple, OTuple oldtuple, char relreplident, uint32 version)
{
	pub static mut WAL_RECORD1: OTuple = std::mem::zeroed();
	pub static mut WAL_RECORD2: OTuple = std::mem::zeroed();
	pub static mut CALL_PFREE1: bool = false;
	pub static mut CALL_PFREE2: bool = false;
	pub static mut SIZE1: std::os::raw::c_int = 0;
	pub static mut SIZE2: std::os::raw::c_int = 0;

	elog(DEBUG4, "[%s] [ %u %u %u ] version %u", __func__,
		 desc->oids.datoid, desc->oids.reloid, desc->oids.relnode,
		 version);

	Assert(!O_TUPLE_IS_NULL(tuple));
	wal_record1 = recovery_rec_update(desc, tuple, &call_pfree1, &size1);
	Assert(desc->type != oIndexToast);

	//
// For REPLICA_IDENTITY_FULL include new and old tuples into
// WAL_REC_UPDATE
//
	if (relreplident != REPLICA_IDENTITY_FULL)
	{
		add_modify_wal_record(WAL_REC_UPDATE, desc, wal_record1, size1, relreplident,
							  version, O_TABLE_INVALID_VERSION	// Asserted no base
// version for non TOAST );
	}
	else
	{
		Assert(!O_TUPLE_IS_NULL(oldtuple));
		wal_record2 = recovery_rec_update(desc, oldtuple, &call_pfree2, &size2);
		add_modify_wal_record_extended(WAL_REC_UPDATE, desc, wal_record1, size1, wal_record2, size2, relreplident,
									   version, O_TABLE_INVALID_VERSION // Asserted no base
// version for non TOAST );
		if (call_pfree2)
			pfree(wal_record2.data);
	}

	if (call_pfree1)
		pfree(wal_record1.data);
}

//
// Makes WAL delete record.
//

o_wal_delete(desc: &mut BTreeDescr, OTuple tuple, char relreplident, uint32 version)
{
	pub static mut WAL_RECORD: OTuple = std::mem::zeroed();
	pub static mut CALL_PFREE: bool = false;
	pub static mut SIZE: std::os::raw::c_int = 0;

	elog(DEBUG4, "[%s] [ %u %u %u ] version %u", __func__,
		 desc->oids.datoid, desc->oids.reloid, desc->oids.relnode,
		 version);

	Assert(!O_TUPLE_IS_NULL(tuple));
	wal_record = recovery_rec_delete(desc, tuple, &call_pfree, &size, relreplident);
	Assert(desc->type != oIndexToast);
	add_modify_wal_record(WAL_REC_DELETE, desc, wal_record, size, relreplident,
						  version, O_TABLE_INVALID_VERSION	// Asserted no base
// version for non TOAST );

	if (call_pfree)
		pfree(wal_record.data);
}

//
// Makes WAL delete+insert record.
//

o_wal_reinsert(desc: &mut BTreeDescr, OTuple oldtuple, OTuple newtuple, char relreplident, uint32 version)
{
	pub static mut OLDRECORD: OTuple = std::mem::zeroed();
	pub static mut NEWRECORD: OTuple = std::mem::zeroed();
	pub static mut NEW_CALL_PFREE: bool = false;
	pub static mut OLD_CALL_PFREE: bool = false;
	pub static mut NEWSIZE: std::os::raw::c_int = 0;
	pub static mut OLDSIZE: std::os::raw::c_int = 0;

	Assert(!O_TUPLE_IS_NULL(newtuple));
	Assert(!O_TUPLE_IS_NULL(oldtuple));

	oldrecord = recovery_rec_delete(desc, oldtuple, &old_call_pfree, &oldsize, relreplident);
	newrecord = recovery_rec_insert(desc, newtuple, &new_call_pfree, &newsize);
	Assert(desc->type != oIndexToast);
	add_modify_wal_record_extended(WAL_REC_REINSERT, desc, newrecord, newsize, oldrecord, oldsize, relreplident,
								   version, O_TABLE_INVALID_VERSION // Asserted no base
// version for non TOAST );
	if (old_call_pfree)
	{
		pfree(oldrecord.data);
	}
	if (new_call_pfree)
	{
		pfree(newrecord.data);
	}
}

// Could be used only for system trees and bridge trees that are not replicated logically

o_wal_delete_key(desc: &mut BTreeDescr, OTuple key, bool is_bridge_index, uint32 version)
{
	pub static mut WAL_RECORD: OTuple = std::mem::zeroed();
	pub static mut CALL_PFREE: bool = false;
	pub static mut SIZE: std::os::raw::c_int = 0;

	Assert(IS_SYS_TREE_OIDS(desc->oids) || is_bridge_index);
	Assert(!O_TUPLE_IS_NULL(key));
	wal_record = recovery_rec_delete_key(desc, key, &call_pfree, &size);
	Assert(desc->type != oIndexToast);
	add_modify_wal_record(WAL_REC_DELETE, desc, wal_record, size, REPLICA_IDENTITY_DEFAULT,
						  version, O_TABLE_INVALID_VERSION	// Asserted no base
// version for non TOAST );

	if (call_pfree)
		pfree(wal_record.data);
}


add_truncate_wal_record(ORelOids oids)
{
	pub static mut WAL_REC_TRUNCATE: *mut rec = std::ptr::null_mut();

	Assert(!is_recovery_process());
	flush_local_wal_if_needed(sizeof(*rec));
	Assert(local_wal.buffer_offset + sizeof(*rec) + XID_RESERVED_LENGTH <= LOCAL_WAL_BUFFER_SIZE);

	add_xid_wal_record_if_needed();

	rec = (WALRecTruncate *) (&local_wal.buffer[local_wal.buffer_offset]);

	rec->recType = WAL_REC_TRUNCATE;
	memcpy(rec->datoid, &oids.datoid, sizeof(Oid));
	memcpy(rec->reloid, &oids.reloid, sizeof(Oid));
	memcpy(rec->relnode, &oids.relnode, sizeof(Oid));

	local_wal.buffer_offset += sizeof(*rec);

	local_wal.ix_type = oIndexInvalid;
	ORelOidsSetInvalid(local_wal.oids);
}

bool
get_local_wal_has_material_changes()
{
	return local_wal.has_material_changes;
}


set_local_wal_has_material_changes(bool value)
{
	local_wal.has_material_changes = value;
}