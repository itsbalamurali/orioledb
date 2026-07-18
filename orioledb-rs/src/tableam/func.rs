use crate::access::genam;
use crate::access::relation;
use crate::access::table;
use crate::access::tupmacs;
use crate::btree::btree;
use crate::btree::check;
use crate::btree::io;
use crate::btree::iterator;
use crate::btree::page_chunks;
use crate::btree::page_contents;
use crate::catalog::indices;
use crate::catalog::o_tables;
use crate::catalog::pg_attribute;
use crate::catalog::pg_type_d;
use crate::commands::defrem;
use crate::funcapi;
use crate::orioledb;
use crate::pgstat;
use crate::tableam::descr;
use crate::tableam::handler;
use crate::tableam::toast;
use crate::tuple::format;
use crate::tuple::toast;
use crate::utils::builtins;
use crate::utils::compress;
use crate::utils::datum;
use crate::utils::fmgroids;
use crate::utils::lsyscache;
use crate::utils::rel;
use pgrx::pg_sys;

// -------------------------------------------------------------------------
//
// func.c
// SQL functions implementation for orioledb module.
//
// Copyright (c) 2021-2026, Oriole DB Inc.
// Copyright (c) 2025-2026, Supabase Inc.
//
// IDENTIFICATION
// contrib/orioledb/src/tableam/func.c
//
// -------------------------------------------------------------------------
//

PG_FUNCTION_INFO_V1(orioledb_tbl_structure);
PG_FUNCTION_INFO_V1(orioledb_idx_structure);
PG_FUNCTION_INFO_V1(orioledb_tbl_bin_structure);
PG_FUNCTION_INFO_V1(orioledb_tbl_check);
PG_FUNCTION_INFO_V1(verify_orioledb);
PG_FUNCTION_INFO_V1(orioledb_compression_max_level);
PG_FUNCTION_INFO_V1(orioledb_tbl_compression_check);
PG_FUNCTION_INFO_V1(orioledb_tbl_indices);
PG_FUNCTION_INFO_V1(orioledb_relation_size);
PG_FUNCTION_INFO_V1(orioledb_tbl_are_indices_equal);
PG_FUNCTION_INFO_V1(orioledb_table_pages);
PG_FUNCTION_INFO_V1(orioledb_tree_stat);

extern  log_btree(desc: &mut BTreeDescr);

// Maximum length for truncated values (when 't' option is used)
#define TRUNCATE_VALUE_LEN 40

fn
o_tuple_print(TupleDesc tupDesc, spec: &mut OTupleFixedFormatSpec,
			  outputFns: &mut FmgrInfo, StringInfo buf, OTuple tup,
			  values: &mut Datum, nulls: &mut bool, bool printVersion,
			  bool truncateValues)
{
	pub static mut ATTI: Form_pg_attribute = std::mem::zeroed();
	int			attnum,
				i;

	appendStringInfo(buf, "(");

	if (printVersion)
		appendStringInfo(buf, "(%u) ", o_tuple_get_version(tup));

	for (i = 0; i < tupDesc->natts; i++)
	{
		if (i > 0)
			appendStringInfo(buf, ", ");
		attnum = i + 1;
		values[i] = o_fastgetattr(tup, attnum, tupDesc, spec, &nulls[i]);
		if (nulls[i])
		{
			appendStringInfo(buf, "null");
		}
		else
		{
			pub static mut CHAR: *mut output = std::ptr::null_mut();

			atti = TupleDescAttr(tupDesc, i);
			if (!atti->attbyval && atti->attlen && !nulls[i])
			{
				Pointer		p = DatumGetPointer(values[i]);

				if (IS_TOAST_POINTER(p))
				{
					appendStringInfo(buf, "TOASTed");
					continue;
				}
			}
			output = OutputFunctionCall(&outputFns[i], values[i]);
			if (truncateValues && strlen(output) > TRUNCATE_VALUE_LEN)
				appendStringInfo(buf, "'%.*s'...(truncated)", TRUNCATE_VALUE_LEN, output);
			else
				appendStringInfo(buf, "'%s'", output);
		}
	}

	appendStringInfo(buf, ")");
}

fn
idx_key_print(desc: &mut BTreeDescr, StringInfo buf, OTuple tup, Pointer arg)
{
	opaque: &mut TuplePrintOpaque = (TuplePrintOpaque *) arg;

	o_tuple_print(opaque->keyDesc, opaque->keySpec, opaque->keyOutputFns, buf,
				  tup, opaque->values, opaque->nulls, false,
				  opaque->truncateValues);
}

fn
idx_tup_print(desc: &mut BTreeDescr, StringInfo buf, OTuple tup, Pointer arg)
{
	opaque: &mut TuplePrintOpaque = (TuplePrintOpaque *) arg;

	o_tuple_print(opaque->desc, opaque->spec, opaque->outputFns, buf,
				  tup, opaque->values, opaque->nulls, opaque->printRowVersion,
				  opaque->truncateValues);
}


init_print_options(printOptions: &mut BTreePrintOptions, optionsArg: &mut VarChar)
{
	pub static mut I: std::os::raw::c_int = 0;
	int			optionsSize = VARSIZE(optionsArg) - VARHDRSZ;
	options: &mut char = (char *) VARDATA(optionsArg);

	// parse options argument and update options
	for (i = 0; i < optionsSize; i++)
	{
		switch (options[i])
		{
			case 'n':
				printOptions->pagePrintType = BTreePrintRelative;
				break;
			case 'C':
				printOptions->csnPrintType = BTreePrintAbsolute;
				break;
			case 'c':
				printOptions->csnPrintType = BTreePrintRelative;
				break;
			case 'b':
				printOptions->backendIdPrintType = BTreePrintAbsolute;
				break;
			case 'U':
				printOptions->undoLogLocationPrintType = BTreePrintAbsolute;
				break;
			case 'u':
				printOptions->undoLogLocationPrintType = BTreePrintRelative;
				break;
			case 'e':
				printOptions->idsPrintType = BTreePrintRelative;
				break;
			case 'i':
				printOptions->changeCountPrintType = BTreePrintAbsolute;
				break;
			case 'K':
				printOptions->checkpointNumPrintType = BTreePrintAbsolute;
				break;
			case 'k':
				printOptions->checkpointNumPrintType = BTreePrintRelative;
				break;
			case 'S':
				printOptions->printStateValue = true;
				break;
			case 'v':
				printOptions->printRowVersion = true;
				break;
			case 'o':
				printOptions->printFileOffset = true;
				break;
			case 'f':
				printOptions->printFormatFlags = true;
				break;
			case 'F':
				printOptions->printFixedFlags = true;
				break;
			case 't':
				printOptions->truncateValues = true;
				break;
			default:
				ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
								(errmsg("invalid option '%c'", options[i]))));
				break;
		}
	}
}

fn
print_unloaded_tree(buf: &mut StringInfoData, td: &mut BTreeDescr, const treeName: &mut char,
					printOptions: &mut BTreePrintOptions)
{
	pub static mut CHAR: *mut prev_chkp_fname = std::ptr::null_mut();
	pub static mut PREV_CHKP_FILE: File = std::mem::zeroed();
	CheckpointFileHeader file_header = {0};
	pub static mut PREV_CHKP_TAG: SeqBufTag = std::mem::zeroed();
	pub static mut EVICTED_TREE_DATA: *mut evicted_data = std::ptr::null_mut();

	memset(&prev_chkp_tag, 0, sizeof(prev_chkp_tag));
	prev_chkp_tag.key.oids = td->oids;
	prev_chkp_tag.key.tablespace = td->tablespace;
	prev_chkp_tag.num = checkpoint_state->lastCheckpointNumber;
	prev_chkp_tag.type = 'm';

	evicted_data = read_evicted_data(td->oids.datoid,
									 td->oids.relnode,
									 false);

	//
// If found in eviction hash then use cached file_header to initialize
// tree
//
	if (evicted_data != NULL)
	{
		file_header = evicted_data->file_header;
		pfree(evicted_data);
	}
	else
	{
		prev_chkp_fname = get_seq_buf_filename(&prev_chkp_tag);
		prev_chkp_file = PathNameOpenFile(prev_chkp_fname, O_RDONLY | PG_BINARY);
		if (prev_chkp_file > 0)
		{
			OFileRead(prev_chkp_file, (Pointer) &file_header, sizeof(file_header), 0, WAIT_EVENT_SLRU_READ);
			FileClose(prev_chkp_file);
		}
		else
		{
			file_header.rootDownlink = InvalidDiskDownlink;
		}
	}

	appendStringInfo(buf, "Index %s: not loaded", treeName);
	if (printOptions->idsPrintType == BTreePrintAbsolute)
	{
		appendStringInfo(buf, ", ");
		appendStringInfo(buf, "datoid = %d, relnode = %d, ",
						 td->oids.datoid, td->oids.relnode);
		if (DiskDownlinkIsValid(file_header.rootDownlink))
			appendStringInfo(buf, "rootOffset = " UINT64_FORMAT ", %u",
							 DOWNLINK_GET_DISK_OFF(file_header.rootDownlink),
							 DOWNLINK_GET_DISK_LEN(file_header.rootDownlink));
		else
			appendStringInfo(buf, "rootOffset is invalid");
	}
	appendStringInfo(buf, "\n");
}

fn
tree_structure(StringInfo buf,
			   id: &mut OIndexDescr,
			   BTreePrintOptions printOptions,
			   int depth)
{
	pub static mut I: std::os::raw::c_int = 0;
	pub static mut OPAQUE: TuplePrintOpaque = std::mem::zeroed();
	SharedRootInfoKey key = {0};
	pub static mut SHARED_ROOT_INFO: *mut sharedRootInfo = std::ptr::null_mut();
	pub static mut B_TREE_DESCR: *mut td = std::ptr::null_mut();
	pub static mut CHAR: *mut const treeName = std::ptr::null_mut();

	opaque.desc = id->leafTupdesc;
	opaque.spec = &id->leafSpec;
	opaque.keyDesc = id->nonLeafTupdesc;
	opaque.keySpec = &id->nonLeafSpec;
	opaque.values = (Datum *) palloc(sizeof(Datum) * opaque.desc->natts);
	opaque.nulls = (bool *) palloc(sizeof(bool) * opaque.desc->natts);
	opaque.outputFns = (FmgrInfo *) palloc(sizeof(FmgrInfo) * opaque.desc->natts);
	opaque.keyOutputFns = (FmgrInfo *) palloc(sizeof(FmgrInfo) * opaque.keyDesc->natts);
	opaque.printRowVersion = printOptions.printRowVersion;
	opaque.truncateValues = printOptions.truncateValues;

	for (i = 0; i < opaque.desc->natts; i++)
	{
		pub static mut OUTPUT: Oid = std::mem::zeroed();
		pub static mut VARLENA: bool = false;

		getTypeOutputInfo(TupleDescAttr(opaque.desc, i)->atttypid,
						  &output, &varlena);
		fmgr_info(output, &opaque.outputFns[i]);
	}

	for (i = 0; i < opaque.keyDesc->natts; i++)
	{
		pub static mut OUTPUT: Oid = std::mem::zeroed();
		pub static mut VARLENA: bool = false;

		getTypeOutputInfo(TupleDescAttr(opaque.keyDesc, i)->atttypid,
						  &output, &varlena);
		fmgr_info(output, &opaque.keyOutputFns[i]);
	}

	td = &id->desc;
	treeName = id->name.data;

	key.datoid = td->oids.datoid;
	key.relnode = td->oids.relnode;
	sharedRootInfo = o_find_shared_root_info(&key);

	if (sharedRootInfo != NULL && !sharedRootInfo->placeholder)
	{
		o_btree_load_shmem(td);

		appendStringInfo(buf, "Index %s contents\n", treeName);
		if (td->type != oIndexToast)
			o_print_btree_pages(td, buf, idx_key_print, idx_tup_print,
								(Pointer) &opaque, &printOptions, depth);
		else
			o_print_btree_pages(td, buf, o_toast_key_print, o_toast_tup_print,
								(Pointer) &opaque, &printOptions, depth);
	}
	else
	{
		print_unloaded_tree(buf, td, treeName, &printOptions);
	}
}

Datum
orioledb_tbl_structure(PG_FUNCTION_ARGS)
{
	Oid			relid = PG_GETARG_OID(0);
	optionsArg: &mut VarChar = (VarChar *) PG_GETARG_VARCHAR_P(1);
	int			depth = PG_GETARG_INT32(2);
	pub static mut O_TABLE_DESCR: *mut descr = std::ptr::null_mut();
	pub static mut REL: Relation = std::mem::zeroed();
	pub static mut TEXT: *mut result = std::ptr::null_mut();
	pub static mut TREEN: std::os::raw::c_int = 0;
	pub static mut BUF: StringInfoData = std::mem::zeroed();
	pub static mut PRINT_OPTIONS: BTreePrintOptions = std::mem::zeroed();

	ASAN_UNPOISON_MEMORY_REGION(&printOptions, sizeof(printOptions));
	MemSet(&printOptions, 0, sizeof(printOptions));

	orioledb_check_shmem();

	rel = relation_open(relid, AccessShareLock);

	if (!rel)
		ereport(ERROR,
				(errcode(ERRCODE_WRONG_OBJECT_TYPE),
				 errmsg("relation oid %u does not exists", relid)));

	descr = relation_get_descr(rel);

	if (!descr)
		ereport(ERROR,
				(errcode(ERRCODE_WRONG_OBJECT_TYPE),
				 errmsg("relation oid %u is not orioledb", relid)));

	initStringInfo(&buf);

	init_print_options(&printOptions, optionsArg);

	// index trees + toast tree
	for (treen = 0; treen < descr->nIndices; treen++)
		tree_structure(&buf, descr->indices[treen], printOptions, depth);
	if (descr->bridge)
		tree_structure(&buf, descr->bridge, printOptions, depth);
	tree_structure(&buf, descr->toast, printOptions, depth);

	result = cstring_to_text(buf.data);
	relation_close(rel, AccessShareLock);

	PG_RETURN_POINTER(result);
}

static inline 
append_bytes(StringInfo str, Page p, offset: &mut OffsetNumber, int len, int level,
			 bool print_bytes)
{
	if (print_bytes)
	{
		pub static mut J: std::os::raw::c_int = 0;

		appendStringInfoSpaces(str, level * 4);
		for (j = 0; j < len; j++)
		{
			appendStringInfo(str, "%4u", (unsigned char) p[*offset + j]);
			if ((j + 1) % 20 == 0)
			{
				appendStringInfo(str, "\n");
				appendStringInfoSpaces(str, level * 4);
			}
		}
		*offset += j;
		appendStringInfo(str, "\n");
	}
	else
	{
		*offset += len;
	}
}

static inline 
append_bits(StringInfo str, Page p, offset: &mut OffsetNumber,
			bit_offset: &mut OffsetNumber, int len, int level, bool print_bytes)
{
	if (print_bytes)
	{
		pub static mut J: std::os::raw::c_int = 0;
		pub static mut BIT: std::os::raw::c_int = 0;
		pub static mut BYTE_START: std::os::raw::c_int = *bit_offset % BITS_PER_BYTE;
		pub static mut BYTE_END: std::os::raw::c_int = 0;

		appendStringInfoSpaces(str, level * 4);
		if (len > BITS_PER_BYTE)
			byte_end = BITS_PER_BYTE;
		else
			byte_end = len;
		for (j = 0; j < len; j++)
		{
			bit = byte_start + (byte_end - (*bit_offset % BITS_PER_BYTE) - 1);
			appendStringInfo(str, "%d", (p[*offset] & (1 << bit)) ? 1 : 0);
			(*bit_offset)++;
			if (*bit_offset % 8 == 0)
			{
				appendStringInfo(str, " ");
				(*offset)++;
				byte_start = 0;
				if (len - j - 1 > BITS_PER_BYTE)
					byte_end = BITS_PER_BYTE;
				else
					byte_end = len - j - 1;
			}
		}
		appendStringInfo(str, "\n");
	}
	else
	{
		*offset +=
			(*bit_offset + len) / BITS_PER_BYTE - *bit_offset / BITS_PER_BYTE;
		*bit_offset += len;
	}
}

#define APPEND_FIELD(name, type)                                              \
	do                                                                        \
	{                                                                         \
		appendStringInfoSpaces(outbuf, level * 4);                            \
		appendStringInfo(outbuf, name ": " #type "(%zu)\n", sizeof(type));    \
		level++;                                                              \
		append_bytes(outbuf, p, &offset, sizeof(type), level, print_bytes);   \
		level--;                                                              \
	} while (0)

#define APPEND_BIT_FIELD(name, size)                                          \
	do                                                                        \
	{                                                                         \
		appendStringInfoSpaces(outbuf, level * 4);                            \
		appendStringInfo(outbuf, name ": %d bit\n", size);                    \
		level++;                                                              \
		append_bits(outbuf, p, &offset, &bit_offset, size, level,             \
					print_bytes);                                             \
		level--;                                                              \
	} while (0)

//
// Print contents of give B-tree page.  If non-leaf page is given, recursively
// print childredn.
//
fn
print_page_bin_structure(desc: &mut BTreeDescr, OInMemoryBlkno blkno,
						 NLRPageNumber: &mut int, Pointer printArg,
						 bool print_bytes, int depthLeft, StringInfo outbuf)
{
	Page		p = O_GET_IN_MEMORY_PAGE(blkno);
	header: &mut BTreePageHeader = (BTreePageHeader *) p;
	pub static mut LOC: BTreePageItemLocator = std::mem::zeroed();
	OffsetNumber i,
				j,
				k;
	OffsetNumber offset,
				bit_offset;
	pub static mut LEVEL: std::os::raw::c_int = 0;

	appendStringInfo(outbuf, "Page %u: ", *NLRPageNumber);
	offset = 0;
	appendStringInfo(outbuf, "\n");
	appendStringInfo(outbuf, "BTreePageHeader(%zu)\n",
					 sizeof(BTreePageHeader));
	level++;					// BTreePageHeader BEGIN

	appendStringInfoSpaces(outbuf, level * 4);
	appendStringInfo(outbuf, "o_header: OrioleDBPageHeader(%zu)\n",
					 sizeof(OrioleDBPageHeader));
	level++;					// o_header BEGIN

	APPEND_FIELD("state", pg_atomic_uint64);
	APPEND_FIELD("usageCount", pg_atomic_uint32);
	APPEND_FIELD("checkpointNum", uint32);

	level--;					// o_header END

	APPEND_FIELD("undoLocation", UndoLocation);
	APPEND_FIELD("csn", CommitSeqNo);
	APPEND_FIELD("rightLink", uint64);

	bit_offset = 0;
	APPEND_BIT_FIELD("flags", 6);
	APPEND_BIT_FIELD("field1", 11);
	APPEND_BIT_FIELD("field2", 15);
	Assert(bit_offset == sizeof(uint32) * BITS_PER_BYTE);

	APPEND_FIELD("maxKeyLen", LocationIndex);
	APPEND_FIELD("prevInsertOffset", OffsetNumber);
	APPEND_FIELD("chunksCount", OffsetNumber);
	APPEND_FIELD("itemsCount", OffsetNumber);
	APPEND_FIELD("hikeysEnd", OffsetNumber);
	APPEND_FIELD("dataSize", LocationIndex);
	appendStringInfoSpaces(outbuf, level * 4);
	appendStringInfo(outbuf, "dataSize VALUE: %u\n", header->dataSize);

	appendStringInfoSpaces(outbuf, level * 4);
	appendStringInfo(outbuf, "chunkDesc: ARRAY:\n");
	level++;					// chunkDesc BEGIN

	for (i = 0; i < header->chunksCount; i++)
	{
		appendStringInfoSpaces(outbuf, level * 4);
		appendStringInfo(outbuf, "[%d]: BTreePageChunkDesc(%lu)\n", i,
						 sizeof(BTreePageChunkDesc));
		level++;				// chunkDesc[i] BEGIN

		bit_offset = 0;
		APPEND_BIT_FIELD("shortLocation", 12);
		APPEND_BIT_FIELD("offset", 10);
		APPEND_BIT_FIELD("hikeyShortLocation", 7);
		APPEND_BIT_FIELD("chunkKeysFixed", 1);
		APPEND_BIT_FIELD("hikeyFlags", 2);
		Assert(bit_offset == sizeof(uint32) * BITS_PER_BYTE);
		level--;				// chunkDesc[i] END
	}
	level--;					// chunkDesc END

	appendStringInfoSpaces(outbuf, level * 4);
	appendStringInfo(outbuf, "BTreePageHeader REST (%lu)\n",
					 MAXALIGN(offset) - offset);
	append_bytes(outbuf, p, &offset, MAXALIGN(offset) - offset, level,
				 print_bytes);

	Assert(MAXALIGN(offsetof(BTreePageHeader, chunkDesc) +
					header->chunksCount * sizeof(BTreePageChunkDesc)) == offset);

	level--;					// BTreePageHeader END

	appendStringInfo(outbuf, "HIKEY DATA (%u) \n", header->hikeysEnd - offset);
	append_bytes(outbuf, p, &offset, header->hikeysEnd - offset,
				 level, print_bytes);

	appendStringInfo(outbuf, "HIKEY AREA REST (%u)\n",
					 SHORT_GET_LOCATION(header->chunkDesc[0].shortLocation) - offset);
	append_bytes(outbuf, p, &offset,
				 SHORT_GET_LOCATION(header->chunkDesc[0].shortLocation) - offset,
				 level, print_bytes);

	appendStringInfo(outbuf, "CHUNKS: ARRAY:\n");
	level++;					// CHUNKS BEGIN
	for (i = 0; i < header->chunksCount; i++)
	{
		pub static mut CHUNK_ITEMS_COUNT: OffsetNumber = std::mem::zeroed();
		pub static mut CHUNK_SIZE: LocationIndex = std::mem::zeroed();
		pub static mut B_TREE_PAGE_CHUNK: *mut chunk = std::ptr::null_mut();
		pub static mut ALIGN: std::os::raw::c_int = 0;

		if (i + 1 < header->chunksCount)
		{
			chunkItemsCount =
				header->chunkDesc[i + 1].offset - header->chunkDesc[i].offset;
			chunkSize =
				SHORT_GET_LOCATION(header->chunkDesc[i + 1].shortLocation) -
				SHORT_GET_LOCATION(header->chunkDesc[i].shortLocation);
		}
		else
		{
			chunkItemsCount = header->itemsCount - header->chunkDesc[i].offset;
			chunkSize = header->dataSize -
				SHORT_GET_LOCATION(header->chunkDesc[i].shortLocation);
		}
		chunk = (BTreePageChunk *) &p[offset];

		appendStringInfoSpaces(outbuf, level * 4);
		appendStringInfo(outbuf, "[%d]: %d\n", i, chunkSize);
		level++;				// CHUNKS[i] BEGIN

		appendStringInfoSpaces(outbuf, level * 4);
		appendStringInfo(outbuf, "ITEMS: ARRAY:\n");
		level++;				// ITEMS BEGIN
		for (j = 0; j < chunkItemsCount; j++)
		{
			appendStringInfoSpaces(outbuf, level * 4);
			appendStringInfo(outbuf, "[%d]: LocationIndex(%zu)\n", j,
							 sizeof(LocationIndex));
			level++;			// ITEMS[j] BEGIN
			append_bytes(outbuf, p, &offset, sizeof(LocationIndex), level,
						 print_bytes);
			level--;			// ITEMS[j] END
		}
		level--;				// ITEMS END

		align = MAXALIGN(sizeof(LocationIndex) * chunkItemsCount) -
			sizeof(LocationIndex) * chunkItemsCount;
		if (align > 0)
		{
			appendStringInfoSpaces(outbuf, level * 4);
			appendStringInfo(outbuf, "ITEMS ARRAY ALIGN (%d)\n", align);
			append_bytes(outbuf, p, &offset, align, level, print_bytes);
		}

		appendStringInfoSpaces(outbuf, level * 4);
		appendStringInfo(outbuf, "ITEM DATA: ARRAY\n");
		level++;				// ITEM DATA BEGIN

		for (j = 0; j < chunkItemsCount; j++)
		{
			if (O_PAGE_IS(p, LEAF))
			{
				pub static mut TUP: OTuple = std::mem::zeroed();
				pub static mut READER: OTupleReaderState = std::mem::zeroed();
				opaque: &mut TuplePrintOpaque = (TuplePrintOpaque *) printArg;
				pub static mut TUPDESC: TupleDesc = opaque->desc;
				pub static mut LEN: LocationIndex = std::mem::zeroed();

				if (j + 1 < chunkItemsCount)
				{
					len = ITEM_GET_OFFSET(chunk->items[j + 1]) -
						ITEM_GET_OFFSET(chunk->items[j]);
				}
				else
				{
					len = chunkSize - ITEM_GET_OFFSET(chunk->items[j]);
				}

				appendStringInfoSpaces(outbuf, level * 4);
				appendStringInfo(outbuf, "[%d]: %d\n", j, len);
				level++;		// ITEM DATA[j] BEGIN

				appendStringInfoSpaces(outbuf, level * 4);
				appendStringInfo(outbuf, "BTreeLeafTuphdr(%zu)\n",
								 sizeof(BTreeLeafTuphdr));
				level++;		// Leaf tuple header BEGIN

				bit_offset = 0;
				APPEND_BIT_FIELD("xactInfo", 61);
				APPEND_BIT_FIELD("deleted", 2);
				APPEND_BIT_FIELD("chainHasLocks", 1);
				Assert(bit_offset == sizeof(OTupleXactInfo) * BITS_PER_BYTE);

				bit_offset = 0;
				APPEND_BIT_FIELD("undoLocation", 62);
				APPEND_BIT_FIELD("formatFlags", 2);
				Assert(bit_offset == sizeof(UndoLocation) * BITS_PER_BYTE);

				level--;		// Leaf tuple header END
				len -= sizeof(BTreeLeafTuphdr);

				tup.data = &p[offset];
				tup.formatFlags = ITEM_GET_FLAGS(chunk->items[j]);
				o_tuple_init_reader(&reader, tup, tupdesc, opaque->spec);

				if (!(tup.formatFlags & O_TUPLE_FLAGS_FIXED_FORMAT))
				{
					appendStringInfoSpaces(outbuf, level * 4);
					appendStringInfo(outbuf, "OTupleHeader(%zu)\n",
									 sizeof(OTupleHeader));
					level++;	// Tuple header BEGIN

					bit_offset = 0;
					APPEND_BIT_FIELD("hasnulls", 1);
					APPEND_BIT_FIELD("len", 15);
					Assert(bit_offset == sizeof(uint16) * BITS_PER_BYTE);

					APPEND_FIELD("natts", uint16);
					APPEND_FIELD("version", uint32);

					level--;	// Tuple header END
					len -= sizeof(OTupleHeader);

					if (reader.hasnulls)
					{
						uint32		bitmapSize = MAXALIGN(BITMAPLEN(reader.natts));

						bit_offset = 0;
						APPEND_BIT_FIELD("Null bitmap", bitmapSize * BITS_PER_BYTE);
						len -= bitmapSize;
					}
				}

				Assert(&p[offset] == reader.tp);

				{
					pub static mut OFF: uint32 = std::mem::zeroed();
					pub static mut NEXT_OFF: uint32 = 0;

					appendStringInfoSpaces(outbuf, level * 4);
					appendStringInfo(outbuf, "Tuple data: %d\n", len);
					level++;	// Tuple data BEGIN
					for (k = 0; k < opaque->desc->natts; k++)
					{
						atti: &mut OTupleAttrFull = OTupleDescAttrSlow(opaque->desc, k);

						if (reader.hasnulls && att_isnull(k, reader.bp))
						{
							reader.slow = true;
							reader.attnum++;
							continue;
						}

						off = o_tuple_next_field_offset(&reader,
														OTupleDescAttrFast(opaque->desc,
																		   k));

						if (next_off < off)
						{
							align = off - next_off;
							appendStringInfoSpaces(outbuf, level * 4);
							appendStringInfo(outbuf, "ATTR ALIGN: %u - %u\n",
											 off, next_off);
							append_bytes(outbuf, p, &offset, align, level,
										 print_bytes);
						}

						next_off = reader.off;

						appendStringInfoSpaces(outbuf, level * 4);
						appendStringInfo(outbuf, "%s: %u - %u: %c\n",
										 atti->attname.data, next_off, off,
										 atti->attalign);
						if (atti->attlen == -1)
						{
							level++;	// LONG DATA BEGIN
							appendStringInfoSpaces(outbuf, level * 4);
							appendStringInfo(outbuf, "LONG DATA\n");
							offset += next_off - off;
							level--;	// LONG DATA END
						}
						else
						{
							append_bytes(outbuf, p, &offset, next_off - off,
										 level, print_bytes);
						}
					}
					align = MAXALIGN(offset) - offset;
					if (align > 0)
					{
						appendStringInfoSpaces(outbuf, level * 4);
						appendStringInfo(outbuf, "TUPLE DATA ALIGN: %u - %u\n",
										 next_off + align, next_off);
						append_bytes(outbuf, p, &offset, align, level,
									 print_bytes);
					}
					level--;	// Tuple data END
				}

				level--;		// ITEM DATA[j] END
			}
			else
			{
			}
		}
		level--;				// ITEM DATA END
		level--;				// CHUNKS[j] END
	}
	level--;					// CHUNKS END

	if (!O_PAGE_IS(p, LEAF))
	{
		BTREE_PAGE_FOREACH_ITEMS(p, &loc)
		{
			Pointer		ptr = BTREE_PAGE_LOCATOR_GET_ITEM(p, &loc);
			tuphdr: &mut BTreeNonLeafTuphdr = (BTreeNonLeafTuphdr *) ptr;

			if (DOWNLINK_IS_IN_MEMORY(tuphdr->downlink))
			{
				pub static mut DOWNLINK: OInMemoryBlkno = std::mem::zeroed();

				downlink = DOWNLINK_GET_IN_MEMORY_BLKNO(tuphdr->downlink);
				(*NLRPageNumber)++;
				print_page_bin_structure(desc, downlink, NLRPageNumber,
										 printArg, print_bytes, depthLeft - 1,
										 outbuf);
			}
		}
	}

	blkno = RIGHTLINK_GET_BLKNO(BTREE_PAGE_GET_RIGHTLINK(p));
	if (OInMemoryBlknoIsValid(blkno))
	{
		(*NLRPageNumber)++;
		print_page_bin_structure(desc, blkno, NLRPageNumber, printArg,
								 print_bytes, depthLeft, outbuf);
	}
}

fn
tree_bin_structure(StringInfo buf, id: &mut OIndexDescr, bool print_bytes,
				   int depth)
{
	pub static mut OPAQUE: TuplePrintOpaque = std::mem::zeroed();
	SharedRootInfoKey key = {0};
	pub static mut SHARED_ROOT_INFO: *mut sharedRootInfo = std::ptr::null_mut();
	pub static mut B_TREE_DESCR: *mut td = std::ptr::null_mut();
	pub static mut CHAR: *mut const treeName = std::ptr::null_mut();

	opaque.desc = id->leafTupdesc;
	opaque.spec = &id->leafSpec;
	opaque.keyDesc = id->nonLeafTupdesc;
	opaque.keySpec = &id->nonLeafSpec;
	opaque.values = (Datum *) palloc(sizeof(Datum) * opaque.desc->natts);
	opaque.nulls = (bool *) palloc(sizeof(bool) * opaque.desc->natts);

	td = &id->desc;
	treeName = id->name.data;

	key.datoid = td->oids.datoid;
	key.relnode = td->oids.relnode;
	sharedRootInfo = o_find_shared_root_info(&key);

	if (sharedRootInfo != NULL && !sharedRootInfo->placeholder)
	{
		o_btree_load_shmem(td);

		appendStringInfo(buf, "Index %s contents\n", treeName);
		if (td->type != oIndexToast)
		{
			pub static mut NLR_PAGE_NUMBER: std::os::raw::c_int = 0;

			print_page_bin_structure(td, td->rootInfo.rootPageBlkno,
									 &NLRPageNumber, (Pointer) &opaque,
									 print_bytes, depth, buf);
		}
	}
}

// Only supports leaf pages of simple indices for now
Datum
orioledb_tbl_bin_structure(PG_FUNCTION_ARGS)
{
	Oid			relid = PG_GETARG_OID(0);
	bool		print_bytes = PG_GETARG_BOOL(1);
	int			depth = PG_GETARG_INT32(2);
	pub static mut O_TABLE_DESCR: *mut descr = std::ptr::null_mut();
	pub static mut REL: Relation = std::mem::zeroed();
	pub static mut TEXT: *mut result = std::ptr::null_mut();
	pub static mut TREEN: std::os::raw::c_int = 0;
	pub static mut BUF: StringInfoData = std::mem::zeroed();

	orioledb_check_shmem();

	rel = relation_open(relid, AccessShareLock);

	if (!rel)
		ereport(ERROR,
				(errcode(ERRCODE_WRONG_OBJECT_TYPE),
				 errmsg("relation oid %u does not exists", relid)));

	descr = relation_get_descr(rel);

	if (!descr)
		ereport(ERROR,
				(errcode(ERRCODE_WRONG_OBJECT_TYPE),
				 errmsg("relation oid %u is not orioledb", relid)));

	initStringInfo(&buf);

	// index trees + toast tree
	for (treen = 0; treen < descr->nIndices; treen++)
		tree_bin_structure(&buf, descr->indices[treen], print_bytes, depth);
	tree_bin_structure(&buf, descr->toast, print_bytes, depth);

	result = cstring_to_text(buf.data);
	relation_close(rel, AccessShareLock);

	PG_RETURN_POINTER(result);
}

Datum
orioledb_idx_structure(PG_FUNCTION_ARGS)
{
	Oid			relid = PG_GETARG_OID(0);
	const treeName: &mut char = text_to_cstring(PG_GETARG_TEXT_PP(1));
	optionsArg: &mut VarChar = (VarChar *) PG_GETARG_VARCHAR_P(2);
	int			depth = PG_GETARG_INT32(3);
	pub static mut O_TABLE_DESCR: *mut descr = std::ptr::null_mut();
	pub static mut REL: Relation = std::mem::zeroed();
	pub static mut TEXT: *mut result = std::ptr::null_mut();
	pub static mut TREEN: std::os::raw::c_int = 0;
	pub static mut BUF: StringInfoData = std::mem::zeroed();
	BTreePrintOptions printOptions = {0};

	orioledb_check_shmem();

	rel = relation_open(relid, AccessShareLock);

	descr = relation_get_descr(rel);
	if (!descr)
		ereport(ERROR,
				(errcode(ERRCODE_WRONG_OBJECT_TYPE),
				 errmsg("relation oid %u is not orioledb", relid)));

	initStringInfo(&buf);

	init_print_options(&printOptions, optionsArg);

	// index trees + toast tree
	for (treen = 0; treen < descr->nIndices; treen++)
	{
		if (!strcmp(treeName, NameStr(descr->indices[treen]->name)))
			tree_structure(&buf, descr->indices[treen], printOptions, depth);
	}
	if (!strcmp(treeName, NameStr(descr->toast->name)))
		tree_structure(&buf, descr->toast, printOptions, depth);
	if (descr->bridge && !strcmp(treeName, NameStr(descr->bridge->name)))
		tree_structure(&buf, descr->bridge, printOptions, depth);

	result = cstring_to_text(buf.data);
	relation_close(rel, AccessShareLock);

	PG_RETURN_POINTER(result);
}

// No existing callers

log_btree(desc: &mut BTreeDescr)
{
	BTreePrintOptions printOptions = {
		.pagePrintType = BTreePrintAbsolute,
		.csnPrintType = BTreePrintAbsolute,
		.undoLogLocationPrintType = BTreePrintAbsolute,
		.idsPrintType = BTreePrintAbsolute,
		.changeCountPrintType = BTreePrintAbsolute,
		.checkpointNumPrintType = BTreePrintAbsolute,
		.printRowVersion = true,
		.printStateValue = true,
		.printFileOffset = true,
		.printFormatFlags = true
	};
	static Oid	typeoids[] = {TIDOID, TEXTOID, INT4OID, INT2OID, BYTEAOID};
	static Oid	outoids[] = {F_TIDOUT, F_TEXTOUT, F_INT4OUT, F_INT2OUT, F_BYTEAOUT};
	pub static mut BUF: StringInfoData = std::mem::zeroed();

	initStringInfo(&buf);
	if (!IS_SYS_TREE_OIDS(desc->oids))
	{
		id: &mut OIndexDescr = (OIndexDescr *) desc->arg;
		pub static mut OPAQUE: TuplePrintOpaque = std::mem::zeroed();
		int			i,
					j;

		opaque.desc = id->leafTupdesc;
		opaque.spec = &id->leafSpec;
		opaque.keyDesc = id->nonLeafTupdesc;
		opaque.keySpec = &id->nonLeafSpec;
		opaque.values = (Datum *) palloc(sizeof(Datum) * opaque.desc->natts);
		opaque.nulls = (bool *) palloc(sizeof(bool) * opaque.desc->natts);
		opaque.outputFns = (FmgrInfo *) palloc(sizeof(FmgrInfo) * opaque.desc->natts);
		opaque.keyOutputFns = (FmgrInfo *) palloc(sizeof(FmgrInfo) * opaque.keyDesc->natts);
		opaque.printRowVersion = printOptions.printRowVersion;
		opaque.truncateValues = printOptions.truncateValues;
		for (i = 0; i < opaque.desc->natts; i++)
		{
			pub static mut OUTPUT: Oid = InvalidOid;
			pub static mut VARLENA: bool = false;
			Form_pg_attribute attr = TupleDescAttr(opaque.desc, i);

			for (j = 0; j < sizeof(typeoids) / sizeof(typeoids[0]); j++)
				if (typeoids[j] == attr->atttypid)
					output = outoids[j];

			if (output == InvalidOid)
				getTypeOutputInfo(attr->atttypid, &output, &varlena);

			fmgr_info(output, &opaque.outputFns[i]);
		}

		for (i = 0; i < opaque.keyDesc->natts; i++)
		{
			pub static mut OUTPUT: Oid = InvalidOid;
			pub static mut VARLENA: bool = false;
			Form_pg_attribute attr = TupleDescAttr(opaque.keyDesc, i);

			for (j = 0; j < sizeof(typeoids) / sizeof(typeoids[0]); j++)
				if (typeoids[j] == attr->atttypid)
					output = outoids[j];

			if (output == InvalidOid)
				getTypeOutputInfo(attr->atttypid, &output, &varlena);

			fmgr_info(output, &opaque.keyOutputFns[i]);
		}
		o_print_btree_pages(desc, &buf, idx_key_print, idx_tup_print,
							(Pointer) &opaque, &printOptions, ORIOLEDB_MAX_DEPTH);
	}
	else
	{
		pub static mut NUM: std::os::raw::c_int = desc->oids.relnode;

		o_print_btree_pages(get_sys_tree(num), &buf,
							sys_tree_key_print(get_sys_tree(num)),
							sys_tree_tup_print(get_sys_tree(num)),
							NULL, &printOptions,
							ORIOLEDB_MAX_DEPTH);
	}

	elog(LOG, "%s", buf.data);
}

fn
table_pages_walk_page(desc: &mut BTreeDescr, BlockNumber blkno,
					  TupleDesc tupdesc, tupstore: &mut Tuplestorestate)
{
	Datum		values[4];
	bool		nulls[4];
	Page		p = O_GET_IN_MEMORY_PAGE(blkno);
	pageHdr: &mut BTreePageHeader = (BTreePageHeader *) p;
	pub static mut J: std::os::raw::c_int = 0;
	pub static mut LOC: BTreePageItemLocator = std::mem::zeroed();

	values[j] = Int64GetDatum(blkno);
	nulls[j] = false;
	j++;
	values[j] = Int32GetDatum(PAGE_GET_LEVEL(p));
	nulls[j] = false;
	j++;
	if (OInMemoryBlknoIsValid(pageHdr->rightLink))
	{
		values[j] = Int64GetDatum(pageHdr->rightLink);
		nulls[j] = false;
	}
	else
	{
		nulls[j] = true;
	}
	j++;
	if (!O_PAGE_IS(p, RIGHTMOST))
	{
		pub static mut JSONB_PARSE_STATE: *mut state = std::ptr::null_mut();
		pub static mut JSONB_VALUE: *mut jsval = std::ptr::null_mut();
		pub static mut HIKEY: OTuple = std::mem::zeroed();

		BTREE_PAGE_GET_HIKEY(hikey, p);
		jsval = o_btree_key_to_jsonb(desc, hikey, &state);
		values[j] = PointerGetDatum(JsonbValueToJsonb(jsval));
		nulls[j] = false;
	}
	else
	{
		nulls[j] = true;
	}
	j++;
	tuplestore_putvalues(tupstore, tupdesc, values, nulls);

	if (O_PAGE_IS(p, LEAF))
		return;

	BTREE_PAGE_FOREACH_ITEMS(p, &loc)
	{
		pub static mut B_TREE_NON_LEAF_TUPHDR: *mut hdr = std::ptr::null_mut();

		hdr = (BTreeNonLeafTuphdr *) BTREE_PAGE_LOCATOR_GET_ITEM(p, &loc);
		if (DOWNLINK_IS_IN_MEMORY(hdr->downlink))
			table_pages_walk_page(desc, DOWNLINK_GET_IN_MEMORY_BLKNO(hdr->downlink),
								  tupdesc, tupstore);
	}

}

Datum
orioledb_table_pages(PG_FUNCTION_ARGS)
{
	rsinfo: &mut ReturnSetInfo = (ReturnSetInfo *) fcinfo->resultinfo;
	Oid			relid = PG_GETARG_OID(0);
	pub static mut RANDOM_ACCESS: bool = false;
	pub static mut TUPDESC: TupleDesc = std::mem::zeroed();
	pub static mut TUPLESTORESTATE: *mut tupstore = std::ptr::null_mut();
	pub static mut OLDCONTEXT: MemoryContext = std::mem::zeroed();
	pub static mut REL: Relation = std::mem::zeroed();
	pub static mut O_TABLE_DESCR: *mut descr = std::ptr::null_mut();
	pub static mut TREEN: std::os::raw::c_int = 0;
	pub static mut ATTNUM: AttrNumber = std::mem::zeroed();

	// check to see if caller supports us returning a tuplestore
	if (rsinfo == NULL || !IsA(rsinfo, ReturnSetInfo))
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("set-valued function called in context that cannot accept a set")));
	if (!(rsinfo->allowedModes & SFRM_Materialize))
		ereport(ERROR,
				(errcode(ERRCODE_SYNTAX_ERROR),
				 errmsg("materialize mode required, but it is not allowed in this context")));

	// The tupdesc and tuplestore must be created in ecxt_per_query_memory
	oldcontext = MemoryContextSwitchTo(rsinfo->econtext->ecxt_per_query_memory);

	tupdesc = CreateTemplateTupleDesc(4);
	attnum = (AttrNumber) 1;
	TupleDescInitEntry(tupdesc, attnum, "blkno", INT8OID, -1, 0);
	attnum++;
	TupleDescInitEntry(tupdesc, attnum, "level", INT4OID, -1, 0);
	attnum++;
	TupleDescInitEntry(tupdesc, attnum, "rightlink", INT8OID, -1, 0);
	attnum++;
	TupleDescInitEntry(tupdesc, attnum, "hikey", JSONBOID, -1, 0);
	attnum++;

	randomAccess = (rsinfo->allowedModes & SFRM_Materialize_Random) != 0;
	tupstore = tuplestore_begin_heap(randomAccess, false, work_mem);
	rsinfo->returnMode = SFRM_Materialize;
	rsinfo->setResult = tupstore;
	rsinfo->setDesc = tupdesc;

	MemoryContextSwitchTo(oldcontext);

	rel = relation_open(relid, AccessShareLock);
	descr = relation_get_descr(rel);;

	if (!descr)
		ereport(ERROR,
				(errcode(ERRCODE_WRONG_OBJECT_TYPE),
				 errmsg("relation oid %u is not orioledb", relid)));

	for (treen = 0; treen < descr->nIndices + 1; treen++)
	{
		pub static mut B_TREE_DESCR: *mut td = std::ptr::null_mut();
		SharedRootInfoKey key = {0};
		pub static mut SHARED_ROOT_INFO: *mut sharedRootInfo = std::ptr::null_mut();

		if (treen < descr->nIndices)
			td = &descr->indices[treen]->desc;
		else
			td = &descr->toast->desc;

		key.datoid = td->oids.datoid;
		key.relnode = td->oids.relnode;
		sharedRootInfo = o_find_shared_root_info(&key);

		if (sharedRootInfo == NULL || sharedRootInfo->placeholder)
			continue;
		o_btree_load_shmem(td);

		table_pages_walk_page(td, td->rootInfo.rootPageBlkno, tupdesc, tupstore);
	}

	relation_close(rel, AccessShareLock);
	return (Datum) 0;
}

Datum
orioledb_tbl_are_indices_equal(PG_FUNCTION_ARGS)
{
	Oid			idx_oid1 = PG_GETARG_OID(0),
				idx_oid2 = PG_GETARG_OID(1);
	descr1: &mut OTableDescr,
			   *descr2;
	Relation	idx1,
				idx2,
				tbl1,
				tbl2;
	pub static mut ARE_EQUAL: bool = true;
	pub static mut I: std::os::raw::c_int = 0;
	td1: &mut OIndexDescr,
			   *td2;
	iter1: &mut BTreeIterator,
			   *iter2;
	OIndexNumber ix_num1,
				ix_num2;

	orioledb_check_shmem();

	idx1 = index_open(idx_oid1, AccessShareLock);
	idx2 = index_open(idx_oid2, AccessShareLock);
	tbl1 = table_open(idx1->rd_index->indrelid, AccessShareLock);
	tbl2 = table_open(idx2->rd_index->indrelid, AccessShareLock);
	descr1 = relation_get_descr(tbl1);
	descr2 = relation_get_descr(tbl2);;

	ix_num1 = o_find_ix_num_by_name(descr1, idx1->rd_rel->relname.data);
	ix_num2 = o_find_ix_num_by_name(descr2, idx2->rd_rel->relname.data);

	relation_close(tbl1, AccessShareLock);
	relation_close(tbl2, AccessShareLock);
	relation_close(idx1, AccessShareLock);
	relation_close(idx2, AccessShareLock);

	if (ix_num1 == InvalidIndexNumber || ix_num2 == InvalidIndexNumber)
		elog(ERROR, "Invalid indexes");

	td1 = descr1->indices[ix_num1];
	td2 = descr2->indices[ix_num2];

	are_equal = td1->leafTupdesc->natts == td2->leafTupdesc->natts;

	for (i = 0; are_equal && i < td1->leafTupdesc->natts; i++)
	{
		are_equal = TupleDescAttr(td1->leafTupdesc, i)->atttypid ==
			TupleDescAttr(td2->leafTupdesc, i)->atttypid;
	}

	if (are_equal)
	{
		iter1 = o_btree_iterator_create(&td1->desc, NULL, BTreeKeyNone,
										&o_in_progress_snapshot,
										ForwardScanDirection);
		iter2 = o_btree_iterator_create(&td2->desc, NULL, BTreeKeyNone,
										&o_in_progress_snapshot,
										ForwardScanDirection);
		while (are_equal)
		{
			OTuple		tuple1,
						tuple2;

			tuple1 = o_btree_iterator_fetch(iter1, NULL, NULL, BTreeKeyNone, true, NULL);
			tuple2 = o_btree_iterator_fetch(iter2, NULL, NULL, BTreeKeyNone, true, NULL);

			if (O_TUPLE_IS_NULL(tuple1) && O_TUPLE_IS_NULL(tuple2))
				break;

			are_equal = !O_TUPLE_IS_NULL(tuple1) && !O_TUPLE_IS_NULL(tuple2) &&
				o_btree_cmp(&td1->desc,
							&tuple1, BTreeKeyLeafTuple,
							&tuple2, BTreeKeyLeafTuple) == 0;

			if (!O_TUPLE_IS_NULL(tuple1))
				pfree(tuple1.data);

			if (!O_TUPLE_IS_NULL(tuple2))
				pfree(tuple2.data);
		}

		btree_iterator_free(iter1);
		btree_iterator_free(iter2);
	}

	PG_RETURN_BOOL(are_equal);
}

Datum
orioledb_tbl_check(PG_FUNCTION_ARGS)
{
	Oid			relid = PG_GETARG_OID(0);
	bool		force_map_check = PG_GETARG_OID(1);
	pub static mut REL: Relation = std::mem::zeroed();
	pub static mut O_TABLE_DESCR: *mut descr = std::ptr::null_mut();
	pub static mut RESULT: bool = true;
	pub static mut I: std::os::raw::c_int = 0;

	orioledb_check_shmem();

	//
// ExclusiveLock helps to avoid changes in map/tmp files and concurrent
// eviction by bgwriter
//
	rel = relation_open(relid, AccessExclusiveLock);
	descr = relation_get_descr(rel);

	if (!descr)
		ereport(ERROR,
				(errcode(ERRCODE_WRONG_OBJECT_TYPE),
				 errmsg("relation oid %u is not orioledb", relid)));

	for (i = 0; i < descr->nIndices; i++)
	{
		pub static mut O_INDEX_DESCR: *mut idx = descr->indices[i];

		o_tables_rel_lock_extended(&idx->oids, AccessExclusiveLock, true);
		o_btree_load_shmem(&idx->desc);
		result = check_btree(&idx->desc, force_map_check, false);
		o_tables_rel_unlock_extended(&idx->oids, AccessExclusiveLock, true);

		if (result == false)
			break;
	}
	relation_close(rel, AccessExclusiveLock);

	PG_RETURN_BOOL(result);
}

//
// amcheck entry point: verify all b-trees of the orioledb relation and emit
// one row per failed index.  `thorough_check` forces the on-disk map check.
// wait_for_checkpoint=true makes check_btree retry across checkpointer
// overlap rather than report it as corruption.
//
Datum
verify_orioledb(PG_FUNCTION_ARGS)
{
	Oid			relid = PG_GETARG_OID(0);
	bool		thorough_check = PG_GETARG_BOOL(1);
	rsinfo: &mut ReturnSetInfo = (ReturnSetInfo *) fcinfo->resultinfo;
	pub static mut REL: Relation = std::mem::zeroed();
	pub static mut O_TABLE_DESCR: *mut descr = std::ptr::null_mut();
	pub static mut I: std::os::raw::c_int = 0;

	InitMaterializedSRF(fcinfo, 0);

	orioledb_check_shmem();

	rel = relation_open(relid, AccessShareLock);
	descr = relation_get_descr(rel);
	if (!descr)
		ereport(ERROR,
				(errcode(ERRCODE_WRONG_OBJECT_TYPE),
				 errmsg("relation \"%s\" is not orioledb",
						RelationGetRelationName(rel))));

	for (i = 0; i < descr->nIndices; i++)
	{
		pub static mut O_INDEX_DESCR: *mut idx = descr->indices[i];
		pub static mut SUCCESS: bool = false;

		o_tables_rel_lock_extended(&idx->oids, AccessExclusiveLock, true);
		o_btree_load_shmem(&idx->desc);
		success = check_btree(&idx->desc, thorough_check, true);
		o_tables_rel_unlock_extended(&idx->oids, AccessExclusiveLock, true);

		if (!success)
		{
			Datum		values[2];
			bool		nulls[2] = {false, false};

			values[0] = CStringGetTextDatum(NameStr(idx->name));
			values[1] = CStringGetTextDatum("check failed");
			tuplestore_putvalues(rsinfo->setResult, rsinfo->setDesc,
								 values, nulls);
		}
	}

	relation_close(rel, AccessShareLock);
	return (Datum) 0;
}

Datum
orioledb_compression_max_level(PG_FUNCTION_ARGS)
{
	int			max_lvl = o_compress_max_lvl();

	PG_RETURN_INT16(max_lvl);
}

Datum
orioledb_tbl_compression_check(PG_FUNCTION_ARGS)
{
	pub static mut STATS: BTreeCompressStats = std::mem::zeroed();
	pub static mut O_TABLE_DESCR: *mut descr = std::ptr::null_mut();
	pub static mut RESULT: StringInfoData = std::mem::zeroed();
	pub static mut ARRAY_TYPE: *mut array = std::ptr::null_mut();
	pub static mut INT32: *mut values = std::ptr::null_mut();
	int			compression_lvl = PG_GETARG_INT16(0),
				i,
				j,
				narray,
				next_from;
	Oid			relid = PG_GETARG_OID(1);
	pub static mut REL: Relation = std::mem::zeroed();

	Assert(PG_NARGS() == 3);

	// checks compression lvl arg
	if (compression_lvl < 0 || compression_lvl > o_compress_max_lvl())
		elog(ERROR, "Compression level must be between 0 and %d", o_compress_max_lvl());

	// checks relation arg
	orioledb_check_shmem();

	rel = relation_open(relid, AccessShareLock);
	descr = relation_get_descr(rel);
	relation_close(rel, AccessShareLock);

	if (descr == NULL)
		elog(ERROR, "orioledb relation not found.");

	// checks range array arg
	if (PG_ARGISNULL(2))
		elog(ERROR, "ranges array must be not NULL");

	array = PG_GETARG_ARRAYTYPE_P(2);
	if (array_contains_nulls(array))
		elog(ERROR, "ranges array must not contain nulls");

	narray = ArrayGetNItems(ARR_NDIM(array), ARR_DIMS(array));
	values = (int32 *) ARR_DATA_PTR(array);

	// fills stats
	stats.errors = 0;
	stats.oversize = 0;
	stats.totalSize = 0;
	stats.totalCompressedSize = 0;
	stats.nranges = narray + 1;
	stats.ranges = palloc(sizeof(BTreeCompressRange) * stats.nranges);

	// fills ranges
	next_from = 0;
	for (i = 0; i < narray; i++)
	{
		if (*values <= 0 || *values >= ORIOLEDB_BLCKSZ)
			elog(ERROR, "range value must be between %d and %d", 0, ORIOLEDB_BLCKSZ);

		if (*values <= next_from)
			elog(ERROR, "range array must be sorted ascending");

		stats.ranges[i].from = next_from;
		next_from = *values++;
		stats.ranges[i].to = next_from - 1;
		stats.ranges[i].leaf_count = 0;
		stats.ranges[i].node_count = 0;
	}
	stats.ranges[narray].from = next_from;
	stats.ranges[narray].to = ORIOLEDB_BLCKSZ;
	stats.ranges[narray].leaf_count = 0;
	stats.ranges[narray].node_count = 0;

	// collect stats for each BTree loop
	initStringInfo(&result);
	for (i = 0; i <= descr->nIndices; i++)
	{
		pub static mut B_TREE_DESCR: *mut td = std::ptr::null_mut();
		pub static mut CHAR: *mut const treeName = std::ptr::null_mut();

		if (i < descr->nIndices)
		{
			td = &descr->indices[i]->desc;
			treeName = descr->indices[i]->name.data;
		}
		else
		{
			td = &descr->toast->desc;
			treeName = "toast";
		}

		check_btree_compression(td, &stats, compression_lvl);

		if (i > 0)
			appendStringInfo(&result, "\n\n");
		appendStringInfo(&result, "Compression check for index %s\n", treeName);
		appendStringInfo(&result, "Errors %d, oversize %d\n", stats.errors, stats.oversize);
		appendStringInfo(&result, "Total size = " INT64_FORMAT "\n", stats.totalSize);
		appendStringInfo(&result, "Total compressed size = " INT64_FORMAT "\n", stats.totalCompressedSize);
		appendStringInfo(&result, "Ratio = %lf\n", (double) stats.totalCompressedSize / (double) stats.totalSize);

		// nodes
		appendStringInfo(&result, "\nCompressed pages size for nodes:\n");
		for (j = 0; j < stats.nranges; j++)
		{
			appendStringInfo(&result, "%4d - %4d = %d nodes\n",
							 stats.ranges[j].from,
							 stats.ranges[j].to,
							 stats.ranges[j].node_count);
		}

		// leafs
		appendStringInfo(&result, "\nCompressed pages size for leafs:\n");
		for (j = 0; j < stats.nranges; j++)
		{
			appendStringInfo(&result, "%4d - %4d = %d leafs\n",
							 stats.ranges[j].from,
							 stats.ranges[j].to,
							 stats.ranges[j].leaf_count);
		}

		// summary
		appendStringInfo(&result, "\nCompressed pages size summary:\n");
		for (j = 0; j < stats.nranges; j++)
		{
			appendStringInfo(&result, "%4d - %4d = %d pages\n",
							 stats.ranges[j].from,
							 stats.ranges[j].to,
							 stats.ranges[j].node_count + stats.ranges[j].leaf_count);
		}

		// reset stats before next BTree
		for (j = 0; j < stats.nranges; j++)
		{
			stats.ranges[j].leaf_count = 0;
			stats.ranges[j].node_count = 0;
		}
		stats.oversize = 0;
		stats.errors = 0;
		stats.totalSize = 0;
		stats.totalCompressedSize = 0;
	}

	pfree(stats.ranges);
	PG_RETURN_TEXT_P(cstring_to_text(result.data));
}

fn
index_description(StringInfo buf, ct: &mut OIndexDescr, bool primary, bool oids)
{
	pub static mut NON_LEAF_SIZE: std::os::raw::c_int = ct->nonLeafTupdesc->natts;
	pub static mut LEAF_SIZE: std::os::raw::c_int = ct->leafTupdesc->natts;
	pub static mut J: std::os::raw::c_int = 0;

	appendStringInfo(buf, "Index %s\n", ct->name.data);
	if (oids)
	{
		appendStringInfo(buf, "    reloid: %u\n", ct->oids.reloid);
		appendStringInfo(buf, "    relnode: %u\n", ct->oids.relnode);
	}
	appendStringInfo(buf, "    Index type: %s", primary ? "primary" : "secondary");
	appendStringInfo(buf, "%s", ct->unique ? ", unique" : "");
	if (OCompressIsValid(ct->compress))
		appendStringInfo(buf, ", compression = %d", ct->compress);
	appendStringInfo(buf, "%s\n", primary && ct->primaryIsCtid ? ", ctid" : "");
	if (ct->predicate)
		appendStringInfo(buf, "    Predicate: %s\n", ct->predicate_str);
	appendStringInfo(buf, "    Leaf tuple size: %d, non-leaf tuple size: %d\n",
					 leafSize, nonLeafSize);
	appendStringInfo(buf, "    Non-leaf tuple fields: ");
	for (j = 0; j < nonLeafSize; j++)
	{
		appendStringInfo(buf, "%s", TupleDescAttr(ct->nonLeafTupdesc, j)->attname.data);
		if (j + 1 != nonLeafSize)
			appendStringInfo(buf, ", ");
	}
	appendStringInfo(buf, "\n");
	appendStringInfo(buf, "    Leaf tuple fields: ");
	for (j = 0; j < leafSize; j++)
	{
		appendStringInfo(buf, "%s", TupleDescAttr(ct->leafTupdesc, j)->attname.data);
		if (j + 1 != leafSize)
			appendStringInfo(buf, ", ");
	}
	appendStringInfo(buf, "\n");
}

Datum
orioledb_tbl_indices(PG_FUNCTION_ARGS)
{
	Oid			relid = PG_GETARG_OID(0);
	pub static mut INTERNAL: bool = false;
	pub static mut OIDS: bool = false;
	pub static mut REL: Relation = std::mem::zeroed();
	pub static mut O_TABLE_DESCR: *mut descr = std::ptr::null_mut();
	pub static mut BUF: StringInfoData = std::mem::zeroed();
	pub static mut TEXT: *mut result = std::ptr::null_mut();
	pub static mut I: std::os::raw::c_int = 0;

	if (PG_NARGS() == 3)
	{
		internal = PG_ARGISNULL(1) ? false : PG_GETARG_BOOL(1);
		oids = PG_ARGISNULL(2) ? false : PG_GETARG_BOOL(2);
	}
	else if (PG_NARGS() != 1)
	{
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_FUNCTION_DEFINITION),
				 errmsg("orioledb_tbl_indices can be defined only as "
						"orioledb_tbl_indices(oid) or orioledb_tbl_indices(oid, bool, bool)")));
	}

	orioledb_check_shmem();

	rel = relation_open(relid, AccessShareLock);
	descr = relation_get_descr(rel);
	if (descr == NULL)
	{
		relation_close(rel, AccessShareLock);
		elog(ERROR, "orioledb relation not found");
	}

	initStringInfo(&buf);

	for (i = 0; i < descr->nIndices; i++)
	{
		pub static mut O_INDEX_DESCR: *mut ct = descr->indices[i];
		pub static mut PRIMARY: bool = i == PrimaryIndexNumber;

		index_description(&buf, ct, primary, oids);
	}

	if (internal)
	{
		if (descr->bridge)
			index_description(&buf, descr->bridge, false, oids);
		if (descr->toast)
			index_description(&buf, descr->toast, false, oids);
	}

	relation_close(rel, AccessShareLock);

	result = cstring_to_text(buf.data);
	PG_RETURN_POINTER(result);
}

//
// Includes table (primary index), TOAST and secondary indices
// Deprecated. Use pg_total_relation_size() instead
//
Datum
orioledb_relation_size(PG_FUNCTION_ARGS)
{
	Oid			relid = PG_GETARG_OID(0);
	pub static mut REL: Relation = std::mem::zeroed();
	pub static mut RESULT: int64 = std::mem::zeroed();

	orioledb_check_shmem();

	rel = relation_open(relid, AccessShareLock);
	result = orioledb_calculate_relation_size(rel, MAIN_FORKNUM, TOTAL_SIZE);
	relation_close(rel, AccessShareLock);

	elog(WARNING, "orioledb_relation_size is deprecated, use pg_total_relation_size() instead");

	PG_RETURN_INT64(result);
}

typedef struct
{
	struct
	{
		pub static mut COUNT: uint64 = std::mem::zeroed();
		pub static mut OCCUPIED: uint64 = std::mem::zeroed();
		pub static mut VACATED: uint64 = std::mem::zeroed();
		pub static mut HIKEY: OFixedKey = std::mem::zeroed();
	}			levels[ORIOLEDB_MAX_DEPTH];
} ORelationStat;

static OIndexDescr *
fetch_index_descr_by_oid(Oid relid)
{
	pub static mut REL: Relation = std::mem::zeroed();
	pub static mut TBL_OIDS: ORelOids = std::mem::zeroed();
	pub static mut IDX_OIDS: ORelOids = std::mem::zeroed();
	pub static mut O_TABLE_DESCR: *mut descr = std::ptr::null_mut();
	pub static mut IXNUM: OIndexNumber = std::mem::zeroed();
	pub static mut INDEX: bool = false;

	ORelOidsSetInvalid(idxOids);

	rel = relation_open(relid, AccessShareLock);
	if (rel->rd_rel->relkind == RELKIND_INDEX)
	{
		pub static mut TBL: Relation = std::mem::zeroed();

		idxOids.datoid = MyDatabaseId;
		idxOids.reloid = rel->rd_rel->oid;
		idxOids.relnode = rel->rd_rel->relfilenode;

		tbl = relation_open(rel->rd_index->indrelid, AccessShareLock);
		relation_close(rel, AccessShareLock);
		rel = tbl;
		index = true;
	}

	tblOids.datoid = MyDatabaseId;
	tblOids.reloid = rel->rd_rel->oid;
	tblOids.relnode = rel->rd_rel->relfilenode;

	descr = o_fetch_table_descr(tblOids);

	if (index)
		ixnum = find_tree_in_descr(descr, idxOids);
	else
		ixnum = PrimaryIndexNumber;

	relation_close(rel, AccessShareLock);

	return descr->indices[ixnum];
}

fn
add_page_stat(desc: &mut BTreeDescr, Page p, stat: &mut ORelationStat)
{
	int			level = PAGE_GET_LEVEL(p);

	if (!O_PAGE_IS(p, RIGHTMOST))
		copy_fixed_hikey(desc, &stat->levels[level].hikey, p);
	else
		clear_fixed_key(&stat->levels[level].hikey);

	stat->levels[level].count++;
	stat->levels[level].occupied += ((BTreePageHeader *) (p))->dataSize;

	if (O_PAGE_IS(p, LEAF))
		stat->levels[level].vacated += PAGE_GET_N_VACATED(p);
}

fn
tree_stat_walker(desc: &mut BTreeDescr, stat: &mut ORelationStat)
{
	pub static mut CONTEXT: OBTreeFindPageContext = std::mem::zeroed();
	int			level,
				maxLevel;

	init_page_find_context(&context, desc,
						   COMMITSEQNO_INPROGRESS,
						   BTREE_PAGE_FIND_IMAGE);

	for (level = 0; level < ORIOLEDB_MAX_DEPTH; level++)
	{
		pub static mut FIND_RESULT: OFindPageResult = std::mem::zeroed();

		findResult = find_page(&context, NULL, BTreeKeyNone, level);
		if (findResult != OFindPageResultSuccess ||
			PAGE_GET_LEVEL(context.img) != level)
			break;

		add_page_stat(desc, context.img, stat);
	}
	maxLevel = level;

	if (maxLevel == 0)
		return;

	while (true)
	{
		pub static mut KEY: OFixedKey = std::mem::zeroed();

		copy_fixed_key(desc, &key, stat->levels[0].hikey.tuple);

		if (O_TUPLE_IS_NULL((key.tuple)))
			break;

		for (level = 0; level < maxLevel; level++)
		{
			if (O_TUPLE_IS_NULL(stat->levels[level].hikey.tuple))
				break;

			if (level == 0 ||
				o_btree_cmp(desc,
							&key.tuple, BTreeKeyNonLeafKey,
							&stat->levels[level].hikey.tuple, BTreeKeyNonLeafKey) >= 0)
			{
				if (find_page(&context, &key.tuple,
							  BTreeKeyNonLeafKey, level) == OFindPageResultSuccess)
					add_page_stat(desc, context.img, stat);
				else
					break;
			}
		}
	}
}

Datum
orioledb_tree_stat(PG_FUNCTION_ARGS)
{
	Oid			relid = PG_GETARG_OID(0);
	rsinfo: &mut ReturnSetInfo = (ReturnSetInfo *) fcinfo->resultinfo;
	pub static mut TUPDESC: TupleDesc = std::mem::zeroed();
	pub static mut TUPLESTORESTATE: *mut tupstore = std::ptr::null_mut();
	pub static mut PER_QUERY_CTX: MemoryContext = std::mem::zeroed();
	pub static mut OLDCONTEXT: MemoryContext = std::mem::zeroed();
	pub static mut O_INDEX_DESCR: *mut descr = std::ptr::null_mut();
	pub static mut O_RELATION_STAT: *mut stat = std::ptr::null_mut();
	pub static mut I: std::os::raw::c_int = 0;

	per_query_ctx = rsinfo->econtext->ecxt_per_query_memory;
	oldcontext = MemoryContextSwitchTo(per_query_ctx);

	// Build a tuple descriptor for our result type
	if (get_call_result_type(fcinfo, NULL, &tupdesc) != TYPEFUNC_COMPOSITE)
		elog(ERROR, "return type must be a row type");

	tupstore = tuplestore_begin_heap(true, false, work_mem);
	rsinfo->returnMode = SFRM_Materialize;
	rsinfo->setResult = tupstore;
	rsinfo->setDesc = tupdesc;

	MemoryContextSwitchTo(oldcontext);

	orioledb_check_shmem();

	descr = fetch_index_descr_by_oid(relid);

	stat = (ORelationStat *) palloc(sizeof(ORelationStat));
	memset(stat, 0, sizeof(*stat));

	o_btree_load_shmem(&descr->desc);
	tree_stat_walker(&descr->desc, stat);

	for (i = 0; i < ORIOLEDB_MAX_DEPTH; i++)
	{
		Datum		values[4];
		bool		nulls[4] = {false};

		if (stat->levels[i].count == 0)
			continue;

		values[0] = Int32GetDatum(i);
		values[1] = Int64GetDatum(stat->levels[i].count);
		values[2] = Float8GetDatum((float8) stat->levels[i].occupied / (float8) stat->levels[i].count);
		values[3] = Float8GetDatum((float8) stat->levels[i].vacated / (float8) stat->levels[i].count);
		tuplestore_putvalues(tupstore, tupdesc, values, nulls);
	}

	pfree(stat);

	return (Datum) 0;
}