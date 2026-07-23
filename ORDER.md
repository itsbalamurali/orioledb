# OrioleDB Rust Port — Build / Dependency Order

This file records the dependency graph for porting the engine module-by-module
in a safe, bottom-up order. Each file should be ported only after the files it
depends on (listed under "Depends on") are themselves ported.

## Environment constraint
A full `cargo check` is **not possible** in this workspace: pgrx's codegen
requires `$PGRX_HOME` + a PostgreSQL install (`pg_config`), neither of which is
present. Ports are therefore verified by: (1) reading the real pgrx/zstd API
from the cargo registry cache, (2) brace/paren balance, (3) absence of C
syntax, and (4) faithful algorithm reproduction. Compilation becomes possible
once the pgrx toolchain is installed.

## Legend
- `[done]`  = already ported (lib.rs, utils/compress.rs, utils/ucm.rs)
- `[stub]`  = exists but broken mechanical C-translation; must be rewritten
- internal headers: `btree/*.h`, `include/orioledb.h`

## Foundation types (no internal deps)
These are the shared structs/constants every other file needs. Port first, into
a single `btree::types` module (mirrors `include/orioledb.h` + `btree/*.h`):

1. `OInMemoryBlkno`, `OInvalidInMemoryBlkno`, `O_PAGE_IS_LOCAL`, `O_BLKNO_MASK`
2. `ORelOids` (+ `ORelOidsIsValid/SetInvalid/IsEqual`), `OIndexType` enum
3. `FileExtent`, `OrioleDBPageHeader` (state/changeCount/checkpointNum),
   `OrioleDBOndiskPageHeader`, `O_PAGE_HEADER_SIZE`
4. `OrioleDBPageDesc` (oids/ionum/fileExtent/flags:type/leftBlkno)
5. `O_PAGE_STATE_*` macros + `PAGE_STATE_*` constants (page_state.h)
6. `OPagePoolType` enum + `OPagePoolTypesCount`
7. `O_GET_IN_MEMORY_PAGE` / `O_GET_IN_MEMORY_PAGEDESC` (via `o_shared_buffers`)
8. `OTuple`, `OTupleXactInfo`, `OXid`, `OIndexNumber`, `LocationIndex`
9. `BTreeRootInfo`, `BTreeStorageType`, `BTreeKeyType`, `BTreeOperationType`,
   `BTreeLeafTupleDeletedStatus`, `OLengthType`, `OSmgr`, `BTreeLocalFreeExtents`
10. `BTreeOps` (vtable of fn pointers), `BTreeDescr`
11. `BTreePageItemLocator`, lock enums (`OLockPageWithTupleResult`, ...)

## btree module port order (bottom-up)
Internal `.c` include graph determines order:

- **L1 `btree/page_state.rs`** — state constants, lock enums, page-state
  helpers, `BTreePageItemLocator` accessors. Depends on: foundation types.
- **L2 `btree/btree.rs`** — `BTreeDescr` init, unique-lwlock setup,
  `o_btree_init`, meta/root page init. Depends on: L1, io.
- **L3 `btree/page_contents.rs`** — page/chunk header layout, level/flags,
  hikey. Depends on: foundation types, page_state.
- **L4 `btree/page_chunks.rs`** — chunk/page item locator math, item CRUD on a
  page. Depends on: L3, page_state, tuple/format.
- **L5 `btree/io.rs`** (largest, 98KB) — shared buffer I/O, page read/write,
  eviction hooks, `O_GET_IN_MEMORY_PAGE`. Depends on: L1–L4, page_pool,
  compress, checkpoint, recovery, catalog/o_sys_cache, s3, workers/bgwriter.
  Split into sub-sections when porting.
- **L6 `btree/find.rs`** — tree search/descend. Depends on: L1–L5.
- **L7 `btree/insert.rs`** — tuple insert, split trigger. Depends on: L1–L6,
  split.
- **L7 `btree/split.rs`** — page split. Depends on: L1–L6, page_chunks.
- **L7 `btree/merge.rs`** — page merge. Depends on: L1–L6, find.
- **L8 `btree/undo.rs`** — undo image application to pages. Depends on:
  L1–L6, transam/undo, recovery, rewind, catalog/o_sys_cache.
- **L8 `btree/modify.rs`** — high-level modify (insert/update/delete/lock).
  Depends on: L1–L7, undo, recovery/wal.
- **L8 `btree/iterator.rs`** — iterator over a tree. Depends on: L1–L6,
  page_chunks, undo, catalog/sys_trees, tableam/key_range.
- **L8 `btree/scan.rs`** — scan/seqscan. Depends on: L1–L7, iterator,
  tableam/descr, tuple/slot, utils/sampling, utils/wait_event.
- **L9 `btree/fastpath.rs`** — fast insert/find path. Depends on: L1–L6,
  btree, find, tableam/key_range.
- **L9 `btree/build.rs`** — CREATE INDEX build. Depends on: L1–L7, insert,
  split, checkpoint, recovery, s3/worker, tuple/sort, tuple/toast.
- **L9 `btree/check.rs`** — page/tree consistency check. Depends on: L1–L6,
  io, page_chunks, catalog/free_extents, catalog/sys_trees, checkpoint,
  compress, ucm, seq_buf.
- **L9 `btree/print.rs`** — debug print. Depends on: L1–L5, merge,
  page_chunks, undo, tuple/format.

## Notes
- `utils/page_pool.rs` (types + estimate/init + ops dispatch) and
  `utils/compress.rs` [done] are prerequisites for io/page_state. `ucm`
  [done] is used by page_pool/page_state.
- `transam` (oxid/undo), `recovery`, `catalog`, `checkpoint`, `tuple/format`,
  `tableam`, `s3`, `workers` remain stub modules that the upper btree layers
  call into; they are ported in later phases (per AGENTS.md order) but their
  function signatures must be agreed when the btree layers reference them.
- `lib.rs` already references `crate::btree::io::*` and `crate::btree::page_state::*`
  for page-header accessors; the foundation `types` module should own the raw
  buffer + `O_PAGE_HEADER(state)` accessors so both `lib.rs`, `ucm.rs`, and
  `btree/*` share one implementation.
