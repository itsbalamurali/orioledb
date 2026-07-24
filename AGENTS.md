# OrioleDB Rust Porting Guide

You are working on a **full Rust rewrite** of OrioleDB extension to rust using pgrx.

## Repository Structure
* Original C implementation:

  * `src/`
  * `include/`

* Rust implementation:
  * `orioledb-rs/`

The Rust implementation is **not** intended to be a thin wrapper around the C implementation.
The goal is to completely replace the C code with idiomatic Rust.

---

## Strategy

Start with small, self-contained files for quick wins, then work up to larger,
more complex files. **Dependencies always gate progress** — you can't fix a file
that depends on a broken one. The foundation types (`types.rs`) are the most
important file — fix them first even though they are large, because everything
else depends on them.

---

# Critical Rules

## Do NOT create wrappers around the C implementation

This means:

* Do NOT use `extern "C"` simply to call the existing C implementation.
* Do NOT leave placeholder implementations.
* Do NOT generate shim layers.
* Do NOT keep logic inside C because it is "easier."

Every algorithm, data structure, state machine, parser, serializer, iterator,
and subsystem must be implemented in Rust. **The C code is only the specification.**

---

# Current Problem

Many files currently contain things like:
* `extern "C"` wrappers
* empty implementations
* TODO stubs
* placeholder functions
* unsafe wrappers calling C
* files or implementations missing

This is **not acceptable**. Replace them with actual Rust implementations that
faithfully reproduce the original C behavior.

---

# Cleanup Checklist (for every file)

Before and during porting, apply these cleanup steps to every file in `orioledb-rs`:

### 1. Convert comments
Replace C-style comments with idiomatic Rust documentation.
Convert block comments, inline comments, function descriptions, and module
descriptions into proper Rust style: `///`, `//!`, normal `//`.

Write comments naturally in Rust instead of mechanically copying C comments.

### 2. Fix Rust Analyzer warnings
Resolve all warnings including:
* non_snake_case
* non_camel_case_types
* upper_case_globals where appropriate
* unused imports, dead code, unused variables
* unnecessary mutability, needless clones, needless returns, unreachable code

The project should move steadily toward a warning-free state.

### 3. Reduce unsafe
Current code contains excessive `unsafe`. Refactor so that:
* unsafe is isolated, safe APIs wrap unsafe internals
* raw pointers become references where possible
* pointer arithmetic becomes slices/iterators
* ownership replaces manual memory management, lifetimes replace manual tracking
* unsafe exists only where absolutely required

---

# Porting Requirements

The Rust implementation should embrace Rust rather than imitate C.

## Prefer:
* ownership, borrowing, RAII, Result, Option
* iterators, slices, traits, enums, pattern matching
* const generics where appropriate

## Avoid:
* global mutable state, raw pointers, manual allocation
* C naming conventions, C-style casts
* memcpy-style programming when safe Rust can express the same behavior

---

# Behaviour Preservation

Maintain identical behavior to the C implementation. This includes:
* on-disk formats, WAL compatibility, page layouts
* B-tree behavior, transaction semantics, checkpoint logic
* recovery, concurrency behavior, serialization formats

Behavioral compatibility is mandatory.

---

# Migration Order — Detailed Phases

## Legend

- `[x]` = **done** — idiomatic Rust, no C shims/stubs/TODOs
- `[~]` = **in progress** — partially ported
- `[ ]` = **not started**

## Quick-start checklist

```
Phase 0 — Foundation types (types.rs)              [x] ~100%
Phase 1 — Tiny utilities (1–50 lines each)         [~]
Phase 2 — Small caches & utils (50–350 lines)      [~]
Phase 3 — Core btree L1–L4 (200–1500 lines)       [ ]
Phase 4 — Btree I/O + traversal (800–3700 lines)  [ ]
Phase 5 — High-level APIs + tuple/tableam           [ ]
Phase 6 — Heavy modules (3000+ lines)               [ ]
Phase 7 — Integration & cleanup                     [ ]
```

---

## Phase 0 — Foundation types

**Module:** `orioledb-rs/src/btree/types.rs`  **C source:** `include/orioledb.h` + `include/btree/*.h`
**Size:** ~1900 lines (foundation — everything depends on this)
**Depends on:** nothing internal

This file owns ALL shared structs/constants. Every other module imports from here.
It is the single most important file — fix it first even though it is large.

- [x] `OInMemoryBlkno`, `OInvalidInMemoryBlkno`, `O_PAGE_IS_LOCAL`, `O_BLKNO_MASK`
- [x] `ORelOids` (+ `is_valid`/`set_invalid`/`is_equal`)
- [x] `OIndexType` enum
- [x] `FileExtent` (16-bit len + 48-bit off packed)
- [x] `OrioleDBPageHeader` (state / changeCount / checkpointNum)
- [x] `OrioleDBOndiskPageHeader` (on-disk layout)
- [x] `O_PAGE_HEADER_SIZE` constant
- [x] `OrioleDBPageDesc` (oids / ionum / fileExtent / flags:type / leftBlkno)
- [x] `O_PAGE_STATE_*` constants & helpers
- [x] `OPagePoolType` enum + `OPagePoolTypesCount`
- [x] `OTuple`, `OTupleXactInfo`, `OXid`, `OIndexNumber`, `LocationIndex`
- [x] `BTreeRootInfo` (+ `root_page_is_valid` / `meta_page_is_valid`)
- [x] `BTreeStorageType` enum
- [x] `BTreeKeyType` enum + `is_bound_key_type()`
- [x] `BTreeOperationType` enum
- [x] `BTreeLeafTupleDeletedStatus` enum
- [x] `OLengthType` enum
- [x] `OSmgr` (array/hash union)
- [x] `BTreeLocalFreeExtents`
- [x] `BTreePageChunkDesc` (12+10+7+1+2 bitfields)
- [x] `BTreePageChunk` (VLA of LocationIndex)
- [x] `BTreePageItemLocator` (+ `is_valid`/`set_invalid`)
- [x] `BTreePageItem`
- [x] `BTreeItemPageFitType` enum
- [x] `BTreeMetaPage`
- [x] `BTreePageHeader` (flags + field1/field2 bitfields)
- [x] `BTreeLeafTuphdr` (xactInfo / deleted / chainHasLocks / undoLocation)
- [x] `BTreeNonLeafTuphdr` (downlink)
- [x] Downlink bit flags
- [x] `O_BTREE_MAX_TUPLE_SIZE`, `O_BTREE_MAX_KEY_SIZE`
- [x] `OFixedTuple`, `OFixedKey`
- [x] `ReadPageResult` enum
- [x] `OPageWaiterStatus` enum
- [x] `OLockPageWithTupleResult` enum
- [x] `OBTreeModifyCallbackAction`, `OBTreeWaitCallbackAction`, `OBTreeModifyResult`
- [x] `RowLockMode` + `row_locks_conflict()`
- [x] `BTreeLocationHint`, `ORowIdAddendumCtid`, `ORowIdBridgeData`
- [x] `ORelOptions`, `OBTOptions`
- [x] `XidVXidMapElement`
- [x] `UndoStackSharedLocations`, `UndoRetainSharedLocations`
- [x] `ODBProcData`
- [x] `PartialPageState`
- [x] `SeqBufDescShared`, `SeqBufDescPrivate`, `SeqBufTag`
- [x] `LWLock`, `LWLockPadded`, `SLock` (placeholders)
- [x] `OXidMapItem`, `RewindItem` (opaque placeholders)
- [x] `MAXALIGN` / `MAXALIGN_DOWN` helpers
- [x] `assert_type_sizes()` compile-time checks
- [x] Module doc comments (Rust `//!` style)

---

## Phase 1 — Tiny utilities (1–50 lines each)

**Strategy:** Quick wins. These files are small and self-contained or have minimal
internal dependencies. Fixing these removes broken module stubs and makes the
project structure cleaner.

### 1a — Workers (interrupt handling)

**Module:** `orioledb-rs/src/workers/interrupt.rs`  **C source:** `src/workers/interrupt.c`
**Size:** 46 C / 42 Rust lines  **Depends on:** `types`

- [x] Interrupt handler / signal handling

### 1b — Indexam (single-file module)

**Module:** `orioledb-rs/src/indexam/handler.rs`  **C source:** `src/indexam/handler.c`
**Size:** 2153 C / 2140 Rust lines  **Depends on:** `types`, `btree`, `catalog`, `tableam`

- [ ] Index access method handler — **BLOCKED** by unported btree, catalog, tableam

### 1c — Module stubs (mod.rs files)

Verify all `mod.rs` files declare modules correctly:

- [x] `orioledb-rs/src/btree/mod.rs` (20 lines — declares 19 btree modules, verified correct)
- [x] `orioledb-rs/src/catalog/mod.rs` (21 lines, verified correct)
- [x] `orioledb-rs/src/checkpoint/mod.rs` (3 lines, verified correct)
- [x] `orioledb-rs/src/recovery/mod.rs` (6 lines, verified correct)
- [x] `orioledb-rs/src/rewind/mod.rs` (2 lines, verified correct)
- [x] `orioledb-rs/src/s3/mod.rs` (9 lines, verified correct)
- [x] `orioledb-rs/src/tableam/mod.rs` (13 lines, verified correct)
- [x] `orioledb-rs/src/transam/mod.rs` (3 lines, verified correct)
- [x] `orioledb-rs/src/tuple/mod.rs` (5 lines, verified correct)
- [x] `orioledb-rs/src/utils/mod.rs` (8 lines, verified correct)
- [x] `orioledb-rs/src/workers/mod.rs` (3 lines, verified correct)
- [x] `orioledb-rs/src/indexam/mod.rs` (2 lines, verified correct)

---

## Phase 2 — Small caches & utilities (50–350 lines each)

**Strategy:** Small, mostly self-contained modules. Many are cache lookups with
simple CRUD operations. Low risk, high satisfaction — each fix removes a broken
translation.

**Depends on:** `types`

### 2a — Compression & control

- [x] `orioledb-rs/src/utils/compress.rs` — zstd compression (112 C / 106 Rust)
  - `OCompress` type, `LazyLock`-based global contexts (`ZSTD_CCTX`, `ZSTD_DCTX`, `ZSTD_DST`),
    `o_compress_init`, `o_compress_page`, `o_decompress_page`, `o_compress_max_lvl`, `validate_compress`
- [x] `orioledb-rs/src/checkpoint/control.rs` — checkpoint control file (152 C / 416 Rust)
  - `CheckpointControl` + `CheckpointUndoInfo` structs, CRC32C implementation,
    `get_checkpoint_control_data`, `check_checkpoint_control`, `write_checkpoint_control`
- [x] `orioledb-rs/src/catalog/o_tablespace_cache.rs` — tablespace cache (58 C / 58 Rust)
  - `o_get_prefixes_for_tablespace` — returns `(String, String)` using pgrx FFI

### 2b — S3 small modules

- [x] `orioledb-rs/src/s3/checksum.rs` — S3 checksums (255 C / 310 Rust)
  - `S3FileChecksum` + `S3ChecksumState` structs, SHA-256 via `sha2` crate,
    `make_s3_checksum_state`, `free_s3_checksum_state`, `flush_s3_checksum_state`,
    `get_s3_file_checksum` using `HashMap` + `std::fs`
- [x] `orioledb-rs/src/s3/control.rs` — S3 control (295 C / 576 Rust)
  - `s3_check_control`, `s3_put_lock_file`, `s3_delete_lock_file`,
    `StringInfo` buffer type, `ORIOLEDB_DATA_DIR` constant,
    conditional S3 upload with retry logic for concurrent lock detection
- [ ] `orioledb-rs/src/s3/archive.rs` — S3 WAL archiving (178 C / 175 Rust) — **BLOCKED** by unported S3TaskLocation, ArchiveModuleCallbacks
- [ ] `orioledb-rs/src/s3/queue.rs` — S3 request queue (348 C / 344 Rust) — **BLOCKED** by pg_atomic_uint64, ConditionVariable

### 2c — Catalog cache modules (tiny CRUD lookups)

All these are cache lookup modules with simple struct + init + get functions:

- [ ] `orioledb-rs/src/catalog/o_tablespace_cache.rs` (62 C / 58 Rust)
- [ ] `orioledb-rs/src/catalog/o_amproc_cache.rs` (143 C / 138 Rust)
- [ ] `orioledb-rs/src/catalog/o_operator_cache.rs` (150 C / 145 Rust)
- [ ] `orioledb-rs/src/catalog/o_opclass_cache.rs` (185 C / 179 Rust)
- [ ] `orioledb-rs/src/catalog/o_range_cache.rs` (242 C / 236 Rust)
- [ ] `orioledb-rs/src/catalog/o_aggregate_cache.rs` (275 C / 269 Rust)
- [ ] `orioledb-rs/src/catalog/o_type_cache.rs` (287 C / 282 Rust)
- [ ] `orioledb-rs/src/catalog/o_class_cache.rs` (293 C / 288 Rust)
- [ ] `orioledb-rs/src/catalog/o_collation_cache.rs` (300 C / 297 Rust)
- [ ] `orioledb-rs/src/catalog/o_amop_cache.rs` (330 C / 324 Rust)
- [ ] `orioledb-rs/src/catalog/o_database_cache.rs` (348 C / 330 Rust)
- [ ] `orioledb-rs/src/catalog/o_enum_cache.rs` (536 C / 531 Rust)

---

## Phase 3 — Core btree layers (L1–L4)

**Strategy:** These are the core btree internals. They depend on `types` and on
each other. `page_state.rs` is the most complex here but also the most critical —
the page locking protocol is the concurrency foundation.

**Depends on:** Phase 0 (types), Phase 2 (utils)

### L1 — Page state & locking

**Module:** `orioledb-rs/src/btree/page_state.rs`  **C source:** `src/btree/page_state.c`
**Size:** 1456 C / 1451 Rust lines  **Depends on:** `types`, `utils::ucm`, `utils::page_pool`

The page locking protocol is the concurrency foundation. Uses CAS on 64-bit
atomic state words, lock-free waiter list, and semaphore-based wakeup.

- [ ] `OPageWaiterShmemState` struct + shmem allocation
- [ ] `my_locked_pages` tracking (`MyLockedPage` array, `MAX_PAGES_PER_PROCESS`)
- [ ] `lock_page()` — acquire page lock via CAS on state word
- [ ] `lock_page_with_tuple()` — lock + waiter registration
- [ ] `unlock_page()` — release with usage-count update (UCM)
- [ ] `unlock_page_internal()` — split-aware unlock with waiter wake
- [ ] `btree_register/unregister_inprogress_split()`
- [ ] `btree_split_mark_finished()`
- [ ] `try_lock_page()`, `relock_page()`, `page_is_locked()`
- [ ] `page_block_reads()`, `page_wait_for_read_enable()`
- [ ] `get_waiters_with_tuples()`, `mark_waiter_tuples_inserted()`
- [ ] `release_all_page_locks()`
- [ ] Debug functions (`o_check_page_struct`, `o_check_btree_page_statistics`)

### L2 — B-tree init & meta page

**Module:** `orioledb-rs/src/btree/btree.rs`  **C source:** `src/btree/btree.c`
**Size:** 425 C / 419 Rust lines  **Depends on:** `types`, `page_state`, `io`

- [ ] `unique_locks` (LWLock array) + `num_unique_locks`
- [ ] `o_btree_init()` — init root + meta pages
- [ ] `o_btree_cleanup_pages()`
- [ ] `btree_ctid_get_and_inc()` / `btree_bridge_ctid_get_and_inc()`
- [ ] `get_page_children()` (recursive descent helper)

### L3 — Page contents & chunk layout

**Module:** `orioledb-rs/src/btree/page_contents.rs`  **C source:** `src/btree/page_contents.c`
**Size:** 871 C / 866 Rust lines  **Depends on:** `types`

- [ ] `BTreeMetaPage` accessors
- [ ] `init_new_btree_page()`, `init_meta_page()`
- [ ] `try_copy_page()`, `o_btree_read_page()`, `o_btree_try_read_page()`
- [ ] `read_page_from_undo()`, `put_page_image()`
- [ ] `page_get_hikey()`, `page_resize_hikey()`, `btree_page_update_max_key_len()`
- [ ] `BTreePageHeader` accessors (O_PAGE_IS, ITEM macros, LOCATOR macros)
- [ ] Downlink helpers (`MAKE_IN_MEMORY_DOWNLINK`, `MAKE_IO_DOWNLINK`, etc.)
- [ ] `RIGHTLINK_GET_BLKNO()` / `InvalidRightLink()` / `RightLinkIsValid()`

### L4 — Page chunks & item CRUD

**Module:** `orioledb-rs/src/btree/page_chunks.rs`  **C source:** `src/btree/page_chunks.c`
**Size:** 1496 C / 1491 Rust lines  **Depends on:** `types`, `page_contents`, `utils::ucm`

- [ ] `partial_load_hikeys_chunk()`, `partial_load_full_page()`, `partial_load_chunk()`
- [ ] `page_locator_fits_item()`, `o_btree_page_calculate_statistics()`
- [ ] `init_page_first_chunk()`, `page_chunk_fill_locator()`
- [ ] `page_item_fill_locator()` / `page_item_fill_locator_backwards()`
- [ ] `page_locator_insert_item()`, `page_locator_fits_new_item()`
- [ ] `page_locator_resize_item()`, `page_locator_delete_item()`
- [ ] `page_split_chunk_if_needed()`, `btree_page_reorg()`, `split_page_by_chunks()`

### L3–L4 supporting utilities

- [ ] `orioledb-rs/src/utils/ucm.rs` — usage count map (596 C / 596 Rust)
- [ ] `orioledb-rs/src/utils/page_pool.rs` — page pool management (803 C / 799 Rust)
- [ ] `orioledb-rs/src/utils/seq_buf.rs` — sequential buffer (803 C / 799 Rust)
- [ ] `orioledb-rs/src/utils/o_buffers.rs` — shared buffer management (716 C / 712 Rust)
- [ ] `orioledb-rs/src/utils/stopevent.rs` — stop event logging (463 C / 459 Rust)

---

## Phase 4 — Btree I/O + traversal (L5–L8)

**Strategy:** These are the complex btree internals. They have cross-dependencies
within the btree module and call into external modules (catalog, transam, s3).

### L5 — Shared buffer I/O

**Module:** `orioledb-rs/src/btree/io.rs`  **C source:** `src/btree/io.c`
**Size:** 3698 C / 3690 Rust lines — **LARGEST SINGLE FILE**
**Depends on:** types, page_state, page_contents, page_chunks, utils, catalog, s3, checkpoint, workers

Split into sub-sections when porting:
- [ ] `O_GET_IN_MEMORY_PAGE()` helpers
- [ ] Page read/write (`o_btree_page_read()`, `o_btree_page_write()`)
- [ ] Eviction hooks (`evict_page()`, `ppool_evict_page()`)
- [ ] File operations (`o_btree_create/open/close_data_file()`)
- [ ] File extent management (`alloc_file_extent()`, `free_file_extent()`)
- [ ] Checkpoint map (`write_checkpoint_map()`, `read_checkpoint_map()`)
- [ ] S3 integration (`s3_upload()`, `s3_download()`)
- [ ] Shared memory loading (`o_btree_load_shmem()`)

### L6 — Tree search / descend

**Module:** `orioledb-rs/src/btree/find.rs`  **C source:** `src/btree/find.c`
**Size:** 2151 C / 2140 Rust lines  **Depends on:** types, page_state, page_contents, page_chunks, io

- [ ] `OBTreeFindPageContext` struct
- [ ] `find_page()` — main tree descent
- [ ] `find_page_descend()`, `find_page_internal()`, `find_page_leaf()`
- [ ] `find_page_follow_rightlink()`, `find_page_check_split()`, `find_page_retry()`

### L7 — Insert

**Module:** `orioledb-rs/src/btree/insert.rs`  **C source:** `src/btree/insert.c`
**Size:** 1722 C / 1717 Rust lines  **Depends on:** types, page_state, page_contents, page_chunks, find, split, undo, io

- [ ] `o_btree_insert_tuple()` — main insert entry point
- [ ] `btree_leaf_probe()`, `btree_leaf_probe_insert_slot()`
- [ ] `btree_insert_split()`, `o_btree_insert_split()`
- [ ] `btree_insert_upwards()` — propagate split to parent

### L7 — Split

**Module:** `orioledb-rs/src/btree/split.rs`  **C source:** `src/btree/split.c`
**Size:** 512 C / 508 Rust lines  **Depends on:** types, page_state, page_contents, page_chunks, find

- [ ] `o_btree_page_split()` — split a page in half
- [ ] `o_btree_split_leaf_page()`, `o_btree_split_internal_page()`
- [ ] `o_btree_split_fix_parent()`, `o_btree_split_fix_right_page()`

### L7 — Merge

**Module:** `orioledb-rs/src/btree/merge.rs`  **C source:** `src/btree/merge.c`
**Size:** 783 C / 777 Rust lines  **Depends on:** types, page_state, page_contents, page_chunks, find

- [ ] `o_btree_page_merge()` — merge two adjacent pages
- [ ] `o_btree_merge_leaf_pages()`, `o_btree_merge_internal_pages()`
- [ ] `o_btree_merge_fix_parent()`, `o_btree_delete_empty_page()`

### L8 — Undo

**Module:** `orioledb-rs/src/btree/undo.rs`  **C source:** `src/btree/undo.c`
**Size:** 2159 C / 2152 Rust lines  **Depends on:** types, page_state, page_contents, page_chunks, find, transam, recovery, rewind, catalog

- [ ] `o_btree_apply_undo_image()` — apply undo to page
- [ ] `o_btree_undo_apply_page_image()`, `o_btree_undo_apply_leaf_tuple()`
- [ ] `o_btree_undo_read_page()`, `o_btree_undo_apply_undo_record()`

### L8 — Modify

**Module:** `orioledb-rs/src/btree/modify.rs`  **C source:** `src/btree/modify.c`
**Size:** 1642 C / 1635 Rust lines  **Depends on:** types, page_state, page_contents, page_chunks, find, insert, split, merge, undo

- [ ] `o_btree_modify()` — high-level modify (insert/update/delete)
- [ ] `o_btree_delete()`, `o_btree_update()`, `o_btree_lock_tuple()`
- [ ] `o_btree_modify_generate_undo()`

### L8 — Iterator

**Module:** `orioledb-rs/src/btree/iterator.rs`  **C source:** `src/btree/iterator.c`
**Size:** 2598 C / 2588 Rust lines  **Depends on:** types, page_state, page_contents, page_chunks, find, undo, catalog::sys_trees, tableam

- [ ] `BTreeIterator` struct
- [ ] `o_btree_iterate_scan()`, `o_btree_index_getnext()`
- [ ] `o_btree_iterate_end()`, `o_btree_iterate_rescan()`
- [ ] `o_btree_iterate_move_forward()`, `o_btree_iterate_move_backward()`

---

## Phase 5 — High-level APIs + tuple/tableam

**Strategy:** These modules are larger but more straightforward — they compose
the lower btree layers into usable APIs. Depends on phases 3–4 being complete.

### L9 — Scan

**Module:** `orioledb-rs/src/btree/scan.rs`  **C source:** `src/btree/scan.c`
**Size:** 2575 C / 2568 Rust lines  **Depends on:** types, page_state, iterator, tableam, tuple::format

- [ ] `o_btree_seq_scan()`, `o_btree_index_scan()`, `o_btree_bitmap_scan()`
- [ ] Scan iteration helpers

### L9 — Fast path

**Module:** `orioledb-rs/src/btree/fastpath.rs`  **C source:** `src/btree/fastpath.c`
**Size:** 800 C / 795 Rust lines  **Depends on:** types, page_state, btree, find, tableam::key_range

- [ ] `o_btree_fast_insert()`, `o_btree_fast_find()`
- [ ] `o_btree_fast_delete()`, `o_btree_fast_update()`

### L9 — Build

**Module:** `orioledb-rs/src/btree/build.rs`  **C source:** `src/btree/build.c`
**Size:** 486 C / 482 Rust lines  **Depends on:** types, page_state, find, insert, split, tuple::sort

- [ ] `o_btree_build()` — CREATE INDEX build entry
- [ ] `o_btree_build_leaf_page()`, `o_btree_build_internal_page()`
- [ ] `o_btree_build_sort_tuples()`

### L9 — Check

**Module:** `orioledb-rs/src/btree/check.rs`  **C source:** `src/btree/check.c`
**Size:** 809 C / 805 Rust lines  **Depends on:** types, page_state, io, page_chunks, utils::ucm

- [ ] `o_btree_check_page()`, `o_btree_check_tree()`
- [ ] `o_btree_check_page_integrity()`, `o_btree_check_page_links()`

### L9 — Print (debug)

**Module:** `orioledb-rs/src/btree/print.rs`  **C source:** `src/btree/print.c`
**Size:** 896 C / 876 Rust lines  **Depends on:** types, page_state, find, merge, page_chunks, undo, tuple::format

- [ ] `o_btree_print_page()`, `o_btree_print_tree()`
- [ ] `o_btree_print_page_items()`, `o_btree_print_page_chunks()`

### Tuple module

- [ ] `orioledb-rs/src/tuple/format.rs` — tuple serialization (875 C / 869 Rust)
- [ ] `orioledb-rs/src/tuple/toast.rs` — TOAST handling (1330 C / 1323 Rust)
- [ ] `orioledb-rs/src/tuple/sort.rs` — tuple sorting (433 C / 429 Rust)
- [ ] `orioledb-rs/src/tuple/slot.rs` — tuple slot management (1998 C / 1994 Rust)

### Tableam module

- [ ] `orioledb-rs/src/tableam/key_range.rs` — key range handling (483 C / 479 Rust)
- [ ] `orioledb-rs/src/tableam/key_bitmap.rs` — key bitmap (709 C / 704 Rust)
- [ ] `orioledb-rs/src/tableam/tree.rs` — tree helper (1072 C / 1066 Rust)
- [ ] `orioledb-rs/src/tableam/scan.rs` — table scan (1168 C / 1164 Rust)
- [ ] `orioledb-rs/src/tableam/index_scan.rs` — index scan (1242 C / 1241 Rust)
- [ ] `orioledb-rs/src/tableam/bitmap_scan.rs` — bitmap scan (2125 C / 2118 Rust)
- [ ] `orioledb-rs/src/tableam/vacuum.rs` — vacuum (1608 C / 1604 Rust)
- [ ] `orioledb-rs/src/tableam/descr.rs` — table descriptor (2745 C / 2736 Rust)
- [ ] `orioledb-rs/src/tableam/handler.rs` — table access method handler (2759 C / 2743 Rust)
- [ ] `orioledb-rs/src/tableam/operations.rs` — table operations (3064 C / 3061 Rust)
- [ ] `orioledb-rs/src/tableam/func.rs` — tableam functions (1811 C / 1801 Rust)

---

## Phase 6 — Heavy modules (3000+ lines)

**Strategy:** These are the biggest, most complex modules. They depend on
lower phases being complete. Tackle these last.

### transam — Oxid (transaction IDs)

**Module:** `orioledb-rs/src/transam/oxid.rs`  **C source:** `src/transam/oxid.c`
**Size:** 2086 C / 2081 Rust lines  **Depends on:** `types`

- [ ] Oxid allocator (shared memory)
- [ ] `oxid_alloc()`, `oxid_get_csn()`
- [ ] `xid_is_finished()`, `xid_is_finished_for_everybody()`
- [ ] Oxid ↔ VirtualXid mapping
- [ ] Oxid lifecycle (begin/commit/abort)

### transam — Undo log

**Module:** `orioledb-rs/src/transam/undo.rs`  **C source:** `src/transam/undo.c`
**Size:** 3752 C / 3738 Rust lines — **second largest file**
**Depends on:** `types`, `oxid`, `utils::page_pool`, `utils::seq_buf`

- [ ] Undo log write / read
- [ ] `undo_alloc()`, `undo_reserve()`, `undo_complete()`, `undo_retain()`
- [ ] `undo_get_image()`, `undo_get_page_level_image()`
- [ ] Undo circular buffer management
- [ ] Undo per-backend data (`ODBProcData` management)

### S3 large modules

- [ ] `orioledb-rs/src/s3/requests.rs` — S3 request handling (710 C / 704 Rust)
- [ ] `orioledb-rs/src/s3/worker.rs` — S3 background worker (1013 C / 1005 Rust)
- [ ] `orioledb-rs/src/s3/headers.rs` — S3 HTTP headers (1323 C / 1316 Rust)
- [ ] `orioledb-rs/src/s3/checkpoint.rs` — S3 checkpoint integration (906 C / 876 Rust)

### Workers

- [ ] `orioledb-rs/src/workers/bgwriter.rs` — background writer (232 C / 227 Rust)
- [ ] `orioledb-rs/src/workers/interrupt.rs` — interrupt handling (46 C / 42 Rust)

### Catalog large modules

- [ ] `orioledb-rs/src/catalog/free_extents.rs` — free extent tracking (742 C / 738 Rust)
- [ ] `orioledb-rs/src/catalog/sys_trees.rs` — system tree definitions (1280 C / 1274 Rust)
- [ ] `orioledb-rs/src/catalog/o_proc_cache.rs` — procedure cache (2154 C / 2144 Rust)
- [ ] `orioledb-rs/src/catalog/o_indices.rs` — index definitions (1993 C / 1987 Rust)
- [ ] `orioledb-rs/src/catalog/indices.rs` — index management (2339 C / 2333 Rust)
- [ ] `orioledb-rs/src/catalog/o_tables.rs` — table metadata (2461 C / 2456 Rust)
- [ ] `orioledb-rs/src/catalog/o_sys_cache.rs` — system cache (2765 C / 2757 Rust)
- [ ] `orioledb-rs/src/catalog/ddl.rs` — DDL hooks (5210 C / 5201 Rust)

### Rewind

- [ ] `orioledb-rs/src/rewind/rewind.rs` — rewind coordinator (2078 C / 2069 Rust)

### Recovery

- [ ] `orioledb-rs/src/recovery/wal_reader.rs` — WAL reader (655 C / 653 Rust)
- [ ] `orioledb-rs/src/recovery/wal.rs` — WAL record parsing (969 C / 965 Rust)
- [ ] `orioledb-rs/src/recovery/logical.rs` — logical replication (1384 C / 1380 Rust)
- [ ] `orioledb-rs/src/recovery/worker.rs` — recovery worker (1161 C / 1157 Rust)
- [ ] `orioledb-rs/src/recovery/recovery.rs` — recovery coordinator (5227 C / 5217 Rust)

### Checkpoint

- [ ] `orioledb-rs/src/checkpoint/checkpoint.rs` — checkpoint coordinator (5983 C / 5967 Rust)
  **LARGEST FILE IN THE PROJECT**

### Utils

- [ ] `orioledb-rs/src/utils/planner.rs` — planner integration (1397 C / 1393 Rust)

---

## Phase 7 — Integration & cleanup

- [ ] `orioledb-rs/src/lib.rs` — module declarations, GUC registration, shmem, hooks
- [ ] Remove all `extern "C"` wrappers and C shims (except pgrx FFI boundaries)
- [ ] Eliminate remaining `unsafe` blocks (keep only where absolutely required)
- [ ] Fix all Rust Analyzer warnings (non_snake_case, unused imports, etc.)
- [ ] Integration tests
- [ ] Regression test suite
- [ ] Documentation (module-level docs, function docs)
- [ ] Code review

---

# Dependency graph (visual)

```
types.rs (Phase 0)
    │
    ├── Phase 1: interrupt, indexam/handler, mod.rs stubs
    │
    ├── Phase 2: compress, control, s3/archive/checksum/control/queue
    │             catalog caches (tablespace, amproc, operator, opclass, etc.)
    │
    ├── Phase 3: page_state → page_contents → page_chunks → btree(init)
    │             utils: ucm, page_pool, seq_buf, o_buffers, stopevent
    │
    ├── Phase 4: io (L5) ◄── everything in Phase 3
    │            find (L6), insert (L7), split (L7), merge (L7) ◄── io, page_chunks
    │            undo (L8), modify (L8), iterator (L8) ◄── find, undo
    │
    ├── Phase 5: scan, fastpath, build, check, print ◄── Phase 4
    │            tuple (format, toast, sort, slot)
    │            tableam (key_range, key_bitmap, tree, scan, index_scan, bitmap_scan,
    │                    vacuum, descr, handler, operations, func)
    │
    ├── Phase 6: transam (oxid, undo) ◄── types, utils
    │            catalog (sys_trees, free_extents, caches, o_indices, o_tables,
    │                    o_sys_cache, indices, ddl) ◄── types, transam
    │            recovery (wal_reader, wal, logical, worker, recovery) ◄── transam, btree
    │            checkpoint ◄── btree::io, catalog
    │            rewind ◄── recovery, catalog
    │            s3 (requests, worker, headers, checkpoint) ◄── types
    │            workers (bgwriter, interrupt) ◄── types, btree
    │            utils (planner)
    │
    └── Phase 7: lib.rs ◄── all modules + cleanup
```

---

# File-by-File Porting Process

For every file:

1. Read the corresponding C implementation.
2. Understand the algorithm completely.
3. Implement it in Rust.
4. Remove any shim or wrapper.
5. Remove unnecessary extern declarations.
6. Replace C memory management with Rust ownership.
7. Replace macros with Rust equivalents.
8. Replace structs with idiomatic Rust structs.
9. Replace enums with Rust enums.
10. Replace linked lists, arrays, and hash structures with appropriate Rust
    collections where doing so preserves semantics.
11. Keep behavior identical.

Do not skip files. Do not partially port files. Finish each file before
proceeding.

---

# Working Strategy

Only work on one file at a time.

For each file:
1. Read C implementation.
2. Understand algorithm.
3. Rewrite in Rust.
4. Compile (once pgrx toolchain is available).
5. Fix warnings.
6. Reduce unsafe.
7. Verify behavior.
8. Move to the next file.

Never perform mass automated conversions without understanding the original implementation.

---

# Quality Expectations

Every completed file should:
* compile
* avoid unnecessary unsafe
* use idiomatic Rust
* include proper Rust documentation
* have no placeholder implementations
* have no TODO stubs
* have no "call into C" implementations
* preserve functionality

---

# Success Criteria

The project is complete only when:
* there are no C implementation shims
* no placeholder implementations remain
* all target modules are fully rewritten in Rust
* `extern "C"` exists only where required for PostgreSQL integration or
  unavoidable FFI boundaries
* Rust Analyzer warnings are eliminated
* unsafe code is minimized and encapsulated
* the Rust implementation faithfully reproduces the original OrioleDB behavior
  without depending on the C implementation

---

# Notes

- Compilation via `cargo check` is not available without the pgrx/PostgreSQL
  toolchain (`$PGRX_HOME` + `pg_config`). Verification is done by: (1) reading
  the real pgrx/zstd API from the cargo registry cache, (2) brace/paren balance,
  (3) absence of C syntax, and (4) faithful algorithm reproduction.
- The C code is the **specification** — never create wrappers around C code.
- `extern "C"` exists only for PostgreSQL FFI boundaries (pgrx integration).