// B-tree I/O — reading, writing, and managing data files.
//
// Ported from `include/btree/io.h` and `src/btree/io.c`.
//
// Copyright (c) 2021-2026, Oriole DB Inc.
// Copyright (c) 2025-2026, Supabase Inc.

use crate::utils::page_pool::OInMemoryBlkno;
use crate::utils::seq_buf::{FileExtent, OIndexKey, ORelOids};

extern "C" {
    pub fn btree_io_shmem_needs() -> usize;
    pub fn btree_io_shmem_init(buf: *mut u8, found: bool);
    pub fn btree_io_error_cleanup();
    pub fn request_btree_io_lwlocks();

    /// Claim an I/O slot for the given block and offset number.
    pub fn assign_io_num(blkno: OInMemoryBlkno, offnum: u16) -> i32;

    /// Evict (or checkpoint-flush) a single page.
    pub fn walk_page(blkno: OInMemoryBlkno, evict: bool) -> i32;

    /// Walk all dirty tree pages up to `max_level`, optionally evicting them.
    pub fn write_tree_pages(desc: *mut std::ffi::c_void, max_level: i32, evict: bool);

    /// Release the I/O slot claimed by `assign_io_num`.
    pub fn unlock_io(ionum: i32);

    /// Wait until all I/O on slot `ionum` is done.
    pub fn wait_for_io_completion(ionum: i32);

    /// Remove data files for `key` from the filesystem.
    pub fn cleanup_btree_files(key: OIndexKey, fsync: bool) -> bool;

    /// Call `fsync` on all data files for `key`.
    pub fn fsync_btree_files(key: OIndexKey) -> bool;

    pub fn OFileRead(
        file: i32,
        buffer: *mut i8,
        amount: i32,
        offset: i64,
        wait_event_info: u32,
    ) -> i32;

    pub fn OFileWrite(
        file: i32,
        buffer: *mut i8,
        amount: i32,
        offset: i64,
        wait_event_info: u32,
    ) -> i32;

    /// Initialise the storage-manager handle for `descr`.
    pub fn btree_init_smgr(descr: *mut std::ffi::c_void);
    pub fn btree_open_smgr(descr: *mut std::ffi::c_void);
    pub fn btree_close_smgr(descr: *mut std::ffi::c_void);

    /// Return the data-file path for `key`, segment `segno`, checkpoint `chkp_num`.
    pub fn btree_filename(key: OIndexKey, segno: i32, chkp_num: u32) -> *mut i8;

    pub fn btree_smgr_filename(
        desc: *mut std::ffi::c_void,
        offset: i64,
        chkp_num: u32,
    ) -> *mut i8;

    pub fn btree_smgr_writeback(desc: *mut std::ffi::c_void, chkp_num: u32, offset: i64, length: i64);
    pub fn btree_smgr_sync(desc: *mut std::ffi::c_void, chkp_num: u32, length: i64);
    pub fn btree_smgr_punch_hole(
        desc: *mut std::ffi::c_void,
        chkp_num: u32,
        offset: i64,
        length: i64,
    );
    pub fn punch_fd_hole(fd: i32, offset: i64, length: i64, wait_event: u32);

    pub fn init_btree_io_lwlocks();

    /// Read a page from disk into `img`.
    pub fn read_page_from_disk(
        desc: *mut std::ffi::c_void,
        img: *mut u8,
        downlink: u64,
        extent: *mut FileExtent,
    ) -> bool;

    /// Load a page from S3 or disk into the buffer pool.
    pub fn load_page(context: *mut std::ffi::c_void);

    /// Write a page (evict or checkpoint).
    pub fn perform_page_io(
        desc: *mut std::ffi::c_void,
        blkno: OInMemoryBlkno,
        checkpoint_number: u32,
        flags: u32,
    ) -> u64;

    /// Write a page in autonomous mode (outside a checkpoint).
    pub fn perform_page_io_autonomous(
        desc: *mut std::ffi::c_void,
        chkp_num: u32,
        img: *mut u8,
        downlink: u64,
    ) -> u64;

    /// Write a pre-built index page during `CREATE INDEX`.
    pub fn perform_page_io_build(
        desc: *mut std::ffi::c_void,
        img: *mut u8,
        downlink: *mut u64,
    ) -> u64;

    /// Look up a B-tree descriptor by OID triple and index type.
    pub fn index_oids_get_btree_descr(oids: ORelOids, index_type: i32)
        -> *mut std::ffi::c_void;

    /// Try to punch holes in the data file to reclaim free space.
    pub fn try_to_punch_holes(desc: *mut std::ffi::c_void);
}
