/*-------------------------------------------------------------------------
 *
 * compress.rs
 *		Compression functions for BTree pages. Wrapper for libzstd.
 *
 * Copyright (c) 2021-2026, Oriole DB Inc.
 * Copyright (c) 2025-2026, Supabase Inc.
 *
 * IDENTIFICATION
 *	  contrib/orioledb/src/utils/compress.rs
 *
 *-------------------------------------------------------------------------
 */

use std::cell::RefCell;
use std::ffi::{c_char, c_int, c_void, CStr};
use std::slice;

pub const ORIOLEDB_BLCKSZ: usize = 8192;

thread_local! {
    static C_CTX: RefCell<Option<zstd_safe::CCtx<'static>>> = RefCell::new(None);
    static D_CTX: RefCell<Option<zstd_safe::DCtx<'static>>> = RefCell::new(None);
    static DST_BUFFER: RefCell<Vec<u8>> = RefCell::new(Vec::new());
}

/*
 * Initializes compression context.
 */
#[no_mangle]
pub extern "C" fn o_compress_init() {
    C_CTX.with(|ctx| {
        let mut slot = ctx.borrow_mut();
        if slot.is_none() {
            *slot = Some(zstd_safe::CCtx::create());
        }
    });

    D_CTX.with(|ctx| {
        let mut slot = ctx.borrow_mut();
        if slot.is_none() {
            *slot = Some(zstd_safe::DCtx::create());
        }
    });

    DST_BUFFER.with(|buf| {
        let mut slot = buf.borrow_mut();
        if slot.is_empty() {
            let bound = zstd_safe::compress_bound(ORIOLEDB_BLCKSZ);
            *slot = vec![0u8; bound];
        }
    });
}

/*
 * Compresses a BTree page.
 */
#[no_mangle]
pub unsafe extern "C" fn o_compress_page(
    page: *mut c_void,
    size: *mut usize,
    lvl: c_int,
) -> *mut c_void {
    let src_slice = slice::from_raw_parts(page as *const u8, ORIOLEDB_BLCKSZ);

    let result = C_CTX.with(|ctx| {
        DST_BUFFER.with(|buf| {
            let mut ctx_borrow = ctx.borrow_mut();
            let mut buf_borrow = buf.borrow_mut();

            let cctx = ctx_borrow.as_mut().expect("o_compress_init not called");
            let dst_slice = buf_borrow.as_mut_slice();

            cctx.compress(dst_slice, src_slice, lvl)
        })
    });

    match result {
        Ok(compressed_size) => {
            if !size.is_null() {
                *size = compressed_size;
            }
            DST_BUFFER.with(|buf| {
                buf.borrow().as_ptr() as *mut c_void
            })
        }
        Err(err) => {
            panic!("Unable to compress page, reason: {:?}", err);
        }
    }
}

/*
 * Decompresses a BTree page.
 */
#[no_mangle]
pub unsafe extern "C" fn o_decompress_page(
    src: *const c_void,
    size: usize,
    page: *mut c_void,
) {
    let src_slice = slice::from_raw_parts(src as *const u8, size);
    let dst_slice = slice::from_raw_parts_mut(page as *mut u8, ORIOLEDB_BLCKSZ);

    let result = D_CTX.with(|ctx| {
        let mut ctx_borrow = ctx.borrow_mut();
        let dctx = ctx_borrow.as_mut().expect("o_compress_init not called");

        dctx.decompress(dst_slice, src_slice)
    });

    match result {
        Ok(decompressed_size) => {
            assert_eq!(
                decompressed_size, ORIOLEDB_BLCKSZ,
                "Decompressed size must match ORIOLEDB_BLCKSZ"
            );
        }
        Err(err) => {
            panic!("Unable to decompress page, reason: {:?}", err);
        }
    }
}

/*
 * Returns max orioledb compression level.
 */
#[no_mangle]
pub extern "C" fn o_compress_max_lvl() -> c_int {
    zstd_safe::max_c_level() as c_int
}

/*
 * Validates compression level.
 */
#[no_mangle]
pub unsafe extern "C" fn validate_compress(compress: c_int, prefix: *const c_char) {
    let max_compress = o_compress_max_lvl();
    if compress < -1 || compress > max_compress {
        let prefix_str = if prefix.is_null() {
            "Unknown"
        } else {
            CStr::from_ptr(prefix).to_str().unwrap_or("Invalid UTF-8")
        };
        panic!(
            "{} compression level must be between -1 and {}",
            prefix_str, max_compress
        );
    }
}
