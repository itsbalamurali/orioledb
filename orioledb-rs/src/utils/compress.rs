//! Compression functions for BTree pages.
//!
//! Thin wrapper around [zstd](https://github.com/facebook/zstd) used to
//! compress and decompress OrioleDB pages on their way to and from disk. A
//! single compression and decompression context is reused across the whole
//! process, matching the original C implementation.

use pgrx::pg_sys;
use zstd_safe;

/// Compression level type, mirroring the C `OCompress` typedef (an `int`).
pub type OCompress = i32;

/// Reusable zstd compression context.
static mut ZSTD_CCTX: Option<zstd_safe::CCtx<'static>> = None;

/// Reusable zstd decompression context.
static mut ZSTD_DCTX: Option<zstd_safe::DCtx<'static>> = None;

/// Reusable destination buffer for compression.
static mut ZSTD_DST: Option<Vec<u8>> = None;

/// Initializes the compression contexts.
///
/// Allocates the compression context, the decompression context, and a reusable
/// destination buffer sized to the worst-case compressed size of an OrioleDB
/// page. Called once during extension startup.
pub fn o_compress_init() {
    unsafe {
        ZSTD_CCTX = Some(zstd_safe::CCtx::create());
        ZSTD_DCTX = Some(zstd_safe::DCtx::create());
        let dst_size = zstd_safe::compress_bound(crate::ORIOLEDB_BLCKSZ);
        ZSTD_DST = Some(vec![0u8; dst_size]);
    }
}

/// Compresses a BTree page using zstd.
///
/// `page` must be exactly one OrioleDB page. The result is written into the
/// shared destination buffer and `(buffer, written_size)` is returned.
pub fn o_compress_page(page: &[u8], lvl: OCompress) -> (&'static [u8], usize) {
    let dst = unsafe { ZSTD_DST.as_mut().expect("o_compress_init not called") };
    let cctx = unsafe { ZSTD_CCTX.as_mut().expect("o_compress_init not called") };

    let written = cctx
        .compress(dst, page, lvl)
        .unwrap_or_else(|e| pg_fatal_compress(e));

    (dst, written)
}

/// Decompresses a BTree page using zstd.
///
/// `src` (of length `size`) is decompressed into `page`, which must be exactly
/// one OrioleDB page. Panics on a zstd error or a size mismatch.
pub fn o_decompress_page(src: &[u8], page: &mut [u8]) {
    let dctx = unsafe { ZSTD_DCTX.as_mut().expect("o_compress_init not called") };

    let written = dctx
        .decompress(page, src)
        .unwrap_or_else(|e| pg_fatal_decompress(e));

    assert_eq!(
        written,
        crate::ORIOLEDB_BLCKSZ,
        "decompressed size does not match OrioleDB page size"
    );
}

/// Returns the maximum supported OrioleDB compression level.
pub fn o_compress_max_lvl() -> OCompress {
    zstd_safe::max_c_level()
}

/// Validates a compression level against the supported range, erroring out
/// (via PostgreSQL `ERROR`) if it is out of bounds.
pub fn validate_compress(compress: OCompress, prefix: &str) {
    let max_compress = o_compress_max_lvl();

    if compress < -1 || compress > max_compress {
        pgrx::error!(
            "{} compression level must be between {} and {}",
            prefix,
            -1,
            max_compress
        );
    }
}

#[cold]
#[inline(never)]
fn pg_fatal_compress(e: zstd_safe::ErrorCode) -> ! {
    pgrx::error!(
        "Unable to compress page, reason: {}",
        zstd_safe::get_error_name(e)
    );
}

#[cold]
#[inline(never)]
fn pg_fatal_decompress(e: zstd_safe::ErrorCode) -> ! {
    pgrx::error!(
        "Unable to decompress page, reason: {}",
        zstd_safe::get_error_name(e)
    );
}
