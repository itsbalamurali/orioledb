//! Compression functions for BTree pages.
//!
//! Thin wrapper around [zstd](https://github.com/facebook/zstd) used to
//! compress and decompress OrioleDB pages on their way to and from disk. A
//! single compression and decompression context is reused across the whole
//! process, matching the original C implementation.

use std::sync::LazyLock;
use zstd_safe;

/// Compression level type, mirroring the C `OCompress` typedef (an `int`).
pub type OCompress = i32;

/// Reusable zstd compression context.
///
/// Initialized on first use via `LazyLock`, matching the C implementation's
/// single global `ZSTD_CCtx` created during `o_compress_init()`.
static ZSTD_CCTX: LazyLock<zstd_safe::CCtx<'static>> = LazyLock::new(|| zstd_safe::CCtx::create());

/// Reusable zstd decompression context.
static ZSTD_DCTX: LazyLock<zstd_safe::DCtx<'static>> = LazyLock::new(|| zstd_safe::DCtx::create());

/// Reusable destination buffer for compression, sized to worst case.
static ZSTD_DST: LazyLock<Vec<u8>> =
    LazyLock::new(|| vec![0u8; zstd_safe::compress_bound(crate::ORIOLEDB_BLCKSZ)]);

/// Initializes the compression contexts.
///
/// This function is a no-op in the Rust implementation because contexts are
/// initialized lazily on first use via `LazyLock`. It exists for API
/// compatibility with the C signature.
pub fn o_compress_init() {
    // Contexts are initialized lazily; ensure they exist.
    let _ = ZSTD_CCTX.get();
    let _ = ZSTD_DCTX.get();
    let _ = ZSTD_DST.get();
}

/// Compresses a BTree page using zstd.
///
/// `page` must be exactly one OrioleDB page. The result is written into the
/// shared destination buffer and `(buffer, written_size)` is returned.
pub fn o_compress_page(page: &[u8], lvl: OCompress) -> (&[u8], usize) {
    let dst = ZSTD_DST.get();
    let cctx = ZSTD_CCTX.get();

    let written = cctx
        .compress(dst, page, lvl)
        .unwrap_or_else(|e| pg_fatal_compress(e));

    (&dst[..written], written)
}

/// Decompresses a BTree page using zstd.
///
/// `src` (of length `size`) is decompressed into `page`, which must be exactly
/// one OrioleDB page. Panics on a zstd error or a size mismatch.
pub fn o_decompress_page(src: &[u8], page: &mut [u8]) {
    let dctx = ZSTD_DCTX.get();

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
