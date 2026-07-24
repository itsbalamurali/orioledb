//! Routines to work with checkpoint control file.
//!
//! This module mirrors `src/checkpoint/control.c` and `include/checkpoint/control.h`.
//! It provides functions for reading, writing, and validating the OrioleDB checkpoint
//! control file that stores cluster-wide metadata including checkpoint numbers,
//! undo locations, transaction state, and version information.

use crate::btree::types::{CommitSeqNo, OXid, UndoLocation};
use pgrx::{ereport, pg_sys, PgLogLevel, PgSqlErrorCode};
use std::ffi::CString;
use std::sync::LazyLock;

// ===========================================================================
// Type definitions
// ===========================================================================

/// PostgreSQL's 32-bit CRC type (from `postgres_ext.h`).
///
/// Mirrors `pg_crc32c` used throughout the PostgreSQL core for CRC32C computation.
pub type pg_crc32c = u32;

/// WAL record pointer type (equivalent to PostgreSQL's `XLogRecPtr`).
///
/// A 64-bit position within the WAL stream.
pub type XLogRecPtr = u64;

/// Undo information recorded at a checkpoint.
///
/// Mirrors `CheckpointUndoInfo` from `include/checkpoint/control.h`.
#[repr(C)]
#[derive(Clone, Copy, Debug, Default)]
pub struct CheckpointUndoInfo {
    /// Last undo location processed.
    pub last_undo_location: UndoLocation,
    /// Undo location to retain for recovery (start).
    pub checkpoint_retain_start_location: UndoLocation,
    /// Undo location to retain for recovery (end).
    pub checkpoint_retain_end_location: UndoLocation,
}

/// Checkpoint control data structure.
///
/// Stored in the checkpoint control file and tracks cluster-wide state
/// across checkpoints. The CRC field at the end covers all preceding fields.
///
/// Mirrors `CheckpointControl` from `include/checkpoint/control.h`.
#[repr(C)]
#[derive(Clone, Copy, Debug, Default)]
pub struct CheckpointControl {
    /// Unique identifier for the control file.
    pub control_identifier: u64,
    /// Last checkpoint number.
    pub last_checkpoint_number: u32,
    /// Control file format version.
    pub control_file_version: u32,
    /// Last commit sequence number.
    pub last_csn: CommitSeqNo,
    /// Last Oxid (transaction identifier).
    pub last_oxid: OXid,
    /// Last undo location.
    pub last_undo_location: UndoLocation,
    /// WAL position for TOAST consistency.
    pub toast_consistent_ptr: XLogRecPtr,
    /// WAL position for replay start.
    pub replay_start_ptr: XLogRecPtr,
    /// WAL position for system trees start.
    pub sys_trees_start_ptr: XLogRecPtr,
    /// Memory-mapped data length.
    pub mmap_data_length: u64,
    /// Undo information per undo log.
    pub undo_info: [CheckpointUndoInfo; NUM_CHECKPOINTABLE_UNDO_LOGS],
    /// Undo location to retain start.
    pub checkpoint_retain_start_location: UndoLocation,
    /// Undo location to retain end.
    pub checkpoint_retain_end_location: UndoLocation,
    /// Oxid to retain as xmin.
    pub checkpoint_retain_xmin: OXid,
    /// Oxid to retain as xmax.
    pub checkpoint_retain_xmax: OXid,
    /// Binary version.
    pub binary_version: u32,
    /// S3 mode enabled flag.
    pub s3_mode: bool,
    /// CRC32C checksum of all fields above.
    pub crc: pg_crc32c,
}

// ===========================================================================
// Constants
// ===========================================================================

/// Number of checkpointable undo logs.
pub const NUM_CHECKPOINTABLE_UNDO_LOGS: usize = 2;

/// Physical size of the checkpoint control file in bytes (8 KB).
///
/// Significantly larger than `sizeof(CheckpointControl)`. Keeps the file
/// size constant across format changes so that an incompatible file
/// produces a version error rather than a read error.
pub const CHECKPOINT_CONTROL_FILE_SIZE: usize = 8192;

/// Checkpoint control file format version.
///
/// Bump this whenever `CheckpointControl` changes layout. When bumping,
/// add an on-the-fly conversion routine to `check_checkpoint_control`.
pub const ORIOLEDB_CHECKPOINT_CONTROL_VERSION: u32 = 1;

/// Binary version of OrioleDB.
///
/// Clusters with different binary versions are binary-incompatible.
pub const ORIOLEDB_BINARY_VERSION: u32 = 9;

// ===========================================================================
// Control file path
// ===========================================================================

/// Returns the filesystem path to the checkpoint control file.
///
/// Constructs the path as `<DataDir>/orioledb/control`.
fn control_path() -> String {
    let data_dir = unsafe {
        std::ffi::CStr::from_ptr(pg_sys::DataDir)
            .to_string_lossy()
            .into_owned()
    };
    format!("{}/orioledb/control", data_dir)
}

// ===========================================================================
// CRC32C implementation (PostgreSQL Castagnuli variant)
// ===========================================================================

/// CRC32C lookup table, initialized on first use.
///
/// Uses the Castagnuli polynomial (0x1EDC6F41 reflected = 0x82F63B78).
///
/// PostgreSQL's CRC32C macros (`INIT_CRC32C`, `COMP_CRC32C`, `FIN_CRC32C`)
/// are reproduced here. The `COMP` function mirrors `crc32c_inline` from
/// PostgreSQL's `src/common/crc32c.c`, which XORs input with 0xFFFFFFFF
/// before processing and returns the bitwise NOT of the result — enabling
/// incremental use across multiple `COMP` calls.
static CRC32C_TABLE: LazyLock<[u32; 256]> = LazyLock::new(|| {
    let mut table = [0u32; 256];
    const POLY: u32 = 0x82F63B78; // Castagnuli, reflected
    for i in 0..256u32 {
        let mut crc = i;
        for _ in 0..8 {
            if crc & 1 != 0 {
                crc = (crc >> 1) ^ POLY;
            } else {
                crc >>= 1;
            }
        }
        table[i as usize] = crc;
    }
    table
});

/// Initialize CRC32C state — mirrors `INIT_CRC32C(crc)`.
///
/// Sets the CRC to zero before the first `COMP_CRC32C` call.
#[inline]
pub fn init_crc32c() -> pg_crc32c {
    0
}

/// Update CRC32C with data — mirrors `COMP_CRC32C(crc, data, len)`.
///
/// Equivalent to PostgreSQL's `crc32c_inline`: XORs the input CRC with
/// 0xFFFFFFFF before processing, then returns the bitwise NOT of the
/// result. This design enables incremental use across multiple calls
/// (e.g. CRC over disjoint struct fields).
#[inline]
pub fn comp_crc32c(crc: pg_crc32c, data: &[u8]) -> pg_crc32c {
    let mut result: u32 = !crc;
    for &byte in data {
        result = CRC32C_TABLE[((result ^ byte as u32) & 0xFF) as usize] ^ (result >> 8);
    }
    !result
}

/// Finalize CRC32C — mirrors `FIN_CRC32C(crc)`.
///
/// Applies the final XOR to produce the checksum. The complete sequence
/// `init + comp + fin` yields a CRC32C over the given data.
#[inline]
pub fn fin_crc32c(crc: pg_crc32c) -> pg_crc32c {
    !crc
}

// ===========================================================================
// Public API
// ===========================================================================

/// Read checkpoint control file data from disk.
///
/// Opens the control file, reads it into `control`, and validates it via
/// `check_checkpoint_control`.
///
/// Returns `true` on success. Returns `false` if the file does not exist
/// or is empty — this is treated as "no checkpoint has been taken yet".
pub fn get_checkpoint_control_data(control: &mut CheckpointControl) -> bool {
    let path = control_path();
    let c_path = CString::new(path.clone()).expect("control path contains null bytes");

    // Open the file read-only
    let fd = unsafe { libc::open(c_path.as_ptr(), libc::O_RDONLY | libc::O_CLOEXEC) };
    if fd < 0 {
        let err = std::io::Error::last_os_error();
        if err.kind() == std::io::ErrorKind::NotFound {
            return false;
        }
        ereport!(
            PgLogLevel::ERROR,
            PgSqlErrorCode::ERRCODE_IO_ERROR,
            "could not open file \"{}\": {}",
            path,
            err
        );
    }

    // Read directly into the control struct (matches C: `read(fd, control, sizeof(...))`)
    let expected = std::mem::size_of::<CheckpointControl>() as isize;
    let n = unsafe {
        libc::read(
            fd,
            std::ptr::addr_of_mut!(*control) as *mut libc::c_void,
            expected as usize,
        )
    };
    unsafe {
        libc::close(fd);
    }

    // Empty file → treat as no checkpoint taken
    if n == 0 {
        return false;
    }
    // Wrong size → error (file is corrupted or incompatible)
    if n != expected {
        let err = std::io::Error::last_os_error();
        ereport!(
            PgLogLevel::ERROR,
            PgSqlErrorCode::ERRCODE_IO_ERROR,
            "could not read data from control file \"{}\": {}",
            path,
            err
        );
    }

    // Validate CRC and version
    check_checkpoint_control(control);

    true
}

/// Validate checkpoint control data.
///
/// Checks the CRC32C checksum, then verifies the control file version,
/// binary version, and S3 mode are compatible with the current server.
///
/// All version mismatches raise a `FATAL` error (terminates the server).
/// CRC mismatches raise an `ERROR` (recoverable).
pub fn check_checkpoint_control(control: &CheckpointControl) {
    // ---- CRC check ----
    let crc_offset = std::mem::offset_of!(CheckpointControl, crc);
    let data = unsafe {
        std::slice::from_raw_parts(control as *const CheckpointControl as *const u8, crc_offset)
    };

    let mut crc = init_crc32c();
    crc = comp_crc32c(crc, data);
    crc = fin_crc32c(crc);

    if crc != control.crc {
        ereport!(
            PgLogLevel::ERROR,
            PgSqlErrorCode::ERRCODE_DATA_CORRUPTED,
            "Wrong CRC in control file"
        );
    }

    // ---- Control file version check ----
    if control.control_file_version != ORIOLEDB_CHECKPOINT_CONTROL_VERSION {
        ereport!(
            PgLogLevel::FATAL,
            PgSqlErrorCode::ERRCODE_INVALID_PARAMETER_VALUE,
            "checkpoint files are incompatible with server: OrioleDB checkpoint control file was initialized with version {}, but the currently required version is {}.",
            control.control_file_version,
            ORIOLEDB_CHECKPOINT_CONTROL_VERSION,
        );
    }

    // ---- Binary version check ----
    if control.binary_version != ORIOLEDB_BINARY_VERSION {
        ereport!(
            PgLogLevel::FATAL,
            PgSqlErrorCode::ERRCODE_INVALID_PARAMETER_VALUE,
            "database files are incompatible with server: OrioleDB was initialized with binary version {}, but the extension is compiled with binary version {}. {}",
            control.binary_version,
            ORIOLEDB_BINARY_VERSION,
            "It looks like you need to initdb.",
        );
    }

    // ---- S3 mode check ----
    // ORIOLEDB_S3_MODE is a static in lib.rs; access with pub(crate) visibility.
    let current_s3_mode = unsafe { crate::ORIOLEDB_S3_MODE };
    if control.s3_mode != current_s3_mode {
        ereport!(
            PgLogLevel::FATAL,
            PgSqlErrorCode::ERRCODE_INVALID_PARAMETER_VALUE,
            "database files are incompatible with server: OrioleDB was initialized with S3 mode {}, but the extension is configured with S3 mode {}.",
            if control.s3_mode { "on" } else { "off" },
            if current_s3_mode { "on" } else { "off" },
        );
    }
}

/// Write checkpoint control file to disk (and sync).
///
/// Computes the CRC32C over all fields (except the CRC itself), zero-pads
/// the buffer to `CHECKPOINT_CONTROL_FILE_SIZE` (8192 bytes), writes the
/// entire buffer, and syncs the file to durable storage.
pub fn write_checkpoint_control(control: &mut CheckpointControl) {
    // ---- Compute and store CRC ----
    let crc_offset = std::mem::offset_of!(CheckpointControl, crc);
    let data = unsafe {
        std::slice::from_raw_parts_mut(control as *mut CheckpointControl as *mut u8, crc_offset)
    };

    let mut crc = init_crc32c();
    crc = comp_crc32c(crc, data);
    crc = fin_crc32c(crc);
    control.crc = crc;

    let path = control_path();
    let c_path = CString::new(path.clone()).expect("control path contains null bytes");

    // ---- Build zero-padded buffer ----
    let mut buffer = vec![0u8; CHECKPOINT_CONTROL_FILE_SIZE];
    unsafe {
        std::ptr::copy_nonoverlapping(
            control as *const CheckpointControl as *const u8,
            buffer.as_mut_ptr(),
            std::mem::size_of::<CheckpointControl>(),
        );
    }

    // ---- Open file (O_RDWR | O_CREAT, matching C) ----
    let fd = unsafe {
        libc::open(
            c_path.as_ptr(),
            libc::O_RDWR | libc::O_CREAT | libc::O_CLOEXEC,
            0o600,
        )
    };
    if fd < 0 {
        let err = std::io::Error::last_os_error();
        ereport!(
            PgLogLevel::FATAL,
            PgSqlErrorCode::ERRCODE_IO_ERROR,
            "could not open checkpoint control file {}: {}",
            path,
            err
        );
    }

    // ---- Write entire buffer ----
    let expected = CHECKPOINT_CONTROL_FILE_SIZE as isize;
    let mut written: isize = 0;
    while written < expected {
        let remaining = expected - written;
        let n = unsafe {
            libc::write(
                fd,
                buffer[written as usize..].as_ptr() as *const libc::c_void,
                remaining as usize,
            )
        };
        if n <= 0 {
            let err = std::io::Error::last_os_error();
            unsafe {
                libc::close(fd);
            }
            ereport!(
                PgLogLevel::FATAL,
                PgSqlErrorCode::ERRCODE_IO_ERROR,
                "could not write checkpoint control to file {}: {}",
                path,
                err
            );
        }
        written += n;
    }

    // ---- Sync to disk ----
    let rc = unsafe { libc::fsync(fd) };
    if rc != 0 {
        let err = std::io::Error::last_os_error();
        unsafe {
            libc::close(fd);
        }
        ereport!(
            PgLogLevel::FATAL,
            PgSqlErrorCode::ERRCODE_IO_ERROR,
            "could not sync checkpoint control to file {}: {}",
            path,
            err
        );
    }

    unsafe {
        libc::close(fd);
    }
}
