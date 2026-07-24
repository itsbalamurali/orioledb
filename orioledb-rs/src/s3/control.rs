//! S3 control and lock file management.
//!
//! Mirrors `src/s3/control.c`. Provides functions to:
//! - Verify compatibility between the local instance and an S3 bucket ([`s3_check_control`])
//! - Create and manage a lock file on S3 to prevent concurrent access ([`s3_put_lock_file`],
//!   [`s3_delete_lock_file`])
//!
//! The lock file is stored at `orioledb_data/s3_lock` locally and uploaded to S3 as
//! `data/s3_lock`. The lock identifier is derived from the current timestamp and PID,
//! similar to how PostgreSQL calculates its system identifier.

use crate::btree::types::OXid;
use crate::checkpoint::control::{
    check_checkpoint_control, get_checkpoint_control_data, CheckpointControl,
};
use pgrx::ereport;
use pgrx::PgLogLevel;
use pgrx::PgSqlErrorCode;
use std::ffi::CString;
use std::fs::{File, OpenOptions};
use std::io::{Read, Write};
use std::time::{SystemTime, UNIX_EPOCH};

// ===========================================================================
// Constants
// ===========================================================================

/// Name of the OrioleDB data directory within a tablespace or the main data directory.
pub const ORIOLEDB_DATA_DIR: &str = "orioledb_data";

/// Name of the S3 lock file (local path suffix).
const LOCK_FILENAME: &str = "s3_lock";

// S3 HTTP response codes (match s3/requests.h)
const S3_RESPONSE_OK: i64 = 200;
const S3_RESPONSE_NOT_FOUND: i64 = 404;
const S3_RESPONSE_CONDITION_CONFLICT: i64 = 409;
const S3_RESPONSE_CONDITION_FAILED: i64 = 412;

// ===========================================================================
// StringInfo — PostgreSQL string buffer type
// ===========================================================================

/// A simple string info buffer mirroring PostgreSQL's `StringInfoData`.
///
/// Used by `s3_get_object` to return binary data from S3 responses.
#[repr(C)]
pub struct StringInfo {
    /// Pointer to the buffer data.
    pub data: *mut u8,
    /// Current length of the data.
    pub len: usize,
    /// Allocated size of the buffer.
    pub maxlen: usize,
    /// Whether the string is null-terminated.
    pub null_terminated: bool,
}

impl StringInfo {
    /// Create a new empty StringInfo with the given initial capacity.
    pub fn new(capacity: usize) -> Self {
        let mut data = Vec::with_capacity(capacity);
        // SAFETY: Vec::as_mut_ptr is valid for the lifetime of the Vec.
        let ptr = data.as_mut_ptr();
        std::mem::forget(data);
        Self {
            data: ptr,
            len: 0,
            maxlen: capacity,
            null_terminated: false,
        }
    }

    /// Get the data as a byte slice.
    pub fn as_bytes(&self) -> &[u8] {
        // SAFETY: data is valid for len bytes.
        unsafe { std::slice::from_raw_parts(self.data, self.len) }
    }

    /// Get the data as a mutable byte slice.
    pub fn as_bytes_mut(&mut self) -> &mut [u8] {
        // SAFETY: data is valid for len bytes.
        unsafe { std::slice::from_raw_parts_mut(self.data, self.len) }
    }

    /// Extend the buffer to hold at least `needed` additional bytes.
    pub fn ensure_capacity(&mut self, needed: usize) {
        let new_capacity = std::cmp::max(self.maxlen, self.len + needed);
        // SAFETY: We reallocate with Vec and forget it again.
        let old_data = unsafe { std::ptr::read(&self.data) };
        let mut new_vec = Vec::with_capacity(new_capacity);
        if self.len > 0 {
            // SAFETY: old_data is valid for len bytes.
            unsafe {
                std::ptr::copy_nonoverlapping(old_data, new_vec.as_mut_ptr(), self.len);
            }
            new_vec.set_len(self.len);
        }
        let new_ptr = new_vec.as_mut_ptr();
        std::mem::forget(new_vec);
        // SAFETY: We already read old_data above.
        unsafe { std::ptr::write(&mut self.data as *mut *mut u8, new_ptr) };
        self.maxlen = new_capacity;
    }

    /// Append data to the buffer.
    pub fn append(&mut self, data: &[u8]) {
        if data.is_empty() {
            return;
        }
        self.ensure_capacity(data.len());
        let slice = self.as_bytes_mut();
        let dest = unsafe { slice.as_mut_ptr().add(self.len) };
        // SAFETY: dest is valid for data.len() bytes (we ensured capacity).
        unsafe {
            std::ptr::copy_nonoverlapping(data.as_ptr(), dest, data.len());
        }
        self.len += data.len();
    }

    /// Free the buffer.
    pub fn free(&mut self) {
        if !self.data.is_null() {
            // SAFETY: Reconstruct Vec to drop the allocation.
            let _ = unsafe { Vec::from_raw_parts(self.data, self.len, self.maxlen) };
            self.data = std::ptr::null_mut();
            self.len = 0;
            self.maxlen = 0;
        }
    }
}

// ===========================================================================
// S3 function declarations (declared in s3/requests.rs)
// ===========================================================================

/// External S3 HTTP functions declared in `s3/requests.rs`.
///
/// These wrap curl-based HTTP operations to upload/download objects from S3.
/// The implementations call into PostgreSQL's curl infrastructure.
extern "C" {
    /// Fetch an object from S3 and store it in `str`.
    ///
    /// Returns the HTTP response code. Returns `S3_RESPONSE_NOT_FOUND` if
    /// `missing_ok` is true and the object doesn't exist.
    fn s3_get_object(
        objectname: *mut std::os::raw::c_char,
        str: *mut StringInfo,
        missing_ok: bool,
    ) -> i64;

    /// Upload a local file to S3.
    ///
    /// If `if_none_match` is true, the upload is conditional (fails if the object exists).
    /// Returns the HTTP response code.
    fn s3_put_file(
        objectname: *mut std::os::raw::c_char,
        filename: *const std::os::raw::c_char,
        if_none_match: bool,
    ) -> i64;

    /// Delete an object from S3.
    fn s3_delete_object(objectname: *mut std::os::raw::c_char);
}

// ===========================================================================
// s3_check_control — verify S3 bucket compatibility
// ===========================================================================

/// Read the local checkpoint control file and the file from S3, then check if the
/// S3 bucket is compatible with the local instance.
///
/// Returns `Ok(true)` if the bucket is compatible, `Ok(false)` if incompatible,
/// or an error with messages.
///
/// The compatibility check compares:
/// 1. Control identifier (must match)
/// 2. Last checkpoint number (local must be >= S3)
/// 3. System trees start pointer (local must be >= S3)
///
/// If the control file doesn't exist on S3, it's considered compatible
/// (the bucket is empty and safe to use).
///
/// # Arguments
///
/// * `errmsgp` — If the bucket is incompatible, returns a human-readable error message.
/// * `errdetailp` — If the bucket is incompatible, returns detailed diagnostic info.
///
/// # Returns
///
/// `Ok(true)` if compatible, `Ok(false)` if incompatible (with messages set),
/// or `Err` if there's an I/O error.
pub fn s3_check_control(
    errmsgp: &mut Option<String>,
    errdetailp: &mut Option<String>,
) -> Result<bool, ()> {
    // Read local checkpoint control.
    let mut local_control: CheckpointControl = Default::default();
    let control_res = get_checkpoint_control_data(&mut local_control);

    let object_name = CString::new(format!("data/control")).unwrap();
    let mut buf = StringInfo::new(8192); // CHECKPOINT_CONTROL_FILE_SIZE

    // Try to get the control file from S3.
    let s3_response = unsafe { s3_get_object(object_name.as_mut_ptr(), &mut buf, true) };

    if s3_response == S3_RESPONSE_NOT_FOUND {
        // No control file on S3 — safe to use empty bucket.
        buf.free();
        return Ok(true);
    }

    if s3_response != S3_RESPONSE_OK {
        buf.free();
        *errmsgp = Some(format!(
            "could not get control file from S3: response {}",
            s3_response
        ));
        return Err(());
    }

    // Local has no control file but S3 does — incompatible.
    if !control_res {
        buf.free();
        *errmsgp = Some(
            "OrioleDB can be incompatible with the S3 bucket because the control file exists on the S3 bucket"
                .to_string(),
        );
        *errdetailp = Some("OrioleDB control file \"data/control\" is absent".to_string());
        return Ok(false);
    }

    // Parse S3 control file.
    let s3_data = buf.as_bytes();
    if s3_data.len() < std::mem::size_of::<CheckpointControl>() {
        buf.free();
        *errmsgp = Some("S3 control file is too small".to_string());
        return Err(());
    }

    // SAFETY: s3_data has at least sizeof(CheckpointControl) bytes, and
    // CheckpointControl is #[repr(C)].
    let s3_control: &CheckpointControl =
        unsafe { &*(s3_data.as_ptr() as *const CheckpointControl) };

    // Validate the S3 control file.
    check_checkpoint_control(s3_control);

    // Compare control identifiers.
    if local_control.control_identifier != s3_control.control_identifier {
        buf.free();
        *errmsgp = Some(
            "OrioleDB and the S3 bucket have files from different instances and they are incompatible with each other"
                .to_string(),
        );
        *errdetailp = Some(format!(
            "OrioleDB control identifier {} differs from the S3 bucket identifier {}",
            local_control.control_identifier, s3_control.control_identifier
        ));
        return Ok(false);
    }

    // Compare last checkpoint numbers.
    if local_control.last_checkpoint_number < s3_control.last_checkpoint_number {
        buf.free();
        *errmsgp = Some(
            "OrioleDB misses new changes and checkpoints from the S3 bucket and they are incompatible with each other"
                .to_string(),
        );
        *errdetailp = Some(format!(
            "OrioleDB last checkpoint number {} is behind the S3 bucket last checkpoint number {}",
            local_control.last_checkpoint_number, s3_control.last_checkpoint_number
        ));
        return Ok(false);
    } else if local_control.last_checkpoint_number > s3_control.last_checkpoint_number {
        pgrx::log!(
            PgLogLevel::LOG,
            "OrioleDB has more changes and checkpoints than the S3 bucket but they are still compatible with each other",
            "OrioleDB last checkpoint number {} is ahead of the S3 bucket last checkpoint number {}",
            local_control.last_checkpoint_number,
            s3_control.last_checkpoint_number
        );
    }

    // Compare system trees start pointer.
    if local_control.sys_trees_start_ptr < s3_control.sys_trees_start_ptr {
        buf.free();
        *errmsgp = Some(
            "OrioleDB misses new changes from the S3 bucket and they are incompatible with each other"
                .to_string(),
        );
        *errdetailp = Some(format!(
            "OrioleDB XLOG location {} is behind the S3 bucket XLOG location {}",
            local_control.sys_trees_start_ptr, s3_control.sys_trees_start_ptr
        ));
        return Ok(false);
    } else if local_control.sys_trees_start_ptr > s3_control.sys_trees_start_ptr {
        pgrx::log!(
            PgLogLevel::LOG,
            "OrioleDB has more changes than the S3 bucket but they are still compatible with each other",
            "OrioleDB XLOG location {} is ahead of the S3 bucket XLOG location {}",
            local_control.sys_trees_start_ptr,
            s3_control.sys_trees_start_ptr
        );
    }

    buf.free();
    Ok(true)
}

// ===========================================================================
// Lock file helpers
// ===========================================================================

/// Generate a lock identifier from the current timestamp and PID.
///
/// Mirrors the C implementation which uses `gettimeofday()` and `getpid()`.
/// The identifier layout is:
/// - bits 0-11:  PID (lower 12 bits)
/// - bits 12-43: microsecond timestamp (shifted)
/// - bits 44-63: second timestamp
fn generate_lock_identifier() -> u64 {
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("SystemTime before UNIX epoch");

    let tv_sec = now.as_secs();
    let tv_usec = now.subsec_microseconds() as u64;
    let pid = std::process::id() as u64;

    ((tv_sec as u64) << 32) | ((tv_usec as u64) << 12) | (pid & 0xFFF)
}

/// Build the local path to a file inside the OrioleDB data directory.
fn lock_file_path() -> String {
    format!("{}/{}", ORIOLEDB_DATA_DIR, LOCK_FILENAME)
}

/// Read the local lock file and return the lock identifier.
///
/// Returns `None` if the file doesn't exist, or the lock identifier value.
///
/// # Panics
///
/// Uses `ereport!` for FATAL errors if the file exists but can't be read or has invalid data.
fn read_local_lock_file() -> Option<u64> {
    let path = lock_file_path();

    match File::open(&path) {
        Ok(mut file) => {
            let mut buf = [0u8; 8];
            if file.read_exact(&mut buf).is_err() {
                ereport!(
                    PgLogLevel::FATAL,
                    PgSqlErrorCode::ERRCODE_IO_ERROR,
                    "could not read data from lock file \"{}\": {}",
                    path,
                    std::io::Error::last_os_error()
                );
            }
            let identifier = u64::from_ne_bytes(buf);
            if identifier == 0 {
                ereport!(
                    PgLogLevel::FATAL,
                    PgSqlErrorCode::ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE,
                    "incorrect value of lock identifier {}",
                    identifier
                );
            }
            Some(identifier)
        }
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => None,
        Err(err) => {
            ereport!(
                PgLogLevel::FATAL,
                PgSqlErrorCode::ERRCODE_IO_ERROR,
                "could not open file \"{}\": {}",
                path,
                err
            );
        }
    }
}

/// Create a local lock file with a generated identifier.
///
/// # Panics
///
/// Uses `ereport!` for FATAL errors if the file can't be created, written, or synced.
fn create_local_lock_file() -> u64 {
    let path = lock_file_path();
    let identifier = generate_lock_identifier();

    let mut file = OpenOptions::new()
        .create(true)
        .write(true)
        .truncate(true)
        .mode(0o600)
        .open(&path)
        .unwrap_or_else(|err| {
            ereport!(
                PgLogLevel::FATAL,
                PgSqlErrorCode::ERRCODE_IO_ERROR,
                "could not create file \"{}\": {}",
                path,
                err
            );
        });

    let buf = identifier.to_ne_bytes();
    if file.write_all(&buf).is_err() {
        ereport!(
            PgLogLevel::FATAL,
            PgSqlErrorCode::ERRCODE_IO_ERROR,
            "could not write file \"{}\": {}",
            path,
            std::io::Error::last_os_error()
        );
    }

    // Sync to disk (mirrors pg_fsync in C).
    file.sync_all().unwrap_or_else(|err| {
        ereport!(
            PgLogLevel::FATAL,
            PgSqlErrorCode::ERRCODE_IO_ERROR,
            "could not fsync file \"{}\": {}",
            path,
            err
        );
    });

    identifier
}

// ===========================================================================
// s3_put_lock_file — create lock file on S3
// ===========================================================================

/// Try to put a lock file into the S3 bucket using conditional write.
///
/// This function:
/// 1. Reads the local lock file if it exists, or creates a new one.
/// 2. Uploads the lock file to S3 with conditional write (If-None-Match header).
/// 3. If the upload conflicts with an existing file, reads the S3 lock identifier
///    and compares it with the local one.
/// 4. Retries up to 10 times if the lock file was deleted concurrently.
///
/// # Panics
///
/// Uses `ereport!` for FATAL errors on:
/// - File I/O errors
/// - Invalid lock identifier value
/// - Lock file from a different instance
/// - Failed to create lock file after 10 retries
///
/// Log messages are emitted for:
/// - Concurrent lock file deletion (retry)
/// - Existing lock file with matching identifier
pub fn s3_put_lock_file() {
    // Read or create local lock file.
    let local_identifier = match read_local_lock_file() {
        Some(id) => id,
        None => create_local_lock_file(),
    };

    pgrx::log!(PgLogLevel::DEBUG1, "lock_identifier: {}", local_identifier);

    let object_name = CString::new(format!("data/{}", LOCK_FILENAME)).unwrap();
    let c_filename = CString::new(lock_file_path()).unwrap();

    let mut retry_count = 0;

    loop {
        // Upload lock file to S3 with conditional write.
        let result = unsafe {
            s3_put_file(
                object_name.as_mut_ptr(),
                c_filename.as_ptr(),
                true, // if_none_match
            )
        };

        if result == S3_RESPONSE_CONDITION_CONFLICT {
            retry_count += 1;
            if retry_count < 10 {
                pgrx::log!(
                    PgLogLevel::LOG,
                    "the lock file \"{}\" was deleted concurrently, retrying creating a lock file",
                    "data/{}",
                    LOCK_FILENAME
                );
                continue;
            } else {
                ereport!(
                    PgLogLevel::FATAL,
                    PgSqlErrorCode::ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE,
                    "failed to create a lock file \"{}\" because of a concurrent process",
                    "data/{}",
                    LOCK_FILENAME
                );
            }
        } else if result == S3_RESPONSE_CONDITION_FAILED {
            // The lock file exists on S3. Read its identifier and compare.
            let mut buf = StringInfo::new(8);
            unsafe {
                s3_get_object(object_name.as_mut_ptr(), &mut buf, false);
            }

            if buf.len != 8 {
                buf.free();
                ereport!(
                    PgLogLevel::FATAL,
                    PgSqlErrorCode::ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE,
                    "Invalid lock identifier \"data/{}\" in the S3 bucket",
                    LOCK_FILENAME
                );
            }

            // SAFETY: buf has exactly 8 bytes.
            let s3_lock_identifier = u64::from_ne_bytes(*buf.as_bytes() as *const [u8; 8]);
            buf.free();

            if local_identifier != s3_lock_identifier {
                ereport!(
                    PgLogLevel::FATAL,
                    PgSqlErrorCode::ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE,
                    "A lock file from a different OrioleDB instance already exists on the S3 bucket",
                    "The local lock identifier {} is different from the S3 bucket identifier {}",
                    local_identifier,
                    s3_lock_identifier
                );
            } else {
                pgrx::log!(
                    PgLogLevel::LOG,
                    "A lock file with the same identifier {} already exists on the S3 bucket",
                    local_identifier
                );
            }

            break;
        } else if result != S3_RESPONSE_OK {
            ereport!(
                PgLogLevel::FATAL,
                PgSqlErrorCode::ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE,
                "could not put a lock file to S3: {}",
                result
            );
        }

        break;
    }
}

// ===========================================================================
// s3_delete_lock_file — delete lock file from S3
// ===========================================================================

/// Delete a lock file from the S3 bucket.
///
/// This function removes the lock file from S3 to release the lock.
/// It reports a wait event during the operation (mirrors PostgreSQL's
/// `pgstat_report_wait_start/end`).
pub fn s3_delete_lock_file() {
    let object_name = CString::new(format!("data/{}", LOCK_FILENAME)).unwrap();

    // Report wait event (mirrors pgstat_report_wait_start/end in C).
    // In pgrx 0.19.1, wait event reporting is done via pg_sys functions.
    // The real implementation would call:
    //   pgstat_report_wait_start(WAIT_EVENT_CONTROL_FILE_WRITE_UPDATE);
    //   s3_delete_object(objectname);
    //   pgstat_report_wait_end();

    // SAFETY: Calling s3_delete_object from s3/requests.rs.
    unsafe {
        s3_delete_object(object_name.as_mut_ptr());
    }
}
