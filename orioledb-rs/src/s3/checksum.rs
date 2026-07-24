//! S3 file checksum management.
//!
//! Mirrors `src/s3/checksum.c`. Provides utilities for tracking checksums of
//! files during S3 checkpoints, detecting which files have changed since the
//! last checkpoint, and persisting those checksums to temporary files.
//!
//! Key types:
//! - [`S3FileChecksum`] — per-file checksum entry (filename, checksum, changed flag, checkpoint number)
//! - [`S3ChecksumState`] — runtime state holding a hash table of previous checksums and a buffer
//!   of new checksum entries collected during checkpointing.
//!
//! The hash table maps filenames to their previously known checksums (loaded from a checksum file
//! at startup). During checkpointing, `get_s3_file_checksum()` computes the SHA-256 of each file,
//! compares it against the previous checksum, and records whether the file changed.

use crate::btree::types::{OXid, MAXPGPATH};
use pgrx::ereport;
use pgrx::PgLogLevel;
use sha2::{Digest, Sha256};
use std::collections::HashMap;
use std::fs::File;
use std::io::{BufRead, BufReader, Write};
use std::path::Path;

/// Maximum length of a SHA-256 digest expressed as a hex string.
///
/// SHA-256 produces a 32-byte digest, which is 64 hex characters + null terminator.
pub const O_SHA256_DIGEST_STRING_LENGTH: usize = Sha256::OUTPUT_LEN * 2 + 1;

/// A single file's checksum entry.
///
/// Mirrors the C `S3FileChecksum` struct. Contains the file path, its computed
/// SHA-256 checksum (as a hex string), a flag indicating whether the checksum
/// changed since the last checkpoint, and the checkpoint number.
#[derive(Clone, Debug, Default)]
pub struct S3FileChecksum {
    /// File path (up to MAXPGPATH characters).
    pub filename: String,

    /// SHA-256 digest as a lowercase hex string (64 chars + null).
    pub checksum: String,

    /// `true` if the checksum differs from the previous checkpoint.
    pub changed: bool,

    /// Checkpoint number at which this checksum was recorded.
    pub checkpoint_number: u32,
}

impl S3FileChecksum {
    /// Creates a new `S3FileChecksum` with the given filename and checksum.
    fn new(filename: String, checksum: String, changed: bool, checkpoint_number: u32) -> Self {
        Self {
            filename,
            checksum,
            changed,
            checkpoint_number,
        }
    }
}

/// Runtime state for S3 checksum tracking during checkpointing.
///
/// Mirrors the C `S3ChecksumState` struct. Holds:
/// - A hash table of previously known checksums (loaded from a checksum file).
/// - A checkpoint number for validation.
/// - A buffer of new `S3FileChecksum` entries collected during checkpointing.
///
/// The caller is responsible for managing the lifetime of the `file_checksums`
/// buffer (allocated externally, freed by the caller).
pub struct S3ChecksumState {
    /// Hash table mapping filenames to their previous checksums.
    ///
    /// `None` if no checksum file was found or loaded.
    hash_table: Option<HashMap<String, S3FileChecksum>>,

    /// Current checkpoint number — used to validate entries in the checksum file.
    checkpoint_number: u32,

    /// Buffer of checksum entries collected during checkpointing.
    file_checksums: Vec<S3FileChecksum>,

    /// Maximum capacity of the `file_checksums` buffer.
    file_checksums_max_len: u32,
}

impl S3ChecksumState {
    /// Creates a new `S3ChecksumState`, initializing the hash table by reading
    /// the checksum file.
    ///
    /// The caller provides a pre-allocated buffer for checksum entries (we store
    /// it as a `Vec`). The caller is responsible for freeing the underlying
    /// buffer when done.
    ///
    /// If the checksum file doesn't exist (`ENOENT`), the hash table is left
    /// `None` and initialization silently succeeds.
    ///
    /// # Panics
    ///
    /// Uses `ereport!` for error reporting via pgrx. Will abort the current
    /// transaction on I/O errors or malformed checksum files.
    pub fn new(checkpoint_number: u32, file_checksums_max_len: u32, filename: &str) -> Self {
        let mut state = Self {
            hash_table: None,
            checkpoint_number,
            file_checksums: Vec::with_capacity(file_checksums_len as usize),
            file_checksums_max_len,
        };

        init_hash_table(&mut state, filename);
        state
    }

    /// Returns the current checkpoint number.
    pub fn checkpoint_number(&self) -> u32 {
        self.checkpoint_number
    }

    /// Returns a reference to the collected checksum entries.
    pub fn file_checksums(&self) -> &[S3FileChecksum] {
        &self.file_checksums
    }

    /// Returns the number of collected checksum entries.
    pub fn file_checksums_len(&self) -> u32 {
        self.file_checksums.len() as u32
    }

    /// Returns `true` if the hash table has been initialized.
    pub fn hash_table_is_valid(&self) -> bool {
        self.hash_table.is_some()
    }

    /// Looks up a previous checksum entry by filename.
    pub fn find_previous_checksum(&self, filename: &str) -> Option<&S3FileChecksum> {
        self.hash_table.as_ref().and_then(|ht| ht.get(filename))
    }

    /// Adds a new checksum entry to the buffer.
    ///
    /// # Panics
    ///
    /// Aborts if the buffer is full (mirrors the C `elog(ERROR)` in the original).
    pub fn add_checksum(&mut self, entry: S3FileChecksum) {
        if self.file_checksums.len() >= self.file_checksums_max_len as usize {
            ereport!(
                PgLogLevel::ERROR,
                pgrx::PgSqlErrorCode::ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE,
                "size of S3FileChecksum buffer is smaller than requested, \
                 current size is {}",
                self.file_checksums_max_len
            );
        }
        self.file_checksums.push(entry);
    }

    /// Clears the collected checksum entries (resets length to 0).
    ///
    /// Note: This does NOT free the underlying Vec capacity — it only resets
    /// the logical length, matching the C behavior of `state->fileChecksumsLen = 0`.
    pub fn clear_checksums(&mut self) {
        self.file_checksums.clear();
    }
}

/// Frees (discards) the checksum state.
///
/// In Rust, this is a no-op since `S3ChecksumState` owns its data and drops
/// everything when dropped. The C version calls `hash_destroy` and `pfree`.
pub fn free_s3_checksum_state(_state: S3ChecksumState) {
    // Rust drops all fields automatically when the struct goes out of scope.
}

/// Flushes the collected checksum entries to a temporary file.
///
/// Writes each entry in the format:
/// ```text
/// FILE: <filename>, CHECKSUM: <checksum>, CHECKPOINT: <checkpoint_number>
/// ```
///
/// Opens the file for append (to support multiple flushes).
///
/// # Panics
///
/// Aborts on I/O errors via `ereport!`.
pub fn flush_s3_checksum_state(state: &mut S3ChecksumState, filename: &str) {
    assert!(
        !state.file_checksums.is_empty(),
        "flush called with empty checksums"
    );

    let path = Path::new(filename);
    let mut file = File::options()
        .create(true)
        .append(true)
        .open(path)
        .unwrap_or_else(|err| {
            ereport!(
                PgLogLevel::ERROR,
                pgrx::PgSqlErrorCode::ERRCODE_IO_ERROR,
                "could not create file \"{}\": {}",
                filename,
                err
            );
        });

    for entry in state.file_checksums.iter() {
        let line = format!(
            "FILE: {}, CHECKSUM: {}, CHECKPOINT: {}\n",
            entry.filename, entry.checksum, entry.checkpoint_number
        );
        if file.write_all(line.as_bytes()).is_err() {
            ereport!(
                PgLogLevel::ERROR,
                pgrx::PgSqlErrorCode::ERRCODE_IO_ERROR,
                "could not write file \"{}\": {}",
                filename,
                std::io::Error::last_os_error()
            );
        }
    }

    // Flush and verify no errors occurred.
    if let Err(err) = file.flush() {
        ereport!(
            PgLogLevel::ERROR,
            pgrx::PgSqlErrorCode::ERRCODE_IO_ERROR,
            "could not write file \"{}\": {}",
            filename,
            err
        );
    }

    // Reset the collected checksums length (mirrors `state->fileChecksumsLen = 0`).
    state.clear_checksums();
}

/// Computes the SHA-256 checksum of the given data and returns it as a hex string.
fn compute_sha256_hex(data: &[u8]) -> String {
    let mut hasher = Sha256::new();
    hasher.update(data);
    let result = hasher.finalize();
    hex::encode(result)
}

/// Initializes the hash table by reading the checksum file.
///
/// The checksum file format is:
/// ```text
/// FILE: <filename>, CHECKSUM: <checksum>, CHECKPOINT: <checkpoint_number>
/// ```
///
/// Each valid line is inserted into the hash table. Duplicate keys cause an
/// error. Checkpoint numbers >= the current checkpoint number also cause an error.
///
/// If the file doesn't exist (`ENOENT`), the function returns silently without
/// initializing the hash table.
fn init_hash_table(state: &mut S3ChecksumState, filename: &str) {
    let path = Path::new(filename);

    // Silently ignore missing files — the hash table may not exist.
    let file = match File::open(path) {
        Ok(f) => f,
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => return,
        Err(err) => {
            ereport!(
                PgLogLevel::ERROR,
                pgrx::PgSqlErrorCode::ERRCODE_IO_ERROR,
                "could not read file \"{}\": {}",
                filename,
                err
            );
        }
    };

    let reader = BufReader::new(file);

    // Create the hash table.
    let mut hash_table: HashMap<String, S3FileChecksum> = HashMap::with_capacity(32);

    for (line_num, line_result) in reader.lines().enumerate() {
        let line = match line_result {
            Ok(l) => l,
            Err(err) => {
                ereport!(
                    PgLogLevel::ERROR,
                    pgrx::PgSqlErrorCode::ERRCODE_IO_ERROR,
                    "could not read line {} from checksum file \"{}\": {}",
                    line_num + 1,
                    filename,
                    err
                );
            }
        };

        // Parse: "FILE: <filename>, CHECKSUM: <checksum>, CHECKPOINT: <number>"
        let mut filename = None;
        let mut checksum = None;
        let mut checkpoint_number = None;

        for part in line.split(',') {
            let part = part.trim();
            if let Some(rest) = part.strip_prefix("FILE: ") {
                filename = Some(rest.trim().to_string());
            } else if let Some(rest) = part.strip_prefix("CHECKSUM: ") {
                checksum = Some(rest.trim().to_string());
            } else if let Some(rest) = part.strip_prefix("CHECKPOINT: ") {
                if let Ok(num) = rest.trim().parse::<u32>() {
                    checkpoint_number = Some(num);
                }
            }
        }

        let entry_filename = match filename {
            Some(f) => f,
            None => {
                ereport!(
                    PgLogLevel::ERROR,
                    pgrx::PgSqlErrorCode::ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE,
                    "invalid line format of the checksum file \"{}\": {}",
                    filename,
                    line
                );
            }
        };

        let entry_checksum = match checksum {
            Some(c) => c,
            None => {
                ereport!(
                    PgLogLevel::ERROR,
                    pgrx::PgSqlErrorCode::ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE,
                    "invalid line format of the checksum file \"{}\": {}",
                    filename,
                    line
                );
            }
        };

        let entry_checkpoint = match checkpoint_number {
            Some(n) => n,
            None => {
                ereport!(
                    PgLogLevel::ERROR,
                    pgrx::PgSqlErrorCode::ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE,
                    "invalid line format of the checksum file \"{}\": {}",
                    filename,
                    line
                );
            }
        };

        // Validate checkpoint number.
        if entry_checkpoint >= state.checkpoint_number {
            ereport!(
                PgLogLevel::ERROR,
                pgrx::PgSqlErrorCode::ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE,
                "unexpected checkpoint number in the checksum file \"{}\": {}",
                filename,
                line
            );
        }

        // Check for duplicate keys.
        let exists = hash_table.contains_key(&entry_filename);
        if exists {
            ereport!(
                PgLogLevel::ERROR,
                pgrx::PgSqlErrorCode::ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE,
                "the file name is duplicated in the checksum file \"{}\": {}",
                filename,
                line
            );
        }

        // Insert into hash table.
        hash_table.insert(
            entry_filename,
            S3FileChecksum::new(
                entry_checksum.clone(),
                entry_checksum,
                false, // changed is always false for previous entries
                entry_checkpoint,
            ),
        );
    }

    state.hash_table = Some(hash_table);
}

/// Computes the checksum for a file and returns an `S3FileChecksum` entry.
///
/// This function:
/// 1. Looks up the previous checksum for the given filename (if the hash table exists).
/// 2. Computes the SHA-256 of the provided data.
/// 3. Compares the new checksum against the previous one.
/// 4. Creates a new `S3FileChecksum` entry with the `changed` flag set accordingly.
/// 5. Appends the entry to the state's checksum buffer.
///
/// # Arguments
///
/// * `state` — The checksum state (must have been initialized via `S3ChecksumState::new`).
/// * `filename` — The file path to compute the checksum for.
/// * `data` — The file data to hash.
///
/// # Returns
///
/// A newly allocated `S3FileChecksum` entry (owned by the caller via the `Vec`).
///
/// # Panics
///
/// Aborts if:
/// - The buffer is full.
/// - The SHA-256 computation fails (should never happen).
pub fn get_s3_file_checksum(
    state: &mut S3ChecksumState,
    filename: &str,
    data: &[u8],
) -> S3FileChecksum {
    // Look up previous entry.
    let prev_entry = state.find_previous_checksum(filename);

    // Compute SHA-256.
    let hex_checksum = compute_sha256_hex(data);

    // Determine if the file changed.
    let (changed, checkpoint_number) = match prev_entry {
        Some(prev) if prev.checksum == hex_checksum => (false, prev.checkpoint_number),
        _ => (true, state.checkpoint_number),
    };

    // Create new entry.
    let entry = S3FileChecksum::new(
        filename.to_string(),
        hex_checksum,
        changed,
        checkpoint_number,
    );

    // Add to buffer.
    state.add_checksum(entry.clone());

    entry
}
