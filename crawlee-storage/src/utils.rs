use std::path::Path;

use sha2::{Digest, Sha256};
use thiserror::Error;
use tokio::fs;
use tracing::warn;

#[derive(Error, Debug)]
pub enum StorageError {
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),

    #[error("JSON error: {0}")]
    Json(#[from] serde_json::Error),

    #[error("Invalid arguments: {0}")]
    InvalidArgs(String),

    #[error("Storage not found: {0}")]
    NotFound(String),
}

pub type Result<T> = std::result::Result<T, StorageError>;

/// Generate a random alphanumeric ID of the given length.
/// Uses the same character set as Python's `crypto_random_object_id`:
/// `abcdefghijklmnopqrstuvwxyzABCEDFGHIJKLMNOPQRSTUVWXYZ0123456789`
/// (Note: The uppercase portion has D and E swapped compared to normal alphabetical order,
/// matching the Python implementation exactly.)
pub fn crypto_random_object_id(length: usize) -> String {
    use rand::Rng;
    const CHARSET: &[u8] = b"abcdefghijklmnopqrstuvwxyzABCEDFGHIJKLMNOPQRSTUVWXYZ0123456789";
    let mut rng = rand::rng();
    (0..length)
        .map(|_| {
            let idx = rng.random_range(0..CHARSET.len());
            CHARSET[idx] as char
        })
        .collect()
}

/// JSON-serialize a value with pretty-printing (2-space indent, non-ASCII preserved).
/// Matches Python's `json.dumps(value, ensure_ascii=False, indent=2, default=str)`.
pub fn json_dumps(value: &serde_json::Value) -> Result<String> {
    let mut buf = Vec::new();
    let formatter = serde_json::ser::PrettyFormatter::with_indent(b"  ");
    let mut serializer = serde_json::Serializer::with_formatter(&mut buf, formatter);
    value.serialize(&mut serializer)?;
    // serde_json always produces valid UTF-8
    Ok(String::from_utf8(buf).expect("serde_json produces valid UTF-8"))
}

use serde::Serialize;

/// JSON-serialize a serde-serializable value with pretty-printing.
pub fn json_dumps_value<T: Serialize>(value: &T) -> Result<String> {
    let json_value = serde_json::to_value(value)?;
    json_dumps(&json_value)
}

/// Atomically write data to a file.
/// Creates a temp file in the same directory, writes data, then renames.
/// This ensures the file is never partially written.
pub async fn atomic_write(path: &Path, data: &[u8]) -> Result<()> {
    let parent = path
        .parent()
        .ok_or_else(|| StorageError::InvalidArgs("path has no parent directory".to_string()))?;

    // Ensure parent directory exists
    fs::create_dir_all(parent).await?;

    // Write to a temp file in the same directory (same filesystem for atomic rename)
    let temp_path = parent.join(format!(".tmp.{}", crypto_random_object_id(12)));

    // Write data to temp file
    match fs::write(&temp_path, data).await {
        Ok(()) => {}
        Err(e) => {
            // Clean up temp file on error
            let _ = fs::remove_file(&temp_path).await;
            return Err(e.into());
        }
    }

    // Atomic rename
    match fs::rename(&temp_path, path).await {
        Ok(()) => Ok(()),
        Err(e) => {
            // Clean up temp file on error
            let _ = fs::remove_file(&temp_path).await;
            Err(e.into())
        }
    }
}

/// URL-encode a key for filesystem safety.
/// Equivalent to Python's `urllib.parse.quote(key, safe='')`.
pub fn encode_key(key: &str) -> String {
    use percent_encoding::{utf8_percent_encode, NON_ALPHANUMERIC};
    utf8_percent_encode(key, NON_ALPHANUMERIC).to_string()
}

/// URL-decode a filesystem-safe key back to its original form.
/// Equivalent to Python's `urllib.parse.unquote(encoded_key)`.
pub fn decode_key(encoded_key: &str) -> String {
    use percent_encoding::percent_decode_str;
    percent_decode_str(encoded_key)
        .decode_utf8_lossy()
        .to_string()
}

/// Compute SHA-256 hash of a string and return the first `len` hex characters.
/// Used for request queue filenames: `sha256(unique_key)[:15]`.
pub fn sha256_prefix(input: &str, len: usize) -> String {
    let mut hasher = Sha256::new();
    hasher.update(input.as_bytes());
    let hash = hasher.finalize();
    let hex = format!("{hash:x}");
    hex[..len.min(hex.len())].to_string()
}

/// The metadata filename constant, matching Python's `METADATA_FILENAME`.
pub const METADATA_FILENAME: &str = "__metadata__.json";

/// Infer MIME type for a value being stored in a KVS.
/// Matches Python's `infer_mime_type()` behavior.
pub fn infer_mime_type(value: &serde_json::Value) -> &'static str {
    match value {
        serde_json::Value::Null => "application/x-none",
        serde_json::Value::String(_) => "text/plain",
        _ => "application/json",
    }
}

/// Validate that at most one of the given options is Some.
/// Equivalent to Python's `raise_if_too_many_kwargs`.
pub fn validate_exclusive_args(id: &Option<String>, name: &Option<String>) -> Result<()> {
    let count = [id.is_some(), name.is_some()]
        .iter()
        .filter(|&&x| x)
        .count();
    if count > 1 {
        return Err(StorageError::InvalidArgs(
            "At most one of 'id' or 'name' may be specified".to_string(),
        ));
    }
    Ok(())
}

/// Find a storage directory by scanning subdirectories for a matching metadata ID.
/// Returns the path to the matching subdirectory if found.
pub async fn find_storage_by_id(
    base_dir: &Path,
    storage_subdir: &str,
    target_id: &str,
) -> Result<Option<std::path::PathBuf>> {
    let search_dir = base_dir.join(storage_subdir);
    if !search_dir.exists() {
        return Ok(None);
    }

    let mut entries = fs::read_dir(&search_dir).await?;
    while let Some(entry) = entries.next_entry().await? {
        let path = entry.path();
        if !path.is_dir() {
            continue;
        }
        let metadata_path = path.join(METADATA_FILENAME);
        if !metadata_path.exists() {
            continue;
        }
        match fs::read_to_string(&metadata_path).await {
            Ok(content) => {
                if let Ok(meta) = serde_json::from_str::<serde_json::Value>(&content) {
                    if meta.get("id").and_then(|v| v.as_str()) == Some(target_id) {
                        return Ok(Some(path));
                    }
                }
            }
            Err(e) => {
                warn!(
                    "Failed to read metadata file {}: {}",
                    metadata_path.display(),
                    e
                );
            }
        }
    }

    Ok(None)
}
