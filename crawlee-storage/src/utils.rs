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

    /// A paginated `list_keys` call (or the core `iterate_keys_page` primitive)
    /// supplied an `exclusive_start_key` cursor that does not correspond to any
    /// existing key (after prefix scoping). The payload carries the offending
    /// key so the binding can format the message.
    #[error(
        "exclusiveStartKey \"{0}\" was not found in the key-value store. \
         This is likely a bug — the key may have been deleted between paginated listKeys calls."
    )]
    ExclusiveStartKeyNotFound(String),
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
///
/// Equivalent to Python's `urllib.parse.quote(key, safe='')`. CPython's
/// `quote` keeps an "always safe" set of unreserved characters unescaped —
/// the ASCII alphanumerics plus `_`, `.`, `-`, and `~` — and `safe=''` only
/// drops the *extra* safe chars (the default `/`), never the always-safe set.
///
/// We therefore start from `NON_ALPHANUMERIC` and remove those four so they
/// pass through verbatim. This keeps keys like `INPUT.json` as the literal
/// filename `INPUT.json` — which is what out-of-band writers (e.g. the Apify
/// CLI) produce, so a relaxed `get_value` can find them. Plain
/// `NON_ALPHANUMERIC` would over-encode the dot (`.` → `%2E`) and break that.
pub fn encode_key(key: &str) -> String {
    use percent_encoding::{utf8_percent_encode, AsciiSet, NON_ALPHANUMERIC};

    // `quote(key, safe='')` leaves the unreserved set `_.-~` unescaped.
    const ENCODE_SET: &AsciiSet = &NON_ALPHANUMERIC
        .remove(b'_')
        .remove(b'.')
        .remove(b'-')
        .remove(b'~');

    utf8_percent_encode(key, ENCODE_SET).to_string()
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

/// Length of a request ID, matching the JS `REQUEST_ID_LENGTH` constant.
pub const REQUEST_ID_LENGTH: usize = 15;

/// Compute a request ID from a unique key, matching the JS
/// `uniqueKeyToRequestId` (and the Apify platform):
/// `sha256(uniqueKey)` → standard base64 → strip `+`/`/`/`=` → first 15 chars.
pub fn unique_key_to_request_id(unique_key: &str) -> String {
    use base64::{engine::general_purpose::STANDARD, Engine as _};

    let mut hasher = Sha256::new();
    hasher.update(unique_key.as_bytes());
    let digest = hasher.finalize();

    let encoded = STANDARD.encode(digest);
    let cleaned: String = encoded
        .chars()
        .filter(|c| *c != '+' && *c != '/' && *c != '=')
        .collect();

    if cleaned.len() > REQUEST_ID_LENGTH {
        cleaned[..REQUEST_ID_LENGTH].to_string()
    } else {
        cleaned
    }
}

/// The metadata filename constant, matching Python's `METADATA_FILENAME`.
pub const METADATA_FILENAME: &str = "__metadata__.json";

/// The content-type sentinel used for `None`/null KVS values (stored on disk
/// as an empty file). Matches crawlee's `application/x-none` MIME type.
///
/// The core never *writes* this itself — callers pass it as the content type
/// when storing a null value — but it's part of the on-disk compatibility
/// contract, so it's exported here for bindings to reference instead of
/// hardcoding the literal string.
pub const NONE_CONTENT_TYPE: &str = "application/x-none";

/// Validate that at most one of the given options is Some.
/// Equivalent to Python's `raise_if_too_many_kwargs`.
pub fn validate_exclusive_args(
    id: &Option<String>,
    name: &Option<String>,
    alias: &Option<String>,
) -> Result<()> {
    let count = [id.is_some(), name.is_some(), alias.is_some()]
        .iter()
        .filter(|&&x| x)
        .count();
    if count > 1 {
        return Err(StorageError::InvalidArgs(
            "At most one of 'id', 'name', or 'alias' may be specified".to_string(),
        ));
    }
    Ok(())
}

/// Resolve a storage subdirectory inside a base directory, rejecting anything
/// that does not map to a single direct child of `base_dir`.
///
/// Joins `subdirectory` onto `base_dir` and verifies the result is a direct
/// child of `base_dir`, so a storage name or alias always maps to one
/// subdirectory under the storage directory rather than a nested path (e.g.
/// `nested/inside`) or somewhere else entirely (e.g. a value containing `..`
/// or an absolute path).
///
/// Normalization is purely lexical (no filesystem access): symlinks are not
/// followed and the check is deterministic. Mirrors the Python
/// `validate_subdirectory` helper.
pub fn validate_subdirectory(base_dir: &Path, subdirectory: &str) -> Result<std::path::PathBuf> {
    let target = normalize_lexically(&base_dir.join(subdirectory));
    let base = normalize_lexically(base_dir);

    // The target must be a direct child of the base directory: its parent,
    // after lexical normalization, must equal the normalized base. This rejects
    // path separators, parent references ("..") and absolute paths.
    if target.parent() != Some(base.as_path()) {
        return Err(StorageError::InvalidArgs(format!(
            "Invalid storage name or alias \"{subdirectory}\". It must map to a single \
             subdirectory under the storage directory and must not contain path separators, \
             parent directory references (\"..\") or absolute paths."
        )));
    }

    Ok(target)
}

/// Lexically normalize a path (no filesystem access): collapse `.`, resolve
/// `..` against preceding normal components, and preserve a leading root.
/// Equivalent to Python's `os.path.normpath` for our purposes.
fn normalize_lexically(path: &Path) -> std::path::PathBuf {
    use std::path::Component;

    let mut out = std::path::PathBuf::new();
    for component in path.components() {
        match component {
            Component::CurDir => {}
            Component::ParentDir => {
                // Pop a preceding normal component if there is one; otherwise
                // keep the `..` so an escaping path stays escaping (and thus
                // gets rejected by the parent-equality check above).
                if matches!(out.components().next_back(), Some(Component::Normal(_))) {
                    out.pop();
                } else {
                    out.push(component.as_os_str());
                }
            }
            other => out.push(other.as_os_str()),
        }
    }
    out
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_encode_key_matches_quote_safe_empty() {
        // The unreserved set `_ . - ~` is preserved verbatim (matching CPython's
        // urllib.parse.quote(key, safe='') and crawlee-python). Everything else
        // non-alphanumeric is percent-encoded.
        assert_eq!(encode_key("INPUT.json"), "INPUT.json");
        assert_eq!(encode_key("my-key"), "my-key");
        assert_eq!(encode_key("a_b.c-d~e"), "a_b.c-d~e");
        assert_eq!(encode_key("plain123"), "plain123");
        // Reserved / unsafe chars still get encoded.
        assert_eq!(encode_key("a/b"), "a%2Fb");
        assert_eq!(encode_key("a b"), "a%20b");
        assert_eq!(encode_key("a:b"), "a%3Ab");
        // Round-trips through decode (and legacy %2E-style encodings still decode).
        assert_eq!(decode_key(&encode_key("INPUT.json")), "INPUT.json");
        assert_eq!(decode_key("INPUT%2Ejson"), "INPUT.json");
    }

    #[test]
    fn test_validate_subdirectory_accepts_plain_names() {
        let base = Path::new("/data/datasets");
        for name in ["default", "my-store", "Some_Name", "with spaces", "123"] {
            let got = validate_subdirectory(base, name).expect("plain name should be accepted");
            assert_eq!(got, base.join(name), "name: {name}");
        }
    }

    #[test]
    fn test_validate_subdirectory_rejects_traversal_and_separators() {
        let base = Path::new("/data/datasets");
        for evil in [
            "..",
            "../escape",
            "../../etc/passwd",
            "nested/inside",
            "a/b",
            "/absolute",
            "/etc/passwd",
            "foo/..", // normalizes back to base itself, not a child
            "./..",   // also escapes
        ] {
            assert!(
                validate_subdirectory(base, evil).is_err(),
                "expected rejection for: {evil:?}"
            );
        }
    }

    #[test]
    fn test_validate_subdirectory_collapses_harmless_curdir() {
        // A leading "./name" normalizes to a direct child and is fine.
        let base = Path::new("/data/key_value_stores");
        let got = validate_subdirectory(base, "./store").expect("./store should normalize");
        assert_eq!(got, base.join("store"));
    }
}
