use std::path::{Path, PathBuf};

use tokio::fs;
use tokio::sync::Mutex;
use tracing::warn;

use crate::clock::{system_clock, ClockRef};
use crate::models::{KeyValueStoreMetadata, KeyValueStoreRecordMetadata, KvsKeysPage};
use crate::utils::{
    atomic_write, crypto_random_object_id, encode_key, find_storage_by_id, json_dumps_value,
    validate_exclusive_args, validate_subdirectory, Result, StorageError, METADATA_FILENAME,
};

const STORAGE_SUBDIR: &str = "key_value_stores";
const DEFAULT_NAME: &str = "default";

/// Filesystem-backed key-value store client.
///
/// Stores each key as a pair of files: the value file and a metadata sidecar.
///
/// Directory layout:
/// ```text
/// {storage_dir}/key_value_stores/{name}/
/// ├── __metadata__.json
/// ├── {encoded_key}                      (value data)
/// ├── {encoded_key}.__metadata__.json    (record metadata sidecar)
/// └── ...
/// ```
pub struct FileSystemKeyValueStoreClient {
    metadata: Mutex<KeyValueStoreMetadata>,
    path: PathBuf,
    clock: ClockRef,
}

impl FileSystemKeyValueStoreClient {
    /// Open an existing KVS or create a new one.
    ///
    /// - `id`: Open by ID (scans directories for matching metadata).
    /// - `name`: Open by name (used as directory name, written to metadata).
    /// - `alias`: Open by alias (used as directory name, but NOT written to metadata).
    /// - `storage_dir`: Base storage directory (e.g., "./storage").
    ///
    /// At most one of `id`, `name`, or `alias` may be provided.
    ///
    /// Uses the default [`SystemClock`](crate::clock::SystemClock). To inject a
    /// custom clock (e.g. for tests), use [`open_with_clock`](Self::open_with_clock).
    pub async fn open(
        id: Option<String>,
        name: Option<String>,
        alias: Option<String>,
        storage_dir: &Path,
    ) -> Result<Self> {
        Self::open_with_clock(id, name, alias, storage_dir, system_clock()).await
    }

    /// Open an existing KVS or create a new one, using the supplied clock.
    pub async fn open_with_clock(
        id: Option<String>,
        name: Option<String>,
        alias: Option<String>,
        storage_dir: &Path,
        clock: ClockRef,
    ) -> Result<Self> {
        validate_exclusive_args(&id, &name, &alias)?;

        let path = if let Some(ref id_val) = id {
            find_storage_by_id(storage_dir, STORAGE_SUBDIR, id_val)
                .await?
                .ok_or_else(|| {
                    StorageError::NotFound(format!("Key-value store with id '{id_val}' not found"))
                })?
        } else {
            let base = storage_dir.join(STORAGE_SUBDIR);
            match name.as_deref().or(alias.as_deref()) {
                // A user-supplied name/alias must map to a single direct child.
                Some(dir_name) => validate_subdirectory(&base, dir_name)?,
                // The default name is a trusted constant, no validation needed.
                None => base.join(DEFAULT_NAME),
            }
        };

        let metadata_path = path.join(METADATA_FILENAME);

        let metadata = if metadata_path.exists() {
            let content = fs::read_to_string(&metadata_path).await?;
            serde_json::from_str::<KeyValueStoreMetadata>(&content)?
        } else {
            // Only `name` goes into metadata, not alias
            let new_id = id.unwrap_or_else(|| crypto_random_object_id(17));
            let mut meta = KeyValueStoreMetadata::new(new_id, name);
            let now = clock.now();
            meta.base.created_at = now;
            meta.base.modified_at = now;
            meta.base.accessed_at = now;
            fs::create_dir_all(&path).await?;
            let json = json_dumps_value(&meta)?;
            atomic_write(&metadata_path, json.as_bytes()).await?;
            meta
        };

        Ok(Self {
            metadata: Mutex::new(metadata),
            path,
            clock,
        })
    }

    /// Return a reference to this client's clock.
    pub fn clock(&self) -> &ClockRef {
        &self.clock
    }

    /// Get the store metadata.
    pub async fn get_metadata(&self) -> KeyValueStoreMetadata {
        self.metadata.lock().await.clone()
    }

    /// Path to the store directory.
    pub fn path(&self) -> &Path {
        &self.path
    }

    /// Path to the metadata file.
    pub fn metadata_path(&self) -> PathBuf {
        self.path.join(METADATA_FILENAME)
    }

    /// Delete the entire store directory.
    pub async fn drop_storage(&self) -> Result<()> {
        if self.path.exists() {
            fs::remove_dir_all(&self.path).await?;
        }
        Ok(())
    }

    /// Delete all value files but keep store metadata.
    ///
    /// Any key listed in `keep` is spared: both its value file and its metadata
    /// sidecar are left on disk. Matching is by exact key (encoded to its on-disk
    /// filename via [`encode_key`]) — no extension globbing or stem matching. A
    /// caller wanting to preserve, say, both `INPUT` and `INPUT.json` must pass
    /// both as separate keys. The store-level `__metadata__.json` is always kept.
    pub async fn purge(&self, keep: &[String]) -> Result<()> {
        let mut meta = self.metadata.lock().await;

        // Build the set of filenames to spare: the store metadata plus, for each
        // kept key, its value file and its per-record sidecar.
        let mut keep_files: std::collections::HashSet<String> = std::collections::HashSet::new();
        keep_files.insert(METADATA_FILENAME.to_string());
        for key in keep {
            let encoded = encode_key(key);
            keep_files.insert(format!("{encoded}.{METADATA_FILENAME}"));
            keep_files.insert(encoded);
        }

        match fs::read_dir(&self.path).await {
            Ok(mut entries) => {
                while let Some(entry) = entries.next_entry().await? {
                    let path = entry.path();
                    if path.is_file() {
                        if let Some(name) = path.file_name().and_then(|n| n.to_str()) {
                            if !keep_files.contains(name) {
                                let _ = fs::remove_file(&path).await;
                            }
                        }
                    }
                }
            }
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                // Directory doesn't exist yet — nothing to purge.
            }
            Err(e) => return Err(e.into()),
        }

        let now = self.clock.now();
        meta.base.accessed_at = now;
        meta.base.modified_at = now;

        let json = json_dumps_value(&*meta)?;
        atomic_write(&self.metadata_path(), json.as_bytes()).await?;

        Ok(())
    }

    /// Delete a value by key.
    pub async fn delete_value(&self, key: &str) -> Result<()> {
        let encoded = encode_key(key);
        let value_path = self.path.join(&encoded);
        let sidecar_path = self.path.join(format!("{encoded}.{METADATA_FILENAME}"));

        if value_path.exists() {
            fs::remove_file(&value_path).await?;
        }
        if sidecar_path.exists() {
            fs::remove_file(&sidecar_path).await?;
        }

        // Update store metadata
        {
            let mut meta = self.metadata.lock().await;
            let now = self.clock.now();
            meta.base.accessed_at = now;
            meta.base.modified_at = now;
            let json = json_dumps_value(&*meta)?;
            atomic_write(&self.metadata_path(), json.as_bytes()).await?;
        }

        Ok(())
    }

    /// Fetch the next page of keys for lazy iteration.
    ///
    /// Returns a [`KvsKeysPage`] containing key metadata entries and a flag
    /// indicating whether more keys are available. The binding layer should
    /// call this repeatedly, using the last key returned as
    /// `exclusive_start_key` for the next call, until `has_more` is `false`.
    ///
    /// `page_size` controls how many keys are read per call (default 1000).
    pub async fn iterate_keys_page(
        &self,
        exclusive_start_key: Option<&str>,
        limit: Option<usize>,
        page_size: usize,
        prefix: Option<&str>,
    ) -> Result<KvsKeysPage> {
        // Fetch one extra beyond the page to detect whether more keys exist.
        let fetch_limit = match limit {
            Some(remaining) => page_size.min(remaining),
            None => page_size,
        };

        let results = self
            .list_keys_raw(exclusive_start_key, Some(fetch_limit + 1), prefix)
            .await?;

        let has_more =
            results.len() > fetch_limit && limit.is_none_or(|remaining| fetch_limit < remaining);
        let items: Vec<KeyValueStoreRecordMetadata> =
            results.into_iter().take(fetch_limit).collect();

        Ok(KvsKeysPage { items, has_more })
    }

    /// Internal helper: list keys with cursor, limit and optional prefix,
    /// returning a flat Vec. Keys are filtered by `prefix` (on the decoded key,
    /// not the encoded filename) before the cursor and limit are applied, so the
    /// page's `limit`/`has_more` accounting only ever counts matching keys.
    async fn list_keys_raw(
        &self,
        exclusive_start_key: Option<&str>,
        limit: Option<usize>,
        prefix: Option<&str>,
    ) -> Result<Vec<KeyValueStoreRecordMetadata>> {
        let mut results = Vec::new();
        let metadata_suffix = format!(".{METADATA_FILENAME}");

        let mut sidecar_paths: Vec<PathBuf> = Vec::new();

        let mut entries = match fs::read_dir(&self.path).await {
            Ok(entries) => entries,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(results),
            Err(e) => return Err(e.into()),
        };

        while let Some(entry) = entries.next_entry().await? {
            let path = entry.path();
            if path.is_file() {
                if let Some(name) = path.file_name().and_then(|n| n.to_str()) {
                    // Find sidecar files (but not the store-level metadata)
                    if name.ends_with(&metadata_suffix) && name != METADATA_FILENAME {
                        sidecar_paths.push(path);
                    }
                }
            }
        }

        // Sort by filename for deterministic ordering
        sidecar_paths.sort();

        for sidecar_path in sidecar_paths {
            let content = fs::read_to_string(&sidecar_path).await?;
            match serde_json::from_str::<KeyValueStoreRecordMetadata>(&content) {
                Ok(mut record_meta) => {
                    // Apply prefix filter (on the decoded key)
                    if let Some(prefix) = prefix {
                        if !record_meta.key.starts_with(prefix) {
                            continue;
                        }
                    }

                    // Apply cursor filter
                    if let Some(start_key) = exclusive_start_key {
                        if record_meta.key.as_str() <= start_key {
                            continue;
                        }
                    }

                    // Backfill `size` for foreign/legacy sidecars that omit it
                    // (this library always writes it, but crawlee-JS / older
                    // Python clients may not) by stating the value file. The
                    // value file lives at the sidecar path minus the
                    // `.{METADATA_FILENAME}` suffix.
                    if record_meta.size.is_none() {
                        let value_path = sidecar_path
                            .to_string_lossy()
                            .strip_suffix(&metadata_suffix)
                            .map(PathBuf::from);
                        if let Some(value_path) = value_path {
                            if let Ok(file_meta) = fs::metadata(&value_path).await {
                                record_meta.size = Some(file_meta.len() as usize);
                            }
                        }
                    }

                    results.push(record_meta);

                    // Apply limit
                    if let Some(lim) = limit {
                        if results.len() >= lim {
                            break;
                        }
                    }
                }
                Err(e) => {
                    warn!(
                        "Failed to parse sidecar metadata {}: {}",
                        sidecar_path.display(),
                        e
                    );
                }
            }
        }

        Ok(results)
    }

    /// Get a file:// URL for a key.
    pub async fn get_public_url(&self, key: &str) -> String {
        let encoded = encode_key(key);
        let value_path = self.path.join(&encoded);
        format!("file://{}", value_path.display())
    }

    /// Get the file path and metadata for a record, without reading its contents.
    ///
    /// Returns `(value_path, record_metadata)` if the key exists, or `None` if it doesn't.
    /// This is useful for streaming reads — the binding layer can open the file
    /// and stream it directly instead of buffering the entire contents.
    ///
    /// The client is a pure byte transport: it returns the raw value bytes (via
    /// the path) and the verbatim `content_type` from the sidecar. Parsing and
    /// value semantics live at the `KeyValueStore` frontend.
    ///
    /// When `require_record_metadata` is `true` (the normal case), a record is
    /// only returned if it has a metadata sidecar; a value file without one is
    /// treated as absent. When `false`, a value file with no sidecar is still
    /// returned, with synthesized metadata: `content_type` is the generic
    /// `application/octet-stream` sentinel (the client never infers a type from
    /// the file extension — that foreign-file convention lives at the frontend)
    /// and `size` is the value-file length. This is the escape hatch for reading
    /// out-of-band files (e.g. a CLI-written `INPUT.json` that has no sidecar).
    pub async fn get_value(
        &self,
        key: &str,
        require_record_metadata: bool,
    ) -> Result<Option<(PathBuf, KeyValueStoreRecordMetadata)>> {
        let encoded = encode_key(key);
        let value_path = self.path.join(&encoded);
        let sidecar_path = self.path.join(format!("{encoded}.{METADATA_FILENAME}"));

        // Always update accessed_at on read, even for missing keys
        {
            let mut meta = self.metadata.lock().await;
            meta.base.accessed_at = self.clock.now();
            let json = json_dumps_value(&*meta)?;
            atomic_write(&self.metadata_path(), json.as_bytes()).await?;
        }

        // The value file is always required.
        if !value_path.exists() {
            return Ok(None);
        }

        if sidecar_path.exists() {
            let sidecar_content = fs::read_to_string(&sidecar_path).await?;
            let mut record_meta: KeyValueStoreRecordMetadata =
                serde_json::from_str(&sidecar_content)?;

            // Backfill `size` for foreign/legacy sidecars that omit it (this
            // library always writes it, but crawlee-JS / older Python clients may
            // not) by stating the value file.
            if record_meta.size.is_none() {
                if let Ok(file_meta) = fs::metadata(&value_path).await {
                    record_meta.size = Some(file_meta.len() as usize);
                }
            }

            return Ok(Some((value_path, record_meta)));
        }

        // No sidecar. By default that means "not a record"; with the opt-in flag
        // we still serve the bytes, synthesizing dumb metadata (no type inference).
        if require_record_metadata {
            return Ok(None);
        }

        let size = fs::metadata(&value_path)
            .await
            .ok()
            .map(|file_meta| file_meta.len() as usize);
        let record_meta = KeyValueStoreRecordMetadata {
            key: key.to_string(),
            content_type: "application/octet-stream".to_string(),
            size,
        };

        Ok(Some((value_path, record_meta)))
    }

    /// Resolve a key to a value, transparently falling back to out-of-band
    /// ("bare") value files that have no metadata sidecar.
    ///
    /// This bundles the lookup that binding layers would otherwise hand-roll: a
    /// run's input may be a properly-tracked record, or an out-of-band file a
    /// CLI/platform dropped on disk under one of several conventional names
    /// (`INPUT`, `INPUT.json`, `INPUT.bin`, ...). The probe order is:
    ///
    /// 1. The tracked record for the literal `key` (value file + sidecar). Its
    ///    `content_type` comes verbatim from the sidecar.
    /// 2. For each `(extension, content_type)` in `bare_fallbacks`, the bare
    ///    file at `key + extension` (no sidecar required). On a match the
    ///    supplied `content_type` is used.
    ///
    /// The first match wins. The returned [`KeyValueStoreRecordMetadata`] is
    /// always keyed by the originally-requested `key` (never the on-disk
    /// filename of a matched bare file), so callers see a stable key.
    ///
    /// The core still performs **no** MIME inference of its own: the caller
    /// declares which extensions to probe and what content type each implies
    /// (the `(extension, content_type)` pairs). That keeps the "which files are
    /// input, and what type is a `.json`" policy at the frontend while the
    /// probing/lookup mechanism lives here, shared by every binding.
    ///
    /// Returns `(value_path, metadata)` for the first match, or `None`.
    pub async fn resolve_value(
        &self,
        key: &str,
        bare_fallbacks: &[(&str, &str)],
    ) -> Result<Option<(PathBuf, KeyValueStoreRecordMetadata)>> {
        // 1. Tracked record for the literal key — sidecar content type wins.
        if let Some(result) = self.get_value(key, true).await? {
            return Ok(Some(result));
        }

        // 2. Out-of-band bare files: probe each conventional extension.
        for (extension, content_type) in bare_fallbacks {
            let candidate = format!("{key}{extension}");
            if let Some((path, mut meta)) = self.get_value(&candidate, false).await? {
                // Re-key to the requested key and apply the caller-declared
                // content type for this extension. An empty extension (the
                // literal key) keeps the synthesized `application/octet-stream`
                // unless the caller declared something else.
                meta.key = key.to_string();
                if !content_type.is_empty() {
                    meta.content_type = (*content_type).to_string();
                }
                return Ok(Some((path, meta)));
            }
        }

        Ok(None)
    }

    /// Check whether a key resolves to a value, using the same fallback probe
    /// order as [`resolve_value`](Self::resolve_value) but without reading the
    /// value file. Returns the matched on-disk key (the literal key or a bare
    /// `key + extension`), or `None` if nothing exists.
    ///
    /// The matched key is what a caller should pass to
    /// [`get_public_url`](Self::get_public_url) so the URL points at the file
    /// that actually exists.
    pub async fn resolve_existing_key(
        &self,
        key: &str,
        bare_fallbacks: &[&str],
    ) -> Option<String> {
        if self.record_exists(key, true).await {
            return Some(key.to_string());
        }
        for extension in bare_fallbacks {
            let candidate = format!("{key}{extension}");
            if self.record_exists(&candidate, false).await {
                return Some(candidate);
            }
        }
        None
    }

    /// Write raw bytes for a key, with sidecar metadata and atomic write.
    ///
    /// The client is a pure byte transport: `data` is written verbatim and
    /// `content_type` is stored as-is in the sidecar — no inference, no
    /// serialization. Value semantics live at the `KeyValueStore` frontend.
    pub async fn set_value(&self, key: &str, data: &[u8], content_type: String) -> Result<()> {
        let encoded = encode_key(key);
        let value_path = self.path.join(&encoded);
        let sidecar_path = self.path.join(format!("{encoded}.{METADATA_FILENAME}"));

        atomic_write(&value_path, data).await?;

        let record_meta = KeyValueStoreRecordMetadata {
            key: key.to_string(),
            content_type,
            size: Some(data.len()),
        };
        let sidecar_json = json_dumps_value(&record_meta)?;
        atomic_write(&sidecar_path, sidecar_json.as_bytes()).await?;

        {
            let mut meta = self.metadata.lock().await;
            let now = self.clock.now();
            meta.base.accessed_at = now;
            meta.base.modified_at = now;
            let json = json_dumps_value(&*meta)?;
            atomic_write(&self.metadata_path(), json.as_bytes()).await?;
        }

        Ok(())
    }

    /// Get a path for a new temp file in the store directory.
    ///
    /// The binding layer uses this to stream data to a temp file, then calls
    /// [`finalize_streamed_value`] to atomically move it into place.
    pub fn temp_file_path(&self) -> PathBuf {
        self.path
            .join(format!(".tmp.{}", crypto_random_object_id(12)))
    }

    /// Finalize a streamed write: atomically rename `temp_path` to the value
    /// file for `key`, write the sidecar metadata, and update store metadata.
    ///
    /// The caller is responsible for having already written the full value data
    /// to `temp_path` (e.g. by piping a stream to it).
    pub async fn finalize_streamed_value(
        &self,
        key: &str,
        temp_path: &Path,
        size: usize,
        content_type: String,
    ) -> Result<()> {
        let encoded = encode_key(key);
        let value_path = self.path.join(&encoded);
        let sidecar_path = self.path.join(format!("{encoded}.{METADATA_FILENAME}"));

        // Atomic rename from temp → final value path
        fs::rename(temp_path, &value_path).await?;

        // Write sidecar metadata
        let record_meta = KeyValueStoreRecordMetadata {
            key: key.to_string(),
            content_type,
            size: Some(size),
        };
        let sidecar_json = json_dumps_value(&record_meta)?;
        atomic_write(&sidecar_path, sidecar_json.as_bytes()).await?;

        // Update store metadata
        {
            let mut meta = self.metadata.lock().await;
            let now = self.clock.now();
            meta.base.accessed_at = now;
            meta.base.modified_at = now;
            let json = json_dumps_value(&*meta)?;
            atomic_write(&self.metadata_path(), json.as_bytes()).await?;
        }

        Ok(())
    }

    /// Check if a record exists for a key.
    ///
    /// When `require_record_metadata` is `true`, both the value file and its
    /// metadata sidecar must exist. When `false`, a value file alone counts —
    /// matching the relaxed [`get_value`](Self::get_value) lookup for reading
    /// out-of-band files that have no sidecar.
    pub async fn record_exists(&self, key: &str, require_record_metadata: bool) -> bool {
        let encoded = encode_key(key);
        let value_path = self.path.join(&encoded);
        if !value_path.exists() {
            return false;
        }
        if !require_record_metadata {
            return true;
        }
        let sidecar_path = self.path.join(format!("{encoded}.{METADATA_FILENAME}"));
        sidecar_path.exists()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use tempfile::TempDir;

    /// Read back the raw value bytes + content type for a key via the byte-only
    /// `get_value` (which returns the value path + sidecar metadata).
    async fn read_back(
        client: &FileSystemKeyValueStoreClient,
        key: &str,
    ) -> Option<(Vec<u8>, String, Option<usize>)> {
        let (path, meta) = client.get_value(key, true).await.unwrap()?;
        let bytes = tokio::fs::read(&path).await.unwrap();
        Some((bytes, meta.content_type, meta.size))
    }

    #[tokio::test]
    async fn test_on_disk_sidecar_uses_camel_case() {
        let temp_dir = TempDir::new().unwrap();
        let storage_dir = temp_dir.path();

        let client = FileSystemKeyValueStoreClient::open(None, None, None, storage_dir)
            .await
            .unwrap();

        client
            .set_value(
                "test-key",
                br#"{"x":1}"#,
                "application/json; charset=utf-8".to_string(),
            )
            .await
            .unwrap();

        // Read the sidecar metadata
        let encoded = encode_key("test-key");
        let sidecar_path = client.path().join(format!("{encoded}.{METADATA_FILENAME}"));
        let raw = tokio::fs::read_to_string(&sidecar_path).await.unwrap();
        let parsed: serde_json::Value = serde_json::from_str(&raw).unwrap();
        let obj = parsed.as_object().unwrap();

        assert!(
            obj.contains_key("contentType"),
            "expected 'contentType', got: {raw}"
        );
        assert!(
            !obj.contains_key("content_type"),
            "unexpected 'content_type'"
        );

        // Store metadata should also be camelCase
        let store_raw = tokio::fs::read_to_string(client.metadata_path())
            .await
            .unwrap();
        let store_parsed: serde_json::Value = serde_json::from_str(&store_raw).unwrap();
        let store_obj = store_parsed.as_object().unwrap();
        assert!(
            store_obj.contains_key("accessedAt"),
            "expected 'accessedAt' in store metadata, got: {store_raw}"
        );
    }

    #[tokio::test]
    async fn test_json_bytes_roundtrip() {
        let temp_dir = TempDir::new().unwrap();
        let storage_dir = temp_dir.path();

        let client = FileSystemKeyValueStoreClient::open(None, None, None, storage_dir)
            .await
            .unwrap();

        // The client is byte transport: the frontend already serialized this.
        let payload = br#"{"hello":"world"}"#;
        client
            .set_value(
                "my-key",
                payload,
                "application/json; charset=utf-8".to_string(),
            )
            .await
            .unwrap();

        let (bytes, content_type, size) = read_back(&client, "my-key").await.unwrap();
        assert_eq!(bytes, payload);
        assert_eq!(content_type, "application/json; charset=utf-8");
        assert_eq!(size, Some(payload.len()));
    }

    #[tokio::test]
    async fn test_text_bytes_roundtrip() {
        let temp_dir = TempDir::new().unwrap();
        let storage_dir = temp_dir.path();

        let client = FileSystemKeyValueStoreClient::open(None, None, None, storage_dir)
            .await
            .unwrap();

        client
            .set_value(
                "greeting",
                b"hello",
                "text/plain; charset=utf-8".to_string(),
            )
            .await
            .unwrap();

        let (bytes, content_type, _) = read_back(&client, "greeting").await.unwrap();
        assert_eq!(bytes, b"hello");
        assert_eq!(content_type, "text/plain; charset=utf-8");
    }

    #[tokio::test]
    async fn test_null_sentinel_roundtrip() {
        let temp_dir = TempDir::new().unwrap();
        let storage_dir = temp_dir.path();

        let client = FileSystemKeyValueStoreClient::open(None, None, None, storage_dir)
            .await
            .unwrap();

        // Null is represented by the frontend as empty bytes + the sentinel CT.
        client
            .set_value("empty", b"", crate::NONE_CONTENT_TYPE.to_string())
            .await
            .unwrap();

        let (bytes, content_type, size) = read_back(&client, "empty").await.unwrap();
        assert!(bytes.is_empty());
        assert_eq!(content_type, crate::NONE_CONTENT_TYPE);
        assert_eq!(size, Some(0));
    }

    #[tokio::test]
    async fn test_size_backfilled_for_legacy_sidecar_without_size() {
        // crawlee-JS MemoryStorage and older Python FileSystemStorageClient
        // wrote sidecars that omit `size`. On read, the client must backfill it
        // from the actual value-file length rather than surfacing `None`.
        let temp_dir = TempDir::new().unwrap();
        let storage_dir = temp_dir.path();

        let client = FileSystemKeyValueStoreClient::open(None, None, None, storage_dir)
            .await
            .unwrap();

        // Hand-write a value file + a sidecar that has no `size` field.
        let key = "legacy-key";
        let payload = b"twelve bytes";
        let encoded = encode_key(key);
        let value_path = client.path().join(&encoded);
        let sidecar_path = client.path().join(format!("{encoded}.{METADATA_FILENAME}"));
        tokio::fs::write(&value_path, payload).await.unwrap();
        tokio::fs::write(
            &sidecar_path,
            br#"{"key":"legacy-key","contentType":"text/plain"}"#,
        )
        .await
        .unwrap();

        // get_value backfills from the file length.
        let (_, meta) = client.get_value(key, true).await.unwrap().unwrap();
        assert_eq!(meta.size, Some(payload.len()));

        // The list/iterate path backfills too.
        let page = client
            .iterate_keys_page(None, None, 1000, None)
            .await
            .unwrap();
        let entry = page.items.iter().find(|m| m.key == key).unwrap();
        assert_eq!(entry.size, Some(payload.len()));
    }

    #[tokio::test]
    async fn test_binary_bytes_roundtrip() {
        let temp_dir = TempDir::new().unwrap();
        let storage_dir = temp_dir.path();

        let client = FileSystemKeyValueStoreClient::open(None, None, None, storage_dir)
            .await
            .unwrap();

        // Store binary data verbatim — no encoding, no inference.
        let raw_bytes: Vec<u8> = vec![0x00, 0x01, 0x02, 0x03, 0x04, 0x89, 0xFF];

        client
            .set_value(
                "binary-key",
                &raw_bytes,
                "application/octet-stream".to_string(),
            )
            .await
            .unwrap();

        let (bytes, content_type, size) = read_back(&client, "binary-key").await.unwrap();
        assert_eq!(bytes, raw_bytes);
        assert_eq!(content_type, "application/octet-stream");
        assert_eq!(size, Some(raw_bytes.len()));
    }

    #[tokio::test]
    async fn test_content_type_stored_verbatim() {
        let temp_dir = TempDir::new().unwrap();
        let storage_dir = temp_dir.path();

        let client = FileSystemKeyValueStoreClient::open(None, None, None, storage_dir)
            .await
            .unwrap();

        // The client must NOT infer or rewrite the content type — even a totally
        // arbitrary one passes through untouched.
        client
            .set_value(
                "weird",
                b"<svg/>",
                "image/svg+xml; charset=utf-8".to_string(),
            )
            .await
            .unwrap();

        let (bytes, content_type, _) = read_back(&client, "weird").await.unwrap();
        assert_eq!(bytes, b"<svg/>");
        assert_eq!(content_type, "image/svg+xml; charset=utf-8");
    }

    #[tokio::test]
    async fn test_get_missing_key_returns_none() {
        let temp_dir = TempDir::new().unwrap();
        let storage_dir = temp_dir.path();

        let client = FileSystemKeyValueStoreClient::open(None, None, None, storage_dir)
            .await
            .unwrap();

        assert!(client.get_value("nope", true).await.unwrap().is_none());
    }

    #[tokio::test]
    async fn test_delete_value() {
        let temp_dir = TempDir::new().unwrap();
        let storage_dir = temp_dir.path();

        let client = FileSystemKeyValueStoreClient::open(None, None, None, storage_dir)
            .await
            .unwrap();

        client
            .set_value("key1", b"1", "application/json; charset=utf-8".to_string())
            .await
            .unwrap();

        assert!(client.record_exists("key1", true).await);
        client.delete_value("key1").await.unwrap();
        assert!(!client.record_exists("key1", true).await);
    }

    #[tokio::test]
    async fn test_sidecar_less_read_requires_opt_in() {
        let temp_dir = TempDir::new().unwrap();
        let storage_dir = temp_dir.path();

        let client = FileSystemKeyValueStoreClient::open(None, None, None, storage_dir)
            .await
            .unwrap();

        // Hand-place a value file with NO sidecar (e.g. a CLI-written INPUT.json).
        // The on-disk name is the LITERAL "INPUT.json" — encode_key preserves the
        // dot (it matches quote(safe='')), so addressing key "INPUT.json" lands on
        // exactly this file. This is what makes the bare-INPUT probe work.
        let payload = br#"{"foo":"bar"}"#;
        assert_eq!(encode_key("INPUT.json"), "INPUT.json");
        let value_path = client.path().join("INPUT.json");
        tokio::fs::write(&value_path, payload).await.unwrap();

        // Default (strict): a value file without a sidecar is "not a record".
        assert!(client
            .get_value("INPUT.json", true)
            .await
            .unwrap()
            .is_none());
        assert!(!client.record_exists("INPUT.json", true).await);

        // Opt-in: the bytes are served with synthesized, non-inferred metadata.
        let (path, meta) = client
            .get_value("INPUT.json", false)
            .await
            .unwrap()
            .unwrap();
        let bytes = tokio::fs::read(&path).await.unwrap();
        assert_eq!(bytes, payload);
        assert_eq!(meta.key, "INPUT.json");
        // No extension-based MIME inference — the generic sentinel only.
        assert_eq!(meta.content_type, "application/octet-stream");
        assert_eq!(meta.size, Some(payload.len()));

        assert!(client.record_exists("INPUT.json", false).await);

        // A genuinely missing file is still absent under either flag.
        assert!(client.get_value("nope", false).await.unwrap().is_none());
        assert!(!client.record_exists("nope", false).await);
    }

    #[tokio::test]
    async fn test_resolve_value_prefers_tracked_record() {
        let temp_dir = TempDir::new().unwrap();
        let storage_dir = temp_dir.path();

        let client = FileSystemKeyValueStoreClient::open(None, None, None, storage_dir)
            .await
            .unwrap();

        // A properly-tracked record for "INPUT" must win over any bare-file
        // probe, and its verbatim sidecar content type is preserved (the
        // caller-declared fallback content types are NOT applied).
        client
            .set_value("INPUT", br#"{"x":1}"#, "application/json".to_string())
            .await
            .unwrap();

        let fallbacks = [("", ""), (".json", "application/json"), (".bin", "")];
        let (path, meta) = client.resolve_value("INPUT", &fallbacks).await.unwrap().unwrap();
        let bytes = tokio::fs::read(&path).await.unwrap();
        assert_eq!(bytes, br#"{"x":1}"#);
        assert_eq!(meta.key, "INPUT");
        assert_eq!(meta.content_type, "application/json");
    }

    #[tokio::test]
    async fn test_resolve_value_falls_back_to_bare_file_with_inferred_type() {
        let temp_dir = TempDir::new().unwrap();
        let storage_dir = temp_dir.path();

        let client = FileSystemKeyValueStoreClient::open(None, None, None, storage_dir)
            .await
            .unwrap();

        // No tracked "INPUT" record; instead a bare "INPUT.json" file (no
        // sidecar), as a CLI/platform writer would leave it.
        let payload = br#"{"foo":"bar"}"#;
        tokio::fs::write(client.path().join("INPUT.json"), payload)
            .await
            .unwrap();

        let fallbacks = [
            ("", ""),
            (".json", "application/json"),
            (".txt", "text/plain"),
            (".bin", ""),
        ];
        let (path, meta) = client.resolve_value("INPUT", &fallbacks).await.unwrap().unwrap();
        let bytes = tokio::fs::read(&path).await.unwrap();
        assert_eq!(bytes, payload);
        // Re-keyed to the requested key, not the on-disk "INPUT.json".
        assert_eq!(meta.key, "INPUT");
        // Caller-declared content type for the matched extension is applied.
        assert_eq!(meta.content_type, "application/json");
        assert_eq!(meta.size, Some(payload.len()));
    }

    #[tokio::test]
    async fn test_resolve_value_bare_empty_extension_keeps_octet_stream() {
        let temp_dir = TempDir::new().unwrap();
        let storage_dir = temp_dir.path();

        let client = FileSystemKeyValueStoreClient::open(None, None, None, storage_dir)
            .await
            .unwrap();

        // A bare file under the literal key with an empty-extension fallback
        // (empty declared content type) keeps the synthesized octet-stream.
        tokio::fs::write(client.path().join("INPUT"), b"raw")
            .await
            .unwrap();

        let fallbacks = [("", ""), (".json", "application/json")];
        let (_, meta) = client.resolve_value("INPUT", &fallbacks).await.unwrap().unwrap();
        assert_eq!(meta.key, "INPUT");
        assert_eq!(meta.content_type, "application/octet-stream");
    }

    #[tokio::test]
    async fn test_resolve_value_missing_returns_none() {
        let temp_dir = TempDir::new().unwrap();
        let storage_dir = temp_dir.path();

        let client = FileSystemKeyValueStoreClient::open(None, None, None, storage_dir)
            .await
            .unwrap();

        let fallbacks = [("", ""), (".json", "application/json")];
        assert!(client
            .resolve_value("nope", &fallbacks)
            .await
            .unwrap()
            .is_none());
    }

    #[tokio::test]
    async fn test_resolve_existing_key() {
        let temp_dir = TempDir::new().unwrap();
        let storage_dir = temp_dir.path();

        let client = FileSystemKeyValueStoreClient::open(None, None, None, storage_dir)
            .await
            .unwrap();

        let fallbacks = ["", ".json", ".txt", ".bin"];

        // Tracked record resolves to the literal key.
        client
            .set_value("tracked", b"x", "text/plain".to_string())
            .await
            .unwrap();
        assert_eq!(
            client.resolve_existing_key("tracked", &fallbacks).await,
            Some("tracked".to_string())
        );

        // Bare file resolves to the matched on-disk filename (key + extension).
        tokio::fs::write(client.path().join("INPUT.json"), b"{}")
            .await
            .unwrap();
        assert_eq!(
            client.resolve_existing_key("INPUT", &fallbacks).await,
            Some("INPUT.json".to_string())
        );

        // Nothing matches → None.
        assert_eq!(client.resolve_existing_key("nope", &fallbacks).await, None);
    }

    #[tokio::test]
    async fn test_sidecar_present_ignores_flag() {
        let temp_dir = TempDir::new().unwrap();
        let storage_dir = temp_dir.path();

        let client = FileSystemKeyValueStoreClient::open(None, None, None, storage_dir)
            .await
            .unwrap();

        // A properly-written record (value + sidecar) reads identically under
        // both flag values — the flag only affects the missing-sidecar branch.
        client
            .set_value("tracked", b"hi", "text/plain; charset=utf-8".to_string())
            .await
            .unwrap();

        for require in [true, false] {
            let (_, meta) = client.get_value("tracked", require).await.unwrap().unwrap();
            assert_eq!(meta.content_type, "text/plain; charset=utf-8");
            assert!(client.record_exists("tracked", require).await);
        }
    }

    #[tokio::test]
    async fn test_purge_with_keep_list() {
        let temp_dir = TempDir::new().unwrap();
        let storage_dir = temp_dir.path();

        let client = FileSystemKeyValueStoreClient::open(None, None, None, storage_dir)
            .await
            .unwrap();

        let ct = "application/json; charset=utf-8".to_string();
        client.set_value("INPUT", b"in", ct.clone()).await.unwrap();
        client.set_value("other", b"x", ct.clone()).await.unwrap();

        // A bare value file (no sidecar) placed out-of-band, NOT in the keep list.
        let bare_path = client.path().join("INPUT.json");
        tokio::fs::write(&bare_path, b"bare").await.unwrap();

        // Keep exactly the "INPUT" key — by exact key, with no extension magic.
        client.purge(&["INPUT".to_string()]).await.unwrap();

        // The kept record (value + sidecar) survives.
        assert!(client.record_exists("INPUT", true).await);
        // The non-kept tracked record is gone (value + sidecar).
        assert!(!client.record_exists("other", true).await);
        // The bare INPUT.json is gone: "INPUT" the key encodes to filename "INPUT",
        // not "INPUT.json", so it is NOT spared. No stem/extension matching.
        assert!(!bare_path.exists());
        // Store metadata is always kept.
        assert!(client.metadata_path().exists());
    }

    #[tokio::test]
    async fn test_purge_empty_keep_list_clears_everything() {
        let temp_dir = TempDir::new().unwrap();
        let storage_dir = temp_dir.path();

        let client = FileSystemKeyValueStoreClient::open(None, None, None, storage_dir)
            .await
            .unwrap();

        let ct = "application/json; charset=utf-8".to_string();
        client.set_value("a", b"1", ct.clone()).await.unwrap();
        client.set_value("b", b"2", ct.clone()).await.unwrap();
        // A bare file too.
        tokio::fs::write(client.path().join("INPUT.json"), b"bare")
            .await
            .unwrap();

        client.purge(&[]).await.unwrap();

        assert!(!client.record_exists("a", true).await);
        assert!(!client.record_exists("b", true).await);
        assert!(!client.path().join("INPUT.json").exists());
        // Only the store metadata remains.
        let mut remaining = Vec::new();
        let mut entries = fs::read_dir(client.path()).await.unwrap();
        while let Some(e) = entries.next_entry().await.unwrap() {
            remaining.push(e.file_name().to_string_lossy().into_owned());
        }
        assert_eq!(remaining, vec![METADATA_FILENAME.to_string()]);
    }

    #[tokio::test]
    async fn test_iterate_keys_page() {
        let temp_dir = TempDir::new().unwrap();
        let storage_dir = temp_dir.path();

        let client = FileSystemKeyValueStoreClient::open(None, None, None, storage_dir)
            .await
            .unwrap();

        let ct = "application/json; charset=utf-8".to_string();
        client.set_value("alpha", b"1", ct.clone()).await.unwrap();
        client.set_value("beta", b"2", ct.clone()).await.unwrap();
        client.set_value("gamma", b"3", ct.clone()).await.unwrap();

        // Fetch all at once (large page_size)
        let page = client
            .iterate_keys_page(None, None, 1000, None)
            .await
            .unwrap();
        assert_eq!(page.items.len(), 3);
        assert!(!page.has_more);

        // With limit
        let page = client
            .iterate_keys_page(None, Some(2), 1000, None)
            .await
            .unwrap();
        assert_eq!(page.items.len(), 2);
        assert!(!page.has_more);

        // Paginate with page_size=2
        let page1 = client.iterate_keys_page(None, None, 2, None).await.unwrap();
        assert_eq!(page1.items.len(), 2);
        assert!(page1.has_more);

        // Second page using cursor from last key
        let last_key = &page1.items.last().unwrap().key;
        let page2 = client
            .iterate_keys_page(Some(last_key), None, 2, None)
            .await
            .unwrap();
        assert_eq!(page2.items.len(), 1);
        assert!(!page2.has_more);

        // Cursor-based: exclusive_start_key
        let page = client
            .iterate_keys_page(Some("alpha"), None, 1000, None)
            .await
            .unwrap();
        assert_eq!(page.items.len(), 2);
        assert_eq!(page.items[0].key, "beta");
        assert_eq!(page.items[1].key, "gamma");
    }

    #[tokio::test]
    async fn test_iterate_keys_page_with_prefix() {
        let temp_dir = TempDir::new().unwrap();
        let storage_dir = temp_dir.path();

        let client = FileSystemKeyValueStoreClient::open(None, None, None, storage_dir)
            .await
            .unwrap();

        let ct = "application/json; charset=utf-8".to_string();
        for key in ["foo:1", "foo:2", "foo:3", "bar:1", "baz"] {
            client.set_value(key, b"x", ct.clone()).await.unwrap();
        }

        // Prefix filters to matching keys only, in lexical order.
        let page = client
            .iterate_keys_page(None, None, 1000, Some("foo:"))
            .await
            .unwrap();
        assert_eq!(page.items.len(), 3);
        assert!(!page.has_more);
        assert_eq!(
            page.items
                .iter()
                .map(|m| m.key.as_str())
                .collect::<Vec<_>>(),
            ["foo:1", "foo:2", "foo:3"]
        );

        // Prefix + limit: has_more reflects the *filtered* set, not the whole store.
        let page = client
            .iterate_keys_page(None, Some(2), 1000, Some("foo:"))
            .await
            .unwrap();
        assert_eq!(page.items.len(), 2);
        assert_eq!(page.items[0].key, "foo:1");
        assert_eq!(page.items[1].key, "foo:2");

        // Prefix + page_size smaller than the match count sets has_more.
        let page1 = client
            .iterate_keys_page(None, None, 2, Some("foo:"))
            .await
            .unwrap();
        assert_eq!(page1.items.len(), 2);
        assert!(page1.has_more);

        // Prefix + cursor: continue after the last key of page1, still prefix-scoped.
        let last_key = &page1.items.last().unwrap().key;
        let page2 = client
            .iterate_keys_page(Some(last_key), None, 1000, Some("foo:"))
            .await
            .unwrap();
        assert_eq!(page2.items.len(), 1);
        assert_eq!(page2.items[0].key, "foo:3");

        // A prefix matching nothing yields an empty page.
        let page = client
            .iterate_keys_page(None, None, 1000, Some("nope"))
            .await
            .unwrap();
        assert!(page.items.is_empty());
        assert!(!page.has_more);
    }

    #[tokio::test]
    async fn test_special_characters_in_key() {
        let temp_dir = TempDir::new().unwrap();
        let storage_dir = temp_dir.path();

        let client = FileSystemKeyValueStoreClient::open(None, None, None, storage_dir)
            .await
            .unwrap();

        client
            .set_value(
                "path/to/key with spaces",
                b"value",
                "text/plain; charset=utf-8".to_string(),
            )
            .await
            .unwrap();

        let (bytes, content_type, _) = read_back(&client, "path/to/key with spaces").await.unwrap();
        assert_eq!(bytes, b"value");
        assert_eq!(content_type, "text/plain; charset=utf-8");
    }

    /// A large opaque value must round-trip byte-for-byte through the
    /// `atomic_write` + FFI byte path without truncation, corruption, or OOM, and
    /// its `size` must be backfilled to the real length. Mirrors the deleted
    /// crawlee-js `no-crash-on-big-buffers` test, but pinned at the library level
    /// where the write path now lives. (1 MiB keeps the test fast; the original
    /// JS bug was a stack overflow on large buffers, which this byte path avoids
    /// entirely.)
    #[tokio::test]
    async fn test_large_value_roundtrip() {
        let temp_dir = TempDir::new().unwrap();
        let storage_dir = temp_dir.path();

        let client = FileSystemKeyValueStoreClient::open(None, None, None, storage_dir)
            .await
            .unwrap();

        // 1 MiB of non-trivial (non-zero, position-dependent) bytes so a partial
        // write or off-by-some truncation would be caught.
        let size = 1024 * 1024;
        let payload: Vec<u8> = (0..size).map(|i| (i % 251) as u8).collect();

        client
            .set_value("big.bin", &payload, "application/octet-stream".to_string())
            .await
            .unwrap();

        let (bytes, content_type, persisted_size) = read_back(&client, "big.bin").await.unwrap();
        assert_eq!(
            bytes.len(),
            size,
            "round-tripped value must keep its length"
        );
        assert_eq!(bytes, payload, "round-tripped value must be byte-identical");
        assert_eq!(content_type, "application/octet-stream");
        assert_eq!(
            persisted_size,
            Some(size),
            "size must reflect the real length"
        );
    }

    /// Records written to a KVS must survive closing and reopening the same
    /// on-disk store by name. The dataset and request-queue clients already have
    /// reopen coverage; this closes the KVS gap. Mirrors the deleted crawlee-js /
    /// crawlee-python `data persistence across reopens` tests.
    #[tokio::test]
    async fn test_reopen_preserves_records() {
        let temp_dir = TempDir::new().unwrap();
        let storage_dir = temp_dir.path();

        {
            let client = FileSystemKeyValueStoreClient::open(
                None,
                Some("kvs".to_string()),
                None,
                storage_dir,
            )
            .await
            .unwrap();

            client
                .set_value(
                    "greeting",
                    b"hello",
                    "text/plain; charset=utf-8".to_string(),
                )
                .await
                .unwrap();
            client
                .set_value("payload", br#"{"x":1}"#, "application/json".to_string())
                .await
                .unwrap();
        }

        // Reopen the same store by name, emulating a fresh process.
        let reopened =
            FileSystemKeyValueStoreClient::open(None, Some("kvs".to_string()), None, storage_dir)
                .await
                .unwrap();

        let (bytes, content_type, _) = read_back(&reopened, "greeting").await.unwrap();
        assert_eq!(bytes, b"hello");
        assert_eq!(content_type, "text/plain; charset=utf-8");

        let (bytes, content_type, _) = read_back(&reopened, "payload").await.unwrap();
        assert_eq!(bytes, br#"{"x":1}"#);
        assert_eq!(content_type, "application/json");

        // A key that was never written must still be absent.
        assert!(reopened.get_value("missing", true).await.unwrap().is_none());
    }

    /// `open()` must tolerate the datetime formats that other writers emit in
    /// `__metadata__.json`: the JS-style `Z` suffix (e.g. `...123Z`) and a
    /// varying number of fractional-second digits. AGENTS.md lists this as an
    /// explicit compatibility constraint; this guards the `deserialize_datetime`
    /// fallbacks against regression.
    #[tokio::test]
    async fn test_open_tolerates_z_suffix_and_varying_fractions() {
        let temp_dir = TempDir::new().unwrap();
        let storage_dir = temp_dir.path();

        // Hand-write a metadata file as a JS MemoryStorage writer would: `Z`
        // suffix, millisecond (3-digit) precision.
        let kvs_dir = storage_dir.join("key_value_stores").join("legacy-kvs");
        fs::create_dir_all(&kvs_dir).await.unwrap();
        let legacy_meta = r#"{
  "id": "kvsid123",
  "name": "legacy-kvs",
  "accessedAt": "2024-01-15T10:30:00.123Z",
  "createdAt": "2024-01-15T10:30:00Z",
  "modifiedAt": "2024-01-15T10:30:00.123456+00:00"
}"#;
        fs::write(kvs_dir.join(METADATA_FILENAME), legacy_meta)
            .await
            .unwrap();

        let client = FileSystemKeyValueStoreClient::open(
            None,
            Some("legacy-kvs".to_string()),
            None,
            storage_dir,
        )
        .await
        .unwrap();

        let meta = client.get_metadata().await;
        assert_eq!(meta.base.id, "kvsid123");

        // All three timestamps must parse to the same 2024-01-15T10:30:00 UTC
        // instant (modulo sub-second precision), proving each format variant was
        // accepted rather than silently defaulted.
        use chrono::{TimeZone, Utc};
        let expected_secs = Utc
            .with_ymd_and_hms(2024, 1, 15, 10, 30, 0)
            .unwrap()
            .timestamp();
        assert_eq!(meta.base.created_at.timestamp(), expected_secs);
        assert_eq!(meta.base.accessed_at.timestamp(), expected_secs);
        assert_eq!(meta.base.modified_at.timestamp(), expected_secs);
        // The `.123Z` fractional part must survive too.
        assert_eq!(meta.base.accessed_at.timestamp_subsec_millis(), 123);
    }
}
