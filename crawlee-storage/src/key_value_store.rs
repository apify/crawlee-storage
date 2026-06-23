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
    pub async fn purge(&self) -> Result<()> {
        let mut meta = self.metadata.lock().await;

        match fs::read_dir(&self.path).await {
            Ok(mut entries) => {
                while let Some(entry) = entries.next_entry().await? {
                    let path = entry.path();
                    if path.is_file() {
                        if let Some(name) = path.file_name().and_then(|n| n.to_str()) {
                            if name != METADATA_FILENAME {
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
    ) -> Result<KvsKeysPage> {
        // Fetch one extra beyond the page to detect whether more keys exist.
        let fetch_limit = match limit {
            Some(remaining) => page_size.min(remaining),
            None => page_size,
        };

        let results = self
            .list_keys_raw(exclusive_start_key, Some(fetch_limit + 1))
            .await?;

        let has_more =
            results.len() > fetch_limit && limit.is_none_or(|remaining| fetch_limit < remaining);
        let items: Vec<KeyValueStoreRecordMetadata> =
            results.into_iter().take(fetch_limit).collect();

        Ok(KvsKeysPage { items, has_more })
    }

    /// Internal helper: list keys with cursor and limit, returning a flat Vec.
    async fn list_keys_raw(
        &self,
        exclusive_start_key: Option<&str>,
        limit: Option<usize>,
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
    pub async fn get_value(
        &self,
        key: &str,
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

        if !value_path.exists() || !sidecar_path.exists() {
            return Ok(None);
        }

        let sidecar_content = fs::read_to_string(&sidecar_path).await?;
        let mut record_meta: KeyValueStoreRecordMetadata = serde_json::from_str(&sidecar_content)?;

        // Backfill `size` for foreign/legacy sidecars that omit it (this
        // library always writes it, but crawlee-JS / older Python clients may
        // not) by stating the value file.
        if record_meta.size.is_none() {
            if let Ok(file_meta) = fs::metadata(&value_path).await {
                record_meta.size = Some(file_meta.len() as usize);
            }
        }

        Ok(Some((value_path, record_meta)))
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
    pub async fn record_exists(&self, key: &str) -> bool {
        let encoded = encode_key(key);
        let value_path = self.path.join(&encoded);
        let sidecar_path = self.path.join(format!("{encoded}.{METADATA_FILENAME}"));
        value_path.exists() && sidecar_path.exists()
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
        let (path, meta) = client.get_value(key).await.unwrap()?;
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
            .set_value("empty", b"", "application/x-none".to_string())
            .await
            .unwrap();

        let (bytes, content_type, size) = read_back(&client, "empty").await.unwrap();
        assert!(bytes.is_empty());
        assert_eq!(content_type, "application/x-none");
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
        let (_, meta) = client.get_value(key).await.unwrap().unwrap();
        assert_eq!(meta.size, Some(payload.len()));

        // The list/iterate path backfills too.
        let page = client.iterate_keys_page(None, None, 1000).await.unwrap();
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

        assert!(client.get_value("nope").await.unwrap().is_none());
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

        assert!(client.record_exists("key1").await);
        client.delete_value("key1").await.unwrap();
        assert!(!client.record_exists("key1").await);
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
        let page = client.iterate_keys_page(None, None, 1000).await.unwrap();
        assert_eq!(page.items.len(), 3);
        assert!(!page.has_more);

        // With limit
        let page = client.iterate_keys_page(None, Some(2), 1000).await.unwrap();
        assert_eq!(page.items.len(), 2);
        assert!(!page.has_more);

        // Paginate with page_size=2
        let page1 = client.iterate_keys_page(None, None, 2).await.unwrap();
        assert_eq!(page1.items.len(), 2);
        assert!(page1.has_more);

        // Second page using cursor from last key
        let last_key = &page1.items.last().unwrap().key;
        let page2 = client
            .iterate_keys_page(Some(last_key), None, 2)
            .await
            .unwrap();
        assert_eq!(page2.items.len(), 1);
        assert!(!page2.has_more);

        // Cursor-based: exclusive_start_key
        let page = client
            .iterate_keys_page(Some("alpha"), None, 1000)
            .await
            .unwrap();
        assert_eq!(page.items.len(), 2);
        assert_eq!(page.items[0].key, "beta");
        assert_eq!(page.items[1].key, "gamma");
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
}
