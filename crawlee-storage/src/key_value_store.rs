use std::path::{Path, PathBuf};

use chrono::Utc;
use serde_json::Value;
use tokio::fs;
use tokio::sync::Mutex;
use tracing::warn;

use crate::models::{
    KeyValueStoreMetadata, KeyValueStoreRecord, KeyValueStoreRecordMetadata, KvsValue,
};
use crate::utils::{
    atomic_write, crypto_random_object_id, encode_key, find_storage_by_id, json_dumps,
    json_dumps_value, validate_exclusive_args, Result, StorageError, METADATA_FILENAME,
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
    pub async fn open(
        id: Option<String>,
        name: Option<String>,
        alias: Option<String>,
        storage_dir: &Path,
    ) -> Result<Self> {
        validate_exclusive_args(&id, &name, &alias)?;

        let path = if let Some(ref id_val) = id {
            find_storage_by_id(storage_dir, STORAGE_SUBDIR, id_val)
                .await?
                .ok_or_else(|| {
                    StorageError::NotFound(format!("Key-value store with id '{id_val}' not found"))
                })?
        } else {
            let dir_name = name.as_deref().or(alias.as_deref()).unwrap_or(DEFAULT_NAME);
            storage_dir.join(STORAGE_SUBDIR).join(dir_name)
        };

        let metadata_path = path.join(METADATA_FILENAME);

        let metadata = if metadata_path.exists() {
            let content = fs::read_to_string(&metadata_path).await?;
            serde_json::from_str::<KeyValueStoreMetadata>(&content)?
        } else {
            // Only `name` goes into metadata, not alias
            let new_id = id.unwrap_or_else(|| crypto_random_object_id(17));
            let meta = KeyValueStoreMetadata::new(new_id, name);
            fs::create_dir_all(&path).await?;
            let json = json_dumps_value(&meta)?;
            atomic_write(&metadata_path, json.as_bytes()).await?;
            meta
        };

        Ok(Self {
            metadata: Mutex::new(metadata),
            path,
        })
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

        let now = Utc::now();
        meta.base.accessed_at = now;
        meta.base.modified_at = now;

        let json = json_dumps_value(&*meta)?;
        atomic_write(&self.metadata_path(), json.as_bytes()).await?;

        Ok(())
    }

    /// Get a record by key. Returns None if the key doesn't exist.
    pub async fn get_value(&self, key: &str) -> Result<Option<KeyValueStoreRecord>> {
        let encoded = encode_key(key);
        let value_path = self.path.join(&encoded);
        let sidecar_path = self.path.join(format!("{encoded}.{METADATA_FILENAME}"));

        // Always update accessed_at on read, even for missing keys
        {
            let mut meta = self.metadata.lock().await;
            meta.base.accessed_at = Utc::now();
            let json = json_dumps_value(&*meta)?;
            atomic_write(&self.metadata_path(), json.as_bytes()).await?;
        }

        if !value_path.exists() || !sidecar_path.exists() {
            return Ok(None);
        }

        // Read sidecar metadata
        let sidecar_content = fs::read_to_string(&sidecar_path).await?;
        let record_meta: KeyValueStoreRecordMetadata = serde_json::from_str(&sidecar_content)?;

        // Read raw bytes once, then parse based on content type
        let raw_bytes = fs::read(&value_path).await?;
        let size = raw_bytes.len();
        let value = self.parse_value(&raw_bytes, &record_meta.content_type)?;

        Ok(Some(KeyValueStoreRecord {
            key: key.to_string(),
            content_type: record_meta.content_type,
            size: Some(size),
            value,
        }))
    }

    /// Set a value for a key.
    ///
    /// The `value` is a [`KvsValue`]:
    /// - `KvsValue::None` → stored with content_type `"application/x-none"`
    /// - `KvsValue::Json(v)` → stored as pretty-printed JSON
    /// - `KvsValue::Text(s)` → stored as UTF-8 text
    /// - `KvsValue::Binary(bytes)` → stored as raw bytes
    ///
    /// If `content_type` is None, it's inferred from the value variant.
    pub async fn set_value(
        &self,
        key: &str,
        value: KvsValue,
        content_type: Option<String>,
    ) -> Result<()> {
        let content_type = content_type.unwrap_or_else(|| {
            match &value {
                KvsValue::None => "application/x-none",
                KvsValue::Json(_) => "application/json",
                KvsValue::Text(_) => "text/plain",
                KvsValue::Binary(_) => "application/octet-stream",
            }
            .to_string()
        });

        let encoded = encode_key(key);
        let value_path = self.path.join(&encoded);
        let sidecar_path = self.path.join(format!("{encoded}.{METADATA_FILENAME}"));

        // Serialize value to bytes
        let data = self.serialize_value(&value)?;

        // Write value file
        atomic_write(&value_path, &data).await?;

        // Write sidecar metadata
        let record_meta = KeyValueStoreRecordMetadata {
            key: key.to_string(),
            content_type,
            size: Some(data.len()),
        };
        let sidecar_json = json_dumps_value(&record_meta)?;
        atomic_write(&sidecar_path, sidecar_json.as_bytes()).await?;

        // Update store metadata
        {
            let mut meta = self.metadata.lock().await;
            let now = Utc::now();
            meta.base.accessed_at = now;
            meta.base.modified_at = now;
            let json = json_dumps_value(&*meta)?;
            atomic_write(&self.metadata_path(), json.as_bytes()).await?;
        }

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
            let now = Utc::now();
            meta.base.accessed_at = now;
            meta.base.modified_at = now;
            let json = json_dumps_value(&*meta)?;
            atomic_write(&self.metadata_path(), json.as_bytes()).await?;
        }

        Ok(())
    }

    /// List keys in the store, with optional cursor-based pagination.
    pub async fn iterate_keys(
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
                Ok(record_meta) => {
                    // Apply cursor filter
                    if let Some(start_key) = exclusive_start_key {
                        if record_meta.key.as_str() <= start_key {
                            continue;
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
    pub fn get_public_url(&self, key: &str) -> String {
        let encoded = encode_key(key);
        let value_path = self.path.join(&encoded);
        format!("file://{}", value_path.display())
    }

    /// Check if a record exists for a key.
    pub async fn record_exists(&self, key: &str) -> bool {
        let encoded = encode_key(key);
        let value_path = self.path.join(&encoded);
        let sidecar_path = self.path.join(format!("{encoded}.{METADATA_FILENAME}"));
        value_path.exists() && sidecar_path.exists()
    }

    // ─── Private ────────────────────────────────────────────────────────────

    fn parse_value(&self, raw_bytes: &[u8], content_type: &str) -> Result<KvsValue> {
        if content_type == "application/x-none" {
            return Ok(KvsValue::None);
        }

        if content_type == "application/json" {
            let text = std::str::from_utf8(raw_bytes).map_err(|e| {
                StorageError::InvalidArgs(format!("Invalid UTF-8 in JSON value: {e}"))
            })?;
            let parsed = serde_json::from_str::<Value>(text)?;
            return Ok(KvsValue::Json(parsed));
        }

        if content_type.starts_with("text/") {
            let text = String::from_utf8(raw_bytes.to_vec()).map_err(|e| {
                StorageError::InvalidArgs(format!("Invalid UTF-8 in text value: {e}"))
            })?;
            return Ok(KvsValue::Text(text));
        }

        // Binary data — return raw bytes directly.
        // Each binding layer converts to its native bytes type (Python bytes, Node Buffer, etc.).
        Ok(KvsValue::Binary(raw_bytes.to_vec()))
    }

    fn serialize_value(&self, value: &KvsValue) -> Result<Vec<u8>> {
        match value {
            KvsValue::None => Ok(Vec::new()),
            KvsValue::Json(v) => {
                let json = json_dumps(v)?;
                Ok(json.into_bytes())
            }
            KvsValue::Text(s) => Ok(s.as_bytes().to_vec()),
            KvsValue::Binary(bytes) => Ok(bytes.clone()),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    #[tokio::test]
    async fn test_create_and_set_value() {
        let temp_dir = TempDir::new().unwrap();
        let storage_dir = temp_dir.path();

        let client = FileSystemKeyValueStoreClient::open(None, None, None, storage_dir)
            .await
            .unwrap();

        // Set a JSON value
        client
            .set_value(
                "my-key",
                KvsValue::Json(serde_json::json!({"hello": "world"})),
                None,
            )
            .await
            .unwrap();

        // Get it back
        let record = client.get_value("my-key").await.unwrap().unwrap();
        assert_eq!(record.key, "my-key");
        assert_eq!(record.content_type, "application/json");
        match &record.value {
            KvsValue::Json(v) => assert_eq!(v, &serde_json::json!({"hello": "world"})),
            other => panic!("Expected KvsValue::Json, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn test_set_text_value() {
        let temp_dir = TempDir::new().unwrap();
        let storage_dir = temp_dir.path();

        let client = FileSystemKeyValueStoreClient::open(None, None, None, storage_dir)
            .await
            .unwrap();

        client
            .set_value(
                "greeting",
                KvsValue::Text("hello".to_string()),
                Some("text/plain".to_string()),
            )
            .await
            .unwrap();

        let record = client.get_value("greeting").await.unwrap().unwrap();
        assert_eq!(record.content_type, "text/plain");
        match &record.value {
            KvsValue::Text(s) => assert_eq!(s, "hello"),
            other => panic!("Expected KvsValue::Text, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn test_null_value() {
        let temp_dir = TempDir::new().unwrap();
        let storage_dir = temp_dir.path();

        let client = FileSystemKeyValueStoreClient::open(None, None, None, storage_dir)
            .await
            .unwrap();

        client
            .set_value("empty", KvsValue::None, None)
            .await
            .unwrap();

        let record = client.get_value("empty").await.unwrap().unwrap();
        assert_eq!(record.content_type, "application/x-none");
        assert!(matches!(record.value, KvsValue::None));
    }

    #[tokio::test]
    async fn test_delete_value() {
        let temp_dir = TempDir::new().unwrap();
        let storage_dir = temp_dir.path();

        let client = FileSystemKeyValueStoreClient::open(None, None, None, storage_dir)
            .await
            .unwrap();

        client
            .set_value("key1", KvsValue::Json(serde_json::json!(1)), None)
            .await
            .unwrap();

        assert!(client.record_exists("key1").await);
        client.delete_value("key1").await.unwrap();
        assert!(!client.record_exists("key1").await);
    }

    #[tokio::test]
    async fn test_iterate_keys() {
        let temp_dir = TempDir::new().unwrap();
        let storage_dir = temp_dir.path();

        let client = FileSystemKeyValueStoreClient::open(None, None, None, storage_dir)
            .await
            .unwrap();

        client
            .set_value("alpha", KvsValue::Json(serde_json::json!(1)), None)
            .await
            .unwrap();
        client
            .set_value("beta", KvsValue::Json(serde_json::json!(2)), None)
            .await
            .unwrap();
        client
            .set_value("gamma", KvsValue::Json(serde_json::json!(3)), None)
            .await
            .unwrap();

        let keys = client.iterate_keys(None, None).await.unwrap();
        assert_eq!(keys.len(), 3);

        // With limit
        let keys = client.iterate_keys(None, Some(2)).await.unwrap();
        assert_eq!(keys.len(), 2);
    }

    #[tokio::test]
    async fn test_binary_value_roundtrip() {
        let temp_dir = TempDir::new().unwrap();
        let storage_dir = temp_dir.path();

        let client = FileSystemKeyValueStoreClient::open(None, None, None, storage_dir)
            .await
            .unwrap();

        // Store binary data directly — no base64 encoding needed
        let raw_bytes: Vec<u8> = vec![0x00, 0x01, 0x02, 0x03, 0x04, 0x89, 0xFF];

        client
            .set_value(
                "binary-key",
                KvsValue::Binary(raw_bytes.clone()),
                Some("application/octet-stream".to_string()),
            )
            .await
            .unwrap();

        // Read it back
        let record = client.get_value("binary-key").await.unwrap().unwrap();
        assert_eq!(record.content_type, "application/octet-stream");
        assert_eq!(record.size, Some(raw_bytes.len()));

        // The value should be raw bytes, no base64 intermediary
        match &record.value {
            KvsValue::Binary(bytes) => assert_eq!(bytes, &raw_bytes),
            other => panic!("Expected KvsValue::Binary, got {other:?}"),
        }
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
                KvsValue::Text("value".to_string()),
                None,
            )
            .await
            .unwrap();

        let record = client
            .get_value("path/to/key with spaces")
            .await
            .unwrap()
            .unwrap();
        match &record.value {
            KvsValue::Text(s) => assert_eq!(s, "value"),
            other => panic!("Expected KvsValue::Text, got {other:?}"),
        }
    }
}
