use std::path::{Path, PathBuf};

use chrono::Utc;
use serde_json::Value;
use tokio::fs;
use tokio::sync::Mutex;
use tracing::warn;

use crate::models::{DatasetItemsListPage, DatasetMetadata};
use crate::utils::{
    atomic_write, crypto_random_object_id, find_storage_by_id, json_dumps, json_dumps_value,
    validate_exclusive_args, Result, StorageError, METADATA_FILENAME,
};

const STORAGE_SUBDIR: &str = "datasets";
const DEFAULT_NAME: &str = "default";
const ITEM_FILENAME_DIGITS: usize = 9;

/// Filesystem-backed dataset client.
///
/// Stores dataset items as individual numbered JSON files in a directory.
///
/// Directory layout:
/// ```text
/// {storage_dir}/datasets/{name}/
/// ├── __metadata__.json
/// ├── 000000001.json
/// ├── 000000002.json
/// └── ...
/// ```
pub struct FileSystemDatasetClient {
    metadata: Mutex<DatasetMetadata>,
    path: PathBuf,
}

impl FileSystemDatasetClient {
    /// Open an existing dataset or create a new one.
    ///
    /// - `id`: Open by ID (scans directories for matching metadata).
    /// - `name`: Open by name (used as directory name). Defaults to "default".
    /// - `storage_dir`: Base storage directory (e.g., "./storage").
    pub async fn open(
        id: Option<String>,
        name: Option<String>,
        storage_dir: &Path,
    ) -> Result<Self> {
        validate_exclusive_args(&id, &name)?;

        let path = if let Some(ref id_val) = id {
            // Find existing dataset by scanning metadata files
            find_storage_by_id(storage_dir, STORAGE_SUBDIR, id_val)
                .await?
                .ok_or_else(|| {
                    StorageError::NotFound(format!("Dataset with id '{id_val}' not found"))
                })?
        } else {
            let dir_name = name.as_deref().unwrap_or(DEFAULT_NAME);
            storage_dir.join(STORAGE_SUBDIR).join(dir_name)
        };

        let metadata_path = path.join(METADATA_FILENAME);

        let metadata = if metadata_path.exists() {
            // Load existing metadata
            let content = fs::read_to_string(&metadata_path).await?;
            serde_json::from_str::<DatasetMetadata>(&content)?
        } else {
            // Create new dataset
            let new_id = id.unwrap_or_else(|| crypto_random_object_id(17));
            let meta = DatasetMetadata::new(new_id, name);
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

    /// Get the dataset metadata.
    pub async fn get_metadata(&self) -> DatasetMetadata {
        self.metadata.lock().await.clone()
    }

    /// Path to the dataset directory.
    pub fn path(&self) -> &Path {
        &self.path
    }

    /// Path to the metadata file.
    pub fn metadata_path(&self) -> PathBuf {
        self.path.join(METADATA_FILENAME)
    }

    /// Delete the entire dataset directory.
    pub async fn drop_storage(&self) -> Result<()> {
        if self.path.exists() {
            fs::remove_dir_all(&self.path).await?;
        }
        Ok(())
    }

    /// Delete all data files but keep metadata. Resets item_count to 0.
    pub async fn purge(&self) -> Result<()> {
        let mut meta = self.metadata.lock().await;

        // Delete all numbered JSON files
        for file in self.get_sorted_data_files().await? {
            fs::remove_file(&file).await?;
        }

        meta.item_count = 0;
        let now = Utc::now();
        meta.base.accessed_at = now;
        meta.base.modified_at = now;

        let json = json_dumps_value(&*meta)?;
        atomic_write(&self.metadata_path(), json.as_bytes()).await?;

        Ok(())
    }

    /// Push one or more items to the dataset.
    pub async fn push_data(&self, data: Value) -> Result<()> {
        let items: Vec<Value> = match data {
            Value::Array(arr) => arr,
            other => vec![other],
        };

        let mut meta = self.metadata.lock().await;

        for item in items {
            let item_id = meta.item_count + 1;
            self.push_item(&item, item_id).await?;
            meta.item_count = item_id;
        }

        let now = Utc::now();
        meta.base.accessed_at = now;
        meta.base.modified_at = now;

        let json = json_dumps_value(&*meta)?;
        atomic_write(&self.metadata_path(), json.as_bytes()).await?;

        Ok(())
    }

    /// Get a paginated list of dataset items.
    pub async fn get_data(
        &self,
        offset: usize,
        limit: usize,
        desc: bool,
        skip_empty: bool,
    ) -> Result<DatasetItemsListPage> {
        let files = {
            let _meta = self.metadata.lock().await;
            self.get_sorted_data_files().await?
        };

        let mut items: Vec<Value> = Vec::new();
        for file in &files {
            match fs::read_to_string(file).await {
                Ok(content) => match serde_json::from_str::<Value>(&content) {
                    Ok(item) => {
                        if skip_empty {
                            if let Value::Object(ref map) = item {
                                if map.is_empty() {
                                    continue;
                                }
                            }
                        }
                        items.push(item);
                    }
                    Err(e) => {
                        warn!("Failed to parse item file {}: {}", file.display(), e);
                    }
                },
                Err(e) => {
                    warn!("Failed to read item file {}: {}", file.display(), e);
                }
            }
        }

        let total = items.len();

        if desc {
            items.reverse();
        }

        let start = offset.min(total);
        let end = (offset + limit).min(total);
        let page_items: Vec<Value> = items[start..end].to_vec();

        // Update accessed_at
        {
            let mut meta = self.metadata.lock().await;
            meta.base.accessed_at = Utc::now();
            let json = json_dumps_value(&*meta)?;
            atomic_write(&self.metadata_path(), json.as_bytes()).await?;
        }

        Ok(DatasetItemsListPage {
            count: page_items.len(),
            offset,
            limit,
            total,
            desc,
            items: page_items,
        })
    }

    /// Iterate over dataset items one by one.
    /// Returns a Vec of items (the binding layer converts this to an async iterator).
    pub async fn iterate_items(
        &self,
        offset: usize,
        limit: Option<usize>,
        desc: bool,
        skip_empty: bool,
    ) -> Result<Vec<Value>> {
        let effective_limit = limit.unwrap_or(usize::MAX);
        let page = self
            .get_data(offset, effective_limit, desc, skip_empty)
            .await?;
        Ok(page.items)
    }

    // ─── Private ────────────────────────────────────────────────────────────

    async fn push_item(&self, item: &Value, item_id: usize) -> Result<()> {
        let filename = format!("{:0>width$}.json", item_id, width = ITEM_FILENAME_DIGITS);
        let file_path = self.path.join(filename);
        let json = json_dumps(item)?;
        atomic_write(&file_path, json.as_bytes()).await?;
        Ok(())
    }

    async fn get_sorted_data_files(&self) -> Result<Vec<PathBuf>> {
        let mut files = Vec::new();
        let mut entries = fs::read_dir(&self.path).await?;

        while let Some(entry) = entries.next_entry().await? {
            let path = entry.path();
            if path.is_file() {
                if let Some(name) = path.file_name().and_then(|n| n.to_str()) {
                    if name.ends_with(".json") && name != METADATA_FILENAME {
                        files.push(path);
                    }
                }
            }
        }

        // Sort numerically by stem
        files.sort_by(|a, b| {
            let a_stem = a
                .file_stem()
                .and_then(|s| s.to_str())
                .and_then(|s| s.parse::<u64>().ok())
                .unwrap_or(0);
            let b_stem = b
                .file_stem()
                .and_then(|s| s.to_str())
                .and_then(|s| s.parse::<u64>().ok())
                .unwrap_or(0);
            a_stem.cmp(&b_stem)
        });

        Ok(files)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    #[tokio::test]
    async fn test_create_and_push_data() {
        let temp_dir = TempDir::new().unwrap();
        let storage_dir = temp_dir.path();

        let client = FileSystemDatasetClient::open(None, None, storage_dir)
            .await
            .unwrap();

        // Push a single item
        client
            .push_data(serde_json::json!({"name": "Alice", "age": 30}))
            .await
            .unwrap();

        let meta = client.get_metadata().await;
        assert_eq!(meta.item_count, 1);

        // Push multiple items
        client
            .push_data(serde_json::json!([
                {"name": "Bob", "age": 25},
                {"name": "Charlie", "age": 35}
            ]))
            .await
            .unwrap();

        let meta = client.get_metadata().await;
        assert_eq!(meta.item_count, 3);
    }

    #[tokio::test]
    async fn test_get_data_pagination() {
        let temp_dir = TempDir::new().unwrap();
        let storage_dir = temp_dir.path();

        let client = FileSystemDatasetClient::open(None, None, storage_dir)
            .await
            .unwrap();

        for i in 1..=5 {
            client
                .push_data(serde_json::json!({"index": i}))
                .await
                .unwrap();
        }

        // Get first 2 items
        let page = client.get_data(0, 2, false, false).await.unwrap();
        assert_eq!(page.count, 2);
        assert_eq!(page.total, 5);
        assert_eq!(page.items[0]["index"], 1);
        assert_eq!(page.items[1]["index"], 2);

        // Get items 3-4 (offset=2, limit=2)
        let page = client.get_data(2, 2, false, false).await.unwrap();
        assert_eq!(page.count, 2);
        assert_eq!(page.items[0]["index"], 3);

        // Descending order
        let page = client.get_data(0, 5, true, false).await.unwrap();
        assert_eq!(page.items[0]["index"], 5);
        assert_eq!(page.items[4]["index"], 1);
    }

    #[tokio::test]
    async fn test_purge() {
        let temp_dir = TempDir::new().unwrap();
        let storage_dir = temp_dir.path();

        let client = FileSystemDatasetClient::open(None, None, storage_dir)
            .await
            .unwrap();

        client.push_data(serde_json::json!({"x": 1})).await.unwrap();
        assert_eq!(client.get_metadata().await.item_count, 1);

        client.purge().await.unwrap();
        assert_eq!(client.get_metadata().await.item_count, 0);

        // Metadata file should still exist
        assert!(client.metadata_path().exists());
    }

    #[tokio::test]
    async fn test_drop_storage() {
        let temp_dir = TempDir::new().unwrap();
        let storage_dir = temp_dir.path();

        let client = FileSystemDatasetClient::open(None, None, storage_dir)
            .await
            .unwrap();

        client.push_data(serde_json::json!({"x": 1})).await.unwrap();

        client.drop_storage().await.unwrap();
        assert!(!client.path().exists());
    }

    #[tokio::test]
    async fn test_reopen_existing() {
        let temp_dir = TempDir::new().unwrap();
        let storage_dir = temp_dir.path();

        // Create and populate
        let client = FileSystemDatasetClient::open(None, Some("my-ds".to_string()), storage_dir)
            .await
            .unwrap();
        client.push_data(serde_json::json!({"x": 1})).await.unwrap();

        let id = client.get_metadata().await.base.id.clone();

        // Reopen by name
        let client2 = FileSystemDatasetClient::open(None, Some("my-ds".to_string()), storage_dir)
            .await
            .unwrap();
        assert_eq!(client2.get_metadata().await.item_count, 1);

        // Reopen by id
        let client3 = FileSystemDatasetClient::open(Some(id), None, storage_dir)
            .await
            .unwrap();
        assert_eq!(client3.get_metadata().await.item_count, 1);
    }
}
