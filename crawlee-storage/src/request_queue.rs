use std::collections::{HashSet, VecDeque};
use std::path::{Path, PathBuf};

use chrono::Utc;
use serde_json::Value;
use tokio::fs;
use tokio::sync::Mutex;
use tracing::warn;

use crate::key_value_store::FileSystemKeyValueStoreClient;
use crate::models::{
    AddRequestsResponse, ProcessedRequest, RequestQueueMetadata, RequestQueueState,
};
use crate::utils::{
    atomic_write, crypto_random_object_id, find_storage_by_id, json_dumps, json_dumps_value,
    sha256_prefix, validate_exclusive_args, Result, StorageError, METADATA_FILENAME,
};

const STORAGE_SUBDIR: &str = "request_queues";
const DEFAULT_NAME: &str = "default";
const MAX_REQUESTS_IN_CACHE: usize = 100_000;

/// Handles persisting RQ state to a key in the default KVS directory.
struct StatePersistence {
    kvs: FileSystemKeyValueStoreClient,
    key: String,
}

impl StatePersistence {
    /// Open (or create) the default KVS and build a state-persistence handle.
    async fn open(storage_dir: &Path, queue_id: &str) -> Result<Self> {
        let kvs = FileSystemKeyValueStoreClient::open(None, None, None, storage_dir).await?;
        Ok(Self {
            kvs,
            key: format!("__RQ_STATE_{queue_id}"),
        })
    }

    async fn load(&self) -> Option<RequestQueueState> {
        match self.kvs.get_value(&self.key).await {
            Ok(Some(record)) => match record.value {
                crate::models::KvsValue::Json(v) => {
                    serde_json::from_value::<RequestQueueState>(v).ok()
                }
                _ => None,
            },
            _ => None,
        }
    }

    async fn save(&self, state: &RequestQueueState) {
        if let Ok(val) = serde_json::to_value(state) {
            let _ = self
                .kvs
                .set_value(
                    &self.key,
                    crate::models::KvsValue::Json(val),
                    Some("application/json".to_string()),
                )
                .await;
        }
    }

    async fn clear(&self) {
        let _ = self.kvs.delete_value(&self.key).await;
    }
}

/// Internal state protected by a mutex.
struct InnerState {
    metadata: RequestQueueMetadata,
    queue_state: RequestQueueState,
    /// In-memory deque for fast fetching. Forefront requests go to the front (LIFO),
    /// regular requests go to the back (FIFO).
    request_cache: VecDeque<Value>,
    request_cache_needs_refresh: bool,
    is_empty_cache: Option<bool>,
    /// Lookup sets derived from queue_state for O(1) checks.
    in_progress_set: HashSet<String>,
    handled_set: HashSet<String>,
}

/// Filesystem-backed request queue client.
///
/// Stores each request as a JSON file named by `sha256(unique_key)[:15].json`.
///
/// Directory layout:
/// ```text
/// {storage_dir}/request_queues/{name}/
/// ├── __metadata__.json
/// ├── 1a2b3c4d5e6f7g8.json     (request files)
/// └── ...
/// ```
pub struct FileSystemRequestQueueClient {
    inner: Mutex<InnerState>,
    path: PathBuf,
    persistence: StatePersistence,
}

impl FileSystemRequestQueueClient {
    /// Open an existing request queue or create a new one.
    ///
    /// - `id`: Open by ID (scans directories for matching metadata).
    /// - `name`: Open by name (used as directory name, written to metadata).
    /// - `alias`: Open by alias (used as directory name, but NOT written to metadata).
    /// - `storage_dir`: Base storage directory (e.g., "./storage").
    ///
    /// At most one of `id`, `name`, or `alias` may be provided.
    ///
    /// Queue state is automatically persisted to the default key-value store
    /// under the key `__RQ_STATE_{queue_id}`. Call [`persist_state`] from
    /// the framework's event system to flush state periodically.
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
                    StorageError::NotFound(format!("Request queue with id '{id_val}' not found"))
                })?
        } else {
            let dir_name = name.as_deref().or(alias.as_deref()).unwrap_or(DEFAULT_NAME);
            storage_dir.join(STORAGE_SUBDIR).join(dir_name)
        };

        let metadata_path = path.join(METADATA_FILENAME);

        let metadata = if metadata_path.exists() {
            let content = fs::read_to_string(&metadata_path).await?;
            serde_json::from_str::<RequestQueueMetadata>(&content)?
        } else {
            let new_id = id.unwrap_or_else(|| crypto_random_object_id(17));
            let meta = RequestQueueMetadata::new(new_id, name);
            fs::create_dir_all(&path).await?;
            let json = json_dumps_value(&meta)?;
            atomic_write(&metadata_path, json.as_bytes()).await?;
            meta
        };

        // Open the default KVS for state persistence, then load any prior state.
        let persistence = StatePersistence::open(storage_dir, &metadata.base.id).await?;
        let mut queue_state = persistence.load().await.unwrap_or_default();

        // After a restart, any previously in-progress requests are no longer
        // being processed. Reclaim them back into the regular queue so they
        // can be fetched again (matches Python master behaviour).
        if !queue_state.in_progress_requests.is_empty() {
            for key in queue_state.in_progress_requests.drain(..) {
                if !queue_state.regular_requests.contains_key(&key)
                    && !queue_state.forefront_requests.contains_key(&key)
                {
                    queue_state.sequence_counter += 1;
                    let seq = queue_state.sequence_counter;
                    queue_state
                        .regular_requests
                        .insert(key, Value::Number(seq.into()));
                }
            }
        }

        // Build lookup sets from (cleaned-up) loaded state.
        // in_progress_set starts empty because we just reclaimed everything.
        let in_progress_set: HashSet<String> = HashSet::new();
        let handled_set: HashSet<String> = queue_state.handled_requests.iter().cloned().collect();

        let client = Self {
            inner: Mutex::new(InnerState {
                metadata,
                queue_state,
                request_cache: VecDeque::new(),
                request_cache_needs_refresh: true,
                is_empty_cache: None,
                in_progress_set,
                handled_set,
            }),
            path,
            persistence,
        };

        // Discover any existing request files not yet tracked in state
        client.discover_existing_requests().await?;

        Ok(client)
    }

    /// Get the queue metadata.
    pub async fn get_metadata(&self) -> RequestQueueMetadata {
        self.inner.lock().await.metadata.clone()
    }

    /// Path to the queue directory.
    pub fn path(&self) -> &Path {
        &self.path
    }

    /// Path to the metadata file.
    pub fn metadata_path(&self) -> PathBuf {
        self.path.join(METADATA_FILENAME)
    }

    /// Delete the entire queue directory.
    pub async fn drop_storage(&self) -> Result<()> {
        // Clear persisted state
        self.persistence.clear().await;

        if self.path.exists() {
            fs::remove_dir_all(&self.path).await?;
        }

        let mut inner = self.inner.lock().await;
        inner.queue_state = RequestQueueState::default();
        inner.request_cache.clear();
        inner.in_progress_set.clear();
        inner.handled_set.clear();
        inner.request_cache_needs_refresh = true;
        inner.is_empty_cache = Some(true);

        Ok(())
    }

    /// Delete all request files and reset state.
    pub async fn purge(&self) -> Result<()> {
        let mut inner = self.inner.lock().await;

        // Delete all request files
        for file in Self::get_request_files(&self.path).await? {
            fs::remove_file(&file).await?;
        }

        // Reset state
        inner.queue_state = RequestQueueState::default();
        inner.request_cache.clear();
        inner.in_progress_set.clear();
        inner.handled_set.clear();
        inner.request_cache_needs_refresh = true;
        inner.is_empty_cache = Some(true);

        // Reset metadata counts
        inner.metadata.handled_request_count = 0;
        inner.metadata.pending_request_count = 0;
        inner.metadata.total_request_count = 0;
        let now = Utc::now();
        inner.metadata.base.accessed_at = now;
        inner.metadata.base.modified_at = now;

        let json = json_dumps_value(&inner.metadata)?;
        atomic_write(&self.path.join(METADATA_FILENAME), json.as_bytes()).await?;

        // Persist the reset state
        self.persistence.save(&inner.queue_state).await;

        Ok(())
    }

    /// Add a batch of requests, deduplicating by unique_key.
    pub async fn add_batch_of_requests(
        &self,
        requests: Vec<Value>,
        forefront: bool,
    ) -> Result<AddRequestsResponse> {
        let mut inner = self.inner.lock().await;
        let mut processed = Vec::new();

        for request in requests {
            let unique_key = request
                .get("uniqueKey")
                .or_else(|| request.get("unique_key"))
                .and_then(|v| v.as_str())
                .ok_or_else(|| {
                    StorageError::InvalidArgs("Request must have a 'uniqueKey' field".to_string())
                })?
                .to_string();

            // Check if already handled
            if inner.handled_set.contains(&unique_key) {
                processed.push(ProcessedRequest {
                    id: None,
                    unique_key,
                    was_already_present: true,
                    was_already_handled: true,
                });
                continue;
            }

            // Check if already in progress
            if inner.in_progress_set.contains(&unique_key) {
                processed.push(ProcessedRequest {
                    id: None,
                    unique_key,
                    was_already_present: true,
                    was_already_handled: false,
                });
                continue;
            }

            // Check if already present (in regular or forefront queues)
            let already_in_regular = inner.queue_state.regular_requests.contains_key(&unique_key);
            let already_in_forefront = inner
                .queue_state
                .forefront_requests
                .contains_key(&unique_key);
            let was_already_present = already_in_regular || already_in_forefront;

            if was_already_present && !forefront {
                // Already queued, not forefront — just report it
                processed.push(ProcessedRequest {
                    id: None,
                    unique_key,
                    was_already_present: true,
                    was_already_handled: false,
                });
                continue;
            }

            // Write request file to disk
            let file_path = self.get_request_path(&unique_key);
            let json = json_dumps(&request)?;
            atomic_write(&file_path, json.as_bytes()).await?;

            if was_already_present && forefront {
                // Move from regular to forefront
                inner.queue_state.regular_requests.remove(&unique_key);
                inner.queue_state.forefront_sequence_counter += 1;
                let seq = inner.queue_state.forefront_sequence_counter;
                inner
                    .queue_state
                    .forefront_requests
                    .insert(unique_key.clone(), Value::Number(seq.into()));
                // Add to front of cache
                inner.request_cache.push_front(request);
                inner.request_cache_needs_refresh = true;
            } else {
                // Brand new request
                if forefront {
                    inner.queue_state.forefront_sequence_counter += 1;
                    let seq = inner.queue_state.forefront_sequence_counter;
                    inner
                        .queue_state
                        .forefront_requests
                        .insert(unique_key.clone(), Value::Number(seq.into()));
                    inner.request_cache.push_front(request);
                } else {
                    inner.queue_state.sequence_counter += 1;
                    let seq = inner.queue_state.sequence_counter;
                    inner
                        .queue_state
                        .regular_requests
                        .insert(unique_key.clone(), Value::Number(seq.into()));
                    inner.request_cache.push_back(request);
                }

                inner.metadata.total_request_count += 1;
                inner.metadata.pending_request_count += 1;
            }

            inner.is_empty_cache = None;

            processed.push(ProcessedRequest {
                id: None,
                unique_key,
                was_already_present,
                was_already_handled: false,
            });
        }

        // Update metadata
        let now = Utc::now();
        inner.metadata.base.accessed_at = now;
        inner.metadata.base.modified_at = now;
        let meta_json = json_dumps_value(&inner.metadata)?;
        atomic_write(&self.path.join(METADATA_FILENAME), meta_json.as_bytes()).await?;

        // Persist queue state
        self.persist_state_inner(&inner).await;

        Ok(AddRequestsResponse {
            processed_requests: processed,
            unprocessed_requests: Vec::new(),
        })
    }

    /// Get a request by unique_key without marking it as in-progress.
    pub async fn get_request(&self, unique_key: &str) -> Result<Option<Value>> {
        let file_path = self.get_request_path(unique_key);
        if !file_path.exists() {
            return Ok(None);
        }

        let content = fs::read_to_string(&file_path).await?;
        let request: Value = serde_json::from_str(&content)?;

        // Update accessed_at
        {
            let mut inner = self.inner.lock().await;
            inner.metadata.base.accessed_at = Utc::now();
            let json = json_dumps_value(&inner.metadata)?;
            atomic_write(&self.path.join(METADATA_FILENAME), json.as_bytes()).await?;
        }

        Ok(Some(request))
    }

    /// Fetch the next request from the queue, marking it as in-progress.
    pub async fn fetch_next_request(&self) -> Result<Option<Value>> {
        let mut inner = self.inner.lock().await;

        // Refresh cache if needed
        if inner.request_cache_needs_refresh {
            self.refresh_cache_inner(&mut inner).await?;
        }

        loop {
            let request = match inner.request_cache.pop_front() {
                Some(r) => r,
                None => {
                    // Cache is empty, but the queue is only truly empty if
                    // there are also no in-progress requests.
                    inner.is_empty_cache = Some(inner.in_progress_set.is_empty());
                    return Ok(None);
                }
            };

            let unique_key = match request
                .get("uniqueKey")
                .or_else(|| request.get("unique_key"))
                .and_then(|v| v.as_str())
            {
                Some(k) => k.to_string(),
                None => continue,
            };

            // Skip if already handled or in progress
            if inner.handled_set.contains(&unique_key)
                || inner.in_progress_set.contains(&unique_key)
            {
                continue;
            }

            // Mark as in-progress
            inner.in_progress_set.insert(unique_key.clone());
            inner
                .queue_state
                .in_progress_requests
                .push(unique_key.clone());
            inner.queue_state.regular_requests.remove(&unique_key);
            inner.queue_state.forefront_requests.remove(&unique_key);
            inner.is_empty_cache = None;

            // Persist state
            self.persist_state_inner(&inner).await;

            return Ok(Some(request));
        }
    }

    /// Mark a request as handled (done).
    pub async fn mark_request_as_handled(
        &self,
        mut request: Value,
    ) -> Result<Option<ProcessedRequest>> {
        let unique_key = match request
            .get("uniqueKey")
            .or_else(|| request.get("unique_key"))
            .and_then(|v| v.as_str())
        {
            Some(k) => k.to_string(),
            None => {
                return Err(StorageError::InvalidArgs(
                    "Request must have a 'uniqueKey' field".to_string(),
                ));
            }
        };

        let mut inner = self.inner.lock().await;

        // Already handled — return immediately with was_already_handled: true
        if inner.handled_set.contains(&unique_key) {
            return Ok(Some(ProcessedRequest {
                id: None,
                unique_key,
                was_already_present: true,
                was_already_handled: true,
            }));
        }

        // Must be in progress
        if !inner.in_progress_set.contains(&unique_key) {
            return Ok(None);
        }

        // Set handledAt timestamp
        let now = Utc::now();
        let handled_at_str = now.format("%Y-%m-%dT%H:%M:%S%.6f+00:00").to_string();
        if let Value::Object(ref mut map) = request {
            map.insert("handledAt".to_string(), Value::String(handled_at_str));
        }

        // Write updated request file
        let file_path = self.get_request_path(&unique_key);
        let json = json_dumps(&request)?;
        atomic_write(&file_path, json.as_bytes()).await?;

        // Move from in-progress to handled
        inner.in_progress_set.remove(&unique_key);
        inner.handled_set.insert(unique_key.clone());
        inner
            .queue_state
            .in_progress_requests
            .retain(|k| k != &unique_key);
        inner.queue_state.handled_requests.push(unique_key.clone());

        // Update metadata
        inner.metadata.handled_request_count += 1;
        inner.metadata.pending_request_count =
            inner.metadata.pending_request_count.saturating_sub(1);
        inner.metadata.base.accessed_at = now;
        inner.metadata.base.modified_at = now;

        let meta_json = json_dumps_value(&inner.metadata)?;
        atomic_write(&self.path.join(METADATA_FILENAME), meta_json.as_bytes()).await?;

        inner.is_empty_cache = None;

        // Persist state
        self.persist_state_inner(&inner).await;

        Ok(Some(ProcessedRequest {
            id: None,
            unique_key,
            was_already_present: true,
            was_already_handled: true,
        }))
    }

    /// Reclaim a request — move it from in-progress back to the queue.
    pub async fn reclaim_request(
        &self,
        request: Value,
        forefront: bool,
    ) -> Result<Option<ProcessedRequest>> {
        let unique_key = match request
            .get("uniqueKey")
            .or_else(|| request.get("unique_key"))
            .and_then(|v| v.as_str())
        {
            Some(k) => k.to_string(),
            None => {
                return Err(StorageError::InvalidArgs(
                    "Request must have a 'uniqueKey' field".to_string(),
                ));
            }
        };

        let mut inner = self.inner.lock().await;

        // Must be in progress
        if !inner.in_progress_set.contains(&unique_key) {
            return Ok(None);
        }

        // Remove from in-progress
        inner.in_progress_set.remove(&unique_key);
        inner
            .queue_state
            .in_progress_requests
            .retain(|k| k != &unique_key);

        // Write updated request to disk (the caller may have modified fields
        // like userData.__crawlee.state, retryCount, etc.)
        let file_path = self.get_request_path(&unique_key);
        let json = json_dumps(&request)?;
        atomic_write(&file_path, json.as_bytes()).await?;

        // Re-add to queue
        if forefront {
            inner.queue_state.forefront_sequence_counter += 1;
            let seq = inner.queue_state.forefront_sequence_counter;
            inner
                .queue_state
                .forefront_requests
                .insert(unique_key.clone(), Value::Number(seq.into()));
            inner.request_cache.push_front(request);
        } else {
            inner.queue_state.sequence_counter += 1;
            let seq = inner.queue_state.sequence_counter;
            inner
                .queue_state
                .regular_requests
                .insert(unique_key.clone(), Value::Number(seq.into()));
            inner.request_cache.push_back(request);
        }

        inner.is_empty_cache = None;

        let now = Utc::now();
        inner.metadata.base.accessed_at = now;
        inner.metadata.base.modified_at = now;
        let meta_json = json_dumps_value(&inner.metadata)?;
        atomic_write(&self.path.join(METADATA_FILENAME), meta_json.as_bytes()).await?;

        // Persist state
        self.persist_state_inner(&inner).await;

        Ok(Some(ProcessedRequest {
            id: None,
            unique_key,
            was_already_present: true,
            was_already_handled: false,
        }))
    }

    /// Check if the queue is empty (no pending or in-progress requests).
    pub async fn is_empty(&self) -> bool {
        let mut inner = self.inner.lock().await;

        if let Some(cached) = inner.is_empty_cache {
            return cached;
        }

        let all_keys: HashSet<&String> = inner
            .queue_state
            .regular_requests
            .keys()
            .chain(inner.queue_state.forefront_requests.keys())
            .chain(inner.in_progress_set.iter())
            .collect();

        let unhandled: usize = all_keys
            .iter()
            .filter(|k| !inner.handled_set.contains(**k))
            .count();

        let result = unhandled == 0;
        inner.is_empty_cache = Some(result);

        // Update accessed_at (best-effort — is_empty returns bool, not Result)
        inner.metadata.base.accessed_at = Utc::now();
        if let Ok(json) = json_dumps_value(&inner.metadata) {
            let _ = atomic_write(&self.path.join(METADATA_FILENAME), json.as_bytes()).await;
        }

        result
    }

    /// Explicitly persist the current queue state via the callback.
    /// Call this from the Python/JS side in response to PERSIST_STATE events.
    pub async fn persist_state(&self) {
        let inner = self.inner.lock().await;
        self.persist_state_inner(&inner).await;
    }

    // ─── Private ────────────────────────────────────────────────────────────

    fn get_request_path(&self, unique_key: &str) -> PathBuf {
        let hash = sha256_prefix(unique_key, 15);
        self.path.join(format!("{hash}.json"))
    }

    async fn get_request_files(path: &Path) -> Result<Vec<PathBuf>> {
        let mut files = Vec::new();
        let mut entries = match fs::read_dir(path).await {
            Ok(entries) => entries,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(files),
            Err(e) => return Err(e.into()),
        };
        while let Some(entry) = entries.next_entry().await? {
            let p = entry.path();
            if p.is_file() {
                if let Some(name) = p.file_name().and_then(|n| n.to_str()) {
                    if name.ends_with(".json") && name != METADATA_FILENAME {
                        files.push(p);
                    }
                }
            }
        }
        Ok(files)
    }

    /// Scan existing request files on disk and add them to state if not already tracked.
    async fn discover_existing_requests(&self) -> Result<()> {
        let request_files = Self::get_request_files(&self.path).await?;

        let mut inner = self.inner.lock().await;
        let mut discovered_pending = 0usize;
        let mut discovered_handled = 0usize;

        for file_path in request_files {
            match fs::read_to_string(&file_path).await {
                Ok(content) => {
                    match serde_json::from_str::<Value>(&content) {
                        Ok(request) => {
                            let unique_key = match request
                                .get("uniqueKey")
                                .or_else(|| request.get("unique_key"))
                                .and_then(|v| v.as_str())
                            {
                                Some(k) => k.to_string(),
                                None => continue,
                            };

                            // Skip if already tracked
                            if inner.queue_state.regular_requests.contains_key(&unique_key)
                                || inner
                                    .queue_state
                                    .forefront_requests
                                    .contains_key(&unique_key)
                                || inner.in_progress_set.contains(&unique_key)
                                || inner.handled_set.contains(&unique_key)
                            {
                                continue;
                            }

                            // Check if already handled (has handled_at)
                            let is_handled = request
                                .get("handledAt")
                                .or_else(|| request.get("handled_at"))
                                .map(|v| !v.is_null())
                                .unwrap_or(false);

                            if is_handled {
                                inner.handled_set.insert(unique_key.clone());
                                inner.queue_state.handled_requests.push(unique_key);
                                discovered_handled += 1;
                            } else {
                                inner.queue_state.sequence_counter += 1;
                                let seq = inner.queue_state.sequence_counter;
                                inner
                                    .queue_state
                                    .regular_requests
                                    .insert(unique_key, Value::Number(seq.into()));
                                discovered_pending += 1;
                            }
                        }
                        Err(e) => {
                            warn!(
                                "Failed to parse request file {}: {}",
                                file_path.display(),
                                e
                            );
                        }
                    }
                }
                Err(e) => {
                    warn!("Failed to read request file {}: {}", file_path.display(), e);
                }
            }
        }

        // Recalculate metadata counts from authoritative state (not incremental,
        // because metadata loaded from disk may already reflect these requests).
        if discovered_pending > 0 || discovered_handled > 0 {
            let handled = inner.handled_set.len();
            let pending = inner.queue_state.regular_requests.len()
                + inner.queue_state.forefront_requests.len()
                + inner.in_progress_set.len();
            inner.metadata.handled_request_count = handled;
            inner.metadata.pending_request_count = pending;
            inner.metadata.total_request_count = handled + pending;

            let json = json_dumps_value(&inner.metadata)?;
            atomic_write(&self.path.join(METADATA_FILENAME), json.as_bytes()).await?;
        }

        Ok(())
    }

    /// Refresh the in-memory request cache from disk.
    async fn refresh_cache_inner(&self, inner: &mut InnerState) -> Result<()> {
        let request_files = Self::get_request_files(&self.path).await?;
        inner.request_cache.clear();

        // Collect forefront requests (LIFO — newest first)
        let mut forefront: Vec<(String, i64, Value)> = Vec::new();
        // Collect regular requests (FIFO — oldest first)
        let mut regular: Vec<(String, i64, Value)> = Vec::new();

        for file_path in request_files {
            let content = match fs::read_to_string(&file_path).await {
                Ok(c) => c,
                Err(_) => continue,
            };
            let request: Value = match serde_json::from_str(&content) {
                Ok(r) => r,
                Err(_) => continue,
            };

            let unique_key = match request
                .get("uniqueKey")
                .or_else(|| request.get("unique_key"))
                .and_then(|v| v.as_str())
            {
                Some(k) => k.to_string(),
                None => continue,
            };

            // Skip handled and in-progress
            if inner.handled_set.contains(&unique_key)
                || inner.in_progress_set.contains(&unique_key)
            {
                continue;
            }

            if let Some(seq) = inner
                .queue_state
                .forefront_requests
                .get(&unique_key)
                .and_then(|v| v.as_i64())
            {
                forefront.push((unique_key, seq, request));
            } else if let Some(seq) = inner
                .queue_state
                .regular_requests
                .get(&unique_key)
                .and_then(|v| v.as_i64())
            {
                regular.push((unique_key, seq, request));
            }
        }

        // Sort forefront: newest first (highest sequence number first = LIFO)
        forefront.sort_by_key(|b| std::cmp::Reverse(b.1));
        // Sort regular: oldest first (lowest sequence number first = FIFO)
        regular.sort_by_key(|a| a.1);

        // Fill cache: forefront first, then regular
        for (_, _, req) in forefront {
            if inner.request_cache.len() >= MAX_REQUESTS_IN_CACHE {
                break;
            }
            inner.request_cache.push_back(req);
        }
        for (_, _, req) in regular {
            if inner.request_cache.len() >= MAX_REQUESTS_IN_CACHE {
                break;
            }
            inner.request_cache.push_back(req);
        }

        inner.request_cache_needs_refresh = false;
        Ok(())
    }

    async fn persist_state_inner(&self, inner: &InnerState) {
        self.persistence.save(&inner.queue_state).await;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    #[tokio::test]
    async fn test_add_and_fetch_request() {
        let temp_dir = TempDir::new().unwrap();
        let storage_dir = temp_dir.path();

        let client = FileSystemRequestQueueClient::open(None, None, None, storage_dir)
            .await
            .unwrap();

        let requests = vec![serde_json::json!({
            "uniqueKey": "https://example.com",
            "url": "https://example.com",
            "method": "GET"
        })];

        let response = client.add_batch_of_requests(requests, false).await.unwrap();
        assert_eq!(response.processed_requests.len(), 1);
        assert!(!response.processed_requests[0].was_already_present);

        // Fetch the request
        let fetched = client.fetch_next_request().await.unwrap();
        assert!(fetched.is_some());
        let fetched = fetched.unwrap();
        assert_eq!(fetched["url"], "https://example.com");

        // Queue should appear empty now (request is in-progress)
        let next = client.fetch_next_request().await.unwrap();
        assert!(next.is_none());
    }

    #[tokio::test]
    async fn test_deduplication() {
        let temp_dir = TempDir::new().unwrap();
        let storage_dir = temp_dir.path();

        let client = FileSystemRequestQueueClient::open(None, None, None, storage_dir)
            .await
            .unwrap();

        let req = serde_json::json!({
            "uniqueKey": "https://example.com",
            "url": "https://example.com",
            "method": "GET"
        });

        // Add twice
        client
            .add_batch_of_requests(vec![req.clone()], false)
            .await
            .unwrap();
        let response = client
            .add_batch_of_requests(vec![req], false)
            .await
            .unwrap();

        assert!(response.processed_requests[0].was_already_present);
    }

    #[tokio::test]
    async fn test_mark_as_handled() {
        let temp_dir = TempDir::new().unwrap();
        let storage_dir = temp_dir.path();

        let client = FileSystemRequestQueueClient::open(None, None, None, storage_dir)
            .await
            .unwrap();

        client
            .add_batch_of_requests(
                vec![serde_json::json!({
                    "uniqueKey": "req1",
                    "url": "https://example.com/1",
                    "method": "GET"
                })],
                false,
            )
            .await
            .unwrap();

        let request = client.fetch_next_request().await.unwrap().unwrap();
        let result = client.mark_request_as_handled(request).await.unwrap();
        assert!(result.is_some());

        assert!(client.is_empty().await);
    }

    #[tokio::test]
    async fn test_reclaim_request() {
        let temp_dir = TempDir::new().unwrap();
        let storage_dir = temp_dir.path();

        let client = FileSystemRequestQueueClient::open(None, None, None, storage_dir)
            .await
            .unwrap();

        client
            .add_batch_of_requests(
                vec![serde_json::json!({
                    "uniqueKey": "req1",
                    "url": "https://example.com/1",
                    "method": "GET"
                })],
                false,
            )
            .await
            .unwrap();

        let request = client.fetch_next_request().await.unwrap().unwrap();

        // Reclaim it
        let result = client.reclaim_request(request, false).await.unwrap();
        assert!(result.is_some());

        // Should be fetchable again
        let refetched = client.fetch_next_request().await.unwrap();
        assert!(refetched.is_some());
    }

    #[tokio::test]
    async fn test_get_request_updates_accessed_at() {
        let temp_dir = TempDir::new().unwrap();
        let storage_dir = temp_dir.path();

        let client = FileSystemRequestQueueClient::open(None, None, None, storage_dir)
            .await
            .unwrap();

        client
            .add_batch_of_requests(
                vec![serde_json::json!({
                    "uniqueKey": "req1",
                    "url": "https://example.com/1",
                    "method": "GET"
                })],
                false,
            )
            .await
            .unwrap();

        let accessed_before = client.get_metadata().await.base.accessed_at;

        // Small delay so timestamps differ
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;

        let _request = client.get_request("req1").await.unwrap();

        let accessed_after = client.get_metadata().await.base.accessed_at;
        assert!(
            accessed_after > accessed_before,
            "get_request should update accessed_at"
        );
    }

    #[tokio::test]
    async fn test_is_empty_updates_accessed_at() {
        let temp_dir = TempDir::new().unwrap();
        let storage_dir = temp_dir.path();

        let client = FileSystemRequestQueueClient::open(None, None, None, storage_dir)
            .await
            .unwrap();

        let accessed_before = client.get_metadata().await.base.accessed_at;

        tokio::time::sleep(std::time::Duration::from_millis(10)).await;

        let _empty = client.is_empty().await;

        let accessed_after = client.get_metadata().await.base.accessed_at;
        assert!(
            accessed_after > accessed_before,
            "is_empty should update accessed_at"
        );
    }

    #[tokio::test]
    async fn test_mark_handled_returns_was_already_handled_true() {
        let temp_dir = TempDir::new().unwrap();
        let storage_dir = temp_dir.path();

        let client = FileSystemRequestQueueClient::open(None, None, None, storage_dir)
            .await
            .unwrap();

        client
            .add_batch_of_requests(
                vec![serde_json::json!({
                    "uniqueKey": "req1",
                    "url": "https://example.com/1",
                    "method": "GET"
                })],
                false,
            )
            .await
            .unwrap();

        let request = client.fetch_next_request().await.unwrap().unwrap();
        let result = client
            .mark_request_as_handled(request.clone())
            .await
            .unwrap()
            .unwrap();

        // After marking as handled, was_already_handled should be true
        assert!(
            result.was_already_handled,
            "mark_request_as_handled should return was_already_handled=true"
        );

        // Calling again should also return was_already_handled: true
        let result2 = client
            .mark_request_as_handled(request)
            .await
            .unwrap()
            .unwrap();
        assert!(
            result2.was_already_handled,
            "re-marking handled request should return was_already_handled=true"
        );
    }

    #[tokio::test]
    async fn test_forefront() {
        let temp_dir = TempDir::new().unwrap();
        let storage_dir = temp_dir.path();

        let client = FileSystemRequestQueueClient::open(None, None, None, storage_dir)
            .await
            .unwrap();

        // Add regular request first
        client
            .add_batch_of_requests(
                vec![serde_json::json!({
                    "uniqueKey": "regular",
                    "url": "https://example.com/regular",
                    "method": "GET"
                })],
                false,
            )
            .await
            .unwrap();

        // Add forefront request
        client
            .add_batch_of_requests(
                vec![serde_json::json!({
                    "uniqueKey": "priority",
                    "url": "https://example.com/priority",
                    "method": "GET"
                })],
                true,
            )
            .await
            .unwrap();

        // Forefront should come first
        let first = client.fetch_next_request().await.unwrap().unwrap();
        assert_eq!(first["uniqueKey"], "priority");
    }

    #[tokio::test]
    async fn test_reopen_does_not_duplicate_counts() {
        let temp_dir = TempDir::new().unwrap();
        let storage_dir = temp_dir.path();

        // Create a queue and add requests
        let client = FileSystemRequestQueueClient::open(None, None, None, storage_dir)
            .await
            .unwrap();

        client
            .add_batch_of_requests(
                vec![
                    serde_json::json!({
                        "uniqueKey": "req1",
                        "url": "https://example.com/1",
                        "method": "GET"
                    }),
                    serde_json::json!({
                        "uniqueKey": "req2",
                        "url": "https://example.com/2",
                        "method": "GET"
                    }),
                ],
                false,
            )
            .await
            .unwrap();

        let meta = client.get_metadata().await;
        assert_eq!(meta.total_request_count, 2);
        assert_eq!(meta.pending_request_count, 2);
        drop(client);

        // Reopen the same queue (no persistence — state starts empty, discovery runs)
        let client2 = FileSystemRequestQueueClient::open(None, None, None, storage_dir)
            .await
            .unwrap();

        let meta2 = client2.get_metadata().await;
        assert_eq!(
            meta2.total_request_count, 2,
            "total_request_count should not double on reopen"
        );
        assert_eq!(
            meta2.pending_request_count, 2,
            "pending_request_count should not double on reopen"
        );
    }

    #[tokio::test]
    async fn test_is_empty_false_while_request_in_progress() {
        let temp_dir = TempDir::new().unwrap();
        let storage_dir = temp_dir.path();

        let client = FileSystemRequestQueueClient::open(None, None, None, storage_dir)
            .await
            .unwrap();

        client
            .add_batch_of_requests(
                vec![serde_json::json!({
                    "uniqueKey": "req1",
                    "url": "https://example.com/1",
                    "method": "GET"
                })],
                false,
            )
            .await
            .unwrap();

        // Fetch the request (marks it as in-progress)
        let request = client.fetch_next_request().await.unwrap();
        assert!(request.is_some());

        // No more requests to fetch, but one is still in-progress
        let next = client.fetch_next_request().await.unwrap();
        assert!(next.is_none());

        // Queue must NOT report as empty — the in-progress request isn't handled yet
        assert!(
            !client.is_empty().await,
            "is_empty() must return false while a request is in-progress"
        );

        // After handling, queue should be empty
        client
            .mark_request_as_handled(request.unwrap())
            .await
            .unwrap();
        assert!(
            client.is_empty().await,
            "is_empty() should return true after all requests are handled"
        );
    }
}
