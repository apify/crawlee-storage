use std::collections::HashMap;
use std::path::{Path, PathBuf};

use serde_json::Value;
use tokio::fs;
use tokio::sync::Mutex;
use tracing::warn;

use crate::clock::{system_clock, ClockRef};
use crate::models::{AddRequestsResponse, ProcessedRequest, RequestQueueMetadata};
use crate::utils::{
    atomic_write, crypto_random_object_id, find_storage_by_id, json_dumps, json_dumps_value,
    sha256_prefix, unique_key_to_request_id, validate_exclusive_args, validate_subdirectory,
    Result, StorageError, METADATA_FILENAME,
};

const STORAGE_SUBDIR: &str = "request_queues";
const DEFAULT_NAME: &str = "default";

/// Default lock duration (how long a fetched request stays reserved) in
/// milliseconds. Matches the JS `DEFAULT_REQUEST_LOCK_SECS` of 3 minutes.
const DEFAULT_LOCK_MILLIS: i64 = 3 * 60 * 1000;

/// Lightweight in-memory index entry for a single request, mirroring the
/// `orderNo` persisted in the request file on disk.
///
/// `order_no` semantics (crawlee-js v3 `_calculateOrderNo`):
/// - `None`             → request has been handled.
/// - `Some(n)`, `n > 0` → regular request.
/// - `Some(n)`, `n < 0` → forefront request.
/// - `|n|` is a unix-millis timestamp. When `|n|` lies in the future the
///   request is *locked* (in progress) until that moment. Collisions in the
///   same millisecond are fine — ordering is resolved by `forefront_request_ids`
///   (for forefront) and `insertion_seq` (a stable in-memory tie-break for
///   regulars), exactly mirroring how JS uses its `forefrontRequestIds` list
///   plus `Map` insertion order.
#[derive(Clone)]
struct RequestEntry {
    order_no: Option<i64>,
    /// Stable in-memory insertion sequence, used purely as a tie-break when two
    /// regular requests share the same `orderNo` (same-millisecond adds). This
    /// is the Rust stand-in for JS's reliance on `Map` iteration order. It is
    /// never persisted and never interpreted as a clock.
    insertion_seq: u64,
}

/// Internal state protected by a mutex.
struct InnerState {
    metadata: RequestQueueMetadata,
    /// unique_key -> entry. The authoritative lock state lives in the request
    /// file on disk; this map is a fast index that is kept in sync and
    /// re-read from disk when lock state matters.
    requests: HashMap<String, RequestEntry>,
    /// Monotonic counter feeding `RequestEntry::insertion_seq`. In-memory only.
    ///
    /// (The forefront ordering list lives in `metadata.forefront_request_ids`,
    /// the single source of truth, persisted on every metadata write.)
    insertion_counter: u64,
    /// How long (ms) a freshly fetched request stays locked. Tunable via
    /// `set_expected_request_processing_time`. Only ever raised, never lowered
    /// (the longest-lived consumer wins), matching the JS frontend policy.
    lock_millis: i64,
}

/// Filesystem-backed request queue client.
///
/// Each request is stored as a JSON file named `sha256(unique_key)[:15].json`.
/// The persisted JSON carries two queue-managed fields in addition to the
/// opaque user payload:
/// - `id`: the sha256-derived request id (matches the Apify platform).
/// - `orderNo`: a signed unix-millis timestamp encoding ordering, forefront
///   priority, lock expiry, and handled state (see [`RequestEntry`]).
///
/// This `orderNo` lock model is the crawlee-js v3 model: because the lock is
/// persisted *inside the request file*, multiple consumers sharing one on-disk
/// queue can coordinate without a central lock service (assuming roughly
/// synchronized clocks). There is no separate state blob.
///
/// Directory layout:
/// ```text
/// {storage_dir}/request_queues/{name}/
/// ├── __metadata__.json
/// ├── 1a2b3c4d5e6f7g8.json     (request files, each carrying id + orderNo)
/// └── ...
/// ```
pub struct FileSystemRequestQueueClient {
    inner: Mutex<InnerState>,
    path: PathBuf,
    clock: ClockRef,
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
    /// Uses the default [`SystemClock`](crate::clock::SystemClock) and the
    /// conservative cross-process default `assume_sole_owner = false` (so any
    /// future-dated `orderNo` on disk is treated as a live peer's lock). To
    /// customize either, use [`open_with_clock`](Self::open_with_clock).
    pub async fn open(
        id: Option<String>,
        name: Option<String>,
        alias: Option<String>,
        storage_dir: &Path,
    ) -> Result<Self> {
        Self::open_with_clock(id, name, alias, storage_dir, system_clock(), false).await
    }

    /// Open an existing request queue or create a new one, using the supplied clock.
    ///
    /// Passing a [`TestClock`](crate::clock::TestClock) here lets the binding
    /// layer (JS, Python) advance the time the queue sees without real waits,
    /// which is the only way to exercise lock-expiry behavior under fake
    /// timers — `vi.useFakeTimers()` etc. don't reach into native code.
    ///
    /// `assume_sole_owner` controls how `rebuild_index` treats future-dated
    /// `orderNo`s on disk at open time:
    ///
    /// - `false` (safe default): respect them as a live peer's locks. Crashed
    ///   peers' locks expire naturally on the wall clock (default 3 min, see
    ///   [`set_expected_request_processing_time`](Self::set_expected_request_processing_time)).
    ///   This is the cross-process-safe mode.
    /// - `true` (single-process opt-in): reclaim every future-dated `orderNo`
    ///   by resetting it to `±now`, so a previously in-progress request is
    ///   immediately re-fetchable. Use only when you know nothing else is
    ///   using this on-disk queue — otherwise you'll clobber a live peer's
    ///   reservation and let two consumers process the same request.
    pub async fn open_with_clock(
        id: Option<String>,
        name: Option<String>,
        alias: Option<String>,
        storage_dir: &Path,
        clock: ClockRef,
        assume_sole_owner: bool,
    ) -> Result<Self> {
        validate_exclusive_args(&id, &name, &alias)?;

        let path = if let Some(ref id_val) = id {
            find_storage_by_id(storage_dir, STORAGE_SUBDIR, id_val)
                .await?
                .ok_or_else(|| {
                    StorageError::NotFound(format!("Request queue with id '{id_val}' not found"))
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
            serde_json::from_str::<RequestQueueMetadata>(&content)?
        } else {
            let new_id = id.unwrap_or_else(|| crypto_random_object_id(17));
            let mut meta = RequestQueueMetadata::new(new_id, name);
            let now = clock.now();
            meta.base.created_at = now;
            meta.base.modified_at = now;
            meta.base.accessed_at = now;
            fs::create_dir_all(&path).await?;
            let json = json_dumps_value(&meta)?;
            atomic_write(&metadata_path, json.as_bytes()).await?;
            meta
        };

        let client = Self {
            inner: Mutex::new(InnerState {
                metadata,
                requests: HashMap::new(),
                insertion_counter: 0,
                lock_millis: DEFAULT_LOCK_MILLIS,
            }),
            path,
            clock,
        };

        // Reconstruct the in-memory index from the request files on disk.
        client.rebuild_index(assume_sole_owner).await?;

        Ok(client)
    }

    /// Return a reference to this client's clock.
    pub fn clock(&self) -> &ClockRef {
        &self.clock
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
        if self.path.exists() {
            fs::remove_dir_all(&self.path).await?;
        }

        let mut inner = self.inner.lock().await;
        inner.requests.clear();

        Ok(())
    }

    /// Delete all request files and reset state.
    pub async fn purge(&self) -> Result<()> {
        let mut inner = self.inner.lock().await;

        for file in Self::get_request_files(&self.path).await? {
            fs::remove_file(&file).await?;
        }

        inner.requests.clear();
        inner.insertion_counter = 0;
        inner.metadata.forefront_request_ids.clear();

        inner.metadata.handled_request_count = 0;
        inner.metadata.pending_request_count = 0;
        inner.metadata.total_request_count = 0;
        let now = self.clock.now();
        inner.metadata.base.accessed_at = now;
        inner.metadata.base.modified_at = now;

        let json = json_dumps_value(&inner.metadata)?;
        atomic_write(&self.path.join(METADATA_FILENAME), json.as_bytes()).await?;

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
        // Track unique_keys seen within *this* batch so a duplicate later in
        // the same call reports `was_already_present` (matching the JS client,
        // which keys its in-memory map by request id).
        let mut seen_in_batch: HashMap<String, ()> = HashMap::new();

        for request in requests {
            let unique_key = Self::extract_unique_key(&request)?;
            let request_id = unique_key_to_request_id(&unique_key);

            // Is this request handled already?
            let already_handled = inner
                .requests
                .get(&unique_key)
                .map(|e| e.order_no.is_none())
                .unwrap_or(false);

            if already_handled {
                processed.push(ProcessedRequest {
                    request_id,
                    unique_key,
                    was_already_present: true,
                    was_already_handled: true,
                });
                continue;
            }

            let already_present =
                inner.requests.contains_key(&unique_key) || seen_in_batch.contains_key(&unique_key);

            // Already pending and not requesting a forefront move: report it.
            if already_present && !forefront {
                processed.push(ProcessedRequest {
                    request_id,
                    unique_key,
                    was_already_present: true,
                    was_already_handled: false,
                });
                continue;
            }

            // Does the *incoming* request carry a non-null `handledAt`? If so it
            // is being added as already-handled (matches JS `_calculateOrderNo`,
            // which returns `null` whenever `request.handledAt` is set, causing
            // the JS `batchAddRequests` to bump `handledRequestCount` rather
            // than `pendingRequestCount`). A forefront move of an existing
            // pending request that *also* carries `handledAt` would be
            // contradictory — treat the `handledAt` intent as authoritative and
            // skip the forefront promotion, matching JS (which never promotes
            // a handled request into the forefront list).
            let add_as_handled = request
                .get("handledAt")
                .or_else(|| request.get("handled_at"))
                .map(|v| !v.is_null())
                .unwrap_or(false);

            // Compute the orderNo exactly like JS `_calculateOrderNo`: a plain
            // signed unix-millis timestamp (negative = forefront), or `null`
            // when the request is being added as already-handled.
            // Same-millisecond collisions are intentional and harmless —
            // ordering is resolved by `forefront_request_ids` + `insertion_seq`.
            let order_no = if add_as_handled {
                None
            } else {
                Some(self.calculate_order_no(forefront))
            };
            let insertion_seq = inner.insertion_counter;
            inner.insertion_counter += 1;

            // Build the persisted request: inject id + orderNo, preserving the
            // opaque user payload. `orderNo: null` marks the request as handled
            // on disk (matches the `mark_request_as_handled` write shape and
            // the `rebuild_index` reader, which treats null/missing orderNo as
            // handled).
            let mut to_write = request.clone();
            if let Value::Object(ref mut map) = to_write {
                map.insert("id".to_string(), Value::String(request_id.clone()));
                let order_no_value = match order_no {
                    Some(n) => Value::Number(n.into()),
                    None => Value::Null,
                };
                map.insert("orderNo".to_string(), order_no_value);
            }

            let file_path = self.get_request_path(&unique_key);
            let json = json_dumps(&to_write)?;
            atomic_write(&file_path, json.as_bytes()).await?;

            inner.requests.insert(
                unique_key.clone(),
                RequestEntry {
                    order_no,
                    insertion_seq,
                },
            );

            // Mirror JS: track forefront ids in an ordered list (LIFO on fetch).
            // A handled add never participates in fetch ordering, so it must
            // not enter the forefront list even if `forefront=true` was passed.
            if forefront && !add_as_handled {
                inner
                    .metadata
                    .forefront_request_ids
                    .push(unique_key.clone());
            }

            if !already_present {
                // Brand new request — bump counts. A forefront move of an
                // already-present request leaves counts unchanged.
                inner.metadata.total_request_count += 1;
                if add_as_handled {
                    inner.metadata.handled_request_count += 1;
                } else {
                    inner.metadata.pending_request_count += 1;
                }
            }

            seen_in_batch.insert(unique_key.clone(), ());

            processed.push(ProcessedRequest {
                request_id,
                unique_key,
                was_already_present: already_present,
                // Mirror JS: even when adding a fresh request that already
                // carries `handledAt`, the response reports
                // `was_already_handled: false`. (The JS comment: "that's how
                // API behaves.")
                was_already_handled: false,
            });
        }

        let now = self.clock.now();
        inner.metadata.base.accessed_at = now;
        inner.metadata.base.modified_at = now;
        let meta_json = json_dumps_value(&inner.metadata)?;
        atomic_write(&self.path.join(METADATA_FILENAME), meta_json.as_bytes()).await?;

        Ok(AddRequestsResponse {
            processed_requests: processed,
            unprocessed_requests: Vec::new(),
        })
    }

    /// Get a request by unique_key without locking it.
    pub async fn get_request(&self, unique_key: &str) -> Result<Option<Value>> {
        let file_path = self.get_request_path(unique_key);
        if !file_path.exists() {
            return Ok(None);
        }

        let content = fs::read_to_string(&file_path).await?;
        let mut request: Value = serde_json::from_str(&content)?;

        {
            let mut inner = self.inner.lock().await;
            inner.metadata.base.accessed_at = self.clock.now();
            let json = json_dumps_value(&inner.metadata)?;
            atomic_write(&self.path.join(METADATA_FILENAME), json.as_bytes()).await?;
        }

        Self::strip_queue_internals(&mut request);
        Ok(Some(request))
    }

    /// Fetch the next fetchable request, locking it for `lock_millis`.
    ///
    /// A request is fetchable if it is not handled and not currently locked
    /// (its `|orderNo|` is not in the future). The lock is persisted to the
    /// request file on disk (the `orderNo` is rewritten to `(now+lock)*sign`)
    /// so other consumers sharing the queue skip it — exactly the crawlee-js v3
    /// `listAndLockHead` model.
    ///
    /// Candidate order mirrors JS `requestKeyIterator`: forefront ids first
    /// (in reverse/LIFO insertion order), then regular requests by ascending
    /// `orderNo` with a stable `insertion_seq` tie-break.
    pub async fn fetch_next_request(&self) -> Result<Option<Value>> {
        let mut inner = self.inner.lock().await;

        let now = self.clock.now().timestamp_millis();
        let lock_millis = inner.lock_millis;

        let candidates = self.ordered_candidate_keys(&inner);

        for unique_key in candidates {
            let file_path = self.get_request_path(&unique_key);

            // Re-read the file: it is the source of truth for the lock. Another
            // consumer may have locked or handled it since we last indexed.
            let mut request: Value = match fs::read_to_string(&file_path).await {
                Ok(content) => match serde_json::from_str(&content) {
                    Ok(v) => v,
                    Err(_) => continue,
                },
                Err(_) => {
                    // File vanished — drop from index and move on.
                    inner.requests.remove(&unique_key);
                    continue;
                }
            };

            let disk_order = Self::read_order_no(&request);

            // Handled or locked on disk (by us-stale or a foreign consumer)?
            match disk_order {
                None => {
                    // Handled elsewhere — sync the index.
                    if let Some(entry) = inner.requests.get_mut(&unique_key) {
                        entry.order_no = None;
                    }
                    continue;
                }
                Some(n) if Self::is_locked_order(n, now) => {
                    // Locked elsewhere — sync the index and skip.
                    if let Some(entry) = inner.requests.get_mut(&unique_key) {
                        entry.order_no = Some(n);
                    }
                    continue;
                }
                Some(n) => {
                    // Fetchable. Lock it: push |orderNo| into the future,
                    // preserving sign (forefront stays forefront). This is the
                    // v3 model — the lock lives in orderNo itself.
                    let sign = if n > 0 { 1 } else { -1 };
                    let locked = (now + lock_millis) * sign;

                    if let Value::Object(ref mut map) = request {
                        map.insert("orderNo".to_string(), Value::Number(locked.into()));
                    }
                    let json = json_dumps(&request)?;
                    atomic_write(&file_path, json.as_bytes()).await?;

                    if let Some(entry) = inner.requests.get_mut(&unique_key) {
                        entry.order_no = Some(locked);
                    }

                    inner.metadata.base.accessed_at = self.clock.now();
                    let meta_json = json_dumps_value(&inner.metadata)?;
                    atomic_write(&self.path.join(METADATA_FILENAME), meta_json.as_bytes()).await?;

                    // Strip the queue-owned lock field before handing the
                    // request to the caller; we persisted the locked orderNo to
                    // disk above. The caller hands the request back to
                    // mark_request_as_handled/reclaim_request, which re-derive
                    // these fields from uniqueKey, so the round-trip is safe.
                    Self::strip_queue_internals(&mut request);
                    return Ok(Some(request));
                }
            }
        }

        Ok(None)
    }

    /// Compute the candidate fetch order: forefront ids first (reverse/LIFO),
    /// then unhandled, unlocked regular requests ordered by `(order_no,
    /// insertion_seq)`. Deduplicated. Mirrors JS `requestKeyIterator`.
    fn ordered_candidate_keys(&self, inner: &InnerState) -> Vec<String> {
        let now = self.clock.now().timestamp_millis();
        let mut seen: std::collections::HashSet<String> = std::collections::HashSet::new();
        let mut out: Vec<String> = Vec::new();

        // Forefront ids, most-recently-added first (LIFO), as JS does.
        for key in inner.metadata.forefront_request_ids.iter().rev() {
            if !seen.insert(key.clone()) {
                continue;
            }
            if let Some(entry) = inner.requests.get(key) {
                if matches!(entry.order_no, Some(n) if !Self::is_locked_order(n, now)) {
                    out.push(key.clone());
                }
            }
        }

        // Regular requests: ascending orderNo, stable insertion_seq tie-break.
        let mut regulars: Vec<(&String, i64, u64)> = inner
            .requests
            .iter()
            .filter_map(|(key, entry)| match entry.order_no {
                Some(n) if !Self::is_locked_order(n, now) && !seen.contains(key) => {
                    Some((key, n, entry.insertion_seq))
                }
                _ => None,
            })
            .collect();
        regulars.sort_by(|a, b| a.1.cmp(&b.1).then(a.2.cmp(&b.2)));
        for (key, _, _) in regulars {
            if seen.insert(key.clone()) {
                out.push(key.clone());
            }
        }

        out
    }

    /// Mark a request as handled.
    ///
    /// This does *not* require the request to still be locked by us — a slow
    /// consumer whose lock expired must still be able to mark it handled
    /// (otherwise the request would be handed out forever). Matches the JS
    /// `markRequestAsHandled` contract.
    pub async fn mark_request_as_handled(
        &self,
        mut request: Value,
    ) -> Result<Option<ProcessedRequest>> {
        let unique_key = Self::extract_unique_key(&request)?;
        let request_id = unique_key_to_request_id(&unique_key);

        let mut inner = self.inner.lock().await;

        let file_path = self.get_request_path(&unique_key);
        if !file_path.exists() && !inner.requests.contains_key(&unique_key) {
            // Unknown request — nothing to do.
            return Ok(None);
        }

        // Already handled? Idempotent success.
        let was_handled = inner
            .requests
            .get(&unique_key)
            .map(|e| e.order_no.is_none())
            .unwrap_or(false);
        if was_handled {
            return Ok(Some(ProcessedRequest {
                request_id,
                unique_key,
                was_already_present: true,
                was_already_handled: true,
            }));
        }

        // Set handledAt + clear orderNo (null => handled).
        let now = self.clock.now();
        let handled_at_str = now.format("%Y-%m-%dT%H:%M:%S%.6f+00:00").to_string();
        if let Value::Object(ref mut map) = request {
            map.insert("handledAt".to_string(), Value::String(handled_at_str));
            map.insert("orderNo".to_string(), Value::Null);
            map.entry("id")
                .or_insert_with(|| Value::String(request_id.clone()));
        }

        let json = json_dumps(&request)?;
        atomic_write(&file_path, json.as_bytes()).await?;

        let insertion_seq = inner
            .requests
            .get(&unique_key)
            .map(|e| e.insertion_seq)
            .unwrap_or(0);
        inner.requests.insert(
            unique_key.clone(),
            RequestEntry {
                order_no: None,
                insertion_seq,
            },
        );
        // A handled request no longer participates in forefront ordering.
        inner
            .metadata
            .forefront_request_ids
            .retain(|k| k != &unique_key);

        inner.metadata.handled_request_count += 1;
        inner.metadata.pending_request_count =
            inner.metadata.pending_request_count.saturating_sub(1);
        inner.metadata.base.accessed_at = now;
        inner.metadata.base.modified_at = now;

        let meta_json = json_dumps_value(&inner.metadata)?;
        atomic_write(&self.path.join(METADATA_FILENAME), meta_json.as_bytes()).await?;

        Ok(Some(ProcessedRequest {
            request_id,
            unique_key,
            was_already_present: true,
            was_already_handled: true,
        }))
    }

    /// Reclaim a request — return it to the pending pool (optionally forefront).
    ///
    /// Like `mark_request_as_handled`, this does not require the request to
    /// still be locked: a consumer whose lock expired must still be able to
    /// reclaim it. Returns `None` only if the request is unknown or already
    /// handled.
    pub async fn reclaim_request(
        &self,
        mut request: Value,
        forefront: bool,
    ) -> Result<Option<ProcessedRequest>> {
        let unique_key = Self::extract_unique_key(&request)?;
        let request_id = unique_key_to_request_id(&unique_key);

        let mut inner = self.inner.lock().await;

        let file_path = self.get_request_path(&unique_key);
        if !file_path.exists() && !inner.requests.contains_key(&unique_key) {
            return Ok(None);
        }

        // Already handled — can't reclaim.
        let was_handled = inner
            .requests
            .get(&unique_key)
            .map(|e| e.order_no.is_none())
            .unwrap_or(false);
        if was_handled {
            return Ok(None);
        }

        // Reset orderNo to "now" (unlocked, fetchable immediately), preserving
        // the forefront/regular sign — matching JS `deleteRequestLock`
        // (`forefront ? -start : start`).
        let order_no = self.calculate_order_no(forefront);

        if let Value::Object(ref mut map) = request {
            map.insert("orderNo".to_string(), Value::Number(order_no.into()));
            map.entry("id")
                .or_insert_with(|| Value::String(request_id.clone()));
        }

        let json = json_dumps(&request)?;
        atomic_write(&file_path, json.as_bytes()).await?;

        // Preserve the original insertion order for a reclaim; only mint a new
        // sequence if (somehow) the request wasn't already indexed.
        let insertion_seq = match inner.requests.get(&unique_key) {
            Some(e) => e.insertion_seq,
            None => {
                let s = inner.insertion_counter;
                inner.insertion_counter += 1;
                s
            }
        };
        inner.requests.insert(
            unique_key.clone(),
            RequestEntry {
                order_no: Some(order_no),
                insertion_seq,
            },
        );
        if forefront && !inner.metadata.forefront_request_ids.contains(&unique_key) {
            inner
                .metadata
                .forefront_request_ids
                .push(unique_key.clone());
        }

        let now_dt = self.clock.now();
        inner.metadata.base.accessed_at = now_dt;
        inner.metadata.base.modified_at = now_dt;
        let meta_json = json_dumps_value(&inner.metadata)?;
        atomic_write(&self.path.join(METADATA_FILENAME), meta_json.as_bytes()).await?;

        Ok(Some(ProcessedRequest {
            request_id,
            unique_key,
            was_already_present: true,
            was_already_handled: false,
        }))
    }

    /// Whether the queue is empty in the *fetchable* sense: would the next
    /// `fetch_next_request()` return `None`?
    ///
    /// Locked (in-progress) requests are NOT counted — a queue holding only
    /// locked requests is "empty" by this definition but not *finished*.
    /// This matches the crawlee-js v4 `isEmpty` contract.
    pub async fn is_empty(&self) -> bool {
        let mut inner = self.inner.lock().await;
        let now = self.clock.now().timestamp_millis();

        let has_fetchable = inner.requests.values().any(|e| match e.order_no {
            Some(n) => !Self::is_locked_order(n, now),
            None => false,
        });

        // Best-effort accessed_at bump (is_empty returns bool, not Result — so
        // any write error is ignored).
        inner.metadata.base.accessed_at = self.clock.now();
        if let Ok(json) = json_dumps_value(&inner.metadata) {
            let _ = atomic_write(&self.path.join(METADATA_FILENAME), json.as_bytes()).await;
        }

        !has_fetchable
    }

    /// Whether the queue is finished: no unhandled requests remain anywhere,
    /// including requests currently locked/in-progress by any consumer.
    ///
    /// This is the signal the crawler's completion logic depends on. It is the
    /// strong counterpart to [`is_empty`](Self::is_empty).
    pub async fn is_finished(&self) -> bool {
        let inner = self.inner.lock().await;
        // Any request with a non-null orderNo is still outstanding (pending or
        // locked). Only when every request is handled (orderNo == None) is the
        // queue finished.
        !inner.requests.values().any(|e| e.order_no.is_some())
    }

    /// Hint how long a fetched request should stay locked, in seconds.
    ///
    /// The crawler knows the only sensible lock duration (long enough to
    /// outlive its request handler). We only ever *raise* the duration so a
    /// short-lived consumer sharing the queue cannot cut short a long-lived
    /// one's reservation. Matches the JS `setExpectedRequestProcessingTime`.
    pub async fn set_expected_request_processing_time(&self, secs: f64) {
        let millis = (secs * 1000.0) as i64;
        let mut inner = self.inner.lock().await;
        if millis > inner.lock_millis {
            inner.lock_millis = millis;
        }
    }

    /// Retained for binding compatibility. The orderNo lock model persists
    /// everything inline in the request files (plus the forefront ordering in
    /// metadata), so there is no separate state blob to flush — this is a no-op.
    pub async fn persist_state(&self) {
        // Intentionally empty: state lives in the request files + metadata.
    }

    // ─── Private ────────────────────────────────────────────────────────────

    /// A request is locked if it is not handled and its `|orderNo|` lies in the
    /// future relative to `now` (unix millis). Matches the JS `isRequestLocked`:
    /// `orderNo > now || orderNo < -now`. Because `orderNo` is always exactly
    /// `±now` at add time (never inflated), a freshly-added request is never
    /// mistaken for a lock — the cause of the earlier creep race is gone.
    fn is_locked_order(order_no: i64, now: i64) -> bool {
        order_no > now || order_no < -now
    }

    fn extract_unique_key(request: &Value) -> Result<String> {
        request
            .get("uniqueKey")
            .or_else(|| request.get("unique_key"))
            .and_then(|v| v.as_str())
            .map(|s| s.to_string())
            .ok_or_else(|| {
                StorageError::InvalidArgs("Request must have a 'uniqueKey' field".to_string())
            })
    }

    /// Remove queue-owned bookkeeping fields from a request before returning it
    /// to the caller. `orderNo` is the lock/ordering state and is purely
    /// internal; it lives on disk but must never leak to consumers. (`id` is
    /// kept — it is a stable, caller-meaningful request identifier.)
    fn strip_queue_internals(request: &mut Value) {
        if let Value::Object(ref mut map) = request {
            map.remove("orderNo");
        }
    }

    fn read_order_no(request: &Value) -> Option<i64> {
        match request.get("orderNo") {
            Some(Value::Number(n)) => n.as_i64(),
            // Missing or explicitly null => handled.
            _ => None,
        }
    }

    /// Compute the orderNo for a freshly added/reclaimed request, exactly like
    /// JS `_calculateOrderNo`: a plain signed unix-millis timestamp. Positive =
    /// regular, negative = forefront.
    ///
    /// Same-millisecond collisions are intentional and harmless. Ordering is
    /// not derived from unique orderNos; it comes from `forefront_request_ids`
    /// (forefront priority/LIFO) and `insertion_seq` (stable FIFO tie-break for
    /// regulars), mirroring how JS uses its `forefrontRequestIds` list plus the
    /// insertion order of its request `Map`. Crucially, the magnitude is always
    /// exactly `now`, so it can never be misread as a future-dated lock.
    fn calculate_order_no(&self, forefront: bool) -> i64 {
        let now = self.clock.now().timestamp_millis();
        if forefront {
            -now
        } else {
            now
        }
    }

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

    /// Rebuild the in-memory index from the request files on disk, and
    /// recompute the metadata counts from the authoritative file contents.
    ///
    /// `assume_sole_owner` controls how future-dated `orderNo`s are handled:
    ///
    /// - `false` (default): trust the on-disk value. A future-dated `|orderNo|`
    ///   is either a still-running peer's live lock (must be respected) or a
    ///   crashed peer's lock (which will expire naturally on the wall clock).
    ///   The worst-case stall after a crash is therefore one lock window
    ///   (default 3 minutes, tunable via `set_expected_request_processing_time`).
    ///   This is the safe cross-process default.
    /// - `true`: reclaim every future-dated `|orderNo|` by rewriting it to
    ///   `±now` (preserving the forefront/regular sign) and persisting it,
    ///   so a previously in-progress request is immediately re-fetchable.
    ///   The caller is asserting nothing else is using this queue — if a peer
    ///   *is* live, this will clobber its reservation and let two consumers
    ///   hand out the same request.
    ///
    /// The `forefront_request_ids` ordering list is restored from metadata (it
    /// deserializes with the metadata) and pruned here to drop handled/missing
    /// entries.
    ///
    /// `insertion_seq` is assigned in (sorted) file-read order so reopened
    /// regular requests keep a stable, deterministic FIFO order.
    async fn rebuild_index(&self, assume_sole_owner: bool) -> Result<()> {
        let mut request_files = Self::get_request_files(&self.path).await?;
        // Stable file order so insertion_seq assignment is deterministic.
        request_files.sort();

        let mut inner = self.inner.lock().await;
        inner.requests.clear();
        let prior_forefront = std::mem::take(&mut inner.metadata.forefront_request_ids);

        let now = self.clock.now().timestamp_millis();
        let mut handled = 0usize;
        let mut pending = 0usize;
        let mut seq = 0u64;

        for file_path in request_files {
            let content = match fs::read_to_string(&file_path).await {
                Ok(c) => c,
                Err(e) => {
                    warn!("Failed to read request file {}: {}", file_path.display(), e);
                    continue;
                }
            };
            let mut request: Value = match serde_json::from_str(&content) {
                Ok(r) => r,
                Err(e) => {
                    warn!(
                        "Failed to parse request file {}: {}",
                        file_path.display(),
                        e
                    );
                    continue;
                }
            };

            let unique_key = match Self::extract_unique_key(&request) {
                Ok(k) => k,
                Err(_) => continue,
            };

            // Determine handled state. A request is handled if it has a
            // non-null `handledAt`, OR its persisted orderNo is null. Otherwise
            // it carries whatever orderNo is on disk (assigning one if a legacy
            // file lacks it).
            let has_handled_at = request
                .get("handledAt")
                .or_else(|| request.get("handled_at"))
                .map(|v| !v.is_null())
                .unwrap_or(false);

            let order_no = if has_handled_at {
                None
            } else {
                let disk_order = Self::read_order_no(&request).unwrap_or(now);

                if assume_sole_owner && Self::is_locked_order(disk_order, now) {
                    // Sole-owner mode: a future-dated orderNo is assumed to be
                    // a stale lock from a previous (now-dead) run, so we reset
                    // it to ±now (preserving the forefront/regular sign) and
                    // persist that, making the request immediately fetchable.
                    let sign = if disk_order > 0 { 1 } else { -1 };
                    let reclaimed = now * sign;
                    if let Value::Object(ref mut map) = request {
                        map.insert("orderNo".to_string(), Value::Number(reclaimed.into()));
                    }
                    if let Err(e) = atomic_write(&file_path, json_dumps(&request)?.as_bytes()).await
                    {
                        warn!(
                            "Failed to clear stale lock for {}: {}",
                            file_path.display(),
                            e
                        );
                    }
                    Some(reclaimed)
                } else {
                    // Default (cross-process-safe) mode: trust the on-disk
                    // value. If it's in the future, it's either a live peer's
                    // lock (must respect) or a crashed peer's lock (will
                    // naturally expire on the wall clock).
                    Some(disk_order)
                }
            };

            if order_no.is_none() {
                handled += 1;
            } else {
                pending += 1;
            }

            let insertion_seq = seq;
            seq += 1;
            inner.requests.insert(
                unique_key,
                RequestEntry {
                    order_no,
                    insertion_seq,
                },
            );
        }

        // Restore the forefront ordering, dropping entries that are gone or no
        // longer pending (mirrors JS pruning handled forefront ids).
        inner.insertion_counter = seq;
        inner.metadata.forefront_request_ids = prior_forefront
            .into_iter()
            .filter(|k| {
                inner
                    .requests
                    .get(k)
                    .map(|e| e.order_no.is_some())
                    .unwrap_or(false)
            })
            .collect();
        inner.metadata.handled_request_count = handled;
        inner.metadata.pending_request_count = pending;
        inner.metadata.total_request_count = handled + pending;

        let json = json_dumps_value(&inner.metadata)?;
        atomic_write(&self.path.join(METADATA_FILENAME), json.as_bytes()).await?;

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    fn req(unique_key: &str) -> Value {
        serde_json::json!({
            "uniqueKey": unique_key,
            "url": format!("https://example.com/{unique_key}"),
            "method": "GET",
        })
    }

    /// Read the raw persisted request file straight off disk, bypassing the
    /// client API. Used by tests that assert on the *on-disk* format (e.g. the
    /// queue-owned `orderNo`/`id` fields that the client strips before
    /// returning requests to callers).
    fn read_persisted_request(client: &FileSystemRequestQueueClient, unique_key: &str) -> Value {
        let path = client.get_request_path(unique_key);
        let content = std::fs::read_to_string(path).expect("request file should exist on disk");
        serde_json::from_str(&content).expect("request file should be valid JSON")
    }

    #[tokio::test]
    async fn test_add_and_fetch_request() {
        let temp_dir = TempDir::new().unwrap();
        let client = FileSystemRequestQueueClient::open(None, None, None, temp_dir.path())
            .await
            .unwrap();

        let response = client
            .add_batch_of_requests(vec![req("https://example.com")], false)
            .await
            .unwrap();
        assert_eq!(response.processed_requests.len(), 1);
        assert!(!response.processed_requests[0].was_already_present);
        assert!(!response.processed_requests[0].request_id.is_empty());

        let fetched = client.fetch_next_request().await.unwrap().unwrap();
        assert_eq!(fetched["uniqueKey"], "https://example.com");

        // Locked now — nothing else fetchable.
        assert!(client.fetch_next_request().await.unwrap().is_none());
    }

    /// fetch_next_request must not leak the queue-owned `orderNo` lock field to
    /// the caller, but the lock must still be persisted to disk (so peers skip
    /// it), and the stripped request must still round-trip through
    /// mark_request_as_handled.
    #[tokio::test]
    async fn test_fetch_next_request_strips_order_no() {
        let temp_dir = TempDir::new().unwrap();
        let client = FileSystemRequestQueueClient::open(None, None, None, temp_dir.path())
            .await
            .unwrap();

        client
            .add_batch_of_requests(vec![req("leaky")], false)
            .await
            .unwrap();

        let fetched = client.fetch_next_request().await.unwrap().unwrap();
        assert!(
            fetched.get("orderNo").is_none(),
            "fetched request must not expose internal orderNo"
        );

        // The lock is still on disk (future-dated, so the request is locked).
        let persisted = read_persisted_request(&client, "leaky");
        assert!(
            persisted.get("orderNo").and_then(|v| v.as_i64()).is_some(),
            "the lock must be persisted to disk even though it's stripped on return"
        );

        // The stripped request round-trips back into mark_request_as_handled.
        let result = client.mark_request_as_handled(fetched).await.unwrap();
        assert!(result.is_some());
        assert!(client.is_finished().await);
    }

    /// Stray non-JSON files (and malformed JSON) dropped into the queue directory must be
    /// ignored when the index is rebuilt on open, not error and not be counted as requests.
    #[tokio::test]
    async fn test_ignore_non_json_files_on_reopen() {
        let temp_dir = TempDir::new().unwrap();
        let storage_dir = temp_dir.path();

        // Create a queue with a single valid request and persist it to disk.
        let client =
            FileSystemRequestQueueClient::open(None, Some("q".to_string()), None, storage_dir)
                .await
                .unwrap();
        client
            .add_batch_of_requests(vec![req("only-valid")], false)
            .await
            .unwrap();
        drop(client);

        // Drop stray files directly into the queue directory, out-of-band.
        let queue_dir = storage_dir.join("request_queues").join("q");
        fs::write(queue_dir.join(".DS_Store"), b"not json at all")
            .await
            .unwrap();
        fs::write(queue_dir.join("invalid.txt"), b"also not json")
            .await
            .unwrap();
        // A malformed *.json file: right extension, garbage contents. This one
        // is enumerated by get_request_files but must be warn-and-skipped by
        // rebuild_index rather than aborting the open.
        fs::write(queue_dir.join("broken.json"), b"{ this is not valid json")
            .await
            .unwrap();

        // Reopening must succeed and see exactly the one valid request.
        let reopened =
            FileSystemRequestQueueClient::open(None, Some("q".to_string()), None, storage_dir)
                .await
                .unwrap();

        assert_eq!(reopened.get_metadata().await.total_request_count, 1);

        let fetched = reopened.fetch_next_request().await.unwrap().unwrap();
        assert_eq!(fetched["uniqueKey"], "only-valid");

        // Only the one valid request existed; nothing else is fetchable.
        assert!(reopened.fetch_next_request().await.unwrap().is_none());
    }

    #[tokio::test]
    async fn test_deduplication() {
        let temp_dir = TempDir::new().unwrap();
        let client = FileSystemRequestQueueClient::open(None, None, None, temp_dir.path())
            .await
            .unwrap();

        client
            .add_batch_of_requests(vec![req("dup")], false)
            .await
            .unwrap();
        let response = client
            .add_batch_of_requests(vec![req("dup")], false)
            .await
            .unwrap();

        assert!(response.processed_requests[0].was_already_present);
    }

    #[tokio::test]
    async fn test_intra_batch_deduplication() {
        let temp_dir = TempDir::new().unwrap();
        let client = FileSystemRequestQueueClient::open(None, None, None, temp_dir.path())
            .await
            .unwrap();

        let response = client
            .add_batch_of_requests(vec![req("dup"), req("dup")], false)
            .await
            .unwrap();

        assert_eq!(response.processed_requests.len(), 2);
        assert!(!response.processed_requests[0].was_already_present);
        assert!(
            response.processed_requests[1].was_already_present,
            "second occurrence of the same uniqueKey within one batch must report was_already_present"
        );
        // Only one pending request should result.
        assert_eq!(client.get_metadata().await.total_request_count, 1);
    }

    #[tokio::test]
    async fn test_request_id_is_deterministic_and_nonempty() {
        let temp_dir = TempDir::new().unwrap();
        let client = FileSystemRequestQueueClient::open(None, None, None, temp_dir.path())
            .await
            .unwrap();

        let response = client
            .add_batch_of_requests(vec![req("abc")], false)
            .await
            .unwrap();
        let id = &response.processed_requests[0].request_id;
        assert_eq!(*id, unique_key_to_request_id("abc"));
        assert!(!id.is_empty());
    }

    #[tokio::test]
    async fn test_mark_as_handled() {
        let temp_dir = TempDir::new().unwrap();
        let client = FileSystemRequestQueueClient::open(None, None, None, temp_dir.path())
            .await
            .unwrap();

        client
            .add_batch_of_requests(vec![req("req1")], false)
            .await
            .unwrap();

        let request = client.fetch_next_request().await.unwrap().unwrap();
        let result = client.mark_request_as_handled(request).await.unwrap();
        assert!(result.is_some());
        assert!(result.unwrap().was_already_handled);

        assert!(client.is_empty().await);
        assert!(client.is_finished().await);
        assert_eq!(client.get_metadata().await.handled_request_count, 1);
    }

    #[tokio::test]
    async fn test_reclaim_request() {
        let temp_dir = TempDir::new().unwrap();
        let client = FileSystemRequestQueueClient::open(None, None, None, temp_dir.path())
            .await
            .unwrap();

        client
            .add_batch_of_requests(vec![req("req1")], false)
            .await
            .unwrap();

        let request = client.fetch_next_request().await.unwrap().unwrap();
        let result = client.reclaim_request(request, false).await.unwrap();
        assert!(result.is_some());

        // Should be fetchable again (lock released).
        assert!(client.fetch_next_request().await.unwrap().is_some());
    }

    #[tokio::test]
    async fn test_forefront() {
        let temp_dir = TempDir::new().unwrap();
        let client = FileSystemRequestQueueClient::open(None, None, None, temp_dir.path())
            .await
            .unwrap();

        client
            .add_batch_of_requests(vec![req("regular")], false)
            .await
            .unwrap();
        client
            .add_batch_of_requests(vec![req("priority")], true)
            .await
            .unwrap();

        let first = client.fetch_next_request().await.unwrap().unwrap();
        assert_eq!(first["uniqueKey"], "priority");
    }

    #[tokio::test]
    async fn test_is_empty_vs_is_finished_with_locked_request() {
        let temp_dir = TempDir::new().unwrap();
        let client = FileSystemRequestQueueClient::open(None, None, None, temp_dir.path())
            .await
            .unwrap();

        client
            .add_batch_of_requests(vec![req("req1")], false)
            .await
            .unwrap();

        // Fetch (locks it).
        let request = client.fetch_next_request().await.unwrap().unwrap();

        // Nothing else fetchable => empty in the "fetchable" sense.
        assert!(
            client.is_empty().await,
            "is_empty() should be true when the only request is locked (in progress)"
        );
        // But work remains => NOT finished.
        assert!(
            !client.is_finished().await,
            "is_finished() must be false while a request is locked/in progress"
        );

        client.mark_request_as_handled(request).await.unwrap();
        assert!(client.is_empty().await);
        assert!(client.is_finished().await);
    }

    #[tokio::test]
    async fn test_lock_expiry_allows_refetch() {
        let temp_dir = TempDir::new().unwrap();
        let client = FileSystemRequestQueueClient::open(None, None, None, temp_dir.path())
            .await
            .unwrap();

        // Make the lock effectively zero so it expires immediately.
        client.set_expected_request_processing_time(0.0).await;
        // set_* only raises, so force the internal value down for the test.
        {
            let mut inner = client.inner.lock().await;
            inner.lock_millis = 0;
        }

        client
            .add_batch_of_requests(vec![req("req1")], false)
            .await
            .unwrap();

        let first = client.fetch_next_request().await.unwrap();
        assert!(first.is_some());

        // Lock is 0ms so the request becomes fetchable again immediately.
        tokio::time::sleep(std::time::Duration::from_millis(5)).await;
        let again = client.fetch_next_request().await.unwrap();
        assert!(
            again.is_some(),
            "an expired lock must allow the request to be fetched again"
        );
    }

    /// Same behavior as `test_lock_expiry_allows_refetch`, but proves the
    /// [`TestClock`](crate::clock::TestClock) injection actually moves the time
    /// the queue observes. This is what the binding layer relies on so that
    /// JS/Python tests can verify lock-expiry without real sleeps (since
    /// `vi.useFakeTimers()` etc. don't cross the FFI).
    #[tokio::test]
    async fn test_lock_expiry_with_injected_test_clock() {
        use crate::clock::TestClock;
        use std::sync::Arc;

        let temp_dir = TempDir::new().unwrap();
        let clock = Arc::new(TestClock::new());
        let client = FileSystemRequestQueueClient::open_with_clock(
            None,
            None,
            None,
            temp_dir.path(),
            clock.clone(),
            false,
        )
        .await
        .unwrap();

        // Keep the lock at the default 3 minutes — we'll travel past it on the
        // test clock instead of shrinking it. The point is to prove the clock
        // hook works, not to retest the zero-lock path.
        client
            .add_batch_of_requests(vec![req("req1")], false)
            .await
            .unwrap();

        let first = client.fetch_next_request().await.unwrap();
        assert!(first.is_some(), "first fetch should succeed");

        // Without advancing, the lock is still in the future — request must
        // not be fetchable.
        let blocked = client.fetch_next_request().await.unwrap();
        assert!(
            blocked.is_none(),
            "request should still be locked while clock hasn't advanced"
        );

        // Jump past the default 3-minute lock window.
        clock.advance(4 * 60 * 1000);

        let again = client.fetch_next_request().await.unwrap();
        assert!(
            again.is_some(),
            "after advancing the test clock past the lock window, the request must be re-fetchable"
        );
    }

    /// A [`TestClock`](crate::clock::TestClock) can be shared by multiple
    /// clients in the same process — both observe the same advancement. This
    /// is how a JS test for two clients sharing one on-disk queue can advance
    /// time uniformly.
    #[tokio::test]
    async fn test_test_clock_is_shared_across_clients() {
        use crate::clock::TestClock;
        use std::sync::Arc;

        let temp_dir = TempDir::new().unwrap();
        let clock = Arc::new(TestClock::new());

        let client_a = FileSystemRequestQueueClient::open_with_clock(
            None,
            None,
            None,
            temp_dir.path(),
            clock.clone(),
            false,
        )
        .await
        .unwrap();

        client_a
            .add_batch_of_requests(vec![req("req1")], false)
            .await
            .unwrap();
        let _locked = client_a.fetch_next_request().await.unwrap().unwrap();

        // Advance time on the shared clock.
        clock.advance(4 * 60 * 1000);

        // A *second* client opened against the same dir+clock must also see
        // the request as unlocked. We open it after the advancement so it
        // inherits the same view of time.
        let client_b = FileSystemRequestQueueClient::open_with_clock(
            None,
            None,
            None,
            temp_dir.path(),
            clock.clone(),
            false,
        )
        .await
        .unwrap();

        let fetched = client_b.fetch_next_request().await.unwrap();
        assert!(
            fetched.is_some(),
            "second client sharing the test clock should see the lock as expired"
        );
    }

    /// Cross-process correctness: when one client locks a request and a second
    /// client opens against the same on-disk queue *while the lock is still
    /// live*, the second client must NOT reclaim the lock. Otherwise two
    /// peers could hand out the same request.
    ///
    /// Previously, `rebuild_index` reset every future-dated `orderNo` on the
    /// theory that it was a stale crash artifact. That heuristic is wrong
    /// under concurrency: a future-dated `orderNo` is exactly what a live
    /// peer's lock looks like. Now we trust the on-disk value; the lock
    /// window itself handles the crashed-peer case via expiry.
    #[tokio::test]
    async fn test_open_does_not_clobber_live_peer_lock() {
        use crate::clock::TestClock;
        use std::sync::Arc;

        let temp_dir = TempDir::new().unwrap();
        // Two independent clocks: A and B don't share a notion of "now". This
        // matches reality across two processes — their wall clocks are merely
        // *roughly* synchronized, not literally identical.
        let clock_a = Arc::new(TestClock::new());
        let clock_b = Arc::new(TestClock::new());

        let client_a = FileSystemRequestQueueClient::open_with_clock(
            None,
            None,
            None,
            temp_dir.path(),
            clock_a.clone(),
            false,
        )
        .await
        .unwrap();

        client_a
            .add_batch_of_requests(vec![req("req1")], false)
            .await
            .unwrap();
        let locked = client_a.fetch_next_request().await.unwrap().unwrap();
        assert_eq!(locked["uniqueKey"], "req1");

        // B opens *now*, while A still holds the lock. B must respect it.
        let client_b = FileSystemRequestQueueClient::open_with_clock(
            None,
            None,
            None,
            temp_dir.path(),
            clock_b.clone(),
            false,
        )
        .await
        .unwrap();

        let blocked = client_b.fetch_next_request().await.unwrap();
        assert!(
            blocked.is_none(),
            "client B must NOT fetch a request that client A has locked — \
             open() must not clobber a live peer's lock"
        );

        // And A can still complete its work as if B had never opened.
        let processed = client_a
            .mark_request_as_handled(locked)
            .await
            .unwrap()
            .unwrap();
        assert!(processed.was_already_handled);
    }

    #[tokio::test]
    async fn test_mark_handled_after_lock_expiry() {
        let temp_dir = TempDir::new().unwrap();
        let client = FileSystemRequestQueueClient::open(None, None, None, temp_dir.path())
            .await
            .unwrap();

        {
            let mut inner = client.inner.lock().await;
            inner.lock_millis = 0;
        }

        client
            .add_batch_of_requests(vec![req("req1")], false)
            .await
            .unwrap();

        let request = client.fetch_next_request().await.unwrap().unwrap();
        tokio::time::sleep(std::time::Duration::from_millis(5)).await;

        // Even though our lock expired, marking handled must still succeed.
        let result = client.mark_request_as_handled(request).await.unwrap();
        assert!(
            result.is_some(),
            "mark_request_as_handled must succeed even after the lock expired"
        );
        assert!(client.is_finished().await);
    }

    #[tokio::test]
    async fn test_reopen_does_not_duplicate_counts() {
        let temp_dir = TempDir::new().unwrap();
        let storage_dir = temp_dir.path();

        let client = FileSystemRequestQueueClient::open(None, None, None, storage_dir)
            .await
            .unwrap();
        client
            .add_batch_of_requests(vec![req("req1"), req("req2")], false)
            .await
            .unwrap();

        let meta = client.get_metadata().await;
        assert_eq!(meta.total_request_count, 2);
        assert_eq!(meta.pending_request_count, 2);
        drop(client);

        let client2 = FileSystemRequestQueueClient::open(None, None, None, storage_dir)
            .await
            .unwrap();
        let meta2 = client2.get_metadata().await;
        assert_eq!(meta2.total_request_count, 2);
        assert_eq!(meta2.pending_request_count, 2);
    }

    #[tokio::test]
    async fn test_purge_resets_everything() {
        let temp_dir = TempDir::new().unwrap();
        let client = FileSystemRequestQueueClient::open(None, None, None, temp_dir.path())
            .await
            .unwrap();

        client
            .add_batch_of_requests(vec![req("req1"), req("req2")], false)
            .await
            .unwrap();
        assert_eq!(client.get_metadata().await.total_request_count, 2);

        client.purge().await.unwrap();

        let meta = client.get_metadata().await;
        assert_eq!(meta.total_request_count, 0);
        assert!(client.is_empty().await);
        assert!(client.is_finished().await);
    }

    #[tokio::test]
    async fn test_get_request_updates_accessed_at() {
        let temp_dir = TempDir::new().unwrap();
        let client = FileSystemRequestQueueClient::open(None, None, None, temp_dir.path())
            .await
            .unwrap();

        client
            .add_batch_of_requests(vec![req("req1")], false)
            .await
            .unwrap();

        let accessed_before = client.get_metadata().await.base.accessed_at;
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        let _ = client.get_request("req1").await.unwrap();
        let accessed_after = client.get_metadata().await.base.accessed_at;
        assert!(accessed_after > accessed_before);
    }

    #[tokio::test]
    async fn test_fetch_after_purge_with_carried_creep() {
        let temp_dir = TempDir::new().unwrap();
        let client = FileSystemRequestQueueClient::open(None, None, None, temp_dir.path())
            .await
            .unwrap();

        // Burst of same-millisecond adds inflates the monotonic magnitude well
        // past `now`, then purge clears the index. If purge fails to reset the
        // creep (or is_locked_order can't tell creep from a real lock), the
        // freshly-added request after purge looks "locked" and is skipped.
        let batch: Vec<Value> = (0..50).map(|i| req(&format!("pre{i}"))).collect();
        client.add_batch_of_requests(batch, false).await.unwrap();
        client.purge().await.unwrap();

        client
            .add_batch_of_requests(vec![req("after")], false)
            .await
            .unwrap();

        assert!(
            !client.is_empty().await,
            "queue must not report empty when a fetchable request exists post-purge"
        );
        let fetched = client.fetch_next_request().await.unwrap();
        assert!(
            fetched.is_some(),
            "fetch_next_request must return the request that exists on disk post-purge"
        );
        assert_eq!(fetched.unwrap()["uniqueKey"], "after");
    }

    #[tokio::test]
    async fn test_large_same_ms_burst_all_fetchable() {
        let temp_dir = TempDir::new().unwrap();
        let client = FileSystemRequestQueueClient::open(None, None, None, temp_dir.path())
            .await
            .unwrap();

        // A big burst in (likely) one millisecond inflates magnitudes far past
        // `now`. Every one of them must still be fetchable — none may be
        // mistaken for a future-dated lock.
        let n = 500;
        let batch: Vec<Value> = (0..n).map(|i| req(&format!("b{i:04}"))).collect();
        client.add_batch_of_requests(batch, false).await.unwrap();

        assert!(!client.is_empty().await);
        let mut fetched = 0;
        while let Some(r) = client.fetch_next_request().await.unwrap() {
            client.mark_request_as_handled(r).await.unwrap();
            fetched += 1;
        }
        assert_eq!(
            fetched, n,
            "every request in a same-ms burst must be fetchable"
        );
    }

    #[tokio::test]
    async fn test_fifo_order_within_same_millisecond_batch() {
        let temp_dir = TempDir::new().unwrap();
        let client = FileSystemRequestQueueClient::open(None, None, None, temp_dir.path())
            .await
            .unwrap();

        // Add many requests in a single batch (same/adjacent millisecond) so
        // any timestamp-only orderNo would collide.
        let keys: Vec<String> = (0..20).map(|i| format!("r{i:02}")).collect();
        let batch: Vec<Value> = keys.iter().map(|k| req(k)).collect();
        client.add_batch_of_requests(batch, false).await.unwrap();

        // They must come back in insertion order (FIFO).
        for expected in &keys {
            let fetched = client.fetch_next_request().await.unwrap().unwrap();
            assert_eq!(
                fetched["uniqueKey"], *expected,
                "FIFO order violated: expected {expected}, got {}",
                fetched["uniqueKey"]
            );
            client.mark_request_as_handled(fetched).await.unwrap();
        }
    }

    #[tokio::test]
    async fn test_forefront_jumps_queue_even_same_millisecond() {
        let temp_dir = TempDir::new().unwrap();
        let client = FileSystemRequestQueueClient::open(None, None, None, temp_dir.path())
            .await
            .unwrap();

        // Add regulars, then a forefront — all likely within one millisecond.
        client
            .add_batch_of_requests(vec![req("first"), req("second")], false)
            .await
            .unwrap();
        client
            .add_batch_of_requests(vec![req("priority")], true)
            .await
            .unwrap();

        let fetched = client.fetch_next_request().await.unwrap().unwrap();
        assert_eq!(
            fetched["uniqueKey"], "priority",
            "forefront request must be fetched first"
        );
    }

    #[tokio::test]
    async fn test_multiple_forefront_preserve_lifo_or_fifo_order() {
        let temp_dir = TempDir::new().unwrap();
        let client = FileSystemRequestQueueClient::open(None, None, None, temp_dir.path())
            .await
            .unwrap();

        client
            .add_batch_of_requests(vec![req("regular")], false)
            .await
            .unwrap();
        // Two forefront requests added in the same batch: both must precede the
        // regular one, and must have a deterministic order between themselves.
        client
            .add_batch_of_requests(vec![req("ff1"), req("ff2")], true)
            .await
            .unwrap();

        let a = client.fetch_next_request().await.unwrap().unwrap();
        let b = client.fetch_next_request().await.unwrap().unwrap();
        let c = client.fetch_next_request().await.unwrap().unwrap();

        assert_eq!(c["uniqueKey"], "regular", "regular must be fetched last");
        let front: Vec<String> = [&a, &b]
            .iter()
            .map(|v| v["uniqueKey"].as_str().unwrap().to_string())
            .collect();
        assert!(
            front.contains(&"ff1".to_string()) && front.contains(&"ff2".to_string()),
            "both forefront requests must precede the regular one, got {front:?}"
        );
    }

    #[tokio::test]
    async fn test_in_progress_requests_recovered_after_crash() {
        use crate::clock::TestClock;
        use std::sync::Arc;

        let temp_dir = TempDir::new().unwrap();
        let storage_dir = temp_dir.path();
        let clock = Arc::new(TestClock::new());

        let client = FileSystemRequestQueueClient::open_with_clock(
            None,
            None,
            Some("crash".to_string()),
            storage_dir,
            clock.clone(),
            false,
        )
        .await
        .unwrap();
        client
            .add_batch_of_requests(vec![req("a"), req("b"), req("c")], false)
            .await
            .unwrap();

        // Fetch one (locks/in-progress), then "crash" (drop without handling).
        let fetched = client.fetch_next_request().await.unwrap().unwrap();
        assert_eq!(fetched["uniqueKey"], "a");
        client.persist_state().await;
        drop(client);

        // Reopen on the *same* test clock. The locked request's lock is still
        // in effect by the queue's reckoning (the lock window hasn't passed),
        // so the request is recovered as pending-but-locked.
        let client2 = FileSystemRequestQueueClient::open_with_clock(
            None,
            None,
            Some("crash".to_string()),
            storage_dir,
            clock.clone(),
            false,
        )
        .await
        .unwrap();

        let meta = client2.get_metadata().await;
        assert_eq!(
            meta.pending_request_count, 3,
            "all 3 unhandled requests must survive a crash"
        );
        assert!(!client2.is_finished().await);

        // Crash recovery story: walk the clock past the lock window so the
        // crashed peer's lock expires. After that, all three requests are
        // fetchable in turn.
        clock.advance(4 * 60 * 1000);

        let mut seen = std::collections::HashSet::new();
        for _ in 0..3 {
            let r = client2.fetch_next_request().await.unwrap().unwrap();
            seen.insert(r["uniqueKey"].as_str().unwrap().to_string());
            client2.mark_request_as_handled(r).await.unwrap();
            // Push the clock forward between fetches so each fresh lock also
            // expires before the next iteration.
            clock.advance(4 * 60 * 1000);
        }
        assert_eq!(
            seen.len(),
            3,
            "all 3 distinct requests must be recovered, got {seen:?}"
        );
        assert!(client2.is_finished().await);
    }

    /// Sole-owner mode: when the caller asserts nothing else is using the
    /// queue, `open()` actively reclaims future-dated `orderNo`s on disk
    /// instead of waiting for them to expire. This restores the historical
    /// "instant crash recovery" behavior for single-process consumers.
    #[tokio::test]
    async fn test_assume_sole_owner_reclaims_locks_on_open() {
        use crate::clock::TestClock;
        use std::sync::Arc;

        let temp_dir = TempDir::new().unwrap();
        let storage_dir = temp_dir.path();
        let clock = Arc::new(TestClock::new());

        // First client locks the request, then "crashes".
        let client = FileSystemRequestQueueClient::open_with_clock(
            None,
            None,
            Some("sole".to_string()),
            storage_dir,
            clock.clone(),
            false,
        )
        .await
        .unwrap();
        client
            .add_batch_of_requests(vec![req("a")], false)
            .await
            .unwrap();
        let _ = client.fetch_next_request().await.unwrap().unwrap(); // locks
        drop(client);

        // Sanity check: opening with the default (safe) mode at the SAME
        // wall-clock time leaves the lock in place — request not fetchable.
        let safe_reopen = FileSystemRequestQueueClient::open_with_clock(
            None,
            None,
            Some("sole".to_string()),
            storage_dir,
            clock.clone(),
            false,
        )
        .await
        .unwrap();
        assert!(
            safe_reopen.fetch_next_request().await.unwrap().is_none(),
            "default safe mode must respect the persisted lock"
        );
        drop(safe_reopen);

        // Now reopen with assume_sole_owner=true on the same clock. The lock
        // is reclaimed during rebuild_index, so the request is immediately
        // fetchable — no clock advancement needed.
        let sole_owner_reopen = FileSystemRequestQueueClient::open_with_clock(
            None,
            None,
            Some("sole".to_string()),
            storage_dir,
            clock.clone(),
            true,
        )
        .await
        .unwrap();
        let fetched = sole_owner_reopen.fetch_next_request().await.unwrap();
        assert!(
            fetched.is_some(),
            "assume_sole_owner=true must reclaim the stale lock on open"
        );
        assert_eq!(fetched.unwrap()["uniqueKey"], "a");
    }

    #[tokio::test]
    async fn test_persisted_order_no_in_file() {
        let temp_dir = TempDir::new().unwrap();
        let client = FileSystemRequestQueueClient::open(None, None, None, temp_dir.path())
            .await
            .unwrap();

        client
            .add_batch_of_requests(vec![req("req1")], false)
            .await
            .unwrap();

        // The persisted file must carry id + orderNo (the on-disk lock format).
        // Read the raw file, not via get_request — the client strips orderNo
        // from requests it returns to callers.
        let stored = read_persisted_request(&client, "req1");
        assert!(stored.get("id").and_then(|v| v.as_str()).is_some());
        assert!(stored.get("orderNo").and_then(|v| v.as_i64()).is_some());

        // get_request, by contrast, must NOT leak the internal orderNo lock
        // field to the caller (id is kept).
        let returned = client.get_request("req1").await.unwrap().unwrap();
        assert!(returned.get("orderNo").is_none());
        assert!(returned.get("id").and_then(|v| v.as_str()).is_some());
    }

    /// Adding a request that already carries a `handledAt` must store it as
    /// handled (not pending) — bumping `handledRequestCount`, leaving
    /// `pendingRequestCount` alone, and persisting `orderNo: null` so the
    /// reader (`rebuild_index`) classifies it consistently. Mirrors the JS
    /// `_calculateOrderNo` behavior (returns `null` when `handledAt` is set).
    #[tokio::test]
    async fn test_add_with_handled_at_counts_as_handled() {
        let temp_dir = TempDir::new().unwrap();
        let client = FileSystemRequestQueueClient::open(None, None, None, temp_dir.path())
            .await
            .unwrap();

        let mut handled_req = req("done");
        if let Value::Object(ref mut map) = handled_req {
            map.insert(
                "handledAt".to_string(),
                Value::String("2024-01-15T10:30:00.123456+00:00".to_string()),
            );
        }

        let response = client
            .add_batch_of_requests(vec![handled_req], false)
            .await
            .unwrap();

        // JS contract: the response itself reports was_already_handled=false
        // even for a fresh handled add. ("That's how API behaves.")
        assert!(!response.processed_requests[0].was_already_handled);
        assert!(!response.processed_requests[0].was_already_present);

        let meta = client.get_metadata().await;
        assert_eq!(
            meta.handled_request_count, 1,
            "adding with handledAt must bump handledRequestCount"
        );
        assert_eq!(
            meta.pending_request_count, 0,
            "adding with handledAt must NOT bump pendingRequestCount"
        );
        assert_eq!(meta.total_request_count, 1);

        // The request must not be fetchable — it is already handled.
        assert!(client.fetch_next_request().await.unwrap().is_none());
        assert!(client.is_empty().await);
        assert!(
            client.is_finished().await,
            "queue with a single handled-on-add request must be finished"
        );

        // The persisted file must carry orderNo: null so re-readers agree.
        // Read the raw file, since get_request strips orderNo on return.
        let stored = read_persisted_request(&client, "done");
        assert!(
            stored.get("orderNo").map(|v| v.is_null()).unwrap_or(false),
            "handled-on-add request must persist with orderNo: null"
        );
    }

    /// Re-opening a queue containing a handled-on-add request must reconstruct
    /// the same counts (handled=1, pending=0). Guards against drift between
    /// the write path (`add_batch_of_requests`) and the read path
    /// (`rebuild_index`), which both interpret `handledAt`.
    #[tokio::test]
    async fn test_add_with_handled_at_survives_reopen() {
        let temp_dir = TempDir::new().unwrap();

        {
            let client = FileSystemRequestQueueClient::open(None, None, None, temp_dir.path())
                .await
                .unwrap();

            let mut handled_req = req("done");
            if let Value::Object(ref mut map) = handled_req {
                map.insert(
                    "handledAt".to_string(),
                    Value::String("2024-01-15T10:30:00.123456+00:00".to_string()),
                );
            }
            client
                .add_batch_of_requests(vec![handled_req, req("pending")], false)
                .await
                .unwrap();

            let meta = client.get_metadata().await;
            assert_eq!(meta.handled_request_count, 1);
            assert_eq!(meta.pending_request_count, 1);
        }

        let client2 = FileSystemRequestQueueClient::open(None, None, None, temp_dir.path())
            .await
            .unwrap();
        let meta = client2.get_metadata().await;
        assert_eq!(
            meta.handled_request_count, 1,
            "reopened queue must classify the handled-on-add request as handled"
        );
        assert_eq!(meta.pending_request_count, 1);
        assert_eq!(meta.total_request_count, 2);

        // Only the pending one is fetchable.
        let fetched = client2.fetch_next_request().await.unwrap().unwrap();
        assert_eq!(fetched["uniqueKey"], "pending");
    }

    /// `forefront=true` combined with an incoming `handledAt` is contradictory.
    /// The `handledAt` intent must win: the request is stored as handled and
    /// must NOT enter the forefront ordering list (which only governs fetch
    /// order, and a handled request is never fetched).
    #[tokio::test]
    async fn test_add_with_handled_at_ignores_forefront_flag() {
        let temp_dir = TempDir::new().unwrap();
        let client = FileSystemRequestQueueClient::open(None, None, None, temp_dir.path())
            .await
            .unwrap();

        let mut handled_req = req("done");
        if let Value::Object(ref mut map) = handled_req {
            map.insert(
                "handledAt".to_string(),
                Value::String("2024-01-15T10:30:00.123456+00:00".to_string()),
            );
        }
        client
            .add_batch_of_requests(vec![handled_req], true)
            .await
            .unwrap();

        let meta = client.get_metadata().await;
        assert_eq!(meta.handled_request_count, 1);
        assert_eq!(meta.pending_request_count, 0);
        assert!(
            meta.forefront_request_ids.is_empty(),
            "a handled-on-add request must not be tracked in forefront ordering"
        );
    }

    /// `handledAt: null` must NOT be interpreted as handled — only a
    /// non-null value flips the bit. (Some serializers emit explicit nulls
    /// for missing fields; we must not regress on that.)
    #[tokio::test]
    async fn test_add_with_null_handled_at_is_still_pending() {
        let temp_dir = TempDir::new().unwrap();
        let client = FileSystemRequestQueueClient::open(None, None, None, temp_dir.path())
            .await
            .unwrap();

        let mut r = req("nullish");
        if let Value::Object(ref mut map) = r {
            map.insert("handledAt".to_string(), Value::Null);
        }
        client.add_batch_of_requests(vec![r], false).await.unwrap();

        let meta = client.get_metadata().await;
        assert_eq!(meta.handled_request_count, 0);
        assert_eq!(meta.pending_request_count, 1);
    }

    /// The write path (`mark_request_as_handled` / `reclaim_request`) must derive
    /// lock/ordering state from disk, never from an `orderNo` the caller hands
    /// back on the request object. A consumer can only ever construct such a
    /// value by accident (the read paths strip `orderNo` — see
    /// `test_fetch_next_request_strips_order_no`), but the bindings accept an
    /// opaque object, so a stale or hostile `orderNo` must be ignored. This pins
    /// that invariant so it can't silently regress.
    #[tokio::test]
    async fn test_write_path_ignores_caller_supplied_order_no() {
        let temp_dir = TempDir::new().unwrap();
        let client = FileSystemRequestQueueClient::open(None, None, None, temp_dir.path())
            .await
            .unwrap();

        // Two requests so we can poison one and still observe the queue.
        client
            .add_batch_of_requests(vec![req("reclaimed"), req("handled")], false)
            .await
            .unwrap();

        // --- reclaim_request ignores a bogus caller orderNo ---
        let mut to_reclaim = client.fetch_next_request().await.unwrap().unwrap();
        // Poison the in-hand object with a far-future lock. If the write path
        // honored this, the request would look locked-until-year-~3.3M and be
        // unfetchable; reclaim must instead reset it to a fetchable `±now`.
        if let Value::Object(ref mut map) = to_reclaim {
            map.insert("orderNo".to_string(), Value::Number(i64::MAX.into()));
        }
        let unique_key = to_reclaim["uniqueKey"].as_str().unwrap().to_string();
        client
            .reclaim_request(to_reclaim, false)
            .await
            .unwrap()
            .expect("reclaim of a known request returns Some");

        // The persisted orderNo must be the library's computed value (≈ now, in
        // the past relative to any future lock), NOT the i64::MAX we supplied.
        let persisted = read_persisted_request(&client, &unique_key);
        let persisted_order = persisted
            .get("orderNo")
            .and_then(|v| v.as_i64())
            .expect("reclaimed request keeps a numeric orderNo on disk");
        assert_ne!(
            persisted_order,
            i64::MAX,
            "reclaim must not persist the caller-supplied orderNo"
        );
        let now = client.clock().now().timestamp_millis();
        assert!(
            !FileSystemRequestQueueClient::is_locked_order(persisted_order, now),
            "a reclaimed request must be immediately fetchable, not locked far in the future"
        );

        // --- mark_request_as_handled ignores a bogus caller orderNo ---
        // Fetch the other request and poison it the same way.
        let mut to_handle = client.fetch_next_request().await.unwrap().unwrap();
        if let Value::Object(ref mut map) = to_handle {
            map.insert("orderNo".to_string(), Value::Number(i64::MAX.into()));
        }
        let handled_key = to_handle["uniqueKey"].as_str().unwrap().to_string();
        client
            .mark_request_as_handled(to_handle)
            .await
            .unwrap()
            .expect("marking a known request handled returns Some");

        // Handled means orderNo == null on disk, regardless of what we passed in.
        let persisted = read_persisted_request(&client, &handled_key);
        assert!(
            persisted
                .get("orderNo")
                .map(|v| v.is_null())
                .unwrap_or(true),
            "a handled request must have a null orderNo on disk, not the caller-supplied value"
        );
    }

    /// `get_request` is a read-only peek: it must NOT lock the request. After a
    /// `get_request`, a subsequent `fetch_next_request` must still return that
    /// same request (it was never reserved). Mirrors the deleted crawlee-js
    /// `get_request does not mark in progress` test.
    #[tokio::test]
    async fn test_get_request_does_not_lock() {
        let temp_dir = TempDir::new().unwrap();
        let client = FileSystemRequestQueueClient::open(None, None, None, temp_dir.path())
            .await
            .unwrap();

        client
            .add_batch_of_requests(vec![req("peek-me")], false)
            .await
            .unwrap();

        // Peek at the request.
        let peeked = client.get_request("peek-me").await.unwrap().unwrap();
        assert_eq!(peeked["uniqueKey"], "peek-me");

        // The queue must still consider it fetchable (peeking did not lock it).
        assert!(
            !client.is_empty().await,
            "get_request must not lock the request — the queue is still non-empty/fetchable"
        );

        // And fetch_next_request must hand back the very same request.
        let fetched = client.fetch_next_request().await.unwrap().unwrap();
        assert_eq!(fetched["uniqueKey"], "peek-me");
    }

    /// `persist_state` is documented as a retained no-op (all queue state lives
    /// in the request files, which are written eagerly on every mutation). This
    /// pins that contract: calling it changes nothing observable and never errors,
    /// so the bindings can keep invoking it without surprises.
    #[tokio::test]
    async fn test_persist_state_is_noop() {
        let temp_dir = TempDir::new().unwrap();
        let client = FileSystemRequestQueueClient::open(None, None, None, temp_dir.path())
            .await
            .unwrap();

        client
            .add_batch_of_requests(vec![req("a"), req("b")], false)
            .await
            .unwrap();
        // Fetch one (locking it) so there's some non-trivial in-progress state.
        let _ = client.fetch_next_request().await.unwrap().unwrap();

        let before = client.get_metadata().await;

        // The no-op must not error and must not mutate counts.
        client.persist_state().await;

        let after = client.get_metadata().await;
        assert_eq!(before.total_request_count, after.total_request_count);
        assert_eq!(before.pending_request_count, after.pending_request_count);
        assert_eq!(before.handled_request_count, after.handled_request_count);
    }

    /// A mutation must update the on-disk `__metadata__.json` immediately (atomic
    /// write), not just the in-memory snapshot returned by `get_metadata`. This
    /// reads the metadata file straight off disk to guard against an atomic-write
    /// regression and to pin the on-disk count shape. Mirrors the deleted
    /// crawlee-js / crawlee-python `metadata file updates` tests.
    #[tokio::test]
    async fn test_metadata_file_updated_on_disk_after_mutation() {
        let temp_dir = TempDir::new().unwrap();
        let client =
            FileSystemRequestQueueClient::open(None, Some("q".to_string()), None, temp_dir.path())
                .await
                .unwrap();

        client
            .add_batch_of_requests(vec![req("one")], false)
            .await
            .unwrap();

        // Read the metadata file directly from disk (bypassing get_metadata).
        let read_disk = || {
            let raw = std::fs::read_to_string(client.metadata_path())
                .expect("metadata file should exist on disk");
            serde_json::from_str::<Value>(&raw).expect("metadata file should be valid JSON")
        };

        let meta = read_disk();
        assert_eq!(meta["totalRequestCount"], 1);
        assert_eq!(meta["pendingRequestCount"], 1);
        assert_eq!(meta["handledRequestCount"], 0);

        // Handle it and confirm the on-disk counts move.
        let request = client.fetch_next_request().await.unwrap().unwrap();
        client.mark_request_as_handled(request).await.unwrap();

        let meta = read_disk();
        assert_eq!(meta["totalRequestCount"], 1);
        assert_eq!(meta["pendingRequestCount"], 0);
        assert_eq!(meta["handledRequestCount"], 1);
    }
}
