mod models;

use std::path::PathBuf;
use std::sync::Arc;

use crawlee_storage::clock::{ClockRef, TestClock};
use crawlee_storage::pagination::{DatasetItemSource, KvsKeySource, PageCursor};
use napi::bindgen_prelude::*;
use napi_derive::napi;
use serde_json::Value;
use tokio::sync::Mutex;

use models::{
    AddRequestsResponse, DatasetItemsListPage, DatasetMetadata, KeyValueStoreMetadata,
    KeyValueStoreRecord, KeyValueStoreRecordMetadata, ProcessedRequest, RequestQueueMetadata,
};

fn storage_err(e: crawlee_storage::utils::StorageError) -> napi::Error {
    use crawlee_storage::utils::StorageError;
    // Listed explicitly so a future refactor of the variant's Display text
    // doesn't silently change the JS-visible message.
    match e {
        StorageError::ExclusiveStartKeyNotFound(_) => napi::Error::from_reason(e.to_string()),
        other => napi::Error::from_reason(other.to_string()),
    }
}

/// The content-type sentinel for null KVS values (stored on disk as an empty
/// file). Re-exported from the core crate so consumers reference the shared
/// constant instead of hardcoding the `application/x-none` literal.
#[napi]
pub const NONE_CONTENT_TYPE: &str = crawlee_storage::NONE_CONTENT_TYPE;

/// How the on-disk request queue is expected to be accessed. Mirrors the
/// Apify Python SDK's `request_queue_access` option so the naming stays
/// consistent across projects.
///
/// Crosses the FFI as a plain string (`'single'` | `'shared'`); the TS type is
/// pinned via `ts_args_type` on `open`. Map to the core's `assume_sole_owner`
/// mechanism flag.
fn request_queue_access_to_sole_owner(value: Option<String>) -> napi::Result<bool> {
    match value.as_deref().unwrap_or("single") {
        "single" => Ok(true),
        "shared" => Ok(false),
        other => Err(napi::Error::from_reason(format!(
            "requestQueueAccess must be 'single' or 'shared', got '{other}'"
        ))),
    }
}

/// Pick a clock for a client given the `useTestClock` flag passed across the
/// FFI. Returns the abstract `ClockRef` to hand to the core client, plus the
/// concrete `TestClock` we keep on the wrapper so JS can drive it later (or
/// `None` when running with a system clock).
fn pick_clock(use_test_clock: Option<bool>) -> (ClockRef, Option<Arc<TestClock>>) {
    if use_test_clock.unwrap_or(false) {
        let tc = Arc::new(TestClock::new());
        (tc.clone() as ClockRef, Some(tc))
    } else {
        (crawlee_storage::clock::system_clock(), None)
    }
}

/// Shared `advanceClockForTesting` implementation. Throws a descriptive error
/// if the client was opened without `useTestClock: true` — calling it on a
/// system-clock-backed client is almost certainly a bug.
fn advance_test_clock(test_clock: &Option<Arc<TestClock>>, millis: i64) -> napi::Result<()> {
    match test_clock {
        Some(tc) => {
            // JS has no native duration type, so the API stays in millis;
            // convert to `chrono::Duration` here (the core owns the unit).
            tc.advance(chrono::Duration::milliseconds(millis));
            Ok(())
        }
        None => Err(napi::Error::from_reason(
            "advanceClockForTesting() requires the client to have been opened \
             with { useTestClock: true }. The default SystemClock cannot be advanced."
                .to_string(),
        )),
    }
}

// ─── Dataset Client ─────────────────────────────────────────────────────────

#[napi]
pub struct FileSystemDatasetClient {
    inner: Arc<crawlee_storage::dataset::FileSystemDatasetClient>,
    test_clock: Option<Arc<TestClock>>,
}

#[napi]
impl FileSystemDatasetClient {
    #[napi(factory)]
    pub async fn open(
        id: Option<String>,
        name: Option<String>,
        alias: Option<String>,
        storage_dir: Option<String>,
        use_test_clock: Option<bool>,
    ) -> napi::Result<Self> {
        let storage_dir = PathBuf::from(storage_dir.unwrap_or_else(|| "./storage".to_string()));
        let (clock, test_clock) = pick_clock(use_test_clock);
        let client = crawlee_storage::dataset::FileSystemDatasetClient::open_with_clock(
            id,
            name,
            alias,
            &storage_dir,
            clock,
        )
        .await
        .map_err(storage_err)?;
        Ok(Self {
            inner: Arc::new(client),
            test_clock,
        })
    }

    /// Advance the client's clock by `millis` milliseconds. Only usable when
    /// the client was opened with `useTestClock: true`; throws otherwise.
    #[napi]
    pub fn advance_clock_for_testing(&self, millis: i64) -> napi::Result<()> {
        advance_test_clock(&self.test_clock, millis)
    }

    #[napi(getter)]
    pub fn path_to_dataset(&self) -> String {
        self.inner.path().to_string_lossy().to_string()
    }

    #[napi(getter)]
    pub fn path_to_metadata(&self) -> String {
        self.inner.metadata_path().to_string_lossy().to_string()
    }

    #[napi]
    pub async fn get_metadata(&self) -> napi::Result<DatasetMetadata> {
        let meta = self.inner.get_metadata().await;
        Ok(DatasetMetadata::from(&meta))
    }

    #[napi]
    pub async fn drop_storage(&self) -> napi::Result<()> {
        self.inner.drop_storage().await.map_err(storage_err)
    }

    #[napi]
    pub async fn purge(&self) -> napi::Result<()> {
        self.inner.purge().await.map_err(storage_err)
    }

    #[napi(ts_args_type = "data: Record<string, unknown> | Record<string, unknown>[]")]
    pub async fn push_data(&self, data: Value) -> napi::Result<()> {
        self.inner.push_data(data).await.map_err(storage_err)
    }

    #[napi]
    pub async fn get_data(
        &self,
        offset: Option<u32>,
        limit: Option<u32>,
        desc: Option<bool>,
        skip_empty: Option<bool>,
    ) -> napi::Result<DatasetItemsListPage> {
        let page = self
            .inner
            .get_data(
                offset.unwrap_or(0) as usize,
                limit.map(|l| l as usize),
                desc.unwrap_or(false),
                skip_empty.unwrap_or(false),
            )
            .await
            .map_err(storage_err)?;
        Ok(DatasetItemsListPage::from(page))
    }

    #[napi]
    pub async fn iterate_items(
        &self,
        offset: Option<u32>,
        limit: Option<u32>,
        desc: Option<bool>,
        skip_empty: Option<bool>,
        page_size: Option<u32>,
    ) -> napi::Result<DatasetItemIterator> {
        let source = DatasetItemSource::new(
            self.inner.clone(),
            offset.unwrap_or(0) as usize,
            page_size.unwrap_or(1000) as usize,
            desc.unwrap_or(false),
            skip_empty.unwrap_or(false),
        );
        Ok(DatasetItemIterator {
            cursor: Arc::new(Mutex::new(PageCursor::new(
                source,
                limit.map(|l| l as usize),
            ))),
        })
    }
}

// ─── Dataset Item Iterator ──────────────────────────────────────────────────

#[napi]
pub struct DatasetItemIterator {
    // The shared core cursor owns the page-buffering state machine; this
    // wrapper only translates exhaustion into `null` for JS.
    cursor: Arc<Mutex<PageCursor<DatasetItemSource>>>,
}

#[napi]
impl DatasetItemIterator {
    /// Fetch the next item. Returns null when iteration is exhausted.
    #[napi(ts_return_type = "Promise<Record<string, unknown> | null>")]
    pub async fn next(&self) -> napi::Result<Option<Value>> {
        self.cursor.lock().await.next().await.map_err(storage_err)
    }
}

// ─── Key-Value Store Client ─────────────────────────────────────────────────

#[napi]
pub struct FileSystemKeyValueStoreClient {
    inner: Arc<crawlee_storage::key_value_store::FileSystemKeyValueStoreClient>,
    test_clock: Option<Arc<TestClock>>,
}

#[napi]
impl FileSystemKeyValueStoreClient {
    #[napi(factory)]
    pub async fn open(
        id: Option<String>,
        name: Option<String>,
        alias: Option<String>,
        storage_dir: Option<String>,
        use_test_clock: Option<bool>,
    ) -> napi::Result<Self> {
        let storage_dir = PathBuf::from(storage_dir.unwrap_or_else(|| "./storage".to_string()));
        let (clock, test_clock) = pick_clock(use_test_clock);
        let client =
            crawlee_storage::key_value_store::FileSystemKeyValueStoreClient::open_with_clock(
                id,
                name,
                alias,
                &storage_dir,
                clock,
            )
            .await
            .map_err(storage_err)?;
        Ok(Self {
            inner: Arc::new(client),
            test_clock,
        })
    }

    /// Advance the client's clock by `millis` milliseconds. Only usable when
    /// the client was opened with `useTestClock: true`; throws otherwise.
    #[napi]
    pub fn advance_clock_for_testing(&self, millis: i64) -> napi::Result<()> {
        advance_test_clock(&self.test_clock, millis)
    }

    #[napi(getter)]
    pub fn path_to_kvs(&self) -> String {
        self.inner.path().to_string_lossy().to_string()
    }

    #[napi(getter)]
    pub fn path_to_metadata(&self) -> String {
        self.inner.metadata_path().to_string_lossy().to_string()
    }

    #[napi]
    pub async fn get_metadata(&self) -> napi::Result<KeyValueStoreMetadata> {
        let meta = self.inner.get_metadata().await;
        Ok(KeyValueStoreMetadata::from(&meta))
    }

    #[napi]
    pub async fn drop_storage(&self) -> napi::Result<()> {
        self.inner.drop_storage().await.map_err(storage_err)
    }

    /// Delete all records except those whose keys are listed in `keep`.
    ///
    /// Matching is by exact key (no extension globbing): to spare both `INPUT`
    /// and `INPUT.json`, pass both. The store metadata is always kept.
    #[napi]
    pub async fn purge(&self, keep: Option<Vec<String>>) -> napi::Result<()> {
        self.inner
            .purge(&keep.unwrap_or_default())
            .await
            .map_err(storage_err)
    }

    /// Get a tracked record (value file + metadata sidecar) by key. Returns the
    /// raw value bytes as a Buffer, or `null` if there is no such tracked record.
    ///
    /// To read out-of-band files that have no metadata sidecar (e.g. a
    /// CLI-written `INPUT.json`), use `resolveValue`, which probes the
    /// conventional bare-file extensions.
    #[napi]
    pub async fn get_value(&self, key: String) -> napi::Result<Option<KeyValueStoreRecord>> {
        let inner = self.inner.clone();
        let result = inner.read_value(&key).await.map_err(storage_err)?;
        Ok(result.map(KeyValueStoreRecord::from))
    }

    /// Resolve a key to a record, transparently falling back to out-of-band
    /// ("bare") value files that have no metadata sidecar.
    ///
    /// Tries the tracked record for the literal `key` first (its content type
    /// comes verbatim from the sidecar), then probes each `bareFallbacks` entry
    /// as a bare `key + extension` file, reporting the declared content type on
    /// a match. The first match wins; the returned record is always keyed by
    /// the requested `key`. Returns `null` if nothing resolves.
    ///
    /// Use this for run-input lookup (`INPUT`, `INPUT.json`, `INPUT.bin`, ...)
    /// instead of hand-rolling the extension probing in JS.
    #[napi]
    pub async fn resolve_value(
        &self,
        key: String,
        bare_fallbacks: Vec<models::BareFallback>,
    ) -> napi::Result<Option<KeyValueStoreRecord>> {
        let fallbacks: Vec<(&str, &str)> = bare_fallbacks
            .iter()
            .map(|f| (f.extension.as_str(), f.content_type.as_str()))
            .collect();
        let inner = self.inner.clone();
        let result = inner
            .resolve_and_read_value(&key, &fallbacks)
            .await
            .map_err(storage_err)?;
        Ok(result.map(KeyValueStoreRecord::from))
    }

    /// Resolve a key to the on-disk key that actually exists, using the same
    /// fallback probe order as `resolveValue` but without reading the value.
    /// Returns the matched key (the literal key or `key + extension`), or
    /// `null` if nothing exists. Pass the result to `getPublicUrl` so the URL
    /// points at the file that exists.
    #[napi]
    pub async fn resolve_existing_key(
        &self,
        key: String,
        bare_fallbacks: Vec<String>,
    ) -> napi::Result<Option<String>> {
        let fallbacks: Vec<&str> = bare_fallbacks.iter().map(String::as_str).collect();
        let inner = self.inner.clone();
        Ok(inner.resolve_existing_key(&key, &fallbacks).await)
    }

    /// Set a value from a Buffer.
    #[napi]
    pub async fn set_value(
        &self,
        key: String,
        value: Buffer,
        content_type: Option<String>,
    ) -> napi::Result<()> {
        let ct = content_type.unwrap_or_else(|| "application/octet-stream".to_string());
        let inner = self.inner.clone();
        inner.set_value(&key, &value, ct).await.map_err(storage_err)
    }

    /// Internal: get file info for a record (path + metadata), used by the JS
    /// wrapper to create a ReadableStream without buffering the entire file.
    #[napi(js_name = "_getValueFileInfo", skip_typescript)]
    pub async fn get_value_file_info(&self, key: String) -> napi::Result<Option<Value>> {
        let inner = self.inner.clone();
        let result = inner.value_file_info(&key).await.map_err(storage_err)?;

        match result {
            Some(info) => {
                let mut map = serde_json::Map::new();
                map.insert("key".to_string(), Value::String(info.key));
                map.insert("contentType".to_string(), Value::String(info.content_type));
                // The core finalizes `size` to a non-optional value (stating the
                // file when the sidecar omits it), shared with the read path.
                map.insert("size".to_string(), Value::Number(info.size.into()));
                map.insert(
                    "filePath".to_string(),
                    Value::String(info.path.to_string_lossy().to_string()),
                );
                Ok(Some(Value::Object(map)))
            }
            None => Ok(None),
        }
    }

    /// Internal: get a temp file path in the store directory for streaming writes.
    #[napi(js_name = "_getTempFilePath", skip_typescript)]
    pub fn get_temp_file_path(&self) -> String {
        self.inner.temp_file_path().to_string_lossy().to_string()
    }

    /// Internal: finalize a streamed write by renaming temp file → value file
    /// and writing sidecar + store metadata.
    #[napi(js_name = "_finalizeStreamedValue", skip_typescript)]
    pub async fn finalize_streamed_value(
        &self,
        key: String,
        temp_path: String,
        size: u32,
        content_type: String,
    ) -> napi::Result<()> {
        let inner = self.inner.clone();
        inner
            .finalize_streamed_value(
                &key,
                std::path::Path::new(&temp_path),
                size as usize,
                content_type,
            )
            .await
            .map_err(storage_err)
    }

    #[napi]
    pub async fn delete_value(&self, key: String) -> napi::Result<()> {
        self.inner.delete_value(&key).await.map_err(storage_err)
    }

    /// Lazily iterate the store's keys.
    ///
    /// `bareFallbacks` additionally surfaces out-of-band ("bare") value files
    /// that have no metadata sidecar (e.g. a CLI-written `INPUT.json`) as regular
    /// keys. Each entry is `{ name, contentType }` where `name` is the file's
    /// on-disk key: if that file exists with no tracked record, it is listed
    /// under `name`. Pass an empty array (the default) to list only tracked
    /// records.
    ///
    /// Round-trip caveat: a surfaced bare key does NOT round-trip through the
    /// strict read path. The listed key is the literal on-disk `name`, but
    /// `getValue` / `recordExists` only see tracked records (value + sidecar) and
    /// return `null` / `false` for a sidecar-less bare file. Read a listed bare
    /// key back via `resolveValue` / `resolveExistingKey`, not `getValue`.
    #[napi]
    pub async fn iterate_keys(
        &self,
        exclusive_start_key: Option<String>,
        limit: Option<u32>,
        page_size: Option<u32>,
        prefix: Option<String>,
        bare_fallbacks: Option<Vec<models::ListBareFallback>>,
    ) -> napi::Result<KvsKeyIterator> {
        let bare_fallbacks = bare_fallbacks
            .unwrap_or_default()
            .into_iter()
            .map(|f| (f.name, f.content_type))
            .collect();
        let source = KvsKeySource::new(
            self.inner.clone(),
            exclusive_start_key,
            page_size.unwrap_or(1000) as usize,
            prefix,
            bare_fallbacks,
        );
        Ok(KvsKeyIterator {
            cursor: Arc::new(Mutex::new(PageCursor::new(
                source,
                limit.map(|l| l as usize),
            ))),
        })
    }

    /// Build a `file://` URL for `key`, or `null` if no value file exists for
    /// it. Stats the encoded path; does not probe bare-file extensions, so the
    /// caller resolves the on-disk key via `resolveExistingKey` first if needed.
    #[napi]
    pub async fn get_public_url(&self, key: String) -> Option<String> {
        self.inner.get_public_url(&key).await
    }

    /// Check whether a tracked record (value file + metadata sidecar) exists for
    /// `key`. To also match out-of-band files with no sidecar, use
    /// `resolveExistingKey`, which probes the conventional bare-file extensions.
    #[napi]
    pub async fn record_exists(&self, key: String) -> bool {
        self.inner.record_exists(&key, true).await
    }
}

// ─── KVS Key Iterator ──────────────────────────────────────────────────────

#[napi]
pub struct KvsKeyIterator {
    cursor: Arc<Mutex<PageCursor<KvsKeySource>>>,
}

#[napi]
impl KvsKeyIterator {
    /// Fetch the next key metadata entry. Returns null when iteration is exhausted.
    #[napi]
    pub async fn next(&self) -> napi::Result<Option<KeyValueStoreRecordMetadata>> {
        match self.cursor.lock().await.next().await.map_err(storage_err)? {
            Some(meta) => Ok(Some(KeyValueStoreRecordMetadata::from(meta))),
            None => Ok(None),
        }
    }
}

// ─── Request Queue Client ───────────────────────────────────────────────────
#[napi]
pub struct FileSystemRequestQueueClient {
    inner: Arc<crawlee_storage::request_queue::FileSystemRequestQueueClient>,
    test_clock: Option<Arc<TestClock>>,
}

#[napi]
impl FileSystemRequestQueueClient {
    /// Open a request queue.
    ///
    /// `useTestClock`: see `advanceClockForTesting` below.
    ///
    /// `requestQueueAccess` (default `'single'`): how the on-disk queue is
    /// expected to be accessed. With `'single'` (the default, tuned for the
    /// common single-process crawl), the caller asserts nothing else is using
    /// this queue and any in-progress locks are reclaimed immediately, so a
    /// request whose previous run died is instantly re-fetchable. Use
    /// `'shared'` when multiple processes share the same on-disk queue
    /// concurrently: any future-dated `orderNo` is then respected as a
    /// potential live peer's lock, and crashed peers' locks expire naturally
    /// on the wall clock — otherwise you risk two peers processing the same
    /// request.
    #[napi(
        factory,
        ts_args_type = "id?: string | undefined | null, name?: string | undefined | null, alias?: string | undefined | null, storageDir?: string | undefined | null, useTestClock?: boolean | undefined | null, requestQueueAccess?: 'single' | 'shared' | undefined | null"
    )]
    pub async fn open(
        id: Option<String>,
        name: Option<String>,
        alias: Option<String>,
        storage_dir: Option<String>,
        use_test_clock: Option<bool>,
        request_queue_access: Option<String>,
    ) -> napi::Result<Self> {
        let storage_dir = PathBuf::from(storage_dir.unwrap_or_else(|| "./storage".to_string()));
        let (clock, test_clock) = pick_clock(use_test_clock);
        let assume_sole_owner = request_queue_access_to_sole_owner(request_queue_access)?;
        let client = crawlee_storage::request_queue::FileSystemRequestQueueClient::open_with_clock(
            id,
            name,
            alias,
            &storage_dir,
            clock,
            assume_sole_owner,
        )
        .await
        .map_err(storage_err)?;
        Ok(Self {
            inner: Arc::new(client),
            test_clock,
        })
    }

    /// Advance the client's clock by `millis` milliseconds. Only usable when
    /// the client was opened with `useTestClock: true`; throws otherwise.
    ///
    /// This is the hook that lets JS tests using `vi.useFakeTimers()` exercise
    /// lock-expiry behavior — fake JS timers don't reach into native code, so
    /// the test must drive the Rust-side clock explicitly via this method.
    #[napi]
    pub fn advance_clock_for_testing(&self, millis: i64) -> napi::Result<()> {
        advance_test_clock(&self.test_clock, millis)
    }

    #[napi(getter)]
    pub fn path_to_rq(&self) -> String {
        self.inner.path().to_string_lossy().to_string()
    }

    #[napi(getter)]
    pub fn path_to_metadata(&self) -> String {
        self.inner.metadata_path().to_string_lossy().to_string()
    }

    #[napi]
    pub async fn get_metadata(&self) -> napi::Result<RequestQueueMetadata> {
        let meta = self.inner.get_metadata().await;
        Ok(RequestQueueMetadata::from(&meta))
    }

    #[napi]
    pub async fn drop_storage(&self) -> napi::Result<()> {
        self.inner.drop_storage().await.map_err(storage_err)
    }

    #[napi]
    pub async fn purge(&self) -> napi::Result<()> {
        self.inner.purge().await.map_err(storage_err)
    }

    #[napi(
        ts_args_type = "requests: Record<string, unknown>[], forefront?: boolean | undefined | null"
    )]
    pub async fn add_batch_of_requests(
        &self,
        requests: Vec<Value>,
        forefront: Option<bool>,
    ) -> napi::Result<AddRequestsResponse> {
        let response = self
            .inner
            .add_batch_of_requests(requests, forefront.unwrap_or(false))
            .await
            .map_err(storage_err)?;
        Ok(AddRequestsResponse::from(response))
    }

    #[napi(ts_return_type = "Promise<Record<string, unknown> | null>")]
    pub async fn get_request(&self, unique_key: String) -> napi::Result<Option<Value>> {
        self.inner
            .get_request(&unique_key)
            .await
            .map_err(storage_err)
    }

    #[napi(ts_return_type = "Promise<Record<string, unknown> | null>")]
    pub async fn fetch_next_request(&self) -> napi::Result<Option<Value>> {
        self.inner.fetch_next_request().await.map_err(storage_err)
    }

    #[napi(ts_args_type = "request: Record<string, unknown>")]
    pub async fn mark_request_as_handled(
        &self,
        request: Value,
    ) -> napi::Result<Option<ProcessedRequest>> {
        let result = self
            .inner
            .mark_request_as_handled(request)
            .await
            .map_err(storage_err)?;
        Ok(result.map(ProcessedRequest::from))
    }

    #[napi(
        ts_args_type = "request: Record<string, unknown>, forefront?: boolean | undefined | null"
    )]
    pub async fn reclaim_request(
        &self,
        request: Value,
        forefront: Option<bool>,
    ) -> napi::Result<Option<ProcessedRequest>> {
        let result = self
            .inner
            .reclaim_request(request, forefront.unwrap_or(false))
            .await
            .map_err(storage_err)?;
        Ok(result.map(ProcessedRequest::from))
    }

    #[napi]
    pub async fn is_empty(&self) -> bool {
        self.inner.is_empty().await
    }

    #[napi]
    pub async fn is_finished(&self) -> bool {
        self.inner.is_finished().await
    }

    #[napi]
    pub async fn set_expected_request_processing_time(&self, secs: f64) {
        // JS has no native duration type, so the API stays in seconds; convert
        // to `chrono::Duration` at the boundary (the core owns the unit). Use
        // millisecond resolution to match the core's internal `lock_millis`.
        let duration = chrono::Duration::milliseconds((secs * 1000.0) as i64);
        self.inner
            .set_expected_request_processing_time(duration)
            .await;
    }

    #[napi]
    pub async fn persist_state(&self) {
        self.inner.persist_state().await;
    }
}
