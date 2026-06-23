mod models;

use std::path::PathBuf;
use std::sync::Arc;

use crawlee_storage::clock::{ClockRef, TestClock};
use napi::bindgen_prelude::*;
use napi_derive::napi;
use serde_json::Value;
use tokio::sync::Mutex;

use models::{DatasetMetadata, KeyValueStoreMetadata, RequestQueueMetadata};

fn storage_err(e: crawlee_storage::utils::StorageError) -> napi::Error {
    napi::Error::from_reason(e.to_string())
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
            tc.advance(millis);
            Ok(())
        }
        None => Err(napi::Error::from_reason(
            "advanceClockForTesting() requires the client to have been opened \
             with { useTestClock: true }. The default SystemClock cannot be advanced."
                .to_string(),
        )),
    }
}

/// Serialize any serde-compatible struct to a `serde_json::Value`.
///
/// The core library models already serialize with camelCase field names
/// (via `#[serde(rename = "...")]`), so no extra transformation is needed.
fn to_js<T: serde::Serialize>(src: &T) -> napi::Result<Value> {
    serde_json::to_value(src).map_err(|e| napi::Error::from_reason(e.to_string()))
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

    #[napi(ts_return_type = "Promise<DatasetItemsListPage>")]
    pub async fn get_data(
        &self,
        offset: Option<u32>,
        limit: Option<u32>,
        desc: Option<bool>,
        skip_empty: Option<bool>,
    ) -> napi::Result<Value> {
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
        to_js(&page)
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
        Ok(DatasetItemIterator {
            state: Arc::new(Mutex::new(DatasetItemIteratorState {
                client: self.inner.clone(),
                offset: offset.unwrap_or(0) as usize,
                remaining_limit: limit.map(|l| l as usize),
                desc: desc.unwrap_or(false),
                skip_empty: skip_empty.unwrap_or(false),
                page_size: page_size.unwrap_or(1000) as usize,
                buffer: Vec::new(),
                buf_index: 0,
                done: false,
            })),
        })
    }
}

// ─── Dataset Item Iterator ──────────────────────────────────────────────────

struct DatasetItemIteratorState {
    client: Arc<crawlee_storage::dataset::FileSystemDatasetClient>,
    offset: usize,
    remaining_limit: Option<usize>,
    desc: bool,
    skip_empty: bool,
    page_size: usize,
    buffer: Vec<Value>,
    buf_index: usize,
    done: bool,
}

#[napi]
pub struct DatasetItemIterator {
    state: Arc<Mutex<DatasetItemIteratorState>>,
}

#[napi]
impl DatasetItemIterator {
    /// Fetch the next item. Returns null when iteration is exhausted.
    #[napi(ts_return_type = "Promise<Record<string, unknown> | null>")]
    pub async fn next(&self) -> napi::Result<Option<Value>> {
        let mut st = self.state.lock().await;

        // If we still have buffered items, return the next one.
        if st.buf_index < st.buffer.len() {
            let item = st.buffer[st.buf_index].clone();
            st.buf_index += 1;
            return Ok(Some(item));
        }

        // If we've exhausted everything, signal done.
        if st.done {
            return Ok(None);
        }

        // Fetch the next page.
        let page = st
            .client
            .iterate_items_page(
                st.offset,
                st.remaining_limit,
                st.page_size,
                st.desc,
                st.skip_empty,
            )
            .await
            .map_err(storage_err)?;

        let page_len = page.items.len();
        if page_len == 0 {
            st.done = true;
            return Ok(None);
        }

        // Update state for the next page fetch.
        st.offset += page_len;
        if let Some(ref mut rem) = st.remaining_limit {
            *rem = rem.saturating_sub(page_len);
        }
        if !page.has_more {
            st.done = true;
        }

        // Buffer the page and return the first item.
        st.buffer = page.items;
        st.buf_index = 1;
        Ok(Some(st.buffer[0].clone()))
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

    #[napi]
    pub async fn purge(&self) -> napi::Result<()> {
        self.inner.purge().await.map_err(storage_err)
    }

    /// Get a record by key. Returns the raw value bytes as a Buffer.
    #[napi(ts_return_type = "Promise<KeyValueStoreRecord | null>")]
    pub async fn get_value(&self, key: String) -> napi::Result<Option<Value>> {
        let inner = self.inner.clone();
        let result = inner.get_value(&key).await.map_err(storage_err)?;

        match result {
            Some((path, meta)) => {
                let raw_bytes = tokio::fs::read(&path)
                    .await
                    .map_err(|e| napi::Error::from_reason(e.to_string()))?;
                let byte_arr: Vec<Value> = raw_bytes
                    .iter()
                    .map(|b| Value::Number((*b).into()))
                    .collect();

                let mut map = serde_json::Map::new();
                map.insert("key".to_string(), Value::String(key));
                map.insert("contentType".to_string(), Value::String(meta.content_type));
                // The core backfills `size` from the value file for any sidecar
                // that lacks it, so it is always present on read; fall back to
                // the actual byte count we just read just in case.
                map.insert(
                    "size".to_string(),
                    Value::Number(meta.size.unwrap_or(raw_bytes.len()).into()),
                );
                map.insert("value".to_string(), Value::Array(byte_arr));

                Ok(Some(Value::Object(map)))
            }
            None => Ok(None),
        }
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
        let result = inner.get_value(&key).await.map_err(storage_err)?;

        match result {
            Some((path, meta)) => {
                let mut map = serde_json::Map::new();
                map.insert("key".to_string(), Value::String(key));
                map.insert("contentType".to_string(), Value::String(meta.content_type));
                // The core backfills `size` from the value file for any sidecar
                // that lacks it, so it is always present on read.
                map.insert(
                    "size".to_string(),
                    Value::Number(meta.size.unwrap_or(0).into()),
                );
                map.insert(
                    "filePath".to_string(),
                    Value::String(path.to_string_lossy().to_string()),
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

    #[napi]
    pub async fn iterate_keys(
        &self,
        exclusive_start_key: Option<String>,
        limit: Option<u32>,
        page_size: Option<u32>,
        prefix: Option<String>,
    ) -> napi::Result<KvsKeyIterator> {
        Ok(KvsKeyIterator {
            state: Arc::new(Mutex::new(KvsKeyIteratorState {
                client: self.inner.clone(),
                exclusive_start_key,
                remaining_limit: limit.map(|l| l as usize),
                page_size: page_size.unwrap_or(1000) as usize,
                prefix,
                buffer: Vec::new(),
                buf_index: 0,
                done: false,
            })),
        })
    }

    #[napi]
    pub async fn get_public_url(&self, key: String) -> String {
        self.inner.get_public_url(&key).await
    }

    #[napi]
    pub async fn record_exists(&self, key: String) -> bool {
        self.inner.record_exists(&key).await
    }
}

// ─── KVS Key Iterator ──────────────────────────────────────────────────────

struct KvsKeyIteratorState {
    client: Arc<crawlee_storage::key_value_store::FileSystemKeyValueStoreClient>,
    exclusive_start_key: Option<String>,
    remaining_limit: Option<usize>,
    page_size: usize,
    prefix: Option<String>,
    buffer: Vec<crawlee_storage::models::KeyValueStoreRecordMetadata>,
    buf_index: usize,
    done: bool,
}

#[napi]
pub struct KvsKeyIterator {
    state: Arc<Mutex<KvsKeyIteratorState>>,
}

#[napi]
impl KvsKeyIterator {
    /// Fetch the next key metadata entry. Returns null when iteration is exhausted.
    #[napi(ts_return_type = "Promise<KeyValueStoreRecordMetadata | null>")]
    pub async fn next(&self) -> napi::Result<Option<Value>> {
        let mut st = self.state.lock().await;

        // If we still have buffered items, return the next one.
        if st.buf_index < st.buffer.len() {
            let val = to_js(&st.buffer[st.buf_index])?;
            st.buf_index += 1;
            return Ok(Some(val));
        }

        // If we've exhausted everything, signal done.
        if st.done {
            return Ok(None);
        }

        // Fetch the next page.
        let page = st
            .client
            .iterate_keys_page(
                st.exclusive_start_key.as_deref(),
                st.remaining_limit,
                st.page_size,
                st.prefix.as_deref(),
            )
            .await
            .map_err(storage_err)?;

        let page_len = page.items.len();
        if page_len == 0 {
            st.done = true;
            return Ok(None);
        }

        // Update cursor to the last key in this page.
        st.exclusive_start_key = Some(page.items.last().unwrap().key.clone());
        if let Some(ref mut rem) = st.remaining_limit {
            *rem = rem.saturating_sub(page_len);
        }
        if !page.has_more {
            st.done = true;
        }

        // Buffer the page and return the first item.
        st.buffer = page.items;
        st.buf_index = 1;
        Ok(Some(to_js(&st.buffer[0])?))
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
    /// `assumeSoleOwner` (default `true`): controls how locks on disk are
    /// treated at open time. With `true` (the default, tuned for the common
    /// single-process crawl), the caller asserts nothing else is using this
    /// queue and any in-progress locks are reclaimed immediately, so a request
    /// whose previous run died is instantly re-fetchable. Set to `false` when
    /// multiple processes share the same on-disk queue concurrently: any
    /// future-dated `orderNo` is then respected as a potential live peer's
    /// lock, and crashed peers' locks expire naturally on the wall clock —
    /// otherwise you risk two peers processing the same request.
    #[napi(factory)]
    pub async fn open(
        id: Option<String>,
        name: Option<String>,
        alias: Option<String>,
        storage_dir: Option<String>,
        use_test_clock: Option<bool>,
        assume_sole_owner: Option<bool>,
    ) -> napi::Result<Self> {
        let storage_dir = PathBuf::from(storage_dir.unwrap_or_else(|| "./storage".to_string()));
        let (clock, test_clock) = pick_clock(use_test_clock);
        let client = crawlee_storage::request_queue::FileSystemRequestQueueClient::open_with_clock(
            id,
            name,
            alias,
            &storage_dir,
            clock,
            assume_sole_owner.unwrap_or(true),
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
        ts_args_type = "requests: Record<string, unknown>[], forefront?: boolean | undefined | null",
        ts_return_type = "Promise<AddRequestsResponse>"
    )]
    pub async fn add_batch_of_requests(
        &self,
        requests: Vec<Value>,
        forefront: Option<bool>,
    ) -> napi::Result<Value> {
        let response = self
            .inner
            .add_batch_of_requests(requests, forefront.unwrap_or(false))
            .await
            .map_err(storage_err)?;
        to_js(&response)
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

    #[napi(
        ts_args_type = "request: Record<string, unknown>",
        ts_return_type = "Promise<ProcessedRequest | null>"
    )]
    pub async fn mark_request_as_handled(&self, request: Value) -> napi::Result<Option<Value>> {
        let result = self
            .inner
            .mark_request_as_handled(request)
            .await
            .map_err(storage_err)?;
        match result {
            Some(r) => Ok(Some(to_js(&r)?)),
            None => Ok(None),
        }
    }

    #[napi(
        ts_args_type = "request: Record<string, unknown>, forefront?: boolean | undefined | null",
        ts_return_type = "Promise<ProcessedRequest | null>"
    )]
    pub async fn reclaim_request(
        &self,
        request: Value,
        forefront: Option<bool>,
    ) -> napi::Result<Option<Value>> {
        let result = self
            .inner
            .reclaim_request(request, forefront.unwrap_or(false))
            .await
            .map_err(storage_err)?;
        match result {
            Some(r) => Ok(Some(to_js(&r)?)),
            None => Ok(None),
        }
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
        self.inner.set_expected_request_processing_time(secs).await;
    }

    #[napi]
    pub async fn persist_state(&self) {
        self.inner.persist_state().await;
    }
}
