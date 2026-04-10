use std::path::PathBuf;
use std::sync::Arc;

use napi::bindgen_prelude::*;
use napi_derive::napi;
use serde::Serialize;
use serde_json::Value;
use tokio::sync::Mutex;

fn storage_err(e: crawlee_storage::utils::StorageError) -> napi::Error {
    napi::Error::from_reason(e.to_string())
}

// ─── camelCase serialization wrappers ───────────────────────────────────────
//
// The core library serializes field names in snake_case (for Python filesystem
// compatibility).  The Node.js convention is camelCase.  Rather than doing a
// recursive key-rename pass over serde_json::Value (which would mangle user
// data), we define thin wrapper structs that re-serialize with camelCase field
// names.  User-supplied payloads (dataset items, request objects) are
// `serde_json::Value` and pass through untouched.

/// Serialize a core-library struct via its camelCase wrapper.
fn to_js_value<'a, T, W>(src: &'a T) -> napi::Result<Value>
where
    W: Serialize + From<&'a T>,
{
    let wrapper = W::from(src);
    serde_json::to_value(wrapper).map_err(|e| napi::Error::from_reason(e.to_string()))
}

// -- Storage metadata (base fields, flattened into each concrete type) --------

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct JsStorageMetadata<'a> {
    id: &'a str,
    name: &'a Option<String>,
    accessed_at: String,
    created_at: String,
    modified_at: String,
}

fn fmt_datetime(dt: &chrono::DateTime<chrono::Utc>) -> String {
    format!("{}+00:00", dt.format("%Y-%m-%dT%H:%M:%S%.6f"))
}

impl<'a> From<&'a crawlee_storage::models::StorageMetadata> for JsStorageMetadata<'a> {
    fn from(m: &'a crawlee_storage::models::StorageMetadata) -> Self {
        Self {
            id: &m.id,
            name: &m.name,
            accessed_at: fmt_datetime(&m.accessed_at),
            created_at: fmt_datetime(&m.created_at),
            modified_at: fmt_datetime(&m.modified_at),
        }
    }
}

// -- Dataset metadata ---------------------------------------------------------

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct JsDatasetMetadata<'a> {
    #[serde(flatten)]
    base: JsStorageMetadata<'a>,
    item_count: usize,
}

impl<'a> From<&'a crawlee_storage::models::DatasetMetadata> for JsDatasetMetadata<'a> {
    fn from(m: &'a crawlee_storage::models::DatasetMetadata) -> Self {
        Self {
            base: JsStorageMetadata::from(&m.base),
            item_count: m.item_count,
        }
    }
}

// -- Dataset items list page --------------------------------------------------

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct JsDatasetItemsListPage<'a> {
    count: usize,
    offset: usize,
    limit: usize,
    total: usize,
    desc: bool,
    /// User data — passed through as-is, no key renaming.
    items: &'a Vec<Value>,
}

impl<'a> From<&'a crawlee_storage::models::DatasetItemsListPage> for JsDatasetItemsListPage<'a> {
    fn from(p: &'a crawlee_storage::models::DatasetItemsListPage) -> Self {
        Self {
            count: p.count,
            offset: p.offset,
            limit: p.limit,
            total: p.total,
            desc: p.desc,
            items: &p.items,
        }
    }
}

// -- Key-value store metadata -------------------------------------------------

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct JsKeyValueStoreMetadata<'a> {
    #[serde(flatten)]
    base: JsStorageMetadata<'a>,
}

impl<'a> From<&'a crawlee_storage::models::KeyValueStoreMetadata> for JsKeyValueStoreMetadata<'a> {
    fn from(m: &'a crawlee_storage::models::KeyValueStoreMetadata) -> Self {
        Self {
            base: JsStorageMetadata::from(&m.base),
        }
    }
}

// -- Key-value store record metadata ------------------------------------------

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct JsKeyValueStoreRecordMetadata<'a> {
    key: &'a str,
    content_type: &'a str,
    size: Option<usize>,
}

impl<'a> From<&'a crawlee_storage::models::KeyValueStoreRecordMetadata>
    for JsKeyValueStoreRecordMetadata<'a>
{
    fn from(m: &'a crawlee_storage::models::KeyValueStoreRecordMetadata) -> Self {
        Self {
            key: &m.key,
            content_type: &m.content_type,
            size: m.size,
        }
    }
}

// -- Request queue metadata ---------------------------------------------------

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct JsRequestQueueMetadata<'a> {
    #[serde(flatten)]
    base: JsStorageMetadata<'a>,
    had_multiple_clients: bool,
    handled_request_count: usize,
    pending_request_count: usize,
    total_request_count: usize,
}

impl<'a> From<&'a crawlee_storage::models::RequestQueueMetadata> for JsRequestQueueMetadata<'a> {
    fn from(m: &'a crawlee_storage::models::RequestQueueMetadata) -> Self {
        Self {
            base: JsStorageMetadata::from(&m.base),
            had_multiple_clients: m.had_multiple_clients,
            handled_request_count: m.handled_request_count,
            pending_request_count: m.pending_request_count,
            total_request_count: m.total_request_count,
        }
    }
}

// -- Processed request --------------------------------------------------------

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct JsProcessedRequest<'a> {
    id: &'a Option<String>,
    unique_key: &'a str,
    was_already_present: bool,
    was_already_handled: bool,
}

impl<'a> From<&'a crawlee_storage::models::ProcessedRequest> for JsProcessedRequest<'a> {
    fn from(r: &'a crawlee_storage::models::ProcessedRequest) -> Self {
        Self {
            id: &r.id,
            unique_key: &r.unique_key,
            was_already_present: r.was_already_present,
            was_already_handled: r.was_already_handled,
        }
    }
}

// -- Unprocessed request ------------------------------------------------------

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct JsUnprocessedRequest<'a> {
    unique_key: &'a str,
    url: &'a str,
    method: &'a Option<String>,
}

impl<'a> From<&'a crawlee_storage::models::UnprocessedRequest> for JsUnprocessedRequest<'a> {
    fn from(r: &'a crawlee_storage::models::UnprocessedRequest) -> Self {
        Self {
            unique_key: &r.unique_key,
            url: &r.url,
            method: &r.method,
        }
    }
}

// -- Add requests response ----------------------------------------------------

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct JsAddRequestsResponse<'a> {
    processed_requests: Vec<JsProcessedRequest<'a>>,
    unprocessed_requests: Vec<JsUnprocessedRequest<'a>>,
}

impl<'a> From<&'a crawlee_storage::models::AddRequestsResponse> for JsAddRequestsResponse<'a> {
    fn from(r: &'a crawlee_storage::models::AddRequestsResponse) -> Self {
        Self {
            processed_requests: r.processed_requests.iter().map(Into::into).collect(),
            unprocessed_requests: r.unprocessed_requests.iter().map(Into::into).collect(),
        }
    }
}

// ─── KVS record helper (unchanged — hand-built, no user-data issue) ─────────

/// Convert a KVS record to a plain JS object with camelCase keys.
fn record_to_value(record: &crawlee_storage::models::KeyValueStoreRecord) -> napi::Result<Value> {
    use crawlee_storage::models::KvsValue;

    let mut map = serde_json::Map::new();
    map.insert("key".to_string(), Value::String(record.key.clone()));
    map.insert(
        "contentType".to_string(),
        Value::String(record.content_type.clone()),
    );
    map.insert(
        "size".to_string(),
        record
            .size
            .map(|s| Value::Number(s.into()))
            .unwrap_or(Value::Null),
    );

    match &record.value {
        KvsValue::None => {
            map.insert("value".to_string(), Value::Null);
        }
        KvsValue::Json(v) => {
            map.insert("value".to_string(), v.clone());
        }
        KvsValue::Text(s) => {
            map.insert("value".to_string(), Value::String(s.clone()));
        }
        KvsValue::Binary(bytes) => {
            let arr: Vec<Value> = bytes.iter().map(|b| Value::Number((*b).into())).collect();
            map.insert("value".to_string(), Value::Array(arr));
            map.insert("__binary__".to_string(), Value::Bool(true));
        }
    }

    Ok(Value::Object(map))
}

// ─── Dataset Client ─────────────────────────────────────────────────────────

#[napi]
pub struct FileSystemDatasetClient {
    inner: Arc<crawlee_storage::dataset::FileSystemDatasetClient>,
}

#[napi]
impl FileSystemDatasetClient {
    #[napi(factory)]
    pub async fn open(
        id: Option<String>,
        name: Option<String>,
        alias: Option<String>,
        storage_dir: Option<String>,
    ) -> napi::Result<Self> {
        let storage_dir = PathBuf::from(storage_dir.unwrap_or_else(|| "./storage".to_string()));
        let client =
            crawlee_storage::dataset::FileSystemDatasetClient::open(id, name, alias, &storage_dir)
                .await
                .map_err(storage_err)?;
        Ok(Self {
            inner: Arc::new(client),
        })
    }

    #[napi(getter)]
    pub fn path_to_dataset(&self) -> String {
        self.inner.path().to_string_lossy().to_string()
    }

    #[napi(getter)]
    pub fn path_to_metadata(&self) -> String {
        self.inner.metadata_path().to_string_lossy().to_string()
    }

    #[napi(ts_return_type = "Promise<DatasetMetadata>")]
    pub async fn get_metadata(&self) -> napi::Result<Value> {
        let meta = self.inner.get_metadata().await;
        to_js_value::<_, JsDatasetMetadata>(&meta)
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
                limit.unwrap_or(999_999_999) as usize,
                desc.unwrap_or(false),
                skip_empty.unwrap_or(false),
            )
            .await
            .map_err(storage_err)?;
        to_js_value::<_, JsDatasetItemsListPage>(&page)
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
}

#[napi]
impl FileSystemKeyValueStoreClient {
    #[napi(factory)]
    pub async fn open(
        id: Option<String>,
        name: Option<String>,
        alias: Option<String>,
        storage_dir: Option<String>,
    ) -> napi::Result<Self> {
        let storage_dir = PathBuf::from(storage_dir.unwrap_or_else(|| "./storage".to_string()));
        let client = crawlee_storage::key_value_store::FileSystemKeyValueStoreClient::open(
            id,
            name,
            alias,
            &storage_dir,
        )
        .await
        .map_err(storage_err)?;
        Ok(Self {
            inner: Arc::new(client),
        })
    }

    #[napi(getter)]
    pub fn path_to_kvs(&self) -> String {
        self.inner.path().to_string_lossy().to_string()
    }

    #[napi(getter)]
    pub fn path_to_metadata(&self) -> String {
        self.inner.metadata_path().to_string_lossy().to_string()
    }

    #[napi(ts_return_type = "Promise<KeyValueStoreMetadata>")]
    pub async fn get_metadata(&self) -> napi::Result<Value> {
        let meta = self.inner.get_metadata().await;
        to_js_value::<_, JsKeyValueStoreMetadata>(&meta)
    }

    #[napi]
    pub async fn drop_storage(&self) -> napi::Result<()> {
        self.inner.drop_storage().await.map_err(storage_err)
    }

    #[napi]
    pub async fn purge(&self) -> napi::Result<()> {
        self.inner.purge().await.map_err(storage_err)
    }

    #[napi(ts_return_type = "Promise<KeyValueStoreRecord | null>")]
    pub async fn get_value(&self, key: String) -> napi::Result<Option<Value>> {
        let record = self.inner.get_value(&key).await.map_err(storage_err)?;
        match record {
            Some(r) => Ok(Some(record_to_value(&r)?)),
            None => Ok(None),
        }
    }

    #[napi(ts_args_type = "key: string, value: unknown, contentType?: string | undefined | null")]
    pub async fn set_value(
        &self,
        key: String,
        value: Value,
        content_type: Option<String>,
    ) -> napi::Result<()> {
        use crawlee_storage::models::KvsValue;

        let kvs_value = if value.is_null() {
            KvsValue::None
        } else if let Some(s) = value.as_str() {
            // String values → Text
            KvsValue::Text(s.to_string())
        } else {
            // Everything else → JSON
            KvsValue::Json(value)
        };

        let content_type = Some(content_type.unwrap_or_else(|| {
            match &kvs_value {
                KvsValue::None => "application/x-none",
                KvsValue::Json(_) => "application/json",
                KvsValue::Text(_) => "text/plain",
                KvsValue::Binary(_) => "application/octet-stream",
            }
            .to_string()
        }));

        self.inner
            .set_value(&key, kvs_value, content_type)
            .await
            .map_err(storage_err)
    }

    /// Set a binary value (Buffer) for a key.
    #[napi]
    pub async fn set_value_buffer(
        &self,
        key: String,
        value: Buffer,
        content_type: Option<String>,
    ) -> napi::Result<()> {
        use crawlee_storage::models::KvsValue;
        let ct = Some(content_type.unwrap_or_else(|| "application/octet-stream".to_string()));
        self.inner
            .set_value(&key, KvsValue::Binary(value.to_vec()), ct)
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
    ) -> napi::Result<KvsKeyIterator> {
        Ok(KvsKeyIterator {
            state: Arc::new(Mutex::new(KvsKeyIteratorState {
                client: self.inner.clone(),
                exclusive_start_key,
                remaining_limit: limit.map(|l| l as usize),
                page_size: page_size.unwrap_or(1000) as usize,
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
            let val = to_js_value::<_, JsKeyValueStoreRecordMetadata>(&st.buffer[st.buf_index])?;
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
        Ok(Some(to_js_value::<_, JsKeyValueStoreRecordMetadata>(
            &st.buffer[0],
        )?))
    }
}

// ─── Request Queue Client ───────────────────────────────────────────────────

#[napi]
pub struct FileSystemRequestQueueClient {
    inner: Arc<crawlee_storage::request_queue::FileSystemRequestQueueClient>,
}

#[napi]
impl FileSystemRequestQueueClient {
    #[napi(factory)]
    pub async fn open(
        id: Option<String>,
        name: Option<String>,
        alias: Option<String>,
        storage_dir: Option<String>,
    ) -> napi::Result<Self> {
        let storage_dir = PathBuf::from(storage_dir.unwrap_or_else(|| "./storage".to_string()));
        let client = crawlee_storage::request_queue::FileSystemRequestQueueClient::open(
            id,
            name,
            alias,
            &storage_dir,
        )
        .await
        .map_err(storage_err)?;
        Ok(Self {
            inner: Arc::new(client),
        })
    }

    #[napi(getter)]
    pub fn path_to_rq(&self) -> String {
        self.inner.path().to_string_lossy().to_string()
    }

    #[napi(getter)]
    pub fn path_to_metadata(&self) -> String {
        self.inner.metadata_path().to_string_lossy().to_string()
    }

    #[napi(ts_return_type = "Promise<RequestQueueMetadata>")]
    pub async fn get_metadata(&self) -> napi::Result<Value> {
        let meta = self.inner.get_metadata().await;
        to_js_value::<_, JsRequestQueueMetadata>(&meta)
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
        to_js_value::<_, JsAddRequestsResponse>(&response)
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
            Some(r) => Ok(Some(to_js_value::<_, JsProcessedRequest>(&r)?)),
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
            Some(r) => Ok(Some(to_js_value::<_, JsProcessedRequest>(&r)?)),
            None => Ok(None),
        }
    }

    #[napi]
    pub async fn is_empty(&self) -> bool {
        self.inner.is_empty().await
    }

    #[napi]
    pub async fn persist_state(&self) {
        self.inner.persist_state().await;
    }
}
