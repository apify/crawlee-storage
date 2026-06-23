use std::path::PathBuf;
use std::sync::Arc;

use chrono::Duration;
use crawlee_storage::clock::{ClockRef, TestClock};
use crawlee_storage::models;
use pyo3::exceptions::{
    PyFileNotFoundError, PyOSError, PyRuntimeError, PyStopAsyncIteration, PyValueError,
};
use pyo3::prelude::*;
use pyo3::types::PyDict;
use pyo3_stub_gen::define_stub_info_gatherer;
use pyo3_stub_gen::derive::{gen_stub_pyclass, gen_stub_pymethods};
use serde_json::Value;
use tokio::sync::Mutex;

/// Pick a clock for a client given the `use_test_clock` flag passed across the
/// FFI. Returns the abstract `ClockRef` to hand to the core client, plus the
/// concrete `TestClock` we keep on the wrapper so Python can drive it later
/// (or `None` when running with a system clock).
fn pick_clock(use_test_clock: bool) -> (ClockRef, Option<Arc<TestClock>>) {
    if use_test_clock {
        let tc = Arc::new(TestClock::new());
        (tc.clone() as ClockRef, Some(tc))
    } else {
        (crawlee_storage::clock::system_clock(), None)
    }
}

/// Shared `advance_clock_for_testing` implementation. Raises `ValueError` if
/// the client was opened without `use_test_clock=True`.
fn advance_test_clock(test_clock: &Option<Arc<TestClock>>, millis: i64) -> PyResult<()> {
    match test_clock {
        Some(tc) => {
            tc.advance(millis);
            Ok(())
        }
        None => Err(PyValueError::new_err(
            "advance_clock_for_testing() requires the client to have been opened \
             with use_test_clock=True. The default SystemClock cannot be advanced.",
        )),
    }
}

fn value_to_py(py: Python<'_>, value: &Value) -> PyResult<Py<PyAny>> {
    use pyo3::IntoPyObject;
    match value {
        Value::Null => Ok(py.None()),
        Value::Bool(b) => Ok(b.into_pyobject(py)?.to_owned().into_any().unbind()),
        Value::Number(n) => {
            if let Some(i) = n.as_i64() {
                Ok(i.into_pyobject(py)?.into_any().unbind())
            } else if let Some(f) = n.as_f64() {
                Ok(f.into_pyobject(py)?.into_any().unbind())
            } else {
                Ok(py.None())
            }
        }
        Value::String(s) => Ok(s.into_pyobject(py)?.into_any().unbind()),
        Value::Array(arr) => {
            let list = pyo3::types::PyList::empty(py);
            for item in arr {
                list.append(value_to_py(py, item)?)?;
            }
            Ok(list.into_any().unbind())
        }
        Value::Object(map) => {
            let dict = PyDict::new(py);
            for (k, v) in map {
                dict.set_item(k, value_to_py(py, v)?)?;
            }
            Ok(dict.into_any().unbind())
        }
    }
}

fn py_to_value(obj: &Bound<'_, pyo3::PyAny>) -> PyResult<Value> {
    if obj.is_none() {
        return Ok(Value::Null);
    }
    if let Ok(b) = obj.extract::<bool>() {
        return Ok(Value::Bool(b));
    }
    if let Ok(i) = obj.extract::<i64>() {
        return Ok(Value::Number(i.into()));
    }
    if let Ok(f) = obj.extract::<f64>() {
        return Ok(serde_json::Number::from_f64(f)
            .map(Value::Number)
            .unwrap_or(Value::Null));
    }
    if let Ok(s) = obj.extract::<String>() {
        return Ok(Value::String(s));
    }
    if let Ok(list) = obj.cast::<pyo3::types::PyList>() {
        let mut arr = Vec::new();
        for item in list.iter() {
            arr.push(py_to_value(&item)?);
        }
        return Ok(Value::Array(arr));
    }
    // Also handle tuples and sets as JSON arrays
    if let Ok(tuple) = obj.cast::<pyo3::types::PyTuple>() {
        let mut arr = Vec::new();
        for item in tuple.iter() {
            arr.push(py_to_value(&item)?);
        }
        return Ok(Value::Array(arr));
    }
    if let Ok(set) = obj.cast::<pyo3::types::PySet>() {
        let mut arr = Vec::new();
        for item in set.iter() {
            arr.push(py_to_value(&item)?);
        }
        return Ok(Value::Array(arr));
    }
    if let Ok(frozenset) = obj.cast::<pyo3::types::PyFrozenSet>() {
        let mut arr = Vec::new();
        for item in frozenset.iter() {
            arr.push(py_to_value(&item)?);
        }
        return Ok(Value::Array(arr));
    }
    if let Ok(dict) = obj.cast::<PyDict>() {
        let mut map = serde_json::Map::new();
        for (k, v) in dict.iter() {
            let key: String = k.extract()?;
            map.insert(key, py_to_value(&v)?);
        }
        return Ok(Value::Object(map));
    }
    // Fallback: convert via str()
    let s: String = obj.str()?.extract()?;
    Ok(Value::String(s))
}

fn storage_err(e: crawlee_storage::utils::StorageError) -> PyErr {
    use crawlee_storage::utils::StorageError;
    match e {
        StorageError::Io(e) => PyOSError::new_err(e.to_string()),
        StorageError::Json(e) => PyValueError::new_err(e.to_string()),
        StorageError::InvalidArgs(msg) => PyValueError::new_err(msg),
        StorageError::NotFound(msg) => PyFileNotFoundError::new_err(msg),
    }
}

/// Convert a serde-serializable struct to a Python dict.
///
/// Used for response payloads that contain no datetime fields
/// (`DatasetItemsListPage`, `ProcessedRequest`, `AddRequestsResponse`,
/// `KeyValueStoreRecordMetadata`). Datetimes wouldn't survive this route as
/// `datetime.datetime` — they'd come out as ISO strings — so the metadata
/// types use the dedicated builders below.
///
/// Keys are camelCase (matching the on-disk format).
fn serde_to_py<T: serde::Serialize>(py: Python<'_>, meta: &T) -> PyResult<Py<PyAny>> {
    let val = serde_json::to_value(meta).map_err(|e| PyRuntimeError::new_err(e.to_string()))?;
    value_to_py(py, &val)
}

/// Set the three shared base-metadata fields on a Python dict: `id`, `name`,
/// `accessedAt`, `createdAt`, `modifiedAt`. The datetime fields cross the FFI
/// as native `datetime.datetime` (timezone-aware UTC) thanks to PyO3's
/// `chrono` feature.
fn set_base_metadata_fields(
    dict: &Bound<'_, PyDict>,
    base: &models::StorageMetadata,
) -> PyResult<()> {
    dict.set_item("id", &base.id)?;
    dict.set_item("name", &base.name)?;
    dict.set_item("accessedAt", base.accessed_at)?;
    dict.set_item("createdAt", base.created_at)?;
    dict.set_item("modifiedAt", base.modified_at)?;
    Ok(())
}

fn dataset_metadata_to_py(py: Python<'_>, meta: &models::DatasetMetadata) -> PyResult<Py<PyAny>> {
    use pyo3::IntoPyObject;
    let dict = PyDict::new(py);
    set_base_metadata_fields(&dict, &meta.base)?;
    dict.set_item("itemCount", meta.item_count)?;
    Ok(dict.into_pyobject(py)?.into_any().unbind())
}

fn kvs_metadata_to_py(py: Python<'_>, meta: &models::KeyValueStoreMetadata) -> PyResult<Py<PyAny>> {
    use pyo3::IntoPyObject;
    let dict = PyDict::new(py);
    set_base_metadata_fields(&dict, &meta.base)?;
    Ok(dict.into_pyobject(py)?.into_any().unbind())
}

fn rq_metadata_to_py(py: Python<'_>, meta: &models::RequestQueueMetadata) -> PyResult<Py<PyAny>> {
    use pyo3::IntoPyObject;
    let dict = PyDict::new(py);
    set_base_metadata_fields(&dict, &meta.base)?;
    dict.set_item("hadMultipleClients", meta.had_multiple_clients)?;
    dict.set_item("handledRequestCount", meta.handled_request_count)?;
    dict.set_item("pendingRequestCount", meta.pending_request_count)?;
    dict.set_item("totalRequestCount", meta.total_request_count)?;
    Ok(dict.into_pyobject(py)?.into_any().unbind())
}

/// Convert a KVS file record (raw bytes) to a Python dict with `bytes` value.
fn record_file_to_py(
    py: Python<'_>,
    key: &str,
    content_type: &str,
    size: Option<usize>,
    data: &[u8],
) -> PyResult<Py<PyAny>> {
    use pyo3::IntoPyObject;

    let dict = PyDict::new(py);
    dict.set_item("key", key)?;
    dict.set_item("contentType", content_type)?;
    dict.set_item("size", size)?;
    dict.set_item("value", pyo3::types::PyBytes::new(py, data))?;

    Ok(dict.into_pyobject(py)?.into_any().unbind())
}

// ─── Dataset Item Iterator ──────────────────────────────────────────────────

const DEFAULT_PAGE_SIZE: usize = 1000;

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

#[gen_stub_pyclass]
#[pyclass]
struct DatasetItemIterator {
    state: Arc<Mutex<DatasetItemIteratorState>>,
}

#[gen_stub_pymethods]
#[pymethods]
impl DatasetItemIterator {
    fn __aiter__(slf: PyRef<'_, Self>) -> PyRef<'_, Self> {
        slf
    }

    #[gen_stub(override_return_type(type_repr = "dict[str, typing.Any]", imports = ("typing")))]
    fn __anext__<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, pyo3::PyAny>> {
        let state = self.state.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let mut st = state.lock().await;

            // If we still have buffered items, return the next one.
            if st.buf_index < st.buffer.len() {
                let item = st.buffer[st.buf_index].clone();
                st.buf_index += 1;
                return Python::attach(|py| value_to_py(py, &item));
            }

            // If we've exhausted everything, signal StopAsyncIteration.
            if st.done {
                return Err(PyStopAsyncIteration::new_err(()));
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
                return Err(PyStopAsyncIteration::new_err(()));
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
            st.buf_index = 1; // We're returning index 0 now.
            let item = st.buffer[0].clone();
            Python::attach(|py| value_to_py(py, &item))
        })
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

#[gen_stub_pyclass]
#[pyclass]
struct KvsKeyIterator {
    state: Arc<Mutex<KvsKeyIteratorState>>,
}

#[gen_stub_pymethods]
#[pymethods]
impl KvsKeyIterator {
    fn __aiter__(slf: PyRef<'_, Self>) -> PyRef<'_, Self> {
        slf
    }

    #[gen_stub(override_return_type(type_repr = "KeyValueStoreRecordMetadata"))]
    fn __anext__<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, pyo3::PyAny>> {
        let state = self.state.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let mut st = state.lock().await;

            // If we still have buffered items, return the next one.
            if st.buf_index < st.buffer.len() {
                let meta = st.buffer[st.buf_index].clone();
                st.buf_index += 1;
                return Python::attach(|py| serde_to_py(py, &meta));
            }

            // If we've exhausted everything, signal StopAsyncIteration.
            if st.done {
                return Err(PyStopAsyncIteration::new_err(()));
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
                return Err(PyStopAsyncIteration::new_err(()));
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
            let meta = st.buffer[0].clone();
            Python::attach(|py| serde_to_py(py, &meta))
        })
    }
}

// ─── Dataset Client ─────────────────────────────────────────────────────────

#[gen_stub_pyclass]
#[pyclass]
struct FileSystemDatasetClient {
    inner: Arc<crawlee_storage::dataset::FileSystemDatasetClient>,
    test_clock: Option<Arc<TestClock>>,
}

#[gen_stub_pymethods]
#[pymethods]
impl FileSystemDatasetClient {
    #[staticmethod]
    #[pyo3(signature = (id=None, name=None, alias=None, storage_dir="./storage", use_test_clock=false))]
    #[gen_stub(override_return_type(type_repr = "FileSystemDatasetClient"))]
    fn open<'py>(
        py: Python<'py>,
        id: Option<String>,
        name: Option<String>,
        alias: Option<String>,
        storage_dir: &str,
        use_test_clock: bool,
    ) -> PyResult<Bound<'py, pyo3::PyAny>> {
        let storage_dir = PathBuf::from(storage_dir);
        let (clock, test_clock) = pick_clock(use_test_clock);
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let client = crawlee_storage::dataset::FileSystemDatasetClient::open_with_clock(
                id,
                name,
                alias,
                &storage_dir,
                clock,
            )
            .await
            .map_err(storage_err)?;
            Ok(FileSystemDatasetClient {
                inner: Arc::new(client),
                test_clock,
            })
        })
    }

    /// Advance the client's clock by ``duration``. Only usable when the client
    /// was opened with ``use_test_clock=True``; raises ``ValueError`` otherwise.
    #[gen_stub(override_return_type(type_repr = "None"))]
    fn advance_clock_for_testing(&self, duration: Duration) -> PyResult<()> {
        advance_test_clock(&self.test_clock, duration.num_milliseconds())
    }

    /// Path to the dataset directory.
    #[getter]
    fn path_to_dataset(&self) -> PathBuf {
        self.inner.path().to_path_buf()
    }

    /// Path to the metadata file.
    #[getter]
    fn path_to_metadata(&self) -> PathBuf {
        self.inner.metadata_path()
    }

    #[gen_stub(override_return_type(type_repr = "DatasetMetadata"))]
    fn get_metadata<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, pyo3::PyAny>> {
        let client = self.inner.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let meta = client.get_metadata().await;
            Python::attach(|py| dataset_metadata_to_py(py, &meta))
        })
    }

    #[gen_stub(override_return_type(type_repr = "None"))]
    fn drop_storage<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, pyo3::PyAny>> {
        let client = self.inner.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            client.drop_storage().await.map_err(storage_err)?;
            Ok(())
        })
    }

    #[gen_stub(override_return_type(type_repr = "None"))]
    fn purge<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, pyo3::PyAny>> {
        let client = self.inner.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            client.purge().await.map_err(storage_err)?;
            Ok(())
        })
    }

    #[gen_stub(override_return_type(type_repr = "None"))]
    fn push_data<'py>(
        &self,
        py: Python<'py>,
        data: &Bound<'py, pyo3::PyAny>,
    ) -> PyResult<Bound<'py, pyo3::PyAny>> {
        let value = py_to_value(data)?;
        let client = self.inner.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            client.push_data(value).await.map_err(storage_err)?;
            Ok(())
        })
    }

    #[pyo3(signature = (offset=0, limit=None, desc=false, skip_empty=false))]
    #[gen_stub(override_return_type(type_repr = "DatasetItemsListPage"))]
    fn get_data<'py>(
        &self,
        py: Python<'py>,
        offset: usize,
        limit: Option<usize>,
        desc: bool,
        skip_empty: bool,
    ) -> PyResult<Bound<'py, pyo3::PyAny>> {
        let client = self.inner.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let page = client
                .get_data(offset, limit, desc, skip_empty)
                .await
                .map_err(storage_err)?;
            Python::attach(|py| serde_to_py(py, &page))
        })
    }

    #[pyo3(signature = (offset=0, limit=None, desc=false, skip_empty=false, page_size=None))]
    fn iterate_items(
        &self,
        offset: usize,
        limit: Option<usize>,
        desc: bool,
        skip_empty: bool,
        page_size: Option<usize>,
    ) -> DatasetItemIterator {
        DatasetItemIterator {
            state: Arc::new(Mutex::new(DatasetItemIteratorState {
                client: self.inner.clone(),
                offset,
                remaining_limit: limit,
                desc,
                skip_empty,
                page_size: page_size.unwrap_or(DEFAULT_PAGE_SIZE),
                buffer: Vec::new(),
                buf_index: 0,
                done: false,
            })),
        }
    }
}

// ─── Key-Value Store Client ─────────────────────────────────────────────────

#[gen_stub_pyclass]
#[pyclass]
struct FileSystemKeyValueStoreClient {
    inner: Arc<crawlee_storage::key_value_store::FileSystemKeyValueStoreClient>,
    test_clock: Option<Arc<TestClock>>,
}

#[gen_stub_pymethods]
#[pymethods]
impl FileSystemKeyValueStoreClient {
    #[staticmethod]
    #[pyo3(signature = (id=None, name=None, alias=None, storage_dir="./storage", use_test_clock=false))]
    #[gen_stub(override_return_type(type_repr = "FileSystemKeyValueStoreClient"))]
    fn open<'py>(
        py: Python<'py>,
        id: Option<String>,
        name: Option<String>,
        alias: Option<String>,
        storage_dir: &str,
        use_test_clock: bool,
    ) -> PyResult<Bound<'py, pyo3::PyAny>> {
        let storage_dir = PathBuf::from(storage_dir);
        let (clock, test_clock) = pick_clock(use_test_clock);
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
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
            Ok(FileSystemKeyValueStoreClient {
                inner: Arc::new(client),
                test_clock,
            })
        })
    }

    /// Advance the client's clock by ``duration``. Only usable when the client
    /// was opened with ``use_test_clock=True``; raises ``ValueError`` otherwise.
    #[gen_stub(override_return_type(type_repr = "None"))]
    fn advance_clock_for_testing(&self, duration: Duration) -> PyResult<()> {
        advance_test_clock(&self.test_clock, duration.num_milliseconds())
    }

    /// Path to the key-value store directory.
    #[getter]
    fn path_to_kvs(&self) -> PathBuf {
        self.inner.path().to_path_buf()
    }

    /// Path to the metadata file.
    #[getter]
    fn path_to_metadata(&self) -> PathBuf {
        self.inner.metadata_path()
    }

    #[gen_stub(override_return_type(type_repr = "KeyValueStoreMetadata"))]
    fn get_metadata<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, pyo3::PyAny>> {
        let client = self.inner.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let meta = client.get_metadata().await;
            Python::attach(|py| kvs_metadata_to_py(py, &meta))
        })
    }

    #[gen_stub(override_return_type(type_repr = "None"))]
    fn drop_storage<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, pyo3::PyAny>> {
        let client = self.inner.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            client.drop_storage().await.map_err(storage_err)?;
            Ok(())
        })
    }

    #[gen_stub(override_return_type(type_repr = "None"))]
    fn purge<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, pyo3::PyAny>> {
        let client = self.inner.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            client.purge().await.map_err(storage_err)?;
            Ok(())
        })
    }

    #[gen_stub(override_return_type(type_repr = "KeyValueStoreRecord | None"))]
    fn get_value<'py>(&self, py: Python<'py>, key: String) -> PyResult<Bound<'py, pyo3::PyAny>> {
        let client = self.inner.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let result = client.get_value(&key).await.map_err(storage_err)?;
            match result {
                Some((path, meta)) => {
                    let data = tokio::fs::read(&path)
                        .await
                        .map_err(|e| storage_err(e.into()))?;
                    Python::attach(|py| {
                        record_file_to_py(py, &key, &meta.content_type, meta.size, &data)
                    })
                }
                None => Ok(Python::attach(|py| py.None())),
            }
        })
    }

    #[pyo3(signature = (key, value, content_type=None))]
    #[gen_stub(override_return_type(type_repr = "None"))]
    fn set_value<'py>(
        &self,
        py: Python<'py>,
        key: String,
        #[gen_stub(override_type(type_repr = "builtins.bytes", imports = ("builtins")))] value: Vec<
            u8,
        >,
        content_type: Option<String>,
    ) -> PyResult<Bound<'py, pyo3::PyAny>> {
        let ct = content_type.unwrap_or_else(|| "application/octet-stream".to_string());
        let client = self.inner.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            client
                .set_value(&key, &value, ct)
                .await
                .map_err(storage_err)?;
            Ok(())
        })
    }

    #[gen_stub(override_return_type(type_repr = "None"))]
    fn delete_value<'py>(&self, py: Python<'py>, key: String) -> PyResult<Bound<'py, pyo3::PyAny>> {
        let client = self.inner.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            client.delete_value(&key).await.map_err(storage_err)?;
            Ok(())
        })
    }

    #[pyo3(signature = (exclusive_start_key=None, limit=None, page_size=None, prefix=None))]
    fn iterate_keys(
        &self,
        exclusive_start_key: Option<String>,
        limit: Option<usize>,
        page_size: Option<usize>,
        prefix: Option<String>,
    ) -> KvsKeyIterator {
        KvsKeyIterator {
            state: Arc::new(Mutex::new(KvsKeyIteratorState {
                client: self.inner.clone(),
                exclusive_start_key,
                remaining_limit: limit,
                page_size: page_size.unwrap_or(DEFAULT_PAGE_SIZE),
                prefix,
                buffer: Vec::new(),
                buf_index: 0,
                done: false,
            })),
        }
    }

    #[gen_stub(override_return_type(type_repr = "builtins.str"))]
    fn get_public_url<'py>(
        &self,
        py: Python<'py>,
        key: String,
    ) -> PyResult<Bound<'py, pyo3::PyAny>> {
        let client = self.inner.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            Ok(client.get_public_url(&key).await)
        })
    }

    #[gen_stub(override_return_type(type_repr = "builtins.bool"))]
    fn record_exists<'py>(
        &self,
        py: Python<'py>,
        key: String,
    ) -> PyResult<Bound<'py, pyo3::PyAny>> {
        let client = self.inner.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            Ok(client.record_exists(&key).await)
        })
    }
}

// ─── Request Queue Client ───────────────────────────────────────────────────

#[gen_stub_pyclass]
#[pyclass]
struct FileSystemRequestQueueClient {
    inner: Arc<crawlee_storage::request_queue::FileSystemRequestQueueClient>,
    test_clock: Option<Arc<TestClock>>,
}

#[gen_stub_pymethods]
#[pymethods]
impl FileSystemRequestQueueClient {
    /// Open a request queue.
    ///
    /// ``use_test_clock``: see ``advance_clock_for_testing`` below.
    ///
    /// ``assume_sole_owner`` (default ``False``): controls how locks on disk
    /// are treated at open time. With ``False`` (the safe default), any
    /// future-dated ``orderNo`` is respected as a potential live peer's lock —
    /// crashed peers' locks expire naturally on the wall clock. With
    /// ``True``, the caller asserts nothing else is using this queue and any
    /// in-progress locks are reclaimed immediately, so a request whose
    /// previous run died is instantly re-fetchable. Set to ``True`` only if
    /// you know you're the sole consumer; otherwise you risk two peers
    /// processing the same request.
    #[staticmethod]
    #[pyo3(signature = (id=None, name=None, alias=None, storage_dir="./storage", use_test_clock=false, assume_sole_owner=false))]
    #[gen_stub(override_return_type(type_repr = "FileSystemRequestQueueClient"))]
    fn open<'py>(
        py: Python<'py>,
        id: Option<String>,
        name: Option<String>,
        alias: Option<String>,
        storage_dir: &str,
        use_test_clock: bool,
        assume_sole_owner: bool,
    ) -> PyResult<Bound<'py, pyo3::PyAny>> {
        let storage_dir = PathBuf::from(storage_dir);
        let (clock, test_clock) = pick_clock(use_test_clock);
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let client =
                crawlee_storage::request_queue::FileSystemRequestQueueClient::open_with_clock(
                    id,
                    name,
                    alias,
                    &storage_dir,
                    clock,
                    assume_sole_owner,
                )
                .await
                .map_err(storage_err)?;
            Ok(FileSystemRequestQueueClient {
                inner: Arc::new(client),
                test_clock,
            })
        })
    }

    /// Advance the client's clock by ``duration``. Only usable when the client
    /// was opened with ``use_test_clock=True``; raises ``ValueError`` otherwise.
    ///
    /// This is the hook that lets Python tests using ``freezegun`` or
    /// similar frameworks exercise lock-expiry behavior — frozen Python
    /// clocks don't reach into native code, so the test must drive the
    /// Rust-side clock explicitly via this method.
    #[gen_stub(override_return_type(type_repr = "None"))]
    fn advance_clock_for_testing(&self, duration: Duration) -> PyResult<()> {
        advance_test_clock(&self.test_clock, duration.num_milliseconds())
    }

    /// Path to the request queue directory.
    #[getter]
    fn path_to_rq(&self) -> PathBuf {
        self.inner.path().to_path_buf()
    }

    /// Path to the metadata file.
    #[getter]
    fn path_to_metadata(&self) -> PathBuf {
        self.inner.metadata_path()
    }

    #[gen_stub(override_return_type(type_repr = "RequestQueueMetadata"))]
    fn get_metadata<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, pyo3::PyAny>> {
        let client = self.inner.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let meta = client.get_metadata().await;
            Python::attach(|py| rq_metadata_to_py(py, &meta))
        })
    }

    #[gen_stub(override_return_type(type_repr = "None"))]
    fn drop_storage<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, pyo3::PyAny>> {
        let client = self.inner.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            client.drop_storage().await.map_err(storage_err)?;
            Ok(())
        })
    }

    #[gen_stub(override_return_type(type_repr = "None"))]
    fn purge<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, pyo3::PyAny>> {
        let client = self.inner.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            client.purge().await.map_err(storage_err)?;
            Ok(())
        })
    }

    #[pyo3(signature = (requests, forefront=false))]
    #[gen_stub(override_return_type(type_repr = "AddRequestsResponse"))]
    fn add_batch_of_requests<'py>(
        &self,
        py: Python<'py>,
        requests: &Bound<'py, pyo3::types::PyList>,
        forefront: bool,
    ) -> PyResult<Bound<'py, pyo3::PyAny>> {
        let mut req_values = Vec::new();
        for item in requests.iter() {
            req_values.push(py_to_value(&item)?);
        }
        let client = self.inner.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let response = client
                .add_batch_of_requests(req_values, forefront)
                .await
                .map_err(storage_err)?;
            Python::attach(|py| serde_to_py(py, &response))
        })
    }

    #[gen_stub(override_return_type(type_repr = "dict[str, typing.Any] | None"))]
    fn get_request<'py>(
        &self,
        py: Python<'py>,
        unique_key: String,
    ) -> PyResult<Bound<'py, pyo3::PyAny>> {
        let client = self.inner.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let request = client.get_request(&unique_key).await.map_err(storage_err)?;
            Python::attach(|py| match request {
                Some(r) => value_to_py(py, &r),
                None => Ok(py.None()),
            })
        })
    }

    #[gen_stub(override_return_type(type_repr = "dict[str, typing.Any] | None"))]
    fn fetch_next_request<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, pyo3::PyAny>> {
        let client = self.inner.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let request = client.fetch_next_request().await.map_err(storage_err)?;
            Python::attach(|py| match request {
                Some(r) => value_to_py(py, &r),
                None => Ok(py.None()),
            })
        })
    }

    #[gen_stub(override_return_type(type_repr = "ProcessedRequest | None"))]
    fn mark_request_as_handled<'py>(
        &self,
        py: Python<'py>,
        request: &Bound<'py, pyo3::PyAny>,
    ) -> PyResult<Bound<'py, pyo3::PyAny>> {
        let val = py_to_value(request)?;
        let client = self.inner.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let result = client
                .mark_request_as_handled(val)
                .await
                .map_err(storage_err)?;
            Python::attach(|py| match result {
                Some(r) => serde_to_py(py, &r),
                None => Ok(py.None()),
            })
        })
    }

    #[pyo3(signature = (request, forefront=false))]
    #[gen_stub(override_return_type(type_repr = "ProcessedRequest | None"))]
    fn reclaim_request<'py>(
        &self,
        py: Python<'py>,
        request: &Bound<'py, pyo3::PyAny>,
        forefront: bool,
    ) -> PyResult<Bound<'py, pyo3::PyAny>> {
        let val = py_to_value(request)?;
        let client = self.inner.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let result = client
                .reclaim_request(val, forefront)
                .await
                .map_err(storage_err)?;
            Python::attach(|py| match result {
                Some(r) => serde_to_py(py, &r),
                None => Ok(py.None()),
            })
        })
    }

    #[gen_stub(override_return_type(type_repr = "builtins.bool"))]
    fn is_empty<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, pyo3::PyAny>> {
        let client = self.inner.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move { Ok(client.is_empty().await) })
    }

    #[gen_stub(override_return_type(type_repr = "builtins.bool"))]
    fn is_finished<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, pyo3::PyAny>> {
        let client = self.inner.clone();
        pyo3_async_runtimes::tokio::future_into_py(
            py,
            async move { Ok(client.is_finished().await) },
        )
    }

    #[gen_stub(override_return_type(type_repr = "None"))]
    fn set_expected_request_processing_time<'py>(
        &self,
        py: Python<'py>,
        duration: Duration,
    ) -> PyResult<Bound<'py, pyo3::PyAny>> {
        // Convert to fractional seconds. `num_microseconds` returns Some unless
        // the duration overflows ~292,000 years — fall back to ms if it does.
        let secs = duration
            .num_microseconds()
            .map(|us| us as f64 / 1_000_000.0)
            .unwrap_or_else(|| duration.num_milliseconds() as f64 / 1_000.0);
        let client = self.inner.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            client.set_expected_request_processing_time(secs).await;
            Ok(())
        })
    }

    #[gen_stub(override_return_type(type_repr = "None"))]
    fn persist_state<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, pyo3::PyAny>> {
        let client = self.inner.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            client.persist_state().await;
            Ok(())
        })
    }
}

// ─── Module ─────────────────────────────────────────────────────────────────

#[pymodule(name = "_native")]
fn _crawlee_storage(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_class::<FileSystemDatasetClient>()?;
    m.add_class::<FileSystemKeyValueStoreClient>()?;
    m.add_class::<FileSystemRequestQueueClient>()?;
    m.add_class::<DatasetItemIterator>()?;
    m.add_class::<KvsKeyIterator>()?;
    Ok(())
}

// Re-export all classes from _native into the top-level crawlee_storage module,
// mirroring what python/crawlee_storage/__init__.py does.
pyo3_stub_gen::reexport_module_members!("crawlee_storage", "crawlee_storage._native");

define_stub_info_gatherer!(stub_info);
