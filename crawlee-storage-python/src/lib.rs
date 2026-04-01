use std::path::PathBuf;
use std::sync::Arc;

use pyo3::exceptions::PyRuntimeError;
use pyo3::prelude::*;
use pyo3::types::PyDict;
use pyo3_stub_gen::define_stub_info_gatherer;
use pyo3_stub_gen::derive::{gen_stub_pyclass, gen_stub_pymethods};
use serde_json::Value;

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
    // Check for bytes/bytearray BEFORE list, but use isinstance to avoid matching lists
    // of small ints (PyO3's extract::<Vec<u8>> happily converts [1, 2, 3] to bytes).
    if obj.is_instance_of::<pyo3::types::PyBytes>()
        || obj.is_instance_of::<pyo3::types::PyByteArray>()
    {
        if let Ok(bytes) = obj.extract::<Vec<u8>>() {
            use base64::Engine;
            let encoded = base64::engine::general_purpose::STANDARD.encode(&bytes);
            return Ok(Value::String(encoded));
        }
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
    PyRuntimeError::new_err(e.to_string())
}

/// Convert a serde_json metadata struct to a Python dict.
fn metadata_to_py<T: serde::Serialize>(py: Python<'_>, meta: &T) -> PyResult<Py<PyAny>> {
    let val = serde_json::to_value(meta).map_err(|e| PyRuntimeError::new_err(e.to_string()))?;
    value_to_py(py, &val)
}

/// Convert a KVS record to a Python dict, decoding binary values to Python `bytes`.
fn record_to_py(
    py: Python<'_>,
    record: &crawlee_storage::models::KeyValueStoreRecord,
) -> PyResult<Py<PyAny>> {
    use pyo3::IntoPyObject;

    let dict = PyDict::new(py);
    dict.set_item("key", &record.key)?;
    dict.set_item("content_type", &record.content_type)?;
    dict.set_item("size", record.size)?;

    // For binary (non-text, non-json, non-null) content types, decode the base64 value
    // back to Python bytes.
    let ct = &record.content_type;
    let is_binary = ct != "application/x-none"
        && ct != "application/json"
        && !ct.starts_with("text/");

    if is_binary {
        if let Value::String(ref b64) = record.value {
            use base64::Engine;
            match base64::engine::general_purpose::STANDARD.decode(b64) {
                Ok(bytes) => {
                    dict.set_item("value", pyo3::types::PyBytes::new(py, &bytes))?;
                }
                Err(_) => {
                    // Fallback: return as string
                    dict.set_item("value", value_to_py(py, &record.value)?)?;
                }
            }
        } else {
            dict.set_item("value", value_to_py(py, &record.value)?)?;
        }
    } else {
        dict.set_item("value", value_to_py(py, &record.value)?)?;
    }

    Ok(dict.into_pyobject(py)?.into_any().unbind())
}

// ─── Dataset Client ─────────────────────────────────────────────────────────

#[gen_stub_pyclass]
#[pyclass]
struct FileSystemDatasetClient {
    inner: Arc<crawlee_storage::dataset::FileSystemDatasetClient>,
}

#[gen_stub_pymethods]
#[pymethods]
impl FileSystemDatasetClient {
    #[staticmethod]
    #[pyo3(signature = (id=None, name=None, storage_dir="./storage"))]
    #[gen_stub(override_return_type(type_repr = "FileSystemDatasetClient"))]
    fn open<'py>(
        py: Python<'py>,
        id: Option<String>,
        name: Option<String>,
        storage_dir: &str,
    ) -> PyResult<Bound<'py, pyo3::PyAny>> {
        let storage_dir = PathBuf::from(storage_dir);
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let client =
                crawlee_storage::dataset::FileSystemDatasetClient::open(id, name, &storage_dir)
                    .await
                    .map_err(storage_err)?;
            Ok(FileSystemDatasetClient {
                inner: Arc::new(client),
            })
        })
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

    #[gen_stub(override_return_type(type_repr = "dict[str, typing.Any]", imports = ("typing")))]
    fn get_metadata<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, pyo3::PyAny>> {
        let client = self.inner.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let meta = client.get_metadata().await;
            Python::attach(|py| metadata_to_py(py, &meta))
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

    #[pyo3(signature = (offset=0, limit=999999999999, desc=false, skip_empty=false))]
    #[gen_stub(override_return_type(type_repr = "dict[str, typing.Any]", imports = ("typing")))]
    fn get_data<'py>(
        &self,
        py: Python<'py>,
        offset: usize,
        limit: usize,
        desc: bool,
        skip_empty: bool,
    ) -> PyResult<Bound<'py, pyo3::PyAny>> {
        let client = self.inner.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let page = client
                .get_data(offset, limit, desc, skip_empty)
                .await
                .map_err(storage_err)?;
            Python::attach(|py| metadata_to_py(py, &page))
        })
    }

    #[pyo3(signature = (offset=0, limit=None, desc=false, skip_empty=false))]
    #[gen_stub(override_return_type(type_repr = "list[typing.Any]", imports = ("typing")))]
    fn iterate_items<'py>(
        &self,
        py: Python<'py>,
        offset: usize,
        limit: Option<usize>,
        desc: bool,
        skip_empty: bool,
    ) -> PyResult<Bound<'py, pyo3::PyAny>> {
        let client = self.inner.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let items = client
                .iterate_items(offset, limit, desc, skip_empty)
                .await
                .map_err(storage_err)?;
            Python::attach(|py| {
                let list = pyo3::types::PyList::empty(py);
                for item in &items {
                    list.append(value_to_py(py, item)?)?;
                }
                Ok(list.into_any().unbind())
            })
        })
    }
}

// ─── Key-Value Store Client ─────────────────────────────────────────────────

#[gen_stub_pyclass]
#[pyclass]
struct FileSystemKeyValueStoreClient {
    inner: Arc<crawlee_storage::key_value_store::FileSystemKeyValueStoreClient>,
}

#[gen_stub_pymethods]
#[pymethods]
impl FileSystemKeyValueStoreClient {
    #[staticmethod]
    #[pyo3(signature = (id=None, name=None, storage_dir="./storage"))]
    #[gen_stub(override_return_type(type_repr = "FileSystemKeyValueStoreClient"))]
    fn open<'py>(
        py: Python<'py>,
        id: Option<String>,
        name: Option<String>,
        storage_dir: &str,
    ) -> PyResult<Bound<'py, pyo3::PyAny>> {
        let storage_dir = PathBuf::from(storage_dir);
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let client = crawlee_storage::key_value_store::FileSystemKeyValueStoreClient::open(
                id,
                name,
                &storage_dir,
            )
            .await
            .map_err(storage_err)?;
            Ok(FileSystemKeyValueStoreClient {
                inner: Arc::new(client),
            })
        })
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

    #[gen_stub(override_return_type(type_repr = "dict[str, typing.Any]", imports = ("typing")))]
    fn get_metadata<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, pyo3::PyAny>> {
        let client = self.inner.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let meta = client.get_metadata().await;
            Python::attach(|py| metadata_to_py(py, &meta))
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

    #[gen_stub(override_return_type(type_repr = "typing.Optional[dict[str, typing.Any]]", imports = ("typing")))]
    fn get_value<'py>(
        &self,
        py: Python<'py>,
        key: String,
    ) -> PyResult<Bound<'py, pyo3::PyAny>> {
        let client = self.inner.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let record = client.get_value(&key).await.map_err(storage_err)?;
            Python::attach(|py| match record {
                Some(r) => record_to_py(py, &r),
                None => Ok(py.None()),
            })
        })
    }

    #[pyo3(signature = (key, value, content_type=None))]
    #[gen_stub(override_return_type(type_repr = "None"))]
    fn set_value<'py>(
        &self,
        py: Python<'py>,
        key: String,
        value: &Bound<'py, pyo3::PyAny>,
        content_type: Option<String>,
    ) -> PyResult<Bound<'py, pyo3::PyAny>> {
        // If the Python value is bytes/bytearray and no content_type was given,
        // default to application/octet-stream (not text/plain).
        let is_bytes = value.is_instance_of::<pyo3::types::PyBytes>()
            || value.is_instance_of::<pyo3::types::PyByteArray>();
        let content_type = if is_bytes && content_type.is_none() {
            Some("application/octet-stream".to_string())
        } else {
            content_type
        };
        let val = py_to_value(value)?;
        let client = self.inner.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            client
                .set_value(&key, val, content_type)
                .await
                .map_err(storage_err)?;
            Ok(())
        })
    }

    #[gen_stub(override_return_type(type_repr = "None"))]
    fn delete_value<'py>(
        &self,
        py: Python<'py>,
        key: String,
    ) -> PyResult<Bound<'py, pyo3::PyAny>> {
        let client = self.inner.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            client.delete_value(&key).await.map_err(storage_err)?;
            Ok(())
        })
    }

    #[pyo3(signature = (exclusive_start_key=None, limit=None))]
    #[gen_stub(override_return_type(type_repr = "list[dict[str, typing.Any]]", imports = ("typing")))]
    fn iterate_keys<'py>(
        &self,
        py: Python<'py>,
        exclusive_start_key: Option<String>,
        limit: Option<usize>,
    ) -> PyResult<Bound<'py, pyo3::PyAny>> {
        let client = self.inner.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let keys = client
                .iterate_keys(exclusive_start_key.as_deref(), limit)
                .await
                .map_err(storage_err)?;
            Python::attach(|py| {
                let list = pyo3::types::PyList::empty(py);
                for key_meta in &keys {
                    list.append(metadata_to_py(py, key_meta)?)?;
                }
                Ok(list.into_any().unbind())
            })
        })
    }

    fn get_public_url(&self, key: String) -> String {
        self.inner.get_public_url(&key)
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
}

#[gen_stub_pymethods]
#[pymethods]
impl FileSystemRequestQueueClient {
    #[staticmethod]
    #[pyo3(signature = (id=None, name=None, storage_dir="./storage", state_loader=None, state_saver=None, state_clearer=None))]
    #[gen_stub(override_return_type(type_repr = "FileSystemRequestQueueClient"))]
    fn open<'py>(
        py: Python<'py>,
        id: Option<String>,
        name: Option<String>,
        storage_dir: &str,
        state_loader: Option<Py<PyAny>>,
        state_saver: Option<Py<PyAny>>,
        state_clearer: Option<Py<PyAny>>,
    ) -> PyResult<Bound<'py, pyo3::PyAny>> {
        let storage_dir = PathBuf::from(storage_dir);

        // Build persistence callbacks if all three are provided
        let persistence = match (state_loader, state_saver, state_clearer) {
            (Some(loader), Some(saver), Some(clearer)) => {
                let loader: Arc<Py<PyAny>> = Arc::new(loader);
                let saver: Arc<Py<PyAny>> = Arc::new(saver);
                let clearer: Arc<Py<PyAny>> = Arc::new(clearer);

                Some(crawlee_storage::request_queue::RqStatePersistence {
                    load: Arc::new({
                        let loader = loader.clone();
                        move || {
                            let loader = loader.clone();
                            Box::pin(async move {
                                let result: Option<Value> = Python::attach(|py| {
                                    let coro = loader.call0(py)?;
                                    // Convert the coroutine result
                                    // For now, we'll handle this synchronously
                                    // In a real implementation, we'd await the coroutine
                                    let _ = coro;
                                    Ok::<_, PyErr>(None)
                                })
                                .unwrap_or(None);
                                result
                            })
                                as std::pin::Pin<
                                    Box<dyn std::future::Future<Output = Option<Value>> + Send>,
                                >
                        }
                    }),
                    save: Arc::new({
                        let _saver = saver.clone();
                        move |_state: Value| {
                            let _saver = _saver.clone();
                            Box::pin(async move {
                                // TODO: Call Python saver callback with state
                            })
                                as std::pin::Pin<
                                    Box<dyn std::future::Future<Output = ()> + Send>,
                                >
                        }
                    }),
                    clear: Arc::new({
                        let _clearer = clearer.clone();
                        move || {
                            let _clearer = _clearer.clone();
                            Box::pin(async move {
                                // TODO: Call Python clearer callback
                            })
                                as std::pin::Pin<
                                    Box<dyn std::future::Future<Output = ()> + Send>,
                                >
                        }
                    }),
                })
            }
            _ => None,
        };

        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let client =
                crawlee_storage::request_queue::FileSystemRequestQueueClient::open(
                    id,
                    name,
                    &storage_dir,
                    persistence,
                )
                .await
                .map_err(storage_err)?;
            Ok(FileSystemRequestQueueClient {
                inner: Arc::new(client),
            })
        })
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

    #[gen_stub(override_return_type(type_repr = "dict[str, typing.Any]", imports = ("typing")))]
    fn get_metadata<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, pyo3::PyAny>> {
        let client = self.inner.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let meta = client.get_metadata().await;
            Python::attach(|py| metadata_to_py(py, &meta))
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
    #[gen_stub(override_return_type(type_repr = "dict[str, typing.Any]", imports = ("typing")))]
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
            Python::attach(|py| metadata_to_py(py, &response))
        })
    }

    #[gen_stub(override_return_type(type_repr = "typing.Optional[dict[str, typing.Any]]", imports = ("typing")))]
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

    #[gen_stub(override_return_type(type_repr = "typing.Optional[dict[str, typing.Any]]", imports = ("typing")))]
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

    #[gen_stub(override_return_type(type_repr = "typing.Optional[dict[str, typing.Any]]", imports = ("typing")))]
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
                Some(r) => metadata_to_py(py, &r),
                None => Ok(py.None()),
            })
        })
    }

    #[pyo3(signature = (request, forefront=false))]
    #[gen_stub(override_return_type(type_repr = "typing.Optional[dict[str, typing.Any]]", imports = ("typing")))]
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
                Some(r) => metadata_to_py(py, &r),
                None => Ok(py.None()),
            })
        })
    }

    #[gen_stub(override_return_type(type_repr = "builtins.bool"))]
    fn is_empty<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, pyo3::PyAny>> {
        let client = self.inner.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            Ok(client.is_empty().await)
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
    Ok(())
}

// Re-export all classes from _native into the top-level crawlee_storage module,
// mirroring what python/crawlee_storage/__init__.py does.
pyo3_stub_gen::reexport_module_members!("crawlee_storage", "crawlee_storage._native");

define_stub_info_gatherer!(stub_info);
