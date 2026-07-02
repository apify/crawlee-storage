pub mod models;

use std::path::PathBuf;
use std::sync::Arc;

use chrono::Duration;
use crawlee_storage::clock::{ClockRef, TestClock};
use pyo3::exceptions::{PyFileNotFoundError, PyOSError, PyRuntimeError, PyTypeError, PyValueError};
use pyo3::prelude::*;
use pyo3_stub_gen::define_stub_info_gatherer;
use pyo3_stub_gen::derive::{gen_stub_pyclass, gen_stub_pymethods};
use serde_json::Value;

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
fn advance_test_clock(test_clock: &Option<Arc<TestClock>>, delta: Duration) -> PyResult<()> {
    match test_clock {
        Some(tc) => {
            tc.advance(delta);
            Ok(())
        }
        None => Err(PyValueError::new_err(
            "advance_clock_for_testing() requires the client to have been opened \
             with use_test_clock=True. The default SystemClock cannot be advanced.",
        )),
    }
}

/// Convert a `serde_json::Value` to a Python object (`None`/`bool`/`int`/`float`/
/// `str`/`list`/`dict`). Thin wrapper over `pythonize`, which walks the value
/// via serde — so this stays correct as the JSON shape evolves without any
/// hand-rolled recursion.
fn value_to_py(py: Python<'_>, value: &Value) -> PyResult<Py<PyAny>> {
    Ok(pythonize::pythonize(py, value)
        .map_err(|e| PyValueError::new_err(e.to_string()))?
        .unbind())
}

/// Convert a Python object to a `serde_json::Value` via `pythonize`'s
/// serde-backed depythonizer. Accepts the JSON-shaped Python types plus the
/// sequence/collection types the old hand-rolled walker did
/// (`None`/`bool`/`int`/`float`/`str`/`list`/`tuple`/`set`/`frozenset`/`dict`;
/// sets and tuples become JSON arrays). Arbitrary objects now raise
/// `TypeError` instead of being silently `str()`-stringified — that fallback
/// was a quiet data-corruption hazard.
fn py_to_value(obj: &Bound<'_, pyo3::PyAny>) -> PyResult<Value> {
    pythonize::depythonize(obj).map_err(|e| PyTypeError::new_err(e.to_string()))
}

fn storage_err(e: crawlee_storage::utils::StorageError) -> PyErr {
    use crawlee_storage::utils::StorageError;
    match e {
        StorageError::Io(e) => PyOSError::new_err(e.to_string()),
        StorageError::Json(e) => PyValueError::new_err(e.to_string()),
        StorageError::InvalidArgs(msg) => PyValueError::new_err(msg),
        StorageError::NotFound(msg) => PyFileNotFoundError::new_err(msg),
        // The Display already renders the exact crawlee contract message
        // ("exclusiveStartKey \"<KEY>\" was not found ..."); surface it as a
        // ValueError so the Python consumer can drop its preflight existence
        // guard and rely on the raise.
        StorageError::ExclusiveStartKeyNotFound(_) => PyValueError::new_err(e.to_string()),
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

// The metadata/record → Python dict builders now live next to their field
// specs in `models.rs` (`models::DatasetMetadata::to_py`, etc.), so the dict a
// caller receives and the `TypedDict` the stub promises are defined together.

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
        advance_test_clock(&self.test_clock, duration)
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
            Python::attach(|py| models::DatasetMetadata(&meta).to_py(py))
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
        advance_test_clock(&self.test_clock, duration)
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
            Python::attach(|py| models::KeyValueStoreMetadata(&meta).to_py(py))
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

    /// Get a tracked record (value file + metadata sidecar) by key.
    ///
    /// To read out-of-band files that have no metadata sidecar (e.g. a
    /// CLI-written `INPUT.json`), use `resolve_value`, which probes the
    /// conventional bare-file extensions.
    #[gen_stub(override_return_type(type_repr = "KeyValueStoreRecord | None"))]
    fn get_value<'py>(&self, py: Python<'py>, key: String) -> PyResult<Bound<'py, pyo3::PyAny>> {
        let client = self.inner.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let result = client.read_value(&key).await.map_err(storage_err)?;
            match result {
                Some(record) => Python::attach(|py| models::KeyValueStoreRecord(&record).to_py(py)),
                None => Ok(Python::attach(|py| py.None())),
            }
        })
    }

    /// Resolve a key to a record, transparently falling back to out-of-band
    /// ("bare") value files that have no metadata sidecar.
    ///
    /// Tries the tracked record for the literal `key` first (its content type
    /// comes verbatim from the sidecar), then probes each `(extension,
    /// content_type)` in `bare_fallbacks` as a bare `key + extension` file,
    /// reporting the declared content type on a match. The first match wins;
    /// the returned record is always keyed by the requested `key`. Returns
    /// `None` if nothing resolves.
    ///
    /// Use this for run-input lookup (`INPUT`, `INPUT.json`, `INPUT.bin`, ...)
    /// instead of hand-rolling the extension probing in Python. The core does
    /// no MIME inference of its own — the caller declares which extensions map
    /// to which content type. An empty `content_type` keeps the matched file's
    /// synthesized `application/octet-stream`.
    #[gen_stub(override_return_type(type_repr = "KeyValueStoreRecord | None"))]
    fn resolve_value<'py>(
        &self,
        py: Python<'py>,
        key: String,
        bare_fallbacks: Vec<(String, String)>,
    ) -> PyResult<Bound<'py, pyo3::PyAny>> {
        let client = self.inner.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let fallbacks: Vec<(&str, &str)> = bare_fallbacks
                .iter()
                .map(|(ext, ct)| (ext.as_str(), ct.as_str()))
                .collect();
            let result = client
                .resolve_and_read_value(&key, &fallbacks)
                .await
                .map_err(storage_err)?;
            match result {
                Some(record) => Python::attach(|py| models::KeyValueStoreRecord(&record).to_py(py)),
                None => Ok(Python::attach(|py| py.None())),
            }
        })
    }

    /// Resolve a key to the on-disk key that actually exists, using the same
    /// fallback probe order as `resolve_value` but without reading the value.
    /// Returns the matched key (the literal key or `key + extension`), or
    /// `None` if nothing exists. Pass the result to `get_public_url` so the URL
    /// points at the file that exists.
    #[gen_stub(override_return_type(type_repr = "builtins.str | None"))]
    fn resolve_existing_key<'py>(
        &self,
        py: Python<'py>,
        key: String,
        bare_fallbacks: Vec<String>,
    ) -> PyResult<Bound<'py, pyo3::PyAny>> {
        let client = self.inner.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let fallbacks: Vec<&str> = bare_fallbacks.iter().map(String::as_str).collect();
            Ok(client.resolve_existing_key(&key, &fallbacks).await)
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

    /// Delete all records except those whose keys are listed in `keep`.
    ///
    /// Matching is by exact key (no extension globbing): to spare both `INPUT`
    /// and `INPUT.json`, pass both. The store metadata is always kept.
    #[gen_stub(override_return_type(type_repr = "None"))]
    #[pyo3(signature = (keep=vec![]))]
    fn purge<'py>(&self, py: Python<'py>, keep: Vec<String>) -> PyResult<Bound<'py, pyo3::PyAny>> {
        let client = self.inner.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            client.purge(&keep).await.map_err(storage_err)?;
            Ok(())
        })
    }

    /// Delete a single value (and its metadata sidecar) by key.
    #[gen_stub(override_return_type(type_repr = "None"))]
    fn delete_value<'py>(&self, py: Python<'py>, key: String) -> PyResult<Bound<'py, pyo3::PyAny>> {
        let client = self.inner.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            client.delete_value(&key).await.map_err(storage_err)?;
            Ok(())
        })
    }

    /// List a single self-describing page of keys.
    ///
    /// Returns a `KeyValueStoreListKeysResult` dict matching crawlee's
    /// `KeyValueStoreListKeysResult` contract: the page's `items` bundled with
    /// the echoed `exclusiveStartKey`/`limit`, an `isTruncated` flag, and the
    /// derived `nextExclusiveStartKey` (the cursor for the next call, set iff
    /// `isTruncated`). Call it repeatedly, feeding `nextExclusiveStartKey` back
    /// as `exclusive_start_key`, to stream every key one page at a time.
    ///
    /// `limit` bounds the page size (defaults to 1000) and is echoed back on
    /// the result. A bare file (declared via `bare_fallbacks`) whose on-disk
    /// value-file name collides with a tracked record is dropped.
    ///
    /// `bare_fallbacks` additionally surfaces out-of-band ("bare") value files
    /// that have no metadata sidecar (e.g. a CLI-written `INPUT.json`) as regular
    /// keys. Each entry is a `(name, content_type)` tuple where `name` is the
    /// file's on-disk key: if that file exists with no tracked record, it is
    /// listed under `name` (an empty `content_type` reports the synthesized
    /// `application/octet-stream`). Pass an empty list (the default) to list only
    /// tracked records.
    ///
    /// Round-trip caveat: a surfaced bare key does NOT round-trip through the
    /// strict read path. The listed key is the literal on-disk `name`, but
    /// `get_value` / `record_exists` only see tracked records (value + sidecar)
    /// and return `None` / `False` for a sidecar-less bare file. Read a listed
    /// bare key back via `resolve_value` / `resolve_existing_key`, not
    /// `get_value`.
    #[gen_stub(override_return_type(type_repr = "KeyValueStoreListKeysResult"))]
    #[pyo3(signature = (exclusive_start_key=None, limit=None, prefix=None, bare_fallbacks=vec![]))]
    fn list_keys<'py>(
        &self,
        py: Python<'py>,
        exclusive_start_key: Option<String>,
        limit: Option<usize>,
        prefix: Option<String>,
        bare_fallbacks: Vec<(String, String)>,
    ) -> PyResult<Bound<'py, pyo3::PyAny>> {
        let client = self.inner.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let bare_refs: Vec<(&str, &str)> = bare_fallbacks
                .iter()
                .map(|(name, ct)| (name.as_str(), ct.as_str()))
                .collect();
            let result = client
                .list_keys(
                    exclusive_start_key.as_deref(),
                    limit,
                    prefix.as_deref(),
                    &bare_refs,
                )
                .await
                .map_err(storage_err)?;
            Python::attach(|py| models::KvsListKeysResult(&result).to_py(py))
        })
    }

    /// Build a `file://` URL for `key`, or `None` if no value file exists for it
    /// (matching the crawlee `str | None` contract). For the bare-file
    /// (`INPUT` -> `INPUT.json`) case the caller resolves the on-disk key via
    /// `resolve_existing_key` first and passes that key here.
    #[gen_stub(override_return_type(type_repr = "builtins.str | None"))]
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

    /// Check whether a tracked record (value file + metadata sidecar) exists for
    /// `key`. To also match out-of-band files with no sidecar, use
    /// `resolve_existing_key`, which probes the conventional bare-file extensions.
    #[gen_stub(override_return_type(type_repr = "builtins.bool"))]
    fn record_exists<'py>(
        &self,
        py: Python<'py>,
        key: String,
    ) -> PyResult<Bound<'py, pyo3::PyAny>> {
        let client = self.inner.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            Ok(client.record_exists(&key, true).await)
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
    /// ``request_queue_access`` (default ``"single"``): how the on-disk queue
    /// is expected to be accessed. With ``"single"`` (the default, tuned for
    /// the common single-process crawl), the caller asserts nothing else is
    /// using this queue and any in-progress locks are reclaimed immediately,
    /// so a request whose previous run died is instantly re-fetchable. Use
    /// ``"shared"`` when multiple processes share the same on-disk queue
    /// concurrently: any future-dated ``orderNo`` is then respected as a
    /// potential live peer's lock, and crashed peers' locks expire naturally
    /// on the wall clock — otherwise you risk two peers processing the same
    /// request.
    #[staticmethod]
    #[pyo3(signature = (id=None, name=None, alias=None, storage_dir="./storage", use_test_clock=false, request_queue_access="single"))]
    #[gen_stub(override_return_type(type_repr = "FileSystemRequestQueueClient"))]
    fn open<'py>(
        py: Python<'py>,
        id: Option<String>,
        name: Option<String>,
        alias: Option<String>,
        storage_dir: &str,
        use_test_clock: bool,
        #[gen_stub(override_type(type_repr = "typing.Literal['single', 'shared']", imports = ("typing")))]
        request_queue_access: &str,
    ) -> PyResult<Bound<'py, pyo3::PyAny>> {
        let storage_dir = PathBuf::from(storage_dir);
        let assume_sole_owner = match request_queue_access {
            "single" => true,
            "shared" => false,
            other => {
                return Err(pyo3::exceptions::PyValueError::new_err(format!(
                    "request_queue_access must be 'single' or 'shared', got '{other}'"
                )))
            }
        };
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
        advance_test_clock(&self.test_clock, duration)
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
            Python::attach(|py| models::RequestQueueMetadata(&meta).to_py(py))
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
        // The core takes a `chrono::Duration` directly (the unit lives there),
        // so the `timedelta` passes straight through.
        let client = self.inner.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            client.set_expected_request_processing_time(duration).await;
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
    // The content-type sentinel for null KVS values (empty file on disk).
    // Exported so consumers reference the shared constant from the core crate
    // instead of hardcoding the `application/x-none` literal.
    m.add("NONE_CONTENT_TYPE", crawlee_storage::NONE_CONTENT_TYPE)?;
    Ok(())
}

// Re-export all classes from _native into the top-level crawlee_storage module,
// mirroring what python/crawlee_storage/__init__.py does.
pyo3_stub_gen::reexport_module_members!("crawlee_storage", "crawlee_storage._native");

define_stub_info_gatherer!(stub_info);
