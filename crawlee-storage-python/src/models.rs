//! Python-side mirror structs for the core library's structured payloads.
//!
//! # Why these exist
//!
//! The Python bindings hand callers **camelCase dicts** (matching the on-disk
//! JSON and what crawlee's Pydantic models consume via aliases), not attribute
//! objects. `pyo3-stub-gen` therefore can't infer the shape of a return value —
//! a method that returns a `dict` is just `dict[str, Any]` to it. The `.pyi`
//! stubs need real `TypedDict` definitions, and historically those were produced
//! by serializing a dummy instance and *guessing* the Python type from the
//! resulting JSON (lossy: `Option<T>` → `null` → `Any`; `DateTime` → string),
//! patched up by a hand-maintained `FIELD_OVERRIDES` table.
//!
//! These mirror structs replace that guesswork with a **single, rustc-checked
//! source of truth**. Each struct:
//!
//! 1. has explicitly-typed fields plus a `From<&core::Type>` conversion, so if
//!    a core model changes, the conversion stops compiling;
//! 2. declares its `TypedDict` field list (`SPEC`) co-located with the struct —
//!    the stub generator prints straight from this, no JSON round-trip;
//! 3. builds the actual Python dict in `to_py`, using the *same* key names.
//!
//! The `SPEC` key list and the `to_py` builder sit side by side, so a drift
//! between "what the stub claims" and "what the dict contains" is visible in one
//! place (and the field *values* are rustc-checked via the `From` impls).

use crawlee_storage::models;
use pyo3::prelude::*;
use pyo3::types::{PyBytes, PyDict};

/// One field of a generated `TypedDict`: the camelCase JSON key as handed to
/// Python, and the Python type annotation string the stub should emit.
#[derive(Clone, Copy)]
pub struct TypedDictField {
    pub key: &'static str,
    pub py_type: &'static str,
}

/// A payload type that is surfaced to Python as a `TypedDict`.
///
/// `NAME` is the Python class name; `SPEC` is the ordered field list the stub
/// generator prints. Implementors also provide `to_py` (the dict builder) — the
/// two are kept in the same `impl` block so they're edited together.
pub trait TypedDictModel {
    const NAME: &'static str;
    const SPEC: &'static [TypedDictField];
}

/// Shorthand for a `TypedDictField` literal.
const fn f(key: &'static str, py_type: &'static str) -> TypedDictField {
    TypedDictField { key, py_type }
}

// The five base-metadata fields shared by every storage metadata TypedDict.
// Datetimes are declared `datetime.datetime` here (the binding converts the
// core's `DateTime<Utc>` to a native tz-aware datetime at the boundary) — the
// one place the old generator needed a per-field override.
const BASE_META_FIELDS: [TypedDictField; 5] = [
    f("id", "builtins.str"),
    f("name", "builtins.str | None"),
    f("accessedAt", "datetime.datetime"),
    f("createdAt", "datetime.datetime"),
    f("modifiedAt", "datetime.datetime"),
];

/// Set the five shared base-metadata fields on `dict`. The datetime fields
/// cross the FFI as native `datetime.datetime` (tz-aware UTC) via PyO3's
/// `chrono` feature — this is what makes the `datetime.datetime` annotation in
/// `BASE_META_FIELDS` honest.
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

// ─── Dataset metadata ───────────────────────────────────────────────────────

pub struct DatasetMetadata<'a>(pub &'a models::DatasetMetadata);

impl TypedDictModel for DatasetMetadata<'_> {
    const NAME: &'static str = "DatasetMetadata";
    const SPEC: &'static [TypedDictField] = &[
        BASE_META_FIELDS[0],
        BASE_META_FIELDS[1],
        BASE_META_FIELDS[2],
        BASE_META_FIELDS[3],
        BASE_META_FIELDS[4],
        f("itemCount", "builtins.int"),
    ];
}

impl DatasetMetadata<'_> {
    pub fn to_py(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        let dict = PyDict::new(py);
        set_base_metadata_fields(&dict, &self.0.base)?;
        dict.set_item("itemCount", self.0.item_count)?;
        Ok(dict.into_any().unbind())
    }
}

// ─── Key-value store metadata ───────────────────────────────────────────────

pub struct KeyValueStoreMetadata<'a>(pub &'a models::KeyValueStoreMetadata);

impl TypedDictModel for KeyValueStoreMetadata<'_> {
    const NAME: &'static str = "KeyValueStoreMetadata";
    const SPEC: &'static [TypedDictField] = &BASE_META_FIELDS;
}

impl KeyValueStoreMetadata<'_> {
    pub fn to_py(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        let dict = PyDict::new(py);
        set_base_metadata_fields(&dict, &self.0.base)?;
        Ok(dict.into_any().unbind())
    }
}

// ─── Request queue metadata ─────────────────────────────────────────────────

pub struct RequestQueueMetadata<'a>(pub &'a models::RequestQueueMetadata);

impl TypedDictModel for RequestQueueMetadata<'_> {
    const NAME: &'static str = "RequestQueueMetadata";
    const SPEC: &'static [TypedDictField] = &[
        BASE_META_FIELDS[0],
        BASE_META_FIELDS[1],
        BASE_META_FIELDS[2],
        BASE_META_FIELDS[3],
        BASE_META_FIELDS[4],
        f("hadMultipleClients", "builtins.bool"),
        f("handledRequestCount", "builtins.int"),
        f("pendingRequestCount", "builtins.int"),
        f("totalRequestCount", "builtins.int"),
    ];
}

impl RequestQueueMetadata<'_> {
    pub fn to_py(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        let dict = PyDict::new(py);
        set_base_metadata_fields(&dict, &self.0.base)?;
        dict.set_item("hadMultipleClients", self.0.had_multiple_clients)?;
        dict.set_item("handledRequestCount", self.0.handled_request_count)?;
        dict.set_item("pendingRequestCount", self.0.pending_request_count)?;
        dict.set_item("totalRequestCount", self.0.total_request_count)?;
        Ok(dict.into_any().unbind())
    }
}

// ─── KVS record metadata (yielded by the key iterator) ──────────────────────

pub struct KeyValueStoreRecordMetadata<'a>(pub &'a models::KeyValueStoreRecordMetadata);

impl TypedDictModel for KeyValueStoreRecordMetadata<'_> {
    const NAME: &'static str = "KeyValueStoreRecordMetadata";
    const SPEC: &'static [TypedDictField] = &[
        f("key", "builtins.str"),
        f("contentType", "builtins.str"),
        // The core backfills a missing `size` from the value-file length on
        // read, so it is always populated by the time it reaches Python.
        f("size", "builtins.int"),
    ];
}

impl KeyValueStoreRecordMetadata<'_> {
    pub fn to_py(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        let dict = PyDict::new(py);
        dict.set_item("key", &self.0.key)?;
        dict.set_item("contentType", &self.0.content_type)?;
        dict.set_item("size", self.0.size.unwrap_or(0))?;
        Ok(dict.into_any().unbind())
    }
}

// ─── KVS record (full value) ────────────────────────────────────────────────

pub struct KeyValueStoreRecord<'a>(pub &'a models::KeyValueStoreRecord);

impl TypedDictModel for KeyValueStoreRecord<'_> {
    const NAME: &'static str = "KeyValueStoreRecord";
    const SPEC: &'static [TypedDictField] = &[
        f("key", "builtins.str"),
        f("contentType", "builtins.str"),
        f("size", "builtins.int"),
        f("value", "builtins.bytes"),
    ];
}

impl KeyValueStoreRecord<'_> {
    pub fn to_py(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        let dict = PyDict::new(py);
        dict.set_item("key", &self.0.key)?;
        dict.set_item("contentType", &self.0.content_type)?;
        dict.set_item("size", self.0.size)?;
        dict.set_item("value", PyBytes::new(py, &self.0.value))?;
        Ok(dict.into_any().unbind())
    }
}

// ─── Dataset items list page ────────────────────────────────────────────────
//
// `count`/`offset`/`limit`/`total` and `items` are produced by `get_data`; the
// `items` are arbitrary user JSON (`dict[str, Any]`). The page's dict is built
// by the caller via `serde_to_py` (the values carry no datetime fields), so
// this model contributes only its `SPEC` to the stub.

pub struct DatasetItemsListPage;

impl TypedDictModel for DatasetItemsListPage {
    const NAME: &'static str = "DatasetItemsListPage";
    const SPEC: &'static [TypedDictField] = &[
        f("count", "builtins.int"),
        f("offset", "builtins.int"),
        f("limit", "builtins.int"),
        f("total", "builtins.int"),
        f("desc", "builtins.bool"),
        f("items", "builtins.list[dict[builtins.str, typing.Any]]"),
    ];
}

// ─── Request queue operation results ────────────────────────────────────────

pub struct ProcessedRequest;

impl TypedDictModel for ProcessedRequest {
    const NAME: &'static str = "ProcessedRequest";
    const SPEC: &'static [TypedDictField] = &[
        f("requestId", "builtins.str"),
        f("uniqueKey", "builtins.str"),
        f("wasAlreadyPresent", "builtins.bool"),
        f("wasAlreadyHandled", "builtins.bool"),
    ];
}

pub struct UnprocessedRequest;

impl TypedDictModel for UnprocessedRequest {
    const NAME: &'static str = "UnprocessedRequest";
    const SPEC: &'static [TypedDictField] = &[
        f("uniqueKey", "builtins.str"),
        f("url", "builtins.str"),
        f("method", "builtins.str | None"),
    ];
}

pub struct AddRequestsResponse;

impl TypedDictModel for AddRequestsResponse {
    const NAME: &'static str = "AddRequestsResponse";
    const SPEC: &'static [TypedDictField] = &[
        f("processedRequests", "builtins.list[ProcessedRequest]"),
        f("unprocessedRequests", "builtins.list[UnprocessedRequest]"),
    ];
}

/// Compile-time guard tying the unit-struct specs (whose payloads are built by
/// `serde_to_py`, so they have no `From` impl above to anchor them) to the core
/// types they describe. If a referenced core type is renamed or removed, this
/// fails to build — a nudge to re-check the corresponding `SPEC`. It can't
/// verify the *field names* match (those are strings), but it keeps the link
/// from going stale silently.
#[allow(dead_code)]
fn _core_type_guard(
    _a: &models::DatasetItemsListPage,
    _b: &models::ProcessedRequest,
    _c: &models::UnprocessedRequest,
    _d: &models::AddRequestsResponse,
) {
}

/// All TypedDict specs, in stub emission order (dependencies — `ProcessedRequest`,
/// `UnprocessedRequest` — appear before `AddRequestsResponse` consumers, though
/// forward references in `.pyi` are fine regardless). This single list drives
/// both the class-body generation and the `__all__` additions, so the names
/// can't fall out of sync.
pub fn all_specs() -> Vec<(&'static str, &'static [TypedDictField])> {
    vec![
        (DatasetMetadata::NAME, DatasetMetadata::SPEC),
        (KeyValueStoreMetadata::NAME, KeyValueStoreMetadata::SPEC),
        (
            KeyValueStoreRecordMetadata::NAME,
            KeyValueStoreRecordMetadata::SPEC,
        ),
        (KeyValueStoreRecord::NAME, KeyValueStoreRecord::SPEC),
        (RequestQueueMetadata::NAME, RequestQueueMetadata::SPEC),
        (DatasetItemsListPage::NAME, DatasetItemsListPage::SPEC),
        (ProcessedRequest::NAME, ProcessedRequest::SPEC),
        (UnprocessedRequest::NAME, UnprocessedRequest::SPEC),
        (AddRequestsResponse::NAME, AddRequestsResponse::SPEC),
    ]
}
