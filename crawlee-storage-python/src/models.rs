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
use pyo3_stub_gen::inventory;

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

/// One registered `TypedDict`, collected via `inventory` so the stub generator
/// can discover every model without a hand-maintained list. Every
/// `typed_dict_model!` / `typed_dict_spec!` invocation submits one of these, so
/// adding a model can no longer silently skip the `.pyi` (the old failure mode
/// of forgetting to append it to `all_specs()`).
pub struct RegisteredTypedDict {
    pub name: &'static str,
    pub spec: &'static [TypedDictField],
}

inventory::collect!(RegisteredTypedDict);

/// Shorthand for a `TypedDictField` literal.
const fn f(key: &'static str, py_type: &'static str) -> TypedDictField {
    TypedDictField { key, py_type }
}

/// Define a builder-backed `TypedDict` model: a newtype wrapper over a core
/// type, its `TypedDictModel` (`NAME` + `SPEC`) impl, and the matching `to_py`
/// dict builder — all from one declaration, so the stub's claimed shape and the
/// dict's actual keys can no longer drift apart (they're generated from the
/// same field list, in the same order).
///
/// Each field is written once as:
///
/// ```ignore
/// "camelCaseKey": "python.type" => |this, py| value_expression,
/// ```
///
/// where `this` is `&self.0` (the borrowed core value) and `py` is the
/// `Python<'_>` token; the closure body is the expression handed to
/// `dict.set_item("camelCaseKey", ...)`. An optional leading `@base($field)`
/// token splices in the five shared base-metadata fields (`id`/`name`/
/// `accessedAt`/`createdAt`/`modifiedAt`) — both in the `SPEC` and via
/// `set_base_metadata_fields(&self.0.$field)` — ahead of the per-field entries.
/// Submit one `TypedDictModel` impl to the `inventory` registry so `all_specs()`
/// (and thus the stub) picks it up automatically. `$wrapper` is the impl type;
/// `$($lt)?` lets it work for both the lifetime-carrying builder wrappers and
/// the bare serde-only unit structs.
macro_rules! register_typed_dict {
    ($wrapper:ident $(<$lt:lifetime>)?) => {
        inventory::submit! {
            RegisteredTypedDict {
                name: <$wrapper $(<$lt>)?as TypedDictModel>::NAME,
                spec: <$wrapper $(<$lt>)? as TypedDictModel>::SPEC,
            }
        }
    };
}

macro_rules! typed_dict_model {
    // With the shared base-metadata block spliced in first. `$base` is the
    // field on the core type holding the `StorageMetadata` (always `base`).
    (
        $wrapper:ident<$lt:lifetime>($core:path),
        $name:literal,
        @base($base:ident),
        { $( $key:literal : $ty:literal => |$this:ident, $py:ident| $val:expr ),* $(,)? }
    ) => {
        pub struct $wrapper<$lt>(pub &$lt $core);

        impl TypedDictModel for $wrapper<'_> {
            const NAME: &'static str = $name;
            const SPEC: &'static [TypedDictField] = &[
                BASE_META_FIELDS[0],
                BASE_META_FIELDS[1],
                BASE_META_FIELDS[2],
                BASE_META_FIELDS[3],
                BASE_META_FIELDS[4],
                $( f($key, $ty), )*
            ];
        }

        register_typed_dict!($wrapper<'static>);

        impl $wrapper<'_> {
            pub fn to_py(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
                let dict = PyDict::new(py);
                set_base_metadata_fields(&dict, &self.0.$base)?;
                $(
                    {
                        let $this = self.0;
                        let $py = py;
                        dict.set_item($key, $val)?;
                    }
                )*
                Ok(dict.into_any().unbind())
            }
        }
    };

    // No base-metadata block: every field is listed explicitly.
    (
        $wrapper:ident<$lt:lifetime>($core:path),
        $name:literal,
        { $( $key:literal : $ty:literal => |$this:ident, $py:ident| $val:expr ),* $(,)? }
    ) => {
        pub struct $wrapper<$lt>(pub &$lt $core);

        impl TypedDictModel for $wrapper<'_> {
            const NAME: &'static str = $name;
            const SPEC: &'static [TypedDictField] = &[
                $( f($key, $ty), )*
            ];
        }

        register_typed_dict!($wrapper<'static>);

        impl $wrapper<'_> {
            pub fn to_py(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
                let dict = PyDict::new(py);
                $(
                    {
                        let $this = self.0;
                        let $py = py;
                        dict.set_item($key, $val)?;
                    }
                )*
                Ok(dict.into_any().unbind())
            }
        }
    };
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

typed_dict_model! {
    DatasetMetadata<'a>(models::DatasetMetadata),
    "DatasetMetadata",
    @base(base),
    {
        "itemCount": "builtins.int" => |this, _py| this.item_count,
    }
}

// ─── Key-value store metadata ───────────────────────────────────────────────

typed_dict_model! {
    KeyValueStoreMetadata<'a>(models::KeyValueStoreMetadata),
    "KeyValueStoreMetadata",
    @base(base),
    {}
}

// ─── Request queue metadata ─────────────────────────────────────────────────

typed_dict_model! {
    RequestQueueMetadata<'a>(models::RequestQueueMetadata),
    "RequestQueueMetadata",
    @base(base),
    {
        "hadMultipleClients": "builtins.bool" => |this, _py| this.had_multiple_clients,
        "handledRequestCount": "builtins.int" => |this, _py| this.handled_request_count,
        "pendingRequestCount": "builtins.int" => |this, _py| this.pending_request_count,
        "totalRequestCount": "builtins.int" => |this, _py| this.total_request_count,
    }
}

// ─── KVS record metadata (yielded by the key iterator) ──────────────────────

typed_dict_model! {
    KeyValueStoreRecordMetadata<'a>(models::KeyValueStoreRecordMetadata),
    "KeyValueStoreRecordMetadata",
    {
        "key": "builtins.str" => |this, _py| &this.key,
        "contentType": "builtins.str" => |this, _py| &this.content_type,
        // The core backfills a missing `size` from the value-file length on
        // read, so it is always populated by the time it reaches Python.
        "size": "builtins.int" => |this, _py| this.size.unwrap_or(0),
    }
}

// ─── KVS record (full value) ────────────────────────────────────────────────

typed_dict_model! {
    KeyValueStoreRecord<'a>(models::KeyValueStoreRecord),
    "KeyValueStoreRecord",
    {
        "key": "builtins.str" => |this, _py| &this.key,
        "contentType": "builtins.str" => |this, _py| &this.content_type,
        "size": "builtins.int" => |this, _py| this.size,
        "value": "builtins.bytes" => |this, py| PyBytes::new(py, &this.value),
    }
}

/// Define a **spec-only** `TypedDict` model: a `TypedDictModel` impl plus its
/// `inventory` registration, with no wrapper struct and no `to_py` builder.
///
/// These describe payloads whose dicts are built by `serde_to_py` (the values
/// carry no datetime fields, so a serde round-trip is lossless), so there's
/// nothing to drift against on the value side — only the `SPEC` matters. The
/// `=> $core` clause names the core type the spec mirrors and emits a
/// compile-time guard binding (`let _: &core::Type`), so renaming or removing
/// that core type fails the build — a nudge to re-check the field list. (It
/// still can't verify the *field names* against the core struct — those are
/// strings — but it keeps the link from going stale silently, replacing the
/// old free-floating `_core_type_guard` fn.)
macro_rules! typed_dict_spec {
    (
        $tyname:ident => $core:path,
        $name:literal,
        [ $( $key:literal : $ty:literal ),* $(,)? ]
    ) => {
        pub struct $tyname;

        impl TypedDictModel for $tyname {
            const NAME: &'static str = $name;
            const SPEC: &'static [TypedDictField] = &[
                $( f($key, $ty), )*
            ];
        }

        register_typed_dict!($tyname);

        // Anchor the spec to the core type it mirrors: if `$core` is renamed or
        // removed, this const-eval fails to compile.
        const _: fn(&$core) = |_| {};
    };
}

// ─── Dataset items list page ────────────────────────────────────────────────
//
// `count`/`offset`/`limit`/`total` and `items` are produced by `get_data`; the
// `items` are arbitrary user JSON (`dict[str, Any]`). The page's dict is built
// by the caller via `serde_to_py` (the values carry no datetime fields), so
// this model contributes only its `SPEC` to the stub.

typed_dict_spec! {
    DatasetItemsListPage => models::DatasetItemsListPage,
    "DatasetItemsListPage",
    [
        "count": "builtins.int",
        "offset": "builtins.int",
        "limit": "builtins.int",
        "total": "builtins.int",
        "desc": "builtins.bool",
        "items": "builtins.list[dict[builtins.str, typing.Any]]",
    ]
}

// ─── Request queue operation results ────────────────────────────────────────

typed_dict_spec! {
    ProcessedRequest => models::ProcessedRequest,
    "ProcessedRequest",
    [
        "requestId": "builtins.str",
        "uniqueKey": "builtins.str",
        "wasAlreadyPresent": "builtins.bool",
        "wasAlreadyHandled": "builtins.bool",
    ]
}

typed_dict_spec! {
    UnprocessedRequest => models::UnprocessedRequest,
    "UnprocessedRequest",
    [
        "uniqueKey": "builtins.str",
        "url": "builtins.str",
        "method": "builtins.str | None",
    ]
}

typed_dict_spec! {
    AddRequestsResponse => models::AddRequestsResponse,
    "AddRequestsResponse",
    [
        "processedRequests": "builtins.list[ProcessedRequest]",
        "unprocessedRequests": "builtins.list[UnprocessedRequest]",
    ]
}

/// All registered TypedDict specs, sorted by name for a deterministic,
/// diff-stable emission order. Collected from the `inventory` registry that
/// every `typed_dict_model!` / `typed_dict_spec!` invocation submits to — so
/// adding a model wires it into the stub automatically, with no hand-maintained
/// list to forget to update. (Forward references in the generated `.pyi` are
/// fine, so name order — rather than a curated dependency order — is enough.)
pub fn all_specs() -> Vec<(&'static str, &'static [TypedDictField])> {
    let mut specs: Vec<(&'static str, &'static [TypedDictField])> =
        inventory::iter::<RegisteredTypedDict>()
            .map(|r| (r.name, r.spec))
            .collect();
    specs.sort_unstable_by_key(|(name, _)| *name);
    specs
}
