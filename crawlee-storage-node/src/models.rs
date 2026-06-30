//! JS-side mirror structs for the core library's metadata types.
//!
//! These exist purely so the napi-rs code generator emits proper TypeScript
//! interfaces with `Date` (not `string`) for datetime fields. We can't derive
//! `#[napi(object)]` on the core types directly because that crate is
//! binding-agnostic. The mirrors convert from `&crawlee_storage::models::…`
//! via `From` impls — chrono's `DateTime<Utc>` crosses napi as a real JS
//! `Date` thanks to the `chrono_date` feature, and the other fields are
//! passed through as-is.

use chrono::{DateTime, Utc};
use napi::bindgen_prelude::Buffer;
use napi_derive::napi;
use serde_json::Value;

/// A KVS record returned by `getValue`: the raw value bytes as a `Buffer`
/// alongside the key, content type, and size. The `value: Buffer` crosses the
/// FFI directly instead of as a per-byte JSON number array.
#[napi(object)]
pub struct KeyValueStoreRecord {
    pub key: String,
    pub content_type: String,
    /// Byte length of the value. Typed `f64` so it crosses the FFI as a JS
    /// `number` (matching crawlee's `KeyValueStoreItemData.size: number`)
    /// without the 4 GiB ceiling a `u32` would impose; a `usize` length is
    /// exact in an `f64` up to 2^53 bytes (8 PiB), well beyond any KVS record.
    pub size: f64,
    pub value: Buffer,
}

impl From<crawlee_storage::models::KeyValueStoreRecord> for KeyValueStoreRecord {
    fn from(r: crawlee_storage::models::KeyValueStoreRecord) -> Self {
        Self {
            key: r.key,
            content_type: r.content_type,
            // The core finalizes `size` to a non-optional `usize`; widen to
            // `f64` for the FFI (exact up to 2^53 bytes).
            size: r.size as f64,
            value: Buffer::from(r.value),
        }
    }
}

/// Metadata for a single KVS record, as yielded by the key iterator
/// (`KvsKeyIterator.next`) and `iterateKeys`. Mirrors the core's
/// `KeyValueStoreRecordMetadata` with `size` finalized to a non-optional value.
#[napi(object)]
pub struct KeyValueStoreRecordMetadata {
    pub key: String,
    pub content_type: String,
    /// Byte length of the value. Typed `f64` (same rationale as
    /// `KeyValueStoreRecord::size`): exact up to 2^53 bytes, no 4 GiB ceiling.
    pub size: f64,
}

impl From<crawlee_storage::models::KeyValueStoreRecordMetadata> for KeyValueStoreRecordMetadata {
    fn from(m: crawlee_storage::models::KeyValueStoreRecordMetadata) -> Self {
        Self {
            key: m.key,
            content_type: m.content_type,
            // The core backfills a missing `size` from the value-file length on
            // read, so by the time it reaches here it is always populated.
            size: m.size.unwrap_or(0) as f64,
        }
    }
}

/// One out-of-band ("bare") file fallback for `resolveValue` / `resolveExistingKey`:
/// an extension appended to the looked-up key, plus the content type to report
/// when a bare file with that extension is matched. An empty `contentType`
/// leaves the matched file's synthesized `application/octet-stream` in place.
///
/// The core does no MIME inference of its own — the caller declares this
/// extension→content-type policy (e.g. `.json` → `application/json`).
#[napi(object)]
pub struct BareFallback {
    pub extension: String,
    pub content_type: String,
}

/// One out-of-band ("bare") file to surface from `iterateKeys`: `name` is the
/// file's on-disk key (e.g. `"INPUT.json"`) and `contentType` is what to report
/// for it (an empty string reports the synthesized `application/octet-stream`).
/// A bare file whose on-disk name already has a tracked record is not listed
/// twice.
///
/// Round-trip caveat: the file is listed under the literal `name`, but that key
/// does NOT round-trip through `getValue` / `recordExists` (which ignore
/// sidecar-less files and return `null` / `false`). Read a listed bare key back
/// via `resolveValue` / `resolveExistingKey` instead.
///
/// As with `resolveValue`, the core does no MIME inference — the caller declares
/// this name→content-type policy.
#[napi(object)]
pub struct ListBareFallback {
    pub name: String,
    pub content_type: String,
}

#[napi(object)]
pub struct DatasetMetadata {
    pub id: String,
    pub name: Option<String>,
    pub accessed_at: DateTime<Utc>,
    pub created_at: DateTime<Utc>,
    pub modified_at: DateTime<Utc>,
    pub item_count: u32,
}

impl From<&crawlee_storage::models::DatasetMetadata> for DatasetMetadata {
    fn from(m: &crawlee_storage::models::DatasetMetadata) -> Self {
        Self {
            id: m.base.id.clone(),
            name: m.base.name.clone(),
            accessed_at: m.base.accessed_at,
            created_at: m.base.created_at,
            modified_at: m.base.modified_at,
            item_count: m.item_count as u32,
        }
    }
}

#[napi(object)]
pub struct KeyValueStoreMetadata {
    pub id: String,
    pub name: Option<String>,
    pub accessed_at: DateTime<Utc>,
    pub created_at: DateTime<Utc>,
    pub modified_at: DateTime<Utc>,
}

impl From<&crawlee_storage::models::KeyValueStoreMetadata> for KeyValueStoreMetadata {
    fn from(m: &crawlee_storage::models::KeyValueStoreMetadata) -> Self {
        Self {
            id: m.base.id.clone(),
            name: m.base.name.clone(),
            accessed_at: m.base.accessed_at,
            created_at: m.base.created_at,
            modified_at: m.base.modified_at,
        }
    }
}

#[napi(object)]
pub struct RequestQueueMetadata {
    pub id: String,
    pub name: Option<String>,
    pub accessed_at: DateTime<Utc>,
    pub created_at: DateTime<Utc>,
    pub modified_at: DateTime<Utc>,
    pub had_multiple_clients: bool,
    pub handled_request_count: u32,
    pub pending_request_count: u32,
    pub total_request_count: u32,
}

impl From<&crawlee_storage::models::RequestQueueMetadata> for RequestQueueMetadata {
    fn from(m: &crawlee_storage::models::RequestQueueMetadata) -> Self {
        Self {
            id: m.base.id.clone(),
            name: m.base.name.clone(),
            accessed_at: m.base.accessed_at,
            created_at: m.base.created_at,
            modified_at: m.base.modified_at,
            had_multiple_clients: m.had_multiple_clients,
            handled_request_count: m.handled_request_count as u32,
            pending_request_count: m.pending_request_count as u32,
            total_request_count: m.total_request_count as u32,
        }
    }
}

/// A page of dataset items returned by `getData`. Mirrors the core's
/// `DatasetItemsListPage` so napi emits an honest TS interface instead of a
/// hand-written one. The `items` are arbitrary user JSON, so they cross the
/// FFI as `serde_json::Value` and are typed `Record<string, unknown>[]`.
#[napi(object)]
pub struct DatasetItemsListPage {
    pub count: u32,
    pub offset: u32,
    pub limit: u32,
    pub total: u32,
    pub desc: bool,
    #[napi(ts_type = "Record<string, unknown>[]")]
    pub items: Vec<Value>,
}

impl From<crawlee_storage::models::DatasetItemsListPage> for DatasetItemsListPage {
    fn from(p: crawlee_storage::models::DatasetItemsListPage) -> Self {
        Self {
            count: p.count as u32,
            offset: p.offset as u32,
            limit: p.limit as u32,
            total: p.total as u32,
            desc: p.desc,
            items: p.items,
        }
    }
}

/// Result of processing one request in `addBatchOfRequests`. Mirrors the core's
/// `ProcessedRequest`.
#[napi(object)]
pub struct ProcessedRequest {
    pub request_id: String,
    pub unique_key: String,
    pub was_already_present: bool,
    pub was_already_handled: bool,
}

impl From<crawlee_storage::models::ProcessedRequest> for ProcessedRequest {
    fn from(r: crawlee_storage::models::ProcessedRequest) -> Self {
        Self {
            request_id: r.request_id,
            unique_key: r.unique_key,
            was_already_present: r.was_already_present,
            was_already_handled: r.was_already_handled,
        }
    }
}

/// A request that could not be processed in `addBatchOfRequests`. Mirrors the
/// core's `UnprocessedRequest`. `method` is optional; omitting `use_nullable`
/// keeps it as `method?: string` rather than `method: string | null`.
#[napi(object)]
pub struct UnprocessedRequest {
    pub unique_key: String,
    pub url: String,
    pub method: Option<String>,
}

impl From<crawlee_storage::models::UnprocessedRequest> for UnprocessedRequest {
    fn from(r: crawlee_storage::models::UnprocessedRequest) -> Self {
        Self {
            unique_key: r.unique_key,
            url: r.url,
            method: r.method,
        }
    }
}

/// Response from `addBatchOfRequests`. Mirrors the core's `AddRequestsResponse`.
#[napi(object)]
pub struct AddRequestsResponse {
    pub processed_requests: Vec<ProcessedRequest>,
    pub unprocessed_requests: Vec<UnprocessedRequest>,
}

impl From<crawlee_storage::models::AddRequestsResponse> for AddRequestsResponse {
    fn from(r: crawlee_storage::models::AddRequestsResponse) -> Self {
        Self {
            processed_requests: r.processed_requests.into_iter().map(Into::into).collect(),
            unprocessed_requests: r.unprocessed_requests.into_iter().map(Into::into).collect(),
        }
    }
}
