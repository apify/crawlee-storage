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

/// A KVS record returned by `getValue`: the raw value bytes as a `Buffer`
/// alongside the key, content type, and size. Replaces the hand-written
/// `KeyValueStoreRecord` interface that used to live in `dts-header.d.ts` —
/// the `value: Buffer` now crosses the FFI directly instead of as a per-byte
/// JSON number array.
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
