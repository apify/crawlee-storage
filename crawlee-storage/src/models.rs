use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::Value;

/// Format string for datetime serialization.
/// Produces 6 fractional digits (microsecond precision) with `+00:00` UTC suffix.
/// Example: `2024-01-15T10:30:00.123456+00:00`
const DATETIME_FORMAT: &str = "%Y-%m-%dT%H:%M:%S%.6f";

pub(crate) fn serialize_datetime<S>(dt: &DateTime<Utc>, serializer: S) -> Result<S::Ok, S::Error>
where
    S: serde::Serializer,
{
    let formatted = dt.format(DATETIME_FORMAT).to_string();
    serializer.serialize_str(&format!("{formatted}+00:00"))
}

pub(crate) fn deserialize_datetime<'de, D>(deserializer: D) -> Result<DateTime<Utc>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    let s = String::deserialize(deserializer)?;
    // Try parsing various formats:
    // 1. With timezone offset: "2024-01-15T10:30:00.123456+00:00"
    // 2. With Z suffix (JS format): "2024-01-15T10:30:00.123Z"
    // 3. Without timezone: "2024-01-15T10:30:00.123456"
    DateTime::parse_from_str(&s, "%Y-%m-%dT%H:%M:%S%.f%:z")
        .map(|dt| dt.with_timezone(&Utc))
        .or_else(|_| {
            // "2024-01-15T10:30:00.123Z" — chrono's parse_from_rfc3339 handles this
            DateTime::parse_from_rfc3339(&s).map(|dt| dt.with_timezone(&Utc))
        })
        .or_else(|_| {
            // Fallback: no timezone info, assume UTC
            chrono::NaiveDateTime::parse_from_str(&s, "%Y-%m-%dT%H:%M:%S%.f")
                .map(|ndt| ndt.and_utc())
        })
        .map_err(serde::de::Error::custom)
}

fn datetime_now() -> DateTime<Utc> {
    Utc::now()
}

// ─── Storage Metadata (base) ────────────────────────────────────────────────

/// Base metadata shared by all storage types.
///
/// On-disk JSON uses camelCase field names (e.g. `accessedAt`, `createdAt`, `modifiedAt`).
/// Snake_case aliases are accepted on deserialization for backward compatibility
/// with files written by the old Python `FileSystemStorageClient`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StorageMetadata {
    pub id: String,
    #[serde(default)]
    pub name: Option<String>,
    #[serde(
        rename = "accessedAt",
        alias = "accessed_at",
        serialize_with = "serialize_datetime",
        deserialize_with = "deserialize_datetime"
    )]
    pub accessed_at: DateTime<Utc>,
    #[serde(
        rename = "createdAt",
        alias = "created_at",
        serialize_with = "serialize_datetime",
        deserialize_with = "deserialize_datetime"
    )]
    pub created_at: DateTime<Utc>,
    #[serde(
        rename = "modifiedAt",
        alias = "modified_at",
        serialize_with = "serialize_datetime",
        deserialize_with = "deserialize_datetime"
    )]
    pub modified_at: DateTime<Utc>,
}

impl StorageMetadata {
    pub fn new(id: String, name: Option<String>) -> Self {
        let now = datetime_now();
        Self {
            id,
            name,
            accessed_at: now,
            created_at: now,
            modified_at: now,
        }
    }
}

// ─── Dataset Metadata ───────────────────────────────────────────────────────

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DatasetMetadata {
    #[serde(flatten)]
    pub base: StorageMetadata,
    #[serde(rename = "itemCount", alias = "item_count")]
    pub item_count: usize,
}

impl DatasetMetadata {
    pub fn new(id: String, name: Option<String>) -> Self {
        Self {
            base: StorageMetadata::new(id, name),
            item_count: 0,
        }
    }
}

// ─── Key-Value Store Metadata ───────────────────────────────────────────────

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct KeyValueStoreMetadata {
    #[serde(flatten)]
    pub base: StorageMetadata,
}

impl KeyValueStoreMetadata {
    pub fn new(id: String, name: Option<String>) -> Self {
        Self {
            base: StorageMetadata::new(id, name),
        }
    }
}

// ─── Key-Value Store Record ─────────────────────────────────────────────────

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct KeyValueStoreRecordMetadata {
    pub key: String,
    #[serde(rename = "contentType", alias = "content_type")]
    pub content_type: String,
    #[serde(default)]
    pub size: Option<usize>,
}

/// The value stored in a key-value store record.
///
/// This avoids base64-encoding binary data just to fit it into `serde_json::Value`.
/// Each binding layer converts these variants to native types directly
/// (e.g. `Binary` → Python `bytes`, Node.js `Buffer`).
#[derive(Debug, Clone)]
pub enum KvsValue {
    /// `application/x-none` — the record represents a null/None value.
    None,
    /// `application/json` — arbitrary JSON.
    Json(Value),
    /// `text/*` — a UTF-8 string.
    Text(String),
    /// Any other content type — raw bytes, no encoding.
    Binary(Vec<u8>),
}

pub struct KeyValueStoreRecord {
    pub key: String,
    pub content_type: String,
    pub size: Option<usize>,
    pub value: KvsValue,
}

// ─── Request Queue Metadata ─────────────────────────────────────────────────

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RequestQueueMetadata {
    #[serde(flatten)]
    pub base: StorageMetadata,
    #[serde(rename = "hadMultipleClients", alias = "had_multiple_clients")]
    pub had_multiple_clients: bool,
    #[serde(rename = "handledRequestCount", alias = "handled_request_count")]
    pub handled_request_count: usize,
    #[serde(rename = "pendingRequestCount", alias = "pending_request_count")]
    pub pending_request_count: usize,
    #[serde(rename = "totalRequestCount", alias = "total_request_count")]
    pub total_request_count: usize,
}

impl RequestQueueMetadata {
    pub fn new(id: String, name: Option<String>) -> Self {
        Self {
            base: StorageMetadata::new(id, name),
            had_multiple_clients: false,
            handled_request_count: 0,
            pending_request_count: 0,
            total_request_count: 0,
        }
    }
}

// ─── Dataset Items List Page ────────────────────────────────────────────────

/// This struct is never deserialized from disk — it's only constructed in Rust
/// and serialized to pass to the binding layer. No aliases needed.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DatasetItemsListPage {
    pub count: usize,
    pub offset: usize,
    pub limit: usize,
    pub total: usize,
    pub desc: bool,
    pub items: Vec<Value>,
}

// ─── Dataset Items Page (for lazy iteration) ───────────────────────────────

/// A single page of dataset items returned by `iterate_items_page`.
///
/// The binding layer fetches pages in a loop until `has_more` is `false`,
/// yielding individual items to the caller as they arrive.
#[derive(Debug, Clone)]
pub struct DatasetItemsPage {
    /// The items in this page.
    pub items: Vec<Value>,
    /// Whether there are more items available after this page.
    pub has_more: bool,
}

// ─── KVS Keys Page (for lazy iteration) ─────────────────────────────────────

/// A single page of key-value store keys returned by `iterate_keys_page`.
///
/// The binding layer fetches pages in a loop until `has_more` is `false`,
/// yielding individual keys to the caller as they arrive.
#[derive(Debug, Clone)]
pub struct KvsKeysPage {
    /// The key metadata entries in this page.
    pub items: Vec<KeyValueStoreRecordMetadata>,
    /// Whether there are more keys available after this page.
    pub has_more: bool,
}

// ─── Request Queue Operation Results ────────────────────────────────────────

/// These structs are never deserialized from disk — they're constructed in Rust
/// and serialized to pass to the binding layer. No aliases needed.
///
/// `request_id` is the sha256-derived id of the request (see
/// [`crate::utils::unique_key_to_request_id`]), matching the JS
/// `QueueOperationInfo.requestId` contract (non-null). The legacy `id` field
/// is kept as an alias of the same value for backward compatibility with
/// callers that still read `id`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProcessedRequest {
    #[serde(rename = "requestId")]
    pub request_id: String,
    #[serde(rename = "uniqueKey")]
    pub unique_key: String,
    #[serde(rename = "wasAlreadyPresent")]
    pub was_already_present: bool,
    #[serde(rename = "wasAlreadyHandled")]
    pub was_already_handled: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct UnprocessedRequest {
    #[serde(rename = "uniqueKey")]
    pub unique_key: String,
    pub url: String,
    #[serde(default)]
    pub method: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AddRequestsResponse {
    #[serde(rename = "processedRequests")]
    pub processed_requests: Vec<ProcessedRequest>,
    #[serde(rename = "unprocessedRequests")]
    pub unprocessed_requests: Vec<UnprocessedRequest>,
}
