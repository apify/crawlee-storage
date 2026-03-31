use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::Value;

/// Format string for datetime serialization compatible with Python's `datetime.isoformat()`.
/// Python produces: `2024-01-15T10:30:00.123456` (6 fractional digits, no trailing zeros trimmed).
/// We use chrono's default which also produces 6-digit microsecond precision for UTC datetimes.
const DATETIME_FORMAT: &str = "%Y-%m-%dT%H:%M:%S%.6f";

pub(crate) fn serialize_datetime<S>(dt: &DateTime<Utc>, serializer: S) -> Result<S::Ok, S::Error>
where
    S: serde::Serializer,
{
    // Python's `str(datetime)` / `datetime.isoformat()` for UTC-aware datetimes produces
    // something like "2024-01-15 10:30:00.123456+00:00" when using default=str in json.dumps,
    // but the actual crawlee code uses `datetime.now(timezone.utc)` and `default=str` which
    // produces "2024-01-15 10:30:00.123456+00:00".
    // However, looking at the actual metadata files, they use ISO format.
    // We'll format to match Python: "2024-01-15T10:30:00.123456"
    // The Python code uses `default=str` which calls `str()` on datetime, producing the space-separated form.
    // But Pydantic's model_dump with mode='python' keeps datetime objects, and json.dumps with default=str
    // converts them. Let's check what Python actually writes...
    // Python: json.dumps(..., default=str) -> str(datetime_obj) -> "2024-01-15 10:30:00.123456+00:00"
    // Wait, that has a space not T. Let me re-check.
    // Actually Python's `str(datetime)` uses isoformat with sep='T' since Python 3.
    // `datetime.now(timezone.utc)` -> `str()` -> "2024-01-15T10:30:00.123456+00:00"
    let formatted = dt.format(DATETIME_FORMAT).to_string();
    // Append "+00:00" to match Python's UTC timezone representation
    serializer.serialize_str(&format!("{formatted}+00:00"))
}

pub(crate) fn deserialize_datetime<'de, D>(deserializer: D) -> Result<DateTime<Utc>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    let s = String::deserialize(deserializer)?;
    // Try parsing various formats Python might produce
    // Format: "2024-01-15T10:30:00.123456+00:00"
    DateTime::parse_from_str(&s, "%Y-%m-%dT%H:%M:%S%.f%:z")
        .map(|dt| dt.with_timezone(&Utc))
        .or_else(|_| {
            // Fallback: "2024-01-15T10:30:00.123456"
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
/// Field names use snake_case to match Python's `model_dump()` output (which does NOT use by_alias).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StorageMetadata {
    pub id: String,
    #[serde(default)]
    pub name: Option<String>,
    #[serde(
        serialize_with = "serialize_datetime",
        deserialize_with = "deserialize_datetime"
    )]
    pub accessed_at: DateTime<Utc>,
    #[serde(
        serialize_with = "serialize_datetime",
        deserialize_with = "deserialize_datetime"
    )]
    pub created_at: DateTime<Utc>,
    #[serde(
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
    pub content_type: String,
    #[serde(default)]
    pub size: Option<usize>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct KeyValueStoreRecord {
    pub key: String,
    pub content_type: String,
    #[serde(default)]
    pub size: Option<usize>,
    /// The value as a JSON-serializable type.
    /// - For `application/json`: parsed JSON value
    /// - For `text/*`: JSON string
    /// - For `application/x-none`: JSON null
    /// - For everything else: base64-encoded string (handled at the binding layer)
    pub value: Value,
}

// ─── Request Queue Metadata ─────────────────────────────────────────────────

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RequestQueueMetadata {
    #[serde(flatten)]
    pub base: StorageMetadata,
    pub had_multiple_clients: bool,
    pub handled_request_count: usize,
    pub pending_request_count: usize,
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

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DatasetItemsListPage {
    pub count: usize,
    pub offset: usize,
    pub limit: usize,
    pub total: usize,
    pub desc: bool,
    pub items: Vec<Value>,
}

// ─── Request Queue Operation Results ────────────────────────────────────────

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProcessedRequest {
    pub id: Option<String>,
    pub unique_key: String,
    pub was_already_present: bool,
    pub was_already_handled: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct UnprocessedRequest {
    pub unique_key: String,
    pub url: String,
    #[serde(default)]
    pub method: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AddRequestsResponse {
    pub processed_requests: Vec<ProcessedRequest>,
    pub unprocessed_requests: Vec<UnprocessedRequest>,
}

// ─── Request Queue Internal State ───────────────────────────────────────────

/// Internal state for the request queue, persisted via callbacks.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RequestQueueState {
    #[serde(default)]
    pub sequence_counter: i64,
    #[serde(default)]
    pub forefront_sequence_counter: i64,
    /// unique_key -> sequence_number
    #[serde(default)]
    pub forefront_requests: serde_json::Map<String, Value>,
    /// unique_key -> sequence_number
    #[serde(default)]
    pub regular_requests: serde_json::Map<String, Value>,
    /// Set of unique_keys currently in progress
    #[serde(default)]
    pub in_progress_requests: Vec<String>,
    /// Set of unique_keys that have been handled
    #[serde(default)]
    pub handled_requests: Vec<String>,
}

impl Default for RequestQueueState {
    fn default() -> Self {
        Self {
            sequence_counter: 0,
            forefront_sequence_counter: 0,
            forefront_requests: serde_json::Map::new(),
            regular_requests: serde_json::Map::new(),
            in_progress_requests: Vec::new(),
            handled_requests: Vec::new(),
        }
    }
}
