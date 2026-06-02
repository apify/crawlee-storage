use crawlee_storage::models;
use pyo3_stub_gen::Result;
use serde::Serialize;
use serde_json::Value;

/// Method names that should remain synchronous (not marked async).
const SYNC_METHODS: &[&str] = &["iterate_items", "iterate_keys"];

/// Dunder methods that ARE async (all other dunders stay sync).
const ASYNC_DUNDERS: &[&str] = &["__anext__", "__aenter__", "__aexit__"];

// ─── TypedDict generation from serde ────────────────────────────────────────

/// A single field in a Python TypedDict.
struct TypedDictField {
    /// The JSON key name (camelCase, as produced by serde).
    name: String,
    /// The Python type annotation string.
    py_type: String,
}

/// A complete TypedDict definition to emit into the `.pyi` file.
struct TypedDictDef {
    /// Python class name.
    class_name: &'static str,
    /// Ordered list of fields.
    fields: Vec<TypedDictField>,
}

impl TypedDictDef {
    /// Render as a Python `class Foo(typing.TypedDict): ...` block.
    fn render(&self) -> String {
        let mut out = format!("class {}(typing.TypedDict):\n", self.class_name);
        for field in &self.fields {
            out.push_str(&format!("    {}: {}\n", field.name, field.py_type));
        }
        out
    }
}

/// Map a `serde_json::Value` (from a dummy instance) to a Python type string.
///
/// Nested references to other TypedDicts are resolved via `known_types`:
/// if a JSON object's key set matches a known TypedDict, that name is used
/// instead of `dict[str, typing.Any]`.
fn json_value_to_py_type(
    val: &Value,
    optional: bool,
    known_types: &[(&str, Vec<String>)],
) -> String {
    let base = match val {
        Value::Bool(_) => "builtins.bool".to_string(),
        Value::Number(n) => {
            if n.is_f64() && !n.is_i64() && !n.is_u64() {
                "builtins.float".to_string()
            } else {
                "builtins.int".to_string()
            }
        }
        Value::String(_) => "builtins.str".to_string(),
        Value::Null => {
            // A null value with `optional` means `Optional[???]`. Since we don't know
            // the inner type from null alone, we use `typing.Any` for pure-null fields.
            // But in practice, optional fields have `#[serde(default)]` and we handle
            // them via the `optional` flag from the caller.
            return "typing.Optional[typing.Any]".to_string();
        }
        Value::Array(arr) => {
            if let Some(first) = arr.first() {
                let inner = json_value_to_py_type(first, false, known_types);
                format!("builtins.list[{inner}]")
            } else {
                "builtins.list[typing.Any]".to_string()
            }
        }
        Value::Object(map) => {
            // Check if this object's key set matches a known TypedDict.
            let keys: Vec<String> = map.keys().cloned().collect();
            for (name, expected_keys) in known_types {
                if keys == *expected_keys {
                    return (*name).to_string();
                }
            }
            "dict[builtins.str, typing.Any]".to_string()
        }
    };

    if optional {
        format!("typing.Optional[{base}]")
    } else {
        base
    }
}

/// Fields whose serialized value is `null` for a dummy instance but should be typed as
/// `Optional[T]` rather than bare `None`. Maps `(struct_name, field_name)` → Python type.
///
/// This is needed because serde serializes `Option::None` as JSON `null`, and we can't
/// recover the inner type from that alone. We only need overrides for `Option<T>` fields
/// where `T` is not `Any`.
const OPTIONAL_OVERRIDES: &[(&str, &str, &str)] = &[
    ("DatasetMetadata", "name", "typing.Optional[builtins.str]"),
    (
        "KeyValueStoreMetadata",
        "name",
        "typing.Optional[builtins.str]",
    ),
    (
        "RequestQueueMetadata",
        "name",
        "typing.Optional[builtins.str]",
    ),
    (
        "KeyValueStoreRecordMetadata",
        "size",
        "typing.Optional[builtins.int]",
    ),
    ("ProcessedRequest", "id", "typing.Optional[builtins.str]"),
    (
        "UnprocessedRequest",
        "method",
        "typing.Optional[builtins.str]",
    ),
];

/// Build a `TypedDictDef` by serializing a dummy instance of `T` and inspecting the JSON keys.
fn typed_dict_from_serde<T: Serialize>(
    class_name: &'static str,
    dummy: &T,
    known_types: &[(&str, Vec<String>)],
) -> TypedDictDef {
    let val = serde_json::to_value(dummy).expect("dummy instance must serialize");
    let map = val.as_object().expect("serialized dummy must be an object");

    let fields = map
        .iter()
        .map(|(key, val)| {
            // Check for explicit override first.
            let py_type = OPTIONAL_OVERRIDES
                .iter()
                .find(|(s, f, _)| *s == class_name && *f == key)
                .map(|(_, _, ty)| (*ty).to_string())
                .unwrap_or_else(|| {
                    let is_null = val.is_null();
                    json_value_to_py_type(val, is_null, known_types)
                });

            TypedDictField {
                name: key.clone(),
                py_type,
            }
        })
        .collect();

    TypedDictDef { class_name, fields }
}

/// Collect the ordered key names from a serialized dummy, for matching nested objects.
fn keys_of<T: Serialize>(dummy: &T) -> Vec<String> {
    let val = serde_json::to_value(dummy).expect("dummy must serialize");
    val.as_object()
        .expect("must be an object")
        .keys()
        .cloned()
        .collect()
}

/// Generate all TypedDict definitions as a single string block.
fn generate_typed_dicts() -> String {
    // Dummy instances — field values don't matter, only types & key names.
    let dataset_meta = models::DatasetMetadata::new("".into(), None);
    let kvs_meta = models::KeyValueStoreMetadata::new("".into(), None);
    let rq_meta = models::RequestQueueMetadata::new("".into(), None);
    let kvs_record_meta = models::KeyValueStoreRecordMetadata {
        key: String::new(),
        content_type: String::new(),
        size: None,
    };
    let dataset_page = models::DatasetItemsListPage {
        count: 0,
        offset: 0,
        limit: 0,
        total: 0,
        desc: false,
        items: vec![serde_json::json!({})],
    };
    let processed_req = models::ProcessedRequest {
        id: None,
        unique_key: String::new(),
        was_already_present: false,
        was_already_handled: false,
    };
    let unprocessed_req = models::UnprocessedRequest {
        unique_key: String::new(),
        url: String::new(),
        method: None,
    };
    let add_requests_resp = models::AddRequestsResponse {
        processed_requests: vec![models::ProcessedRequest {
            id: None,
            unique_key: String::new(),
            was_already_present: false,
            was_already_handled: false,
        }],
        unprocessed_requests: vec![models::UnprocessedRequest {
            unique_key: String::new(),
            url: String::new(),
            method: None,
        }],
    };

    // Known types for nested object resolution (ordered by dependency).
    let known_types: Vec<(&str, Vec<String>)> = vec![
        ("ProcessedRequest", keys_of(&processed_req)),
        ("UnprocessedRequest", keys_of(&unprocessed_req)),
    ];

    // Build the TypedDict definitions (order matters for forward references).
    let defs: Vec<TypedDictDef> = vec![
        typed_dict_from_serde("DatasetMetadata", &dataset_meta, &known_types),
        typed_dict_from_serde("KeyValueStoreMetadata", &kvs_meta, &known_types),
        typed_dict_from_serde(
            "KeyValueStoreRecordMetadata",
            &kvs_record_meta,
            &known_types,
        ),
        // KeyValueStoreRecord is a special case: it's built manually by `record_to_py()`
        // with snake_case keys, and `value` is `KvsValue` (not serializable via serde).
        TypedDictDef {
            class_name: "KeyValueStoreRecord",
            fields: vec![
                TypedDictField {
                    name: "key".into(),
                    py_type: "builtins.str".into(),
                },
                TypedDictField {
                    name: "contentType".into(),
                    py_type: "builtins.str".into(),
                },
                TypedDictField {
                    name: "size".into(),
                    py_type: "typing.Optional[builtins.int]".into(),
                },
                TypedDictField {
                    name: "value".into(),
                    py_type: "builtins.bytes".into(),
                },
            ],
        },
        typed_dict_from_serde("RequestQueueMetadata", &rq_meta, &known_types),
        typed_dict_from_serde("DatasetItemsListPage", &dataset_page, &known_types),
        typed_dict_from_serde("ProcessedRequest", &processed_req, &known_types),
        typed_dict_from_serde("UnprocessedRequest", &unprocessed_req, &known_types),
        typed_dict_from_serde("AddRequestsResponse", &add_requests_resp, &known_types),
    ];

    let mut out = String::new();
    for def in &defs {
        out.push('\n');
        out.push_str(&def.render());
    }
    out
}

/// Collect all TypedDict class names (for `__all__` injection), in sorted order.
fn typed_dict_names() -> Vec<&'static str> {
    let mut names = vec![
        "AddRequestsResponse",
        "DatasetItemsListPage",
        "DatasetMetadata",
        "KeyValueStoreMetadata",
        "KeyValueStoreRecord",
        "KeyValueStoreRecordMetadata",
        "ProcessedRequest",
        "RequestQueueMetadata",
        "UnprocessedRequest",
    ];
    names.sort();
    names
}

// ─── Stub file post-processing ──────────────────────────────────────────────

/// Post-process a generated `.pyi` stub file:
/// 1. Inject `TypedDict` definitions after the imports.
/// 2. Append TypedDict names to `__all__`.
/// 3. Mark methods as `async def` where appropriate.
///
/// pyo3_stub_gen cannot detect async methods that use
/// `pyo3_async_runtimes::tokio::future_into_py` (they appear as sync `fn` in Rust),
/// so we fix them up here.
fn fixup_stubs(path: &std::path::Path, typed_dicts: &str) -> std::io::Result<()> {
    let content = std::fs::read_to_string(path)?;
    let mut output = String::with_capacity(content.len() + typed_dicts.len());

    let lines: Vec<&str> = content.lines().collect();
    let names = typed_dict_names();

    // Find the insertion point: after imports and __all__, before the first class.
    let insert_before = lines
        .iter()
        .position(|line| line.starts_with("@typing.final") || line.starts_with("class "))
        .unwrap_or(lines.len());

    // Track whether we're inside the __all__ block so we can append TypedDict names.
    let mut in_all_block = false;

    for (i, line) in lines.iter().enumerate() {
        // Inject TypedDicts right before the first class definition.
        if i == insert_before {
            output.push_str(typed_dicts);
            output.push('\n');
        }

        // Detect __all__ = [ ... ] and inject TypedDict names before the closing `]`.
        if line.contains("__all__") && line.contains('[') {
            in_all_block = true;
        }
        if in_all_block && line.trim_start().starts_with(']') {
            for name in &names {
                output.push_str(&format!("    \"{name}\",\n"));
            }
            in_all_block = false;
        }

        let trimmed = line.trim_start();

        if let Some(after_def) = trimmed.strip_prefix("def ") {
            // Extract method name: "foo(" -> "foo"
            let method_name = after_def.split('(').next().unwrap_or("");

            // Check if the previous non-empty line is a @property decorator
            let is_property = (0..i)
                .rev()
                .find(|&j| !lines[j].trim().is_empty())
                .is_some_and(|j| lines[j].trim() == "@property");

            let is_dunder = method_name.starts_with("__");
            let is_sync = SYNC_METHODS.contains(&method_name)
                || (is_dunder && !ASYNC_DUNDERS.contains(&method_name))
                || is_property;

            if !is_sync {
                // Replace "def " with "async def " preserving indentation
                let indent = &line[..line.len() - trimmed.len()];
                output.push_str(indent);
                output.push_str("async def ");
                output.push_str(after_def);
                output.push('\n');
                continue;
            }
        }

        output.push_str(line);
        output.push('\n');
    }

    std::fs::write(path, output)?;
    Ok(())
}

/// Append TypedDict names to the `__all__` list in a re-export stub file.
fn fixup_reexport_stubs(path: &std::path::Path) -> std::io::Result<()> {
    let content = std::fs::read_to_string(path)?;
    let mut output = String::with_capacity(content.len());

    let lines: Vec<&str> = content.lines().collect();
    let names = typed_dict_names();
    let mut in_all_block = false;

    for line in &lines {
        if line.contains("__all__") && line.contains('[') {
            in_all_block = true;
        }
        if in_all_block && line.trim_start().starts_with(']') {
            for name in &names {
                output.push_str(&format!("    \"{name}\",\n"));
            }
            in_all_block = false;
        }

        output.push_str(line);
        output.push('\n');
    }

    std::fs::write(path, output)?;
    Ok(())
}

fn main() -> Result<()> {
    let stub = _crawlee_storage::stub_info()?;
    stub.generate()?;

    let typed_dicts = generate_typed_dicts();

    let manifest_dir: &std::path::Path = env!("CARGO_MANIFEST_DIR").as_ref();
    let python_dir = manifest_dir.join("python").join("crawlee_storage");

    // Post-process _native/__init__.pyi: inject TypedDicts and add `async` markers
    let native_stub_path = python_dir.join("_native").join("__init__.pyi");
    if native_stub_path.exists() {
        fixup_stubs(&native_stub_path, &typed_dicts).expect("Failed to post-process native stubs");
        eprintln!(
            "Post-processed stubs: injected TypedDicts and async markers into {}",
            native_stub_path.display()
        );
    }

    // Post-process top-level __init__.pyi: add TypedDict names to __all__
    let toplevel_stub_path = python_dir.join("__init__.pyi");
    if toplevel_stub_path.exists() {
        fixup_reexport_stubs(&toplevel_stub_path).expect("Failed to post-process top-level stubs");
        eprintln!(
            "Post-processed stubs: added TypedDict names to __all__ in {}",
            toplevel_stub_path.display()
        );
    }

    Ok(())
}
