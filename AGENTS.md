# Coding guidelines

This file provides guidance to programming agents when working with code in this repository.

## Project Overview

Rust implementation of filesystem storage clients for Crawlee crawlers, with Python and Node.js bindings. The project follows the same structure as [apify/impit](https://github.com/apify/impit) — a Cargo workspace with a core library crate and per-language binding crates.

The Rust library implements three storage clients (`FileSystemDatasetClient`, `FileSystemKeyValueStoreClient`, `FileSystemRequestQueueClient`) with a filesystem layout matching [crawlee (JS)](https://github.com/apify/crawlee). Requests are treated as opaque JSON blobs (requiring at minimum a `uniqueKey` field).

## Development Commands

```bash
# Build the entire workspace
cargo build

# Build just the core library
cargo build -p crawlee-storage

# Run all Rust tests
cargo test

# Run tests for just the core library
cargo test -p crawlee-storage

# Run a single test
cargo test -p crawlee-storage -- test_name

# Run tests with output shown
cargo test -p crawlee-storage -- --nocapture

# Build Python bindings (requires maturin)
cd crawlee-storage-python && maturin develop --release

# Build Python bindings in debug mode (faster compile)
cd crawlee-storage-python && maturin develop

# Build Node.js bindings (requires @napi-rs/cli)
cd crawlee-storage-node && npm install && npm run build

# Run Node.js tests
cd crawlee-storage-node && npm test

# Lint Node.js code (type-aware, via oxlint + tsgolint)
cd crawlee-storage-node && npm run lint

# Format Node.js code (via oxfmt)
cd crawlee-storage-node && npm run fmt

# Check Node.js formatting without writing
cd crawlee-storage-node && npm run fmt:check
```

## Code Style

- **Rust edition**: 2021
- **Rust formatting**: `cargo fmt` (default rustfmt settings)
- **Rust linting**: `cargo clippy`
- **Node.js linting**: `oxlint` with type-aware linting via `tsgolint` (config in `.oxlintrc.json`)
- **Node.js formatting**: `oxfmt` (config in `.oxfmtrc.json`; 4-space indent, single quotes, trailing commas)
- **Commit format**: Conventional Commits (`feat:`, `fix:`, `docs:`, `refactor:`, `test:`, etc.)

## Architecture

### Workspace Layout

```
crawlee-storage/              Core Rust library (no FFI dependencies)
├── src/
│   ├── lib.rs                Module root
│   ├── models.rs             Shared data models (metadata, responses, queue state)
│   ├── utils.rs              Utilities (atomic_write, JSON formatting, hashing, encoding)
│   ├── dataset.rs            FileSystemDatasetClient
│   ├── key_value_store.rs    FileSystemKeyValueStoreClient
│   └── request_queue.rs      FileSystemRequestQueueClient

crawlee-storage-python/       PyO3/maturin Python bindings
├── src/lib.rs                PyO3 module and wrapper classes
├── python/crawlee_storage/   Pure Python package (re-exports from native module)
└── pyproject.toml            maturin build config

crawlee-storage-node/         napi-rs Node.js bindings
├── src/lib.rs                napi-rs module (napi v3)
├── build.rs                  napi-build setup
├── dts-header.d.ts           Custom TypeScript interfaces (prepended to auto-generated index.d.ts)
├── index.js                  Auto-generated native module loader (by napi-rs CLI)
├── index.d.ts                Auto-generated TypeScript declarations (by napi-rs CLI)
├── .oxlintrc.json            Oxlint config (type-aware linting)
├── .oxfmtrc.json             Oxfmt config (formatting)
├── tsconfig.json             TypeScript config (for test compilation)
├── test/                     Vitest tests (TypeScript)
└── package.json              npm package config
```

### Core Library Design

There is no `StorageClient` facade or trait in Rust. The three client structs are independent and self-contained. The Python/JS side provides its own facade that instantiates these clients and handles concerns like `purge_on_start` and `Configuration` resolution.

**Concurrency model**: Each client uses `tokio::sync::Mutex` internally to protect shared state. All file I/O uses `tokio::fs` (async). The clients are `Send + Sync` and safe for concurrent use from multiple async tasks within a single process. They are NOT safe for multi-process concurrent access.

**Request model**: Requests are `serde_json::Value` objects. The Rust code only accesses `uniqueKey` (for dedup and file naming) and `handledAt` (for marking as handled). Everything else passes through opaquely.

**Request queue state persistence**: The `FileSystemRequestQueueClient` uses a private `StatePersistence` struct that directly opens the default `FileSystemKeyValueStoreClient` to persist queue state (sequence counters, in-progress/handled sets) under the key `__RQ_STATE_{queue_id}`. The binding layer is responsible for calling `persist_state()` periodically (e.g. via the framework's event system). See [#12](https://github.com/apify/crawlee-storage/issues/12) for discussion about making this injectable.

**KVS value model**: KVS record values use the `KvsValue` enum (`None`, `Json(Value)`, `Text(String)`, `Binary(Vec<u8>)`) instead of `serde_json::Value`. This avoids base64-encoding binary data at the core level — each binding layer converts `KvsValue` variants directly to native types (e.g. `Binary` → Python `bytes`, Node.js `Buffer`).

### Filesystem Layout

```
{storage_dir}/
├── datasets/{name}/
│   ├── __metadata__.json
│   ├── 000000001.json          (9-digit zero-padded item files)
│   └── ...
├── key_value_stores/{name}/
│   ├── __metadata__.json
│   ├── {percent_encoded_key}           (value data file)
│   ├── {percent_encoded_key}.__metadata__.json  (record sidecar)
│   └── ...
└── request_queues/{name}/
    ├── __metadata__.json
    ├── {sha256(uniqueKey)[:15]}.json   (request files)
    └── ...
```

### Key Compatibility Constraints

These must be preserved for compatibility with the JS Crawlee `MemoryStorage` on-disk format:

- **JSON formatting**: Pretty-printed, 2-space indent, non-ASCII preserved (`ensure_ascii=False` equivalent). Use `serde_json::ser::PrettyFormatter::with_indent(b"  ")`.
- **Metadata field names**: camelCase in JSON (e.g., `itemCount`, `accessedAt`, `contentType`), matching JS conventions. Rust struct fields stay snake_case with `#[serde(rename_all = "camelCase")]`. All multi-word fields also have `#[serde(alias = "snake_case")]` so legacy files written by the old Python `FileSystemStorageClient` can still be loaded.
- **Datetime format**: Written as `2024-01-15T10:30:00.123456+00:00` — 6 fractional digits (microsecond precision), `+00:00` suffix for UTC. Deserialization also accepts JS-style `Z` suffix (e.g., `2024-01-15T10:30:00.123Z`).
- **Directory names**: snake_case (`datasets`, `key_value_stores`, `request_queues`) — unchanged, matching both JS and Python.
- **KVS key encoding**: `percent_encoding::utf8_percent_encode(key, NON_ALPHANUMERIC)` — equivalent to Python's `urllib.parse.quote(key, safe='')`.
- **RQ filenames**: `sha256(unique_key_bytes).hexdigest()[:15] + ".json"`.
- **Atomic writes**: Write to temp file in same directory, then `rename()`.
- **`application/x-none` sentinel**: KVS uses this custom MIME type for `None`/null values (empty file on disk).
- **`serde_json` `preserve_order` feature**: Enabled to maintain JSON key insertion order (matching Python dict ordering).
- **Python bindings return camelCase**: The Python bindings pass camelCase dicts directly to Python. The Pydantic models in crawlee-python accept both camelCase (via alias) and snake_case (via field name) thanks to `validate_by_name=True, validate_by_alias=True`.

### Python Bindings

- Uses **PyO3 0.28** with **pyo3-async-runtimes** (tokio feature) for native Python coroutines.
- Each Rust client is wrapped in `Arc` so it can be cloned into async blocks (standard pattern for pyo3 async methods).
- JSON data crosses the FFI boundary as Python dicts/lists, converted to/from `serde_json::Value` via `value_to_py` / `py_to_value` helper functions.
- KVS values are `bytes`-only in the Python bindings. `setValue` accepts `bytes` (PyO3 `Vec<u8>`) directly, and `getValue` returns raw file bytes as Python `bytes`. The caller is responsible for serialization/deserialization.
- The compiled native module is `crawlee_storage._native`, re-exported by `crawlee_storage/__init__.py`.

### Node.js Bindings

- Uses **napi-rs v3** (`napi = "3"`, `napi-derive = "3"`) with `async`, `serde-json`, `napi4`, `napi5`, `web_stream`, and `tokio_rt` features.
- `build.rs` calls `napi_build::setup()` — standard napi-rs build script.
- `index.js` and `index.d.ts` are **auto-generated** by `napi build` (via `@napi-rs/cli`). Do not edit them manually.
- `dts-header.d.ts` contains hand-written TypeScript interfaces (`DatasetMetadata`, `KeyValueStoreRecord`, etc.) that are prepended to the auto-generated `index.d.ts`. This is configured via `"dtsHeaderFile"` in `package.json`'s `napi` section.
- `#[napi(ts_return_type = "...")]` and `#[napi(ts_args_type = "...")]` annotations on Rust methods override auto-generated types to reference the header interfaces instead of `any`.
- **camelCase convention**: The core Rust library serializes with snake_case (for Python compatibility). The Node binding layer converts all object keys from snake_case to camelCase via `to_camel_case_keys()` before returning to JS. The `dts-header.d.ts` interfaces use camelCase field names accordingly.
- Each Rust client is wrapped in `Arc` so it can be cloned into async blocks.
- JSON data crosses the FFI boundary as `serde_json::Value` ↔ JS objects (via napi's `serde-json` feature).
- KVS values are `Buffer`-only in the Node bindings. `setValue` accepts `napi::bindgen_prelude::Buffer` directly, and `getValue` returns raw file bytes as a JSON array that the JS wrapper in `lib.js` converts to a `Buffer` before returning to the caller.
- KVS streaming is supported: `getValueStream` returns a Web `ReadableStream<Uint8Array>` (created via `fs.createReadStream` + `Readable.toWeb` in the JS wrapper), and `setValueStream` pipes a `ReadableStream` directly to a temp file on disk (via `Writable.toWeb`), then calls a Rust method to atomically finalize it. No in-memory buffering.
- Tests are TypeScript (`.test.ts`) using Vitest, importing directly from `../index.js`.
- Linting uses `oxlint` with type-aware rules (via `tsgolint`). Formatting uses `oxfmt`.

### Key Directories

- `crawlee-storage/src/` — All core Rust implementation
- `crawlee-storage-python/src/` — PyO3 binding code
- `crawlee-storage-python/python/` — Pure Python package
- `crawlee-storage-node/src/` — napi-rs binding code

### Dependencies

Core library (`crawlee-storage`):
- `tokio` — async runtime and filesystem I/O
- `serde` / `serde_json` — serialization (with `preserve_order`)
- `chrono` — datetime handling
- `sha2` — SHA-256 for request queue filenames
- `percent-encoding` — URL-encoding KVS keys
- `tempfile` — atomic write temp files
- `thiserror` — error types
- `tracing` — logging
- `rand` — random ID generation

Python bindings (`crawlee-storage-python`):
- `pyo3` — Python FFI
- `pyo3-async-runtimes` — native async Python coroutines via tokio

Node.js bindings (`crawlee-storage-node`):
- `napi` / `napi-derive` — Node.js FFI
- `napi-build` — build script for napi-rs
- `oxlint` / `oxlint-tsgolint` — linting (with type-aware rules)
- `oxfmt` — formatting
- `vitest` — test framework
- `typescript` / `@types/node` — TypeScript support for tests
