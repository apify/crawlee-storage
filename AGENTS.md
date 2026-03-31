# Coding guidelines

This file provides guidance to programming agents when working with code in this repository.

## Project Overview

Rust implementation of filesystem storage clients for Crawlee crawlers, with Python and Node.js bindings. The project follows the same structure as [apify/impit](https://github.com/apify/impit) — a Cargo workspace with a core library crate and per-language binding crates.

The Rust library implements three storage clients (`FileSystemDatasetClient`, `FileSystemKeyValueStoreClient`, `FileSystemRequestQueueClient`) that are byte-for-byte filesystem-compatible with the Python implementations in [crawlee-py](https://github.com/apify/crawlee-py). Requests are treated as opaque JSON blobs (requiring at minimum a `uniqueKey` field).

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
cd crawlee-storage-node && npm run build
```

## Code Style

- **Rust edition**: 2021
- **Formatting**: `cargo fmt` (default rustfmt settings)
- **Linting**: `cargo clippy`
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

crawlee-storage-node/         napi-rs Node.js bindings (placeholder)
├── src/lib.rs                napi-rs module
└── package.json              npm package config
```

### Core Library Design

There is no `StorageClient` facade or trait in Rust. The three client structs are independent and self-contained. The Python/JS side provides its own facade that instantiates these clients and handles concerns like `purge_on_start` and `Configuration` resolution.

**Concurrency model**: Each client uses `tokio::sync::Mutex` internally to protect shared state. All file I/O uses `tokio::fs` (async). The clients are `Send + Sync` and safe for concurrent use from multiple async tasks within a single process. They are NOT safe for multi-process concurrent access.

**Request model**: Requests are `serde_json::Value` objects. The Rust code only accesses `uniqueKey` (for dedup and file naming) and `handledAt` (for marking as handled). Everything else passes through opaquely.

**Request queue state persistence**: The `FileSystemRequestQueueClient` accepts an optional `RqStatePersistence` struct with three async callbacks (`load`, `save`, `clear`). The Python/JS binding layer wires these to a KeyValueStore + event system on their respective sides. This avoids a circular dependency (RQ -> KVS -> StorageClient -> RQ).

### Filesystem Layout (must match Python exactly)

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

These must be preserved for drop-in compatibility with the Python `FileSystemStorageClient`:

- **JSON formatting**: Pretty-printed, 2-space indent, non-ASCII preserved (`ensure_ascii=False` equivalent). Use `serde_json::ser::PrettyFormatter::with_indent(b"  ")`.
- **Metadata field names**: snake_case in JSON (e.g., `item_count`, `created_at`), matching Python's `model_dump()` output.
- **Datetime format**: `2024-01-15T10:30:00.123456+00:00` — 6 fractional digits, `+00:00` suffix for UTC.
- **KVS key encoding**: `percent_encoding::utf8_percent_encode(key, NON_ALPHANUMERIC)` — equivalent to Python's `urllib.parse.quote(key, safe='')`.
- **RQ filenames**: `sha256(unique_key_bytes).hexdigest()[:15] + ".json"`.
- **Atomic writes**: Write to temp file in same directory, then `rename()`.
- **`application/x-none` sentinel**: KVS uses this custom MIME type for `None`/null values (empty file on disk).
- **`serde_json` `preserve_order` feature**: Enabled to maintain JSON key insertion order (matching Python dict ordering).

### Python Bindings

- Uses **PyO3 0.24** with **pyo3-async-runtimes** (tokio feature) for native Python coroutines.
- Each Rust client is wrapped in `Arc` so it can be cloned into async blocks (standard pattern for pyo3 async methods).
- Data crosses the FFI boundary as Python dicts/lists, converted to/from `serde_json::Value` via `value_to_py` / `py_to_value` helper functions.
- The compiled native module is `crawlee_storage._native`, re-exported by `crawlee_storage/__init__.py`.

### Node.js Bindings

- Uses **napi-rs** with `async` and `serde-json` features.
- TypeScript declarations (`.d.ts`) are auto-generated from `#[napi]` annotations.
- Currently a placeholder — implementation is Phase 2.

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
- `base64` — binary value encoding in KVS

Python bindings (`crawlee-storage-python`):
- `pyo3` — Python FFI
- `pyo3-async-runtimes` — native async Python coroutines via tokio

Node.js bindings (`crawlee-storage-node`):
- `napi` / `napi-derive` — Node.js FFI
