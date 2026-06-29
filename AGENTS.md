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

# Build Python bindings (requires maturin; in the venv)
cd crawlee-storage-python && uv sync && uv run maturin develop --release

# Build Python bindings in debug mode (faster compile)
cd crawlee-storage-python && uv run maturin develop

# Regenerate Python type stubs (.pyi). Run after changing any binding signature
# or any TypedDict spec in src/models.rs. The post-processor also requires
# `ruff` on PATH to keep stub formatting stable across regenerations.
cd crawlee-storage-python && cargo run --bin stub_gen

# Run Python tests
cd crawlee-storage-python && uv run pytest

# Lint / format Python
cd crawlee-storage-python && uvx ruff check && uvx ruff format --check

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
├── src/
│   ├── lib.rs                PyO3 module and wrapper classes
│   ├── models.rs             TypedDict mirror specs + dict builders (rustc-checked source of truth for the stubs)
│   └── bin/stub_gen.rs       Generates and post-processes `.pyi` type stubs
├── python/crawlee_storage/
│   ├── __init__.py           Re-exports from the native module
│   ├── __init__.pyi          Top-level type stubs (auto-generated)
│   └── _native/__init__.pyi  Stubs for the compiled native module (auto-generated)
├── tests/                    Pytest suite (pytest-asyncio)
├── pyproject.toml            maturin build + ruff + pytest config
└── uv.lock                   uv-managed dev environment

crawlee-storage-node/         napi-rs Node.js bindings
├── src/
│   ├── lib.rs                napi-rs module (napi v3)
│   └── models.rs             #[napi(object)] mirror structs for all structured payloads
├── build.rs                  napi-build setup
├── index.js                  Auto-generated native module loader (by napi-rs CLI)
├── index.d.ts                Auto-generated TypeScript declarations (by napi-rs CLI)
├── lib.js                    Thin JS wrapper layer (iterator + KVS-streaming helpers)
├── lib.d.ts                  Declarations for the lib.js-only wrappers (module augmentation)
├── .oxlintrc.json            Oxlint config (type-aware linting)
├── .oxfmtrc.json             Oxfmt config (formatting)
├── tsconfig.json             TypeScript config (for test compilation)
├── test/                     Vitest tests (TypeScript)
└── package.json              npm package config
```

### Core Library Design

There is no `StorageClient` facade or trait in Rust. The three client structs are independent and self-contained. The Python/JS side provides its own facade that instantiates these clients and handles concerns like `purge_on_start` and `Configuration` resolution.

**Concurrency model**: Each client uses `tokio::sync::Mutex` internally to protect shared state. All file I/O uses `tokio::fs` (async). The clients are `Send + Sync` and safe for concurrent use from multiple async tasks within a single process. The `FileSystemRequestQueueClient` additionally persists its per-request lock (`orderNo`) on disk, so multiple processes/consumers sharing one on-disk queue coordinate via the file contents (assuming roughly synchronized clocks — see [#32](https://github.com/apify/crawlee-storage/issues/32)). Datasets and KVS remain single-process-oriented.

**Request model**: Requests are `serde_json::Value` objects. The Rust code only accesses `uniqueKey` (for dedup and file naming) and `handledAt` (for marking as handled), and manages two queue-owned fields it injects into each persisted request: `id` (sha256-derived request id) and `orderNo` (lock/ordering state). Everything else passes through opaquely.

**Request queue locking (`orderNo` model)**: Mirrors crawlee-js v3. Each request file carries a signed `orderNo` (a unix-millis timestamp): `null` means handled, positive means a regular pending request, negative means forefront (priority). A request whose `|orderNo|` lies in the future is *locked* (in progress) until that moment. `fetch_next_request` picks the lowest unlocked `orderNo`, rewrites it to `(now + lock_millis) * sign`, and persists that to the file — so any consumer reading the file sees the lock and skips it. `mark_request_as_handled`/`reclaim_request` are lock-expiry tolerant (they don't require the request to still be locked by the caller). The lock duration defaults to 3 minutes and is tunable via `set_expected_request_processing_time` (only ever raised). There is **no** separate `__RQ_STATE_*` state blob anymore — all state lives in the request files — and `persist_state()` is a retained no-op for binding compatibility. `is_empty()` means "next `fetch_next_request` would return null" (locked requests are NOT counted); `is_finished()` is the strong predicate ("no unhandled requests remain anywhere, including locked ones") and is what completion logic should use.

**Crash recovery**: By default, `open()` does not reclaim future-dated `orderNo`s — a future-dated lock on disk could be a live peer's reservation, and clobbering it would let two consumers hand out the same request. Recovery is handled by the lock window itself: once wall-clock time passes `|orderNo|`, the lock is expired and any consumer (the original one if it comes back, or anyone else) can pick the request up via `fetch_next_request`. The worst-case stall after a crash is therefore one lock window — by default 3 minutes, tunable via `set_expected_request_processing_time`.

Callers that *know* nothing else is using the on-disk queue (the typical single-process Crawlee case) can opt into immediate crash recovery by opening with `assume_sole_owner = true` (Rust) / `assumeSoleOwner: true` (Node) / `assume_sole_owner=True` (Python). In that mode, every future-dated `orderNo` is reclaimed at open time so previously in-progress requests are instantly re-fetchable. Set it only when you're sure — if a live peer is using the queue, this will clobber its reservation.

Tests that need to drive lock expiry without real sleeps should use a `TestClock` (see [clock injection](#clock-injection-for-testing)).

**Clock injection for testing**: Every client takes an optional `Arc<dyn Clock>` (see `crawlee-storage/src/clock.rs`). The default is `SystemClock` (wraps `Utc::now`). `TestClock` carries a settable `AtomicI64` offset, advanceable via `.advance(millis)`. The bindings expose this as `useTestClock: true` on `open()` plus an `advanceClockForTesting` method on each client — Node takes a `number` of milliseconds, Python takes a `datetime.timedelta`. The hook is necessary because JS fake timers (`vi.useFakeTimers()`) and Python equivalents don't reach into native code, so the Rust-side clock has to be driven explicitly.

**KVS value model**: KVS record values are opaque raw byte sequences (`&[u8]`) on the way in and on the way out. The core never parses or serializes record contents — it persists exactly the bytes it's given alongside a `contentType` sidecar. Each binding exposes these as the language's native byte container (Python `bytes`, Node.js `Buffer`); callers handle (de)serialization themselves.

**KVS bare-file resolution (`resolve_value` / `resolve_existing_key`)**: A run's input may be a properly-tracked record *or* an out-of-band ("bare") value file with no metadata sidecar that a CLI/platform dropped on disk under one of several conventional names (`INPUT`, `INPUT.json`, `INPUT.bin`, ...). Rather than have each binding hand-roll the extension-probing loop, the core exposes `resolve_value(key, bare_fallbacks)` and `resolve_existing_key(key, bare_fallbacks)`. They try the tracked record for the literal key first (its `contentType` is taken verbatim from the sidecar), then probe each caller-supplied fallback as a bare `key + extension` file. `resolve_value` takes `(extension, content_type)` pairs and, on a bare match, re-keys the result to the originally-requested key and reports the caller-declared `content_type` (an empty string keeps the synthesized `application/octet-stream`); `resolve_existing_key` takes plain extension strings and returns the matched on-disk key (literal or `key + extension`) for feeding to `get_public_url`. The core still does **no** MIME inference of its own — the *mechanism* (probe order, lookup, re-keying) lives in Rust and is shared by both bindings, while the *policy* (which extensions are "input", and that `.json` → `application/json`) is declared by the caller. Exposed as `resolveValue`/`resolveExistingKey` (Node, with a `BareFallback { extension, contentType }` object) and `resolve_value`/`resolve_existing_key` (Python, with `(extension, content_type)` tuples). The bindings' `get_value`/`getValue` and `record_exists`/`recordExists` are therefore **strict-only** — they only ever see tracked records (value file + sidecar). The core's `get_value`/`record_exists` still take an internal `require_record_metadata` bool (it's how the resolver reads a sidecar-less file), but that flag is not surfaced to either binding's public API: reaching a bare file is `resolve_value`/`resolve_existing_key`'s job.

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
- **Metadata field names**: camelCase in JSON (e.g., `itemCount`, `accessedAt`, `contentType`), matching JS conventions. Rust struct fields stay snake_case with per-field `#[serde(rename = "camelCase")]` annotations. Multi-word fields also carry `#[serde(alias = "snake_case")]` so legacy files written by the old Python `FileSystemStorageClient` can still be loaded.
- **Datetime format**: Written as `2024-01-15T10:30:00.123456+00:00` — 6 fractional digits (microsecond precision), `+00:00` suffix for UTC. Deserialization also accepts JS-style `Z` suffix (e.g., `2024-01-15T10:30:00.123Z`).
- **Directory names**: snake_case (`datasets`, `key_value_stores`, `request_queues`) — unchanged, matching both JS and Python.
- **KVS key encoding**: `percent_encoding::utf8_percent_encode(key, NON_ALPHANUMERIC)` — equivalent to Python's `urllib.parse.quote(key, safe='')`.
- **RQ filenames**: `sha256(unique_key_bytes).hexdigest()[:15] + ".json"`.
- **Atomic writes**: Write to temp file in same directory, then `rename()`.
- **`application/x-none` sentinel**: KVS uses this custom MIME type for `None`/null values (empty file on disk).
- **KVS record `size` backfill**: `size` is `Option<usize>` on disk (this library always writes it, but crawlee-JS `MemoryStorage` and older Python `FileSystemStorageClient` sidecars may omit it). On read, `get_value`/`list_keys_raw` backfill a missing `size` from the actual value-file length, so the returned `KeyValueStoreRecordMetadata.size` is always populated. Both bindings therefore type it as a non-optional integer (`size: number` / `size: int`), matching crawlee's `KeyValueStoreItemData.size: number`.
- **`serde_json` `preserve_order` feature**: Enabled to maintain JSON key insertion order (matching Python dict ordering).
- **Binding boundary translation**: Both bindings hand callers **camelCase keys** (matching the on-disk JSON), but datetime fields are converted to language-native types at the FFI boundary. The Node binding returns `Date`; the Python binding returns timezone-aware `datetime.datetime` (UTC). The Python bindings' callers may still pass dicts to Pydantic models that accept both camelCase aliases and snake_case field names via `validate_by_name=True, validate_by_alias=True`.

### Python Bindings

- Uses **PyO3 0.28** with **pyo3-async-runtimes** (tokio feature) for native Python coroutines. The `pyo3` `chrono` feature is enabled so `chrono::DateTime<Utc>` ↔ tz-aware `datetime.datetime` and `chrono::Duration` ↔ `datetime.timedelta` cross the FFI as native Python types.
- Each Rust client is wrapped in `Arc` so it can be cloned into async blocks (standard pattern for pyo3 async methods).
- JSON request bodies and dataset items cross the FFI as Python dicts/lists, converted to/from `serde_json::Value` via `value_to_py` / `py_to_value` helper functions. Non-date payloads (`DatasetItemsListPage`, `ProcessedRequest`, `AddRequestsResponse`, `KeyValueStoreRecordMetadata`) go through `serde_to_py` (i.e. via `serde_json::Value`).
- **Metadata is built directly, not via serde**: `dataset_metadata_to_py`, `kvs_metadata_to_py`, and `rq_metadata_to_py` construct the result dict field-by-field so the datetime fields (`accessedAt`, `createdAt`, `modifiedAt`) cross the FFI as native `datetime.datetime`. They share `set_base_metadata_fields` for the common base fields.
- `set_expected_request_processing_time` takes `chrono::Duration` (i.e. `datetime.timedelta`) — passing a number raises `TypeError`. The corresponding Node API still uses `number` of seconds since JS has no built-in duration type.
- KVS values are `bytes`-only in the Python bindings. `set_value` accepts `bytes` (PyO3 `Vec<u8>`) directly, and `get_value` returns raw file bytes as Python `bytes`. The caller is responsible for serialization/deserialization.
- The compiled native module is `crawlee_storage._native`, re-exported by `crawlee_storage/__init__.py`.
- **TypedDict source of truth** (`src/models.rs`): the bindings hand callers **camelCase dicts** (not attribute objects), so `pyo3-stub-gen` can't infer their shape — a dict return is just `dict[str, Any]` to it. `src/models.rs` declares each payload's `TypedDict` field list (`TypedDictModel::SPEC`, a list of `(camelCaseKey, pythonType)`) co-located with a `From<&core::Type>` conversion and a `to_py` dict builder. The `From` impls are rustc-checked against the core models (rename a core field and the conversion stops compiling), and the `SPEC` + `to_py` builder sit in the same `impl` so the stub's claimed shape and the dict's actual keys are edited together. This replaced the old approach of serializing a dummy instance and *guessing* Python types from the resulting JSON (lossy: `Option<T>` → `null` → `Any`, `DateTime` → string), which needed a hand-maintained `FIELD_OVERRIDES` table to patch up.
- **Type stubs** (`.pyi`) are generated by `cargo run --bin stub_gen` (see `src/bin/stub_gen.rs`). The generator post-processes the output of `pyo3-stub-gen` to: (a) emit `TypedDict` definitions straight from the `src/models.rs` specs, (b) re-mark `future_into_py`-based methods as `async def` (everything is async except a small explicit `SYNC_METHODS` list), (c) inject module constants and `import datetime`, and (d) append `TypedDict`/constant names to `__all__`. PEP 604 rewriting (`Optional[X]` → `X | None`) and formatting are delegated to `ruff` (`check --fix --select I,UP` then `format`) rather than hand-rolled.
- Tests live in `tests/`, use `pytest-asyncio` (`asyncio_mode = "auto"`), and run via `uv run pytest`.

### Node.js Bindings

- Uses **napi-rs v3** (`napi = "3"`, `napi-derive = "3"`) with `async`, `chrono_date`, `serde-json`, `napi4`, `napi5`, `web_stream`, and `tokio_rt` features. The `chrono_date` feature makes `chrono::DateTime<Utc>` cross the FFI as a native JS `Date`.
- `build.rs` calls `napi_build::setup()` — standard napi-rs build script.
- `index.js` and `index.d.ts` are **auto-generated** by `napi build` (via `@napi-rs/cli`). Do not edit them manually.
- **Mirror structs are the single source of types**: `src/models.rs` defines `#[napi(object)]` structs that mirror **all** structured payloads — metadata (`DatasetMetadata`, `KeyValueStoreMetadata`, `RequestQueueMetadata`), records (`KeyValueStoreRecord`, `KeyValueStoreRecordMetadata`), and request-queue results (`DatasetItemsListPage`, `ProcessedRequest`, `UnprocessedRequest`, `AddRequestsResponse`) — each with a `From<&core::Type>`/`From<core::Type>` impl. Every binding method returns one of these typed structs (or `Option<…>` of one), so napi-rs auto-generates honest TypeScript interfaces (`accessedAt: Date`, `processedRequests: Array<ProcessedRequest>`, etc.) with **no hand-written `.d.ts` fragment** — the old `dts-header.d.ts` + `dtsHeaderFile` mechanism is gone. Metadata datetime fields cross as native `Date` via the `chrono_date` feature; the structs deliberately omit `use_nullable = true` so `Option<String>` (the `name` field) serializes as an optional `name?: string` rather than `name: string | null`, matching crawlee's `DatasetInfo`/`KeyValueStoreInfo`/`RequestQueueInfo` in `@crawlee/types`.
- **Opaque user JSON** (dataset items, request bodies) stays `serde_json::Value` across the FFI and is typed via `#[napi(ts_type = "...")]` on the relevant struct field or `#[napi(ts_args_type = "...")]`/`ts_return_type` on the method — e.g. `DatasetItemsListPage.items` is `Record<string, unknown>[]`, and `getRequest`/`fetchNextRequest` return `Record<string, unknown> | null`. These are the only places a TS type is written by hand, and they correctly describe arbitrary JSON.
- **lib.js-only wrappers**: `lib.d.ts` declares the types that have no Rust counterpart (the `Symbol.asyncIterator` glue on the iterators, and `getValueStream`/`setValueStream` + `KeyValueStoreStreamRecord`) by **augmenting** the napi-generated classes via `declare module './index.js'`, then re-exporting everything. `package.json` `"types"` points at `lib.d.ts`.
- **camelCase convention**: napi-rs auto-camelCases `snake_case` field names on `#[napi(object)]` structs when generating the d.ts and the JS object keys. The core Rust library uses per-field `#[serde(rename = "camelCase")]` so on-disk JSON also matches.
- Each Rust client is wrapped in `Arc` so it can be cloned into async blocks.
- Non-metadata JSON data crosses the FFI as `serde_json::Value` ↔ JS objects (via napi's `serde-json` feature).
- KVS values are `Buffer`-only in the Node bindings. `setValue` accepts `napi::bindgen_prelude::Buffer` directly. `getValue` returns a `KeyValueStoreRecord` mirror struct whose `value` field crosses the FFI as a `Buffer` directly.
- KVS streaming is supported: `getValueStream` returns a Web `ReadableStream<Uint8Array>` (created via `fs.createReadStream` + `Readable.toWeb` in the JS wrapper), and `setValueStream` pipes a `ReadableStream` directly to a temp file on disk (via `Writable.toWeb`), then calls a Rust method to atomically finalize it. No in-memory buffering.
- `lib.js` is the canonical entry point — it imports everything from `./index.js`, attaches `Symbol.asyncIterator` to the iterator classes, and adds `getValueStream`/`setValueStream`. `package.json` `"main"` points at `lib.js`.
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
- `pyo3` (with `chrono` feature) — Python FFI plus `DateTime`/`Duration` ↔ `datetime` bridging
- `pyo3-async-runtimes` — native async Python coroutines via tokio
- `pyo3-stub-gen` — `.pyi` stub generation (driven by `src/bin/stub_gen.rs`)
- `chrono` — datetime / duration types crossing the FFI
- `pytest` + `pytest-asyncio` (dev) — test suite

Node.js bindings (`crawlee-storage-node`):
- `napi` / `napi-derive` (with `chrono_date` feature) — Node.js FFI plus `DateTime<Utc>` → JS `Date`
- `napi-build` — build script for napi-rs
- `chrono` — datetime types crossing the FFI
- `oxlint` / `oxlint-tsgolint` — linting (with type-aware rules)
- `oxfmt` — formatting
- `vitest` — test framework
- `typescript` / `@types/node` — TypeScript support for tests
