use std::path::{Path, PathBuf};

use tokio::fs;
use tokio::sync::Mutex;
use tracing::warn;

use crate::clock::{system_clock, ClockRef};
use crate::models::{
    KeyValueStoreMetadata, KeyValueStoreRecord, KeyValueStoreRecordMetadata,
    KeyValueStoreValueFileInfo, KvsKeysPage, KvsListKeysResult,
};
use crate::utils::{
    atomic_write, crypto_random_object_id, encode_key, find_storage_by_id, json_dumps_value,
    validate_exclusive_args, validate_subdirectory, Result, StorageError, METADATA_FILENAME,
};

const STORAGE_SUBDIR: &str = "key_value_stores";
const DEFAULT_NAME: &str = "default";

/// Filesystem-backed key-value store client.
///
/// Stores each key as a pair of files: the value file and a metadata sidecar.
///
/// Directory layout:
/// ```text
/// {storage_dir}/key_value_stores/{name}/
/// ├── __metadata__.json
/// ├── {encoded_key}                      (value data)
/// ├── {encoded_key}.__metadata__.json    (record metadata sidecar)
/// └── ...
/// ```
pub struct FileSystemKeyValueStoreClient {
    metadata: Mutex<KeyValueStoreMetadata>,
    path: PathBuf,
    clock: ClockRef,
}

impl FileSystemKeyValueStoreClient {
    /// Open an existing KVS or create a new one.
    ///
    /// - `id`: Open by ID (scans directories for matching metadata).
    /// - `name`: Open by name (used as directory name, written to metadata).
    /// - `alias`: Open by alias (used as directory name, but NOT written to metadata).
    /// - `storage_dir`: Base storage directory (e.g., "./storage").
    ///
    /// At most one of `id`, `name`, or `alias` may be provided.
    ///
    /// Uses the default [`SystemClock`](crate::clock::SystemClock). To inject a
    /// custom clock (e.g. for tests), use [`open_with_clock`](Self::open_with_clock).
    pub async fn open(
        id: Option<String>,
        name: Option<String>,
        alias: Option<String>,
        storage_dir: &Path,
    ) -> Result<Self> {
        Self::open_with_clock(id, name, alias, storage_dir, system_clock()).await
    }

    /// Open an existing KVS or create a new one, using the supplied clock.
    pub async fn open_with_clock(
        id: Option<String>,
        name: Option<String>,
        alias: Option<String>,
        storage_dir: &Path,
        clock: ClockRef,
    ) -> Result<Self> {
        validate_exclusive_args(&id, &name, &alias)?;

        let path = if let Some(ref id_val) = id {
            find_storage_by_id(storage_dir, STORAGE_SUBDIR, id_val)
                .await?
                .ok_or_else(|| {
                    StorageError::NotFound(format!("Key-value store with id '{id_val}' not found"))
                })?
        } else {
            let base = storage_dir.join(STORAGE_SUBDIR);
            match name.as_deref().or(alias.as_deref()) {
                // A user-supplied name/alias must map to a single direct child.
                Some(dir_name) => validate_subdirectory(&base, dir_name)?,
                // The default name is a trusted constant, no validation needed.
                None => base.join(DEFAULT_NAME),
            }
        };

        let metadata_path = path.join(METADATA_FILENAME);

        let metadata = if metadata_path.exists() {
            let content = fs::read_to_string(&metadata_path).await?;
            serde_json::from_str::<KeyValueStoreMetadata>(&content)?
        } else {
            // Only `name` goes into metadata, not alias
            let new_id = id.unwrap_or_else(|| crypto_random_object_id(17));
            let mut meta = KeyValueStoreMetadata::new(new_id, name);
            let now = clock.now();
            meta.base.created_at = now;
            meta.base.modified_at = now;
            meta.base.accessed_at = now;
            fs::create_dir_all(&path).await?;
            let json = json_dumps_value(&meta)?;
            atomic_write(&metadata_path, json.as_bytes()).await?;
            meta
        };

        Ok(Self {
            metadata: Mutex::new(metadata),
            path,
            clock,
        })
    }

    /// Return a reference to this client's clock.
    pub fn clock(&self) -> &ClockRef {
        &self.clock
    }

    /// Get the store metadata.
    pub async fn get_metadata(&self) -> KeyValueStoreMetadata {
        self.metadata.lock().await.clone()
    }

    /// Path to the store directory.
    pub fn path(&self) -> &Path {
        &self.path
    }

    /// Path to the metadata file.
    pub fn metadata_path(&self) -> PathBuf {
        self.path.join(METADATA_FILENAME)
    }

    /// Delete the entire store directory.
    pub async fn drop_storage(&self) -> Result<()> {
        if self.path.exists() {
            fs::remove_dir_all(&self.path).await?;
        }
        Ok(())
    }

    /// Delete all value files but keep store metadata.
    ///
    /// Any key listed in `keep` is spared: both its value file and its metadata
    /// sidecar are left on disk. Matching is by exact key (encoded to its on-disk
    /// filename via [`encode_key`]) — no extension globbing or stem matching. A
    /// caller wanting to preserve, say, both `INPUT` and `INPUT.json` must pass
    /// both as separate keys. The store-level `__metadata__.json` is always kept.
    pub async fn purge(&self, keep: &[String]) -> Result<()> {
        let mut meta = self.metadata.lock().await;

        // Build the set of filenames to spare: the store metadata plus, for each
        // kept key, its value file and its per-record sidecar.
        let mut keep_files: std::collections::HashSet<String> = std::collections::HashSet::new();
        keep_files.insert(METADATA_FILENAME.to_string());
        for key in keep {
            let encoded = encode_key(key);
            keep_files.insert(format!("{encoded}.{METADATA_FILENAME}"));
            keep_files.insert(encoded);
        }

        match fs::read_dir(&self.path).await {
            Ok(mut entries) => {
                while let Some(entry) = entries.next_entry().await? {
                    let path = entry.path();
                    if path.is_file() {
                        if let Some(name) = path.file_name().and_then(|n| n.to_str()) {
                            if !keep_files.contains(name) {
                                let _ = fs::remove_file(&path).await;
                            }
                        }
                    }
                }
            }
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                // Directory doesn't exist yet — nothing to purge.
            }
            Err(e) => return Err(e.into()),
        }

        let now = self.clock.now();
        meta.base.accessed_at = now;
        meta.base.modified_at = now;

        let json = json_dumps_value(&*meta)?;
        atomic_write(&self.metadata_path(), json.as_bytes()).await?;

        Ok(())
    }

    /// Delete a value by key.
    pub async fn delete_value(&self, key: &str) -> Result<()> {
        let encoded = encode_key(key);
        let value_path = self.path.join(&encoded);
        let sidecar_path = self.path.join(format!("{encoded}.{METADATA_FILENAME}"));

        if value_path.exists() {
            fs::remove_file(&value_path).await?;
        }
        if sidecar_path.exists() {
            fs::remove_file(&sidecar_path).await?;
        }

        // Update store metadata
        {
            let mut meta = self.metadata.lock().await;
            let now = self.clock.now();
            meta.base.accessed_at = now;
            meta.base.modified_at = now;
            let json = json_dumps_value(&*meta)?;
            atomic_write(&self.metadata_path(), json.as_bytes()).await?;
        }

        Ok(())
    }

    /// Fetch the next page of keys for lazy iteration.
    ///
    /// Returns a [`KvsKeysPage`] containing key metadata entries and a flag
    /// indicating whether more keys are available. The binding layer should
    /// call this repeatedly, using the last key returned as
    /// `exclusive_start_key` for the next call, until `has_more` is `false`.
    ///
    /// `page_size` controls how many keys are read per call (default 1000).
    ///
    /// `bare_fallbacks` lets the caller surface out-of-band ("bare") value files
    /// that have no metadata sidecar (e.g. a CLI-written `INPUT.json`) as regular
    /// keys. Each entry is `(name, content_type)` where `name` is the file's
    /// on-disk key (e.g. `"INPUT.json"`): if that file exists with no tracked
    /// sidecar, it is folded into the listing under `name` with the declared
    /// `content_type` (an empty string keeps the synthesized
    /// `application/octet-stream`) and a `size` stated from the file. The core
    /// does **no** MIME inference: the "which files are input, what type a
    /// `.json` implies" policy stays at the caller. Pass an empty slice to list
    /// only tracked records.
    ///
    /// **Round-trip caveat:** a surfaced bare key does **not** round-trip through
    /// the strict read path. The listed key is the literal on-disk `name`, but
    /// [`get_value`](Self::get_value) / [`record_exists`](Self::record_exists)
    /// only ever see tracked records (value file + sidecar) and therefore return
    /// `None` / `false` for a sidecar-less bare file. To read a listed bare key
    /// back, go through [`resolve_value`](Self::resolve_value) /
    /// [`resolve_existing_key`](Self::resolve_existing_key) (with an empty-string
    /// extension fallback for the literal `name`), not `get_value`.
    pub async fn iterate_keys_page(
        &self,
        exclusive_start_key: Option<&str>,
        limit: Option<usize>,
        page_size: usize,
        prefix: Option<&str>,
        bare_fallbacks: &[(&str, &str)],
    ) -> Result<KvsKeysPage> {
        // Fetch one extra beyond the page to detect whether more keys exist.
        let fetch_limit = match limit {
            Some(remaining) => page_size.min(remaining),
            None => page_size,
        };

        let results = self
            .list_keys_raw(
                exclusive_start_key,
                Some(fetch_limit + 1),
                prefix,
                bare_fallbacks,
            )
            .await?;

        let has_more =
            results.len() > fetch_limit && limit.is_none_or(|remaining| fetch_limit < remaining);
        let items: Vec<KeyValueStoreRecordMetadata> =
            results.into_iter().take(fetch_limit).collect();

        Ok(KvsKeysPage { items, has_more })
    }

    /// List a single self-describing page of keys.
    ///
    /// Returns a [`KvsListKeysResult`] mirroring crawlee's
    /// `KeyValueStoreListKeysResult` contract (upstream crawlee PR #3800): the
    /// page's `items` bundled with the echoed request cursor/limit, a
    /// truncation flag, and the derived next cursor. Unlike
    /// [`iterate_keys_page`](Self::iterate_keys_page) (which is the primitive
    /// behind lazy cursor iteration and returns a bare `Vec` + `has_more`), this
    /// hands the binding layer everything it needs to emit the crawlee page
    /// shape in one shot, so the JS/Python wrappers don't have to re-derive the
    /// next cursor or re-count.
    ///
    /// `limit` bounds the page size (defaults to 1000 when `None`). It is both
    /// the fetch size for this single page *and* the value echoed back as
    /// `KvsListKeysResult::limit`.
    ///
    /// `prefix` and `bare_fallbacks` behave exactly as in
    /// [`iterate_keys_page`](Self::iterate_keys_page) — the same shared
    /// pagination + bare-file dedup logic in
    /// [`list_keys_raw`](Self::list_keys_raw) backs both. In particular, a bare
    /// file whose *encoded on-disk value-file name* collides with a tracked
    /// record is dropped (the tracked record wins). Note this is a
    /// name-for-name collision: a tracked record under the logical key `INPUT`
    /// does **not** shadow a bare `INPUT.json` (different on-disk names) — that
    /// policy, if desired, is expressed by which `bare_fallbacks` the caller
    /// passes, not by the core (which does no MIME/extension inference).
    ///
    /// Field derivation:
    /// - `count` = `items.len()`.
    /// - `limit` = the resolved per-page limit (echoed).
    /// - `exclusive_start_key` = the caller's supplied cursor, echoed back.
    /// - `is_truncated` = whether more keys exist beyond this page (from
    ///   [`iterate_keys_page`](Self::iterate_keys_page)'s `has_more`).
    /// - `next_exclusive_start_key` = the last item's key when `is_truncated`,
    ///   else `None`.
    pub async fn list_keys(
        &self,
        exclusive_start_key: Option<&str>,
        limit: Option<usize>,
        prefix: Option<&str>,
        bare_fallbacks: &[(&str, &str)],
    ) -> Result<KvsListKeysResult> {
        let page_limit = limit.unwrap_or(1000);

        // Reuse the shared pagination + bare-file dedup with `page_size` set to
        // the requested limit and no overall `limit` budget: we want a single
        // page bounded by `page_limit`, whose `has_more` means "keys exist
        // beyond this page". (Passing `limit == page_size` instead would make
        // `iterate_keys_page` treat the limit as an exhausted total budget and
        // report `has_more = false` on an exactly-full page — the wrong signal
        // for a truncation flag.)
        let page = self
            .iterate_keys_page(
                exclusive_start_key,
                None,
                page_limit,
                prefix,
                bare_fallbacks,
            )
            .await?;

        let is_truncated = page.has_more;
        // The next cursor is the last key of this page, but only when there is
        // a next page to fetch.
        let next_exclusive_start_key = if is_truncated {
            page.items.last().map(|m| m.key.clone())
        } else {
            None
        };

        Ok(KvsListKeysResult {
            count: page.items.len(),
            items: page.items,
            limit: page_limit,
            exclusive_start_key: exclusive_start_key.map(str::to_string),
            is_truncated,
            next_exclusive_start_key,
        })
    }

    /// Internal helper: list keys with cursor, limit and optional prefix,
    /// returning a flat Vec. Keys are filtered by `prefix` (on the decoded key,
    /// not the encoded filename) before the cursor and limit are applied, so the
    /// page's `limit`/`has_more` accounting only ever counts matching keys.
    ///
    /// Tracked records (value file + sidecar) and caller-declared bare files (see
    /// [`iterate_keys_page`](Self::iterate_keys_page) for the `(name,
    /// content_type)` shape) are merged into a single stream, sorted by encoded
    /// value-file name, before the prefix/cursor/limit logic runs — so pagination
    /// treats both kinds uniformly. A bare file whose on-disk name already has a
    /// tracked record is dropped (the tracked record wins).
    async fn list_keys_raw(
        &self,
        exclusive_start_key: Option<&str>,
        limit: Option<usize>,
        prefix: Option<&str>,
        bare_fallbacks: &[(&str, &str)],
    ) -> Result<Vec<KeyValueStoreRecordMetadata>> {
        let mut results = Vec::new();
        let metadata_suffix = format!(".{METADATA_FILENAME}");

        // Each candidate carries the encoded value-file name it sorts by, plus
        // its (already finalized) metadata. `None` metadata means "read the
        // sidecar lazily" — only tracked records defer; bare files are resolved
        // eagerly up front (a handful of cheap stats).
        struct Candidate {
            /// Encoded value-file name, used purely for deterministic ordering.
            sort_name: String,
            /// Sidecar path to read lazily for tracked records; `None` for bare
            /// files whose metadata is already finalized in `meta`.
            sidecar_path: Option<PathBuf>,
            /// Pre-resolved metadata for bare files; `None` for tracked records.
            meta: Option<KeyValueStoreRecordMetadata>,
        }

        let mut candidates: Vec<Candidate> = Vec::new();
        // Encoded value-file names already claimed by a tracked record, so a bare
        // file pointing at the same on-disk name is not surfaced twice.
        let mut tracked_value_names: std::collections::HashSet<String> =
            std::collections::HashSet::new();

        match fs::read_dir(&self.path).await {
            Ok(mut entries) => {
                while let Some(entry) = entries.next_entry().await? {
                    let path = entry.path();
                    if !path.is_file() {
                        continue;
                    }
                    let Some(name) = path.file_name().and_then(|n| n.to_str()) else {
                        continue;
                    };
                    // Find sidecar files (but not the store-level metadata).
                    if name.ends_with(&metadata_suffix) && name != METADATA_FILENAME {
                        let sort_name = name
                            .strip_suffix(&metadata_suffix)
                            .unwrap_or(name)
                            .to_string();
                        tracked_value_names.insert(sort_name.clone());
                        candidates.push(Candidate {
                            sort_name,
                            sidecar_path: Some(path),
                            meta: None,
                        });
                    }
                }
            }
            // A missing store directory has no tracked records, but bare files
            // are resolved by explicit path below — so don't early-return here.
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
            Err(e) => return Err(e.into()),
        }

        // Resolve caller-declared bare files: each `name` is the file's literal
        // on-disk key. Probe it on disk, skip any that already have a tracked
        // record (the tracked record wins), and dedupe by name (first declared
        // fallback for a name wins). The surfaced key is the literal `name`.
        let mut seen_bare_names: std::collections::HashSet<&str> = std::collections::HashSet::new();
        for (name, content_type) in bare_fallbacks {
            if !seen_bare_names.insert(*name) {
                continue;
            }
            let candidate_name = encode_key(name);
            // A tracked record (value file + sidecar) for the same on-disk name
            // takes precedence over the bare file.
            if tracked_value_names.contains(&candidate_name) {
                continue;
            }
            let value_path = self.path.join(&candidate_name);
            let Ok(file_meta) = fs::metadata(&value_path).await else {
                continue;
            };
            if !file_meta.is_file() {
                continue;
            }
            let resolved_type = if content_type.is_empty() {
                "application/octet-stream".to_string()
            } else {
                (*content_type).to_string()
            };
            candidates.push(Candidate {
                sort_name: candidate_name,
                sidecar_path: None,
                meta: Some(KeyValueStoreRecordMetadata {
                    key: (*name).to_string(),
                    content_type: resolved_type,
                    size: Some(file_meta.len() as usize),
                }),
            });
        }

        // Sort by encoded value-file name for deterministic ordering (same basis
        // the old sidecar-path sort used, now shared with bare files).
        candidates.sort_by(|a, b| a.sort_name.cmp(&b.sort_name));

        // Track whether a key exactly equal to `exclusive_start_key` was seen
        // among the (prefix-filtered) candidates. The cursor key is skipped from
        // the results by the `<= start_key` filter, so we observe it separately —
        // and over the *whole* candidate set, not just the keys under `limit`.
        let mut cursor_key_seen = false;

        for candidate in candidates {
            // Resolve the metadata: pre-finalized for bare files, lazily read
            // from the sidecar for tracked records.
            let mut record_meta = match candidate.meta {
                Some(meta) => meta,
                None => {
                    let sidecar_path = candidate
                        .sidecar_path
                        .as_ref()
                        .expect("tracked candidate has a sidecar path");
                    let content = fs::read_to_string(sidecar_path).await?;
                    match serde_json::from_str::<KeyValueStoreRecordMetadata>(&content) {
                        Ok(meta) => meta,
                        Err(e) => {
                            warn!(
                                "Failed to parse sidecar metadata {}: {}",
                                sidecar_path.display(),
                                e
                            );
                            continue;
                        }
                    }
                }
            };

            // Apply prefix filter (on the decoded key)
            if let Some(prefix) = prefix {
                if !record_meta.key.starts_with(prefix) {
                    continue;
                }
            }

            // Cursor existence check: does this (prefix-scoped) key
            // exactly match the supplied cursor? Recorded before the
            // `<=` filter drops it from the page.
            if let Some(start_key) = exclusive_start_key {
                if record_meta.key.as_str() == start_key {
                    cursor_key_seen = true;
                }
            }

            // Apply cursor filter. Note we keep scanning the remaining
            // candidates even once the page is full, because the cursor
            // key may sort after the page boundary and we still need to
            // confirm it exists.
            if let Some(start_key) = exclusive_start_key {
                if record_meta.key.as_str() <= start_key {
                    continue;
                }
            }

            // Once the page is full, stop *collecting* further results,
            // but keep iterating to validate the cursor (unless there is
            // no cursor to validate, in which case we can stop now).
            if let Some(lim) = limit {
                if results.len() >= lim {
                    if exclusive_start_key.is_some() && !cursor_key_seen {
                        continue;
                    }
                    break;
                }
            }

            // Backfill `size` for foreign/legacy sidecars that omit it
            // (this library always writes it, but crawlee-JS / older
            // Python clients may not) by stating the value file. Bare files
            // already carry a stated `size`, so this only ever touches
            // tracked records. The value file is the sidecar path minus the
            // `.{METADATA_FILENAME}` suffix.
            if record_meta.size.is_none() {
                if let Some(sidecar_path) = candidate.sidecar_path.as_ref() {
                    let value_path = sidecar_path
                        .to_string_lossy()
                        .strip_suffix(&metadata_suffix)
                        .map(PathBuf::from);
                    if let Some(value_path) = value_path {
                        if let Ok(file_meta) = fs::metadata(&value_path).await {
                            record_meta.size = Some(file_meta.len() as usize);
                        }
                    }
                }
            }

            results.push(record_meta);
        }

        // A supplied cursor that never matched an existing (prefix-scoped) key
        // is an error — distinct from "all keys are <= cursor".
        if let Some(start_key) = exclusive_start_key {
            if !cursor_key_seen {
                return Err(StorageError::ExclusiveStartKeyNotFound(
                    start_key.to_string(),
                ));
            }
        }

        Ok(results)
    }

    /// Get a `file://` URL for a key, or `None` if no value file exists for it
    /// (a single existence `stat` on the encoded path).
    ///
    /// Does **not** probe the conventional bare-file extensions: that *policy*
    /// lives at the caller, which resolves the on-disk key via
    /// [`resolve_existing_key`](Self::resolve_existing_key) and hands the matched
    /// key here, preserving the "core = mechanism, caller = policy" split.
    pub async fn get_public_url(&self, key: &str) -> Option<String> {
        let encoded = encode_key(key);
        let value_path = self.path.join(&encoded);
        if fs::metadata(&value_path).await.is_ok() {
            Some(format!("file://{}", value_path.display()))
        } else {
            None
        }
    }

    /// Get the file path and metadata for a record, without reading its contents.
    ///
    /// Returns `(value_path, record_metadata)` if the key exists, or `None` if it doesn't.
    /// This is useful for streaming reads — the binding layer can open the file
    /// and stream it directly instead of buffering the entire contents.
    ///
    /// The client is a pure byte transport: it returns the raw value bytes (via
    /// the path) and the verbatim `content_type` from the sidecar. Parsing and
    /// value semantics live at the `KeyValueStore` frontend.
    ///
    /// When `require_record_metadata` is `true` (the normal case), a record is
    /// only returned if it has a metadata sidecar; a value file without one is
    /// treated as absent. When `false`, a value file with no sidecar is still
    /// returned, with synthesized metadata: `content_type` is the generic
    /// `application/octet-stream` sentinel (the client never infers a type from
    /// the file extension — that foreign-file convention lives at the frontend)
    /// and `size` is the value-file length. This is the escape hatch for reading
    /// out-of-band files (e.g. a CLI-written `INPUT.json` that has no sidecar).
    pub async fn get_value(
        &self,
        key: &str,
        require_record_metadata: bool,
    ) -> Result<Option<(PathBuf, KeyValueStoreRecordMetadata)>> {
        let encoded = encode_key(key);
        let value_path = self.path.join(&encoded);
        let sidecar_path = self.path.join(format!("{encoded}.{METADATA_FILENAME}"));

        // Always update accessed_at on read, even for missing keys
        {
            let mut meta = self.metadata.lock().await;
            meta.base.accessed_at = self.clock.now();
            let json = json_dumps_value(&*meta)?;
            atomic_write(&self.metadata_path(), json.as_bytes()).await?;
        }

        // The value file is always required.
        if !value_path.exists() {
            return Ok(None);
        }

        if sidecar_path.exists() {
            let sidecar_content = fs::read_to_string(&sidecar_path).await?;
            let mut record_meta: KeyValueStoreRecordMetadata =
                serde_json::from_str(&sidecar_content)?;

            // Backfill `size` for foreign/legacy sidecars that omit it (this
            // library always writes it, but crawlee-JS / older Python clients may
            // not) by stating the value file.
            if record_meta.size.is_none() {
                if let Ok(file_meta) = fs::metadata(&value_path).await {
                    record_meta.size = Some(file_meta.len() as usize);
                }
            }

            return Ok(Some((value_path, record_meta)));
        }

        // No sidecar. By default that means "not a record"; with the opt-in flag
        // we still serve the bytes, synthesizing dumb metadata (no type inference).
        if require_record_metadata {
            return Ok(None);
        }

        let size = fs::metadata(&value_path)
            .await
            .ok()
            .map(|file_meta| file_meta.len() as usize);
        let record_meta = KeyValueStoreRecordMetadata {
            key: key.to_string(),
            content_type: "application/octet-stream".to_string(),
            size,
        };

        Ok(Some((value_path, record_meta)))
    }

    /// Read a tracked record (value file + metadata sidecar) by key, returning
    /// its raw bytes alongside metadata with a *guaranteed* non-optional `size`.
    ///
    /// This is the read counterpart that binding layers should use instead of
    /// calling [`get_value`](Self::get_value) and re-reading the file + patching
    /// `size` themselves (each binding previously did this slightly differently).
    /// The `size` is finalized once, here: the sidecar's value if present, else
    /// the length of the bytes just read. Returns `None` for a key with no
    /// tracked record (strict — like `get_value(key, true)`).
    pub async fn read_value(&self, key: &str) -> Result<Option<KeyValueStoreRecord>> {
        match self.get_value(key, true).await? {
            Some((path, meta)) => Ok(Some(self.read_record_bytes(path, meta).await?)),
            None => Ok(None),
        }
    }

    /// Read the bytes for a `(path, metadata)` pair (as returned by
    /// [`get_value`](Self::get_value) / [`resolve_value`](Self::resolve_value))
    /// into a [`KeyValueStoreRecord`], finalizing `size` from the byte count
    /// read when the sidecar didn't carry one. This is the single place the
    /// "size is always populated" invariant is enforced.
    async fn read_record_bytes(
        &self,
        path: PathBuf,
        meta: KeyValueStoreRecordMetadata,
    ) -> Result<KeyValueStoreRecord> {
        let value = fs::read(&path).await?;
        // `get_value`/`resolve_value` already backfill `size` from the value
        // file, so it's normally present; fall back to the bytes we just read
        // to keep the non-optional guarantee airtight.
        let size = meta.size.unwrap_or(value.len());
        Ok(KeyValueStoreRecord {
            key: meta.key,
            content_type: meta.content_type,
            size,
            value,
        })
    }

    /// Get the value file path + finalized metadata for a record without
    /// reading its bytes — for streaming reads. Like
    /// [`read_value`](Self::read_value), `size` is guaranteed non-optional
    /// (stated from the value file when the sidecar omits it). Returns `None`
    /// for a key with no tracked record.
    pub async fn value_file_info(&self, key: &str) -> Result<Option<KeyValueStoreValueFileInfo>> {
        match self.get_value(key, true).await? {
            Some((path, meta)) => {
                let size = match meta.size {
                    Some(size) => size,
                    None => fs::metadata(&path)
                        .await
                        .map(|m| m.len() as usize)
                        .unwrap_or(0),
                };
                Ok(Some(KeyValueStoreValueFileInfo {
                    key: meta.key,
                    content_type: meta.content_type,
                    size,
                    path,
                }))
            }
            None => Ok(None),
        }
    }

    /// Like [`resolve_value`](Self::resolve_value), but reads the matched value
    /// file into a [`KeyValueStoreRecord`] (bytes + non-optional `size`) in one
    /// call. This is the bare-file-aware read counterpart binding layers should
    /// use so they don't re-implement the read + size-finalization.
    pub async fn resolve_and_read_value(
        &self,
        key: &str,
        bare_fallbacks: &[(&str, &str)],
    ) -> Result<Option<KeyValueStoreRecord>> {
        match self.resolve_value(key, bare_fallbacks).await? {
            Some((path, meta)) => Ok(Some(self.read_record_bytes(path, meta).await?)),
            None => Ok(None),
        }
    }

    /// Resolve a key to a value, transparently falling back to out-of-band
    /// ("bare") value files that have no metadata sidecar.
    ///
    /// This bundles the lookup that binding layers would otherwise hand-roll: a
    /// run's input may be a properly-tracked record, or an out-of-band file a
    /// CLI/platform dropped on disk under one of several conventional names
    /// (`INPUT`, `INPUT.json`, `INPUT.bin`, ...). The probe order is:
    ///
    /// 1. The tracked record for the literal `key` (value file + sidecar). Its
    ///    `content_type` comes verbatim from the sidecar.
    /// 2. For each `(extension, content_type)` in `bare_fallbacks`, the bare
    ///    file at `key + extension` (no sidecar required). On a match the
    ///    supplied `content_type` is used.
    ///
    /// The first match wins. The returned [`KeyValueStoreRecordMetadata`] is
    /// always keyed by the originally-requested `key` (never the on-disk
    /// filename of a matched bare file), so callers see a stable key.
    ///
    /// The core still performs **no** MIME inference of its own: the caller
    /// declares which extensions to probe and what content type each implies
    /// (the `(extension, content_type)` pairs). That keeps the "which files are
    /// input, and what type is a `.json`" policy at the frontend while the
    /// probing/lookup mechanism lives here, shared by every binding.
    ///
    /// Returns `(value_path, metadata)` for the first match, or `None`.
    pub async fn resolve_value(
        &self,
        key: &str,
        bare_fallbacks: &[(&str, &str)],
    ) -> Result<Option<(PathBuf, KeyValueStoreRecordMetadata)>> {
        // 1. Tracked record for the literal key — sidecar content type wins.
        if let Some(result) = self.get_value(key, true).await? {
            return Ok(Some(result));
        }

        // 2. Out-of-band bare files: probe each conventional extension.
        for (extension, content_type) in bare_fallbacks {
            let candidate = format!("{key}{extension}");
            if let Some((path, mut meta)) = self.get_value(&candidate, false).await? {
                // Re-key to the requested key and apply the caller-declared
                // content type for this extension. An empty extension (the
                // literal key) keeps the synthesized `application/octet-stream`
                // unless the caller declared something else.
                meta.key = key.to_string();
                if !content_type.is_empty() {
                    meta.content_type = (*content_type).to_string();
                }
                return Ok(Some((path, meta)));
            }
        }

        Ok(None)
    }

    /// Check whether a key resolves to a value, using the same fallback probe
    /// order as [`resolve_value`](Self::resolve_value) but without reading the
    /// value file. Returns the matched on-disk key (the literal key or a bare
    /// `key + extension`), or `None` if nothing exists.
    ///
    /// The matched key is what a caller should pass to
    /// [`get_public_url`](Self::get_public_url) so the URL points at the file
    /// that actually exists.
    pub async fn resolve_existing_key(&self, key: &str, bare_fallbacks: &[&str]) -> Option<String> {
        if self.record_exists(key, true).await {
            return Some(key.to_string());
        }
        for extension in bare_fallbacks {
            let candidate = format!("{key}{extension}");
            if self.record_exists(&candidate, false).await {
                return Some(candidate);
            }
        }
        None
    }

    /// Write raw bytes for a key, with sidecar metadata and atomic write.
    ///
    /// The client is a pure byte transport: `data` is written verbatim and
    /// `content_type` is stored as-is in the sidecar — no inference, no
    /// serialization. Value semantics live at the `KeyValueStore` frontend.
    pub async fn set_value(&self, key: &str, data: &[u8], content_type: String) -> Result<()> {
        let encoded = encode_key(key);
        let value_path = self.path.join(&encoded);
        let sidecar_path = self.path.join(format!("{encoded}.{METADATA_FILENAME}"));

        atomic_write(&value_path, data).await?;

        let record_meta = KeyValueStoreRecordMetadata {
            key: key.to_string(),
            content_type,
            size: Some(data.len()),
        };
        let sidecar_json = json_dumps_value(&record_meta)?;
        atomic_write(&sidecar_path, sidecar_json.as_bytes()).await?;

        {
            let mut meta = self.metadata.lock().await;
            let now = self.clock.now();
            meta.base.accessed_at = now;
            meta.base.modified_at = now;
            let json = json_dumps_value(&*meta)?;
            atomic_write(&self.metadata_path(), json.as_bytes()).await?;
        }

        Ok(())
    }

    /// Get a path for a new temp file in the store directory.
    ///
    /// The binding layer uses this to stream data to a temp file, then calls
    /// [`finalize_streamed_value`] to atomically move it into place.
    pub fn temp_file_path(&self) -> PathBuf {
        self.path
            .join(format!(".tmp.{}", crypto_random_object_id(12)))
    }

    /// Finalize a streamed write: atomically rename `temp_path` to the value
    /// file for `key`, write the sidecar metadata, and update store metadata.
    ///
    /// The caller is responsible for having already written the full value data
    /// to `temp_path` (e.g. by piping a stream to it).
    pub async fn finalize_streamed_value(
        &self,
        key: &str,
        temp_path: &Path,
        size: usize,
        content_type: String,
    ) -> Result<()> {
        let encoded = encode_key(key);
        let value_path = self.path.join(&encoded);
        let sidecar_path = self.path.join(format!("{encoded}.{METADATA_FILENAME}"));

        // Atomic rename from temp → final value path
        fs::rename(temp_path, &value_path).await?;

        // Write sidecar metadata
        let record_meta = KeyValueStoreRecordMetadata {
            key: key.to_string(),
            content_type,
            size: Some(size),
        };
        let sidecar_json = json_dumps_value(&record_meta)?;
        atomic_write(&sidecar_path, sidecar_json.as_bytes()).await?;

        // Update store metadata
        {
            let mut meta = self.metadata.lock().await;
            let now = self.clock.now();
            meta.base.accessed_at = now;
            meta.base.modified_at = now;
            let json = json_dumps_value(&*meta)?;
            atomic_write(&self.metadata_path(), json.as_bytes()).await?;
        }

        Ok(())
    }

    /// Check if a record exists for a key.
    ///
    /// When `require_record_metadata` is `true`, both the value file and its
    /// metadata sidecar must exist. When `false`, a value file alone counts —
    /// matching the relaxed [`get_value`](Self::get_value) lookup for reading
    /// out-of-band files that have no sidecar.
    pub async fn record_exists(&self, key: &str, require_record_metadata: bool) -> bool {
        let encoded = encode_key(key);
        let value_path = self.path.join(&encoded);
        if !value_path.exists() {
            return false;
        }
        if !require_record_metadata {
            return true;
        }
        let sidecar_path = self.path.join(format!("{encoded}.{METADATA_FILENAME}"));
        sidecar_path.exists()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use tempfile::TempDir;

    /// Read back the raw value bytes + content type for a key via the byte-only
    /// `get_value` (which returns the value path + sidecar metadata).
    async fn read_back(
        client: &FileSystemKeyValueStoreClient,
        key: &str,
    ) -> Option<(Vec<u8>, String, Option<usize>)> {
        let (path, meta) = client.get_value(key, true).await.unwrap()?;
        let bytes = tokio::fs::read(&path).await.unwrap();
        Some((bytes, meta.content_type, meta.size))
    }

    #[tokio::test]
    async fn test_on_disk_sidecar_uses_camel_case() {
        let temp_dir = TempDir::new().unwrap();
        let storage_dir = temp_dir.path();

        let client = FileSystemKeyValueStoreClient::open(None, None, None, storage_dir)
            .await
            .unwrap();

        client
            .set_value(
                "test-key",
                br#"{"x":1}"#,
                "application/json; charset=utf-8".to_string(),
            )
            .await
            .unwrap();

        // Read the sidecar metadata
        let encoded = encode_key("test-key");
        let sidecar_path = client.path().join(format!("{encoded}.{METADATA_FILENAME}"));
        let raw = tokio::fs::read_to_string(&sidecar_path).await.unwrap();
        let parsed: serde_json::Value = serde_json::from_str(&raw).unwrap();
        let obj = parsed.as_object().unwrap();

        assert!(
            obj.contains_key("contentType"),
            "expected 'contentType', got: {raw}"
        );
        assert!(
            !obj.contains_key("content_type"),
            "unexpected 'content_type'"
        );

        // Store metadata should also be camelCase
        let store_raw = tokio::fs::read_to_string(client.metadata_path())
            .await
            .unwrap();
        let store_parsed: serde_json::Value = serde_json::from_str(&store_raw).unwrap();
        let store_obj = store_parsed.as_object().unwrap();
        assert!(
            store_obj.contains_key("accessedAt"),
            "expected 'accessedAt' in store metadata, got: {store_raw}"
        );
    }

    #[tokio::test]
    async fn test_json_bytes_roundtrip() {
        let temp_dir = TempDir::new().unwrap();
        let storage_dir = temp_dir.path();

        let client = FileSystemKeyValueStoreClient::open(None, None, None, storage_dir)
            .await
            .unwrap();

        // The client is byte transport: the frontend already serialized this.
        let payload = br#"{"hello":"world"}"#;
        client
            .set_value(
                "my-key",
                payload,
                "application/json; charset=utf-8".to_string(),
            )
            .await
            .unwrap();

        let (bytes, content_type, size) = read_back(&client, "my-key").await.unwrap();
        assert_eq!(bytes, payload);
        assert_eq!(content_type, "application/json; charset=utf-8");
        assert_eq!(size, Some(payload.len()));
    }

    #[tokio::test]
    async fn test_text_bytes_roundtrip() {
        let temp_dir = TempDir::new().unwrap();
        let storage_dir = temp_dir.path();

        let client = FileSystemKeyValueStoreClient::open(None, None, None, storage_dir)
            .await
            .unwrap();

        client
            .set_value(
                "greeting",
                b"hello",
                "text/plain; charset=utf-8".to_string(),
            )
            .await
            .unwrap();

        let (bytes, content_type, _) = read_back(&client, "greeting").await.unwrap();
        assert_eq!(bytes, b"hello");
        assert_eq!(content_type, "text/plain; charset=utf-8");
    }

    #[tokio::test]
    async fn test_null_sentinel_roundtrip() {
        let temp_dir = TempDir::new().unwrap();
        let storage_dir = temp_dir.path();

        let client = FileSystemKeyValueStoreClient::open(None, None, None, storage_dir)
            .await
            .unwrap();

        // Null is represented by the frontend as empty bytes + the sentinel CT.
        client
            .set_value("empty", b"", crate::NONE_CONTENT_TYPE.to_string())
            .await
            .unwrap();

        let (bytes, content_type, size) = read_back(&client, "empty").await.unwrap();
        assert!(bytes.is_empty());
        assert_eq!(content_type, crate::NONE_CONTENT_TYPE);
        assert_eq!(size, Some(0));
    }

    #[tokio::test]
    async fn test_size_backfilled_for_legacy_sidecar_without_size() {
        // crawlee-JS MemoryStorage and older Python FileSystemStorageClient
        // wrote sidecars that omit `size`. On read, the client must backfill it
        // from the actual value-file length rather than surfacing `None`.
        let temp_dir = TempDir::new().unwrap();
        let storage_dir = temp_dir.path();

        let client = FileSystemKeyValueStoreClient::open(None, None, None, storage_dir)
            .await
            .unwrap();

        // Hand-write a value file + a sidecar that has no `size` field.
        let key = "legacy-key";
        let payload = b"twelve bytes";
        let encoded = encode_key(key);
        let value_path = client.path().join(&encoded);
        let sidecar_path = client.path().join(format!("{encoded}.{METADATA_FILENAME}"));
        tokio::fs::write(&value_path, payload).await.unwrap();
        tokio::fs::write(
            &sidecar_path,
            br#"{"key":"legacy-key","contentType":"text/plain"}"#,
        )
        .await
        .unwrap();

        // get_value backfills from the file length.
        let (_, meta) = client.get_value(key, true).await.unwrap().unwrap();
        assert_eq!(meta.size, Some(payload.len()));

        // The list/iterate path backfills too.
        let page = client
            .iterate_keys_page(None, None, 1000, None, &[])
            .await
            .unwrap();
        let entry = page.items.iter().find(|m| m.key == key).unwrap();
        assert_eq!(entry.size, Some(payload.len()));
    }

    #[tokio::test]
    async fn test_binary_bytes_roundtrip() {
        let temp_dir = TempDir::new().unwrap();
        let storage_dir = temp_dir.path();

        let client = FileSystemKeyValueStoreClient::open(None, None, None, storage_dir)
            .await
            .unwrap();

        // Store binary data verbatim — no encoding, no inference.
        let raw_bytes: Vec<u8> = vec![0x00, 0x01, 0x02, 0x03, 0x04, 0x89, 0xFF];

        client
            .set_value(
                "binary-key",
                &raw_bytes,
                "application/octet-stream".to_string(),
            )
            .await
            .unwrap();

        let (bytes, content_type, size) = read_back(&client, "binary-key").await.unwrap();
        assert_eq!(bytes, raw_bytes);
        assert_eq!(content_type, "application/octet-stream");
        assert_eq!(size, Some(raw_bytes.len()));
    }

    #[tokio::test]
    async fn test_content_type_stored_verbatim() {
        let temp_dir = TempDir::new().unwrap();
        let storage_dir = temp_dir.path();

        let client = FileSystemKeyValueStoreClient::open(None, None, None, storage_dir)
            .await
            .unwrap();

        // The client must NOT infer or rewrite the content type — even a totally
        // arbitrary one passes through untouched.
        client
            .set_value(
                "weird",
                b"<svg/>",
                "image/svg+xml; charset=utf-8".to_string(),
            )
            .await
            .unwrap();

        let (bytes, content_type, _) = read_back(&client, "weird").await.unwrap();
        assert_eq!(bytes, b"<svg/>");
        assert_eq!(content_type, "image/svg+xml; charset=utf-8");
    }

    #[tokio::test]
    async fn test_get_missing_key_returns_none() {
        let temp_dir = TempDir::new().unwrap();
        let storage_dir = temp_dir.path();

        let client = FileSystemKeyValueStoreClient::open(None, None, None, storage_dir)
            .await
            .unwrap();

        assert!(client.get_value("nope", true).await.unwrap().is_none());
    }

    #[tokio::test]
    async fn test_delete_value() {
        let temp_dir = TempDir::new().unwrap();
        let storage_dir = temp_dir.path();

        let client = FileSystemKeyValueStoreClient::open(None, None, None, storage_dir)
            .await
            .unwrap();

        client
            .set_value("key1", b"1", "application/json; charset=utf-8".to_string())
            .await
            .unwrap();

        assert!(client.record_exists("key1", true).await);
        client.delete_value("key1").await.unwrap();
        assert!(!client.record_exists("key1", true).await);
    }

    #[tokio::test]
    async fn test_sidecar_less_read_requires_opt_in() {
        let temp_dir = TempDir::new().unwrap();
        let storage_dir = temp_dir.path();

        let client = FileSystemKeyValueStoreClient::open(None, None, None, storage_dir)
            .await
            .unwrap();

        // Hand-place a value file with NO sidecar (e.g. a CLI-written INPUT.json).
        // The on-disk name is the LITERAL "INPUT.json" — encode_key preserves the
        // dot (it matches quote(safe='')), so addressing key "INPUT.json" lands on
        // exactly this file. This is what makes the bare-INPUT probe work.
        let payload = br#"{"foo":"bar"}"#;
        assert_eq!(encode_key("INPUT.json"), "INPUT.json");
        let value_path = client.path().join("INPUT.json");
        tokio::fs::write(&value_path, payload).await.unwrap();

        // Default (strict): a value file without a sidecar is "not a record".
        assert!(client
            .get_value("INPUT.json", true)
            .await
            .unwrap()
            .is_none());
        assert!(!client.record_exists("INPUT.json", true).await);

        // Opt-in: the bytes are served with synthesized, non-inferred metadata.
        let (path, meta) = client
            .get_value("INPUT.json", false)
            .await
            .unwrap()
            .unwrap();
        let bytes = tokio::fs::read(&path).await.unwrap();
        assert_eq!(bytes, payload);
        assert_eq!(meta.key, "INPUT.json");
        // No extension-based MIME inference — the generic sentinel only.
        assert_eq!(meta.content_type, "application/octet-stream");
        assert_eq!(meta.size, Some(payload.len()));

        assert!(client.record_exists("INPUT.json", false).await);

        // A genuinely missing file is still absent under either flag.
        assert!(client.get_value("nope", false).await.unwrap().is_none());
        assert!(!client.record_exists("nope", false).await);
    }

    #[tokio::test]
    async fn test_resolve_value_prefers_tracked_record() {
        let temp_dir = TempDir::new().unwrap();
        let storage_dir = temp_dir.path();

        let client = FileSystemKeyValueStoreClient::open(None, None, None, storage_dir)
            .await
            .unwrap();

        // A properly-tracked record for "INPUT" must win over any bare-file
        // probe, and its verbatim sidecar content type is preserved (the
        // caller-declared fallback content types are NOT applied).
        client
            .set_value("INPUT", br#"{"x":1}"#, "application/json".to_string())
            .await
            .unwrap();

        let fallbacks = [("", ""), (".json", "application/json"), (".bin", "")];
        let (path, meta) = client
            .resolve_value("INPUT", &fallbacks)
            .await
            .unwrap()
            .unwrap();
        let bytes = tokio::fs::read(&path).await.unwrap();
        assert_eq!(bytes, br#"{"x":1}"#);
        assert_eq!(meta.key, "INPUT");
        assert_eq!(meta.content_type, "application/json");
    }

    #[tokio::test]
    async fn test_resolve_value_falls_back_to_bare_file_with_inferred_type() {
        let temp_dir = TempDir::new().unwrap();
        let storage_dir = temp_dir.path();

        let client = FileSystemKeyValueStoreClient::open(None, None, None, storage_dir)
            .await
            .unwrap();

        // No tracked "INPUT" record; instead a bare "INPUT.json" file (no
        // sidecar), as a CLI/platform writer would leave it.
        let payload = br#"{"foo":"bar"}"#;
        tokio::fs::write(client.path().join("INPUT.json"), payload)
            .await
            .unwrap();

        let fallbacks = [
            ("", ""),
            (".json", "application/json"),
            (".txt", "text/plain"),
            (".bin", ""),
        ];
        let (path, meta) = client
            .resolve_value("INPUT", &fallbacks)
            .await
            .unwrap()
            .unwrap();
        let bytes = tokio::fs::read(&path).await.unwrap();
        assert_eq!(bytes, payload);
        // Re-keyed to the requested key, not the on-disk "INPUT.json".
        assert_eq!(meta.key, "INPUT");
        // Caller-declared content type for the matched extension is applied.
        assert_eq!(meta.content_type, "application/json");
        assert_eq!(meta.size, Some(payload.len()));
    }

    #[tokio::test]
    async fn test_resolve_value_bare_empty_extension_keeps_octet_stream() {
        let temp_dir = TempDir::new().unwrap();
        let storage_dir = temp_dir.path();

        let client = FileSystemKeyValueStoreClient::open(None, None, None, storage_dir)
            .await
            .unwrap();

        // A bare file under the literal key with an empty-extension fallback
        // (empty declared content type) keeps the synthesized octet-stream.
        tokio::fs::write(client.path().join("INPUT"), b"raw")
            .await
            .unwrap();

        let fallbacks = [("", ""), (".json", "application/json")];
        let (_, meta) = client
            .resolve_value("INPUT", &fallbacks)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(meta.key, "INPUT");
        assert_eq!(meta.content_type, "application/octet-stream");
    }

    #[tokio::test]
    async fn test_resolve_value_missing_returns_none() {
        let temp_dir = TempDir::new().unwrap();
        let storage_dir = temp_dir.path();

        let client = FileSystemKeyValueStoreClient::open(None, None, None, storage_dir)
            .await
            .unwrap();

        let fallbacks = [("", ""), (".json", "application/json")];
        assert!(client
            .resolve_value("nope", &fallbacks)
            .await
            .unwrap()
            .is_none());
    }

    #[tokio::test]
    async fn test_resolve_existing_key() {
        let temp_dir = TempDir::new().unwrap();
        let storage_dir = temp_dir.path();

        let client = FileSystemKeyValueStoreClient::open(None, None, None, storage_dir)
            .await
            .unwrap();

        let fallbacks = ["", ".json", ".txt", ".bin"];

        // Tracked record resolves to the literal key.
        client
            .set_value("tracked", b"x", "text/plain".to_string())
            .await
            .unwrap();
        assert_eq!(
            client.resolve_existing_key("tracked", &fallbacks).await,
            Some("tracked".to_string())
        );

        // Bare file resolves to the matched on-disk filename (key + extension).
        tokio::fs::write(client.path().join("INPUT.json"), b"{}")
            .await
            .unwrap();
        assert_eq!(
            client.resolve_existing_key("INPUT", &fallbacks).await,
            Some("INPUT.json".to_string())
        );

        // Nothing matches → None.
        assert_eq!(client.resolve_existing_key("nope", &fallbacks).await, None);
    }

    #[tokio::test]
    async fn test_sidecar_present_ignores_flag() {
        let temp_dir = TempDir::new().unwrap();
        let storage_dir = temp_dir.path();

        let client = FileSystemKeyValueStoreClient::open(None, None, None, storage_dir)
            .await
            .unwrap();

        // A properly-written record (value + sidecar) reads identically under
        // both flag values — the flag only affects the missing-sidecar branch.
        client
            .set_value("tracked", b"hi", "text/plain; charset=utf-8".to_string())
            .await
            .unwrap();

        for require in [true, false] {
            let (_, meta) = client.get_value("tracked", require).await.unwrap().unwrap();
            assert_eq!(meta.content_type, "text/plain; charset=utf-8");
            assert!(client.record_exists("tracked", require).await);
        }
    }

    #[tokio::test]
    async fn test_purge_with_keep_list() {
        let temp_dir = TempDir::new().unwrap();
        let storage_dir = temp_dir.path();

        let client = FileSystemKeyValueStoreClient::open(None, None, None, storage_dir)
            .await
            .unwrap();

        let ct = "application/json; charset=utf-8".to_string();
        client.set_value("INPUT", b"in", ct.clone()).await.unwrap();
        client.set_value("other", b"x", ct.clone()).await.unwrap();

        // A bare value file (no sidecar) placed out-of-band, NOT in the keep list.
        let bare_path = client.path().join("INPUT.json");
        tokio::fs::write(&bare_path, b"bare").await.unwrap();

        // Keep exactly the "INPUT" key — by exact key, with no extension magic.
        client.purge(&["INPUT".to_string()]).await.unwrap();

        // The kept record (value + sidecar) survives.
        assert!(client.record_exists("INPUT", true).await);
        // The non-kept tracked record is gone (value + sidecar).
        assert!(!client.record_exists("other", true).await);
        // The bare INPUT.json is gone: "INPUT" the key encodes to filename "INPUT",
        // not "INPUT.json", so it is NOT spared. No stem/extension matching.
        assert!(!bare_path.exists());
        // Store metadata is always kept.
        assert!(client.metadata_path().exists());
    }

    #[tokio::test]
    async fn test_purge_empty_keep_list_clears_everything() {
        let temp_dir = TempDir::new().unwrap();
        let storage_dir = temp_dir.path();

        let client = FileSystemKeyValueStoreClient::open(None, None, None, storage_dir)
            .await
            .unwrap();

        let ct = "application/json; charset=utf-8".to_string();
        client.set_value("a", b"1", ct.clone()).await.unwrap();
        client.set_value("b", b"2", ct.clone()).await.unwrap();
        // A bare file too.
        tokio::fs::write(client.path().join("INPUT.json"), b"bare")
            .await
            .unwrap();

        client.purge(&[]).await.unwrap();

        assert!(!client.record_exists("a", true).await);
        assert!(!client.record_exists("b", true).await);
        assert!(!client.path().join("INPUT.json").exists());
        // Only the store metadata remains.
        let mut remaining = Vec::new();
        let mut entries = fs::read_dir(client.path()).await.unwrap();
        while let Some(e) = entries.next_entry().await.unwrap() {
            remaining.push(e.file_name().to_string_lossy().into_owned());
        }
        assert_eq!(remaining, vec![METADATA_FILENAME.to_string()]);
    }

    #[tokio::test]
    async fn test_iterate_keys_page() {
        let temp_dir = TempDir::new().unwrap();
        let storage_dir = temp_dir.path();

        let client = FileSystemKeyValueStoreClient::open(None, None, None, storage_dir)
            .await
            .unwrap();

        let ct = "application/json; charset=utf-8".to_string();
        client.set_value("alpha", b"1", ct.clone()).await.unwrap();
        client.set_value("beta", b"2", ct.clone()).await.unwrap();
        client.set_value("gamma", b"3", ct.clone()).await.unwrap();

        // Fetch all at once (large page_size)
        let page = client
            .iterate_keys_page(None, None, 1000, None, &[])
            .await
            .unwrap();
        assert_eq!(page.items.len(), 3);
        assert!(!page.has_more);

        // With limit
        let page = client
            .iterate_keys_page(None, Some(2), 1000, None, &[])
            .await
            .unwrap();
        assert_eq!(page.items.len(), 2);
        assert!(!page.has_more);

        // Paginate with page_size=2
        let page1 = client
            .iterate_keys_page(None, None, 2, None, &[])
            .await
            .unwrap();
        assert_eq!(page1.items.len(), 2);
        assert!(page1.has_more);

        // Second page using cursor from last key
        let last_key = &page1.items.last().unwrap().key;
        let page2 = client
            .iterate_keys_page(Some(last_key), None, 2, None, &[])
            .await
            .unwrap();
        assert_eq!(page2.items.len(), 1);
        assert!(!page2.has_more);

        // Cursor-based: exclusive_start_key
        let page = client
            .iterate_keys_page(Some("alpha"), None, 1000, None, &[])
            .await
            .unwrap();
        assert_eq!(page.items.len(), 2);
        assert_eq!(page.items[0].key, "beta");
        assert_eq!(page.items[1].key, "gamma");
    }

    #[tokio::test]
    async fn test_iterate_keys_page_with_prefix() {
        let temp_dir = TempDir::new().unwrap();
        let storage_dir = temp_dir.path();

        let client = FileSystemKeyValueStoreClient::open(None, None, None, storage_dir)
            .await
            .unwrap();

        let ct = "application/json; charset=utf-8".to_string();
        for key in ["foo:1", "foo:2", "foo:3", "bar:1", "baz"] {
            client.set_value(key, b"x", ct.clone()).await.unwrap();
        }

        // Prefix filters to matching keys only, in lexical order.
        let page = client
            .iterate_keys_page(None, None, 1000, Some("foo:"), &[])
            .await
            .unwrap();
        assert_eq!(page.items.len(), 3);
        assert!(!page.has_more);
        assert_eq!(
            page.items
                .iter()
                .map(|m| m.key.as_str())
                .collect::<Vec<_>>(),
            ["foo:1", "foo:2", "foo:3"]
        );

        // Prefix + limit: has_more reflects the *filtered* set, not the whole store.
        let page = client
            .iterate_keys_page(None, Some(2), 1000, Some("foo:"), &[])
            .await
            .unwrap();
        assert_eq!(page.items.len(), 2);
        assert_eq!(page.items[0].key, "foo:1");
        assert_eq!(page.items[1].key, "foo:2");

        // Prefix + page_size smaller than the match count sets has_more.
        let page1 = client
            .iterate_keys_page(None, None, 2, Some("foo:"), &[])
            .await
            .unwrap();
        assert_eq!(page1.items.len(), 2);
        assert!(page1.has_more);

        // Prefix + cursor: continue after the last key of page1, still prefix-scoped.
        let last_key = &page1.items.last().unwrap().key;
        let page2 = client
            .iterate_keys_page(Some(last_key), None, 1000, Some("foo:"), &[])
            .await
            .unwrap();
        assert_eq!(page2.items.len(), 1);
        assert_eq!(page2.items[0].key, "foo:3");

        // A prefix matching nothing yields an empty page.
        let page = client
            .iterate_keys_page(None, None, 1000, Some("nope"), &[])
            .await
            .unwrap();
        assert!(page.items.is_empty());
        assert!(!page.has_more);
    }

    #[tokio::test]
    async fn test_iterate_keys_includes_bare_fallback_files() {
        let temp_dir = TempDir::new().unwrap();
        let storage_dir = temp_dir.path();

        let client = FileSystemKeyValueStoreClient::open(None, None, None, storage_dir)
            .await
            .unwrap();

        // One tracked record, plus a bare INPUT.json with no sidecar (as a
        // CLI/platform writer would leave it).
        let ct = "application/json; charset=utf-8".to_string();
        client.set_value("alpha", b"1", ct).await.unwrap();
        let payload = br#"{"foo":"bar"}"#;
        tokio::fs::write(client.path().join("INPUT.json"), payload)
            .await
            .unwrap();

        // Without declaring the fallback, the bare file is invisible to listing.
        let page = client
            .iterate_keys_page(None, None, 1000, None, &[])
            .await
            .unwrap();
        assert_eq!(
            page.items
                .iter()
                .map(|m| m.key.as_str())
                .collect::<Vec<_>>(),
            ["alpha"]
        );

        // Declaring the bare file by its on-disk name surfaces it under that
        // literal key, with the caller-declared content type and a stated size.
        let fallbacks = [("INPUT.json", "application/json")];
        let page = client
            .iterate_keys_page(None, None, 1000, None, &fallbacks)
            .await
            .unwrap();
        let keys = page
            .items
            .iter()
            .map(|m| m.key.as_str())
            .collect::<Vec<_>>();
        // "INPUT.json" encodes lexically before "alpha", so it sorts first.
        assert_eq!(keys, ["INPUT.json", "alpha"]);
        let input = page.items.iter().find(|m| m.key == "INPUT.json").unwrap();
        assert_eq!(input.content_type, "application/json");
        assert_eq!(input.size, Some(payload.len()));
    }

    #[tokio::test]
    async fn test_iterate_keys_tracked_record_wins_over_bare_fallback() {
        let temp_dir = TempDir::new().unwrap();
        let storage_dir = temp_dir.path();

        let client = FileSystemKeyValueStoreClient::open(None, None, None, storage_dir)
            .await
            .unwrap();

        // A tracked record literally keyed "INPUT.json" (value + sidecar) AND a
        // bare fallback declared for the same on-disk name. The tracked record
        // must win — "INPUT.json" appears once, with the sidecar's content type.
        client
            .set_value("INPUT.json", b"tracked", "application/json".to_string())
            .await
            .unwrap();

        let fallbacks = [("INPUT.json", "text/plain")];
        let page = client
            .iterate_keys_page(None, None, 1000, None, &fallbacks)
            .await
            .unwrap();
        let inputs = page
            .items
            .iter()
            .filter(|m| m.key == "INPUT.json")
            .collect::<Vec<_>>();
        assert_eq!(inputs.len(), 1);
        assert_eq!(inputs[0].content_type, "application/json");
        assert_eq!(inputs[0].size, Some(b"tracked".len()));
    }

    #[tokio::test]
    async fn test_iterate_keys_bare_fallback_respects_prefix_and_pagination() {
        let temp_dir = TempDir::new().unwrap();
        let storage_dir = temp_dir.path();

        let client = FileSystemKeyValueStoreClient::open(None, None, None, storage_dir)
            .await
            .unwrap();

        // Tracked records under a prefix, plus a bare file inside and one outside it.
        let ct = "application/json".to_string();
        client.set_value("foo:a", b"1", ct.clone()).await.unwrap();
        client.set_value("foo:b", b"2", ct).await.unwrap();
        tokio::fs::write(client.path().join("foo%3Az.json"), b"bare-in")
            .await
            .unwrap();
        tokio::fs::write(client.path().join("bar.json"), b"bare-out")
            .await
            .unwrap();

        // Bare files declared by their on-disk name; the surfaced key is that name.
        let fallbacks = [
            ("foo:z.json", "application/json"),
            ("bar.json", "application/json"),
        ];

        // Prefix filter applies to bare files via their surfaced key: "bar.json"
        // is excluded.
        let page = client
            .iterate_keys_page(None, None, 1000, Some("foo:"), &fallbacks)
            .await
            .unwrap();
        assert_eq!(
            page.items
                .iter()
                .map(|m| m.key.as_str())
                .collect::<Vec<_>>(),
            ["foo:a", "foo:b", "foo:z.json"]
        );

        // Cursor + limit treat the bare file like any other key.
        let page = client
            .iterate_keys_page(Some("foo:b"), None, 1000, Some("foo:"), &fallbacks)
            .await
            .unwrap();
        assert_eq!(
            page.items
                .iter()
                .map(|m| m.key.as_str())
                .collect::<Vec<_>>(),
            ["foo:z.json"]
        );
    }

    #[tokio::test]
    async fn test_iterate_keys_valid_cursor_paginates() {
        // (a) A valid, existing cursor still paginates correctly.
        let temp_dir = TempDir::new().unwrap();
        let storage_dir = temp_dir.path();

        let client = FileSystemKeyValueStoreClient::open(None, None, None, storage_dir)
            .await
            .unwrap();

        let ct = "application/json; charset=utf-8".to_string();
        for key in ["alpha", "beta", "gamma", "delta"] {
            client.set_value(key, b"x", ct.clone()).await.unwrap();
        }

        // Cursor "beta" exists → keys strictly greater than it, in lexical order.
        let page = client
            .iterate_keys_page(Some("beta"), None, 1000, None, &[])
            .await
            .unwrap();
        assert_eq!(
            page.items
                .iter()
                .map(|m| m.key.as_str())
                .collect::<Vec<_>>(),
            ["delta", "gamma"]
        );
        assert!(!page.has_more);
    }

    #[tokio::test]
    async fn test_iterate_keys_nonexistent_cursor_errors() {
        // (b) A nonexistent cursor returns the typed error variant.
        let temp_dir = TempDir::new().unwrap();
        let storage_dir = temp_dir.path();

        let client = FileSystemKeyValueStoreClient::open(None, None, None, storage_dir)
            .await
            .unwrap();

        let ct = "application/json; charset=utf-8".to_string();
        client.set_value("alpha", b"x", ct.clone()).await.unwrap();
        client.set_value("beta", b"x", ct.clone()).await.unwrap();

        // A cursor that does not match any existing key must error, even though
        // there are keys lexically greater than it (the old behavior silently
        // returned "beta").
        let err = client
            .iterate_keys_page(Some("alfa"), None, 1000, None, &[])
            .await
            .unwrap_err();
        match err {
            StorageError::ExclusiveStartKeyNotFound(key) => assert_eq!(key, "alfa"),
            other => panic!("expected ExclusiveStartKeyNotFound, got: {other:?}"),
        }

        // A stale/deleted cursor (a key that once existed) errors too.
        client.delete_value("alpha").await.unwrap();
        let err = client
            .iterate_keys_page(Some("alpha"), None, 1000, None, &[])
            .await
            .unwrap_err();
        assert!(matches!(
            err,
            StorageError::ExclusiveStartKeyNotFound(ref k) if k == "alpha"
        ));
    }

    #[tokio::test]
    async fn test_iterate_keys_cursor_validation_with_prefix() {
        // (c) Cursor validation is scoped to the prefix-filtered set.
        let temp_dir = TempDir::new().unwrap();
        let storage_dir = temp_dir.path();

        let client = FileSystemKeyValueStoreClient::open(None, None, None, storage_dir)
            .await
            .unwrap();

        let ct = "application/json; charset=utf-8".to_string();
        for key in ["foo:1", "foo:2", "foo:3", "bar:1"] {
            client.set_value(key, b"x", ct.clone()).await.unwrap();
        }

        // A cursor that exists AND is within the prefix → paginates fine.
        let page = client
            .iterate_keys_page(Some("foo:1"), None, 1000, Some("foo:"), &[])
            .await
            .unwrap();
        assert_eq!(
            page.items
                .iter()
                .map(|m| m.key.as_str())
                .collect::<Vec<_>>(),
            ["foo:2", "foo:3"]
        );

        // A cursor that exists in the store but is OUTSIDE the prefix scope is
        // treated as not found (v4 searches within the prefix-filtered set).
        let err = client
            .iterate_keys_page(Some("bar:1"), None, 1000, Some("foo:"), &[])
            .await
            .unwrap_err();
        assert!(matches!(
            err,
            StorageError::ExclusiveStartKeyNotFound(ref k) if k == "bar:1"
        ));
    }

    #[tokio::test]
    async fn test_iterate_keys_cursor_validation_with_small_page() {
        // The cursor key may sort after the page boundary; validation must still
        // confirm its existence by scanning the whole candidate set.
        let temp_dir = TempDir::new().unwrap();
        let storage_dir = temp_dir.path();

        let client = FileSystemKeyValueStoreClient::open(None, None, None, storage_dir)
            .await
            .unwrap();

        let ct = "application/json; charset=utf-8".to_string();
        for key in ["a", "b", "c", "d", "z"] {
            client.set_value(key, b"x", ct.clone()).await.unwrap();
        }

        // Cursor "z" exists but sorts last; with a small page the result set
        // fills before "z" is reached. It must NOT error, and (since nothing is
        // greater than "z") the page must be empty.
        let page = client
            .iterate_keys_page(Some("z"), None, 2, None, &[])
            .await
            .unwrap();
        assert!(page.items.is_empty());
        assert!(!page.has_more);

        // Cursor "b" exists; small page returns the next 2 keys after it.
        let page = client
            .iterate_keys_page(Some("b"), None, 2, None, &[])
            .await
            .unwrap();
        assert_eq!(
            page.items
                .iter()
                .map(|m| m.key.as_str())
                .collect::<Vec<_>>(),
            ["c", "d"]
        );
    }

    #[tokio::test]
    async fn test_list_keys_result_shape() {
        let temp_dir = TempDir::new().unwrap();
        let storage_dir = temp_dir.path();

        let client = FileSystemKeyValueStoreClient::open(None, None, None, storage_dir)
            .await
            .unwrap();

        // Empty store: no items, not truncated, no next cursor. `limit` echoes
        // the caller's request; `exclusive_start_key` echoes the (absent) input.
        let result = client.list_keys(None, Some(10), None, &[]).await.unwrap();
        assert!(result.items.is_empty());
        assert_eq!(result.count, 0);
        assert_eq!(result.limit, 10);
        assert_eq!(result.exclusive_start_key, None);
        assert!(!result.is_truncated);
        assert_eq!(result.next_exclusive_start_key, None);

        let ct = "application/json; charset=utf-8".to_string();
        for key in ["alpha", "beta", "gamma"] {
            client.set_value(key, b"x", ct.clone()).await.unwrap();
        }

        // Full listing (limit larger than the store): all items, not truncated,
        // no next cursor. count == items.len(); limit echoed.
        let result = client.list_keys(None, Some(100), None, &[]).await.unwrap();
        assert_eq!(
            result
                .items
                .iter()
                .map(|m| m.key.as_str())
                .collect::<Vec<_>>(),
            ["alpha", "beta", "gamma"]
        );
        assert_eq!(result.count, 3);
        assert_eq!(result.limit, 100);
        assert_eq!(result.exclusive_start_key, None);
        assert!(!result.is_truncated);
        assert_eq!(result.next_exclusive_start_key, None);

        // Truncated first page: limit=2 leaves a third key behind, so
        // is_truncated is set and next_exclusive_start_key is this page's last
        // key ("beta").
        let page1 = client.list_keys(None, Some(2), None, &[]).await.unwrap();
        assert_eq!(
            page1
                .items
                .iter()
                .map(|m| m.key.as_str())
                .collect::<Vec<_>>(),
            ["alpha", "beta"]
        );
        assert_eq!(page1.count, 2);
        assert_eq!(page1.limit, 2);
        assert_eq!(page1.exclusive_start_key, None);
        assert!(page1.is_truncated);
        assert_eq!(page1.next_exclusive_start_key.as_deref(), Some("beta"));

        // Final page via the returned cursor: the remaining key, not truncated,
        // no next cursor. The supplied cursor is echoed in exclusive_start_key.
        let cursor = page1.next_exclusive_start_key.clone().unwrap();
        let page2 = client
            .list_keys(Some(&cursor), Some(2), None, &[])
            .await
            .unwrap();
        assert_eq!(
            page2
                .items
                .iter()
                .map(|m| m.key.as_str())
                .collect::<Vec<_>>(),
            ["gamma"]
        );
        assert_eq!(page2.count, 1);
        assert_eq!(page2.exclusive_start_key.as_deref(), Some("beta"));
        assert!(!page2.is_truncated);
        assert_eq!(page2.next_exclusive_start_key, None);

        // Default limit (None → 1000) surfaces everything without truncation.
        let result = client.list_keys(None, None, None, &[]).await.unwrap();
        assert_eq!(result.count, 3);
        assert_eq!(result.limit, 1000);
        assert!(!result.is_truncated);
    }

    #[tokio::test]
    async fn test_list_keys_prefix_and_limit_interaction() {
        let temp_dir = TempDir::new().unwrap();
        let storage_dir = temp_dir.path();

        let client = FileSystemKeyValueStoreClient::open(None, None, None, storage_dir)
            .await
            .unwrap();

        let ct = "application/json".to_string();
        for key in ["foo:1", "foo:2", "foo:3", "bar:1"] {
            client.set_value(key, b"x", ct.clone()).await.unwrap();
        }

        // Prefix + limit: truncation and count reflect only the matching
        // ("foo:") subset, and the next cursor is the last matched key.
        let page = client
            .list_keys(None, Some(2), Some("foo:"), &[])
            .await
            .unwrap();
        assert_eq!(
            page.items
                .iter()
                .map(|m| m.key.as_str())
                .collect::<Vec<_>>(),
            ["foo:1", "foo:2"]
        );
        assert_eq!(page.count, 2);
        assert_eq!(page.limit, 2);
        assert!(page.is_truncated);
        assert_eq!(page.next_exclusive_start_key.as_deref(), Some("foo:2"));

        // Continuing with the cursor within the same prefix yields the last
        // matching key, no truncation.
        let cursor = page.next_exclusive_start_key.clone().unwrap();
        let page2 = client
            .list_keys(Some(&cursor), Some(2), Some("foo:"), &[])
            .await
            .unwrap();
        assert_eq!(
            page2
                .items
                .iter()
                .map(|m| m.key.as_str())
                .collect::<Vec<_>>(),
            ["foo:3"]
        );
        assert!(!page2.is_truncated);
        assert_eq!(page2.next_exclusive_start_key, None);
    }

    #[tokio::test]
    async fn test_get_public_url_existence_aware() {
        // (d) get_public_url returns Some(url) for an existing key, None for a
        // missing one.
        let temp_dir = TempDir::new().unwrap();
        let storage_dir = temp_dir.path();

        let client = FileSystemKeyValueStoreClient::open(None, None, None, storage_dir)
            .await
            .unwrap();

        // Missing key → None.
        assert_eq!(client.get_public_url("missing").await, None);

        // Existing tracked record → Some(url) that points at the encoded path.
        client
            .set_value("my-key", b"v", "text/plain".to_string())
            .await
            .unwrap();
        let url = client.get_public_url("my-key").await.unwrap();
        let expected = format!(
            "file://{}",
            client.path().join(encode_key("my-key")).display()
        );
        assert_eq!(url, expected);

        // A bare value file (no sidecar) still has its file present, so a URL
        // for the on-disk key (as resolve_existing_key would return) resolves.
        tokio::fs::write(client.path().join("INPUT.json"), b"{}")
            .await
            .unwrap();
        assert!(client.get_public_url("INPUT.json").await.is_some());

        // After delete, the URL is gone.
        client.delete_value("my-key").await.unwrap();
        assert_eq!(client.get_public_url("my-key").await, None);
    }

    #[tokio::test]
    async fn test_special_characters_in_key() {
        let temp_dir = TempDir::new().unwrap();
        let storage_dir = temp_dir.path();

        let client = FileSystemKeyValueStoreClient::open(None, None, None, storage_dir)
            .await
            .unwrap();

        client
            .set_value(
                "path/to/key with spaces",
                b"value",
                "text/plain; charset=utf-8".to_string(),
            )
            .await
            .unwrap();

        let (bytes, content_type, _) = read_back(&client, "path/to/key with spaces").await.unwrap();
        assert_eq!(bytes, b"value");
        assert_eq!(content_type, "text/plain; charset=utf-8");
    }

    /// A large opaque value must round-trip byte-for-byte through the
    /// `atomic_write` + FFI byte path without truncation, corruption, or OOM, and
    /// its `size` must be backfilled to the real length. Mirrors the deleted
    /// crawlee-js `no-crash-on-big-buffers` test, but pinned at the library level
    /// where the write path now lives. (1 MiB keeps the test fast; the original
    /// JS bug was a stack overflow on large buffers, which this byte path avoids
    /// entirely.)
    #[tokio::test]
    async fn test_large_value_roundtrip() {
        let temp_dir = TempDir::new().unwrap();
        let storage_dir = temp_dir.path();

        let client = FileSystemKeyValueStoreClient::open(None, None, None, storage_dir)
            .await
            .unwrap();

        // 1 MiB of non-trivial (non-zero, position-dependent) bytes so a partial
        // write or off-by-some truncation would be caught.
        let size = 1024 * 1024;
        let payload: Vec<u8> = (0..size).map(|i| (i % 251) as u8).collect();

        client
            .set_value("big.bin", &payload, "application/octet-stream".to_string())
            .await
            .unwrap();

        let (bytes, content_type, persisted_size) = read_back(&client, "big.bin").await.unwrap();
        assert_eq!(
            bytes.len(),
            size,
            "round-tripped value must keep its length"
        );
        assert_eq!(bytes, payload, "round-tripped value must be byte-identical");
        assert_eq!(content_type, "application/octet-stream");
        assert_eq!(
            persisted_size,
            Some(size),
            "size must reflect the real length"
        );
    }

    /// Records written to a KVS must survive closing and reopening the same
    /// on-disk store by name. The dataset and request-queue clients already have
    /// reopen coverage; this closes the KVS gap. Mirrors the deleted crawlee-js /
    /// crawlee-python `data persistence across reopens` tests.
    #[tokio::test]
    async fn test_reopen_preserves_records() {
        let temp_dir = TempDir::new().unwrap();
        let storage_dir = temp_dir.path();

        {
            let client = FileSystemKeyValueStoreClient::open(
                None,
                Some("kvs".to_string()),
                None,
                storage_dir,
            )
            .await
            .unwrap();

            client
                .set_value(
                    "greeting",
                    b"hello",
                    "text/plain; charset=utf-8".to_string(),
                )
                .await
                .unwrap();
            client
                .set_value("payload", br#"{"x":1}"#, "application/json".to_string())
                .await
                .unwrap();
        }

        // Reopen the same store by name, emulating a fresh process.
        let reopened =
            FileSystemKeyValueStoreClient::open(None, Some("kvs".to_string()), None, storage_dir)
                .await
                .unwrap();

        let (bytes, content_type, _) = read_back(&reopened, "greeting").await.unwrap();
        assert_eq!(bytes, b"hello");
        assert_eq!(content_type, "text/plain; charset=utf-8");

        let (bytes, content_type, _) = read_back(&reopened, "payload").await.unwrap();
        assert_eq!(bytes, br#"{"x":1}"#);
        assert_eq!(content_type, "application/json");

        // A key that was never written must still be absent.
        assert!(reopened.get_value("missing", true).await.unwrap().is_none());
    }

    /// `open()` must tolerate the datetime formats that other writers emit in
    /// `__metadata__.json`: the JS-style `Z` suffix (e.g. `...123Z`) and a
    /// varying number of fractional-second digits. AGENTS.md lists this as an
    /// explicit compatibility constraint; this guards the `deserialize_datetime`
    /// fallbacks against regression.
    #[tokio::test]
    async fn test_open_tolerates_z_suffix_and_varying_fractions() {
        let temp_dir = TempDir::new().unwrap();
        let storage_dir = temp_dir.path();

        // Hand-write a metadata file as a JS MemoryStorage writer would: `Z`
        // suffix, millisecond (3-digit) precision.
        let kvs_dir = storage_dir.join("key_value_stores").join("legacy-kvs");
        fs::create_dir_all(&kvs_dir).await.unwrap();
        let legacy_meta = r#"{
  "id": "kvsid123",
  "name": "legacy-kvs",
  "accessedAt": "2024-01-15T10:30:00.123Z",
  "createdAt": "2024-01-15T10:30:00Z",
  "modifiedAt": "2024-01-15T10:30:00.123456+00:00"
}"#;
        fs::write(kvs_dir.join(METADATA_FILENAME), legacy_meta)
            .await
            .unwrap();

        let client = FileSystemKeyValueStoreClient::open(
            None,
            Some("legacy-kvs".to_string()),
            None,
            storage_dir,
        )
        .await
        .unwrap();

        let meta = client.get_metadata().await;
        assert_eq!(meta.base.id, "kvsid123");

        // All three timestamps must parse to the same 2024-01-15T10:30:00 UTC
        // instant (modulo sub-second precision), proving each format variant was
        // accepted rather than silently defaulted.
        use chrono::{TimeZone, Utc};
        let expected_secs = Utc
            .with_ymd_and_hms(2024, 1, 15, 10, 30, 0)
            .unwrap()
            .timestamp();
        assert_eq!(meta.base.created_at.timestamp(), expected_secs);
        assert_eq!(meta.base.accessed_at.timestamp(), expected_secs);
        assert_eq!(meta.base.modified_at.timestamp(), expected_secs);
        // The `.123Z` fractional part must survive too.
        assert_eq!(meta.base.accessed_at.timestamp_subsec_millis(), 123);
    }

    #[tokio::test]
    async fn test_read_value_returns_bytes_and_size() {
        let temp_dir = TempDir::new().unwrap();
        let client = FileSystemKeyValueStoreClient::open(None, None, None, temp_dir.path())
            .await
            .unwrap();

        client
            .set_value("k", b"hello", "text/plain".to_string())
            .await
            .unwrap();

        let record = client.read_value("k").await.unwrap().unwrap();
        assert_eq!(record.key, "k");
        assert_eq!(record.content_type, "text/plain");
        assert_eq!(record.value, b"hello");
        // Non-optional size, populated from the sidecar.
        assert_eq!(record.size, 5);

        // A missing key reads as None.
        assert!(client.read_value("nope").await.unwrap().is_none());
        // A bare file with no sidecar is not a tracked record (strict).
        tokio::fs::write(client.path().join("bare"), b"x")
            .await
            .unwrap();
        assert!(client.read_value("bare").await.unwrap().is_none());
    }

    #[tokio::test]
    async fn test_read_value_finalizes_size_for_sidecar_without_size() {
        // A sidecar written by crawlee-JS / older Python may omit `size`.
        // read_value must finalize it from the actual byte count, never None.
        let temp_dir = TempDir::new().unwrap();
        let client = FileSystemKeyValueStoreClient::open(None, None, None, temp_dir.path())
            .await
            .unwrap();

        let key = "legacy";
        let payload = b"twelve bytes";
        let encoded = encode_key(key);
        tokio::fs::write(client.path().join(&encoded), payload)
            .await
            .unwrap();
        tokio::fs::write(
            client.path().join(format!("{encoded}.{METADATA_FILENAME}")),
            br#"{"key":"legacy","contentType":"text/plain"}"#,
        )
        .await
        .unwrap();

        let record = client.read_value(key).await.unwrap().unwrap();
        assert_eq!(record.value, payload);
        assert_eq!(record.size, payload.len());
    }

    #[tokio::test]
    async fn test_value_file_info_has_size_without_reading_bytes() {
        let temp_dir = TempDir::new().unwrap();
        let client = FileSystemKeyValueStoreClient::open(None, None, None, temp_dir.path())
            .await
            .unwrap();

        client
            .set_value("k", b"abcd", "application/json".to_string())
            .await
            .unwrap();

        let info = client.value_file_info("k").await.unwrap().unwrap();
        assert_eq!(info.key, "k");
        assert_eq!(info.content_type, "application/json");
        assert_eq!(info.size, 4);
        assert_eq!(info.path, client.path().join(encode_key("k")));
        // The bytes at that path match what we'd stream.
        assert_eq!(tokio::fs::read(&info.path).await.unwrap(), b"abcd");

        assert!(client.value_file_info("nope").await.unwrap().is_none());
    }

    #[tokio::test]
    async fn test_resolve_and_read_value_bare_fallback() {
        let temp_dir = TempDir::new().unwrap();
        let client = FileSystemKeyValueStoreClient::open(None, None, None, temp_dir.path())
            .await
            .unwrap();

        // Bare INPUT.json with no sidecar, as a CLI/platform writer leaves it.
        let payload = br#"{"foo":"bar"}"#;
        tokio::fs::write(client.path().join("INPUT.json"), payload)
            .await
            .unwrap();

        let fallbacks = [("", ""), (".json", "application/json"), (".bin", "")];
        let record = client
            .resolve_and_read_value("INPUT", &fallbacks)
            .await
            .unwrap()
            .unwrap();
        // Re-keyed to the requested key, caller-declared content type applied,
        // bytes read, and size finalized — all in one call.
        assert_eq!(record.key, "INPUT");
        assert_eq!(record.content_type, "application/json");
        assert_eq!(record.value, payload);
        assert_eq!(record.size, payload.len());

        // Nothing resolves → None.
        assert!(client
            .resolve_and_read_value("missing", &fallbacks)
            .await
            .unwrap()
            .is_none());
    }
}
