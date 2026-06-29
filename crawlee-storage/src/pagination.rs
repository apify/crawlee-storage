//! Generic lazy paging cursor shared by the language bindings.
//!
//! Both the dataset item iterator and the KVS key iterator need the exact same
//! buffering state machine on top of the core's `iterate_*_page` methods: fetch
//! a page, hand out its items one at a time, then fetch the next page until the
//! source reports no more. Only two things actually differ between them — how
//! the on-disk cursor advances (dataset: an integer `offset`; KVS: the last key
//! string) and what type of item is yielded. Everything else (the buffer, the
//! `remaining_limit` accounting, the "page came back empty / `has_more` was
//! false → we're done" logic) was previously copy-pasted into all four binding
//! iterators (dataset + KVS, Python + Node).
//!
//! [`PageCursor`] owns that shared loop once; [`PageSource`] is the small,
//! per-iterator hook that knows how to fetch one page and advance its own
//! cursor. The bindings now hold a `Mutex<PageCursor<…>>` and do nothing but
//! convert the yielded item to the language-native type and translate
//! end-of-stream into `StopAsyncIteration` (Python) / `null` (Node).

use crate::dataset::FileSystemDatasetClient;
use crate::key_value_store::FileSystemKeyValueStoreClient;
use crate::models::KeyValueStoreRecordMetadata;
use crate::utils::Result;

use std::sync::Arc;

use serde_json::Value;

/// A source of successive pages for a [`PageCursor`].
///
/// An implementor owns whatever cursor state it needs (an offset, a last-key,
/// …) and advances it itself on each [`fetch_page`](PageSource::fetch_page)
/// call. The generic cursor never inspects that state — it only consumes the
/// `(items, has_more)` a source returns. `remaining_limit` is the number of
/// items the caller still wants overall (`None` = unbounded); a source should
/// pass it through to the underlying `iterate_*_page` so the page's own
/// `has_more`/limit accounting stays correct.
pub trait PageSource {
    /// The item type yielded one-at-a-time by the cursor. `Clone` because the
    /// cursor buffers a page and hands out a clone of each item in turn — the
    /// same per-item clone the binding iterators did before this was unified.
    type Item: Clone;

    /// Fetch the next page and advance this source's internal cursor.
    ///
    /// Returns the page's items plus whether more items exist after it. An
    /// empty `items` vec is treated by the cursor as end-of-stream regardless
    /// of the `has_more` flag.
    fn fetch_page(
        &mut self,
        remaining_limit: Option<usize>,
    ) -> impl std::future::Future<Output = Result<(Vec<Self::Item>, bool)>> + Send;
}

/// A lazy, buffered cursor over successive pages from a [`PageSource`].
///
/// Call [`next`](PageCursor::next) repeatedly; it yields `Some(item)` until the
/// source is exhausted, then `None` forever after. The cursor takes `&mut self`
/// (the bindings keep it behind a `tokio::sync::Mutex`), so a single logical
/// iterator is driven sequentially even when polled from multiple tasks.
pub struct PageCursor<S: PageSource> {
    source: S,
    /// Items the caller still wants overall; `None` = unbounded. Decremented by
    /// each page's length so the source can cap its final page.
    remaining_limit: Option<usize>,
    buffer: Vec<S::Item>,
    buf_index: usize,
    done: bool,
}

impl<S: PageSource> PageCursor<S> {
    /// Create a cursor over `source`, yielding at most `limit` items total
    /// (`None` = unbounded).
    pub fn new(source: S, limit: Option<usize>) -> Self {
        Self {
            source,
            remaining_limit: limit,
            buffer: Vec::new(),
            buf_index: 0,
            done: false,
        }
    }

    /// Yield the next item, fetching a fresh page from the source when the
    /// current buffer is drained. Returns `None` once the source is exhausted.
    pub async fn next(&mut self) -> Result<Option<S::Item>> {
        // 1. Drain the current buffer before fetching anything new.
        if self.buf_index < self.buffer.len() {
            let item = self.buffer[self.buf_index].clone();
            self.buf_index += 1;
            return Ok(Some(item));
        }

        // 2. Already exhausted.
        if self.done {
            return Ok(None);
        }

        // 3. Fetch the next page (the source advances its own cursor).
        let (items, has_more) = self.source.fetch_page(self.remaining_limit).await?;

        // 4. An empty page is end-of-stream, whatever has_more claims.
        if items.is_empty() {
            self.done = true;
            return Ok(None);
        }

        // 5. Account for the items consumed and decide if this is the last page.
        let page_len = items.len();
        if let Some(rem) = self.remaining_limit.as_mut() {
            *rem = rem.saturating_sub(page_len);
        }
        if !has_more {
            self.done = true;
        }

        // Buffer the page and hand out its first item.
        self.buffer = items;
        self.buf_index = 1;
        Ok(Some(self.buffer[0].clone()))
    }
}

// ─── Dataset item source ────────────────────────────────────────────────────

/// [`PageSource`] over a dataset's items, advancing by integer `offset`.
pub struct DatasetItemSource {
    client: Arc<FileSystemDatasetClient>,
    offset: usize,
    page_size: usize,
    desc: bool,
    skip_empty: bool,
}

impl DatasetItemSource {
    pub fn new(
        client: Arc<FileSystemDatasetClient>,
        offset: usize,
        page_size: usize,
        desc: bool,
        skip_empty: bool,
    ) -> Self {
        Self {
            client,
            offset,
            page_size,
            desc,
            skip_empty,
        }
    }
}

impl PageSource for DatasetItemSource {
    type Item = Value;

    async fn fetch_page(&mut self, remaining_limit: Option<usize>) -> Result<(Vec<Value>, bool)> {
        let page = self
            .client
            .iterate_items_page(
                self.offset,
                remaining_limit,
                self.page_size,
                self.desc,
                self.skip_empty,
            )
            .await?;
        // Advance past the items we just consumed.
        self.offset += page.items.len();
        Ok((page.items, page.has_more))
    }
}

// ─── KVS key source ─────────────────────────────────────────────────────────

/// [`PageSource`] over a KVS's keys, advancing by the last key seen.
pub struct KvsKeySource {
    client: Arc<FileSystemKeyValueStoreClient>,
    exclusive_start_key: Option<String>,
    page_size: usize,
    prefix: Option<String>,
}

impl KvsKeySource {
    pub fn new(
        client: Arc<FileSystemKeyValueStoreClient>,
        exclusive_start_key: Option<String>,
        page_size: usize,
        prefix: Option<String>,
    ) -> Self {
        Self {
            client,
            exclusive_start_key,
            page_size,
            prefix,
        }
    }
}

impl PageSource for KvsKeySource {
    type Item = KeyValueStoreRecordMetadata;

    async fn fetch_page(
        &mut self,
        remaining_limit: Option<usize>,
    ) -> Result<(Vec<KeyValueStoreRecordMetadata>, bool)> {
        let page = self
            .client
            .iterate_keys_page(
                self.exclusive_start_key.as_deref(),
                remaining_limit,
                self.page_size,
                self.prefix.as_deref(),
            )
            .await?;
        // Advance the cursor to the last key of this page so the next fetch
        // continues strictly after it.
        if let Some(last) = page.items.last() {
            self.exclusive_start_key = Some(last.key.clone());
        }
        Ok((page.items, page.has_more))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A synthetic source over a fixed list of integers, paging by a fixed
    /// chunk size, so the generic cursor's buffering/limit/`done` logic can be
    /// exercised without touching the filesystem.
    struct VecSource {
        data: Vec<i32>,
        pos: usize,
        page_size: usize,
    }

    impl PageSource for VecSource {
        type Item = i32;

        async fn fetch_page(&mut self, remaining_limit: Option<usize>) -> Result<(Vec<i32>, bool)> {
            let want = match remaining_limit {
                Some(rem) => self.page_size.min(rem),
                None => self.page_size,
            };
            let end = (self.pos + want).min(self.data.len());
            let items = self.data[self.pos..end].to_vec();
            self.pos = end;
            let has_more = self.pos < self.data.len();
            Ok((items, has_more))
        }
    }

    async fn drain<S: PageSource>(mut cursor: PageCursor<S>) -> Vec<S::Item> {
        let mut out = Vec::new();
        while let Some(item) = cursor.next().await.unwrap() {
            out.push(item);
        }
        out
    }

    #[tokio::test]
    async fn yields_every_item_in_order_across_pages() {
        let src = VecSource {
            data: (0..10).collect(),
            pos: 0,
            page_size: 3,
        };
        let cursor = PageCursor::new(src, None);
        assert_eq!(drain(cursor).await, (0..10).collect::<Vec<_>>());
    }

    #[tokio::test]
    async fn respects_overall_limit() {
        let src = VecSource {
            data: (0..10).collect(),
            pos: 0,
            page_size: 3,
        };
        let cursor = PageCursor::new(src, Some(4));
        // The source caps each fetch at the remaining limit, and the cursor
        // stops once the limited supply runs dry.
        assert_eq!(drain(cursor).await, vec![0, 1, 2, 3]);
    }

    #[tokio::test]
    async fn empty_source_yields_nothing() {
        let src = VecSource {
            data: Vec::new(),
            pos: 0,
            page_size: 3,
        };
        let mut cursor = PageCursor::new(src, None);
        assert!(cursor.next().await.unwrap().is_none());
        // Exhausted cursors keep returning None.
        assert!(cursor.next().await.unwrap().is_none());
    }

    #[tokio::test]
    async fn exact_multiple_of_page_size_terminates() {
        // 9 items, page_size 3 → three full pages, then a clean stop with no
        // spurious empty trailing fetch surfacing as an item.
        let src = VecSource {
            data: (0..9).collect(),
            pos: 0,
            page_size: 3,
        };
        let cursor = PageCursor::new(src, None);
        assert_eq!(drain(cursor).await, (0..9).collect::<Vec<_>>());
    }
}
