//! Pluggable clock abstraction.
//!
//! Real code uses [`SystemClock`] (a zero-overhead newtype around [`chrono::Utc::now`]).
//! Tests can use [`TestClock`], whose `now()` reads `Utc::now() + offset`, where the
//! offset is settable from the outside (including across the FFI boundary). This is
//! how the napi/PyO3 bindings let JS/Python tests advance the time the Rust code sees
//! — `vi.useFakeTimers()` can't reach into native code, so we need an in-band hook.
//!
//! Clocks are stored as `Arc<dyn Clock>` on each client. The default is `SystemClock`
//! and incurs nothing more than an extra virtual call per `now()`, which is fine —
//! every `now()` call is already paired with a filesystem write.

use std::fmt::Debug;
use std::sync::atomic::{AtomicI64, Ordering};
use std::sync::Arc;

use chrono::{DateTime, Duration, Utc};

/// Source of "now" for storage clients. Implementations must be cheap and
/// monotonic-in-practice (we don't formally require monotonicity, but the
/// orderNo lock model breaks if `now()` goes backwards by more than the lock
/// duration).
pub trait Clock: Send + Sync + Debug {
    /// Current wall-clock time as a UTC `DateTime`.
    fn now(&self) -> DateTime<Utc>;

    /// Current wall-clock time as unix milliseconds. Default impl derives it
    /// from [`now`]; implementations can override for efficiency.
    fn now_millis(&self) -> i64 {
        self.now().timestamp_millis()
    }
}

/// Default clock: thin wrapper around [`chrono::Utc::now`].
#[derive(Debug, Default, Clone, Copy)]
pub struct SystemClock;

impl Clock for SystemClock {
    #[inline]
    fn now(&self) -> DateTime<Utc> {
        Utc::now()
    }
}

/// Testing clock. Its `now()` is `Utc::now() + offset_millis`, where the offset
/// starts at zero and can be advanced from anywhere — including from JS or
/// Python bindings via a dedicated client method. Cloning is cheap (`Arc`)
/// and shares the same underlying offset, so multiple clients in the same
/// process can be wired to a single test clock.
#[derive(Debug, Clone, Default)]
pub struct TestClock {
    offset_millis: Arc<AtomicI64>,
}

impl TestClock {
    /// Create a fresh test clock with zero offset (i.e. `now() == Utc::now()`).
    pub fn new() -> Self {
        Self::default()
    }

    /// Move the clock forward by `delta`. Negative deltas move it backwards,
    /// which is supported but rarely what you want — the orderNo lock model
    /// assumes a roughly monotonic clock.
    pub fn advance(&self, delta: Duration) {
        self.offset_millis
            .fetch_add(delta.num_milliseconds(), Ordering::SeqCst);
    }

    /// Replace the current offset wholesale (useful for setting an absolute
    /// offset from epoch-now rather than relative advancement). Sub-millisecond
    /// precision is truncated.
    pub fn set_offset(&self, offset: Duration) {
        self.offset_millis
            .store(offset.num_milliseconds(), Ordering::SeqCst);
    }

    /// Read the current offset as a [`Duration`].
    pub fn offset(&self) -> Duration {
        Duration::milliseconds(self.offset_millis.load(Ordering::SeqCst))
    }
}

impl Clock for TestClock {
    fn now(&self) -> DateTime<Utc> {
        let offset = self.offset_millis.load(Ordering::SeqCst);
        Utc::now() + chrono::Duration::milliseconds(offset)
    }

    fn now_millis(&self) -> i64 {
        let offset = self.offset_millis.load(Ordering::SeqCst);
        Utc::now().timestamp_millis() + offset
    }
}

/// Convenience alias used throughout the crate.
pub type ClockRef = Arc<dyn Clock>;

/// Convenience helper: a system clock as a trait object.
pub fn system_clock() -> ClockRef {
    Arc::new(SystemClock)
}
