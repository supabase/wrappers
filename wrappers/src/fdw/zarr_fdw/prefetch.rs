//! Ordered, bounded foreground prefetch for encoded Zarr chunk objects.
//!
//! Futures are polled directly by the PostgreSQL backend's `Runtime::block_on`
//! call and are never spawned. This keeps output deterministic and makes
//! cancellation explicit: every queued future is dropped before
//! [`PrefetchNext::Interrupted`] is returned. The caller may then leave
//! `block_on` and raise PostgreSQL's interrupt on a clean Rust stack.

use futures_util::FutureExt;
use futures_util::future::LocalBoxFuture;
use futures_util::stream::{FuturesOrdered, StreamExt};
use std::future::Future;
use std::sync::Arc;
use std::time::Duration;
use thiserror::Error;
use tokio::time::{MissedTickBehavior, interval};

use super::cache::{CachedObject, CompressedChunkCache};

#[derive(Debug, Error, PartialEq, Eq)]
pub(crate) enum PrefetchConfigError {
    #[error("max concurrent reads must be greater than zero")]
    NoConcurrentReads,
    #[error("max inflight bytes must be greater than zero")]
    EmptyByteBudget,
    #[error("interrupt poll interval must be greater than zero")]
    DisabledInterruptPolling,
}

/// One object fetch plus caller-owned context such as an N-dimensional chunk
/// coordinate.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct PrefetchRequest<T> {
    pub context: T,
    pub key: String,
    /// Maximum bytes the storage read is allowed to return. The prefetcher
    /// reserves this conservative amount before issuing the request.
    pub max_bytes: usize,
}

#[derive(Debug, PartialEq, Eq)]
pub(crate) enum ScheduleError<T> {
    /// Retry after consuming the next ordered result.
    WindowFull(PrefetchRequest<T>),
    /// This request can never fit the configured inflight byte budget.
    RequestTooLarge {
        request: PrefetchRequest<T>,
        max_inflight_bytes: usize,
    },
    /// A cached object must obey the same bounded-read contract as a remote
    /// response, even if another caller previously used a larger limit.
    CachedObjectTooLarge {
        request: PrefetchRequest<T>,
        actual_bytes: usize,
    },
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum PrefetchSource {
    Cache,
    Remote,
}

#[derive(Debug, PartialEq, Eq)]
pub(crate) struct PrefetchedObject<T> {
    pub request: PrefetchRequest<T>,
    pub object: CachedObject,
    pub source: PrefetchSource,
    /// Actual encoded bytes fetched from the object store. Cache hits and
    /// explicit missing-object responses report zero.
    pub remote_bytes: usize,
}

#[derive(Debug, PartialEq, Eq)]
pub(crate) enum PrefetchNext<T, E> {
    Ready(PrefetchedObject<T>),
    FetchError {
        request: PrefetchRequest<T>,
        error: E,
    },
    /// All queued futures were dropped before this value was exposed.
    Interrupted,
    Empty,
}

enum Completion<T, E> {
    Cached(PrefetchRequest<T>, CachedObject),
    Fetched(PrefetchRequest<T>, Result<Option<Vec<u8>>, E>),
}

type Pending<T, E> = LocalBoxFuture<'static, Completion<T, E>>;

/// A deterministic request window with both request-count and encoded-byte
/// backpressure.
pub(crate) struct OrderedPrefetch<T: 'static, E: 'static> {
    pending: FuturesOrdered<Pending<T, E>>,
    max_concurrent_reads: usize,
    max_inflight_bytes: usize,
    interrupt_poll_interval: Duration,
    inflight_reads: usize,
    reserved_bytes: usize,
}

impl<T: 'static, E: 'static> OrderedPrefetch<T, E> {
    pub(crate) fn new(
        max_concurrent_reads: usize,
        max_inflight_bytes: usize,
        interrupt_poll_interval: Duration,
    ) -> Result<Self, PrefetchConfigError> {
        if max_concurrent_reads == 0 {
            return Err(PrefetchConfigError::NoConcurrentReads);
        }
        if max_inflight_bytes == 0 {
            return Err(PrefetchConfigError::EmptyByteBudget);
        }
        if interrupt_poll_interval.is_zero() {
            return Err(PrefetchConfigError::DisabledInterruptPolling);
        }
        Ok(Self {
            pending: FuturesOrdered::new(),
            max_concurrent_reads,
            max_inflight_bytes,
            interrupt_poll_interval,
            inflight_reads: 0,
            reserved_bytes: 0,
        })
    }

    /// Schedule one request without spawning it.
    ///
    /// `fetch` is not called on a cache hit. It receives an owned key so the
    /// returned future can own a cloned storage client and remain independent
    /// of the scan struct's borrow.
    pub(crate) fn try_schedule<F, Fut>(
        &mut self,
        request: PrefetchRequest<T>,
        cache: &mut CompressedChunkCache,
        fetch: F,
    ) -> Result<PrefetchSource, ScheduleError<T>>
    where
        F: FnOnce(String, usize) -> Fut,
        Fut: Future<Output = Result<Option<Vec<u8>>, E>> + 'static,
    {
        if self.pending.len() >= self.max_concurrent_reads {
            return Err(ScheduleError::WindowFull(request));
        }

        if let Some(object) = cache.get(&request.key) {
            if let CachedObject::Present(bytes) = &object
                && bytes.len() > request.max_bytes
            {
                return Err(ScheduleError::CachedObjectTooLarge {
                    request,
                    actual_bytes: bytes.len(),
                });
            }
            self.pending
                .push_back(async move { Completion::Cached(request, object) }.boxed_local());
            return Ok(PrefetchSource::Cache);
        }

        if request.max_bytes > self.max_inflight_bytes {
            return Err(ScheduleError::RequestTooLarge {
                request,
                max_inflight_bytes: self.max_inflight_bytes,
            });
        }
        if self.inflight_reads >= self.max_concurrent_reads
            || self
                .reserved_bytes
                .checked_add(request.max_bytes)
                .is_none_or(|next| next > self.max_inflight_bytes)
        {
            return Err(ScheduleError::WindowFull(request));
        }

        let key = request.key.clone();
        let max_bytes = request.max_bytes;
        let future = fetch(key, max_bytes);
        self.pending
            .push_back(async move { Completion::Fetched(request, future.await) }.boxed_local());
        self.inflight_reads += 1;
        self.reserved_bytes += max_bytes;
        Ok(PrefetchSource::Remote)
    }

    /// Return the next result in scheduling order.
    ///
    /// `interrupt_requested` must only inspect cancellation state; it must not
    /// raise a PostgreSQL error. On `Interrupted`, this method first clears the
    /// entire queue and resets accounting. The caller can safely return from
    /// `block_on` before invoking PostgreSQL interrupt processing.
    pub(crate) async fn next_interruptible<I>(
        &mut self,
        cache: &mut CompressedChunkCache,
        mut interrupt_requested: I,
    ) -> PrefetchNext<T, E>
    where
        I: FnMut() -> bool,
    {
        if self.pending.is_empty() {
            return PrefetchNext::Empty;
        }
        if interrupt_requested() {
            self.clear();
            return PrefetchNext::Interrupted;
        }

        let mut ticker = interval(self.interrupt_poll_interval);
        ticker.set_missed_tick_behavior(MissedTickBehavior::Skip);
        // Tokio intervals tick immediately once. Consume that tick so the
        // queued I/O receives a full polling interval before the next check.
        ticker.tick().await;

        loop {
            enum Wait<T, E> {
                Completion(Option<Completion<T, E>>),
                Tick,
            }

            let wait = {
                let next = self.pending.next();
                tokio::pin!(next);
                tokio::select! {
                    biased;
                    _ = ticker.tick() => Wait::Tick,
                    completion = &mut next => Wait::Completion(completion),
                }
            };

            match wait {
                Wait::Tick if interrupt_requested() => {
                    self.clear();
                    return PrefetchNext::Interrupted;
                }
                Wait::Tick => continue,
                Wait::Completion(None) => return PrefetchNext::Empty,
                Wait::Completion(Some(Completion::Cached(request, object))) => {
                    return PrefetchNext::Ready(PrefetchedObject {
                        request,
                        object,
                        source: PrefetchSource::Cache,
                        remote_bytes: 0,
                    });
                }
                Wait::Completion(Some(Completion::Fetched(request, result))) => {
                    self.inflight_reads = self.inflight_reads.saturating_sub(1);
                    self.reserved_bytes = self.reserved_bytes.saturating_sub(request.max_bytes);
                    return match result {
                        Ok(Some(bytes)) => {
                            let remote_bytes = bytes.len();
                            let bytes: Arc<[u8]> = Arc::from(bytes);
                            cache.insert_present(request.key.clone(), Arc::clone(&bytes));
                            PrefetchNext::Ready(PrefetchedObject {
                                request,
                                object: CachedObject::Present(bytes),
                                source: PrefetchSource::Remote,
                                remote_bytes,
                            })
                        }
                        Ok(None) => {
                            cache.insert_missing(request.key.clone());
                            PrefetchNext::Ready(PrefetchedObject {
                                request,
                                object: CachedObject::Missing,
                                source: PrefetchSource::Remote,
                                remote_bytes: 0,
                            })
                        }
                        Err(error) => {
                            // A storage failure aborts this ordered window. Do
                            // not leave later requests alive while the caller
                            // converts the error into a PostgreSQL error.
                            self.clear();
                            PrefetchNext::FetchError { request, error }
                        }
                    };
                }
            }
        }
    }

    pub(crate) fn clear(&mut self) {
        self.pending = FuturesOrdered::new();
        self.inflight_reads = 0;
        self.reserved_bytes = 0;
    }

    #[cfg(test)]
    pub(crate) fn is_empty(&self) -> bool {
        self.pending.is_empty()
    }

    #[cfg(test)]
    pub(crate) fn inflight_reads(&self) -> usize {
        self.inflight_reads
    }

    #[cfg(test)]
    pub(crate) fn reserved_bytes(&self) -> usize {
        self.reserved_bytes
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::cell::Cell;
    use std::convert::Infallible;
    use std::rc::Rc;

    fn request(id: usize, max_bytes: usize) -> PrefetchRequest<usize> {
        PrefetchRequest {
            context: id,
            key: format!("chunk-{id}"),
            max_bytes,
        }
    }

    fn runtime() -> tokio::runtime::Runtime {
        tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap()
    }

    #[test]
    fn invalid_limits_are_rejected() {
        assert!(matches!(
            OrderedPrefetch::<(), Infallible>::new(0, 1, Duration::from_millis(1)),
            Err(PrefetchConfigError::NoConcurrentReads)
        ));
        assert!(matches!(
            OrderedPrefetch::<(), Infallible>::new(1, 0, Duration::from_millis(1)),
            Err(PrefetchConfigError::EmptyByteBudget)
        ));
        assert!(matches!(
            OrderedPrefetch::<(), Infallible>::new(1, 1, Duration::ZERO),
            Err(PrefetchConfigError::DisabledInterruptPolling)
        ));
    }

    #[test]
    fn results_remain_in_schedule_order_when_completions_do_not() {
        let rt = runtime();
        rt.block_on(async {
            let mut cache = CompressedChunkCache::new(64, 8);
            let mut prefetch =
                OrderedPrefetch::<usize, Infallible>::new(3, 30, Duration::from_millis(2)).unwrap();
            for (id, delay_ms) in [(0, 30), (1, 1), (2, 5)] {
                prefetch
                    .try_schedule(request(id, 10), &mut cache, move |_key, _max| async move {
                        tokio::time::sleep(Duration::from_millis(delay_ms)).await;
                        Ok(Some(vec![id as u8]))
                    })
                    .unwrap();
            }

            let mut observed = Vec::new();
            while !prefetch.is_empty() {
                let PrefetchNext::Ready(value) =
                    prefetch.next_interruptible(&mut cache, || false).await
                else {
                    panic!("expected a prefetched object");
                };
                observed.push(value.request.context);
            }
            assert_eq!(observed, vec![0, 1, 2]);
        });
    }

    #[test]
    fn request_and_byte_limits_apply_before_fetch_creation() {
        let mut cache = CompressedChunkCache::new(16, 4);
        let mut prefetch =
            OrderedPrefetch::<usize, Infallible>::new(2, 10, Duration::from_millis(1)).unwrap();
        prefetch
            .try_schedule(request(0, 6), &mut cache, |_key, _max| async {
                Ok(Some(vec![0]))
            })
            .unwrap();
        assert!(matches!(
            prefetch.try_schedule(request(1, 5), &mut cache, |_key, _max| async {
                Ok(Some(vec![1]))
            }),
            Err(ScheduleError::WindowFull(_))
        ));
        assert!(matches!(
            prefetch.try_schedule(request(2, 11), &mut cache, |_key, _max| async {
                Ok(Some(vec![2]))
            }),
            Err(ScheduleError::RequestTooLarge { .. })
        ));
        assert_eq!(prefetch.inflight_reads(), 1);
        assert_eq!(prefetch.reserved_bytes(), 6);
    }

    #[test]
    fn cache_hits_preserve_order_without_invoking_fetch() {
        let rt = runtime();
        rt.block_on(async {
            let mut cache = CompressedChunkCache::new(16, 4);
            let cached: Arc<[u8]> = Arc::from(vec![7_u8]);
            cache.insert_present("chunk-1".to_string(), cached);
            let fetch_calls = Rc::new(Cell::new(0));
            let mut prefetch =
                OrderedPrefetch::<usize, Infallible>::new(1, 8, Duration::from_millis(1)).unwrap();
            let calls = Rc::clone(&fetch_calls);
            prefetch
                .try_schedule(request(1, 8), &mut cache, move |_key, _max| {
                    calls.set(calls.get() + 1);
                    async { Ok(Some(vec![9])) }
                })
                .unwrap();

            let PrefetchNext::Ready(value) =
                prefetch.next_interruptible(&mut cache, || false).await
            else {
                panic!("expected a cache hit");
            };
            assert_eq!(value.source, PrefetchSource::Cache);
            assert_eq!(value.remote_bytes, 0);
            assert_eq!(fetch_calls.get(), 0);
        });
    }

    #[test]
    fn cache_hits_still_obey_the_request_read_limit() {
        let mut cache = CompressedChunkCache::new(16, 4);
        let cached: Arc<[u8]> = Arc::from(vec![1_u8, 2, 3, 4]);
        cache.insert_present("chunk-1".to_string(), cached);
        let mut prefetch =
            OrderedPrefetch::<usize, Infallible>::new(1, 8, Duration::from_millis(1)).unwrap();

        assert!(matches!(
            prefetch.try_schedule(request(1, 3), &mut cache, |_key, _max| async {
                Ok(Some(Vec::new()))
            }),
            Err(ScheduleError::CachedObjectTooLarge {
                actual_bytes: 4,
                ..
            })
        ));
        assert!(prefetch.is_empty());
    }

    struct ActiveGuard {
        active: Rc<Cell<usize>>,
        dropped: Rc<Cell<usize>>,
    }

    impl ActiveGuard {
        fn new(active: Rc<Cell<usize>>, dropped: Rc<Cell<usize>>) -> Self {
            active.set(active.get() + 1);
            Self { active, dropped }
        }
    }

    impl Drop for ActiveGuard {
        fn drop(&mut self) {
            self.active.set(self.active.get() - 1);
            self.dropped.set(self.dropped.get() + 1);
        }
    }

    #[test]
    fn cancellation_drops_every_future_before_returning_interrupted() {
        let rt = runtime();
        rt.block_on(async {
            let mut cache = CompressedChunkCache::new(16, 4);
            let active = Rc::new(Cell::new(0));
            let dropped = Rc::new(Cell::new(0));
            let mut prefetch =
                OrderedPrefetch::<usize, Infallible>::new(2, 16, Duration::from_millis(1)).unwrap();
            for id in 0..2 {
                let active = Rc::clone(&active);
                let dropped = Rc::clone(&dropped);
                prefetch
                    .try_schedule(request(id, 8), &mut cache, move |_key, _max| async move {
                        let _guard = ActiveGuard::new(active, dropped);
                        std::future::pending::<Result<Option<Vec<u8>>, Infallible>>().await
                    })
                    .unwrap();
            }

            let checks = Cell::new(0);
            let result = prefetch
                .next_interruptible(&mut cache, || {
                    checks.set(checks.get() + 1);
                    checks.get() >= 2
                })
                .await;

            assert_eq!(result, PrefetchNext::Interrupted);
            assert_eq!(active.get(), 0);
            assert_eq!(dropped.get(), 2);
            assert!(prefetch.is_empty());
            assert_eq!(prefetch.inflight_reads(), 0);
            assert_eq!(prefetch.reserved_bytes(), 0);
        });
    }

    #[test]
    fn first_fetch_error_drops_every_later_future_and_resets_accounting() {
        let rt = runtime();
        rt.block_on(async {
            let mut cache = CompressedChunkCache::new(24, 4);
            let active = Rc::new(Cell::new(0));
            let dropped = Rc::new(Cell::new(0));
            let mut prefetch =
                OrderedPrefetch::<usize, &'static str>::new(3, 24, Duration::from_millis(1))
                    .unwrap();
            prefetch
                .try_schedule(request(0, 8), &mut cache, |_key, _max| async {
                    Err("first fetch failed")
                })
                .unwrap();
            for id in 1..=2 {
                let guard = ActiveGuard::new(Rc::clone(&active), Rc::clone(&dropped));
                prefetch
                    .try_schedule(request(id, 8), &mut cache, move |_key, _max| async move {
                        let _guard = guard;
                        std::future::pending::<Result<Option<Vec<u8>>, &'static str>>().await
                    })
                    .unwrap();
            }

            let result = prefetch.next_interruptible(&mut cache, || false).await;
            assert!(matches!(
                result,
                PrefetchNext::FetchError {
                    error: "first fetch failed",
                    ..
                }
            ));
            assert_eq!(active.get(), 0);
            assert_eq!(dropped.get(), 2);
            assert!(prefetch.is_empty());
            assert_eq!(prefetch.inflight_reads(), 0);
            assert_eq!(prefetch.reserved_bytes(), 0);
        });
    }

    #[test]
    fn remote_missing_objects_are_cached_without_bytes() {
        let rt = runtime();
        rt.block_on(async {
            let mut cache = CompressedChunkCache::new(16, 4);
            let mut prefetch =
                OrderedPrefetch::<usize, Infallible>::new(1, 8, Duration::from_millis(1)).unwrap();
            prefetch
                .try_schedule(request(0, 8), &mut cache, |_key, _max| async { Ok(None) })
                .unwrap();
            let PrefetchNext::Ready(value) =
                prefetch.next_interruptible(&mut cache, || false).await
            else {
                panic!("expected a missing result");
            };
            assert_eq!(value.object, CachedObject::Missing);
            assert_eq!(value.source, PrefetchSource::Remote);
            assert_eq!(cache.get("chunk-0"), Some(CachedObject::Missing));
            assert_eq!(cache.resident_bytes(), 0);
        });
    }
}
