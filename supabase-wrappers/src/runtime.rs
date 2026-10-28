//! Async runtime for running FDW I/O without blocking Postgres interrupts
//!
//! Postgres callbacks are synchronous, so FDWs have to block the backend while
//! waiting on async I/O. A plain `tokio::runtime::Runtime::block_on()` waits
//! inside tokio's own event loop, which never looks at Postgres' interrupt
//! flags, so a backend stuck on an unresponsive remote ignores query cancel,
//! `statement_timeout` and `pg_terminate_backend()`.
//!
//! This module takes the same approach as `postgres_fdw` (see
//! `libpqsrv_get_result()`): the backend waits in `WaitLatchOrSocket()` and
//! runs `CHECK_FOR_INTERRUPTS()` every time it wakes up. To make that
//! possible, the roles are split:
//!
//! - socket and timer readiness is handled by tokio's I/O driver on a single
//!   background worker thread, shared by every FDW instance in the backend;
//! - the future itself is polled on the backend's main thread, so it doesn't
//!   need to be `Send` and can still call Postgres functions;
//! - when the I/O driver wakes the future, the waker writes a byte to a
//!   process-local pipe which `WaitLatchOrSocket()` is watching. Postgres
//!   signal handlers set `MyLatch`, which wakes the same wait.
//!
//! Nothing wakes up periodically: the backend sleeps until there is I/O, a
//! timer fires, or Postgres sets the latch.
//!
//! On interrupt, `CHECK_FOR_INTERRUPTS()` raises an `ERROR` (or exits on
//! `FATAL`), which unwinds through `block_on()` and drops the in-flight
//! future, closing any connections it owns.
//!
//! Futures which block inside `poll()` without yielding (e.g. synchronous I/O)
//! can't be interrupted by this mechanism.

use pgrx::pg_sys;
use std::future::Future;
use std::io::{self, Read, Write};
use std::os::fd::AsRawFd;
use std::os::unix::net::UnixStream;
use std::pin::pin;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, OnceLock};
use std::task::{Context, Poll, Wake, Waker};
use tokio::runtime::Builder;
use tokio::task::JoinHandle;

/// The process-wide tokio runtime driving I/O and timers for all FDWs
static TOKIO_RUNTIME: OnceLock<tokio::runtime::Runtime> = OnceLock::new();

/// Process-local pipe used by wakers to wake up the backend's latch wait
static WAKE_PIPE: OnceLock<WakePipe> = OnceLock::new();

struct WakePipe {
    rx: UnixStream,
    tx: UnixStream,
}

impl WakePipe {
    fn new() -> io::Result<Self> {
        let (rx, tx) = UnixStream::pair()?;
        rx.set_nonblocking(true)?;
        tx.set_nonblocking(true)?;
        Ok(Self { rx, tx })
    }

    fn drain(&self) {
        let mut buf = [0u8; 64];
        while matches!((&self.rx).read(&mut buf), Ok(n) if n > 0) {}
    }
}

/// Waker for a single `block_on()` call
struct BlockOnWaker {
    notified: AtomicBool,
    pipe: &'static WakePipe,
}

impl Wake for BlockOnWaker {
    fn wake(self: Arc<Self>) {
        self.wake_by_ref();
    }

    fn wake_by_ref(self: &Arc<Self>) {
        // called from tokio's worker thread, so only signal-safe, non-Postgres
        // operations are allowed here
        if !self.notified.swap(true, Ordering::AcqRel) {
            // a full pipe already guarantees a pending wakeup
            let _ = (&self.pipe.tx).write(&[1]);
        }
    }
}

fn block_all_signals() {
    // Postgres signals must be handled on the backend's main thread, so make
    // sure they're never delivered to the runtime's threads
    unsafe {
        let mut set = std::mem::MaybeUninit::<libc::sigset_t>::uninit();
        libc::sigfillset(set.as_mut_ptr());
        libc::pthread_sigmask(libc::SIG_BLOCK, set.as_ptr(), std::ptr::null_mut());
    }
}

fn tokio_runtime() -> io::Result<&'static tokio::runtime::Runtime> {
    if let Some(rt) = TOKIO_RUNTIME.get() {
        return Ok(rt);
    }
    let rt = Builder::new_multi_thread()
        .worker_threads(1)
        .thread_name("wrappers-io")
        .on_thread_start(block_all_signals)
        .enable_all()
        .build()?;
    Ok(TOKIO_RUNTIME.get_or_init(|| rt))
}

fn wake_pipe() -> io::Result<&'static WakePipe> {
    if let Some(pipe) = WAKE_PIPE.get() {
        return Ok(pipe);
    }
    let pipe = WakePipe::new()?;
    Ok(WAKE_PIPE.get_or_init(|| pipe))
}

/// Async runtime handle for FDWs
///
/// Create it with [`create_async_runtime`](crate::utils::create_async_runtime).
/// All instances share one process-wide tokio runtime, so creating and dropping
/// it is cheap.
#[derive(Clone, Copy, Debug)]
pub struct Runtime {
    rt: &'static tokio::runtime::Runtime,
    pipe: &'static WakePipe,
}

impl std::fmt::Debug for WakePipe {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("WakePipe").finish_non_exhaustive()
    }
}

impl Runtime {
    pub(crate) fn new() -> io::Result<Self> {
        Ok(Self {
            rt: tokio_runtime()?,
            pipe: wake_pipe()?,
        })
    }

    /// Run a future to completion on the current thread, servicing Postgres
    /// interrupts while waiting
    ///
    /// If a query cancel, `statement_timeout` or backend termination arrives
    /// while waiting, the corresponding Postgres error is raised and the
    /// future is dropped.
    ///
    /// Must be called from the backend's main thread.
    pub fn block_on<F: Future>(&self, fut: F) -> F::Output {
        let _guard = self.rt.enter();
        let state = Arc::new(BlockOnWaker {
            notified: AtomicBool::new(false),
            pipe: self.pipe,
        });
        let waker = Waker::from(state.clone());
        let mut cx = Context::from_waker(&waker);
        let mut fut = pin!(fut);

        loop {
            if let Poll::Ready(output) = fut.as_mut().poll(&mut cx) {
                return output;
            }

            // only sleep if the future wasn't woken while it was being polled
            if !state.notified.load(Ordering::Acquire) {
                unsafe {
                    pg_sys::WaitLatchOrSocket(
                        pg_sys::MyLatch,
                        (pg_sys::WL_LATCH_SET
                            | pg_sys::WL_SOCKET_READABLE
                            | pg_sys::WL_EXIT_ON_PM_DEATH) as _,
                        self.pipe.rx.as_raw_fd(),
                        -1,
                        pg_sys::PG_WAIT_EXTENSION,
                    );
                    pg_sys::ResetLatch(pg_sys::MyLatch);
                }
            }
            state.notified.store(false, Ordering::Release);
            self.pipe.drain();

            // raises an error on cancel / termination, which drops `fut`
            pgrx::check_for_interrupts!();
        }
    }

    /// Run blocking code on the runtime's blocking thread pool, servicing
    /// Postgres interrupts while waiting for it to finish
    ///
    /// For synchronous libraries which can't be polled. If a query cancel,
    /// `statement_timeout` or backend termination arrives while waiting,
    /// `on_interrupt` is called to make `f` stop early (e.g. by interrupting a
    /// query) and the corresponding Postgres error is raised without waiting
    /// for `f` to return.
    ///
    /// `f` must not call any Postgres functions, since it doesn't run on the
    /// backend's main thread.
    pub fn run_blocking<F, R>(&self, f: F, on_interrupt: impl FnOnce()) -> R
    where
        F: FnOnce() -> R + Send + 'static,
        R: Send + 'static,
    {
        struct OnDrop<C: FnOnce()>(Option<C>);

        impl<C: FnOnce()> Drop for OnDrop<C> {
            fn drop(&mut self) {
                if let Some(f) = self.0.take() {
                    f();
                }
            }
        }

        let task = self.rt.spawn_blocking(f);
        let mut on_interrupt = OnDrop(Some(on_interrupt));
        let result = self.block_on(task);
        on_interrupt.0 = None;

        match result {
            Ok(output) => output,
            Err(err) => std::panic::resume_unwind(err.into_panic()),
        }
    }

    /// Spawn a `Send` future onto the runtime's worker thread
    ///
    /// The task must not call any Postgres functions, since it doesn't run on
    /// the backend's main thread.
    pub fn spawn<F>(&self, fut: F) -> JoinHandle<F::Output>
    where
        F: Future + Send + 'static,
        F::Output: Send + 'static,
    {
        self.rt.spawn(fut)
    }
}
