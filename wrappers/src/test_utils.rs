//! Helpers for testing that FDWs stay responsive to Postgres interrupts while
//! waiting on a remote that never answers

use pgrx::pg_sys::panic::CaughtError;
use pgrx::prelude::*;
use std::net::TcpListener;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

/// How long to wait after the query is blocked before cancelling it, so the
/// backend is inside the wait rather than still sending the request
const CANCEL_GRACE: Duration = Duration::from_millis(500);

/// How long the cancelled query may take to return after the cancel is sent
const MAX_CANCEL_LATENCY: Duration = Duration::from_secs(3);

/// When to give up on a backend which ignores the cancel and kill it, so the
/// test fails instead of hanging
const KILL_AFTER: Duration = Duration::from_secs(30);

const CANCEL_MESSAGE: &str = "canceling statement due to user request";

/// A TCP server inside this backend which accepts connections but never reads
/// or writes anything
// only used by FDW tests, which are enabled by feature flags
#[allow(dead_code)]
pub(crate) struct UnresponsiveServer {
    pub(crate) port: u16,
    connections: Arc<AtomicUsize>,
}

#[allow(dead_code)]
impl UnresponsiveServer {
    pub(crate) fn start() -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let port = listener.local_addr().unwrap().port();
        let connections = Arc::new(AtomicUsize::new(0));
        let accepted = connections.clone();
        std::thread::spawn(move || {
            block_all_signals();
            let mut held = Vec::new();
            for stream in listener.incoming().flatten() {
                held.push(stream);
                accepted.fetch_add(1, Ordering::Release);
            }
        });
        Self { port, connections }
    }

    /// Run `sql`, which is expected to block on this server, cancel it once
    /// it has connected, and assert the cancel is serviced promptly
    pub(crate) fn assert_cancellable(&self, sql: &str) {
        let connections = self.connections.clone();
        assert_cancellable_when(sql, move || connections.load(Ordering::Acquire) > 0);
        assert!(
            self.connections.load(Ordering::Acquire) > 0,
            "expected the query to connect to the unresponsive server"
        );
    }
}

/// Run `sql`, which is expected to block forever, cancel it and assert the
/// cancel is serviced promptly
pub(crate) fn assert_cancellable(sql: &str) {
    assert_cancellable_when(sql, || true);
}

/// Make sure Postgres signals are delivered to the backend's main thread
fn block_all_signals() {
    unsafe {
        let mut set = std::mem::MaybeUninit::<libc::sigset_t>::uninit();
        libc::sigfillset(set.as_mut_ptr());
        libc::pthread_sigmask(libc::SIG_BLOCK, set.as_ptr(), std::ptr::null_mut());
    }
}

/// Run `sql` and, once `blocked` returns true, cancel it the same way
/// `pg_cancel_backend()` does
fn assert_cancellable_when(sql: &str, blocked: impl Fn() -> bool + Send + 'static) {
    let done = Arc::new(AtomicBool::new(false));
    let cancelled_at = Arc::new(Mutex::new(None::<Instant>));
    let pid = std::process::id() as libc::pid_t;
    {
        let done = done.clone();
        let cancelled_at = cancelled_at.clone();
        std::thread::spawn(move || {
            block_all_signals();
            let start = Instant::now();
            let wait_until = |cond: &dyn Fn() -> bool| {
                while !done.load(Ordering::Acquire) && start.elapsed() < KILL_AFTER {
                    if cond() {
                        return true;
                    }
                    std::thread::sleep(Duration::from_millis(10));
                }
                false
            };

            if !wait_until(&blocked) {
                if !done.load(Ordering::Acquire) {
                    unsafe { libc::kill(pid, libc::SIGKILL) };
                }
                return;
            }
            std::thread::sleep(CANCEL_GRACE);
            if done.load(Ordering::Acquire) {
                return;
            }
            *cancelled_at.lock().unwrap() = Some(Instant::now());
            unsafe { libc::kill(pid, libc::SIGINT) };

            // the backend ignored the cancel, kill it so the test fails
            // instead of hanging
            if !wait_until(&|| false) && !done.load(Ordering::Acquire) {
                unsafe { libc::kill(pid, libc::SIGKILL) };
            }
        });
    }

    let message = PgTryBuilder::new(|| match Spi::run(sql) {
        Ok(_) => "query unexpectedly succeeded".to_string(),
        Err(e) => format!("unexpected SPI error: {e}"),
    })
    .catch_others(|e| match e {
        CaughtError::PostgresError(r)
        | CaughtError::ErrorReport(r)
        | CaughtError::RustPanic { ereport: r, .. } => r.message().to_string(),
    })
    .execute();
    let returned_at = Instant::now();
    done.store(true, Ordering::Release);

    assert_eq!(
        message, CANCEL_MESSAGE,
        "expected the query to be cancelled while blocked"
    );
    let cancelled_at = cancelled_at
        .lock()
        .unwrap()
        .expect("query was cancelled before the cancel was sent");
    let latency = returned_at.duration_since(cancelled_at);
    assert!(
        latency < MAX_CANCEL_LATENCY,
        "query cancel took too long to be serviced: {latency:?}"
    );
}
