//! Helpers for testing that FDWs stay responsive to Postgres interrupts while
//! waiting on a remote that never answers

use pgrx::pg_sys::panic::CaughtError;
use pgrx::prelude::*;
use std::io::{BufRead, BufReader, Read, Write};
use std::net::{TcpListener, TcpStream};
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

/// A request received by [`HttpServer`]
#[allow(dead_code)]
pub(crate) struct HttpRequest {
    pub(crate) method: String,
    pub(crate) target: String,
    headers: Vec<(String, String)>,
}

#[allow(dead_code)]
impl HttpRequest {
    pub(crate) fn header(&self, name: &str) -> Option<&str> {
        self.headers
            .iter()
            .find(|(k, _)| k.eq_ignore_ascii_case(name))
            .map(|(_, v)| v.as_str())
    }
}

/// A response sent by [`HttpServer`]
pub(crate) struct HttpResponse {
    pub(crate) status: u16,
    pub(crate) headers: Vec<(String, String)>,
    pub(crate) body: Vec<u8>,
}

type HttpHandler = dyn Fn(&HttpRequest) -> Option<HttpResponse> + Send + Sync;

/// A minimal HTTP/1.1 server inside this backend, for remotes which answer
/// some requests and then stop responding
///
/// The handler returns the response to send, or `None` to leave the request
/// unanswered forever.
// only used by FDW tests, which are enabled by feature flags
#[allow(dead_code)]
pub(crate) struct HttpServer {
    pub(crate) port: u16,
    answered: Arc<AtomicUsize>,
    hung: Arc<AtomicUsize>,
}

#[allow(dead_code)]
impl HttpServer {
    pub(crate) fn start(
        handler: impl Fn(&HttpRequest) -> Option<HttpResponse> + Send + Sync + 'static,
    ) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let port = listener.local_addr().unwrap().port();
        let answered = Arc::new(AtomicUsize::new(0));
        let hung = Arc::new(AtomicUsize::new(0));
        let handler: Arc<HttpHandler> = Arc::new(handler);
        {
            let answered = answered.clone();
            let hung = hung.clone();
            std::thread::spawn(move || {
                block_all_signals();
                for stream in listener.incoming().flatten() {
                    let handler = handler.clone();
                    let answered = answered.clone();
                    let hung = hung.clone();
                    std::thread::spawn(move || {
                        block_all_signals();
                        serve_connection(stream, &*handler, &answered, &hung);
                    });
                }
            });
        }
        Self {
            port,
            answered,
            hung,
        }
    }

    /// Condition for [`assert_cancellable_when`]: a request is left unanswered
    pub(crate) fn when_hung(&self) -> impl Fn() -> bool + Send + 'static {
        let hung = self.hung.clone();
        move || hung.load(Ordering::Acquire) > 0
    }

    /// Condition for [`assert_cancellable_when`]: `n` requests are answered
    pub(crate) fn when_answered(&self, n: usize) -> impl Fn() -> bool + Send + 'static {
        let answered = self.answered.clone();
        move || answered.load(Ordering::Acquire) >= n
    }
}

fn serve_connection(
    stream: TcpStream,
    handler: &HttpHandler,
    answered: &AtomicUsize,
    hung: &AtomicUsize,
) {
    let mut writer = stream.try_clone().unwrap();
    let mut reader = BufReader::new(stream);
    loop {
        let mut line = String::new();
        if reader.read_line(&mut line).unwrap_or(0) == 0 {
            return;
        }
        let mut parts = line.split_whitespace();
        let method = parts.next().unwrap_or_default().to_string();
        let target = parts.next().unwrap_or_default().to_string();

        let mut headers = Vec::new();
        loop {
            let mut line = String::new();
            if reader.read_line(&mut line).unwrap_or(0) == 0 {
                return;
            }
            let line = line.trim_end();
            if line.is_empty() {
                break;
            }
            if let Some((k, v)) = line.split_once(':') {
                headers.push((k.trim().to_string(), v.trim().to_string()));
            }
        }
        let req = HttpRequest {
            method,
            target,
            headers,
        };
        let body_len = req
            .header("content-length")
            .and_then(|v| v.parse::<usize>().ok())
            .unwrap_or(0);
        let mut body = vec![0u8; body_len];
        if reader.read_exact(&mut body).is_err() {
            return;
        }

        let Some(resp) = handler(&req) else {
            // hold the connection open without answering until the client
            // goes away
            hung.fetch_add(1, Ordering::Release);
            let _ = reader.read_to_end(&mut Vec::new());
            return;
        };

        let mut out = format!("HTTP/1.1 {} Status\r\n", resp.status);
        for (k, v) in &resp.headers {
            out.push_str(&format!("{k}: {v}\r\n"));
        }
        out.push_str(&format!("Content-Length: {}\r\n\r\n", resp.body.len()));
        if writer.write_all(out.as_bytes()).is_err() || writer.write_all(&resp.body).is_err() {
            return;
        }
        answered.fetch_add(1, Ordering::Release);
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
pub(crate) fn assert_cancellable_when(sql: &str, blocked: impl Fn() -> bool + Send + 'static) {
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
