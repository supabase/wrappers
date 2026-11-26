// Allowing dead code because this helper is only used by
// FDW tests that opt into it, so it is unused in builds
// which don't enable any of those FDWs.
#![allow(dead_code)]

use std::{
    net::TcpListener,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    thread,
    time::{Duration, Instant},
};

use pgrx::pg_sys::panic::CaughtError;
use pgrx::{PgSqlErrorCode, pg_sys, prelude::*};

/// A TCP server which listens on an available port but doesn't
/// respond to anything. It doesn't read or write anything on the
/// established connection. This is used during tests to simulate
/// stuck remote servers, useful to test blocking I/O calls made
/// by wrappers' FDWs and whether they can be cancelled or not.
pub(crate) struct UnresponsiveServer {
    /// Port on which the server is listening
    pub(crate) port: u16,

    /// True when the client has connected, false otherwise
    pub(crate) client_connected: Arc<AtomicBool>,
}

impl UnresponsiveServer {
    /// Starts the server listening on an available port on the loopback
    pub(crate) fn start() -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let port = listener.local_addr().unwrap().port();

        let client_connected = Arc::new(AtomicBool::new(false));
        let client_connected_clone = client_connected.clone();

        thread::spawn(move || {
            // Since we are running in a Postgres backend, block signals
            // on any thread we spawn here
            block_all_signals();

            let mut streams = Vec::new();

            for stream in listener.incoming() {
                let stream = stream.unwrap();

                // Keep the streams around to avoid dropping the connections
                streams.push(stream);

                // Signal that the client has connected
                client_connected_clone.store(true, Ordering::Release);
            }
        });

        UnresponsiveServer {
            port,
            client_connected,
        }
    }

    /// Runs the query and then cancels it by calling `pg_cancel_backend`
    /// to test that it can be cancelled.
    pub(crate) fn assert_cancellable(&self, query: &str) {
        let query_result = self.run_and_interrupt(query, Interrupt::Cancel);

        assert_eq!(
            query_result,
            Some(PgSqlErrorCode::ERRCODE_QUERY_CANCELED),
            "query should have been cancelled by pg_cancel_backend"
        );
    }

    /// Runs the query and then terminates the backend by calling
    /// `pg_terminate_backend` to test that it can be terminated.
    /// A terminated backend exits with a FATAL error, so this function
    /// never returns when termination works.
    pub(crate) fn assert_terminable(&self, query: &str) {
        let query_result = self.run_and_interrupt(query, Interrupt::Terminate);

        panic!(
            "backend should have been terminated by pg_terminate_backend, but the query returned {query_result:?}"
        );
    }

    /// Runs the query and interrupts it from another thread. Interrupt
    /// can be either `pg_cancel_backend` or `pg_terminate_backend`.
    /// If the backend doesn't interrupt the query within 10 seconds it
    /// is sigkilled to avoid hanging the test indefinitely.
    ///
    /// Returns the query error code, if any.
    fn run_and_interrupt(&self, query: &str, interrupt: Interrupt) -> Option<PgSqlErrorCode> {
        let pid = unsafe { pg_sys::MyProcPid };
        let conn_settings = ConnectionSettings::from_current_server();

        let client_connected = self.client_connected.clone();
        let query_finished = Arc::new(AtomicBool::new(false));
        let query_finished_clone = query_finished.clone();

        let canceller_thread = thread::spawn(move || {
            block_all_signals();

            // Connect from this thread to issue the cancel command
            let mut client = postgres::Config::new()
                .host(&conn_settings.host)
                .port(conn_settings.port)
                .user(&conn_settings.user)
                .dbname(&conn_settings.dbname)
                .connect(postgres::NoTls)
                .expect("failed to connect to cancel the backend");

            // Make sure the query is stuck before trying to cancel it.
            while !client_connected.load(Ordering::Acquire) {
                thread::sleep(Duration::from_millis(100));
            }

            // Actually send the interrupt command
            let sent: bool = client
                .query_one(&format!("SELECT {}($1)", interrupt.sql_function()), &[&pid])
                .unwrap()
                .get(0);
            assert!(sent, "{}({pid}) returned false", interrupt.sql_function());

            // Keep looking for the interrupt to succeed for 10 seconds
            // and sigkill the backend after that to avoid hanging the
            // test indefinitely
            let deadline = Instant::now() + Duration::from_secs(10);
            while Instant::now() < deadline {
                if query_finished_clone.load(Ordering::Acquire) {
                    return;
                }
                thread::sleep(Duration::from_millis(100));
            }

            unsafe { libc::kill(pid, libc::SIGKILL) };
        });

        // Actually run the query to cancel. We wrap in PG_TRY/PG_CATCh
        // to avoid the code unwinding past the code after this call
        let query_result = run_query(query);

        // Tell the canceller thread that the query has returned
        query_finished.store(true, Ordering::Release);

        canceller_thread.join().unwrap();

        query_result
    }
}

/// How a backend is interrupted
#[derive(Clone, Copy)]
enum Interrupt {
    /// Backend is interrupted by calling `pg_cancel_backend`
    Cancel,
    /// Backend is interrupted by calling `pg_terminate_backend`
    Terminate,
}

impl Interrupt {
    /// Return the sql function name used to interrupt a backend
    fn sql_function(self) -> &'static str {
        match self {
            Interrupt::Cancel => "pg_cancel_backend",
            Interrupt::Terminate => "pg_terminate_backend",
        }
    }
}

/// Runs the query inside the PG_TRY / PG_CATCh blocks to
/// avoid the control unwinding past the code after this
/// call
fn run_query(query: &str) -> Option<PgSqlErrorCode> {
    PgTryBuilder::new(|| {
        Spi::run(query).unwrap();
        None
    })
    .catch_others(|e| match e {
        CaughtError::PostgresError(report)
        | CaughtError::ErrorReport(report)
        | CaughtError::RustPanic {
            ereport: report, ..
        } => Some(report.sql_error_code()),
    })
    .execute()
}

struct ConnectionSettings {
    host: String,
    port: u16,
    user: String,
    dbname: String,
}

impl ConnectionSettings {
    fn from_current_server() -> Self {
        let host = "127.0.0.1".to_string();
        let port: u16 = Spi::get_one::<String>("SELECT current_setting('port')")
            .unwrap()
            .unwrap()
            .parse()
            .unwrap();
        let user = Spi::get_one::<String>("SELECT current_user::text")
            .unwrap()
            .unwrap();
        let dbname = Spi::get_one::<String>("SELECT current_database()::text")
            .unwrap()
            .unwrap();

        ConnectionSettings {
            host,
            port,
            user,
            dbname,
        }
    }
}

/// Blocks all signals on the current thread. Postgres threads
/// spawned by a backend should have their signals blocked because
/// we always want the main thread to handle the signals.
fn block_all_signals() {
    unsafe {
        let mut set = std::mem::MaybeUninit::<libc::sigset_t>::uninit();
        libc::sigfillset(set.as_mut_ptr());
        libc::pthread_sigmask(libc::SIG_BLOCK, set.as_ptr(), std::ptr::null_mut());
    }
}
