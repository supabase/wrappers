use pgrx::pg_sys::panic::ErrorReportable;
use pgrx::prelude::*;
use std::collections::HashSet;
use supabase_wrappers::attrs::check_foreign_table_column_types;
use supabase_wrappers::event_triggers::called_as_event_trigger;

// pgrx::extension_sql! requires a string literal, so build.rs generates
// event_triggers_sql.rs with the versioned library name (e.g. "wrappers-0.6.0")
// embedded as a literal from CARGO_PKG_NAME/VERSION at compile time.
include!(concat!(env!("OUT_DIR"), "/event_triggers_sql.rs"));

/// Fired on `ddl_command_end` for `CREATE FOREIGN TABLE` / `ALTER FOREIGN TABLE`; rejects
/// the statement if a `wrappers`-backed foreign table it touched has an unsupported column
/// type. Scoped via `pg_depend` to tables whose FDW handler belongs to the `wrappers`
/// extension, so unrelated FDWs (`postgres_fdw`, etc.) are untouched.
#[unsafe(no_mangle)]
#[pg_guard]
pub unsafe extern "C-unwind" fn check_supported_column_types(
    fcinfo: &pg_sys::FunctionCallInfoBaseData,
) -> pg_sys::Datum {
    if unsafe { !called_as_event_trigger(fcinfo as *const _ as *mut _) } {
        return pg_sys::Datum::from(0);
    }

    let objids: Vec<pg_sys::Oid> = Spi::connect(|client| {
        client
            .select(
                "SELECT cmd.objid
                 FROM pg_event_trigger_ddl_commands() cmd
                 WHERE EXISTS (
                     SELECT 1
                     FROM pg_foreign_table ft
                     JOIN pg_foreign_server fs ON fs.oid = ft.ftserver
                     JOIN pg_foreign_data_wrapper fdw ON fdw.oid = fs.srvfdw
                     JOIN pg_depend dep
                         ON dep.classid = 'pg_proc'::regclass
                         AND dep.objid = fdw.fdwhandler
                         AND dep.refclassid = 'pg_extension'::regclass
                         AND dep.deptype = 'e'
                     JOIN pg_extension ext
                         ON ext.oid = dep.refobjid
                         AND ext.extname = 'wrappers'
                     WHERE ft.ftrelid = cmd.objid
                 )",
                None,
                &[],
            )
            .and_then(|table| {
                table
                    .map(|row| Ok(row.get::<pg_sys::Oid>(1)?.unwrap_or_default()))
                    .collect::<Result<Vec<_>, pgrx::spi::Error>>()
            })
    })
    .unwrap_or_report();

    // dedupe: a single ALTER TABLE with multiple ADD COLUMN subcommands can
    // yield one ddl_commands row per subcommand, all against the same table
    let mut seen = HashSet::new();
    for objid in objids {
        if seen.insert(objid) {
            check_foreign_table_column_types(objid).unwrap_or_report();
        }
    }

    pg_sys::Datum::from(0)
}

#[unsafe(no_mangle)]
pub extern "C-unwind" fn pg_finfo_check_supported_column_types() -> *const pg_sys::Pg_finfo_record {
    const MY_FINFO: pg_sys::Pg_finfo_record = pg_sys::Pg_finfo_record { api_version: 1 };
    &MY_FINFO
}

// Minimal FDW so the tests below have a `wrappers`-backed foreign table to test against
// without needing an optional FDW feature enabled (CI's default test run skips helloworld_fdw).
#[cfg(any(test, feature = "pg_test"))]
mod test_fdw {
    use pgrx::PgSqlErrorCode;
    use pgrx::pg_sys::panic::ErrorReport;
    use std::collections::HashMap;
    use supabase_wrappers::prelude::*;

    #[wrappers_fdw(
        version = "0.1.0",
        author = "test",
        website = "https://github.com/supabase/wrappers",
        error_type = "EventTriggerTestFdwError"
    )]
    pub(crate) struct EventTriggerTestFdw;

    pub(crate) enum EventTriggerTestFdwError {}

    impl From<EventTriggerTestFdwError> for ErrorReport {
        fn from(_value: EventTriggerTestFdwError) -> Self {
            ErrorReport::new(PgSqlErrorCode::ERRCODE_FDW_ERROR, "", "")
        }
    }

    type EventTriggerTestFdwResult<T> = Result<T, EventTriggerTestFdwError>;

    impl ForeignDataWrapper<EventTriggerTestFdwError> for EventTriggerTestFdw {
        fn new(_server: ForeignServer) -> EventTriggerTestFdwResult<Self> {
            Ok(Self)
        }

        fn begin_scan(
            &mut self,
            _quals: &[Qual],
            _columns: &[Column],
            _sorts: &[Sort],
            _limit: &Option<Limit>,
            _options: &HashMap<String, String>,
        ) -> EventTriggerTestFdwResult<()> {
            Ok(())
        }

        fn iter_scan(&mut self, _row: &mut Row) -> EventTriggerTestFdwResult<Option<()>> {
            Ok(None)
        }

        fn end_scan(&mut self) -> EventTriggerTestFdwResult<()> {
            Ok(())
        }
    }
}

#[cfg(any(test, feature = "pg_test"))]
#[pgrx::pg_schema]
mod tests {
    use pgrx::prelude::*;

    // no IF NOT EXISTS: each #[pg_test] runs in its own rolled-back subtransaction, and
    // CREATE FOREIGN DATA WRAPPER doesn't support IF NOT EXISTS anyway
    fn setup_test_fdw() {
        Spi::run(
            "create foreign data wrapper event_trigger_test_wrapper \
             handler event_trigger_test_fdw_handler validator event_trigger_test_fdw_validator",
        )
        .unwrap();
        Spi::run("create server event_trigger_test_server foreign data wrapper event_trigger_test_wrapper")
            .unwrap();
    }

    #[pg_test]
    fn test_create_foreign_table_with_supported_types_succeeds() {
        setup_test_fdw();
        Spi::run(
            "create foreign table ett_good (
                id bigint,
                name varchar(50),
                label char(3),
                notes text,
                tags text[],
                meta jsonb
             ) server event_trigger_test_server",
        )
        .expect("foreign table with supported types should be created");
    }

    #[pg_test(
        error = "foreign table \"ett_bad\" has columns with unsupported data types: \"loc\" (type point)"
    )]
    fn test_create_foreign_table_with_unsupported_type_is_rejected() {
        setup_test_fdw();
        Spi::run(
            "create foreign table ett_bad (id bigint, loc point) server event_trigger_test_server",
        )
        .unwrap();
    }

    #[pg_test(
        error = "foreign table \"ett_multi_bad\" has columns with unsupported data types: \"loc\" (type point), \"addr\" (type inet)"
    )]
    fn test_multiple_unsupported_columns_are_all_reported() {
        setup_test_fdw();
        Spi::run(
            "create foreign table ett_multi_bad (id bigint, loc point, addr inet) server event_trigger_test_server",
        )
        .unwrap();
    }

    #[pg_test]
    fn test_create_foreign_table_with_domain_over_supported_type_succeeds() {
        setup_test_fdw();
        Spi::run("create domain ett_domain_text as text").unwrap();
        Spi::run("create foreign table ett_domain (id bigint, label ett_domain_text) server event_trigger_test_server")
            .expect("foreign table with a domain over a supported base type should be created");
    }

    #[pg_test(
        error = "foreign table \"ett_alter1\" has columns with unsupported data types: \"bad_col\" (type point)"
    )]
    fn test_alter_table_add_column_with_unsupported_type_is_rejected() {
        setup_test_fdw();
        Spi::run("create foreign table ett_alter1 (id bigint) server event_trigger_test_server")
            .unwrap();
        Spi::run("alter table ett_alter1 add column ok_col int, add column bad_col point").unwrap();
    }

    #[pg_test(
        error = "foreign table \"ett_alter2\" has columns with unsupported data types: \"bad_col\" (type point)"
    )]
    fn test_alter_foreign_table_add_column_with_unsupported_type_is_rejected() {
        setup_test_fdw();
        Spi::run("create foreign table ett_alter2 (id bigint) server event_trigger_test_server")
            .unwrap();
        Spi::run("alter foreign table ett_alter2 add column bad_col point").unwrap();
    }

    #[pg_test]
    fn test_regular_table_with_unsupported_type_is_unaffected() {
        Spi::run("create table ett_plain (id bigint, loc point)")
            .expect("regular tables aren't scoped by the check");
        Spi::run("alter table ett_plain add column loc2 point")
            .expect("regular tables aren't scoped by the check");
    }

    #[pg_test(error = "event trigger \"check_supported_column_types\" already exists")]
    fn test_reregistering_event_trigger_without_guard_fails() {
        Spi::run(
            "create event trigger check_supported_column_types on ddl_command_end \
             when tag in ('CREATE FOREIGN TABLE') execute function check_supported_column_types()",
        )
        .unwrap();
    }

    // simulates ALTER EXTENSION wrappers UPDATE re-running build.rs's registration DO block
    #[pg_test]
    fn test_reregistering_event_trigger_with_guard_is_idempotent() {
        Spi::run(
            "DO $$
             BEGIN
                 CREATE EVENT TRIGGER check_supported_column_types
                     ON ddl_command_end
                     WHEN TAG IN ('CREATE FOREIGN TABLE')
                     EXECUTE FUNCTION check_supported_column_types();
             EXCEPTION WHEN duplicate_object THEN NULL;
             END $$;",
        )
        .expect("re-registering the event trigger through the guarded DO block must not error");
    }
}
