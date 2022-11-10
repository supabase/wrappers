use pgx::prelude::*;

pgx::pg_module_magic!();

#[pg_extern]
fn hello() -> &'static str {
    "Hello"
}
