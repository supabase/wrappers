//! Wrappers is a development framework for Postgres Foreign Data Wrappers ([FDW](https://wiki.postgresql.org/wiki/Foreign_data_wrappers)) based on [pgrx](https://github.com/tcdi/pgrx).
//!
//! Its goal is to make Postgres FDW development easier while keeping Rust language's modern capabilities, such as high performance, strong types, and safety.
//!
//! # Usage
//!
//! Wrappers is a pgrx extension, please follow the [installation steps](https://github.com/tcdi/pgrx#system-requirements) to install `pgrx` first.
//!
//! After pgrx is installed, create your project using command like below,
//!
//! ```bash
//! $ cargo pgrx new my_project
//! ```
//!
//! And then change default Postgres version to `pg14` or `pg15` and add below dependencies to your project's `Cargo.toml`,
//!
//! ```toml
//! [features]
//! default = ["pg15"]
//! ...
//!
//! [dependencies]
//! pgrx = "=0.11.2"
//! supabase-wrappers = "0.1"
//! ```
//!
//! # Supported Types
//!
//! For simplicity purpose, only a subset of [pgrx types](https://github.com/tcdi/pgrx#mapping-of-postgres-types-to-rust) are supported currently. For example,
//!
//! - bool
//! - f64
//! - i64
//! - String
//! - Timestamp
//! - JsonB
//!
//! See the full supported types list in [`interface::Cell`]. More types will be added in the future if needed or you can [raise a request](https://github.com/supabase/wrappers/issues) to us.
//!
//! # Developing a FDW
//!
//! The core interface is the [`interface::ForeignDataWrapper`] trait which provides callback functions to be called by Postgres during different querying phases. For example,
//!
//! - Query planning phase
//!   - [get_rel_size()](`interface::ForeignDataWrapper#method.get_rel_size`)
//! - Scan phase
//!   - [begin_scan()](`interface::ForeignDataWrapper#tymethod.begin_scan`) *required*
//!   - [iter_scan()](`interface::ForeignDataWrapper#tymethod.iter_scan`) *required*
//!   - [re_scan()](`interface::ForeignDataWrapper#method.re_scan`)
//!   - [end_scan()](`interface::ForeignDataWrapper#tymethod.end_scan`) *required*
//! - Modify phase
//!   - [begin_modify()](`interface::ForeignDataWrapper#method.begin_modify`)
//!   - [insert()](`interface::ForeignDataWrapper#method.insert`)
//!   - [update()](`interface::ForeignDataWrapper#method.update`)
//!   - [delete()](`interface::ForeignDataWrapper#method.delete`)
//!   - [end_modify()](`interface::ForeignDataWrapper#method.end_modify`)
//!
//! To give different functionalities to your FDW, you can choose different callback functions to implement. The required ones are `begin_scan`, `iter_scan` and `end_scan`, all the others are optional. See [Postgres FDW document](https://www.postgresql.org/docs/current/fdw-callbacks.html) for more details about FDW development.
//!
//! The FDW implements [`interface::ForeignDataWrapper`] trait must use [`wrappers_fdw`] macro and implement a `new()` initialization function. For example,
//!
//! ```rust,no_run
//! # mod wrapper {
//! use std::collections::HashMap;
//! use pgrx::pg_sys::panic::ErrorReport;
//! use pgrx::PgSqlErrorCode;
//! use supabase_wrappers::prelude::*;
//! #[wrappers_fdw(
//!    version = "0.1.1",
//!    author = "Supabase",
//!    website = "https://github.com/supabase/wrappers/tree/main/wrappers/src/fdw/helloworld_fdw",
//!    error_type = "HelloWorldFdwError"
//! )]
//! pub struct HelloWorldFdw {
//!     //row counter
//!     row_cnt: i64,
//!
//!     // target column list
//!     tgt_cols: Vec<Column>,
//! }
//!
//! enum HelloWorldFdwError {}
//!
//! impl From<HelloWorldFdwError> for ErrorReport {
//!     fn from(_value: HelloWorldFdwError) -> Self {
//!         ErrorReport::new(PgSqlErrorCode::ERRCODE_FDW_ERROR, "", "")
//!     }
//! }
//!
//! type HelloWorldFdwResult<T> = Result<T, HelloWorldFdwError>;
//!
//! impl ForeignDataWrapper<HelloWorldFdwError> for HelloWorldFdw {
//!     fn new(options: &HashMap<String, String>) -> HelloWorldFdwResult<Self> {
//!         // 'options' is the key-value pairs defined in `CREATE SERVER` SQL, for example,
//!         //
//!         // create server my_helloworld_server
//!         //   foreign data wrapper wrappers_helloworld
//!         //   options (
//!         //     foo 'bar'
//!         // );
//!         //
//!         // 'options' passed here will be a hashmap { 'foo' -> 'bar' }.
//!         //
//!         // You can do any initalization in this new() function, like saving connection
//!         // info or API url in an variable, but don't do heavy works like database
//!         // connection or API call.
//!         Ok(Self {
//!             row_cnt: 0,
//!             tgt_cols: Vec::new(),
//!         })
//!     }
//!
//!     fn begin_scan(&mut self, quals: &[Qual], columns: &[Column], sorts: &[Sort], limit: &Option<Limit>, options: &HashMap<String, String>) -> HelloWorldFdwResult<()> {
//!         // Do any initilization
//!         Ok(())
//!     }
//!
//!     fn iter_scan(&mut self, row: &mut Row) -> HelloWorldFdwResult<Option<()>> {
//!         // Return None when done
//!         Ok(None)
//!     }
//!
//!     fn end_scan(&mut self) -> HelloWorldFdwResult<()> {
//!         // Cleanup any resources
//!         Ok(())
//!     }
//! }
//! # }
//! ```
//!
//! To develop a simple FDW supports basic query `SELECT`, you need to implement `begin_scan`, `iter_scan` and `end_scan`.
//!
//! - `begin_scan` - called once at the beginning of `SELECT`
//! - `iter_scan` - called for each row to be returned to Postgres, return `None` to stop the scan
//! - `end_scan` - called once at the end of `SELECT`
//!
//! Suppose the foreign table DDL is like below,
//!
//! ```sql
//! create foreign table hello (
//!   id bigint,
//!   col text
//! )
//!   server my_helloworld_server
//!   options (
//!     foo 'bar'
//!   );
//! ```
//!
//! Then we can implement [`interface::ForeignDataWrapper`] trait like below,
//!
//! ```rust,no_run
//! use std::collections::HashMap;
//! use pgrx::pg_sys::panic::ErrorReport;
//! use pgrx::PgSqlErrorCode;
//! use supabase_wrappers::prelude::*;
//!
//! pub(crate) struct HelloWorldFdw {
//!     // row counter
//!     row_cnt: i64,
//!
//!     // target column name list
//!     tgt_cols: Vec<Column>,
//! }
//!
//! enum HelloWorldFdwError {}
//!
//! impl From<HelloWorldFdwError> for ErrorReport {
//!     fn from(_value: HelloWorldFdwError) -> Self {
//!         ErrorReport::new(PgSqlErrorCode::ERRCODE_FDW_ERROR, "", "")
//!     }
//! }
//!
//! impl ForeignDataWrapper<HelloWorldFdwError> for HelloWorldFdw {
//!     fn new(options: &HashMap<String, String>) -> Result<Self, HelloWorldFdwError> {
//!         Ok(Self {
//!             row_cnt: 0,
//!             tgt_cols: Vec::new(),
//!         })
//!     }
//!
//!     fn begin_scan(
//!         &mut self,
//!         _quals: &[Qual],
//!         columns: &[Column],
//!         _sorts: &[Sort],
//!         _limit: &Option<Limit>,
//!         _options: &HashMap<String, String>,
//!     ) -> Result<(), HelloWorldFdwError> {
//!         // reset row count
//!         self.row_cnt = 0;
//!
//!         // save a copy of target columns
//!         self.tgt_cols = columns.to_vec();
//!         Ok(())
//!     }
//!
//!     fn iter_scan(&mut self, row: &mut Row) -> Result<Option<()>, HelloWorldFdwError> {
//!         // this is called on each row and we only return one row here
//!         if self.row_cnt < 1 {
//!             // add values to row if they are in target column list
//!             for tgt_col in &self.tgt_cols {
//!                 match tgt_col.name.as_str() {
//!                     "id" => row.push("id", Some(Cell::I64(self.row_cnt))),
//!                     "col" => row.push("col", Some(Cell::String("Hello world".to_string()))),
//!                     _ => {}
//!                 }
//!             }
//!
//!             self.row_cnt += 1;
//!
//!             // return the 'Some(())' to Postgres and continue data scan
//!             return Ok(Some(()));
//!         }
//!
//!         // return 'None' to stop data scan
//!         Ok(None)
//!     }
//!
//!     fn end_scan(&mut self) -> Result<(), HelloWorldFdwError> {
//!         // we do nothing here, but you can do things like resource cleanup and etc.
//!         Ok(())
//!     }
//! }
//! ```
//!
//! And that's it. Now your FDW is ready to run,
//!
//! ```bash
//! $ cargo pgrx run
//! ```
//!
//! Then create the FDW and foreign table, and make a query on it,
//!
//! ```sql
//! create extension my_project;
//!
//! create foreign data wrapper helloworld_wrapper
//!   handler hello_world_fdw_handler
//!   validator hello_world_fdw_validator;
//!
//! create server my_helloworld_server
//!   foreign data wrapper helloworld_wrapper;
//!
//! create foreign table hello (
//!   id bigint,
//!   col text
//! )
//!   server my_helloworld_server;
//!
//! select * from hello;
//!
//!  id |    col
//! ----+-------------
//!   0 | Hello world
//! (1 row)
//! ```
//!
//! ### Pro Tips
//!
//! You can use `EXPLAIN` to check what have been pushed down. For example,
//!
//! ```sql
//! explain select * from hello where id = 1 order by col limit 1;
//!
//!                                                        QUERY PLAN
//! --------------------------------------------------------------------------------------------------------------------------
//!  Limit  (cost=1.01..1.01 rows=1 width=40)
//!    ->  Sort  (cost=1.01..1.01 rows=1 width=40)
//!          Sort Key: col
//!          ->  Foreign Scan on hello  (cost=0.00..1.00 rows=1 width=0)
//!                Filter: (id = 1)
//!                Wrappers: quals = [Qual { field: "id", operator: "=", value: Cell(I32(1)), use_or: false, param: None }]
//!                Wrappers: tgts = [Column { name: "id", num: 1, type_oid: 20 }, Column { name: "col", num: 2, type_oid: 25 }]
//!                Wrappers: sorts = [Sort { field: "col", field_no: 2, reversed: false, nulls_first: false, collate: None }]
//!                Wrappers: limit = Some(Limit { count: 1, offset: 0 })
//! (9 rows)
//! ```
//!
//! ### More FDW Examples
//!
//! See more FDW examples which interact with RDBMS or RESTful API.
//! - [HelloWorld](https://github.com/supabase/wrappers/tree/main/wrappers/src/fdw/helloworld_fdw): A demo FDW to show how to develop a baisc FDW.
//! - [BigQuery](https://github.com/supabase/wrappers/tree/main/wrappers/src/fdw/bigquery_fdw): A FDW for Google [BigQuery](https://cloud.google.com/bigquery) which supports data read and modify.
//! - [Clickhouse](https://github.com/supabase/wrappers/tree/main/wrappers/src/fdw/clickhouse_fdw): A FDW for [ClickHouse](https://clickhouse.com/) which supports data read and modify.
//! - [Stripe](https://github.com/supabase/wrappers/tree/main/wrappers/src/fdw/stripe_fdw): A FDW for [Stripe](https://stripe.com/) API which supports data read and modify.
//! - [Firebase](https://github.com/supabase/wrappers/tree/main/wrappers/src/fdw/firebase_fdw): A FDW for Google [Firebase](https://firebase.google.com/) which supports data read only.
//! - [Airtable](https://github.com/supabase/wrappers/tree/main/wrappers/src/fdw/airtable_fdw): A FDW for [Airtable](https://airtable.com/) API which supports data read only.
//! - [S3](https://github.com/supabase/wrappers/tree/main/wrappers/src/fdw/s3_fdw): A FDW for [AWS S3](https://aws.amazon.com/s3/) which supports data read only.
//! - [Logflare](https://github.com/supabase/wrappers/tree/main/wrappers/src/fdw/logflare_fdw): A FDW for [Logflare](https://logflare.app/) which supports data read only.
//! - [Auth0](https://github.com/supabase/wrappers/tree/main/wrappers/src/fdw/auth0_fdw): A FDW for [Auth0](https://auth0.com/) which supports data read only.
//! - [SQL Server](https://github.com/supabase/wrappers/tree/main/wrappers/src/fdw/mssql_fdw): A FDW for [Microsoft SQL Server](https://www.microsoft.com/en-au/sql-server/) which supports data read only.
//! - [Redis](https://github.com/supabase/wrappers/tree/main/wrappers/src/fdw/redis_fdw): A FDW for [Redis](https://redis.io/) which supports data read only.

pub mod interface;
pub mod options;
pub mod utils;

/// The prelude includes all necessary imports to make Wrappers work
pub mod prelude {
    pub use crate::interface::*;
    pub use crate::options::*;
    pub use crate::utils::*;
    pub use crate::wrappers_fdw;
    pub use tokio::runtime::Runtime;
}

use pgrx::prelude::*;
use pgrx::AllocatedByPostgres;

mod instance;
mod limit;
mod memctx;
mod modify;
mod polyfill;
mod qual;
mod scan;
mod sort;

/// PgBox'ed `FdwRoutine`, used in [`fdw_routine`](interface::ForeignDataWrapper::fdw_routine)
pub type FdwRoutine<A = AllocatedByPostgres> = PgBox<pg_sys::FdwRoutine, A>;

pub use supabase_wrappers_macros::wrappers_fdw;
