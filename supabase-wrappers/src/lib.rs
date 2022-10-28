//! Wrappers is a development framework for Postgres Foreign Data Wrappers ([FDW](https://wiki.postgresql.org/wiki/Foreign_data_wrappers)) based on [pgx](https://github.com/tcdi/pgx).
//!
//! Its goal is to make Postgres FDW development easier while keeping Rust language's modern capabilities, such as high performance, strong types, and safety.
//!
//! # Installation
//!
//! Wrappers is a pgx extension, please follow the [installation steps](https://github.com/tcdi/pgx#system-requirements) to install pgx.
//!
//! After pgx is installed, create your project using command like below,
//!
//! ```bash
//! $ cargo pgx new my_project
//! ```
//!
//! And then change default Postgres version to `pg14` and add dependencies in your project's `Cargo.toml`,
//!
//! ```toml
//! [features]
//! default = ["pg14"]
//! ...
//!
//! [dependencies]
//! pgx = "=0.5.6"
//! cfg-if = "1.0"
//! supabase-wrappers = "0.1"
//! ```
//!
//! # Developing a FDW
//!
//! The core interface is the [`ForeignDataWrapper`] trait which provides callback functions to be
//! called by Postgres during different querying phases.
//!
//! - Query planning phase
//!   - [get_rel_size()](`ForeignDataWrapper#method.get_rel_size`)
//! - Scan phase
//!   - [begin_scan()](`ForeignDataWrapper#method.begin_scan`) *required*
//!   - [iter_scan()](`ForeignDataWrapper#method.iter_scan`) *required*
//!   - [re_scan()](`ForeignDataWrapper#method.re_scan`)
//!   - [end_scan()](`ForeignDataWrapper#method.end_scan`) *required*
//! - Modify phase
//!   - [begin_modify()](`ForeignDataWrapper#method.begin_modify`)
//!   - [insert()](`ForeignDataWrapper#method.insert`)
//!   - [update()](`ForeignDataWrapper#method.update`)
//!   - [delete()](`ForeignDataWrapper#method.delete`)
//!   - [end_modify()](`ForeignDataWrapper#method.end_modify`)
//!
//! To give different functionalities to your FDW, you can choose different callback functions to implement. The required ones are `begin_scan`, `iter_scan` and `end_scan`, all the others are optional. See [Postgres FDW document](https://www.postgresql.org/docs/current/fdw-callbacks.html) for more details about FDW development.
//!
//! The struct implements [`ForeignDataWrapper`] trait also needs to provide a `new()` initialization function. For example,
//!
//! ```rust
//! use supabase_wrappers::ForeignDataWrapper;
//!
//! pub struct HelloWorldFdw;
//!
//! impl HelloWorldFdw {
//!     pub fn new(options: &HashMap<String, String>) -> Self {
//!         // 'options' is the key-value pairs defined in 'create server` SQL, for example,
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
//!         // info or API url in an variable, but don't do any heavy works like making a
//!         // database connection or API call.
//!         Self {}
//!     }
//! }
//! ```
//!
//! # Supported Types
//!
//! For simplicity purpose, only a subset of [pgx types](https://github.com/tcdi/pgx#mapping-of-postgres-types-to-rust) are supported currently. For example,
//!
//! - bool
//! - f64
//! - i64
//! - String
//! - Timestamp
//! - JsonB
//!
//! See the full supported types list in [`Cell`]. More types will be added in the future if needed or you can [raise a request](https://github.com/supabase/wrappers/issues) to us.
//!
//! # Example
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
//! Then we can implement [`ForeignDataWrapper`] trait like below,
//!
//! ```rust
//! impl ForeignDataWrapper for HelloWorldFdw {
//!     fn begin_scan(
//!         &mut self,
//!         _quals: &Vec<Qual>,
//!         _columns: &Vec<String>,
//!         _sorts: &Vec<Sort>,
//!         _limit: &Option<Limit>,
//!         _options: &HashMap<String, String>,
//!     ) {
//!         // reset row count
//!         self.row_cnt = 0;
//!     }
//!
//!     fn iter_scan(&mut self) -> Option<Row> {
//!         // this is called on each row and we only return one row here
//!         if self.row_cnt < 1 {
//!             // create an empty row
//!             let mut row = Row::new();
//!
//!             // add value to 'id' column
//!             row.push("id", Some(Cell::I64(self.row_cnt)));
//!
//!             // add value to 'col' column
//!             row.push("col", Some(Cell::String("Hello world".to_string())));
//!
//!             self.row_cnt += 1;
//!
//!             // return the 'Some(row)' to Postgres and continue data scan
//!             return Some(row);
//!         }
//!
//!         // return 'None' to stop data scan
//!         None
//!     }
//!
//!     fn end_scan(&mut self) {
//!         // we do nothing here, but you can do things like resource cleanup and etc.
//!     }
//! ```
//!
//! See more FDW examples which interact with RDBMS or Restful API.
//! - [HelloWorld](https://github.com/supabase/wrappers/tree/main/wrappers/src/fdw/helloworld_fdw): A demo FDW to show how to develop a baisc FDW.
//! - [BigQuery](https://github.com/supabase/wrappers/tree/main/wrappers/src/fdw/bigquery_fdw): A FDW for [BigQuery](https://cloud.google.com/bigquery) which only supports async data scan at this moment.
//! - [Clickhouse](https://github.com/supabase/wrappers/tree/main/wrappers/src/fdw/clickhouse_fdw): A FDW for [ClickHouse](https://clickhouse.com/) which supports both async data scan and modify.
//! - [Stripe](https://github.com/supabase/wrappers/tree/main/wrappers/src/fdw/stripe_fdw): A FDW for [Stripe](https://stripe.com/) API.

mod interface;
mod utils;

pub use interface::*;
pub use supabase_wrappers_macros::wrappers_magic;
pub use tokio::runtime::Runtime;
pub use utils::*;
