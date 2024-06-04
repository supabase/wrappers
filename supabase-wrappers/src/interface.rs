//! Provides interface types and trait to develop Postgres foreign data wrapper
//!

use crate::FdwRoutine;
use pgrx::pg_sys::panic::ErrorReport;
use pgrx::prelude::{Date, Timestamp, TimestampWithTimeZone};
use pgrx::{
    fcinfo,
    pg_sys::{self, BuiltinOid, Datum, Oid},
    AllocatedByRust, AnyNumeric, FromDatum, IntoDatum, JsonB, PgBuiltInOids, PgOid,
};
use std::collections::HashMap;
use std::ffi::CStr;
use std::fmt;
use std::iter::Zip;
use std::mem;
use std::slice::Iter;

// fdw system catalog oids
// https://doxygen.postgresql.org/pg__foreign__data__wrapper_8h.html
// https://doxygen.postgresql.org/pg__foreign__server_8h.html
// https://doxygen.postgresql.org/pg__foreign__table_8h.html

/// Constant can be used in [validator](ForeignDataWrapper::validator)
pub const FOREIGN_DATA_WRAPPER_RELATION_ID: Oid = BuiltinOid::ForeignDataWrapperRelationId.value();

/// Constant can be used in [validator](ForeignDataWrapper::validator)
pub const FOREIGN_SERVER_RELATION_ID: Oid = BuiltinOid::ForeignServerRelationId.value();

/// Constant can be used in [validator](ForeignDataWrapper::validator)
pub const FOREIGN_TABLE_RELATION_ID: Oid = BuiltinOid::ForeignTableRelationId.value();

/// A data cell in a data row
#[derive(Debug)]
pub enum Cell {
    Bool(bool),
    I8(i8),
    I16(i16),
    F32(f32),
    I32(i32),
    F64(f64),
    I64(i64),
    Numeric(AnyNumeric),
    String(String),
    Date(Date),
    Timestamp(Timestamp),
    Timestamptz(TimestampWithTimeZone),
    Json(JsonB),
}

impl Clone for Cell {
    fn clone(&self) -> Self {
        match self {
            Cell::Bool(v) => Cell::Bool(*v),
            Cell::I8(v) => Cell::I8(*v),
            Cell::I16(v) => Cell::I16(*v),
            Cell::F32(v) => Cell::F32(*v),
            Cell::I32(v) => Cell::I32(*v),
            Cell::F64(v) => Cell::F64(*v),
            Cell::I64(v) => Cell::I64(*v),
            Cell::Numeric(v) => Cell::Numeric(v.clone()),
            Cell::String(v) => Cell::String(v.clone()),
            Cell::Date(v) => Cell::Date(*v),
            Cell::Timestamp(v) => Cell::Timestamp(*v),
            Cell::Timestamptz(v) => Cell::Timestamptz(*v),
            Cell::Json(v) => Cell::Json(JsonB(v.0.clone())),
        }
    }
}

impl fmt::Display for Cell {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Cell::Bool(v) => write!(f, "{}", v),
            Cell::I8(v) => write!(f, "{}", v),
            Cell::I16(v) => write!(f, "{}", v),
            Cell::F32(v) => write!(f, "{}", v),
            Cell::I32(v) => write!(f, "{}", v),
            Cell::F64(v) => write!(f, "{}", v),
            Cell::I64(v) => write!(f, "{}", v),
            Cell::Numeric(v) => write!(f, "{}", v),
            Cell::String(v) => write!(f, "'{}'", v),
            Cell::Date(v) => unsafe {
                let dt =
                    fcinfo::direct_function_call_as_datum(pg_sys::date_out, &[(*v).into_datum()])
                        .expect("cell should be a valid date");
                let dt_cstr = CStr::from_ptr(dt.cast_mut_ptr());
                write!(
                    f,
                    "'{}'",
                    dt_cstr.to_str().expect("date should be a valid string")
                )
            },
            Cell::Timestamp(v) => unsafe {
                let ts = fcinfo::direct_function_call_as_datum(
                    pg_sys::timestamp_out,
                    &[(*v).into_datum()],
                )
                .expect("cell should be a valid timestamp");
                let ts_cstr = CStr::from_ptr(ts.cast_mut_ptr());
                write!(
                    f,
                    "'{}'",
                    ts_cstr
                        .to_str()
                        .expect("timestamp should be a valid string")
                )
            },
            Cell::Timestamptz(v) => unsafe {
                let ts = fcinfo::direct_function_call_as_datum(
                    pg_sys::timestamptz_out,
                    &[(*v).into_datum()],
                )
                .expect("cell should be a valid timestamptz");
                let ts_cstr = CStr::from_ptr(ts.cast_mut_ptr());
                write!(
                    f,
                    "'{}'",
                    ts_cstr
                        .to_str()
                        .expect("timestamptz should be a valid string")
                )
            },
            Cell::Json(v) => write!(f, "{:?}", v),
        }
    }
}

impl IntoDatum for Cell {
    fn into_datum(self) -> Option<Datum> {
        match self {
            Cell::Bool(v) => v.into_datum(),
            Cell::I8(v) => v.into_datum(),
            Cell::I16(v) => v.into_datum(),
            Cell::F32(v) => v.into_datum(),
            Cell::I32(v) => v.into_datum(),
            Cell::F64(v) => v.into_datum(),
            Cell::I64(v) => v.into_datum(),
            Cell::Numeric(v) => v.into_datum(),
            Cell::String(v) => v.into_datum(),
            Cell::Date(v) => v.into_datum(),
            Cell::Timestamp(v) => v.into_datum(),
            Cell::Timestamptz(v) => v.into_datum(),
            Cell::Json(v) => v.into_datum(),
        }
    }

    fn type_oid() -> Oid {
        Oid::INVALID
    }

    fn is_compatible_with(other: Oid) -> bool {
        Self::type_oid() == other
            || other == pg_sys::BOOLOID
            || other == pg_sys::CHAROID
            || other == pg_sys::INT2OID
            || other == pg_sys::FLOAT4OID
            || other == pg_sys::INT4OID
            || other == pg_sys::FLOAT8OID
            || other == pg_sys::INT8OID
            || other == pg_sys::NUMERICOID
            || other == pg_sys::TEXTOID
            || other == pg_sys::DATEOID
            || other == pg_sys::TIMESTAMPOID
            || other == pg_sys::TIMESTAMPTZOID
            || other == pg_sys::JSONBOID
    }
}

impl FromDatum for Cell {
    unsafe fn from_polymorphic_datum(datum: Datum, is_null: bool, typoid: Oid) -> Option<Self>
    where
        Self: Sized,
    {
        let oid = PgOid::from(typoid);
        match oid {
            PgOid::BuiltIn(PgBuiltInOids::BOOLOID) => {
                bool::from_datum(datum, is_null).map(Cell::Bool)
            }
            PgOid::BuiltIn(PgBuiltInOids::CHAROID) => i8::from_datum(datum, is_null).map(Cell::I8),
            PgOid::BuiltIn(PgBuiltInOids::INT2OID) => {
                i16::from_datum(datum, is_null).map(Cell::I16)
            }
            PgOid::BuiltIn(PgBuiltInOids::FLOAT4OID) => {
                f32::from_datum(datum, is_null).map(Cell::F32)
            }
            PgOid::BuiltIn(PgBuiltInOids::INT4OID) => {
                i32::from_datum(datum, is_null).map(Cell::I32)
            }
            PgOid::BuiltIn(PgBuiltInOids::FLOAT8OID) => {
                f64::from_datum(datum, is_null).map(Cell::F64)
            }
            PgOid::BuiltIn(PgBuiltInOids::INT8OID) => {
                i64::from_datum(datum, is_null).map(Cell::I64)
            }
            PgOid::BuiltIn(PgBuiltInOids::NUMERICOID) => {
                AnyNumeric::from_datum(datum, is_null).map(Cell::Numeric)
            }
            PgOid::BuiltIn(PgBuiltInOids::TEXTOID) => {
                String::from_datum(datum, is_null).map(Cell::String)
            }
            PgOid::BuiltIn(PgBuiltInOids::DATEOID) => {
                Date::from_datum(datum, is_null).map(Cell::Date)
            }
            PgOid::BuiltIn(PgBuiltInOids::TIMESTAMPOID) => {
                Timestamp::from_datum(datum, is_null).map(Cell::Timestamp)
            }
            PgOid::BuiltIn(PgBuiltInOids::TIMESTAMPTZOID) => {
                TimestampWithTimeZone::from_datum(datum, is_null).map(Cell::Timestamptz)
            }
            PgOid::BuiltIn(PgBuiltInOids::JSONBOID) => {
                JsonB::from_datum(datum, is_null).map(Cell::Json)
            }
            _ => None,
        }
    }
}

/// A data row in a table
///
/// The row contains a column name list and cell list with same number of
/// elements.
#[derive(Debug, Clone, Default)]
pub struct Row {
    /// column names
    pub cols: Vec<String>,

    /// column cell list, should match with cols
    pub cells: Vec<Option<Cell>>,
}

impl Row {
    /// Create an empty row
    pub fn new() -> Self {
        Self::default()
    }

    /// Push a cell with column name to this row
    pub fn push(&mut self, col: &str, cell: Option<Cell>) {
        self.cols.push(col.to_owned());
        self.cells.push(cell);
    }

    /// Return a zipped <column_name, cell> iterator
    pub fn iter(&self) -> Zip<Iter<'_, String>, Iter<'_, Option<Cell>>> {
        self.cols.iter().zip(self.cells.iter())
    }

    /// Remove a cell at the specified index
    pub fn retain<F>(&mut self, f: F)
    where
        F: FnMut((&String, &Option<Cell>)) -> bool,
    {
        let keep: Vec<bool> = self.iter().map(f).collect();
        let mut iter = keep.iter();
        self.cols.retain(|_| *iter.next().unwrap_or(&true));
        iter = keep.iter();
        self.cells.retain(|_| *iter.next().unwrap_or(&true));
    }

    /// Replace `self` with the source row
    #[inline]
    pub fn replace_with(&mut self, src: Row) {
        let _ = mem::replace(self, src);
    }

    /// Clear the row, removing all column names and cells
    pub fn clear(&mut self) {
        self.cols.clear();
        self.cells.clear();
    }
}

/// A column definition in a table
///
/// The column represents a column definition in a table.
#[derive(Debug, Clone, Default)]
pub struct Column {
    /// column name
    pub name: String,

    /// 1-based column number
    pub num: usize,

    /// column type OID, can be used to match pg_sys::BuiltinOid
    pub type_oid: Oid,
}

/// A restiction value used in [`Qual`], either a [`Cell`] or an array of [`Cell`]
#[derive(Debug, Clone)]
pub enum Value {
    Cell(Cell),
    Array(Vec<Cell>),
}

/// Query parameter
#[derive(Debug, Clone)]
pub struct Param {
    /// 1-based parameter id
    pub id: usize,

    /// parameter type OID
    pub type_oid: Oid,
}

/// Query restrictions, a.k.a conditions in `WHERE` clause
///
/// A Qual defines a simple condition wich can be used by the FDW to restrict the number
/// of the results.
///
/// <div class="example-wrap" style="display:inline-block"><pre class="compile_fail" style="white-space:normal;font:inherit;">
/// <strong>Warning</strong>: Currently only simple conditions are supported, see below for examples. Other kinds of conditions, like JSON attribute filter e.g. `where json_col->>'key' = 'value'`, are not supported yet.
/// </pre></div>
///
/// ## Examples
///
/// ```sql
/// where id = 1;
/// -- [Qual { field: "id", operator: "=", value: Cell(I32(1)), use_or: false }]
/// ```
///
/// ```sql
/// where id in (1, 2);
/// -- [Qual { field: "id", operator: "=", value: Array([I64(1), I64(2)]), use_or: true }]
/// ```
///
/// ```sql
/// where col is null
/// -- [Qual { field: "col", operator: "is", value: Cell(String("null")), use_or: false }]
/// ```
///
/// ```sql
/// where bool_col
/// -- [Qual { field: "bool_col", operator: "=", value: Cell(Bool(true)), use_or: false }]
/// ```
///
/// ```sql
/// where id > 1 and col = 'foo';
/// -- [
/// --   Qual { field: "id", operator: ">", value: Cell(I32(1)), use_or: false },
/// --   Qual { field: "col", operator: "=", value: Cell(String("foo")), use_or: false }
/// -- ]
/// ```
#[derive(Debug, Clone)]
pub struct Qual {
    pub field: String,
    pub operator: String,
    pub value: Value,
    pub use_or: bool,
    pub param: Option<Param>,
}

impl Qual {
    pub fn deparse(&self) -> String {
        if self.use_or {
            match &self.value {
                Value::Cell(_) => unreachable!(),
                Value::Array(cells) => {
                    let conds: Vec<String> = cells
                        .iter()
                        .map(|cell| format!("{} {} {}", self.field, self.operator, cell))
                        .collect();
                    conds.join(" or ")
                }
            }
        } else {
            match &self.value {
                Value::Cell(cell) => match self.operator.as_str() {
                    "is" | "is not" => match cell {
                        Cell::String(cell) if cell == "null" => {
                            format!("{} {} null", self.field, self.operator)
                        }
                        _ => format!("{} {} {}", self.field, self.operator, cell),
                    },
                    "~~" => format!("{} like {}", self.field, cell),
                    "!~~" => format!("{} not like {}", self.field, cell),
                    _ => format!("{} {} {}", self.field, self.operator, cell),
                },
                Value::Array(_) => unreachable!(),
            }
        }
    }
}

/// Query sort, a.k.a `ORDER BY` clause
///
/// ## Examples
///
/// ```sql
/// order by id;
/// -- [Sort { field: "id", field_no: 1, reversed: false, nulls_first: false, collate: None]
/// ```
///
/// ```sql
/// order by id desc;
/// -- [Sort { field: "id", field_no: 1, reversed: true, nulls_first: true, collate: None]
/// ```
///
/// ```sql
/// order by id desc, col;
/// -- [
/// --   Sort { field: "id", field_no: 1, reversed: true, nulls_first: true, collate: None },
/// --   Sort { field: "col", field_no: 2, reversed: false, nulls_first: false, collate: None }
/// -- ]
/// ```
///
/// ```sql
/// order by id collate "de_DE";
/// -- [Sort { field: "col", field_no: 2, reversed: false, nulls_first: false, collate: Some("de_DE") }]
/// ```
#[derive(Debug, Clone, Default)]
pub struct Sort {
    pub field: String,
    pub field_no: usize,
    pub reversed: bool,
    pub nulls_first: bool,
    pub collate: Option<String>,
}

impl Sort {
    pub fn deparse(&self) -> String {
        let mut sql = self.field.to_string();

        if self.reversed {
            sql.push_str(" desc");
        } else {
            sql.push_str(" asc");
        }

        if self.nulls_first {
            sql.push_str(" nulls first")
        } else {
            sql.push_str(" nulls last")
        }

        sql
    }

    pub fn deparse_with_collate(&self) -> String {
        let mut sql = self.deparse();

        if let Some(collate) = &self.collate {
            sql.push_str(&format!(" collate {}", collate));
        }

        sql
    }
}

/// Query limit, a.k.a `LIMIT count OFFSET offset` clause
///
/// ## Examples
///
/// ```sql
/// limit 42;
/// -- Limit { count: 42, offset: 0 }
/// ```
///
/// ```sql
/// limit 42 offset 7;
/// -- Limit { count: 42, offset: 7 }
/// ```
#[derive(Debug, Clone, Default)]
pub struct Limit {
    pub count: i64,
    pub offset: i64,
}

impl Limit {
    pub fn deparse(&self) -> String {
        format!("limit {} offset {}", self.count, self.offset)
    }
}

/// The Foreign Data Wrapper trait
///
/// This is the main interface for your foreign data wrapper. Required functions
/// are listed below, all the others are optional.
///
/// 1. new
/// 2. begin_scan
/// 3. iter_scan
/// 4. end_scan
///
/// See the module-level document for more details.
///
pub trait ForeignDataWrapper<E: Into<ErrorReport>> {
    /// Create a FDW instance
    ///
    /// `options` is the key-value pairs defined in `CREATE SERVER` SQL. For example,
    ///
    /// ```sql
    /// create server my_helloworld_server
    ///   foreign data wrapper wrappers_helloworld
    ///   options (
    ///     foo 'bar'
    /// );
    /// ```
    ///
    /// `options` passed here will be a hashmap { 'foo' -> 'bar' }.
    ///
    /// You can do any initalization in this function, like saving connection
    /// info or API url in an variable, but don't do heavy works like database
    /// connection or API call.
    fn new(options: &HashMap<String, String>) -> Result<Self, E>
    where
        Self: Sized;

    /// Obtain relation size estimates for a foreign table
    ///
    /// Return the expected number of rows and row size (in bytes) by the
    /// foreign table scan.
    ///
    /// [See more details](https://www.postgresql.org/docs/current/fdw-callbacks.html#FDW-CALLBACKS-SCAN).
    fn get_rel_size(
        &mut self,
        _quals: &[Qual],
        _columns: &[Column],
        _sorts: &[Sort],
        _limit: &Option<Limit>,
        _options: &HashMap<String, String>,
    ) -> Result<(i64, i32), E> {
        Ok((0, 0))
    }

    /// Called when begin executing a foreign scan
    ///
    /// - `quals` - `WHERE` clause pushed down
    /// - `columns` - target columns to be queried
    /// - `sorts` - `ORDER BY` clause pushed down
    /// - `limit` - `LIMIT` clause pushed down
    /// - `options` - the options defined when `CREATE FOREIGN TABLE`
    ///
    /// [See more details](https://www.postgresql.org/docs/current/fdw-callbacks.html#FDW-CALLBACKS-SCAN).
    fn begin_scan(
        &mut self,
        quals: &[Qual],
        columns: &[Column],
        sorts: &[Sort],
        limit: &Option<Limit>,
        options: &HashMap<String, String>,
    ) -> Result<(), E>;

    /// Called when fetch one row from the foreign source
    ///
    /// FDW must save fetched foreign data into the [`Row`], or return `None` if no more rows to read.
    ///
    /// [See more details](https://www.postgresql.org/docs/current/fdw-callbacks.html#FDW-CALLBACKS-SCAN).
    fn iter_scan(&mut self, row: &mut Row) -> Result<Option<()>, E>;

    /// Called when restart the scan from the beginning.
    ///
    /// [See more details](https://www.postgresql.org/docs/current/fdw-callbacks.html#FDW-CALLBACKS-SCAN).
    fn re_scan(&mut self) -> Result<(), E> {
        Ok(())
    }

    /// Called when end the scan
    ///
    /// [See more details](https://www.postgresql.org/docs/current/fdw-callbacks.html#FDW-CALLBACKS-SCAN).
    fn end_scan(&mut self) -> Result<(), E>;

    /// Called when begin executing a foreign table modification operation.
    ///
    /// - `options` - the options defined when `CREATE FOREIGN TABLE`
    ///
    /// The foreign table must include a `rowid_column` option which specify
    /// the unique identification column of the foreign table to enable data
    /// modification.
    ///
    /// For example,
    ///
    /// ```sql
    /// create foreign table my_foreign_table (
    ///   id bigint,
    ///   name text
    /// )
    ///   server my_server
    ///   options (
    ///     rowid_column 'id'
    ///   );
    /// ```
    ///
    /// [See more details](https://www.postgresql.org/docs/current/fdw-callbacks.html#FDW-CALLBACKS-UPDATE).
    fn begin_modify(&mut self, _options: &HashMap<String, String>) -> Result<(), E> {
        Ok(())
    }

    /// Called when insert one row into the foreign table
    ///
    /// - row - the new row to be inserted
    ///
    /// [See more details](https://www.postgresql.org/docs/current/fdw-callbacks.html#FDW-CALLBACKS-UPDATE).
    fn insert(&mut self, _row: &Row) -> Result<(), E> {
        Ok(())
    }

    /// Called when update one row into the foreign table
    ///
    /// - rowid - the `rowid_column` cell
    /// - new_row - the new row with updated cells
    ///
    /// [See more details](https://www.postgresql.org/docs/current/fdw-callbacks.html#FDW-CALLBACKS-UPDATE).
    fn update(&mut self, _rowid: &Cell, _new_row: &Row) -> Result<(), E> {
        Ok(())
    }

    /// Called when delete one row into the foreign table
    ///
    /// - rowid - the `rowid_column` cell
    ///
    /// [See more details](https://www.postgresql.org/docs/current/fdw-callbacks.html#FDW-CALLBACKS-UPDATE).
    fn delete(&mut self, _rowid: &Cell) -> Result<(), E> {
        Ok(())
    }

    /// Called when end the table update
    ///
    /// [See more details](https://www.postgresql.org/docs/current/fdw-callbacks.html#FDW-CALLBACKS-UPDATE).
    fn end_modify(&mut self) -> Result<(), E> {
        Ok(())
    }

    /// Returns a FdwRoutine for the FDW
    ///
    /// Not to be used directly, use [`wrappers_fdw`](crate::wrappers_fdw) macro instead.
    fn fdw_routine() -> FdwRoutine
    where
        Self: Sized,
    {
        unsafe {
            use crate::{modify, scan};
            let mut fdw_routine =
                FdwRoutine::<AllocatedByRust>::alloc_node(pg_sys::NodeTag::T_FdwRoutine);

            // plan phase
            fdw_routine.GetForeignRelSize = Some(scan::get_foreign_rel_size::<E, Self>);
            fdw_routine.GetForeignPaths = Some(scan::get_foreign_paths::<E, Self>);
            fdw_routine.GetForeignPlan = Some(scan::get_foreign_plan::<E, Self>);
            fdw_routine.ExplainForeignScan = Some(scan::explain_foreign_scan::<E, Self>);

            // scan phase
            fdw_routine.BeginForeignScan = Some(scan::begin_foreign_scan::<E, Self>);
            fdw_routine.IterateForeignScan = Some(scan::iterate_foreign_scan::<E, Self>);
            fdw_routine.ReScanForeignScan = Some(scan::re_scan_foreign_scan::<E, Self>);
            fdw_routine.EndForeignScan = Some(scan::end_foreign_scan::<E, Self>);

            // modify phase
            fdw_routine.AddForeignUpdateTargets = Some(modify::add_foreign_update_targets);
            fdw_routine.PlanForeignModify = Some(modify::plan_foreign_modify::<E, Self>);
            fdw_routine.BeginForeignModify = Some(modify::begin_foreign_modify::<E, Self>);
            fdw_routine.ExecForeignInsert = Some(modify::exec_foreign_insert::<E, Self>);
            fdw_routine.ExecForeignDelete = Some(modify::exec_foreign_delete::<E, Self>);
            fdw_routine.ExecForeignUpdate = Some(modify::exec_foreign_update::<E, Self>);
            fdw_routine.EndForeignModify = Some(modify::end_foreign_modify::<E, Self>);

            Self::fdw_routine_hook(&mut fdw_routine);
            fdw_routine.into_pg_boxed()
        }
    }

    /// Additional FwdRoutine setup, called by default `Self::fdw_routine()`
    /// after completing its initialization.
    fn fdw_routine_hook(_routine: &mut FdwRoutine<AllocatedByRust>) {}

    /// Validator function for validating options given in `CREATE` and `ALTER`
    /// commands for its foreign data wrapper, as well as foreign servers, user
    /// mappings, and foreign tables using the wrapper.
    ///
    /// [See more details about validator](https://www.postgresql.org/docs/current/fdw-functions.html)
    ///
    /// # Example
    ///
    /// ```rust,no_run
    /// use pgrx::pg_sys::Oid;
    /// use supabase_wrappers::prelude::check_options_contain;
    ///
    /// use pgrx::pg_sys::panic::ErrorReport;
    /// use pgrx::PgSqlErrorCode;
    ///
    /// enum FdwError {
    ///     InvalidFdwOption,
    ///     InvalidServerOption,
    ///     InvalidTableOption,
    /// }
    ///
    /// impl From<FdwError> for ErrorReport {
    ///     fn from(value: FdwError) -> Self {
    ///         let error_message = match value {
    ///             FdwError::InvalidFdwOption => "invalid foreign data wrapper option",
    ///             FdwError::InvalidServerOption => "invalid foreign server option",
    ///             FdwError::InvalidTableOption => "invalid foreign table option",
    ///         };
    ///         ErrorReport::new(PgSqlErrorCode::ERRCODE_FDW_ERROR, error_message, "")
    ///     }
    /// }
    ///
    /// fn validator(opt_list: Vec<Option<String>>, catalog: Option<Oid>) -> Result<(), FdwError> {
    ///     if let Some(oid) = catalog {
    ///         match oid {
    ///             FOREIGN_DATA_WRAPPER_RELATION_ID => {
    ///                 // check a required option when create foreign data wrapper
    ///                 check_options_contain(&opt_list, "foreign_data_wrapper_required_option");
    ///             }
    ///             FOREIGN_SERVER_RELATION_ID => {
    ///                 // check option here when create server
    ///                 check_options_contain(&opt_list, "foreign_server_required_option");
    ///             }
    ///             FOREIGN_TABLE_RELATION_ID => {
    ///                 // check option here when create foreign table
    ///                 check_options_contain(&opt_list, "foreign_table_required_option");
    ///             }
    ///             _ => {}
    ///         }
    ///     }
    ///
    ///     Ok(())
    /// }
    /// ```
    fn validator(_options: Vec<Option<String>>, _catalog: Option<Oid>) -> Result<(), E> {
        Ok(())
    }
}
