use pgx::pg_sys::Oid;
use pgx::prelude::{Date, Timestamp};
use pgx::{Datum, FromDatum, IntoDatum, JsonB, PgBuiltInOids, PgOid};
use std::collections::HashMap;
use std::fmt;
use std::iter::Zip;
use std::slice::Iter;

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
    String(String),
    Date(Date),
    Timestamp(Timestamp),
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
            Cell::String(v) => Cell::String(v.clone()),
            Cell::Date(v) => Cell::Date(v.clone()),
            Cell::Timestamp(v) => Cell::Timestamp(v.clone()),
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
            Cell::String(v) => write!(f, "'{}'", v),
            Cell::Date(v) => write!(f, "{:?}", v),
            Cell::Timestamp(v) => write!(f, "{:?}", v),
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
            Cell::String(v) => v.into_datum(),
            Cell::Date(v) => v.into_datum(),
            Cell::Timestamp(v) => v.into_datum(),
            Cell::Json(v) => v.into_datum(),
        }
    }

    fn type_oid() -> Oid {
        0
    }
}

impl FromDatum for Cell {
    unsafe fn from_polymorphic_datum(datum: Datum, is_null: bool, typoid: Oid) -> Option<Self>
    where
        Self: Sized,
    {
        if is_null {
            return None;
        }
        let oid = PgOid::from(typoid);
        match oid {
            PgOid::BuiltIn(PgBuiltInOids::BOOLOID) => {
                Some(Cell::Bool(bool::from_datum(datum, false).unwrap()))
            }
            PgOid::BuiltIn(PgBuiltInOids::CHAROID) => {
                Some(Cell::I8(i8::from_datum(datum, false).unwrap()))
            }
            PgOid::BuiltIn(PgBuiltInOids::INT2OID) => {
                Some(Cell::I16(i16::from_datum(datum, false).unwrap()))
            }
            PgOid::BuiltIn(PgBuiltInOids::FLOAT4OID) => {
                Some(Cell::F32(f32::from_datum(datum, false).unwrap()))
            }
            PgOid::BuiltIn(PgBuiltInOids::INT4OID) => {
                Some(Cell::I32(i32::from_datum(datum, false).unwrap()))
            }
            PgOid::BuiltIn(PgBuiltInOids::FLOAT8OID) => {
                Some(Cell::F64(f64::from_datum(datum, false).unwrap()))
            }
            PgOid::BuiltIn(PgBuiltInOids::INT8OID) => {
                Some(Cell::I64(i64::from_datum(datum, false).unwrap()))
            }
            PgOid::BuiltIn(PgBuiltInOids::TEXTOID) => {
                Some(Cell::String(String::from_datum(datum, false).unwrap()))
            }
            PgOid::BuiltIn(PgBuiltInOids::DATEOID) => {
                Some(Cell::Date(Date::from_datum(datum, false).unwrap()))
            }
            PgOid::BuiltIn(PgBuiltInOids::TIMESTAMPOID) => Some(Cell::Timestamp(
                Timestamp::from_datum(datum, false).unwrap(),
            )),
            PgOid::BuiltIn(PgBuiltInOids::JSONBOID) => {
                Some(Cell::Json(JsonB::from_datum(datum, false).unwrap()))
            }
            _ => None,
        }
    }
}

/// A data row in a table
///
/// The row contains a column name list and cell list.
#[derive(Debug, Clone)]
pub struct Row {
    /// column names
    pub cols: Vec<String>,

    /// column cell list, should match with cols
    pub cells: Vec<Option<Cell>>,
}

impl Row {
    /// Create an empty row
    pub fn new() -> Self {
        Row {
            cols: Vec::new(),
            cells: Vec::new(),
        }
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
}

/// A restiction value used in [`Qual`], either a [`Cell`] or an array of [`Cell`]
#[derive(Debug, Clone)]
pub enum Value {
    Cell(Cell),
    Array(Vec<Cell>),
}

/// Query restrictions, a.k.a conditions in `WHERE` clause
///
/// A Qual defines a simple condition wich can be used by the FDW to restrict the number
/// of the results. Only simple conditions are supported currently.
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
}

impl Qual {
    pub fn deparse(&self) -> String {
        if self.use_or {
            "".to_string()
        } else {
            match &self.value {
                Value::Cell(cell) => format!("{} {} {}", self.field, self.operator, cell),
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

/// The Foreign Data Wrapper trait
///
/// This is the main interface for your foreign data wrapper. Required functions
/// are listed below, all the others are optional.
///
/// 1. begin_scan
/// 2. iter_scan
/// 3. end_scan
///
/// See the module-level document for more details.
///
pub trait ForeignDataWrapper {
    fn get_rel_size(
        &mut self,
        _quals: &Vec<Qual>,
        _columns: &Vec<String>,
        _sorts: &Vec<Sort>,
        _limit: &Option<Limit>,
        _options: &HashMap<String, String>,
    ) -> (i64, i32) {
        (0, 0)
    }

    fn begin_scan(
        &mut self,
        quals: &Vec<Qual>,
        columns: &Vec<String>,
        sorts: &Vec<Sort>,
        limit: &Option<Limit>,
        options: &HashMap<String, String>,
    );
    fn iter_scan(&mut self) -> Option<Row>;
    fn re_scan(&mut self) {}
    fn end_scan(&mut self);

    fn begin_modify(&mut self, _options: &HashMap<String, String>) {}
    fn insert(&mut self, _row: &Row) {}
    fn update(&mut self, _rowid: &Cell, _new_row: &Row) {}
    fn delete(&mut self, _rowid: &Cell) {}
    fn end_modify(&mut self) {}
}
