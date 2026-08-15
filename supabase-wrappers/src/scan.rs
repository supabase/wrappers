use pgrx::FromDatum;
use pgrx::{
    IntoDatum, PgSqlErrorCode, debug2,
    list::List,
    memcxt::PgMemoryContexts,
    pg_sys::{Datum, MemoryContext, MemoryContextData, Oid, ParamKind},
    prelude::*,
};
use std::collections::HashMap;
use std::ffi::c_void;
use std::marker::PhantomData;

use pgrx::pg_sys::panic::ErrorReport;
use std::os::raw::c_int;
use std::ptr;

use crate::instance;
use crate::interface::{
    Aggregate, AggregateKind, Cell, Column, ExprEval, Limit, Param, ParamValue, Qual, Row, Sort,
    Value,
};
use crate::limit::*;
use crate::memctx;
use crate::options::options_to_hashmap;
use crate::polyfill;
use crate::prelude::ForeignDataWrapper;
use crate::qual::*;
use crate::sort::*;
use crate::utils::{self, ReportableError, report_error};

const FDW_SCAN_PRIVATE_VERSION: &str = "wrappers-scan-v1";

unsafe extern "C" {
    #[link_name = "datumCopy"]
    fn datum_copy(value: Datum, type_by_value: bool, type_len: c_int) -> Datum;
}

/// CopyObject-safe data stored in `ForeignScan.fdw_private`.
///
/// PostgreSQL copies cached plans after FDW planning callbacks return, so no
/// Rust pointer may be stored in the plan. This value is serialized entirely
/// as PostgreSQL Lists, Const nodes, and (for PARAM_EXEC) the original Expr
/// node, all of which PostgreSQL's `copyObject` understands.
struct FdwScanPrivate {
    foreigntableid: Oid,
    quals: Vec<Qual>,
    tgts: Vec<Column>,
    sorts: Vec<Sort>,
    limit: Option<Limit>,
    opts: HashMap<String, String>,
    aggregates: Vec<Aggregate>,
    group_by: Vec<Column>,
    all_base_quals_extracted: bool,
    aggregate_base_columns: Vec<Column>,
}

unsafe fn make_node_list(nodes: impl IntoIterator<Item = *mut c_void>) -> *mut pg_sys::List {
    unsafe {
        nodes
            .into_iter()
            .fold(ptr::null_mut(), |list, node| pg_sys::lappend(list, node))
    }
}

unsafe fn list_nodes(list: *mut pg_sys::List) -> Option<Vec<*mut c_void>> {
    if list.is_null() {
        return Some(Vec::new());
    }
    pgrx::memcx::current_context(|mcx| unsafe {
        List::<*mut c_void>::downcast_ptr_in_memcx(list, mcx)
            .map(|list| list.iter().copied().collect())
    })
}

unsafe fn make_text_const(value: &str) -> *mut pg_sys::Const {
    unsafe {
        pg_sys::makeConst(
            pg_sys::TEXTOID,
            -1,
            pg_sys::InvalidOid,
            -1,
            value.to_owned().into_datum().unwrap(),
            false,
            false,
        )
    }
}

unsafe fn text_from_node(node: *mut c_void) -> Option<String> {
    if node.is_null()
        || !unsafe { pgrx::is_a(node.cast::<pg_sys::Node>(), pg_sys::NodeTag::T_Const) }
    {
        return None;
    }
    unsafe {
        let constant = &*node.cast::<pg_sys::Const>();
        if constant.consttype != pg_sys::TEXTOID {
            return None;
        }
        String::from_datum(constant.constvalue, constant.constisnull)
    }
}

unsafe fn bool_from_node(node: *mut c_void) -> Option<bool> {
    match unsafe { text_from_node(node) }?.as_str() {
        "0" => Some(false),
        "1" => Some(true),
        _ => None,
    }
}

fn cell_type_oid(cell: &Cell) -> Oid {
    match cell {
        Cell::Bool(_) => pg_sys::BOOLOID,
        Cell::I8(_) => pg_sys::CHAROID,
        Cell::I16(_) => pg_sys::INT2OID,
        Cell::F32(_) => pg_sys::FLOAT4OID,
        Cell::I32(_) => pg_sys::INT4OID,
        Cell::F64(_) => pg_sys::FLOAT8OID,
        Cell::I64(_) => pg_sys::INT8OID,
        Cell::Numeric(_) => pg_sys::NUMERICOID,
        Cell::String(_) => pg_sys::TEXTOID,
        Cell::Date(_) => pg_sys::DATEOID,
        Cell::Time(_) => pg_sys::TIMEOID,
        Cell::Timestamp(_) => pg_sys::TIMESTAMPOID,
        Cell::Timestamptz(_) => pg_sys::TIMESTAMPTZOID,
        Cell::Interval(_) => pg_sys::INTERVALOID,
        Cell::Json(_) => pg_sys::JSONBOID,
        Cell::Bytea(_) => pg_sys::BYTEAOID,
        Cell::Uuid(_) => pg_sys::UUIDOID,
        Cell::BoolArray(_) => pg_sys::BOOLARRAYOID,
        Cell::I16Array(_) => pg_sys::INT2ARRAYOID,
        Cell::I32Array(_) => pg_sys::INT4ARRAYOID,
        Cell::I64Array(_) => pg_sys::INT8ARRAYOID,
        Cell::F32Array(_) => pg_sys::FLOAT4ARRAYOID,
        Cell::F64Array(_) => pg_sys::FLOAT8ARRAYOID,
        Cell::StringArray(_) => pg_sys::TEXTARRAYOID,
    }
}

unsafe fn make_cell_const(cell: &Cell) -> *mut pg_sys::Const {
    unsafe {
        let type_oid = cell_type_oid(cell);
        let mut type_len = 0;
        let mut type_by_value = false;
        pg_sys::get_typlenbyval(type_oid, &mut type_len, &mut type_by_value);
        let datum = datum_copy(
            cell.clone().into_datum().unwrap(),
            type_by_value,
            type_len as c_int,
        );
        pg_sys::makeConst(
            type_oid,
            -1,
            pg_sys::InvalidOid,
            type_len as _,
            datum,
            false,
            type_by_value,
        )
    }
}

unsafe fn cell_from_node(node: *mut c_void) -> Option<Cell> {
    if node.is_null()
        || !unsafe { pgrx::is_a(node.cast::<pg_sys::Node>(), pg_sys::NodeTag::T_Const) }
    {
        return None;
    }
    unsafe {
        let constant = &*node.cast::<pg_sys::Const>();
        Cell::from_polymorphic_datum(
            constant.constvalue,
            constant.constisnull,
            constant.consttype,
        )
    }
}

unsafe fn serialize_column(column: &Column) -> *mut pg_sys::List {
    unsafe {
        make_node_list([
            make_text_const(&column.name).cast(),
            make_text_const(&column.num.to_string()).cast(),
            make_text_const(&column.type_oid.to_u32().to_string()).cast(),
        ])
    }
}

unsafe fn deserialize_column(list: *mut pg_sys::List) -> Option<Column> {
    unsafe {
        let nodes = list_nodes(list)?;
        if nodes.len() != 3 {
            return None;
        }
        Some(Column {
            name: text_from_node(*nodes.first()?)?,
            num: text_from_node(*nodes.get(1)?)?.parse().ok()?,
            type_oid: Oid::from(text_from_node(*nodes.get(2)?)?.parse::<u32>().ok()?),
        })
    }
}

unsafe fn serialize_qual(
    qual: &Qual,
    param_exprs: &mut Vec<*mut pg_sys::Expr>,
) -> *mut pg_sys::List {
    unsafe {
        let (value_is_array, cells) = match &qual.value {
            Value::Cell(cell) => (false, vec![cell]),
            Value::Array(cells) => (true, cells.iter().collect()),
        };
        let value_nodes =
            make_node_list(cells.into_iter().map(|cell| make_cell_const(cell).cast()));

        let (has_param, param_kind, param_id, param_type_oid, param_expr_index) =
            if let Some(param) = &qual.param {
                let kind = match param.kind {
                    ParamKind::PARAM_EXTERN => "extern",
                    ParamKind::PARAM_EXEC => "exec",
                    _ => "unsupported",
                };
                let expr_index =
                    if param.kind == ParamKind::PARAM_EXEC && !param.expr_eval.expr.is_null() {
                        param_exprs.push(param.expr_eval.expr);
                        (param_exprs.len() - 1).to_string()
                    } else {
                        "-1".to_string()
                    };
                (
                    true,
                    kind,
                    param.id.to_string(),
                    param.type_oid.to_u32().to_string(),
                    expr_index,
                )
            } else {
                (
                    false,
                    "none",
                    "0".to_string(),
                    "0".to_string(),
                    "-1".to_string(),
                )
            };

        make_node_list([
            make_text_const(&qual.field).cast(),
            make_text_const(&qual.operator).cast(),
            make_text_const(if qual.use_or { "1" } else { "0" }).cast(),
            make_text_const(if value_is_array { "1" } else { "0" }).cast(),
            value_nodes.cast(),
            make_text_const(if has_param { "1" } else { "0" }).cast(),
            make_text_const(param_kind).cast(),
            make_text_const(&param_id).cast(),
            make_text_const(&param_type_oid).cast(),
            make_text_const(&param_expr_index).cast(),
        ])
    }
}

unsafe fn deserialize_qual(
    list: *mut pg_sys::List,
    param_exprs: &[*mut pg_sys::Expr],
) -> Option<Qual> {
    unsafe {
        let nodes = list_nodes(list)?;
        if nodes.len() != 10 {
            return None;
        }
        let value_is_array = bool_from_node(*nodes.get(3)?)?;
        let cells = list_nodes(*nodes.get(4)? as *mut pg_sys::List)?
            .into_iter()
            .map(|node| cell_from_node(node))
            .collect::<Option<Vec<_>>>()?;
        let value = if value_is_array {
            Value::Array(cells)
        } else {
            if cells.len() != 1 {
                return None;
            }
            Value::Cell(cells.into_iter().next()?)
        };

        let has_param = bool_from_node(*nodes.get(5)?)?;
        let param = if has_param {
            let kind = match text_from_node(*nodes.get(6)?)?.as_str() {
                "extern" => ParamKind::PARAM_EXTERN,
                "exec" => ParamKind::PARAM_EXEC,
                _ => return None,
            };
            let expr = if kind == ParamKind::PARAM_EXEC {
                let index = text_from_node(*nodes.get(9)?)?.parse::<usize>().ok()?;
                let expr = *param_exprs.get(index)?;
                if expr.is_null()
                    || !pgrx::is_a(expr.cast(), pg_sys::NodeTag::T_Param)
                    || (*expr.cast::<pg_sys::Param>()).paramkind != ParamKind::PARAM_EXEC
                {
                    return None;
                }
                expr
            } else {
                if text_from_node(*nodes.get(9)?)? != "-1" {
                    return None;
                }
                ptr::null_mut()
            };
            Some(Param {
                kind,
                id: text_from_node(*nodes.get(7)?)?.parse().ok()?,
                type_oid: Oid::from(text_from_node(*nodes.get(8)?)?.parse::<u32>().ok()?),
                eval_value: std::sync::Mutex::new(None).into(),
                eval_state: std::sync::Mutex::new(ParamValue::Unevaluated).into(),
                expr_eval: ExprEval {
                    expr,
                    expr_state: ptr::null_mut(),
                },
            })
        } else {
            if text_from_node(*nodes.get(6)?)? != "none"
                || text_from_node(*nodes.get(7)?)? != "0"
                || text_from_node(*nodes.get(8)?)? != "0"
                || text_from_node(*nodes.get(9)?)? != "-1"
            {
                return None;
            }
            None
        };

        Some(Qual {
            field: text_from_node(*nodes.first()?)?,
            operator: text_from_node(*nodes.get(1)?)?,
            use_or: bool_from_node(*nodes.get(2)?)?,
            value,
            param,
        })
    }
}

unsafe fn serialize_sort(sort: &Sort) -> *mut pg_sys::List {
    unsafe {
        make_node_list([
            make_text_const(&sort.field).cast(),
            make_text_const(&sort.field_no.to_string()).cast(),
            make_text_const(if sort.reversed { "1" } else { "0" }).cast(),
            make_text_const(if sort.nulls_first { "1" } else { "0" }).cast(),
            make_text_const(if sort.collate.is_some() { "1" } else { "0" }).cast(),
            make_text_const(sort.collate.as_deref().unwrap_or("")).cast(),
        ])
    }
}

unsafe fn deserialize_sort(list: *mut pg_sys::List) -> Option<Sort> {
    unsafe {
        let nodes = list_nodes(list)?;
        if nodes.len() != 6 {
            return None;
        }
        let has_collate = bool_from_node(*nodes.get(4)?)?;
        let collate = text_from_node(*nodes.get(5)?)?;
        if !has_collate && !collate.is_empty() {
            return None;
        }
        Some(Sort {
            field: text_from_node(*nodes.first()?)?,
            field_no: text_from_node(*nodes.get(1)?)?.parse().ok()?,
            reversed: bool_from_node(*nodes.get(2)?)?,
            nulls_first: bool_from_node(*nodes.get(3)?)?,
            collate: has_collate.then_some(collate),
        })
    }
}

fn aggregate_kind_tag(kind: AggregateKind) -> &'static str {
    match kind {
        AggregateKind::Count => "count",
        AggregateKind::CountColumn => "count-column",
        AggregateKind::Sum => "sum",
        AggregateKind::Avg => "avg",
        AggregateKind::Min => "min",
        AggregateKind::Max => "max",
    }
}

fn aggregate_kind_from_tag(tag: &str) -> Option<AggregateKind> {
    match tag {
        "count" => Some(AggregateKind::Count),
        "count-column" => Some(AggregateKind::CountColumn),
        "sum" => Some(AggregateKind::Sum),
        "avg" => Some(AggregateKind::Avg),
        "min" => Some(AggregateKind::Min),
        "max" => Some(AggregateKind::Max),
        _ => None,
    }
}

unsafe fn serialize_aggregate(aggregate: &Aggregate) -> *mut pg_sys::List {
    unsafe {
        let (has_column, name, num, type_oid) = aggregate
            .column
            .as_ref()
            .map(|column| {
                (
                    true,
                    column.name.as_str(),
                    column.num,
                    column.type_oid.to_u32(),
                )
            })
            .unwrap_or((false, "", 0, 0));
        make_node_list([
            make_text_const(aggregate_kind_tag(aggregate.kind)).cast(),
            make_text_const(if has_column { "1" } else { "0" }).cast(),
            make_text_const(name).cast(),
            make_text_const(&num.to_string()).cast(),
            make_text_const(&type_oid.to_string()).cast(),
            make_text_const(if aggregate.distinct { "1" } else { "0" }).cast(),
            make_text_const(&aggregate.alias).cast(),
            make_text_const(&aggregate.type_oid.to_u32().to_string()).cast(),
        ])
    }
}

unsafe fn deserialize_aggregate(list: *mut pg_sys::List) -> Option<Aggregate> {
    unsafe {
        let nodes = list_nodes(list)?;
        if nodes.len() != 8 {
            return None;
        }
        let column = if bool_from_node(*nodes.get(1)?)? {
            Some(Column {
                name: text_from_node(*nodes.get(2)?)?,
                num: text_from_node(*nodes.get(3)?)?.parse().ok()?,
                type_oid: Oid::from(text_from_node(*nodes.get(4)?)?.parse::<u32>().ok()?),
            })
        } else {
            if !text_from_node(*nodes.get(2)?)?.is_empty()
                || text_from_node(*nodes.get(3)?)? != "0"
                || text_from_node(*nodes.get(4)?)? != "0"
            {
                return None;
            }
            None
        };
        Some(Aggregate {
            kind: aggregate_kind_from_tag(&text_from_node(*nodes.first()?)?)?,
            column,
            distinct: bool_from_node(*nodes.get(5)?)?,
            alias: text_from_node(*nodes.get(6)?)?,
            type_oid: Oid::from(text_from_node(*nodes.get(7)?)?.parse::<u32>().ok()?),
        })
    }
}

impl FdwScanPrivate {
    fn from_state<E: Into<ErrorReport>, W: ForeignDataWrapper<E>>(state: &FdwState<E, W>) -> Self {
        Self {
            foreigntableid: state.foreigntableid,
            quals: state.quals.clone(),
            tgts: state.tgts.clone(),
            sorts: state.sorts.clone(),
            limit: state.limit.clone(),
            opts: state.opts.clone(),
            aggregates: state.aggregates.clone(),
            group_by: state.group_by.clone(),
            all_base_quals_extracted: state.all_base_quals_extracted,
            aggregate_base_columns: state.aggregate_base_columns.clone(),
        }
    }

    /// Serialize to nested PostgreSQL Lists containing only copyObject-safe
    /// nodes. The positional schema is versioned by element zero.
    unsafe fn serialize_to_list(&self) -> (*mut pg_sys::List, *mut pg_sys::List) {
        unsafe {
            let mut param_exprs = Vec::new();
            let quals = make_node_list(
                self.quals
                    .iter()
                    .map(|qual| serialize_qual(qual, &mut param_exprs).cast()),
            );
            let tgts = make_node_list(
                self.tgts
                    .iter()
                    .map(|column| serialize_column(column).cast()),
            );
            let sorts = make_node_list(self.sorts.iter().map(|sort| serialize_sort(sort).cast()));
            let limit = make_node_list([
                make_text_const(if self.limit.is_some() { "1" } else { "0" }).cast(),
                make_text_const(
                    &self
                        .limit
                        .as_ref()
                        .map(|limit| limit.count)
                        .unwrap_or_default()
                        .to_string(),
                )
                .cast(),
                make_text_const(
                    &self
                        .limit
                        .as_ref()
                        .map(|limit| limit.offset)
                        .unwrap_or_default()
                        .to_string(),
                )
                .cast(),
            ]);
            let mut options = self.opts.iter().collect::<Vec<_>>();
            options.sort_unstable_by(|left, right| left.0.cmp(right.0));
            let opts = make_node_list(options.into_iter().map(|(key, value)| {
                make_node_list([make_text_const(key).cast(), make_text_const(value).cast()]).cast()
            }));
            let aggregates = make_node_list(
                self.aggregates
                    .iter()
                    .map(|aggregate| serialize_aggregate(aggregate).cast()),
            );
            let group_by = make_node_list(
                self.group_by
                    .iter()
                    .map(|column| serialize_column(column).cast()),
            );
            let aggregate_base_columns = make_node_list(
                self.aggregate_base_columns
                    .iter()
                    .map(|column| serialize_column(column).cast()),
            );

            let private = make_node_list([
                make_text_const(FDW_SCAN_PRIVATE_VERSION).cast(),
                make_text_const(&self.foreigntableid.to_u32().to_string()).cast(),
                quals.cast(),
                tgts.cast(),
                sorts.cast(),
                limit.cast(),
                opts.cast(),
                aggregates.cast(),
                group_by.cast(),
                make_text_const(if self.all_base_quals_extracted {
                    "1"
                } else {
                    "0"
                })
                .cast(),
                aggregate_base_columns.cast(),
            ]);
            let fdw_exprs = make_node_list(param_exprs.into_iter().map(|expr| expr.cast()));
            (private, fdw_exprs)
        }
    }

    unsafe fn deserialize_from_list(
        list: *mut pg_sys::List,
        fdw_exprs: *mut pg_sys::List,
    ) -> Option<Self> {
        unsafe {
            let nodes = list_nodes(list)?;
            if nodes.len() != 11 || text_from_node(*nodes.first()?)? != FDW_SCAN_PRIVATE_VERSION {
                return None;
            }
            let param_exprs = list_nodes(fdw_exprs)?
                .into_iter()
                .map(|node| {
                    if node.is_null() || !pgrx::is_a(node.cast(), pg_sys::NodeTag::T_Param) {
                        None
                    } else {
                        Some(node.cast::<pg_sys::Expr>())
                    }
                })
                .collect::<Option<Vec<_>>>()?;
            let quals = list_nodes(*nodes.get(2)? as *mut pg_sys::List)?
                .into_iter()
                .map(|node| deserialize_qual(node.cast(), &param_exprs))
                .collect::<Option<Vec<_>>>()?;
            if quals
                .iter()
                .filter(|qual| {
                    qual.param
                        .as_ref()
                        .is_some_and(|param| param.kind == ParamKind::PARAM_EXEC)
                })
                .count()
                != param_exprs.len()
            {
                return None;
            }
            let tgts = list_nodes(*nodes.get(3)? as *mut pg_sys::List)?
                .into_iter()
                .map(|node| deserialize_column(node.cast()))
                .collect::<Option<Vec<_>>>()?;
            let sorts = list_nodes(*nodes.get(4)? as *mut pg_sys::List)?
                .into_iter()
                .map(|node| deserialize_sort(node.cast()))
                .collect::<Option<Vec<_>>>()?;
            let limit_nodes = list_nodes(*nodes.get(5)? as *mut pg_sys::List)?;
            if limit_nodes.len() != 3 {
                return None;
            }
            let limit = if bool_from_node(*limit_nodes.first()?)? {
                Some(Limit {
                    count: text_from_node(*limit_nodes.get(1)?)?.parse().ok()?,
                    offset: text_from_node(*limit_nodes.get(2)?)?.parse().ok()?,
                })
            } else {
                None
            };
            let opts = list_nodes(*nodes.get(6)? as *mut pg_sys::List)?
                .into_iter()
                .map(|node| {
                    let pair = list_nodes(node.cast())?;
                    if pair.len() != 2 {
                        return None;
                    }
                    Some((
                        text_from_node(*pair.first()?)?,
                        text_from_node(*pair.get(1)?)?,
                    ))
                })
                .collect::<Option<HashMap<_, _>>>()?;
            let aggregates = list_nodes(*nodes.get(7)? as *mut pg_sys::List)?
                .into_iter()
                .map(|node| deserialize_aggregate(node.cast()))
                .collect::<Option<Vec<_>>>()?;
            let group_by = list_nodes(*nodes.get(8)? as *mut pg_sys::List)?
                .into_iter()
                .map(|node| deserialize_column(node.cast()))
                .collect::<Option<Vec<_>>>()?;
            let aggregate_base_columns = list_nodes(*nodes.get(10)? as *mut pg_sys::List)?
                .into_iter()
                .map(|node| deserialize_column(node.cast()))
                .collect::<Option<Vec<_>>>()?;

            Some(Self {
                foreigntableid: Oid::from(text_from_node(*nodes.get(1)?)?.parse::<u32>().ok()?),
                quals,
                tgts,
                sorts,
                limit,
                opts,
                aggregates,
                group_by,
                all_base_quals_extracted: bool_from_node(*nodes.get(9)?)?,
                aggregate_base_columns,
            })
        }
    }
}

// Fdw private state for scan
pub(crate) struct FdwState<E: Into<ErrorReport>, W: ForeignDataWrapper<E>> {
    // foreign table used to construct a fresh FDW for each execution
    foreigntableid: Oid,

    // foreign data wrapper instance
    pub(crate) instance: Option<W>,

    // query conditions
    pub(crate) quals: Vec<Qual>,

    // query target column list
    pub(crate) tgts: Vec<Column>,

    // sort list
    pub(crate) sorts: Vec<Sort>,

    // limit
    pub(crate) limit: Option<Limit>,

    // foreign table options
    pub(crate) opts: HashMap<String, String>,

    // aggregate pushdown
    pub(crate) aggregates: Vec<Aggregate>,
    pub(crate) group_by: Vec<Column>,
    pub(crate) all_base_quals_extracted: bool,
    pub(crate) aggregate_base_columns: Vec<Column>,

    // temporary memory context per foreign table, created under Wrappers root
    // memory context
    tmp_ctx: MemoryContext,

    // query result list
    values: Vec<Datum>,
    nulls: Vec<bool>,
    row: Row,
    // fingerprint of current parameter values to detect rescan changes
    param_fingerprint: String,
    // whether begin_scan/begin_aggregate_scan ran for this execution
    scan_started: bool,
    _phantom: PhantomData<E>,
}

impl<E: Into<ErrorReport>, W: ForeignDataWrapper<E>> FdwState<E, W> {
    unsafe fn new(foreigntableid: Oid, tmp_ctx: MemoryContext) -> Self {
        let mut state = Self::new_without_instance(foreigntableid, tmp_ctx);
        state.instance =
            Some(unsafe { instance::create_fdw_instance_from_table_id(foreigntableid) });
        state
    }

    fn new_without_instance(foreigntableid: Oid, tmp_ctx: MemoryContext) -> Self {
        Self {
            foreigntableid,
            instance: None,
            quals: Vec::new(),
            tgts: Vec::new(),
            sorts: Vec::new(),
            limit: None,
            opts: HashMap::new(),
            aggregates: Vec::new(),
            group_by: Vec::new(),
            all_base_quals_extracted: true,
            aggregate_base_columns: Vec::new(),
            tmp_ctx,
            values: Vec::new(),
            nulls: Vec::new(),
            row: Row::new(),
            param_fingerprint: String::new(),
            scan_started: false,
            _phantom: PhantomData,
        }
    }

    #[inline]
    fn get_rel_size(&mut self) -> Result<(i64, i32), E> {
        if let Some(ref mut instance) = self.instance {
            instance.get_rel_size(
                &self.quals,
                &self.tgts,
                &self.sorts,
                &self.limit,
                &self.opts,
            )
        } else {
            Ok((0, 0))
        }
    }

    #[inline]
    pub(crate) fn is_aggregate_scan(&self) -> bool {
        !self.aggregates.is_empty()
    }

    #[inline]
    pub(crate) fn can_pushdown_aggregate(
        &mut self,
        aggregates: &[Aggregate],
        group_by: &[Column],
        base_columns: &[Column],
    ) -> Result<bool, E> {
        if let Some(ref mut instance) = self.instance {
            instance.can_pushdown_aggregate(
                aggregates,
                group_by,
                &self.quals,
                base_columns,
                self.all_base_quals_extracted,
                &self.opts,
            )
        } else {
            Ok(false)
        }
    }

    #[inline]
    pub(crate) fn get_aggregate_rel_size(
        &mut self,
        aggregates: &[Aggregate],
        group_by: &[Column],
    ) -> Result<(i64, i32), E> {
        if let Some(ref mut instance) = self.instance {
            instance.get_aggregate_rel_size(aggregates, group_by, &self.quals, &self.opts)
        } else {
            Ok((0, 0))
        }
    }

    #[inline]
    fn begin_aggregate_scan(&mut self) -> Result<(), E> {
        if let Some(ref mut instance) = self.instance {
            instance.begin_aggregate_scan_with_base_columns(
                &self.aggregates,
                &self.group_by,
                &self.quals,
                &self.aggregate_base_columns,
                &self.opts,
            )
        } else {
            Ok(())
        }
    }

    #[inline]
    fn begin_scan(&mut self) -> Result<(), E> {
        if let Some(ref mut instance) = self.instance {
            instance.begin_scan(
                &self.quals,
                &self.tgts,
                &self.sorts,
                &self.limit,
                &self.opts,
            )
        } else {
            Ok(())
        }
    }

    #[inline]
    fn iter_scan(&mut self) -> Result<Option<()>, E> {
        if let Some(ref mut instance) = self.instance {
            instance.iter_scan(&mut self.row)
        } else {
            Ok(None)
        }
    }

    #[inline]
    fn re_scan(&mut self) -> Result<(), E> {
        if let Some(ref mut instance) = self.instance {
            instance.re_scan()
        } else {
            Ok(())
        }
    }

    #[inline]
    fn end_scan(&mut self) -> Result<(), E> {
        if let Some(ref mut instance) = self.instance {
            instance.end_scan()
        } else {
            Ok(())
        }
    }
}

impl<E: Into<ErrorReport>, W: ForeignDataWrapper<E>> Drop for FdwState<E, W> {
    fn drop(&mut self) {
        // drop foreign data wrapper instance
        self.instance.take();

        // remove the allocated memory context
        unsafe {
            memctx::delete_wrappers_memctx(self.tmp_ctx);
            self.tmp_ctx = ptr::null::<MemoryContextData>() as _;
        }
    }
}

#[pg_guard]
pub(super) extern "C-unwind" fn get_foreign_rel_size<
    E: Into<ErrorReport>,
    W: ForeignDataWrapper<E>,
>(
    root: *mut pg_sys::PlannerInfo,
    baserel: *mut pg_sys::RelOptInfo,
    foreigntableid: pg_sys::Oid,
) {
    debug2!("---> get_foreign_rel_size");
    unsafe {
        // create memory context for scan
        let ctx_name = format!("Wrappers_scan_{}", foreigntableid.to_u32());
        let ctx = memctx::create_wrappers_memctx(&ctx_name);

        // create scan state
        let mut state = FdwState::<E, W>::new(foreigntableid, ctx);

        PgMemoryContexts::For(state.tmp_ctx).switch_to(|_| {
            // extract qual list
            let extracted_quals = extract_quals(root, baserel, foreigntableid);
            state.quals = extracted_quals.quals;
            state.all_base_quals_extracted = extracted_quals.all_extracted;

            // extract target column list from target and restriction expression
            state.tgts = utils::extract_target_columns(root, baserel);

            // extract sort list
            state.sorts = extract_sorts(root, baserel, foreigntableid);

            // extract limit
            state.limit = extract_limit(root, baserel, foreigntableid);

            // get foreign table options
            let ftable = pg_sys::GetForeignTable(foreigntableid);
            state.opts = options_to_hashmap((*ftable).options).report_unwrap();

            // add additional metadata to the options
            state.opts.insert(
                "wrappers.fserver_oid".into(),
                (*ftable).serverid.to_u32().to_string(),
            );
            state.opts.insert(
                "wrappers.ftable_oid".into(),
                (*ftable).relid.to_u32().to_string(),
            );
        });

        // get estimate row count and mean row width
        let (rows, width) = state.get_rel_size().report_unwrap();
        (*baserel).rows = rows as f64;
        (*(*baserel).reltarget).width = width;

        // This is planner-only state. Register it on PlannerInfo.planner_cxt
        // explicitly so it survives every FDW planning callback but never
        // escapes into the copyObject'd CachedPlan.
        (*baserel).fdw_private =
            PgMemoryContexts::For((*root).planner_cxt).leak_and_drop_on_delete(state) as _;
    }
}

#[pg_guard]
pub(super) extern "C-unwind" fn get_foreign_paths<
    E: Into<ErrorReport>,
    W: ForeignDataWrapper<E>,
>(
    root: *mut pg_sys::PlannerInfo,
    baserel: *mut pg_sys::RelOptInfo,
    _foreigntableid: pg_sys::Oid,
) {
    debug2!("---> get_foreign_paths");
    unsafe {
        let state = PgBox::<FdwState<E, W>>::from_pg((*baserel).fdw_private as _);

        // get startup cost from foreign table options
        let startup_cost = state
            .opts
            .get("startup_cost")
            .map(|c| match c.parse::<f64>() {
                Ok(v) => v,
                Err(_) => {
                    pgrx::error!("invalid option startup_cost: {}", c);
                }
            })
            .unwrap_or(0.0);
        let total_cost = startup_cost + (*baserel).rows;

        // create a ForeignPath node and add it as the only possible path
        let path = pg_sys::create_foreignscan_path(
            root,
            baserel,
            ptr::null_mut(), // default pathtarget
            (*baserel).rows,
            #[cfg(feature = "pg18")]
            0, // disabled_nodes
            startup_cost,
            total_cost,
            ptr::null_mut(), // no pathkeys
            ptr::null_mut(), // no outer rel either
            ptr::null_mut(), // no extra plan
            #[cfg(any(feature = "pg17", feature = "pg18"))]
            ptr::null_mut(), // no restrict info
            ptr::null_mut(), // no fdw_private data
        );
        pg_sys::add_path(baserel, &mut ((*path).path));
    }
}

#[pg_guard]
pub(super) extern "C-unwind" fn get_foreign_plan<E: Into<ErrorReport>, W: ForeignDataWrapper<E>>(
    _root: *mut pg_sys::PlannerInfo,
    baserel: *mut pg_sys::RelOptInfo,
    _foreigntableid: pg_sys::Oid,
    _best_path: *mut pg_sys::ForeignPath,
    tlist: *mut pg_sys::List,
    scan_clauses: *mut pg_sys::List,
    outer_plan: *mut pg_sys::Plan,
) -> *mut pg_sys::ForeignScan {
    debug2!("---> get_foreign_plan");
    unsafe {
        let mut state = PgBox::<FdwState<E, W>>::from_pg((*baserel).fdw_private as _);

        // make foreign scan plan
        let scan_clauses = pg_sys::extract_actual_clauses(scan_clauses, false);

        // Aggregate pushdown: state.aggregates was populated by upper.rs via the
        // shared FdwState pointer (input_rel.fdw_private and output_rel.fdw_private
        // alias the same object). That mutation is visible regardless of which
        // path the planner picked, so it cannot be used as the discriminator —
        // we must key off baserel.reloptkind. When the planner picked the upper
        // aggregate path, baserel IS the upper rel; otherwise we are being called
        // for the base-rel scan path (with a local Aggregate node above us) and
        // must NOT treat this as an aggregate scan.
        let is_agg = (*baserel).reloptkind == pg_sys::RelOptKind::RELOPT_UPPER_REL
            && state.is_aggregate_scan();

        if !is_agg && state.is_aggregate_scan() {
            // Upper-path was registered but the planner chose the base-rel scan
            // (typically because a local HashAgg over a small input was cheaper).
            // Drop the aggregate state so begin_foreign_scan dispatches to
            // begin_scan and the tuple slot is the base-rel's row type.
            state.aggregates = Vec::new();
            state.group_by = Vec::new();
            state.aggregate_base_columns = Vec::new();
        }

        let (final_tlist, agg_fdw_scan_tlist) = if is_agg {
            // baserel here is the GROUP_AGG upper rel; its reltarget->exprs
            // contains the aggregate outputs (Var nodes for GROUP BY columns
            // and Aggref nodes for the aggregates). Build both the plan tlist
            // and fdw_scan_tlist from it so:
            //   1. final_tlist has the right Aggref/Var nodes for
            //      set_foreignscan_references to rewrite into Var(INDEX_VAR,n).
            //   2. fdw_scan_tlist drives ExecTypeFromTL to a tuple descriptor
            //      whose attribute types match what iter_scan returns —
            //      otherwise heap_form_tuple dereferences a non-pointer Datum
            //      and segfaults.
            let reltarget = (*baserel).reltarget;
            let exprs = (*reltarget).exprs;
            let n = if exprs.is_null() {
                0
            } else {
                (*exprs).length as usize
            };
            let mut agg_tlist: *mut pg_sys::List = ptr::null_mut();
            for i in 0..n {
                let cell = (*exprs).elements.add(i);
                let expr = (*cell).ptr_value as *mut pg_sys::Expr;
                let tle = pg_sys::makeTargetEntry(
                    expr,
                    (i + 1) as pg_sys::AttrNumber,
                    ptr::null_mut(),
                    false,
                );
                // Preserve sortgrouprefs so Sort nodes can identify columns.
                if !(*reltarget).sortgrouprefs.is_null() {
                    (*tle).ressortgroupref = *(*reltarget).sortgrouprefs.add(i);
                }
                agg_tlist = pg_sys::lappend(agg_tlist, tle as *mut std::ffi::c_void);
            }
            let fdw_scan_tlist = pg_sys::list_copy(agg_tlist);

            // Now that we know the upper path is in the plan, project state.tgts
            // to match the aggregate output shape. iterate_foreign_scan uses
            // tgts to map cells returned by the FDW's iter_scan into the
            // correct attribute slots; for aggregate scans this is GROUP BY
            // columns first, then aggregate result columns keyed by alias.
            // We keep this off the base-rel-scan code path so state.tgts (the
            // base-rel scan columns set in get_foreign_rel_size) is preserved
            // when the planner picks the base-rel path.
            let mut new_tgts = Vec::new();
            let mut col_num = 1usize;
            for col in &state.group_by {
                new_tgts.push(Column {
                    name: col.name.clone(),
                    num: col_num,
                    type_oid: col.type_oid,
                });
                col_num += 1;
            }
            for agg in &state.aggregates {
                new_tgts.push(Column {
                    name: agg.alias.clone(),
                    num: col_num,
                    type_oid: agg.type_oid,
                });
                col_num += 1;
            }
            state.tgts = new_tgts;

            (agg_tlist, fdw_scan_tlist)
        } else {
            (tlist, ptr::null_mut())
        };

        // Serialize only PostgreSQL-native nodes into the plan. Cached plans
        // are copyObject'd after this callback, so storing the Rust state
        // pointer here would leave the copied plan with a dangling pointer.
        let private = FdwScanPrivate::from_state(&state);
        state.instance.take();
        let (fdw_private, fdw_exprs) = private.serialize_to_list();

        let foreign_scan = pg_sys::make_foreignscan(
            final_tlist,
            scan_clauses,
            (*baserel).relid,
            ptr::null_mut(),
            fdw_private as _,
            agg_fdw_scan_tlist,
            ptr::null_mut(),
            outer_plan,
        );
        // PARAM_EXEC expressions live in the dedicated ForeignScan expression
        // list so core planner fixups and copyObject process them normally.
        (*foreign_scan).fdw_exprs = fdw_exprs;
        foreign_scan
    }
}

#[pg_guard]
pub(super) extern "C-unwind" fn explain_foreign_scan<
    E: Into<ErrorReport>,
    W: ForeignDataWrapper<E>,
>(
    node: *mut pg_sys::ForeignScanState,
    es: *mut pg_sys::ExplainState,
) {
    debug2!("---> explain_foreign_scan");
    unsafe {
        let fdw_state = (*node).fdw_state as *mut FdwState<E, W>;
        if fdw_state.is_null() {
            return;
        }

        let state = PgBox::<FdwState<E, W>>::from_pg(fdw_state);

        let ctx = PgMemoryContexts::For(state.tmp_ctx);

        let label = ctx.pstrdup("Wrappers");

        let value = ctx.pstrdup(&format!("quals = {:?}", state.quals));
        pg_sys::ExplainPropertyText(label, value, es);

        let value = ctx.pstrdup(&format!("tgts = {:?}", state.tgts));
        pg_sys::ExplainPropertyText(label, value, es);

        let value = ctx.pstrdup(&format!("sorts = {:?}", state.sorts));
        pg_sys::ExplainPropertyText(label, value, es);

        let value = ctx.pstrdup(&format!("limit = {:?}", state.limit));
        pg_sys::ExplainPropertyText(label, value, es);

        if !state.aggregates.is_empty() {
            let value = ctx.pstrdup(&format!("aggregates = {:?}", state.aggregates));
            pg_sys::ExplainPropertyText(label, value, es);

            let value = ctx.pstrdup(&format!("group_by = {:?}", state.group_by));
            pg_sys::ExplainPropertyText(label, value, es);
        }
    }
}

// extract parameter value and assign it to qual in scan state
unsafe fn assign_parameter_value<E: Into<ErrorReport>, W: ForeignDataWrapper<E>>(
    node: *mut pg_sys::ForeignScanState,
    state: &mut FdwState<E, W>,
) {
    unsafe {
        let estate = (*node).ss.ps.state;
        let econtext = (*node).ss.ps.ps_ExprContext;

        // assign parameter value to qual
        for qual in &mut state.quals.iter_mut() {
            if let Some(param) = &mut qual.param {
                let current_value = match param.kind {
                    ParamKind::PARAM_EXTERN => {
                        // get parameter list in execution state
                        let plist_info = (*estate).es_param_list_info;
                        if !plist_info.is_null() {
                            let params_cnt = (*plist_info).numParams as usize;
                            if param.id > 0 && param.id <= params_cnt {
                                let plist = (*plist_info).params.as_slice(params_cnt);
                                let p: pg_sys::ParamExternData = plist[param.id - 1];
                                if p.isnull {
                                    ParamValue::Null
                                } else if let Some(cell) =
                                    Cell::from_polymorphic_datum(p.value, p.isnull, p.ptype)
                                {
                                    qual.value = Value::Cell(cell.clone());
                                    ParamValue::Value(Value::Cell(cell))
                                } else {
                                    ParamValue::Unevaluated
                                }
                            } else {
                                ParamValue::Unevaluated
                            }
                        } else {
                            ParamValue::Unevaluated
                        }
                    }
                    ParamKind::PARAM_EXEC => {
                        // evaluate parameter value
                        param.expr_eval.expr_state = pg_sys::ExecInitExpr(
                            param.expr_eval.expr,
                            node as *mut pg_sys::PlanState,
                        );
                        let mut isnull = false;
                        match polyfill::exec_eval_expr(
                            param.expr_eval.expr_state,
                            econtext,
                            &mut isnull,
                        ) {
                            Some(_) if isnull => ParamValue::Null,
                            Some(datum) => {
                                if let Some(cell) =
                                    Cell::from_polymorphic_datum(datum, false, param.type_oid)
                                {
                                    qual.value = Value::Cell(cell.clone());
                                    ParamValue::Value(Value::Cell(cell))
                                } else {
                                    ParamValue::Unevaluated
                                }
                            }
                            None => ParamValue::Unevaluated,
                        }
                    }
                    _ => ParamValue::Unevaluated,
                };

                param.set_evaluated_value(current_value);
            }
        }
    }
}

fn parameter_fingerprint(qual: &Qual) -> Option<String> {
    qual.param.as_ref().map(|param| {
        let eval_value = format!("{:?}", param.evaluated_value());
        format!(
            "{}|{}|{}|{}|{}|{}|{}",
            qual.field,
            qual.operator,
            qual.use_or,
            param.kind,
            param.id,
            param.type_oid,
            eval_value,
        )
    })
}

fn compute_param_fingerprint<E: Into<ErrorReport>, W: ForeignDataWrapper<E>>(
    state: &FdwState<E, W>,
) -> String {
    state
        .quals
        .iter()
        .filter_map(parameter_fingerprint)
        .collect::<Vec<_>>()
        .join(";")
}

#[pg_guard]
pub(super) extern "C-unwind" fn begin_foreign_scan<
    E: Into<ErrorReport>,
    W: ForeignDataWrapper<E>,
>(
    node: *mut pg_sys::ForeignScanState,
    eflags: c_int,
) {
    debug2!("---> begin_foreign_scan");
    unsafe {
        let scan_state = (*node).ss;
        let plan = scan_state.ps.plan as *mut pg_sys::ForeignScan;
        let Some(private) =
            FdwScanPrivate::deserialize_from_list((*plan).fdw_private, (*plan).fdw_exprs)
        else {
            report_error(
                PgSqlErrorCode::ERRCODE_FDW_ERROR,
                "invalid fdw_private data in begin_foreign_scan",
            );
            return;
        };

        // Every execution, including each use of a cached/generic plan, owns a
        // fresh Rust state, FDW instance, parameter cells, and temporary
        // context. The plan contains no mutable Rust allocation.
        let explain_only = eflags & pg_sys::EXEC_FLAG_EXPLAIN_ONLY as c_int > 0;
        let ctx_name = format!("Wrappers_scan_exec_{}", private.foreigntableid.to_u32());
        let ctx = memctx::create_wrappers_memctx(&ctx_name);
        // Start without a client so the query context can become the owner
        // before W::new performs any fallible work. EXPLAIN without ANALYZE
        // keeps this metadata-only state for ExplainForeignScan.
        let mut state = FdwState::<E, W>::new_without_instance(private.foreigntableid, ctx);
        PgMemoryContexts::For(ctx).switch_to(|_| {
            state.quals = private.quals;
            state.tgts = private.tgts;
            state.sorts = private.sorts;
            state.limit = private.limit;
            state.opts = private.opts;
            state.aggregates = private.aggregates;
            state.group_by = private.group_by;
            state.all_base_quals_extracted = private.all_base_quals_extracted;
            state.aggregate_base_columns = private.aggregate_base_columns;
        });

        // The executor query context is the sole owner. Its reset callback
        // drops the state and its Wrappers temp context even when PostgreSQL
        // exits through ERROR before EndForeignScan runs.
        let estate = scan_state.ps.state;
        let state_ptr =
            PgMemoryContexts::For((*estate).es_query_cxt).leak_and_drop_on_delete(state);
        (*node).fdw_state = state_ptr.cast();
        let mut state = PgBox::<FdwState<E, W>>::from_pg(state_ptr);

        // begin scan if it is not EXPLAIN statement
        if !explain_only {
            state.instance = Some(instance::create_fdw_instance_from_table_id(
                state.foreigntableid,
            ));

            // assign parameter values to qual
            assign_parameter_value(node, &mut state);
            state.param_fingerprint = compute_param_fingerprint(&state);

            // choose aggregate scan or normal scan based on state
            let result = if state.is_aggregate_scan() {
                state.begin_aggregate_scan()
            } else {
                state.begin_scan()
            };
            if result.is_err() {
                result.report_unwrap();
                return;
            }
            state.scan_started = true;

            // For aggregate upper-rel scans, scanrelid=0 so ss_currentRelation is
            // NULL. Use the number of output columns from state.tgts instead.
            let natts = if state.is_aggregate_scan() {
                state.tgts.len()
            } else {
                let rel = scan_state.ss_currentRelation;
                (*(*rel).rd_att).natts as usize
            };

            // initialize scan result lists
            state
                .values
                .extend_from_slice(&vec![0.into_datum().unwrap(); natts]);
            state.nulls.extend_from_slice(&vec![true; natts]);
        }
    }
}

#[pg_guard]
pub(super) extern "C-unwind" fn iterate_foreign_scan<
    E: Into<ErrorReport>,
    W: ForeignDataWrapper<E>,
>(
    node: *mut pg_sys::ForeignScanState,
) -> *mut pg_sys::TupleTableSlot {
    // `debug!` macros are quite expensive at the moment, so avoid logging in the inner loop
    // debug2!("---> iterate_foreign_scan");
    unsafe {
        let mut state = PgBox::<FdwState<E, W>>::from_pg((*node).fdw_state as _);

        // evaluate parameter values
        assign_parameter_value(node, &mut state);

        // clear slot
        let slot = (*node).ss.ss_ScanTupleSlot;
        polyfill::exec_clear_tuple(slot);

        state.row.clear();

        let result = state.iter_scan();
        if result.report_unwrap().is_some() {
            if state.row.cols.len() != state.tgts.len() {
                report_error(
                    PgSqlErrorCode::ERRCODE_FDW_INVALID_COLUMN_NUMBER,
                    "target column number not match",
                );
                return slot;
            }

            let is_agg = state.is_aggregate_scan();
            PgMemoryContexts::For(state.tmp_ctx).switch_to(|_| {
                for i in 0..state.row.cells.len() {
                    let att_idx = state.tgts[i].num - 1;
                    let cell = state.row.cells.get_unchecked_mut(i);
                    match cell.take() {
                        Some(cell) => {
                            state.values[att_idx] = cell.into_datum().unwrap();
                            state.nulls[att_idx] = false;
                        }
                        None => {
                            state.nulls[att_idx] = true;
                        }
                    }
                }

                if is_agg {
                    // For aggregate scans the slot type is TTSOpsHeapTuple (because
                    // fdw_scan_tlist != NIL).  ExecStoreVirtualTuple is only correct
                    // for TTSOpsVirtual slots; using it on a HeapTuple slot leaves
                    // hslot->tuple == NULL, which causes tts_heap_materialize (called
                    // by Sort) to re-read tts_values after zeroing tts_nvalid —
                    // resulting in a SIGSEGV when a Sort node is present (ORDER BY).
                    // Form a proper HeapTuple and use ExecStoreHeapTuple instead.
                    let desc = (*slot).tts_tupleDescriptor;
                    let htup = pg_sys::heap_form_tuple(
                        desc,
                        state.values.as_mut_ptr(),
                        state.nulls.as_mut_ptr(),
                    );
                    pg_sys::ExecStoreHeapTuple(htup, slot, true);
                } else {
                    (*slot).tts_values = state.values.as_mut_ptr();
                    (*slot).tts_isnull = state.nulls.as_mut_ptr();
                    pg_sys::ExecStoreVirtualTuple(slot);
                }
            });
        }

        slot
    }
}

#[pg_guard]
pub(super) extern "C-unwind" fn re_scan_foreign_scan<
    E: Into<ErrorReport>,
    W: ForeignDataWrapper<E>,
>(
    node: *mut pg_sys::ForeignScanState,
) {
    debug2!("---> re_scan_foreign_scan");
    unsafe {
        let fdw_state = (*node).fdw_state as *mut FdwState<E, W>;
        if !fdw_state.is_null() {
            let mut state = PgBox::<FdwState<E, W>>::from_pg(fdw_state);
            assign_parameter_value(node, &mut state);
            let next_fingerprint = compute_param_fingerprint(&state);
            let result = if next_fingerprint != state.param_fingerprint {
                state.param_fingerprint = next_fingerprint;
                // end the active scan to release resources before restarting with new params
                let _ = state.end_scan();
                if state.is_aggregate_scan() {
                    state.begin_aggregate_scan()
                } else {
                    state.begin_scan()
                }
            } else {
                state.re_scan()
            };
            if result.is_err() {
                result.report_unwrap();
            }
        }
    }
}

#[pg_guard]
pub(super) extern "C-unwind" fn end_foreign_scan<E: Into<ErrorReport>, W: ForeignDataWrapper<E>>(
    node: *mut pg_sys::ForeignScanState,
) {
    debug2!("---> end_foreign_scan");
    unsafe {
        let fdw_state = (*node).fdw_state as *mut FdwState<E, W>;
        if fdw_state.is_null() {
            return;
        }

        let mut state = PgBox::<FdwState<E, W>>::from_pg(fdw_state);
        let result = if state.scan_started {
            let result = state.end_scan();
            state.scan_started = false;
            result
        } else {
            Ok(())
        };
        // The es_query_cxt callback remains the sole owner and will drop this
        // state exactly once after executor teardown.
        (*node).fdw_state = ptr::null::<FdwState<E, W>>() as _;

        result.report_unwrap();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::interface::{ExprEval, Param};
    use std::sync::Mutex;

    fn parameterized_qual() -> Qual {
        Qual {
            field: "value".to_string(),
            operator: "=".to_string(),
            value: Value::Cell(Cell::I64(0)),
            use_or: false,
            param: Some(Param {
                kind: ParamKind::PARAM_EXTERN,
                id: 1,
                type_oid: pg_sys::INT8OID,
                eval_value: Mutex::new(None).into(),
                eval_state: Mutex::new(ParamValue::Unevaluated).into(),
                expr_eval: ExprEval {
                    expr: ptr::null_mut(),
                    expr_state: ptr::null_mut(),
                },
            }),
        }
    }

    #[test]
    fn test_parameter_fingerprint_distinguishes_null_transitions() {
        let qual = parameterized_qual();
        let unevaluated = parameter_fingerprint(&qual).expect("parameter fingerprint");

        qual.param
            .as_ref()
            .expect("parameter")
            .set_evaluated_value(ParamValue::Null);
        let null = parameter_fingerprint(&qual).expect("parameter fingerprint");

        qual.param
            .as_ref()
            .expect("parameter")
            .set_evaluated_value(ParamValue::Value(Value::Cell(Cell::I64(7))));
        let value = parameter_fingerprint(&qual).expect("parameter fingerprint");

        assert_ne!(unevaluated, null);
        assert_ne!(null, value);
        assert_ne!(unevaluated, value);
        assert!(null.contains("Null"));
    }
}
