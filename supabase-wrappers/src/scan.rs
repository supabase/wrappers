use pgrx::FromDatum;
use pgrx::{
    IntoDatum, PgSqlErrorCode, debug2,
    list::List,
    memcx::MemCx,
    memcxt::PgMemoryContexts,
    pg_sys::{Datum, MemoryContext, MemoryContextData, Oid, ParamKind},
    prelude::*,
};
use std::collections::HashMap;
use std::ffi::c_void;
use std::marker::PhantomData;
use std::mem;
use std::sync::Mutex;

use pgrx::pg_sys::panic::ErrorReport;
use std::os::raw::c_int;
use std::ptr;

use crate::instance;
use crate::interface::{
    Aggregate, AggregateKind, Cell, Column, ExprEval, Limit, Param, Qual, Row, Sort, Value,
};
use crate::limit::*;
use crate::memctx;
use crate::options::options_to_hashmap;
use crate::polyfill;
use crate::prelude::ForeignDataWrapper;
use crate::qual::*;
use crate::sort::*;
use crate::utils::{self, ReportableError, report_error};

// Fdw private state for scan
pub(crate) struct FdwState<E: Into<ErrorReport>, W: ForeignDataWrapper<E>> {
    // The base relation's foreign table Oid, captured once during
    // `get_foreign_rel_size` (always called for the base rel, so always valid).
    // `get_foreign_plan` must use this rather than its own `foreigntableid`
    // parameter: for an aggregate-pushdown plan, `baserel` there is the upper
    // (GROUP_AGG) relation, and Postgres passes `InvalidOid` in that case since
    // an upper rel isn't tied to a single base relation.
    pub(crate) foreigntableid: Oid,

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

    // temporary memory context per foreign table, created under Wrappers root
    // memory context
    tmp_ctx: MemoryContext,

    // query result list
    values: Vec<Datum>,
    nulls: Vec<bool>,
    row: Row,
    // fingerprint of current parameter values to detect rescan changes
    param_fingerprint: String,
    _phantom: PhantomData<E>,
}

impl<E: Into<ErrorReport>, W: ForeignDataWrapper<E>> FdwState<E, W> {
    // Used only for planning (`get_foreign_rel_size`). `get_rel_size`,
    // `supported_aggregates` and `supports_group_by` are the only planning-time
    // trait hooks, and none of them take a `self`, so planning never needs a
    // live FDW instance — leaving `instance: None` here means the (potentially
    // expensive) `W::new()` only ever runs once per actual execution.
    unsafe fn new(foreigntableid: Oid, tmp_ctx: MemoryContext) -> Self {
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
            tmp_ctx,
            values: Vec::new(),
            nulls: Vec::new(),
            row: Row::new(),
            param_fingerprint: String::new(),
            _phantom: PhantomData,
        }
    }

    #[inline]
    fn get_rel_size(&mut self) -> Result<(i64, i32), E> {
        W::get_rel_size(
            &self.quals,
            &self.tgts,
            &self.sorts,
            &self.limit,
            &self.opts,
        )
    }

    #[inline]
    pub(crate) fn is_aggregate_scan(&self) -> bool {
        !self.aggregates.is_empty()
    }

    #[inline]
    fn begin_aggregate_scan(&mut self) -> Result<(), E> {
        if let Some(ref mut instance) = self.instance {
            instance.begin_aggregate_scan(&self.aggregates, &self.group_by, &self.quals, &self.opts)
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

// drop the scan state, so the inner fdw instance can be dropped too
unsafe fn drop_fdw_state<E: Into<ErrorReport>, W: ForeignDataWrapper<E>>(
    fdw_state: *mut FdwState<E, W>,
) {
    let boxed_fdw_state = unsafe { Box::from_raw(fdw_state) };
    drop(boxed_fdw_state);
}

/// This struct is a serializable state of the planning time data needed to
/// rebuild [`FdwState`] in the execution phase.
///
/// Unline [`FdwState`] which owns a live FDW instance, a Postgres MemoryContext,
/// and per-scan row buffers, this struct holds only plain data. This struct will
/// be serialized as a [`pg_sys::List`] of [`pg_sys::Const`] nodes so that when
/// Postgres calls `copyObject` on it at the end of the plan phasse (after the
/// function call [`get_foreign_plan`]) it is deep copied correctly and rebuilt
/// successfully at the beginning of the [`begin_foreign_scan`] function.
struct FdwScanPrivate {
    foreigntableid: Oid,
    quals: Vec<Qual>,
    tgts: Vec<Column>,
    sorts: Vec<Sort>,
    limit: Option<Limit>,
    aggregates: Vec<Aggregate>,
    group_by: Vec<Column>,
}

/// How a `Qual::value` is encoded in [`FdwScanPrivate`]'s serialized list. `ScalarConst`/
/// `ArrayConst` embed the original `pg_sys::Const` node (see [`Qual::value_const`]) so
/// `copyObject` deep-copies it with the correct `consttype`; `Bool` and `Placeholder`
/// have no source `Const` node to preserve (see `push_qual`/`read_qual`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum QualValueMode {
    Bool = 0,
    Placeholder = 1,
    ScalarConst = 2,
    ArrayConst = 3,
}

impl QualValueMode {
    fn from_i32(val: i32) -> Option<Self> {
        match val {
            0 => Some(Self::Bool),
            1 => Some(Self::Placeholder),
            2 => Some(Self::ScalarConst),
            3 => Some(Self::ArrayConst),
            _ => None,
        }
    }
}

impl FdwScanPrivate {
    unsafe fn serialize_to_list(&self) -> *mut pg_sys::List {
        unsafe {
            pgrx::memcx::current_context(|mcx| {
                let mut ret = List::<*mut c_void>::Nil;
                Self::push_oid(&mut ret, mcx, self.foreigntableid);
                Self::push_quals(&mut ret, mcx, &self.quals);
                Self::push_columns(&mut ret, mcx, &self.tgts);
                Self::push_sorts(&mut ret, mcx, &self.sorts);
                Self::push_limit(&mut ret, mcx, &self.limit);
                Self::push_aggregates(&mut ret, mcx, &self.aggregates);
                Self::push_columns(&mut ret, mcx, &self.group_by);
                ret.into_ptr()
            })
        }
    }

    unsafe fn deserialize_from_list(list: *mut pg_sys::List) -> Option<Self> {
        unsafe {
            pgrx::memcx::current_context(|mcx| {
                let list = List::<*mut c_void>::downcast_ptr_in_memcx(list, mcx)?;
                let mut idx = 0usize;

                let foreigntableid = Self::read_oid(&list, &mut idx)?;
                let quals = Self::read_quals(&list, &mut idx)?;
                let tgts = Self::read_columns(&list, &mut idx)?;
                let sorts = Self::read_sorts(&list, &mut idx)?;
                let limit = Self::read_limit(&list, &mut idx)?;
                let aggregates = Self::read_aggregates(&list, &mut idx)?;
                let group_by = Self::read_columns(&list, &mut idx)?;

                Some(FdwScanPrivate {
                    foreigntableid,
                    quals,
                    tgts,
                    sorts,
                    limit,
                    aggregates,
                    group_by,
                })
            })
        }
    }

    unsafe fn push_i32<'cx>(list: &mut List<'cx, *mut c_void>, mcx: &'cx MemCx<'_>, val: i32) {
        unsafe {
            let cst = pg_sys::makeConst(
                pg_sys::INT4OID,
                -1,
                pg_sys::InvalidOid,
                4,
                val.into_datum().unwrap(),
                false,
                true,
            );
            list.unstable_push_in_context(cst as _, mcx);
        }
    }

    unsafe fn push_i64<'cx>(list: &mut List<'cx, *mut c_void>, mcx: &'cx MemCx<'_>, val: i64) {
        unsafe {
            let cst = pg_sys::makeConst(
                pg_sys::INT8OID,
                -1,
                pg_sys::InvalidOid,
                8,
                val.into_datum().unwrap(),
                false,
                true,
            );
            list.unstable_push_in_context(cst as _, mcx);
        }
    }

    unsafe fn push_bool<'cx>(list: &mut List<'cx, *mut c_void>, mcx: &'cx MemCx<'_>, val: bool) {
        unsafe {
            let cst = pg_sys::makeConst(
                pg_sys::BOOLOID,
                -1,
                pg_sys::InvalidOid,
                1,
                val.into_datum().unwrap(),
                false,
                true,
            );
            list.unstable_push_in_context(cst as _, mcx);
        }
    }

    unsafe fn push_text<'cx>(list: &mut List<'cx, *mut c_void>, mcx: &'cx MemCx<'_>, val: &str) {
        unsafe {
            let cst = pg_sys::makeConst(
                pg_sys::TEXTOID,
                -1,
                pg_sys::InvalidOid,
                -1,
                val.to_string().into_datum().unwrap(),
                false,
                false,
            );
            list.unstable_push_in_context(cst as _, mcx);
        }
    }

    unsafe fn push_oid<'cx>(list: &mut List<'cx, *mut c_void>, mcx: &'cx MemCx<'_>, val: Oid) {
        unsafe { Self::push_i32(list, mcx, val.to_u32() as i32) };
    }

    // Reads the raw `Const` at the current cursor position and advances the cursor.
    unsafe fn read_const(list: &List<*mut c_void>, idx: &mut usize) -> Option<pg_sys::Const> {
        let cst_ptr = *list.get(*idx)? as *mut pg_sys::Const;
        *idx += 1;
        Some(unsafe { *cst_ptr })
    }

    unsafe fn read_i32(list: &List<*mut c_void>, idx: &mut usize) -> Option<i32> {
        unsafe {
            let cst = Self::read_const(list, idx)?;
            i32::from_datum(cst.constvalue, cst.constisnull)
        }
    }

    unsafe fn read_i64(list: &List<*mut c_void>, idx: &mut usize) -> Option<i64> {
        unsafe {
            let cst = Self::read_const(list, idx)?;
            i64::from_datum(cst.constvalue, cst.constisnull)
        }
    }

    unsafe fn read_bool(list: &List<*mut c_void>, idx: &mut usize) -> Option<bool> {
        unsafe {
            let cst = Self::read_const(list, idx)?;
            bool::from_datum(cst.constvalue, cst.constisnull)
        }
    }

    unsafe fn read_text(list: &List<*mut c_void>, idx: &mut usize) -> Option<String> {
        unsafe {
            let cst = Self::read_const(list, idx)?;
            String::from_datum(cst.constvalue, cst.constisnull)
        }
    }

    unsafe fn read_oid(list: &List<*mut c_void>, idx: &mut usize) -> Option<Oid> {
        unsafe { Self::read_i32(list, idx) }.map(|v| Oid::from(v as u32))
    }

    unsafe fn push_column<'cx>(
        list: &mut List<'cx, *mut c_void>,
        mcx: &'cx MemCx<'_>,
        col: &Column,
    ) {
        unsafe {
            Self::push_text(list, mcx, &col.name);
            // usize to i32 cast is safe as Postgres has a maximum of 1600 columns
            Self::push_i32(list, mcx, col.num as i32);
            Self::push_oid(list, mcx, col.type_oid);
        }
    }

    unsafe fn read_column(list: &List<*mut c_void>, idx: &mut usize) -> Option<Column> {
        unsafe {
            let name = Self::read_text(list, idx)?;
            let num = Self::read_i32(list, idx)? as usize;
            let type_oid = Self::read_oid(list, idx)?;
            Some(Column {
                name,
                num,
                type_oid,
            })
        }
    }

    unsafe fn push_columns<'cx>(
        list: &mut List<'cx, *mut c_void>,
        mcx: &'cx MemCx<'_>,
        cols: &[Column],
    ) {
        unsafe {
            Self::push_i32(list, mcx, cols.len() as i32);
            for col in cols {
                Self::push_column(list, mcx, col);
            }
        }
    }

    unsafe fn read_columns(list: &List<*mut c_void>, idx: &mut usize) -> Option<Vec<Column>> {
        unsafe {
            let count = Self::read_i32(list, idx)? as usize;
            let mut cols = Vec::with_capacity(count);
            for _ in 0..count {
                cols.push(Self::read_column(list, idx)?);
            }
            Some(cols)
        }
    }

    unsafe fn push_sort<'cx>(list: &mut List<'cx, *mut c_void>, mcx: &'cx MemCx<'_>, sort: &Sort) {
        unsafe {
            Self::push_text(list, mcx, &sort.field);
            // usize to i32 cast is safe field_no is also bound by Postgres maximum number of columns(1600)
            Self::push_i32(list, mcx, sort.field_no as i32);
            Self::push_bool(list, mcx, sort.reversed);
            Self::push_bool(list, mcx, sort.nulls_first);
            Self::push_bool(list, mcx, sort.collate.is_some());
            if let Some(collate) = &sort.collate {
                Self::push_text(list, mcx, collate);
            }
        }
    }

    unsafe fn read_sort(list: &List<*mut c_void>, idx: &mut usize) -> Option<Sort> {
        unsafe {
            let field = Self::read_text(list, idx)?;
            let field_no = Self::read_i32(list, idx)? as usize;
            let reversed = Self::read_bool(list, idx)?;
            let nulls_first = Self::read_bool(list, idx)?;
            let has_collate = Self::read_bool(list, idx)?;
            let collate = if has_collate {
                Some(Self::read_text(list, idx)?)
            } else {
                None
            };

            Some(Sort {
                field,
                field_no,
                reversed,
                nulls_first,
                collate,
            })
        }
    }

    unsafe fn push_sorts<'cx>(
        list: &mut List<'cx, *mut c_void>,
        mcx: &'cx MemCx<'_>,
        sorts: &[Sort],
    ) {
        unsafe {
            Self::push_i32(list, mcx, sorts.len() as i32);
            for sort in sorts {
                Self::push_sort(list, mcx, sort);
            }
        }
    }

    unsafe fn read_sorts(list: &List<*mut c_void>, idx: &mut usize) -> Option<Vec<Sort>> {
        unsafe {
            let count = Self::read_i32(list, idx)? as usize;
            let mut sorts = Vec::with_capacity(count);
            for _ in 0..count {
                sorts.push(Self::read_sort(list, idx)?);
            }
            Some(sorts)
        }
    }

    unsafe fn push_limit<'cx>(
        list: &mut List<'cx, *mut c_void>,
        mcx: &'cx MemCx<'_>,
        limit: &Option<Limit>,
    ) {
        unsafe {
            Self::push_bool(list, mcx, limit.is_some());
            if let Some(limit) = limit {
                Self::push_i64(list, mcx, limit.count);
                Self::push_i64(list, mcx, limit.offset);
            }
        }
    }

    unsafe fn read_limit(list: &List<*mut c_void>, idx: &mut usize) -> Option<Option<Limit>> {
        unsafe {
            let has_limit = Self::read_bool(list, idx)?;
            if has_limit {
                let count = Self::read_i64(list, idx)?;
                let offset = Self::read_i64(list, idx)?;
                Some(Some(Limit { count, offset }))
            } else {
                Some(None)
            }
        }
    }

    fn aggregate_kind_to_i32(kind: AggregateKind) -> i32 {
        match kind {
            AggregateKind::Count => 0,
            AggregateKind::CountColumn => 1,
            AggregateKind::Sum => 2,
            AggregateKind::Avg => 3,
            AggregateKind::Min => 4,
            AggregateKind::Max => 5,
        }
    }

    fn aggregate_kind_from_i32(val: i32) -> Option<AggregateKind> {
        match val {
            0 => Some(AggregateKind::Count),
            1 => Some(AggregateKind::CountColumn),
            2 => Some(AggregateKind::Sum),
            3 => Some(AggregateKind::Avg),
            4 => Some(AggregateKind::Min),
            5 => Some(AggregateKind::Max),
            _ => None,
        }
    }

    unsafe fn push_aggregate<'cx>(
        list: &mut List<'cx, *mut c_void>,
        mcx: &'cx MemCx<'_>,
        agg: &Aggregate,
    ) {
        unsafe {
            Self::push_i32(list, mcx, Self::aggregate_kind_to_i32(agg.kind));
            Self::push_bool(list, mcx, agg.column.is_some());
            if let Some(col) = &agg.column {
                Self::push_column(list, mcx, col);
            }
            Self::push_bool(list, mcx, agg.distinct);
            Self::push_text(list, mcx, &agg.alias);
            Self::push_oid(list, mcx, agg.type_oid);
        }
    }

    unsafe fn read_aggregate(list: &List<*mut c_void>, idx: &mut usize) -> Option<Aggregate> {
        unsafe {
            let kind = Self::aggregate_kind_from_i32(Self::read_i32(list, idx)?)?;
            let has_column = Self::read_bool(list, idx)?;
            let column = if has_column {
                Some(Self::read_column(list, idx)?)
            } else {
                None
            };
            let distinct = Self::read_bool(list, idx)?;
            let alias = Self::read_text(list, idx)?;
            let type_oid = Self::read_oid(list, idx)?;
            Some(Aggregate {
                kind,
                column,
                distinct,
                alias,
                type_oid,
            })
        }
    }

    unsafe fn push_aggregates<'cx>(
        list: &mut List<'cx, *mut c_void>,
        mcx: &'cx MemCx<'_>,
        aggregates: &[Aggregate],
    ) {
        unsafe {
            Self::push_i32(list, mcx, aggregates.len() as i32);
            for agg in aggregates {
                Self::push_aggregate(list, mcx, agg);
            }
        }
    }

    unsafe fn read_aggregates(list: &List<*mut c_void>, idx: &mut usize) -> Option<Vec<Aggregate>> {
        unsafe {
            let count = Self::read_i32(list, idx)? as usize;
            let mut aggregates = Vec::with_capacity(count);
            for _ in 0..count {
                aggregates.push(Self::read_aggregate(list, idx)?);
            }
            Some(aggregates)
        }
    }

    unsafe fn push_qual<'cx>(list: &mut List<'cx, *mut c_void>, mcx: &'cx MemCx<'_>, qual: &Qual) {
        unsafe {
            Self::push_text(list, mcx, &qual.field);
            Self::push_text(list, mcx, &qual.operator);
            Self::push_bool(list, mcx, qual.use_or);

            match qual.value_const {
                Some(addr) => {
                    let mode = if matches!(qual.value, Value::Array(_)) {
                        QualValueMode::ArrayConst
                    } else {
                        QualValueMode::ScalarConst
                    };
                    Self::push_i32(list, mcx, mode as i32);
                    list.unstable_push_in_context(addr as *mut c_void, mcx);
                }
                None => match &qual.value {
                    Value::Cell(Cell::Bool(b)) => {
                        Self::push_i32(list, mcx, QualValueMode::Bool as i32);
                        Self::push_bool(list, mcx, *b);
                    }
                    _ => {
                        Self::push_i32(list, mcx, QualValueMode::Placeholder as i32);
                    }
                },
            }

            Self::push_param(list, mcx, &qual.param);
        }
    }

    unsafe fn push_param<'cx>(
        list: &mut List<'cx, *mut c_void>,
        mcx: &'cx MemCx<'_>,
        param: &Option<Param>,
    ) {
        unsafe {
            Self::push_bool(list, mcx, param.is_some());
            if let Some(param) = param {
                Self::push_i32(list, mcx, param.kind as i32);
                Self::push_i32(list, mcx, param.id as i32);
                Self::push_oid(list, mcx, param.type_oid);
            }
        }
    }

    unsafe fn push_quals<'cx>(
        list: &mut List<'cx, *mut c_void>,
        mcx: &'cx MemCx<'_>,
        quals: &[Qual],
    ) {
        unsafe {
            Self::push_i32(list, mcx, quals.len() as i32);
            for qual in quals {
                Self::push_qual(list, mcx, qual);
            }
        }
    }

    unsafe fn read_qual(list: &List<*mut c_void>, idx: &mut usize) -> Option<Qual> {
        unsafe {
            let field = Self::read_text(list, idx)?;
            let operator = Self::read_text(list, idx)?;
            let use_or = Self::read_bool(list, idx)?;

            let mode = QualValueMode::from_i32(Self::read_i32(list, idx)?)?;
            let value = match mode {
                QualValueMode::Bool => Value::Cell(Cell::Bool(Self::read_bool(list, idx)?)),
                QualValueMode::Placeholder => Value::Cell(Cell::String("null".to_string())),
                QualValueMode::ScalarConst => {
                    let cst = Self::read_const(list, idx)?;
                    Value::Cell(Cell::from_polymorphic_datum(
                        cst.constvalue,
                        cst.constisnull,
                        cst.consttype,
                    )?)
                }
                QualValueMode::ArrayConst => {
                    let cst = Self::read_const(list, idx)?;
                    Value::Array(form_array_from_datum(
                        cst.constvalue,
                        cst.constisnull,
                        cst.consttype,
                    )?)
                }
            };

            let param = Self::read_param(list, idx);

            Some(Qual {
                field,
                operator,
                value,
                use_or,
                param,
                value_const: None,
            })
        }
    }
    unsafe fn read_param(list: &List<*mut c_void>, idx: &mut usize) -> Option<Param> {
        unsafe {
            let has_param = Self::read_bool(list, idx)?;
            if has_param {
                let kind = Self::read_i32(list, idx)? as pg_sys::ParamKind::Type;
                let id = Self::read_i32(list, idx)? as usize;
                let type_oid = Self::read_oid(list, idx)?;
                Some(Param {
                    kind,
                    id,
                    type_oid,
                    eval_value: Mutex::new(None).into(),
                    expr_eval: ExprEval {
                        expr: ptr::null_mut(),
                        expr_state: ptr::null_mut(),
                    },
                })
            } else {
                None
            }
        }
    }

    unsafe fn read_quals(list: &List<*mut c_void>, idx: &mut usize) -> Option<Vec<Qual>> {
        unsafe {
            let count = Self::read_i32(list, idx)? as usize;
            let mut quals = Vec::with_capacity(count);
            for _ in 0..count {
                quals.push(Self::read_qual(list, idx)?);
            }
            Some(quals)
        }
    }
}

impl<E: Into<ErrorReport>, W: ForeignDataWrapper<E>> FdwState<E, W> {
    /// Deserialize [`FdwState`] from a [`FdwScanPrivate`] struct.
    unsafe fn from_scan_private(private: FdwScanPrivate, tmp_ctx: MemoryContext) -> Self {
        unsafe {
            let foreigntableid = private.foreigntableid;
            let instance = instance::create_fdw_instance_from_table_id(foreigntableid);

            let ftable = pg_sys::GetForeignTable(foreigntableid);
            let mut opts = options_to_hashmap((*ftable).options).report_unwrap();
            opts.insert(
                "wrappers.fserver_oid".into(),
                (*ftable).serverid.to_u32().to_string(),
            );
            opts.insert(
                "wrappers.ftable_oid".into(),
                (*ftable).relid.to_u32().to_string(),
            );

            let mut quals = private.quals;

            // Reallocate the `pg_sys::ParamKind::PARAM_EXEC` node in the `tmp_ctx`
            // memory context.
            PgMemoryContexts::For(tmp_ctx).switch_to(|_| {
                for qual in &mut quals {
                    if let Some(param) = &mut qual.param
                        && param.kind == pg_sys::ParamKind::PARAM_EXEC
                    {
                        let mut node = PgBox::<pg_sys::Param>::alloc_node(pg_sys::NodeTag::T_Param);
                        node.paramkind = param.kind;
                        node.paramid = param.id as _;
                        node.paramtype = param.type_oid;
                        node.paramtypmod = -1;
                        node.paramcollid = pg_sys::InvalidOid;
                        node.location = -1;
                        param.expr_eval.expr = node.into_pg() as _;
                    }
                }
            });

            Self {
                foreigntableid,
                instance: Some(instance),
                quals,
                tgts: private.tgts,
                sorts: private.sorts,
                limit: private.limit,
                opts,
                aggregates: private.aggregates,
                group_by: private.group_by,
                tmp_ctx,
                values: Vec::new(),
                nulls: Vec::new(),
                row: Row::new(),
                param_fingerprint: String::new(),
                _phantom: PhantomData,
            }
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
            state.quals = extract_quals(baserel, foreigntableid);

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

        // save the state for following callbacks
        (*baserel).fdw_private = Box::leak(Box::new(state)) as *mut FdwState<E, W> as _;
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
    // Not `state.foreigntableid`'s source: unreliable (`InvalidOid`) for
    // aggregate-pushdown (upper-rel) plans — see the comment below.
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

        // It is critical that the data we pass in `fdw_private` be deep copyable
        // via a Postgres `copyObject` call. Since `get_foreign_plan` is the last
        // callback of the plan phase, Postgres needs to potentially be able to
        // cache the plan and run the scan phase repeatedly using this cached plan.
        // When Postgres runs the scan phase it calls `copyObject` on the plan (including
        // `fdw_private`) before passing it to the scan phase's `begin_foreign_scan`
        // callback where this state will be reconstituted.
        // Use `state.foreigntableid` (captured for the base rel during
        // `get_foreign_rel_size`), not this callback's own `foreigntableid`
        // parameter: for an aggregate-pushdown plan, `baserel` here is the
        // upper (GROUP_AGG) relation and Postgres passes `InvalidOid` for it.
        let private = FdwScanPrivate {
            foreigntableid: state.foreigntableid,
            quals: mem::take(&mut state.quals),
            tgts: mem::take(&mut state.tgts),
            sorts: mem::take(&mut state.sorts),
            limit: state.limit.take(),
            aggregates: mem::take(&mut state.aggregates),
            group_by: mem::take(&mut state.group_by),
        };
        let fdw_private = private.serialize_to_list();

        // Drop the state struct because its values have been serialized into
        // `fdw_private` and it is no longer needed.
        drop_fdw_state(state.as_ptr());

        pg_sys::make_foreignscan(
            final_tlist,
            scan_clauses,
            (*baserel).relid,
            ptr::null_mut(),
            fdw_private as _,
            agg_fdw_scan_tlist,
            ptr::null_mut(),
            outer_plan,
        )
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
                let mut current_value: Option<Value> = None;
                match param.kind {
                    ParamKind::PARAM_EXTERN => {
                        // get parameter list in execution state
                        let plist_info = (*estate).es_param_list_info;
                        if !plist_info.is_null() {
                            let params_cnt = (*plist_info).numParams as usize;
                            if param.id > 0 && param.id <= params_cnt {
                                let plist = (*plist_info).params.as_slice(params_cnt);
                                let p: pg_sys::ParamExternData = plist[param.id - 1];
                                if let Some(cell) =
                                    Cell::from_polymorphic_datum(p.value, p.isnull, p.ptype)
                                {
                                    qual.value = Value::Cell(cell.clone());
                                    current_value = Some(Value::Cell(cell));
                                }
                            }
                        }
                    }
                    ParamKind::PARAM_EXEC => {
                        // evaluate parameter value
                        param.expr_eval.expr_state = pg_sys::ExecInitExpr(
                            param.expr_eval.expr,
                            node as *mut pg_sys::PlanState,
                        );
                        let mut isnull = false;
                        if let Some(datum) = polyfill::exec_eval_expr(
                            param.expr_eval.expr_state,
                            econtext,
                            &mut isnull,
                        ) && let Some(cell) =
                            Cell::from_polymorphic_datum(datum, isnull, param.type_oid)
                        {
                            qual.value = Value::Cell(cell.clone());
                            current_value = Some(Value::Cell(cell));
                        }
                    }
                    _ => {}
                }

                let mut eval_value = param
                    .eval_value
                    .lock()
                    .expect("param.eval_value should be locked");
                *eval_value = current_value;
            }
        }
    }
}

fn compute_param_fingerprint<E: Into<ErrorReport>, W: ForeignDataWrapper<E>>(
    state: &FdwState<E, W>,
) -> String {
    state
        .quals
        .iter()
        .filter_map(|qual| {
            qual.param.as_ref().map(|param| {
                let eval_value = match param.eval_value.lock() {
                    Ok(value) => format!("{:?}", *value),
                    Err(_) => "lock_error".to_string(),
                };
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
        })
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

        let Some(private) = FdwScanPrivate::deserialize_from_list((*plan).fdw_private as _) else {
            report_error(
                PgSqlErrorCode::ERRCODE_FDW_ERROR,
                "invalid fdw_private data in begin_foreign_scan",
            );
            return;
        };

        // Rebuild the scan state again from the serialized `FdwScanPrivate` afresh each time
        // `begin_foreign_scan` is called to avoid state struct lifetime issues. The plan phase
        // might have cached the plan, so we create a fresh copy in the scan phase.
        let foreigntableid = private.foreigntableid;
        let ctx_name = format!("Wrappers_scan_{}", foreigntableid.to_u32());
        let tmp_ctx = memctx::create_wrappers_memctx(&ctx_name);
        let mut state = FdwState::<E, W>::from_scan_private(private, tmp_ctx);

        // assign parameter values to qual
        assign_parameter_value(node, &mut state);
        state.param_fingerprint = compute_param_fingerprint(&state);

        // begin scan if it is not EXPLAIN statement
        if eflags & pg_sys::EXEC_FLAG_EXPLAIN_ONLY as c_int <= 0 {
            // choose aggregate scan or normal scan based on state
            let result = if state.is_aggregate_scan() {
                state.begin_aggregate_scan()
            } else {
                state.begin_scan()
            };
            result.report_unwrap();

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

        // This is leaked here but dropped in `end_foreign_scan`
        (*node).fdw_state = Box::leak(Box::new(state)) as *mut FdwState<E, W> as _;
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
        if result.is_err() {
            drop_fdw_state(state.as_ptr());
            (*node).fdw_state = ptr::null::<FdwState<E, W>>() as _;
        }
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
                state.begin_scan()
            } else {
                state.re_scan()
            };
            if result.is_err() {
                drop_fdw_state(state.as_ptr());
                (*node).fdw_state = ptr::null::<FdwState<E, W>>() as _;
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

        // the scan state is actually not allocated by PG, but we use 'from_pg()'
        // here just to tell PgBox don't free the state, instead we will handle
        // drop the state by ourselves
        let mut state = PgBox::<FdwState<E, W>>::from_pg(fdw_state);
        let result = state.end_scan();
        drop_fdw_state(state.as_ptr());
        (*node).fdw_state = ptr::null::<FdwState<E, W>>() as _;

        result.report_unwrap();
    }
}
