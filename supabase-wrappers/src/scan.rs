use pgrx::FromDatum;
use pgrx::{
    IntoDatum, PgSqlErrorCode, debug2,
    list::List,
    memcxt::PgMemoryContexts,
    pg_sys::{Datum, MemoryContext, MemoryContextData, Oid, ParamKind},
    prelude::*,
};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::marker::PhantomData;

use pgrx::pg_sys::panic::ErrorReport;
use std::ffi::{CStr, c_void};
use std::os::raw::c_int;
use std::ptr;

use crate::instance;
use crate::interface::{
    Aggregate, Cell, Column, FullQuery, FullQueryRelation, Limit, Qual, RemoteQueryContext,
    RemoteQueryParameter, RemoteQueryPolicy, Row, Sort, Value,
};
use crate::limit::*;
use crate::memctx;
use crate::options::options_to_hashmap;
use crate::polyfill;
use crate::prelude::ForeignDataWrapper;
use crate::qual::*;
use crate::sort::*;
use crate::utils::{self, ReportableError, SerdeList, report_error};

pub(crate) const FULL_QUERY_PARTIAL_PATH_PENALTY: f64 = 1_000_000_000.0;
pub(crate) const REQUIRED_REMOTE_QUERY_LOCAL_PATH_PENALTY: f64 = 1_000_000_000_000.0;

// Fdw private state for scan
pub(crate) struct FdwState<E: Into<ErrorReport>, W: ForeignDataWrapper<E>> {
    // foreign table oid used to create fresh execution instances from a cached plan
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

    // full-query pushdown
    pub(crate) full_query: Option<FullQuery>,
    pub(crate) requires_full_query: bool,
    pub(crate) full_query_upper_only: bool,
    pub(crate) full_query_executable: bool,
    pub(crate) remote_query_policy: RemoteQueryPolicy,

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
    pub(crate) unsafe fn new(foreigntableid: Oid, tmp_ctx: MemoryContext) -> Self {
        Self {
            foreigntableid,
            instance: Some(unsafe { instance::create_fdw_instance_from_table_id(foreigntableid) }),
            quals: Vec::new(),
            tgts: Vec::new(),
            sorts: Vec::new(),
            limit: None,
            opts: HashMap::new(),
            aggregates: Vec::new(),
            group_by: Vec::new(),
            full_query: None,
            requires_full_query: false,
            full_query_upper_only: false,
            full_query_executable: true,
            remote_query_policy: RemoteQueryPolicy::Optional,
            tmp_ctx,
            values: Vec::new(),
            nulls: Vec::new(),
            row: Row::new(),
            param_fingerprint: String::new(),
            _phantom: PhantomData,
        }
    }

    pub(crate) unsafe fn clone_for_execution(&self) -> Self {
        unsafe {
            let ctx_name = format!(
                "Wrappers_scan_exec_{}_{}",
                self.foreigntableid.to_u32(),
                self as *const _ as usize
            );
            let ctx = memctx::create_wrappers_memctx(&ctx_name);
            let mut state = Self::new(self.foreigntableid, ctx);
            state.quals = self.quals.clone();
            state.tgts = self.tgts.clone();
            state.sorts = self.sorts.clone();
            state.limit = self.limit.clone();
            state.opts = self.opts.clone();
            state.aggregates = self.aggregates.clone();
            state.group_by = self.group_by.clone();
            state.full_query = self.full_query.clone();
            state.requires_full_query = self.requires_full_query;
            state.full_query_upper_only = self.full_query_upper_only;
            state.full_query_executable = self.full_query_executable;
            state.remote_query_policy = self.remote_query_policy;
            state
        }
    }

    pub(crate) unsafe fn clone_for_plan_template(&self) -> Self {
        unsafe {
            let ctx_name = format!(
                "Wrappers_scan_plan_{}_{}",
                self.foreigntableid.to_u32(),
                self as *const _ as usize
            );
            let ctx = memctx::create_wrappers_memctx(&ctx_name);
            Self {
                foreigntableid: self.foreigntableid,
                instance: None,
                quals: self.quals.clone(),
                tgts: self.tgts.clone(),
                sorts: self.sorts.clone(),
                limit: self.limit.clone(),
                opts: self.opts.clone(),
                aggregates: self.aggregates.clone(),
                group_by: self.group_by.clone(),
                full_query: self.full_query.clone(),
                requires_full_query: self.requires_full_query,
                full_query_upper_only: self.full_query_upper_only,
                full_query_executable: self.full_query_executable,
                remote_query_policy: self.remote_query_policy,
                tmp_ctx: ctx,
                values: Vec::new(),
                nulls: Vec::new(),
                row: Row::new(),
                param_fingerprint: String::new(),
                _phantom: PhantomData,
            }
        }
    }

    #[inline]
    fn begin_current_scan(&mut self) -> Result<(), E> {
        if self.is_full_query_scan() {
            self.begin_full_query_scan()
        } else if self.is_aggregate_scan() {
            self.begin_aggregate_scan()
        } else {
            self.begin_scan()
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
    pub(crate) fn is_full_query_scan(&self) -> bool {
        self.full_query.is_some()
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
    fn begin_full_query_scan(&mut self) -> Result<(), E> {
        if let (Some(instance), Some(full_query)) =
            (self.instance.as_mut(), self.full_query.as_ref())
        {
            instance.begin_remote_query(full_query, &self.opts)
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

impl<E: Into<ErrorReport>, W: ForeignDataWrapper<E>> utils::SerdeList for FdwState<E, W> {}

impl<E: Into<ErrorReport>, W: ForeignDataWrapper<E>> Drop for FdwState<E, W> {
    fn drop(&mut self) {
        // drop foreign data wrapper instance
        self.instance.take();

        // remove the allocated memory context
        unsafe {
            if !self.tmp_ctx.is_null() {
                memctx::delete_wrappers_memctx(self.tmp_ctx);
                self.tmp_ctx = ptr::null::<MemoryContextData>() as _;
            }
        }
    }
}

// drop the scan state, so the inner fdw instance can be dropped too
unsafe fn drop_fdw_state<E: Into<ErrorReport>, W: ForeignDataWrapper<E>>(
    fdw_state: *mut FdwState<E, W>,
) {
    if fdw_state.is_null() {
        return;
    }
    let boxed_fdw_state = unsafe { Box::from_raw(fdw_state) };
    drop(boxed_fdw_state);
}

pub(crate) unsafe fn leak_state_in_current_context<
    E: Into<ErrorReport>,
    W: ForeignDataWrapper<E>,
>(
    state: FdwState<E, W>,
) -> *mut FdwState<E, W> {
    let mut context = PgMemoryContexts::CurrentMemoryContext;
    context.leak_and_drop_on_delete(state)
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct PlanStateSnapshot {
    foreigntableid: u32,
    tgts: Vec<PlanColumnSnapshot>,
    opts: HashMap<String, String>,
    full_query: PlanFullQuerySnapshot,
    requires_full_query: bool,
    full_query_upper_only: bool,
    full_query_executable: bool,
    remote_query_policy: PlanRemoteQueryPolicySnapshot,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct PlanColumnSnapshot {
    name: String,
    num: usize,
    type_oid: u32,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct PlanFullQuerySnapshot {
    sql: String,
    columns: Vec<PlanColumnSnapshot>,
    relations: Vec<PlanFullQueryRelationSnapshot>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct PlanFullQueryRelationSnapshot {
    relid: u32,
    server_oid: u32,
    local_schema: String,
    local_table: String,
    options: HashMap<String, String>,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
enum PlanRemoteQueryPolicySnapshot {
    Optional,
    Prefer,
    Require,
}

impl From<&Column> for PlanColumnSnapshot {
    fn from(column: &Column) -> Self {
        Self {
            name: column.name.clone(),
            num: column.num,
            type_oid: column.type_oid.to_u32(),
        }
    }
}

impl From<PlanColumnSnapshot> for Column {
    fn from(column: PlanColumnSnapshot) -> Self {
        Self {
            name: column.name,
            num: column.num,
            type_oid: Oid::from(column.type_oid),
        }
    }
}

impl From<&FullQueryRelation> for PlanFullQueryRelationSnapshot {
    fn from(relation: &FullQueryRelation) -> Self {
        Self {
            relid: relation.relid.to_u32(),
            server_oid: relation.server_oid.to_u32(),
            local_schema: relation.local_schema.clone(),
            local_table: relation.local_table.clone(),
            options: relation.options.clone(),
        }
    }
}

impl From<PlanFullQueryRelationSnapshot> for FullQueryRelation {
    fn from(relation: PlanFullQueryRelationSnapshot) -> Self {
        Self {
            relid: Oid::from(relation.relid),
            server_oid: Oid::from(relation.server_oid),
            local_schema: relation.local_schema,
            local_table: relation.local_table,
            options: relation.options,
        }
    }
}

impl From<&FullQuery> for PlanFullQuerySnapshot {
    fn from(query: &FullQuery) -> Self {
        Self {
            sql: query.sql.clone(),
            columns: query.columns.iter().map(PlanColumnSnapshot::from).collect(),
            relations: query
                .relations
                .iter()
                .map(PlanFullQueryRelationSnapshot::from)
                .collect(),
        }
    }
}

impl From<PlanFullQuerySnapshot> for FullQuery {
    fn from(query: PlanFullQuerySnapshot) -> Self {
        Self {
            sql: query.sql,
            columns: query.columns.into_iter().map(Column::from).collect(),
            relations: query
                .relations
                .into_iter()
                .map(FullQueryRelation::from)
                .collect(),
            parameters: Vec::new(),
        }
    }
}

impl From<RemoteQueryPolicy> for PlanRemoteQueryPolicySnapshot {
    fn from(policy: RemoteQueryPolicy) -> Self {
        match policy {
            RemoteQueryPolicy::Optional => Self::Optional,
            RemoteQueryPolicy::Prefer => Self::Prefer,
            RemoteQueryPolicy::Require => Self::Require,
        }
    }
}

impl From<PlanRemoteQueryPolicySnapshot> for RemoteQueryPolicy {
    fn from(policy: PlanRemoteQueryPolicySnapshot) -> Self {
        match policy {
            PlanRemoteQueryPolicySnapshot::Optional => Self::Optional,
            PlanRemoteQueryPolicySnapshot::Prefer => Self::Prefer,
            PlanRemoteQueryPolicySnapshot::Require => Self::Require,
        }
    }
}

impl PlanStateSnapshot {
    fn from_state<E: Into<ErrorReport>, W: ForeignDataWrapper<E>>(
        state: &FdwState<E, W>,
    ) -> Option<Self> {
        let full_query = state.full_query.as_ref()?;
        Some(Self {
            foreigntableid: state.foreigntableid.to_u32(),
            tgts: state.tgts.iter().map(PlanColumnSnapshot::from).collect(),
            opts: state.opts.clone(),
            full_query: PlanFullQuerySnapshot::from(full_query),
            requires_full_query: state.requires_full_query,
            full_query_upper_only: state.full_query_upper_only,
            full_query_executable: state.full_query_executable,
            remote_query_policy: PlanRemoteQueryPolicySnapshot::from(state.remote_query_policy),
        })
    }

    unsafe fn into_state<E: Into<ErrorReport>, W: ForeignDataWrapper<E>>(self) -> FdwState<E, W> {
        unsafe {
            let ctx_name = format!("Wrappers_scan_full_query_exec_{}", self.foreigntableid);
            let ctx = memctx::create_wrappers_memctx(&ctx_name);
            let mut state = FdwState::<E, W>::new(Oid::from(self.foreigntableid), ctx);
            state.tgts = self.tgts.into_iter().map(Column::from).collect();
            state.opts = self.opts;
            state.full_query = Some(FullQuery::from(self.full_query));
            state.requires_full_query = self.requires_full_query;
            state.full_query_upper_only = self.full_query_upper_only;
            state.full_query_executable = self.full_query_executable;
            state.remote_query_policy = RemoteQueryPolicy::from(self.remote_query_policy);
            state
        }
    }
}

unsafe fn serialize_plan_template<E: Into<ErrorReport>, W: ForeignDataWrapper<E>>(
    state: &FdwState<E, W>,
) -> *mut pg_sys::List {
    unsafe {
        if let Some(snapshot) = PlanStateSnapshot::from_state(state) {
            return serialize_plan_snapshot(&snapshot);
        }
        let plan_template = state.clone_for_plan_template();
        let plan_template = leak_state_in_current_context(plan_template);
        FdwState::<E, W>::serialize_to_list(PgBox::from_pg(plan_template))
    }
}

unsafe fn serialize_plan_snapshot(snapshot: &PlanStateSnapshot) -> *mut pg_sys::List {
    unsafe {
        let json = serde_json::to_string(snapshot).unwrap_or_else(|error| {
            report_error(
                PgSqlErrorCode::ERRCODE_FDW_ERROR,
                &format!("failed to serialize FDW full-query plan state: {error}"),
            );
            unreachable!("report_error(ERROR) should not return")
        });
        pgrx::memcx::current_context(|mcx| {
            let mut list = List::<*mut c_void>::Nil;
            let datum = json.into_datum().unwrap_or_else(|| {
                report_error(
                    PgSqlErrorCode::ERRCODE_FDW_ERROR,
                    "failed to convert FDW full-query plan state into PostgreSQL text",
                );
                unreachable!("report_error(ERROR) should not return")
            });
            let cst: *mut pg_sys::Const = pg_sys::makeConst(
                pg_sys::TEXTOID,
                -1,
                pg_sys::DEFAULT_COLLATION_OID,
                -1,
                datum,
                false,
                false,
            );
            list.unstable_push_in_context(cst as _, mcx);
            list.into_ptr()
        })
    }
}

unsafe fn deserialize_plan_snapshot<E: Into<ErrorReport>, W: ForeignDataWrapper<E>>(
    list: *mut pg_sys::List,
) -> Option<FdwState<E, W>> {
    unsafe {
        pgrx::memcx::current_context(|mcx| {
            let list = List::<*mut c_void>::downcast_ptr_in_memcx(list, mcx)?;
            let cst = list.get(0)?;
            let cst = *(*cst as *mut pg_sys::Const);
            if cst.consttype != pg_sys::TEXTOID {
                return None;
            }
            let json = String::from_datum(cst.constvalue, cst.constisnull)?;
            let snapshot = serde_json::from_str::<PlanStateSnapshot>(&json).ok()?;
            Some(snapshot.into_state())
        })
    }
}

pub(crate) unsafe fn full_query_placeholder_from_planner(
    root: *mut pg_sys::PlannerInfo,
    columns: Vec<Column>,
) -> Option<FullQuery> {
    unsafe {
        let (_, relations) = query_foreign_relations(root)?;
        Some(FullQuery {
            sql: String::new(),
            columns,
            relations,
            parameters: Vec::new(),
        })
    }
}

unsafe fn full_query_sql_from_planner(root: *mut pg_sys::PlannerInfo) -> Option<String> {
    unsafe {
        if root.is_null() || (*root).parse.is_null() {
            return None;
        }

        if query_has_multiple_base_relations(root) || query_has_non_relation_inputs(root) {
            return current_statement_sql_from_debug_query_string(root);
        }

        let sql = pg_sys::pg_get_querydef((*root).parse, false);
        if sql.is_null() {
            return current_statement_sql_from_debug_query_string(root);
        }

        Some(CStr::from_ptr(sql).to_string_lossy().into_owned())
    }
}

unsafe fn current_statement_sql_from_debug_query_string(
    root: *mut pg_sys::PlannerInfo,
) -> Option<String> {
    unsafe {
        if root.is_null() || (*root).parse.is_null() || pg_sys::debug_query_string.is_null() {
            return None;
        }

        let raw = CStr::from_ptr(pg_sys::debug_query_string)
            .to_string_lossy()
            .into_owned();
        let query = (*root).parse;
        let start = if (*query).stmt_location > 0 {
            (*query).stmt_location as usize
        } else {
            0
        };
        let bytes = raw.as_bytes();
        if start >= bytes.len() {
            return normalize_current_statement_sql(&raw);
        }

        let len = if (*query).stmt_len > 0 {
            (*query).stmt_len as usize
        } else {
            bytes.len().saturating_sub(start)
        };
        let end = start.saturating_add(len).min(bytes.len());
        let stmt = std::str::from_utf8(&bytes[start..end]).ok()?;
        normalize_current_statement_sql(stmt)
    }
}

fn normalize_current_statement_sql(sql: &str) -> Option<String> {
    let mut sql = sql.trim().trim_end_matches(';').trim();
    if sql.is_empty() {
        return None;
    }

    if starts_with_ascii_keyword(sql, "prepare") {
        let lower = sql.to_ascii_lowercase();
        if let Some(as_offset) = lower.find(" as ") {
            sql = sql[as_offset + 4..].trim();
        }
    }

    if starts_with_ascii_keyword(sql, "explain") {
        let lower = sql.to_ascii_lowercase();
        let select_offset = find_ascii_keyword(&lower, "select");
        let with_offset = find_ascii_keyword(&lower, "with");
        let offset = match (select_offset, with_offset) {
            (Some(select_offset), Some(with_offset)) => Some(select_offset.min(with_offset)),
            (Some(select_offset), None) => Some(select_offset),
            (None, Some(with_offset)) => Some(with_offset),
            (None, None) => None,
        }?;
        sql = sql[offset..].trim();
    }

    let sql = sql.trim().trim_end_matches(';').trim();
    if sql.is_empty() {
        None
    } else {
        Some(sql.to_string())
    }
}

fn starts_with_ascii_keyword(sql: &str, keyword: &str) -> bool {
    let Some(prefix) = sql.get(..keyword.len()) else {
        return false;
    };
    prefix.eq_ignore_ascii_case(keyword)
        && sql
            .as_bytes()
            .get(keyword.len())
            .is_none_or(|byte| !byte.is_ascii_alphanumeric() && *byte != b'_')
}

fn find_ascii_keyword(sql_lowercase: &str, keyword: &str) -> Option<usize> {
    let mut offset = 0usize;
    while let Some(relative) = sql_lowercase[offset..].find(keyword) {
        let found = offset + relative;
        let before_is_boundary = found == 0
            || sql_lowercase
                .as_bytes()
                .get(found - 1)
                .is_none_or(|byte| !byte.is_ascii_alphanumeric() && *byte != b'_');
        let after = found + keyword.len();
        let after_is_boundary = sql_lowercase
            .as_bytes()
            .get(after)
            .is_none_or(|byte| !byte.is_ascii_alphanumeric() && *byte != b'_');
        if before_is_boundary && after_is_boundary {
            return Some(found);
        }
        offset = after;
    }
    None
}

pub(crate) unsafe fn target_columns_from_reltarget(rel: *mut pg_sys::RelOptInfo) -> Vec<Column> {
    unsafe {
        if rel.is_null() || (*rel).reltarget.is_null() {
            return Vec::new();
        }

        let exprs = (*(*rel).reltarget).exprs;
        let len = if exprs.is_null() {
            0
        } else {
            (*exprs).length as usize
        };
        let mut columns = Vec::new();

        for i in 0..len {
            let cell = (*exprs).elements.add(i);
            let expr = (*cell).ptr_value as *mut pg_sys::Node;
            let type_oid = if expr.is_null() {
                pg_sys::InvalidOid
            } else {
                pg_sys::exprType(expr)
            };
            columns.push(Column {
                name: format!("column_{}", i + 1),
                num: i + 1,
                type_oid,
            });
        }

        columns
    }
}

pub(crate) unsafe fn target_columns_from_tlist(tlist: *mut pg_sys::List) -> Vec<Column> {
    unsafe {
        let len = if tlist.is_null() {
            0
        } else {
            (*tlist).length as usize
        };
        let mut columns = Vec::new();
        let mut output_num = 1usize;

        for i in 0..len {
            let cell = (*tlist).elements.add(i);
            let tle = (*cell).ptr_value as *mut pg_sys::TargetEntry;
            if tle.is_null() || (*tle).resjunk {
                continue;
            }

            let name = if !(*tle).resname.is_null() {
                CStr::from_ptr((*tle).resname)
                    .to_string_lossy()
                    .into_owned()
            } else {
                format!("column_{}", output_num)
            };
            let type_oid = if (*tle).expr.is_null() {
                pg_sys::InvalidOid
            } else {
                pg_sys::exprType((*tle).expr as *const pg_sys::Node)
            };

            columns.push(Column {
                name,
                num: output_num,
                type_oid,
            });
            output_num += 1;
        }

        columns
    }
}

unsafe fn list_is_empty(list: *mut pg_sys::List) -> bool {
    unsafe { list.is_null() || (*list).length == 0 }
}

unsafe fn query_foreign_relations(
    root: *mut pg_sys::PlannerInfo,
) -> Option<(*mut pg_sys::Bitmapset, Vec<FullQueryRelation>)> {
    unsafe {
        if root.is_null() || (*root).parse.is_null() {
            return None;
        }

        let rtable = (*(*root).parse).rtable;
        let len = if rtable.is_null() {
            0
        } else {
            (*rtable).length as usize
        };
        let mut relids: *mut pg_sys::Bitmapset = ptr::null_mut();
        let mut relations = Vec::new();

        for i in 0..len {
            let cell = (*rtable).elements.add(i);
            let rte = (*cell).ptr_value as *mut pg_sys::RangeTblEntry;
            if rte.is_null() {
                continue;
            }

            match (*rte).rtekind {
                pg_sys::RTEKind::RTE_RELATION => {
                    if (*rte).relkind as u8 != pg_sys::RELKIND_FOREIGN_TABLE {
                        return None;
                    }

                    relids = pg_sys::bms_add_member(relids, (i + 1) as c_int);
                    relations.push(foreign_relation_from_rte(rte)?);
                }
                pg_sys::RTEKind::RTE_JOIN => {}
                _ => return None,
            }
        }

        if relations.is_empty() {
            None
        } else {
            Some((relids, relations))
        }
    }
}

unsafe fn foreign_relation_from_rte(rte: *mut pg_sys::RangeTblEntry) -> Option<FullQueryRelation> {
    unsafe {
        let relid = (*rte).relid;
        let ftable = pg_sys::GetForeignTable(relid);
        if ftable.is_null() {
            return None;
        }

        let local_table = pg_sys::get_rel_name(relid);
        if local_table.is_null() {
            return None;
        }

        let namespace = pg_sys::get_rel_namespace(relid);
        let local_schema = pg_sys::get_namespace_name(namespace);
        if local_schema.is_null() {
            return None;
        }

        Some(FullQueryRelation {
            relid,
            server_oid: (*ftable).serverid,
            local_schema: CStr::from_ptr(local_schema).to_string_lossy().into_owned(),
            local_table: CStr::from_ptr(local_table).to_string_lossy().into_owned(),
            options: options_to_hashmap((*ftable).options).report_unwrap(),
        })
    }
}

pub(crate) unsafe fn query_has_upper_operations(root: *mut pg_sys::PlannerInfo) -> bool {
    unsafe {
        if root.is_null() || (*root).parse.is_null() {
            return false;
        }

        let query = (*root).parse;
        (*query).hasAggs
            || (*query).hasWindowFuncs
            || !(*query).groupClause.is_null()
            || !(*query).havingQual.is_null()
            || !(*query).distinctClause.is_null()
            || !(*query).sortClause.is_null()
            || !(*query).limitOffset.is_null()
            || !(*query).limitCount.is_null()
            || !(*query).setOperations.is_null()
    }
}

pub(crate) unsafe fn query_requires_full_query(root: *mut pg_sys::PlannerInfo) -> bool {
    unsafe {
        query_has_upper_operations(root)
            || query_has_multiple_base_relations(root)
            || query_has_non_relation_inputs(root)
            || query_has_non_var_targets(root)
    }
}

pub(crate) unsafe fn remote_query_context_from_planner(
    root: *mut pg_sys::PlannerInfo,
    has_unpushed_quals: bool,
) -> RemoteQueryContext {
    unsafe {
        let foreign_relations = query_foreign_relations(root).map(|(_, relations)| relations);
        RemoteQueryContext {
            has_upper_operations: query_has_upper_operations(root),
            has_multiple_base_relations: query_has_multiple_base_relations(root),
            has_non_relation_inputs: query_has_non_relation_inputs(root),
            has_non_var_targets: query_has_non_var_targets(root),
            has_unpushed_quals,
            all_referenced_relations_are_foreign: foreign_relations.is_some(),
            foreign_relation_count: foreign_relations.as_ref().map_or(0, Vec::len),
        }
    }
}

pub(crate) fn remote_query_local_path_penalty(policy: RemoteQueryPolicy) -> f64 {
    match policy {
        RemoteQueryPolicy::Optional => 0.0,
        RemoteQueryPolicy::Prefer => FULL_QUERY_PARTIAL_PATH_PENALTY,
        RemoteQueryPolicy::Require => REQUIRED_REMOTE_QUERY_LOCAL_PATH_PENALTY,
    }
}

unsafe fn baserel_restriction_count(baserel: *mut pg_sys::RelOptInfo) -> usize {
    unsafe {
        if baserel.is_null() || (*baserel).baserestrictinfo.is_null() {
            0
        } else {
            (*(*baserel).baserestrictinfo).length as usize
        }
    }
}

unsafe fn query_has_multiple_base_relations(root: *mut pg_sys::PlannerInfo) -> bool {
    unsafe {
        if root.is_null() || (*root).parse.is_null() {
            return false;
        }

        let rtable = (*(*root).parse).rtable;
        let len = if rtable.is_null() {
            0
        } else {
            (*rtable).length as usize
        };
        let mut relation_count = 0usize;

        for i in 0..len {
            let cell = (*rtable).elements.add(i);
            let rte = (*cell).ptr_value as *mut pg_sys::RangeTblEntry;
            if rte.is_null() {
                continue;
            }

            if (*rte).rtekind == pg_sys::RTEKind::RTE_RELATION {
                relation_count += 1;
                if relation_count > 1 {
                    return true;
                }
            }
        }

        false
    }
}

unsafe fn query_has_non_relation_inputs(root: *mut pg_sys::PlannerInfo) -> bool {
    unsafe {
        if root.is_null() || (*root).parse.is_null() {
            return false;
        }

        let rtable = (*(*root).parse).rtable;
        let len = if rtable.is_null() {
            0
        } else {
            (*rtable).length as usize
        };

        for i in 0..len {
            let cell = (*rtable).elements.add(i);
            let rte = (*cell).ptr_value as *mut pg_sys::RangeTblEntry;
            if rte.is_null() {
                continue;
            }

            match (*rte).rtekind {
                pg_sys::RTEKind::RTE_RELATION | pg_sys::RTEKind::RTE_JOIN => {}
                _ => return true,
            }
        }

        false
    }
}

unsafe fn query_has_non_var_targets(root: *mut pg_sys::PlannerInfo) -> bool {
    unsafe {
        if root.is_null() || (*root).parse.is_null() {
            return false;
        }

        let target_list = (*(*root).parse).targetList;
        let len = if target_list.is_null() {
            0
        } else {
            (*target_list).length as usize
        };

        for i in 0..len {
            let cell = (*target_list).elements.add(i);
            let tle = (*cell).ptr_value as *mut pg_sys::TargetEntry;
            if tle.is_null() || (*tle).resjunk {
                continue;
            }

            let expr = (*tle).expr as *mut pg_sys::Node;
            if expr.is_null() {
                continue;
            }

            if (*expr).type_ != pg_sys::NodeTag::T_Var {
                return true;
            }
        }

        false
    }
}

#[pg_guard]
pub(super) extern "C-unwind" fn get_foreign_join_paths<
    E: Into<ErrorReport>,
    W: ForeignDataWrapper<E>,
>(
    root: *mut pg_sys::PlannerInfo,
    joinrel: *mut pg_sys::RelOptInfo,
    _outerrel: *mut pg_sys::RelOptInfo,
    _innerrel: *mut pg_sys::RelOptInfo,
    _jointype: pg_sys::JoinType::Type,
    _extra: *mut pg_sys::JoinPathExtraData,
) {
    debug2!("---> get_foreign_join_paths");
    unsafe {
        let Some((query_relids, relations)) = query_foreign_relations(root) else {
            debug2!("get_foreign_join_paths: query has non-foreign inputs");
            return;
        };
        debug2!("get_foreign_join_paths: relations={}", relations.len());
        if joinrel.is_null() || !pg_sys::bms_equal((*joinrel).relids, query_relids) {
            debug2!("get_foreign_join_paths: joinrel does not cover full foreign query");
            return;
        }
        debug2!("get_foreign_join_paths: joinrel covers full foreign query");

        let Some(first_relation) = relations.first() else {
            debug2!("get_foreign_join_paths: no first relation");
            return;
        };

        let ctx_name = format!("Wrappers_full_query_join_{}", first_relation.relid.to_u32());
        let ctx = memctx::create_wrappers_memctx(&ctx_name);
        let mut state = FdwState::<E, W>::new(first_relation.relid, ctx);
        debug2!("get_foreign_join_paths: created FDW state");

        let context = remote_query_context_from_planner(root, false);
        debug2!(
            "get_foreign_join_paths: context foreign_count={} upper={} multiple_base={}",
            context.foreign_relation_count,
            context.has_upper_operations,
            context.has_multiple_base_relations
        );
        let remote_query_policy = state
            .instance
            .as_ref()
            .map(|instance| instance.remote_query_policy(&context))
            .unwrap_or(RemoteQueryPolicy::Optional);
        debug2!(
            "get_foreign_join_paths: remote_query_policy wants={}",
            remote_query_policy.wants_remote_query()
        );
        if !remote_query_policy.wants_remote_query() {
            return;
        }

        let columns = target_columns_from_reltarget(joinrel);
        debug2!("get_foreign_join_paths: columns={}", columns.len());
        let Some(full_query) = full_query_placeholder_from_planner(root, columns.clone()) else {
            debug2!("get_foreign_join_paths: could not build full-query placeholder");
            return;
        };
        let upper_only = context.has_upper_operations;

        state.tgts = columns;
        state.opts = first_relation.options.clone();
        state.full_query = Some(full_query);
        state.requires_full_query = true;
        // Do not call pg_get_querydef() here. During join path construction the
        // planner can still hold a partially-normalized query tree; deparsing it
        // here has been observed to segfault. get_foreign_plan fills in the SQL
        // only after the planner has selected an executable path.
        state.full_query_upper_only = upper_only;
        state.full_query_executable = !upper_only;
        state.remote_query_policy = remote_query_policy;
        debug2!(
            "get_foreign_join_paths: configured state upper_only={} executable={}",
            state.full_query_upper_only,
            state.full_query_executable
        );

        let rows = 1.0;
        let startup_cost = 0.0;
        let total_cost = if upper_only {
            remote_query_local_path_penalty(remote_query_policy)
        } else {
            startup_cost
        };
        (*joinrel).rows = rows;
        (*joinrel).fdw_private = leak_state_in_current_context(state) as _;
        debug2!("get_foreign_join_paths: stored joinrel fdw_private");

        let path = pg_sys::create_foreign_join_path(
            root,
            joinrel,
            (*joinrel).reltarget,
            rows,
            #[cfg(feature = "pg18")]
            0,
            startup_cost,
            total_cost,
            ptr::null_mut(),
            ptr::null_mut(),
            ptr::null_mut(),
            #[cfg(any(feature = "pg17", feature = "pg18"))]
            ptr::null_mut(),
            ptr::null_mut(),
        );
        debug2!("get_foreign_join_paths: created foreign join path");
        pg_sys::add_path(joinrel, &mut ((*path).path));
        debug2!("get_foreign_join_paths: added foreign join path");
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
        if baserel.is_null() || (*baserel).reltarget.is_null() {
            report_error(
                PgSqlErrorCode::ERRCODE_FDW_ERROR,
                "invalid planner relation state in get_foreign_rel_size",
            );
            return;
        }

        // create memory context for scan
        let ctx_name = format!("Wrappers_scan_{}", foreigntableid.to_u32());
        let ctx = memctx::create_wrappers_memctx(&ctx_name);

        // create scan state
        let mut state = FdwState::<E, W>::new(foreigntableid, ctx);

        PgMemoryContexts::For(state.tmp_ctx).switch_to(|_| {
            // extract qual list
            state.quals = extract_quals(root, baserel, foreigntableid);

            // extract target column list from target and restriction expression
            state.tgts = utils::extract_target_columns(root, baserel);

            // extract sort list
            state.sorts = extract_sorts(root, baserel, foreigntableid);

            // extract limit
            state.limit = extract_limit(root, baserel, foreigntableid);

            // get foreign table options
            let ftable = pg_sys::GetForeignTable(foreigntableid);
            if ftable.is_null() {
                report_error(
                    PgSqlErrorCode::ERRCODE_FDW_ERROR,
                    "foreign table metadata was not found",
                );
                return;
            }
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

        let has_unpushed_quals = state.quals.len() < baserel_restriction_count(baserel);
        let remote_query_context = remote_query_context_from_planner(root, has_unpushed_quals);
        state.remote_query_policy = state
            .instance
            .as_ref()
            .map(|instance| instance.remote_query_policy(&remote_query_context))
            .unwrap_or(RemoteQueryPolicy::Optional);
        state.requires_full_query = state.remote_query_policy.wants_remote_query()
            && remote_query_context.requires_remote_query_shape()
            && remote_query_context.all_referenced_relations_are_foreign;

        let (rows, width) = state.get_rel_size().report_unwrap();
        (*baserel).rows = rows as f64;
        (*(*baserel).reltarget).width = width;

        // save the state for following callbacks
        (*baserel).fdw_private = leak_state_in_current_context(state) as _;
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
        if baserel.is_null() || (*baserel).fdw_private.is_null() {
            report_error(
                PgSqlErrorCode::ERRCODE_FDW_ERROR,
                "missing FDW planner state in get_foreign_paths",
            );
            return;
        }
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
        let partial_path_penalty = if state.requires_full_query {
            remote_query_local_path_penalty(state.remote_query_policy)
        } else {
            0.0
        };
        let startup_cost = startup_cost + partial_path_penalty;
        let total_cost = startup_cost + (*baserel).rows + partial_path_penalty;

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
    root: *mut pg_sys::PlannerInfo,
    baserel: *mut pg_sys::RelOptInfo,
    _foreigntableid: pg_sys::Oid,
    _best_path: *mut pg_sys::ForeignPath,
    tlist: *mut pg_sys::List,
    scan_clauses: *mut pg_sys::List,
    outer_plan: *mut pg_sys::Plan,
) -> *mut pg_sys::ForeignScan {
    debug2!("---> get_foreign_plan");
    unsafe {
        if baserel.is_null() || (*baserel).fdw_private.is_null() {
            report_error(
                PgSqlErrorCode::ERRCODE_FDW_ERROR,
                "missing FDW planner state in get_foreign_plan",
            );
            return ptr::null_mut();
        }
        debug2!(
            "get_foreign_plan: reloptkind={:?} fdw_private={:p}",
            (*baserel).reloptkind,
            (*baserel).fdw_private
        );
        let mut state = PgBox::<FdwState<E, W>>::from_pg((*baserel).fdw_private as _);
        let wants_remote_query = state.remote_query_policy.wants_remote_query();
        debug2!(
            "get_foreign_plan: state full_query={} requires={} upper_only={} executable={} policy_wants={}",
            state.is_full_query_scan(),
            state.requires_full_query,
            state.full_query_upper_only,
            state.full_query_executable,
            wants_remote_query
        );

        // make foreign scan plan
        let mut scan_clauses = pg_sys::extract_actual_clauses(scan_clauses, false);
        debug2!("get_foreign_plan: extracted scan clauses");

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
        let is_full_query = state.is_full_query_scan()
            && ((*baserel).reloptkind == pg_sys::RelOptKind::RELOPT_JOINREL
                || (*baserel).reloptkind == pg_sys::RelOptKind::RELOPT_UPPER_REL);
        debug2!("get_foreign_plan: is_agg={is_agg} is_full_query={is_full_query}");

        if state.full_query_upper_only
            && (*baserel).reloptkind != pg_sys::RelOptKind::RELOPT_UPPER_REL
        {
            pgrx::error!(
                "remote-query execution must be planned at an upper relation for this query; refusing a non-executable intermediate remote-query path"
            );
        }
        if state.requires_full_query && !is_full_query && !is_agg {
            if state.remote_query_policy.requires_remote_query() {
                pgrx::error!(
                    "remote-query execution is required by this FDW, but PostgreSQL selected a partial local/base foreign scan plan"
                );
            } else {
                pgrx::warning!(
                    "remote-query pushdown was not selected; executing remaining query work in PostgreSQL"
                );
            }
        }
        if state.is_full_query_scan() && !state.full_query_executable {
            pgrx::error!(
                "remote-query execution must reach the final upper relation before execution; refusing an intermediate remote-query plan"
            );
        }

        if !is_agg && state.is_aggregate_scan() {
            // Upper-path was registered but the planner chose the base-rel scan
            // (typically because a local HashAgg over a small input was cheaper).
            // Drop the aggregate state so begin_foreign_scan dispatches to
            // begin_scan and the tuple slot is the base-rel's row type.
            state.aggregates = Vec::new();
            state.group_by = Vec::new();
        }
        if !is_full_query && state.is_full_query_scan() {
            state.full_query = None;
            state.full_query_upper_only = false;
            state.full_query_executable = true;
        }
        if is_full_query {
            scan_clauses = ptr::null_mut();
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
            if reltarget.is_null() {
                report_error(
                    PgSqlErrorCode::ERRCODE_FDW_ERROR,
                    "aggregate pushdown plan has no relation target",
                );
                return ptr::null_mut();
            }
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
        } else if is_full_query {
            debug2!("get_foreign_plan: building full-query target list");
            let mut final_tlist = tlist;
            if list_is_empty(final_tlist) && !root.is_null() && !(*root).parse.is_null() {
                final_tlist = pg_sys::list_copy_deep((*(*root).parse).targetList);
            }

            let mut output_columns = target_columns_from_tlist(final_tlist);
            if output_columns.is_empty() {
                output_columns = target_columns_from_reltarget(baserel);
            }
            debug2!(
                "get_foreign_plan: full-query output columns={}",
                output_columns.len()
            );
            if let Some(full_query) = state.full_query.as_mut() {
                full_query.columns = output_columns.clone();
                if full_query.sql.is_empty() {
                    debug2!("get_foreign_plan: deparsing full-query SQL");
                    let Some(sql) = full_query_sql_from_planner(root) else {
                        report_error(
                            PgSqlErrorCode::ERRCODE_FDW_ERROR,
                            "failed to deparse PostgreSQL query for remote execution",
                        );
                        return ptr::null_mut();
                    };
                    full_query.sql = sql;
                    debug2!("get_foreign_plan: deparsed full-query SQL");
                }
            }
            state.tgts = output_columns;

            (final_tlist, pg_sys::list_copy_deep(final_tlist))
        } else {
            (tlist, ptr::null_mut())
        };

        // Keep a plan-owned template. Prepared statements can execute the same
        // ForeignScan plan multiple times, so begin_foreign_scan clones this
        // template for each execution. The template is dropped when Postgres
        // deletes the plan memory context.
        let fdw_private = serialize_plan_template(&state);

        let scanrelid = if (*baserel).reloptkind == pg_sys::RelOptKind::RELOPT_BASEREL {
            (*baserel).relid
        } else {
            0
        };
        pg_sys::make_foreignscan(
            final_tlist,
            scan_clauses,
            scanrelid,
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
        if node.is_null() {
            return;
        }
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
        if node.is_null() {
            return;
        }
        let estate = (*node).ss.ps.state;
        let econtext = (*node).ss.ps.ps_ExprContext;

        // assign parameter value to qual
        for qual in &mut state.quals.iter_mut() {
            if let Some(param) = &mut qual.param {
                let mut current_value: Option<Value> = None;
                match param.kind {
                    ParamKind::PARAM_EXTERN => {
                        // get parameter list in execution state
                        let plist_info = if estate.is_null() {
                            ptr::null_mut()
                        } else {
                            (*estate).es_param_list_info
                        };
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
                        if econtext.is_null() {
                            continue;
                        }
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

                match param.eval_value.lock() {
                    Ok(mut eval_value) => *eval_value = current_value,
                    Err(_) => debug2!("parameter evaluation cache lock was poisoned"),
                }
            }
        }
    }
}

unsafe fn assign_remote_query_parameters<E: Into<ErrorReport>, W: ForeignDataWrapper<E>>(
    node: *mut pg_sys::ForeignScanState,
    state: &mut FdwState<E, W>,
) {
    unsafe {
        if node.is_null() {
            return;
        }

        if state.full_query.is_none() {
            return;
        }

        let mut parameters = Vec::new();

        let estate = (*node).ss.ps.state;
        let plist_info = if estate.is_null() {
            ptr::null_mut()
        } else {
            (*estate).es_param_list_info
        };
        if !plist_info.is_null() {
            let params_cnt = (*plist_info).numParams as usize;
            let plist = (*plist_info).params.as_slice(params_cnt);
            for (idx, param) in plist.iter().enumerate() {
                if param.ptype == Oid::INVALID {
                    continue;
                }
                upsert_remote_query_parameter(
                    &mut parameters,
                    RemoteQueryParameter {
                        kind: ParamKind::PARAM_EXTERN,
                        id: idx + 1,
                        type_oid: param.ptype,
                        value: Cell::from_polymorphic_datum(param.value, param.isnull, param.ptype)
                            .map(Value::Cell),
                    },
                );
            }
        }

        for qual in &state.quals {
            let Some(param) = &qual.param else {
                continue;
            };
            let value = match param.eval_value.lock() {
                Ok(value) => value.clone(),
                Err(_) => {
                    debug2!("remote-query parameter cache lock was poisoned");
                    None
                }
            };
            upsert_remote_query_parameter(
                &mut parameters,
                RemoteQueryParameter {
                    kind: param.kind,
                    id: param.id,
                    type_oid: param.type_oid,
                    value,
                },
            );
        }

        if let Some(full_query) = state.full_query.as_mut() {
            full_query.parameters = parameters;
        }
    }
}

fn upsert_remote_query_parameter(
    parameters: &mut Vec<RemoteQueryParameter>,
    parameter: RemoteQueryParameter,
) {
    if let Some(existing) = parameters
        .iter_mut()
        .find(|existing| existing.kind == parameter.kind && existing.id == parameter.id)
    {
        *existing = parameter;
    } else {
        parameters.push(parameter);
    }
}

fn compute_param_fingerprint<E: Into<ErrorReport>, W: ForeignDataWrapper<E>>(
    state: &FdwState<E, W>,
) -> String {
    let mut parts = state
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
        .collect::<Vec<_>>();

    if let Some(full_query) = &state.full_query {
        parts.extend(full_query.parameters.iter().map(|param| {
            format!(
                "remote|{}|{}|{}|{:?}",
                param.kind, param.id, param.type_oid, param.value
            )
        }));
    }

    parts.join(";")
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
        if node.is_null() {
            report_error(
                PgSqlErrorCode::ERRCODE_FDW_ERROR,
                "invalid null ForeignScanState in begin_foreign_scan",
            );
            return;
        }
        let scan_state = (*node).ss;
        let plan = scan_state.ps.plan as *mut pg_sys::ForeignScan;
        if plan.is_null() || (*plan).fdw_private.is_null() {
            report_error(
                PgSqlErrorCode::ERRCODE_FDW_ERROR,
                "missing FDW plan state in begin_foreign_scan",
            );
            return;
        }
        let mut state = if let Some(snapshot_state) =
            deserialize_plan_snapshot::<E, W>((*plan).fdw_private as _)
        {
            PgBox::<FdwState<E, W>>::from_pg(Box::into_raw(Box::new(snapshot_state)))
        } else {
            let plan_state = FdwState::<E, W>::deserialize_from_list((*plan).fdw_private as _);
            if plan_state.is_null() {
                report_error(
                    PgSqlErrorCode::ERRCODE_FDW_ERROR,
                    "FDW plan state could not be deserialized",
                );
                return;
            }
            PgBox::<FdwState<E, W>>::from_pg(Box::into_raw(Box::new(
                plan_state.clone_for_execution(),
            )))
        };

        // assign parameter values to qual
        assign_parameter_value(node, &mut state);
        assign_remote_query_parameters(node, &mut state);
        state.param_fingerprint = compute_param_fingerprint(&state);

        // begin scan if it is not EXPLAIN statement
        if eflags & pg_sys::EXEC_FLAG_EXPLAIN_ONLY as c_int <= 0 {
            // choose aggregate scan or normal scan based on state
            let result = state.begin_current_scan();
            if result.is_err() {
                drop_fdw_state(state.as_ptr());
                result.report_unwrap();
            }

            // For aggregate upper-rel scans, scanrelid=0 so ss_currentRelation is
            // NULL. Use the number of output columns from state.tgts instead.
            let natts = if state.is_aggregate_scan() || state.is_full_query_scan() {
                state.tgts.len()
            } else {
                let rel = scan_state.ss_currentRelation;
                if rel.is_null() || (*rel).rd_att.is_null() {
                    drop_fdw_state(state.as_ptr());
                    report_error(
                        PgSqlErrorCode::ERRCODE_FDW_ERROR,
                        "base foreign scan has no current relation descriptor",
                    );
                    return;
                }
                (*(*rel).rd_att).natts as usize
            };

            // initialize scan result lists
            state.values.extend_from_slice(&vec![Datum::from(0); natts]);
            state.nulls.extend_from_slice(&vec![true; natts]);
        }

        (*node).fdw_state = state.into_pg() as _;
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
        if node.is_null() {
            return ptr::null_mut();
        }
        if (*node).fdw_state.is_null() {
            return (*node).ss.ss_ScanTupleSlot;
        }
        let mut state = PgBox::<FdwState<E, W>>::from_pg((*node).fdw_state as _);

        // evaluate parameter values
        assign_parameter_value(node, &mut state);
        assign_remote_query_parameters(node, &mut state);

        // clear slot
        let slot = (*node).ss.ss_ScanTupleSlot;
        if slot.is_null() {
            report_error(
                PgSqlErrorCode::ERRCODE_FDW_ERROR,
                "foreign scan has no tuple slot",
            );
            return ptr::null_mut();
        }
        polyfill::exec_clear_tuple(slot);

        state.row.clear();

        let result = state.iter_scan();
        if result.is_err() {
            drop_fdw_state(state.as_ptr());
            (*node).fdw_state = ptr::null::<FdwState<E, W>>() as _;
        }
        if result.report_unwrap().is_some() {
            if state.row.cols.len() != state.tgts.len() || state.row.cells.len() != state.tgts.len()
            {
                report_error(
                    PgSqlErrorCode::ERRCODE_FDW_INVALID_COLUMN_NUMBER,
                    "target column number not match",
                );
                return slot;
            }

            let uses_heap_tuple_slot = state.is_aggregate_scan() || state.is_full_query_scan();
            PgMemoryContexts::For(state.tmp_ctx).switch_to(|_| {
                for i in 0..state.row.cells.len() {
                    let Some(att_idx) = state.tgts[i].num.checked_sub(1) else {
                        report_error(
                            PgSqlErrorCode::ERRCODE_FDW_INVALID_COLUMN_NUMBER,
                            "target column index must be greater than zero",
                        );
                        return;
                    };
                    if att_idx >= state.values.len() || att_idx >= state.nulls.len() {
                        report_error(
                            PgSqlErrorCode::ERRCODE_FDW_INVALID_COLUMN_NUMBER,
                            "target column index is outside the tuple descriptor",
                        );
                        return;
                    }
                    let Some(cell) = state.row.cells.get_mut(i) else {
                        report_error(
                            PgSqlErrorCode::ERRCODE_FDW_INVALID_COLUMN_NUMBER,
                            "missing cell for target column",
                        );
                        return;
                    };
                    match cell.take() {
                        Some(cell) => match cell.into_datum() {
                            Some(datum) => {
                                state.values[att_idx] = datum;
                                state.nulls[att_idx] = false;
                            }
                            None => {
                                report_error(
                                    PgSqlErrorCode::ERRCODE_FDW_ERROR,
                                    "failed to convert FDW cell into PostgreSQL datum",
                                );
                                return;
                            }
                        },
                        None => {
                            state.nulls[att_idx] = true;
                        }
                    }
                }

                if uses_heap_tuple_slot {
                    // For upper/join scans the slot type is TTSOpsHeapTuple (because
                    // fdw_scan_tlist != NIL). ExecStoreVirtualTuple is only correct
                    // for TTSOpsVirtual slots; using it on a HeapTuple slot leaves
                    // hslot->tuple == NULL, which causes tts_heap_materialize (called
                    // by Sort) to re-read tts_values after zeroing tts_nvalid —
                    // resulting in a SIGSEGV when a Sort node is present (ORDER BY).
                    // Form a proper HeapTuple and use ExecStoreHeapTuple instead.
                    let desc = (*slot).tts_tupleDescriptor;
                    if desc.is_null() {
                        report_error(
                            PgSqlErrorCode::ERRCODE_FDW_ERROR,
                            "foreign scan heap tuple slot has no tuple descriptor",
                        );
                        return;
                    }
                    let htup = pg_sys::heap_form_tuple(
                        desc,
                        state.values.as_mut_ptr(),
                        state.nulls.as_mut_ptr(),
                    );
                    if htup.is_null() {
                        report_error(
                            PgSqlErrorCode::ERRCODE_FDW_ERROR,
                            "failed to form heap tuple for foreign scan",
                        );
                        return;
                    }
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
        if node.is_null() {
            return;
        }
        let fdw_state = (*node).fdw_state as *mut FdwState<E, W>;
        if !fdw_state.is_null() {
            let mut state = PgBox::<FdwState<E, W>>::from_pg(fdw_state);
            assign_parameter_value(node, &mut state);
            assign_remote_query_parameters(node, &mut state);
            let next_fingerprint = compute_param_fingerprint(&state);
            let result = if next_fingerprint != state.param_fingerprint {
                state.param_fingerprint = next_fingerprint;
                // end the active scan to release resources before restarting with new params
                let _ = state.end_scan();
                state.begin_current_scan()
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
        if node.is_null() {
            return;
        }
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
