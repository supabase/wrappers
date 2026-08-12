//! Upper path planning for aggregate pushdown
//!
//! This module implements the GetForeignUpperPaths callback which enables
//! aggregate pushdown to foreign data sources.

use pgrx::pg_sys::panic::ErrorReport;
use pgrx::{PgBox, debug2, pg_guard, pg_sys};
use std::ptr;

use crate::interface::{Aggregate, AggregateKind, Column};
use crate::prelude::ForeignDataWrapper;
use crate::scan::{
    FdwState, full_query_placeholder_from_planner, leak_state_in_current_context,
    query_requires_full_query, remote_query_context_from_planner, remote_query_local_path_penalty,
    target_columns_from_reltarget,
};

/// Helper to iterate over a pg_sys::List using raw pointer access.
/// Returns an iterator over pointers to the list elements.
///
/// # Safety
///
/// The caller must ensure `list` is a valid pg_sys::List pointer (or null).
unsafe fn list_iter<T>(list: *mut pg_sys::List) -> impl Iterator<Item = *mut T> {
    let len = if list.is_null() {
        0
    } else {
        unsafe { (*list).length as usize }
    };

    (0..len).map(move |i| unsafe {
        let cell = (*list).elements.add(i);
        (*cell).ptr_value as *mut T
    })
}

/// Map a PostgreSQL aggregate function OID to our AggregateKind enum
/// by looking up the function name.
fn oid_to_aggregate_kind(aggfnoid: pg_sys::Oid) -> Option<AggregateKind> {
    unsafe {
        let agg_name = pg_sys::get_func_name(aggfnoid);
        if agg_name.is_null() {
            return None;
        }

        let name_cstr = std::ffi::CStr::from_ptr(agg_name);
        let name = name_cstr.to_str().ok()?;

        match name {
            "count" => {
                let nargs = pg_sys::get_func_nargs(aggfnoid);
                if nargs == 0 {
                    Some(AggregateKind::Count)
                } else {
                    Some(AggregateKind::CountColumn)
                }
            }
            "sum" => Some(AggregateKind::Sum),
            "avg" => Some(AggregateKind::Avg),
            "min" => Some(AggregateKind::Min),
            "max" => Some(AggregateKind::Max),
            _ => None,
        }
    }
}

/// Extract column info from a Var node.
///
/// # Safety
///
/// `var` must be a valid pointer to a pg_sys::Var node. `root` must be valid.
unsafe fn extract_column_from_var(
    root: *mut pg_sys::PlannerInfo,
    var: *mut pg_sys::Var,
) -> Option<Column> {
    unsafe {
        if root.is_null() || var.is_null() || (*var).varattno <= 0 {
            return None;
        }
        let relid = (*var).varno as pg_sys::Index;
        let attno = (*var).varattno;
        let rte = pg_sys::planner_rt_fetch(relid, root);
        if rte.is_null() {
            return None;
        }
        let rel_oid = (*rte).relid;
        let att_name = pg_sys::get_attname(rel_oid, attno, false);
        if att_name.is_null() {
            return None;
        }
        let name_cstr = std::ffi::CStr::from_ptr(att_name);
        let name = name_cstr.to_str().ok()?;
        Some(Column {
            name: name.to_string(),
            num: attno as usize,
            type_oid: (*var).vartype,
        })
    }
}

/// Extract aggregate information from the query's output relation target list.
///
/// # Safety
///
/// All pointer parameters must be valid PostgreSQL planner structures.
unsafe fn extract_aggregates(
    root: *mut pg_sys::PlannerInfo,
    output_rel: *mut pg_sys::RelOptInfo,
    extra: *mut std::ffi::c_void,
) -> Option<Vec<Aggregate>> {
    unsafe {
        if root.is_null() || output_rel.is_null() {
            return None;
        }
        if extra.is_null() {
            return None;
        }

        let group_extra = extra as *mut pg_sys::GroupPathExtraData;
        if !(*group_extra).havingQual.is_null() {
            debug2!("HAVING clause present, skipping aggregate pushdown");
            return None;
        }

        let reltarget = (*output_rel).reltarget;
        if reltarget.is_null() {
            return None;
        }

        let exprs = (*reltarget).exprs;
        if exprs.is_null() {
            return None;
        }

        let mut aggregates = Vec::new();
        let mut resno = 1;

        for expr in list_iter::<pg_sys::Node>(exprs) {
            if expr.is_null() {
                resno += 1;
                continue;
            }
            if (*expr).type_ == pg_sys::NodeTag::T_Aggref {
                let aggref = expr as *mut pg_sys::Aggref;

                let kind = match oid_to_aggregate_kind((*aggref).aggfnoid) {
                    Some(k) => k,
                    None => {
                        let func_name = pg_sys::get_func_name((*aggref).aggfnoid);
                        if !func_name.is_null() {
                            let name = std::ffi::CStr::from_ptr(func_name).to_string_lossy();
                            debug2!("Unsupported aggregate function '{name}', skipping pushdown");
                        } else {
                            debug2!("Unknown aggregate function, skipping pushdown");
                        }
                        return None;
                    }
                };

                // FILTER clause not supported for pushdown
                if !(*aggref).aggfilter.is_null() {
                    debug2!("Aggregate has FILTER clause, skipping pushdown");
                    return None;
                }

                // DISTINCT only supported for COUNT(column)
                if !(*aggref).aggdistinct.is_null() {
                    match kind {
                        AggregateKind::CountColumn => {
                            debug2!("COUNT(DISTINCT) detected, pushdown supported");
                        }
                        _ => {
                            debug2!(
                                "DISTINCT modifier on {kind:?} not supported, skipping pushdown"
                            );
                            return None;
                        }
                    }
                }

                // Get the column being aggregated (if any)
                let column = if !(*aggref).args.is_null() && (*(*aggref).args).length > 0 {
                    let first_cell = (*(*aggref).args).elements;
                    let target_entry = (*first_cell).ptr_value as *mut pg_sys::TargetEntry;
                    if !target_entry.is_null() {
                        let arg_expr = (*target_entry).expr as *mut pg_sys::Node;
                        if !arg_expr.is_null() && (*arg_expr).type_ == pg_sys::NodeTag::T_Var {
                            extract_column_from_var(root, arg_expr as *mut pg_sys::Var)
                        } else {
                            None
                        }
                    } else {
                        None
                    }
                } else {
                    None
                };

                // Aggregates that require a column (SUM, AVG, MIN, MAX, CountColumn)
                // must have a resolved column reference. Reject pushdown for
                // non-column expressions like SUM(a + b) to avoid invalid SQL.
                if column.is_none() && kind != AggregateKind::Count {
                    debug2!("Aggregate {kind:?} has no simple column reference, skipping pushdown");
                    return None;
                }

                aggregates.push(Aggregate {
                    kind,
                    column,
                    distinct: !(*aggref).aggdistinct.is_null(),
                    alias: format!("agg_{resno}"),
                    type_oid: (*aggref).aggtype,
                });
            }

            resno += 1;
        }

        if aggregates.is_empty() {
            return None;
        }

        debug2!(
            "Extracted {} aggregates for pushdown: {:?}",
            aggregates.len(),
            aggregates.iter().map(|a| a.kind).collect::<Vec<_>>()
        );

        Some(aggregates)
    }
}

/// Extract GROUP BY columns from the query.
///
/// # Safety
///
/// All pointer parameters must be valid PostgreSQL planner structures.
/// Returns `None` if any GROUP BY item is not a plain column reference,
/// which means we cannot safely push down the grouping.
unsafe fn extract_group_by_columns(
    root: *mut pg_sys::PlannerInfo,
    _input_rel: *mut pg_sys::RelOptInfo,
) -> Option<Vec<Column>> {
    unsafe {
        if root.is_null() {
            return None;
        }
        let mut group_by = Vec::new();

        let parse = (*root).parse;
        if parse.is_null() {
            return Some(group_by);
        }

        let group_clause = (*parse).groupClause;
        if group_clause.is_null() || (*group_clause).length == 0 {
            return Some(group_by);
        }

        let target_list = (*parse).targetList;
        if target_list.is_null() {
            return Some(group_by);
        }

        for sort_group_clause in list_iter::<pg_sys::SortGroupClause>(group_clause) {
            if sort_group_clause.is_null() {
                continue;
            }
            let tle_resno = (*sort_group_clause).tleSortGroupRef;
            let mut found = false;

            for tle in list_iter::<pg_sys::TargetEntry>(target_list) {
                if tle.is_null() {
                    continue;
                }
                if (*tle).ressortgroupref == tle_resno {
                    let expr = (*tle).expr as *mut pg_sys::Node;
                    if !expr.is_null()
                        && (*expr).type_ == pg_sys::NodeTag::T_Var
                        && let Some(col) = extract_column_from_var(root, expr as *mut pg_sys::Var)
                    {
                        group_by.push(col);
                        found = true;
                    }
                    break;
                }
            }

            if !found {
                debug2!("GROUP BY item is not a plain column, skipping pushdown");
                return None;
            }
        }

        Some(group_by)
    }
}

/// GetForeignUpperPaths callback
///
/// Called by the PostgreSQL planner to create paths for upper-level processing
/// (aggregation, sorting, etc.) that can be pushed down to the foreign server.
#[pg_guard]
pub(super) extern "C-unwind" fn get_foreign_upper_paths<
    E: Into<ErrorReport>,
    W: ForeignDataWrapper<E>,
>(
    root: *mut pg_sys::PlannerInfo,
    stage: pg_sys::UpperRelationKind::Type,
    input_rel: *mut pg_sys::RelOptInfo,
    output_rel: *mut pg_sys::RelOptInfo,
    extra: *mut std::ffi::c_void,
) {
    debug2!("---> get_foreign_upper_paths, stage: {stage:?}");

    if input_rel.is_null() || output_rel.is_null() {
        return;
    }

    unsafe {
        if add_full_query_upper_path::<E, W>(root, stage, input_rel, output_rel) {
            return;
        }
    }

    if stage == pg_sys::UpperRelationKind::UPPERREL_FINAL {
        return;
    }

    // Only handle GROUP_AGG stage
    if stage != pg_sys::UpperRelationKind::UPPERREL_GROUP_AGG {
        return;
    }

    unsafe {
        // Get the FDW state from the input relation (set during get_foreign_rel_size)
        let fdw_private = (*input_rel).fdw_private;
        if fdw_private.is_null() {
            return;
        }

        let mut state = PgBox::<FdwState<E, W>>::from_pg(fdw_private as _);

        // Check if FDW supports any aggregates
        let supported = {
            let Some(ref instance) = state.instance else {
                return;
            };
            let supported = instance.supported_aggregates();
            if supported.is_empty() {
                return;
            }
            supported
        };

        // Extract aggregates from the query
        let aggregates = match extract_aggregates(root, output_rel, extra) {
            Some(aggs) => aggs,
            None => return,
        };

        // Check if all aggregates are supported
        for agg in &aggregates {
            if !supported.contains(&agg.kind) {
                debug2!("Aggregate {:?} not supported, skipping pushdown", agg.kind);
                return;
            }
        }

        // Extract GROUP BY columns (returns None if any item is not a plain column)
        let group_by = match extract_group_by_columns(root, input_rel) {
            Some(cols) => cols,
            None => return,
        };
        if !group_by.is_empty() {
            debug2!(
                "Extracted GROUP BY columns: {:?}",
                group_by.iter().map(|c| c.name.as_str()).collect::<Vec<_>>()
            );
        }

        // Check if GROUP BY is supported (if present)
        if !group_by.is_empty() {
            let Some(ref instance) = state.instance else {
                return;
            };
            if !instance.supports_group_by() {
                debug2!("GROUP BY not supported, skipping pushdown");
                return;
            }
        }

        // Store aggregates and group_by in the FdwState so they survive to
        // execution. Note: input_rel.fdw_private and output_rel.fdw_private
        // share the same FdwState pointer, so this mutation is visible through
        // both. get_foreign_plan must therefore key off baserel.reloptkind to
        // tell whether the planner actually picked the upper path or fell back
        // to the base-rel scan with a local Aggregate on top.
        state.aggregates = aggregates.clone();
        state.group_by = group_by.clone();

        // Cost estimation. We deliberately price the pushdown at ~0 so the
        // planner prefers it over the local HashAgg/GroupAgg alternatives that
        // also live on grouped_rel — pushdown collapses the row stream at the
        // remote side and is essentially always cheaper than fetching rows and
        // aggregating locally.
        let rows: i64 = 1;
        let startup_cost = state
            .opts
            .get("startup_cost")
            .and_then(|c| c.parse::<f64>().ok())
            .unwrap_or(0.0);
        let total_cost = startup_cost;

        debug2!(
            "Aggregate pushdown cost estimate: rows={rows}, startup={startup_cost}, total={total_cost}"
        );

        // Store the FdwState pointer in output_rel->fdw_private so that
        // get_foreign_plan can find it when building the plan for this path.
        // This is the critical fix: previously fdw_private was null, so aggregates
        // were extracted but never passed through to the executor.
        (*output_rel).fdw_private = fdw_private;
        let _ = state.into_pg();

        // Create the foreign upper path
        let path = pg_sys::create_foreign_upper_path(
            root,
            output_rel,
            (*output_rel).reltarget, // pathtarget
            rows as f64,             // rows
            #[cfg(feature = "pg18")]
            0, // disabled_nodes (pg18 only)
            startup_cost,            // startup_cost
            total_cost,              // total_cost
            ptr::null_mut(),         // pathkeys
            ptr::null_mut(),         // fdw_outerpath
            #[cfg(any(feature = "pg17", feature = "pg18"))]
            ptr::null_mut(), // fdw_restrictinfo (pg17+ only)
            ptr::null_mut(),         // fdw_private (path-level, not needed)
        );

        pg_sys::add_path(output_rel, &mut ((*path).path));

        debug2!(
            "Created aggregate pushdown path: {} aggregates, {} group by columns",
            aggregates.len(),
            group_by.len()
        );
    }
}

unsafe fn add_full_query_upper_path<E: Into<ErrorReport>, W: ForeignDataWrapper<E>>(
    root: *mut pg_sys::PlannerInfo,
    stage: pg_sys::UpperRelationKind::Type,
    input_rel: *mut pg_sys::RelOptInfo,
    output_rel: *mut pg_sys::RelOptInfo,
) -> bool {
    unsafe {
        if root.is_null() || input_rel.is_null() || output_rel.is_null() {
            debug2!("add_full_query_upper_path: missing planner relation state");
            return false;
        }
        if !query_requires_full_query(root) {
            debug2!("add_full_query_upper_path: query does not require full-query path");
            return false;
        }

        let fdw_private = (*input_rel).fdw_private;
        if fdw_private.is_null() {
            debug2!("add_full_query_upper_path: input rel has no fdw_private");
            return false;
        }
        debug2!("add_full_query_upper_path: input rel has fdw_private");

        let input_state = PgBox::<FdwState<E, W>>::from_pg(fdw_private as _);
        let mut remote_query_policy = input_state.remote_query_policy;
        debug2!(
            "add_full_query_upper_path: initial remote policy wants={}",
            remote_query_policy.wants_remote_query()
        );
        if !remote_query_policy.wants_remote_query() {
            let context = remote_query_context_from_planner(root, false);
            debug2!(
                "add_full_query_upper_path: recomputed context foreign_count={} upper={} multiple_base={}",
                context.foreign_relation_count,
                context.has_upper_operations,
                context.has_multiple_base_relations
            );
            remote_query_policy = input_state
                .instance
                .as_ref()
                .map(|instance| instance.remote_query_policy(&context))
                .unwrap_or_default();
        }
        if !remote_query_policy.wants_remote_query() {
            debug2!("add_full_query_upper_path: remote query policy does not want pushdown");
            return false;
        }

        let columns = target_columns_from_reltarget(output_rel);
        debug2!(
            "add_full_query_upper_path: columns={} stage={:?}",
            columns.len(),
            stage
        );
        let Some(full_query) = full_query_placeholder_from_planner(root, columns.clone()) else {
            debug2!("add_full_query_upper_path: could not build full query placeholder");
            return false;
        };
        debug2!("add_full_query_upper_path: built full query placeholder");
        let Some(first_relation) = full_query.relations.first() else {
            debug2!("add_full_query_upper_path: full query has no relation");
            return false;
        };

        let is_final = stage == pg_sys::UpperRelationKind::UPPERREL_FINAL;
        debug2!("add_full_query_upper_path: is_final={is_final}");
        let ctx_name = format!(
            "Wrappers_full_query_upper_{}_{}",
            first_relation.relid.to_u32(),
            stage
        );
        let ctx = crate::memctx::create_wrappers_memctx(&ctx_name);
        let mut state = FdwState::<E, W>::new(first_relation.relid, ctx);
        state.tgts = columns;
        state.opts = first_relation.options.clone();
        state.full_query = Some(full_query);
        state.requires_full_query = true;
        state.full_query_upper_only = true;
        state.full_query_executable = is_final;
        state.remote_query_policy = remote_query_policy;

        let rows = 1.0;
        let startup_cost = state
            .opts
            .get("startup_cost")
            .and_then(|c| c.parse::<f64>().ok())
            .unwrap_or(0.0);
        let total_cost = if is_final {
            startup_cost
        } else {
            startup_cost + remote_query_local_path_penalty(remote_query_policy)
        };

        (*output_rel).rows = rows;
        (*output_rel).fdw_private = leak_state_in_current_context(state) as _;

        let path = pg_sys::create_foreign_upper_path(
            root,
            output_rel,
            (*output_rel).reltarget,
            rows,
            #[cfg(feature = "pg18")]
            0,
            startup_cost,
            total_cost,
            ptr::null_mut(),
            ptr::null_mut(),
            #[cfg(any(feature = "pg17", feature = "pg18"))]
            ptr::null_mut(),
            ptr::null_mut(),
        );

        pg_sys::add_path(output_rel, &mut ((*path).path));
        debug2!("Created full-query pushdown upper path");
        true
    }
}
