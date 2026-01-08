//! Upper path planning for aggregate pushdown
//!
//! This module implements the GetForeignUpperPaths callback which enables
//! aggregate pushdown to foreign data sources.

use pgrx::pg_sys::panic::ErrorReport;
use pgrx::{debug2, pg_guard, pg_sys, PgBox};
use std::ptr;

use crate::interface::{Aggregate, AggregateKind, Column};
use crate::prelude::ForeignDataWrapper;
use crate::scan::FdwState;

/// Check if a given PostgreSQL aggregate OID is supported by the FDW
fn oid_to_aggregate_kind(aggfnoid: pg_sys::Oid) -> Option<AggregateKind> {
    // PostgreSQL built-in aggregate function OIDs
    // These are from pg_proc.dat in PostgreSQL source
    // COUNT(*) = 2803, COUNT(any) = 2147
    // SUM(int8) = 2107, SUM(int4) = 2108, SUM(float8) = 2111, etc.
    // AVG(int8) = 2100, AVG(float8) = 2105, etc.
    // MIN(any) = 2145, MAX(any) = 2146

    unsafe {
        // Get the aggregate function name from the OID
        let agg_name = pg_sys::get_func_name(aggfnoid);
        if agg_name.is_null() {
            return None;
        }

        let name_cstr = std::ffi::CStr::from_ptr(agg_name);
        let name = name_cstr.to_str().ok()?;

        match name {
            "count" => {
                // Determine if it's COUNT(*) or COUNT(column)
                // COUNT(*) has no arguments, COUNT(column) has one
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

/// Extract aggregate information from the query
unsafe fn extract_aggregates(
    root: *mut pg_sys::PlannerInfo,
    output_rel: *mut pg_sys::RelOptInfo,
    extra: *mut std::ffi::c_void,
) -> Option<Vec<Aggregate>> {
    // The extra parameter for UPPERREL_GROUP_AGG is GroupPathExtraData
    if extra.is_null() {
        return None;
    }

    let group_extra = extra as *mut pg_sys::GroupPathExtraData;
    if !(*group_extra).havingQual.is_null() {
        // HAVING clause not supported for pushdown
        debug2!("HAVING clause present, skipping aggregate pushdown");
        return None;
    }

    // Get the target list from the output relation
    let reltarget = (*output_rel).reltarget;
    if reltarget.is_null() {
        return None;
    }

    let mut aggregates = Vec::new();
    let mut resno = 1;

    // Iterate through the target expressions using PgList
    let exprs = (*reltarget).exprs;
    if exprs.is_null() {
        return None;
    }

    let exprs_list: pgrx::PgList<pg_sys::Node> = pgrx::PgList::from_pg(exprs);

    for expr in exprs_list.iter_ptr() {
        // Check if this is an Aggref (aggregate reference)
        if (*expr).type_ == pg_sys::NodeTag::T_Aggref {
            let aggref = expr as *mut pg_sys::Aggref;

            let kind = match oid_to_aggregate_kind((*aggref).aggfnoid) {
                Some(k) => k,
                None => {
                    // Get function name for debug message
                    let func_name = pg_sys::get_func_name((*aggref).aggfnoid);
                    if !func_name.is_null() {
                        let name = std::ffi::CStr::from_ptr(func_name).to_string_lossy();
                        debug2!(
                            "Unsupported aggregate function '{}', skipping pushdown",
                            name
                        );
                    } else {
                        debug2!("Unknown aggregate function, skipping pushdown");
                    }
                    return None;
                }
            };

            // Check for DISTINCT - only supported for COUNT
            if !(*aggref).aggdistinct.is_null() {
                match kind {
                    AggregateKind::CountColumn => {
                        // COUNT(DISTINCT col) is supported
                        debug2!("COUNT(DISTINCT) detected, pushdown supported");
                    }
                    _ => {
                        debug2!(
                            "DISTINCT modifier on {:?} not supported, skipping pushdown",
                            kind
                        );
                        return None;
                    }
                }
            }

            // Get the column being aggregated (if any)
            let column = if !(*aggref).args.is_null() && (*(*aggref).args).length > 0 {
                let args_list: pgrx::PgList<pg_sys::TargetEntry> =
                    pgrx::PgList::from_pg((*aggref).args);
                // Store the first entry before the if-let to avoid lifetime issues
                let first_entry = args_list.iter_ptr().next();
                if let Some(target_entry) = first_entry {
                    let arg_expr = (*target_entry).expr as *mut pg_sys::Node;

                    if (*arg_expr).type_ == pg_sys::NodeTag::T_Var {
                        let var = arg_expr as *mut pg_sys::Var;
                        // Get column name from the var
                        let relid = (*var).varno as pg_sys::Index;
                        let attno = (*var).varattno;

                        // Try to get the column name
                        let rte = pg_sys::planner_rt_fetch(relid, root);
                        if !rte.is_null() {
                            let rel_oid = (*rte).relid;
                            let att_name = pg_sys::get_attname(rel_oid, attno, false);
                            if !att_name.is_null() {
                                let name_cstr = std::ffi::CStr::from_ptr(att_name);
                                if let Ok(name) = name_cstr.to_str() {
                                    Some(Column {
                                        name: name.to_string(),
                                        num: attno as usize,
                                        type_oid: (*var).vartype,
                                    })
                                } else {
                                    None
                                }
                            } else {
                                None
                            }
                        } else {
                            None
                        }
                    } else {
                        // Complex expression, not a simple column reference
                        None
                    }
                } else {
                    None
                }
            } else {
                None
            };

            let aggregate = Aggregate {
                kind,
                column,
                distinct: !(*aggref).aggdistinct.is_null(),
                alias: format!("agg_{resno}"),
            };

            aggregates.push(aggregate);
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

/// Extract GROUP BY columns from the query
unsafe fn extract_group_by_columns(
    root: *mut pg_sys::PlannerInfo,
    _input_rel: *mut pg_sys::RelOptInfo,
) -> Vec<Column> {
    let mut group_by = Vec::new();

    let parse = (*root).parse;
    if parse.is_null() {
        return group_by;
    }

    let group_clause = (*parse).groupClause;
    if group_clause.is_null() || (*group_clause).length == 0 {
        return group_by;
    }

    // Get the target list
    let target_list = (*parse).targetList;
    if target_list.is_null() {
        return group_by;
    }

    // Iterate through group clause items using PgList
    let group_list: pgrx::PgList<pg_sys::SortGroupClause> = pgrx::PgList::from_pg(group_clause);
    let target_entries: pgrx::PgList<pg_sys::TargetEntry> = pgrx::PgList::from_pg(target_list);

    for sort_group_clause in group_list.iter_ptr() {
        // Find the target entry for this group clause
        let tle_resno = (*sort_group_clause).tleSortGroupRef;

        // Search target list for matching resno
        for tle in target_entries.iter_ptr() {
            if (*tle).ressortgroupref == tle_resno {
                let expr = (*tle).expr as *mut pg_sys::Node;

                if (*expr).type_ == pg_sys::NodeTag::T_Var {
                    let var = expr as *mut pg_sys::Var;
                    let relid = (*var).varno as pg_sys::Index;
                    let attno = (*var).varattno;

                    let rte = pg_sys::planner_rt_fetch(relid, root);
                    if !rte.is_null() {
                        let rel_oid = (*rte).relid;
                        let att_name = pg_sys::get_attname(rel_oid, attno, false);
                        if !att_name.is_null() {
                            let name_cstr = std::ffi::CStr::from_ptr(att_name);
                            if let Ok(name) = name_cstr.to_str() {
                                group_by.push(Column {
                                    name: name.to_string(),
                                    num: attno as usize,
                                    type_oid: (*var).vartype,
                                });
                            }
                        }
                    }
                }
                break;
            }
        }
    }

    group_by
}

/// GetForeignUpperPaths callback
///
/// This callback is called by the PostgreSQL planner to create paths for
/// upper-level processing (aggregation, sorting, etc.) that can be pushed
/// down to the foreign server.
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
    debug2!("---> get_foreign_upper_paths, stage: {:?}", stage);

    // Only handle GROUP_AGG stage
    if stage != pg_sys::UpperRelationKind::UPPERREL_GROUP_AGG {
        return;
    }

    unsafe {
        // Get the FDW state from the input relation
        let fdw_private = (*input_rel).fdw_private;
        if fdw_private.is_null() {
            return;
        }

        let state = PgBox::<FdwState<E, W>>::from_pg(fdw_private as _);

        // Check if FDW supports any aggregates
        if let Some(ref instance) = state.instance {
            let supported = instance.supported_aggregates();
            if supported.is_empty() {
                return;
            }

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

            // Extract GROUP BY columns
            let group_by = extract_group_by_columns(root, input_rel);
            if !group_by.is_empty() {
                debug2!(
                    "Extracted GROUP BY columns: {:?}",
                    group_by.iter().map(|c| c.name.as_str()).collect::<Vec<_>>()
                );
            }

            // Check if GROUP BY is supported (if present)
            if !group_by.is_empty() && !instance.supports_group_by() {
                debug2!("GROUP BY not supported, skipping pushdown");
                return;
            }

            // Get cost estimates
            let (rows, _width) = {
                // We need a mutable reference, but we're in a const context
                // For now, use default estimates
                let rows = if group_by.is_empty() { 1 } else { 100 };
                let width = 8 * aggregates.len() as i32;
                (rows, width)
            };

            // Get startup cost from options
            let startup_cost = state
                .opts
                .get("startup_cost")
                .and_then(|c| c.parse::<f64>().ok())
                .unwrap_or(0.0);

            let total_cost = startup_cost + rows as f64;

            debug2!(
                "Aggregate pushdown cost estimate: rows={}, startup={}, total={}",
                rows,
                startup_cost,
                total_cost
            );

            // Create the foreign upper path
            // Note: The function signature differs across PostgreSQL versions:
            // - PG13-16: 9 parameters (no disabled_nodes, no fdw_restrictinfo)
            // - PG17: 10 parameters (added fdw_restrictinfo)
            // - PG18: 11 parameters (added disabled_nodes)
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
                ptr::null_mut(),         // fdw_private
            );

            // Add the path to the output relation
            pg_sys::add_path(output_rel, &mut ((*path).path));

            debug2!(
                "Created aggregate pushdown path: {} aggregates, {} group by columns",
                aggregates.len(),
                group_by.len()
            );
        }
    }
}
