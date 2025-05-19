use crate::interface::Limit;
use pgrx::{is_a, pg_sys, FromDatum};

// extract limit
pub(crate) unsafe fn extract_limit(
    root: *mut pg_sys::PlannerInfo,
    _baserel: *mut pg_sys::RelOptInfo,
    _baserel_id: pg_sys::Oid,
) -> Option<Limit> {
    let parse = (*root).parse;

    // don't push down LIMIT if the query has a GROUP BY clause or aggregates
    if !(*parse).groupClause.is_null() || (*parse).hasAggs {
        return None;
    }

    // only push down constant LIMITs that are not NULL
    let limit_count = (*parse).limitCount as *mut pg_sys::Const;
    if limit_count.is_null() || !is_a(limit_count as *mut pg_sys::Node, pg_sys::NodeTag::T_Const) {
        return None;
    }

    let mut limit = Limit::default();

    if let Some(count) = i64::from_polymorphic_datum(
        (*limit_count).constvalue,
        (*limit_count).constisnull,
        (*limit_count).consttype,
    ) {
        limit.count = count;
    } else {
        return None;
    }

    // only consider OFFSETS that are non-NULL constants
    let limit_offset = (*parse).limitOffset as *mut pg_sys::Const;
    if !limit_offset.is_null() && is_a(limit_offset as *mut pg_sys::Node, pg_sys::NodeTag::T_Const)
    {
        if let Some(offset) = i64::from_polymorphic_datum(
            (*limit_offset).constvalue,
            (*limit_offset).constisnull,
            (*limit_offset).consttype,
        ) {
            limit.offset = offset;
        }
    }

    Some(limit)
}
