/// Whether an aggregate can be pushed to the foreign server.
///
/// A base-scan qual that `extract_quals` does not turn into a [`Qual`] stays in
/// `baserestrictinfo` and Postgres rechecks it above the scan. A pushed
/// aggregate replaces that scan, so a clause that never became a qual is
/// dropped and the remote aggregate is wrong. `extract_quals` emits at most one
/// qual per restriction, so a shorter qual list means something was left
/// behind. Grouping sets are more than one group; pushing them as a single
/// group under-counts.
pub(crate) fn aggregate_pushdown_allowed(
    restrict_clauses: usize,
    extracted_quals: usize,
    has_grouping_sets: bool,
) -> bool {
    if has_grouping_sets {
        return false;
    }
    extracted_quals >= restrict_clauses
}

#[cfg(test)]
mod tests {
    use super::aggregate_pushdown_allowed;

    #[test]
    fn plain_where_and_no_filter_can_be_pushed() {
        assert!(aggregate_pushdown_allowed(0, 0, false));
        assert!(aggregate_pushdown_allowed(1, 1, false));
        assert!(aggregate_pushdown_allowed(2, 2, false));
    }

    #[test]
    fn unpushed_where_and_grouping_sets_are_not_pushed() {
        assert!(!aggregate_pushdown_allowed(1, 0, false));
        assert!(!aggregate_pushdown_allowed(2, 1, false));
        assert!(!aggregate_pushdown_allowed(0, 0, true));
        assert!(!aggregate_pushdown_allowed(1, 1, true));
    }
}
