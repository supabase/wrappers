//! Pagination state tracking and loop detection

/// A pagination token: either a cursor string or a full/partial URL.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum PaginationToken {
    /// Token-based pagination (e.g., Stripe next_cursor)
    Cursor(String),
    /// Link-based pagination (e.g., GitHub Link header, HAL _links)
    Url(String),
}

impl PaginationToken {
    /// Returns the inner cursor string, or None if this is a URL.
    pub(crate) fn as_cursor(&self) -> Option<&str> {
        match self {
            Self::Cursor(s) => Some(s),
            Self::Url(_) => None,
        }
    }

    /// Returns the inner URL string, or None if this is a cursor.
    pub(crate) fn as_url(&self) -> Option<&str> {
        match self {
            Self::Url(s) => Some(s),
            Self::Cursor(_) => None,
        }
    }
}

/// Tracks pagination state across pages within a single scan.
///
/// Detects infinite loops (duplicate token) and enforces page limits.
#[derive(Debug, Default)]
pub(crate) struct PaginationState {
    /// Token for the next page (cursor or URL)
    pub(crate) next: Option<PaginationToken>,
    /// Token from the previous page (for loop detection)
    pub(crate) previous: Option<PaginationToken>,
    /// Number of pages fetched so far
    pub(crate) pages_fetched: usize,
}

impl PaginationState {
    /// Reset all pagination state for a new scan.
    pub(crate) fn reset(&mut self) {
        self.next = None;
        self.previous = None;
        self.pages_fetched = 0;
    }

    /// Returns true when there are no more pages to fetch.
    pub(crate) fn is_exhausted(&self) -> bool {
        self.next.is_none()
    }

    /// Detect a pagination loop (duplicate token).
    ///
    /// Returns a human-readable reason if a loop is detected.
    pub(crate) fn detect_loop(&self) -> Option<&'static str> {
        match (&self.next, &self.previous) {
            (Some(PaginationToken::Cursor(n)), Some(PaginationToken::Cursor(p))) if n == p => {
                Some("duplicate cursor detected (possible infinite loop)")
            }
            (Some(PaginationToken::Url(n)), Some(PaginationToken::Url(p))) if n == p => {
                Some("duplicate URL detected (possible infinite loop)")
            }
            _ => None,
        }
    }

    /// Returns true if the page limit has been reached.
    pub(crate) fn exceeds_limit(&self, max_pages: usize) -> bool {
        self.pages_fetched >= max_pages
    }

    /// Save current next value as previous (for loop detection) and increment page count.
    ///
    /// Call this before fetching each subsequent page.
    pub(crate) fn advance(&mut self) {
        self.previous = self.next.clone();
        self.pages_fetched += 1;
    }

    /// Record the first page after initial make_request in begin_scan.
    ///
    /// Only sets pages_fetched = 1. Does NOT copy next into previous --
    /// there was no token sent for the first page, so previous must stay
    /// None to avoid a false-positive loop detection.
    pub(crate) fn record_first_page(&mut self) {
        self.pages_fetched = 1;
    }

    /// Clear next-page token (e.g., on 404 or empty response).
    pub(crate) fn clear_next(&mut self) {
        self.next = None;
    }
}

#[cfg(test)]
#[path = "pagination_tests.rs"]
mod tests;
