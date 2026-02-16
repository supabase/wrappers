use super::{PaginationState, PaginationToken};

// --- Default state ---

#[test]
fn test_default_state_is_exhausted() {
    let state = PaginationState::default();
    assert!(state.is_exhausted());
}

#[test]
fn test_default_state_no_loop() {
    let state = PaginationState::default();
    assert!(state.detect_loop().is_none());
}

#[test]
fn test_default_state_zero_pages() {
    let state = PaginationState::default();
    assert_eq!(state.pages_fetched, 0);
}

// --- is_exhausted ---

#[test]
fn test_not_exhausted_with_cursor() {
    let state = PaginationState {
        next: Some(PaginationToken::Cursor("abc".to_string())),
        ..Default::default()
    };
    assert!(!state.is_exhausted());
}

#[test]
fn test_not_exhausted_with_url() {
    let state = PaginationState {
        next: Some(PaginationToken::Url(
            "https://api.example.com/items?page=2".to_string(),
        )),
        ..Default::default()
    };
    assert!(!state.is_exhausted());
}

#[test]
fn test_exhausted_when_none() {
    let state = PaginationState {
        next: None,
        previous: Some(PaginationToken::Cursor("old".to_string())), // prev doesn't matter
        pages_fetched: 5,
    };
    assert!(state.is_exhausted());
}

// --- detect_loop ---

#[test]
fn test_detect_loop_duplicate_cursor() {
    let state = PaginationState {
        next: Some(PaginationToken::Cursor("cursor_abc".to_string())),
        previous: Some(PaginationToken::Cursor("cursor_abc".to_string())),
        ..Default::default()
    };
    let reason = state.detect_loop();
    assert!(reason.is_some());
    assert!(reason.unwrap().contains("duplicate cursor"));
}

#[test]
fn test_detect_loop_duplicate_url() {
    let state = PaginationState {
        next: Some(PaginationToken::Url(
            "https://api.example.com/items?page=2".to_string(),
        )),
        previous: Some(PaginationToken::Url(
            "https://api.example.com/items?page=2".to_string(),
        )),
        ..Default::default()
    };
    let reason = state.detect_loop();
    assert!(reason.is_some());
    assert!(reason.unwrap().contains("duplicate URL"));
}

#[test]
fn test_no_loop_different_cursors() {
    let state = PaginationState {
        next: Some(PaginationToken::Cursor("cursor_2".to_string())),
        previous: Some(PaginationToken::Cursor("cursor_1".to_string())),
        ..Default::default()
    };
    assert!(state.detect_loop().is_none());
}

#[test]
fn test_no_loop_different_urls() {
    let state = PaginationState {
        next: Some(PaginationToken::Url(
            "https://api.example.com/items?page=3".to_string(),
        )),
        previous: Some(PaginationToken::Url(
            "https://api.example.com/items?page=2".to_string(),
        )),
        ..Default::default()
    };
    assert!(state.detect_loop().is_none());
}

#[test]
fn test_no_loop_when_next_none() {
    // None next should never be a loop
    let state = PaginationState {
        next: None,
        previous: None,
        ..Default::default()
    };
    assert!(state.detect_loop().is_none());
}

#[test]
fn test_no_loop_cursor_set_prev_none() {
    // First page: cursor set but no previous yet
    let state = PaginationState {
        next: Some(PaginationToken::Cursor("cursor_1".to_string())),
        previous: None,
        ..Default::default()
    };
    assert!(state.detect_loop().is_none());
}

#[test]
fn test_no_loop_url_set_prev_none() {
    let state = PaginationState {
        next: Some(PaginationToken::Url(
            "https://api.example.com/items?page=2".to_string(),
        )),
        previous: None,
        ..Default::default()
    };
    assert!(state.detect_loop().is_none());
}

#[test]
fn test_no_loop_different_token_types() {
    // Cursor vs URL should never match as a loop
    let state = PaginationState {
        next: Some(PaginationToken::Cursor("same_value".to_string())),
        previous: Some(PaginationToken::Url("same_value".to_string())),
        ..Default::default()
    };
    assert!(state.detect_loop().is_none());
}

// --- exceeds_limit ---

#[test]
fn test_exceeds_limit_at_boundary() {
    let state = PaginationState {
        pages_fetched: 10,
        ..Default::default()
    };
    assert!(state.exceeds_limit(10));
}

#[test]
fn test_exceeds_limit_over() {
    let state = PaginationState {
        pages_fetched: 15,
        ..Default::default()
    };
    assert!(state.exceeds_limit(10));
}

#[test]
fn test_does_not_exceed_limit_under() {
    let state = PaginationState {
        pages_fetched: 9,
        ..Default::default()
    };
    assert!(!state.exceeds_limit(10));
}

#[test]
fn test_does_not_exceed_limit_zero_pages() {
    let state = PaginationState::default();
    assert!(!state.exceeds_limit(10));
}

#[test]
fn test_exceeds_limit_max_pages_one() {
    let state = PaginationState {
        pages_fetched: 1,
        ..Default::default()
    };
    assert!(state.exceeds_limit(1));
}

#[test]
fn test_exceeds_limit_zero_max_pages() {
    // max_pages=0 means every page count exceeds the limit
    let state = PaginationState::default();
    assert!(state.exceeds_limit(0));
}

// --- reset ---

#[test]
fn test_reset_clears_all_state() {
    let mut state = PaginationState {
        next: Some(PaginationToken::Cursor("cursor_5".to_string())),
        previous: Some(PaginationToken::Cursor("cursor_4".to_string())),
        pages_fetched: 5,
    };
    state.reset();
    assert!(state.next.is_none());
    assert!(state.previous.is_none());
    assert_eq!(state.pages_fetched, 0);
}

#[test]
fn test_reset_already_default() {
    let mut state = PaginationState::default();
    state.reset(); // should be idempotent
    assert!(state.is_exhausted());
    assert_eq!(state.pages_fetched, 0);
}

#[test]
fn test_reset_makes_exhausted() {
    let mut state = PaginationState {
        next: Some(PaginationToken::Cursor("abc".to_string())),
        pages_fetched: 3,
        ..Default::default()
    };
    assert!(!state.is_exhausted());
    state.reset();
    assert!(state.is_exhausted());
}

// --- advance ---

#[test]
fn test_advance_copies_cursor_to_prev() {
    let mut state = PaginationState {
        next: Some(PaginationToken::Cursor("cursor_2".to_string())),
        previous: None,
        pages_fetched: 1,
    };
    state.advance();
    assert_eq!(
        state.previous,
        Some(PaginationToken::Cursor("cursor_2".to_string()))
    );
    assert_eq!(
        state.next,
        Some(PaginationToken::Cursor("cursor_2".to_string()))
    ); // next unchanged
    assert_eq!(state.pages_fetched, 2);
}

#[test]
fn test_advance_copies_url_to_prev() {
    let mut state = PaginationState {
        next: Some(PaginationToken::Url(
            "https://api.example.com/items?page=3".to_string(),
        )),
        previous: None,
        pages_fetched: 1,
    };
    state.advance();
    assert_eq!(
        state.previous,
        Some(PaginationToken::Url(
            "https://api.example.com/items?page=3".to_string()
        ))
    );
    assert_eq!(state.pages_fetched, 2);
}

#[test]
fn test_advance_increments_page_count() {
    let mut state = PaginationState::default();
    assert_eq!(state.pages_fetched, 0);
    state.advance();
    assert_eq!(state.pages_fetched, 1);
    state.advance();
    assert_eq!(state.pages_fetched, 2);
    state.advance();
    assert_eq!(state.pages_fetched, 3);
}

#[test]
fn test_advance_overwrites_prev() {
    let mut state = PaginationState {
        next: Some(PaginationToken::Cursor("cursor_3".to_string())),
        previous: Some(PaginationToken::Cursor("cursor_1".to_string())),
        pages_fetched: 2,
    };
    state.advance();
    assert_eq!(
        state.previous,
        Some(PaginationToken::Cursor("cursor_3".to_string()))
    );
}

#[test]
fn test_advance_clears_prev_when_next_is_none() {
    let mut state = PaginationState {
        next: None,
        previous: Some(PaginationToken::Cursor("old_cursor".to_string())),
        pages_fetched: 1,
    };
    state.advance();
    assert!(state.previous.is_none());
    assert_eq!(state.pages_fetched, 2);
}

// --- record_first_page ---

#[test]
fn test_record_first_page_sets_count_to_one() {
    let mut state = PaginationState::default();
    state.record_first_page();
    assert_eq!(state.pages_fetched, 1);
}

#[test]
fn test_record_first_page_does_not_set_prev() {
    let mut state = PaginationState {
        next: Some(PaginationToken::Cursor("cursor_1".to_string())),
        ..Default::default()
    };
    state.record_first_page();
    // prev must stay None to avoid false loop detection on page 2
    assert!(state.previous.is_none());
    assert_eq!(state.pages_fetched, 1);
}

#[test]
fn test_record_first_page_preserves_next() {
    let mut state = PaginationState {
        next: Some(PaginationToken::Cursor("cursor_1".to_string())),
        ..Default::default()
    };
    state.record_first_page();
    assert_eq!(
        state.next,
        Some(PaginationToken::Cursor("cursor_1".to_string()))
    );
}

// --- clear_next ---

#[test]
fn test_clear_next_removes_token() {
    let mut state = PaginationState {
        next: Some(PaginationToken::Cursor("cursor_5".to_string())),
        previous: Some(PaginationToken::Cursor("cursor_4".to_string())),
        pages_fetched: 5,
    };
    state.clear_next();
    assert!(state.next.is_none());
    // prev and pages_fetched should be untouched
    assert_eq!(
        state.previous,
        Some(PaginationToken::Cursor("cursor_4".to_string()))
    );
    assert_eq!(state.pages_fetched, 5);
}

#[test]
fn test_clear_next_makes_exhausted() {
    let mut state = PaginationState {
        next: Some(PaginationToken::Cursor("abc".to_string())),
        ..Default::default()
    };
    assert!(!state.is_exhausted());
    state.clear_next();
    assert!(state.is_exhausted());
}

#[test]
fn test_clear_next_already_none() {
    let mut state = PaginationState::default();
    state.clear_next(); // should be idempotent
    assert!(state.is_exhausted());
}

// --- PaginationToken accessors ---

#[test]
fn test_token_as_cursor() {
    let token = PaginationToken::Cursor("abc".to_string());
    assert_eq!(token.as_cursor(), Some("abc"));
    assert_eq!(token.as_url(), None);
}

#[test]
fn test_token_as_url() {
    let token = PaginationToken::Url("https://example.com".to_string());
    assert_eq!(token.as_url(), Some("https://example.com"));
    assert_eq!(token.as_cursor(), None);
}

// --- State machine sequences ---

#[test]
fn test_full_pagination_lifecycle_cursor() {
    let mut state = PaginationState {
        next: Some(PaginationToken::Cursor("cursor_1".to_string())),
        ..Default::default()
    };

    // After begin_scan: API returns first page with cursor
    state.record_first_page();
    assert_eq!(state.pages_fetched, 1);
    assert!(!state.is_exhausted());
    assert!(state.detect_loop().is_none()); // prev is still None

    // Before page 2: advance saves cursor_1 as prev
    state.advance();
    assert_eq!(
        state.previous,
        Some(PaginationToken::Cursor("cursor_1".to_string()))
    );
    assert_eq!(state.pages_fetched, 2);

    // Page 2 returns new cursor
    state.next = Some(PaginationToken::Cursor("cursor_2".to_string()));
    assert!(state.detect_loop().is_none()); // cursor_2 != cursor_1

    // Before page 3: advance saves cursor_2 as prev
    state.advance();
    assert_eq!(
        state.previous,
        Some(PaginationToken::Cursor("cursor_2".to_string()))
    );
    assert_eq!(state.pages_fetched, 3);

    // Page 3 returns same cursor (loop!)
    state.next = Some(PaginationToken::Cursor("cursor_2".to_string()));
    assert!(state.detect_loop().is_some());

    // Reset for re_scan
    state.reset();
    assert!(state.is_exhausted());
    assert_eq!(state.pages_fetched, 0);
    assert!(state.detect_loop().is_none());
}

#[test]
fn test_full_pagination_lifecycle_url() {
    let mut state = PaginationState {
        next: Some(PaginationToken::Url(
            "https://api.example.com/items?page=2".to_string(),
        )),
        ..Default::default()
    };

    // First page: API returns next URL
    state.record_first_page();
    assert!(!state.is_exhausted());
    assert!(state.detect_loop().is_none());

    // Before page 2
    state.advance();
    assert_eq!(
        state.previous,
        Some(PaginationToken::Url(
            "https://api.example.com/items?page=2".to_string()
        ))
    );

    // Page 2: next URL points to page 3
    state.next = Some(PaginationToken::Url(
        "https://api.example.com/items?page=3".to_string(),
    ));
    assert!(state.detect_loop().is_none());

    // Before page 3
    state.advance();

    // Page 3: no next URL (last page)
    state.next = None;
    assert!(state.is_exhausted());
    assert!(state.detect_loop().is_none());
}

#[test]
fn test_page_limit_enforcement_in_lifecycle() {
    let mut state = PaginationState::default();
    let max_pages = 3;

    state.next = Some(PaginationToken::Cursor("c1".to_string()));
    state.record_first_page();
    assert!(!state.exceeds_limit(max_pages)); // 1 < 3

    state.advance();
    state.next = Some(PaginationToken::Cursor("c2".to_string()));
    assert!(!state.exceeds_limit(max_pages)); // 2 < 3

    state.advance();
    state.next = Some(PaginationToken::Cursor("c3".to_string()));
    assert!(state.exceeds_limit(max_pages)); // 3 >= 3
}

#[test]
fn test_clear_next_after_404() {
    let mut state = PaginationState {
        next: Some(PaginationToken::Cursor("c1".to_string())),
        ..Default::default()
    };
    state.record_first_page();
    state.advance();

    // Simulate 404 response: clear next token
    state.next = Some(PaginationToken::Cursor("c2".to_string()));
    state.clear_next();
    assert!(state.is_exhausted());
    // pages_fetched should still reflect actual fetches
    assert_eq!(state.pages_fetched, 2);
}

#[test]
fn test_reset_then_new_scan() {
    let mut state = PaginationState {
        next: Some(PaginationToken::Cursor("old".to_string())),
        previous: Some(PaginationToken::Cursor("older".to_string())),
        pages_fetched: 10,
    };

    state.reset();

    // Simulate new scan
    state.next = Some(PaginationToken::Url(
        "https://new-api.example.com/data?page=2".to_string(),
    ));
    state.record_first_page();
    assert_eq!(state.pages_fetched, 1);
    assert!(!state.is_exhausted());
    assert!(state.detect_loop().is_none());
}

// --- Edge cases ---

#[test]
fn test_empty_string_cursor_is_not_none() {
    // Empty string cursor is still Some, NOT exhausted
    let state = PaginationState {
        next: Some(PaginationToken::Cursor(String::new())),
        ..Default::default()
    };
    assert!(!state.is_exhausted());
}

#[test]
fn test_empty_string_cursor_duplicate_detection() {
    // Two empty string cursors should still be detected as a loop
    let state = PaginationState {
        next: Some(PaginationToken::Cursor(String::new())),
        previous: Some(PaginationToken::Cursor(String::new())),
        ..Default::default()
    };
    assert!(state.detect_loop().is_some());
}

#[test]
fn test_whitespace_cursors_are_distinct() {
    let state = PaginationState {
        next: Some(PaginationToken::Cursor(" ".to_string())),
        previous: Some(PaginationToken::Cursor("  ".to_string())),
        ..Default::default()
    };
    assert!(state.detect_loop().is_none());
}
