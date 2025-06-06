pub(super) mod v1;
pub(super) mod v2;

// 'pg epoch' (2000-01-01 00:00:00) in macroseconds and seconds
const PG_EPOCH_MS: i64 = 946_684_800_000_000;
const PG_EPOCH_SEC: i64 = 946_684_800;
