-- SQL statements are intended to go before all other generated SQL.

DROP TABLE IF EXISTS wrappers_fdw_stats;

CREATE TABLE wrappers_fdw_stats (
  fdw_name          text NOT NULL PRIMARY KEY,
  create_times      bigint NULL,
  rows_in           bigint NULL,
  rows_out          bigint NULL,
  bytes_in          bigint NULL,
  bytes_out         bigint NULL,
  metadata          jsonb NULL,
  created_at        timestamptz NOT NULL DEFAULT timezone('utc'::text, now()),
  updated_at        timestamptz NOT NULL DEFAULT timezone('utc'::text, now())
);

COMMENT ON TABLE wrappers_fdw_stats IS 'Wrappers Foreign Data Wrapper statistics';
COMMENT ON COLUMN wrappers_fdw_stats.create_times IS 'Total number of times the FDW instacne has been created';
COMMENT ON COLUMN wrappers_fdw_stats.rows_in IS 'Total rows input from origin';
COMMENT ON COLUMN wrappers_fdw_stats.rows_out IS 'Total rows output to Postgres';
COMMENT ON COLUMN wrappers_fdw_stats.bytes_in IS 'Total bytes input from origin';
COMMENT ON COLUMN wrappers_fdw_stats.bytes_out IS 'Total bytes output to Postgres';
COMMENT ON COLUMN wrappers_fdw_stats.metadata IS 'Metadata specific for the FDW';

