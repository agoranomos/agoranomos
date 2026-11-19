-- The partial day (ADR-0051): a load of the kind intraday takes the current day of each account in
-- its time zone, and the day before while no whole load covers it, at the level of campaigns only,
-- every few hours, into the raw tables of its source, and the one transform takes it like any
-- load. Its rows in core.ad_stats_campaign_daily carry is_partial_day until a load of the kind
-- window or history takes the day and replaces them whole. The CHECK of core.loads.kind of 0008,
-- which PostgreSQL named loads_kind_check, takes the third kind. The column has a constant
-- default, which PostgreSQL 18 records in the catalog and does not write into the rows, so that
-- the partitions are not rewritten, and every row that a pin wrote before this migration is
-- whole. Neither is a right: the grants of 0005 and 0006 cover them.

ALTER TABLE core.loads DROP CONSTRAINT loads_kind_check;
ALTER TABLE core.loads
  ADD CONSTRAINT loads_kind_check CHECK (kind IN ('window', 'history', 'intraday'));

ALTER TABLE core.ad_stats_campaign_daily
  ADD COLUMN is_partial_day boolean NOT NULL DEFAULT false;

COMMENT ON TABLE core.loads IS
'The journal of loads: one row for each load of a source and each account in it, which a run of the transform pinned before it read the load, and which it marks accepted once the core holds the window of the account. A failed account has a row with no window. source, load_id and account_id are its key, and load_sk is the reference of the facts. kind tells the usual window of an account from a load of its history, whose rows of the account give the progress of the history, and from a load of its current day.';
COMMENT ON COLUMN core.loads.kind IS
'window for a load of the usual window of the account, history for a load of the days before it, which comes in the background in portions by quarter, from the newest to the oldest, or by hand with the parameters of a run, and intraday for a load of the current day of the account, and of the day before while no whole load covers it, every few hours, of the statistics of campaigns only. A whole load, of the kind window or history, takes a day before an intraday one, whatever their load IDs.';
COMMENT ON TABLE core.ad_stats_campaign_daily IS
'Advertising statistics by day and campaign of every ad platform: one row for a campaign of an account on a day, in the time zone of the account, as the platform reports its campaign level, with the amounts in the currency of the account. account_sk, campaign_id and report_date are its key. A day that only a load of the kind intraday took is partial (is_partial_day) until a whole load replaces it. Partitioned by month of report_date; a month that ended before the window of restatements is frozen (the trigger freeze_the_months_before_the_window).';
COMMENT ON COLUMN core.ad_stats_campaign_daily.is_partial_day IS
'Whether the row is of a day still in progress, as a load of the kind intraday read it at the moment of its load: impressions, clicks and spend so far, and no conversions. A load of the kind window or history replaces the day whole, with false, and the journal of restatements does not keep the partial row. The other facts have no partial day.';
COMMENT ON TABLE core.ad_stats_campaign_daily_restatements IS
'The journal of restatements of core.ad_stats_campaign_daily: one row for each row of the fact that a load changed or removed when it replaced the days of its window, with its old values, the load that had written it and the one that replaced it; a partial row of a day still in progress (is_partial_day) goes nowhere. Partitioned by month of replaced_at, and kept 13 months.';

-- The partitions that exist take the comment of the new column from their parent, as a partition
-- that core.ensure_partition makes later copies it: ADD COLUMN gives them the column and not its
-- comment. They are those of 0005 on a new warehouse, and every month of the facts on one that a
-- release before this one wrote. migrator owns them, as ensure_partition runs as migrator.
DO $$
DECLARE
  v_parent CONSTANT regclass := 'core.ad_stats_campaign_daily';
  v_comment text;
  v_schema name;
  v_name name;
BEGIN
  SELECT col_description(a.attrelid, a.attnum) INTO STRICT v_comment
  FROM pg_attribute AS a
  WHERE a.attrelid = v_parent AND a.attname = 'is_partial_day';
  FOR v_schema, v_name IN
    SELECT n.nspname, c.relname
    FROM pg_inherits AS i
    JOIN pg_class AS c ON c.oid = i.inhrelid
    JOIN pg_namespace AS n ON n.oid = c.relnamespace
    WHERE i.inhparent = v_parent
  LOOP
    EXECUTE format('COMMENT ON COLUMN %I.%I.is_partial_day IS %L', v_schema, v_name, v_comment);
  END LOOP;
END
$$;
