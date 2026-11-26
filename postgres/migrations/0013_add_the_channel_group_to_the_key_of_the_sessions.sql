-- The default channel group in the key of the sessions (ADR-0057). GA4 can split the sessions of
-- one source, medium and campaign of a day between two default channel groups, as it does for a
-- campaign of Google Ads that runs across networks, Cross-network and Display, and the report of
-- the load asks for the group among its six dimensions (ADR-0054), so such a day has two rows of
-- the key of 0005, which the core refused. The group joins the primary key of
-- core.web_sessions_daily, before report_date, as the other columns of the grain come between the
-- property and the day.
--
-- A column of a primary key is NOT NULL, and this one was nullable. GA4 gives the group
-- Unassigned to sessions that no rule of its channels matches, and (not set) to a dimension that
-- it has no value of; staging now writes (not set) for a row whose group is empty or missing (the
-- shape macro ga4_sessions_daily), and this migration gives the same value to each row that a
-- release before it wrote without a group, and touches no other row. The rows of a month before
-- the window of restatements are frozen (ADR-0035), so the migration lifts the freeze for its own
-- transaction, as a load of history does.
--
-- DROP CONSTRAINT and ADD PRIMARY KEY on the parent drop and build the index of the key on each
-- partition, which PostgreSQL 18.6 does in the transaction of the migration, with the parent and
-- its partitions locked ACCESS EXCLUSIVE until it commits; a partition that core.ensure_partition
-- makes later copies the new key (LIKE ... INCLUDING ALL). No grant changes.

SELECT set_config('agoranomos.thaw', pg_current_xact_id()::text, true) <> '' AS thawed;

UPDATE core.web_sessions_daily
SET session_default_channel_group = '(not set)'
WHERE session_default_channel_group IS NULL;

ALTER TABLE core.web_sessions_daily ALTER COLUMN session_default_channel_group SET NOT NULL;
ALTER TABLE core.web_sessions_daily DROP CONSTRAINT web_sessions_daily_pkey;
ALTER TABLE core.web_sessions_daily
  ADD CONSTRAINT web_sessions_daily_pkey PRIMARY KEY (
    account_sk, session_source, session_medium, session_campaign_name, session_campaign_id,
    session_default_channel_group, report_date
  );

COMMENT ON TABLE core.web_sessions_daily IS
'Sessions of web analytics by day, property, source, medium, campaign and default channel group: one row for the sessions of a property on a day, in the time zone of the property, with one source, medium, campaign and default channel group, and the revenue in the currency of the property. account_sk, session_source, session_medium, session_campaign_name, session_campaign_id, session_default_channel_group and report_date are its key, compared byte by byte. Partitioned by month of report_date; a month that ended before the window of restatements is frozen (the trigger freeze_the_months_before_the_window).';
COMMENT ON COLUMN core.web_sessions_daily.session_default_channel_group IS
'The channel of the sessions, by the rules of web analytics, such as Paid Search, Paid Social, Organic Search, Direct, or Unassigned for sessions that no rule matches; (not set) when web analytics gave no group. Part of the key: web analytics can split the sessions of one source, medium and campaign of a day between two groups, such as Cross-network and Display.';

-- The partitions that exist take the new comment of the column from their parent, as a partition
-- that core.ensure_partition makes later copies it. migrator owns them.
DO $$
DECLARE
  v_parent CONSTANT regclass := 'core.web_sessions_daily';
  v_comment text;
  v_schema name;
  v_name name;
BEGIN
  SELECT col_description(a.attrelid, a.attnum) INTO STRICT v_comment
  FROM pg_attribute AS a
  WHERE a.attrelid = v_parent AND a.attname = 'session_default_channel_group';
  FOR v_schema, v_name IN
    SELECT n.nspname, c.relname
    FROM pg_inherits AS i
    JOIN pg_class AS c ON c.oid = i.inhrelid
    JOIN pg_namespace AS n ON n.oid = c.relnamespace
    WHERE i.inhparent = v_parent
  LOOP
    EXECUTE format(
      'COMMENT ON COLUMN %I.%I.session_default_channel_group IS %L', v_schema, v_name, v_comment
    );
  END LOOP;
END
$$;
