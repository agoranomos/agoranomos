-- The journals of the core (ADR-0035, ADR-0036, ADR-0046): the loads that a run of the transform
-- pins and accepts, the accounts that a load leaves in quarantine, the totals that a platform
-- gives for an account and a day, and the old values of the rows of the facts of statistics that a
-- load replaced. The journals of the loads, of quarantine and of the totals keep the natural keys
-- of a load, source and account_id, as an account in quarantine has no row in core.dim_account;
-- a journal of restatements keeps the columns of its fact. No journal has a foreign key: the drop
-- of a month of a journal with one would take ACCESS EXCLUSIVE on the table that it points at
-- (#189). The journals hold no row yet: dbt is to write them in v0.5. transformer may read and
-- change their rows, and loader may read core.loads, to start the window of an account after its
-- last accepted day (ADR-0038).

-- The schema of #196, by load and account, as one load can be accepted for some accounts and wait
-- in quarantine for others. It has no partitions: its key holds no date, and a key with one would
-- break the ON CONFLICT of the pin. The maintenance deletes the rows past 13 months (ADR-0046).
CREATE TABLE core.loads (
  load_sk integer GENERATED ALWAYS AS IDENTITY UNIQUE,
  source text NOT NULL,
  load_id text NOT NULL,
  account_id text NOT NULL,
  window_from date,
  window_to date,
  outcome text NOT NULL CHECK (outcome IN ('loaded', 'failed')),
  error text,
  finished_at timestamptz NOT NULL,
  invocation_id uuid NOT NULL,
  pinned_at timestamptz NOT NULL DEFAULT now(),
  status text NOT NULL DEFAULT 'pinned' CHECK (status IN ('pinned', 'accepted')),
  accepted_at timestamptz,
  PRIMARY KEY (source, load_id, account_id),
  CHECK (outcome = 'failed' OR (window_from IS NOT NULL AND window_to IS NOT NULL))
);
CREATE INDEX loads_pinned ON core.loads (source, account_id) WHERE status = 'pinned';
CREATE INDEX loads_last_accepted ON core.loads (source, account_id, window_to DESC)
  WHERE status = 'accepted';

CREATE TABLE core.account_quarantine (
  source text NOT NULL,
  load_id text NOT NULL,
  account_id text NOT NULL,
  reason text NOT NULL,
  detail text,
  window_from date,
  window_to date,
  invocation_id uuid NOT NULL,
  found_at timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (source, load_id, account_id, reason)
);

CREATE TABLE core.platform_totals_daily (
  source text NOT NULL,
  account_id text NOT NULL,
  report_date date NOT NULL,
  impressions bigint NOT NULL,
  clicks bigint NOT NULL,
  spend numeric(18,6) NOT NULL,
  currency char(3) NOT NULL,
  load_id text NOT NULL,
  PRIMARY KEY (source, account_id, report_date)
);

-- The journals of restatements: the columns of their fact, in its order, then those of the
-- replacement, so that a row of a fact goes in as it is. They are partitioned by month of
-- replaced_at, the time of the transaction, so that core.drop_partition takes a whole month past
-- the retention of 13 months (ADR-0044). They have no key and no index: they are written in the
-- transaction of a window, and read seldom.
CREATE TABLE core.ad_stats_campaign_daily_restatements (
  account_sk integer NOT NULL,
  campaign_id bigint NOT NULL,
  report_date date NOT NULL,
  impressions bigint NOT NULL,
  clicks bigint NOT NULL,
  spend numeric(18,6) NOT NULL,
  conversions numeric(18,6),
  all_conversions numeric(18,6),
  conversion_value numeric(18,6),
  all_conversion_value numeric(18,6),
  currency char(3) NOT NULL,
  load_sk integer NOT NULL,
  new_load_sk integer NOT NULL,
  replaced_at timestamptz NOT NULL,
  is_vanished boolean NOT NULL
) PARTITION BY RANGE (replaced_at);

CREATE TABLE core.ad_stats_ad_group_daily_restatements (
  account_sk integer NOT NULL,
  campaign_id bigint NOT NULL,
  ad_group_id bigint NOT NULL,
  report_date date NOT NULL,
  impressions bigint NOT NULL,
  clicks bigint NOT NULL,
  spend numeric(18,6) NOT NULL,
  conversions numeric(18,6),
  all_conversions numeric(18,6),
  conversion_value numeric(18,6),
  all_conversion_value numeric(18,6),
  currency char(3) NOT NULL,
  load_sk integer NOT NULL,
  new_load_sk integer NOT NULL,
  replaced_at timestamptz NOT NULL,
  is_vanished boolean NOT NULL
) PARTITION BY RANGE (replaced_at);

CREATE TABLE core.ad_stats_ad_daily_restatements (
  account_sk integer NOT NULL,
  campaign_id bigint NOT NULL,
  ad_group_id bigint NOT NULL,
  ad_id bigint NOT NULL,
  report_date date NOT NULL,
  impressions bigint NOT NULL,
  clicks bigint NOT NULL,
  spend numeric(18,6) NOT NULL,
  conversions numeric(18,6),
  all_conversions numeric(18,6),
  conversion_value numeric(18,6),
  all_conversion_value numeric(18,6),
  currency char(3) NOT NULL,
  load_sk integer NOT NULL,
  new_load_sk integer NOT NULL,
  replaced_at timestamptz NOT NULL,
  is_vanished boolean NOT NULL
) PARTITION BY RANGE (replaced_at);

GRANT SELECT, INSERT, UPDATE, DELETE
  ON core.loads, core.account_quarantine, core.platform_totals_daily,
    core.ad_stats_campaign_daily_restatements, core.ad_stats_ad_group_daily_restatements,
    core.ad_stats_ad_daily_restatements
  TO transformer;
GRANT MAINTAIN
  ON core.ad_stats_campaign_daily_restatements, core.ad_stats_ad_group_daily_restatements,
    core.ad_stats_ad_daily_restatements
  TO transformer;
GRANT SELECT ON core.loads TO loader;

COMMENT ON TABLE core.loads IS
'The journal of loads: one row for each load of a source and each account in it, which a run of the transform pinned before it read the load, and which it marks accepted once the core holds the window of the account. A failed account has a row with no window. source, load_id and account_id are its key, and load_sk is the reference of the facts.';
COMMENT ON COLUMN core.loads.load_sk IS
'The surrogate key of the load of the account, from an identity, which each row of a fact that the load wrote carries.';
COMMENT ON COLUMN core.loads.source IS
'The source of the load, the schema of the raw layer without its prefix raw_, such as google_ads or demo_search.';
COMMENT ON COLUMN core.loads.load_id IS
'The ID of the load, _dlt_load_id of dlt: a time in seconds as text, whose fractions can have different lengths, so that it compares as a number.';
COMMENT ON COLUMN core.loads.account_id IS
'The ID of the account as the platform gives it, as text, account_id of core.dim_account in the platform of the source.';
COMMENT ON COLUMN core.loads.window_from IS
'The first day of the window that the load took for the account; none for a failed account.';
COMMENT ON COLUMN core.loads.window_to IS
'The last day of the window that the load took for the account; none for a failed account.';
COMMENT ON COLUMN core.loads.outcome IS
'How the load ended for the account: loaded, with its window, or failed, with its error.';
COMMENT ON COLUMN core.loads.error IS
'The error of a failed account, as the connector wrote it, which leaves secrets out; none for a loaded account.';
COMMENT ON COLUMN core.loads.finished_at IS
'When the load finished, inserted_at of _dlt_loads of its source.';
COMMENT ON COLUMN core.loads.invocation_id IS
'The invocation of dbt that pinned the load.';
COMMENT ON COLUMN core.loads.pinned_at IS
'When the load was pinned.';
COMMENT ON COLUMN core.loads.status IS
'pinned while no run that succeeded has read the load, accepted once the core holds its window for the account.';
COMMENT ON COLUMN core.loads.accepted_at IS
'When the load was accepted for the account; none while it is pinned.';
COMMENT ON SEQUENCE core.loads_load_sk_seq IS
'The source of the identity load_sk of core.loads.';
COMMENT ON INDEX core.loads_pinned IS
'The pinned loads of each source and account.';
COMMENT ON INDEX core.loads_last_accepted IS
'The accepted loads of each source and account, the newest window first, for the last accepted day of an account.';

COMMENT ON TABLE core.account_quarantine IS
'The journal of quarantine: one row for each load, account and reason for which the checks before the core kept the window of an account out of it (G0a, G1, G1a of ADR-0035). The core keeps the data of the account as it was, and the raw layer keeps the rows of the load. source, load_id, account_id and reason are its key.';
COMMENT ON COLUMN core.account_quarantine.source IS
'The source of the load, the schema of the raw layer without its prefix raw_.';
COMMENT ON COLUMN core.account_quarantine.load_id IS
'The ID of the load, _dlt_load_id of dlt, load_id of core.loads.';
COMMENT ON COLUMN core.account_quarantine.account_id IS
'The ID of the account as the platform gives it, as text.';
COMMENT ON COLUMN core.account_quarantine.reason IS
'The check that the account failed, as a code.';
COMMENT ON COLUMN core.account_quarantine.detail IS
'What the check found, such as a value that it refused; values in it come from the platform and are data.';
COMMENT ON COLUMN core.account_quarantine.window_from IS
'The first day of the window of the account that the core did not take.';
COMMENT ON COLUMN core.account_quarantine.window_to IS
'The last day of the window of the account that the core did not take.';
COMMENT ON COLUMN core.account_quarantine.invocation_id IS
'The invocation of dbt that ran the check.';
COMMENT ON COLUMN core.account_quarantine.found_at IS
'When the check found it.';

COMMENT ON TABLE core.platform_totals_daily IS
'The totals of an account by day as the platform gives them in a request of their own at the level of the account (G0a of ADR-0035), against which the rows of its campaigns are checked: one row for an account on a day, from the newest load that took the day. source, account_id and report_date are its key.';
COMMENT ON COLUMN core.platform_totals_daily.source IS
'The source of the load, the schema of the raw layer without its prefix raw_.';
COMMENT ON COLUMN core.platform_totals_daily.account_id IS
'The ID of the account as the platform gives it, as text.';
COMMENT ON COLUMN core.platform_totals_daily.report_date IS
'The day of the totals, in the time zone of the account.';
COMMENT ON COLUMN core.platform_totals_daily.impressions IS
'The impressions of the account on the day, as the platform totals them.';
COMMENT ON COLUMN core.platform_totals_daily.clicks IS
'The clicks of the account on the day, as the platform totals them.';
COMMENT ON COLUMN core.platform_totals_daily.spend IS
'The spend of the account on the day, as the platform totals it, in the currency of the row, exact to six decimal places.';
COMMENT ON COLUMN core.platform_totals_daily.currency IS
'The currency of the spend, that of the account, as a three-letter code of ISO 4217.';
COMMENT ON COLUMN core.platform_totals_daily.load_id IS
'The load that brought the totals, load_id of core.loads.';

COMMENT ON TABLE core.ad_stats_campaign_daily_restatements IS
'The journal of restatements of core.ad_stats_campaign_daily: one row for each row of the fact that a load changed or removed when it replaced the days of its window, with its old values, the load that had written it and the one that replaced it. Partitioned by month of replaced_at, and kept 13 months.';
COMMENT ON TABLE core.ad_stats_ad_group_daily_restatements IS
'The journal of restatements of core.ad_stats_ad_group_daily: one row for each row of the fact that a load changed or removed when it replaced the days of its window, with its old values, the load that had written it and the one that replaced it. Partitioned by month of replaced_at, and kept 13 months.';
COMMENT ON TABLE core.ad_stats_ad_daily_restatements IS
'The journal of restatements of core.ad_stats_ad_daily: one row for each row of the fact that a load changed or removed when it replaced the days of its window, with its old values, the load that had written it and the one that replaced it. Partitioned by month of replaced_at, and kept 13 months.';

COMMENT ON COLUMN core.ad_stats_campaign_daily_restatements.account_sk IS
'The account of the row, account_sk of core.dim_account.';
COMMENT ON COLUMN core.ad_stats_campaign_daily_restatements.campaign_id IS
'The ID of the campaign of the row, as in its fact.';
COMMENT ON COLUMN core.ad_stats_campaign_daily_restatements.report_date IS
'The day of the row of the fact, in the time zone of its account.';
COMMENT ON COLUMN core.ad_stats_campaign_daily_restatements.impressions IS
'The impressions of the row before the replacement.';
COMMENT ON COLUMN core.ad_stats_campaign_daily_restatements.clicks IS
'The clicks of the row before the replacement.';
COMMENT ON COLUMN core.ad_stats_campaign_daily_restatements.spend IS
'The spend of the row before the replacement, in the currency of the row.';
COMMENT ON COLUMN core.ad_stats_campaign_daily_restatements.conversions IS
'The conversions of the row before the replacement.';
COMMENT ON COLUMN core.ad_stats_campaign_daily_restatements.all_conversions IS
'All the conversions of the row before the replacement.';
COMMENT ON COLUMN core.ad_stats_campaign_daily_restatements.conversion_value IS
'The value of the conversions of the row before the replacement, in the currency of the row.';
COMMENT ON COLUMN core.ad_stats_campaign_daily_restatements.all_conversion_value IS
'The value of all the conversions of the row before the replacement, in the currency of the row.';
COMMENT ON COLUMN core.ad_stats_campaign_daily_restatements.currency IS
'The currency of the amounts of the row, as a three-letter code of ISO 4217.';
COMMENT ON COLUMN core.ad_stats_campaign_daily_restatements.load_sk IS
'The load that had written the row, load_sk of core.loads.';
COMMENT ON COLUMN core.ad_stats_campaign_daily_restatements.new_load_sk IS
'The load whose window replaced the row, load_sk of core.loads.';
COMMENT ON COLUMN core.ad_stats_campaign_daily_restatements.replaced_at IS
'When the row was replaced: the start of the transaction that replaced the window. Its month in UTC is the partition of the row.';
COMMENT ON COLUMN core.ad_stats_campaign_daily_restatements.is_vanished IS
'Whether the load that replaced the window had no row for the key, so that the row left the fact; false for a row whose values changed.';

COMMENT ON COLUMN core.ad_stats_ad_group_daily_restatements.account_sk IS
'The account of the row, account_sk of core.dim_account.';
COMMENT ON COLUMN core.ad_stats_ad_group_daily_restatements.campaign_id IS
'The ID of the campaign of the row, as in its fact.';
COMMENT ON COLUMN core.ad_stats_ad_group_daily_restatements.ad_group_id IS
'The ID of the ad group of the row, as in its fact.';
COMMENT ON COLUMN core.ad_stats_ad_group_daily_restatements.report_date IS
'The day of the row of the fact, in the time zone of its account.';
COMMENT ON COLUMN core.ad_stats_ad_group_daily_restatements.impressions IS
'The impressions of the row before the replacement.';
COMMENT ON COLUMN core.ad_stats_ad_group_daily_restatements.clicks IS
'The clicks of the row before the replacement.';
COMMENT ON COLUMN core.ad_stats_ad_group_daily_restatements.spend IS
'The spend of the row before the replacement, in the currency of the row.';
COMMENT ON COLUMN core.ad_stats_ad_group_daily_restatements.conversions IS
'The conversions of the row before the replacement.';
COMMENT ON COLUMN core.ad_stats_ad_group_daily_restatements.all_conversions IS
'All the conversions of the row before the replacement.';
COMMENT ON COLUMN core.ad_stats_ad_group_daily_restatements.conversion_value IS
'The value of the conversions of the row before the replacement, in the currency of the row.';
COMMENT ON COLUMN core.ad_stats_ad_group_daily_restatements.all_conversion_value IS
'The value of all the conversions of the row before the replacement, in the currency of the row.';
COMMENT ON COLUMN core.ad_stats_ad_group_daily_restatements.currency IS
'The currency of the amounts of the row, as a three-letter code of ISO 4217.';
COMMENT ON COLUMN core.ad_stats_ad_group_daily_restatements.load_sk IS
'The load that had written the row, load_sk of core.loads.';
COMMENT ON COLUMN core.ad_stats_ad_group_daily_restatements.new_load_sk IS
'The load whose window replaced the row, load_sk of core.loads.';
COMMENT ON COLUMN core.ad_stats_ad_group_daily_restatements.replaced_at IS
'When the row was replaced: the start of the transaction that replaced the window. Its month in UTC is the partition of the row.';
COMMENT ON COLUMN core.ad_stats_ad_group_daily_restatements.is_vanished IS
'Whether the load that replaced the window had no row for the key, so that the row left the fact; false for a row whose values changed.';

COMMENT ON COLUMN core.ad_stats_ad_daily_restatements.account_sk IS
'The account of the row, account_sk of core.dim_account.';
COMMENT ON COLUMN core.ad_stats_ad_daily_restatements.campaign_id IS
'The ID of the campaign of the row, as in its fact.';
COMMENT ON COLUMN core.ad_stats_ad_daily_restatements.ad_group_id IS
'The ID of the ad group of the row, as in its fact.';
COMMENT ON COLUMN core.ad_stats_ad_daily_restatements.ad_id IS
'The ID of the ad of the row, as in its fact.';
COMMENT ON COLUMN core.ad_stats_ad_daily_restatements.report_date IS
'The day of the row of the fact, in the time zone of its account.';
COMMENT ON COLUMN core.ad_stats_ad_daily_restatements.impressions IS
'The impressions of the row before the replacement.';
COMMENT ON COLUMN core.ad_stats_ad_daily_restatements.clicks IS
'The clicks of the row before the replacement.';
COMMENT ON COLUMN core.ad_stats_ad_daily_restatements.spend IS
'The spend of the row before the replacement, in the currency of the row.';
COMMENT ON COLUMN core.ad_stats_ad_daily_restatements.conversions IS
'The conversions of the row before the replacement.';
COMMENT ON COLUMN core.ad_stats_ad_daily_restatements.all_conversions IS
'All the conversions of the row before the replacement.';
COMMENT ON COLUMN core.ad_stats_ad_daily_restatements.conversion_value IS
'The value of the conversions of the row before the replacement, in the currency of the row.';
COMMENT ON COLUMN core.ad_stats_ad_daily_restatements.all_conversion_value IS
'The value of all the conversions of the row before the replacement, in the currency of the row.';
COMMENT ON COLUMN core.ad_stats_ad_daily_restatements.currency IS
'The currency of the amounts of the row, as a three-letter code of ISO 4217.';
COMMENT ON COLUMN core.ad_stats_ad_daily_restatements.load_sk IS
'The load that had written the row, load_sk of core.loads.';
COMMENT ON COLUMN core.ad_stats_ad_daily_restatements.new_load_sk IS
'The load whose window replaced the row, load_sk of core.loads.';
COMMENT ON COLUMN core.ad_stats_ad_daily_restatements.replaced_at IS
'When the row was replaced: the start of the transaction that replaced the window. Its month in UTC is the partition of the row.';
COMMENT ON COLUMN core.ad_stats_ad_daily_restatements.is_vanished IS
'Whether the load that replaced the window had no row for the key, so that the row left the fact; false for a row whose values changed.';

-- The partitions of the month in which this migration runs and of the two after it, in UTC, last,
-- as for the facts: the maintenance makes the months ahead (ADR-0039).
DO $$
DECLARE
  v_parent regclass;
  v_month date;
BEGIN
  FOREACH v_parent IN ARRAY ARRAY[
    'core.ad_stats_campaign_daily_restatements', 'core.ad_stats_ad_group_daily_restatements',
    'core.ad_stats_ad_daily_restatements'
  ]::regclass[] LOOP
    FOR v_month IN
      SELECT (date_trunc('month', now() AT TIME ZONE 'UTC') + months * interval '1 month')::date
      FROM generate_series(0, 2) AS months
    LOOP
      PERFORM core.ensure_partition(v_parent, v_month);
    END LOOP;
  END LOOP;
END
$$;
