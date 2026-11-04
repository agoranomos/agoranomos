-- The facts of the core (ADR-0035, ADR-0036): narrow tables of one grain each, partitioned by month
-- of report_date, with the surrogate of the account, the IDs of the platform as bigint, the
-- measures as the platform reports them and the currency in every row. Ratios and converted
-- amounts are not stored: the views compute them. Each fact has
--   * a primary key that ends in report_date, which a key of a partitioned table must hold, and an
--     index on (account_sk, report_date), for the days of an account;
--   * CHECK >= 0 on its counters and amounts, but for the revenue of web analytics, which the
--     refunds of a day can make negative ("minus refunded transaction revenue", the API schema of
--     the GA4 Data API, read on 3 November 2026);
--   * a foreign key to core.dim_account, its one foreign key: a journal has none, as the drop of a
--     partition with one takes ACCESS EXCLUSIVE on core.dim_account, which the dashboards read (#189);
--   * no DEFAULT partition, so that a row of a month without a partition fails loudly (ADR-0036);
--   * the trigger of the freeze, below.
-- The facts hold no row yet: dbt is to write them in v0.5, by whole days of an account.
-- transformer may read, insert and delete rows, and neither update, truncate nor drop a table:
-- it does not own them (ADR-0035). It has MAINTAIN on each parent, without which ANALYZE ONLY
-- skips the table with a WARNING and succeeds (#189), and core.ensure_partition gives it MAINTAIN
-- on each partition, for VACUUM.

CREATE TABLE core.ad_stats_campaign_daily (
  account_sk integer NOT NULL REFERENCES core.dim_account (account_sk),
  campaign_id bigint NOT NULL,
  report_date date NOT NULL,
  impressions bigint NOT NULL CHECK (impressions >= 0),
  clicks bigint NOT NULL CHECK (clicks >= 0),
  spend numeric(18,6) NOT NULL CHECK (spend >= 0),
  conversions numeric(18,6) CHECK (conversions >= 0),
  all_conversions numeric(18,6) CHECK (all_conversions >= 0),
  conversion_value numeric(18,6) CHECK (conversion_value >= 0),
  all_conversion_value numeric(18,6) CHECK (all_conversion_value >= 0),
  currency char(3) NOT NULL,
  load_sk integer NOT NULL,
  PRIMARY KEY (account_sk, campaign_id, report_date)
) PARTITION BY RANGE (report_date);
CREATE INDEX ad_stats_campaign_daily_account_sk_report_date_idx
  ON core.ad_stats_campaign_daily (account_sk, report_date);

CREATE TABLE core.ad_stats_ad_group_daily (
  account_sk integer NOT NULL REFERENCES core.dim_account (account_sk),
  campaign_id bigint NOT NULL,
  ad_group_id bigint NOT NULL,
  report_date date NOT NULL,
  impressions bigint NOT NULL CHECK (impressions >= 0),
  clicks bigint NOT NULL CHECK (clicks >= 0),
  spend numeric(18,6) NOT NULL CHECK (spend >= 0),
  conversions numeric(18,6) CHECK (conversions >= 0),
  all_conversions numeric(18,6) CHECK (all_conversions >= 0),
  conversion_value numeric(18,6) CHECK (conversion_value >= 0),
  all_conversion_value numeric(18,6) CHECK (all_conversion_value >= 0),
  currency char(3) NOT NULL,
  load_sk integer NOT NULL,
  PRIMARY KEY (account_sk, ad_group_id, report_date)
) PARTITION BY RANGE (report_date);
CREATE INDEX ad_stats_ad_group_daily_account_sk_report_date_idx
  ON core.ad_stats_ad_group_daily (account_sk, report_date);

-- An ad is defined only inside its ad group, so the ad group is part of the key.
CREATE TABLE core.ad_stats_ad_daily (
  account_sk integer NOT NULL REFERENCES core.dim_account (account_sk),
  campaign_id bigint NOT NULL,
  ad_group_id bigint NOT NULL,
  ad_id bigint NOT NULL,
  report_date date NOT NULL,
  impressions bigint NOT NULL CHECK (impressions >= 0),
  clicks bigint NOT NULL CHECK (clicks >= 0),
  spend numeric(18,6) NOT NULL CHECK (spend >= 0),
  conversions numeric(18,6) CHECK (conversions >= 0),
  all_conversions numeric(18,6) CHECK (all_conversions >= 0),
  conversion_value numeric(18,6) CHECK (conversion_value >= 0),
  all_conversion_value numeric(18,6) CHECK (all_conversion_value >= 0),
  currency char(3) NOT NULL,
  load_sk integer NOT NULL,
  PRIMARY KEY (account_sk, ad_group_id, ad_id, report_date)
) PARTITION BY RANGE (report_date);
CREATE INDEX ad_stats_ad_daily_account_sk_report_date_idx
  ON core.ad_stats_ad_daily (account_sk, report_date);

-- A conversion action is text on Meta, so the fact carries the surrogate of the registry of
-- actions, with no foreign key to it.
CREATE TABLE core.ad_conversions_campaign_daily (
  account_sk integer NOT NULL REFERENCES core.dim_account (account_sk),
  campaign_id bigint NOT NULL,
  conversion_action_sk integer NOT NULL,
  report_date date NOT NULL,
  conversions numeric(18,6) NOT NULL CHECK (conversions >= 0),
  all_conversions numeric(18,6) CHECK (all_conversions >= 0),
  conversion_value numeric(18,6) CHECK (conversion_value >= 0),
  all_conversion_value numeric(18,6) CHECK (all_conversion_value >= 0),
  attribution_setting text,
  currency char(3) NOT NULL,
  load_sk integer NOT NULL,
  PRIMARY KEY (account_sk, campaign_id, conversion_action_sk, report_date)
) PARTITION BY RANGE (report_date);
CREATE INDEX ad_conversions_campaign_daily_account_sk_report_date_idx
  ON core.ad_conversions_campaign_daily (account_sk, report_date);

-- The keys of web analytics are text of the source, compared byte by byte (COLLATE "C"). The
-- property is an account of core.dim_account, as the settings of the model name it.
CREATE TABLE core.web_sessions_daily (
  account_sk integer NOT NULL REFERENCES core.dim_account (account_sk),
  session_source text COLLATE "C" NOT NULL,
  session_medium text COLLATE "C" NOT NULL,
  session_campaign_name text COLLATE "C" NOT NULL,
  session_campaign_id text COLLATE "C" NOT NULL,
  report_date date NOT NULL,
  session_default_channel_group text COLLATE "C",
  sessions bigint NOT NULL CHECK (sessions >= 0),
  engaged_sessions bigint NOT NULL CHECK (engaged_sessions >= 0),
  key_events numeric(18,6) CHECK (key_events >= 0),
  total_revenue numeric(18,6),
  currency char(3) NOT NULL,
  load_sk integer NOT NULL,
  PRIMARY KEY (
    account_sk, session_source, session_medium, session_campaign_name, session_campaign_id,
    report_date
  )
) PARTITION BY RANGE (report_date);
CREATE INDEX web_sessions_daily_account_sk_report_date_idx
  ON core.web_sessions_daily (account_sk, report_date);

-- The freeze (ADR-0035): a row of a month that ended before the window of restatements is neither
-- deleted nor updated, so a wrong condition of a delete cannot reach the history that the platforms
-- no longer serve. INSERT stays allowed, so the first history of a new account goes in. The window
-- is that of ADR-0038, from yesterday back: 90 days, the longest of the platforms, those of Google
-- and Microsoft, and two days more for the time zones of the accounts and of the session, whose
-- current_date can be a day off. A month is frozen when it ended before that, so the frozen months
-- move by themselves and no task has to move the trigger. The trigger is on the parent, and
-- PostgreSQL clones it to each partition, the ones that core.ensure_partition attaches later among
-- them. The WHEN clause is evaluated for each row that a statement deletes, and only a row of a
-- frozen month calls the function, so a delete inside the window calls none.
CREATE TRIGGER freeze_the_months_before_the_window
  BEFORE UPDATE OR DELETE ON core.ad_stats_campaign_daily
  FOR EACH ROW WHEN (OLD.report_date < date_trunc('month', current_date - 92)::date)
  EXECUTE FUNCTION core.refuse_a_change_of_a_frozen_month();
CREATE TRIGGER freeze_the_months_before_the_window
  BEFORE UPDATE OR DELETE ON core.ad_stats_ad_group_daily
  FOR EACH ROW WHEN (OLD.report_date < date_trunc('month', current_date - 92)::date)
  EXECUTE FUNCTION core.refuse_a_change_of_a_frozen_month();
CREATE TRIGGER freeze_the_months_before_the_window
  BEFORE UPDATE OR DELETE ON core.ad_stats_ad_daily
  FOR EACH ROW WHEN (OLD.report_date < date_trunc('month', current_date - 92)::date)
  EXECUTE FUNCTION core.refuse_a_change_of_a_frozen_month();
CREATE TRIGGER freeze_the_months_before_the_window
  BEFORE UPDATE OR DELETE ON core.ad_conversions_campaign_daily
  FOR EACH ROW WHEN (OLD.report_date < date_trunc('month', current_date - 92)::date)
  EXECUTE FUNCTION core.refuse_a_change_of_a_frozen_month();
CREATE TRIGGER freeze_the_months_before_the_window
  BEFORE UPDATE OR DELETE ON core.web_sessions_daily
  FOR EACH ROW WHEN (OLD.report_date < date_trunc('month', current_date - 92)::date)
  EXECUTE FUNCTION core.refuse_a_change_of_a_frozen_month();

GRANT SELECT, INSERT, DELETE, MAINTAIN
  ON core.ad_stats_campaign_daily, core.ad_stats_ad_group_daily, core.ad_stats_ad_daily,
    core.ad_conversions_campaign_daily, core.web_sessions_daily
  TO transformer;

COMMENT ON TABLE core.ad_stats_campaign_daily IS
'Advertising statistics by day and campaign of every ad platform: one row for a campaign of an account on a day, in the time zone of the account, as the platform reports its campaign level, with the amounts in the currency of the account. account_sk, campaign_id and report_date are its key. Partitioned by month of report_date; a month that ended before the window of restatements is frozen (the trigger freeze_the_months_before_the_window).';
COMMENT ON COLUMN core.ad_stats_campaign_daily.account_sk IS
'The account of the row, account_sk of core.dim_account, which gives its platform, ID, client, currency and time zone.';
COMMENT ON COLUMN core.ad_stats_campaign_daily.campaign_id IS
'The ID of the campaign as the ad platform gives it, a number, unique inside its account; core.dim_campaign holds its attributes.';
COMMENT ON COLUMN core.ad_stats_campaign_daily.report_date IS
'The day that the row covers, in the time zone of its account; not the day that the data was loaded. Its month is the partition of the row.';
COMMENT ON COLUMN core.ad_stats_campaign_daily.impressions IS
'How many times the ads of the campaign were shown on the day, as the ad platform counts them: showings, not people.';
COMMENT ON COLUMN core.ad_stats_campaign_daily.clicks IS
'How many times the ads of the campaign were clicked on the day with a click that leads to the site, as the ad platform counts them: the clicks of Google, the clicks (destination) of TikTok, the link clicks of Meta.';
COMMENT ON COLUMN core.ad_stats_campaign_daily.spend IS
'What the ads of the campaign cost on the day, as the ad platform reports it in its statistics, in the currency of the row, exact to six decimal places; not the amount that the platform invoices.';
COMMENT ON COLUMN core.ad_stats_campaign_daily.conversions IS
'The conversions of the campaign on the day in the own metric of conversions of the ad platform, such as metrics.conversions of Google, fractional where attribution shares a conversion; none where the platform has no such metric.';
COMMENT ON COLUMN core.ad_stats_campaign_daily.all_conversions IS
'All the conversions of the campaign on the day, those of the actions that the own metric of the platform leaves out among them, such as metrics.all_conversions of Google; none where the platform has no such metric.';
COMMENT ON COLUMN core.ad_stats_campaign_daily.conversion_value IS
'The value of the conversions of the column conversions, in the currency of the row; none where the platform gives none.';
COMMENT ON COLUMN core.ad_stats_campaign_daily.all_conversion_value IS
'The value of the conversions of the column all_conversions, in the currency of the row; none where the platform gives none.';
COMMENT ON COLUMN core.ad_stats_campaign_daily.currency IS
'The currency of the amounts of the row, that of its account, as a three-letter code of ISO 4217.';
COMMENT ON COLUMN core.ad_stats_campaign_daily.load_sk IS
'The load that wrote the row, load_sk of core.loads.';
COMMENT ON INDEX core.ad_stats_campaign_daily_account_sk_report_date_idx IS
'The days of an account, for a filter on an account and a period.';
COMMENT ON TRIGGER freeze_the_months_before_the_window ON core.ad_stats_campaign_daily IS
'Refuses the DELETE and the UPDATE of a row of a month that ended more than 92 days before the current date, before the window of restatements, unless agoranomos.thaw holds the ID of the current transaction.';

COMMENT ON TABLE core.ad_stats_ad_group_daily IS
'Advertising statistics by day and ad group of every ad platform, ad sets of Meta among them: one row for an ad group of an account on a day, in the time zone of the account, as the platform reports its ad group level, with the amounts in the currency of the account. account_sk, ad_group_id and report_date are its key. Partitioned by month of report_date; a month that ended before the window of restatements is frozen (the trigger freeze_the_months_before_the_window).';
COMMENT ON COLUMN core.ad_stats_ad_group_daily.account_sk IS
'The account of the row, account_sk of core.dim_account, which gives its platform, ID, client, currency and time zone.';
COMMENT ON COLUMN core.ad_stats_ad_group_daily.campaign_id IS
'The campaign of the ad group, as the ad platform gives its ID, a number.';
COMMENT ON COLUMN core.ad_stats_ad_group_daily.ad_group_id IS
'The ID of the ad group as the ad platform gives it, a number, unique inside its account; core.dim_ad_group holds its attributes.';
COMMENT ON COLUMN core.ad_stats_ad_group_daily.report_date IS
'The day that the row covers, in the time zone of its account; not the day that the data was loaded. Its month is the partition of the row.';
COMMENT ON COLUMN core.ad_stats_ad_group_daily.impressions IS
'How many times the ads of the ad group were shown on the day, as the ad platform counts them: showings, not people.';
COMMENT ON COLUMN core.ad_stats_ad_group_daily.clicks IS
'How many times the ads of the ad group were clicked on the day with a click that leads to the site, as the ad platform counts them: the clicks of Google, the clicks (destination) of TikTok, the link clicks of Meta.';
COMMENT ON COLUMN core.ad_stats_ad_group_daily.spend IS
'What the ads of the ad group cost on the day, as the ad platform reports it in its statistics, in the currency of the row, exact to six decimal places; not the amount that the platform invoices.';
COMMENT ON COLUMN core.ad_stats_ad_group_daily.conversions IS
'The conversions of the ad group on the day in the own metric of conversions of the ad platform, such as metrics.conversions of Google, fractional where attribution shares a conversion; none where the platform has no such metric.';
COMMENT ON COLUMN core.ad_stats_ad_group_daily.all_conversions IS
'All the conversions of the ad group on the day, those of the actions that the own metric of the platform leaves out among them, such as metrics.all_conversions of Google; none where the platform has no such metric.';
COMMENT ON COLUMN core.ad_stats_ad_group_daily.conversion_value IS
'The value of the conversions of the column conversions, in the currency of the row; none where the platform gives none.';
COMMENT ON COLUMN core.ad_stats_ad_group_daily.all_conversion_value IS
'The value of the conversions of the column all_conversions, in the currency of the row; none where the platform gives none.';
COMMENT ON COLUMN core.ad_stats_ad_group_daily.currency IS
'The currency of the amounts of the row, that of its account, as a three-letter code of ISO 4217.';
COMMENT ON COLUMN core.ad_stats_ad_group_daily.load_sk IS
'The load that wrote the row, load_sk of core.loads.';
COMMENT ON INDEX core.ad_stats_ad_group_daily_account_sk_report_date_idx IS
'The days of an account, for a filter on an account and a period.';
COMMENT ON TRIGGER freeze_the_months_before_the_window ON core.ad_stats_ad_group_daily IS
'Refuses the DELETE and the UPDATE of a row of a month that ended more than 92 days before the current date, before the window of restatements, unless agoranomos.thaw holds the ID of the current transaction.';

COMMENT ON TABLE core.ad_stats_ad_daily IS
'Advertising statistics by day and ad of every ad platform: one row for an ad of an account on a day, in the time zone of the account, as the platform reports its ad level, with the amounts in the currency of the account. account_sk, ad_group_id, ad_id and report_date are its key, as an ad is defined only inside its ad group. Partitioned by month of report_date; a month that ended before the window of restatements is frozen (the trigger freeze_the_months_before_the_window).';
COMMENT ON COLUMN core.ad_stats_ad_daily.account_sk IS
'The account of the row, account_sk of core.dim_account, which gives its platform, ID, client, currency and time zone.';
COMMENT ON COLUMN core.ad_stats_ad_daily.campaign_id IS
'The campaign of the ad, as the ad platform gives its ID, a number.';
COMMENT ON COLUMN core.ad_stats_ad_daily.ad_group_id IS
'The ad group of the ad, as the ad platform gives its ID, a number.';
COMMENT ON COLUMN core.ad_stats_ad_daily.ad_id IS
'The ID of the ad as the ad platform gives it, a number, unique inside its ad group; core.dim_ad holds its attributes.';
COMMENT ON COLUMN core.ad_stats_ad_daily.report_date IS
'The day that the row covers, in the time zone of its account; not the day that the data was loaded. Its month is the partition of the row.';
COMMENT ON COLUMN core.ad_stats_ad_daily.impressions IS
'How many times the ad was shown on the day, as the ad platform counts it: showings, not people.';
COMMENT ON COLUMN core.ad_stats_ad_daily.clicks IS
'How many times the ad was clicked on the day with a click that leads to the site, as the ad platform counts them: the clicks of Google, the clicks (destination) of TikTok, the link clicks of Meta.';
COMMENT ON COLUMN core.ad_stats_ad_daily.spend IS
'What the ad cost on the day, as the ad platform reports it in its statistics, in the currency of the row, exact to six decimal places; not the amount that the platform invoices.';
COMMENT ON COLUMN core.ad_stats_ad_daily.conversions IS
'The conversions of the ad on the day in the own metric of conversions of the ad platform, such as metrics.conversions of Google, fractional where attribution shares a conversion; none where the platform has no such metric.';
COMMENT ON COLUMN core.ad_stats_ad_daily.all_conversions IS
'All the conversions of the ad on the day, those of the actions that the own metric of the platform leaves out among them, such as metrics.all_conversions of Google; none where the platform has no such metric.';
COMMENT ON COLUMN core.ad_stats_ad_daily.conversion_value IS
'The value of the conversions of the column conversions, in the currency of the row; none where the platform gives none.';
COMMENT ON COLUMN core.ad_stats_ad_daily.all_conversion_value IS
'The value of the conversions of the column all_conversions, in the currency of the row; none where the platform gives none.';
COMMENT ON COLUMN core.ad_stats_ad_daily.currency IS
'The currency of the amounts of the row, that of its account, as a three-letter code of ISO 4217.';
COMMENT ON COLUMN core.ad_stats_ad_daily.load_sk IS
'The load that wrote the row, load_sk of core.loads.';
COMMENT ON INDEX core.ad_stats_ad_daily_account_sk_report_date_idx IS
'The days of an account, for a filter on an account and a period.';
COMMENT ON TRIGGER freeze_the_months_before_the_window ON core.ad_stats_ad_daily IS
'Refuses the DELETE and the UPDATE of a row of a month that ended more than 92 days before the current date, before the window of restatements, unless agoranomos.thaw holds the ID of the current transaction.';

COMMENT ON TABLE core.ad_conversions_campaign_daily IS
'Conversions by day, campaign and conversion action of every ad platform: one row for a conversion action of a campaign of an account on a day, in the time zone of the account, with the value in the currency of the account. account_sk, campaign_id, conversion_action_sk and report_date are its key. Partitioned by month of report_date; a month that ended before the window of restatements is frozen (the trigger freeze_the_months_before_the_window).';
COMMENT ON COLUMN core.ad_conversions_campaign_daily.account_sk IS
'The account of the row, account_sk of core.dim_account, which gives its platform, ID, client, currency and time zone.';
COMMENT ON COLUMN core.ad_conversions_campaign_daily.campaign_id IS
'The ID of the campaign as the ad platform gives it, a number, unique inside its account; core.dim_campaign holds its attributes.';
COMMENT ON COLUMN core.ad_conversions_campaign_daily.conversion_action_sk IS
'The conversion action of the row, conversion_action_sk of core.dim_conversion_action, which gives its ID, name, category and whether it is primary.';
COMMENT ON COLUMN core.ad_conversions_campaign_daily.report_date IS
'The day that the row covers, in the time zone of its account; not the day that the data was loaded. Its month is the partition of the row.';
COMMENT ON COLUMN core.ad_conversions_campaign_daily.conversions IS
'The conversions of the action in the campaign on the day that the own metric of conversions of the ad platform counts, such as metrics.conversions of Google, 0 for an action that the metric leaves out; fractional where attribution shares a conversion.';
COMMENT ON COLUMN core.ad_conversions_campaign_daily.all_conversions IS
'All the conversions of the action in the campaign on the day, such as metrics.all_conversions of Google; none where the platform has no such metric.';
COMMENT ON COLUMN core.ad_conversions_campaign_daily.conversion_value IS
'The value of the conversions of the column conversions, in the currency of the row; none where the platform gives none.';
COMMENT ON COLUMN core.ad_conversions_campaign_daily.all_conversion_value IS
'The value of the conversions of the column all_conversions, in the currency of the row; none where the platform gives none.';
COMMENT ON COLUMN core.ad_conversions_campaign_daily.attribution_setting IS
'The window of attribution with which the platform counted the conversions of the row, as the platform writes it, such as 7d_click_1d_view; none where the platform gives none.';
COMMENT ON COLUMN core.ad_conversions_campaign_daily.currency IS
'The currency of the amounts of the row, that of its account, as a three-letter code of ISO 4217.';
COMMENT ON COLUMN core.ad_conversions_campaign_daily.load_sk IS
'The load that wrote the row, load_sk of core.loads.';
COMMENT ON INDEX core.ad_conversions_campaign_daily_account_sk_report_date_idx IS
'The days of an account, for a filter on an account and a period.';
COMMENT ON TRIGGER freeze_the_months_before_the_window ON core.ad_conversions_campaign_daily IS
'Refuses the DELETE and the UPDATE of a row of a month that ended more than 92 days before the current date, before the window of restatements, unless agoranomos.thaw holds the ID of the current transaction.';

COMMENT ON TABLE core.web_sessions_daily IS
'Sessions of web analytics by day, property, source, medium and campaign: one row for the sessions of a property on a day, in the time zone of the property, with one source, medium and campaign, and the revenue in the currency of the property. account_sk, session_source, session_medium, session_campaign_name, session_campaign_id and report_date are its key, compared byte by byte. Partitioned by month of report_date; a month that ended before the window of restatements is frozen (the trigger freeze_the_months_before_the_window).';
COMMENT ON COLUMN core.web_sessions_daily.account_sk IS
'The property of web analytics of the row, account_sk of core.dim_account, which registers the properties beside the ad accounts and gives its platform, ID, client, currency and time zone.';
COMMENT ON COLUMN core.web_sessions_daily.session_source IS
'Where the sessions came from, as web analytics records it: the utm_source of a link, or a site or a search engine; (direct) for sessions with no source.';
COMMENT ON COLUMN core.web_sessions_daily.session_medium IS
'The medium of the sessions, as web analytics records it: the utm_medium of a link, such as cpc, or organic, referral or (none).';
COMMENT ON COLUMN core.web_sessions_daily.session_campaign_name IS
'The name of the campaign of the sessions as their links carried it on the day, the utm_campaign; a value in parentheses, such as (organic), (direct) or (not set), for sessions without one.';
COMMENT ON COLUMN core.web_sessions_daily.session_campaign_id IS
'The ID of the campaign of the sessions as their links carried it, the utm_id: the ID of the ad campaign when its links carry it; (not set) otherwise.';
COMMENT ON COLUMN core.web_sessions_daily.report_date IS
'The day that the row covers, in the time zone of its property; not the day that the data was loaded. Its month is the partition of the row.';
COMMENT ON COLUMN core.web_sessions_daily.session_default_channel_group IS
'The channel of the sessions, by the rules of web analytics, such as Paid Search, Paid Social, Organic Search or Direct.';
COMMENT ON COLUMN core.web_sessions_daily.sessions IS
'How many sessions web analytics counted on the day for the row: visits, not people, and not the clicks of the ad platforms.';
COMMENT ON COLUMN core.web_sessions_daily.engaged_sessions IS
'How many of the sessions web analytics counted as engaged, by its own rule, such as a session of more than ten seconds.';
COMMENT ON COLUMN core.web_sessions_daily.key_events IS
'How many key events web analytics counted in the sessions, the events that the property marks as key, such as a purchase.';
COMMENT ON COLUMN core.web_sessions_daily.total_revenue IS
'The revenue that web analytics records for the sessions, less the refunds of the day, in the currency of the row; negative on a day whose refunds exceed it.';
COMMENT ON COLUMN core.web_sessions_daily.currency IS
'The currency of the revenue of the row, that of its property, as a three-letter code of ISO 4217.';
COMMENT ON COLUMN core.web_sessions_daily.load_sk IS
'The load that wrote the row, load_sk of core.loads.';
COMMENT ON INDEX core.web_sessions_daily_account_sk_report_date_idx IS
'The days of a property, for a filter on a property and a period.';
COMMENT ON TRIGGER freeze_the_months_before_the_window ON core.web_sessions_daily IS
'Refuses the DELETE and the UPDATE of a row of a month that ended more than 92 days before the current date, before the window of restatements, unless agoranomos.thaw holds the ID of the current transaction.';

-- The partitions of the month in which this migration runs and of the two after it, in UTC, through
-- core.ensure_partition, as every later partition comes: the months of a load of history before it
-- is written, and the months ahead from the maintenance (ADR-0039). The months come from the clock
-- and not from the text, so a warehouse migrated later gets the months of its own day. They come
-- last, as a partition copies the comments of the columns of its parent when it is made.
DO $$
DECLARE
  v_parent regclass;
  v_month date;
BEGIN
  FOREACH v_parent IN ARRAY ARRAY[
    'core.ad_stats_campaign_daily', 'core.ad_stats_ad_group_daily', 'core.ad_stats_ad_daily',
    'core.ad_conversions_campaign_daily', 'core.web_sessions_daily'
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
