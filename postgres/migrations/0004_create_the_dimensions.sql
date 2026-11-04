-- The dimensions of the core (ADR-0036): the clients, the campaigns, the ad groups, the ads and the
-- calendar, with the current attributes of each. Their versions are snapshots of dbt, tables that
-- transformer creates in core itself (core.dim_*_versions), and not these. An entity carries the
-- surrogate of its account and its ID of the platform as bigint, which the facts carry too, and the
-- same ID as text, which the views of the marts show: the IDs of Meta exceed 2^53, which the
-- browser of Metabase rounds. The common state of an entity maps the statuses of the platforms to
-- active, paused, removed or other. The dimensions hold no row yet: dbt is to write them in v0.5.
-- transformer may read and change their rows.

CREATE TABLE core.dim_client (
  client_id text PRIMARY KEY,
  client_name text NOT NULL,
  is_default boolean NOT NULL
);

CREATE TABLE core.dim_campaign (
  account_sk integer NOT NULL,
  campaign_id bigint NOT NULL,
  campaign_id_text text NOT NULL,
  campaign_name text,
  campaign_status text,
  campaign_state text NOT NULL CHECK (campaign_state IN ('active', 'paused', 'removed', 'other')),
  campaign_type text,
  start_date date,
  end_date date,
  budget numeric(18,6) CHECK (budget >= 0),
  budget_period text,
  name_count integer,
  first_report_date date,
  last_report_date date,
  last_seen_at timestamptz,
  is_listed boolean NOT NULL,
  PRIMARY KEY (account_sk, campaign_id),
  CHECK (campaign_id_text = campaign_id::text)
);

CREATE TABLE core.dim_ad_group (
  account_sk integer NOT NULL,
  ad_group_id bigint NOT NULL,
  ad_group_id_text text NOT NULL,
  campaign_id bigint NOT NULL,
  ad_group_name text,
  ad_group_status text,
  ad_group_state text NOT NULL CHECK (ad_group_state IN ('active', 'paused', 'removed', 'other')),
  ad_group_type text,
  start_date date,
  end_date date,
  budget numeric(18,6) CHECK (budget >= 0),
  budget_period text,
  first_report_date date,
  last_report_date date,
  last_seen_at timestamptz,
  is_listed boolean NOT NULL,
  PRIMARY KEY (account_sk, ad_group_id),
  CHECK (ad_group_id_text = ad_group_id::text)
);

-- An ad is defined only inside its ad group, so the ad group is part of its key, as in the fact
-- of ads. No platform gives an ad dates or a budget of its own.
CREATE TABLE core.dim_ad (
  account_sk integer NOT NULL,
  ad_group_id bigint NOT NULL,
  ad_id bigint NOT NULL,
  ad_id_text text NOT NULL,
  campaign_id bigint NOT NULL,
  ad_name text,
  ad_status text,
  ad_state text NOT NULL CHECK (ad_state IN ('active', 'paused', 'removed', 'other')),
  ad_type text,
  first_report_date date,
  last_report_date date,
  last_seen_at timestamptz,
  is_listed boolean NOT NULL,
  PRIMARY KEY (account_sk, ad_group_id, ad_id),
  CHECK (ad_id_text = ad_id::text)
);

CREATE TABLE core.dim_date (
  calendar_date date PRIMARY KEY,
  year integer NOT NULL,
  quarter integer NOT NULL CHECK (quarter BETWEEN 1 AND 4),
  month integer NOT NULL CHECK (month BETWEEN 1 AND 12),
  month_start date NOT NULL,
  iso_year integer NOT NULL,
  iso_week integer NOT NULL CHECK (iso_week BETWEEN 1 AND 53),
  week_start date NOT NULL,
  day_of_week integer NOT NULL CHECK (day_of_week BETWEEN 1 AND 7),
  is_weekend boolean NOT NULL
);

GRANT SELECT, INSERT, UPDATE, DELETE
  ON core.dim_client, core.dim_campaign, core.dim_ad_group, core.dim_ad, core.dim_date
  TO transformer;

COMMENT ON TABLE core.dim_client IS
'The clients of the business, such as brands, shops or legal entities, one row each, as the settings of the model name them, with the default client Unassigned, which holds the accounts that no setting names (ADR-0024).';
COMMENT ON COLUMN core.dim_client.client_id IS
'The ID of the client, as the settings of the model name it: those of the deployment in extensions/model/clients.toml, or the file of the demo; unassigned for the default client.';
COMMENT ON COLUMN core.dim_client.client_name IS
'The name of the client, as the settings of the model give it; Unassigned for the default client.';
COMMENT ON COLUMN core.dim_client.is_default IS
'Whether the client is the default one, Unassigned, which holds the accounts that no setting names.';

COMMENT ON TABLE core.dim_campaign IS
'The campaigns of every ad platform, one row each, with their current attributes: those of the newest list of their account, or, for a campaign that only the statistics name, its name from the statistics. account_sk and campaign_id are its key. A campaign that the platform no longer lists stays, with is_listed false.';
COMMENT ON COLUMN core.dim_campaign.account_sk IS
'The account of the campaign, account_sk of core.dim_account.';
COMMENT ON COLUMN core.dim_campaign.campaign_id IS
'The ID of the campaign as the ad platform gives it, a number, unique inside its account.';
COMMENT ON COLUMN core.dim_campaign.campaign_id_text IS
'The same ID as text, as the marts show it: a number above 2^53 keeps every digit as text.';
COMMENT ON COLUMN core.dim_campaign.campaign_name IS
'The current name of the campaign, as the ad platform gives it, where anyone with access to the account can change it.';
COMMENT ON COLUMN core.dim_campaign.campaign_status IS
'The status of the campaign at the last load that listed it, as the ad platform writes it, such as ENABLED, PAUSED or REMOVED for Google, ACTIVE or DELETED for Meta, or ENABLE for TikTok.';
COMMENT ON COLUMN core.dim_campaign.campaign_state IS
'The status of the campaign in the words of every platform: active, paused, removed, or other for a status that none of these names.';
COMMENT ON COLUMN core.dim_campaign.campaign_type IS
'The type of the campaign as the ad platform gives it: the advertising channel type of Google, such as SEARCH or PERFORMANCE_MAX, or the objective of Meta or of TikTok.';
COMMENT ON COLUMN core.dim_campaign.start_date IS
'The first day on which the campaign may serve, as the ad platform gives it; none where it gives none.';
COMMENT ON COLUMN core.dim_campaign.end_date IS
'The last day on which the campaign may serve, as the ad platform gives it; none for a campaign without an end.';
COMMENT ON COLUMN core.dim_campaign.budget IS
'The budget of the campaign, in the currency of its account, for the period of budget_period; none for a campaign without a budget of its own.';
COMMENT ON COLUMN core.dim_campaign.budget_period IS
'The period of the budget, as the ad platform writes it, such as DAILY for Google or BUDGET_MODE_TOTAL for TikTok.';
COMMENT ON COLUMN core.dim_campaign.name_count IS
'How many different names the campaign had, as the loads saw them.';
COMMENT ON COLUMN core.dim_campaign.first_report_date IS
'The first day that core.ad_stats_campaign_daily holds for the campaign; none for one without statistics.';
COMMENT ON COLUMN core.dim_campaign.last_report_date IS
'The last day that core.ad_stats_campaign_daily holds for the campaign; none for one without statistics.';
COMMENT ON COLUMN core.dim_campaign.last_seen_at IS
'When the newest list of campaigns of the account that named the campaign was loaded; none for a campaign that only the statistics name.';
COMMENT ON COLUMN core.dim_campaign.is_listed IS
'Whether the newest list of campaigns of the account names the campaign: false for one that the platform no longer shows, or that only the statistics name, whose spend still counts.';

COMMENT ON TABLE core.dim_ad_group IS
'The ad groups of every ad platform, ad sets of Meta among them, one row each, with their campaign and current attributes: those of the newest list of their account, or, for an ad group that only the statistics name, its name from the statistics. account_sk and ad_group_id are its key. An ad group that the platform no longer lists stays, with is_listed false.';
COMMENT ON COLUMN core.dim_ad_group.account_sk IS
'The account of the ad group, account_sk of core.dim_account.';
COMMENT ON COLUMN core.dim_ad_group.ad_group_id IS
'The ID of the ad group as the ad platform gives it, a number, unique inside its account.';
COMMENT ON COLUMN core.dim_ad_group.ad_group_id_text IS
'The same ID as text, as the marts show it: a number above 2^53 keeps every digit as text.';
COMMENT ON COLUMN core.dim_ad_group.campaign_id IS
'The campaign of the ad group, campaign_id of core.dim_campaign in the same account.';
COMMENT ON COLUMN core.dim_ad_group.ad_group_name IS
'The current name of the ad group, as the ad platform gives it, where anyone with access to the account can change it.';
COMMENT ON COLUMN core.dim_ad_group.ad_group_status IS
'The status of the ad group itself at the last load that listed it, as the ad platform writes it, such as ENABLED or REMOVED for Google, ACTIVE or DELETED for Meta, or ENABLE for TikTok: an ad group of a paused campaign can still be enabled.';
COMMENT ON COLUMN core.dim_ad_group.ad_group_state IS
'The status of the ad group in the words of every platform: active, paused, removed, or other for a status that none of these names.';
COMMENT ON COLUMN core.dim_ad_group.ad_group_type IS
'The type of the ad group as the ad platform gives it: the type of Google, such as SEARCH_STANDARD, or the optimization goal of the ad set of Meta or of the ad group of TikTok.';
COMMENT ON COLUMN core.dim_ad_group.start_date IS
'The first day on which the ad group may serve, as the ad platform gives it; none where it gives none.';
COMMENT ON COLUMN core.dim_ad_group.end_date IS
'The last day on which the ad group may serve, as the ad platform gives it; none for an ad group without an end.';
COMMENT ON COLUMN core.dim_ad_group.budget IS
'The budget of the ad group, in the currency of its account, for the period of budget_period; none for an ad group without a budget of its own, such as every ad group of Google.';
COMMENT ON COLUMN core.dim_ad_group.budget_period IS
'The period of the budget, as the ad platform writes it.';
COMMENT ON COLUMN core.dim_ad_group.first_report_date IS
'The first day that core.ad_stats_ad_group_daily holds for the ad group; none for one without statistics.';
COMMENT ON COLUMN core.dim_ad_group.last_report_date IS
'The last day that core.ad_stats_ad_group_daily holds for the ad group; none for one without statistics.';
COMMENT ON COLUMN core.dim_ad_group.last_seen_at IS
'When the newest list of ad groups of the account that named the ad group was loaded; none for an ad group that only the statistics name.';
COMMENT ON COLUMN core.dim_ad_group.is_listed IS
'Whether the newest list of ad groups of the account names the ad group: false for one that the platform no longer shows, or that only the statistics name, whose spend still counts.';

COMMENT ON TABLE core.dim_ad IS
'The ads of every ad platform, one row each, with their campaign, ad group and current attributes: those of the newest list of their account, or, for an ad that only the statistics name, its name from the statistics. account_sk, ad_group_id and ad_id are its key, as an ad is defined only inside its ad group. An ad that the platform no longer lists stays, with is_listed false.';
COMMENT ON COLUMN core.dim_ad.account_sk IS
'The account of the ad, account_sk of core.dim_account.';
COMMENT ON COLUMN core.dim_ad.ad_group_id IS
'The ad group of the ad, ad_group_id of core.dim_ad_group in the same account.';
COMMENT ON COLUMN core.dim_ad.ad_id IS
'The ID of the ad as the ad platform gives it, a number, unique inside its ad group.';
COMMENT ON COLUMN core.dim_ad.ad_id_text IS
'The same ID as text, as the marts show it: a number above 2^53 keeps every digit as text.';
COMMENT ON COLUMN core.dim_ad.campaign_id IS
'The campaign of the ad, campaign_id of core.dim_campaign in the same account.';
COMMENT ON COLUMN core.dim_ad.ad_name IS
'The current name of the ad, as the ad platform gives it, where anyone with access to the account can change it; none for a type of ad that has no name on its platform, such as a responsive search ad of Google.';
COMMENT ON COLUMN core.dim_ad.ad_status IS
'The status of the ad at the last load that listed it, as the ad platform writes it, such as ENABLED or REMOVED for an ad in its ad group of Google, ACTIVE or DELETED for Meta, or ENABLE for TikTok.';
COMMENT ON COLUMN core.dim_ad.ad_state IS
'The status of the ad in the words of every platform: active, paused, removed, or other for a status that none of these names.';
COMMENT ON COLUMN core.dim_ad.ad_type IS
'The type of the ad as the ad platform gives it: the type of Google, such as RESPONSIVE_SEARCH_AD, or the format of TikTok, such as SINGLE_VIDEO; none for Meta, whose ads have no type.';
COMMENT ON COLUMN core.dim_ad.first_report_date IS
'The first day that core.ad_stats_ad_daily holds for the ad; none for one without statistics.';
COMMENT ON COLUMN core.dim_ad.last_report_date IS
'The last day that core.ad_stats_ad_daily holds for the ad; none for one without statistics.';
COMMENT ON COLUMN core.dim_ad.last_seen_at IS
'When the newest list of ads of the account that named the ad was loaded; none for an ad that only the statistics name.';
COMMENT ON COLUMN core.dim_ad.is_listed IS
'Whether the newest list of ads of the account names the ad: false for one that the platform no longer shows, or that only the statistics name, whose spend still counts.';

COMMENT ON TABLE core.dim_date IS
'The calendar: one row for each day, with its year, quarter, month and week, for the views that need a day with no fact, such as a continuous series of a chart.';
COMMENT ON COLUMN core.dim_date.calendar_date IS
'The day.';
COMMENT ON COLUMN core.dim_date.year IS
'The year of the day.';
COMMENT ON COLUMN core.dim_date.quarter IS
'The quarter of the year of the day, from 1 to 4.';
COMMENT ON COLUMN core.dim_date.month IS
'The month of the year of the day, from 1 to 12.';
COMMENT ON COLUMN core.dim_date.month_start IS
'The first day of the month of the day.';
COMMENT ON COLUMN core.dim_date.iso_year IS
'The year of the week of the day by ISO 8601, which differs from year in the first and last days of some years.';
COMMENT ON COLUMN core.dim_date.iso_week IS
'The week of the day by ISO 8601, from 1 to 53; a week starts on Monday.';
COMMENT ON COLUMN core.dim_date.week_start IS
'The Monday of the week of the day.';
COMMENT ON COLUMN core.dim_date.day_of_week IS
'The day of the week by ISO 8601, from 1 for Monday to 7 for Sunday.';
COMMENT ON COLUMN core.dim_date.is_weekend IS
'Whether the day is a Saturday or a Sunday.';
