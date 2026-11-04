-- The registries of the core (ADR-0036, ADR-0038): the accounts and the conversion actions, each
-- with a surrogate key of integer that the facts carry. dbt writes every column of a table that it
-- merges into, which an identity column refuses, so each key has a sequence of its own, and a model
-- takes the key as coalesce(the existing key, nextval of the sequence). core.dim_account is the one
-- table that a foreign key of the facts points at (ADR-0035, G2). The registries hold no row yet:
-- dbt is to write them in v0.5. transformer may read and change their rows, as for every dimension,
-- and may take the next value of each sequence.

CREATE SEQUENCE core.dim_account_account_sk_seq AS integer;

-- The accounts of ad platforms and the properties of web analytics, as the settings of the model
-- name both by platform and account_id (ADR-0024).
CREATE TABLE core.dim_account (
  account_sk integer PRIMARY KEY DEFAULT nextval('core.dim_account_account_sk_seq'),
  platform text NOT NULL,
  account_id text NOT NULL,
  account_name text,
  client_id text NOT NULL,
  currency char(3) CHECK (currency ~ '^[A-Z]{3}$'),
  time_zone text,
  manager_account_id text,
  account_status text,
  hierarchy_rule text,
  is_active boolean NOT NULL,
  is_stub boolean NOT NULL,
  first_report_date date,
  last_report_date date,
  UNIQUE (platform, account_id)
);
ALTER SEQUENCE core.dim_account_account_sk_seq OWNED BY core.dim_account.account_sk;

CREATE SEQUENCE core.dim_conversion_action_conversion_action_sk_seq AS integer;

CREATE TABLE core.dim_conversion_action (
  conversion_action_sk integer PRIMARY KEY
    DEFAULT nextval('core.dim_conversion_action_conversion_action_sk_seq'),
  platform text NOT NULL,
  conversion_action_id text NOT NULL,
  conversion_action_name text,
  conversion_action_status text,
  platform_category text,
  conversion_category text NOT NULL
    CHECK (conversion_category IN ('purchase', 'lead', 'sign_up', 'add_to_cart', 'app_install', 'other')),
  include_in_conversions_metric boolean,
  is_primary boolean NOT NULL,
  UNIQUE (platform, conversion_action_id)
);
ALTER SEQUENCE core.dim_conversion_action_conversion_action_sk_seq
  OWNED BY core.dim_conversion_action.conversion_action_sk;

GRANT SELECT, INSERT, UPDATE, DELETE ON core.dim_account, core.dim_conversion_action TO transformer;
GRANT USAGE ON SEQUENCE core.dim_account_account_sk_seq,
  core.dim_conversion_action_conversion_action_sk_seq TO transformer;

COMMENT ON SEQUENCE core.dim_account_account_sk_seq IS
'The source of the surrogate keys of the accounts, account_sk of core.dim_account.';
COMMENT ON TABLE core.dim_account IS
'The registry of accounts: one row for each account of an ad platform and each property of web analytics, with its surrogate key account_sk, which every fact of the core carries and points at with its one foreign key. platform and account_id identify an account too. It holds the current attributes of each account.';
COMMENT ON COLUMN core.dim_account.account_sk IS
'The surrogate key of the account, from the sequence core.dim_account_account_sk_seq; it never changes for an account.';
COMMENT ON COLUMN core.dim_account.platform IS
'The code of the platform of the account, such as google_ads or demo_search, as the marts name it.';
COMMENT ON COLUMN core.dim_account.account_id IS
'The ID of the account as the platform gives it, as text: of an ad account, or of a property of web analytics without its prefix properties/. platform and account_id together identify the account.';
COMMENT ON COLUMN core.dim_account.account_name IS
'The current name of the account, as the platform gives it, where anyone with access to the account can change it; none for an account that no load has listed.';
COMMENT ON COLUMN core.dim_account.client_id IS
'The client of the business that the account belongs to, client_id of core.dim_client, as the settings of the model name it; unassigned for an account that no setting names.';
COMMENT ON COLUMN core.dim_account.currency IS
'The currency of the account, in which the platform reports its amounts, as a three-letter code of ISO 4217; none for an account that no load has listed.';
COMMENT ON COLUMN core.dim_account.time_zone IS
'The time zone of the account, as a name of the tz database of IANA, such as Europe/Berlin, in which the platform reports its days; none for an account that no load has listed.';
COMMENT ON COLUMN core.dim_account.manager_account_id IS
'The ID of the manager account that the account is under, as the platform gives it; none where there is none.';
COMMENT ON COLUMN core.dim_account.account_status IS
'The status of the account at the last load that listed it, as the platform writes it, such as ENABLED for Google.';
COMMENT ON COLUMN core.dim_account.hierarchy_rule IS
'The rule by which the connector of the platform maps the levels of the platform onto the three grains of the core, campaign, ad group and ad, for a platform whose levels differ from them, such as the campaign groups of LinkedIn; none where the levels of the platform are those three.';
COMMENT ON COLUMN core.dim_account.is_active IS
'Whether the settings of the model still name the account: an account that leaves the settings keeps its data and is no longer active.';
COMMENT ON COLUMN core.dim_account.is_stub IS
'Whether the row is a stub, made for an account that data names and that no load of the list of accounts gave, so that its name, currency and time zone are unknown.';
COMMENT ON COLUMN core.dim_account.first_report_date IS
'The first day that the facts of advertising statistics of the core hold for the account; none for an account without statistics.';
COMMENT ON COLUMN core.dim_account.last_report_date IS
'The last day that the facts of advertising statistics of the core hold for the account; none for an account without statistics.';

COMMENT ON SEQUENCE core.dim_conversion_action_conversion_action_sk_seq IS
'The source of the surrogate keys of the conversion actions, conversion_action_sk of core.dim_conversion_action.';
COMMENT ON TABLE core.dim_conversion_action IS
'The registry of conversion actions: one row for each conversion action of each platform, with its surrogate key conversion_action_sk, which core.ad_conversions_campaign_daily carries. platform and conversion_action_id identify an action too, as the settings of the model name it. It holds the current attributes of each action and whether it counts as primary.';
COMMENT ON COLUMN core.dim_conversion_action.conversion_action_sk IS
'The surrogate key of the conversion action, from the sequence core.dim_conversion_action_conversion_action_sk_seq; it never changes for an action.';
COMMENT ON COLUMN core.dim_conversion_action.platform IS
'The code of the platform of the conversion action, such as google_ads or demo_search, as the marts name it.';
COMMENT ON COLUMN core.dim_conversion_action.conversion_action_id IS
'The ID of the conversion action as the platform names it, as text: the ID of a conversion action of Google, the action type of Meta, such as offsite_conversion.fb_pixel_purchase, or the metric of TikTok, such as complete_payment.';
COMMENT ON COLUMN core.dim_conversion_action.conversion_action_name IS
'The name of the conversion action, as the platform gives it, or as the seed of conversion actions of the base package names it where it names one.';
COMMENT ON COLUMN core.dim_conversion_action.conversion_action_status IS
'The status of the conversion action at the last load that listed it, as the platform writes it, such as ENABLED, HIDDEN or REMOVED for Google; none where the platform lists no actions.';
COMMENT ON COLUMN core.dim_conversion_action.platform_category IS
'The category that the platform gives the conversion action, in lower case, such as purchase or submit_lead_form; none where the platform gives none.';
COMMENT ON COLUMN core.dim_conversion_action.conversion_category IS
'What kind of conversion the action is: purchase, lead, sign_up, add_to_cart, app_install or other, from the settings of the deployment, the seed of the base package, or the category that the platform gives, in that order.';
COMMENT ON COLUMN core.dim_conversion_action.include_in_conversions_metric IS
'Whether the platform counts the action in its own metric of conversions, as it says of the action, such as include_in_conversions_metric of Google; none where the platform says nothing of it.';
COMMENT ON COLUMN core.dim_conversion_action.is_primary IS
'Whether the action counts in the conversions of the primary actions of the deployment: as extensions/model/conversion_actions.toml says where it names the action, else as the platform counts it (include_in_conversions_metric), else as the seed of conversion actions says; false for an action that none of them names.';
