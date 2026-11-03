-- The rates of the core (ADR-0036, ADR-0042): the rate of each currency of the accounts into the
-- reporting currency on each calendar day, with the last published rate carried over a day without
-- one, so that a view converts an amount with one join and rounds it once, for display. The rate
-- into the reporting currency goes through the base currency of the source with no rounding on the
-- way, as Regulation (EC) 1103/97 asks, so it has no fixed scale. The reporting currency is part
-- of the key, as the reporting currency of each client is planned beside that of the deployment.
-- It has no partitions: a day and a currency make a few thousand rows a year. It holds no row yet:
-- dbt is to write it in v0.5. transformer may read and change its rows, as for a dimension.

CREATE TABLE core.fx_rates_daily (
  rate_date date NOT NULL,
  currency char(3) NOT NULL CHECK (currency ~ '^[A-Z]{3}$'),
  reporting_currency char(3) NOT NULL CHECK (reporting_currency ~ '^[A-Z]{3}$'),
  rate_into_reporting_currency numeric NOT NULL CHECK (rate_into_reporting_currency > 0),
  base_currency char(3) CHECK (base_currency ~ '^[A-Z]{3}$'),
  base_rate numeric(20,10) CHECK (base_rate > 0),
  rate_source text NOT NULL,
  published_on date,
  is_carried_forward boolean NOT NULL,
  PRIMARY KEY (rate_date, currency, reporting_currency),
  CHECK (published_on <= rate_date)
);

GRANT SELECT, INSERT, UPDATE, DELETE ON core.fx_rates_daily TO transformer;

COMMENT ON TABLE core.fx_rates_daily IS
'The rates into the reporting currency: one row for each calendar day, each currency of the accounts and each reporting currency, with the last rate that the source published on or before the day. rate_date, currency and reporting_currency are its key. Each deployment takes the rates from the source itself, which rate_source names.';
COMMENT ON COLUMN core.fx_rates_daily.rate_date IS
'The calendar day of the rate, whether or not the source published a rate on it.';
COMMENT ON COLUMN core.fx_rates_daily.currency IS
'The currency that the rate converts from, as a three-letter code of ISO 4217.';
COMMENT ON COLUMN core.fx_rates_daily.reporting_currency IS
'The currency that the rate converts into, as a three-letter code of ISO 4217.';
COMMENT ON COLUMN core.fx_rates_daily.rate_into_reporting_currency IS
'The value of one unit of currency in reporting_currency on the day, through the base currency of the source with no rounding: an amount in currency times this rate is the amount in reporting_currency. The reporting currency has the rate 1.';
COMMENT ON COLUMN core.fx_rates_daily.base_currency IS
'The base currency of the source of the rate, against which it publishes its rates, such as EUR for the reference rates of the euro; none where currency is the reporting currency.';
COMMENT ON COLUMN core.fx_rates_daily.base_rate IS
'The rate of currency against base_currency as the source published it, in the convention of the source, such as the units of currency for one euro for the reference rates of the euro; none where currency is the reporting currency.';
COMMENT ON COLUMN core.fx_rates_daily.rate_source IS
'The source of the rate, such as ecb for the European Central Bank or demo_fx for the demo, or identity where currency is the reporting currency.';
COMMENT ON COLUMN core.fx_rates_daily.published_on IS
'The day of the publication of the rate, rate_date or an earlier day for a carried rate; none where currency is the reporting currency.';
COMMENT ON COLUMN core.fx_rates_daily.is_carried_forward IS
'Whether the rate is the last one published before the day, on a day without a publication, such as a weekend or a holiday.';
