-- The second leg of a rate of the core (ADR-0042): the rates come from the source that the setting
-- AGORANOMOS_RATES_SOURCE of the deployment names first, and a currency that it lacks from a second
-- source, through the base currency of that one, such as RUB of the National Bank of Kazakhstan
-- through KZT of the Bank of Italy into the euro. Such a row has two sources: rate_source names
-- that of the rate of the currency, and these columns name that of the rate of its base currency
-- against the base of the first source, with the rate as that source published it and its day, so
-- that the table names the source of both legs and a later run computes the row again from the
-- table alone. A rate against the base of the first source has none of them. The rows that the
-- table held before keep their columns as they were, with none of these: no row is rewritten here.
-- The grants of 0007 on the table cover the new columns, so the migration grants nothing.

ALTER TABLE core.fx_rates_daily
  ADD COLUMN cross_source text;
ALTER TABLE core.fx_rates_daily
  ADD COLUMN cross_rate numeric(20,10) CHECK (cross_rate > 0);
ALTER TABLE core.fx_rates_daily
  ADD COLUMN cross_published_on date;
ALTER TABLE core.fx_rates_daily
  ADD CONSTRAINT fx_rates_daily_cross_leg_is_whole CHECK (
    (cross_source IS NULL) = (cross_rate IS NULL)
    AND (cross_rate IS NULL) = (cross_published_on IS NULL)
    AND cross_published_on <= rate_date
  );

COMMENT ON TABLE core.fx_rates_daily IS
'The rates into the reporting currency: one row for each calendar day, each currency of the accounts and each reporting currency, with the last rate that the sources published on or before the day. rate_date, currency and reporting_currency are its key. Each deployment takes the rates from the sources itself, in the order that its setting AGORANOMOS_RATES_SOURCE gives: rate_source names the source of the rate of the currency, and cross_source the source of the second leg of a rate that goes through the base currency of another source.';
COMMENT ON COLUMN core.fx_rates_daily.rate_into_reporting_currency IS
'The value of one unit of currency in reporting_currency on the day, through the base currency of the first source of the rates with no rounding: an amount in currency times this rate is the amount in reporting_currency. The rate of the reporting currency itself is that of the same day, or of the last day before it on which one was published. The reporting currency has the rate 1.';
COMMENT ON COLUMN core.fx_rates_daily.base_currency IS
'The base currency of the source of the rate, against which it publishes its rates, EUR for the Bank of Italy, the European Central Bank and the demo, and KZT for the National Bank of Kazakhstan; none where currency is the reporting currency.';
COMMENT ON COLUMN core.fx_rates_daily.base_rate IS
'The rate of currency against base_currency as the source published it, in the convention of the source: the units of currency for one euro for the Bank of Italy, the European Central Bank and the demo, and the tenge for one unit of currency for the National Bank of Kazakhstan; none where currency is the reporting currency.';
COMMENT ON COLUMN core.fx_rates_daily.rate_source IS
'The source of the rate of currency: bank_of_italy for the Bank of Italy, whose rates it calls indicative and no financial benchmark, nbk for the National Bank of Kazakhstan, ecb for the European Central Bank or demo_fx for the demo, or identity where currency is the reporting currency.';
COMMENT ON COLUMN core.fx_rates_daily.cross_source IS
'The source of the second leg of a rate that goes through the base currency of its source, the rate of base_currency against the base currency of the first source of the rates, such as bank_of_italy for a rate of RUB of nbk through KZT into the euro; none for a rate against the base currency of the first source.';
COMMENT ON COLUMN core.fx_rates_daily.cross_rate IS
'The rate of the second leg as cross_source published it, in its convention, such as the tenge for one euro of the Bank of Italy, or the tenge for one euro of the National Bank of Kazakhstan; none for a rate against the base currency of the first source.';
COMMENT ON COLUMN core.fx_rates_daily.cross_published_on IS
'The day of the publication of the second leg, the day of the publication of the rate or the last day before it on which cross_source published one, as the National Bank of Kazakhstan publishes on every calendar day and the Bank of Italy on the business days of TARGET; none for a rate against the base currency of the first source.';
