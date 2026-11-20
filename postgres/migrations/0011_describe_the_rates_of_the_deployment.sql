-- The rates of the deployment (ADR-0042, C7 of #227): a deployment may give its own rates, such as
-- agreed ones, in extensions/model/rates.toml, and those go before every source of the rates on
-- the days of their entries. A row of such a rate has rate_source deployment, the quote of the
-- entry as base_currency and its rate as base_rate, the first day of the entry as published_on,
-- and, for a quote that is not the reporting currency, the rate of the quote as its second leg.
-- rate_source is free text, and the columns of 0007 and 0010 hold such a row as they are: only the
-- comments that name the sources change, so no row is rewritten and nothing is granted.

COMMENT ON TABLE core.fx_rates_daily IS
'The rates into the reporting currency: one row for each calendar day, each currency of the accounts and each reporting currency, with the last rate that the sources published on or before the day, or the rate that the deployment gives for the day in extensions/model/rates.toml, which goes before them. rate_date, currency and reporting_currency are its key. Each deployment takes the rates from the sources itself, in the order that its setting AGORANOMOS_RATES_SOURCE gives: rate_source names the source of the rate of the currency, and cross_source the source of the second leg of a rate that goes through the base currency of another source or through the quote of a rate of the deployment.';
COMMENT ON COLUMN core.fx_rates_daily.rate_into_reporting_currency IS
'The value of one unit of currency in reporting_currency on the day, through the base currency of the first source of the rates with no rounding: an amount in currency times this rate is the amount in reporting_currency. The rate of the reporting currency itself is that of the same day, or of the last day before it on which one was published. A rate of the deployment in the reporting currency is this rate; one in another quote is its rate times the rate of the quote, in one division. The reporting currency has the rate 1.';
COMMENT ON COLUMN core.fx_rates_daily.base_currency IS
'The base currency of the source of the rate, against which it publishes its rates, EUR for the Bank of Italy, the European Central Bank and the demo, and KZT for the National Bank of Kazakhstan; the quote of a rate of the deployment, in which the file gives it; none where currency is the reporting currency.';
COMMENT ON COLUMN core.fx_rates_daily.base_rate IS
'The rate of currency against base_currency as the source published it, in the convention of the source: the units of currency for one euro for the Bank of Italy, the European Central Bank and the demo, and the tenge for one unit of currency for the National Bank of Kazakhstan; for a rate of the deployment, the units of base_currency for one unit of currency, as the file gives it; none where currency is the reporting currency.';
COMMENT ON COLUMN core.fx_rates_daily.rate_source IS
'The source of the rate of currency: bank_of_italy for the Bank of Italy, whose rates it calls indicative and no financial benchmark, nbk for the National Bank of Kazakhstan, ecb for the European Central Bank, demo_fx for the demo, deployment for a rate that the deployment gives in extensions/model/rates.toml, which goes before the sources, or identity where currency is the reporting currency.';
COMMENT ON COLUMN core.fx_rates_daily.published_on IS
'The day of the publication of the rate, rate_date or an earlier day for a carried rate; the first day of its entry for a rate of the deployment; none where currency is the reporting currency.';
COMMENT ON COLUMN core.fx_rates_daily.is_carried_forward IS
'Whether the rate is the last one published before the day, on a day without a publication, such as a weekend or a holiday; never for a rate of the deployment, which holds on every day of its entry.';
COMMENT ON COLUMN core.fx_rates_daily.cross_source IS
'The source of the second leg of a rate that goes through the base currency of its source, the rate of base_currency against the base currency of the first source of the rates, such as bank_of_italy for a rate of RUB of nbk through KZT into the euro, or through the quote of a rate of the deployment that is not the reporting currency, the source of the rate of that quote; none for a rate against the base currency of the first source.';
COMMENT ON COLUMN core.fx_rates_daily.cross_rate IS
'The rate of the second leg as cross_source published it, in its convention, such as the tenge for one euro of the Bank of Italy, or the tenge for one euro of the National Bank of Kazakhstan, or the rate of the quote of a rate of the deployment; none for a rate against the base currency of the first source.';
COMMENT ON COLUMN core.fx_rates_daily.cross_published_on IS
'The day of the publication of the second leg, the day of the publication of the rate or the last day before it on which cross_source published one, as the National Bank of Kazakhstan publishes on every calendar day and the Bank of Italy on the business days of TARGET, and for a rate of the deployment the last day on or before rate_date; none for a rate against the base currency of the first source.';
