---
status: accepted
date: 2026-10-31
decision-makers: Andrei Gromov
---

# Take the exchange rates from the Bank of Italy and the National Bank of Kazakhstan

## Context and Problem Statement

[ADR-0032](0032-connect-google-ads-first-with-the-reference-rates-of-the-ecb.md) takes the reference rates of the European Central Bank: 29 currencies against the euro, with no key. That leaves out a large part of the accounts of a real agency: the ECB has no rate for RUB since 1 March 2022, and none for KZT, BYN, UAH, AED, SAR, VND or ARS. Such a currency fails the tests of the models that convert it. The rates also load all or nothing, and do not start the transform.

The maintainer also asked whether a reporting currency of tenge can use the official rate of the National Bank of Kazakhstan, and whether Google's rates could serve. Rates become a table of the core, so that a view converts each row by one join ([ADR-0037](0037-serve-the-marts-as-views-over-the-core.md)).

Where do the rates come from, and what happens when a source or a rate is missing?

## Decision Drivers

* Every currency of a platform has a rate: the Google Ads API lists 52 live currencies.
* A rate comes from a primary source, with no key, and no intermediary.
* An official rate where a person expects one: an accountant in Kazakhstan checks the tenge against the National Bank.
* A missing rate never leaves a person without data, and never becomes 1, 0 or NULL.
* Each deployment fetches its data itself, and the product distributes no rate.

## Considered Options

* The Bank of Italy for all currencies, and the National Bank of Kazakhstan for the tenge, RUB and BYN
* The ECB, as today
* Frankfurter
* The rates of Google
* A connector for each central bank

## Decision Outcome

Chosen option: "The Bank of Italy for all currencies, and the National Bank of Kazakhstan for the tenge, RUB and BYN", because one request to the first gives what a dozen connectors would, the second gives an official tenge and the two currencies that the first lacks, and neither needs a key or an intermediary.

In practice this means:

* **Sources.**

    | Source | What it gives | Checked on 30 October 2026 |
    |---|---|---|
    | Bank of Italy, REST API `tassidicambio.bancaditalia.it/terzevalute-wf-web/rest/v1.0/dailyRates` | 150 currencies against the euro on TARGET business days, soon after 16:00 CET; for the currencies of the ECB its reference rates, and for others an average of market and institutional sources, for some the rates of their central banks | All 52 live currencies of Google Ads but RUB; history checked back to 2010; RUB and BYN unavailable since 2023 |
    | National Bank of Kazakhstan, `nationalbank.kz/rss/get_rates.cfm?fdate=DD.MM.YYYY` | The official rate of 48 currencies against the tenge on a date, with a field `quant`, as a rate may be for 10, 100 or 1,000 units | Has RUB, BYN, UZS, AED, SAR and VND |

* **A setting of the deployment.** The source of rates is a setting, and the table is one. A deployment with a reporting currency of tenge takes the National Bank of Kazakhstan. The others take the Bank of Italy, and the National Bank of Kazakhstan for RUB and BYN, which the first lacks, through the tenge. The cross USD/KZT through the Bank of Italy differs from the official rate by 0.24% on average, and by 0.52% at most, over 10 business days.
* **`core.fx_rates_daily`.** A calendar day and a currency give the rate against the base currency of the source as published (`numeric(20,10)`, with no fixed scale), the rate into the reporting currency, the source, the date of publication and a flag of a carried rate. The table covers each calendar day of each currency that appears in the accounts, carrying the last published rate forward, and a gap is not allowed: G3 catches it ([ADR-0035](0035-keep-four-layers-and-erase-the-raw-layer-after-acceptance.md)). It replaces `int_fx_rates__filled`, whose `LATERAL` reads all the facts.
* **One rounding.** The rate into the reporting currency goes through the base currency of the source with no rounding on the way, as Regulation (EC) 1103/97 asks, so a view makes one join and an amount is rounded once, for display. A change of the reporting currency in `.env` recomputes only this small table at `./agoranomos up`.
* **When a source fails.** Amounts in the currency of the account do not depend on rates. A view takes the last published rate, with `is_fx_rate_carried_forward`. A load reads the last 14 days again, so a gap closes by itself, and a view is right at once, with no rebuild. Ops and `./agoranomos status` show the age of the last rate, with a warning after 3 business days, and that check is the same as the freshness of an ad source.
* **A currency that the source lacks.** The account goes to quarantine ([ADR-0035](0035-keep-four-layers-and-erase-the-raw-layer-after-acceptance.md)), and it does not get a rate of 1 or 0.
* **Rates of a deployment.** A deployment may give its own rates in a file in `extensions/`, such as agreed ones, and those go first.
* **What is distributed.** Each deployment fetches the rates itself, once a day with a reread of 14 days, and the product ships none. The table and the descriptions of the columns name the source, as the policy of the Bank of Italy on the statistics of the ESCB asks: free reuse, commercial too, with the source quoted and the data unmodified. The portal of the rates itself states no terms, and its answers call the rates indicative and no financial benchmark.
* **The transform.** A load of rates starts it, like any other asset ([ADR-0039](0039-run-one-dbt-build-in-the-watcher-mode.md)).

The other options were not chosen for these reasons:

* **The ECB** stops at 29 currencies, and its data is contained in the Bank of Italy's for those.
* **Frankfurter** would give the same through one connector, and through an intermediary: 2,049 stars and the MIT License as of October 2026, in essence one developer, with no SLA.
* **The rates of Google** have no API: the history of `GOOGLEFINANCE` is unavailable through the Sheets API and Apps Script, and the data is "not for financial industry professional use".
* **A connector for each central bank** multiplies code and failure modes for what one request gives.

### Consequences

* Good, because every currency of Google Ads gets a rate, and RUB and BYN through the second source.
* Good, because a failure of a source costs a flag, not the data.
* Good, because a reporting currency of tenge matches what an accountant in Kazakhstan sees.
* Good, because a change of the reporting currency needs no rebuild of the facts.
* Bad, because the quotes of the Bank of Italy are indicative, and its instructions for the API are old, though the address works.
* Bad, because two sources mean two connectors, and the second has a `quant` to get right.
* Bad, because a reporting amount of today almost always uses a carried rate, as the publication comes after 16:00 CET.
* Bad, because a converted amount is an estimate on any source: only the amount in the currency of the account is exact.

## More Information

* **Implementation status, as of October 2026:** planned for v0.5 as the table, and for v0.6 as the connectors. The rates of the ECB stay in place until then. A file of the deployment's own rates is planned for v0.7.
* The facts behind this record, read on 30 October 2026: the tests above; the ECB publishes at about 16:00 CET on TARGET business days, "for information purposes only" ([reference rates](https://www.ecb.europa.eu/stats/policy_and_exchange_rates/euro_reference_exchange_rates/html/index.en.html)); [Regulation (EC) 1103/97](https://eur-lex.europa.eu/legal-content/EN/TXT/?uri=CELEX:31997R1103); the [National Bank of Kazakhstan](https://nationalbank.kz/en/exchangerates/ezhednevnye-oficialnye-rynochnye-kursy-valyut); [Frankfurter](https://frankfurter.dev/). Adverity and Supermetrics also carry the last rate to a day without one ([Adverity](https://docs.adverity.com/guides/enriching-data/currency-conversion.html), [Supermetrics](https://docs.supermetrics.com/docs/how-to-convert-currencies-in-supermetrics)).
* The terms, read on 1 November 2026: the portal `tassidicambio.bancaditalia.it` and the [manual of its API](https://tassidicambio.bancaditalia.it/terzevalute-wf-ui-web/assets/files/Operating_Instructions.pdf) state no terms, registration or rate limit; each answer carries a notice that the rates "are only indicative and are not intended to be used as a financial benchmark under Regulation (EU) 2016/1011". The [policy of the ESCB statistics](https://www.bancaditalia.it/statistiche/condizioni-utilizzo/index.html) on the site of the bank says that they "may be reused free of charge on the condition that the source is quoted ... and that the statistics (including metadata) are not modified", "irrespective of any subsequent commercial or non-commercial use", and that access may be restricted for a user "acting in a manner contrary to the interests of other users". The [copyright of the site](https://www.bancaditalia.it/footer/copyright/index.html) allows personal use only for the material of `bancaditalia.it`, which the portal is not on. The [methodology](https://www.bancaditalia.it/compiti/operazioni-cambi/portale-tassi/Metodologia-determinazione-tassi-cambio.pdf) of 4 June 2025 says that the rates follow the reference rates of the ECB at 16:00, and the 29 rates of the ECB of 30 October 2026 are the same number in both feeds. For those 29 currencies the [feed of the ECB](https://www.ecb.europa.eu/stats/eurofxref/eurofxref-daily.xml), under its [terms](https://www.ecb.europa.eu/home/disclaimer/html/index.en.html) of free use with the ECB as the source, is the fallback.
* The maintainer decided on 1 November 2026 that the second source serves RUB and BYN in every deployment, not only one that reports in tenge, so that such an account does not go to quarantine.
* Open: whether the bank counts the portal as ESCB statistics is nowhere stated. A letter of the maintainer to the bank is to ask.
* Related decisions: at acceptance this record replaces the exchange rates of [ADR-0032](0032-connect-google-ads-first-with-the-reference-rates-of-the-ecb.md), and the decision to connect Google Ads first stays.
* Revisit this record when RUB or BYN return to the first source, when the Bank of Italy changes its API or its terms, or when a deployment needs a rate that neither has.
