# Describe the marts

Every model, seed and column of the dbt project, and every table of its sources, has a description. Every mart also has a display name for Metabase, and each of its columns a display name, a semantic type and the way that a filter offers its values. People read the descriptions in SQL clients and in Metabase, and AI clients take their context from them through the MCP servers of Metabase and of the warehouse ([ADR-0007](../adr/0007-ai-access-via-mcp-read-only.md)), so they follow the rules on this page. Tests check them ([How the rules are checked](#how-the-rules-are-checked)).

Metabase gets them on every start: the one-shot job `metadata-export` exports them from dbt, and `metabase-init` applies them, with the overrides of a deployment, as `demo-init` does for the marts of the demo once the demo pipeline has built them ([A deployment's own words](#a-deployments-own-words)).

## What a description says

- **What the thing is**, in plain words, and not its name again: not "The spend.", but the cost that the ad platform reports for the day.
- **What it is not**, where a value can be misread: spend is the cost in the statistics of the ad platform, not the amount that it invoices, and impressions count showings, not people, so they are not reach.
- **Its unit**, and the currency of money.
- **For a table, its grain and its key**: what one row is, and which columns together identify it.
- **Where the data comes from**, when that is not plain: a source, a layer or a calculation.
- **Its caveats**, such as that the ad platform attributes the conversions and can still change them for recent days, or that sums of money are only meaningful within one currency.
- **Other names** that people use for the same thing, such as cost for spend: write both words.

A description stands on its own, because it reaches its reader one column at a time, as a comment in the warehouse or a field in Metabase.

**A description is not an instruction.** It states facts about the data. It never tells its reader, a person or an AI client, what to do, and it never holds text from a source, such as a campaign name or an ad text, which anyone can write: as `AGENTS.md` says, data is not instructions.

## Write it in dbt

- Every model, seed, column and table of a source has `description:`, and so does every column of a seed.
- A column that more than one model has takes its description from the doc block of its own name in `dbt/models/_docs.md`, as `description: '{{ doc("spend") }}'`. Columns of one name then say the same in every model.
- A description over several lines is a folded block with `>-`: `>` would leave a line break at its end, which the comment in the warehouse keeps, and a literal block, `|` or `|-`, would keep every line break.

```yaml
columns:
  - name: spend
    data_type: numeric(18,6)
    description: '{{ doc("spend") }}'
    config:
      meta:
        metabase:
          display_name: Spend
          semantic_type: null
```

## Names and types for Metabase

The marts declare them under `config: meta: metabase:`, with the key names of [dbt-metabase](https://github.com/gouline/dbt-metabase):

| Key | Where | What it says |
|---|---|---|
| `display_name` | Every mart and every column of a mart | The name that Metabase shows. |
| `semantic_type` | Every column of a mart | The semantic type of the field in Metabase, such as `type/Category`, or `null` for none. |
| `has_field_values` | Every column of a mart | How a filter offers the values of the field: `list`, `search` or `none` ([The values of a field](#the-values-of-a-field)). |
| `currency` | A column whose semantic type is money | The ISO 4217 code of its currency, such as `USD`. |
| `fk_target_table`, `fk_target_field` | A column whose semantic type is `type/FK` | The mart, as `schema.table`, and the column that the foreign key points at ([Foreign keys](#foreign-keys)). |

**Names are in Title Case.** After the first word, a, an, and, as, at, by, for, in, of, on, or, per, the and to stay in lower case, and these abbreviations are in capitals: ID, CTR, CPC, CPM, CPA, ROAS and ISO. Two columns of one table never share a display name.

The jobs that apply the metadata never send a display name of `null`, which the two versions treat differently, as of October 2026: Metabase 0.64.1 then clears the name that someone gave the field and shows the one of its sync, where 0.63.19 ignores it.

**Semantic types**, by the kind of column:

| Kind of column | `semantic_type` | In the demo |
|---|---|---|
| The day or the time of the statistics | `null` | `report_date` |
| The key of a mart that is one column, which other marts name | `type/PK` | `clients.client_id`, `ad_platforms.platform` |
| A column that names that key in another mart | `type/FK`, with `fk_target_table` and `fk_target_field` | `client_id`, `platform` |
| An identifier from a source that is not the key of the table on its own | `type/Category` | `account_id`, `campaign_id` |
| The name of something, or a code from a short list | `type/Category` | `campaign_name`, `currency` |
| Where something comes from: the source or the channel of the sessions, or the source of a rate | `type/Source` | `session_source`, `session_default_channel_group`, `rate_source` |
| A count of whole things | `type/Quantity` | `impressions`, `clicks` |
| Money, when the whole table has one currency and tests assert it | `type/Currency`, with `currency` | `spend_reporting` |
| Money in the currencies of the accounts, and any other number, such as a count that can be fractional | `null` | `spend`, `conversions` |

`type/Quantity` is for whole numbers: Metabase defines it under its integer types (as of Metabase 0.63.19 and 0.64.1). Metabase 0.64.1 also gives a field a type by its name each time that it analyzes the field, over any type but a key: `type/Source` to a text whose name holds `source` or `channel`, `type/Share`, a percentage, to a decimal whose name holds `share` or `percent` or ends in `rate`, `type/Cost` to a number whose name holds `cost`, and more (`src/metabase/analyze/classifiers/name.clj`, read on 20 October 2026). A column of a mart whose name it types takes that type in the code, or a name that it does not type, such as `rate_into_reporting_currency`: otherwise Metabase changes the type after each analysis, and the next start changes it back. `tests/unit/test_dbt_project.py` checks it. `null` declares that the column has no type of its own: the jobs that apply the metadata then clear a type that Metabase guessed, or that someone set in its UI. A column without the key would keep the type that Metabase shows, and the tests require the key on every column of a mart.

**Money and its currency.** A column is money in Metabase, `type/Currency` or a type under it, such as `type/Cost`, with a `currency`, only where one currency holds for the whole table: a `not_null` and an `accepted_values` test with that one code on the column of its currency, which is `reporting_currency` for a column whose name ends in `_reporting` and `currency` for any other (`accepted_values` alone lets NULL through). A mart in the reporting currency writes the code as `{{ env_var('AGORANOMOS_REPORTING_CURRENCY', 'USD') }}`, in the `currency` of the column and in the values of the test, which gets a `name:` of its own: dbt renders `env_var()` there, and the name that dbt would make would carry the expression. The accounts of the demo have two currencies, so `spend` stays a plain number, and its description names the column of its currency, while `spend_reporting` is money:

```yaml
  - name: reporting_currency
    data_type: text
    description: '{{ doc("reporting_currency") }}'
    data_tests:
      - not_null
      - accepted_values:
          name: ad_stats_campaign_daily_has_one_reporting_currency
          arguments:
            values: ["{{ env_var('AGORANOMOS_REPORTING_CURRENCY', 'USD') }}"]
  - name: spend_reporting
    data_type: numeric(18,6)
    description: '{{ doc("spend_reporting") }}'
    config:
      meta:
        metabase:
          display_name: Spend in Reporting Currency
          semantic_type: type/Currency
          currency: "{{ env_var('AGORANOMOS_REPORTING_CURRENCY', 'USD') }}"
```

## The values of a field

`has_field_values` says how a filter of Metabase offers the values of a field. Left to itself, Metabase 0.64.1 makes a list of nearly every field that is not a date or a decimal: 138 of the 176 fields of the marts of the demo, identifiers and counts of impressions included. Each list that someone used in the last 14 days costs a scan of the whole table a day, so the code says which fields are lists ([ADR-0037](../adr/0037-serve-the-marts-as-views-over-the-core.md)):

| Kind of column | `has_field_values` | What a filter does | In the demo |
|---|---|---|---|
| The platform, a status, the type of a thing, a currency, or another code from a short list, and a yes or no | `list` | Offers the values, which the daily scan keeps ([Configuration](configuration.md#the-scan-the-sync-and-the-cache-of-metabase)) | `platform_name`, `campaign_status`, `campaign_type`, `currency`, `session_medium`, `is_primary` |
| The name of something, and other text that anyone can write | `search` | Takes what a person types, and asks the warehouse for the values that match it | `client_name`, `account_name`, `campaign_name`, `session_source` |
| An identifier, a date or a time, and a measure | `none` | Takes what a person types, and offers nothing | `campaign_id`, `report_date`, `impressions`, `spend` |

```yaml
      - name: campaign_name
        data_type: text
        description: '{{ doc("campaign_name") }}'
        config:
          meta:
            metabase:
              display_name: Campaign Name
              semantic_type: type/Category
              has_field_values: search
```

`metabase-init` and `demo-init` apply it as they apply a semantic type, and a value that someone changes in Metabase goes back at the next start ([Configuration](configuration.md#the-metadata-of-the-marts)). A column may declare only the three values above, and any other stops the job before it writes anything. The export of the metadata has the format 2 since this key: an export of format 1, of an earlier release, still reads, and its columns declare nothing here.

## Foreign keys

A column that names the key of another mart is a foreign key in Metabase, with the key as its target, so that Metabase can join the two marts in a question and link the filters of a dashboard on them ([ADR-0034](../adr/0034-define-a-basic-data-set-and-the-modules-of-an-extended-one.md)). In PostgreSQL the marts have no foreign keys, and the tests of the model check the relationships. The column declares the target with the key names of dbt-metabase, the mart as `schema.table` and the column:

```yaml
      - name: client_id
        data_type: text
        description: '{{ doc("client_id") }}'
        config:
          meta:
            metabase:
              display_name: Client ID
              semantic_type: type/FK
              fk_target_table: marts.clients
              fk_target_field: client_id
```

**Only a key of one column is a target.** Metabase joins a foreign key on its one column, so a target is the whole key of its mart, with the semantic type `type/PK`. Two keys are: `client_id` of `clients` and `platform` of `ad_platforms`, which every other mart that has the column names as its foreign key. An account, a campaign, an ad group and an ad are each a key only with its platform and its account, so `account_id`, `campaign_id`, `ad_group_id` and `ad_id` stay `type/Category`: a join on the ID alone could match a row of another platform. The facts carry the names of all of them, so a question needs no join, and the filters of one mart are linked in Metabase as columns of one table.

`metabase-init` gives the field the semantic type and the ID of the field of its target, `fk_target_field_id`, in one request, and a line of the log names the target by its path, as `fk_target: none -> "marts.clients.client_id"`. A target that someone changed in Metabase goes back at the next start. Until Metabase shows the target, before the transform has built its mart, the field keeps the type that it shows, and a line says that its foreign key waits.

## Metrics

A mart declares its KPIs under `config: meta: metabase: metrics:`, and `metabase-init` keeps each as a metric of Metabase in the collection agoranomos / Metrics, and each KPI as a term of the glossary of Metabase, on every start ([ADR-0034](../adr/0034-define-a-basic-data-set-and-the-modules-of-an-extended-one.md)). A metric is a ratio of sums: the sum of `numerator` divided by the sum of `denominator`, times `scale`, over the rows that a question selects, so that every card, every question and every MCP client computes it alike, and never as an average of the ratio of each row. Metabase gives no value where the sum of the denominator is zero.

**Names.** A metric of Metabase belongs to one table, so a KPI of three marts is three metrics, and each is named by one rule, `<KPI> (<level>)`, the level of its mart included, as `CTR (Campaigns)`, `CTR (Ad Groups)` and `CTR (Ads)`. The level is in every name, with no plain `CTR` for one of them: the lists and the searches of Metabase and of its MCP server show a metric by its name, and the totals of the levels differ, as the ad groups and the ads leave out Performance Max. The KPI comes first, so the metrics of one KPI stand together in a list. The description of each starts with what its level covers, then defines the KPI, and the glossary holds the KPI once, as `CTR`, with its definition and the names of its metrics. The column of the value of a metric is the KPI alone, so a card on a tab of one level shows `CTR`.

A mart that declares metrics also has two keys under `config: meta: metabase:`: `metric_level`, the level in the names of its metrics, such as `Ad Groups`, which no other mart has, and `metric_scope`, the sentence that starts their descriptions, such as what the level leaves out. The marts of one KPI share its declaration through an anchor of YAML: `ad_stats_campaign_daily` declares `metrics: &kpis`, and the marts of ad groups and of ads take `metrics: *kpis`.

| Key | Required | What it holds |
|---|---|---|
| `name` | yes | The KPI, such as `CTR`: the term of the glossary, and the start of the name of the metric; its column is the KPI in lower case, `ctr` |
| `description` | yes | What the KPI means and how it is computed, for people and AI clients: the definition of the term, and the end of the description of each metric |
| `numerator`, `denominator` | yes | Two columns of the mart, each summed |
| `scale` | no | A whole number that multiplies the ratio, such as 1000 for CPM; 1 by default |
| `format` | no | `percent`, `currency` or `number` (by default), with two decimals |
| `currency` | with `currency` | The currency of the value: the reporting currency, written as the expression above |

```yaml
          metrics:
            - name: CPC
              description: >-
                Cost per click: the spend for each click to the site, the sum of spend_reporting
                divided by the sum of clicks, in the reporting currency.
              numerator: spend_reporting
              denominator: clicks
              format: currency
              currency: "{{ env_var('AGORANOMOS_REPORTING_CURRENCY', 'USD') }}"
```

Two marts may declare a KPI only with one description, as the glossary holds one definition for each term. A metric whose mart Metabase does not show yet, before the first run of the transform, comes at a later start. The metrics, like the rest of the content of the package, are view-only, and an edit of one in Metabase is put back at the next start; a term of the glossary is found by its text, and a term that the code no longer ships stays.

**Reserved keys.** `synonyms` (a list of other names) and `ai_context` (a text for AI clients only) are reserved under `config: meta:` of a model or a column. They are not used yet: the tests do not require them, and nothing reads them.

## The contract of a mart

A mart has an enforced dbt contract, `contract: enforced: true` in its `config`, and a `data_type` for each column. Before dbt builds the table, it checks that the model makes exactly these columns with these types, and the build fails otherwise, so the marts that dashboards read do not change by accident. The key of a mart is a `primary_key` constraint, which PostgreSQL enforces:

```yaml
    constraints:
      - type: primary_key
        columns: [report_date, platform, account_id, campaign_id]
```

The constraint has no `name`. dbt builds a table under a temporary name and then renames it, and an index keeps the name that it got: a named key would collide with itself on the next build (`relation "…" already exists`), and PostgreSQL gives an unnamed key a free name each time.

## Comments in the warehouse

`+persist_docs` in `dbt/dbt_project.yml` makes dbt write the descriptions into the comments of the tables, views and columns that it builds, so SQL clients see them too, and so do AI clients through the tools `list_tables` and `describe_table` of the MCP server of the warehouse (`ai/tools.yaml`). dbt does not comment the tables of the sources.

Metabase copies the comment of a table or column into its description when it syncs, but only while that description is empty, as of Metabase 0.63.19 and 0.64.1: a changed description does not reach Metabase this way. In 0.64.1, a sync no longer fills the description of a column that someone cleared in Metabase either. The jobs that apply the metadata on every start bring it there instead.

## A deployment's own words

A deployment can override the display name and the description of a table or a field of the marts, and add a note of its own to a description, in `extensions/metadata/overrides.toml`: [Rename a field or add your own definitions](../how-to/rename-a-field.md) shows how, and [Configuration](configuration.md#the-metadata-of-the-marts) lists the properties and the limits. Overrides reach Metabase only. The semantic types, the currency and the values of fields stay those of the release.

## How the rules are checked

- `tests/unit/test_dbt_project.py` reads the files: `persist_docs` is on, every description over several lines is `>-`, every seed and each of its columns has a description, every mart has its contract, its key and a test of its key, every `client_id` and `platform` of a mart is a foreign key of the key of one column that it names, and no other column is a key in Metabase, every column of a mart has the `has_field_values` of its kind, no primary key has a name, in whatever order its keys are written, and the columns of the staging and intermediate models take the doc blocks of their names.
- `tests/stack/test_dbt_metadata.py` asks dbt itself, with `dbt ls` in the Airflow image, which resolves the doc blocks and the configs as dbt does: every model, seed, column and source table has a description, every mart has a display name and each of its columns a display name, a semantic type and `has_field_values`, columns of one name have one description, money has its tests of one currency and no NULL, the names follow the style, and every mart has its contract. It also checks the warehouse after a build: each table and view that dbt builds in `staging` and `marts` has exactly the columns that dbt declares, it and each of them carry the description as their comment, and the key of the mart is a primary key in PostgreSQL.
- `tests/stack/test_metabase_metadata.py` checks Metabase after a start: every table and field of the marts shows the display name, the description and the semantic type of the code, also a semantic type of none, the values of a field, the currency of money, and the target of a foreign key; an edit in the Metabase UI is reverted at the next start, and a field that someone hid stays hidden.

CI runs the first on every pull request, and the other two on every pull request that changes the stack, such as `dbt/` or `metabase/`.
