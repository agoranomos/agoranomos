# Describe the marts

Every model and column of the dbt project, and every table of its sources, has a description. Every mart also has a display name for Metabase, and each of its columns a display name and a semantic type. People read the descriptions in SQL clients and in Metabase, and the planned AI access through MCP ([ADR-0007](../adr/0007-ai-access-via-mcp-read-only.md)) is to take its context from them, so they follow the rules on this page. Tests check them ([How the rules are checked](#how-the-rules-are-checked)).

Metabase gets them on every start: the one-shot job `metadata-export` exports them from dbt, and `metabase-init` applies them, with the overrides of a deployment, as `demo-init` does for the mart of the demo once the demo pipeline has built it ([A deployment's own words](#a-deployments-own-words)).

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

- Every model, column and table of a source has `description:`.
- A column that more than one model has takes its description from the doc block of its own name in `dbt/models/_docs.md`, as `description: '{{ doc("spend") }}'`. Columns of one name then say the same in every model.
- A description over several lines is a folded block with `>-`: `>` would leave a line break at its end, which the comment in the warehouse keeps, and a literal block, `|` or `|-`, would keep every line break.

```yaml
columns:
  - name: spend
    data_type: numeric(12,2)
    description: '{{ doc("spend") }}'
    config:
      meta:
        metabase:
          display_name: Spend
          semantic_type: type/Currency
          currency: USD
```

## Names and types for Metabase

The marts declare them under `config: meta: metabase:`, with the key names of [dbt-metabase](https://github.com/gouline/dbt-metabase):

| Key | Where | What it says |
|---|---|---|
| `display_name` | Every mart and every column of a mart | The name that Metabase shows. |
| `semantic_type` | Every column of a mart | The semantic type of the field in Metabase, such as `type/Category`, or `null` for none. |
| `currency` | A column whose semantic type is money | The ISO 4217 code of its currency, such as `USD`. |

**Names are in Title Case.** After the first word, a, an, and, as, at, by, for, in, of, on, or, per, the and to stay in lower case, and these abbreviations are in capitals: ID, CTR, CPC, CPM, CPA, ROAS and ISO. Two columns of one table never share a display name.

The jobs that apply the metadata never send a display name of `null`, which the two versions treat differently: Metabase 0.64.1 then clears the name that someone gave the field and shows the one of its sync, where 0.63.19 ignores it.

**Semantic types**, by the kind of column:

| Kind of column | `semantic_type` | In the demo |
|---|---|---|
| The day or the time of the statistics | `null` | `report_date` |
| An identifier from a source that is not the key of the table on its own | `type/Category` | `account_id`, `campaign_id` |
| The name of something, or a code from a short list | `type/Category` | `campaign_name`, `currency` |
| A count of whole things | `type/Quantity` | `impressions`, `clicks` |
| Money, when the whole table has one currency and tests assert it | `type/Currency`, with `currency` | `spend` |
| Any other number, such as a count that can be fractional | `null` | `conversions` |

`type/Quantity` is for whole numbers: Metabase defines it under its integer types (as of Metabase 0.63.19 and 0.64.1). `null` declares that the column has no type of its own: the jobs that apply the metadata then clear a type that Metabase guessed, or that someone set in its UI. A column without the key would keep the type that Metabase shows, and the tests require the key on every column of a mart.

**Money and its currency.** `type/Currency` and the types under it, such as `type/Cost`, go together with `currency`, and only in a mart whose column `currency` has an `accepted_values` test with that one code and a `not_null` test: `accepted_values` lets NULL through. The demo is in USD only. In a mart that mixes currencies, the columns of money stay plain numbers, and their descriptions name the column of the currency.

**Reserved keys.** `synonyms` (a list of other names) and `ai_context` (a text for AI clients only) are reserved under `config: meta:` of a model or a column, for the step that adds a glossary and metrics. They are not used yet: the tests do not require them, and nothing reads them.

## The contract of a mart

A mart has an enforced dbt contract, `contract: enforced: true` in its `config`, and a `data_type` for each column. Before dbt builds the table, it checks that the model makes exactly these columns with these types, and the build fails otherwise, so the marts that dashboards read do not change by accident. The key of a mart is a `primary_key` constraint, which PostgreSQL enforces:

```yaml
    constraints:
      - type: primary_key
        columns: [report_date, account_id, campaign_id]
```

The constraint has no `name`. dbt builds a table under a temporary name and then renames it, and an index keeps the name that it got: a named key would collide with itself on the next build (`relation "…" already exists`), and PostgreSQL gives an unnamed key a free name each time.

## Comments in the warehouse

`+persist_docs` in `dbt/dbt_project.yml` makes dbt write the descriptions into the comments of the tables, views and columns that it builds, so SQL clients see them too, and so will the planned AI access to the warehouse. dbt does not comment the tables of the sources.

Metabase copies the comment of a table or column into its description when it syncs, but only while that description is empty, as of Metabase 0.63.19 and 0.64.1: a changed description does not reach Metabase this way. In 0.64.1, a sync no longer fills the description of a column that someone cleared in Metabase either. The jobs that apply the metadata on every start bring it there instead.

## A deployment's own words

A deployment can override the display name and the description of a table or a field of the marts, and add a note of its own to a description, in `extensions/metadata/overrides.toml`: [Rename a field or add your own definitions](../how-to/rename-a-field.md) shows how, and [Configuration](configuration.md#the-metadata-of-the-marts) lists the properties and the limits. Overrides reach Metabase only. The semantic types and the currency stay those of the release.

## How the rules are checked

- `tests/unit/test_dbt_project.py` reads the files: `persist_docs` is on, every description over several lines is `>-`, the mart has its contract and its key, and no primary key has a name, in whatever order its keys are written.
- `tests/stack/test_dbt_metadata.py` asks dbt itself, with `dbt ls` in the Airflow image, which resolves the doc blocks and the configs as dbt does: every model, column and source table has a description, every mart has a display name and each of its columns a display name and a semantic type, columns of one name have one description, money has its tests of one currency and no NULL, the names follow the style, and every mart has its contract. It also checks the warehouse after a build: each table and view that dbt builds in `staging` and `marts` has exactly the columns that dbt declares, it and each of them carry the description as their comment, and the key of the mart is a primary key in PostgreSQL.
- `tests/stack/test_metabase_metadata.py` checks Metabase after a start: every table and field of the marts shows the display name, the description and the semantic type of the code, also a semantic type of none, and the currency of money; an edit in the Metabase UI is reverted at the next start, and a field that someone hid stays hidden.

CI runs the first on every pull request, and the other two on every pull request that changes the stack, such as `dbt/` or `metabase/`.
