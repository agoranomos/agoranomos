# Rename a field or add your own definitions

The names and descriptions of the tables and fields of the marts in Metabase come from the dbt project of Agoranomos, in English. Your deployment can give them names of its own, descriptions of its own, or a note of its own, such as what a conversion is in your business, in a file that releases never change ([ADR-0016](../adr/0016-keep-the-metadata-of-the-marts-as-code.md)). An edit in the Metabase UI does not last: the next start sets the metadata of the code again, and its log says so.

## Write your overrides

1. Open `extensions/metadata/overrides.toml`, in the folder that `AGORANOMOS_EXTENSIONS_DIR` names, and create the file if it is not there: `./agoranomos init` creates the folder.
2. Add an entry for each table or field. A table is `[marts.<table>]`, and a field of it is `[marts.<table>.columns.<column>]`:

    ```toml
    [marts.ad_stats_campaign_daily]
    display_name = "Campaigns by Day"

    [marts.ad_stats_campaign_daily.columns.conversions]
    display_name = "Purchases"
    note = "In this business, a conversion is a paid order in the online shop."
    ```

    `display_name` and `description` replace what the release says. `note` keeps the description of the release and adds yours after an empty line, so that an improved description in a later release still reaches you: prefer it to `description`.

3. See what will change: `./agoranomos metadata diff` prints each change, and changes nothing.
4. Apply it: `./agoranomos up`. `./agoranomos logs metabase-init` shows one line for each change; on the first start with the demo, `./agoranomos logs demo-init` shows them, as it applies the metadata once the demo pipeline has built the marts.

The tables and columns go by their names in the warehouse, the ones that a SQL query uses, which Admin > Table Metadata in Metabase shows next to the display names. [Describe the marts](../reference/describe-the-marts.md) has the rules that the names and descriptions of the release follow.

## What the file can do

- It names the tables of `marts`, which Metabase shows, and their columns.
- Each value is a text in quotes. A display name is one line of at most 100 characters, a description or a note at most 2,000 characters, and the file at most 256 KiB.
- A mistake stops `metabase-init` before it writes anything, with one line that names the entry, and `up` fails: fix the file and run `up` again. The same happens after an upgrade that renames or removes a column that your file names: rename the entry, or delete it.
- To go back to the name or the description of the release, delete the entry and run `up`.
- A change or the removal of an entry shows in the log as the `revert of an edit`, with the value of the entry before: the jobs keep no copy of the earlier file, so they cannot tell it from an edit in the Metabase UI.
- Two columns of a table cannot get one display name.
- Metabase OSS shows one name for each field, to everyone, so names in another language are for the whole deployment. Keep the English word in a note, so that a question asked in English still finds the field.
- It changes Metabase only: SQL clients, and the planned AI access to the warehouse, see the comments of the release.

A description is not an instruction: write facts about the data, and never copy text from an ad platform into the file, because anyone can write that text.

## The questions of the demo

The questions of the demo take the titles of their columns and axes from the display names, with the entries of this file, on every start; the ratios that they compute, such as the click-through rate, are no column of a mart and have titles of their own: `demo-init` puts the settings of display of each question back as the code says ([See the demo](see-the-demo.md#what-later-starts-do)). So a display name that you change here reaches their titles at the next start, which logs it as a `revert of an edit` of the `visualization_settings` of each question that it changed.

## Keep the file in git

Keep the extensions folder in a git repository of your own ([ADR-0013](../adr/0013-keep-extensions-apart-from-the-base-package.md)), so that the words of your deployment have a history and can be reviewed, as the metadata of the release can.
