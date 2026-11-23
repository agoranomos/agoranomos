# How-to guides

How-to guides are short recipes for people who want to get a specific job done with agoranomos, from starting it to changing what it shows. Each guide starts from a goal and lists the steps to reach it. For background and reasoning, see [Explanation](../explanation/index.md).

## Guides

- [Run agoranomos locally](run-locally.md): start the stack on your own machine or on another machine of yours through an SSH tunnel, sign in, update it, switch to separate workers, and give a service more memory.
- [Deploy on a server](deploy-on-a-server.md): put Metabase on the internet over HTTPS, with Airflow behind an SSH tunnel and daily backups.
- [Back up and restore](back-up-and-restore.md): take, schedule and copy backups, and restore them, also on a new server.
- [See the demo](see-the-demo.md): what the demo shows, how it stays up to date, and how to turn it off.
- [Connect Google Ads](connect-google-ads.md): load the Google Ads accounts of your business every day, with the exchange rates of the Bank of Italy, of the National Bank of Kazakhstan and of the ECB, as a service account of your own Google Cloud project. No load has reached a real account yet.
- [Connect Meta](connect-meta.md): load the Meta ad accounts of your business every day, through an app and a system user of your own business portfolio. No load has reached a real account yet.
- [Connect TikTok](connect-tiktok.md): load the TikTok advertisers of your business every day into the warehouse and the marts, through an app of your own developer account and the token of a person who reaches them, with the history of each advertiser by quarters in the background. No load has reached a real advertiser yet.
- [Connect GA4](connect-ga4.md): load the sessions of the GA4 properties of your business every day into the raw layer, with their history, as a service account of your own Google Cloud project. Their core and marts are not built yet, and no load has reached a real property.
- [Manage clients and accounts](manage-clients-and-accounts.md): add a client or an account, move an account to another client, and take one out, whose data stays.
- [Read the advertising dashboard](read-the-advertising-dashboard.md): what the tabs and the filters of the dashboard Advertising show, and how its numbers are computed.
- [Talk to your data](talk-to-your-data.md): connect Claude Code, the Claude apps or another AI client to your data through MCP, choose what it may do, and turn it off.
- [Watch the pipeline](watch-the-pipeline.md): what the dashboard of the Ops collection shows about the loads, the runs of the transform and the warehouse, where its data comes from, and how to get alerts through Apprise.
- [Rename a field or add your own definitions](rename-a-field.md): give the tables and fields of the marts names, descriptions and notes of your deployment's own.

## Planned guides

- **Add a data source**: connect another ad platform, analytics or CRM source, or load files.
- **Upgrade**: move to a newer release of agoranomos.

Guides are published together with the features they describe.
