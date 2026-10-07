{#- The descriptions of the columns that more than one model has: a column takes the block of its
    own name, so that it says the same in every model (docs/reference/describe-the-marts.md). -#}

{% docs report_date %}
The day that the statistics cover, as a date in the time zone of the ad account (UTC in the demo). It is not the day that they were loaded.
{% enddocs %}

{% docs account_id %}
The ID of the ad account that the statistics belong to, as the ad platform gives it.
{% enddocs %}

{% docs campaign_id %}
The ID of the campaign, as the ad platform gives it. It is unique within its ad account, and some platforms make it unique across accounts too; account_id and campaign_id together identify a campaign on any platform.
{% enddocs %}

{% docs campaign_name %}
The name of the campaign on the ad platform. A campaign can be renamed, so its name can differ between days while its campaign_id stays the same.
{% enddocs %}

{% docs currency %}
The currency of the ad account, as a three-letter ISO 4217 code, such as USD. The amounts of money in the row, such as spend, are in this currency, so rows with different codes hold amounts in different units.
{% enddocs %}

{% docs impressions %}
How many times the ads of the campaign were shown on the day, as the ad platform counts them. It counts showings, not people: one person can see an ad many times, so impressions are not reach.
{% enddocs %}

{% docs clicks %}
How many times the ads of the campaign were clicked on the day, as the ad platform counts them. It counts clicks, not people, and not the visits that web analytics records, which can differ.
{% enddocs %}

{% docs spend %}
What the ads of the campaign cost on the day, as the ad platform reports it in its statistics, in the currency of the ad account (see currency), with two decimal places. Also called cost or ad spend. It is not the amount that the platform invoices, which can differ from it, for example by taxes or later credits.
{% enddocs %}

{% docs conversions %}
The conversions that the ad platform attributes to the campaign on the day. Which actions count as conversions, and how they are attributed, are set in the ad account. Attribution can give a campaign part of a conversion, so the number can be fractional, and the platform can still change it for recent days. It is the count of the ad platform, which can differ from the conversions that web analytics or a CRM records.
{% enddocs %}
