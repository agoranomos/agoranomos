{#- The descriptions of the columns that more than one model has: a column takes the block of its
    own name, so that it says the same in every model (docs/reference/describe-the-marts.md). -#}

{% docs report_date %}
The day that the statistics cover, as a date in the time zone of the ad account (UTC in the demo).
{% enddocs %}

{% docs account_id %}
The ID of the ad account that the statistics belong to, as the ad platform gives it.
{% enddocs %}

{% docs campaign_id %}
The ID of the campaign, as the ad platform gives it. It is unique within its ad account; across accounts, use it together with account_id.
{% enddocs %}

{% docs campaign_name %}
The name of the campaign on the ad platform. A campaign can be renamed, so follow one campaign over time by campaign_id.
{% enddocs %}

{% docs currency %}
The currency of the ad account, as a three-letter ISO 4217 code, such as USD. Spend is in this currency, so add up spend only within one currency.
{% enddocs %}

{% docs impressions %}
How many times the ads of the campaign were shown on the day, as the ad platform counts them.
{% enddocs %}

{% docs clicks %}
How many times the ads of the campaign were clicked on the day, as the ad platform counts them.
{% enddocs %}

{% docs spend %}
What the ad platform charged for the ads of the campaign on the day, also called cost, in the currency of the ad account (see currency), with two decimal places.
{% enddocs %}

{% docs conversions %}
The conversions that the ad platform attributes to the campaign on the day. Which actions count as conversions, and how they are attributed, are set in the ad account. Attribution can give a campaign part of a conversion, so the number can be fractional, and the platform can still change it for recent days.
{% enddocs %}
