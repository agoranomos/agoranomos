-- The names that each campaign had, with the time from which and until which it had each.
select
    platform,
    account_id,
    campaign_id,
    observed_name,
    valid_from,
    valid_to,
    valid_to is null as is_current
from {{ ref('int_ad_campaign_names__collapsed') }}
