-- The conversions of a campaign on a day are the sum of its primary actions, by construction: a
-- difference means that a join doubled or lost rows.
with by_action as (
    select report_date, platform, account_id, campaign_id, sum(conversions) as conversions
    from {{ ref('int_ad_conversions__unioned') }}
    where is_primary
    group by report_date, platform, account_id, campaign_id
)

select s.report_date, s.platform, s.account_id, s.campaign_id
from {{ ref('int_ad_stats__unioned') }} as s
left join by_action as a
    on a.report_date = s.report_date and a.platform = s.platform
    and a.account_id = s.account_id and a.campaign_id = s.campaign_id
where s.conversions <> coalesce(a.conversions, 0)
