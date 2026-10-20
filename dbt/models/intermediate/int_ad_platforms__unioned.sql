-- The ad platforms of the model, from the seed of the base package: the one list that every mart
-- that names a platform joins, so that a platform from another source needs one branch here.
select platform, platform_name, kind, mimics, is_demo
from {{ ref('seed_ad_platforms') }}
