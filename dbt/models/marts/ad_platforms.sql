-- The platforms of the model, a view (ADR-0037): the rows of the seed of the platforms of the base
-- package, written into the text of the view when it is built (macros/platforms.sql).
select platform, platform_name, kind, mimics, is_demo
from {{ platforms(ref('seed_ad_platforms')) }}
