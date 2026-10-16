-- The platforms of the model, whichever source names them.
select platform, platform_name, kind, mimics, is_demo
from {{ ref('int_ad_platforms__unioned') }}
