select 
    census_date,
    provider_code,
    provider_site,
    count,
    ccg_code,
    gp_practice_code,

    gender,
    ethnicity,
    age,       
from {{ ref('raw_ptl_ptl_weekly_data') }}