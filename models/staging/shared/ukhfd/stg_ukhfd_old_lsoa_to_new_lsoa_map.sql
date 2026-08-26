{{
    config(materialized = 'table')
}}

-- 2011 LSOA to 2021 LSOA bridge; one row per old code (splits collapse to
-- the lowest new code - LAD attribution is stable across splits)
--
-- Source: UKHFD.Old_LSOA_To_New_LSOA_To_LAD.dim_Map_SCD
--   source('ukhfd_geo', 'old_lsoa_to_new_lsoa_map') -> raw_ukhfd_old_lsoa_to_new_lsoa_map
select
    old_lsoa_code
    , new_lsoa_code
from {{ ref('raw_ukhfd_old_lsoa_to_new_lsoa_map') }}
where is_latest = 1
qualify row_number() over (
    partition by old_lsoa_code
    order by new_lsoa_code
) = 1
