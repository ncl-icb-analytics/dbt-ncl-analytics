{{
    config(
        description="Raw layer (UKHFD ONS geography). 1:1 passthrough with cleaned column names. \nSource: UKHFD.Old_LSOA_To_New_LSOA_To_LAD.dim_Map_SCD \ndbt: source(''ukhfd_geo'', ''old_lsoa_to_new_lsoa_map'')"
    )
}}
select
    "Old_LSOA_Code" as old_lsoa_code,
    "New_LSOA_Code" as new_lsoa_code,
    "LAD_Code" as lad_code,
    "Is_Latest" as is_latest,
    "Effective_From" as effective_from,
    "Effective_To" as effective_to
from {{ source('ukhfd_geo', 'old_lsoa_to_new_lsoa_map') }}
