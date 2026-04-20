{{
    config(
        description="Raw layer (AIC pipelines). 1:1 passthrough with cleaned column names. \nSource: DATA_LAKE__NCL.AIC_DEV.BASE_GOV__IMD_2019 \ndbt: source(''aic'', ''BASE_GOV__IMD_2019'') \nColumns:\n  LSOA_CODE_2011 -> lsoa_code_2011\n  LSOA_NAME_2011 -> lsoa_name_2011\n  LA_CODE_2019 -> la_code_2019\n  LA_NAME_2019 -> la_name_2019\n  IMD_RANK -> imd_rank\n  IMD_DECILE -> imd_decile"
    )
}}
select
    "LSOA_CODE_2011" as lsoa_code_2011,
    "LSOA_NAME_2011" as lsoa_name_2011,
    "LA_CODE_2019" as la_code_2019,
    "LA_NAME_2019" as la_name_2019,
    "IMD_RANK" as imd_rank,
    "IMD_DECILE" as imd_decile
from {{ source('aic', 'BASE_GOV__IMD_2019') }}
