-- Raw layer model for aic.BASE_GOV__IMD_2019
-- Source: "DATA_LAKE__NCL"."AIC_DEV"
-- Description: AIC pipelines
-- This is a 1:1 passthrough from source with standardized column names
select
    "LSOA_CODE_2011" as lsoa_code_2011,
    "LSOA_NAME_2011" as lsoa_name_2011,
    "LA_CODE_2019" as la_code_2019,
    "LA_NAME_2019" as la_name_2019,
    "IMD_RANK" as imd_rank,
    "IMD_DECILE" as imd_decile
from {{ source('aic', 'BASE_GOV__IMD_2019') }}
