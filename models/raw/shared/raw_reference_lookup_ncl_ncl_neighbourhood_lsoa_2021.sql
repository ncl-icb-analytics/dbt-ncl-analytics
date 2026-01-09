-- Raw layer model for reference_lookup_ncl.NCL_NEIGHBOURHOOD_LSOA_2021
-- Source: "MODELLING"."LOOKUP_NCL"
-- Description: Analyst-managed reference datasets and business rules in the MODELLING environment
-- This is a 1:1 passthrough from source with standardized column names
select
    "LSOA_2021_CODE" as lsoa_2021_code,
    "LSOA_2021_NAME" as lsoa_2021_name,
    "NEIGHBOURHOOD_CODE" as neighbourhood_code,
    "NEIGHBOURHOOD_NAME" as neighbourhood_name,
    "START_DATE" as start_date
from {{ source('reference_lookup_ncl', 'NCL_NEIGHBOURHOOD_LSOA_2021') }}
