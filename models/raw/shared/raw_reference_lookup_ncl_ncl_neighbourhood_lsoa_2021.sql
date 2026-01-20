{{
    config(
        description="Raw layer (Analyst-managed reference datasets and business rules in the MODELLING environment). 1:1 passthrough with cleaned column names. \nSource: MODELLING.LOOKUP_NCL.NCL_NEIGHBOURHOOD_LSOA_2021 \ndbt: source(''reference_lookup_ncl'', ''NCL_NEIGHBOURHOOD_LSOA_2021'') \nColumns:\n  LSOA_2021_CODE -> lsoa_2021_code\n  LSOA_2021_NAME -> lsoa_2021_name\n  NEIGHBOURHOOD_CODE -> neighbourhood_code\n  NEIGHBOURHOOD_NAME -> neighbourhood_name\n  START_DATE -> start_date"
    )
}}
select
    "LSOA_2021_CODE" as lsoa_2021_code,
    "LSOA_2021_NAME" as lsoa_2021_name,
    "NEIGHBOURHOOD_CODE" as neighbourhood_code,
    "NEIGHBOURHOOD_NAME" as neighbourhood_name,
    "START_DATE" as start_date
from {{ source('reference_lookup_ncl', 'NCL_NEIGHBOURHOOD_LSOA_2021') }}
