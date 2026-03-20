{{
    config(
        description="Raw layer (Analyst-managed reference datasets and business rules). 1:1 passthrough with cleaned column names. \nSource: DATA_LAKE__NCL.ANALYST_MANAGED.CANCER__QOF_ACHIEVEMENT_LONDON_2223 \ndbt: source(''reference_analyst_managed'', ''CANCER__QOF_ACHIEVEMENT_LONDON_2223'') \nColumns:\n  REGION_ODS_CODE -> region_ods_code\n  REGION_ONS_CODE -> region_ons_code\n  REGION_NAME -> region_name\n  PRACTICE_CODE -> practice_code\n  INDICATOR_CODE -> indicator_code\n  MEASURE -> measure\n  VALUE -> value"
    )
}}
select
    "REGION_ODS_CODE" as region_ods_code,
    "REGION_ONS_CODE" as region_ons_code,
    "REGION_NAME" as region_name,
    "PRACTICE_CODE" as practice_code,
    "INDICATOR_CODE" as indicator_code,
    "MEASURE" as measure,
    "VALUE" as value
from {{ source('reference_analyst_managed', 'CANCER__QOF_ACHIEVEMENT_LONDON_2223') }}
