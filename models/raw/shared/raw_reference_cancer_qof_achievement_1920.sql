{{
    config(
        description="Raw layer (Analyst-managed reference datasets and business rules). 1:1 passthrough with cleaned column names. \nSource: DATA_LAKE__NCL.ANALYST_MANAGED.CANCER__QOF_ACHIEVEMENT_1920 \ndbt: source(''reference_analyst_managed'', ''CANCER__QOF_ACHIEVEMENT_1920'') \nColumns:\n  PRACTICE_CODE -> practice_code\n  INDICATOR_CODE -> indicator_code\n  MEASURE -> measure\n  VALUE -> value"
    )
}}
select
    "PRACTICE_CODE" as practice_code,
    "INDICATOR_CODE" as indicator_code,
    "MEASURE" as measure,
    "VALUE" as value
from {{ source('reference_analyst_managed', 'CANCER__QOF_ACHIEVEMENT_1920') }}
