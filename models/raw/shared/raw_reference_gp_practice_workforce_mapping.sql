{{
    config(
        description="Raw layer (Analyst-managed reference datasets and business rules). 1:1 passthrough with cleaned column names. \nSource: DATA_LAKE__NCL.ANALYST_MANAGED.GP_PRACTICE_WORKFORCE_MAPPING \ndbt: source(''reference_analyst_managed'', ''GP_PRACTICE_WORKFORCE_MAPPING'') \nColumns:\n  STAFF_GROUP -> staff_group\n  DETAILED_STAFF_ROLE -> detailed_staff_role\n  MEASURE -> measure\n  DESCRIPTION -> description\n  OUTPUT_NAME -> output_name\n  VALUE_TYPE -> value_type\n  COMMENTS -> comments"
    )
}}
select
    "STAFF_GROUP" as staff_group,
    "DETAILED_STAFF_ROLE" as detailed_staff_role,
    "MEASURE" as measure,
    "DESCRIPTION" as description,
    "OUTPUT_NAME" as output_name,
    "VALUE_TYPE" as value_type,
    "COMMENTS" as comments
from {{ source('reference_analyst_managed', 'GP_PRACTICE_WORKFORCE_MAPPING') }}
