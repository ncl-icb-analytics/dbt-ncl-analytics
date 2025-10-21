-- Raw layer model for reference_analyst_managed.GP_REG_PAT_PRAC_LSOA_ALL
-- Source: "DATA_LAKE__NCL"."ANALYST_MANAGED"
-- Description: Analyst-managed reference datasets and business rules
-- This is a 1:1 passthrough from source with standardized column names
select
    "PUBLICATION" as publication,
    "EXTRACT_DATE" as extract_date,
    "PRACTICE_CODE" as practice_code,
    "PRACTICE_NAME" as practice_name,
    "LSOA_CODE" as lsoa_code,
    "GENDER" as gender,
    "NUMBER_OF_PATIENTS" as number_of_patients
from {{ source('reference_analyst_managed', 'GP_REG_PAT_PRAC_LSOA_ALL') }}
