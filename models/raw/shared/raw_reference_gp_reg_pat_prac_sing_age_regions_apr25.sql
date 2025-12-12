-- Raw layer model for reference_analyst_managed.GP_REG_PAT_PRAC_SING_AGE_REGIONS_APR25
-- Source: "DATA_LAKE__NCL"."ANALYST_MANAGED"
-- Description: Analyst-managed reference datasets and business rules
-- This is a 1:1 passthrough from source with standardized column names
select
    "PUBLICATION" as publication,
    "EXTRACT_DATE" as extract_date,
    "ORG_TYPE" as org_type,
    "ORG_CODE" as org_code,
    "ONS_CODE" as ons_code,
    "SEX" as sex,
    "AGE" as age,
    "NUMBER_OF_PATIENTS" as number_of_patients
from {{ source('reference_analyst_managed', 'GP_REG_PAT_PRAC_SING_AGE_REGIONS_APR25') }}
