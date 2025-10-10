-- Raw layer model for reference_lookup_ncl.GP_RAW_LIST_SIZE_AGE_SEX_LATEST
-- Source: "MODELLING"."LOOKUP_NCL"
-- Description: Analyst-managed reference datasets and business rules in the MODELLING environment
-- This is a 1:1 passthrough from source with standardized column names
select
    "EXTRACT_DATE" as extract_date,
    "SUB_ICB_LOCATION_CODE" as sub_icb_location_code,
    "ONS_SUB_ICB_LOCATION_CODE" as ons_sub_icb_location_code,
    "ORG_CODE" as org_code,
    "POSTCODE" as postcode,
    "SEX" as sex,
    "AGE" as age,
    "NUMBER_OF_PATIENTS" as number_of_patients,
    "CREATE_TS" as create_ts
from {{ source('reference_lookup_ncl', 'GP_RAW_LIST_SIZE_AGE_SEX_LATEST') }}
