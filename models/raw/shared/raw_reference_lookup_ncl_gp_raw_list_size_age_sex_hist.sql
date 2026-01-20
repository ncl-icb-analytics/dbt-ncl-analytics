{{
    config(
        description="Raw layer (Analyst-managed reference datasets and business rules in the MODELLING environment). 1:1 passthrough with cleaned column names. \nSource: MODELLING.LOOKUP_NCL.GP_RAW_LIST_SIZE_AGE_SEX_HIST \ndbt: source(''reference_lookup_ncl'', ''GP_RAW_LIST_SIZE_AGE_SEX_HIST'') \nColumns:\n  EXTRACT_DATE -> extract_date\n  SUB_ICB_LOCATION_CODE -> sub_icb_location_code\n  ONS_SUB_ICB_LOCATION_CODE -> ons_sub_icb_location_code\n  ORG_CODE -> org_code\n  POSTCODE -> postcode\n  SEX -> sex\n  AGE -> age\n  NUMBER_OF_PATIENTS -> number_of_patients\n  CREATE_TS -> create_ts"
    )
}}
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
from {{ source('reference_lookup_ncl', 'GP_RAW_LIST_SIZE_AGE_SEX_HIST') }}
