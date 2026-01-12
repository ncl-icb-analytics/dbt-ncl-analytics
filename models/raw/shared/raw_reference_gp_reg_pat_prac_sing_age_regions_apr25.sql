{{
    config(
        description="Raw layer (Analyst-managed reference datasets and business rules). 1:1 passthrough with cleaned column names. \nSource: DATA_LAKE__NCL.ANALYST_MANAGED.GP_REG_PAT_PRAC_SING_AGE_REGIONS_APR25 \ndbt: source(''reference_analyst_managed'', ''GP_REG_PAT_PRAC_SING_AGE_REGIONS_APR25'') \nColumns:\n  PUBLICATION -> publication\n  EXTRACT_DATE -> extract_date\n  ORG_TYPE -> org_type\n  ORG_CODE -> org_code\n  ONS_CODE -> ons_code\n  SEX -> sex\n  AGE -> age\n  NUMBER_OF_PATIENTS -> number_of_patients"
    )
}}
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
