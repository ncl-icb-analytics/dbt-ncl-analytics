{{
    config(
        description="Raw layer (Analyst-managed reference datasets and business rules). 1:1 passthrough with cleaned column names. \nSource: DATA_LAKE__NCL.ANALYST_MANAGED.GP_REG_PAT_PRAC_LSOA_ALL \ndbt: source(''reference_analyst_managed'', ''GP_REG_PAT_PRAC_LSOA_ALL'') \nColumns:\n  PUBLICATION -> publication\n  EXTRACT_DATE -> extract_date\n  PRACTICE_CODE -> practice_code\n  PRACTICE_NAME -> practice_name\n  LSOA_CODE -> lsoa_code\n  SEX -> sex\n  NUMBER_OF_PATIENTS -> number_of_patients"
    )
}}
select
    "PUBLICATION" as publication,
    "EXTRACT_DATE" as extract_date,
    "PRACTICE_CODE" as practice_code,
    "PRACTICE_NAME" as practice_name,
    "LSOA_CODE" as lsoa_code,
    "SEX" as sex,
    "NUMBER_OF_PATIENTS" as number_of_patients
from {{ source('reference_analyst_managed', 'GP_REG_PAT_PRAC_LSOA_ALL') }}
