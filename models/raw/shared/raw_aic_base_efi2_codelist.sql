{{
    config(
        description="Raw layer. 1:1 passthrough with cleaned column names. \nSource: DATA_LAKE__NCL.COMMON.EFI2_CODELISTS \ndbt: source(''common'', ''EFI2_CODELISTS'') \nColumns:\n  DEFICIT -> deficit\n  SNOMEDCT_CONCEPTID -> snomedct_conceptid\n  CTV3 -> ctv3\n  PROVENANCE -> provenance\n  CODE_DESCRIPTION -> code_description\n  TIME_CONSTRAINT_YEARS -> time_constraint_years\n  AGE_LIMIT -> age_limit\n  OTHER_INSTRUCTIONS -> other_instructions\n  NEW_DESCRIPTIONS -> new_descriptions"
    )
}}
-- eFI2 codelist, AIC-curated but published to the shared COMMON schema (not
-- AIC_DEV). Named *_aic_* to keep all eFI2 assets grouped with the rest of the
-- AIC frailty pipeline; the source is `common` — see models/sources/sources.yml.
select
    "DEFICIT" as deficit,
    "SNOMEDCT_CONCEPTID" as snomedct_conceptid,
    "CTV3" as ctv3,
    "PROVENANCE" as provenance,
    "CODE_DESCRIPTION" as code_description,
    "TIME_CONSTRAINT_YEARS" as time_constraint_years,
    "AGE_LIMIT" as age_limit,
    "OTHER_INSTRUCTIONS" as other_instructions,
    "NEW_DESCRIPTIONS" as new_descriptions
from {{ source('common', 'EFI2_CODELISTS') }}
