{{
    config(
        description="Raw layer (Analyst-managed reference datasets and business rules). 1:1 passthrough with cleaned column names. \nSource: DATA_LAKE__NCL.ANALYST_MANAGED.EFI2_SNOMED \ndbt: source(''reference_analyst_managed'', ''EFI2_SNOMED'') \nColumns:\n  DEFICIT -> deficit\n  SNOMEDCT_CONCEPTID -> snomedct_conceptid\n  CTV3 -> ctv3\n  PROVENANCE -> provenance\n  CODEDESCRIPTION -> codedescription\n  TIMECONSTRAINTYEARS -> timeconstraintyears\n  AGELIMIT -> agelimit\n  OTHERINSTRUCTIONS -> otherinstructions"
    )
}}
select
    "DEFICIT" as deficit,
    "SNOMEDCT_CONCEPTID" as snomedct_conceptid,
    "CTV3" as ctv3,
    "PROVENANCE" as provenance,
    "CODEDESCRIPTION" as codedescription,
    "TIMECONSTRAINTYEARS" as timeconstraintyears,
    "AGELIMIT" as agelimit,
    "OTHERINSTRUCTIONS" as otherinstructions
from {{ source('reference_analyst_managed', 'EFI2_SNOMED') }}
