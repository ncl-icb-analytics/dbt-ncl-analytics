-- Raw layer model for reference_analyst_managed.EFI2_SNOMED
-- Source: "DATA_LAKE__NCL"."ANALYST_MANAGED"
-- Description: Analyst-managed reference datasets and business rules
-- This is a 1:1 passthrough from source with standardized column names
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
