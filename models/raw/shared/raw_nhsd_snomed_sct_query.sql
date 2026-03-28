{{
    config(
        description="Raw layer (NHS Digital SNOMED CT reporting model with concept status and history). 1:1 passthrough with cleaned column names. \nSource: Dictionary.NHSD_SnomedReportingModel.SCT_Query \ndbt: source(''nhsd_snomed'', ''SCT_Query'') \nColumns:\n  SuperTypeId -> super_type_id\n  SubTypeId -> sub_type_id\n  Provenance -> provenance"
    )
}}
select
    "SuperTypeId" as super_type_id,
    "SubTypeId" as sub_type_id,
    "Provenance" as provenance
from {{ source('nhsd_snomed', 'SCT_Query') }}
