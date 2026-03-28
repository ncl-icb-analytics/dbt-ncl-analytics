{{
    config(
        description="Raw layer (NHS Digital SNOMED CT reporting model with concept status and history). 1:1 passthrough with cleaned column names. \nSource: Dictionary.NHSD_SnomedReportingModel.SCT_Concept \ndbt: source(''nhsd_snomed'', ''SCT_Concept'') \nColumns:\n  Id -> id\n  EffectiveTime -> effective_time\n  Active -> active\n  ModuleId -> module_id\n  DefinitionStatusId -> definition_status_id"
    )
}}
select
    "Id" as id,
    "EffectiveTime" as effective_time,
    "Active" as active,
    "ModuleId" as module_id,
    "DefinitionStatusId" as definition_status_id
from {{ source('nhsd_snomed', 'SCT_Concept') }}
