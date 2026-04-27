{{
    config(
        description="Raw layer (NHS Digital SNOMED CT reporting model with concept status and history). 1:1 passthrough with cleaned column names. \nSource: Dictionary.NHSD_SnomedReportingModel.SCT_RefSet_Simple \ndbt: source(''nhsd_snomed'', ''SCT_RefSet_Simple'') \nColumns:\n  Id -> id\n  EffectiveTime -> effective_time\n  Active -> active\n  ModuleId -> module_id\n  RefSetId -> ref_set_id\n  ReferencedComponentId -> referenced_component_id"
    )
}}
select
    "Id" as id,
    "EffectiveTime" as effective_time,
    "Active" as active,
    "ModuleId" as module_id,
    "RefSetId" as ref_set_id,
    "ReferencedComponentId" as referenced_component_id
from {{ source('nhsd_snomed', 'SCT_RefSet_Simple') }}
