{{
    config(
        description="Raw layer (NHS Digital SNOMED CT reporting model with concept status and history). 1:1 passthrough with cleaned column names. \nSource: Dictionary.NHSD_SnomedReportingModel.SCT_Relationship \ndbt: source(''nhsd_snomed'', ''SCT_Relationship'') \nColumns:\n  Id -> id\n  EffectiveTime -> effective_time\n  Active -> active\n  ModuleId -> module_id\n  SourceId -> source_id\n  DestinationId -> destination_id\n  RelationshipGroup -> relationship_group\n  TypeId -> type_id\n  CharacteristicTypeId -> characteristic_type_id\n  ModifierId -> modifier_id"
    )
}}
select
    "Id" as id,
    "EffectiveTime" as effective_time,
    "Active" as active,
    "ModuleId" as module_id,
    "SourceId" as source_id,
    "DestinationId" as destination_id,
    "RelationshipGroup" as relationship_group,
    "TypeId" as type_id,
    "CharacteristicTypeId" as characteristic_type_id,
    "ModifierId" as modifier_id
from {{ source('nhsd_snomed', 'SCT_Relationship') }}
