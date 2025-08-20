-- Staging model for dictionary_snomed.Ref_Set_Complex_Map
-- Source: "Dictionary"."Snomed"
-- Description: Reference data for snomed

select
    "ID" as id,
    "Active" as active,
    "Module_ID" as module_id,
    "Ref_Set_ID" as ref_set_id,
    "Referenced_Component_ID" as referenced_component_id,
    "Map_Group" as map_group,
    "Map_Priority" as map_priority,
    "Map_Rule" as map_rule,
    "Map_Advice" as map_advice,
    "Map_Target" as map_target,
    "Correlation_ID" as correlation_id,
    "Map_Block" as map_block
from {{ source('dictionary_snomed', 'Ref_Set_Complex_Map') }}
