{{
    config(
        description="Raw layer (Reference data for snomed). 1:1 passthrough with cleaned column names. \nSource: Dictionary.Snomed.Ref_Set_Complex_Map \ndbt: source(''dictionary_snomed'', ''Ref_Set_Complex_Map'') \nColumns:\n  ID -> id\n  Active -> active\n  Module_ID -> module_id\n  Ref_Set_ID -> ref_set_id\n  Referenced_Component_ID -> referenced_component_id\n  Map_Group -> map_group\n  Map_Priority -> map_priority\n  Map_Rule -> map_rule\n  Map_Advice -> map_advice\n  Map_Target -> map_target\n  Correlation_ID -> correlation_id\n  Map_Block -> map_block"
    )
}}
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
