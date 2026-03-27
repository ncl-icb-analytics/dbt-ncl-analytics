{{
    config(
        description="Raw layer (NHS Digital SNOMED CT reporting model with concept status and history). 1:1 passthrough with cleaned column names. \nSource: Dictionary.NHSD_SnomedReportingModel.SCT_History \ndbt: source(''nhsd_snomed'', ''SCT_History'') \nColumns:\n  OldConceptId -> old_concept_id\n  OldConceptStatus -> old_concept_status\n  NewConceptId -> new_concept_id\n  NewConceptStatus -> new_concept_status\n  Path -> path\n  IsAmbiguous -> is_ambiguous\n  Iterations -> iterations\n  OldConceptFullySpecifiedName -> old_concept_fully_specified_name\n  OldConceptFullySpecifiedNameTagCount -> old_concept_fully_specified_name_tag_count\n  NewConceptFullySpecifiedName -> new_concept_fully_specified_name\n  NewConceptFullySpecifiedNameStatus -> new_concept_fully_specified_name_status\n  TopLevelHierarchyIdenticalFlag -> top_level_hierarchy_identical_flag\n  FullySpecifiedNameTaglessIdenticalFlag -> fully_specified_name_tagless_identical_flag\n  FullySpecifiedNameTagIdenticalFlag -> fully_specified_name_tag_identical_flag"
    )
}}
select
    "OldConceptId" as old_concept_id,
    "OldConceptStatus" as old_concept_status,
    "NewConceptId" as new_concept_id,
    "NewConceptStatus" as new_concept_status,
    "Path" as path,
    "IsAmbiguous" as is_ambiguous,
    "Iterations" as iterations,
    "OldConceptFullySpecifiedName" as old_concept_fully_specified_name,
    "OldConceptFullySpecifiedNameTagCount" as old_concept_fully_specified_name_tag_count,
    "NewConceptFullySpecifiedName" as new_concept_fully_specified_name,
    "NewConceptFullySpecifiedNameStatus" as new_concept_fully_specified_name_status,
    "TopLevelHierarchyIdenticalFlag" as top_level_hierarchy_identical_flag,
    "FullySpecifiedNameTaglessIdenticalFlag" as fully_specified_name_tagless_identical_flag,
    "FullySpecifiedNameTagIdenticalFlag" as fully_specified_name_tag_identical_flag
from {{ source('nhsd_snomed', 'SCT_History') }}
