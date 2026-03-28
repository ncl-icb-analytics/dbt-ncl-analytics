{{
    config(
        description="Raw layer (NHS Digital SNOMED CT reporting model with concept status and history). 1:1 passthrough with cleaned column names. \nSource: Dictionary.NHSD_SnomedReportingModel.SCT_Description \ndbt: source(''nhsd_snomed'', ''SCT_Description'') \nColumns:\n  Id -> id\n  EffectiveTime -> effective_time\n  Active -> active\n  ModuleId -> module_id\n  ConceptId -> concept_id\n  LanguageCode -> language_code\n  TypeId -> type_id\n  Term -> term\n  CaseSignificanceId -> case_significance_id\n  DescriptionType -> description_type"
    )
}}
select
    "Id" as id,
    "EffectiveTime" as effective_time,
    "Active" as active,
    "ModuleId" as module_id,
    "ConceptId" as concept_id,
    "LanguageCode" as language_code,
    "TypeId" as type_id,
    "Term" as term,
    "CaseSignificanceId" as case_significance_id,
    "DescriptionType" as description_type
from {{ source('nhsd_snomed', 'SCT_Description') }}
