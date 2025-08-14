-- Staging model for sus_ae.clinical.diagnoses.snomed.max_core_diagnoses
-- Source: "DATA_LAKE"."SUS_UNIFIED_ECDS"
-- Description: SUS emergency care attendances and activity

select
    "ROWNUMBER_ID" as rownumber_id,
    "PRIMARYKEY_ID" as primarykey_id,
    "SNOMED_ID" as snomed_id,
    "MAX_CORE_DIAGNOSES_ID" as max_core_diagnoses_id,
    "code" as code,
    "equivalent_ae_code" as equivalent_ae_code,
    "is_injury_related" as is_injury_related,
    "is_aec_related" as is_aec_related,
    "is_allergy_related" as is_allergy_related,
    "is_applicable_to_females" as is_applicable_to_females,
    "is_applicable_to_males" as is_applicable_to_males,
    "is_notifiable_disease" as is_notifiable_disease,
    "dmicImportLogId" as dmicimportlogid
from {{ source('sus_ae', 'clinical.diagnoses.snomed.max_core_diagnoses') }}
