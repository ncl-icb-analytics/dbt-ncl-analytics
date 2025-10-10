-- Raw layer model for sus_ae.clinical.diagnoses.snomed
-- Source: "DATA_LAKE"."SUS_UNIFIED_ECDS"
-- Description: SUS emergency care attendances and activity
-- This is a 1:1 passthrough from source with standardized column names
select
    "is_applicable_to_females" as is_applicable_to_females,
    "is_applicable_to_males" as is_applicable_to_males,
    "is_qualifier_approved" as is_qualifier_approved,
    "is_notifiable_disease" as is_notifiable_disease,
    "dmicImportLogId" as dmic_import_log_id,
    "ROWNUMBER_ID" as rownumber_id,
    "PRIMARYKEY_ID" as primarykey_id,
    "SNOMED_ID" as snomed_id,
    "code" as code,
    "is_code_approved" as is_code_approved,
    "is_injury_related" as is_injury_related,
    "equivalent_ae_code" as equivalent_ae_code,
    "sequence_number" as sequence_number,
    "is_aec_related" as is_aec_related,
    "is_allergy_related" as is_allergy_related,
    "is_primary" as is_primary,
    "qualifier" as qualifier
from {{ source('sus_ae', 'clinical.diagnoses.snomed') }}
