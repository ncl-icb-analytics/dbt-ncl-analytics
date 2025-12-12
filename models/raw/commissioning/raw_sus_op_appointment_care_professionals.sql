-- Raw layer model for sus_op.appointment.care_professionals
-- Source: "DATA_LAKE"."SUS_UNIFIED_OP"
-- Description: SUS outpatient appointments and activity
-- This is a 1:1 passthrough from source with standardized column names
select
    "ROWNUMBER_ID" as rownumber_id,
    "PRIMARYKEY_ID" as primarykey_id,
    "CARE_PROFESSIONALS_ID" as care_professionals_id,
    "professional_registration_issuer" as professional_registration_issuer,
    "professional_registration_identifier" as professional_registration_identifier,
    "main_specialty" as main_specialty,
    "treatment_function" as treatment_function,
    "local_sub_specialty" as local_sub_specialty,
    "clinical_responsibility_indicator" as clinical_responsibility_indicator,
    "dmicImportLogId" as dmic_import_log_id
from {{ source('sus_op', 'appointment.care_professionals') }}
