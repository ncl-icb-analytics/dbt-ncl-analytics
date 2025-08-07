-- Staging model for sus_op.appointment.clinical_coding.procedure.opcs
-- Source: "DATA_LAKE"."SUS_UNIFIED_OP"
-- Description: SUS outpatient appointments and activity

select
    "dmicImportLogId" as dmicimportlogid,
    "OPCS_ID" as opcs_id,
    "code" as code,
    "main_operating_professional.identifier" as main_operating_professional_identifier,
    "main_operating_professional.registration_issuer" as main_operating_professional_registration_issuer,
    "ROWNUMBER_ID" as rownumber_id,
    "PRIMARYKEY_ID" as primarykey_id,
    "responsible_anaesthetist.registration_issuer" as responsible_anaesthetist_registration_issuer,
    "responsible_anaesthetist.identifier" as responsible_anaesthetist_identifier,
    "date" as date
from {{ source('sus_op', 'appointment.clinical_coding.procedure.opcs') }}
