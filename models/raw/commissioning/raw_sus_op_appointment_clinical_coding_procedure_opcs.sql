{{
    config(
        description="Raw layer (SUS outpatient appointments and activity). 1:1 passthrough with cleaned column names. \nSource: DATA_LAKE.SUS_UNIFIED_OP.appointment.clinical_coding.procedure.opcs \ndbt: source(''sus_op'', ''appointment.clinical_coding.procedure.opcs'') \nColumns:\n  ROWNUMBER_ID -> rownumber_id\n  PRIMARYKEY_ID -> primarykey_id\n  OPCS_ID -> opcs_id\n  code -> code\n  date -> date\n  main_operating_professional.identifier -> main_operating_professional_identifier\n  main_operating_professional.registration_issuer -> main_operating_professional_registration_issuer\n  responsible_anaesthetist.identifier -> responsible_anaesthetist_identifier\n  responsible_anaesthetist.registration_issuer -> responsible_anaesthetist_registration_issuer\n  dmicImportLogId -> dmic_import_log_id"
    )
}}
select
    "ROWNUMBER_ID" as rownumber_id,
    "PRIMARYKEY_ID" as primarykey_id,
    "OPCS_ID" as opcs_id,
    "code" as code,
    "date" as date,
    "main_operating_professional.identifier" as main_operating_professional_identifier,
    "main_operating_professional.registration_issuer" as main_operating_professional_registration_issuer,
    "responsible_anaesthetist.identifier" as responsible_anaesthetist_identifier,
    "responsible_anaesthetist.registration_issuer" as responsible_anaesthetist_registration_issuer,
    "dmicImportLogId" as dmic_import_log_id
from {{ source('sus_op', 'appointment.clinical_coding.procedure.opcs') }}
