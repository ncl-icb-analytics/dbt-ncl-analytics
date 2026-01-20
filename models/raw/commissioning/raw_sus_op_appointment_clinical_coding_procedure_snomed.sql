{{
    config(
        description="Raw layer (SUS outpatient appointments and activity). 1:1 passthrough with cleaned column names. \nSource: DATA_LAKE.SUS_UNIFIED_OP.appointment.clinical_coding.procedure.snomed \ndbt: source(''sus_op'', ''appointment.clinical_coding.procedure.snomed'') \nColumns:\n  ROWNUMBER_ID -> rownumber_id\n  PRIMARYKEY_ID -> primarykey_id\n  SNOMED_ID -> snomed_id\n  code -> code\n  sequence_number -> sequence_number\n  timestamp -> timestamp\n  main_operating_professional.identifier -> main_operating_professional_identifier\n  main_operating_professional.registration_issuer -> main_operating_professional_registration_issuer\n  responsible_anaesthetist.identifier -> responsible_anaesthetist_identifier\n  responsible_anaesthetist.registration_issuer -> responsible_anaesthetist_registration_issuer\n  is_data_absent -> is_data_absent\n  data_absent_reason -> data_absent_reason\n  dmicImportLogId -> dmic_import_log_id"
    )
}}
select
    "ROWNUMBER_ID" as rownumber_id,
    "PRIMARYKEY_ID" as primarykey_id,
    "SNOMED_ID" as snomed_id,
    "code" as code,
    "sequence_number" as sequence_number,
    "timestamp" as timestamp,
    "main_operating_professional.identifier" as main_operating_professional_identifier,
    "main_operating_professional.registration_issuer" as main_operating_professional_registration_issuer,
    "responsible_anaesthetist.identifier" as responsible_anaesthetist_identifier,
    "responsible_anaesthetist.registration_issuer" as responsible_anaesthetist_registration_issuer,
    "is_data_absent" as is_data_absent,
    "data_absent_reason" as data_absent_reason,
    "dmicImportLogId" as dmic_import_log_id
from {{ source('sus_op', 'appointment.clinical_coding.procedure.snomed') }}
