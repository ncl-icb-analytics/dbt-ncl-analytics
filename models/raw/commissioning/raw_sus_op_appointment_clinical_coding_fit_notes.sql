{{
    config(
        description="Raw layer (SUS outpatient appointments and activity). 1:1 passthrough with cleaned column names. \nSource: DATA_LAKE.SUS_UNIFIED_OP.appointment.clinical_coding.fit_notes \ndbt: source(''sus_op'', ''appointment.clinical_coding.fit_notes'') \nColumns:\n  ROWNUMBER_ID -> rownumber_id\n  PRIMARYKEY_ID -> primarykey_id\n  FIT_NOTES_ID -> fit_notes_id\n  condition -> condition\n  assessment_date -> assessment_date\n  diagnosis -> diagnosis\n  start_date -> start_date\n  end_date -> end_date\n  duration -> duration\n  recorded_date -> recorded_date\n  follow_up_assessment_required_indicator -> follow_up_assessment_required_indicator\n  issuer -> issuer\n  dmicImportLogId -> dmic_import_log_id"
    )
}}
select
    "ROWNUMBER_ID" as rownumber_id,
    "PRIMARYKEY_ID" as primarykey_id,
    "FIT_NOTES_ID" as fit_notes_id,
    "condition" as condition,
    "assessment_date" as assessment_date,
    "diagnosis" as diagnosis,
    "start_date" as start_date,
    "end_date" as end_date,
    "duration" as duration,
    "recorded_date" as recorded_date,
    "follow_up_assessment_required_indicator" as follow_up_assessment_required_indicator,
    "issuer" as issuer,
    "dmicImportLogId" as dmic_import_log_id
from {{ source('sus_op', 'appointment.clinical_coding.fit_notes') }}
