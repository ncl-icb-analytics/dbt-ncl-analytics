{{
    config(
        description="Raw layer (SUS outpatient appointments and activity). 1:1 passthrough with cleaned column names. \nSource: DATA_LAKE.SUS_UNIFIED_OP.appointment.commissioning.tariff_calculation.exclusion_reasons \ndbt: source(''sus_op'', ''appointment.commissioning.tariff_calculation.exclusion_reasons'') \nColumns:\n  ROWNUMBER_ID -> rownumber_id\n  PRIMARYKEY_ID -> primarykey_id\n  EXCLUSION_REASONS_ID -> exclusion_reasons_id\n  exclusion_reasons -> exclusion_reasons\n  dmicImportLogId -> dmic_import_log_id"
    )
}}
select
    "ROWNUMBER_ID" as rownumber_id,
    "PRIMARYKEY_ID" as primarykey_id,
    "EXCLUSION_REASONS_ID" as exclusion_reasons_id,
    "exclusion_reasons" as exclusion_reasons,
    "dmicImportLogId" as dmic_import_log_id
from {{ source('sus_op', 'appointment.commissioning.tariff_calculation.exclusion_reasons') }}
