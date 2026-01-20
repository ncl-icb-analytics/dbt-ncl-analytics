{{
    config(
        description="Raw layer (SUS outpatient appointments and activity). 1:1 passthrough with cleaned column names. \nSource: DATA_LAKE.SUS_UNIFIED_OP.appointment.commissioning.grouping.unbundled_hrg \ndbt: source(''sus_op'', ''appointment.commissioning.grouping.unbundled_hrg'') \nColumns:\n  ROWNUMBER_ID -> rownumber_id\n  PRIMARYKEY_ID -> primarykey_id\n  UNBUNDLED_HRG_ID -> unbundled_hrg_id\n  code -> code\n  multiple_applies -> multiple_applies\n  tariff -> tariff\n  dmicImportLogId -> dmic_import_log_id\n  tariff_applied -> tariff_applied"
    )
}}
select
    "ROWNUMBER_ID" as rownumber_id,
    "PRIMARYKEY_ID" as primarykey_id,
    "UNBUNDLED_HRG_ID" as unbundled_hrg_id,
    "code" as code,
    "multiple_applies" as multiple_applies,
    "tariff" as tariff,
    "dmicImportLogId" as dmic_import_log_id,
    "tariff_applied" as tariff_applied
from {{ source('sus_op', 'appointment.commissioning.grouping.unbundled_hrg') }}
