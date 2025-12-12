-- Raw layer model for sus_op.appointment.commissioning.grouping.unbundled_hrg
-- Source: "DATA_LAKE"."SUS_UNIFIED_OP"
-- Description: SUS outpatient appointments and activity
-- This is a 1:1 passthrough from source with standardized column names
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
