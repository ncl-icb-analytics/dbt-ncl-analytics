-- Staging model for sus_op.appointment.commissioning.grouping.unbundled_hrg
-- Source: "DATA_LAKE"."SUS_UNIFIED_OP"
{% if source.get('description') %}
-- Description: SUS outpatient appointments and activity
{% endif %}

select
    "multiple_applies" as multiple_applies,
    "ROWNUMBER_ID" as rownumber_id,
    "PRIMARYKEY_ID" as primarykey_id,
    "tariff" as tariff,
    "dmicImportLogId" as dmicimportlogid,
    "UNBUNDLED_HRG_ID" as unbundled_hrg_id,
    "code" as code
from {{ source('sus_op', 'appointment.commissioning.grouping.unbundled_hrg') }}
