-- Staging model for sus_op.appointment.clinical_coding.observations
-- Source: "DATA_LAKE"."SUS_UNIFIED_OP"
{% if source.get('description') %}
-- Description: SUS outpatient appointments and activity
{% endif %}

select
    "ROWNUMBER_ID" as rownumber_id,
    "PRIMARYKEY_ID" as primarykey_id,
    "OBSERVATIONS_ID" as observations_id,
    "code" as code,
    "value" as value,
    "ucum_unit_of_measurement" as ucum_unit_of_measurement,
    "timestamp" as timestamp,
    "is_data_absent" as is_data_absent,
    "data_absent_reason" as data_absent_reason,
    "dmicImportLogId" as dmicimportlogid
from {{ source('sus_op', 'appointment.clinical_coding.observations') }}
