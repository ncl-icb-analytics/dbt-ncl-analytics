-- Staging model for sus_op.appointment.commissioning.service_agreements
-- Source: "DATA_LAKE"."SUS_UNIFIED_OP"
{% if source.get('description') %}
-- Description: SUS outpatient appointments and activity
{% endif %}

select
    "service_code" as service_code,
    "dmicImportLogId" as dmicimportlogid,
    "commissioner_assignment_period_end_date" as commissioner_assignment_period_end_date,
    "commissioning_serial_number" as commissioning_serial_number,
    "line_number" as line_number,
    "commissioner_reference_number" as commissioner_reference_number,
    "provider_reference_number" as provider_reference_number,
    "is_commissioner_recognised" as is_commissioner_recognised,
    "commissioner_derived" as commissioner_derived,
    "commissioner_assignment_period_start_date" as commissioner_assignment_period_start_date,
    "SERVICE_AGREEMENTS_ID" as service_agreements_id,
    "commissioner" as commissioner,
    "ROWNUMBER_ID" as rownumber_id,
    "PRIMARYKEY_ID" as primarykey_id
from {{ source('sus_op', 'appointment.commissioning.service_agreements') }}
