-- Staging model for sus_apc.spell.commissioning.service_agreements
-- Source: "DATA_LAKE"."SUS_UNIFIED_APC"
-- Description: SUS admitted patient care episodes and procedures

select
    "commissioner_assignment_period_end_date" as commissioner_assignment_period_end_date,
    "commissioning_serial_number" as commissioning_serial_number,
    "line_number" as line_number,
    "commissioner_reference_number" as commissioner_reference_number,
    "provider_reference_number" as provider_reference_number,
    "service_code" as service_code,
    "dmicImportLogId" as dmic_import_log_id,
    "ROWNUMBER_ID" as rownumber_id,
    "PRIMARYKEY_ID" as primarykey_id,
    "SERVICE_AGREEMENTS_ID" as service_agreements_id,
    "commissioner" as commissioner,
    "is_commissioner_recognised" as is_commissioner_recognised,
    "commissioner_derived" as commissioner_derived,
    "commissioner_assignment_period_start_date" as commissioner_assignment_period_start_date
from {{ source('sus_apc', 'spell.commissioning.service_agreements') }}
