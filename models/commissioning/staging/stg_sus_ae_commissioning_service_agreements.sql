-- Staging model for sus_ae.commissioning.service_agreements
-- Source: "DATA_LAKE"."SUS_UNIFIED_ECDS"
-- Description: SUS emergency care attendances and activity

select
    "ROWNUMBER_ID" as rownumber_id,
    "PRIMARYKEY_ID" as primarykey_id,
    "SERVICE_AGREEMENTS_ID" as service_agreements_id,
    "commissioner" as commissioner,
    "is_commissioner_recognised" as is_commissioner_recognised,
    "sha_commissioner" as sha_commissioner,
    "commissioner_assignment_period_start_date" as commissioner_assignment_period_start_date,
    "commissioner_assignment_period_end_date" as commissioner_assignment_period_end_date,
    "commissioning_serial_number" as commissioning_serial_number,
    "line_number" as line_number,
    "commissioner_reference_number" as commissioner_reference_number,
    "provider_reference_number" as provider_reference_number,
    "service_code" as service_code,
    "dmicImportLogId" as dmic_import_log_id
from {{ source('sus_ae', 'commissioning.service_agreements') }}
