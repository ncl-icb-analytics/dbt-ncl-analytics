-- Staging model for sus_ae.clinical.coded_observations
-- Source: "DATA_LAKE"."SUS_UNIFIED_ECDS"
-- Description: SUS emergency care attendances and activity

select
    "is_code_approved" as is_code_approved,
    "value" as value,
    "code" as code,
    "CODED_OBSERVATIONS_ID" as coded_observations_id,
    "timestamp" as timestamp,
    "dmicImportLogId" as dmic_import_log_id,
    "ucum_unit_of_measurement" as ucum_unit_of_measurement,
    "ROWNUMBER_ID" as rownumber_id,
    "PRIMARYKEY_ID" as primarykey_id
from {{ source('sus_ae', 'clinical.coded_observations') }}
