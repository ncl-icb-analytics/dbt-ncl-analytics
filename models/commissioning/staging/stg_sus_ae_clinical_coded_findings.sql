-- Staging model for sus_ae.clinical.coded_findings
-- Source: "DATA_LAKE"."SUS_UNIFIED_ECDS"
-- Description: SUS emergency care attendances and activity

select
    "ROWNUMBER_ID" as rownumber_id,
    "PRIMARYKEY_ID" as primarykey_id,
    "CODED_FINDINGS_ID" as coded_findings_id,
    "code" as code,
    "is_code_approved" as is_code_approved,
    "timestamp" as timestamp,
    "dmicImportLogId" as dmicimportlogid
from {{ source('sus_ae', 'clinical.coded_findings') }}
