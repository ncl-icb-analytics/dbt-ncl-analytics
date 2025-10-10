-- Raw layer model for sus_op.system.transaction.cds_copy_recipients
-- Source: "DATA_LAKE"."SUS_UNIFIED_OP"
-- Description: SUS outpatient appointments and activity
-- This is a 1:1 passthrough from source with standardized column names
select
    "CDS_COPY_RECIPIENTS_ID" as cds_copy_recipients_id,
    "PRIMARYKEY_ID" as primarykey_id,
    "ROWNUMBER_ID" as rownumber_id,
    "cds_copy_recipients" as cds_copy_recipients,
    "dmicImportLogId" as dmic_import_log_id
from {{ source('sus_op', 'system.transaction.cds_copy_recipients') }}
