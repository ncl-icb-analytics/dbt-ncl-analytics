-- Staging model for sus_op.system.transaction.cds_copy_recipients
-- Source: "DATA_LAKE"."SUS_UNIFIED_OP"
-- Description: SUS outpatient appointments and activity

select
    "CDS_COPY_RECIPIENTS_ID" as cds_copy_recipients_id,
    "PRIMARYKEY_ID" as primarykey_id,
    "ROWNUMBER_ID" as rownumber_id,
    "cds_copy_recipients" as cds_copy_recipients,
    "dmicImportLogId" as dmicimportlogid
from {{ source('sus_op', 'system.transaction.cds_copy_recipients') }}
