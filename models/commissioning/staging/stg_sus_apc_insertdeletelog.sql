-- Staging model for sus_apc.InsertDeleteLog
-- Source: "DATA_LAKE"."SUS_UNIFIED_APC"
{% if source.get('description') %}
-- Description: SUS admitted patient care episodes and procedures
{% endif %}

select
    "PRIMARYKEY_ID" as primarykey_id,
    "dmicImportLogId" as dmicimportlogid,
    "dmicIsDeleted" as dmicisdeleted,
    "dmicDeletedImportLogId" as dmicdeletedimportlogid
from {{ source('sus_apc', 'InsertDeleteLog') }}
