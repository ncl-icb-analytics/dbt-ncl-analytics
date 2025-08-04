-- Staging model for dictionary.OperationStatus
-- Source: "Dictionary"."dbo"
{% if source.get('description') %}
-- Description: Reference data including PDS and lookup tables
{% endif %}

select
    "SK_OperationStatusID" as sk_operationstatusid,
    "BK_OperationStatus" as bk_operationstatus,
    "OperationStatus" as operationstatus
from {{ source('dictionary', 'OperationStatus') }}
