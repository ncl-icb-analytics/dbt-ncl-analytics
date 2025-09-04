-- Staging model for dictionary_dbo.OperationStatus
-- Source: "Dictionary"."dbo"
-- Description: Reference data including PDS and lookup tables

select
    "SK_OperationStatusID" as sk_operation_status_id,
    "BK_OperationStatus" as bk_operation_status,
    "OperationStatus" as operation_status
from {{ source('dictionary_dbo', 'OperationStatus') }}
