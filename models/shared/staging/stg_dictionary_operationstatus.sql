-- Staging model for dictionary.OperationStatus
-- Source: "Dictionary"."dbo"
-- Description: Reference data including PDS and lookup tables

select
    "SK_OperationStatusID" as sk_operationstatusid,
    "BK_OperationStatus" as bk_operationstatus,
    "OperationStatus" as operationstatus
from {{ source('dictionary', 'OperationStatus') }}
