{{
    config(
        description="Raw layer (Reference data including PDS and lookup tables). 1:1 passthrough with cleaned column names. \nSource: Dictionary.dbo.OperationStatus \ndbt: source(''dictionary_dbo'', ''OperationStatus'') \nColumns:\n  SK_OperationStatusID -> sk_operation_status_id\n  BK_OperationStatus -> bk_operation_status\n  OperationStatus -> operation_status"
    )
}}
select
    "SK_OperationStatusID" as sk_operation_status_id,
    "BK_OperationStatus" as bk_operation_status,
    "OperationStatus" as operation_status
from {{ source('dictionary_dbo', 'OperationStatus') }}
