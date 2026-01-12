{{
    config(
        description="Raw layer (Reference data for outpatient procedures and treatments). 1:1 passthrough with cleaned column names. \nSource: Dictionary.OP.ServiceTypeRequested \ndbt: source(''dictionary_op'', ''ServiceTypeRequested'') \nColumns:\n  SK_ServiceTypeRequestedID -> sk_service_type_requested_id\n  BK_ServiceTypeRequestedCode -> bk_service_type_requested_code\n  ServiceTypeRequestedDescription -> service_type_requested_description"
    )
}}
select
    "SK_ServiceTypeRequestedID" as sk_service_type_requested_id,
    "BK_ServiceTypeRequestedCode" as bk_service_type_requested_code,
    "ServiceTypeRequestedDescription" as service_type_requested_description
from {{ source('dictionary_op', 'ServiceTypeRequested') }}
