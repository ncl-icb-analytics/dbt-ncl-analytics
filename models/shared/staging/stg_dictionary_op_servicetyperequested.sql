-- Staging model for dictionary_op.ServiceTypeRequested
-- Source: "Dictionary"."OP"
-- Description: Reference data for outpatient procedures and treatments

select
    "SK_ServiceTypeRequestedID" as sk_service_type_requested_id,
    "BK_ServiceTypeRequestedCode" as bk_service_type_requested_code,
    "ServiceTypeRequestedDescription" as service_type_requested_description
from {{ source('dictionary_op', 'ServiceTypeRequested') }}
