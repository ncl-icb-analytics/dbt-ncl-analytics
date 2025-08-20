-- Staging model for dictionary_op.ServiceTypeRequested
-- Source: "Dictionary"."OP"
-- Description: Reference data for outpatient procedures and treatments

select
    "SK_ServiceTypeRequestedID" as sk_servicetyperequestedid,
    "BK_ServiceTypeRequestedCode" as bk_servicetyperequestedcode,
    "ServiceTypeRequestedDescription" as servicetyperequesteddescription
from {{ source('dictionary_op', 'ServiceTypeRequested') }}
