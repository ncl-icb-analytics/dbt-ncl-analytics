-- Staging model for dictionary_ip.IntendedManagement
-- Source: "Dictionary"."IP"
-- Description: Reference data for inpatient procedures and treatments

select
    "SK_IntendedManagementID" as sk_intendedmanagementid,
    "BK_IntendedManagementCode" as bk_intendedmanagementcode,
    "IntendedManagementDescription" as intendedmanagementdescription
from {{ source('dictionary_ip', 'IntendedManagement') }}
