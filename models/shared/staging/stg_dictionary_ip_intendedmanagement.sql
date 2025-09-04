-- Staging model for dictionary_ip.IntendedManagement
-- Source: "Dictionary"."IP"
-- Description: Reference data for inpatient procedures and treatments

select
    "SK_IntendedManagementID" as sk_intended_management_id,
    "BK_IntendedManagementCode" as bk_intended_management_code,
    "IntendedManagementDescription" as intended_management_description
from {{ source('dictionary_ip', 'IntendedManagement') }}
