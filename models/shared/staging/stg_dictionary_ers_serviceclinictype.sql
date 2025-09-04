-- Staging model for dictionary_eRS.ServiceClinicType
-- Source: "Dictionary"."E-Referral"
-- Description: Primary care referrals lookups

select
    "Service_Id" as service_id,
    "ClinicType" as clinic_type
from {{ source('dictionary_eRS', 'ServiceClinicType') }}
