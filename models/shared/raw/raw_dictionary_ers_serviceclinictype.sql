-- Raw layer model for dictionary_eRS.ServiceClinicType
-- Source: "Dictionary"."E-Referral"
-- Description: Primary care referrals lookups
-- This is a 1:1 passthrough from source with standardized column names
select
    "Service_Id" as service_id,
    "ClinicType" as clinic_type
from {{ source('dictionary_eRS', 'ServiceClinicType') }}
