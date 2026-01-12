{{
    config(
        description="Raw layer (Primary care referrals lookups). 1:1 passthrough with cleaned column names. \nSource: Dictionary.E-Referral.ServiceClinicType \ndbt: source(''dictionary_eRS'', ''ServiceClinicType'') \nColumns:\n  Service_Id -> service_id\n  ClinicType -> clinic_type"
    )
}}
select
    "Service_Id" as service_id,
    "ClinicType" as clinic_type
from {{ source('dictionary_eRS', 'ServiceClinicType') }}
