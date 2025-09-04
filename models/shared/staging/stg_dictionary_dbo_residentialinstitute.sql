-- Staging model for dictionary_dbo.ResidentialInstitute
-- Source: "Dictionary"."dbo"
-- Description: Reference data including PDS and lookup tables

select
    "SK_ResidentialInstituteID" as sk_residential_institute_id,
    "Cipher" as cipher,
    "ResidentialInstituteCode" as residential_institute_code,
    "ResidentialInstituteName" as residential_institute_name,
    "AttractsGlobalSumUplift" as attracts_global_sum_uplift,
    "SK_OrganisationID" as sk_organisation_id,
    "DateCreated" as date_created,
    "DateUpdated" as date_updated
from {{ source('dictionary_dbo', 'ResidentialInstitute') }}
