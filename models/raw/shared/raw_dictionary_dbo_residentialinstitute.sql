{{
    config(
        description="Raw layer (Reference data including PDS and lookup tables). 1:1 passthrough with cleaned column names. \nSource: Dictionary.dbo.ResidentialInstitute \ndbt: source(''dictionary_dbo'', ''ResidentialInstitute'') \nColumns:\n  SK_ResidentialInstituteID -> sk_residential_institute_id\n  Cipher -> cipher\n  ResidentialInstituteCode -> residential_institute_code\n  ResidentialInstituteName -> residential_institute_name\n  AttractsGlobalSumUplift -> attracts_global_sum_uplift\n  SK_OrganisationID -> sk_organisation_id\n  DateCreated -> date_created\n  DateUpdated -> date_updated"
    )
}}
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
