-- Staging model for dictionary.ResidentialInstitute
-- Source: "Dictionary"."dbo"
{% if source.get('description') %}
-- Description: Reference data including PDS and lookup tables
{% endif %}

select
    "SK_ResidentialInstituteID" as sk_residentialinstituteid,
    "Cipher" as cipher,
    "ResidentialInstituteCode" as residentialinstitutecode,
    "ResidentialInstituteName" as residentialinstitutename,
    "AttractsGlobalSumUplift" as attractsglobalsumuplift,
    "SK_OrganisationID" as sk_organisationid,
    "DateCreated" as datecreated,
    "DateUpdated" as dateupdated
from {{ source('dictionary', 'ResidentialInstitute') }}
