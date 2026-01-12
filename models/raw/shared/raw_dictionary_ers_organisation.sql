{{
    config(
        description="Raw layer (Primary care referrals lookups). 1:1 passthrough with cleaned column names. \nSource: Dictionary.E-Referral.Organisation \ndbt: source(''dictionary_eRS'', ''Organisation'') \nColumns:\n  OrganisationID -> organisation_id\n  ParentOrganisationID -> parent_organisation_id\n  OrganisationRoleCode -> organisation_role_code\n  OrganisationName -> organisation_name\n  Postcode -> postcode\n  SK_OrganisationID -> sk_organisation_id\n  SK_OrganisationID_Parent -> sk_organisation_id_parent\n  SK_PostcodeID -> sk_postcode_id"
    )
}}
select
    "OrganisationID" as organisation_id,
    "ParentOrganisationID" as parent_organisation_id,
    "OrganisationRoleCode" as organisation_role_code,
    "OrganisationName" as organisation_name,
    "Postcode" as postcode,
    "SK_OrganisationID" as sk_organisation_id,
    "SK_OrganisationID_Parent" as sk_organisation_id_parent,
    "SK_PostcodeID" as sk_postcode_id
from {{ source('dictionary_eRS', 'Organisation') }}
