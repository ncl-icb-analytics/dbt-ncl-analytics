-- Staging model for dictionary_dbo.GP
-- Source: "Dictionary"."dbo"
-- Description: Reference data including PDS and lookup tables

select
    "SK_GPID" as sk_gpid,
    "GP_Code" as gp_code,
    "GP_Name" as gp_name,
    "Contact_Telephone_Number" as contact_telephone_number,
    "SK_OrganisationID_NationalGrouping" as sk_organisation_id_national_grouping,
    "SK_OrganisationID_HealthAuthority" as sk_organisation_id_health_authority,
    "SK_OrganisationID_CurrentCareOrg" as sk_organisation_id_current_care_org,
    "SK_PostcodeID" as sk_postcode_id,
    "StartDate" as start_date,
    "EndDate" as end_date,
    "SK_OrganisationID_ParentOrg" as sk_organisation_id_parent_org,
    "Join_Parent_Date" as join_parent_date,
    "Left_Parent_Date" as left_parent_date,
    "FirstCreated" as first_created,
    "LastUpdated" as last_updated,
    "SK_GP_ID" as sk_gp_id,
    "SK_NationalGrouping_ID" as sk_national_grouping_id,
    "SK_HealthAuthority_ID" as sk_health_authority_id,
    "SK_CurrentCareOrg_ID" as sk_current_care_org_id,
    "SK_Postcode_ID" as sk_postcode_id,
    "SK_ParentOrg_ID" as sk_parent_org_id,
    "GMC_GivenName" as gmc_given_name,
    "GMC_Surname" as gmc_surname,
    "GMC_ReferenceNumber" as gmc_reference_number
from {{ source('dictionary_dbo', 'GP') }}
