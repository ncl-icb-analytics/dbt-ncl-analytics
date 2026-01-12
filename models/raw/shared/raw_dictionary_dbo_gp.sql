{{
    config(
        description="Raw layer (Reference data including PDS and lookup tables). 1:1 passthrough with cleaned column names. \nSource: Dictionary.dbo.GP \ndbt: source(''dictionary_dbo'', ''GP'') \nColumns:\n  SK_GPID -> sk_gpid\n  GP_Code -> gp_code\n  GP_Name -> gp_name\n  Contact_Telephone_Number -> contact_telephone_number\n  SK_OrganisationID_NationalGrouping -> sk_organisation_id_national_grouping\n  SK_OrganisationID_HealthAuthority -> sk_organisation_id_health_authority\n  SK_OrganisationID_CurrentCareOrg -> sk_organisation_id_current_care_org\n  SK_PostcodeID -> sk_postcode_id\n  StartDate -> start_date\n  EndDate -> end_date\n  SK_OrganisationID_ParentOrg -> sk_organisation_id_parent_org\n  Join_Parent_Date -> join_parent_date\n  Left_Parent_Date -> left_parent_date\n  FirstCreated -> first_created\n  LastUpdated -> last_updated\n  SK_GP_ID -> sk_gp_id\n  SK_NationalGrouping_ID -> sk_national_grouping_id\n  SK_HealthAuthority_ID -> sk_health_authority_id\n  SK_CurrentCareOrg_ID -> sk_current_care_org_id\n  SK_Postcode_ID -> sk_postcode_id_1\n  SK_ParentOrg_ID -> sk_parent_org_id\n  GMC_GivenName -> gmc_given_name\n  GMC_Surname -> gmc_surname\n  GMC_ReferenceNumber -> gmc_reference_number"
    )
}}
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
    "SK_Postcode_ID" as sk_postcode_id_1,
    "SK_ParentOrg_ID" as sk_parent_org_id,
    "GMC_GivenName" as gmc_given_name,
    "GMC_Surname" as gmc_surname,
    "GMC_ReferenceNumber" as gmc_reference_number
from {{ source('dictionary_dbo', 'GP') }}
