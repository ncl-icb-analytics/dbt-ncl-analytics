{{
    config(
        description="Raw layer (Reference data including PDS and lookup tables). 1:1 passthrough with cleaned column names. \nSource: Dictionary.dbo.Staff \ndbt: source(''dictionary_dbo'', ''Staff'') \nColumns:\n  SK_StaffID -> sk_staff_id\n  SK_ServiceProviderID -> sk_service_provider_id\n  SK_OrganisationTypeID -> sk_organisation_type_id\n  FirstName -> first_name\n  Surname -> surname\n  LocalStaffRole -> local_staff_role\n  StaffCode -> staff_code\n  DateCreated -> date_created\n  DateUpdated -> date_updated"
    )
}}
select
    "SK_StaffID" as sk_staff_id,
    "SK_ServiceProviderID" as sk_service_provider_id,
    "SK_OrganisationTypeID" as sk_organisation_type_id,
    "FirstName" as first_name,
    "Surname" as surname,
    "LocalStaffRole" as local_staff_role,
    "StaffCode" as staff_code,
    "DateCreated" as date_created,
    "DateUpdated" as date_updated
from {{ source('dictionary_dbo', 'Staff') }}
