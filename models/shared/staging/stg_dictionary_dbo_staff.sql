-- Staging model for dictionary_dbo.Staff
-- Source: "Dictionary"."dbo"
-- Description: Reference data including PDS and lookup tables

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
