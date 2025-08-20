-- Staging model for dictionary_dbo.Staff
-- Source: "Dictionary"."dbo"
-- Description: Reference data including PDS and lookup tables

select
    "SK_StaffID" as sk_staffid,
    "SK_ServiceProviderID" as sk_serviceproviderid,
    "SK_OrganisationTypeID" as sk_organisationtypeid,
    "FirstName" as firstname,
    "Surname" as surname,
    "LocalStaffRole" as localstaffrole,
    "StaffCode" as staffcode,
    "DateCreated" as datecreated,
    "DateUpdated" as dateupdated
from {{ source('dictionary_dbo', 'Staff') }}
