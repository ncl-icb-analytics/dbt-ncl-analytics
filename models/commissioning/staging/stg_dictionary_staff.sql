-- Staging model for dictionary.Staff
-- Source: "Dictionary"."dbo"
{% if source.get('description') %}
-- Description: Reference data including PDS and lookup tables
{% endif %}

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
from {{ source('dictionary', 'Staff') }}
