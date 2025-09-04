-- Staging model for dictionary_dbo.OrganisationRole
-- Source: "Dictionary"."dbo"
-- Description: Reference data including PDS and lookup tables

select
    "SK_OrganisationID" as sk_organisation_id,
    "RoleType" as role_type,
    "LegalStartDate" as legal_start_date,
    "LegalEndDate" as legal_end_date,
    "OperationalStartDate" as operational_start_date,
    "OperationalEndDate" as operational_end_date,
    "IsPrimaryRole" as is_primary_role,
    "IsActive" as is_active,
    "StartDate" as start_date,
    "EndDate" as end_date,
    "UniqueRoleID" as unique_role_id
from {{ source('dictionary_dbo', 'OrganisationRole') }}
