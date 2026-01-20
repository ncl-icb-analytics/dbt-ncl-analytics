{{
    config(
        description="Raw layer (Reference data including PDS and lookup tables). 1:1 passthrough with cleaned column names. \nSource: Dictionary.dbo.OrganisationRole \ndbt: source(''dictionary_dbo'', ''OrganisationRole'') \nColumns:\n  SK_OrganisationID -> sk_organisation_id\n  RoleType -> role_type\n  LegalStartDate -> legal_start_date\n  LegalEndDate -> legal_end_date\n  OperationalStartDate -> operational_start_date\n  OperationalEndDate -> operational_end_date\n  IsPrimaryRole -> is_primary_role\n  IsActive -> is_active\n  StartDate -> start_date\n  EndDate -> end_date\n  UniqueRoleID -> unique_role_id"
    )
}}
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
