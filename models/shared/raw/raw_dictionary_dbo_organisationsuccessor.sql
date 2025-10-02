-- Raw layer model for dictionary_dbo.OrganisationSuccessor
-- Source: "Dictionary"."dbo"
-- Description: Reference data including PDS and lookup tables
-- This is a 1:1 passthrough from source with standardized column names
select
    "SK_OrganisationID" as sk_organisation_id,
    "SK_OrganisationID_Successor" as sk_organisation_id_successor,
    "Succession_Effective_Date" as succession_effective_date,
    "SK_OrganisationID_FinalSuccessor" as sk_organisation_id_final_successor,
    "UniqueKey" as unique_key,
    "SuccessorType" as successor_type,
    "SuccessorPrimaryRoleType" as successor_primary_role_type,
    "SuccessorAssignedBy" as successor_assigned_by,
    "UniqueSuccessorID" as unique_successor_id
from {{ source('dictionary_dbo', 'OrganisationSuccessor') }}
