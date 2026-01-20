{{
    config(
        description="Raw layer (Reference data including PDS and lookup tables). 1:1 passthrough with cleaned column names. \nSource: Dictionary.dbo.OrganisationSuccessor \ndbt: source(''dictionary_dbo'', ''OrganisationSuccessor'') \nColumns:\n  SK_OrganisationID -> sk_organisation_id\n  SK_OrganisationID_Successor -> sk_organisation_id_successor\n  Succession_Effective_Date -> succession_effective_date\n  SK_OrganisationID_FinalSuccessor -> sk_organisation_id_final_successor\n  UniqueKey -> unique_key\n  SuccessorType -> successor_type\n  SuccessorPrimaryRoleType -> successor_primary_role_type\n  SuccessorAssignedBy -> successor_assigned_by\n  UniqueSuccessorID -> unique_successor_id"
    )
}}
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
