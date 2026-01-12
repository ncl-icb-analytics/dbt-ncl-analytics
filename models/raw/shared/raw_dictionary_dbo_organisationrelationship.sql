{{
    config(
        description="Raw layer (Reference data including PDS and lookup tables). 1:1 passthrough with cleaned column names. \nSource: Dictionary.dbo.OrganisationRelationship \ndbt: source(''dictionary_dbo'', ''OrganisationRelationship'') \nColumns:\n  SK_OrganisationID -> sk_organisation_id\n  SK_OrganisationID_Target -> sk_organisation_id_target\n  TargetRelationshipType -> target_relationship_type\n  TargetPrimaryRoleType -> target_primary_role_type\n  LegalStartDate -> legal_start_date\n  LegalEndDate -> legal_end_date\n  OperationalStartDate -> operational_start_date\n  OperationalEndDate -> operational_end_date\n  TargetAssignedBy -> target_assigned_by\n  IsActive -> is_active\n  StartDate -> start_date\n  EndDate -> end_date\n  UniqueRelationshipID -> unique_relationship_id"
    )
}}
select
    "SK_OrganisationID" as sk_organisation_id,
    "SK_OrganisationID_Target" as sk_organisation_id_target,
    "TargetRelationshipType" as target_relationship_type,
    "TargetPrimaryRoleType" as target_primary_role_type,
    "LegalStartDate" as legal_start_date,
    "LegalEndDate" as legal_end_date,
    "OperationalStartDate" as operational_start_date,
    "OperationalEndDate" as operational_end_date,
    "TargetAssignedBy" as target_assigned_by,
    "IsActive" as is_active,
    "StartDate" as start_date,
    "EndDate" as end_date,
    "UniqueRelationshipID" as unique_relationship_id
from {{ source('dictionary_dbo', 'OrganisationRelationship') }}
