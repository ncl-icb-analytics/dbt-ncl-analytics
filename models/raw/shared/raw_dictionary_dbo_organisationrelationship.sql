-- Raw layer model for dictionary_dbo.OrganisationRelationship
-- Source: "Dictionary"."dbo"
-- Description: Reference data including PDS and lookup tables
-- This is a 1:1 passthrough from source with standardized column names
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
