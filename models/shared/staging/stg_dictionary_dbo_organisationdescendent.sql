-- Staging model for dictionary_dbo.OrganisationDescendent
-- Source: "Dictionary"."dbo"
-- Description: Reference data including PDS and lookup tables

select
    "SK_OrganisationID_Root" as sk_organisation_id_root,
    "OrganisationCode_Root" as organisation_code_root,
    "OrganisationPrimaryRole_Root" as organisation_primary_role_root,
    "SK_OrganisationID_Parent" as sk_organisation_id_parent,
    "OrganisationCode_Parent" as organisation_code_parent,
    "OrganisationPrimaryRole_Parent" as organisation_primary_role_parent,
    "SK_OrganisationID_Child" as sk_organisation_id_child,
    "OrganisationCode_Child" as organisation_code_child,
    "OrganisationPrimaryRole_Child" as organisation_primary_role_child,
    "RelationshipType" as relationship_type,
    "RelationshipStartDate" as relationship_start_date,
    "RelationshipEndDate" as relationship_end_date,
    "Path" as path,
    "Depth" as depth,
    "PathStartDate" as path_start_date,
    "PathEndDate" as path_end_date,
    "DateAdded" as date_added,
    "DateUpdated" as date_updated
from {{ source('dictionary_dbo', 'OrganisationDescendent') }}
