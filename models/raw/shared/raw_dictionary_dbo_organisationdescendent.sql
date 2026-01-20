{{
    config(
        description="Raw layer (Reference data including PDS and lookup tables). 1:1 passthrough with cleaned column names. \nSource: Dictionary.dbo.OrganisationDescendent \ndbt: source(''dictionary_dbo'', ''OrganisationDescendent'') \nColumns:\n  SK_OrganisationID_Root -> sk_organisation_id_root\n  OrganisationCode_Root -> organisation_code_root\n  OrganisationPrimaryRole_Root -> organisation_primary_role_root\n  SK_OrganisationID_Parent -> sk_organisation_id_parent\n  OrganisationCode_Parent -> organisation_code_parent\n  OrganisationPrimaryRole_Parent -> organisation_primary_role_parent\n  SK_OrganisationID_Child -> sk_organisation_id_child\n  OrganisationCode_Child -> organisation_code_child\n  OrganisationPrimaryRole_Child -> organisation_primary_role_child\n  RelationshipType -> relationship_type\n  RelationshipStartDate -> relationship_start_date\n  RelationshipEndDate -> relationship_end_date\n  Path -> path\n  Depth -> depth\n  PathStartDate -> path_start_date\n  PathEndDate -> path_end_date\n  DateAdded -> date_added\n  DateUpdated -> date_updated"
    )
}}
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
