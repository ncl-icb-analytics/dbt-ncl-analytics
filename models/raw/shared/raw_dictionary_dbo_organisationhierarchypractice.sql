{{
    config(
        description="Raw layer (Reference data including PDS and lookup tables). 1:1 passthrough with cleaned column names. \nSource: Dictionary.dbo.OrganisationHierarchyPractice \ndbt: source(''dictionary_dbo'', ''OrganisationHierarchyPractice'') \nColumns:\n  SK_OrganisationID -> sk_organisation_id\n  SK_OrganisationID_Parent -> sk_organisation_id_parent\n  Level -> level"
    )
}}
select
    "SK_OrganisationID" as sk_organisation_id,
    "SK_OrganisationID_Parent" as sk_organisation_id_parent,
    "Level" as level
from {{ source('dictionary_dbo', 'OrganisationHierarchyPractice') }}
