{{
    config(
        description="Raw layer (Mental Health Services Data Set (MHSDS)). 1:1 passthrough with cleaned column names. \nSource: DATA_LAKE.MHSDS.MHSDS_RelatedOrganisation \ndbt: source(''mhsds'', ''MHSDS_RelatedOrganisation'') \nColumns:\n  RecordNumber -> record_number\n  SourceObject -> source_object\n  SourceColumn -> source_column\n  OrganisationCode_Related -> organisation_code_related"
    )
}}
select
    "RecordNumber" as record_number,
    "SourceObject" as source_object,
    "SourceColumn" as source_column,
    "OrganisationCode_Related" as organisation_code_related
from {{ source('mhsds', 'MHSDS_RelatedOrganisation') }}
