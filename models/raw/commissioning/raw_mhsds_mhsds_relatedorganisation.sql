-- Raw layer model for mhsds.MHSDS_RelatedOrganisation
-- Source: "DATA_LAKE"."MHSDS"
-- Description: Mental Health Services Data Set (MHSDS)
-- This is a 1:1 passthrough from source with standardized column names
select
    "RecordNumber" as record_number,
    "SourceObject" as source_object,
    "SourceColumn" as source_column,
    "OrganisationCode_Related" as organisation_code_related
from {{ source('mhsds', 'MHSDS_RelatedOrganisation') }}
