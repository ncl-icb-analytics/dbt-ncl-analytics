-- Raw layer model for fact_patient.DimDataSource
-- Source: "DATA_LAKE"."FACT_PATIENT"
-- Description: Patient fact tables
-- This is a 1:1 passthrough from source with standardized column names
select
    "SK_DataSourceID" as sk_data_source_id,
    "SK_DataSourceParentID" as sk_data_source_parent_id,
    "DataSource" as data_source,
    "Description" as description
from {{ source('fact_patient', 'DimDataSource') }}
