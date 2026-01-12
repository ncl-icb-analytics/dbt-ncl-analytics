{{
    config(
        description="Raw layer (Patient fact tables). 1:1 passthrough with cleaned column names. \nSource: DATA_LAKE.FACT_PATIENT.DimDataSource \ndbt: source(''fact_patient'', ''DimDataSource'') \nColumns:\n  SK_DataSourceID -> sk_data_source_id\n  SK_DataSourceParentID -> sk_data_source_parent_id\n  DataSource -> data_source\n  Description -> description"
    )
}}
select
    "SK_DataSourceID" as sk_data_source_id,
    "SK_DataSourceParentID" as sk_data_source_parent_id,
    "DataSource" as data_source,
    "Description" as description
from {{ source('fact_patient', 'DimDataSource') }}
