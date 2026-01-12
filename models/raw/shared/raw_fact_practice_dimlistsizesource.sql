{{
    config(
        description="Raw layer (Practice fact tables). 1:1 passthrough with cleaned column names. \nSource: DATA_LAKE.FACT_PRACTICE.DimListSizeSource \ndbt: source(''fact_practice'', ''DimListSizeSource'') \nColumns:\n  SK_ListSizeSourceID -> sk_list_size_source_id\n  ListSizeSource -> list_size_source\n  Description -> description"
    )
}}
select
    "SK_ListSizeSourceID" as sk_list_size_source_id,
    "ListSizeSource" as list_size_source,
    "Description" as description
from {{ source('fact_practice', 'DimListSizeSource') }}
