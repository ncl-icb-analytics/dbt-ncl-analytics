{{
    config(
        description="Raw layer (Mental Health Services Data Set (MHSDS)). 1:1 passthrough with cleaned column names. \nSource: DATA_LAKE.MHSDS.bridging \ndbt: source(''mhsds'', ''bridging'') \nColumns:\n  Person_ID -> person_id\n  NHSNumber Pseudo -> nhs_number_pseudo\n  Person_Index_id -> person_index_id\n  Pseudo_NHS_Number -> pseudo_nhs_number"
    )
}}
select
    "Person_ID" as person_id,
    "NHSNumber Pseudo" as nhs_number_pseudo,
    "Person_Index_id" as person_index_id,
    "Pseudo_NHS_Number" as pseudo_nhs_number
from {{ source('mhsds', 'bridging') }}
