{{
    config(
        description="Raw layer (Personal Demographics Service data). 1:1 passthrough with cleaned column names. \nSource: DATA_LAKE.PDS.PDS_Person_Merger \ndbt: source(''pds'', ''PDS_Person_Merger'') \nColumns:\n  RowID -> row_id\n  Pseudo NHS Number -> pseudo_nhs_number\n  Pseudo Superseded NHS Number -> pseudo_superseded_nhs_number\n  Person Merger Business Effective From Date -> person_merger_business_effective_from_date\n  Person Merger Business Effective To Date -> person_merger_business_effective_to_date"
    )
}}
select
    "RowID" as row_id,
    "Pseudo NHS Number" as pseudo_nhs_number,
    "Pseudo Superseded NHS Number" as pseudo_superseded_nhs_number,
    "Person Merger Business Effective From Date" as person_merger_business_effective_from_date,
    "Person Merger Business Effective To Date" as person_merger_business_effective_to_date
from {{ source('pds', 'PDS_Person_Merger') }}
