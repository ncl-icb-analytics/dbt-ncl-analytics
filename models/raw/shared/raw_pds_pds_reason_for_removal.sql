{{
    config(
        description="Raw layer (Personal Demographics Service data). 1:1 passthrough with cleaned column names. \nSource: DATA_LAKE.PDS.PDS_Reason_For_Removal \ndbt: source(''pds'', ''PDS_Reason_For_Removal'') \nColumns:\n  RowID -> row_id\n  Pseudo NHS Number -> pseudo_nhs_number\n  Reason for Removal -> reason_for_removal\n  Reason for Removal Business Effective From Date -> reason_for_removal_business_effective_from_date\n  Reason for Removal Business Effective To Date -> reason_for_removal_business_effective_to_date"
    )
}}
select
    "RowID" as row_id,
    "Pseudo NHS Number" as pseudo_nhs_number,
    "Reason for Removal" as reason_for_removal,
    "Reason for Removal Business Effective From Date" as reason_for_removal_business_effective_from_date,
    "Reason for Removal Business Effective To Date" as reason_for_removal_business_effective_to_date
from {{ source('pds', 'PDS_Reason_For_Removal') }}
