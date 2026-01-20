{{
    config(
        description="Raw layer (Personal Demographics Service data). 1:1 passthrough with cleaned column names. \nSource: DATA_LAKE.PDS.PDS_Person \ndbt: source(''pds'', ''PDS_Person'') \nColumns:\n  RowID -> row_id\n  Pseudo NHS Number -> pseudo_nhs_number\n  YearMonth_Of_Birth -> year_month_of_birth\n  Gender -> gender\n  Date of Death -> date_of_death\n  Death Status -> death_status\n  Preferred Language -> preferred_language\n  Interpreter required -> interpreter_required\n  Person Business Effective From Date -> person_business_effective_from_date\n  Person Business Effective To Date -> person_business_effective_to_date"
    )
}}
select
    "RowID" as row_id,
    "Pseudo NHS Number" as pseudo_nhs_number,
    "YearMonth_Of_Birth" as year_month_of_birth,
    "Gender" as gender,
    "Date of Death" as date_of_death,
    "Death Status" as death_status,
    "Preferred Language" as preferred_language,
    "Interpreter required" as interpreter_required,
    "Person Business Effective From Date" as person_business_effective_from_date,
    "Person Business Effective To Date" as person_business_effective_to_date
from {{ source('pds', 'PDS_Person') }}
