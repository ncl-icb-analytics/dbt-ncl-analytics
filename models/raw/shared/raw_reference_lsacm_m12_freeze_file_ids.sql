{{
    config(
        description="Raw layer (Analyst-managed reference datasets and business rules). 1:1 passthrough with cleaned column names. \nSource: DATA_LAKE__NCL.ANALYST_MANAGED.LSACM_M12_FREEZE_FILE_IDS \ndbt: source(''reference_analyst_managed'', ''LSACM_M12_FREEZE_FILE_IDS'') \nColumns:\n  PROVIDER_CODE -> provider_code\n  FINANCIAL_YEAR -> financial_year\n  FILEID -> fileid"
    )
}}
select
    "PROVIDER_CODE" as provider_code,
    "FINANCIAL_YEAR" as financial_year,
    "FILEID" as fileid
from {{ source('reference_analyst_managed', 'LSACM_M12_FREEZE_FILE_IDS') }}
