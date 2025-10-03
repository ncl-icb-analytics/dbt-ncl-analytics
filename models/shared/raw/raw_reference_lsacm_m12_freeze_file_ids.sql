-- Raw layer model for reference_analyst_managed.LSACM_M12_FREEZE_FILE_IDS
-- Source: "DATA_LAKE__NCL"."ANALYST_MANAGED"
-- Description: Analyst-managed reference datasets and business rules
-- This is a 1:1 passthrough from source with standardized column names
select
    "PROVIDER_CODE" as provider_code,
    "FINANCIAL_YEAR" as financial_year,
    "FILEID" as fileid
from {{ source('reference_analyst_managed', 'LSACM_M12_FREEZE_FILE_IDS') }}
