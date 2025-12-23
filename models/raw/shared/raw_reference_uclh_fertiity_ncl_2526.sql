-- Raw layer model for reference_analyst_managed.UCLH_FERTIITY_NCL_2526
-- Source: "DATA_LAKE__NCL"."ANALYST_MANAGED"
-- Description: Analyst-managed reference datasets and business rules
-- This is a 1:1 passthrough from source with standardized column names
select
    "Received Yr" as received_yr,
    "DATE" as date,
    "DESCRIPTION" as description,
    "CUSTOMER" as customer,
    "VALUE" as value,
    "Month of treatment text" as month_of_treatment_text,
    "Month of treatment" as month_of_treatment,
    "Fin Yr" as fin_yr
from {{ source('reference_analyst_managed', 'UCLH_FERTIITY_NCL_2526') }}
