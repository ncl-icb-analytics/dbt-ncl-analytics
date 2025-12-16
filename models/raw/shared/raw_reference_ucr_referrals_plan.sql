-- Raw layer model for reference_analyst_managed.UCR_REFERRALS_PLAN
-- Source: "DATA_LAKE__NCL"."ANALYST_MANAGED"
-- Description: Analyst-managed reference datasets and business rules
-- This is a 1:1 passthrough from source with standardized column names
select
    "FIN_YEAR" as fin_year,
    "FIN_MONTH" as fin_month,
    "FIN_MONTH_NO" as fin_month_no,
    "UCR_REFERRALS" as ucr_referrals
from {{ source('reference_analyst_managed', 'UCR_REFERRALS_PLAN') }}
