{{
    config(
        description="Raw layer (Analyst-managed reference datasets and business rules). 1:1 passthrough with cleaned column names. \nSource: DATA_LAKE__NCL.ANALYST_MANAGED.UCR_REFERRALS_PLAN \ndbt: source(''reference_analyst_managed'', ''UCR_REFERRALS_PLAN'') \nColumns:\n  FIN_YEAR -> fin_year\n  FIN_MONTH -> fin_month\n  FIN_MONTH_NO -> fin_month_no\n  UCR_REFERRALS -> ucr_referrals"
    )
}}
select
    "FIN_YEAR" as fin_year,
    "FIN_MONTH" as fin_month,
    "FIN_MONTH_NO" as fin_month_no,
    "UCR_REFERRALS" as ucr_referrals
from {{ source('reference_analyst_managed', 'UCR_REFERRALS_PLAN') }}
