-- Raw layer model for reference_analyst_managed.IA_NEW_USERS_MONTHLY
-- Source: "DATA_LAKE__NCL"."ANALYST_MANAGED"
-- Description: Analyst-managed reference datasets and business rules
-- This is a 1:1 passthrough from source with standardized column names
select
    "Full name" as full_name,
    "Username" as username,
    "Trust / Hospital" as trust_hospital,
    "What best describes your role?" as what_best_describes_your_role?,
    "Band/Grade" as band_grade,
    "Date and time registered" as date_and_time_registered,
    "Full name with link" as full_name_with_link,
    "Full name 2" as full_name_2
from {{ source('reference_analyst_managed', 'IA_NEW_USERS_MONTHLY') }}
