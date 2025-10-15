-- Raw layer model for reference_analyst_managed.IA_NEW_USERS_MONTHLY
-- Source: "DATA_LAKE__NCL"."ANALYST_MANAGED"
-- Description: Analyst-managed reference datasets and business rules
-- This is a 1:1 passthrough from source with standardized column names
select
    "FULL_NAME" as full_name,
    "USERNAME" as username,
    "TRUST_HOSPITAL" as trust_hospital,
    "WHAT_BEST_DESCRIBES_YOUR_ROLE" as what_best_describes_your_role,
    "BAND_GRADE" as band_grade,
    "DATE_AND_TIME_REGISTERED" as date_and_time_registered,
    "FULL_NAME_WITH_LINK" as full_name_with_link,
    "FULL_NAME_2" as full_name_2
from {{ source('reference_analyst_managed', 'IA_NEW_USERS_MONTHLY') }}
