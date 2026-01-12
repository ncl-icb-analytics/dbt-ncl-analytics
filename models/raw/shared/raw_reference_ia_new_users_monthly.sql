{{
    config(
        description="Raw layer (Analyst-managed reference datasets and business rules). 1:1 passthrough with cleaned column names. \nSource: DATA_LAKE__NCL.ANALYST_MANAGED.IA_NEW_USERS_MONTHLY \ndbt: source(''reference_analyst_managed'', ''IA_NEW_USERS_MONTHLY'') \nColumns:\n  Full name -> full_name\n  Username -> username\n  Trust / Hospital -> trust_hospital\n  What best describes your role? -> what_best_describes_your_role\n  Band/Grade -> band_grade\n  Date and time registered -> date_and_time_registered\n  Full name with link -> full_name_with_link\n  Full name 2 -> full_name_2"
    )
}}
select
    "Full name" as full_name,
    "Username" as username,
    "Trust / Hospital" as trust_hospital,
    "What best describes your role?" as what_best_describes_your_role,
    "Band/Grade" as band_grade,
    "Date and time registered" as date_and_time_registered,
    "Full name with link" as full_name_with_link,
    "Full name 2" as full_name_2
from {{ source('reference_analyst_managed', 'IA_NEW_USERS_MONTHLY') }}
