{{
    config(
        description="Raw layer (Analyst-managed reference datasets and business rules). 1:1 passthrough with cleaned column names. \nSource: DATA_LAKE__NCL.ANALYST_MANAGED.VW_WORKFORCE_PRACTICE_LEVEL_DETAILED \ndbt: source(''reference_analyst_managed'', ''VW_WORKFORCE_PRACTICE_LEVEL_DETAILED'') \nColumns:\n  Period -> period\n  Year -> year\n  Month -> month\n  Month_No -> month_no\n  Borough -> borough\n  PCN -> pcn\n  Neighbourhood -> neighbourhood\n  Practice code -> practice_code\n  GP Practice -> gp_practice\n  Staff Group -> staff_group\n  Staff Role -> staff_role\n  FTE/Headcount -> fte_headcount\n  Staff in Post -> staff_in_post\n  Measure -> measure\n  Weighted List Size -> weighted_list_size\n  Per1000 weighted population -> per1000_weighted_population\n  Latest Period Flag -> latest_period_flag\n  Same Month In Previous Years Flag -> same_month_in_previous_years_flag"
    )
}}
select
    "Period" as period,
    "Year" as year,
    "Month" as month,
    "Month_No" as month_no,
    "Borough" as borough,
    "PCN" as pcn,
    "Neighbourhood" as neighbourhood,
    "Practice code" as practice_code,
    "GP Practice" as gp_practice,
    "Staff Group" as staff_group,
    "Staff Role" as staff_role,
    "FTE/Headcount" as fte_headcount,
    "Staff in Post" as staff_in_post,
    "Measure" as measure,
    "Weighted List Size" as weighted_list_size,
    "Per1000 weighted population" as per1000_weighted_population,
    "Latest Period Flag" as latest_period_flag,
    "Same Month In Previous Years Flag" as same_month_in_previous_years_flag
from {{ source('reference_analyst_managed', 'VW_WORKFORCE_PRACTICE_LEVEL_DETAILED') }}
