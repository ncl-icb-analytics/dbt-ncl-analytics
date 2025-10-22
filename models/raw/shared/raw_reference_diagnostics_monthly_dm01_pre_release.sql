-- Raw layer model for reference_analyst_managed.DIAGNOSTICS_MONTHLY_DM01_Pre-release
-- Source: "DATA_LAKE__NCL"."ANALYST_MANAGED"
-- Description: Analyst-managed reference datasets and business rules
-- This is a 1:1 passthrough from source with standardized column names
select
    "PERIOD" as period,
    "Provider Parent Org Code" as provider_parent_org_code,
    "Provider Parent Name" as provider_parent_name,
    "Provider Org Code" as provider_org_code,
    "Provider Org Name" as provider_org_name,
    "Commissioner Parent Org Code" as commissioner_parent_org_code,
    "Commissioner Parent Name" as commissioner_parent_name,
    "Commissioner Org Code" as commissioner_org_code,
    "Commissioner Org Name" as commissioner_org_name,
    "Diagnostic Tests Sort Order" as diagnostic_tests_sort_order,
    "Diagnostic Tests" as diagnostic_tests,
    "00 < 01 Week" as week_00_lt_01,
    "01 < 02 Weeks" as weeks_01_lt_02,
    "02 < 03 Weeks" as weeks_02_lt_03,
    "03 < 04 Weeks" as weeks_03_lt_04,
    "04 < 05 Weeks" as weeks_04_lt_05,
    "05 < 06 Weeks" as weeks_05_lt_06,
    "06 < 07 Weeks" as weeks_06_lt_07,
    "07 < 08 Weeks" as weeks_07_lt_08,
    "08 < 09 Weeks" as weeks_08_lt_09,
    "09 < 10 Weeks" as weeks_09_lt_10,
    "10 < 11 Weeks" as weeks_10_lt_11,
    "11 < 12 Weeks" as weeks_11_lt_12,
    "12 < 13 Weeks" as weeks_12_lt_13,
    "13+ Weeks" as weeks_13_plus,
    "Total WL" as total_wl,
    "Waiting List Activity" as waiting_list_activity,
    "Planned Activity" as planned_activity,
    "Unscheduled Activity" as unscheduled_activity,
    "Total Activity" as total_activity,
    "<6Weeks" as lt_6_weeks,
    "6+WW" as ww_6_plus
from {{ source('reference_analyst_managed', 'DIAGNOSTICS_MONTHLY_DM01_Pre-release') }}
