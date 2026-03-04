{{
    config(
        description="Raw layer (Central Performance Analytics Team (PMCT)). 1:1 passthrough with cleaned column names. \nSource: DATA_LAKE.PMCT.DiagnosticsMonthlySourceAppendReviseProvComm \ndbt: source(''pmct'', ''DiagnosticsMonthlySourceAppendReviseProvComm'') \nColumns:\n  Period -> period\n  Provider Parent Org Code -> provider_parent_org_code\n  Provider Parent Name -> provider_parent_name\n  Provider Org Code -> provider_org_code\n  Provider Org Name -> provider_org_name\n  Commissioner Parent Org Code -> commissioner_parent_org_code\n  Commissioner Parent Name -> commissioner_parent_name\n  Commissioner Org Code -> commissioner_org_code\n  Commissioner Org Name -> commissioner_org_name\n  Diagnostic Tests Sort Order -> diagnostic_tests_sort_order\n  Diagnostic Tests -> diagnostic_tests\n  00 < 01 Week -> week_00_lt_01\n  01 < 02 Weeks -> weeks_01_lt_02\n  02 < 03 Weeks -> weeks_02_lt_03\n  03 < 04 Weeks -> weeks_03_lt_04\n  04 < 05 Weeks -> weeks_04_lt_05\n  05 < 06 Weeks -> weeks_05_lt_06\n  06 < 07 Weeks -> weeks_06_lt_07\n  07 < 08 Weeks -> weeks_07_lt_08\n  08 < 09 Weeks -> weeks_08_lt_09\n  09 < 10 Weeks -> weeks_09_lt_10\n  10 < 11 Weeks -> weeks_10_lt_11\n  11 < 12 Weeks -> weeks_11_lt_12\n  12 < 13 Weeks -> weeks_12_lt_13\n  13+ Weeks -> weeks_13_plus\n  Total WL -> total_wl\n  Waiting List Activity -> waiting_list_activity\n  Planned Activity -> planned_activity\n  Unscheduled Activity -> unscheduled_activity\n  Total Activity -> total_activity\n  CreateTS -> create_ts"
    )
}}
select
    "Period" as period,
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
    "CreateTS" as create_ts
from {{ source('pmct', 'DiagnosticsMonthlySourceAppendReviseProvComm') }}
