{{
    config(
        description="Raw layer (Analyst-managed reference datasets and business rules). 1:1 passthrough with cleaned column names. \nSource: DATA_LAKE__NCL.ANALYST_MANAGED.COMMUNITY_WAITING_TIMES_LOCAL \ndbt: source(''reference_analyst_managed'', ''COMMUNITY_WAITING_TIMES_LOCAL'') \nColumns:\n  SUBMITTED_MONTH -> submitted_month\n  REPORTED_MONTH -> reported_month\n  PROVIDER -> provider\n  PROVIDER_SITE -> provider_site\n  SERVICE_LINE -> service_line\n  0 - 6 weeks -> weeks_0_6\n  6 - 12 weeks -> weeks_6_12\n  12 - 16 weeks -> weeks_12_16\n  16 - 18 weeks -> weeks_16_18\n  18 - 30 weeks -> weeks_18_30\n  30 - 40 weeks -> weeks_30_40\n  40 - 52 weeks -> weeks_40_52\n  52+ weeks -> weeks_52_plus\n  TOTAL -> total\n  AVERAGE_WEEKS_WAITING -> average_weeks_waiting\n  SERVICE_LINE_GROUP -> service_line_group\n  SERVICE_LINE_GROUP_2 -> service_line_group_2\n  AGE_GROUP -> age_group\n  BOROUGH -> borough\n  PROVIDER_SERVICE_LINE -> provider_service_line\n  IS_EXCLUDED -> is_excluded\n  EXCLUSION_GROUP -> exclusion_group"
    )
}}
select
    "SUBMITTED_MONTH" as submitted_month,
    "REPORTED_MONTH" as reported_month,
    "PROVIDER" as provider,
    "PROVIDER_SITE" as provider_site,
    "SERVICE_LINE" as service_line,
    "0 - 6 weeks" as weeks_0_6,
    "6 - 12 weeks" as weeks_6_12,
    "12 - 16 weeks" as weeks_12_16,
    "16 - 18 weeks" as weeks_16_18,
    "18 - 30 weeks" as weeks_18_30,
    "30 - 40 weeks" as weeks_30_40,
    "40 - 52 weeks" as weeks_40_52,
    "52+ weeks" as weeks_52_plus,
    "TOTAL" as total,
    "AVERAGE_WEEKS_WAITING" as average_weeks_waiting,
    "SERVICE_LINE_GROUP" as service_line_group,
    "SERVICE_LINE_GROUP_2" as service_line_group_2,
    "AGE_GROUP" as age_group,
    "BOROUGH" as borough,
    "PROVIDER_SERVICE_LINE" as provider_service_line,
    "IS_EXCLUDED" as is_excluded,
    "EXCLUSION_GROUP" as exclusion_group
from {{ source('reference_analyst_managed', 'COMMUNITY_WAITING_TIMES_LOCAL') }}
