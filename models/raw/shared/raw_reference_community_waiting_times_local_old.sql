-- Raw layer model for reference_analyst_managed.COMMUNITY_WAITING_TIMES_LOCAL_OLD
-- Source: "DATA_LAKE__NCL"."ANALYST_MANAGED"
-- Description: Analyst-managed reference datasets and business rules
-- This is a 1:1 passthrough from source with standardized column names
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
from {{ source('reference_analyst_managed', 'COMMUNITY_WAITING_TIMES_LOCAL_OLD') }}
