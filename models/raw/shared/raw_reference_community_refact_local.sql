-- Raw layer model for reference_analyst_managed.COMMUNITY_REFACT_LOCAL
-- Source: "DATA_LAKE__NCL"."ANALYST_MANAGED"
-- Description: Analyst-managed reference datasets and business rules
-- This is a 1:1 passthrough from source with standardized column names
select
    "PROVIDER" as provider,
    "WEEK_END" as week_end,
    "SERVICE_LINE" as service_line,
    "TEAM" as team,
    "No. of Referrals" as no_of_referrals,
    "ACCEPTED_REFERRALS" as accepted_referrals,
    "REFERRAL_SOURCE" as referral_source,
    "ACTIVITY" as activity,
    "ACTIVITY_TYPE" as activity_type,
    "POINT_OF_DELIVERY" as point_of_delivery,
    "SERVICE_LINE_GROUP" as service_line_group,
    "WEEK_ENDING" as week_ending,
    "YEAR" as year,
    "WEEK_NUMBER" as week_number,
    "CCG" as ccg
from {{ source('reference_analyst_managed', 'COMMUNITY_REFACT_LOCAL') }}
