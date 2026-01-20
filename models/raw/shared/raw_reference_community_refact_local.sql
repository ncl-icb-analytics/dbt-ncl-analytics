{{
    config(
        description="Raw layer (Analyst-managed reference datasets and business rules). 1:1 passthrough with cleaned column names. \nSource: DATA_LAKE__NCL.ANALYST_MANAGED.COMMUNITY_REFACT_LOCAL \ndbt: source(''reference_analyst_managed'', ''COMMUNITY_REFACT_LOCAL'') \nColumns:\n  PROVIDER -> provider\n  WEEK_END -> week_end\n  SERVICE_LINE -> service_line\n  TEAM -> team\n  No. of Referrals -> no_of_referrals\n  ACCEPTED_REFERRALS -> accepted_referrals\n  REFERRAL_SOURCE -> referral_source\n  ACTIVITY -> activity\n  ACTIVITY_TYPE -> activity_type\n  POINT_OF_DELIVERY -> point_of_delivery\n  SERVICE_LINE_GROUP -> service_line_group\n  WEEK_ENDING -> week_ending\n  YEAR -> year\n  WEEK_NUMBER -> week_number\n  CCG -> ccg"
    )
}}
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
