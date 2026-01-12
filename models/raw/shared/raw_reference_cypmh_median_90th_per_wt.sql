{{
    config(
        description="Raw layer (Analyst-managed reference datasets and business rules). 1:1 passthrough with cleaned column names. \nSource: DATA_LAKE__NCL.ANALYST_MANAGED.CYPMH_Median & 90th Per WT \ndbt: source(''reference_analyst_managed'', ''CYPMH_Median & 90th Per WT'') \nColumns:\n  Organisation Code -> organisation_code\n  Organisation Name -> organisation_name\n  Metric -> metric\n  August 2023 -> august_2023\n  September 2023 -> september_2023\n  October 2023 -> october_2023\n  November 2023 -> november_2023\n  December 2023 -> december_2023\n  January 2024 -> january_2024\n  February 2024 -> february_2024\n  March 2024 -> march_2024\n  April 2024 -> april_2024\n  May 2024 -> may_2024\n  June 2024 -> june_2024\n  July 2024 -> july_2024\n  August 2024 -> august_2024\n  September 2024 -> september_2024\n  October 2024 -> october_2024\n  November 2024 -> november_2024\n  December 2024 -> december_2024\n  January 2025 -> january_2025\n  February 2025 -> february_2025\n  March 2025 -> march_2025\n  April 2025 -> april_2025\n  May 2025 -> may_2025\n  June 2025 -> june_2025\n  July 2025 -> july_2025\n  August 2025 -> august_2025\n  September 2025 -> september_2025"
    )
}}
select
    "Organisation Code" as organisation_code,
    "Organisation Name" as organisation_name,
    "Metric" as metric,
    "August 2023" as august_2023,
    "September 2023" as september_2023,
    "October 2023" as october_2023,
    "November 2023" as november_2023,
    "December 2023" as december_2023,
    "January 2024" as january_2024,
    "February 2024" as february_2024,
    "March 2024" as march_2024,
    "April 2024" as april_2024,
    "May 2024" as may_2024,
    "June 2024" as june_2024,
    "July 2024" as july_2024,
    "August 2024" as august_2024,
    "September 2024" as september_2024,
    "October 2024" as october_2024,
    "November 2024" as november_2024,
    "December 2024" as december_2024,
    "January 2025" as january_2025,
    "February 2025" as february_2025,
    "March 2025" as march_2025,
    "April 2025" as april_2025,
    "May 2025" as may_2025,
    "June 2025" as june_2025,
    "July 2025" as july_2025,
    "August 2025" as august_2025,
    "September 2025" as september_2025
from {{ source('reference_analyst_managed', 'CYPMH_Median & 90th Per WT') }}
