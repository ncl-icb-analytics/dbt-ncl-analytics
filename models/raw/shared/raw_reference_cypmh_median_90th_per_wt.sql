-- Raw layer model for reference_analyst_managed.CYPMH_Median & 90th Per WT
-- Source: "DATA_LAKE__NCL"."ANALYST_MANAGED"
-- Description: Analyst-managed reference datasets and business rules
-- This is a 1:1 passthrough from source with standardized column names
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
