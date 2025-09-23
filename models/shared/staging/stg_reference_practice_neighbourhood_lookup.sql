-- Staging model for reference_analyst_managed.PRACTICE_NEIGHBOURHOOD_LOOKUP
-- Source: "DATA_LAKE__NCL"."ANALYST_MANAGED"
-- Description: Analyst-managed reference datasets and business rules

select
    "PRACTICECODE" as practicecode,
    "PRACTICENAME" as practicename,
    "PCNCODE" as pcncode,
    "LOCALAUTHORITY" as localauthority,
    "PRACTICENEIGHBOURHOOD" as practiceneighbourhood
from {{ source('reference_analyst_managed', 'PRACTICE_NEIGHBOURHOOD_LOOKUP') }}
