{{
    config(
        description="Raw layer (Analyst-managed reference datasets and business rules). 1:1 passthrough with cleaned column names. \nSource: DATA_LAKE__NCL.ANALYST_MANAGED.PRACTICE_NEIGHBOURHOOD_LOOKUP_BACKUP \ndbt: source(''reference_analyst_managed'', ''PRACTICE_NEIGHBOURHOOD_LOOKUP_BACKUP'') \nColumns:\n  PRACTICECODE -> practicecode\n  PRACTICENAME -> practicename\n  PCNCODE -> pcncode\n  LOCALAUTHORITY -> localauthority\n  PRACTICENEIGHBOURHOOD -> practiceneighbourhood"
    )
}}
select
    "PRACTICECODE" as practicecode,
    "PRACTICENAME" as practicename,
    "PCNCODE" as pcncode,
    "LOCALAUTHORITY" as localauthority,
    "PRACTICENEIGHBOURHOOD" as practiceneighbourhood
from {{ source('reference_analyst_managed', 'PRACTICE_NEIGHBOURHOOD_LOOKUP_BACKUP') }}
