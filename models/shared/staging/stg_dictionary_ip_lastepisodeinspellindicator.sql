-- Staging model for dictionary_ip.LastEpisodeInSpellIndicator
-- Source: "Dictionary"."IP"
-- Description: Reference data for inpatient procedures and treatments

select
    "SK_LastEpisodeInSpellIndicatorID" as sk_lastepisodeinspellindicatorid,
    "BK_LastEpisodeInSpellIndicator" as bk_lastepisodeinspellindicator,
    "LastEpisodeInSpellIndicator" as lastepisodeinspellindicator
from {{ source('dictionary_ip', 'LastEpisodeInSpellIndicator') }}
