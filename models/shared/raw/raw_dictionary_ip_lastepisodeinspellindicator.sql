-- Raw layer model for dictionary_ip.LastEpisodeInSpellIndicator
-- Source: "Dictionary"."IP"
-- Description: Reference data for inpatient procedures and treatments
-- This is a 1:1 passthrough from source with standardized column names
select
    "SK_LastEpisodeInSpellIndicatorID" as sk_last_episode_in_spell_indicator_id,
    "BK_LastEpisodeInSpellIndicator" as bk_last_episode_in_spell_indicator,
    "LastEpisodeInSpellIndicator" as last_episode_in_spell_indicator
from {{ source('dictionary_ip', 'LastEpisodeInSpellIndicator') }}
