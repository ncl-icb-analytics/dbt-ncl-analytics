{{
    config(
        description="Raw layer (Reference data for inpatient procedures and treatments). 1:1 passthrough with cleaned column names. \nSource: Dictionary.IP.LastEpisodeInSpellIndicator \ndbt: source(''dictionary_ip'', ''LastEpisodeInSpellIndicator'') \nColumns:\n  SK_LastEpisodeInSpellIndicatorID -> sk_last_episode_in_spell_indicator_id\n  BK_LastEpisodeInSpellIndicator -> bk_last_episode_in_spell_indicator\n  LastEpisodeInSpellIndicator -> last_episode_in_spell_indicator"
    )
}}
select
    "SK_LastEpisodeInSpellIndicatorID" as sk_last_episode_in_spell_indicator_id,
    "BK_LastEpisodeInSpellIndicator" as bk_last_episode_in_spell_indicator,
    "LastEpisodeInSpellIndicator" as last_episode_in_spell_indicator
from {{ source('dictionary_ip', 'LastEpisodeInSpellIndicator') }}
