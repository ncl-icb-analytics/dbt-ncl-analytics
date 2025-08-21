{{ config(materialized='view') }}

SELECT
    PRIMARYKEY_ID,
    care_professional_treatment_function,
    care_professional_main_specialty
FROM {{ ref('stg_apc_spell_episodes') }}
WHERE dominant_episode_flag = 1
