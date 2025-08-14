{{ config(materialized='view') }}

SELECT
    PRIMARYKEY_ID,
    system_transaction_cds_unique_identifier
FROM {{ ref('stg_apc_spell_episodes') }}
WHERE episodes_id = 1
