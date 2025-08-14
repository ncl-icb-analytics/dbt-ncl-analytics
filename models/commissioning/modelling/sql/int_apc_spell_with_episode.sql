
-- Pre-filter dictionaries

{{ config(materialized='view') }}



SELECT
    sp.*,
    dom.care_professional.treatment_function AS dom_tfc,
    dom.care_professional.main_specialty AS dom_main_spec,
    fe."system.transaction.cds_unique_identifier" AS unique_id
FROM {{ ref('stg_apc_spell') }} sp
JOIN {{ ref('int_dominant_episode') }} dom
    ON sp.PRIMARYKEY_ID = dom.PRIMARYKEY_ID
JOIN {{ ref('int_first_episode') }} fe
    ON sp.PRIMARYKEY_ID = fe.PRIMARYKEY_ID
JOIN {{ source('stg_sus_apc_insertdeletelog') }} idl
    ON sp.PRIMARYKEY_ID = idl.PRIMARYKEY_ID
   AND sp."dmicImportLogId" = idl."dmicImportLogId"
   AND idl."dmicIsDeleted" = 0
