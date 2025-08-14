{{ config(materialized='view') }}

/*
All emergency care activity from unified SUS ECDS data. Removing from ImportLog
*/

WITH removed AS (
    SELECT primarykey_id, dmicimportlogid
    FROM {{ ref('stg_sus_ae_insertdeletelog') }}
    WHERE dmicisdeleted = 0
)

SELECT a.*
FROM {{ ref('stg_sus_ae_emergency_care') }} AS a
INNER JOIN removed AS IDL 
    ON a.primarykey_id = IDL.primarykey_id
    AND a.dmicimportlogid = IDL.dmicimportlogid