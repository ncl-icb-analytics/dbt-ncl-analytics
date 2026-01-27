-- PDS vs OLIDS: Identifier Bridge Test
-- Tests whether nhs_number_hash (binary) from OLIDS patient table
-- could match pseudo_nhs_number (sk_patient_id) from PDS
--
-- Split into parts to avoid long-running cross-system joins.
-- Run each part separately if needed.
--
-- Usage: dbt compile -s pds_olids_identifier_bridge_test

-- PART 1: What do the identifiers look like?
-- Shows sample values and data types side by side
SELECT
    id AS patient_id,
    sk_patient_id::VARCHAR AS sk_varchar,
    HEX_ENCODE(nhs_number_hash) AS hash_hex,
    nhs_number_hash::VARCHAR AS hash_varchar,
    LENGTH(sk_patient_id::VARCHAR) AS sk_length,
    LENGTH(HEX_ENCODE(nhs_number_hash)) AS hash_hex_length,
    LENGTH(nhs_number_hash::VARCHAR) AS hash_varchar_length,
    CASE WHEN sk_patient_id::VARCHAR = nhs_number_hash::VARCHAR THEN 'YES' ELSE 'NO' END AS sk_eq_hash_varchar,
    CASE WHEN sk_patient_id::VARCHAR = HEX_ENCODE(nhs_number_hash) THEN 'YES' ELSE 'NO' END AS sk_eq_hash_hex
FROM {{ ref('stg_olids_patient') }}
WHERE sk_patient_id IS NOT NULL AND nhs_number_hash IS NOT NULL
LIMIT 20
