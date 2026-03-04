{{ config(
    materialized='table') }}

-- Intermediate model for CVD-related medications for LTC LCS case finding
-- Includes statin medications, statin allergies/contraindications, and statin decisions
-- STAT_COD is a drugs cluster in this project (SCT_PREP medication issues), so
-- it is sourced via get_medication_orders rather than observation events.
-- Do not apply get_medication_orders source filtering here: in
-- stg_reference_combined_codesets, `source` is coding-system provenance.

{{ get_medication_orders(
    cluster_id="'STAT_COD'"
) }}
