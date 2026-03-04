{{ config(
    materialized='table') }}

-- Intermediate model for CVD-related medications for LTC LCS case finding
-- Includes statin medications, statin allergies/contraindications, and statin decisions
-- STAT_COD is a drugs cluster in this project (SCT_PREP medication issues), so
-- it is sourced via get_medication_orders rather than observation events.

{{ get_medication_orders(
    cluster_id="'STAT_COD'",
    source='LTC_LCS'
) }}
