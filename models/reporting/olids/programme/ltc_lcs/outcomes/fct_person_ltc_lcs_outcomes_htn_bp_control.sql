{{
    config(
        materialized='table',
        cluster_by=['person_id'],
        tags=['ltc_lcs', 'outcomes', 'hypertension'])
}}

-- LTC LCS Outcomes: Hypertension good blood pressure control - ROLLING (default).
-- Window: last known paired BP in the trailing 12 months ending today.
-- This is the "current position" view of the register's BP control.
-- See macro get_ltc_lcs_htn_bp_control for the control logic and threshold rules.

{{ get_ltc_lcs_htn_bp_control(
    window_start="dateadd(month, -12, current_date())",
    window_end="current_date()"
) }}
