{{
    config(materialized = 'view')
}}
select
    sk_encounter_id,
    diagnosis_number,
    diagnosis_code,
    activity_period
from {{ ref('raw_sus_op_monthly_encounterdiagnosis') }}
