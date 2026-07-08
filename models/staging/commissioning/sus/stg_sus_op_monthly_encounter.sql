{{
    config(materialized = 'view')
}}

select
     sk_encounter_id,
    row_id,
    sk_sus_data_mart_id,
    activity_period
from {{ ref('raw_sus_op_monthly_encounter') }}
