{{
    config(materialized = 'view')
}}
select
    sk_encounter_id,
    procedure_number,
    procedure_code,
    procedure_date,
    activity_period
from {{ ref('raw_sus_op_monthly_encounterprocedure') }}
