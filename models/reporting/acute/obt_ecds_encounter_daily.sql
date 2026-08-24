{{
    config(
        materialized='view',
        tags=['daily']
    )
}}

-- Daily-managed view of the ECDS encounter reporting dataset.
-- The intermediate model contains the complete original encounter output plus
-- the additional fields introduced on this branch.
select *
from {{ ref('int_sus_uec_encounter') }}
