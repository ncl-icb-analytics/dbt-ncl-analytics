{{ 
    config(
        materialized='view',
        tags='daily')
    }}

select 
    *
from 
    {{ ref("fct_person_myria_high_risk_patients") }} 