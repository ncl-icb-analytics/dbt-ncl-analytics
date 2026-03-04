{{ config(materialized="view") }}

select 
    *
from 
    {{ ref("fct_person_myria_control_group") }} 