{%
    set in_scope_borough_list = ['Haringey']
%}

with in_scope_practice_list as (
    select  local_authority, practice_code, neighbourhood_code, neighbourhood_registered
    from {{ ref('dim_practice_neighbourhood')}}
    where local_authority in (
        {% for b in in_scope_borough_list %}
            '{{ b }}'{% if not loop.last %}, {% endif %}
        {% endfor %})
)

select 
    -- practice details
      dp.practice_code
    , dp.practice_name
    , dp.borough_registered
    , ap.active_patient_count
    -- area details
    , ip.neighbourhood_code as area_code
    , ip.neighbourhood_registered as area_name
    , ip.local_authority as area_borough
    -- geography
    , dp.practice_msoa
    , dp.practice_latitude
    , dp.practice_longitude
    
from {{ref('dim_practice')}} dp
inner join in_scope_practice_list ip on dp.practice_code = ip.practice_code
left join {{ref('fct_organisation_active_patients')}} ap on dp.organisation_id = ap.organisation_id