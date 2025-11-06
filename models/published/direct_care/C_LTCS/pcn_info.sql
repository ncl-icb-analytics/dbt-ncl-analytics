select 
    -- practice details
      dp.practice_code
    , dp.practice_name
    , dp.borough_registered
    , ap.active_patient_count
    -- pcn details
    , dp.pcn_code
    , dp.pcn_name
    , dp.pcn_borough
    -- geography
    , dp.practice_msoa
    , dp.practice_latitude
    , dp.practice_longitude
    
from {{ref('dim_practice')}} dp
left join {{ref('fct_organisation_active_patients')}} ap on dp.organisation_id = ap.organisation_id 
where pcn_code in (select distinct pcn_code from {{source('c_ltcs','MDT_LOOKUP')}} )