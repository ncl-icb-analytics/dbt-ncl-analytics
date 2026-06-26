select cast(practice_code as varchar) as practice_code
    , cast(neighbourhood_code as varchar) as neighbourhood_code
    , cast(practice_name as varchar) as practice_name
    , cast(neighbourhood_registered as varchar) as neighbourhood_registered 
    , cast(local_authority as varchar) as local_authority
from {{ref('raw_c_ltcs_cltcs_local_mapping_nh_gp')}}
where local_authority in ('Haringey', 'Camden')