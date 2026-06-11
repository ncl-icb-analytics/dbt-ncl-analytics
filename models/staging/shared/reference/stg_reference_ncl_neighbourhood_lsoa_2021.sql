select
    lsoa_2021_code,
    lsoa_2021_name,
    neighbourhood_code,
    neighbourhood_name,
    start_date
from {{ ref('raw_reference_ncl_neighbourhood_lsoa_2021') }}
