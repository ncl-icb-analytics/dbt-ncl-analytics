-- WNL residence neighbourhood: LSOA 2021 -> neighbourhood, all 13 WNL boroughs
-- (NCL + NWL). The WNL source is name-only ("INT"); the NCL neighbourhood code
-- is recovered by enrichment join so NCL consumers keep their code (NWL none).
select
    w.lsoa21_cd as lsoa_2021_code,
    w.lsoa21_nm as lsoa_2021_name,
    w.laname as borough,
    ncl.neighbourhood_code,
    nullif(trim(w."INT"), '') as neighbourhood_name,
    w.imd25_decil as imd_2025_decile
from {{ ref('raw_reference_wnlneighbourhoods') }} w
left join {{ ref('stg_reference_lookup_ncl_ncl_neighbourhood_lsoa_2021') }} ncl
    on w.lsoa21_cd = ncl.lsoa_2021_code
